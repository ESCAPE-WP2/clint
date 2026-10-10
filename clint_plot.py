"""Plots for CLINT runs.

Reads the per-link ledgers (clint_ledger_<src>_to_<dst>.jsonl) that a run leaves in its
work folder and draws:

  clint_run_<label>.png   one run: volume injected / completed / wanted over time, data in
                          flight, time from rule to OK, and what happened per link
  clint_compare.png       several runs (for example one per student) on the same axes
  clint_class.png         from the CSV files of clint_export.py of many participants: throughput of the
                          class against the target, achievement ratio per participant and link,
                          where the time goes, and the rules in flight

The ledger is the only input. "Completed" means that CLINT saw the rule in state OK: it
checks once per cycle, so the time from rule to OK is an upper bound with the resolution
of one cycle, and the last rules of a run may be missing.

This module needs matplotlib (pip install matplotlib) but not rucio, so it also runs on a laptop.

Usage:
    python clint_plot.py [RUN_DIR ...] [--out OUT_DIR] [--label NAME ...]
    python clint_plot.py --csv CSV_DIR [--out OUT_DIR]
"""

import argparse
import csv
import datetime
import glob
import json
import os
import re
import statistics
import sys

LEDGER_RE = re.compile(r'clint_ledger_(?P<src>.+?)_to_(?P<dst>.+)\.jsonl$')
MB = 1e6
MBPS_BYTES = 125000  # bytes per second in one Mbps

# Events counted in the per-link summary, in the order of the bars.
SUMMARY_EVENTS = ('rule_ok', 'rule_gone', 'skipped_bad', 'throttled', 'blocked_no_space', 'injection_failed')


def _ts(text):
    return datetime.datetime.fromisoformat(text)


def read_ledger(path):
    """Events of one ledger, in file order. A malformed line is skipped."""
    events = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                record['_t'] = _ts(record['ts'])
            except (ValueError, KeyError):
                continue
            events.append(record)
    return events


def load_run(folder):
    """{(src, dst): [events]} for every ledger in `folder`. Empty if there is none."""
    links = {}
    for path in sorted(glob.glob(os.path.join(folder, 'clint_ledger_*_to_*.jsonl'))):
        match = LEDGER_RE.match(os.path.basename(path))
        if match:
            links[(match['src'], match['dst'])] = read_ledger(path)
    return links


def run_start(links):
    """Time of the first event of any link, or None."""
    times = [e['_t'] for events in links.values() for e in events]
    return min(times) if times else None


def _cumulative(points, t0):
    """[(t, value)] -> ([minutes since t0], [running sum]) with a point at t0."""
    xs, ys, total = [0.0], [0.0], 0
    for t, value in sorted(points):
        total += value
        xs.append((t - t0).total_seconds() / 60)
        ys.append(total)
    return xs, ys


def link_series(events, t0):
    """Curves of one link, in minutes since t0 and bytes.

    injected   running sum of the bytes of the rules that CLINT made
    completed  running sum of the bytes of the rules that CLINT saw OK
    wanted     running sum of the bytes that the rate asked for, one step per cycle
    in_flight  injected - completed - gone, as (minutes, bytes)
    """
    injected = [(e['_t'], e.get('bytes', 0)) for e in events if e['event'] == 'injected' and e.get('rule_id') != 'dry-run']
    completed = [(e['_t'], e.get('bytes', 0)) for e in events if e['event'] == 'rule_ok']
    gone = [(e['_t'], e.get('bytes', 0)) for e in events if e['event'] == 'rule_gone']
    wanted = [(e['_t'], e.get('wanted_bytes', 0)) for e in events if e['event'] == 'cycle']

    flow = [(t, b) for t, b in injected] + [(t, -b) for t, b in completed] + [(t, -b) for t, b in gone]
    return {
        'injected': _cumulative(injected, t0),
        'completed': _cumulative(completed, t0),
        'wanted': _cumulative(wanted, t0),
        'in_flight': _cumulative(flow, t0),
    }


def latencies(events):
    """Seconds from the rule to the first time CLINT saw it OK, for each rule that has both."""
    made = {e['rule_id']: e['_t'] for e in events if e['event'] == 'injected' and e.get('rule_id') != 'dry-run'}
    return [(e['_t'] - made[e['rule_id']]).total_seconds()
            for e in events if e['event'] == 'rule_ok' and e.get('rule_id') in made]


def event_counts(events):
    counts = {name: 0 for name in SUMMARY_EVENTS}
    for e in events:
        if e['event'] in counts:
            counts[e['event']] += 1
    return counts


def cycle_seconds(events):
    """Median time between two cycles of a link, or 0 if there are fewer than two."""
    times = [e['_t'] for e in events if e['event'] == 'cycle']
    gaps = [(b - a).total_seconds() for a, b in zip(times, times[1:])]
    return statistics.median(gaps) if gaps else 0.0


def run_summary(links):
    """Average rates of a run, summed over its links.

    Active time goes from the first cycle to the last cycle plus one cycle. All of it is
    counted, so a pause between two phases in the same folder lowers the averages.
    """
    achieved = wanted = 0.0
    for events in links.values():
        cycles = [e['_t'] for e in events if e['event'] == 'cycle']
        if not cycles:
            continue
        duration = (max(cycles) - min(cycles)).total_seconds() + cycle_seconds(events)
        if duration <= 0:
            continue
        achieved += sum(e.get('bytes', 0) for e in events if e['event'] == 'rule_ok') / duration / MBPS_BYTES
        wanted += sum(e.get('wanted_bytes', 0) for e in events if e['event'] == 'cycle') / duration / MBPS_BYTES
    return {'achieved_mbps': achieved, 'wanted_mbps': wanted}


def _pyplot():
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
    except ImportError:
        raise SystemExit('Error: matplotlib is needed for the plots: pip install matplotlib')
    return plt


def _link_names(links):
    """Names for the legends. When all links start at one source, only the destination is shown."""
    sources = {src for src, _ in links}
    if len(sources) == 1:
        return {link: link[1] for link in links}, f'from {next(iter(sources))}'
    return {link: f'{link[0]}→{link[1]}' for link in links}, ''


def _palette(plt, n):
    """n distinguishable colours: the default cycle up to 10, then tab20, then a colour map."""
    if n <= 10:
        return plt.rcParams['axes.prop_cycle'].by_key()['color']
    if n <= 20:
        return list(plt.get_cmap('tab20').colors)
    return [plt.get_cmap('viridis')(i / (n - 1)) for i in range(n)]


def _no_data(ax, text):
    """Replace the content of an axes by a short message, without ticks."""
    ax.set_xticks([])
    ax.set_yticks([])
    ax.text(.5, .5, text, ha='center', va='center', transform=ax.transAxes, color='grey')


def plot_run(links, label, path):
    """One figure for one run."""
    plt = _pyplot()
    t0 = run_start(links)
    if t0 is None:
        raise ValueError(f'{label}: no ledger events to plot')
    fig, ((ax_vol, ax_fly), (ax_lat, ax_cnt)) = plt.subplots(2, 2, figsize=(12, 8))
    colors = _palette(plt, len(links))
    names, origin = _link_names(links)

    all_latencies, lifetime = [], None
    for i, (link, events) in enumerate(sorted(links.items())):
        color, name = colors[i % len(colors)], names[link]
        width = max(2.6 - 1.2 * i, 1.2)  # links with identical data stay visible
        s = link_series(events, t0)
        ax_vol.step(s['wanted'][0], [b / 1e9 for b in s['wanted'][1]], where='post', ls='--', color=color, alpha=.6,
                    label=f'{name} wanted')
        ax_vol.step(s['injected'][0], [b / 1e9 for b in s['injected'][1]], where='post', ls=':', color=color,
                    label=f'{name} injected')
        ax_vol.step(s['completed'][0], [b / 1e9 for b in s['completed'][1]], where='post', color=color, lw=width,
                    label=f'{name} completed')
        ax_fly.step(s['in_flight'][0], [b / MB for b in s['in_flight'][1]], where='post', color=color, lw=width,
                    label=name)
        all_latencies += latencies(events)
        for e in events:
            if e['event'] == 'injected' and e.get('lifetime'):
                lifetime = e['lifetime']

    ax_vol.set(title=f'Volume over time {origin}'.strip(), xlabel='minutes since the start', ylabel='GB')
    ax_vol.legend(fontsize=7)
    ax_fly.set(title='Data in flight (rules not yet seen OK)', xlabel='minutes since the start', ylabel='MB')
    if any(max(link_series(ev, t0)['in_flight'][1]) > 0 for ev in links.values()):
        ax_fly.legend(fontsize=8)
    else:
        _no_data(ax_fly, 'no rule in flight')

    ax_lat.set(title='Time from rule to OK (upper bound, one cycle resolution)')
    if all_latencies:
        ax_lat.hist([x / 60 for x in all_latencies], bins=12, color=colors[0])
        ax_lat.set(xlabel='minutes', ylabel='rules')
        if lifetime:
            ax_lat.axvline(lifetime / 60, color='red', ls='--', label=f'rule lifetime ({lifetime // 60} min)')
            ax_lat.legend(fontsize=8)
    else:
        _no_data(ax_lat, 'no rule was seen OK')

    kinds = list(SUMMARY_EVENTS)
    width = 0.8 / max(len(links), 1)
    all_counts = {link: event_counts(events) for link, events in links.items()}
    ax_cnt.set(title='What happened')
    if any(any(c.values()) for c in all_counts.values()):
        for i, (link, counts) in enumerate(sorted(all_counts.items())):
            ax_cnt.bar([k + i * width for k in range(len(kinds))], [counts[n] for n in kinds], width,
                       color=colors[i % len(colors)], label=names[link])
        ax_cnt.set_xticks([k + width * (len(links) - 1) / 2 for k in range(len(kinds))])
        ax_cnt.set_xticklabels(kinds, rotation=25, ha='right', fontsize=8)
        ax_cnt.set_ylabel('events')
        ax_cnt.legend(fontsize=8)
    else:
        _no_data(ax_cnt, 'no events')

    summary = run_summary(links)
    fig.suptitle(f'CLINT run {label}: wanted {summary["wanted_mbps"]:.1f} Mbps, '
                 f'completed {summary["achieved_mbps"]:.1f} Mbps on average')
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def plot_compare(runs, path):
    """One figure for several runs: {label: links}."""
    plt = _pyplot()
    fig, (ax_cum, ax_bar) = plt.subplots(1, 2, figsize=(12, 5))
    labels = sorted(runs)
    colors = _palette(plt, len(labels))
    for i, label in enumerate(labels):
        links = runs[label]
        t0 = run_start(links)
        if t0 is None:
            continue
        points = [(e['_t'], e.get('bytes', 0)) for events in links.values() for e in events if e['event'] == 'rule_ok']
        xs, ys = _cumulative(points, t0)
        ax_cum.step(xs, [y / 1e9 for y in ys], where='post', lw=1.4, color=colors[i % len(colors)], label=label)
    ax_cum.set(title='Completed volume, all links of a run', xlabel='minutes since the start of the run', ylabel='GB')
    ax_cum.legend(fontsize=7 if len(labels) > 8 else 8, ncol=2 if len(labels) > 8 else 1, loc='upper left')

    summaries = [run_summary(runs[label]) for label in labels]
    xs = range(len(labels))
    ax_bar.bar([x - .2 for x in xs], [s['wanted_mbps'] for s in summaries], .4, color='lightgrey', label='wanted')
    ax_bar.bar([x + .2 for x in xs], [s['achieved_mbps'] for s in summaries], .4, label='completed')
    ax_bar.set_xticks(list(xs))
    ax_bar.set_xticklabels(labels, rotation=25, ha='right', fontsize=8)
    ax_bar.set(title='Average rate', ylabel='Mbps')
    ax_bar.set_ylim(0, max([s['wanted_mbps'] for s in summaries] + [s['achieved_mbps'] for s in summaries] + [1]) * 1.25)
    ax_bar.legend(fontsize=8, loc='upper right', ncol=2)
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


# --- Class plots from the CSV files of clint_export.py ---------------------------------------------------

# Colour bands of the DC24 report for observed / expected: green >= 0.9, yellow >= 0.7, orange >= 0.5, red below.
RATIO_BANDS = ((0.9, 'green'), (0.7, 'gold'), (0.5, 'orange'), (0.0, 'red'))
STAGES = (('conveyor_s', 'rule waits for the submitter'), ('fts_queue_s', 'queue in FTS'),
          ('transfer_s', 'transfer'), ('after_transfer_s', 'poller and finisher'))


def ratio_color(ratio):
    for limit, color in RATIO_BANDS:
        if ratio >= limit:
            return color
    return 'red'


def _num(text):
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _time(text):
    return datetime.datetime.fromisoformat(text) if text else None


def load_csv_class(folder):
    """(rules, cycles) from every clint_rules_*.csv and clint_cycles_*.csv in `folder`."""
    rules, cycles = [], []
    for pattern, target in (('clint_rules_*.csv', rules), ('clint_cycles_*.csv', cycles)):
        for path in sorted(glob.glob(os.path.join(folder, pattern))):
            with open(path, newline='') as f:
                target.extend(csv.DictReader(f))
    for r in rules:
        r['bytes'] = _num(r.get('bytes')) or 0
        r['_inj'] = _time(r.get('injected_at'))
        # A request that Rucio closed is complete. Else CLINT may have seen the rule OK.
        r['_done'] = _time(r.get('closed_at')) or _time(r.get('ok_seen_at'))
    for c in cycles:
        c['wanted_bytes'] = _num(c.get('wanted_bytes')) or 0
        c['_t'] = _time(c.get('cycle_at'))
        c['_mbps'] = _num(c.get('link_mbps'))
        c['_interval'] = _num(c.get('interval_s'))
    return rules, cycles


def class_series(rules, cycles, bin_s=120):
    """Per time bin: completed bytes by participant, and wanted bytes of the class.

    Returns (t0, edges_in_minutes, {participant: [bytes per bin]}, [wanted bytes per bin]).
    """
    stamps = [r['_inj'] for r in rules if r['_inj']] + [c['_t'] for c in cycles if c['_t']]
    if not stamps:
        return None, [], {}, []
    t0 = min(stamps)
    ends = [r['_done'] for r in rules if r['_done']] + stamps
    n = int((max(ends) - t0).total_seconds() // bin_s) + 1
    done = {}
    for r in rules:
        if r['_done']:
            done.setdefault(r['participant'], [0.0] * n)[int((r['_done'] - t0).total_seconds() // bin_s)] += r['bytes']
    wanted = _spread_wanted(cycles, t0, bin_s, n)
    return t0, [i * bin_s / 60 for i in range(n + 1)], done, wanted


def _spread_wanted(cycles, t0, bin_s, n):
    """Wanted bytes per bin. The bytes of a cycle are spread evenly over the length of the cycle, so that
    cycles of different length (the two phases) give a steady target rate. Needs link_mbps and interval_s
    in the cycle rows for an exact result; older files use an estimate."""
    wanted = [0.0] * n
    by_link = {}
    for c in cycles:
        if c['_t']:
            by_link.setdefault((c['participant'], c['src'], c['dst']), []).append(c)
    for link_cycles in by_link.values():
        link_cycles.sort(key=lambda c: c['_t'])
        gaps = [(b['_t'] - a['_t']).total_seconds() for a, b in zip(link_cycles, link_cycles[1:])]
        for i, c in enumerate(link_cycles):
            start = (c['_t'] - t0).total_seconds()
            # The interval of a cycle is the larger gap next to it: the last cycle of a run that stops early
            # is followed by a shorter gap, and the first cycle of the next run is preceded by one.
            if c['_mbps'] is not None and c['_interval']:
                # exact: the link target, for as long as the cycle ran (a run that stops early cuts its last cycle)
                period = min(c['_interval'], gaps[i]) if i < len(gaps) else c['_interval']
                rate = c['_mbps'] * 125000
            else:
                # older ledgers: the larger gap next to the cycle
                neighbours = gaps[max(i - 1, 0):i + 1]
                period = max(neighbours) if neighbours else bin_s
                rate = c['wanted_bytes'] / period if period > 0 else 0
            if period <= 0:
                continue
            for b in range(int(start // bin_s), min(int((start + period) // bin_s) + 1, n)):
                overlap = min(start + period, (b + 1) * bin_s) - max(start, b * bin_s)
                if overlap > 0:
                    wanted[b] += rate * overlap
    return wanted


def run_ends(cycles):
    """{(participant, link): end of the injection} = time of the last cycle plus one cycle."""
    by_link = {}
    for c in cycles:
        if c['_t']:
            by_link.setdefault((c['participant'], c['dst']), []).append(c)
    ends = {}
    for key, link_cycles in by_link.items():
        link_cycles.sort(key=lambda c: c['_t'])
        gaps = [(b['_t'] - a['_t']).total_seconds() for a, b in zip(link_cycles, link_cycles[1:])]
        last = link_cycles[-1]
        interval = last['_interval'] or (statistics.median(gaps) if gaps else 0)
        ends[key] = last['_t'] + datetime.timedelta(seconds=interval)
    return ends


def achievement(rules, cycles):
    """{(participant, link): completed bytes / wanted bytes}. A link without wanted bytes is left out.

    Only rules that finished before the injection of their link ended count as completed: a rule that
    finishes minutes later does not help the rate that the link had during the run (the report also
    measures the rate per interval, not in the end).
    """
    wanted, completed = {}, {}
    ends = run_ends(cycles)
    for c in cycles:
        wanted[(c['participant'], c['dst'])] = wanted.get((c['participant'], c['dst']), 0) + c['wanted_bytes']
    for r in rules:
        key = (r['participant'], r['dst'])
        if r['_done'] and (key not in ends or r['_done'] <= ends[key]):
            completed[key] = completed.get(key, 0) + r['bytes']
    return {k: completed.get(k, 0) / w for k, w in wanted.items() if w > 0}


def stage_medians(rules):
    """{participant: {stage column: median seconds}} for participants that have stage times."""
    out = {}
    for who in sorted({r['participant'] for r in rules}):
        values = {}
        for column, _ in STAGES:
            data = [_num(r.get(column)) for r in rules if r['participant'] == who and _num(r.get(column)) is not None]
            if data:
                values[column] = statistics.median(data)
        if values:
            out[who] = values
    return out


def in_flight(rules, t0):
    """([minutes], [rules in flight]) for the whole class. A rule without a known end ends at its expiry."""
    points = []
    for r in rules:
        if not r['_inj']:
            continue
        end = r['_done'] or (r['_inj'] + datetime.timedelta(seconds=_num(r.get('lifetime_s')) or 0))
        points += [(r['_inj'], 1), (end, -1)]
    return _cumulative(points, t0)


def plot_class(rules, cycles, path):
    """One figure for the whole class."""
    plt = _pyplot()
    t0, edges, done, wanted = class_series(rules, cycles)
    if t0 is None:
        raise ValueError('no rows to plot')
    who = sorted({r['participant'] for r in rules})
    colors = _palette(plt, len(who))
    fig, ((ax_thr, ax_hm), (ax_stage, ax_fly)) = plt.subplots(2, 2, figsize=(13, 9))
    bin_s = (edges[1] - edges[0]) * 60 if len(edges) > 1 else 120
    to_mbps = 8 / 1e6 / bin_s

    mids = [(edges[i] + edges[i + 1]) / 2 for i in range(len(edges) - 1)]
    series = [[b * to_mbps for b in done.get(p, [0.0] * len(mids))] for p in who]
    if any(any(s) for s in series):
        ax_thr.stackplot(mids, series, labels=who, colors=colors[:len(who)], step='mid')
    ax_thr.step(edges[:-1], [b * to_mbps for b in wanted], where='post', color='black', ls='--', lw=1.6, label='target')
    ax_thr.set(title=f'Completed rate of the class ({bin_s / 60:.0f}-minute bins)', xlabel='minutes since the start',
               ylabel='Mbps')
    ax_thr.legend(fontsize=6, ncol=2 if len(who) > 8 else 1, loc='upper left')

    ratios = achievement(rules, cycles)
    links = sorted({k[1] for k in ratios})
    if ratios:
        for i, p in enumerate(who):
            for j, link in enumerate(links):
                value = ratios.get((p, link))
                if value is None:
                    continue
                ax_hm.add_patch(plt.Rectangle((j, i), 1, 1, color=ratio_color(value)))
                ax_hm.text(j + .5, i + .5, f'{value:.2f}', ha='center', va='center', fontsize=8)
        ax_hm.set_xlim(0, len(links))
        ax_hm.set_ylim(len(who), 0)
        ax_hm.set_xticks([j + .5 for j in range(len(links))])
        ax_hm.set_xticklabels(links, fontsize=8)
        ax_hm.set_yticks([i + .5 for i in range(len(who))])
        ax_hm.set_yticklabels(who, fontsize=8)
        ax_hm.set_title('Achievement: finished during the run / wanted\n(green ≥ .9, yellow ≥ .7, orange ≥ .5, red < .5)', fontsize=9)
    else:
        _no_data(ax_hm, 'no cycles in the files')

    medians = stage_medians(rules)
    if medians:
        left = [0.0] * len(medians)
        names = list(medians)
        for k, (column, label) in enumerate(STAGES):
            widths = [medians[n].get(column, 0) for n in names]
            ax_stage.barh(names, widths, left=left, label=label, color=plt.get_cmap('Set2')(k))
            left = [a + b for a, b in zip(left, widths)]
        ax_stage.invert_yaxis()
        ax_stage.tick_params(axis='y', labelsize=8)
        ax_stage.set(title='Where the time goes (median per rule)', xlabel='seconds')
        ax_stage.legend(fontsize=7, loc='lower right')
    else:
        _no_data(ax_stage, 'no stage times: run clint_export.py with Rucio access')

    xs, ys = in_flight(rules, t0)
    ax_fly.step(xs, ys, where='post', color='tab:blue')
    ax_fly.set(title='Rules in flight, whole class', xlabel='minutes since the start', ylabel='rules')

    total_done = sum(r['bytes'] for r in rules if r['_done']) / 1e9
    total_wanted = sum(c['wanted_bytes'] for c in cycles) / 1e9
    fig.suptitle(f'CLINT data challenge: {len(who)} participant{"s" if len(who) != 1 else ""}, '
                 f'{total_done:.1f} GB finished in the end, {total_wanted:.1f} GB wanted')
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('folders', nargs='*', default=['.'], help='work folder of a run (default: the current folder)')
    ap.add_argument('--out', default='.', help='folder for the PNG files (default: the current folder)')
    ap.add_argument('--label', nargs='*', help='one name per folder (default: the folder name)')
    ap.add_argument('--csv', metavar='DIR', help='folder with the clint_rules_*.csv and clint_cycles_*.csv files of many '
                    'participants: draws clint_class.png instead of the run figures')
    args = ap.parse_args(argv)

    if args.csv:
        rules, cycles = load_csv_class(args.csv)
        if not rules and not cycles:
            print(f'Error: no clint_rules_*.csv or clint_cycles_*.csv in {args.csv}', file=sys.stderr)
            return 1
        os.makedirs(args.out, exist_ok=True)
        path = os.path.join(args.out, 'clint_class.png')
        plot_class(rules, cycles, path)
        print(path)
        return 0

    if args.label and len(args.label) != len(args.folders):
        ap.error('give one --label for each folder')
    labels = args.label or [os.path.basename(os.path.abspath(f)) for f in args.folders]
    if len(set(labels)) != len(labels):
        ap.error('the labels must differ: use --label')

    runs = {}
    for folder, label in zip(args.folders, labels):
        links = load_run(folder)
        if not links:
            print(f'Warning: no clint_ledger_*.jsonl in {folder}: skipped', file=sys.stderr)
            continue
        runs[label] = links
    if not runs:
        print('Error: nothing to plot', file=sys.stderr)
        return 1

    os.makedirs(args.out, exist_ok=True)
    for label, links in runs.items():
        path = os.path.join(args.out, f'clint_run_{label}.png')
        plot_run(links, label, path)
        print(path)
    if len(runs) > 1:
        path = os.path.join(args.out, 'clint_compare.png')
        plot_compare(runs, path)
        print(path)
    return 0


if __name__ == '__main__':
    sys.exit(main())
