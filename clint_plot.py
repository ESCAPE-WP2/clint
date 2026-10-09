"""Plots for CLINT runs.

Reads the per-link ledgers (clint_ledger_<src>_to_<dst>.jsonl) that a run leaves in its
work folder and draws:

  clint_run_<label>.png   one run: volume injected / completed / wanted over time, data in
                          flight, time from rule to OK, and what happened per link
  clint_compare.png       several runs (for example one per student) on the same axes

The ledger is the only input. "Completed" means that CLINT saw the rule in state OK: it
checks once per cycle, so the time from rule to OK is an upper bound with the resolution
of one cycle, and the last rules of a run may be missing.

This module needs matplotlib (pip install matplotlib) but not rucio, so it also runs on a laptop.

Usage:
    python clint_plot.py [RUN_DIR ...] [--out OUT_DIR] [--label NAME ...]
"""

import argparse
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


def _link_name(link):
    return f'{link[0]}→{link[1]}'


def plot_run(links, label, path):
    """One figure for one run."""
    plt = _pyplot()
    t0 = run_start(links)
    if t0 is None:
        raise ValueError(f'{label}: no ledger events to plot')
    fig, ((ax_vol, ax_fly), (ax_lat, ax_cnt)) = plt.subplots(2, 2, figsize=(12, 8))
    colors = plt.rcParams['axes.prop_cycle'].by_key()['color']

    all_latencies, lifetime = [], None
    for i, (link, events) in enumerate(sorted(links.items())):
        color, name = colors[i % len(colors)], _link_name(link)
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

    ax_vol.set(title='Volume over time', xlabel='minutes since the start', ylabel='GB')
    ax_vol.legend(fontsize=7)
    ax_fly.set(title='Data in flight (rules not yet seen OK)', xlabel='minutes since the start', ylabel='MB')
    ax_fly.legend(fontsize=8)

    if all_latencies:
        ax_lat.hist([x / 60 for x in all_latencies], bins=12, color=colors[0])
    else:
        ax_lat.text(.5, .5, 'no rule was seen OK', ha='center', va='center', transform=ax_lat.transAxes)
    if lifetime:
        ax_lat.axvline(lifetime / 60, color='red', ls='--', label=f'rule lifetime ({lifetime // 60} min)')
        ax_lat.legend(fontsize=8)
    ax_lat.set(title='Time from rule to OK (upper bound, one cycle resolution)', xlabel='minutes', ylabel='rules')

    names = list(SUMMARY_EVENTS)
    width = 0.8 / max(len(links), 1)
    for i, (link, events) in enumerate(sorted(links.items())):
        counts = event_counts(events)
        ax_cnt.bar([k + i * width for k in range(len(names))], [counts[n] for n in names], width,
                   color=colors[i % len(colors)], label=_link_name(link))
    ax_cnt.set_xticks([k + width * (len(links) - 1) / 2 for k in range(len(names))])
    ax_cnt.set_xticklabels(names, rotation=25, ha='right', fontsize=8)
    ax_cnt.set(title='What happened', ylabel='events')
    ax_cnt.legend(fontsize=8)

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
    for label in labels:
        links = runs[label]
        t0 = run_start(links)
        if t0 is None:
            continue
        points = [(e['_t'], e.get('bytes', 0)) for events in links.values() for e in events if e['event'] == 'rule_ok']
        xs, ys = _cumulative(points, t0)
        ax_cum.step(xs, [y / 1e9 for y in ys], where='post', label=label)
    ax_cum.set(title='Completed volume, all links of a run', xlabel='minutes since the start of the run', ylabel='GB')
    ax_cum.legend(fontsize=8)

    summaries = [run_summary(runs[label]) for label in labels]
    xs = range(len(labels))
    ax_bar.bar([x - .2 for x in xs], [s['wanted_mbps'] for s in summaries], .4, color='lightgrey', label='wanted')
    ax_bar.bar([x + .2 for x in xs], [s['achieved_mbps'] for s in summaries], .4, label='completed')
    ax_bar.set_xticks(list(xs))
    ax_bar.set_xticklabels(labels, rotation=25, ha='right', fontsize=8)
    ax_bar.set(title='Average rate', ylabel='Mbps')
    ax_bar.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('folders', nargs='*', default=['.'], help='work folder of a run (default: the current folder)')
    ap.add_argument('--out', default='.', help='folder for the PNG files (default: the current folder)')
    ap.add_argument('--label', nargs='*', help='one name per folder (default: the folder name)')
    args = ap.parse_args(argv)

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
