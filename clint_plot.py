"""Plots for CLINT runs, in the style of mplhep (https://scikit-hep.org/mplhep).

One figure per call:

  one run     clint_run_<label>.png   completed rate against the target, and where the time goes
  many runs   clint_class.png         the same, plus the achievement ratio of each participant and link

Input is either the work folders of the runs (the ledgers clint_ledger_<src>_to_<dst>.jsonl), or a folder with
the CSV files of clint_export.py of many participants (--csv). The CSV files have the stage times from Rucio:
conveyor, queue in FTS, transfer, and the time until Rucio closes the request. Ledgers alone only know when
CLINT saw the rule in state OK, so the last panel then shows that time (an upper bound, with the resolution of
one cycle).

Needs matplotlib. With mplhep (pip install mplhep) the figures get the HEP look; without it they get the same
layout in the plain style. rucio is not needed, so it also runs on a laptop.

Usage:
    python clint_plot.py [RUN_DIR ...] [--out OUT_DIR] [--label NAME ...] [--align]
    python clint_plot.py --csv CSV_DIR [--out OUT_DIR] [--align]
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
MBPS_BYTES = 125000  # bytes per second in one Mbps

# Colour bands of the WLCG/DOMA Data Challenge 2024 report for observed / expected.
RATIO_BANDS = ((0.9, 'green'), (0.7, 'gold'), (0.5, 'orange'), (0.0, 'red'))
STAGES = (('conveyor_s', 'rule waits for the submitter'), ('fts_queue_s', 'queue in FTS'),
          ('transfer_s', 'transfer'), ('after_transfer_s', 'Rucio notices and closes it'))
SEEN = ('seen_latency_s', 'until CLINT saw it OK')  # used when there are no stage times
MAX_STACKED = 6  # more participants than this are drawn as one block in the rate panel


# --- Reading ------------------------------------------------------------------------------------------------

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


def _num(text):
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _time(text):
    return datetime.datetime.fromisoformat(text) if text else None


def _typed(rules, cycles):
    """Add parsed values (names with a leading underscore) to the rows of the export, in place."""
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


def load_csv_class(folder):
    """(rules, cycles) from every clint_rules_*.csv and clint_cycles_*.csv in `folder`."""
    rules, cycles = [], []
    for pattern, target in (('clint_rules_*.csv', rules), ('clint_cycles_*.csv', cycles)):
        for path in sorted(glob.glob(os.path.join(folder, pattern))):
            with open(path, newline='') as f:
                target.extend(csv.DictReader(f))
    return _typed(rules, cycles)


def align_starts(rules, cycles):
    """Shift the times of each participant so that all start together, in place. For runs that were made at
    different times; a class that started together does not need it."""
    first = {}
    for r in rules:
        if r['_inj']:
            first[r['participant']] = min(first.get(r['participant'], r['_inj']), r['_inj'])
    for c in cycles:
        if c['_t']:
            first[c['participant']] = min(first.get(c['participant'], c['_t']), c['_t'])
    if not first:
        return rules, cycles
    start = min(first.values())
    for r in rules:
        shift = first.get(r['participant'], start) - start
        for key in ('_inj', '_done'):
            if r[key]:
                r[key] -= shift
    for c in cycles:
        if c['_t']:
            c['_t'] -= first.get(c['participant'], start) - start
    return rules, cycles


def load_run_from_events(events, link=('S', 'D')):
    """{link: events} for events that are already in memory (dicts of a ledger, with 'ts'). For tests and scripts."""
    return {link: [dict(e, _t=_ts(e['ts'])) for e in events]}


def rows_from_ledgers(runs):
    """(rules, cycles) from {label: links}: the same rows as the CSV export, without stage times."""
    import clint_export  # not at the top: clint_export imports this module
    rules, cycles = [], []
    for label, links in runs.items():
        rules += clint_export.rule_rows(links, label)
        cycles += clint_export.cycle_rows(links, label)
    return _typed(rules, cycles)


# --- Numbers behind the figures -------------------------------------------------------------------------------

def ratio_color(ratio):
    for limit, color in RATIO_BANDS:
        if ratio >= limit:
            return color
    return 'red'


def class_series(rules, cycles, bin_s=120):
    """Per time bin: completed bytes by participant, and wanted bytes of everybody.

    Returns (t0, edges in minutes, {participant: [bytes per bin]}, [wanted bytes per bin]).
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
    return t0, [i * bin_s / 60 for i in range(n + 1)], done, _spread_wanted(cycles, t0, bin_s, n)


def _spread_wanted(cycles, t0, bin_s, n):
    """Wanted bytes per bin, as a steady rate. A cycle asks for link_mbps for the time that it ran: its
    interval, or less if the run stopped or the next phase began. Needs link_mbps and interval_s in the
    cycle rows; older files use the larger gap next to the cycle as an estimate."""
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
            if c['_mbps'] is not None and c['_interval']:
                period = min(c['_interval'], gaps[i]) if i < len(gaps) else c['_interval']
                rate = c['_mbps'] * MBPS_BYTES
            else:
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
    finishes minutes later does not help the rate that the link had during the run. The report also
    measures the rate per interval, not in the end.
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
    """{participant: {column: median seconds}}. Stage times if there are any, else the time until CLINT saw OK."""
    out = {}
    for who in sorted({r['participant'] for r in rules}):
        mine = [r for r in rules if r['participant'] == who]
        values = {}
        for column, _ in STAGES:
            data = [_num(r.get(column)) for r in mine if _num(r.get(column)) is not None]
            if data:
                values[column] = statistics.median(data)
        if not values:
            data = [_num(r.get(SEEN[0])) for r in mine if _num(r.get(SEEN[0])) is not None]
            if data:
                values[SEEN[0]] = statistics.median(data)
        if values:
            out[who] = values
    return out


# --- Drawing --------------------------------------------------------------------------------------------------

def _setup():
    """(pyplot, mplhep or None). The HEP style is applied when mplhep is there."""
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
    except ImportError:
        raise SystemExit('Error: matplotlib is needed for the plots: pip install matplotlib mplhep')
    try:
        import mplhep as hep
        hep.style.use('CMS')
    except ImportError:
        hep = None
        plt.rcParams.update({'xtick.direction': 'in', 'ytick.direction': 'in', 'xtick.top': True, 'ytick.right': True})
    plt.rcParams.update({'font.size': 14, 'axes.labelsize': 15, 'legend.fontsize': 11, 'figure.dpi': 100})
    return plt, hep


def _palette(plt, n):
    """n distinguishable colours: the default cycle up to 10, then tab20, then a colour map."""
    if n <= 10:
        return plt.rcParams['axes.prop_cycle'].by_key()['color']
    if n <= 20:
        return list(plt.get_cmap('tab20').colors)
    return [plt.get_cmap('viridis')(i / (n - 1)) for i in range(n)]


def _label(hep, ax, text):
    if hep is not None:
        try:
            hep.label.exp_text(exp='CLINT', text=text, loc=0, ax=ax)
            return
        except Exception:  # the label is decoration: an older mplhep may not have it
            pass
    ax.set_title(f'CLINT {text}', loc='left', fontsize=13)


def _no_data(ax, text):
    ax.text(.5, .5, text, ha='center', va='center', transform=ax.transAxes, color='grey')
    ax.set_xticks([])
    ax.set_yticks([])


def _draw_rate(plt, hep, ax, edges, done, wanted, who, bin_s, text):
    to_mbps = 8 / 1e6 / bin_s
    n = len(edges) - 1
    per_person = [[b * to_mbps for b in done.get(p, [0.0] * n)] for p in who]
    if len(who) > MAX_STACKED:
        per_person, names = [[sum(col) for col in zip(*per_person)]], ['completed (all)']
    else:
        names = [f'{p} completed' for p in who] if len(who) > 1 else ['completed']
    colors = _palette(plt, len(per_person))
    target = [b * to_mbps for b in wanted]
    if hep is not None:
        hep.histplot(per_person, bins=edges, stack=True, histtype='fill', label=names, color=colors[:len(per_person)], ax=ax)
        hep.histplot(target, bins=edges, histtype='step', color='black', linestyle='--', linewidth=2, label='target', ax=ax)
    else:
        bottom = [0.0] * n
        for series, name, color in zip(per_person, names, colors):
            top = [b + s for b, s in zip(bottom, series)]
            ax.stairs(top, edges, baseline=bottom, fill=True, label=name, color=color)
            bottom = top
        ax.stairs(target, edges, color='black', linestyle='--', linewidth=2, label='target')
    ax.set_xlabel('time since the start [min]')
    ax.set_ylabel(f'completed rate [Mbps], {bin_s / 60:.0f}-min bins')
    ax.set_xlim(edges[0], edges[-1])
    ax.set_ylim(0, ax.get_ylim()[1] * 1.45)
    ax.legend(loc='upper right', ncol=2 if len(names) > 3 else 1, frameon=False)
    _label(hep, ax, text)


def _draw_ratio(plt, ax, rules, cycles, who):
    from matplotlib.colors import BoundaryNorm, ListedColormap
    ratios = achievement(rules, cycles)
    links = sorted({k[1] for k in ratios})
    if not ratios:
        _no_data(ax, 'no cycles in the files')
        return
    grid = [[ratios.get((p, link), float('nan')) for link in links] for p in who]
    cmap = ListedColormap(['red', 'orange', 'gold', 'green'])
    top = max(1.2, max(v for row in grid for v in row if v == v))
    mesh = ax.pcolormesh(grid, cmap=cmap, norm=BoundaryNorm([0, .5, .7, .9, top], cmap.N), edgecolors='white', linewidth=2)
    for i, row in enumerate(grid):
        for j, value in enumerate(row):
            if value == value:
                ax.text(j + .5, i + .5, f'{value:.2f}', ha='center', va='center', fontsize=12)
    ax.set_xticks([j + .5 for j in range(len(links))])
    ax.set_xticklabels(links, fontsize=12)
    ax.set_yticks([i + .5 for i in range(len(who))])
    ax.set_yticklabels(who, fontsize=11)
    ax.invert_yaxis()
    ax.tick_params(which='both', length=0)
    ax.set_title('finished during the run / wanted', fontsize=13)
    cbar = ax.figure.colorbar(mesh, ax=ax, ticks=[.25, .6, .8, (.9 + top) / 2], fraction=.06, pad=.03)
    cbar.ax.set_yticklabels(['< 0.5', '0.5 - 0.7', '0.7 - 0.9', '≥ 0.9'], fontsize=10)
    cbar.ax.tick_params(length=0)


def _draw_stages(plt, ax, rules, who):
    medians = stage_medians(rules)
    if not medians:
        _no_data(ax, 'no rule was seen OK')
        return
    names = [p for p in who if p in medians]
    stages = [s for s in STAGES if any(s[0] in medians[n] for n in names)] or [SEEN]
    colors = plt.get_cmap('Set2')
    left = [0.0] * len(names)
    for k, (column, label) in enumerate(stages):
        widths = [medians[n].get(column, 0) for n in names]
        ax.barh(range(len(names)), widths, left=left, label=label, color=colors(k), height=.7)
        left = [a + b for a, b in zip(left, widths)]
    ax.set_ylim(max(len(names), 4) - .5, -.5)  # at least four rows, so that one bar does not fill the panel
    ax.set_xlabel('median time per rule [s]')
    ax.set_yticks(range(len(names)))
    ax.set_yticklabels(names, fontsize=11)
    ax.legend(loc='lower right', frameon=False, fontsize=10)


def plot_summary(rules, cycles, path, label=''):
    """One figure: the rate panel and the stage panel, plus the achievement grid for more than one participant."""
    plt, hep = _setup()
    t0, edges, done, wanted = class_series(rules, cycles)
    if t0 is None:
        raise ValueError('no rows to plot')
    who = sorted({r['participant'] for r in rules} | {c['participant'] for c in cycles})
    bin_s = (edges[1] - edges[0]) * 60 if len(edges) > 1 else 120

    if len(who) > 1:
        fig = plt.figure(figsize=(15, 10.5))
        grid = fig.add_gridspec(2, 2, height_ratios=[1.1, 1], width_ratios=[1, 1])
        ax_rate = fig.add_subplot(grid[0, :])
        ax_ratio = fig.add_subplot(grid[1, 0])
        ax_stage = fig.add_subplot(grid[1, 1])
        _draw_ratio(plt, ax_ratio, rules, cycles, who)
        text = f'mini data challenge, {len(who)} participants'
    else:
        fig, (ax_rate, ax_stage) = plt.subplots(1, 2, figsize=(15, 6), gridspec_kw={'width_ratios': [1.6, 1]})
        text = f'mini data challenge, {label or who[0]}'
    _draw_rate(plt, hep, ax_rate, edges, done, wanted, who, bin_s, text)
    _draw_stages(plt, ax_stage, rules, who)
    fig.tight_layout()
    fig.savefig(path, dpi=110)
    plt.close(fig)


# --- Command line -----------------------------------------------------------------------------------------------

def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('folders', nargs='*', default=['.'], help='work folder of a run (default: the current folder)')
    ap.add_argument('--out', default='.', help='folder for the PNG file (default: the current folder)')
    ap.add_argument('--label', nargs='*', help='one name per folder (default: the folder name)')
    ap.add_argument('--align', action='store_true',
                    help='start all runs at the same time (for runs that were made at different times)')
    ap.add_argument('--csv', metavar='DIR', help='folder with the clint_rules_*.csv and clint_cycles_*.csv files of '
                    'many participants, instead of work folders')
    args = ap.parse_args(argv)
    os.makedirs(args.out, exist_ok=True)

    if args.csv:
        rules, cycles = load_csv_class(args.csv)
        if not rules and not cycles:
            print(f'Error: no clint_rules_*.csv or clint_cycles_*.csv in {args.csv}', file=sys.stderr)
            return 1
        if args.align:
            align_starts(rules, cycles)
        path = os.path.join(args.out, 'clint_class.png')
        plot_summary(rules, cycles, path)
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
    rules, cycles = rows_from_ledgers(runs)
    if args.align:
        align_starts(rules, cycles)
    first = next(iter(runs))
    path = os.path.join(args.out, f'clint_run_{first}.png' if len(runs) == 1 else 'clint_class.png')
    plot_summary(rules, cycles, path, label=first if len(runs) == 1 else '')
    print(path)
    return 0


if __name__ == '__main__':
    sys.exit(main())
