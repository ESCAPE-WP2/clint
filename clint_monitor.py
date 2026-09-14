"""Deterministic collector for CLINT.

Reads each link's ledger (written by clint.py's run_transfer_thread) plus
the live Rucio state of the rules it recorded, and appends one scorecard
per link to a JSONL file. Sets or clears an anomaly flag file against
fixed thresholds. No LLM involved -- this is the "sensing" half of the
observe+report split; a separate agent does "interpretation" from the
scorecards, reading them from disk.

Every invocation is a fresh process: history lives in --out
(scorecards.jsonl), not in memory, so an hourly cron/condor invocation
always starts cold and rebuilds its view of trends from that file.
"""

import argparse
import datetime
import json
import os
import sys

import rucio.client

import clint

RULE_STATE_OK = 'OK'
RULE_STATE_REPLICATING = 'REPLICATING'

# Escalation thresholds from the Study doc (Study section 3, "Escalation thresholds").
BACKLOG_FACTOR_THRESHOLD = 2.0
BACKLOG_CONSECUTIVE_CYCLES = 3
ACHIEVED_RATIO_THRESHOLD = 0.6
ACHIEVED_RATIO_WINDOW_CYCLES = 4
RSE_USAGE_THRESHOLD = 0.90


def _read_ledger_events(src, dst, window_seconds, now):
    """
    Ledger events for this link within the last window_seconds. A missing
    ledger file (the link hasn't started yet) is not an error.
    """
    cutoff = now - datetime.timedelta(seconds=window_seconds)
    events = []
    try:
        with open(clint.ledger_filename(src, dst)) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    ts = datetime.datetime.fromisoformat(record['ts'])
                except (ValueError, KeyError):
                    print(f'clint_monitor: skipping malformed ledger line for {src}->{dst}', file=sys.stderr)
                    continue
                if ts >= cutoff:
                    events.append(record)
    except FileNotFoundError:
        pass
    return events


def _rule_states(rucio_client, rule_ids):
    """
    Batch-query state for each rule id. A query failure is reported to
    stderr and that rule's state is left as None (treated as backlog,
    conservatively, the same way is_dataset_bad in clint.py treats an
    unvalidatable dataset as bad rather than assuming success).
    """
    states = {}
    for rule_id in rule_ids:
        try:
            states[rule_id] = rucio_client.get_replication_rule(rule_id).get('state')
        except Exception as e:
            print(f'clint_monitor: could not query rule {rule_id}: {e}', file=sys.stderr)
            states[rule_id] = None
    return states


def _rse_usage_fraction(rucio_client, rse):
    """Fraction of `rse` used (0..1), or None if the backend doesn't report space."""
    try:
        for usage in rucio_client.get_rse_usage(rse):
            if usage.get('free') is not None and usage.get('total'):
                return (usage['total'] - usage['free']) / usage['total']
    except Exception as e:
        print(f'clint_monitor: could not query RSE usage for {rse}: {e}', file=sys.stderr)
    return None


def build_scorecard(rucio_client, src, dst, mbps, window_seconds, now):
    """Compute one link's scorecard from its ledger plus live Rucio rule state."""
    events = _read_ledger_events(src, dst, window_seconds, now)
    injected_events = [e for e in events if e.get('event') == 'injected']

    rule_ids = sorted({e['rule_id'] for e in injected_events if e.get('rule_id') != 'dry-run'})
    states = _rule_states(rucio_client, rule_ids)

    injected_bytes = 0
    completed_bytes = 0
    backlog_bytes = 0
    stuck_rule_ids = []
    for e in injected_events:
        size = e.get('bytes', 0)
        injected_bytes += size
        rule_id = e.get('rule_id')
        if rule_id == 'dry-run':
            # Never a real Rucio rule -- treated as immediately complete,
            # the same way the backlog throttle in clint.py never tracks it.
            completed_bytes += size
            continue
        state = states.get(rule_id)
        if state == RULE_STATE_OK:
            completed_bytes += size
        else:
            backlog_bytes += size
            if state != RULE_STATE_REPLICATING:
                # Anything that isn't progressing (STUCK, or unknown because
                # the query failed) is bucketed as stuck, conservatively.
                stuck_rule_ids.append(rule_id)

    target_bytes = int(125000 * mbps) * window_seconds
    achieved_mbps = (completed_bytes / window_seconds / 125000) if window_seconds else 0.0

    return {
        'ts': now.isoformat(),
        'src': src,
        'dst': dst,
        'link_mbps': mbps,
        'window_seconds': window_seconds,
        'target_bytes': target_bytes,
        'injected_bytes': injected_bytes,
        'completed_bytes': completed_bytes,
        'backlog_bytes': backlog_bytes,
        'stuck_rule_ids': stuck_rule_ids,
        'achieved_mbps': achieved_mbps,
        'dest_usage_fraction': _rse_usage_fraction(rucio_client, dst),
    }


def append_scorecard(out_path, scorecard):
    with open(out_path, 'a') as f:
        f.write(json.dumps(scorecard) + '\n')


def read_scorecards(out_path):
    scorecards = []
    try:
        with open(out_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    scorecards.append(json.loads(line))
                except ValueError:
                    print(f'clint_monitor: skipping malformed scorecard line in {out_path}', file=sys.stderr)
    except FileNotFoundError:
        pass
    return scorecards


def _link_key(record):
    return f"{record['src']}->{record['dst']}"


def check_anomalies(scorecards, links):
    """
    Evaluate the Study's thresholds per link over scorecard history read
    back from disk -- each invocation is a fresh process, so this history,
    not memory, is where trend detection happens. Returns a list of
    human-readable reason strings; empty if nothing is breached.
    """
    by_link = {}
    for sc in scorecards:
        by_link.setdefault(_link_key(sc), []).append(sc)

    reasons = []
    for link in links:
        key = f"{link['src']}->{link['dst']}"
        history = by_link.get(key, [])
        if not history:
            continue
        latest = history[-1]

        recent = history[-BACKLOG_CONSECUTIVE_CYCLES:]
        if len(recent) == BACKLOG_CONSECUTIVE_CYCLES and all(
                sc['backlog_bytes'] > BACKLOG_FACTOR_THRESHOLD * sc['target_bytes'] for sc in recent):
            reasons.append(f"{key}: backlog > {BACKLOG_FACTOR_THRESHOLD}x target for "
                            f"{BACKLOG_CONSECUTIVE_CYCLES} consecutive cycles")

        if latest['stuck_rule_ids']:
            reasons.append(f"{key}: {len(latest['stuck_rule_ids'])} stuck rule(s)")

        window = history[-ACHIEVED_RATIO_WINDOW_CYCLES:]
        if len(window) == ACHIEVED_RATIO_WINDOW_CYCLES:
            avg_ratio = sum(
                (sc['achieved_mbps'] / sc['link_mbps']) if sc['link_mbps'] else 1.0
                for sc in window
            ) / len(window)
            if avg_ratio < ACHIEVED_RATIO_THRESHOLD:
                reasons.append(f"{key}: achieved/target averaged {avg_ratio:.0%} over "
                                f"{ACHIEVED_RATIO_WINDOW_CYCLES} cycles (< {ACHIEVED_RATIO_THRESHOLD:.0%})")

        usage = latest.get('dest_usage_fraction')
        if usage is not None and usage > RSE_USAGE_THRESHOLD:
            reasons.append(f"{key}: destination RSE {latest['dst']} at {usage:.0%} used")

    return reasons


def update_anomaly_flag(flag_path, reasons):
    if reasons:
        with open(flag_path, 'w') as f:
            f.write('\n'.join(reasons) + '\n')
    else:
        try:
            os.remove(flag_path)
        except FileNotFoundError:
            pass


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog='clint_monitor',
        description="Deterministic collector: joins CLINT's per-link ledgers to live Rucio rule "
                    "state and writes scorecards. No LLM involved.",
    )
    parser.add_argument('--config', type=str, default='config.csv',
                        help='Path to config CSV file (default=config.csv)')
    parser.add_argument('--window', type=int, default=1200,
                        help='Lookback window in seconds (default=1200)')
    parser.add_argument('--out', type=str, default='scorecards.jsonl',
                        help='Scorecard output file (default=scorecards.jsonl)')
    parser.add_argument('--anomaly-flag', type=str, default='ANOMALY.flag',
                        help='Anomaly flag file, written/cleared each run (default=ANOMALY.flag)')
    args = parser.parse_args(argv)

    # Never crash the schedule: every failure path below logs to stderr and
    # falls through to exit 0, per the approved plan.
    try:
        links = clint.read_config_csv(args.config)
    except (OSError, ValueError) as e:
        print(f'clint_monitor: could not read config {args.config}: {e}', file=sys.stderr)
        return 0

    try:
        rucio_client = rucio.client.Client()
    except Exception as e:
        print(f'clint_monitor: could not set up Rucio client: {e}', file=sys.stderr)
        rucio_client = None

    now = datetime.datetime.now(datetime.timezone.utc)

    if rucio_client is not None:
        for link in links:
            try:
                scorecard = build_scorecard(rucio_client, link['src'], link['dst'], link['mbps'],
                                             args.window, now)
                append_scorecard(args.out, scorecard)
            except Exception as e:
                print(f"clint_monitor: failed to build scorecard for {link['src']}->{link['dst']}: {e}",
                      file=sys.stderr)
                continue

    try:
        scorecards = read_scorecards(args.out)
        reasons = check_anomalies(scorecards, links)
        update_anomaly_flag(args.anomaly_flag, reasons)
    except Exception as e:
        print(f'clint_monitor: anomaly check failed: {e}', file=sys.stderr)

    return 0


if __name__ == '__main__':
    sys.exit(main())
