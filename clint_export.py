"""Export the metrics of a CLINT run to two small CSV files, to hand in and to add up with other runs.

  clint_rules_<participant>.csv   one row per rule that CLINT made
  clint_cycles_<participant>.csv  one row per cycle of each link: what was wanted, injected, skipped

The rules file is built from the ledgers. With a Rucio client it also gets the stage times of the
transfer request of each rule, from the request history of Rucio:

  created -> submitted    conveyor submitter (the rule waits for the daemon)
  submitted -> started    queue in FTS
  started -> transferred  the transfer itself (FTS)
  transferred -> closed   Rucio poller and finisher, until the request is closed

Times in the files are UTC, ISO 8601. An empty field means that the value is not known.

Usage:
    python clint_export.py --participant NAME [--folder DIR] [--out DIR] [--no-rucio]
"""

import argparse
import csv
import datetime
import os
import sys

import clint_plot

RULE_FIELDS = [
    'participant', 'src', 'dst', 'rule_id', 'did', 'bytes', 'lifetime_s', 'injected_at',
    'ok_seen_at', 'status', 'seen_latency_s',
    'request_state', 'retries', 'fts_host', 'fts_job_id',
    'request_created_at', 'submitted_at', 'started_at', 'transferred_at', 'closed_at',
    'conveyor_s', 'fts_queue_s', 'transfer_s', 'after_transfer_s', 'rule_total_s', 'transfer_mbps',
]
CYCLE_FIELDS = ['participant', 'src', 'dst', 'cycle_at', 'link_mbps', 'interval_s', 'wanted_bytes', 'injected_bytes', 'n_rules',
                'skipped_bad', 'throttled', 'blocked_no_space', 'injection_failed']


def _iso(value):
    """A datetime (naive means UTC) or text -> ISO 8601 text in UTC, or ''."""
    if value in (None, ''):
        return ''
    if isinstance(value, str):
        value = datetime.datetime.fromisoformat(value)
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.timezone.utc)
    return value.astimezone(datetime.timezone.utc).isoformat()


def _seconds(start, end):
    if not start or not end:
        return ''
    return round((datetime.datetime.fromisoformat(end) - datetime.datetime.fromisoformat(start)).total_seconds(), 1)


def rule_rows(links, participant):
    """One row per injected rule (dry-run rules are left out), from the ledgers only."""
    rows = []
    for (src, dst), events in sorted(links.items()):
        ok = {e['rule_id']: e for e in events if e['event'] == 'rule_ok'}
        gone = {e['rule_id'] for e in events if e['event'] == 'rule_gone'}
        for e in events:
            if e['event'] != 'injected' or e.get('rule_id') == 'dry-run':
                continue
            seen = ok.get(e['rule_id'])
            rows.append({
                'participant': participant, 'src': src, 'dst': dst, 'rule_id': e['rule_id'], 'did': e.get('did', ''),
                'bytes': e.get('bytes', 0), 'lifetime_s': e.get('lifetime', ''), 'injected_at': _iso(e['ts']),
                'ok_seen_at': _iso(seen['ts']) if seen else '',
                'status': 'ok_seen' if seen else ('expired_unseen' if e['rule_id'] in gone else 'not_confirmed'),
                'seen_latency_s': _seconds(_iso(e['ts']), _iso(seen['ts'])) if seen else '',
            })
    return rows


def cycle_rows(links, participant):
    """One row per cycle. The events between two cycles (skips, throttle) belong to the later cycle."""
    rows = []
    for (src, dst), events in sorted(links.items()):
        counts = dict(skipped_bad=0, throttled=0, blocked_no_space=0, injection_failed=0)
        for e in events:
            if e['event'] in counts:
                counts[e['event']] += 1
            elif e['event'] == 'cycle':
                rows.append({'participant': participant, 'src': src, 'dst': dst, 'cycle_at': _iso(e['ts']),
                             'link_mbps': e.get('link_mbps', ''), 'interval_s': e.get('interval_s', ''),
                             'wanted_bytes': e.get('wanted_bytes', 0), 'injected_bytes': e.get('injected_bytes', 0),
                             'n_rules': e.get('n_rules', 0), **counts})
                counts = dict.fromkeys(counts, 0)
    return rows


def fetch_requests(client, scope, name, rse):
    """All request records Rucio keeps for a file at a destination, as dicts. [] if there is none.

    The public client call returns only the first record, so the stream is read here, with the
    same URL. If the private helpers are not there, the public call is used.
    """
    from random import choice
    from urllib.parse import quote_plus
    from rucio.common.utils import build_url
    try:
        from rucio.common.constants import HTTPMethod
        method = HTTPMethod.GET
    except ImportError:
        method = 'GET'
    try:
        path = '/'.join([client.REQUEST_BASEURL, 'history', quote_plus(scope), quote_plus(name), rse])
        response = client._send_request(build_url(choice(client.list_hosts), path=path), method=method)
        if response.status_code != 200:
            return []
        return [r for r in client._load_json_data(response) if isinstance(r, dict)]
    except AttributeError:
        record = client.list_request_history_by_did(name, rse, scope)
        return [record] if isinstance(record, dict) else []


def add_stage_times(rows, client):
    """Fill the request columns of the rows, in place. A row stays empty if its request is not found
    or is not the one of this rule (Rucio keeps the latest request of a file at a destination)."""
    done = 0
    for row in rows:
        try:
            scope, name = row['did'].split(':', 1)
            files = list(client.list_files(scope, name))
            requests = []
            for f in files:
                requests += [r for r in fetch_requests(client, f['scope'], f['name'], row['dst'])
                             if r.get('rule_id') == row['rule_id']]
        except Exception as e:  # a row without stage times is better than no export
            print(f"Warning: no request data for {row['did']}->{row['dst']}: {e}", file=sys.stderr)
            continue
        if not requests or len(requests) < len(files):
            continue

        def first(key):
            return min((_iso(r[key]) for r in requests if r.get(key)), default='')

        def last(key):
            return max((_iso(r[key]) for r in requests if r.get(key)), default='')

        row.update({
            'request_state': requests[0].get('state', ''), 'retries': max(r.get('retry_count') or 0 for r in requests),
            'fts_host': requests[0].get('external_host') or '', 'fts_job_id': requests[0].get('external_id') or '',
            'request_created_at': first('created_at'), 'submitted_at': first('submitted_at'),
            'started_at': first('started_at'), 'transferred_at': last('transferred_at'),
            'closed_at': last('updated_at') if all(r.get('state') == 'DONE' for r in requests) else '',
        })
        row['conveyor_s'] = _seconds(row['request_created_at'], row['submitted_at'])
        row['fts_queue_s'] = _seconds(row['submitted_at'], row['started_at'])
        row['transfer_s'] = _seconds(row['started_at'], row['transferred_at'])
        row['after_transfer_s'] = _seconds(row['transferred_at'], row['closed_at'])
        row['rule_total_s'] = _seconds(row['injected_at'], row['closed_at'])
        if row['transfer_s'] != '':
            row['transfer_mbps'] = round(row['bytes'] * 8 / 1e6 / max(row['transfer_s'], 1.0), 1)
        done += 1
    return done


def write_csv(path, fields, rows):
    with open(path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, '') for k in fields})


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--participant', required=True, help='your name or account: it labels your rows')
    ap.add_argument('--folder', default='.', help='work folder of the run (default: the current folder)')
    ap.add_argument('--out', default='.', help='folder for the CSV files (default: the current folder)')
    ap.add_argument('--no-rucio', action='store_true', help='do not ask Rucio for the stage times of the transfers')
    args = ap.parse_args(argv)

    links = clint_plot.load_run(args.folder)
    if not links:
        print(f'Error: no clint_ledger_*.jsonl in {args.folder}', file=sys.stderr)
        return 1
    rows = rule_rows(links, args.participant)
    if not args.no_rucio:
        try:
            import rucio.client
            client = rucio.client.Client()
        except Exception as e:
            print(f'Warning: no Rucio client ({e}): the stage times are left empty', file=sys.stderr)
        else:
            n = add_stage_times(rows, client)
            print(f'Stage times found for {n} of {len(rows)} rules')

    os.makedirs(args.out, exist_ok=True)
    rules_path = os.path.join(args.out, f'clint_rules_{args.participant}.csv')
    cycles_path = os.path.join(args.out, f'clint_cycles_{args.participant}.csv')
    write_csv(rules_path, RULE_FIELDS, rows)
    write_csv(cycles_path, CYCLE_FIELDS, cycle_rows(links, args.participant))
    print(rules_path)
    print(cycles_path)
    return 0


if __name__ == '__main__':
    sys.exit(main())
