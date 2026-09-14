"""Command-line interface for CLINT.

Exposes `clint <command> [arguments] [options]`. Today that is:

  clint run [flags]         -- run the injection daemon (same as `python clint.py [flags]`)
  clint preflight [flags]   -- check a config against the real dataset pool and
                                destination RSEs before starting a run

`python clint.py [flags]` keeps working unchanged for existing HTCondor
submission scripts; this module is an additional entry point, not a
replacement.

Informational output goes to stdout as printed by the underlying functions
(matching clint.py's existing style); `preflight --json` prints a single
JSON document to stdout for scripting. Errors are reported as one line on
stderr, not a traceback.
"""

import argparse
import json
import signal
import sys

import rucio.client
from rucio.common.exception import RSENotFound

import clint


def _cmd_run(args):
    # clint.py registers these in its own __main__ block; cli.py needs to do
    # the same so `clint run` shuts down as gracefully as `python clint.py`.
    signal.signal(signal.SIGINT, clint.signal_handler)
    signal.signal(signal.SIGTERM, clint.signal_handler)
    clint.validate_run_args(args)
    clint.run(args)


def _rse_space(rucio_client, rse):
    """Free/total bytes for `rse`, or None if the backend doesn't report space."""
    for usage in rucio_client.get_rse_usage(rse):
        if usage.get('free') is not None and usage.get('total') is not None:
            return {'free_bytes': usage['free'], 'total_bytes': usage['total']}
    return None


def _account_quota(rucio_client, account, rse):
    """Quota remaining for `account` at `rse`, or 'unlimited' if none is set."""
    for usage in rucio_client.get_local_account_usage(account=account, rse=rse):
        if usage.get('rse') == rse:
            limit = usage.get('bytes_limit')
            if not limit:
                return 'unlimited'
            return max(0, usage.get('bytes_remaining', 0))
    return None


def _check_destination(rucio_client, account, dst):
    """Resolve `dst` and report its space/quota. Read-only: get_*/list_* calls only."""
    result = {'dst': dst, 'exists': True, 'space': None, 'quota_remaining_bytes': None}
    try:
        rucio_client.get_rse(dst)
    except RSENotFound:
        result['exists'] = False
        return result

    result['space'] = _rse_space(rucio_client, dst)
    result['quota_remaining_bytes'] = _account_quota(rucio_client, account, dst)
    return result


def _cmd_preflight(args):
    config_data = clint.read_config_csv(args.config)
    storage_data = clint.load_storage_data(args.storage_dir, big_first=False)

    print('Setting up Rucio client...')
    rucio_client = rucio.client.Client()
    account = rucio_client.whoami()['account']

    pool_bytes_by_src = {
        src: sum(d['bytes'] for d in datasets)
        for src, datasets in storage_data.items()
    }
    requested_mbps_by_src = {}
    for link in config_data:
        requested_mbps_by_src[link['src']] = requested_mbps_by_src.get(link['src'], 0) + link['mbps']

    rows = []
    feasible_overall = True
    for link in config_data:
        src, dst, mbps = link['src'], link['dst'], link['mbps']
        pool_bytes = pool_bytes_by_src.get(src, 0)
        sustainable = clint.sustainable_rate_mbps(pool_bytes, args.rule_lifetime, args.expiration_delay)
        requested = requested_mbps_by_src.get(src, 0)
        feasible = requested <= sustainable

        dest = _check_destination(rucio_client, account, dst)
        if not dest['exists']:
            feasible = False

        feasible_overall = feasible_overall and feasible
        rows.append({
            'src': src,
            'dst': dst,
            'link_mbps': mbps,
            'requested_mbps_for_src': requested,
            'sustainable_mbps_for_src': sustainable,
            'feasible': feasible,
            'dst_exists': dest['exists'],
            'dst_space': dest['space'],
            'dst_quota_remaining_bytes': dest['quota_remaining_bytes'],
        })

    if args.json:
        print(json.dumps({'feasible': feasible_overall, 'links': rows}, indent=2, default=str))
    else:
        _print_preflight_table(rows, feasible_overall)

    if not feasible_overall and not args.force:
        return 1
    return 0


def _print_preflight_table(rows, feasible_overall):
    for row in rows:
        status = 'OK' if row['feasible'] else 'INFEASIBLE'
        print(f"{row['src']} -> {row['dst']}: link target {row['link_mbps']:.0f} Mbps, "
              f"source total requested {row['requested_mbps_for_src']:.0f} Mbps, "
              f"sustainable {row['sustainable_mbps_for_src']:.1f} Mbps -- {status}")
        if not row['dst_exists']:
            print(f"  destination RSE '{row['dst']}' not found")
            continue
        space = row['dst_space']
        if space is None:
            print("  destination space: unknown (backend does not report it)")
        else:
            print(f"  destination space: {space['free_bytes'] / 1e9:.1f}GB free of "
                  f"{space['total_bytes'] / 1e9:.1f}GB")
        quota = row['dst_quota_remaining_bytes']
        if quota == 'unlimited':
            print("  account quota: unlimited")
        elif quota is None:
            print("  account quota: unknown")
        else:
            print(f"  account quota remaining: {quota / 1e9:.1f}GB")

    print()
    print('FEASIBLE' if feasible_overall else 'INFEASIBLE -- rerun with --force to start anyway')


def _build_parser():
    parser = argparse.ArgumentParser(
        prog='clint',
        description='CLINT (Controlled Load INjector Tool) - Flexible dataset injection for scientific data management',
    )
    sub = parser.add_subparsers(dest='command', required=True)

    run_parser = sub.add_parser('run', help='Run the injection daemon')
    clint.add_run_arguments(run_parser)
    run_parser.set_defaults(func=_cmd_run)

    preflight_parser = sub.add_parser(
        'preflight',
        help='Check a config against the dataset pool and destination RSEs before running',
    )
    preflight_parser.add_argument('--config', type=str, default='config.csv',
                                   help='Path to config CSV file (default=config.csv)')
    preflight_parser.add_argument('--storage-dir', type=str, default='.',
                                   help='Directory containing .lst files (default=current directory)')
    preflight_parser.add_argument('--rule-lifetime', type=int, default=7200,
                                   help='Rucio replication rule lifetime in seconds (default=7200)')
    preflight_parser.add_argument('--expiration-delay', type=int, default=300,
                                   help='Minimum seconds to wait before re-using a dataset (default=300)')
    preflight_parser.add_argument('--force', action='store_true',
                                   help='Exit 0 even if the config is infeasible')
    preflight_parser.add_argument('--json', action='store_true',
                                   help='Print a single JSON document instead of a table')
    preflight_parser.set_defaults(func=_cmd_preflight)

    return parser


def main(argv=None):
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args) or 0
    except (ValueError, OSError) as e:
        print(f'Error: {e}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
