import argparse
import copy
import csv
import datetime
import json
import sys
import os
import threading
import time
import signal
import logging
import rucio.client
from rucio.common.exception import RuleNotFound

# Setup monitoring logger to track injection activity
logger = logging.getLogger("clint_monitor")
logger.setLevel(logging.INFO)
fh = logging.FileHandler("clint_injection.log")
formatter = logging.Formatter('%(asctime)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

event_object = threading.Event()


def signal_handler(sig, frame):
    print("Signal received, shutting down... Please wait for threads to dump their state!")
    event_object.set()


def format_ts():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S :: ")


def read_config_csv(config_filename):
    """
    Read CSV config that defines data injection links between sources and destinations with target rates.
    Expected fields: src (source RSE), dst (destination RSE), mbps (target throughput)
    """
    data = []
    with open(config_filename, 'r') as file:
        reader = csv.DictReader(file, fieldnames=['src', 'dst', 'mbps'])
        for row in reader:
            data.append({'src': row['src'], 'dst': row['dst'], 'mbps': float(row['mbps'])})
    return data


def read_storage_csv(storage_filename, big_first):
    """
    Read storage data CSV listing datasets and their sizes.
    Sort the list depending on 'big_first' to prioritize larger or smaller datasets.
    """
    data = []
    with open(storage_filename, 'r') as file:
        reader = csv.DictReader(file, fieldnames=['did', 'bytes'])
        for row in reader:
            try:
                data.append({'did': row['did'], 'bytes': int(row['bytes']),
                             'injected_at': datetime.datetime.utcfromtimestamp(0)})
            except Exception as e:
                print(f'{storage_filename}: skipping unparseable row {row}: {e}', file=sys.stderr)
    print(f'{storage_filename}: Preferring big DIDs: {big_first}')
    sorted_data = sorted(data, key=lambda x: x['bytes'], reverse=big_first)
    return sorted_data


def load_storage_data(storage_dir, big_first):
    """
    Load every .lst file in storage_dir into a dict keyed by storage name
    (the .lst filename without its extension). Shared by run() and cli.py's
    `preflight` command, which needs pool sizes without starting any threads.
    """
    storage_data = {}
    lst_files = [f for f in os.listdir(storage_dir) if f.endswith(".lst")]
    if not lst_files:
        print(f'ERROR: No .lst files found in {storage_dir}')
        sys.exit(1)

    for filename in lst_files:
        filepath = os.path.join(storage_dir, filename)
        print(f'Loading storage data from {filename}...')
        storage_name = os.path.splitext(filename)[0]
        storage_data[storage_name] = read_storage_csv(filepath, big_first)

    return storage_data


def dump_used_datasets(used_datasets, src, dst):
    """
    Persist the state of used datasets with timestamps to disk to avoid re-injection during a single run.
    Creates a file per source-to-destination link. Writes atomically (temp file
    + rename) since this now runs once per cycle, not only on shutdown.
    """
    filename = f"used_datasets_dump_{src}_to_{dst}.csv"
    tmp_filename = f"{filename}.tmp"
    with open(tmp_filename, "w") as file:
        for dataset, timestamp in used_datasets.items():
            formatted_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            file.write(f"{dataset},{formatted_timestamp}\n")
    os.replace(tmp_filename, filename)
    print(f"Data dumped for {src}->{dst} into {filename}")


def ledger_filename(src, dst):
    """One ledger file per link -- concurrent O_APPEND across links on EOS
    FUSE is not guaranteed atomic, so links must not share a ledger file."""
    return f"clint_ledger_{src}_to_{dst}.jsonl"


def log_ledger_event(src, dst, event, **fields):
    """
    Append one JSON line to this link's ledger: {"ts", "event", ...fields}.
    This is the join key the collector (clint_monitor.py) uses to match
    injections to rule outcomes.
    """
    record = {'ts': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'event': event}
    record.update(fields)
    with open(ledger_filename(src, dst), 'a') as f:
        f.write(json.dumps(record) + '\n')


def read_used_datasets_dump(src, dst):
    """
    Load state of previously used datasets for a given source-destination link.
    Returns a dict of dataset -> last injection timestamp.
    """
    filename = f"used_datasets_dump_{src}_to_{dst}.csv"
    used_datasets = {}
    try:
        with open(filename, "r") as file:
            for line in file:
                dataset, timestamp_str = line.strip().split(',')
                timestamp = datetime.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                used_datasets[dataset] = timestamp
        print(f"Already used datasets loaded for {src}->{dst} from {filename}")
    except FileNotFoundError:
        print(f"No previously used datasets found for {src}->{dst}. Starting fresh.")
    except Exception as e:
        print(f"Error reading from file {filename}: {e}")

    return used_datasets


def is_rse_expression(rse_string):
    """
    True if `rse_string` looks like a Rucio RSE expression (e.g.
    'tier=2&country=it') rather than a plain RSE name. is_dataset_bad()
    compares a rule's destination by exact RSE name, which silently never
    matches an expression -- every dataset would look "not yet at
    destination" forever. Plain RSE names don't contain these characters.
    """
    return any(c in rse_string for c in '&|\\()=')


def is_dataset_bad(src, dst, did, rucio_client):
    """
    Check that the dataset exists on the source RSE with at least 80% availability
    and is NOT present on the destination RSE.
    """
    scope, name = did.split(':')
    try:
        rse_list = list(rucio_client.list_dataset_replicas(scope, name))
    except Exception as e:
        print(f'{format_ts()}{src}->{dst} :: Error checking dataset replicas for {did}: {e}')
        return True  # Conservative: treat as bad if we cannot validate

    in_src = False
    in_dst = False

    for rse in rse_list:
        if rse['rse'] == src:
            if rse['length'] != 0 and rse['available_length'] / rse['length'] > 0.80:
                in_src = True
        if rse['rse'] == dst:
            in_dst = True

    return not (in_src and not in_dst)


def log_injection(dataset, src, dst, user):
    """
    Log injection activity to file with timestamp, dataset, source, destination, and user.
    """
    logger.info(f"Injected dataset={dataset} from={src} to={dst} by user={user}")


def sustainable_rate_mbps(pool_bytes, rule_lifetime, expiration_delay):
    """
    Max Mbps a dataset pool can sustain, given how long a dataset takes to
    become eligible for re-injection (rule_lifetime + expiration_delay).
    """
    cycle_seconds = rule_lifetime + expiration_delay
    return pool_bytes / cycle_seconds / 125000


heartbeat_lock = threading.Lock()


def write_heartbeat(src, dst, wanted_bytes, injected_bytes, cumulative_bytes, n_rules_total,
                     eligible_pool_bytes=None, sustainable_mbps=None, state='running', cycle_duration_s=None):
    """
    Merge this link's state into clint_heartbeat.json (atomic temp+replace).
    Guarded by a lock since every link thread writes the same file.

    eligible_pool_bytes/sustainable_mbps are what let a reader (human or
    agent) tell "the pool is exhausted" apart from "the network is slow" --
    the gap analysis found this was the single highest-value field missing
    from the original observability plan.
    """
    key = f'{src}->{dst}'
    entry = {
        'last_cycle_ts': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'wanted_bytes': wanted_bytes,
        'injected_bytes': injected_bytes,
        'cumulative_bytes': cumulative_bytes,
        'n_rules_total': n_rules_total,
        'eligible_pool_bytes': eligible_pool_bytes,
        'sustainable_mbps': sustainable_mbps,
        'state': state,
        'cycle_duration_s': cycle_duration_s,
    }
    with heartbeat_lock:
        try:
            with open('clint_heartbeat.json') as f:
                data = json.load(f)
        except (FileNotFoundError, ValueError):
            data = {}
        data[key] = entry
        tmp = 'clint_heartbeat.json.tmp'
        with open(tmp, 'w') as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, 'clint_heartbeat.json')


CIRCUIT_BREAKER_THRESHOLD = 5  # consecutive all-failed cycles before a link quarantines itself


def has_room_for_cycle(rucio_client, account, dst, needed_bytes):
    """
    True if `dst` has enough free space and `account` enough quota there
    for `needed_bytes`. If a query itself fails (transient network issue,
    unsupported backend), returns True -- a monitoring hiccup must not be
    able to halt injection; only a confirmed shortfall should.
    """
    try:
        for usage in rucio_client.get_rse_usage(dst):
            if usage.get('free') is not None and usage.get('total') is not None:
                if usage['free'] < needed_bytes:
                    return False
                break
    except Exception as e:
        print(f'Warning: could not check space on {dst}: {e}', file=sys.stderr)

    try:
        for usage in rucio_client.get_local_account_usage(account=account, rse=dst):
            if usage.get('rse') == dst:
                limit = usage.get('bytes_limit')
                remaining = usage.get('bytes_remaining')
                if limit and remaining is not None and remaining < needed_bytes:
                    return False
    except Exception as e:
        print(f'Warning: could not check quota on {dst} for {account}: {e}', file=sys.stderr)

    return True


def refresh_backlog(rucio_client, pending_rules, src=None, dst=None):
    """
    Query state for rules not yet confirmed OK; drop the ones that now are.
    Returns (backlog_bytes, still_pending). dry-run rule ids are never
    added to pending_rules in the first place, so --dryrun never throttles.
    A rule whose state can't be confirmed is conservatively kept as backlog.

    A rule that Rucio no longer knows (RuleNotFound) has expired and been removed
    by the cleaner daemon; it is not backlog any more. With src and dst given, the
    outcome is written to the link's ledger, so that clint_monitor.py can still
    tell a rule that finished (rule_ok) from one that vanished unseen (rule_gone).
    """
    still_pending = []
    backlog_bytes = 0
    for entry in pending_rules:
        try:
            state = rucio_client.get_replication_rule(entry['rule_id']).get('state')
        except RuleNotFound:
            if src and dst:
                log_ledger_event(src, dst, 'rule_gone', rule_id=entry['rule_id'], bytes=entry['bytes'])
            continue
        except Exception:
            state = None
        if state == 'OK':
            if src and dst:
                log_ledger_event(src, dst, 'rule_ok', rule_id=entry['rule_id'], bytes=entry['bytes'])
            continue
        still_pending.append(entry)
        backlog_bytes += entry['bytes']
    return backlog_bytes, still_pending


def run_transfer_thread(link, storage_data, injection_interval, rule_lifetime, fudge_factor, dryrun,
                        expiration_delay, max_injection_factor, user, skip_validation,
                        duration=None, total_volume=None, max_backlog_factor=3.0):
    src, dst, mbps = link['src'], link['dst'], link['mbps']

    # One Rucio client per thread (a shared client is not guaranteed thread-safe).
    rucio_client = rucio.client.Client()

    # Deep copy: two links sharing one source (the ESCAPE config has three)
    # must not share dataset objects, or updating 'injected_at' on one link's
    # copy makes the dataset look stale on the other link too.
    link_datasets = copy.deepcopy(storage_data.get(src, []))
    pool_bytes_total = sum(d['bytes'] for d in link_datasets)
    sustainable_mbps = sustainable_rate_mbps(pool_bytes_total, rule_lifetime, expiration_delay)

    total_unfudged = int(125000 * mbps)  # bytes per second approx based on Mbps to Bytes/sec
    total_bytes = total_unfudged * (1 + fudge_factor) if fudge_factor > 0 else total_unfudged
    injection_bytes = int(total_bytes * injection_interval)

    used_datasets = read_used_datasets_dump(src, dst)
    if used_datasets:
        for dataset in link_datasets:
            if dataset['did'] in used_datasets:
                dataset['injected_at'] = used_datasets[dataset['did']]

    print(f'{src}->{dst}): Average target rate: {mbps} Mbps/hour')
    print(f'{src}->{dst}): Injecting approx {injection_bytes} bytes every {injection_interval} seconds including fudge factor {fudge_factor}')
    print(f'{src}->{dst}): Pool sustains ~{sustainable_mbps:.1f} Mbps for this link\'s rule lifetime/expiration-delay cycle')
    if skip_validation:
        print(f'{src}->{dst}): Validation DISABLED - skipping dataset availability checks')

    deadline = time.time() + duration if duration is not None else None
    total_volume_bytes = total_volume * 1e9 if total_volume is not None else None
    cumulative_bytes = 0
    n_rules_total = 0
    pending_rules = []  # [{'rule_id', 'bytes'}] for this link's rules not yet confirmed OK
    consecutive_failed_cycles = 0
    injection_bytes_this_cycle = 0
    eligible_pool_bytes = 0

    def write_link_heartbeat(state):
        write_heartbeat(src, dst, wanted_bytes=injection_bytes_this_cycle, injected_bytes=injected_bytes,
                         cumulative_bytes=cumulative_bytes, n_rules_total=n_rules_total,
                         eligible_pool_bytes=eligible_pool_bytes, sustainable_mbps=sustainable_mbps,
                         state=state, cycle_duration_s=round(time.time() - cycle_start_time, 3))

    injected_bytes = 0  # visible to write_link_heartbeat even before the first cycle runs
    cycle_start_time = time.time()

    while not event_object.is_set():
        cycle_start_time = time.time()

        if deadline is not None and time.time() >= deadline:
            print(f'{format_ts()}{src}->{dst} :: Reached --duration, stopping')
            break

        print(f'{format_ts()}{src}->{dst} :: Selecting fresh datasets on {src} for injection')

        freshness_cutoff = datetime.datetime.utcnow() - datetime.timedelta(seconds=rule_lifetime + expiration_delay)
        fresh_datasets = [x for x in link_datasets if x['injected_at'] < freshness_cutoff]
        eligible_pool_bytes = sum(x['bytes'] for x in fresh_datasets)
        injection_bytes_this_cycle = injection_bytes

        injected_bytes = 0
        selected_datasets = []
        succeeded_this_cycle = 0
        throttled = False
        blocked = False

        # Refresh every cycle, also with the throttle off: this is what writes rule_ok to the ledger.
        backlog_bytes, pending_rules = refresh_backlog(rucio_client, pending_rules, src, dst)
        if max_backlog_factor > 0:
            if backlog_bytes > max_backlog_factor * injection_bytes:
                throttled = True
                print(f'{format_ts()}{src}->{dst} :: Backlog {backlog_bytes / 1e9:.3f}GB exceeds '
                      f'{max_backlog_factor}x this cycle\'s target -- throttling, skipping injection')
                log_ledger_event(src, dst, 'throttled', backlog_bytes=backlog_bytes,
                                  max_backlog_bytes=max_backlog_factor * injection_bytes)

        if not throttled:
            needed_bytes = int(injection_bytes * (1 + max_injection_factor))
            if not has_room_for_cycle(rucio_client, user, dst, needed_bytes):
                blocked = True
                print(f'{format_ts()}{src}->{dst} :: Destination {dst} lacks space/quota for this '
                      f'cycle -- blocking, skipping injection')
                log_ledger_event(src, dst, 'blocked_no_space', needed_bytes=needed_bytes)

        if not throttled and not blocked:
            for dataset in fresh_datasets:
                if injected_bytes + dataset['bytes'] > (injection_bytes * (1 + max_injection_factor)):
                    continue
                if not skip_validation and is_dataset_bad(src, dst, dataset['did'], rucio_client):
                    print(f'{format_ts()}Skipping bad dataset {dataset["did"]}')
                    log_ledger_event(src, dst, 'skipped_bad', did=dataset['did'], reason='failed_validation')
                    continue
                selected_datasets.append(dataset)
                injected_bytes += dataset['bytes']
                if injected_bytes >= injection_bytes:
                    break

            injection_gbytes = injection_bytes / 1e9
            injected_gbytes = injected_bytes / 1e9

            if injected_bytes == injection_bytes:
                print(f'{format_ts()}{src}->{dst} :: Wanted {injection_gbytes:.3f}GB, injecting {injected_gbytes:.3f}GB -- EXACT MATCH')
            elif injected_bytes > injection_bytes:
                print(f'{format_ts()}{src}->{dst} :: Wanted {injection_gbytes:.3f}GB, injecting {injected_gbytes:.3f}GB -- OVER INJECTION')
            else:
                print(f'{format_ts()}{src}->{dst} :: Wanted {injection_gbytes:.3f}GB, injecting {injected_gbytes:.3f}GB -- UNDER INJECTION')

            print(f'{format_ts()}{src}->{dst} :: Adding replication rules for {len(selected_datasets)} datasets')

            for dataset in selected_datasets:
                print(f'{format_ts()}{src}->{dst} :: Adding rule for {dataset["did"]}')
                scope, name = dataset['did'].split(':')
                rule_id = None
                try:
                    if dryrun:
                        rule_id = ["dry-run"]
                    else:
                        rule_id = rucio_client.add_replication_rule(
                            dids=[{'scope': scope, 'name': name}],
                            copies=1,
                            rse_expression=dst,
                            lifetime=rule_lifetime,
                            source_replica_expression=src,
                            purge_replicas=True)
                except Exception as e:
                    err_str = str(e).replace('\n', ';;;')
                    print(f'{format_ts()}{src}->{dst} :: Injection failed: {err_str} -- SKIPPING')
                    log_ledger_event(src, dst, 'injection_failed', did=dataset['did'], error=err_str)
                    continue

                if rule_id:
                    print(f'{format_ts()}{src}->{dst} :: Finished adding rule for {dataset["did"]}: {rule_id[0]}')
                    current_time = datetime.datetime.utcnow()
                    used_datasets[dataset['did']] = current_time
                    dataset['injected_at'] = current_time
                    log_injection(dataset['did'], src, dst, user)
                    log_ledger_event(src, dst, 'injected', rule_id=rule_id[0], did=dataset['did'],
                                      bytes=dataset['bytes'], lifetime=rule_lifetime, account=user)
                    cumulative_bytes += dataset['bytes']
                    n_rules_total += 1
                    succeeded_this_cycle += 1
                    if rule_id[0] != 'dry-run':
                        pending_rules.append({'rule_id': rule_id[0], 'bytes': dataset['bytes']})

            # Circuit breaker: a cycle that tried to inject and got zero
            # successes counts as a failure; any success resets the streak.
            # A cycle with nothing to select (pool exhausted) is a different
            # problem and doesn't count here.
            if selected_datasets and succeeded_this_cycle == 0:
                consecutive_failed_cycles += 1
            elif succeeded_this_cycle > 0:
                consecutive_failed_cycles = 0

        log_ledger_event(src, dst, 'cycle', wanted_bytes=injection_bytes, injected_bytes=injected_bytes,
                          n_rules=len(selected_datasets), cycle_duration_s=round(time.time() - cycle_start_time, 3),
                          link_mbps=mbps, interval_s=injection_interval)

        # Checkpoint every cycle, not only on shutdown -- a hard kill between
        # checkpoints would otherwise lose re-injection state and re-inject
        # datasets before rule_lifetime + expiration_delay has really passed.
        dump_used_datasets(used_datasets, src, dst)
        cycle_state = 'blocked' if blocked else ('throttled' if throttled else 'running')
        write_link_heartbeat(cycle_state)

        if consecutive_failed_cycles >= CIRCUIT_BREAKER_THRESHOLD:
            print(f'{format_ts()}{src}->{dst} :: {consecutive_failed_cycles} consecutive cycles with zero '
                  f'successful injections -- quarantining this link')
            log_ledger_event(src, dst, 'link_disabled', consecutive_failed_cycles=consecutive_failed_cycles)
            write_link_heartbeat('quarantined')
            break

        if total_volume_bytes is not None and cumulative_bytes >= total_volume_bytes:
            print(f'{format_ts()}{src}->{dst} :: Reached --total-volume ({cumulative_bytes / 1e9:.3f}GB), stopping this link')
            write_link_heartbeat('stopped')
            break

        print(f'{format_ts()}{src}->{dst} :: Sleeping for {injection_interval} seconds')

        # Deadline-based scheduling: wait until injection_interval seconds
        # after this cycle STARTED, not injection_interval seconds after it
        # finished -- otherwise cadence drifts by however long the cycle's
        # own work (dataset validation, rule creation) took.
        wait_time = max(0, cycle_start_time + injection_interval - time.time())
        if deadline is not None:
            wait_time = min(wait_time, max(0, deadline - time.time()))
        if event_object.wait(wait_time):
            break

    dump_used_datasets(used_datasets, src, dst)


def _run_transfer_thread_guarded(link, outcomes, *args, **kwargs):
    """
    Wraps run_transfer_thread so an unhandled exception in one link's thread
    (e.g. a malformed .lst line's ValueError) is caught and recorded instead
    of silently killing the thread while the process looks healthy.
    `outcomes` is a plain list; each thread appends exactly one entry, and
    list.append is thread-safe under the GIL, so no lock is needed.
    """
    src, dst = link['src'], link['dst']
    try:
        run_transfer_thread(link, *args, **kwargs)
        outcomes.append('stopped')
    except Exception as e:
        print(f'{format_ts()}{src}->{dst} :: Thread died with unhandled exception: {e}')
        try:
            log_ledger_event(src, dst, 'link_died', error=str(e))
        except Exception:
            pass  # don't let a ledger-write failure mask the original crash
        outcomes.append('died')


def add_run_arguments(parser):
    """
    Add the injection-run flags to a parser (or subparser). Shared by clint.py's
    own top-level parser and cli.py's `run` subcommand so the two can't drift.
    """
    parser.add_argument('--config', type=str, default='config.csv',
                        help='Path to config CSV file (default=config.csv)')
    parser.add_argument('--storage-dir', type=str, default='.',
                        help='Directory containing .lst files (default=current directory)')
    parser.add_argument('--injection-interval', type=int, default=15*60,
                        help='Injection interval in seconds (default=900, 1...3600)')
    parser.add_argument('--max-injection-factor', type=float, default=0.20,
                        help='Max percentage over target bytes allowed (default=0.20)')
    parser.add_argument('--rule-lifetime', type=int, default=7200,
                        help='Rucio replication rule lifetime in seconds (default=7200)')
    parser.add_argument('--big-first', action='store_true', default=False,
                        help='Inject big datasets first, otherwise smaller ones first (default)')
    parser.add_argument('--fudge-factor', type=float, default=0,
                        help='Increase target injection amount by fudge factor (float 0..1, default=0)')
    parser.add_argument('--expiration-delay', type=int, default=300,
                        help='Minimum seconds to wait before re-using dataset after rule expiration (default=300)')
    parser.add_argument('--dryrun', action='store_true',
                        help='Do NOT perform actual injection, simulate only')
    parser.add_argument('--skip-validation', action='store_true',
                        help='Skip dataset availability checks (faster but riskier - Rucio will reject invalid rules)')
    parser.add_argument('--single-thread', action='store_true',
                        help='For debugging: process only one link')
    parser.add_argument('--duration', type=int, default=None,
                        help='Stop injecting after this many seconds (default=unlimited)')
    parser.add_argument('--total-volume', type=float, default=None,
                        help='Stop a link after injecting this many GB total (default=unlimited)')
    parser.add_argument('--max-backlog-factor', type=float, default=3.0,
                        help='Skip a cycle if this link\'s not-yet-OK rules exceed this multiple of '
                             'one cycle\'s injection target; 0 disables the check (default=3.0)')
    return parser


def validate_run_args(args):
    """
    Validate the run flags, printing a message and exiting 1 on the first
    invalid value (unchanged behaviour from the original inline checks).
    """
    if not (1 <= args.injection_interval <= 3600):
        print('Injection interval must be between 1 and 3600 seconds')
        sys.exit(1)
    if not (1 <= args.rule_lifetime <= 86400):
        print('Rule lifetime must be between 1 and 86400 seconds')
        sys.exit(1)
    if not (0 <= args.fudge_factor <= 1):
        print('Fudge factor must be between 0 and 1')
        sys.exit(1)
    if not (0 <= args.expiration_delay <= 7200):
        print('Expiration delay must be between 0 and 7200 seconds')
        sys.exit(1)
    if not (0 <= args.max_injection_factor <= 1):
        print('Max injection factor must be between 0 and 1')
        sys.exit(1)


def run(args):
    """
    Set up the Rucio client, load configuration and storage data, and run
    the injection threads. Entry point shared by clint.py's main() and
    cli.py's `run` subcommand.
    """
    print('Setting up Rucio client...')
    rucio_client = rucio.client.Client()
    user = rucio_client.whoami()['account']
    print(f'Using Rucio account: {user}')

    print(f'Reading configuration from {args.config}...')
    config_data = read_config_csv(args.config)

    print(f'Loading storage dataset info from {args.storage_dir}...')
    storage_data = load_storage_data(args.storage_dir, args.big_first)

    # Fail fast and loudly rather than silently running a no-op thread: a
    # typo'd 'src' in config.csv used to fall through to
    # storage_data.get(src, []) returning an empty list forever.
    for link in config_data:
        if not storage_data.get(link['src']):
            print(f"ERROR: config.csv references src '{link['src']}' with no matching "
                  f".lst file (or an empty one) in {args.storage_dir}")
            sys.exit(1)
        if is_rse_expression(link['dst']) and not args.skip_validation:
            print(f"ERROR: dst '{link['dst']}' looks like a Rucio RSE expression, not a plain RSE "
                  f"name. is_dataset_bad()'s destination check compares by exact name and would "
                  f"never match it, silently treating every dataset as available forever. Use a "
                  f"plain RSE name, or pass --skip-validation if this is intentional.")
            sys.exit(1)

    threads = []

    if args.single_thread:
        for link in config_data:
            run_transfer_thread(link, storage_data, args.injection_interval, args.rule_lifetime,
                                args.fudge_factor, args.dryrun, args.expiration_delay,
                                args.max_injection_factor, user, args.skip_validation,
                                duration=args.duration, total_volume=args.total_volume,
                                max_backlog_factor=args.max_backlog_factor)
    else:
        outcomes = []
        for link in config_data:
            thread = threading.Thread(target=_run_transfer_thread_guarded,
                                      args=(link, outcomes, storage_data, args.injection_interval, args.rule_lifetime,
                                            args.fudge_factor, args.dryrun, args.expiration_delay,
                                            args.max_injection_factor, user, args.skip_validation),
                                      kwargs={'duration': args.duration, 'total_volume': args.total_volume,
                                              'max_backlog_factor': args.max_backlog_factor})
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        if outcomes and all(o == 'died' for o in outcomes):
            print('ERROR: every link thread died -- exiting non-zero so the scheduler notices')
            sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="CLINT (Controlled Load INjector Tool) - Flexible dataset injection for scientific data management")
    add_run_arguments(parser)
    args = parser.parse_args()
    validate_run_args(args)
    run(args)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    main()
