import argparse
import csv
import datetime
import sys
import os
import threading
import time
import signal
import logging
import rucio.client

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
            except Exception:
                pass
    print(f'{storage_filename}: Preferring big DIDs: {big_first}')
    sorted_data = sorted(data, key=lambda x: x['bytes'], reverse=big_first)
    return sorted_data


def dump_used_datasets(used_datasets, src, dst):
    """
    Persist the state of used datasets with timestamps to disk to avoid re-injection during a single run.
    Creates a file per source-to-destination link.
    """
    filename = f"used_datasets_dump_{src}_to_{dst}.csv"
    with open(filename, "w") as file:
        for dataset, timestamp in used_datasets.items():
            formatted_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            file.write(f"{dataset},{formatted_timestamp}\n")
    print(f"Data dumped for {src}->{dst} into {filename}")


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


def run_transfer_thread(link, storage_data, injection_interval, rule_lifetime, fudge_factor, dryrun,
                        expiration_delay, max_injection_factor, used_datasets, rucio_client, user):
    src, dst, mbps = link['src'], link['dst'], link['mbps']

    total_unfudged = int(125000 * mbps)  # bytes per second approx based on Mbps to Bytes/sec
    total_bytes = total_unfudged * (1 + fudge_factor) if fudge_factor > 0 else total_unfudged
    injection_bytes = int(total_bytes * injection_interval)

    used_datasets = read_used_datasets_dump(src, dst)
    if used_datasets:
        for dataset in storage_data.get(src, []):
            if dataset['did'] in used_datasets:
                dataset['injected_at'] = used_datasets[dataset['did']]

    print(f'{src}->{dst}): Average target rate: {mbps} Mbps/hour')
    print(f'{src}->{dst}): Injecting approx {injection_bytes} bytes every {injection_interval} seconds including fudge factor {fudge_factor}')

    while not event_object.is_set():
        print(f'{format_ts()}{src}->{dst} :: Selecting fresh datasets on {src} for injection')

        freshness_cutoff = datetime.datetime.utcnow() - datetime.timedelta(seconds=rule_lifetime + expiration_delay)
        fresh_datasets = [x for x in storage_data.get(src, []) if x['injected_at'] < freshness_cutoff]

        injected_bytes = 0
        selected_datasets = []

        for dataset in fresh_datasets:
            if injected_bytes + dataset['bytes'] > (injection_bytes * (1 + max_injection_factor)):
                continue
            if is_dataset_bad(src, dst, dataset['did'], rucio_client):
                print(f'{format_ts()}Skipping bad dataset {dataset["did"]}')
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
                continue

            if rule_id:
                print(f'{format_ts()}{src}->{dst} :: Finished adding rule for {dataset["did"]}: {rule_id[0]}')
                current_time = datetime.datetime.utcnow()
                used_datasets[dataset['did']] = current_time
                dataset['injected_at'] = current_time
                log_injection(dataset['did'], src, dst, user)

        print(f'{format_ts()}{src}->{dst} :: Sleeping for {injection_interval} seconds')

        # Wait for injection interval or stop signal
        if event_object.wait(injection_interval):
            break

    dump_used_datasets(used_datasets, src, dst)


def main():
    parser = argparse.ArgumentParser(description="CLINT (Controlled Load INjector Tool) - Flexible dataset injection for scientific data management")
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
    parser.add_argument('--single-thread', action='store_true',
                        help='For debugging: process only one link')
    args = parser.parse_args()

    # Validate args
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

    print('Setting up Rucio client...')
    rucio_client = rucio.client.Client()
    user = rucio_client.whoami()['account']
    print(f'Using Rucio account: {user}')

    print(f'Reading configuration from {args.config}...')
    config_data = read_config_csv(args.config)

    print(f'Loading storage dataset info from {args.storage_dir}...')
    storage_data = {}
    lst_files = [f for f in os.listdir(args.storage_dir) if f.endswith(".lst")]
    if not lst_files:
        print(f'ERROR: No .lst files found in {args.storage_dir}')
        sys.exit(1)
    
    for filename in lst_files:
        filepath = os.path.join(args.storage_dir, filename)
        print(f'Loading storage data from {filename}...')
        storage_name = os.path.splitext(filename)[0]
        storage_data[storage_name] = read_storage_csv(filepath, args.big_first)

    threads = []

    if args.single_thread:
        for link in config_data:
            used = {}
            run_transfer_thread(link, storage_data, args.injection_interval, args.rule_lifetime,
                                args.fudge_factor, args.dryrun, args.expiration_delay,
                                args.max_injection_factor, used, rucio_client, user)
    else:
        for link in config_data:
            used = {}
            thread = threading.Thread(target=run_transfer_thread,
                                      args=(link, storage_data, args.injection_interval, args.rule_lifetime,
                                            args.fudge_factor, args.dryrun, args.expiration_delay,
                                            args.max_injection_factor, used, rucio_client, user))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    main()
