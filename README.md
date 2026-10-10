# CLINT (Common Load INjector Tool)

CLINT injects datasets into a storage system via Rucio to emulate, test, or challenge data management infrastructures through controlled load injection.

*CLINT is largely inspired by the [`dc_inject`](https://gitlab.cern.ch/wlcg-doma/dc_inject) tool developed in the context of the WLCG Data Operations Management Activity.*

## Features

- Flexible injection scheduling and rate control  
- Works with any Rucio-backed storage system, adaptable beyond HEP domains  
- Monitors and logs all injection events for auditing and analysis  
- Supports multi-threaded injection for multiple source-destination pairs  
- Graceful shutdown and state persistence to avoid duplicate injections  

## Installation

```
pip install -r requirements.txt
```

## Authentication into Rucio
Set up your Rucio client configuration and authentication as per [the ESCAPE documentation](https://vre-hub.github.io/docs/rucio#the-ruciocfg-configuration-file).


## Usage

Prepare your `config.csv` defining injection links, and dataset list files (`*.lst`).

### CLI (recommended)

```
python cli.py preflight --config config.csv --storage-dir .
python cli.py run --injection-interval=900 --rule-lifetime=7200
```

`preflight` checks a config before you start a run: it resolves every RSE,
computes the maximum rate the dataset pool can actually sustain given
`--rule-lifetime`/`--expiration-delay`, compares it to what `config.csv`
requests, and checks destination space and account quota. It exits non-zero
on an infeasible config unless `--force` is given. Add `--json` for
machine-readable output.

More options:

```
python cli.py --help
python cli.py run --help
python cli.py preflight --help
```

### Direct invocation (still supported)

`clint.py` remains directly runnable, for existing HTCondor submission
scripts and anything that already depends on this exact invocation:

```
python clint.py --injection-interval=900 --rule-lifetime=7200
```

For dry run mode (no actual injection; simulation only):

```
python clint.py --dryrun
```

More options:

```
python clint.py --help
```

## Configuration

- `config.csv` should include rows with fields:  
  `src`,`dst`,`mbps`  
  - `src` = source RSE or storage endpoint  
  - `dst` = destination RSE or storage endpoint  
  - `mbps` = target throughput in megabits per second  

- Dataset files (`*.lst`) list datasets available per storage location with fields:  
  `did`,`bytes`  
  - `did` = dataset identifier, e.g. `scope:name`  
  - `bytes` = size in bytes  

## Logs and Monitoring

- Injection events are logged to `clint_injection.log` with dataset, source, destination, and user info for auditing.

## Plots

`clint_plot.py` draws the metrics of a run from the ledgers (`clint_ledger_*.jsonl`) in its work folder. It needs matplotlib
(`pip install -r requirements-plot.txt`), but not rucio.

```
python cli.py plot                          # the run in the current folder: clint_run_<folder>.png
python cli.py plot runA runB runC --out out # one figure per run, and clint_compare.png
python clint_plot.py runA --label student-1 # same, without rucio installed
```

Export and class plots, for a data challenge with many participants:

```
python cli.py export --participant ann         # clint_rules_ann.csv and clint_cycles_ann.csv (a few kB)
python clint_plot.py --csv folder-with-all-csv --out plots     # clint_class.png
```

`export` writes one row per rule (`clint_rules_<name>.csv`) and one row per cycle (`clint_cycles_<name>.csv`). With Rucio
access it adds the stage times of the transfer request of each rule: conveyor (rule to submitted), queue in FTS, the
transfer itself, and the time until Rucio closes the request (poller and finisher). It also adds the FTS job id. Use
`--no-rucio` to skip this. Rucio keeps one request record per file and destination, so a rule on a dataset that you
used in an earlier run has empty stage times. The class figure shows the completed rate of all participants against the
target, the achievement ratio (completed / wanted, with the colour bands of the WLCG/DOMA Data Challenge 2024 report),
where the time goes, and the rules in flight.

A run figure shows the volume wanted, injected and completed over time, the data in flight, the time from rule to `OK`, and
the events per link (`skipped_bad`, `throttled`, ...). The comparison overlays the completed volume and the average rate of
several runs, for example one per student in a data challenge.

"Completed" means that CLINT saw the rule in state `OK`. CLINT looks once per cycle, so times have the resolution of a cycle,
and the last rules of a run may be missing.

## Contributing

Contributions are welcome! Please fork, create feature branches, and submit pull requests.  

See `docs/` directory for developer documentation and contribution guidelines.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
