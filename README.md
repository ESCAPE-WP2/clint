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

`clint_plot.py` draws one figure per call, in the style of [mplhep](https://scikit-hep.org/mplhep) (without mplhep: the same
layout in the plain style). It needs matplotlib and, for the HEP look, mplhep (`pip install -r requirements-plot.txt`).
It does not need rucio.

```
python cli.py plot                              # the run in the current folder: clint_run_<folder>.png
python cli.py plot runA runB runC --out out     # many runs in one figure: clint_class.png
python clint_plot.py runA --label student-1     # the same, without rucio installed
python cli.py plot runA runB --align            # runs made at different times: start them together
```

The figure of one run has two panels: the **completed rate against the target** over time, and **where the time goes**.
With more than one run there is a third panel, the **achievement ratio** (finished during the run / wanted) of each
participant and link, with the colour bands of the WLCG/DOMA Data Challenge 2024 report (green ≥ 0.9, yellow ≥ 0.7,
orange ≥ 0.5, red < 0.5). More than six participants are drawn as one block in the rate panel.

For a data challenge with many participants, each one exports two small CSV files and the instructor draws the class figure:

```
python cli.py export --participant ann                         # clint_rules_ann.csv and clint_cycles_ann.csv (a few kB)
python clint_plot.py --csv folder-with-all-csv --out plots     # clint_class.png
```

`export` writes one row per rule (`clint_rules_<name>.csv`) and one row per cycle (`clint_cycles_<name>.csv`). With Rucio
access it adds the stage times of the transfer request of each rule: conveyor (rule to submitted), queue in FTS, the
transfer itself, and the time until Rucio closes the request (poller and finisher). It also adds the FTS job id. Use
`--no-rucio` to skip this. Rucio keeps one request record per file and destination, so a rule on a dataset that you
used in an earlier run has empty stage times. Without stage times (ledgers only), the last panel shows the time until CLINT
saw the rule in state `OK`: CLINT looks once per cycle, so this is an upper bound with the resolution of a cycle.

## Contributing

Contributions are welcome! Please fork, create feature branches, and submit pull requests.  

See `docs/` directory for developer documentation and contribution guidelines.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
