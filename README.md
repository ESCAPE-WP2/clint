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

## Contributing

Contributions are welcome! Please fork, create feature branches, and submit pull requests.  

See `docs/` directory for developer documentation and contribution guidelines.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
