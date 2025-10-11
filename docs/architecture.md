# CLINT Architecture
The architecture of CLINT is largely inspired by the [`dc_inject`](https://gitlab.cern.ch/wlcg-doma/dc_inject) tool developed in the context of the WLCG Data Operations Management Activity.

## Core Components

### 1. Configuration Module

- Reads CSV configuration files listing source-to-destination injection links, specifying target throughput rates.
- Loads dataset lists per storage, sorted either by size or preference.
- Supports runtime options to fine-tune injection behavior (intervals, fudge factors, rule lifetimes).

### 2. Injection Engine

- Manages injection threads, one per source-destination link.
- Each thread selects suitable datasets available on the source but not on the destination.
- Injects data respecting configured throughput targets and limits, including fudge factors and maximum over-injection thresholds.
- Supports single-threaded mode for debugging.

### 3. Storage Backend Interface

- Currently implemented with Rucio client to interact with datasets and replication rules.
- Abstracts dataset presence and replication rule creation to enable potential future support for other storage systems.

### 4. Monitoring and Logging

- Logs every injected dataset with metadata: timestamp, dataset ID, source, destination, and user account.
- Persists information on injected datasets to avoid duplication during the same run.
- Enables auditing and post-mortem analysis.

### 5. Graceful Shutdown

- Listens to system signals (SIGINT, SIGTERM) to initiate safe termination.
- Ensures all running threads dump current state and stop cleanly.

***

## Data Flow

1. **Initialization:** Reads configuration and dataset files; creates Rucio client; validates runtime parameters.  
2. **Thread Launch:** Creates a thread per injection link to manage parallel dataset injections.  
3. **Dataset Selection:** Each thread repeatedly selects a subset of fresh, eligible datasets from its source storage.  
4. **Injection:** Datasets are injected by creating replication rules on Rucio, respecting configured throughput and timing.  
5. **Logging:** Injection events are logged immediately; the tool maintains an internal map of injected datasets with timestamps.  
6. **Shutdown:** On signal, threads finalize their state dumps, log completion, and the program exits.
