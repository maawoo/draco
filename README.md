# draco

**draco** is a custom Python wrapper around [dask-jobqueue](https://jobqueue.dask.org/)'s `SLURMCluster`, 
named after the HPC system *Draco* of the Friedrich Schiller University Jena, where it has been actively used in research and teaching.

While `draco` was designed to be flexible, it has only been tested on the Draco HPC system and has not been verified on other SLURM-based HPC systems. 
Feedback and reports from other systems are welcome!

## What draco adds on top of `SLURMCluster`

- **Adaptive scaling** — workers are scaled adaptively between 1 and a configurable multiple of the requested number of processes.
- **Automatic configuration fallback** — if the cluster fails to become ready within a timeout period, `draco` automatically retries with the next available configuration, cycling through a combination of SLURM reservation (if provided) and SLURM queues (e.g., `short` → `standard`).
- **Cluster readiness check** — polls SLURM job state and the number of connected Dask workers before returning, so the client is ready to use immediately.
- **InfiniBand detection** — automatically detects and configures an InfiniBand network interface if one is available on the login node.
- **Dashboard port assignment** — automatically selects a free port for the Dask dashboard based on the user ID to avoid conflicts in multi-user environments.
- **Log management** — writes SLURM job logs to `~/.draco_logs/<timestamp>` and cleans up old log directories automatically.
- **Graceful shutdown** — cancels all associated SLURM jobs on timeout, exception, or user interrupt.

## Usage

Usage is quite simple. There is one main function to start a SLURM cluster:

```python
from draco.cluster import start_slurm_cluster

dask_client, cluster = start_slurm_cluster()
```

### Options

The default setting will launch 20 Dask workers, each with 2 cores and 4 GiB of memory.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `processes` | `int` | `20` | Number of processes (workers) per job. |
| `cores` | `int` | `40` | Total number of cores per job. |
| `memory` | `str` | `'80 GiB'` | Total amount of memory per job. |
| `walltime` | `str` | `'01:30:00'` | Walltime in `HH:MM:SS` format. Minimum is `00:45:00`. |
| `wait_timeout` | `int` | `300` | Seconds to wait for the cluster to become ready before trying the next configuration. |
| `adaptive_scale_factor` | `int` | `2` | Maximum number of workers is set to `adaptive_scale_factor × processes`. |
| `use_scratch_dir` | `bool` | `True` | If `True`, Dask workers use `/scratch/$USER` as their local directory. Set to `False` if compute nodes do not have a local scratch filesystem. |
| `reservation` | `str` | `None` | SLURM reservation name. If provided and active, it will be tried first across all queues. |
| `queues_to_try` | `list[str]` | `['short', 'standard']` | SLURM queues to try in order. If a reservation is active, each queue is tried with the reservation first, then without. |
