import os
import shutil
from pathlib import Path
import datetime
import time
import subprocess as sp
from dask_jobqueue import SLURMCluster
from distributed.utils import TimeoutError

from typing import Optional
from distributed import Client


def start_slurm_cluster(processes: int = 20,
                        cores: int = 40,
                        memory: str = '80 GiB',
                        walltime: str = '01:30:00',
                        wait_timeout: int = 300,
                        adaptive_scale_factor: int = 2,
                        use_scratch_dir: bool = True,
                        reservation: Optional[str] = None,
                        queues_to_try: Optional[list[str]] = None
                        ) -> tuple[Client, SLURMCluster]:
    """
    Start a Dask cluster on a SLURM-managed HPC cluster with adaptive scaling and robust 
    job monitoring. The function will attempt to start the cluster using multiple 
    configurations (with and without reservation) if the initial attempt fails due to high 
    demand on the cluster.
    
    Parameters
    ----------
    processes : int, optional
        Number of processes per job. Default is 20.
    cores : int, optional
        Total number of cores per job. Default is 40.
    memory : str, optional
        Total amount of memory per job. Default is '80 GiB'.
    walltime : str, optional
        The walltime for the job in the format HH:MM:SS. Minimum is 00:45:00. Default is
        '01:30:00'.
    wait_timeout : int, optional
        Timeout in seconds to wait for the cluster to start. Default is 300 seconds
        (5 minutes).
    adaptive_scale_factor : int, optional
        The factor by which to increase the maximum number of workers when scaling up.
        Default is 2 (i.e., double the maximum workers each time).
    use_scratch_dir : bool, optional
        Whether to use a scratch directory for faster I/O of Dask workers. Default is 
        True. Note that the function can't check if compute nodes actually have access to 
        the scratch directory, so set this to False if you're not sure and/or check with 
        your HPC administrators.
    reservation : str, optional
        SLURM reservation name to use. Default is None.
    queues_to_try : list of str, optional
        List of SLURM queues to try in order when starting the cluster. Default is None, 
        which will use ['short', 'standard']. If a reservation is provided and active, it 
        will be tried first on all specified queues before trying without the reservation.
    
    Returns
    -------
    dask_client : Client
        The dask distributed Client object.
    cluster : SLURMCluster
        The dask_jobqueue SLURMCluster object.
    
    Examples
    --------
    >>> from sdc.cluster import start_slurm_cluster
    >>> dask_client, cluster = start_slurm_cluster()
    """
    user = os.getenv('USER')
    home_directory = os.getenv('HOME')
    if any(x is None for x in [user, home_directory]):
        raise RuntimeError("Cannot determine user name or home directory")
    home_directory = Path(home_directory)
    now = datetime.datetime.now()

    # Set up log directory and clean logs older than 2 weeks
    log_dir_base = home_directory.joinpath('.draco_logs')
    log_dir_base.mkdir(exist_ok=True)
    _clean_old_logs(log_dir_base, now, weeks_to_keep=2)
    log_directory = log_dir_base.joinpath(now.strftime('%Y-%m-%dT%H:%M'))
    log_directory.mkdir(parents=True, exist_ok=True)

    # Validate walltime and calculate worker lifetime
    walltime = _validate_walltime(walltime, min_minutes=45)
    worker_lifetime_minutes = _walltime_to_minutes(walltime) - 10

    # Generate unique dashboard port and check for InfiniBand interfaces 
    port = _dashboard_port()
    scheduler_options = {'dashboard_address': f':{port}'}
    job_name = f"dask-worker-{port}"
    interface = _check_infiniband()

    # Common parameters for all configurations
    common_params = {
        'processes': processes,
        'cores': cores,
        'memory': memory,
        'walltime': walltime,
        'interface': interface,
        'death_timeout': 120,
        'job_script_prologue': ['mkdir -p /scratch/$USER'] if use_scratch_dir else [],
        'worker_extra_args': ['--lifetime', f'{worker_lifetime_minutes}m',
                              '--lifetime-stagger', '5m',
                              '--death-timeout', '120'],
        'local_directory': os.path.join('/', 'scratch', user) if use_scratch_dir else None,
        'log_directory': str(log_directory) if log_directory else None,
        'scheduler_options': scheduler_options,
        'job_name': job_name
    }
    
    # Define the configurations to try, starting with the reservation if provided and active
    if queues_to_try is None:
        queues_to_try = ['short', 'standard']
    configurations = []
    if _check_reservation_active(reservation):
        for queue in queues_to_try:
            configurations.append({
                **common_params,
                'queue': queue,
                'job_extra_directives': [f'--reservation={reservation}']
            })
    else:
        reservation = None
    for queue in queues_to_try:
        configurations.append({**common_params, 'queue': queue})
    
    # Try each configuration until we successfully start the cluster or exhaust all options
    start_time = time.time()
    config_index = 0
    try:
        while config_index < len(configurations):
            config = configurations[config_index]
            active_reservation = next(
                (d.split('=', 1)[1] for d in config.get('job_extra_directives', [])
                 if d.startswith('--reservation=')),
                None
            )
            print(f"[INFO] Trying to allocate requested resources using configuration "
                  f"{config_index + 1}/{len(configurations)}:\nqueue={config['queue']}, "
                  f"reservation={active_reservation or 'None'}")
            
            dask_client, cluster = _create_cluster(adaptive_scale_factor, **config)

            while not _is_cluster_ready(dask_client, job_name=job_name):
                if time.time() - start_time > wait_timeout:
                    if config_index < len(configurations) - 1:
                        # Move to the next configuration
                        if reservation:
                            reservation = None
                        config_index += 1
                        try:
                            dask_client.close(timeout=30)
                            cluster.close()
                        except Exception:
                            _cancel_slurm_jobs(job_name)
                        start_time = time.time()  # Reset the timer
                        break
                    else:
                        raise TimeoutError("[INFO] Cluster failed to start within "
                                           "timeout period. This could be due to high "
                                           "demand on the cluster.")
                time.sleep(10)
            else:
                # If we exited the while loop without breaking (i.e., cluster is ready)
                print(f"[INFO] Cluster is ready for computation! :) Dask dashboard "
                      f"available via 'localhost:{port}'")
                return dask_client, cluster

        # If we exhausted all configurations
        raise TimeoutError("[INFO] Cluster failed to start with any configuration within "
                           "the timeout period. This could be due to high demand on the "
                           "cluster.")
    except (SystemExit, KeyboardInterrupt):
        _cancel_slurm_jobs(job_name)
        raise
    except Exception as e:
        _cancel_slurm_jobs(job_name)
        raise e


def _clean_old_logs(log_directory, now, weeks_to_keep: int = 2):
    log_path = Path(log_directory)
    if not log_path.exists():
        return
    
    cutoff_date = now - datetime.timedelta(weeks=weeks_to_keep)
    for dir_path in log_path.iterdir():
        if dir_path.is_dir():
            try:
                dir_date = datetime.datetime.strptime(dir_path.name, "%Y-%m-%dT%H:%M")
                if dir_date < cutoff_date:
                    shutil.rmtree(dir_path)
            except ValueError:
                print(f"Skipping {dir_path}: name does not match expected date format.")
            except Exception as e:
                print(f"Error processing {dir_path}: {e}")


def _validate_walltime(walltime: str, min_minutes: int = 45) -> str:
    """Validate and enforce a minimum walltime. Returns the walltime string unchanged
    if it meets the minimum, otherwise raises a ValueError."""
    try:
        total_minutes = _walltime_to_minutes(walltime)
    except (ValueError, AttributeError):
        raise ValueError(f"Invalid walltime format '{walltime}'. Expected HH:MM:SS.")
    if total_minutes < min_minutes:
        raise ValueError(
            f"Walltime '{walltime}' is too short. Minimum is "
            f"{min_minutes // 60:02d}:{min_minutes % 60:02d}:00."
        )
    return walltime


def _walltime_to_minutes(walltime: str) -> int:
    """Convert a walltime string in HH:MM:SS format to total minutes."""
    h, m, s = walltime.split(':')
    return int(h) * 60 + int(m) + int(s) // 60


def _dashboard_port(port: int = 8787) -> int:
    """Find a free port for the Dask dashboard based on the user id."""
    try:
        uid = sp.check_output(["id", "-u"], text=True).strip()
        for i in uid:
            port += int(i)
    except (sp.SubprocessError, ValueError):
        pass  # if we can't get the user id, just return the base port
    try:
        listening = sp.check_output(
            ["bash", "-c", "lsof -i -P -n | grep LISTEN"],
            text=True,
        ).strip().splitlines()
        used_ports = set()
        for line in listening:
            parts = line.split(":")
            if len(parts) >= 2:
                port_str = parts[1].split()[0]
                if port_str.isdigit():
                    used_ports.add(int(port_str))
        while port in used_ports:
            port += 1
    except sp.SubprocessError:
        pass  # if lsof fails, return the base port
    return port


def _check_infiniband() -> Optional[str]:
    try:
        # Check if InfiniBand interfaces are present
        output = sp.check_output(
            ["ibstat", "-l"], text=True
        ).strip()
        if not output:
            return None

        # Get the name of the first InfiniBand interface found in /sys/class/net
        ib_interfaces = [p.name for p in Path("/sys/class/net").iterdir() if 
                         p.name.startswith("ib")]
        return ib_interfaces[0] if ib_interfaces else None

    except Exception:
        return None


def _check_reservation_active(reservation_name: str) -> bool:
    """Check if a SLURM reservation is active."""
    if not reservation_name:
        return False
    try:
        output = sp.check_output(
            ["scontrol", "show", "reservation", reservation_name],
            text=True,
        ).strip()
    except Exception:
        return False

    return (
        f"ReservationName={reservation_name}" in output
        and "State=ACTIVE" in output
    )


def _create_cluster(adaptive_scale_factor: int, **kwargs) -> tuple[Client, SLURMCluster]:
    """Create a dask_jobqueue.SLURMCluster and a distributed.Client."""
    cluster = SLURMCluster(**kwargs)
    dask_client = Client(cluster)
    cluster.adapt(minimum=1, maximum=adaptive_scale_factor * kwargs['processes'],
                  # https://github.com/dask/dask-jobqueue/issues/498#issuecomment-1233716189
                  worker_key=lambda state: state.address.split(':')[0], interval='10s')
    return dask_client, cluster


def _is_cluster_ready(client: Client,
                     min_workers: int = 1,
                     recent_job_time: int = 120,
                     job_name: str = "dask-worker"
                     ) -> bool:
    """Check the status of recent SLURM jobs for dask workers"""
    try:
        current_time = datetime.datetime.now()
        user = os.getenv("USER")
        if not user:
            raise RuntimeError("Cannot determine user name from environment variable 'USER'")
        
        # Get all Slurm job IDs for current user with the given job name
        cmd = ["squeue", "-u", user, "-n", job_name, "-h", "-o", "%i %S"]
        output = sp.check_output(cmd, text=True, stderr=sp.DEVNULL).strip().splitlines()
        
        # Filter jobs that are N/A or started recently
        recent_job_ids = []
        for line in output:
            if not line:
                continue

            parts = line.split()
            if len(parts) != 2:
                continue

            job_id, start_time = parts
            if start_time == 'N/A':
                recent_job_ids.append(job_id)
            else:
                try:
                    start_dt = datetime.datetime.strptime(start_time,
                                                          '%Y-%m-%dT%H:%M:%S')
                    if (current_time - start_dt).total_seconds() <= recent_job_time:
                        recent_job_ids.append(job_id)
                except ValueError:
                    continue
        
        if not recent_job_ids:
            print(f"No recent SLURM jobs found for {job_name}")
            return False
        
        running_jobs = []
        pending_jobs = []
        
        # Check status of each job
        for job_id in recent_job_ids:
            job_info = _get_slurm_job_info(job_id)
            if not job_info:
                continue
            
            if job_info['state'] == 'RUNNING':
                running_jobs.append(job_id)
            elif job_info['state'] == 'PENDING':
                pending_jobs.append(job_id)
        
        # If we have any running jobs, check if we have enough workers
        if running_jobs:
            n_workers = len(client.scheduler_info()['workers'])
            if n_workers >= min_workers:
                return True
            else:
                print(f"Cluster has {n_workers} workers, but {min_workers} required")
                return False
        
        if pending_jobs:
            print(f"All jobs are pending. Job IDs: {', '.join(pending_jobs)}")
            return False
        
        return False
    
    except Exception as e:
        print(f"Error checking cluster status: {e}")
        return False


def _get_slurm_job_info(job_id: str) -> dict:
    """Get information about a SLURM job using squeue."""
    try:
        cmd = ["squeue", "-j", job_id, "-o", "%i|%T|%S", "-h"]
        output = sp.check_output(cmd, text=True, stderr=sp.DEVNULL).strip()
        if not output:
            return {}
        
        parts = output.split("|")
        if len(parts) != 3:
            return {}
        
        job_id, state, start_time = parts
        return {
            'job_id': job_id,
            'state': state.strip(),
            'start_time': start_time.strip()
        }
    except sp.SubprocessError:
        return {}


def _cancel_slurm_jobs(job_name: str):
    """Cancel all SLURM jobs for the current user with the given job name."""
    try:
        user = os.getenv("USER")
        if not user:
            raise RuntimeError("Cannot determine user name from environment variable 'USER'")
        cmd = ["squeue", "-u", user, "-n", job_name, "-h", "-o", "%i"]
        output = sp.check_output(cmd, text=True, stderr=sp.DEVNULL).strip().splitlines()
        job_ids = [line.strip() for line in output if line.strip()]
        for job_id in job_ids:
            try:
                sp.run(["scancel", job_id], check=True, timeout=5)
                print(f"Canceled job {job_id}")
            except sp.TimeoutExpired:
                print(f"Timeout while trying to cancel job {job_id}")
            except sp.SubprocessError as e:
                print(f"Failed to cancel job {job_id}: {e}")
    except Exception as e:
        print(f"Error canceling jobs: {e}")
