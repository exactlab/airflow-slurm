# Airflow SSH Operator for SLURM

This repository provides an Airflow operator and trigger that integrate SLURM workload manager commands over SSH. Largerly ispired by [this gist by elehcim](https://gist.github.com/elehcim/52150ab6711d49b63d175bd4c8fe3efd).

## SSH Connection Setup

The operator requires an SSH connection configured in Airflow. You can set this up via the Airflow web UI or environment variables:

### Via Airflow Web UI:
1. Go to Admin → Connections
2. Create a new connection with:
   - **Connection Id**: `slurm` (or your preferred name)
   - **Connection Type**: `SSH`
   - **Host**: Your SLURM cluster hostname
   - **Username**: Your SSH username
   - **Port**: SSH port (default: 22)
   - **Extra**: JSON with optional SSH key file path:
     ```json
     {"key_file": "/path/to/your/private/key"}
     ```

### Via Environment Variable:
```bash
# Basic SSH connection
export AIRFLOW_CONN_SLURM='ssh://username@hostname:22'

# With custom SSH key file
export AIRFLOW_CONN_SLURM='ssh://username@hostname:22?key_file=%2Fpath%2Fto%2Fyour%2Fprivate%2Fkey'
```

Note: When using environment variables, URL-encode the key file path (e.g., `/` becomes `%2F`).

### Using SSH Agent (Recommended)

Instead of specifying key files manually, you can use SSH agent for automatic key management:

```bash
# Start SSH agent (if not already running)
eval "$(ssh-agent -s)"

# Add your private key to the agent
ssh-add /path/to/your/private/key

# Verify keys are loaded
ssh-add -l

# Test SSH connection
ssh username@hostname
```

When using SSH agent, simply omit the `key_file` parameter from your connection configuration. The operator will automatically use keys loaded in the SSH agent. This approach is more secure as private keys remain encrypted in memory and don't need to be specified in configuration files.

## Example DAG:

Here's an example of how to use the `SSHSlurmOperator` in an Airflow DAG:

```python
from airflow.decorators import dag
from airflow_slurm.ssh_slurm_operator import SSHSlurmOperator


@dag(dag_id="slurm_job")
def slurm():
    slurm_task = SSHSlurmOperator(
        task_id='submit_slurm_task',
        ssh_conn_id='slurm', # Airflow connection ID (AIRFLOW_CONN_{CONN_ID})
        command='srun bash -c "sleep 20; echo Running task \$SLURM_PROCID on node \$(hostname)"',  # Example command for SLURM job
        slurm_options={
            "JOB_NAME": "example_job_name",
            "OUTPUT_FILE": "/path/to/slurmTEST-%j.out",
            "TIME": "01:00:00",
            "NODES": 2,
            "NTASKS": 8
        },
        tdelta_between_checks=10  # Poll interval (in seconds) for job status
    )

    slurm_task


dag = slurm()
```

# SLURM Options

For a complete list of all available `slurm_options`, please refer to the [SLURM Options Instructions](./SLURM_OPTIONS.md). This document contains detailed descriptions of each option and how to use them with the Airflow operator.
