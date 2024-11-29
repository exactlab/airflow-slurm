# Airflow SSH Operator for SLURM

This repository provides an Airflow operator and trigger that integrate SLURM workload manager commands over SSH. Largerly ispired by [this gist by elehcim](https://gist.github.com/elehcim/52150ab6711d49b63d175bd4c8fe3efd).

# Example DAG:

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
