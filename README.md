# Airflow SSH Operator for SLURM

This repository provides an Airflow operator and trigger that integrate SLURM workload manager commands over SSH. Largerly ispired by [this gist by elehcim](https://gist.github.com/elehcim/52150ab6711d49b63d175bd4c8fe3efd).

# Example dag:

Here's an example of how to use the `SSHSlurmOperator` in an Airflow DAG:

```python
from airflow.decorators import dag
from airflow_slurm.ssh_slurm_operator import SSHSlurmOperator


@dag(dag_id="slurm_job")
def slurm():
    slurm_task = SSHSlurmOperator(
        task_id='submit_slurm_task',
        ssh_conn_id='slurm', # Airflow connection ID (AIRFLOW_CONN_{CONN_ID})
        command='--wrap "sleep 20"',  # Example command for SLURM job
        env={'SBATCH_JOB_NAME': 'example_job_name'},
        slurm_options={
            'NODES': 1,
            'NTASKS': 1
        },
        tdelta_between_checks=10  # Poll interval (in seconds) for job status
    )

    slurm_task


dag = slurm()
```
