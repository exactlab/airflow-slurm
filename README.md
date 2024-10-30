# Airflow operator for slurm via SSH

This contains SSH slurm deferral operator and trigger for airflow, largerly ispired by [this gist](https://gist.github.com/elehcim/52150ab6711d49b63d175bd4c8fe3efd).

Example of dag:

```python
from airflow.decorators import dag
from airflow_slurm.ssh_slurm_operator import SSHSlurmOperator


@dag(dag_id="slurm_job")
def slurm():
    slurm_task = SSHSlurmOperator(
        task_id='submit_slurm_task',
        ssh_conn_id='slurm', # Airflow connection defined by AIRFLOW_CONN_{CONN_ID}
        command='/path/to/job.slurm',  # Example command for SLURM job
        env={'SBATCH_JOB_NAME': 'example_job_name'},
        slurm_options={
            'NODES': 1,
            'NTASKS': 1
        },
        cwd='/tmp',  # Set working directory if needed
        tdelta_between_checks=10  # Check job every 10 seconds
    )

    slurm_task


dag = slurm()
```
