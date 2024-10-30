# This program is free software: you can redistribute it and/or modify it 
# under the terms of the GNU General Public License as published by 
# the Free Software Foundation, version 3 of the License.
#
# This program is distributed in the hope that it will be useful, 
# but WITHOUT ANY WARRANTY; without even the implied warranty of 
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. 
# See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License 
# along with this program. If not, see <https://www.gnu.org/licenses/>.
#
# Original Copyright @ecodina and Michele Mastropietro
# Modified by Andrea Recchia, 2024
# Licence: GPLv3

from pathlib import Path
from typing import Dict, Optional, Sequence, Any, List
import dateutil.parser

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import Variable
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from airflow.utils.operator_helpers import context_to_airflow_vars

from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow_slurm.constants import SACCT_COMPLETED_OK, SACCT_FAILED, SACCT_FINISHED, SACCT_RUNNING, SLURM_OPTS

from airflow_slurm.ssh_slurm_trigger import SSHSlurmTrigger



class SSHSlurmOperator(BaseOperator):
    r"""
    Run a linux script or command through Slurm.

    :param command: the command or path to the script to be executed. Allow use of jinja
    :param env: a dictionary with the environment variables that slurm will see. At least "SBATCH_JOB_NAME".
        Allow use of jinja
    :param slurm_options: other parameters we'll pass to SBATCH because it doesn't accept working with variables
        of environment To see the list: SLURM_OPTS dictionary of this file
    :param tdelta_between_checks: how many seconds do we check SACCT to know the status of the job?
    :param cwd: Working directory to execute the command in.
        If None (default), the command is run in a temporary directory.
        This is because of the subprocess that runs SBATCH. Alternatively, the parameter slurm_options["CHDIR"] can be passed

    If do_xcom_push = True, the last line of the subprocess will be written to XCom
    """

    template_fields: Sequence[str] = ('command', 'env', 'slurm_options')
    template_fields_renderers = {'command': 'bash', 'env': 'json'}
    template_ext: Sequence[str] = (
        '.bash',
    )
    ui_color = '#e4ecf7'

    def __init__(
            self,
            *,
            command: str,
            ssh_conn_id: str,
            env: Optional[Dict[str, str]] = None,
            cwd: Optional[str] = None,
            tdelta_between_checks: int = 5,
            slurm_options: Optional[Dict[str, Any]] = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.command = command
        self.ssh_conn_id = ssh_conn_id
        self.env = env
        self.cwd = cwd
        self.slurm_options = slurm_options
        self.tdelta_between_checks = tdelta_between_checks

    @cached_property
    def ssh_hook(self):
        """Returns hook for running the command"""
        return SSHHook(ssh_conn_id=self.ssh_conn_id)

    def get_env(self, context):
        """
        Create a dictionary with the environment variables that sbatch will see.

        In addition, modify the job name in order to put the date and time of execution
        """
        self.log.info("ENV: " + str(self.env))
        if self.env is not None:
            env = self.env
        else:
            env = dict()

        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        job_date = dateutil.parser.isoparse(airflow_context_vars["AIRFLOW_CTX_EXECUTION_DATE"]).strftime("%Y%m%dT%H%M")
        self.log.debug(
            'Exporting the following env vars:\n%s',
            '\n'.join(f"{k}={v}" for k, v in airflow_context_vars.items()),
        )
        env.update(airflow_context_vars)
        env["SBATCH_JOB_NAME"] = f'{env["SBATCH_JOB_NAME"]}_{job_date}'
        return env

    def build_sbatch_command(self) -> List[str]:
        """
        It is the function that builds the order of the sbatch from the instructions given to the DAG.

        :return: the command that will execute the subprocess, in a list
        """
        # There must be the --parsable option: this way we can read the job id much easier
        cmd = ["sbatch", "--parsable"]

        slurm_log_folder = Variable.get("SLURM_LOGS_FOLDER", default_var="/tmp")
        if not slurm_log_folder:
            raise AirflowException("Please set the Airflow global variable SLURM_LOGS_FOLDER")
        cmd.append(f"--output={Path(slurm_log_folder) / 'slurm-%j.out'}")

        if self.slurm_options:
            # We have extra options, which we cannot pass to slurm via environment variables
            try:
                for opt in self.slurm_options:
                    if not SLURM_OPTS[opt][0]:
                        cmd.append(SLURM_OPTS[opt][1])
                    else:
                        cmd.append(f"{SLURM_OPTS[opt][1]}{self.slurm_options[opt]}")
            except:
                raise AirflowException(
                    f"Please make sure all the slurm options are correctly set: {self.slurm_options}")

        cmd.append(self.command)

        return cmd

    def execute(self, context: Context):
        """
        The function that is executed when we call this operator
        """
        if self.cwd is not None:
            if not Path(self.cwd).exists():
                raise AirflowException(f"Can not find the cwd: {self.cwd}")
            if not Path(self.cwd).is_dir():
                raise AirflowException(f"The cwd {self.cwd} must be a directory")

        env = self.get_env(context)
        self.check_job_not_running(env, context)

        command = self.build_sbatch_command()

        with self.ssh_hook.get_conn() as client:
            exit_code, stdout, stderr = self.ssh_hook.exec_ssh_client_command(
                ssh_client=client,
                command=" ".join(command),
                environment=env,
                get_pty=True,
            )
        output = stdout.decode().strip()
        error = stderr.decode().strip()
        self.log.debug(f"{output}")

        if exit_code != 0:
            raise AirflowException(
                f'SBATCH command failed. The command returned a non-zero exit code {exit_code}, '
                f'with output {output} and error {error}'
            )
        elif not str(output).isdigit():
            raise AirflowException(f"SBATCH command did not return a job id: {output}")

        self.log.info(f"Submitted batch job {output}")

        # We already have the job id. We expect a status change / new lines in the log
        self.defer(trigger=SSHSlurmTrigger(output, tdelta_between_pokes=self.tdelta_between_checks),
                   method_name="new_slurm_state_log")

        return output

    def check_job_not_running(self, env: dict, context):
        """
        Check that there is no job in the slurm with the current execution date (hour and minute). If so,
        skips the current task.

        In case the user passes, through the dag_config (--config / UI) the parameter "ignore_multiple_jobs"=true,
        we run the task anyway.

        :param env:
        :param context:
        :return:
        """
        if context["params"].get("ignore_multiple_jobs", False):
            self.log.info(f"Ignoring if there are multiple slurm jobs submitted with the same job name ({env['SBATCH_JOB_NAME']})")
            return

        self.log.info(f"Checking if job {env['SBATCH_JOB_NAME']} is already running...")
        with self.ssh_hook.get_conn() as client:
            exit_code, stdout, stderr = self.ssh_hook.exec_ssh_client_command(
                ssh_client=client,
                command=" ".join(["squeue", "-n", env["SBATCH_JOB_NAME"], "-h", "-o", "%i"]),
                environment=env,
                get_pty=True,
            )
        out = stdout.decode()
        self.log.info(f"{out}")
        if len(out.split()) > 0:
            raise AirflowSkipException("According to SQUEUE this job is already running for this date!")

    def new_slurm_state_log(self, context, event: Dict[str, Any] = None):
        """
        It is the function that SSHSlurmTrigger calls when there has been a state change in the SLURM or there are new lines in the file
        of log.

        :param context: some airflow variables
        :param event: {"slurm_job": {"job_id": str:
                                     "job_name": str:
                                     "state": str,
                                     "reason": str},
                      "slurm_changed_state: bool,
                      "log_number_lines": int,      # those that the log has in total, it is not len(event["log_new_lines"])
                      "log_new_lines": List[str]}
        :return:
        """
        if event["slurm_changed_state"] and event["slurm_job"]["state"] not in SACCT_FINISHED:
            # This is ugly and repetitive, but this way the command stays in the log!
            self._log_status_change(event)

        if event["log_new_lines"]:
            for line in event["log_new_lines"]:
                self.log.info(line.rstrip())

        if event["slurm_job"]["state"] in SACCT_COMPLETED_OK:
            if event["slurm_changed_state"]:
                self._log_status_change(event)
            return None
        elif event["slurm_job"]["state"] in SACCT_FAILED:
            if event["slurm_changed_state"]:
                self._log_status_change(event)
            raise AirflowException("Slurm job failed!")
        elif event["slurm_job"]["state"] in SACCT_RUNNING:
            self.defer(trigger=SSHSlurmTrigger(event["slurm_job"]["job_id"],
                                            last_known_state=event["slurm_job"]["state"],
                                            last_known_log_lines=event["log_number_lines"],
                                            tdelta_between_pokes=self.tdelta_between_checks),
                       method_name="new_slurm_state_log")
        else:
            raise AirflowException(f"SACCT returned an unknown state for job #{event['slurm_job']['job_id']}: "
                                   f"{event['slurm_job']['state']}")

    def _log_status_change(self, event: Dict[str, Any]):
        self.log.info(f"Slurm reports job #{event['slurm_job']['job_id']} ({event['slurm_job']['job_name']}) has "
                      f"changed its state to {event['slurm_job']['state']} "
                      f"with reason {event['slurm_job']['reason']}")

    def on_kill(self) -> None:
        pass