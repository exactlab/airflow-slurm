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
from typing import Any
from typing import Sequence

import dateutil.parser
from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.context import Context
from airflow.utils.operator_helpers import context_to_airflow_vars

from airflow_slurm.constants import SACCT_COMPLETED_OK
from airflow_slurm.constants import SACCT_FAILED
from airflow_slurm.constants import SACCT_FINISHED
from airflow_slurm.constants import SACCT_RUNNING
from airflow_slurm.constants import SLURM_OPTS
from airflow_slurm.ssh_slurm_trigger import SSHSlurmTrigger
from airflow_slurm.templates import SLURM_FILE


class SSHSlurmOperator(BaseOperator):
    r"""Run a linux script or command through Slurm.

    :param command: the command or path to the script to be executed. Allow use of jinja
    :param slurm_options: other parameters we'll pass to SBATCH because it doesn't accept working with variables
        of environment To see the list: SLURM_OPTS dictionary of this file
    :param tdelta_between_checks: how many seconds do we check SACCT to know the status of the job?

    If do_xcom_push = True, the last line of the subprocess will be written to XCom
    """

    template_fields: Sequence[str] = (
        "command",
        "slurm_options",
        "setup_commands",
    )
    template_fields_renderers = {"command": "bash"}
    template_ext: Sequence[str] = (".bash",)
    ui_color = "#e4ecf7"

    def __init__(
        self,
        *,
        command: str,
        ssh_conn_id: str,
        tdelta_between_checks: int = 5,
        slurm_options: dict[str, Any] | None = None,
        setup_commands: list[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.command = command
        self.ssh_conn_id = ssh_conn_id
        self.slurm_options = slurm_options
        self.tdelta_between_checks = tdelta_between_checks
        self.setup_commands = setup_commands

    @cached_property
    def ssh_hook(self):
        """Returns hook for running the command."""
        return SSHHook(ssh_conn_id=self.ssh_conn_id)

    def parse_input_and_render_slurm_script(self, context: Context) -> str:
        """Render the SLURM script using the Jinja2 template and the operator's
        context.

        :param context: Airflow context
        :return: Rendered SLURM script as a string
        """

        # Mangle job name with submission date
        airflow_context_vars = context_to_airflow_vars(
            context, in_env_var_format=True
        )
        job_date = dateutil.parser.isoparse(
            airflow_context_vars["AIRFLOW_CTX_EXECUTION_DATE"]
        ).strftime("%Y%m%dT%H%M")
        self.slurm_options["JOB_NAME"] = (
            f'{self.slurm_options.get("JOB_NAME", "airflow_slurm_job")}_{job_date}'
        )

        # Process slurm options
        slurm_opts = {}
        for key, value in self.slurm_options.items():
            if key in SLURM_OPTS:
                has_value, opt_string = SLURM_OPTS[key]
                if has_value:
                    slurm_opts[key] = f"{opt_string}{value}"
                else:
                    slurm_opts[key] = opt_string

        slurm_script = SLURM_FILE.render(
            slurm_opts=slurm_opts,
            job_command=self.command,
            setup_commands=self.setup_commands,
        )
        return slurm_script

    def execute(self, context: Context):
        """The function that is executed when we call this operator."""

        slurm_script = self.parse_input_and_render_slurm_script(context)

        self.check_job_not_running(context)

        with self.ssh_hook.get_conn() as client:
            stdin, stdout, stderr = client.exec_command(
                "bash -l -c 'sbatch --parsable'"
            )
            stdin.write(slurm_script)
            stdin.channel.shutdown_write()
            self.log.info(f"Running script:\n{slurm_script}")
            output = stdout.read().decode().strip()
            error = stderr.read().decode().strip()
            self.log.debug(f"{output}")
            exit_code = stdout.channel.recv_exit_status()

            if exit_code != 0:
                raise AirflowException(
                    f"SBATCH command failed. The command returned a non-zero exit code {exit_code}, "
                    f"with output {output} and error {error}"
                )
            elif not str(output).isdigit():
                raise AirflowException(
                    f"SBATCH command did not return a job id: {output}"
                )

            self.log.info(f"Submitted batch job {output}")

            # Defer execution for the SLURM trigger
            self.defer(
                trigger=SSHSlurmTrigger(
                    output,
                    ssh_conn_id=self.ssh_conn_id,
                    tdelta_between_pokes=self.tdelta_between_checks,
                ),
                method_name="new_slurm_state_log",
            )

    def check_job_not_running(self, context):
        """Check that there is no job in the slurm with the current execution
        date (hour and minute). If so, skips the current task.

        In case the user passes, through the dag_config (--config / UI)
        the parameter "ignore_multiple_jobs"=true, we run the task
        anyway.

        :param context:
        :return:
        """
        if context["params"].get("ignore_multiple_jobs", False):
            self.log.info(
                f"Ignoring if there are multiple slurm jobs submitted with the same job name ({self.slurm_options['JOB_NAME']})"
            )
            return

        self.log.info(
            f"Checking if job {self.slurm_options['JOB_NAME']} is already running..."
        )

        with self.ssh_hook.get_conn() as client:
            exit_code, stdout, stderr = self.ssh_hook.exec_ssh_client_command(
                ssh_client=client,
                command=(
                    "bash -l -c 'squeue -n"
                    f" {self.slurm_options['JOB_NAME']} -h -o %i'"
                ),
                environment=None,
                get_pty=True,
            )
        out = stdout.decode()
        if exit_code > 0:
            raise AirflowException(
                f"Command execution failed. Exit code: {exit_code}. Error output: {out}"
            )
        self.log.info(f"{out}")
        # if len(out.split()) > 0:
        #     raise AirflowSkipException(
        #         "According to SQUEUE this job is already running for this date!"
        #     )

    def new_slurm_state_log(self, context, event: dict[str, Any] = None):
        """It is the function that SSHSlurmTrigger calls when there has been a
        state change in the SLURM or there are new lines in the file of log.

        :param context: some airflow variables
        :param event: {"slurm_job": {"job_id": str: "job_name": str:
            "state": str, "reason": str}, "slurm_changed_state: bool,
            "log_number_lines": int, # those that the log has in total,
            it is not len(event["log_new_lines"]) "log_new_lines":
            List[str]}
        :return:
        """
        if (
            event["slurm_changed_state"]
            and event["slurm_job"]["state"] not in SACCT_FINISHED
        ):
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
            self.defer(
                trigger=SSHSlurmTrigger(
                    event["slurm_job"]["job_id"],
                    ssh_conn_id=self.ssh_conn_id,
                    last_known_state=event["slurm_job"]["state"],
                    last_known_log_lines=event["log_number_lines"],
                    tdelta_between_pokes=self.tdelta_between_checks,
                ),
                method_name="new_slurm_state_log",
            )
        else:
            raise AirflowException(
                f"SACCT returned an unknown state for job #{event['slurm_job']['job_id']}: "
                f"{event['slurm_job']['state']}"
            )

    def _log_status_change(self, event: dict[str, Any]):
        self.log.info(
            f"Slurm reports job #{event['slurm_job']['job_id']} ({event['slurm_job']['job_name']}) has "
            f"changed its state to {event['slurm_job']['state']} "
            f"with reason {event['slurm_job']['reason']}"
        )

    def on_kill(self) -> None:
        pass
