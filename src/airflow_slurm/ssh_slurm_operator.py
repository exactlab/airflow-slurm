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
import subprocess  # nosec
from typing import Any, Sequence

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context

from .constants import (
    SCONTROL_COMPLETED_OK,
    SCONTROL_FAILED,
    SCONTROL_FINISHED,
    SCONTROL_RUNNING,
    SLURM_OPTS,
)
from .ssh_slurm_trigger import SSHSlurmTrigger
from .ssh_utils import get_ssh_connection_details
from .templates import SLURM_FILE


class SSHSlurmOperator(BaseOperator):
    r"""Run a linux script or command through Slurm.

    :param command: the command or path to the script to be executed. Allow use of jinja
    :param slurm_options: other parameters we'll pass to SBATCH because it doesn't accept working with variables
        of environment To see the list: SLURM_OPTS dictionary of this file
    :param tdelta_between_checks: how many seconds do we check scontrol to know the status of the job?
    :param do_xcom_push: if True, the last line of the job output will be pushed to XCom when job completes

    If do_xcom_push = True, the last line of the subprocess will be written to XCom
    """

    template_fields: Sequence[str] = (
        "command",
        "slurm_options",
        "modules",
        "setup_commands",
    )
    template_fields_renderers = {"command": "bash"}
    template_ext: Sequence[str] = (".bash",)
    ui_color = "#e4ecf7"

    def __init__(
        self,
        *,
        command: str,
        # TODO: this is useless at this point,
        # I'll leave it here to avoid breaking changes
        ssh_conn_id: str,
        tdelta_between_checks: int = 5,
        slurm_options: dict[str, Any] | None = None,
        modules: list[str] | None = None,
        setup_commands: list[str] | None = None,
        do_xcom_push: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.command = command
        self.ssh_conn_id = ssh_conn_id
        self.slurm_options = slurm_options
        self.tdelta_between_checks = tdelta_between_checks
        self.modules = modules
        self.setup_commands = setup_commands
        self.do_xcom_push = do_xcom_push

    def _build_ssh_command(self, remote_command: list[str]) -> list[str]:
        """Build SSH command by prefixing with ssh invocation.

        Args:
            remote_command: Command to execute on remote host

        Returns:
            Complete SSH command list ready for subprocess
        """
        host, username, port = get_ssh_connection_details(self.ssh_conn_id)

        user_host = f"{username}@{host}" if username else host

        ssh_cmd = ["ssh"]
        if port != 22:
            ssh_cmd.extend(["-p", str(port)])

        ssh_cmd.extend(
            [
                "-o",
                "BatchMode=yes",
                "-o",
                "StrictHostKeyChecking=no",
                user_host,
            ]
        )

        if isinstance(remote_command, str):
            ssh_cmd.append(remote_command)
        else:
            ssh_cmd.extend(remote_command)

        return ssh_cmd

    def _execute_ssh_command(
        self,
        remote_command: list[str] | str,
        input_data: str | None = None,
        timeout: int = 60,
    ) -> tuple[int, str, str]:
        """Execute command via SSH with proper error handling.

        Args:
            remote_command: Command to execute on remote host
            input_data: Optional input to pass to command stdin
            timeout: Command timeout in seconds

        Returns:
            Tuple of (exit_code, stdout, stderr)
        """
        ssh_cmd = self._build_ssh_command(remote_command)

        try:
            process = subprocess.run(
                ssh_cmd,
                input=input_data,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            return process.returncode, process.stdout, process.stderr

        except subprocess.TimeoutExpired:
            raise AirflowException(
                f"SSH command timed out after {timeout} seconds: {ssh_cmd}"
            )
        except subprocess.SubprocessError as e:
            raise AirflowException(f"SSH command failed: {e}")

    def parse_input_and_render_slurm_script(self, context: Context) -> str:
        """Render the SLURM script using the Jinja2 template.

        Uses the operator's context to render the Jinja2 template with
        appropriate variables.

        Args:
            context: Airflow context containing execution information.

        Returns:
            Rendered SLURM script as a string.
        """
        # Mangle job name with submission date
        logical_date = context.get("logical_date") or context.get(
            "execution_date"
        )
        job_date = logical_date.strftime("%Y%m%dT%H%M")
        self.slurm_options["JOB_NAME"] = (
            f"{self.slurm_options.get('JOB_NAME', 'airflow_slurm_job')}_{job_date}"
        )

        # Process slurm options
        slurm_opts = {}
        for key, value in self.slurm_options.items():
            if key in SLURM_OPTS:
                has_value, opt_string = SLURM_OPTS[key]
                if has_value:
                    if value is not None:
                        slurm_opts[key] = f"{opt_string}{value}"
                else:
                    slurm_opts[key] = opt_string

        slurm_script = SLURM_FILE.render(
            slurm_opts=slurm_opts,
            job_command=self.command,
            modules=self.modules,
            setup_commands=self.setup_commands,
        )
        return slurm_script

    def execute(self, context: Context):
        """The function that is executed when we call this operator."""

        def extract_job_id(sbatch_output: str) -> str:
            if sbatch_output.strip().isdigit():
                return sbatch_output.strip()
            elif sbatch_output.split()[-1].isdigit():
                return sbatch_output.split()[-1]
            else:
                raise AirflowException(
                    "Could not determine job id "
                    f"from SBATCH output: {sbatch_output}"
                )

        slurm_script = self.parse_input_and_render_slurm_script(context)

        self.check_job_not_running(context)

        try:
            self.log.info(f"Running script:\n{slurm_script}")

            exit_code, output, error = self._execute_ssh_command(
                ["sbatch", "--parsable"], input_data=slurm_script, timeout=120
            )

            self.log.debug(f"{output}")

            if exit_code != 0:
                raise AirflowException(
                    "SBATCH command failed. "
                    f"The command returned {exit_code=}, "
                    f"with output {output} and error {error}"
                )

            job_id = extract_job_id(output)
            self.log.info(f"Submitted batch job {job_id}")

            # Defer execution for the SLURM trigger
            self.defer(
                trigger=SSHSlurmTrigger(
                    job_id,
                    ssh_conn_id=self.ssh_conn_id,
                    tdelta_between_pokes=self.tdelta_between_checks,
                ),
                method_name="new_slurm_state_log",
            )

        except Exception as e:
            raise AirflowException(f"Error running sbatch: {e}")

    def check_job_not_running(self, context):
        """Check that there is no job running with the current execution date.

        Checks if there is a job in SLURM with the current execution date
        (hour and minute). If so, skips the current task. If the user passes
        the parameter "ignore_multiple_jobs"=true through dag_config
        (--config / UI), the task runs anyway.

        Args:
            context: Airflow context containing execution information.
        """
        if context["params"].get("ignore_multiple_jobs", False):
            self.log.info(
                f"Ignoring if there are multiple slurm jobs submitted with the same job name ({self.slurm_options['JOB_NAME']})"
            )
            return

        self.log.info(
            f"Checking if job {self.slurm_options['JOB_NAME']} is already running..."
        )

        exit_code, stdout, stderr = self._execute_ssh_command(
            # NOTE: --noheader removes column headers, --format=%i returns only job IDs for clean parsing
            [
                "squeue",
                "--name",
                self.slurm_options["JOB_NAME"],
                "--noheader",
                "--format=%i",
            ],
            timeout=30,
        )

        # Log and handle errors
        if exit_code > 0:
            raise AirflowException(
                f"Command execution failed. Exit code: {exit_code}. Error output: {stderr.strip()}"
            )

        self.log.info(f"squeue stdout:\n{stdout.strip()}")

        if len(stdout.split()) > 0:
            raise AirflowSkipException(
                "According to SQUEUE this job is already running for this date!"
            )

    def _get_last_line_from_output(self, output_file: str) -> str | None:
        """Extract the last line from the SLURM job output file.

        :param output_file: Path to the SLURM output file
        :return: Last line or None if file doesn't exist
        """
        try:
            exit_code, stdout, _stderr = self._execute_ssh_command(
                ["tail", "-n1", output_file], timeout=10
            )

            if exit_code == 0:
                return stdout.strip()

        except AirflowException as e:
            self.log.warning(
                f"Failed to read remote output file {output_file} via SSH: {e}"
            )

        return None

    def new_slurm_state_log(self, context, event: dict[str, Any] = None):
        """Handle SLURM state changes and log updates.

        Called by SSHSlurmTrigger when there has been a state change in SLURM
        or there are new lines in the log file.

        Args:
            context: Airflow variables and execution context.
            event: Dictionary containing SLURM job information with keys:
                - slurm_job: Dict with job_id, job_name, state, reason
                - slurm_changed_state: Boolean indicating state change
                - log_number_lines: Total lines in log
                - log_new_lines: List of new log lines
        """
        if (
            event["slurm_changed_state"]
            and event["slurm_job"]["state"] not in SCONTROL_FINISHED
        ):
            # This is ugly and repetitive, but this way the command stays in the log!
            self._log_status_change(event)

        if event["log_new_lines"]:
            for line in event["log_new_lines"]:
                self.log.info(line.rstrip())

        if event["slurm_job"]["state"] in SCONTROL_COMPLETED_OK:
            if event["slurm_changed_state"]:
                self._log_status_change(event)

            # Push last line to XCom if requested
            if self.do_xcom_push and "slurm_job" in event:
                output_file = event["slurm_job"].get("log_out")
                if output_file and output_file != "/dev/null":
                    last_line = self._get_last_line_from_output(output_file)
                    if last_line:
                        self.log.info(f"Pushing to XCom: {last_line}")
                        context["task_instance"].xcom_push(
                            key="return_value", value=last_line
                        )

            return None
        elif event["slurm_job"]["state"] in SCONTROL_FAILED:
            if event["slurm_changed_state"]:
                self._log_status_change(event)
            raise AirflowException("Slurm job failed!")
        elif event["slurm_job"]["state"] in SCONTROL_RUNNING:
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
                f"scontrol returned an unknown state for job #{event['slurm_job']['job_id']}: "
                f"{event['slurm_job']['state']}"
            )

    def _log_status_change(self, event: dict[str, Any]):
        self.log.info(
            f"Slurm reports job #{event['slurm_job']['job_id']} ({event['slurm_job']['job_name']}) has "
            f"changed its state to {event['slurm_job']['state']} "
            f"with reason {event['slurm_job']['reason']}"
        )

    def on_kill(self) -> None:
        """Handle task termination.

        Called when the task is killed or cancelled.
        """
        pass
