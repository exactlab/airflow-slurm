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
import asyncio
import logging
from collections import Counter
from typing import Any, Iterable

import asyncssh
from airflow.exceptions import AirflowException
from airflow.triggers.base import BaseTrigger, TriggerEvent

from .ssh_utils import aget_ssh_connection_details

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

TERMINAL_STATES = {
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "TIMEOUT",
    "NODE_FAIL",
    "PREEMPTED",
    "OUT_OF_MEMORY",
    "BOOT_FAIL",
    "DEADLINE",
    "REVOKED",
    "SUSPENDED",
    "SPECIAL_EXIT",
}


def parse_scontrol_record(line):
    out = {}
    for kv in line.split():
        k, v = kv.split("=", maxsplit=1)
        out[k] = v
    return out


class SSHSlurmTrigger(BaseTrigger):
    def __init__(
        self,
        jobid: str,
        ssh_conn_id: str,
        last_known_state: str | None = None,
        last_known_log_lines: int = 0,
        tdelta_between_pokes: int = 20,
        **kwargs,
    ):
        """:param jobid: the slurm's job id

        :param last_known_state: the last known slurm's state
        :param last_known_log_lines: how many lines did the log have IN TOTAL the last time we opened it?
        :param tdelta_between_pokes: how many SECONDS should we wait between checks of the log file & scontrol
        """
        super().__init__()
        self.jobid = jobid
        self.ssh_conn_id = ssh_conn_id
        # FIXME:
        # Initialising with safe-ish values. This should be improved, e.g.,
        # by capturing the output of the job submission call.
        self.last_full_state = {
            "job_id": jobid,
            "job_name": "unknown",
            "state": "unknown",
            "reason": "unknown",
            "log_out": "/dev/null",
            "log_err": "/dev/null",
        }
        self.last_known_state = last_known_state
        self.last_known_log_lines = last_known_log_lines
        self.tdelta_between_pokes = tdelta_between_pokes

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the trigger for Airflow.

        Returns:
            Tuple containing the trigger class path and initialization parameters.
        """
        return (
            "airflow_slurm.ssh_slurm_trigger.SSHSlurmTrigger",
            {
                "job_id": self.jobid,
                "jobid": self.jobid,
                "ssh_conn_id": self.ssh_conn_id,
                "last_known_state": self.last_known_state,
                "last_known_log_lines": self.last_known_log_lines,
                "tdelta_between_pokes": self.tdelta_between_pokes,
            },
        )

    async def _execute_ssh_command(
        self, command: list[str] | str, timeout: int = 10
    ) -> tuple[int, str, str]:
        """Execute command via SSH using asyncssh.

        Args:
            command: Command to execute on remote host
            timeout: Command timeout in seconds

        Returns:
            Tuple of (exit_code, stdout, stderr)
        """
        ssh_args = await aget_ssh_connection_details(self.ssh_conn_id)

        connect_kwargs = {
            "known_hosts": None,
        }

        if ssh_args.key_file:
            connect_kwargs["client_keys"] = ssh_args.key_file

        if ssh_args.server_host_key_algs:
            connect_kwargs["server_host_key_algs"] = (
                ssh_args.server_host_key_algs
            )

        command_str = (
            " ".join(command) if isinstance(command, list) else command
        )
        max_retries = 5

        for attempt in range(max_retries):
            try:
                async with asyncssh.connect(
                    ssh_args.host,
                    username=ssh_args.username,
                    port=ssh_args.port,
                    **connect_kwargs,
                ) as conn:
                    result = await conn.run(command_str, timeout=timeout)
                    return result.exit_status, result.stdout, result.stderr

            except asyncio.TimeoutError:
                if attempt < max_retries - 1:
                    delay = 2 ** (attempt + 1)
                    logger.warning(
                        "SSH command '%s' timed out after %d seconds. "
                        "Retry %d/%d in %d seconds.",
                        command_str,
                        timeout,
                        attempt + 1,
                        max_retries - 1,
                        delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    raise AirflowException(
                        f"SSH command '{command_str}' timed out after "
                        f"{timeout} seconds and {max_retries} retry attempts"
                    )
            except asyncssh.Error as e:
                raise AirflowException(
                    f"SSH connection failed for command '{command_str}': {e}"
                )

    async def get_scontrol_output(self) -> dict | None:
        """Get SLURM job status using scontrol with sacct fallback.

        Attempts to retrieve job status following this flow:

        1. Primary method: Execute `scontrol show job <jobid>`
           - If successful with output: parse and return job state
           - If successful but empty output: return None to retry later
             (allows up to 3 attempts tracked by self.scontrol_try)
           - If fails or returns non-zero exit code: proceed to fallback

        2. Fallback method: Execute `sacct -P --format=JobID,State,ExitCode
           --noheader -j <jobid>`
           - Used when job has left the active queue
           - Parses pipe-delimited output to extract main job state
           - Checks if main job is in a terminal state
           - Returns the actual state (COMPLETED, FAILED, etc.)

        Returns:
            Dictionary containing job information or None if scontrol
            returned empty output (caller should retry).

        Raises:
            RuntimeError: When job state cannot be determined or is
                non-terminal.
            AirflowException: When scontrol fails after 3 empty responses.
        """
        try:
            exit_code, output, error = await self._execute_ssh_command(
                ["scontrol", "--oneliner", "show", "job", self.jobid]
            )
        except AirflowException as e:
            logger.warning(
                "scontrol command failed: %s. Attempting sacct fallback.", e
            )
            exit_code, output, error = -1, "", str(e)

        if exit_code == 0 and len(output) > 0:
            if not output:
                if self.scontrol_try > 2:
                    raise AirflowException(
                        "scontrol didn't return any job information"
                    )
                else:
                    self.scontrol_try += 1
                    return

            array_status, records = await self.parse_scontrol(
                output.splitlines()
            )

            out = records[self.jobid.strip()]
            out["JobState"] = array_status
            self.last_full_state = out
            return {
                "job_id": out["JobId"],
                "job_name": out["JobName"],
                "state": out["JobState"],
                "reason": out["Reason"],
                "log_out": out["StdOut"],
                "log_err": out["StdErr"],
            }
        else:
            logger.warning(
                "scontrol returned %s with error %s.", exit_code, error
            )
            logger.warning("scontrol output", output)

            exit_code, stdout, stderr = await self._execute_ssh_command(
                [
                    "sacct",
                    "-P",
                    "--format=JobID,State,ExitCode",
                    "--noheader",
                    "-j",
                    self.jobid,
                ],
            )

            if exit_code != 0:
                logger.warning("sacct returned %s: %s", exit_code, stderr)
                raise RuntimeError(
                    f"Could not determine state of job {self.jobid}"
                )

            lines = stdout.strip().splitlines()
            if not lines:
                raise RuntimeError(
                    f"Could not determine state of job {self.jobid}"
                )

            main_job_state = None
            main_job_exit_code = None
            for line in lines:
                job_id, state, exit_code_str = line.split("|")

                if job_id == self.jobid:
                    main_job_state = state
                    main_job_exit_code = exit_code_str
                    break

            if main_job_state is None:
                raise RuntimeError(
                    f"Could not determine state of job {self.jobid}"
                )

            if main_job_state not in TERMINAL_STATES:
                raise RuntimeError(
                    f"Could not determine state of job {self.jobid}"
                )

            if main_job_exit_code and main_job_exit_code != "0:0":
                logger.warning(
                    "Job %s has non-zero exit code: %s",
                    self.jobid,
                    main_job_exit_code,
                )

            out = dict(**self.last_full_state)
            out["state"] = main_job_state
            return out

    async def parse_scontrol(
        self, scontrol_output: Iterable[str], cancel_pending: bool = True
    ) -> tuple[str, dict[str, dict[str, str]]]:
        """Parse `scontrol` output for single job or job array.

        Parse the output of `scontrol`. A global status is determined for job
        arrays, such that if any job is failed the global state is failed, too.

        Args:
            scontrol_output: The output of `scontrol`, already split into
                individual lines.
            cancel_pending: If True, remaining jobs in a failed job array are
                cancelled.

        Returns:
            global_state
            record_dict: A dictionary of job id to job record
        """
        records = tuple(
            parse_scontrol_record(line) for line in scontrol_output
        )
        record_dict = {r["JobId"]: r for r in records}
        state_counter = Counter(r["JobState"] for r in records)
        logger.info("States: %s", state_counter)
        if state_counter.get("FAILED", False):
            if cancel_pending:
                await self.cancel_remaining_jobs(records)
            return "FAILED", record_dict
        elif state_counter.get("PENDING", 0) > 0:
            return "PENDING", record_dict
        elif state_counter.get("RUNNING", 0):
            return "RUNNING", record_dict
        elif state_counter.get("COMPLETED", 0) == len(records):
            return "COMPLETED", record_dict
        else:
            # extract one of the (state, count) pairs and return it.
            _state_count = state_counter.popitem()
            return _state_count[0], record_dict

    async def cancel_remaining_jobs(self, records):
        """Cancel remaining SLURM jobs that are not failed.

        Args:
            records: List of job records from SLURM.
        """
        ids = tuple(r["JobId"] for r in records if r["JobState"] != "FAILED")
        logger.error(f"Cancelling pending jobs of failed array: {ids}")

        # NOTE: scancel accepts multiple job IDs as separate arguments
        exit_code, output, error = await self._execute_ssh_command(
            ["scancel"] + list(ids),
        )

        if exit_code != 0:
            logger.error(f"Could not cancel jobs: {exit_code=}")
            logger.error(f"Error: {error}")
            logger.error(f"Output: {output}")

    async def get_log(self, out_file) -> list[str]:
        r"""Read log from the last known position to the last complete line.

        Reads the log from the last line we had read to the last complete
        line (that has \n at the end). In some cases, the file takes a while
        to appear. We will try 3 times. From then on, the Trigger will call
        the SlurmOperator and a line will be added to the Airflow log warning
        that the Slurm log does not exist.

        Args:
            out_file: Path to the output file to read.

        Returns:
            List of all new lines from the log file.
        """
        try:
            exit_code, stdout, stderr = await self._execute_ssh_command(
                ["cat", out_file],
            )

            if exit_code != 0:
                raise Exception(f"Failed to read remote file: {stderr}")

            log = stdout.strip().split("\n")
            if not isinstance(log, list):
                raise TypeError(
                    f"Expected 'log' to be of type 'list', but got {type(log)}"
                )

            if len(log) != self.last_known_log_lines:
                # The log has new lines
                to_return = log[self.last_known_log_lines :]
                self.last_known_log_lines = len(log)
                if "\n" not in to_return[-1]:
                    # To ensure that the last line is written completely
                    to_return = to_return[:-1]
                    self.last_known_log_lines -= 1
            else:
                to_return = []

        except Exception as e:
            if self.log_try > 2:
                to_return = [
                    f"{e}\nSlurm's file log is still not available: {out_file}"
                ]
            else:
                self.log_try += 1
                to_return = []

        return to_return

    async def run(self):
        """The function that runs when we do a defer of the SlurmOperator."""
        # How many attempts do we have to read the job information and the log?
        # In some cases, the log file and information in scontrol take a while to appear
        # We allow 3 attempts at each thing before failing / showing an error
        self.log_try = 0
        self.scontrol_try = 0

        while True:
            await asyncio.sleep(self.tdelta_between_pokes)

            slurm_job = await self.get_scontrol_output()
            slurm_log = await self.get_log(slurm_job.get("log_out", None))

            self.log.debug(f"{slurm_job=} \n {slurm_log=}")

            if slurm_job:
                # In some cases we do not have the information in the scontrol instantly, we will try again from here
                # self.tdelta_between_pokes seconds

                slurm_changed_state = (
                    slurm_job["state"] != self.last_known_state
                )
                self.last_known_state = slurm_job["state"]

                if slurm_log or slurm_changed_state:
                    # We will only send a TriggerEvent when there is a state change or new lines in the log
                    break

        yield TriggerEvent(
            {
                "slurm_job": slurm_job,
                "slurm_changed_state": slurm_changed_state,
                "log_number_lines": self.last_known_log_lines,
                "log_new_lines": slurm_log,
            }
        )
