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
from typing import Any
from typing import Iterable

from airflow.exceptions import AirflowException
from airflow.triggers.base import BaseTrigger
from airflow.triggers.base import TriggerEvent

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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
    ):
        """
        :param jobid: the slurm's job id
        :param last_known_state: the last known slurm's state
        :param last_known_log_lines: how many lines did the log have IN TOTAL the last time we opened it?
        :param tdelta_between_pokes: how many SECONDS should we wait between checks of the log file & scontrol
        """
        super().__init__()
        self.jobid = jobid
        self.ssh_conn_id = ssh_conn_id
        self.last_known_state = last_known_state
        self.last_known_log_lines = last_known_log_lines
        self.tdelta_between_pokes = tdelta_between_pokes

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow_slurm.ssh_slurm_trigger.SSHSlurmTrigger",
            {
                "jobid": self.jobid,
                "ssh_conn_id": self.ssh_conn_id,
                "last_known_state": self.last_known_state,
                "last_known_log_lines": self.last_known_log_lines,
                "tdelta_between_pokes": self.tdelta_between_pokes,
            },
        )

    async def get_scontrol_output(self) -> dict | None:
        proc = await asyncio.create_subprocess_shell(
            f"bash --login -c 'scontrol --oneliner show job {self.jobid}'",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout, stderr = await proc.communicate()
        output = stdout.decode().strip()
        error = stderr.decode().strip()
        exit_code = proc.returncode

        if exit_code == 0:
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
            try:
                proc = await asyncio.create_subprocess_shell(
                    f"bash --login -c 'sacct --noheader -j {self.jobid}'",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                exit_code = proc.returncode
                stdout, stderr = await proc.communicate()
                if exit_code != 0:
                    error = stderr.decode().strip()
                    logger.warning(error)
                    raise RuntimeError(
                        "sacct returned %s: %s", exit_code, error
                    )
                output = stdout.decode().strip().splitlines()
                is_completed = all("COMPLETED" in line for line in output)
            except RuntimeError:
                logger.warning(
                    "Could not infer job state from running "
                    "scontrol and sacct. Assuming completed."
                )
                is_completed = True
            out = dict(**self.last_full_state)
            if is_completed:
                out["state"] = "COMPLETED"
            else:
                raise RuntimeError(
                    f"Could not determine state of job {self.jobid}"
                )
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
        records = tuple(parse_scontrol_record(line) for line in scontrol_output)
        record_dict = {r["JobId"]: r for r in records}
        state_counter = Counter(r["JobState"] for r in records)
        logger.info("States: %s", state_counter)
        if state_counter.get("FAILED", False):
            if cancel_pending:
                await self.cancel_remaining_jobs(records)
            return "FAILED", record_dict
        if state_counter.get("PENDING", 0) > 0:
            return "PENDING", record_dict
        if state_counter.get("RUNNING", 0):
            return "RUNNING", record_dict
        if state_counter.get("COMPLETED", 0) == len(records):
            return "COMPLETED", record_dict
        print(state_counter)

    async def cancel_remaining_jobs(self, records):
        ids = tuple(r["JobId"] for r in records if r["JobState"] != "FAILED")
        logger.error(f"Cancelling pending jobs of failed array: {ids}")
        proc = await asyncio.create_subprocess_shell(
            f"bash --login -c 'scancel {' '.join(ids)}'",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        output = stdout.decode().strip()
        error = stderr.decode().strip()
        exit_code = proc.returncode

        if exit_code != 0:
            raise AirflowException(f"scontrol returned {exit_code=}\n{error=}")
        logger.error(output)
        logger.error(error)

    async def get_log(self, out_file) -> list[str]:
        """We read the log from the last line we had read to the last complete
        line (that has \n at the end). In some cases, the file takes a while to
        appear. We will try 3 times. From then on, the Trigger will call the
        SlurmOperator and a line will be added to the Airflow log warning that
        the Slurm log does not exist.

        :return: a list with all lines
        """
        try:
            proc = await asyncio.create_subprocess_shell(
                f"cat {out_file}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            stdout, _ = await proc.communicate()
            log = stdout.decode().strip().split("\n")
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
