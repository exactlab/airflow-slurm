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
from pathlib import Path
from typing import Tuple, Dict, Any, Optional, List

from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.exceptions import AirflowException
from airflow.models import Variable
import asyncssh

from airflow_slurm.config import settings

# from airflow_slurm.config import get_settings

# Use this to hide asyncssh connections logs
asyncssh.set_log_level("WARN")

def parse_scontrol(input: str) -> dict:
    d = dict()
    for i in input.split():
        l = i.split("=")
        k = l[0]
        v = "=".join(l[1:])
        d[k] = v
    return d


class SSHSlurmTrigger(BaseTrigger):
    def __init__(
        self,
        jobid: str,
        last_known_state: Optional[str] = None,
        last_known_log_lines: int = 0,
        tdelta_between_pokes: int = 20,
    ):
        """
        :param jobid: the slurm's job id
        :param last_known_state: the last known slurm's state (see mycompany.operators.ssh_slurm_operator.SACCT_*)
        :param last_known_log_lines: how many lines did the log have IN TOTAL the last time we opened it?
        :param tdelta_between_pokes: how many SECONDS should we wait between checks of the log file & SACCT
        """
        super().__init__()
        # settings = get_settings()
        self.jobid = jobid
        self.ssh_host = settings.SSH_HOST
        self.ssh_opt = asyncssh.SSHClientConnectionOptions(
            username=settings.SSH_USER,
            port=settings.SSH_PORT, 
            client_keys=settings.SSH_KEY_PATH
        )
        self.last_known_state = last_known_state
        self.last_known_log_lines = last_known_log_lines
        self.tdelta_between_pokes = tdelta_between_pokes
        self.slurm_log_path = Path(Variable.get("SLURM_LOGS_FOLDER", default_var="/tmp")) / f"slurm-{self.jobid}.out"

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return (
            "airflow_slurm.ssh_slurm_trigger.SSHSlurmTrigger",
            {
                "jobid": self.jobid,
                "last_known_state": self.last_known_state,
                "last_known_log_lines": self.last_known_log_lines,
                "tdelta_between_pokes": self.tdelta_between_pokes,
            },
        )

    async def get_scontrol_output(self) -> Optional[dict]:
        """
        With the job id we look at what state it is in the slurm using scontrol.
        In some cases we may find that the job still does not appear.
        In this case, we will retry later up to 3 times.

        :return: a dictionary with job information or None if we haven't found the job

        """
        async with asyncssh.connect(host=self.ssh_host, options=self.ssh_opt) as conn:
            result = await conn.run(f"scontrol show job {self.jobid}")

        output = result.stdout
        error = result.stderr

        if result.returncode != 0:
            raise AirflowException(
                f"SCONTROL returned a non-zero exit code: {error}"
            )

        if not output:
            if self.sacct_try > 2:
                raise AirflowException("SCONTROL didn't return any job information")
            else:
                self.sacct_try += 1
                return

        sl_out = parse_scontrol(output)

        return {
            "job_id": sl_out["JobId"],
            "job_name": sl_out["JobName"],
            "state": sl_out["JobState"],
            "reason": sl_out["Reason"],
            "log_out": sl_out["StdOut"],
            "log_err": sl_out["StdErr"],
        }

    async def get_log(self) -> List[str]:
        """
        We read the log from the last line we had read to the last complete line (that has \n at the end).
        In some cases, the file takes a while to appear. We will try 3 times. From then on, the Trigger
        will call the SlurmOperator and a line will be added to the Airflow log warning that the Slurm log does not exist.

        :return: a list with all lines
        """
        try:
            async with asyncssh.connect(host=self.ssh_host, options=self.ssh_opt) as conn:
                result = await conn.run(f"cat {self.slurm_log_path}")
            
            log = result.stdout.split("\n")
            assert type(log) == list
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
                    f"{e}\nSlurm's file log is still not available: {self.slurm_log_path}"
                ]
            else:
                self.log_try += 1
                to_return = []

        return to_return

    async def run(self):
        """
        The function that runs when we do a defer of the SlurmOperator
        """
        # How many attempts do we have to read the job information and the log?
        # In some cases, the log file and information in sacct take a while to appear
        # We allow 3 attempts at each thing before failing / showing an error
        self.log_try = 0
        self.sacct_try = 0

        while True:
            await asyncio.sleep(self.tdelta_between_pokes)

            slurm_job = await self.get_scontrol_output()
            slurm_log = await self.get_log()

            self.log.debug(f"{slurm_job=} \n {slurm_log=}")

            if slurm_job:
                # In some cases we do not have the information in the sacct instantly, we will try again from here
                # self.tdelta_between_pokes seconds

                slurm_changed_state = slurm_job["state"] != self.last_known_state
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
