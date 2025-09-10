from __future__ import annotations

import asyncio, logging, shlex
from collections import Counter
from typing import Any, Dict, Iterable, Tuple
from airflow.triggers.base import BaseTrigger, TriggerEvent
import io
import paramiko
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

import io
import paramiko

def parse_scontrol_record(line: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for kv in line.split():
        if "=" in kv:
            k, v = kv.split("=", maxsplit=1)
            out[k] = v
    return out

class SSHSlurmTrigger(BaseTrigger):
    def __init__(
        self,
        job_id: str,
        ssh_conn_id: str | None = None,
        use_ssh: bool | None = None,
        last_known_state: str | None = None,
        last_known_log_lines: int = 0,
        tdelta_between_pokes: int = 20,
        fire_initial_event: bool = True,
        ssh_params: dict | None = None
    ) -> None:
        super().__init__()
        self.job_id = str(job_id).strip()
        self.ssh_conn_id = (ssh_conn_id or "").strip()
        self.use_ssh = bool(self.ssh_conn_id) if use_ssh is None else bool(use_ssh)
        self._ssh_params = ssh_params
        self.last_full_state = {
            "job_id": self.job_id,
            "job_name": "unknown",
            "state": "unknown",
            "reason": "unknown",
            "log_out": "/dev/null",
            "log_err": "/dev/null",
        }
        self.last_known_state = last_known_state
        self.last_known_log_lines = int(last_known_log_lines)
        self.tdelta_between_pokes = int(tdelta_between_pokes)
        self.fire_initial_event = fire_initial_event

            
    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            f"{__name__}.SSHSlurmTrigger",
            {
                "job_id": self.job_id,
                "ssh_conn_id": self.ssh_conn_id,
                "use_ssh": self.use_ssh,
                "last_known_state": self.last_known_state,
                "last_known_log_lines": self.last_known_log_lines,
                "tdelta_between_pokes": self.tdelta_between_pokes,
                "fire_initial_event": self.fire_initial_event,
                "ssh_params": self._ssh_params,
            },
        )


    async def _run_local(self, cmd: str) -> tuple[int, str, str]:
        proc = await asyncio.create_subprocess_shell(
            cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        out_b, err_b = await proc.communicate()
        return proc.returncode, out_b.decode().strip(), err_b.decode().strip()

    def _ensure_ssh_params(self):
        if self._ssh_params or not self.use_ssh:
            return
        if not self.ssh_conn_id:
            raise RuntimeError("SSH required but ssh_conn_id is empty")
        conn = BaseHook.get_connection(self.ssh_conn_id)
        extras = conn.extra_dejson or {}
        key_file = extras.get("key_file") or extras.get("keyfile")
        private_key = extras.get("private_key")
        passphrase = extras.get("private_key_passphrase") or extras.get("passphrase")
        if not private_key and key_file:
            try:
                with open(key_file, "r") as f:
                    private_key = f.read()
            except Exception:
                private_key = None  # fall back to password

        self._ssh_params = {
            "hostname": conn.host,
            "port": conn.port or 22,
            "username": conn.login,
            "password": conn.password,
            "private_key": private_key,
            "private_key_passphrase": passphrase,
        }

    def _load_pkey(self, pem: str | None, passphrase: str | None):
        if not pem:
            return None
        for cls in (paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey):
            try:
                return cls.from_private_key(io.StringIO(pem), password=passphrase)
            except Exception:
                continue
        return None

    def _run_ssh_sync(self, cmd: str) -> tuple[int, str, str]:
        self._ensure_ssh_params()
        assert self._ssh_params is not None, "SSH params not initialized"

        host = self._ssh_params.get("hostname")
        port = self._ssh_params.get("port") or 22
        user = self._ssh_params.get("username")
        pwd  = self._ssh_params.get("password")
        pkey = self._load_pkey(
            self._ssh_params.get("private_key"),
            self._ssh_params.get("private_key_passphrase"),
        )

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(
                hostname=host,
                port=port,
                username=user,
                password=pwd if pkey is None else None,
                pkey=pkey,
                look_for_keys=False,
                allow_agent=False,
            )
            _, stdout, stderr = client.exec_command(cmd, get_pty=True)
            out = stdout.read().decode().strip()
            err = stderr.read().decode().strip()
            rc = stdout.channel.recv_exit_status()
            return rc, out, err
        finally:
            client.close()


    async def _run(self, cmd: str) -> tuple[int, str, str]:
        if self.use_ssh:
            return await asyncio.to_thread(self._run_ssh_sync, cmd)
        return await self._run_local(cmd)

    async def get_scontrol_output(self) -> dict | None:
        job_q = shlex.quote(self.job_id)
        rc, out, err = await self._run("bash --login -c 'scontrol --oneliner show job %s'" % job_q)
        if rc == 0 and out:
            array_status, records = await self.parse_scontrol(out.splitlines())
            record = records.get(self.job_id) or next(iter(records.values()))
            record["JobState"] = array_status
            self.last_full_state = record
            return {
                "job_id": record.get("JobId", self.job_id),
                "job_name": record.get("JobName", "unknown"),
                "state": record.get("JobState", "unknown"),
                "reason": record.get("Reason", "unknown"),
                "log_out": record.get("StdOut", "/dev/null"),
                "log_err": record.get("StdErr", "/dev/null"),
            }

        logger.warning("scontrol rc=%s err=%r (use_ssh=%s, ssh_conn_id=%r)", rc, err, self.use_ssh, self.ssh_conn_id)
        rc2, out2, err2 = await self._run("bash --login -c 'sacct --noheader -j %s'" % job_q)
        if rc2 != 0:
            logger.warning("sacct rc=%s err=%r (use_ssh=%s)", rc2, err2, self.use_ssh)
            out_state = dict(**self.last_full_state)
            out_state["state"] = "COMPLETED"  # last-resort assumption
            return out_state

        lines = [ln for ln in out2.splitlines() if ln.strip()]
        is_completed = all("COMPLETED" in ln for ln in lines) if lines else False
        out_state = dict(**self.last_full_state)
        out_state["state"] = "COMPLETED" if is_completed else "FAILED"
        return out_state

    async def parse_scontrol(
        self, scontrol_output: Iterable[str], cancel_pending: bool = True
    ) -> Tuple[str, Dict[str, Dict[str, str]]]:
        records = tuple(parse_scontrol_record(line) for line in scontrol_output)
        record_dict = {r.get("JobId", ""): r for r in records if r.get("JobId")}
        state_counter = Counter(r.get("JobState", "UNKNOWN") for r in records)
        logger.info("States: %s", state_counter)
        if state_counter.get("FAILED", 0) > 0:
            if cancel_pending:
                await self.cancel_remaining_jobs(records)
            return "FAILED", record_dict
        if state_counter.get("PENDING", 0) > 0: return "PENDING", record_dict
        if state_counter.get("RUNNING", 0) > 0: return "RUNNING", record_dict
        if state_counter.get("COMPLETED", 0) == len(records) and records: return "COMPLETED", record_dict
        if state_counter: return next(iter(state_counter.items()))[0], record_dict
        return "UNKNOWN", record_dict

    async def cancel_remaining_jobs(self, records: Iterable[Dict[str, str]]) -> None:
        ids = tuple(r.get("JobId", "") for r in records if r.get("JobState") != "FAILED" and r.get("JobId"))
        if not ids: return
        joined = " ".join(shlex.quote(jid) for jid in ids)
        logger.error("Cancelling pending jobs of failed array: %s", ids)
        rc, out, err = await self._run(f"bash --login -c 'scancel {joined}'")
        if rc != 0: logger.error("Could not cancel jobs: rc=%s err=%r out=%r", rc, err, out)

    async def get_log(self, out_file: str | None) -> list[str]:
        if not out_file: return []
        path = shlex.quote(out_file)
        rc, out, err = await self._run(f"cat {path}")
        if rc != 0:
            if self.log_try > 2:
                return [f"{err}\nSlurm's file log is still not available: {out_file}"]
            self.log_try += 1
            return []
        lines = out.split("\n") if out else []
        if len(lines) != self.last_known_log_lines:
            new = lines[self.last_known_log_lines :]; self.last_known_log_lines = len(lines)
            if new and not out.endswith("\n"): new = new[:-1]; self.last_known_log_lines -= 1
            return new
        return []

    async def run(self):
        logger.info("SSHSlurmTrigger starting: use_ssh=%s ssh_conn_id=%r job_id=%s",
                    self.use_ssh, self.ssh_conn_id, self.job_id)
        self.log_try = 0; self.scontrol_try = 0
        while True:
            await asyncio.sleep(self.tdelta_between_pokes)
            slurm_job = await self.get_scontrol_output()
            if not slurm_job: continue
            slurm_log = await self.get_log(slurm_job.get("log_out")) if slurm_job.get("log_out") else []
            slurm_changed_state = slurm_job.get("state") != self.last_known_state
            self.last_known_state = slurm_job.get("state")
            if slurm_log or slurm_changed_state: break
        yield TriggerEvent({
            "slurm_job": slurm_job,
            "slurm_changed_state": slurm_changed_state,
            "log_number_lines": self.last_known_log_lines,
            "log_new_lines": slurm_log,
        })



