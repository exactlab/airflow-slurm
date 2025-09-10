from __future__ import annotations

import re
import shlex
import subprocess  # nosec
from typing import Any, Sequence

import dateutil.parser
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from airflow_slurm.ssh_slurm_trigger import SSHSlurmTrigger
from airflow_slurm.templates import SLURM_FILE

import uuid
from airflow.utils.context import Context
from airflow.sdk.definitions.context import Context

from airflow_slurm.constants import (
    SCONTROL_COMPLETED_OK,
    SCONTROL_FAILED,
    SCONTROL_FINISHED,
    SCONTROL_RUNNING,
    SLURM_OPTS,
)

# optional: only needed when ssh_conn_id is set
try:
    from airflow.providers.ssh.hooks.ssh import SSHHook
except Exception:
    SSHHook = None  # type: ignore


class SSHSlurmOperator(BaseOperator):
    """Run a linux script or command through Slurm (local or via SSH).

    If `ssh_conn_id` is provided, commands are executed on the remote host using
    the Airflow SSH connection. Otherwise, they run locally.
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
        ssh_conn_id: str,
        tdelta_between_checks: int = 5,
        slurm_options: dict[str, Any] | None = None,
        modules: list[str] | None = None,
        setup_commands: list[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.command = command
        self.ssh_conn_id = ssh_conn_id
        self.slurm_options: dict[str, Any] = slurm_options or {}
        self.tdelta_between_checks = int(tdelta_between_checks)
        self.modules = modules
        self.setup_commands = setup_commands

        self._use_ssh = bool(self.ssh_conn_id)
        if self._use_ssh and SSHHook is None:
            raise AirflowException(
                "SSH provider not installed. Install apache-airflow-providers-ssh."
            )

    # ----------------------------- helpers ----------------------------- #
    def _render_job_name(self, context: Context) -> str:
        job_name = self.slurm_options.get("JOB_NAME", "airflow_slurm_job")
        try:
            logical_date = context["logical_date"]
        except Exception:
            airflow_ctx = context.get("airflow.ctx", {})  # type: ignore
            if "AIRFLOW_CTX_EXECUTION_DATE" in airflow_ctx:
                logical_date = dateutil.parser.isoparse(
                    airflow_ctx["AIRFLOW_CTX_EXECUTION_DATE"]
                )
            else:
                logical_date = None
        if logical_date is not None:
            suffix = logical_date.strftime("%Y%m%dT%H%M")
            return f"{job_name}_{suffix}"
        return job_name

    def parse_input_and_render_slurm_script(self, context: Context) -> str:
        self.slurm_options["JOB_NAME"] = self._render_job_name(context)
        slurm_opts: dict[str, str] = {}
        for key, value in self.slurm_options.items():
            if key in SLURM_OPTS:
                has_value, opt_string = SLURM_OPTS[key]
                if has_value:
                    if value is not None:
                        slurm_opts[key] = f"{opt_string}{value}"
                else:
                    slurm_opts[key] = opt_string

        return SLURM_FILE.render(
            slurm_opts=slurm_opts,
            job_command=self.command,
            modules=self.modules,
            setup_commands=self.setup_commands,
        )


    def _build_ssh_params(self) -> dict | None:
        if not self._use_ssh:
            return None
        conn = BaseHook.get_connection(self.ssh_conn_id)
        extras = conn.extra_dejson or {}

        key_file   = extras.get("key_file") or extras.get("keyfile")
        private_key = extras.get("private_key")
        passphrase  = extras.get("private_key_passphrase") or extras.get("passphrase")

        if not private_key and key_file:
            try:
                with open(key_file, "r") as f:
                    private_key = f.read()
            except Exception:
                private_key = None  # fall back to password

        return {
            "hostname": conn.host,
            "port": conn.port or 22,
            "username": conn.login,
            "password": conn.password,
            "private_key": private_key,               # PEM string or None
            "private_key_passphrase": passphrase,     # optional
        }

    def _ssh_exec(self, command: str) -> tuple[int, str, str]:
        """Run a command on the remote host via SSHHook."""
        assert SSHHook is not None
        hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        client = hook.get_conn()
        try:
            stdin, stdout, stderr = client.exec_command(command, get_pty=True)
            out = stdout.read().decode()
            err = stderr.read().decode()
            rc = stdout.channel.recv_exit_status()
            return rc, out, err
        finally:
            client.close()

    def _ssh_upload(self, content: str, remote_path: str) -> None:
        assert SSHHook is not None
        hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        client = hook.get_conn()
        try:
            sftp = client.open_sftp()
            try:
                with sftp.file(remote_path, "w") as f:
                    f.write(content)
            finally:
                sftp.close()
        finally:
            client.close()

    # ------------------------------ run ------------------------------- #
    def execute(self, context: Context):
        """Submit the job with sbatch (local or remote) and defer to trigger."""
        def extract_job_id(sbatch_output: str) -> str:
            m = re.search(r"\b(\d+)\b", sbatch_output)
            if not m:
                raise AirflowException(
                    f"Could not determine job id from SBATCH output: {sbatch_output!r}"
                )
            return m.group(1)

        slurm_script = self.parse_input_and_render_slurm_script(context)

        # Skip if duplicate job name already queued/running
        self.check_job_not_running(context)

        if self._use_ssh:
            # Upload script to remote /tmp and submit with sbatch
            remote_path = f"/tmp/airflow-slurm-{uuid.uuid4().hex}.sh"
            self._ssh_upload(slurm_script, remote_path)

            cmd = f"bash -lc 'sbatch --parsable {shlex.quote(remote_path)}'"
            self.log.info("Submitting remote SLURM job: %s", cmd)
            rc, out, err = self._ssh_exec(cmd)

            # best-effort cleanup
            self._ssh_exec(f"rm -f {shlex.quote(remote_path)}")

            if rc != 0:
                raise AirflowException(
                    f"Remote sbatch failed: rc={rc}, stdout={out!r}, stderr={err!r}"
                )

            job_id = extract_job_id(out.strip())
        else:
            # Local submission (stdin to sbatch)
            cmd = "sbatch --parsable"
            self.log.info("Submitting local SLURM job: %s", cmd)
            proc = subprocess.run(  # nosec
                ["bash", "-l", "-c", cmd],
                input=slurm_script,
                text=True,
                capture_output=True,
                check=False,
            )
            if proc.returncode != 0:
                raise AirflowException(
                    "SBATCH command failed. "
                    f"returncode={proc.returncode}, stdout={proc.stdout!r}, stderr={proc.stderr!r}"
                )
            job_id = extract_job_id(proc.stdout.strip())

        self.log.info("Submitted batch job %s", job_id)

        # Defer to trigger (it will also run remote if ssh_conn_id is set)
        self.defer(
            trigger=SSHSlurmTrigger(
                job_id=job_id,
                ssh_conn_id=self.ssh_conn_id if self._use_ssh else "",
                use_ssh=self._use_ssh,
                tdelta_between_pokes=self.tdelta_between_checks,
                ssh_params=self._build_ssh_params(),
            ),
            method_name="new_slurm_state_log",
        )


    # ---------------------------- utilities --------------------------- #
    def check_job_not_running(self, context: Context) -> None:
        params = context.get("params", {}) or {}
        if params.get("ignore_multiple_jobs", False):
            self.log.info(
                "Ignoring duplicate-job check for %s",
                self.slurm_options.get("JOB_NAME"),
            )
            return

        job_name = self.slurm_options.get("JOB_NAME")
        if not job_name:
            return
        quoted_name = shlex.quote(job_name)
        cmd = f"bash -lc 'squeue -n {quoted_name} -h -o %i'"

        if self._use_ssh:
            rc, out, err = self._ssh_exec(cmd)
            if rc != 0:
                raise AirflowException(f"squeue (remote) failed: {err.strip()!r}")
            stdout = out.strip()
        else:
            proc = subprocess.run(  # nosec
                ["bash", "-l", "-c", f"squeue -n {quoted_name} -h -o %i"],
                text=True, capture_output=True, check=False
            )
            if proc.returncode != 0:
                raise AirflowException(
                    f"squeue (local) failed: {proc.stderr.strip()!r}"
                )
            stdout = proc.stdout.strip()

        self.log.info("squeue stdout (same-name jobs):\n%s", stdout)
        if stdout.split():
            raise AirflowSkipException(
                "According to squeue this job is already running for this date!"
            )

    def new_slurm_state_log(self, context: Context, event: dict[str, Any] | None = None):
        if not event:
            raise AirflowException("Missing trigger event")

        if (
            event.get("slurm_changed_state")
            and event["slurm_job"]["state"] not in SCONTROL_FINISHED
        ):
            self._log_status_change(event)

        for line in event.get("log_new_lines", []) or []:
            self.log.info(line.rstrip())

        state = event["slurm_job"]["state"]
        if state in SCONTROL_COMPLETED_OK:
            if event.get("slurm_changed_state"):
                self._log_status_change(event)
            return None
        elif state in SCONTROL_FAILED:
            if event.get("slurm_changed_state"):
                self._log_status_change(event)
            raise AirflowException("Slurm job failed!")
        elif state in SCONTROL_RUNNING:
            self.defer(
                trigger=SSHSlurmTrigger(
                    job_id=event["slurm_job"]["job_id"],
                    ssh_conn_id=self.ssh_conn_id if self._use_ssh else "",
                    use_ssh=self._use_ssh,
                    last_known_state=state,
                    last_known_log_lines=event.get("log_number_lines", 0),
                    tdelta_between_pokes=self.tdelta_between_checks,
                    ssh_params=self._build_ssh_params(),
                ),
                method_name="new_slurm_state_log",
            )
        else:
            raise AirflowException(
                "scontrol returned an unknown state for job #{}: {}".format(
                    event["slurm_job"]["job_id"], state
                )
            )

    def _log_status_change(self, event: dict[str, Any]) -> None:
        self.log.info(
            "Slurm reports job #%s (%s) changed state to %s with reason %s",
            event["slurm_job"]["job_id"],
            event["slurm_job"]["job_name"],
            event["slurm_job"]["state"],
            event["slurm_job"].get("reason"),
        )

    def on_kill(self) -> None:
        pass

