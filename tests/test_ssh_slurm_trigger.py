import asyncio

from airflow_slurm.ssh_slurm_trigger import SSHSlurmTrigger

SCONTROL_OUTPUT = (
    "JobId=132 JobName=wrap UserId=mer(2001) GroupId=mer(2001) "
    "MCS_label=N/A Priority=1 Nice=0 Account=(null) QOS=normal "
    "JobState=RUNNING Reason=None Dependency=(null) Requeue=1 Restarts=0 "
    "BatchFlag=1 Reboot=0 ExitCode=0:0 RunTime=00:00:22 "
    "TimeLimit=UNLIMITED TimeMin=N/A SubmitTime=2026-10-02T14:27:29 "
    "EligibleTime=2026-10-02T14:27:29 AccrueTime=2026-10-02T14:27:29 "
    "StartTime=2026-10-02T14:27:29 EndTime=Unknown Deadline=N/A "
    "SuspendTime=None SecsPreSuspend=0 LastSchedEval=2026-10-02T14:27:29 "
    "Scheduler=Main Partition=main AllocNode:Sid=frontend1:2787279 "
    "ReqNodeList=(null) ExcNodeList=(null) NodeList=n01 BatchHost=n01 "
    "NumNodes=1 NumCPUs=2 NumTasks=1 CPUs/Task=1 ReqB:S:C:T=0:0:*:* "
    "ReqTRES=cpu=1,mem=1547579M,node=1,billing=1 "
    "AllocTRES=cpu=2,mem=1547579M,node=1,billing=2 Socks/Node=* "
    "NtasksPerN:B:S:C=0:0:*:* CoreSpec=* MinCPUsNode=1 MinMemoryNode=0 "
    "MinTmpDiskNode=0 Features=(null) DelayBoot=00:00:00 OverSubscribe=NO "
    "Exclusive=NO Contiguous=0 Licenses=(null) LicensesAlloc=(null) "
    "Network=(null) Command=(null) SubmitLine=sbatch -n1 --wrap sleep 60 "
    "WorkDir=/u/mer StdErr= StdIn=/dev/null StdOut=/u/mer/slurm-132.out\n"
)


def query_scontrol(monkeypatch, output):
    async def execute_ssh_command(*_args, **_kwargs):
        return 0, output, ""

    trigger = SSHSlurmTrigger(jobid="132", ssh_conn_id="slurm")
    monkeypatch.setattr(trigger, "_execute_ssh_command", execute_ssh_command)
    return asyncio.run(trigger._try_scontrol())


def test_scontrol_record_with_spaces_in_submit_line(monkeypatch):
    """
    GIVEN a scontrol record whose SubmitLine contains spaces
    WHEN the job state is queried through scontrol
    THEN the record is parsed and its state and job name are reported
    """
    job = query_scontrol(monkeypatch, SCONTROL_OUTPUT)

    assert (job.job_id, job.job_name, job.state) == ("132", "wrap", "RUNNING")


def test_empty_stderr_falls_back_to_stdout(monkeypatch):
    """
    GIVEN a scontrol record with an empty StdErr, as without `sbatch -e`
    WHEN the job state is queried through scontrol
    THEN the stderr log is the stdout file, where Slurm writes stderr
    """
    job = query_scontrol(monkeypatch, SCONTROL_OUTPUT)

    assert job.log_err == "/u/mer/slurm-132.out"
