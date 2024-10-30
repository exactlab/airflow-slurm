#########  SLURM STATES ###########
SACCT_COMPLETED_OK = ('COMPLETED',)
SACCT_FAILED = ('BOOT_FAIL', 'CANCELLED', 'DEADLINE', 'FAILED', 'NODE_FAIL', 'OUT_OF_MEMORY', 'REVOKED', 'TIMEOUT')
SACCT_RUNNING = ('PENDING', 'PREEMPTED', 'RUNNING', 'REQUEUED', 'RESIZING', 'SUSPENDED')
SACCT_FINISHED = SACCT_COMPLETED_OK + SACCT_FAILED

######### SBATCH OPTIONS (NOT AVAILABLE THROUGH ENV VARS) ##############
# Dictionary: slurm option: (need user input?, slurm argument, with '=' if necessary)
SLURM_OPTS = {
    "EXTRA_NODE_INFO": (True, "--extra-node-info="),
    "BURST_BUFFER": (True, "--bb="),
    "BURST_BUFFER_FILE": (True, "--bbf="),
    "BEGIN": (True, "--begin="),
    "CHDIR": (True, "--chdir="),
    "CLUSTER_CONSTRAINT": (True, "--cluster-constraint="),
    "COMMENT": (True, "--comment="),
    "CONTIGUOUS": (False, "--contiguous"),
    "CORES_PER_SOCKET": (True, "--cores-per-socket="),
    "CPU_FREQ": (True, "--cpu-freq="),
    "CPUS_PER_TASK": (True, "--cpus-per-task="),
    "DEADLINE": (True, "--deadline="),
    "DEPENDENCY": (True, "--dependency="),
    "ERROR": (True, "--error="),
    "EXPORT_FILE": (True, "--export-file="),
    "NODE_FILE": (True, "--nodefile="),
    "GID": (True, "--gid="),
    "GPUS_PER_SOCKET": (True, "--gpus-per-socket="),
    "HOLD": (False, "--hold"),
    "INPUT": (True, "--input="),
    "KILL_ON_INVALID_DEP": (True, "--kill-on-invalid-dep="),
    "LICENSES": (True, "--licenses="),
    "MAIL_TYPE": (True, "--mail-type="),
    "MAIL_USER": (True, "--mail-user="),
    "MIN_CPUS": (True, "--mincpus="),
    "NODES": (True, "--nodes="),
    "NTASKS": (True, "--ntasks="),
    "NICE": (True, "--nice="),
    "NTASKS_PER_CORE": (True, "--ntasks-per-core="),
    "NTASKS_PER_NODE": (True, "--ntasks-per-node="),
    "NTASKS_PER_SOCKET": (True, "--ntasks-per-socket="),
    "PRIORITY": (True, "--priority="),
    "PROPAGATE": (True, "--propagate="),
    "REBOOT": (False, "--reboot"),
    "OVERSUBSCRIBE": (False, "--oversubscribe"),
    "CORE_SPEC": (True, "--core-spec="),
    "SOCKETS_PER_NODE": (True, "--sockets-per-node="),
    "THREAD_SPEC": (True, "--thread-spec="),
    "THREADS_PER_CORE": (True, "--threads-per-core="),
    "TIME_MIN": (True, "--time-min="),
    "TMP": (True, "--tmp="),
    "UID": (True, "--uid="),
    "VERBOSE": (True, "--verbose"),
    "NODE_LIST": (True, "--nodelist="),
    "WRAP": (True, "--wrap="),
    "EXCLUDE": (True, "--exclude=")
}
