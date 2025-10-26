# Available `slurm_options`

This document lists all the available keys that can be used in the `slurm_options` field of the `SSHSlurmOperator`. These options correspond to SLURM job parameters that allow you to configure and customize your job submission.

### Job Configuration Options

- **`JOB_NAME`**: Set the job name.
  - Example: `--job-name=example_job`

- **`OUTPUT_FILE`**: Specify the output file for standard output.
  - Example: `--output=output.log`

- **`ERROR_FILE`**: Specify the error file for standard error.
  - Example: `--error=error.log`

- **`PARTITION`**: Define the partition (queue) to submit the job to.
  - Example: `--partition=batch`

- **`ACCOUNT`**: Define the account that submits the job.
  - Example: `--account=account_name`

- **`TIME`**: Set the maximum wall time for the job.
  - Example: `--time=01:00:00` (1 hour)

- **`EXTRA_NODE_INFO`**: Add extra node information.
  - Example: `--extra-node-info=extra_info`

- **`BURST_BUFFER`**: Enable burst buffer.
  - Example: `--bb=enabled`

- **`BURST_BUFFER_FILE`**: Specify the burst buffer file.
  - Example: `--bbf=bb_file.txt`

- **`BEGIN`**: Specify when the job should start.
  - Example: `--begin=now`

- **`CHDIR`**: Change the directory before running the job.
  - Example: `--chdir=/home/user`

- **`CLUSTER_CONSTRAINT`**: Set cluster constraints for the job.
  - Example: `--cluster-constraint=cluster1`

- **`COMMENT`**: Add a comment to the job.
  - Example: `--comment="Important job"`

### Node and CPU Options

- **`CONTIGUOUS`**: Ensure the job runs on contiguous nodes.
  - Example: `--contiguous` (No value required)

- **`CORES_PER_SOCKET`**: Set the number of cores per socket.
  - Example: `--cores-per-socket=8`

- **`CPU_FREQ`**: Set the CPU frequency.
  - Example: `--cpu-freq=high`

- **`CPUS_PER_TASK`**: Set the number of CPUs per task.
  - Example: `--cpus-per-task=4`

- **`DEADLINE`**: Set a deadline for the job.
  - Example: `--deadline=2024-12-31T23:59:59`

- **`DEPENDENCY`**: Set job dependencies.
  - Example: `--dependency=afterok:12345`

- **`EXPORT_FILE`**: Specify a file to export environment variables.
  - Example: `--export-file=env_file.sh`

- **`NODE_FILE`**: Specify a file containing node list.
  - Example: `--nodefile=node_file.txt`

- **`GID`**: Set the group ID for the job.
  - Example: `--gid=1001`

- **`GPUS_PER_SOCKET`**: Specify the number of GPUs per socket.
  - Example: `--gpus-per-socket=2`

- **`HOLD`**: Hold the job until it is manually released.
  - Example: `--hold` (No value required)

- **`INPUT`**: Specify an input file for the job.
  - Example: `--input=input_file.txt`

- **`KILL_ON_INVALID_DEP`**: Terminate the job if its dependencies are invalid.
  - Example: `--kill-on-invalid-dep=true`

- **`MEM`**: Specify the real memory required per node. Default units are megabytes. Different units can be specified using the suffix [K|M|G|T].
  - Example: `--mem=8G`
- **`MEM_PER_CPU`**: Minimum memory required per usable allocated CPU. Default units are megabytes. The --mem, --mem-per-cpu and --mem-per-gpu options are mutually exclusive. If --mem, --mem-per-cpu or --mem-per-gpu are specified as command line arguments, then they will take precedence over the environment.
  - Example: `--mem-per-cpu=2G`

### Mail and User Options

- **`LICENSES`**: Request specific licenses for the job.
  - Example: `--licenses=license1`

- **`MAIL_TYPE`**: Set the mail notification type.
  - Example: `--mail-type=ALL`

- **`MAIL_USER`**: Specify the user to send email notifications to.
  - Example: `--mail-user=user@example.com`

- **`MIN_CPUS`**: Set the minimum number of CPUs required for the job.
  - Example: `--mincpus=4`

### Resource Allocation

- **`NODES`**: Specify the number of nodes for the job.
  - Example: `--nodes=2`

- **`NTASKS`**: Specify the total number of tasks.
  - Example: `--ntasks=16`

- **`NICE`**: Set the priority of the job.
  - Example: `--nice=10`

- **`NTASKS_PER_CORE`**: Set the number of tasks per core.
  - Example: `--ntasks-per-core=1`

- **`NTASKS_PER_NODE`**: Set the number of tasks per node.
  - Example: `--ntasks-per-node=8`

- **`NTASKS_PER_SOCKET`**: Set the number of tasks per socket.
  - Example: `--ntasks-per-socket=4`

- **`PRIORITY`**: Set the priority of the job.
  - Example: `--priority=high`

- **`PROPAGATE`**: Set propagation of environment variables.
  - Example: `--propagate=env_vars`

- **`GRES`**: Generic Resource scheduling. Used for requesting special hardware resources.
  - Example: `--gres=gpu:tesla:2,gpu:kepler:2,mps:400,bandwidth:lustre:no_consume:4G`

### System Options

- **`REBOOT`**: Reboot the system after the job completes.
  - Example: `--reboot` (No value required)

- **`OVERSUBSCRIBE`**: Allow over-subscribing of nodes.
  - Example: `--oversubscribe` (No value required)

### Core and Thread Allocation

- **`CORE_SPEC`**: Specify core specifications for the job.
  - Example: `--core-spec=spec`

- **`SOCKETS_PER_NODE`**: Set the number of sockets per node.
  - Example: `--sockets-per-node=2`

- **`THREAD_SPEC`**: Specify thread specifications for the job.
  - Example: `--thread-spec=spec`

- **`THREADS_PER_CORE`**: Set the number of threads per core.
  - Example: `--threads-per-core=2`

### Time and Disk Options

- **`TIME_MIN`**: Set the minimum runtime for the job.
  - Example: `--time-min=00:10:00`

- **`TMP`**: Set the temporary directory for the job.
  - Example: `--tmp=/tmp`

- **`UID`**: Set the user ID for the job.
  - Example: `--uid=1000`

- **`VERBOSE`**: Enable verbose output for the job.
  - Example: `--verbose` (No value required)

- **`NODE_LIST`**: Set the list of nodes the job should run on.
  - Example: `--nodelist=node1,node2`

- **`WRAP`**: Wrap the job command in a script.
  - Example: `--wrap="bash script.sh"`

- **`EXCLUDE`**: Exclude specific nodes from job execution.
  - Example: `--exclude=node3`

- **`ARRAY`**: Submit a job array, multiple jobs to be executed with identical parameters.
  - Example: `--array=0-15`
