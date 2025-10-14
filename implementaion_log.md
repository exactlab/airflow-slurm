# Run command via SSH

## Context

This package allows running some airflow tasks by offloading to a SLURM
cluster. Right now, the package assumes that Airflow is installed on a machine
that can directly submit jobs to a SLURM cluster. This assumption needs be relaxed by invoking the SLURM commands via SSH. Since the target cluster has tight security, we can assume that an SSH agent is running to provide with the required secrets to open the connection.

## Feature request

Implement running SLURM commands via SSH. The implementation needs not be
super general because this system is going to be superseeded in the near
future.

## Implementation plan

The current implementation executes SLURM commands directly on the local machine
using `subprocess`. To enable SSH functionality, we will simply prefix all
SLURM commands with `ssh user@host` to execute them on the remote cluster.

**Proposed solution**: Transform existing subprocess calls from
`subprocess.run(['scontrol', 'show', 'job', job_id])` to
`subprocess.run(['ssh', 'user@host', 'scontrol', 'show', 'job', job_id])`.
This leverages the existing SSH infrastructure (SSH agent, keys) and requires
minimal code changes. The operator will parse the existing `ssh_conn_id`
parameter to extract connection details, then prefix all SLURM commands
(sbatch, scontrol, squeue, sacct, scancel) and file operations (cat, tail)
with the SSH invocation.

**Caveats**: This approach establishes a new SSH connection for each command,
resulting in higher connection overhead compared to connection pooling or
persistent connections. SSH key negotiation occurs repeatedly, and there's no
sophisticated error differentiation between SSH connection failures and SLURM
command failures. The implementation also relies on the SSH client being
available on the Airflow worker and proper SSH agent/key configuration.

### 1. Add SSH configuration and helper utility
- [x] Extract SSH connection details from `ssh_conn_id` (user, host, port)
- [x] Create helper method to prefix commands with SSH invocation
- [x] Handle SSH command timeouts and error codes appropriately

### 2. Update operator SLURM command execution
- [x] Modify sbatch execution in `execute()` to use `ssh user@host sbatch`
- [x] Update squeue call in `check_job_not_running()` to use SSH prefix
- [x] Update `_get_last_line_from_output()` to use `ssh user@host tail`

### 3. Update trigger SLURM command execution
- [x] Modify `get_scontrol_output()` to use `ssh user@host scontrol/sacct`
- [x] Update `get_log()` to use `ssh user@host cat` for remote file reading
- [x] Update `cancel_remaining_jobs()` to use `ssh user@host scancel`

## Implementation log

### Step 1: SSH configuration and helper utility

Added SSH infrastructure to SSHSlurmOperator with three helper methods:

- `_get_ssh_connection_details()`: Extracts user@host and port from Airflow's
  connection registry using the existing `ssh_conn_id` parameter. Handles missing
  connection validation and defaults port to 22.

- `_build_ssh_command()`: Constructs SSH command arrays by prefixing remote
  commands with `ssh -p <port> -o BatchMode=yes -o StrictHostKeyChecking=no
  user@host`. Uses BatchMode to prevent interactive prompts and disables host
  key checking for automation environments.

- `_execute_ssh_command()`: Wrapper for subprocess.run with SSH command
  construction, timeout handling, and standardized error reporting. Returns
  (exit_code, stdout, stderr) tuple.

**Key design choices**: Used `subprocess.run()` instead of `Popen()` for
simpler error handling and timeout support. Added `BatchMode=yes` and
`StrictHostKeyChecking=no` SSH options to ensure non-interactive operation
suitable for automated environments. The approach assumes SSH agent or
key-based authentication is already configured on the Airflow worker.

### Step 2: Update operator SLURM command execution

Updated three SLURM command execution points in SSHSlurmOperator to use SSH:

- **sbatch execution**: Replaced `subprocess.Popen` with `_execute_ssh_command()`
  for job submission. Removed `bash --login -c` wrapper since SSH handles
  remote environment setup. Changed from bytes to string handling for job ID
  extraction.

- **squeue check**: Updated `check_job_not_running()` to use SSH command array
  `["squeue", "--name", job_name, "--noheader", "--format=%i"]` instead of
  shell string `bash -l -c 'squeue ...'`. Used long-form arguments for
  clarity. The `--noheader` flag removes column headers and `--format=%i`
  returns only job IDs, enabling clean parsing to detect running jobs. This
  eliminates shell injection risks and simplifies argument handling.

- **tail output**: Modified `_get_last_line_from_output()` to read remote log
  files via SSH. Maintained original graceful error handling by catching
  `AirflowException` and returning `None` on failures.

**Key changes**: Eliminated unnecessary `bash --login` wrappers that were
needed for local SLURM environment loading but are redundant over SSH.
Converted shell string commands to safer command arrays. All operations now
use consistent SSH timeout and error handling patterns.

### Step 3: Update trigger SLURM command execution

Updated SSHSlurmTrigger to use async SSH via asyncssh library with shared
connection logic:

- **SSH utility module**: Created `ssh_utils.py` with shared
  `get_ssh_connection_details()` function to extract (host, username, port)
  from Airflow connections. Refactored operator to use this shared utility,
  eliminating code duplication.

- **async SSH helper**: Added `_execute_ssh_command()` method using asyncssh
  for native async SSH operations. Configured with `known_hosts=None` and
  `client_keys=None` to use SSH agent authentication and disable host key
  checking for automation environments.

- **scontrol/sacct commands**: Updated `get_scontrol_output()` to use SSH for
  both primary scontrol query and sacct fallback. Removed `bash --login -c`
  wrappers and converted to command arrays: `["scontrol", "--oneliner",
  "show", "job", jobid]` and `["sacct", "--noheader", "-j", jobid]`.

- **remote log reading**: Modified `get_log()` to read SLURM output files via
  `["cat", out_file]` SSH command. Maintained original error handling and
  retry logic for missing files.

- **job cancellation**: Updated `cancel_remaining_jobs()` to use `["scancel"]
  + list(job_ids)` format, passing multiple job IDs as separate arguments
  rather than shell string concatenation.

**Key design choices**: Used asyncssh for proper async SSH instead of mixing
sync subprocess with async trigger methods. Shared SSH connection extraction
logic between operator and trigger for maintainability. All SLURM commands
now use consistent command array format with proper argument separation.
