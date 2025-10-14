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
- [ ] Extract SSH connection details from `ssh_conn_id` (user, host, port)
- [ ] Create helper method to prefix commands with SSH invocation
- [ ] Handle SSH command timeouts and error codes appropriately

### 2. Update operator SLURM command execution
- [ ] Modify sbatch execution in `execute()` to use `ssh user@host sbatch`
- [ ] Update squeue call in `check_job_not_running()` to use SSH prefix
- [ ] Update `_get_last_line_from_output()` to use `ssh user@host tail`

### 3. Update trigger SLURM command execution
- [ ] Modify `get_scontrol_output()` to use `ssh user@host scontrol/sacct`
- [ ] Update `get_log()` to use `ssh user@host cat` for remote file reading
- [ ] Update `cancel_remaining_jobs()` to use `ssh user@host scancel`

### 4. Handle SSH connection configuration
- [ ] Parse Airflow SSH connection to extract user@host details
- [ ] Add proper error handling for SSH connection failures
- [ ] Ensure SSH agent/key authentication works correctly

### 5. Testing and validation
- [ ] Test end-to-end job submission and monitoring flow
- [ ] Verify error handling for SSH connection issues
- [ ] Validate log streaming and XCom functionality works over SSH


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
