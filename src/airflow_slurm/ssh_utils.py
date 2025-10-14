"""SSH utilities for airflow-slurm package."""

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


def get_ssh_connection_details(ssh_conn_id: str) -> tuple[str, str | None, int]:
    """Extract SSH connection details from ssh_conn_id.
    
    Args:
        ssh_conn_id: Airflow connection ID for SSH connection
        
    Returns:
        Tuple of (host, username, port) for SSH connection
    """
    if not ssh_conn_id:
        raise AirflowException("ssh_conn_id is required for SSH operations")
        
    connection = BaseHook.get_connection(ssh_conn_id)
    
    if not connection.host:
        raise AirflowException(f"Host not specified in connection {ssh_conn_id}")
        
    host = connection.host
    username = connection.login
    port = connection.port or 22
    
    return host, username, port