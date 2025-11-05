"""SSH utilities for airflow-slurm package."""

from dataclasses import dataclass

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.sdk.definitions.connection import Connection


@dataclass
class SSHConnectionArgs:
    """SSH connection arguments extracted from Airflow connection."""

    host: str
    username: str
    port: int = 22
    key_file: str | None = None
    server_host_key_algs: list[str] | None = None


def _extract_ssh_args_from_connection(connection) -> SSHConnectionArgs:
    """Extract SSH configuration from an Airflow connection object.

    Args:
        connection: Airflow connection object

    Returns:
        SSHConnectionArgs with extracted configuration
    """
    if not connection.host:
        raise AirflowException(
            f"Host not specified in connection {connection.conn_id}"
        )

    if not connection.login:
        raise AirflowException(
            f"Username not specified in connection {connection.conn_id}"
        )

    # Extract SSH configuration from connection extras
    key_file = None
    server_host_key_algs = None

    if connection.extra_dejson:
        key_file = connection.extra_dejson.get("key_file")
        server_host_key_algs = connection.extra_dejson.get(
            "server_host_key_algs"
        )

    return SSHConnectionArgs(
        host=connection.host,
        username=connection.login,
        port=connection.port or 22,
        key_file=key_file,
        server_host_key_algs=server_host_key_algs,
    )


def get_ssh_connection_details(ssh_conn_id: str) -> SSHConnectionArgs:
    """Extract SSH connection details from ssh_conn_id.

    Args:
        ssh_conn_id: Airflow connection ID for SSH connection

    Returns:
        SSHConnectionArgs with connection details
    """
    if not ssh_conn_id:
        raise AirflowException("ssh_conn_id is required for SSH operations")

    connection = BaseHook.get_connection(ssh_conn_id)
    return _extract_ssh_args_from_connection(connection)


async def aget_ssh_connection_details(ssh_conn_id: str) -> SSHConnectionArgs:
    """Extract SSH connection details from ssh_conn_id (async version).

    Args:
        ssh_conn_id: Airflow connection ID for SSH connection

    Returns:
        SSHConnectionArgs with connection details
    """
    if not ssh_conn_id:
        raise AirflowException("ssh_conn_id is required for SSH operations")

    connection = await Connection.async_get(ssh_conn_id)
    return _extract_ssh_args_from_connection(connection)
