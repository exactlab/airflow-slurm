"""SSH utilities for airflow-slurm package."""

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.sdk.definitions.connection import Connection


def get_ssh_connection_details(
    ssh_conn_id: str,
) -> tuple[str, str | None, int, str | None]:
    """Extract SSH connection details from ssh_conn_id.

    Args:
        ssh_conn_id: Airflow connection ID for SSH connection

    Returns:
        Tuple of (host, username, port, key_file) for SSH connection
    """
    if not ssh_conn_id:
        raise AirflowException("ssh_conn_id is required for SSH operations")

    connection = BaseHook.get_connection(ssh_conn_id)

    if not connection.host:
        raise AirflowException(
            f"Host not specified in connection {ssh_conn_id}"
        )

    host = connection.host
    username = connection.login
    port = connection.port or 22

    # Extract SSH key file from connection extras
    key_file = None
    if connection.extra_dejson:
        key_file = connection.extra_dejson.get("key_file")

    return host, username, port, key_file


async def aget_ssh_connection_details(
    ssh_conn_id: str,
) -> tuple[str, str | None, int, str | None]:
    """Extract SSH connection details from ssh_conn_id (async version).

    Args:
        ssh_conn_id: Airflow connection ID for SSH connection

    Returns:
        Tuple of (host, username, port, key_file) for SSH connection
    """
    if not ssh_conn_id:
        raise AirflowException("ssh_conn_id is required for SSH operations")

    connection = await Connection.async_get(ssh_conn_id)

    if not connection.host:
        raise AirflowException(
            f"Host not specified in connection {ssh_conn_id}"
        )

    host = connection.host
    username = connection.login
    port = connection.port or 22

    # Extract SSH key file from connection extras
    key_file = None
    if connection.extra_dejson:
        key_file = connection.extra_dejson.get("key_file")

    return host, username, port, key_file
