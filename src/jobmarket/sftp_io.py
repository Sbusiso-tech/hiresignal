"""Small SFTP helpers shared by the partner feed simulator and the SFTP source."""
from __future__ import annotations

import posixpath
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

from jobmarket.config import Settings

if TYPE_CHECKING:
    import paramiko


@contextmanager
def connect(settings: Settings) -> Iterator[paramiko.SFTPClient]:
    import paramiko

    client = paramiko.SSHClient()
    # The demo SFTP container generates a new host key whenever it is recreated.
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        settings.sftp_host,
        port=settings.sftp_port,
        username=settings.sftp_user,
        password=settings.sftp_password,
        timeout=settings.http_timeout,
        allow_agent=False,
        look_for_keys=False,
    )
    try:
        yield client.open_sftp()
    finally:
        client.close()


def makedirs(sftp: paramiko.SFTPClient, path: str) -> None:
    current = ""
    for part in path.strip("/").split("/"):
        current = posixpath.join(current, part)
        try:
            if not stat.S_ISDIR(sftp.stat(current).st_mode):
                raise NotADirectoryError(current)
        except FileNotFoundError:
            sftp.mkdir(current)
