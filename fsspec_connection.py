from __future__ import annotations

from contextlib import contextmanager
from datetime import timedelta
from io import TextIOWrapper
from pathlib import Path
from typing import Iterator

import pandas as pd
from fsspec import AbstractFileSystem, filesystem
from fsspec.spec import AbstractBufferedFile
from streamlit import cache_data
from streamlit.connections import BaseConnection

TTLType = int | float | timedelta


class FileConnection(BaseConnection[AbstractFileSystem]):
    _default_connection_name = "file"

    def __init__(
        self, connection_name: str = "default", protocol: str | None = None, **kwargs
    ) -> None:
        self.protocol = protocol
        super().__init__(connection_name, **kwargs)

    def connect(self, **kwargs) -> AbstractFileSystem:
        """
        Pass a protocol such as "s3", "gcs", or "file"
        """
        self._closed = False

        _secrets = self.get_secrets()
        secrets = dict(_secrets)

        protocol = secrets.pop("protocol", self.protocol)

        if protocol is None:
            protocol = "file"

        fs = filesystem(protocol, **secrets)

        return fs

    def disconnect(self) -> None:
        self._closed = True

    def is_connected(self) -> bool:
        return not self._closed

    @contextmanager
    def open(
        self, path: str | Path, mode: str = "rb", *args, **kwargs
    ) -> Iterator[TextIOWrapper | AbstractBufferedFile]:
        with self.instance.open(path, mode, *args, **kwargs) as f:
            yield f

    def _read_data(
        self, path: str | Path, binary: bool = True, *args, **kwargs
    ) -> str | bytes:
        mode = "rb" if binary else "rt"
        with self.instance.open(path, mode, *args, **kwargs) as f:
            return f.read()

    def read_text(self, path: str | Path, ttl: TTLType = 3600, *args, **kwargs) -> str:
        return cache_data(self._read_data, ttl=ttl)(  # type: ignore
            path, binary=False, *args, **kwargs
        )

    def read_bytes(self, path: str | Path, ttl: TTLType = 3600, **kwargs) -> bytes:
        return cache_data(self._read_data, ttl=ttl)(  # type: ignore
            path, binary=True, **kwargs
        )

    def _read_csv(self, path: str | Path, **kwargs) -> pd.DataFrame:
        with self.open(path, "rt") as f:
            return pd.read_csv(f, **kwargs)

    def read_csv(self, path: str | Path, ttl: TTLType = 3600, **kwargs) -> pd.DataFrame:
        return cache_data(self._read_csv, ttl=ttl)(path, **kwargs)  # type: ignore

    def _read_parquet(self, path: str | Path, **kwargs) -> pd.DataFrame:
        with self.open(path, "rb") as f:
            return pd.read_parquet(f, **kwargs)  # type: ignore

    def read_parquet(
        self, path: str | Path, ttl: TTLType = 3600, **kwargs
    ) -> pd.DataFrame:
        return cache_data(self._read_parquet, ttl=ttl)(path, **kwargs)  # type: ignore
