#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
"""SnowflakeFile for UDFs in Snowpark."""
from __future__ import annotations

import array
from typing import Iterable

from snowflake.snowpark._internal.utils import private_preview

_DEFER_IMPLEMENTATION_ERR_MSG = (
    "The file operations is implemented in Snowflake servers"
)


class SnowflakeFile:
    """
    SnowflakeFile provides developers an interface to operate files as a Python IOBase-like object in UDFs.
    SnowflakeFile supports most operations supported by Python IOBase
    For more information about the class, please check ...(docs TBD)

    The constructor of this class is not supposed to be called directly. Call :meth:`~snowflake.snowpark.file.SnowflakeFile.open` to create an instance of SnowflakeFile. It can be used as a python file object.
    The input type can be a column name as a :class:`str`, or a :class:`~snowflake.snowpark.Column` object.
    """

    def __init__(
        self,
        file_location: str,
        mode: str = "r",
    ) -> None:
        # The URL/URI of the file to be opened and wrapped by the SnowflakeFile instance.
        self._file_location: str = file_location
        # The mode of file stream
        self._mode: str = mode

        # The attributes supported as part of IOBase
        self.buffer = None
        self.closed = None
        self.encoding = None
        self.errors = None

    @classmethod
    @private_preview(version="0.12.0")
    def open(
        cls,
        file_location: str,
        mode: str = "r",
    ) -> SnowflakeFile:
        """
        Returns a :class:`~snowflake.snowpark.file.SnowflakeFile` that works like a Python file object(IOBase) and as a wrapper for IO stream of remote/local files.

        Args:
            file_location: A string of file location. It may include a remote URL/URI or a local file location.
            mode: A string used to mark the type of IO stream.
        """
        return cls(file_location, mode)

    def close(self) -> None:
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def detach(self) -> None:
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def fileno(self) -> None:
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def flush(self) -> None:
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def isatty(self) -> None:
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def read(self, size: int = -1) -> None:
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def read1(self, size: int = -1) -> None:
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def readline(self, size: int = -1) -> None:
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def readable(self) -> None:
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def readall(self) -> None:
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def readinto(self, b: bytes | bytearray | array.array) -> None:
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def readinto1(self, b: bytes | bytearray | array.array) -> None:
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def readlines(self, hint: int = -1) -> None:
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def seekable(self) -> None:
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def tell(self) -> None:
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def truncate(self, size: int | None = None) -> None:
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def write(self, b: bytes | bytearray | array.array) -> None:
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def writable(self) -> None:
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def writelines(self, lines: Iterable[str] | list[str]) -> None:
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)
