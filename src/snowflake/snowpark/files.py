#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
"""SnowflakeFile for UDFs in Snowpark."""
from __future__ import annotations

import array
import io
from io import RawIOBase
from typing import Iterable

from snowflake.snowpark._internal.utils import private_preview

_DEFER_IMPLEMENTATION_ERR_MSG = "SnowflakeFile currently only works in UDF and Stored Procedures. It doesn't work locally yet."


class SnowflakeFile(RawIOBase):
    """
    SnowflakeFile provides an interface to operate on files as Python IOBase-like objects in UDFs and stored procedures.
    SnowflakeFile supports most operations supported by Python IOBase objects.
    A SnowflakeFile object can be used as a Python IOBase object.

    The constructor of this class is not supposed to be called directly. Call :meth:`~snowflake.snowpark.file.SnowflakeFile.open` to create a SnowflakeFile object.
    """

    def __init__(
        self,
        file_location: str,
        mode: str = "r",
        is_owner_file: bool = False,
    ) -> None:
        super().__init__()
        # The URL/URI of the file to be opened by the SnowflakeFile object
        self._file_location: str = file_location
        # The mode of file stream
        self._mode: str = mode
        # Whether it is intended to access owner's files
        self._is_owner_file = is_owner_file 

        # The attributes supported as part of IOBase
        self.buffer = None
        self.encoding = None
        self.errors = None

    @classmethod
    @private_preview(version="0.12.0")
    def open(
        cls,
        file_location: str,
        mode: str = "r",
        is_owner_file: bool = False,
    ) -> SnowflakeFile:
        """
        Returns a :class:`~snowflake.snowpark.file.SnowflakeFile`.
        In UDF and Stored Procedures, the object works like a Python IOBase object and as a wrapper for an IO stream of remote files. The IO Stream is to support the file operations defined in this class.

	All files are accessed in the context of the UDF owner and therefore when accessing call files user should prefer setting is_owner_file to default false value as it only allows scoped URLs and therefore a caller cannot inadvertently or maliciously access the owner's files.

        Args:
            file_location: A string of file location. It can be a remote URL/URI.
            mode: A string used to mark the type of an IO stream.
            is_owner_file: A boolean value, if True, the API is intended to access owner's files and all url/uri are allowed. If False, the API is intended to access files passed into the function by the caller and only scope url is allowed.
        """
        return cls(file_location, mode, is_owner_file)

    def close(self) -> None:
        """
        In UDF and Stored Procedures, the close func closes the IO Stream included in the SnowflakeFile.
        """
        return

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

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
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
