#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
SnowflakeFile for UDFs and stored procedures in Snowpark.

This class is intended for usage within stored procedures and UDFs and many methods do not work locally.
"""
from __future__ import annotations

import array
import io
import sys
from io import RawIOBase

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

_DEFER_IMPLEMENTATION_ERR_MSG = "SnowflakeFile currently only works in UDF and Stored Procedures. It doesn't work locally yet."


class SnowflakeFile(RawIOBase):
    """
    SnowflakeFile provides an interface to operate on files as Python IOBase-like objects in UDFs and stored procedures.
    SnowflakeFile supports most operations supported by Python IOBase objects.
    A SnowflakeFile object can be used as a Python IOBase object.

    The constructor of this class is not supposed to be called directly. Call :meth:`~snowflake.snowpark.file.SnowflakeFile.open` to create a read-only SnowflakeFile object, and call :meth:`~snowflake.snowpark.file.SnowflakeFile.open_new_result` to create a write-only SnowflakeFile object.

    This class is intended for usage within UDFs and stored procedures and many methods do not work locally.
    """

    def __init__(
        self,
        file_location: str,
        mode: str = "r",
        is_owner_file: bool = False,
        *,
        require_scoped_url: bool = True,
        from_result_api: bool = False,
    ) -> None:
        super().__init__()
        # The URL/URI of the file to be opened by the SnowflakeFile object
        self._file_location: str = file_location
        # The mode of file stream
        self._mode: str = mode
        # Whether it is intended to access owner's files
        self._is_owner_file = is_owner_file
        # Whether a non-scoped URL can be accessed
        self._require_scoped_url = require_scoped_url

        # The attributes supported as part of IOBase
        self.buffer = None
        self.encoding = None
        self.errors = None

    @classmethod
    def open(
        cls,
        file_location: str,
        mode: str = "r",
        is_owner_file: bool = False,
        *,
        require_scoped_url: bool = True,
    ) -> SnowflakeFile:
        """
        Used to create a :class:`~snowflake.snowpark.file.SnowflakeFile` which can only be used for read-based IO operations on the file.

        In UDFs and Stored Procedures, the object works like a read-only Python IOBase object and as a wrapper for an IO stream of remote files.

        All files are accessed in the context of the UDF owner (with the exception of caller's rights stored procedures which use the caller's context).
        UDF callers should use scoped URLs to allow the UDF to access their files. By accepting only scoped URLs the UDF owner can ensure
        the UDF caller had access to the provided file. Removing the requirement that the URL is a scoped URL (require_scoped_url=False) allows the caller
        to provide URLs that may be only accessible by the UDF owner.

        is_owner_file is marked for deprecation. For Snowflake release 7.8 and onwards please use require_scoped_url instead.

        Args:
            file_location: scoped URL, file URL, or string path for files located in a stage
            mode: A string used to mark the type of an IO stream. Supported modes are "r" for text read and "rb" for binary read.
            is_owner_file: (Deprecated) A boolean value, if True, the API is intended to access owner's files and all URI/URL are allowed. If False, the API is intended to access files passed into the function by the caller and only scoped URL is allowed.
            require_scoped_url: A boolean value, if True, file_location must be a scoped URL. A scoped URL ensures that the caller cannot access the UDF owners files that the caller does not have access to.
        """
        return cls(
            file_location, mode, is_owner_file, require_scoped_url=require_scoped_url
        )

    @classmethod
    def open_new_result(cls, mode: str = "w") -> SnowflakeFile:
        """
        Used to create a :class:`~snowflake.snowpark.file.SnowflakeFile` which can only be used for write-based IO operations. UDFs/Stored Procedures should return the file to materialize it, and it is then made accessible via a scoped URL returned in the query results.

        In UDFs and Stored Procedures, the object works like a write-only Python IOBase object and as a wrapper for an IO stream of remote files.

        Args:
            mode: A string used to mark the type of an IO stream. Supported modes are "w" for text write and "wb" for binary write.
        """
        return cls("new results file", mode, require_scoped_url=0, from_result_api=True)

    def close(self) -> None:
        """
        In UDF and Stored Procedures, the close func closes the IO Stream included in the SnowflakeFile.
        """
        return

    def detach(self) -> None:
        """
        Not yet supported in UDF and Stored Procedures.

        See https://docs.python.org/3/library/io.html#io.BufferedIOBase.detach
        """
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def fileno(self) -> None:
        """
        See https://docs.python.org/3/library/io.html#io.IOBase.fileno
        """
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def flush(self) -> None:
        """
        Not yet supported in UDF and Stored Procedures.
        """
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def isatty(self) -> None:
        """
        Returns False, file streams in stored procedures and UDFs are never interactive in Snowflake.
        """
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def read(self, size: int = -1) -> None:
        """
        See https://docs.python.org/3/library/io.html#io.RawIOBase.read
        """
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def read1(self, size: int = -1) -> None:
        """
        See https://docs.python.org/3/library/io.html#io.BufferedIOBase.read1
        """
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def readable(self) -> None:
        """
        See https://docs.python.org/3/library/io.html#io.IOBase.readable
        """
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def readall(self) -> None:
        """
        See https://docs.python.org/3/library/io.html#io.RawIOBase.readall
        """
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def readinto(self, b: bytes | bytearray | array.array) -> None:
        """
        See https://docs.python.org/3/library/io.html#io.IOBase.readinto
        """
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def readinto1(self, b: bytes | bytearray | array.array) -> None:
        """
        See https://docs.python.org/3/library/io.html#io.BufferedIOBase.readinto1
        """
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def readline(self, size: int = -1) -> None:
        """
        See https://docs.python.org/3/library/io.html#io.IOBase.readline
        """
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def readlines(self, hint: int = -1) -> None:
        """
        See https://docs.python.org/3/library/io.html#io.IOBase.readlines
        """
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        """
        See https://docs.python.org/3/library/io.html#io.IOBase.seek
        """
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def seekable(self) -> None:
        """
        See https://docs.python.org/3/library/io.html#io.IOBase.seekable
        """
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def tell(self) -> None:
        """
        See https://docs.python.org/3/library/io.html#io.IOBase.tell
        """
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def truncate(self, size: int | None = None) -> None:
        """
        Not yet supported in UDF and Stored Procedures.
        """
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def write(self, b: bytes | bytearray | array.array) -> None:
        """
        See https://docs.python.org/3/library/io.html#io.RawIOBase.write
        """
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def writable(self) -> None:
        """
        See https://docs.python.org/3/library/io.html#io.IOBase.writable
        """
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def writelines(self, lines: Iterable[str] | list[str]) -> None:
        """
        See https://docs.python.org/3/library/io.html#io.IOBase.writelines
        """
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)
