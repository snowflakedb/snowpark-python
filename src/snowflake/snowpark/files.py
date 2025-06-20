#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import array
import sys
import tempfile
import os
from io import (
    RawIOBase,
    UnsupportedOperation,
    BufferedReader,
    SEEK_SET,
    SEEK_END,
    SEEK_CUR,
)
from snowflake.snowpark._internal.utils import RELATIVE_PATH_PREFIX
from typing import Sequence
import logging

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

_NON_LOCAL_PATH_ERR_MSG = "SnowflakeFile currently supports only relative paths and read apis in local testing."
_WRITE_MODE_ERR_MSG = (
    "SnowflakeFile currently doesn't support write APIs in local testing."
)
_DEFER_IMPLEMENTATION_ERR_MSG = "Not yet supported in UDF and Stored Procedures."
READ_MODES = ["r", "rb"]
WRITE_MODES = ["w", "wb"]
_logger = logging.getLogger(__name__)


class SnowflakeFile(RawIOBase):
    """
    SnowflakeFile provides an interface to operate on files as Python IOBase-like objects in UDFs and stored procedures.
    SnowflakeFile supports most operations supported by Python IOBase objects.
    A SnowflakeFile object can be used as a Python IOBase object.

    The constructor of this class is not supposed to be called directly. Call :meth:`~snowflake.snowpark.file.SnowflakeFile.open` to create a read-only SnowflakeFile object, and call :meth:`~snowflake.snowpark.file.SnowflakeFile.open_new_result` to create a write-only SnowflakeFile object.

    This class is used to read and write files in UDFs and stored procedures. On Snowflake, it is used to read and write files from scoped URLs, stages, and versioned stages. It also
    supports Python IOBase and BufferedBase methods such as :meth:`close`, :meth:`read1`, :meth:`readinto`, :meth:`readinto1`, :meth:`readline`, :meth:`readlines`, :meth:`seek`, :meth:`tell`, :meth:`readable`, :meth:`writable`, :meth:`seekable`.
    To read from a SnowflakeFile object opened from :meth:`~snowflake.snowpark.file.SnowflakeFile.open_new_result`, you can return the object in the UDF or scoped procedure and receive a
    scoped URL, which can then be passed as the file location to :meth:`~snowflake.snowpark.file.SnowflakeFile.open`.

    Snowflake Example:
        >>> from snowflake.snowpark.file import SnowflakeFile
        >>> @udf
        >>> def write_file(content: str) -> str:
        ...     file = SnowflakeFile.open_new_result("w")
        ...     file.write(content)
        ...     return file
        >>> @udf
        >>> def read_file(url: str) -> str:
        ...     file = SnowflakeFile.open(url, "r")
        ...     return file.read()
        >>> read_file(write_file("Hello World!"));

    Locally, SnowflakeFile currently only supports reading files from relative paths. Write APIs are not supported in local testing.
    Stage and versioned stage support is currently being implemented.

    Local Testing Example:
        >>> from snowflake.snowpark.file import SnowflakeFile
        >>> from snowflake.snowpark.functions import udf
        >>> # Write APIs are not supported in local testing so Python IO should be used instead.
        >>> file_location = "relative/path/to/file.txt"
        >>> with open(file_location, "w") as f:
        ...     f.write("Hello World!")
        >>> @udf
        >>> def read_file(file_location: str) -> str:
        ...     with SnowflakeFile.open("relative/path/to/file.txt", "r") as f:
        ...     return f.read()
        >>> print(read_file(file_location))

    Note:
        1. All of the implementation in this file is for local testing purposes.

        2. There may be slight implementation differences between local testing and Snowflake execution environments. If
        you encounter any issues, please file a bug report at https://github.com/snowflakedb/snowpark-python.

        3. UDF implementation is dependent on the Snowflake release.
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

        # Attributes required for local testing functionality
        _DEFAULT_READ_BUFFER_SIZE = 32 * 1024
        if mode in READ_MODES:
            # Buffered Reader used to support BufferedIOBase methods such as read1 and readinto1
            self._file_stream = BufferedReader(
                open(self._file_location, self._mode), _DEFAULT_READ_BUFFER_SIZE
            )
        elif mode in WRITE_MODES:
            # need to still open a file stream for testing
            self._file_stream = open(self._file_location, self._mode)
        self._pos = 0
        self._is_local_file = (
            True
            if self._file_location.startswith((RELATIVE_PATH_PREFIX, "C:"))
            else False
        )

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
        if mode not in READ_MODES:
            raise ValueError(
                f"Invalid mode '{mode}' for SnowflakeFile.open. Supported modes are 'r' and 'rb'."
            )
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
        if mode not in WRITE_MODES:
            raise ValueError(
                f"Invalid mode '{mode}' for SnowflakeFile.open_new_result. Supported modes are 'w' and 'wb'."
            )
        return cls(
            tempfile.NamedTemporaryFile().name,
            mode,
            require_scoped_url=0,
            from_result_api=True,
        )

    def _raise_if_not_read(self) -> None:
        """
        Internal function to validate read mode of the file object before performing an IO operation.
        """
        if self._mode not in READ_MODES:
            raise UnsupportedOperation(f"Not readable mode={self._mode}")

    def _raise_if_not_write(self) -> None:
        """
        Internal function to validate write mode of the file object before performing a IO operation.
        """
        if self._mode not in WRITE_MODES:
            raise UnsupportedOperation(f"Not writable mode={self._mode}")

    def _raise_if_closed(self) -> None:
        """
        Internal function to validate open status of the file object before performing an IO operation.
        """
        if self._file_stream.closed:
            raise ValueError("I/O operation on closed file.")

    def _read_into_buffer(self, b: bytes | bytearray | array.array) -> int:
        """
        Internal function to read bytes into a pre-allocated, writable bytes-like object buffer.
        This is used by readinto and readinto1 methods.
        """
        buffer_len = len(b)
        if buffer_len == 0:
            return 0
        content = self.read1(buffer_len)
        size = memoryview(content)
        self._pos += size.nbytes
        b[:buffer_len] = content
        return size.nbytes

    def close(self) -> None:
        """
        See https://docs.python.org/3/library/io.html#io.IOBase.close

        Closes the underlying IO Stream of the SnowflakeFile
        """
        self._file_stream.close()

    def detach(self) -> None:
        """
        Not yet supported in UDF and Stored Procedures.

        See https://docs.python.org/3/library/io.html#io.BufferedIOBase.detach
        """
        self._raise_if_closed()
        raise UnsupportedOperation("Detaching stream from file is unsupported")

    def fileno(self) -> int:
        """
        Getting a file descriptor number is not supported in Snowflake. Raises an OSError.
        """
        self._raise_if_closed()
        raise OSError("This object does not use a file descriptor")

    def flush(self) -> None:
        """
        Fail if the stream is closed. Does nothing in read mode, not implemented in write mode.
        """
        self._raise_if_closed()
        if self._mode in READ_MODES:
            pass
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def isatty(self) -> bool:
        """
        Returns False, file streams in stored procedures and UDFs are never interactive in Snowflake.
        """
        self._raise_if_closed()
        return False

    def read(self, size: int = -1) -> Sequence:
        """
        From https://docs.python.org/3/library/io.html#io.RawIOBase.read

        Read up to size bytes from the object and return them. As a convenience, if size is unspecified or -1,
        all bytes until EOF are returned. Otherwise, only one system call is ever made.
        Fewer than size bytes may be returned if the operating system call returns fewer than size bytes.

        If 0 bytes are returned, and size was not 0, this indicates end of file. If the object is in non-blocking mode
        and no bytes are available, None is returned.
        """
        self._raise_if_closed()
        self._raise_if_not_read()
        if self._is_local_file:
            content = self._file_stream.raw.read(size)
            self._pos += len(content)
            return content
        raise NotImplementedError(_NON_LOCAL_PATH_ERR_MSG)

    def read1(self, size: int = -1) -> Sequence:
        """
        From https://docs.python.org/3/library/io.html#io.BufferedIOBase.read1

        Read and return up to size bytes, with at most one call to the underlying raw stream’s read() (or readinto()) method.

        If size is -1 (the default), an arbitrary number of bytes are returned (more than zero unless EOF is reached).
        """
        self._raise_if_closed()
        self._raise_if_not_read()
        if self._is_local_file:
            if self._mode == "r":
                content = self.read(size).encode()
            else:
                content = self._file_stream.read1(size)
            self._pos += len(content)
            return content
        raise NotImplementedError(_NON_LOCAL_PATH_ERR_MSG)

    def readable(self) -> bool:
        """
        From https://docs.python.org/3/library/io.html#io.IOBase.readable

        Returns whether or not the stream is readable.
        """
        self._raise_if_closed()
        return self._file_stream.readable()

    def readall(self) -> Sequence:
        """
        From https://docs.python.org/3/library/io.html#io.RawIOBase.readall

        Read and return all the bytes from the stream until EOF, using multiple calls to the stream if necessary.
        """
        return self.read(-1)

    def readinto(self, b: bytes | bytearray | array.array) -> int:
        """
        From https://docs.python.org/3/library/io.html#io.RawIOBase.readinto

        Read bytes into a pre-allocated, writable bytes-like object b, and return the number of bytes read. For example, b might
        be a bytearray. If the object is in non-blocking mode and no bytes are available, None is returned.
        """
        self._raise_if_closed()
        self._raise_if_not_read()
        if self._is_local_file:
            if self._mode == "r":
                return self._read_into_buffer(b)
            size = self._file_stream.raw.readinto(b)
            self._pos += size
            return size
        raise NotImplementedError(_NON_LOCAL_PATH_ERR_MSG)

    def readinto1(self, b: bytes | bytearray | array.array) -> int:
        """
        From https://docs.python.org/3/library/io.html#io.BufferedIOBase.readinto1

        Read bytes into a pre-allocated, writable bytes-like object b, using at most one call to the underlying raw stream’s
        read() (or readinto()) method. Return the number of bytes read.

        A BlockingIOError is raised if the underlying raw stream is in non blocking-mode, and has no data available at the
        moment.
        """
        self._raise_if_closed()
        self._raise_if_not_read()
        if self._is_local_file:
            if self._mode == "r":
                return self._read_into_buffer(b)
            size = self._file_stream.readinto1(b)
            self._pos += size
            return size
        raise NotImplementedError(_NON_LOCAL_PATH_ERR_MSG)

    def readline(self, size: int = -1) -> Sequence:
        """
        From https://docs.python.org/3/library/io.html#io.IOBase.readline

        Read and return one line from the stream. If size is specified, at most size bytes will be read.

        The line terminator is always b\'\\n\' for binary files; for text files, the newline argument to open() can be used to
        select the line terminator(s) recognized.
        """
        self._raise_if_closed()
        self._raise_if_not_read()
        if self._is_local_file:
            content = self._file_stream.raw.readline(size)
            self._pos += len(content)
            return content
        raise NotImplementedError(_NON_LOCAL_PATH_ERR_MSG)

    def readlines(self, hint: int = -1) -> list[Sequence]:
        """
        From https://docs.python.org/3/library/io.html#io.IOBase.readlines

        Read and return a list of lines from the stream. hint can be specified to control the number of lines read: no more
        lines will be read if the total size (in bytes/characters) of all lines so far exceeds hint.

        hint values of 0 or less, as well as None, are treated as no hint.

        Note that it’s already possible to iterate on file objects using for line in file: ... without calling file.readlines().
        """
        self._raise_if_closed()
        self._raise_if_not_read()
        if self._is_local_file:
            content = self._file_stream.raw.readlines(hint)
            self._pos += sum(len(line) for line in content)
            return content
        raise NotImplementedError(_NON_LOCAL_PATH_ERR_MSG)

    def seek(self, offset: int, whence: int = SEEK_SET) -> int:
        """
        See https://docs.python.org/3/library/io.html#io.IOBase.seek

        Move the stream position to a new location given an offset and a starting position. SEEK_SET/0
        indicates a position relative to the start of the file. SEEK_CUR/1 indicates a position relative
        to the current stream position. SEEK_END/2 indicates a position relative to the end of the file.
        Only supported in read mode.

        Returns the new stream position.
        """
        self._raise_if_closed()
        self._raise_if_not_read()
        if self._is_local_file:
            if whence == SEEK_SET:
                self._pos = offset
            elif whence == SEEK_CUR:
                self._pos = self._file_stream.tell() + offset
            elif whence == SEEK_END:
                self._pos = os.path.getsize(self._file_location) + offset
            else:
                raise NotImplementedError(f"Unsupported whence value {whence}")
            if self._pos < 0:
                raise ValueError(f"Negative seek position {self._pos}")
            return self._file_stream.seek(self._pos, SEEK_SET)
        raise NotImplementedError(_NON_LOCAL_PATH_ERR_MSG)

    def seekable(self) -> bool:
        """
        See https://docs.python.org/3/library/io.html#io.IOBase.seekable

        Returns whether or not the stream is seekable.
        """
        self._raise_if_closed()
        if self._is_local_file:
            return self._mode in READ_MODES
        raise NotImplementedError(_NON_LOCAL_PATH_ERR_MSG)

    def tell(self) -> int:
        """
        See https://docs.python.org/3/library/io.html#io.IOBase.tell

        Gets the current stream position.
        """
        self._raise_if_closed()
        self._raise_if_not_read()
        if self._is_local_file:
            return self._pos
        raise NotImplementedError(_NON_LOCAL_PATH_ERR_MSG)

    def truncate(self, size: int | None = None) -> int:
        """
        Not yet supported in UDF and Stored Procedures.
        """
        self._raise_if_closed()
        self._raise_if_not_write()
        raise NotImplementedError(_DEFER_IMPLEMENTATION_ERR_MSG)

    def write(self, b: bytes | bytearray | array.array) -> int:
        """
        See https://docs.python.org/3/library/io.html#io.RawIOBase.write

        Write the given bytes-like object, b, to the underlying raw stream, and return the number of bytes-like objects
        written (e.g. unicode characters provided to a text input count as 1). The number of bytes should equal the input
        bytes, since all bytes are written directly to the stream. Local testing support is not implemented yet.
        """
        raise NotImplementedError(_WRITE_MODE_ERR_MSG)

    def writable(self) -> bool:
        """
        See https://docs.python.org/3/library/io.html#io.IOBase.writable

        Returns whether or not the stream is writable.
        """
        self._raise_if_closed()
        return self._file_stream.writable()

    def writelines(self, lines: Iterable[str] | list[str]) -> None:
        """
        From https://docs.python.org/3/library/io.html#io.IOBase.writelines

        Write a list of lines to the stream. Line separators are not added, so it is usual for each of the lines provided to
        have a line separator at the end. Local testing support is not implemented yet.
        """
        raise NotImplementedError(_WRITE_MODE_ERR_MSG)
