#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from functools import partial
import pytest
import re
import os
import io
from io import UnsupportedOperation

from snowflake.snowpark.files import SnowflakeFile, _DEFER_IMPLEMENTATION_ERR_MSG
from typing import Union

from snowflake.snowpark._internal.utils import generate_random_alphanumeric
import tempfile
import logging

_logger = logging.getLogger(__name__)


def _write_test_msg(
    write_mode: str, file_location: str, test_msg: str = None
) -> Union[str, bytes]:
    """
    Generates a test message or uses the provided message and writes it to the specified file location.

    Used to create a test message for reading in SnowflakeFile tests.
    """
    if test_msg is None:
        test_msg = generate_random_alphanumeric()
    if write_mode == "wb":
        test_msg = test_msg.encode()
    with open(file_location, write_mode) as f:
        f.write(test_msg)
    return test_msg


def _generate_and_write_lines(
    num_lines: int, write_mode: str, file_location: str
) -> list[Union[str, bytes]]:
    """
    Generates a list of test messages and writes them to the specified file location.
    """
    lines = [f"{generate_random_alphanumeric()}\n" for _ in range(num_lines)]
    if write_mode == "wb":
        lines = [line.encode() for line in lines]

    with open(file_location, write_mode) as f:
        for line in lines:
            f.write(line)

    return lines


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_create_snowflakefile(read_mode, write_mode, tmp_path):
    temp_file = os.path.join(tmp_path, "test.txt")
    _write_test_msg(write_mode, temp_file)

    def sf_open_new_result(mode: str) -> None:
        with SnowflakeFile.open_new_result(mode) as f:
            assert f._file_location is not None
            assert f._mode == write_mode

    def sf_open(file_location: str, mode: str) -> None:
        with SnowflakeFile.open(file_location, mode) as f:
            assert f._file_location == file_location
            assert f._mode == read_mode

    sf_open_new_result(write_mode)
    sf_open(temp_file, read_mode)


def test_invalid_mode_snowflakefile():
    def sf_open_new_result(mode: str) -> None:
        SnowflakeFile.open_new_result(mode)

    def sf_open(file_location: str, mode: str) -> None:
        SnowflakeFile.open(file_location, mode)

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Invalid mode 'w+' for SnowflakeFile.open_new_result. Supported modes are 'w' and 'wb'."
        ),
    ):
        sf_open_new_result("w+")

    with pytest.raises(
        ValueError,
        match="Invalid mode 'rw' for SnowflakeFile.open. Supported modes are 'r' and 'rb'.",
    ):
        sf_open("test", "rw")


def test_default_mode_snowflakefile(tmp_path):
    temp_file = os.path.join(tmp_path, "test.txt")
    _write_test_msg("w", temp_file)

    def sf_open_new_result() -> None:
        with SnowflakeFile.open_new_result() as f:
            assert f._file_location is not None
            assert f._mode == "w"

    def sf_open(file_location: str) -> None:
        with SnowflakeFile.open(file_location) as f:
            assert f._file_location is not None
            assert f._mode == "r"

    sf_open_new_result()
    sf_open(temp_file)


def test_snowflake_file_attribute(tmp_path):
    temp_file = os.path.join(tmp_path, "test.txt")
    _write_test_msg("w", temp_file)

    def sf_open_new_result() -> None:
        with SnowflakeFile.open_new_result() as f:
            assert f.buffer is None
            assert f.encoding is None
            assert f.errors is None

    def sf_open(file_location: str) -> None:
        with SnowflakeFile.open(file_location) as f:
            assert f.buffer is None
            assert f.encoding is None
            assert f.errors is None

    sf_open_new_result()
    sf_open(temp_file)


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_read_snowflakefile_local(read_mode, write_mode, tmp_path):
    temp_file = os.path.join(tmp_path, "test.txt")
    test_msg = _write_test_msg(write_mode, temp_file)

    def read_file(file_location: str, mode: str) -> Union[str, bytes]:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.read()

    assert read_file(temp_file, read_mode) == test_msg


@pytest.mark.parametrize("mode", ["r", "rb"])
def test_read_empty_file_snowflakefile_local(mode, tmp_path):
    temp_file = os.path.join(tmp_path, "test.txt")
    _write_test_msg("w", temp_file, "")

    def read_file(file_location: str, mode: str) -> Union[str, bytes]:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.read()

    content = read_file(temp_file, mode)
    if mode == "rb":
        assert content == b""
    else:
        assert content == ""


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "size"],
    [
        ("r", "w", 1),
        ("r", "w", 0),
        ("r", "w", -1),
        ("rb", "wb", 1),
        ("rb", "wb", 0),
        ("rb", "wb", -1),
    ],
)
def test_read_with_size_snowflakefile_local(read_mode, write_mode, size, tmp_path):
    temp_file = os.path.join(tmp_path, "test.txt")
    test_msg = _write_test_msg(write_mode, temp_file)

    def read_file(file_location: str, mode: str, size: int) -> Union[str, bytes]:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.read(size)

    content = read_file(temp_file, read_mode, size)
    if size == -1:
        assert content == test_msg
    else:
        assert content == test_msg[:size]


@pytest.mark.parametrize("mode", ["r", "rb"])
def test_read_non_existent_snowflakefile_local(mode):
    with tempfile.TemporaryDirectory() as temp_dir:
        with pytest.raises(FileNotFoundError):
            not_found_file_path = os.path.join(temp_dir, "non_existent_file.txt")

            def read_file(file_location: str, mode: str) -> Union[str, bytes]:
                with SnowflakeFile.open(file_location, mode) as f:
                    return f.read()

            assert read_file(not_found_file_path, mode)


@pytest.mark.parametrize("mode", ["w", "wb"])
def test_read_api_in_write_mode_snowflakefile_local(mode):
    def sf_throw_errors(mode: str) -> None:
        with SnowflakeFile.open_new_result(mode=mode) as f:
            methods = [
                f.read,
                f.read1,
                f.readall,
                partial(f.readinto, bytearray(1)),
                partial(f.readinto1, bytearray(1)),
                f.readline,
                f.readlines,
                partial(f.seek, 1),
                f.tell,
            ]
            for method in methods:
                with pytest.raises(
                    UnsupportedOperation, match=f"Not readable mode={mode}"
                ):
                    method()

    sf_throw_errors(mode)


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "test_msg"],
    [
        ("r", "w", "a"),
        ("r", "w", "This is a test message with escape characters: \n\t"),
        ("rb", "wb", "a"),
        ("rb", "wb", "This is a test message with escape characters: \n\t"),
    ],
)
def test_read_special_msg_snowflakefile_local(
    read_mode, write_mode, test_msg, tmp_path
):
    temp_file = os.path.join(tmp_path, "test.txt")
    test_msg = _write_test_msg(write_mode, temp_file, test_msg)

    def read_file(file_location: str, mode: str) -> Union[str, bytes]:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.read()

    assert read_file(temp_file, read_mode) == test_msg


@pytest.mark.parametrize("mode", ["w", "wb"])
def test_read_deleted_snowflakefile_local(mode, tmp_path):
    temp_file = os.path.join(tmp_path, "test.txt")
    _write_test_msg(mode, temp_file)
    os.remove(temp_file)

    with pytest.raises(
        FileNotFoundError,
        match=re.escape(f"No such file or directory: '{temp_file}'"),
    ):

        def sf_open(file_location: str) -> None:
            SnowflakeFile.open(file_location)

        sf_open(temp_file)


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_detach_snowflakefile_local(read_mode, write_mode, tmp_path):
    temp_file = os.path.join(tmp_path, "test.txt")
    _write_test_msg(write_mode, temp_file)
    detach_error = "Detaching stream from file is unsupported"

    def sf_detach_write(mode: str) -> None:
        with SnowflakeFile.open_new_result(mode) as f:
            f.detach()

    def sf_detach_read(file_location: str, mode: str) -> None:
        with SnowflakeFile.open(file_location, mode) as f:
            f.detach()

    with pytest.raises(UnsupportedOperation, match=detach_error):
        sf_detach_write(write_mode)

    with pytest.raises(UnsupportedOperation, match=detach_error):
        sf_detach_read(temp_file, read_mode)


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_fileno_snowflakefile_local(read_mode, write_mode, tmp_path):
    temp_file = os.path.join(tmp_path, "test.txt")
    _write_test_msg(write_mode, temp_file)
    fileno_error = "This object does not use a file descriptor"

    def sf_fileno_write(mode: str) -> int:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.fileno()

    def sf_fileno_read(file_location: str, mode: str) -> int:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.fileno()

    with pytest.raises(OSError, match=fileno_error):
        fileno = sf_fileno_write(write_mode)
        assert fileno

    with pytest.raises(OSError, match=fileno_error):
        fileno = sf_fileno_read(temp_file, read_mode)
        assert fileno


def test_flush_snowflakefile():
    # Flush currently has no implementation
    pass


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_isatty_snowflakefile_local(read_mode, write_mode, tmp_path):
    temp_file = os.path.join(tmp_path, "test.txt")
    _write_test_msg(write_mode, temp_file)

    def get_atty_write(mode: str) -> bool:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.isatty()

    def get_atty_read(file_location: str, mode: str) -> bool:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.isatty()

    assert not get_atty_write(write_mode)
    assert not get_atty_read(temp_file, read_mode)


@pytest.mark.parametrize("mode", ["r", "rb"])
def test_truncate_read_mode_snowflakefile_local(mode, tmp_path):
    temp_file = os.path.join(tmp_path, "test.txt")
    _write_test_msg("w", temp_file)

    def sf_truncate(file_location: str, mode: str) -> int:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.truncate(1)

    with pytest.raises(UnsupportedOperation, match="Not writable mode"):
        num_bytes = sf_truncate(temp_file, mode)
        assert num_bytes


@pytest.mark.parametrize("mode", ["w", "wb"])
def test_truncate_write_mode_snowflakefile_local(mode):
    def sf_truncate(mode: str) -> int:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.truncate(1)

    with pytest.raises(NotImplementedError, match=_DEFER_IMPLEMENTATION_ERR_MSG):
        num_bytes = sf_truncate(mode)
        assert num_bytes


@pytest.mark.parametrize("mode", ["r", "rb"])
def test_methods_closed_snowflakefile_local(tmp_path, mode):
    temp_file = os.path.join(tmp_path, "test.txt")
    _write_test_msg("w", temp_file)

    def sf_closed(file_location: str, mode: str) -> None:
        with SnowflakeFile.open(file_location, mode) as f:
            f.close()

            methods = [
                f.detach,
                f.fileno,
                f.flush,
                f.isatty,
                f.read,
                f.read1,
                f.readable,
                f.readall,
                partial(f.readinto, bytearray(1)),
                partial(f.readinto1, bytearray(1)),
                f.readline,
                f.readlines,
                partial(f.seek, 1),
                f.seekable,
                f.tell,
                f.truncate,
                f.writable,
            ]
            for method in methods:
                with pytest.raises(ValueError, match="I/O operation on closed file."):
                    method()

    sf_closed(temp_file, mode)


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_readable_snowflakefile_local(read_mode, write_mode, tmp_path):
    temp_file = os.path.join(tmp_path, "test.txt")
    _write_test_msg("w", temp_file)

    def is_readable_write(mode: str) -> bool:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.readable()

    def is_readable_read(file_location: str, mode: str) -> bool:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readable()

    assert not is_readable_write(write_mode)
    assert is_readable_read(temp_file, read_mode)


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_readall_snowflakefile_local(read_mode, write_mode, tmp_path):
    temp_file = os.path.join(tmp_path, "test.txt")
    num_lines = 5
    lines = _generate_and_write_lines(num_lines, write_mode, temp_file)

    def sf_readall(file_location: str, mode: str) -> list:
        with SnowflakeFile.open(file_location, mode) as snowflake_file:
            return snowflake_file.readall()

    content = sf_readall(temp_file, read_mode)
    if write_mode == "wb":
        assert content == b"".join(lines)
    else:
        assert content == "".join(lines)


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_readline_snowflakefile_local(read_mode, write_mode, tmp_path):
    temp_file = os.path.join(tmp_path, "test.txt")
    num_lines = 5
    lines = _generate_and_write_lines(num_lines, write_mode, temp_file)

    def sf_readline(file_location: str, mode: str) -> Union[str, bytes]:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readline()

    def sf_read_num_lines(file_location: str, mode: str, num_lines: int) -> list:
        with SnowflakeFile.open(file_location, mode) as f:
            return [f.readline() for _ in range(num_lines + 1)]

    assert sf_readline(temp_file, read_mode) == lines[0]
    content = sf_read_num_lines(temp_file, read_mode, num_lines)

    for i in range(num_lines):
        assert content[i] == lines[i]

    if write_mode == "wb":
        assert content[-1] == b""
    else:
        assert content[-1] == ""


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "size"],
    [
        ("r", "w", 2),
        ("r", "w", 0),
        ("r", "w", -1),
        ("rb", "wb", 2),
        ("rb", "wb", 0),
        ("rb", "wb", -1),
    ],
)
def test_readline_with_size_snowflakefile_local(read_mode, write_mode, size, tmp_path):
    temp_file = os.path.join(tmp_path, "test.txt")
    num_lines = 5
    lines = _generate_and_write_lines(num_lines, write_mode, temp_file)

    def sf_readline_with_size(
        file_location: str, mode: str, size: int
    ) -> Union[str, bytes]:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readline(size)

    content = sf_readline_with_size(temp_file, read_mode, size)
    if size == -1:
        assert content == lines[0]
    else:
        assert content == lines[0][:size]


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_readlines_snowflakefile_local(read_mode, write_mode, tmp_path):
    temp_file = os.path.join(tmp_path, "test.txt")
    num_lines = 5
    lines = _generate_and_write_lines(num_lines, write_mode, temp_file)

    def sf_readlines(file_location: str, mode: str) -> list:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readlines()

    content = sf_readlines(temp_file, read_mode)
    for i in range(num_lines):
        assert content[i] == lines[i]


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "hint"],
    [
        ("r", "w", 25),
        ("r", "w", 2),
        ("r", "w", 0),
        ("r", "w", -1),
        ("rb", "wb", 25),
        ("rb", "wb", 2),
        ("rb", "wb", 0),
        ("rb", "wb", -1),
    ],
)
def test_readlines_with_hint_snowflakefile_local(read_mode, write_mode, hint, tmp_path):
    temp_file = os.path.join(tmp_path, "test.txt")
    num_lines = 5
    lines = _generate_and_write_lines(num_lines, write_mode, temp_file)

    def sf_readlines_with_hint(file_location: str, mode: str, hint: int) -> list:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readlines(hint=hint)

    content = sf_readlines_with_hint(temp_file, read_mode, hint)
    for i in range(min(num_lines, len(content))):
        assert content[i] == lines[i]


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "offset", "whence"],
    [
        ("r", "w", 3, io.SEEK_SET),
        ("r", "w", 0, io.SEEK_SET),
        ("rb", "wb", 3, io.SEEK_SET),
        ("rb", "wb", 0, io.SEEK_SET),
        ("r", "w", 3, io.SEEK_CUR),
        ("r", "w", 0, io.SEEK_CUR),
        ("r", "w", -3, io.SEEK_CUR),
        ("rb", "wb", 3, io.SEEK_CUR),
        ("rb", "wb", 0, io.SEEK_CUR),
        ("rb", "wb", -3, io.SEEK_CUR),
        ("r", "w", 3, io.SEEK_END),
        ("r", "w", 0, io.SEEK_END),
        ("r", "w", -3, io.SEEK_END),
        ("rb", "wb", 3, io.SEEK_END),
        ("rb", "wb", 0, io.SEEK_END),
        ("rb", "wb", -3, io.SEEK_END),
    ],
)
def test_seek_snowflakefile_local(read_mode, write_mode, offset, whence, tmp_path):
    temp_file = os.path.join(tmp_path, "test.txt")
    test_msg = _write_test_msg(write_mode, temp_file)

    def sf_seek(
        file_location: str, mode: str, initial_offset: int, offset: int, whence: int
    ) -> int:
        with SnowflakeFile.open(file_location, mode) as f:
            f.seek(initial_offset)
            return f.seek(offset, whence)

    # Used to support the negative seeks for seek_cur
    initial_offset = 5
    pos = sf_seek(temp_file, read_mode, initial_offset, offset, whence)
    if whence == io.SEEK_SET:
        assert pos == offset
    elif whence == io.SEEK_CUR:
        assert pos == offset + initial_offset
    else:
        assert pos == (len(test_msg) + offset)


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "offset", "whence"],
    [
        ("r", "w", -1, io.SEEK_SET),
        ("rb", "wb", -1, io.SEEK_SET),
        ("r", "w", 3, 4),
        ("rb", "wb", 3, 4),
        ("r", "w", 3, -1),
        ("rb", "wb", 3, -1),
    ],
)
def test_seek_error_snowflakefile_local(
    read_mode, write_mode, offset, whence, tmp_path
):
    temp_file = os.path.join(tmp_path, "test.txt")
    _write_test_msg(write_mode, temp_file)

    def sf_seek(file_location: str, mode: str, offset: int, whence: int) -> int:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.seek(offset, whence)

    if whence not in range(3):
        with pytest.raises(
            NotImplementedError, match=f"Unsupported whence value {whence}"
        ):
            assert sf_seek(temp_file, read_mode, offset, whence)
    else:
        with pytest.raises(ValueError, match="Negative seek position"):
            assert sf_seek(temp_file, read_mode, offset, whence)


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_seekable_snowflakefile_local(read_mode, write_mode, tmp_path):
    temp_file = os.path.join(tmp_path, "test.txt")
    _write_test_msg(write_mode, temp_file)

    def is_seekable_write(mode: str) -> bool:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.seekable()

    def is_seekable_read(file_location: str, mode: str) -> bool:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.seekable()

    assert not is_seekable_write(write_mode)
    assert is_seekable_read(temp_file, read_mode)


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_tell_snowflakefile_local(read_mode, write_mode, tmp_path):
    temp_file = os.path.join(tmp_path, "test.txt")
    test_msg = generate_random_alphanumeric(9) + "\n"
    length = len(test_msg)
    if write_mode == "wb":
        test_msg = test_msg.encode()

    with open(temp_file, mode=write_mode) as f:
        f.writelines(
            [test_msg, test_msg]
        )  # used to test reads that don't read an entire file
        counter = 2

    def sf_tell(file_location: str, mode: str, counter: int, length: int) -> None:
        with SnowflakeFile.open(file_location, mode) as f:
            # Methods that read the entire file
            methods = [
                f.read,
                f.readall,
                f.readlines,
            ]

            if read_mode == "rb":
                methods.append(f.read1)

            for method in methods:
                method()
                assert f.tell() == counter * length
                f.seek(0)

            # Methods that read a portion of the file
            methods = [
                f.readline,
                partial(f.read, length),
                partial(f.seek, length),
            ]

            for method in methods:
                method()
                assert f.tell() == length
                f.seek(0)

            if read_mode == "rb":
                size = 5
                methods = [
                    partial(f.readinto, bytearray(size)),
                    partial(f.readinto1, bytearray(size)),
                ]

                for method in methods:
                    method()
                    assert f.tell() == size
                    f.seek(0)

    sf_tell(temp_file, read_mode, counter, length)


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_writable_snowflakefile_local(read_mode, write_mode, tmp_path):
    temp_file = os.path.join(tmp_path, "test.txt")
    _write_test_msg(write_mode, temp_file)

    def is_writable_write(mode: str) -> bool:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.writable()

    def is_writable_read(file_location: str, mode: str) -> bool:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.writable()

    assert is_writable_write(write_mode)
    assert not is_writable_read(temp_file, read_mode)


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_read1_snowflakefile_local(read_mode, write_mode, tmp_path):
    temp_file = os.path.join(tmp_path, "test.txt")
    test_msg = _write_test_msg(write_mode, temp_file)
    if type(test_msg) is str:
        test_msg = test_msg.encode()

    def sf_read1(file_location: str, mode: str) -> bytes:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.read1()

    assert sf_read1(temp_file, read_mode) == test_msg


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "size"],
    [
        ("r", "w", 1),
        ("r", "w", 0),
        ("r", "w", -1),
        ("rb", "wb", 1),
        ("rb", "wb", 0),
        ("rb", "wb", -1),
    ],
)
def test_read1_with_size_snowflakefile_local(read_mode, write_mode, size, tmp_path):
    temp_file = os.path.join(tmp_path, "test.txt")
    test_msg = _write_test_msg(write_mode, temp_file)
    if type(test_msg) is str:
        test_msg = test_msg.encode()

    def sf_read1(file_location: str, mode: str, size: int) -> bytes:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.read1(size=size)

    content = sf_read1(temp_file, read_mode, size)
    if size == -1:
        assert content == test_msg
    else:
        assert content == test_msg[:size]


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "size"],
    [
        ("r", "w", 0),
        ("r", "w", 1),
        ("r", "w", 5),
        ("r", "w", 10),
        ("rb", "wb", 0),
        ("rb", "wb", 1),
        ("rb", "wb", 5),
        ("rb", "wb", 10),
    ],
)
def test_readinto_snowflakefile_local(read_mode, write_mode, size, tmp_path):
    temp_file = os.path.join(tmp_path, "test.txt")
    test_msg = generate_random_alphanumeric(size)
    _write_test_msg(write_mode, temp_file, test_msg)
    encoded_test_msg = test_msg.encode()

    def sf_readinto(file_location: str, mode: str, buffer: bytearray) -> int:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readinto(buffer)

    buffer_size = 5
    buffer = bytearray(buffer_size)
    length = sf_readinto(temp_file, read_mode, buffer)
    num_read = min(size, buffer_size)
    buffer = bytes(buffer)

    assert length == num_read
    assert buffer[:num_read] == encoded_test_msg[:num_read]
    for byte in buffer[num_read:]:
        assert byte == 0


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_readinto_escape_chars_snowflakefile_local(read_mode, write_mode, tmp_path):
    temp_file = os.path.join(tmp_path, "test.txt")
    test_msg = "This is a test message with escape characters: \n\t"
    _write_test_msg(write_mode, temp_file, test_msg)
    encoded_test_msg = test_msg.encode()

    def sf_readinto(file_location: str, mode: str, buffer: bytearray) -> int:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readinto(buffer)

    buffer_size = 50
    buffer = bytearray(buffer_size)
    length = sf_readinto(temp_file, read_mode, buffer)
    buffer = bytes(buffer)

    assert length == len(encoded_test_msg)
    assert buffer[:length] == encoded_test_msg[:length]
    for byte in buffer[length:]:
        assert byte == 0


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "size"],
    [
        ("r", "w", 0),
        ("r", "w", 1),
        ("r", "w", 5),
        ("r", "w", 10),
        ("rb", "wb", 0),
        ("rb", "wb", 1),
        ("rb", "wb", 5),
        ("rb", "wb", 10),
    ],
)
def test_readinto1_snowflakefile_local(read_mode, write_mode, size, tmp_path):
    test_msg = generate_random_alphanumeric(size)
    temp_file = os.path.join(tmp_path, "test.txt")
    _write_test_msg(write_mode, temp_file, test_msg)
    encoded_test_msg = test_msg.encode()

    def sf_readinto1(file_location: str, mode: str, buffer: bytearray) -> int:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readinto1(buffer)

    buffer_size = 5
    buffer = bytearray(buffer_size)
    length = sf_readinto1(temp_file, read_mode, buffer)
    num_read = min(size, buffer_size)
    buffer = bytes(buffer)

    assert length == num_read
    assert buffer[:num_read] == encoded_test_msg[:num_read]
    for byte in buffer[num_read:]:
        assert byte == 0
