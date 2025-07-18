#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
import io
from snowflake.snowpark._internal.utils import generate_random_alphanumeric
from snowflake.snowpark.files import SnowflakeFile
from tests.utils import Utils
from typing import Union


_STANDARD_ARGS = ["read_mode", "write_mode"]
_STANDARD_ARGVALUES = [("r", "w"), ("rb", "wb")]


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_read_snowflakefile(read_mode, write_mode, tmp_path, temp_stage, session):
    test_msg, temp_file = Utils.write_test_msg_to_stage(
        write_mode, tmp_path, temp_stage, session
    )

    def read_file(file_location: str, mode: str) -> Union[str, bytes]:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.read()

    assert test_msg == read_file(temp_file, read_mode)


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_read1_snowflakefile(read_mode, write_mode, tmp_path, temp_stage, session):
    test_msg, temp_file = Utils.write_test_msg_to_stage(
        write_mode, tmp_path, temp_stage, session
    )
    if type(test_msg) is str:
        test_msg = test_msg.encode()

    def sf_read1(file_location: str, mode: str) -> bytes:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.read1()

    assert sf_read1(temp_file, read_mode) == test_msg


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_readall_snowflakefile(read_mode, write_mode, tmp_path, temp_stage, session):
    num_lines = 5
    lines, temp_file = Utils.generate_and_write_lines_to_stage(
        num_lines, write_mode, tmp_path, temp_stage, session
    )

    def sf_readall(file_location: str, mode: str) -> list:
        with SnowflakeFile.open(file_location, mode) as snowflake_file:
            return snowflake_file.readall()

    content = sf_readall(temp_file, read_mode)
    windows_lines = [
        line[:-1] + b"\r\n" if read_mode == "rb" else line[:-1] + "\r\n"
        for line in lines
    ]  # need for windows testing as \r is added
    if write_mode == "wb":
        assert content == b"".join(lines) or content == b"".join(windows_lines)
    else:
        assert content == "".join(lines) or content == "".join(windows_lines)


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_readlines_snowflakefile(read_mode, write_mode, tmp_path, temp_stage, session):
    num_lines = 5
    lines, temp_file = Utils.generate_and_write_lines_to_stage(
        num_lines, write_mode, tmp_path, temp_stage, session
    )

    def sf_readlines(file_location: str, mode: str) -> list:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readlines()

    content = sf_readlines(temp_file, read_mode)
    windows_lines = [
        line[:-1] + b"\r\n" if read_mode == "rb" else line[:-1] + "\r\n"
        for line in lines
    ]  # need for windows testing as \r is added
    for i in range(num_lines):
        assert content[i] == lines[i] or content[i] == windows_lines[i]


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_isatty_snowflakefile(read_mode, write_mode, tmp_path, temp_stage, session):
    _, temp_file = Utils.write_test_msg_to_stage(
        write_mode, tmp_path, temp_stage, session
    )

    def get_atty_write(mode: str) -> bool:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.isatty()

    def get_atty_read(file_location: str, mode: str) -> bool:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.isatty()

    assert not get_atty_write(write_mode)
    assert not get_atty_read(temp_file, read_mode)


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_readable_snowflakefile(read_mode, write_mode, tmp_path, temp_stage, session):
    _, temp_file = Utils.write_test_msg_to_stage(
        write_mode, tmp_path, temp_stage, session
    )

    def is_readable_write(mode: str) -> bool:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.readable()

    def is_readable_read(file_location: str, mode: str) -> bool:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readable()

    assert not is_readable_write(write_mode)
    assert is_readable_read(temp_file, read_mode)


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_readline_snowflakefile(read_mode, write_mode, tmp_path, temp_stage, session):
    num_lines = 5
    lines, temp_file = Utils.generate_and_write_lines_to_stage(
        num_lines, write_mode, tmp_path, temp_stage, session
    )

    def get_line(file_location: str, mode: str) -> Union[str, bytes]:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readline()

    actual_line = get_line(temp_file, read_mode)

    # used to ensure compatibility with tests on windows OS which adds \r before \n
    if read_mode == "r":
        actual_line = actual_line.replace("\r", "")

    assert lines[0] == actual_line


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "offset", "whence"],
    [
        ("r", "w", 3, io.SEEK_SET),
        ("rb", "wb", 3, io.SEEK_SET),
    ],
)
def test_seek_snowflakefile(
    read_mode, write_mode, offset, whence, tmp_path, temp_stage, session
):
    _, temp_file = Utils.write_test_msg_to_stage(
        write_mode, tmp_path, temp_stage, session
    )

    def seek(file_location: str, mode: str, offset: int, whence: int) -> int:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.seek(offset, whence)

    assert offset == seek(temp_file, read_mode, offset, whence)


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_seekable_snowflakefile(read_mode, write_mode, tmp_path, temp_stage, session):
    _, temp_file = Utils.write_test_msg_to_stage(
        write_mode, tmp_path, temp_stage, session
    )

    def is_seekable_write(mode: str) -> bool:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.seekable()

    def is_seekable_read(file_location: str, mode: str) -> bool:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.seekable()

    assert not is_seekable_write(write_mode)
    assert is_seekable_read(temp_file, read_mode)


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_tell_snowflakefile(read_mode, write_mode, tmp_path, temp_stage, session):
    _, temp_file = Utils.write_test_msg_to_stage(
        write_mode, tmp_path, temp_stage, session
    )

    def try_tell(file_location: str, mode: str, size: int) -> int:
        with SnowflakeFile.open(file_location, mode) as f:
            f.read(size)
            return f.tell()

    assert try_tell(temp_file, read_mode, 5) == 5


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_writable_snowflakefile(read_mode, write_mode, tmp_path, temp_stage, session):
    _, temp_file = Utils.write_test_msg_to_stage(
        write_mode, tmp_path, temp_stage, session
    )

    def is_writable_write(mode: str) -> bool:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.writable()

    def is_writable_read(file_location: str, mode: str) -> bool:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.writable()

    assert is_writable_write(write_mode)
    assert not is_writable_read(temp_file, read_mode)


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "size"],
    [
        ("r", "w", 0),
        ("r", "w", 1),
        ("rb", "wb", 0),
        ("rb", "wb", 1),
    ],
)
def test_readinto_snowflakefile(
    read_mode, write_mode, size, tmp_path, temp_stage, session
):
    test_msg = generate_random_alphanumeric(size)
    _, temp_file = Utils.write_test_msg_to_stage(
        write_mode, tmp_path, temp_stage, session, test_msg
    )
    encoded_test_msg = test_msg.encode()

    def sf_readinto(file_location: str, mode: str, buffer_size: int) -> bytearray:
        buffer = bytearray(buffer_size)
        with SnowflakeFile.open(file_location, mode) as f:
            f.readinto(buffer)
        return buffer

    buffer_size = 5
    buffer = sf_readinto(temp_file, read_mode, buffer_size)
    num_read = min(size, buffer_size)
    buffer = bytes(buffer)

    assert buffer[:num_read] == encoded_test_msg[:num_read]
    for byte in buffer[num_read:]:
        assert byte == 0


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "size"],
    [
        ("r", "w", 0),
        ("r", "w", 1),
        ("rb", "wb", 0),
        ("rb", "wb", 1),
    ],
)
def test_readinto1_snowflakefile(
    read_mode, write_mode, size, tmp_path, temp_stage, session
):
    test_msg = generate_random_alphanumeric(size)
    _, temp_file = Utils.write_test_msg_to_stage(
        write_mode, tmp_path, temp_stage, session, test_msg
    )

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
