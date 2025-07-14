#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
import io
from snowflake.snowpark._internal.utils import generate_random_alphanumeric
from snowflake.snowpark.files import SnowflakeFile
from tests.utils import Utils


@pytest.fixture(scope="module", autouse=True)
def setup(session):
    session.add_packages("snowflake-snowpark-python")


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

    def read_file(file_location: str, mode: str) -> str:
        with SnowflakeFile.open(file_location, mode, require_scoped_url=False) as f:
            return f.read()

    assert test_msg == read_file(temp_file, read_mode)


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
        with SnowflakeFile.open(file_location, mode, require_scoped_url=False) as f:
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
        with SnowflakeFile.open(file_location, mode, require_scoped_url=False) as f:
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

    def get_line(file_location: str, mode: str) -> str:
        with SnowflakeFile.open(file_location, mode, require_scoped_url=False) as f:
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
        with SnowflakeFile.open(file_location, mode, require_scoped_url=False) as f:
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
        with SnowflakeFile.open(file_location, mode, require_scoped_url=False) as f:
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
        with SnowflakeFile.open(file_location, mode, require_scoped_url=False) as f:
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
        with SnowflakeFile.open(file_location, mode, require_scoped_url=False) as f:
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
        with SnowflakeFile.open(file_location, mode, require_scoped_url=False) as f:
            f.readinto(buffer)
        return buffer

    buffer_size = 5
    buffer = sf_readinto(temp_file, read_mode, buffer_size)
    num_read = min(size, buffer_size)
    buffer = bytes(buffer)

    assert buffer[:num_read] == encoded_test_msg[:num_read]
    for byte in buffer[num_read:]:
        assert byte == 0
