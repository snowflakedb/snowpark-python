#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
import io
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, udf, sproc
from snowflake.snowpark._internal.utils import generate_random_alphanumeric
from snowflake.snowpark.row import Row
from snowflake.snowpark.files import SnowflakeFile
from tests.utils import Utils
from typing import Union


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
def test_read_snowflakefile_local(read_mode, write_mode, temp_file, session):
    test_msg = _write_test_msg(write_mode, temp_file)

    @udf
    def read_file(file_location: str, mode: str) -> str:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.read()

    df = session.create_dataframe(
        [[read_mode, temp_file]],
        schema=["read_mode", "temp_file"],
    )
    result = df.select(read_file(col("temp_file"), col("read_mode"))).collect()
    Utils.check_answer(result, [Row(test_msg)])


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_read_sproc_snowflakefile_local(read_mode, write_mode, temp_file, session):
    test_msg = _write_test_msg(write_mode, temp_file)

    @udf
    def read_file(file_location: str, mode: str) -> str:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.read()

    @sproc
    def read_file_sp(session_: Session, file_location: str, mode: str) -> str:
        df = session_.create_dataframe(
            [[read_mode, temp_file]],
            schema=["read_mode", "temp_file"],
        )

        return df.select(read_file(col("temp_file"), col("read_mode"))).collect()

    result = read_file_sp(temp_file, read_mode)
    Utils.check_answer(result, [Row(test_msg)])


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_isatty_snowflakefile_local(read_mode, write_mode, temp_file, session):
    @udf
    def get_atty_write(mode: str) -> bool:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.isatty()

    @udf
    def get_atty_read(file_location: str, mode: str) -> bool:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.isatty()

    df = session.create_dataframe(
        [[read_mode, write_mode, temp_file]],
        schema=["read_mode", "write_mode", "temp_file"],
    )
    result = df.select(get_atty_write(col("write_mode"))).collect()
    Utils.check_answer(result, [Row(False)])

    result = df.select(get_atty_read(col("temp_file"), col("read_mode"))).collect()
    Utils.check_answer(result, [Row(False)])


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_readable_snowflakefile_local(read_mode, write_mode, temp_file, session):
    @udf
    def is_readable_write(mode: str) -> bool:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.readable()

    @udf
    def is_readable_read(file_location: str, mode: str) -> bool:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readable()

    df = session.create_dataframe(
        [[read_mode, write_mode, temp_file]],
        schema=["read_mode", "write_mode", "temp_file"],
    )
    result = df.select(is_readable_write(col("write_mode"))).collect()
    Utils.check_answer(result, [Row(False)])

    result = df.select(is_readable_read(col("temp_file"), col("read_mode"))).collect()
    Utils.check_answer(result, [Row(True)])


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_readline_snowflakefile_local(read_mode, write_mode, temp_file, session):
    num_lines = 5
    lines = _generate_and_write_lines(num_lines, write_mode, temp_file)

    @udf
    def get_line(file_location: str, mode: str) -> str:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readline()

    df = session.create_dataframe(
        [[read_mode, temp_file]],
        schema=["read_mode", "temp_file"],
    )
    result = df.select(get_line(col("temp_file"), col("read_mode"))).collect()
    Utils.check_answer(result, [Row(lines[0])])


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "offset", "whence"],
    [
        ("r", "w", 3, io.SEEK_SET),
        ("rb", "wb", 3, io.SEEK_SET),
    ],
)
def test_seek_snowflakefile_local(
    read_mode, write_mode, offset, whence, temp_file, session
):
    _write_test_msg(write_mode, temp_file)

    @udf
    def seek(file_location: str, mode: str, offset: int, whence: int) -> int:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.seek(offset, whence)

    df = session.create_dataframe(
        [[read_mode, temp_file, offset, whence]],
        schema=["read_mode", "temp_file", "offset", "whence"],
    )
    result = df.select(
        seek(col("temp_file"), col("read_mode"), col("offset"), col("whence"))
    ).collect()
    Utils.check_answer(result, [Row(offset)])


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_seekable_snowflakefile_local(read_mode, write_mode, temp_file, session):
    @udf
    def is_seekable_write(mode: str) -> bool:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.seekable()

    @udf
    def is_seekable_read(file_location: str, mode: str) -> bool:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.seekable()

    df = session.create_dataframe(
        [[read_mode, write_mode, temp_file]],
        schema=["read_mode", "write_mode", "temp_file"],
    )
    result = df.select(is_seekable_write(col("write_mode"))).collect()
    Utils.check_answer(result, [Row(False)])

    result = df.select(is_seekable_read(col("temp_file"), col("read_mode"))).collect()
    Utils.check_answer(result, [Row(True)])


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_tell_snowflakefile_local(read_mode, write_mode, temp_file, session):
    _write_test_msg(write_mode, temp_file)

    @udf
    def try_tell(file_location: str, mode: str, size: int) -> int:
        with SnowflakeFile.open(file_location, mode) as f:
            f.read(size)
            return f.tell()

    df = session.create_dataframe(
        [[read_mode, temp_file, 5]],
        schema=["read_mode", "temp_file", "size"],
    )
    result = df.select(
        try_tell(col("temp_file"), col("read_mode"), col("size"))
    ).collect()
    Utils.check_answer(result, [Row(5)])


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_writable_snowflakefile_local(read_mode, write_mode, temp_file, session):
    @udf
    def is_writable_write(mode: str) -> bool:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.writable()

    @udf
    def is_writable_read(file_location: str, mode: str) -> bool:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.writable()

    df = session.create_dataframe(
        [[read_mode, write_mode, temp_file]],
        schema=["read_mode", "write_mode", "temp_file"],
    )
    result = df.select(is_writable_write(col("write_mode"))).collect()
    Utils.check_answer(result, [Row(True)])

    result = df.select(is_writable_read(col("temp_file"), col("read_mode"))).collect()
    Utils.check_answer(result, [Row(False)])


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
def test_readinto_snowflakefile_local(read_mode, write_mode, size, temp_file, session):
    test_msg = generate_random_alphanumeric(size)
    _write_test_msg(write_mode, temp_file, test_msg)
    encoded_test_msg = test_msg.encode()

    @udf
    def sf_readinto(file_location: str, mode: str, buffer: bytearray) -> int:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readinto(buffer)

    buffer_size = 5
    buffer = bytearray(buffer_size)
    df = session.create_dataframe(
        [[read_mode, temp_file, buffer]], schema=["read_mode", "temp_file", "buffer"]
    )

    result = df.select(
        sf_readinto(col("temp_file"), col("read_mode"), col("buffer"))
    ).collect()
    num_read = min(size, buffer_size)
    buffer = bytes(buffer)

    Utils.check_answer(result, [Row(num_read)])
    assert buffer[:num_read] == encoded_test_msg[:num_read]
    for byte in buffer[num_read:]:
        assert byte == 0
