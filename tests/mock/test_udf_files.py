#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
import io
import os
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, udf, sproc
from snowflake.snowpark._internal.utils import generate_random_alphanumeric
from snowflake.snowpark.row import Row
from snowflake.snowpark.files import SnowflakeFile
from tests.utils import Utils
from typing import Union


def _write_test_msg(
    write_mode: str, file_location: str, test_msg: str = None
) -> tuple[Union[str, bytes], str]:
    """
    Generates a test message or uses the provided message and writes it to the specified file location.

    Used to create a test message for reading in SnowflakeFile tests.
    """
    file_location = os.path.join(file_location, f"{generate_random_alphanumeric()}.txt")
    if test_msg is None:
        test_msg = generate_random_alphanumeric()
    if write_mode == "wb":
        test_msg = test_msg.encode()
    encoding = "utf-8" if write_mode == "w" else None
    with open(file_location, write_mode, encoding=encoding) as f:
        f.write(test_msg)
    return test_msg, file_location


def _write_test_msg_to_stage(
    write_mode: str,
    file_location: str,
    tmp_stage: str,
    session: Session,
    test_msg: str = None,
) -> tuple[Union[str, bytes], str]:
    test_msg, file_location = _write_test_msg(write_mode, file_location, test_msg)
    Utils.upload_to_stage(session, f"@{tmp_stage}", file_location, compress=False)
    return test_msg, f"@{tmp_stage}/{file_location.split('/')[-1]}"


def _generate_and_write_lines(
    num_lines: int,
    write_mode: str,
    file_location: str,
    msg: Union[str, bytes] = None,
) -> tuple[list[Union[str, bytes]], str]:
    """
    Generates a list of test messages and writes them to the specified file location.
    """
    file_location = os.path.join(file_location, f"{generate_random_alphanumeric()}.txt")
    lines = [
        f"{generate_random_alphanumeric()}\n" if msg is None else f"{msg}\n"
        for _ in range(num_lines)
    ]
    if write_mode == "wb":
        lines = [line.encode() for line in lines]
    encoding = "utf-8" if write_mode == "w" else None
    with open(file_location, write_mode, encoding=encoding) as f:
        for line in lines:
            f.write(line)

    return lines, file_location


def _generate_and_write_lines_to_stage(
    num_lines: int,
    write_mode: str,
    file_location: str,
    tmp_stage: str,
    session: Session,
    msg: Union[str, bytes] = None,
) -> tuple[list[Union[str, bytes]], str]:
    lines, file_location = _generate_and_write_lines(
        num_lines, write_mode, file_location, msg
    )
    Utils.upload_to_stage(session, f"@{tmp_stage}", file_location, compress=False)
    return lines, f"@{tmp_stage}/{file_location.split('/')[-1]}"


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "use_stage"],
    [("r", "w", True), ("r", "w", False), ("rb", "wb", True), ("rb", "wb", False)],
)
def test_read_snowflakefile(
    read_mode, write_mode, use_stage, tmp_path, tmp_stage, session
):
    if not use_stage:
        test_msg, temp_file = _write_test_msg(write_mode, tmp_path)
    else:
        test_msg, temp_file = _write_test_msg_to_stage(
            write_mode, tmp_path, tmp_stage, session
        )

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


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "use_stage"],
    [("r", "w", True), ("r", "w", False), ("rb", "wb", True), ("rb", "wb", False)],
)
def test_read_sproc_snowflakefile(
    read_mode, write_mode, use_stage, tmp_path, tmp_stage, session
):
    if not use_stage:
        test_msg, temp_file = _write_test_msg(write_mode, tmp_path)
    else:
        test_msg, temp_file = _write_test_msg_to_stage(
            write_mode, tmp_path, tmp_stage, session
        )

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


def test_read_snowurl_snowflakefile(tmp_path, session):
    test_msg, temp_file = _write_test_msg("w", tmp_path)
    snowurl = "snow://"
    session.file.put(temp_file, snowurl, auto_compress=False)

    @udf
    def read_file(file_location: str, mode: str) -> str:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.read()

    df = session.create_dataframe(
        [["r", f"{snowurl}{temp_file}"]],
        schema=["read_mode", "temp_file"],
    )
    result = df.select(read_file(col("temp_file"), col("read_mode"))).collect()
    Utils.check_answer(result, [Row(test_msg)])


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "use_stage"],
    [("r", "w", True), ("r", "w", False), ("rb", "wb", True), ("rb", "wb", False)],
)
def test_isatty_snowflakefile(
    read_mode, write_mode, use_stage, tmp_path, tmp_stage, session
):
    if not use_stage:
        _, temp_file = _write_test_msg(write_mode, tmp_path)
    else:
        _, temp_file = _write_test_msg_to_stage(
            write_mode, tmp_path, tmp_stage, session
        )

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


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "use_stage"],
    [("r", "w", True), ("r", "w", False), ("rb", "wb", True), ("rb", "wb", False)],
)
def test_readable_snowflakefile(
    read_mode, write_mode, use_stage, tmp_path, tmp_stage, session
):
    if not use_stage:
        _, temp_file = _write_test_msg(write_mode, tmp_path)
    else:
        _, temp_file = _write_test_msg_to_stage(
            write_mode, tmp_path, tmp_stage, session
        )

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


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "use_stage"],
    [("r", "w", True), ("r", "w", False), ("rb", "wb", True), ("rb", "wb", False)],
)
def test_readline_snowflakefile(
    read_mode, write_mode, use_stage, tmp_path, tmp_stage, session
):
    num_lines = 5
    if not use_stage:
        lines, temp_file = _generate_and_write_lines(num_lines, write_mode, tmp_path)
    else:
        lines, temp_file = _generate_and_write_lines_to_stage(
            num_lines, write_mode, tmp_path, tmp_stage, session
        )

    @udf
    def get_line(file_location: str, mode: str) -> str:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readline()

    df = session.create_dataframe(
        [[read_mode, temp_file]],
        schema=["read_mode", "temp_file"],
    )
    result = df.select(get_line(col("temp_file"), col("read_mode"))).collect()
    actual_line = result[0][0]
    # used to ensure compatibility with tests on windows OS which adds \r before \n

    if read_mode == "rb":
        actual_line = actual_line.replace(b"\r", b"")
    else:
        actual_line = actual_line.replace("\r", "")

    Utils.check_answer(Row(actual_line), Row(lines[0]))


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "offset", "whence", "use_stage"],
    [
        ("r", "w", 3, io.SEEK_SET, True),
        ("r", "w", 3, io.SEEK_SET, False),
        ("rb", "wb", 3, io.SEEK_SET, True),
        ("rb", "wb", 3, io.SEEK_SET, False),
    ],
)
def test_seek_snowflakefile(
    read_mode, write_mode, offset, whence, use_stage, tmp_path, tmp_stage, session
):
    if not use_stage:
        _, temp_file = _write_test_msg(write_mode, tmp_path)
    else:
        _, temp_file = _write_test_msg_to_stage(
            write_mode, tmp_path, tmp_stage, session
        )

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


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "use_stage"],
    [("r", "w", True), ("r", "w", False), ("rb", "wb", True), ("rb", "wb", False)],
)
def test_seekable_snowflakefile(
    read_mode, write_mode, use_stage, tmp_path, tmp_stage, session
):
    if not use_stage:
        _, temp_file = _write_test_msg(write_mode, tmp_path)
    else:
        _, temp_file = _write_test_msg_to_stage(
            write_mode, tmp_path, tmp_stage, session
        )

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


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "use_stage"],
    [("r", "w", True), ("r", "w", False), ("rb", "wb", True), ("rb", "wb", False)],
)
def test_tell_snowflakefile(
    read_mode, write_mode, use_stage, tmp_path, tmp_stage, session
):
    if not use_stage:
        _, temp_file = _write_test_msg(write_mode, tmp_path)
    else:
        _, temp_file = _write_test_msg_to_stage(
            write_mode, tmp_path, tmp_stage, session
        )

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


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "use_stage"],
    [("r", "w", True), ("r", "w", False), ("rb", "wb", True), ("rb", "wb", False)],
)
def test_writable_snowflakefile(
    read_mode, write_mode, use_stage, tmp_path, tmp_stage, session
):
    if not use_stage:
        _, temp_file = _write_test_msg(write_mode, tmp_path)
    else:
        _, temp_file = _write_test_msg_to_stage(
            write_mode, tmp_path, tmp_stage, session
        )

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
    ["read_mode", "write_mode", "size", "use_stage"],
    [
        ("r", "w", 0, True),
        ("r", "w", 0, False),
        ("r", "w", 1, True),
        ("r", "w", 1, False),
        ("rb", "wb", 0, True),
        ("rb", "wb", 0, False),
        ("rb", "wb", 1, True),
        ("rb", "wb", 1, False),
    ],
)
def test_readinto_snowflakefile(
    read_mode, write_mode, size, use_stage, tmp_path, tmp_stage, session
):
    test_msg = generate_random_alphanumeric(size)
    if not use_stage:
        _, temp_file = _write_test_msg(write_mode, tmp_path, test_msg)
    else:
        _, temp_file = _write_test_msg_to_stage(
            write_mode, tmp_path, tmp_stage, session, test_msg
        )
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
