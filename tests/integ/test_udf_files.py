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

    @udf
    def read_file(file_location: str, mode: str) -> str:
        with SnowflakeFile.open(file_location, mode, require_scoped_url=False) as f:
            return f.read()

    df = session.create_dataframe(
        [[read_mode, temp_file]],
        schema=["read_mode", "temp_file"],
    )
    result = df.select(read_file(col("temp_file"), col("read_mode"))).collect()
    Utils.check_answer(result, [Row(f"{test_msg}")])


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_read_sproc_snowflakefile(read_mode, write_mode, tmp_path, temp_stage, session):
    test_msg, temp_file = Utils.write_test_msg_to_stage(
        write_mode, tmp_path, temp_stage, session
    )

    @udf
    def read_file(file_location: str, mode: str) -> str:
        with SnowflakeFile.open(file_location, mode, require_scoped_url=False) as f:
            return f.read()

    @sproc
    def read_file_sp(session_: Session, file_location: str, mode: str) -> str:
        df = session_.create_dataframe(
            [[read_mode, temp_file]],
            schema=["read_mode", "temp_file"],
        )

        return df.select(read_file(col("temp_file"), col("read_mode"))).collect()

    result = read_file_sp(temp_file, read_mode)
    assert test_msg in result


def test_read_snowurl_snowflakefile(tmp_path, session, temp_stage):
    test_msg, temp_file = Utils.write_test_msg("w", tmp_path)
    snowurl = f"snow://{temp_stage}"
    session.file.put(temp_file, snowurl, auto_compress=False)

    @udf
    def read_file(file_location: str, mode: str) -> str:
        with SnowflakeFile.open(file_location, mode, require_scoped_url=False) as f:
            return f.read()

    df = session.create_dataframe(
        [["r", f"{snowurl}{temp_file}"]],
        schema=["read_mode", "temp_file"],
    )
    result = df.select(read_file(col("temp_file"), col("read_mode"))).collect()
    Utils.check_answer(result, [Row(test_msg)])


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_isatty_snowflakefile(read_mode, write_mode, tmp_path, temp_stage, session):
    _, temp_file = Utils.write_test_msg_to_stage(
        write_mode, tmp_path, temp_stage, session
    )

    @udf
    def get_atty_write(mode: str) -> bool:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.isatty()

    @udf
    def get_atty_read(file_location: str, mode: str) -> bool:
        with SnowflakeFile.open(file_location, mode, require_scoped_url=False) as f:
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
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_readable_snowflakefile(read_mode, write_mode, tmp_path, temp_stage, session):
    _, temp_file = Utils.write_test_msg_to_stage(
        write_mode, tmp_path, temp_stage, session
    )

    @udf
    def is_readable_write(mode: str) -> bool:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.readable()

    @udf
    def is_readable_read(file_location: str, mode: str) -> bool:
        with SnowflakeFile.open(file_location, mode, require_scoped_url=False) as f:
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
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_readline_snowflakefile(read_mode, write_mode, tmp_path, temp_stage, session):
    num_lines = 5
    lines, temp_file = Utils.generate_and_write_lines_to_stage(
        num_lines, write_mode, tmp_path, temp_stage, session
    )

    @udf
    def get_line(file_location: str, mode: str) -> str:
        with SnowflakeFile.open(file_location, mode, require_scoped_url=False) as f:
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

    @udf
    def seek(file_location: str, mode: str, offset: int, whence: int) -> int:
        with SnowflakeFile.open(file_location, mode, require_scoped_url=False) as f:
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
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_seekable_snowflakefile(read_mode, write_mode, tmp_path, temp_stage, session):
    _, temp_file = Utils.write_test_msg_to_stage(
        write_mode, tmp_path, temp_stage, session
    )

    @udf
    def is_seekable_write(mode: str) -> bool:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.seekable()

    @udf
    def is_seekable_read(file_location: str, mode: str) -> bool:
        with SnowflakeFile.open(file_location, mode, require_scoped_url=False) as f:
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
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_tell_snowflakefile(read_mode, write_mode, tmp_path, temp_stage, session):
    _, temp_file = Utils.write_test_msg_to_stage(
        write_mode, tmp_path, temp_stage, session
    )

    @udf
    def try_tell(file_location: str, mode: str, size: int) -> int:
        with SnowflakeFile.open(file_location, mode, require_scoped_url=False) as f:
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
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_writable_snowflakefile(read_mode, write_mode, tmp_path, temp_stage, session):
    _, temp_file = Utils.write_test_msg_to_stage(
        write_mode, tmp_path, temp_stage, session
    )

    @udf
    def is_writable_write(mode: str) -> bool:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.writable()

    @udf
    def is_writable_read(file_location: str, mode: str) -> bool:
        with SnowflakeFile.open(file_location, mode, require_scoped_url=False) as f:
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

    @udf
    def sf_readinto(file_location: str, mode: str, buffer: bytearray) -> int:
        with SnowflakeFile.open(file_location, mode, require_scoped_url=False) as f:
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
