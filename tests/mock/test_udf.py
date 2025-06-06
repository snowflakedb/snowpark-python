#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import sys
import pytest
import io
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import call_udf, col, lit, udf
from snowflake.snowpark.mock._udf import MockUDFRegistration
from snowflake.snowpark.mock.exceptions import SnowparkLocalTestingException
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import IntegerType
from snowflake.snowpark._internal.utils import generate_random_alphanumeric
from snowflake.snowpark.row import Row
from snowflake.snowpark.files import SnowflakeFile
from tests.utils import Utils


def test_udf_cleanup_on_err(session):
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    test_file = os.path.join(cur_dir, "files", "udf_file.py")

    df = session.create_dataframe([[3, 4], [5, 6]]).to_df("a", "b")
    sys_path_copy = list(sys.path)

    mod5_udf = session.udf.register_from_file(
        test_file,
        "raise_err",
        return_type=IntegerType(),
        input_types=[IntegerType()],
        immutable=True,
    )
    assert isinstance(mod5_udf.func, tuple)
    with pytest.raises(SnowparkLocalTestingException):
        df.select(mod5_udf("a"), mod5_udf("b")).collect()
    assert (
        sys_path_copy == sys.path
    )  # assert sys.path is cleaned up after UDF exits on exception


def test_registering_udf_with_qualified_identifier(session):
    custom_schema = "test_identifier_schema"

    def add_fn(x: int, y: int) -> int:
        return x + y

    session.udf.register(add_fn, name=f"{custom_schema}.add")

    df = session.create_dataframe([[3, 4]], schema=["num1", "num2"])
    assert (
        df.select(call_udf(f"{custom_schema}.add", col("num1"), col("num2"))).collect()[
            0
        ][0]
        == 7
    )

    session.use_schema(custom_schema)
    assert df.select(call_udf("add", col("num1"), col("num2"))).collect()[0][0] == 7

    session.use_database("test_identifier_database")
    with pytest.raises(SnowparkLocalTestingException):
        assert (
            df.select(
                call_udf(f"{custom_schema}.add", col("num1"), col("num2"))
            ).collect()[0][0]
            == 7
        )


def test_registering_sproc_with_qualified_identifier(session):
    custom_schema = "test_identifier_schema"

    def increment_by_one_fn(session: Session, x: int) -> int:
        df = session.create_dataframe([[]]).select((lit(1) + lit(x)).as_("RESULT"))
        return df.collect()[0]["RESULT"]

    session.sproc.register(
        increment_by_one_fn, name=f"{custom_schema}.increment_by_one"
    )
    assert session.call(f"{custom_schema}.increment_by_one", 5) == 6

    session.use_schema(custom_schema)
    assert session.call("increment_by_one", 5) == 6

    session.use_database("test_identifier_database")
    with pytest.raises(SnowparkSQLException):
        assert session.call("increment_by_one", 5) == 6


def test_get_udf_negative(session):
    reg = MockUDFRegistration(session)
    with pytest.raises(SnowparkLocalTestingException):
        reg.get_udf("does_not_exist")


def test_get_udf_imports_negative(session):
    reg = MockUDFRegistration(session)
    assert reg.get_udf_imports("does_not_exist") == set()


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_read_snowflakefile_local(read_mode, write_mode, temp_file, session):
    test_msg = generate_random_alphanumeric()
    if write_mode == "wb":
        test_msg = test_msg.encode()

    with open(temp_file, write_mode) as f:
        f.write(test_msg)

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
def test_isatty_snowflakefile_local(read_mode, write_mode, temp_file, session):
    @udf
    def get_atty(mode: str) -> bool:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.isatty()

    @udf
    def get_atty2(file_location: str, mode: str) -> bool:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.isatty()

    df = session.create_dataframe(
        [[read_mode, write_mode, temp_file]],
        schema=["read_mode", "write_mode", "temp_file"],
    )
    result = df.select(get_atty(col("write_mode"))).collect()
    Utils.check_answer(result, [Row(False)])

    result = df.select(get_atty2(col("temp_file"), col("read_mode"))).collect()
    Utils.check_answer(result, [Row(False)])


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_readable_snowflakefile_local(read_mode, write_mode, temp_file, session):
    @udf
    def is_readable(mode: str) -> bool:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.readable()

    @udf
    def is_readable2(file_location: str, mode: str) -> bool:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readable()

    df = session.create_dataframe(
        [[read_mode, write_mode, temp_file]],
        schema=["read_mode", "write_mode", "temp_file"],
    )
    result = df.select(is_readable(col("write_mode"))).collect()
    Utils.check_answer(result, [Row(False)])

    result = df.select(is_readable2(col("temp_file"), col("read_mode"))).collect()
    Utils.check_answer(result, [Row(True)])


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_readline_snowflakefile_local(read_mode, write_mode, temp_file, session):
    num_lines = 5

    lines = [f"{generate_random_alphanumeric()}\n" for _ in range(num_lines)]
    if write_mode == "wb":
        lines = [line.encode() for line in lines]

    with open(temp_file, write_mode) as f:
        for line in lines:
            f.write(line)

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
    test_msg = generate_random_alphanumeric()
    if write_mode == "wb":
        test_msg = test_msg.encode()
    with open(temp_file, mode=write_mode) as f:
        f.write(test_msg)

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
    def is_seekable(mode: str) -> bool:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.seekable()

    @udf
    def is_seekable2(file_location: str, mode: str) -> bool:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.seekable()

    df = session.create_dataframe(
        [[read_mode, write_mode, temp_file]],
        schema=["read_mode", "write_mode", "temp_file"],
    )
    result = df.select(is_seekable(col("write_mode"))).collect()
    Utils.check_answer(result, [Row(False)])

    result = df.select(is_seekable2(col("temp_file"), col("read_mode"))).collect()
    Utils.check_answer(result, [Row(True)])


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_tell_snowflakefile_local(read_mode, write_mode, temp_file, session):
    test_msg = generate_random_alphanumeric()
    if write_mode == "wb":
        test_msg = test_msg.encode()

    with open(temp_file, mode=write_mode) as f:
        f.write(test_msg)

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
    def is_writable(mode: str) -> bool:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.writable()

    @udf
    def is_writable2(file_location: str, mode: str) -> bool:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.writable()

    df = session.create_dataframe(
        [[read_mode, write_mode, temp_file]],
        schema=["read_mode", "write_mode", "temp_file"],
    )
    result = df.select(is_writable(col("write_mode"))).collect()
    Utils.check_answer(result, [Row(True)])

    result = df.select(is_writable2(col("temp_file"), col("read_mode"))).collect()
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
    encoded_test_msg = test_msg.encode()

    with open(temp_file, write_mode) as f:
        if write_mode == "wb":
            f.write(encoded_test_msg)
        else:
            f.write(test_msg)

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
