#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from functools import partial

import pytest
import re
from snowflake.snowpark.files import _DEFER_IMPLEMENTATION_ERR_MSG, SnowflakeFile


def test_create_snowflakefile():
    with SnowflakeFile.open("test_file_location") as snowflake_file:
        assert snowflake_file._file_location == "test_file_location"
        assert snowflake_file._mode == "r"

    with SnowflakeFile.open("test_file_location", mode="rb") as snowflake_file:
        assert snowflake_file._file_location == "test_file_location"
        assert snowflake_file._mode == "rb"

    with pytest.raises(
        ValueError,
        match="Invalid mode 'rw' for SnowflakeFile.open. Supported modes are 'r' and 'rb'.",
    ):
        snowflake_file = SnowflakeFile.open("test_file_location", mode="rw")


def test_write_snowflakefile():
    with SnowflakeFile.open_new_result() as snowflake_file:
        assert snowflake_file._file_location is not None
        assert snowflake_file._mode == "w"

    with SnowflakeFile.open_new_result("w") as snowflake_file:
        assert snowflake_file._file_location is not None
        assert snowflake_file._mode == "w"

    with SnowflakeFile.open_new_result("wb") as snowflake_file:
        assert snowflake_file._file_location is not None
        assert snowflake_file._mode == "wb"

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Invalid mode 'w+' for SnowflakeFile.open_new_result. Supported modes are 'w' and 'wb'."
        ),
    ):
        snowflake_file = SnowflakeFile.open_new_result("w+")


def test_snowflake_file_attribute():
    with SnowflakeFile.open("test_file_location") as snowflake_file:
        assert snowflake_file.buffer is None
        assert snowflake_file.encoding is None
        assert snowflake_file.errors is None


def test_snowflake_file_method():
    with SnowflakeFile.open("test_file_location") as snowflake_file:
        with pytest.raises(NotImplementedError):
            snowflake_file.readable()


def test_operation_methods():
    with SnowflakeFile.open("test_file_location") as snowflake_file:
        methods = [
            snowflake_file.detach,
            snowflake_file.fileno,
            snowflake_file.flush,
            snowflake_file.isatty,
            snowflake_file.read,
            snowflake_file.read1,
            snowflake_file.readable,
            snowflake_file.readall,
            partial(snowflake_file.readinto, b"a"),
            partial(snowflake_file.readinto1, b"a"),
            snowflake_file.readline,
            snowflake_file.readlines,
            snowflake_file.seekable,
            snowflake_file.tell,
            snowflake_file.truncate,
            snowflake_file.writable,
            partial(snowflake_file.write, b"a"),
            partial(snowflake_file.writelines, ["a line"]),
        ]
        for method in methods:
            with pytest.raises(
                NotImplementedError, match=_DEFER_IMPLEMENTATION_ERR_MSG
            ):
                method()
