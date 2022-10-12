#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import pytest

from snowflake.snowpark.files import SnowflakeFile


def test_create_snowflakefile():
    with SnowflakeFile.open("test_file_location") as snowflake_file:
        assert snowflake_file._file_location == "test_file_location"
        assert snowflake_file._mode == "r"


def test_snowflake_file_attribute():
    with SnowflakeFile.open("test_file_location") as snowflake_file:
        assert snowflake_file.buffer is None
        assert snowflake_file.encoding is None
        assert snowflake_file.errors is None


def test_snowflake_file_method():
    with SnowflakeFile.open("test_file_location") as snowflake_file:
        with pytest.raises(NotImplementedError):
            snowflake_file.readable()
