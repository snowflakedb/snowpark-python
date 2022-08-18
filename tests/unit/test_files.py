#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import pytest

from snowflake.snowpark.files import SnowflakeFile


def test_create_snowflakefile():
    snowflake_file = SnowflakeFile.open("test_file_location")

    assert snowflake_file._file_location == "test_file_location"
    assert snowflake_file._mode == "r"

    snowflake_file.close()


def test_snowflake_file_attribute():
    snowflake_file = SnowflakeFile.open("test_file_location")

    assert snowflake_file.buffer is None
    assert snowflake_file.closed is None
    assert snowflake_file.encoding is None
    assert snowflake_file.errors is None

    snowflake_file.close()


def test_snowflake_file_method():
    snowflake_file = SnowflakeFile.open("test_file_location")

    with pytest.raises(NotImplementedError):
        snowflake_file.close()
