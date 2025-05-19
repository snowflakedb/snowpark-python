#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import io
import logging
import os

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark._internal.utils import generate_random_alphanumeric
from tests.integ.modin.utils import assert_frame_equal
from tests.integ.utils.sql_counter import sql_count_checker
from tests.utils import TestFiles, Utils

tmp_stage_name1 = Utils.random_stage_name()
test_file_parquet = "test_file_with_special_characters.parquet"


@pytest.fixture(scope="module", autouse=True)
def setup(session, resources_path):
    test_files = TestFiles(resources_path)
    Utils.create_stage(session, tmp_stage_name1, is_temporary=True)
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name1,
        test_files.test_file_with_special_characters_parquet,
        compress=False,
    )

    yield
    # tear down the resources after yield (pytest fixture feature)
    # https://docs.pytest.org/en/6.2.x/fixture.html#yield-fixtures-recommended
    session.sql(f"DROP STAGE IF EXISTS {tmp_stage_name1}").collect()


@sql_count_checker(
    query_count=10,
    high_count_expected=True,
    high_count_reason="Expected dtypes with parquet high query count",
)
def test_read_parquet_all_dtypes(resources_path):
    test_files = TestFiles(resources_path)

    native_df = native_pd.read_parquet(test_files.test_file_all_data_types_parquet)
    snow_df = pd.read_parquet(test_files.test_file_all_data_types_parquet)

    # native pandas fails to recognize that column 'N' is of float type.
    native_df[["N"]] = native_df[["N"]].astype("float64")
    assert_frame_equal(snow_df, native_df, check_index_type=False, check_dtype=False)


@sql_count_checker(query_count=9)
def test_read_parquet_stage_file(resources_path):
    got = pd.read_parquet(f"@{tmp_stage_name1}/{test_file_parquet}")
    test_files = TestFiles(resources_path)
    expected = native_pd.read_parquet(
        test_files.test_file_with_special_characters_parquet
    )

    assert_frame_equal(got, expected, check_dtype=False)


@sql_count_checker(
    query_count=10,
    high_count_expected=True,
    high_count_reason="Expected special chars parquet high query count",
)
def test_read_parquet_special_chars_in_column_names(resources_path):
    test_files = TestFiles(resources_path)
    native_df = native_pd.read_parquet(
        test_files.test_file_with_special_characters_parquet
    )
    snow_df = pd.read_parquet(test_files.test_file_with_special_characters_parquet)

    assert_frame_equal(native_df, snow_df, check_dtype=False)


@pytest.mark.parametrize(
    "columns",
    [
        [
            "Ema!l",
            "Address",
            "Av@t@r",
            "Avg. $ession Length",
            "T!me on App",
            "T!me on Website",
            "Length of Membership",
            "Ye@rly Amount $pent",
        ],
        ["Ema!l"],
        ["Avg. $ession Length", "T!me on App"],
        ["T!me on Website", "Av@t@r", "Ye@rly Amount $pent"],
        [],
    ],
)
@sql_count_checker(query_count=9)
def test_read_parquet_columns(resources_path, columns):

    got = pd.read_parquet(f"@{tmp_stage_name1}/{test_file_parquet}", columns=columns)

    test_files = TestFiles(resources_path)

    expected = native_pd.read_parquet(
        test_files.test_file_with_special_characters_parquet, columns=columns
    )

    assert_frame_equal(got, expected, check_dtype=False, check_index_type=False)


@pytest.mark.parametrize(
    "columns",
    [
        "Ema!l",
        ("Ema!l", "Av@t@r"),
        0,
        np.float64(123),
        [1, "Length of Membership"],
        [1, [2, 3], 4],
        [12.33, np.float64(13.2333), np.double(2.5)],
        [datetime.time(1, 2, 3)],
        [datetime.date(2021, 1, 9), datetime.datetime(2023, 1, 1, 1, 2, 3)],
    ],
)
@sql_count_checker(query_count=0)
def test_read_parquet_columns_invalid_types_negative(resources_path, columns):

    with pytest.raises(
        ValueError, match="'columns' must either be list of all strings."
    ):
        pd.read_parquet(f"@{tmp_stage_name1}/{test_file_parquet}", columns=columns)


@pytest.mark.parametrize(
    "columns",
    [["Ema!l", "non_existent_col"], ["non_existent_col_in_list"]],
)
@sql_count_checker(query_count=8)
def test_read_parquet_columns_non_existent_column_negative(resources_path, columns):

    with pytest.raises(
        ValueError,
        match="'usecols' do not match columns, columns expected but not found",
    ):
        pd.read_parquet(f"@{tmp_stage_name1}/{test_file_parquet}", columns=columns)


@sql_count_checker(query_count=0)
def test_read_parquet_unimplemented_parameter_negative():
    with pytest.raises(
        NotImplementedError, match="use_nullable_dtypes is not implemented."
    ):
        pd.read_parquet("file.parquet", use_nullable_dtypes=True)


@pytest.mark.parametrize(
    "parameter, argument",
    [
        ("engine", "pyarrow"),
        ("storage_options", "random_option"),
        ("filesystem", "test invalid link"),
        ("filters", [("foo", "==", "1")]),
    ],
)
@sql_count_checker(
    query_count=10,
    high_count_expected=True,
    high_count_reason="Expected high query count multiple reads",
)
def test_read_parquet_warning(caplog, parameter, argument):
    # generate a random temp name so these tests can be run in parallel
    temp_file_name = (
        f"test_read_parquet_warning_{generate_random_alphanumeric(4)}.parquet"
    )

    df = native_pd.DataFrame({"foo": range(5), "bar": range(5, 10)})

    df.to_parquet(temp_file_name)

    warning_msg = f"The argument `{parameter}` of `pd.read_parquet` has been ignored by Snowpark pandas API"

    caplog.clear()

    with caplog.at_level(logging.WARNING):
        snow_df = pd.read_parquet(temp_file_name, **{parameter: argument})

    assert warning_msg in caplog.text

    assert_frame_equal(df, snow_df, check_dtype=False)

    os.remove(temp_file_name)


@sql_count_checker(query_count=0)
def test_read_parquet_filepath_negative():
    df = native_pd.DataFrame({"foo": range(5), "bar": range(5, 10)})

    bytes_data = df.to_parquet()
    buffer = io.BytesIO(bytes_data)

    with pytest.raises(
        NotImplementedError,
        match="'path' must be a path to a file or folder stored locally or on a Snowflake stage.",
    ):
        pd.read_parquet(buffer)
