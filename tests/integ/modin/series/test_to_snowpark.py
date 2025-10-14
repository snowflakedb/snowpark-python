#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin._internal.utils import (
    extract_pandas_label_from_snowflake_quoted_identifier,
)
from snowflake.snowpark.types import LongType, StringType
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.fixture
def snow_series_basic(session):
    native_series = native_pd.Series(
        [1, 2, 3, 4],
        index=native_pd.Index(["A", "B", "C", "D"], name="index"),
        name="SER",
    )
    snow_series = pd.Series(native_series)
    return snow_series


@pytest.mark.parametrize("index", [True, False])
@pytest.mark.parametrize("index_label", [None, "test_index"])
@sql_count_checker(query_count=0)
def test_to_snowpark_from_pandas_series(snow_series_basic, index, index_label) -> None:
    snowpark_df = snow_series_basic.to_snowpark(index=index, index_label=index_label)

    start = 0
    if index:
        # verify the index column is included
        expected_index_label = "index"
        if index_label:
            expected_index_label = index_label
        assert (
            extract_pandas_label_from_snowflake_quoted_identifier(
                snowpark_df.schema[start].column_identifier.quoted_name
            )
            == expected_index_label
        )
        assert isinstance(snowpark_df.schema[start].datatype, StringType)
        start += 1

    # verify the data column is included
    assert snowpark_df.schema[start].column_identifier.quoted_name == '"SER"'
    assert isinstance(snowpark_df.schema[start].datatype, LongType)


@sql_count_checker(query_count=0)
def test_to_snowpark_multiindex(test_table_name):
    index = native_pd.MultiIndex.from_arrays(
        [[1, 1, 2, 2], ["red", "blue", "red", "blue"]], names=("number", "color")
    )
    snow_series = pd.Series([1, 2, 5, 3], index=index, name="a")
    snowpark_df = snow_series.to_snowpark()

    assert snowpark_df.schema[0].column_identifier.quoted_name == '"number"'
    assert snowpark_df.schema[1].column_identifier.quoted_name == '"color"'
    assert snowpark_df.schema[2].column_identifier.quoted_name == '"a"'


@sql_count_checker(query_count=0)
def test_to_snowpark_with_no_series_name_raises(snow_series_basic) -> None:
    snow_series_basic.name = None

    message = re.escape(
        "Label None is found in the data columns [None], which is invalid in Snowflake. "
    )
    with pytest.raises(ValueError, match=message):
        snow_series_basic.to_snowpark()


@sql_count_checker(query_count=0)
def test_to_snowpark_with_no_index_name() -> None:
    native_series = native_pd.Series(
        [1, 2, 3, 4], index=native_pd.Index(["A", "B", "C", "D"]), name="SER"
    )
    snow_series = pd.Series(native_series)

    snowpark_df = snow_series.to_snowpark()

    # verify the index column is included
    assert snowpark_df.schema[0].column_identifier.quoted_name == '"index"'
    assert isinstance(snowpark_df.schema[0].datatype, StringType)

    # verify the data column is included
    assert snowpark_df.schema[1].column_identifier.quoted_name == '"SER"'
    assert isinstance(snowpark_df.schema[1].datatype, LongType)
