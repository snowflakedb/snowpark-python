#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import VALID_PANDAS_LABELS
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.fixture(scope="module")
def snow_series():
    return pd.Series([1, 2, 3], name="a", index=pd.Index(["a", "b", "c"], name="index"))


def _verify_columns(table_name: str, expected: list[str]) -> None:
    actual = pd.read_snowflake(table_name).columns
    assert actual.tolist() == expected


def _verify_num_rows(session, table_name: str, expected: int) -> None:
    actual = session.table(table_name).count()
    assert actual == expected


@pytest.mark.parametrize("index", [True, False])
@pytest.mark.parametrize("index_labels", [None, ["my_index"]])
@sql_count_checker(query_count=2, join_count=1)
def test_to_snowflake_index(test_table_name, snow_series, index, index_labels):
    snow_series.to_snowflake(
        test_table_name, if_exists="replace", index=index, index_label=index_labels
    )

    expected_columns = []
    if index:
        # if index is retained in the result, add it as the first expected column
        expected_index = ["index"]
        if index_labels:
            expected_index = index_labels
        expected_columns = expected_columns + expected_index
    # add the expected data columns
    expected_columns = expected_columns + ["a"]
    _verify_columns(test_table_name, expected_columns)


@sql_count_checker(query_count=0)
def test_to_snowflake_index_name_conflict_negative(test_table_name, snow_series):
    # Verify error is thrown if 'index_label' conflicts with existing column name
    message = re.escape(
        "Duplicated labels ['a'] found in index columns ['a'] and data columns ['a']. "
        "Snowflake does not allow duplicated identifiers"
    )
    with pytest.raises(ValueError, match=message):
        snow_series.to_snowflake(test_table_name, if_exists="replace", index_label="a")


@sql_count_checker(query_count=2)
def test_to_snowflake_index_label_none(test_table_name):
    snow_series = pd.Series([1, 2, 3], name="a", index=native_pd.Index([4, 5, 6]))
    snow_series.to_snowflake(test_table_name, if_exists="replace", index=True)
    _verify_columns(test_table_name, ["index", "a"])


@sql_count_checker(query_count=2)
def test_to_snowflake_multiindex(test_table_name, snow_series):
    index = native_pd.MultiIndex.from_arrays(
        [[1, 1, 2, 2], ["red", "blue", "red", "blue"]], names=("number", "color")
    )
    snow_series = pd.Series([1, 2, 5, 3], index=index, name="a")
    snow_series.to_snowflake(test_table_name, if_exists="replace", index=True)
    _verify_columns(test_table_name, ["number", "color", "a"])


@sql_count_checker(query_count=0)
def test_to_snowflake_index_label_invalid_length_negative(test_table_name, snow_series):
    with pytest.raises(
        ValueError, match="Length of 'index_label' should match number of levels"
    ):
        snow_series.to_snowflake(
            test_table_name, if_exists="replace", index=True, index_label=["a", "b"]
        )


def test_to_snowflake_if_exists(session, test_table_name, snow_series):
    # Verify new table is created
    with SqlCounter(query_count=3):
        snow_series.to_snowflake(test_table_name, if_exists="fail", index=False)
        _verify_columns(test_table_name, ["a"])

    # Verify existing table is replaced with new data
    with SqlCounter(query_count=2):
        snow_series = pd.Series([4, 5, 6], name="b")
        snow_series.to_snowflake(test_table_name, if_exists="replace", index=False)
        _verify_columns(test_table_name, ["b"])

    # Verify data is appended to existing table
    with SqlCounter(query_count=5):
        _verify_num_rows(session, test_table_name, 3)
        snow_series.to_snowflake(test_table_name, if_exists="append", index=False)
        _verify_columns(test_table_name, ["b"])
        _verify_num_rows(session, test_table_name, 6)


@sql_count_checker(query_count=4, join_count=1)
def test_to_snowflake_if_exists_negative(session, test_table_name, snow_series):
    # Create a table.
    snow_series.to_snowflake(test_table_name, if_exists="fail", index=False)

    # Verify attempt to write to existing table fails
    with pytest.raises(ValueError, match=f"Table '{test_table_name}' already exists"):
        snow_series.to_snowflake(test_table_name, if_exists="fail", index=False)

    # Verify by default attempt to write to existing table fails
    with pytest.raises(ValueError, match=f"Table '{test_table_name}' already exists"):
        snow_series.to_snowflake(test_table_name, index=False)

    # Verify invalid 'if_exists' value.
    with pytest.raises(ValueError, match="'abc' is not valid for if_exists"):
        snow_series.to_snowflake(test_table_name, if_exists="abc")


@pytest.mark.parametrize("index_label", VALID_PANDAS_LABELS)
@sql_count_checker(query_count=2, join_count=1)
def test_to_snowflake_index_column_labels(index_label, test_table_name, snow_series):
    snow_series.to_snowflake(
        test_table_name, if_exists="replace", index=True, index_label=index_label
    )
    _verify_columns(test_table_name, [str(index_label), "a"])


@pytest.mark.parametrize("col_label", VALID_PANDAS_LABELS)
@sql_count_checker(query_count=2, join_count=1)
def test_to_snowflake_data_column_labels(col_label, test_table_name, snow_series):
    snow_series = snow_series.rename(col_label)
    snow_series.to_snowflake(test_table_name, if_exists="replace", index=False)
    _verify_columns(test_table_name, [str(col_label)])
