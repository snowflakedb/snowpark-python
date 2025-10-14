#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    VALID_PANDAS_LABELS,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    to_snowflake_counter,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.fixture(autouse=True)
def use_starting_backend_and_parquet_threshold(starting_backend_and_parquet_threshold):
    return starting_backend_and_parquet_threshold


@pytest.fixture
def snow_series():
    return pd.Series(
        [1, 2, 3], name="a", index=native_pd.Index(["a", "b", "c"], name="index")
    )


@pytest.fixture
def unnamed_snow_series_with_named_index():
    return pd.Series([1, 2, 3], index=native_pd.Index(["a", "b", "c"], name="index"))


@pytest.fixture
def unnamed_snow_series_with_unnamed_index():
    return pd.Series([1, 2, 3], index=native_pd.Index(["a", "b", "c"]))


def _verify_columns(table_name: str, expected: list[str]) -> None:
    actual = pd.read_snowflake(table_name).columns
    assert actual.tolist() == expected


def _verify_num_rows(session, table_name: str, expected: int) -> None:
    actual = session.table(table_name).count()
    assert actual == expected


@pytest.mark.parametrize("index", [True, False])
@pytest.mark.parametrize("index_labels", [None, ["my_index"]])
def test_to_snowflake_index(test_table_name, snow_series, index, index_labels):
    if_exists = "replace"
    with to_snowflake_counter(dataset=snow_series, if_exists=if_exists):
        snow_series.to_snowflake(
            test_table_name, if_exists=if_exists, index=index, index_label=index_labels
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


def test_to_snowflake_index_label_none(test_table_name):
    snow_series = pd.Series([1, 2, 3], name="a", index=native_pd.Index([4, 5, 6]))
    if_exists = "replace"
    with to_snowflake_counter(dataset=snow_series, if_exists=if_exists):
        snow_series.to_snowflake(test_table_name, if_exists=if_exists, index=True)
    _verify_columns(test_table_name, ["index", "a"])


def test_to_snowflake_multiindex(test_table_name, snow_series):
    index = native_pd.MultiIndex.from_arrays(
        [[1, 1, 2, 2], ["red", "blue", "red", "blue"]], names=("number", "color")
    )
    snow_series = pd.Series([1, 2, 5, 3], index=index, name="a")
    if_exists = "replace"
    with to_snowflake_counter(dataset=snow_series, if_exists=if_exists):
        snow_series.to_snowflake(test_table_name, if_exists=if_exists, index=True)
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
    if_exists = "fail"
    with to_snowflake_counter(dataset=snow_series, if_exists=if_exists):
        snow_series.to_snowflake(test_table_name, if_exists=if_exists, index=False)
    _verify_columns(test_table_name, ["a"])

    # Verify existing table is replaced with new data
    if_exists = "replace"
    snow_series = pd.Series([4, 5, 6], name="b")
    with to_snowflake_counter(dataset=snow_series, if_exists=if_exists):
        snow_series.to_snowflake(test_table_name, if_exists=if_exists, index=False)
    _verify_columns(test_table_name, ["b"])

    # Verify data is appended to existing table
    _verify_num_rows(session, test_table_name, 3)
    if_exists = "append"
    with to_snowflake_counter(dataset=snow_series, if_exists=if_exists):
        snow_series.to_snowflake(test_table_name, if_exists=if_exists, index=False)
    _verify_columns(test_table_name, ["b"])
    _verify_num_rows(session, test_table_name, 6)


def test_to_snowflake_if_exists_negative(session, test_table_name, snow_series):
    # Create a table.
    if_exists = "fail"
    with to_snowflake_counter(dataset=snow_series, if_exists=if_exists):
        snow_series.to_snowflake(test_table_name, if_exists=if_exists, index=False)

    # Verify attempt to write to existing table fails
    with pytest.raises(
        ValueError, match=f"Table '{test_table_name}' already exists"
    ), SqlCounter(query_count=1):
        snow_series.to_snowflake(test_table_name, if_exists="fail", index=False)

    # Verify by default attempt to write to existing table fails
    with pytest.raises(
        ValueError, match=f"Table '{test_table_name}' already exists"
    ), SqlCounter(query_count=1):
        snow_series.to_snowflake(test_table_name, index=False)

    # Verify invalid 'if_exists' value.
    with pytest.raises(
        ValueError, match="'abc' is not valid for if_exists"
    ), SqlCounter(query_count=0):
        snow_series.to_snowflake(test_table_name, if_exists="abc")


@pytest.mark.parametrize("index_label", VALID_PANDAS_LABELS)
def test_to_snowflake_index_column_labels(index_label, test_table_name, snow_series):
    if_exists = "replace"
    with to_snowflake_counter(dataset=snow_series, if_exists=if_exists):
        snow_series.to_snowflake(
            test_table_name, if_exists=if_exists, index=True, index_label=index_label
        )
    _verify_columns(test_table_name, [str(index_label), "a"])


@pytest.mark.parametrize("col_label", VALID_PANDAS_LABELS)
def test_to_snowflake_data_column_labels(col_label, test_table_name, snow_series):
    if isinstance(col_label, tuple):
        pytest.xfail(reason="https://github.com/modin-project/modin/issues/7689")
    snow_series = snow_series.rename(col_label)
    if_exists = "replace"
    with to_snowflake_counter(dataset=snow_series, if_exists=if_exists):
        snow_series.to_snowflake(test_table_name, if_exists=if_exists, index=False)
    _verify_columns(test_table_name, [str(col_label)])


@pytest.mark.skip(reason="SNOW-2389980")
@pytest.mark.parametrize("index", [True, False])
def test_unnamed_series_with_unnamed_index(
    test_table_name, unnamed_snow_series_with_unnamed_index, index
):
    if_exists = "replace"
    with to_snowflake_counter(
        dataset=unnamed_snow_series_with_unnamed_index, if_exists=if_exists
    ):
        unnamed_snow_series_with_unnamed_index.to_snowflake(
            test_table_name, if_exists=if_exists, index=index
        )
    _verify_columns(test_table_name, ["index"])


@pytest.mark.skip(reason="SNOW-2389980")
@pytest.mark.parametrize("index", [True, False])
def test_unnamed_series_with_named_index(
    test_table_name, unnamed_snow_series_with_named_index, index
):
    if_exists = "replace"
    with to_snowflake_counter(
        dataset=unnamed_snow_series_with_named_index, if_exists=if_exists
    ):
        unnamed_snow_series_with_named_index.to_snowflake(
            test_table_name, if_exists=if_exists, index=index
        )
    _verify_columns(test_table_name, ["index"])


class TestTableName:
    def test_lower_case_unquoted(self, lower_case_table_name_unquoted):
        native_series = native_pd.Series([1], name="a")
        series = pd.Series(native_series)
        if_exists = "replace"
        with to_snowflake_counter(dataset=series, if_exists=if_exists):
            series.to_snowflake(
                lower_case_table_name_unquoted, if_exists=if_exists, index=False
            )
        written = pd.read_snowflake(lower_case_table_name_unquoted)
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            written, native_series.to_frame()
        )

    def test_lower_case_quoted(self, lower_case_table_name_quoted):
        native_series = native_pd.Series([1], name="a")
        series = pd.Series(native_series)
        if_exists = "replace"
        with to_snowflake_counter(dataset=series, if_exists=if_exists):
            series.to_snowflake(
                lower_case_table_name_quoted, if_exists=if_exists, index=False
            )
        written = pd.read_snowflake(lower_case_table_name_quoted)
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            written, native_series.to_frame()
        )

    def test_upper_case_quoted(self, upper_case_table_name_quoted):
        native_series = native_pd.Series([1], name="a")
        series = pd.Series(native_series)
        if_exists = "replace"
        with to_snowflake_counter(dataset=series, if_exists=if_exists):
            series.to_snowflake(
                upper_case_table_name_quoted, if_exists=if_exists, index=False
            )
        written = pd.read_snowflake(upper_case_table_name_quoted)
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            written, native_series.to_frame()
        )

    def test_upper_case_with_space_quoted(
        self, upper_case_table_name_with_space_quoted
    ):
        native_series = native_pd.Series([1], name="a")
        series = pd.Series(native_series)
        if_exists = "replace"
        with to_snowflake_counter(dataset=series, if_exists=if_exists):
            series.to_snowflake(
                upper_case_table_name_with_space_quoted,
                if_exists=if_exists,
                index=False,
            )
        written = pd.read_snowflake(upper_case_table_name_with_space_quoted)
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            written, native_series.to_frame()
        )

    def test_special_chars_unquoted(self, valid_unquoted_identifier_table_name):
        native_series = native_pd.Series([1], name="a")
        series = pd.Series(native_series)
        if_exists = "replace"
        with to_snowflake_counter(dataset=series, if_exists=if_exists):
            series.to_snowflake(
                valid_unquoted_identifier_table_name, if_exists=if_exists, index=False
            )
        written = pd.read_snowflake(valid_unquoted_identifier_table_name)
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            written, native_series.to_frame()
        )
