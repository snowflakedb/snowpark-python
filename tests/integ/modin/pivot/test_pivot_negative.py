#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from functools import reduce

import modin.pandas as pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.pivot.pivot_utils import (
    pivot_table_test_helper_expects_exception,
)
from tests.integ.modin.sql_counter import sql_count_checker


@pytest.mark.parametrize(
    "pivot_table_kwargs",
    [
        # Reference to None not allowed
        {"index": [None], "columns": "E", "values": "F"},
        # Duplicate columns 'A' exist in the dataframe
        {"index": "A", "columns": "C", "values": "E"},
        # Reference a column multiple times that only occurs once in dataframe
        {"index": ["C", "C"], "columns": "F", "values": "E"},
        # Reference a non-existant column in dataframe
        {"index": "Z", "columns": "F", "values": "E"},
    ],
)
@sql_count_checker(query_count=0)
def test_pivot_table_invalid_index_columns_not_supported(
    df_data_with_duplicates, pivot_table_kwargs
):
    pivot_table_test_helper_expects_exception(
        df_data_with_duplicates, pivot_table_kwargs
    )


@pytest.mark.parametrize(
    "pivot_table_kwargs",
    [
        # Reference to None not allowed
        {"index": "E", "columns": [None], "values": "F"},
        # Duplicate columns 'A' exist in the dataframe
        {"index": "C", "columns": "A", "values": "E"},
        # Reference a column multiple times that only occurs once in dataframe
        {"index": None, "columns": ["C", "C"], "values": "E"},
        # Reference a non-existent column in dataframe
        {"index": "C", "columns": "Z", "values": "E"},
    ],
)
@sql_count_checker(query_count=0)
def test_pivot_table_invalid_pivot_labels_not_supported(
    df_data_with_duplicates, pivot_table_kwargs
):
    pivot_table_test_helper_expects_exception(
        df_data_with_duplicates, pivot_table_kwargs
    )


@pytest.mark.parametrize(
    "pivot_table_kwargs",
    [
        # Reference to None not allowed
        {"index": "A", "columns": "B", "values": [None]},
        # Reference to None not allowed
        {"index": "A", "columns": "B", "values": ["E", None]},
        # Reference a non-existent column in dataframe
        {"index": "A", "columns": "B", "values": "Z"},
    ],
)
@sql_count_checker(query_count=0)
def test_pivot_table_invalid_values_columns_not_supported(df_data, pivot_table_kwargs):
    pivot_table_test_helper_expects_exception(
        df_data,
        pivot_table_kwargs,
    )


@sql_count_checker(query_count=0)
def test_pivot_table_no_index_no_column_single_value_raises_error(df_data):
    pivot_table_test_helper_expects_exception(
        df_data,
        {
            "index": None,
            "columns": None,
            "values": "D",
        },
        # we currently throws NotImplementedError if no "index" configuration is provided.
        # TODO (SNOW-959913): Enable support for no "index" configuration
        expect_exception_type=NotImplementedError,
        expect_exception_match="pivot_table with no index configuration is currently not supported",
        assert_exception_equal=False,
    )


@pytest.mark.parametrize(
    "aggfunc",
    [
        None,
        [None],
        ["sum", None],
        {"C": None},
        {"C": [None]},
        {"C": ["max", None]},
    ],
)
@sql_count_checker(query_count=0)
def test_pivot_table_not_supported_aggfunc_with_null(df_data, aggfunc):
    pivot_table_test_helper_expects_exception(
        df_data, {"index": "A", "columns": "C", "values": "E", "aggfunc": aggfunc}
    )


@sql_count_checker(query_count=0)
def test_pivot_table_not_supported_aggfunc_with_empty_list(df_data):
    pivot_table_test_helper_expects_exception(
        df_data,
        {"index": "A", "columns": "C", "values": "E", "aggfunc": []},
        expect_exception_type=ValueError,
        expect_exception_match="Expected at least one aggregation function",
        assert_exception_equal=False,
    )


@sql_count_checker(query_count=0)
def test_pivot_table_not_implemented_or_supported(df_data):
    snow_df = pd.DataFrame(df_data)

    with pytest.raises(NotImplementedError, match="Not implemented observed"):
        snow_df.pivot_table(index="A", columns="C", values="E", observed=True)

    with pytest.raises(NotImplementedError, match="Not implemented not sorted"):
        snow_df.pivot_table(index="A", columns="C", values="E", sort=False)

    class Foo:
        pass

    class Baz:
        pass

    foo = Foo()
    baz = Baz()
    snow_df2 = pd.DataFrame(
        {
            foo: ["abc", "def"],
            "A": ["a", "b"],
            "B": ["x", "y"],
            "C": [2, 3],
            baz: [3, 4],
        },
    )
    foo = Foo()
    baz = Baz()

    with pytest.raises(NotImplementedError, match="Not implemented non-string"):
        snow_df2.pivot_table(index=[foo], columns="B", values="C")

    with pytest.raises(NotImplementedError, match="Not implemented non-string"):
        snow_df2.pivot_table(index="A", columns=[foo], values="E")

    with pytest.raises(NotImplementedError, match="Not implemented non-string"):
        snow_df2.pivot_table(index="A", columns="B", values=[baz])

    def dummy_aggr_func(series):
        return reduce(lambda x, y: x + y, series)

    with pytest.raises(
        NotImplementedError, match=r"Not implemented callable aggregation function .*"
    ):
        snow_df.pivot_table(index="A", columns="C", values="E", aggfunc=dummy_aggr_func)

    with pytest.raises(KeyError, match="foo"):
        snow_df.pivot_table(index="A", columns="C", values="E", aggfunc="foo")

    with pytest.raises(
        KeyError,
        match="median",
    ):
        snow_df.pivot_table(index="A", columns="C", values="D", aggfunc="median")

    with pytest.raises(
        NotImplementedError,
        match="pivot_table with no index configuration is currently not supported",
    ):
        snow_df.pivot_table(index=None, columns="C", values="D")

    with pytest.raises(
        NotImplementedError,
        match="pivot_table with no index configuration is currently not supported",
    ):
        snow_df.pivot_table(index=None, columns=None, values="D")
