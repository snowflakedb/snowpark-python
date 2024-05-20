#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#


import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.sql_counter import SqlCounter
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    create_test_dfs,
)


def assert_snowpark_pandas_dataframes_are_equal(snow_df1, snow_df2):
    assert snow_df1.empty is snow_df2.empty
    assert snow_df1.columns.equals(snow_df2.columns)
    assert snow_df1.index.equals(snow_df2.index)
    if not snow_df1.empty:
        with SqlCounter(query_count=2):
            assert np.all(snow_df1.values == snow_df2.values)


def assert_empty_snowpark_pandas_equals_to_pandas(snow_df, native_df):
    native_snow_df = snow_df.to_pandas()
    assert native_df.empty and native_snow_df.empty
    # When columns or index are empty, we have an empty Index, but pandas has a RangeIndex with no elements.
    if len(native_snow_df.columns) == 0:
        assert len(native_df.columns) == 0
    else:
        assert native_snow_df.columns.equals(native_df.columns)
    if len(native_snow_df.index) == 0:
        assert len(native_df.index) == 0
    else:
        assert native_snow_df.index.equals(native_df.index)


@pytest.fixture(scope="function")
def simple_test_data():
    return {
        "col0": ["foo", "bar", "baz", "foo", "bar", "baz", "foo", "bar", "baz"],
        "col1": ["abc", "def", "ghi", "ghi", "abc", "def", "def", "ghi", "abc"],
        "col2": list(range(9)),
    }


def cache_and_return_df(snow_df, inplace):
    """
    Helper method to cache and return a reference to cached DataFrame depending on inplace.

    Notes
    -----
    If inplace=True, the returned df is a reference to the inputted df.
    """
    if not inplace:
        cached_snow_df = snow_df.cache_result()
    else:
        # If inplace=True, there is no return, so we set `cached_snow_df` equal to `snow_df`.
        cached_snow_df = snow_df
        cached_snow_df.cache_result(inplace=inplace)
    return cached_snow_df


def perform_chained_operations(df, module):
    df = df.reset_index(drop=True)
    return module.concat([df] * 10)


@pytest.mark.parametrize("inplace", [True, False])
def test_cache_result_empty_dataframe(inplace):
    native_df = native_pd.DataFrame()
    snow_df = pd.DataFrame()
    snow_df_copy = snow_df.copy(deep=True)
    with SqlCounter(query_count=1):
        cached_snow_df = cache_and_return_df(snow_df, inplace)
    with SqlCounter(query_count=2):
        assert_snowpark_pandas_dataframes_are_equal(snow_df_copy, cached_snow_df)
    with SqlCounter(query_count=1):
        assert_empty_snowpark_pandas_equals_to_pandas(cached_snow_df, native_df)

    native_df = native_pd.DataFrame(columns=["A", "B", "C"])
    snow_df = pd.DataFrame(native_df)
    snow_df_copy = snow_df.copy(deep=True)
    with SqlCounter(query_count=1):
        cached_snow_df = cache_and_return_df(snow_df, inplace)
    with SqlCounter(query_count=4):
        assert_snowpark_pandas_dataframes_are_equal(snow_df_copy, cached_snow_df)
    with SqlCounter(query_count=1):
        assert_empty_snowpark_pandas_equals_to_pandas(cached_snow_df, native_df)

    native_df = native_pd.DataFrame(index=["A", "B", "C"])
    snow_df = pd.DataFrame(native_df)
    snow_df_copy = snow_df.copy(deep=True)
    with SqlCounter(query_count=1):
        cached_snow_df = cache_and_return_df(snow_df, inplace)
    with SqlCounter(query_count=2):
        assert_snowpark_pandas_dataframes_are_equal(snow_df_copy, cached_snow_df)
    with SqlCounter(query_count=1):
        assert_empty_snowpark_pandas_equals_to_pandas(cached_snow_df, native_df)

    native_df = native_pd.DataFrame(columns=["A", "B", "C"], index=[0, 1, 2])
    snow_df = pd.DataFrame(native_df)
    snow_df_copy = snow_df.copy(deep=True)
    with SqlCounter(query_count=1):
        cached_snow_df = cache_and_return_df(snow_df, inplace)
    with SqlCounter(query_count=6):
        assert_snowpark_pandas_dataframes_are_equal(snow_df_copy, cached_snow_df)
    with SqlCounter(query_count=1):
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            cached_snow_df, native_df
        )


@pytest.mark.parametrize("inplace", [True, False])
def test_cache_result_dataframe_complex_correctness(
    time_index_string_column_data,
    inplace,
):
    df_data, kwargs = time_index_string_column_data
    snow_df, native_df = create_test_dfs(df_data, **kwargs)

    snow_df = snow_df.resample("2H").mean()
    snow_df_copy = snow_df.copy(deep=True)
    with SqlCounter(query_count=1):
        cached_snow_df = cache_and_return_df(snow_df, inplace)
    with SqlCounter(query_count=6):
        assert_snowpark_pandas_dataframes_are_equal(snow_df_copy, cached_snow_df)
    native_df = native_df.resample("2H").mean()

    cached_snow_df = cached_snow_df.set_index("b", drop=False)
    native_df = native_df.set_index("b", drop=False)
    with SqlCounter(query_count=1):
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            cached_snow_df, native_df, check_freq=False
        )


@pytest.mark.parametrize("inplace", [True, False])
def test_cache_result_dataframe_cleanup(
    db_parameters,
    time_index_string_column_data,
    inplace,
):
    old_session = pd.session
    new_session = Session.builder.configs(db_parameters).create()
    pd.session = new_session
    df_data, kwargs = time_index_string_column_data
    snow_df, _ = create_test_dfs(df_data, **kwargs)
    snow_df = cache_and_return_df(snow_df, inplace)
    table_name = (
        snow_df._query_compiler._modin_frame.ordered_dataframe._dataframe_ref.snowpark_dataframe.table_name
    )
    new_session.close()
    with pytest.raises(SnowparkSQLException, match="does not exist or not authorized"):
        old_session.table(table_name).show()


@pytest.mark.parametrize("inplace", [True, False])
class TestCacheResultReducesQueryCount:
    def test_cache_result_simple(self, inplace):
        snow_df = pd.concat([pd.DataFrame([range(i, i + 5)]) for i in range(0, 150, 5)])
        native_df = native_pd.DataFrame(np.arange(150).reshape((30, 5)))
        native_df = native_pd.concat([native_df] * 10)
        with SqlCounter(query_count=1, union_count=299):
            snow_df = perform_chained_operations(snow_df, pd)
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                snow_df, native_df
            )

        with SqlCounter(query_count=1, union_count=29):
            snow_df = pd.concat(
                [pd.DataFrame([range(i, i + 5)]) for i in range(0, 150, 5)]
            )
            cached_snow_df = cache_and_return_df(snow_df, inplace)

        with SqlCounter(query_count=1, union_count=9):
            cached_snow_df = perform_chained_operations(cached_snow_df, pd)
            cached_snow_df.to_pandas()

        with SqlCounter(query_count=1):
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                cached_snow_df, native_df
            )

    def test_cache_result_post_pivot(self, inplace, simple_test_data):
        pivot_kwargs = {
            "index": "col1",
            "columns": ["col0", "col1"],
            "values": "col2",
            "aggfunc": ["mean", "max"],
        }
        snow_df = pd.DataFrame(simple_test_data).pivot_table(**pivot_kwargs)
        native_df = native_pd.DataFrame(simple_test_data)
        native_df = perform_chained_operations(
            native_df.pivot_table(**pivot_kwargs), native_pd
        )
        with SqlCounter(query_count=1, join_count=10, union_count=9):
            snow_df = perform_chained_operations(snow_df, pd)
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                snow_df, native_df
            )

        with SqlCounter(query_count=1, join_count=1):
            snow_df = pd.DataFrame(simple_test_data).pivot_table(**pivot_kwargs)
            cached_snow_df = cache_and_return_df(snow_df, inplace)

        with SqlCounter(query_count=1, union_count=9):
            cached_snow_df = perform_chained_operations(cached_snow_df, pd)
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                cached_snow_df, native_df
            )

    def test_cache_result_post_apply(self, inplace, simple_test_data):
        native_df = perform_chained_operations(
            native_pd.DataFrame(simple_test_data).apply(lambda x: x + x, axis=1),
            native_pd,
        )
        with SqlCounter(query_count=6, union_count=9, udtf_count=1):
            snow_df = pd.DataFrame(simple_test_data).apply(lambda x: x + x, axis=1)
            repr(snow_df)
            snow_df = perform_chained_operations(snow_df, pd)
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                snow_df, native_df
            )

        with SqlCounter(query_count=5, udtf_count=1):
            snow_df = pd.DataFrame(simple_test_data).apply(lambda x: x + x, axis=1)
            cached_snow_df = cache_and_return_df(snow_df, inplace)

        with SqlCounter(query_count=2, union_count=9):
            repr(cached_snow_df)
            cached_snow_df = perform_chained_operations(cached_snow_df, pd)
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                cached_snow_df,
                native_df,
            )

    def test_cache_result_post_applymap(self, inplace, simple_test_data):
        native_df = perform_chained_operations(
            native_pd.DataFrame(simple_test_data).applymap(lambda x: x + x), native_pd
        )
        with SqlCounter(
            query_count=11,
            union_count=9,
            udf_count=2,
            high_count_expected=True,
            high_count_reason="applymap requires additional queries.",
        ):
            snow_df = pd.DataFrame(simple_test_data).applymap(lambda x: x + x)
            repr(snow_df)
            snow_df = perform_chained_operations(snow_df, pd)
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                snow_df, native_df
            )

        with SqlCounter(
            query_count=10,
            high_count_expected=True,
            high_count_reason="caching result of udf seems to have high query count.",
        ):
            snow_df = pd.DataFrame(simple_test_data).applymap(lambda x: x + x)
            cached_snow_df = cache_and_return_df(snow_df, inplace)

        with SqlCounter(query_count=2, union_count=9, udf_count=0):
            repr(cached_snow_df)
            cached_snow_df = perform_chained_operations(cached_snow_df, pd)
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                cached_snow_df,
                native_df,
            )
