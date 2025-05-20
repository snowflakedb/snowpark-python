#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas.core.groupby.generic import DataFrameGroupBy as PandasDFGroupBy

import snowflake.snowpark.modin.plugin  # noqa : F401
from modin.pandas.groupby import (
    DataFrameGroupBy as SnowparkPandasDFGroupBy,
    SeriesGroupBy as SnowparkPandasSerGroupBy,
)
from tests.integ.modin.utils import assert_frame_equal, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.fixture(scope="function")
def single_row_dfs():
    native_df = native_pd.DataFrame([[1, 2]], columns=["A", "B"])
    snow_df = pd.DataFrame(native_df)
    return native_df, snow_df


# TODO (SNOW-887758): add test with as_index once as_index is handled correctly
@pytest.mark.parametrize("by", ["col1", ["col1", "col2"], ["col2", "col2"]])
@pytest.mark.parametrize(
    "cols",
    ["col3", ["col3"], ["col4", "col3", "col3"], ["col3", "col4", "col5", "col3"]],
)
@sql_count_checker(query_count=2)
def test_column_select(basic_snowpark_pandas_df, by, cols):
    native_df = basic_snowpark_pandas_df.to_pandas()
    snow_grp_col = basic_snowpark_pandas_df.groupby(by=by)[cols]
    native_grp_col = native_df.groupby(by=by)[cols]

    if isinstance(native_grp_col, PandasDFGroupBy):
        assert isinstance(snow_grp_col, SnowparkPandasDFGroupBy)
    else:
        assert isinstance(snow_grp_col, SnowparkPandasSerGroupBy)

    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(snow_grp_col, native_grp_col, lambda grp: grp.sum())


@pytest.mark.parametrize("level", [0, [0, 1], [1, 1]])
@pytest.mark.parametrize(
    "cols",
    ["C", ["D"], ["C", "D"], ["D", "C", "D"]],
)
@sql_count_checker(query_count=2)
def test_column_select_with_level(df_multi, level, cols):
    native_df = df_multi.to_pandas()
    snow_grp_col = df_multi.groupby(level=level)[cols]
    native_grp_col = native_df.groupby(level=level)[cols]

    if isinstance(native_grp_col, PandasDFGroupBy):
        assert isinstance(snow_grp_col, SnowparkPandasDFGroupBy)
    else:
        assert isinstance(snow_grp_col, SnowparkPandasSerGroupBy)

    eval_snowpark_pandas_result(snow_grp_col, native_grp_col, lambda grp: grp.sum())


@pytest.mark.parametrize("by", ["col1", ["col1", "col2"], ["col2", "col2"]])
@sql_count_checker(query_count=2)
def test_column_select_via_attr(basic_snowpark_pandas_df, by):
    native_pandas_df = basic_snowpark_pandas_df.to_pandas()
    expected_count = 1
    with SqlCounter(query_count=expected_count):
        eval_snowpark_pandas_result(
            basic_snowpark_pandas_df,
            native_pandas_df,
            lambda df: df.groupby(by=by).col3.sum(),
        )


@sql_count_checker(query_count=2)
def test_getitem_list_of_columns_with_indexing():
    df = pd.DataFrame(
        {
            "A": ["foo", "bar", "foo", "bar", "foo", "bar", "foo", "foo"],
            "B": ["one", "one", "two", "three", "two", "two", "one", "three"],
            "C": np.random.randn(8),
            "D": np.random.randn(8),
            "E": np.random.randn(8),
        }
    )

    result = df.groupby("A")[df.columns[2:4]].median()

    native_df = df.to_pandas()
    expected = native_df.loc[:, ["A", "C", "D"]].groupby("A").median()

    assert_frame_equal(result, expected)


@pytest.mark.xfail(
    strict=True,
    reason="SNOW-1057810: Indexing groupby with unwrapped lists should fail",
)
@sql_count_checker(query_count=2)
def test_getitem_single_tuple_of_columns(basic_snowpark_pandas_df):
    # from pandas 2.0.0, select column with single tuple like ("col2", "col3") will be deprecated.
    # select set of columns must use a list df.groupby("col1")[["col2", "col3"]]
    # This test will fail once updated to 2.0.0.
    eval_snowpark_pandas_result(
        basic_snowpark_pandas_df,
        basic_snowpark_pandas_df.to_pandas(),
        lambda df: df.groupby("col1")["col2", "col3"].sum(),
    )


@pytest.mark.parametrize("cols", ["C", ["A", "C"]])
@sql_count_checker(query_count=0)
def test_select_bad_cols_raise(cols, single_row_dfs):
    native_df, snow_df = single_row_dfs
    snow_g = snow_df.groupby("A")
    native_g = native_df.groupby("A")
    eval_snowpark_pandas_result(
        snow_g,
        native_g,
        lambda g: g[["C"]],
        expect_exception=True,
        expect_exception_type=KeyError,
        expect_exception_match="Columns not found: 'C'",
    )


@sql_count_checker(query_count=0)
def test_select_overlapped_by_cols_raise(single_row_dfs):
    _, snow_df = single_row_dfs

    # we currently do not support select data columns that overlaps with by columns, like
    # df.groupby("A")["A", "C"], where column "A" occurs in both the groupby and column selection.
    # This is because in regular groupby, one by column cannot be mapped to multiple columns,
    # for example with a dataframe have columns=['A', 'B', 'A'], where 'A' corresponds to two columns,
    # df.groupby('A') will raise an error. However, with getitem, the new columns selected
    # is treated differently and they can be duplicate of the by column. For example: it is valid to
    # have df.groupby("A")["A", "A", "C"] even though the result dataframe after colum select have
    # multiple column "A".
    # In order to handle this correctly, we need to record the column to exclude during by label matching.
    # Since Modin does not support this case also, we raise an exception and deffer the support for now.
    # TODO (SNOW-894942): Handle getitem overlap with groupby column
    message = (
        "Data column selection with overlap of 'by' columns is not yet supported, "
        "please duplicate the overlapped by columns and rename it to a different name"
    )
    with pytest.raises(NotImplementedError, match=message):
        snow_df.groupby("A")["A", "B"]


@sql_count_checker(query_count=1)
def test_select_index_cols_raise(df_multi):
    native_df = df_multi.to_pandas()
    eval_snowpark_pandas_result(
        df_multi,
        native_df,
        lambda df: df.groupby("C")["A"],
        expect_exception=True,
        expect_exception_type=KeyError,
        # this message is slightly different with native pandas with quotes, native pandas not found message
        # wraps the label with quotes for data columns but not index columns, which is a wired behavior, here
        # we just stay consistent across different column types.
        expect_exception_match="Columns not found: 'A'",
        assert_exception_equal=False,
    )


@sql_count_checker(query_count=0)
def test_select_cols_with_axis_1_raise(single_row_dfs):
    native_df, snow_df = single_row_dfs
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby("A", axis=1)["B"],
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="Cannot subset columns when using axis=1",
    )


@pytest.mark.parametrize("as_index", [True, False])
@sql_count_checker(query_count=1)
def test_as_index_select_single_column(as_index):
    native_df = native_pd.DataFrame([[1, 2], [1, 4], [5, 6]], columns=["A", "B"])
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.groupby("A", as_index=as_index)["B"].max()
    )


@pytest.mark.xfail(
    strict=True,
    reason="SNOW-1057819: Investigate whether groupby operations should drop df.columns.name",
)
@sql_count_checker(query_count=1)
def test_groupby_as_index_select_column_sum_empty_df():
    native_df = native_pd.DataFrame(
        columns=native_pd.Index(["A", "B", "C"], name="alpha"), dtype="int64"
    )
    snow_df = pd.DataFrame(native_df)
    # the result for this is an empty dataframe with column alpha, "A", "B"
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby(by="A", as_index=False)["B"].sum(numeric_only=False),
    )
