#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    PANDAS_VERSION_PREDICATE,
    create_test_dfs,
    eval_snowpark_pandas_result as _eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

pytestmark = pytest.mark.skipif(
    PANDAS_VERSION_PREDICATE,
    reason="SNOW-1739034: tests with UDFs/sprocs cannot run without pandas 2.2.3 in Snowflake anaconda",
)


def eval_snowpark_pandas_result(*args, **kwargs):
    # Some calls to the native pandas function propagate attrs while some do not, depending on the values of its arguments.
    return _eval_snowpark_pandas_result(*args, test_attrs=False, **kwargs)


@pytest.mark.parametrize("dropna", [True, False])
@pytest.mark.parametrize("as_index", [True, False])
@pytest.mark.parametrize("group_keys", [True, False])
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize(
    "func",
    [
        "mean",
        "count",
        np.sqrt,
        np.square,
        lambda df: df * 10,
        lambda _: 3,
        lambda df: df.min() + df.max(),
    ],
)
@pytest.mark.parametrize("grouping_columns", ["B", ["A", "B"]])
def test_dataframe_groupby_transform(
    dropna, as_index, group_keys, sort, func, grouping_columns, df_with_multiple_columns
):
    """
    Test DataFrameGroupBy.transform with some basic functions.
    """
    # - A UDTF is created to run `groupby.transform(func)` on every group via `apply`.
    # - One join always occurs when joining the original DataFrame's table with the
    #   temporary function's resultant table.
    # - A second join is performed only when the groupby object specifies dropna=True.
    #   This is because a loc set operation is being performed to correctly set NA values.
    with SqlCounter(query_count=6, join_count=1 + (1 if dropna else 0), udtf_count=1):
        eval_snowpark_pandas_result(
            *df_with_multiple_columns,
            lambda df: df.groupby(
                by=grouping_columns,
                dropna=dropna,
                as_index=as_index,
                group_keys=group_keys,
                sort=sort,
            ).transform(func),
        )


@pytest.mark.parametrize("dropna", [True, False])
@pytest.mark.parametrize("as_index", [True, False])
@pytest.mark.parametrize("group_keys", [True, False])
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize(
    "func, args, kwargs",
    [
        (lambda df, arg1, arg2: (df.max() * arg1) + arg2, [2], {"arg2": -25}),
        (lambda df, arg1, arg2: df.mean() + arg1 * arg2, [2, 3], {}),
        (
            lambda df, arg1, arg2, arg3, arg4, arg5: df.head(arg1)
            + (arg4 * arg5) / (arg2 * arg3),
            [1, 2, 3],
            {"arg5": 4, "arg4": 5},
        ),
    ],
)
@pytest.mark.parametrize("grouping_columns", ["B", ["A", "B"]])
def test_dataframe_groupby_transform_with_func_args_and_kwargs(
    dropna,
    as_index,
    group_keys,
    sort,
    func,
    args,
    kwargs,
    grouping_columns,
    df_with_multiple_columns,
):
    """
    Test DataFrameGroupby.transform with functions that require *args and **kwargs.
    """
    # - A UDTF is created to run `groupby.transform(func)` on every group via `apply`.
    # - One join always occurs when joining the original DataFrame's table with the
    #   temporary function's resultant table.
    # - A second join is performed only when the groupby object specifies dropna=True.
    #   This is because a loc set operation is being performed to correctly set NA values.
    with SqlCounter(query_count=6, join_count=1 + (1 if dropna else 0), udtf_count=1):
        eval_snowpark_pandas_result(
            *df_with_multiple_columns,
            lambda df: df.groupby(
                by=grouping_columns,
                dropna=dropna,
                as_index=as_index,
                group_keys=group_keys,
                sort=sort,
            ).transform(func, *args, **kwargs),
        )


@sql_count_checker(
    query_count=9,
    join_count=4,
    udtf_count=2,
    high_count_expected=True,
    high_count_reason="performing two groupby transform operations that use UDTFs",
)
def test_dataframe_groupby_transform_conflicting_labels_negative():
    """
    Based on SNOW-1361200 - The bug occurred because of conflicting UDTF columns appended during groupby transform
    operations in `create_udtf_for_groupby_apply`.
    This test is supposed to raise NotImplementedError because it's supposed to have pandas labels that
    conflict with each other, not Snowflake column labels.
    """
    df = pd.DataFrame({"X": [1, 2, 3, 1, 2, 2], "Y": [4, 5, 6, 7, 8, 9]})
    df["X_DATA"] = df["X"]
    df["A"] = df.groupby("X")["X_DATA"].transform("count")
    err_msg = (
        "No support for applying a function that returns two dataframes that have different labels for the"
        " column at a given position, a function that returns two dataframes that have different column index"
        " names, or a function that returns two series with different names or conflicting labels for the row"
        " at a given position."
    )
    with pytest.raises(NotImplementedError, match=err_msg):
        df["B"] = df.groupby("X")["X_DATA"].transform("cumcount")
        pd.show(df)


@sql_count_checker(
    query_count=11,
    join_count=8,
    udtf_count=2,
    high_count_expected=True,
    high_count_reason="performing two groupby transform operations that use UDTFs and compare with pandas",
)
def test_dataframe_groupby_transform_conflicting_labels():
    """
    Based on SNOW-1361200 - The bug occurred because of conflicting UDTF columns appended during groupby transform
    operations in `create_udtf_for_groupby_apply`.
    This test is supposed to work correctly and match native pandas.
    """

    def transform_helper(df):
        df["X_DATA"] = df["X"]
        df["A"] = df.groupby("X")["X_DATA"].transform("count")
        df["B"] = df.groupby("X")["X_DATA"].transform("count")

    eval_snowpark_pandas_result(
        *create_test_dfs({"X": [1, 2, 3, 1, 2, 2], "Y": [4, 5, 6, 7, 8, 9]}),
        transform_helper,
        inplace=True,
    )


@sql_count_checker(
    query_count=11,
    join_count=5,
    udtf_count=2,
    high_count_expected=True,
    high_count_reason="performing two groupby transform operations that use UDTFs and compare with pandas",
)
def test_dataframe_groupby_transform_conflicting_labels_chained():
    """
    Based on SNOW-1361200 - The bug occurred because of conflicting UDTF columns appended during groupby transform
    operations in `create_udtf_for_groupby_apply`.
    This test is supposed to work correctly and match native pandas.
    """

    def transform_helper(df):
        df1 = df.groupby("X").transform("count")
        df2 = df1.groupby("Y").transform("count")
        return df2

    eval_snowpark_pandas_result(
        *create_test_dfs(
            {"X": [1, 2, 3, 1, 2, 2], "Y": [4, 5, 6, 7, 8, 9], "Z": [9, 8, 7, 6, 5, 4]}
        ),
        transform_helper,
    )


@pytest.mark.xfail(strict=True, raises=AssertionError, reason="SNOW-1619940")
def test_return_timedelta():
    eval_snowpark_pandas_result(
        *create_test_dfs([[5, 7]]),
        lambda df: df.groupby(0).transform(
            lambda series: native_pd.Series([native_pd.Timedelta(series.sum())])
        ),
    )


@pytest.mark.xfail(strict=True, raises=NotImplementedError)
@pytest.mark.parametrize(
    "pandas_df",
    [
        param(
            native_pd.DataFrame([["key0", native_pd.Timedelta(1)]]),
            id="timedelta_column",
        ),
        param(
            native_pd.DataFrame(
                [["key0", "value1"]], index=native_pd.Index([native_pd.Timedelta(1)])
            ),
            id="timedelta_index",
        ),
    ],
)
def test_timedelta_input(pandas_df):
    eval_snowpark_pandas_result(
        *create_test_dfs(pandas_df),
        lambda df: df.groupby(0).transform(lambda series: 1),
    )
