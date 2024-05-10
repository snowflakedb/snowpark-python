#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import create_test_dfs, eval_snowpark_pandas_result


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
            ).transform(func)
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
            ).transform(func, *args, **kwargs)
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
    join_count=10,
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
        inplace=True
    )
