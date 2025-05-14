#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re
from typing import Any

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    eval_snowpark_pandas_result,
    try_cast_to_snowpark_pandas_dataframe,
    try_cast_to_snowpark_pandas_series,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

# In this file, the following 4 cases how values can be supplied to the
# Dataframe.isin(values) API are included
# Case 1: values is list-like
# Case 2: values is (Snowpark pandas) Series
# Case 3: values is (Snowpark pandas) DataFrame
# Case 3: values is dict

# type hint pd.DataFrame | native_pd.DataFrame does not work here, therefore Any is used.
def _test_isin_with_snowflake_logic(df: Any, values, query_count=0):  # noqa: E302

    # convert to Snowpark pandas API universe
    if isinstance(df, pd.DataFrame):
        if isinstance(values, native_pd.Series):
            values = try_cast_to_snowpark_pandas_series(values)
        elif isinstance(values, native_pd.DataFrame):
            values = try_cast_to_snowpark_pandas_dataframe(values)
    else:
        # set expected query counts to 0 if native pandas is used.
        query_count = 0

    with SqlCounter(query_count=query_count):
        ans = df.isin(values)

        # Following code is to emulate Snowflake behavior:
        # In Snowflake semantics, preserve nulls. E.g., NULL.isin([NULL]) will yield NULL, but not True/False
        # similarly, if the values passed to isin contain a single NULL, the result will be NULL
        if isinstance(ans, pd.DataFrame):
            ans = ans.to_pandas()

    # mask with N/A from original data
    mask = (
        df.to_pandas().isna().values
        if isinstance(df, pd.DataFrame)
        else df.isna().values
    )

    masked_data = np.where(mask, None, ans)
    ans = native_pd.DataFrame(masked_data, columns=ans.columns, index=ans.index)

    # convert back to use eval function below.
    if isinstance(df, pd.DataFrame):
        ans = pd.DataFrame(ans)

    return ans


@pytest.mark.parametrize(
    "values",
    [
        [],
        (),
        set(),
        np.array([]),
        [1, 2],
        [None, 1, 2],
        {1, 2, 1},
        np.array([None, 2]),
        ("a",),
        np.array([None, "b"]),
    ],
)
@pytest.mark.parametrize(
    "data,columns",
    [
        ([[1, 2, 3, None], [10, 10, 10], [None, None, None]], ["a", "b b", 20, None]),
        ([["a", "b", None], [None, None, None]], ["X", "Y", "Z"]),
    ],
)
def test_isin_with_listlike_scalars(values, data, columns):
    # test here list-like values, we consider lists, tuples, sets and numpy arrays as list like.
    # note that the index of the original dataframe here is irrelevant as it is a cell-based operation.
    native_df = native_pd.DataFrame(data, columns=columns)
    snow_df = pd.DataFrame(data, columns=columns)

    # SQL count checker found within _test_isin_with_snowflake_logic as additional queries are run
    # there to test deviating semantics against pandas (conversion from adjusted result to Snowpark pandas again).
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: _test_isin_with_snowflake_logic(df, values, query_count=1),
    )


@pytest.mark.parametrize(
    "values",
    [
        # To prevent dtype mismatch error, we cast the empty index (default int dtype) to object
        native_pd.Series([], index=native_pd.Index([], dtype=object)),
        native_pd.Series(index=[1, 2, 3]),
        native_pd.Series([1, 3], index=[10, 11]),
        native_pd.Series([1, 10], index=[10, 11]),
        native_pd.Series(
            ["a", "A", "a", "A", "X", "Z"], index=["b", "c", "a", "d", "f", "e"]
        ),
    ],
)
@pytest.mark.parametrize(
    "data,columns,index",
    [
        (
            [[1, 2, 3, None], [10, 11, 10], [None, None, None]],
            ["a", "b b", 20, None],
            [1, 11, 90],
        ),
        (
            [["a", "b", None], [None, None, None], ["b", None, None]],
            ["X", "Y", "Z"],
            ["a", "b", "c"],
        ),
    ],
)
def test_isin_with_Series(values, data, columns, index):
    native_df = native_pd.DataFrame(data, columns=columns, index=index)
    snow_df = pd.DataFrame(native_df)

    # skip when index types are different ( type coercion in Snowpark pandas )
    if snow_df.index.dtype != values.dtype:
        pytest.skip("Snowpark pandas does not support different index types")

    # SQL count checker found within _test_isin_with_snowflake_logic as additional queries are run
    # there to test deviating semantics against pandas (conversion from adjusted result to Snowpark pandas again).
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        # 2 queries: 1 for the isin, 1 extra query to handle empty dataframe special case
        lambda df: _test_isin_with_snowflake_logic(df, values, query_count=1),
    )


# Examples to test with here are
# 1. doctest example
# 2. doctest example modified so isin has a column that's not present in the original dataframe
# 3. duplicate columns (differing result for each).
# -> note that duplicates along the index are not allowed in pandas, the index is assumed to be unique.
@pytest.mark.parametrize(
    "df,other",
    [
        (
            native_pd.DataFrame(
                {"num_legs": [2, 4], "num_wings": [2, 0]}, index=["falcon", "dog"]
            ),
            native_pd.DataFrame(
                {"num_legs": [8, 3], "num_wings": [0, 2]}, index=["spider", "falcon"]
            ),
        ),
        (
            native_pd.DataFrame(
                {"num_legs": [2, 4], "num_wings": [2, 0]}, index=["falcon", "dog"]
            ),
            native_pd.DataFrame(
                {"num_legs": [8, 3, 4], "num_wings": [0, 2, 0]},
                index=["spider", "falcon", "elephant"],
            ),
        ),
        (
            native_pd.DataFrame(
                [[4, 2, 2, 0, 99], [2, 0, 2, 2, 99]],
                columns=["num_legs", "num_wings", "num_legs", "num_wings", "unrelated"],
                index=["falcon", "dog"],
            ),
            native_pd.DataFrame(
                {
                    "num_legs": [8, 2, 4],
                    "num_wings": [0, 2, 0],
                    "not_existing": [1, 2, 3],
                },
                index=["spider", "falcon", "elephant"],
            ),
        ),
    ],
)
def test_isin_with_Dataframe(df, other):
    snow_df = pd.DataFrame(df)

    def eval_dataframe_isin(df):
        if isinstance(df, pd.DataFrame):
            values = pd.DataFrame(other)
        else:
            values = other
        #  3 queries: 2 for the isin of which one is caused by set, 1 extra query to handle empty dataframe special case
        return _test_isin_with_snowflake_logic(df, values, query_count=1)

    eval_snowpark_pandas_result(
        snow_df,
        df,
        eval_dataframe_isin,
    )


# doctest example {'num_wings': [0, 3]}
@pytest.mark.parametrize(
    "df,values",
    [
        (
            native_pd.DataFrame(
                {"num_legs": [2, 4], "num_wings": [2, 0]}, index=["falcon", "dog"]
            ),
            {"num_wings": [0, 3]},
        )
    ],
)
def test_isin_with_dict(df, values):
    snow_df = pd.DataFrame(df)

    eval_snowpark_pandas_result(
        snow_df,
        df,
        lambda df: _test_isin_with_snowflake_logic(df, values, query_count=1),
    )


@sql_count_checker(query_count=1)
def test_isin_duplicate_columns_negative():
    with pytest.raises(ValueError, match="cannot compute isin with a duplicate axis."):
        df = pd.DataFrame({"A": [1, 2, 3]})
        other = pd.DataFrame([[1, 2], [3, 4]], columns=["A", "A"])

        df.isin(other)


@sql_count_checker(query_count=0)
def test_isin_dataframe_values_type_negative():
    with pytest.raises(
        TypeError,
        match=re.escape(
            "only list-like or dict-like objects are allowed to "
            "be passed to DataFrame.isin(), you passed a 'str'"
        ),
    ):
        df = pd.DataFrame([1, 2, 3])
        df.isin(values="abcdef")


@sql_count_checker(query_count=3)
@pytest.mark.parametrize(
    "values",
    [
        pytest.param([2, 3], id="integers"),
        pytest.param([pd.Timedelta(2), pd.Timedelta(3)], id="timedeltas"),
    ],
)
def test_isin_timedelta(values):
    native_df = native_pd.DataFrame({"a": [1, 2, 3], "b": [None, 4, 2]}).astype(
        {"b": "timedelta64[ns]"}
    )
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: _test_isin_with_snowflake_logic(df, values, query_count=1),
    )
