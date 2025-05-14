#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin.extensions.utils import try_convert_index_to_native
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
    try_cast_to_snowpark_pandas_dataframe,
    try_cast_to_snowpark_pandas_series,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


def _test_isin_with_snowflake_logic(s, values):
    # convert to Snowpark pandas API universe
    if isinstance(s, pd.Series):
        if isinstance(values, native_pd.Series):
            values = try_cast_to_snowpark_pandas_series(values)
        if isinstance(values, native_pd.DataFrame):
            values = try_cast_to_snowpark_pandas_dataframe(values)

    ans = s.isin(values)

    # Following code is to emulate Snowflake behavior:
    # In Snowflake semantics, preserve nulls. E.g., NULL.isin([NULL]) will yield NULL, but not True/False
    # similarly, if the values passed to isin contain a single NULL, the result will be NULL
    if isinstance(ans, pd.Series):
        ans = ans.to_pandas()
    data = s.to_pandas().values if isinstance(s, pd.Series) else s.values
    for i, v in enumerate(data):
        if v is None or pd.isna(v):
            ans.iloc[i] = None
    ans = ans.fillna(value=np.nan)
    if any([pd.isna(value) for value in values]):
        ans.iloc[:] = np.nan

    # convert back to use eval function below.
    if isinstance(s, pd.Series):
        ans = pd.Series(ans)

    return ans


@pytest.mark.parametrize(
    "values, expected_query_count",
    [
        ([], 3),
        ([1, 2, 3], 3),
        ([None], 3),
        ([None, 2], 3),
        ([99, 97, 93, 100000], 3),
        (np.array([]), 3),
        (np.array([1, 2, 1]), 3),
        (np.array([None, 1, 2]), 3),
        (native_pd.Series(), 5),
        # (native_pd.Series([2, 3], index=["A", "B"]), 1), # not supported anymore because of index type mismatch
        # (native_pd.Series(index=["A", "B"]), 1), # not supported anymore because of index type mismatch
        (native_pd.Series([None, -10]), 4),
        (native_pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]}), 3),
        (native_pd.Index([4, 5, 6]), 4),
    ],
)
def test_isin_integer_data(values, expected_query_count):
    if isinstance(values, native_pd.Index):
        values = pd.Index(values)
    data = [3, 4, 2, 1, None, 0, 5, 4, 2, -10, -20, -42, None]
    with SqlCounter(query_count=expected_query_count):
        snow_series = pd.Series(data)
        native_series = native_pd.Series(data)

        # Because _test_isin_with_snowflake_logic helper wraps the initial
        # Snowpark pandas result with a new pd.Series object, it doesn't make sense to test attrs.
        eval_snowpark_pandas_result(
            snow_series,
            native_series,
            lambda s: _test_isin_with_snowflake_logic(
                s, try_convert_index_to_native(values)
            ),
            test_attrs=False,
        )


@pytest.mark.parametrize(
    "values, expected_query_count",
    [
        (native_pd.Series([2, 3], index=["A", "B"]), 1),
        (native_pd.Series(index=["A", "B"]), 1),
    ],
)
@pytest.mark.xfail(
    reason="Snowpark pandas does not support isin with index type mismatch."
)
def test_isin_with_incompatible_index(values, expected_query_count):
    data = [3, 4, 2, 1, None, 0, 5, 4, 2, -10, -20, -42, None]
    with SqlCounter(query_count=expected_query_count):
        snow_series = pd.Series(data)
        native_series = native_pd.Series(data)

        eval_snowpark_pandas_result(
            snow_series,
            native_series,
            lambda s: _test_isin_with_snowflake_logic(s, values),
        )


@pytest.mark.parametrize(
    "data,values,expected_query_count",
    [
        ([], native_pd.Series([]), 4),
        ([1, 2, 3], native_pd.Series([]), 4),
        ([], native_pd.Series([2, 3, 4]), 4),
        ([1, 2, 3, 8], native_pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}), 3),
        (["A", "B", ""], [], 3),
        (["A", "B", ""], ["A"], 3),
        (["A", "B", ""], ["A", "B", "C", "D"], 3),
        (["A", "B", None, ""], [], 3),
        (["A", "B", None, ""], ["A"], 3),
        (["A", "B", None, ""], ["A", "B", "C", "D"], 3),
        (["A", "B", None, ""], [None], 3),
        (["A", "B", ""], [None], 3),
        (["A", "B", ""], np.array(["A", "B", "C", "D"]), 3),
        ([False, True], [], 3),
        ([False, True], [False], 3),
        ([False, True], [True], 3),
        ([False, True, None], [False, True], 3),
        ([False, True], [None, True, False], 3),
        ([1.0, 1.1, 1.2, 1.3], [1.2], 3),
        ([1.0, 1.1, 1.2, 1.3], [99.99999], 3),
        ([1.0, 1.1, 1.2, 1.3], [1, 2, 3], 3),
        ([1, 2, 3], [1, 2, 99.9], 3),
        (["string 1", 0.012, -3, None, "string 2", 3.14], [3.14, "string 1"], 3),
        (
            ["string 1", 0.012, -3, None, "string 2", 3.14],
            np.array([3.14, "string 1"]),
            3,
        ),
        (["string 1", 0.012, -3, None, "string 2", 3.14], [7, "test", 89.9], 3),
        ([1, 2, 3, 2, 1], {3, 4, 3}, 3),
    ],
)
def test_isin_various_combos(data, values, expected_query_count):
    with SqlCounter(query_count=expected_query_count):
        snow_series = pd.Series(data)
        native_series = native_pd.Series(data)

        # Because _test_isin_with_snowflake_logic helper wraps the initial
        # Snowpark pandas result with a new pd.Series object, it doesn't make sense to test attrs.
        eval_snowpark_pandas_result(
            snow_series,
            native_series,
            lambda s: _test_isin_with_snowflake_logic(s, values),
            test_attrs=False,
        )


@sql_count_checker(query_count=1, join_count=1)
def test_isin_lazy():
    s_data = [1, 2, 3, 4, 5]
    df_data = {"a": [1, 2, "test"], "b": [4, 5, 6]}

    snow_series = pd.Series(s_data)
    snow_df = pd.DataFrame(df_data)
    snow_ans = snow_series.isin(snow_df["a"])

    native_series = native_pd.Series(s_data)
    native_df = native_pd.DataFrame(df_data)
    native_ans = native_series.isin(native_df["a"])

    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_ans, native_ans)


@sql_count_checker(query_count=0)
def test_isin_with_str_negative():
    s = pd.Series([1, 2, 3])
    with pytest.raises(
        TypeError,
        match=re.escape(
            "only list-like objects are allowed to be passed to isin(), you passed a [str]"
        ),
    ):
        s.isin("test")
