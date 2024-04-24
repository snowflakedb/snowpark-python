#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#


import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.conftest import running_on_public_ci
from tests.integ.modin.sql_counter import SqlCounter
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    assert_values_equal,
    eval_snowpark_pandas_result,
)


def nonempty_boolagg_sql_counter(axis):
    # All operations incur 1 query to perform the initial aggregation, regardless of axis.
    # axis=0 incurs an extra call to check the size of the index, and axis=None
    # calls the query compiler function with axis=0 twice.
    # There is no extra call for df.columnarize() because the result is already transposed in the QC.
    # These numbers differ for empty dataframes depending on whether the columns/rows
    # are empty or not.
    expected_query_count = 3 if axis is None else 1 if axis in (1, "columns") else 2
    return SqlCounter(query_count=expected_query_count)


def boolagg_comparator(axis):
    return (
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck
        if axis is not None
        else assert_values_equal
    )


@pytest.mark.parametrize("axis", [0, 1])
@pytest.mark.parametrize(
    "data",
    [
        [],
        [[]],
        [[], [], []],
        pytest.param(
            {"a": [], "b": []},
            marks=pytest.mark.skip(
                "empty rows are treated as float64; TO_BOOLEAN cast does not accept float arguments"
            ),
        ),
    ],
)
def test_empty(data, axis):
    with SqlCounter(query_count=1):
        # Treat index like any other column in a DataFrame when it comes to types,
        # therefore Snowpark pandas returns an Index(dtype="object") for an empty index
        # whereas pandas returns RangeIndex()
        # This is compatible with behavior for empty dataframe in other tests.
        eval_snowpark_pandas_result(
            pd.DataFrame(data),
            native_pd.DataFrame(data),
            lambda df: df.all(axis=axis),
            comparator=assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
            check_index_type=False,
        )
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            pd.DataFrame(data),
            native_pd.DataFrame(data),
            lambda df: df.any(axis=axis),
            comparator=assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
            check_index_type=False,
        )


@pytest.mark.parametrize(
    "data",
    [
        param(
            [],
            marks=pytest.mark.xfail(
                strict=True, raises=AssertionError, reason="SNOW-1017231"
            ),
        ),
        param(
            [[]],
            marks=pytest.mark.xfail(
                strict=True, raises=AssertionError, reason="SNOW-1017231"
            ),
        ),
        param(
            [[], [], []],
            marks=pytest.mark.xfail(
                strict=True, raises=AssertionError, reason="SNOW-1017231"
            ),
        ),
        pytest.param(
            {"a": [], "b": []},
            marks=pytest.mark.skip(
                "empty rows are treated as float64; TO_BOOLEAN cast does not accept float arguments"
            ),
        ),
    ],
)
@pytest.mark.parametrize("method", ["any", "all"])
def test_empty_axis_none(data, method):
    """This test function is separate from the other empty dataframe test
    function because we expected a scalar result, so we can't pass the
    check_index_type kwarg."""
    with SqlCounter(query_count=2):
        eval_snowpark_pandas_result(
            pd.DataFrame(data),
            native_pd.DataFrame(data),
            lambda df: getattr(df, method)(axis=None),
            comparator=assert_values_equal,
        )


@pytest.mark.parametrize("axis", [0, 1, None])
@pytest.mark.parametrize(
    "data",
    [
        {"a": [1, 2, 3], "b": [4, 5, 6]},
        [[0, 0, 1], [0, 1, 1], [1, 1, 1]],
        [[1, 1, 1], [1, 1, 1], [1, 1, 1]],
    ],
)
def test_all_int(data, axis):
    with nonempty_boolagg_sql_counter(axis):
        eval_snowpark_pandas_result(
            pd.DataFrame(data),
            native_pd.DataFrame(data),
            lambda df: df.all(axis=axis),
            comparator=boolagg_comparator(axis),
        )


@pytest.mark.parametrize("axis", [0, 1, None])
@pytest.mark.parametrize(
    "data",
    [
        {"a": [1, 2, 3], "b": [4, 5, 6]},
        [[0, 0, 1], [0, 1, 1], [0, 1, 1]],
        [[0, 0, 0], [0, 0, 0], [0, 0, 0]],
    ],
)
def test_any_int(data, axis):
    with nonempty_boolagg_sql_counter(axis):
        eval_snowpark_pandas_result(
            pd.DataFrame(data),
            native_pd.DataFrame(data),
            lambda df: df.any(axis=axis),
            comparator=boolagg_comparator(axis),
        )


@pytest.mark.parametrize("axis", ["columns", "index"])
def test_all_axis_str_arg(axis):
    with nonempty_boolagg_sql_counter(axis):
        data = [[0, 1], [0, 1]]
        eval_snowpark_pandas_result(
            pd.DataFrame(data),
            native_pd.DataFrame(data),
            lambda df: df.all(axis=axis),
        )


@pytest.mark.parametrize("axis", ["columns", "index"])
def test_any_axis_str_arg(axis):
    with nonempty_boolagg_sql_counter(axis):
        data = [[0, 1], [0, 1]]
        eval_snowpark_pandas_result(
            pd.DataFrame(data),
            native_pd.DataFrame(data),
            lambda df: df.any(axis=axis),
        )


@pytest.mark.parametrize("axis", [None, 0, 1])
def test_all_named_index(axis):
    with nonempty_boolagg_sql_counter(axis):
        data = {"a": [1, 0, 3], "b": [4, 5, 6]}
        index_name = ["c", "d", "e"]
        eval_snowpark_pandas_result(
            pd.DataFrame(data, index_name),
            native_pd.DataFrame(data, index_name),
            lambda df: df.all(axis=axis),
            comparator=boolagg_comparator(axis),
        )


@pytest.mark.parametrize("axis", [None, 0, 1])
def test_any_named_index(axis):
    with nonempty_boolagg_sql_counter(axis):
        data = {"a": [1, 0, 3], "b": [4, 5, 6]}
        index_name = ["c", "d", "e"]
        eval_snowpark_pandas_result(
            pd.DataFrame(data, index_name),
            native_pd.DataFrame(data, index_name),
            lambda df: df.any(axis=axis),
            comparator=boolagg_comparator(axis),
        )


@pytest.mark.parametrize("axis", [0, 1, None])
@pytest.mark.parametrize(
    "data",
    [
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
            "c": [True, False, False],
            "d": [True, True, True],
        }
    ],
)
def test_all_bool_only(data, axis):
    with nonempty_boolagg_sql_counter(axis):
        eval_snowpark_pandas_result(
            pd.DataFrame(data),
            native_pd.DataFrame(data),
            lambda df: df.all(axis=axis),
            comparator=boolagg_comparator(axis),
        )


@pytest.mark.parametrize("axis", [0, 1, None])
@pytest.mark.parametrize(
    "data",
    [
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
            "c": [True, False, False],
            "d": [False, False, False],
        }
    ],
)
def test_any_bool_only(data, axis):
    with nonempty_boolagg_sql_counter(axis):
        eval_snowpark_pandas_result(
            pd.DataFrame(data),
            native_pd.DataFrame(data),
            lambda df: df.any(axis=axis),
            comparator=boolagg_comparator(axis),
        )


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.skipif(running_on_public_ci(), reason="slow fallback test")
@pytest.mark.parametrize("axis", [0, 1, None])
@pytest.mark.parametrize(
    "data",
    [
        {"a": [0.1, 0.0, 0.3], "b": [0.4, 0.5, 0.6]},
        {"a": [np.nan, 0.0, np.nan], "b": [0.4, 0.5, np.nan]},
    ],
)
@pytest.mark.parametrize("skipna", [True, False])
def test_all_float_fallback(data, axis, skipna):
    # Because axis=None calls the method with axis=0 twice, it incurs an extra query
    # to check the length of the index after the first call is handled by a fallback
    with SqlCounter(
        query_count=9 if axis is None else 8, fallback_count=1, sproc_count=1
    ):
        eval_snowpark_pandas_result(
            pd.DataFrame(data),
            native_pd.DataFrame(data),
            lambda df: df.all(axis=axis, skipna=skipna),
            comparator=boolagg_comparator(axis),
        )


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.skipif(running_on_public_ci(), reason="slow fallback test")
@pytest.mark.parametrize("axis", [0, 1, None])
@pytest.mark.parametrize(
    "data",
    [
        {"a": [0.1, 0.0, 0.3], "b": [0.4, 0.5, 0.6]},
        {"a": [np.nan, 0.0, np.nan], "b": [0.4, 0.5, np.nan]},
    ],
)
@pytest.mark.parametrize("skipna", [True, False])
def test_any_float_fallback(data, axis, skipna):
    # Because axis=None calls the method with axis=0 twice, it incurs an extra query
    # to check the length of the index after the first call is handled by a fallback
    with SqlCounter(
        query_count=9 if axis is None else 8, fallback_count=1, sproc_count=1
    ):
        eval_snowpark_pandas_result(
            pd.DataFrame(data),
            native_pd.DataFrame(data),
            lambda df: df.any(axis=axis, skipna=skipna),
            comparator=boolagg_comparator(axis),
        )


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.skipif(running_on_public_ci(), reason="slow fallback test")
@pytest.mark.parametrize("axis", [0, 1, None])
@pytest.mark.parametrize(
    "data",
    [
        {"a": ["", "b", "c"], "b": ["d", "e", "f"]},
    ],
)
def test_all_str_fallback(data, axis):
    # Because axis=None calls the method with axis=0 twice, it incurs an extra query
    # to check the length of the index after the first call is handled by a fallback
    with SqlCounter(
        query_count=9 if axis is None else 8, fallback_count=1, sproc_count=1
    ):
        eval_snowpark_pandas_result(
            pd.DataFrame(data),
            native_pd.DataFrame(data),
            lambda df: df.all(axis=axis),
            comparator=boolagg_comparator(axis),
        )


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.skipif(running_on_public_ci(), reason="slow fallback test")
@pytest.mark.parametrize("axis", [0, 1, None])
@pytest.mark.parametrize(
    "data",
    [
        {"a": ["", "b", "c"], "b": ["", "e", "f"]},
    ],
)
def test_any_str_fallback(data, axis):
    # Because axis=None calls the method with axis=0 twice, it incurs an extra query
    # to check the length of the index after the first call is handled by a fallback
    with SqlCounter(
        query_count=9 if axis is None else 8, fallback_count=1, sproc_count=1
    ):
        eval_snowpark_pandas_result(
            pd.DataFrame(data),
            native_pd.DataFrame(data),
            lambda df: df.any(axis=axis),
            comparator=boolagg_comparator(axis),
        )
