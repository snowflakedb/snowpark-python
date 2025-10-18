#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    create_test_dfs,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker

pytestmark = pytest.mark.filterwarnings("ignore::FutureWarning")

INTERPOLATE_TEST_DATA = {
    "starts_with_nan": [np.nan, np.nan, -0.5, np.nan, 1, -1.5, 2, np.nan, 100],
    "ends_with_nan": [-0.5, np.nan, 1, -1.5, 2, np.nan, 100, np.nan, np.nan],
    "starts_and_ends_with_nan": [
        np.nan,
        np.nan,
        -0.5,
        np.nan,
        np.nan,
        100,
        np.nan,
        -30,
        np.nan,
    ],
    "all_nan": [np.nan] * 9,
    "str_data": [None, "a", "b", None, "cde", None, "x", None, None],
}

INTERPOLATE_DATETIME_DATA = {
    f"datetime_{freq}": native_pd.to_datetime(
        native_pd.date_range("2025-01-01", periods=8, freq=freq)
    )
    for freq in ("D", "h", "min", "s", "ms", "us")
}


# INTERPOLATE_LINEAR in nested selects does not work due to a server-side bug. We skip these tests
# instead of XFAILing because each failed query takes ~10 seconds to run, while successful queries
# take <100ms.
LINEAR_FAIL_REASON = (
    "SNOW-2405318: INTERPOLATE_LINEAR in nested select causes SQL error"
)


@pytest.mark.skip(reason=LINEAR_FAIL_REASON)
@sql_count_checker(query_count=1)
def test_df_interpolate_default_params():
    """Test basic DataFrame.interpolate functionality with default linear method."""
    eval_snowpark_pandas_result(
        *create_test_dfs(INTERPOLATE_TEST_DATA),
        lambda df: df.interpolate(),
    )


@pytest.mark.skip(reason=LINEAR_FAIL_REASON)
@pytest.mark.parametrize(
    "data",
    [
        param(INTERPOLATE_TEST_DATA, id="numeric"),
        param(INTERPOLATE_DATETIME_DATA, id="datetime"),
    ],
)
@pytest.mark.parametrize("limit_direction", ["forward", "backward", None])
@pytest.mark.parametrize("limit_area", ["inside", None])
@sql_count_checker(query_count=1)
def test_df_interpolate_linear(data, limit_direction, limit_area):
    eval_snowpark_pandas_result(
        *create_test_dfs(data),
        lambda df: df.interpolate(
            method="linear", limit_direction=limit_direction, limit_area=limit_area
        ),
    )


@pytest.mark.parametrize(
    "data",
    [
        param(INTERPOLATE_TEST_DATA, id="numeric"),
        param(INTERPOLATE_DATETIME_DATA, id="datetime"),
    ],
)
@pytest.mark.parametrize("method", ["ffill", "pad", "bfill", "backfill"])
@sql_count_checker(query_count=1)
def test_df_interpolate_fill(data, method):
    eval_snowpark_pandas_result(
        *create_test_dfs(data),
        lambda df: df.interpolate(method=method),
    )


@pytest.mark.parametrize(
    "kwargs, expected_error_regex",
    [
        param({"axis": 1}, "axis = 1 is not supported", id="axis=1"),
        param(
            {"method": "nearest"},
            "method = 'nearest' is not supported",
            id="unsupported_method",
        ),
        param({"limit": 1}, "limit = 1 is not supported", id="limit=1"),
        param(
            {"method": "linear", "limit_area": "outside"},
            "with limit_area = outside for method = linear",
            id="linear_outside",
        ),
        param(
            {"method": "pad", "limit_area": "outside"},
            "with limit_area = outside for method = pad",
            id="pad_outside",
        ),
        param(
            {"method": "pad", "limit_area": "inside"},
            "with limit_area = inside for method = pad",
            id="pad_inside",
        ),
        param(
            {"method": "bfill", "limit_area": "outside"},
            "with limit_area = outside for method = bfill",
            id="bfill_outside",
        ),
        param(
            {"method": "bfill", "limit_area": "inside"},
            "with limit_area = inside for method = bfill",
            id="bfill_inside",
        ),
        param(
            {"downcast": "infer"}, "downcast = 'infer' is not supported", id="downcast"
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_df_interpolate_unsupported_parameters(kwargs, expected_error_regex):
    with pytest.raises(NotImplementedError, match=expected_error_regex):
        pd.DataFrame(INTERPOLATE_TEST_DATA).interpolate(**kwargs).to_pandas()


@pytest.mark.parametrize(
    "method, limit_direction, expected_direction",
    [
        ["pad", "backward", "forward"],
        ["pad", "both", "forward"],
        ["bfill", "forward", "backward"],
        ["bfill", "both", "backward"],
    ],
)
@sql_count_checker(query_count=0)
def test_df_interpolate_bad_direction(method, limit_direction, expected_direction):
    eval_snowpark_pandas_result(
        *create_test_dfs(INTERPOLATE_TEST_DATA),
        lambda df: df.interpolate(method=method, limit_direction=limit_direction),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match=f"`limit_direction` must be '{expected_direction}' for method `{method}`",
    )


@pytest.mark.parametrize("inplace", [True, False])
@sql_count_checker(query_count=1)
def test_df_interpolate_inplace(inplace):
    eval_snowpark_pandas_result(
        *create_test_dfs(INTERPOLATE_TEST_DATA),
        lambda df: df.interpolate(method="pad", inplace=inplace),
        inplace=inplace,
    )


@sql_count_checker(query_count=1)
def test_df_interpolate_empty_rows():
    eval_snowpark_pandas_result(
        *create_test_dfs({"A": [], "B": []}),
        lambda df: df.interpolate(method="pad"),
    )


@sql_count_checker(query_count=1)
def test_df_interpolate_empty_columns():
    eval_snowpark_pandas_result(
        *create_test_dfs([]),
        lambda df: df.interpolate(method="pad"),
        check_column_type=False,
    )


@sql_count_checker(query_count=1)
def test_df_interpolate_single_rows():
    eval_snowpark_pandas_result(
        *create_test_dfs({"A": [1], "B": [np.nan]}),
        lambda df: df.interpolate(method="pad"),
    )


@pytest.mark.parametrize(
    "method, test_data",
    [
        param(
            "linear",
            native_pd.DataFrame(
                {"A": [1, np.nan, np.nan, 4]},
                index=native_pd.MultiIndex.from_arrays(
                    [["a", "a", "b", "b"], [1, 2, 1, 2]], names=["letters", "numbers"]
                ),
            ),
            id="multiindex_rows",
            marks=pytest.mark.skip(reason=LINEAR_FAIL_REASON),
        ),
        param(
            "ffill",  # non-linear methods are valid if columns are multi-index but rows are not
            native_pd.DataFrame(
                [
                    [1, np.nan, np.nan, 100],
                    [0, 2, np.nan, 4],
                    [3, np.nan, -2, np.nan],
                    [9, -1, np.nan, 9],
                ],
                columns=native_pd.MultiIndex.from_arrays(
                    [["a", "a", "b", "b"], [1, 2, 1, 2]], names=["letters", "numbers"]
                ),
            ),
            id="multiindex_columns",
        ),
        param(
            "linear",
            native_pd.DataFrame(
                [
                    [1, np.nan, np.nan, 100],
                    [0, 2, np.nan, 4],
                    [3, np.nan, -2, np.nan],
                    [9, -1, np.nan, 9],
                ],
                index=native_pd.MultiIndex.from_arrays(
                    [["a", "a", "b", "b"], [1, 2, 1, 2]], names=["letters", "numbers"]
                ),
                columns=native_pd.MultiIndex.from_arrays(
                    [["a", "a", "b", "b"], [1, 2, 1, 2]], names=["letters", "numbers"]
                ),
            ),
            id="multiindex_both",
            marks=pytest.mark.skip(reason=LINEAR_FAIL_REASON),
        ),
    ],
)
@sql_count_checker(query_count=1)
def test_df_interpolate_multiindex(method, test_data):
    eval_snowpark_pandas_result(
        *create_test_dfs(test_data),
        lambda df: df.interpolate(method=method),
    )


@sql_count_checker(query_count=0)
def test_df_interpolate_multiindex_invalid_method():
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {"A": [1, np.nan, np.nan, 4]},
            index=native_pd.MultiIndex.from_arrays(
                [["a", "a", "b", "b"], [1, 2, 1, 2]], names=["letters", "numbers"]
            ),
        ),
        lambda df: df.interpolate(method="ffill"),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="Only `method=linear` interpolation is supported on MultiIndexes.",
    )


@pytest.mark.parametrize(
    "method",
    [
        param("linear", marks=pytest.mark.skip(reason=LINEAR_FAIL_REASON)),
        "pad",
        "bfill",
    ],
)
@sql_count_checker(query_count=1)
def test_df_interpolate_unordered_index(method):
    eval_snowpark_pandas_result(
        *create_test_dfs(
            [[1, np.nan, 3, np.nan, 100]],
            index=[5, 3, 1, 2, 0],
        ),
        lambda df: df.interpolate(method),
    )


@sql_count_checker(query_count=2)
def test_df_interpolate_with_datetime_index():
    dates = native_pd.date_range("2023-01-01", periods=6, freq="D")
    data = {"value": [np.nan, 1, np.nan, np.nan, 4, np.nan]}
    eval_snowpark_pandas_result(
        *create_test_dfs(data, index=dates),
        lambda df: df.interpolate(method="pad"),
        # Snowpark pandas always drops the freq field
        check_freq=False,
    )
