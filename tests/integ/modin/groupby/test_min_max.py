#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

# tests pulled from pandas/pandas/tests/groupby/test_min_max.py
#
import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    MIXED_NUMERIC_STR_DATA_AND_TYPE,
    assert_frame_equal,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    create_test_dfs,
    eval_snowpark_pandas_result as _eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker


def eval_snowpark_pandas_result(*args, **kwargs):
    # Some calls to the native pandas function propagate attrs while some do not, depending on the values of its arguments.
    return _eval_snowpark_pandas_result(*args, test_attrs=False, **kwargs)


@sql_count_checker(query_count=0)
def test_max_min_non_numeric():
    aa = pd.DataFrame({"nn": [11, 11, 22, 22], "ii": [1, 2, 3, 4], "ss": 4 * ["mama"]})

    result = aa.groupby("nn").max()
    assert "ss" in result

    result = aa.groupby("nn").max(numeric_only=False)
    assert "ss" in result

    result = aa.groupby("nn").min()
    assert "ss" in result

    result = aa.groupby("nn").min(numeric_only=False)
    assert "ss" in result


@sql_count_checker(query_count=2)
def test_aggregate_numeric_object_dtype():
    # simplified case: multiple object columns where one is all-NaN
    # -> gets split as the all-NaN is inferred as float
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {"key": ["A", "A", "B", "B"], "col1": list("abcd"), "col2": [np.nan] * 4},
            dtype=object,
        ),
        lambda df: df.groupby("key").min(),
    )
    # same but with numbers
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {"key": ["A", "A", "B", "B"], "col1": list("abcd"), "col2": range(4)},
            dtype=object,
        ),
        lambda df: df.groupby("key").min(),
    )


@pytest.mark.parametrize(
    "agg_func",
    [
        (lambda df: df.groupby("b").min()),
        (lambda df: df.groupby("b").max()),
    ],
)
@sql_count_checker(query_count=1)
def test_min_max_date(agg_func):
    # GH26321
    dates = native_pd.to_datetime(
        native_pd.Series(["2019-05-09", "2019-05-09", "2019-05-09"]), format="%Y-%m-%d"
    ).dt.date
    eval_snowpark_pandas_result(
        *create_test_dfs({"b": [0, 1, 1], "c": dates}), agg_func
    )


@pytest.mark.parametrize(
    "agg_func",
    [
        (lambda df: df.groupby("id").min()),
        (lambda df: df.groupby("id").max()),
        (lambda df: df.groupby("id").count()),
    ],
)
@pytest.mark.parametrize(
    "dtype", ["Int64", "Int32", "Float64", "Float32", "boolean", "string"]
)
@sql_count_checker(query_count=1)
def test_groupby_min_max_nullable(agg_func, dtype):
    if dtype == "boolean":
        ts = 0
    else:
        ts = 4.0

    native_df = native_pd.DataFrame(
        {"id": [2, 2, 2, 1], "ts": [ts, pd.NA, ts + 1, pd.NA]}
    )
    native_df["ts"] = native_df["ts"].astype(dtype)

    df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(df, native_df, agg_func)


@pytest.mark.parametrize("method", ["min", "max"])
@pytest.mark.parametrize("min_count", [1, 3, 50])
@sql_count_checker(query_count=1)
def test_groupby_min_count_string_nullable(method, min_count):
    native_df = native_pd.DataFrame(
        {"id": [2, 2, 2, 1], "ts": ["a", pd.NA, "b", pd.NA]}
    )
    native_df["ts"] = native_df["ts"].astype("string")
    snow_df = pd.DataFrame(native_df)

    snowpark_pandas_groupby = snow_df.groupby("id")
    pandas_groupby = native_df.groupby("id")

    if min_count > 2:
        # min_count doesn't work with string nullable dtypes in pandas. With above test case,
        #       id  ts
        # 0     2   a
        # 1     2   <NA>
        # 2     2   b
        # 3     1   <NA>
        # after we call df.groupby("id").max(min_count=4) in native pandas we got result
        # id    ts
        # 1     <NA>
        # 2     a
        # However, the groupby with value "2" only have 2 valid record, and should have result <NA>.
        # In Snowpark, we provide the correct behavior, and will end up with following result
        # id    ts
        # 1     <NA>
        # 2     <NA>
        expected = native_pd.DataFrame({"id": [1, 2], "ts": [np.nan, np.nan]})
        expected = expected.set_index("id")
        result = getattr(snowpark_pandas_groupby, method)(min_count=min_count)
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(result, expected)
    else:
        eval_snowpark_pandas_result(
            snowpark_pandas_groupby,
            pandas_groupby,
            lambda grp: getattr(grp, method)(min_count=min_count),
        )


@sql_count_checker(query_count=2)
def test_min_max_with_mixed_str_numeric_type():
    mixed_data, _ = MIXED_NUMERIC_STR_DATA_AND_TYPE
    pandas_df = native_pd.DataFrame(
        {
            "col_grp": ["a", "a", "b", "b"],
            "col_mixed": mixed_data,
        }
    )
    snow_df = pd.DataFrame(pandas_df)
    result_max = snow_df.groupby(by="col_grp").max()
    # This behavior is different compare with native pandas, in native pandas
    # min/max comparison between string and numeric value is invalid. However,
    # it is valid with snowflake.
    expected_df = native_pd.DataFrame(
        {
            "col_grp": ["a", "b"],
            "col_mixed": ["A", 2.5],
        }
    )
    expected_df = expected_df.set_index("col_grp")
    assert_frame_equal(result_max, expected_df, check_dtype=False)

    result_min = snow_df.groupby(by="col_grp").min()
    expected_df = native_pd.DataFrame(
        {
            "col_grp": ["a", "b"],
            "col_mixed": [1.0, 2.5],
        }
    )
    expected_df = expected_df.set_index("col_grp")
    assert_frame_equal(result_min, expected_df, check_dtype=False)


@pytest.mark.parametrize("agg_func", ["min", "max"])
@pytest.mark.parametrize("by", ["A", "B"])
@sql_count_checker(query_count=1)
def test_timedelta(agg_func, by):
    native_df = native_pd.DataFrame(
        {
            "A": native_pd.to_timedelta(
                ["1 days 06:05:01.00003", "16us", "nan", "16us"]
            ),
            "B": [8, 8, 12, 10],
            "C": ["the", "name", "is", "bond"],
        }
    )
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: getattr(df.groupby(by), agg_func)()
    )
