#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker

# TODO: SNOW-782594 Add tests for categorical data.


@pytest.fixture(scope="function")
def snow_df():
    return pd.DataFrame(
        {
            "A": [1, 10, -1, 100, 0, -11],
            "B": [321, 312, 123, 132, 231, 213],
            "a": ["abc", " ", "", "ABC", "_", "XYZ"],
            "b": ["1", "10", "xyz", "0", "2", "abc"],
        },
        index=pd.Index([1, 2, 3, 4, 5, 6], name="ind"),
    )


@pytest.fixture(scope="function")
def snow_series(snow_df):
    return pd.Series([1, 10, -1, 100, 0, -11], name="A")


@pytest.mark.parametrize("by", ["A", "B", "a", "b"])
@pytest.mark.parametrize("ascending", [True, False])
@sql_count_checker(query_count=3)
def test_sort_values(snow_df, by, ascending):
    snow_series = snow_df[by]
    native_series = snow_series.to_pandas()
    eval_snowpark_pandas_result(
        snow_series, native_series, lambda s: s.sort_values(ascending=ascending)
    )
    eval_snowpark_pandas_result(
        snow_series, native_series, lambda s: s.sort_values(ascending=[ascending])
    )


@sql_count_checker(query_count=1)
def test_sort_values_by_ascending_length_mismatch_negative(snow_series):
    eval_snowpark_pandas_result(
        snow_series,
        snow_series.to_pandas(),
        lambda s: s.sort_values(ascending=[True] * 5),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match=r"Length of ascending \(5\) must be 1 for Series",
    )


@pytest.mark.parametrize("axis", [1, 2])
@sql_count_checker(query_count=1)
def test_sort_values_invalid_axis_negative(snow_series, axis):
    eval_snowpark_pandas_result(
        snow_series,
        snow_series.to_pandas(),
        lambda s: s.sort_values(axis=axis),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match=f"No axis named {axis} for object type Series",
    )


@pytest.mark.parametrize("ascending", [True, False])
@sql_count_checker(query_count=2)
def test_sort_values_inplace(snow_series, ascending):
    eval_snowpark_pandas_result(
        snow_series,
        snow_series.to_pandas().copy(),
        lambda s: s.sort_values(ascending=ascending, inplace=True),
        inplace=True,
    )


@pytest.mark.parametrize("ascending", [True, False])
@pytest.mark.parametrize("na_position", ["first", "last"])
@sql_count_checker(query_count=2)
def test_sort_values_nan(ascending, na_position):
    snow_series = pd.Series([11, 2, np.nan, 1, np.nan, None, 8], name="A")

    eval_snowpark_pandas_result(
        snow_series,
        snow_series.to_pandas(),
        lambda s: s.sort_values(ascending=ascending, na_position=na_position),
    )


@sql_count_checker(query_count=1)
def test_sort_values_invalid_na_position_negative(snow_series):
    eval_snowpark_pandas_result(
        snow_series,
        snow_series.to_pandas(),
        lambda s: s.sort_values(na_position="nulls_first"),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="invalid na_position: nulls_first",
    )


@pytest.mark.parametrize("ascending", [True, False])
@pytest.mark.parametrize("kind", ["stable", "mergesort"])
@sql_count_checker(query_count=2)
def test_sort_values_stable(ascending, kind):
    data = [3] * 10 + [1] * 10 + [2] * 10
    snow_series = pd.Series(data, name="A")

    eval_snowpark_pandas_result(
        snow_series,
        snow_series.to_pandas(),
        lambda s: s.sort_values(ascending=ascending, kind=kind),
    )


@sql_count_checker(query_count=0)
def test_sort_values_invalid_kind_negative(snow_series):
    invalid_kind = "fastsort"
    # We have slightly different error message as compared to nativ pandas.
    msg = r"sort kind must be 'stable' or None \(got 'fastsort'\)"
    with pytest.raises(ValueError, match=msg):
        snow_series.sort_values(kind=invalid_kind)


@sql_count_checker(query_count=1)
def test_sort_values_datetime():
    data = [
        native_pd.Timestamp(x)
        for x in [
            "2004-02-11",
            "2004-01-21",
            "2004-01-26",
            "2005-09-20",
            "2010-10-04",
            "2009-05-12",
            "2008-11-12",
            "2010-09-28",
            "2010-09-28",
        ]
    ]
    native_series = native_pd.Series(data, name="A")

    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(snow_series, native_series, lambda s: s.sort_values())


@pytest.mark.parametrize("ascending", [True, False])
@pytest.mark.parametrize("ignore_index", [True, False])
@sql_count_checker(query_count=2)
def test_sort_values_ignore_index(snow_series, ascending, ignore_index):
    eval_snowpark_pandas_result(
        snow_series,
        snow_series.to_pandas(),
        lambda s: s.sort_values(ascending=ascending, ignore_index=ignore_index),
    )


@sql_count_checker(query_count=0)
def test_sort_values_key(snow_series):
    msg = "Snowpark pandas sort_values API doesn't yet support 'key' parameter"
    with pytest.raises(NotImplementedError, match=msg):
        snow_series.sort_values(key=lambda x: x + 5)
    with pytest.raises(NotImplementedError, match=msg):
        snow_series.sort_values(key=lambda x: -x)


@sql_count_checker(query_count=2)
def test_sort_values_repeat(snow_series):
    eval_snowpark_pandas_result(
        snow_series,
        snow_series.to_pandas(),
        lambda s: s.sort_values().sort_values(ascending=False),
    )


@sql_count_checker(query_count=1)
def test_sort_values_shared_name_with_index():
    # Bug fix: SNOW-1649780
    native_series = native_pd.Series(
        [1, 3, 2], name="X", index=native_pd.Index([2, 1, 3], name="X")
    )
    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(snow_series, native_series, lambda s: s.sort_values())
