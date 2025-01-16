#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd

# This file contains tests for pd.merge.  pd.merge is just a wrapper on top of
# DataFrame.merge method, so we didn't duplicate all test cases but only
# write tests for differences.
# Only difference between these APIs is that pd.merge allows a Series on left side of
# merge but DataFrame.merge can only have DataFrame on left side of merge.
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_frame_equal
from tests.integ.utils.sql_counter import sql_count_checker
from tests.utils import multithreaded_run


@pytest.fixture(scope="function")
def left_df():
    return native_pd.DataFrame(
        {
            "A": [3, 2, 1, 4, 4],
            "B": [2, 3, 1, 2, 1],
        },
        index=native_pd.Index([0, 1, 3, 2, 4], name="left_i"),
    )


@pytest.fixture(scope="function")
def right_df():
    return native_pd.DataFrame(
        {
            "A": [4, 3, 1, 4, 4],
            "C": [3, 4, 2, 1, 1],
        },
        index=native_pd.Index([8, 4, 2, 9, 1], name="right_i"),
    )


@pytest.fixture(scope="function")
def unnamed_series():
    return native_pd.Series([1, 2, 3])


@pytest.fixture(scope="function")
def named_series():
    return native_pd.Series([1, 2, 3], name="S")


@pytest.fixture(params=["left", "inner", "right", "outer"])
def how(request):
    """
    how keyword to pass to merge.
    """
    return request.param


@multithreaded_run()
@sql_count_checker(query_count=2, join_count=2)
def test_merge(left_df, right_df, how):
    left_df, right_df = pd.DataFrame(left_df), pd.DataFrame(right_df)
    res = pd.merge(left_df, right_df, on="A", how=how)
    expected = left_df.merge(right_df, on="A", how=how)
    assert_frame_equal(res, expected)


@sql_count_checker(query_count=2, join_count=2)
def test_merge_series_on_left(named_series, right_df, how):
    named_series, right_df = pd.Series(named_series), pd.DataFrame(right_df)
    res = pd.merge(named_series, right_df, left_on="S", right_on="A", how=how)
    expected = named_series.to_frame().merge(
        right_df, left_on="S", right_on="A", how=how
    )
    assert_frame_equal(res, expected)


@sql_count_checker(query_count=0)
def test_merge_unnamed_series_negative(unnamed_series, right_df):
    with pytest.raises(ValueError) as pd_e:
        native_pd.merge(unnamed_series, right_df)
    unnamed_series, right_df = pd.Series(unnamed_series), pd.DataFrame(right_df)
    with pytest.raises(ValueError) as snow_e:
        pd.merge(unnamed_series, right_df)
    assert str(pd_e.value) == str(snow_e.value)


@sql_count_checker(query_count=0)
def test_merge_native_pandas_object_negative(left_df, right_df):
    right_df = pd.DataFrame(right_df)
    msg = (
        f"{type(left_df)} is not supported as 'value' argument. Please convert this to Snowpark pandas"
        r" objects by calling modin.pandas.Series\(\)/DataFrame\(\)"
    )
    # Left frame as native pandas object
    with pytest.raises(TypeError, match=msg):
        pd.merge(left_df, right_df, on="A")

    # right frame as native pandas object
    with pytest.raises(TypeError, match=msg):
        pd.merge(right_df, left_df, on="A")


@sql_count_checker(query_count=0)
def test_merge_invalid_object_type_negative(left_df):
    right_df = "abc"
    with pytest.raises(TypeError) as pd_e:
        native_pd.merge(left_df, right_df)
    with pytest.raises(TypeError) as snow_e:
        pd.merge(pd.DataFrame(left_df), right_df)
    assert str(pd_e.value) == str(snow_e.value)
