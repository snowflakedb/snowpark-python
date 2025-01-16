#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_frame_equal, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.fixture
def left():
    return native_pd.DataFrame(
        {"a": [1, 1, 0, 4]}, index=native_pd.Index([2, 1, 0, 3], name="li")
    )


@pytest.fixture
def right():
    return native_pd.DataFrame(
        {"b": [300, 100, 200]}, index=pd.Index([3, 1, 2], name="ri")
    )


@pytest.fixture(params=["left", "inner", "right", "outer"])
def how(request):
    """
    how keyword to pass to merge.
    """
    return request.param


@pytest.fixture(params=[True, False])
def sort(request):
    """
    sort keyword to pass to merge.
    """
    return request.param


@sql_count_checker(query_count=2, join_count=2)
def test_join_index_to_index(left, right, how, sort):
    left, right = pd.DataFrame(left), pd.DataFrame(right)
    result = left.join(right, how=how, sort=sort)
    expected = left.merge(right, left_index=True, right_index=True, how=how, sort=sort)
    assert_frame_equal(result, expected)


@sql_count_checker(query_count=2, join_count=2)
def test_join_column_to_index(left, right, how, sort):
    left, right = pd.DataFrame(left), pd.DataFrame(right)
    result = left.join(right, on="a", how=how, sort=sort)
    expected = left.merge(right, left_on="a", right_index=True, how=how, sort=sort)
    assert_frame_equal(result, expected)


@sql_count_checker(query_count=0)
def test_join_list_with_on_negative(left, right):
    eval_snowpark_pandas_result(
        pd.DataFrame(left),
        left,
        lambda df: df.join(
            [pd.DataFrame(right) if isinstance(df, pd.DataFrame) else right], on="a"
        ),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="Joining multiple DataFrames only supported for joining on index",
    )


@sql_count_checker(query_count=2, join_count=2)
def test_join_suffix_on_list_negative():
    first = pd.DataFrame({"key": [1, 2, 3, 4, 5]})
    second = pd.DataFrame({"key": [1, 8, 3, 2, 5], "v1": [1, 2, 3, 4, 5]})
    third = pd.DataFrame({"keys": [5, 2, 3, 4, 1], "v2": [1, 2, 3, 4, 5]})

    # check proper errors are raised
    msg = "Suffixes not supported when joining multiple DataFrames"
    with pytest.raises(ValueError, match=msg):
        first.join([second], lsuffix="y")
    with pytest.raises(ValueError, match=msg):
        first.join([second, third], rsuffix="x")
    with pytest.raises(ValueError, match=msg):
        first.join([second, third], lsuffix="y", rsuffix="x")
    with pytest.raises(
        ValueError, match="Join dataframes have overlapping column labels"
    ):
        first.join([second, third])

    # no errors should be raised
    arr_joined = first.join([third])
    norm_joined = first.join(third)
    assert_frame_equal(arr_joined, norm_joined)


@pytest.mark.parametrize(
    "lsuffix, rsuffix", [("_left", None), (None, "_right"), ("_left", "_right")]
)
@sql_count_checker(query_count=2, join_count=2)
def test_join_overlapping_columns(left, lsuffix, rsuffix):
    left = pd.DataFrame(left)
    result = left.join(left, how="left", lsuffix=lsuffix, rsuffix=rsuffix)
    expected = left.merge(
        left, how="left", left_index=True, right_index=True, suffixes=(lsuffix, rsuffix)
    )
    assert_frame_equal(result, expected)


@sql_count_checker(query_count=0)
def test_join_overlapping_columns_negative(left):
    eval_snowpark_pandas_result(
        pd.DataFrame(left),
        left,
        lambda df: df.join(df),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="columns overlap but no suffix",
    )


@sql_count_checker(query_count=0)
def test_join_invalid_how_negative(left):
    eval_snowpark_pandas_result(
        pd.DataFrame(left),
        left,
        lambda df: df.join(df, how="full_outer_join"),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="do not recognize join method full_outer_join",
    )


@sql_count_checker(query_count=2, join_count=2)
def test_join_with_series(left):
    left = pd.DataFrame(left)
    right = pd.Series([1, 0, 2], name="s")
    result = left.join(right)
    expected = left.merge(right, left_index=True, right_index=True, how="left")
    assert_frame_equal(result, expected)


@sql_count_checker(query_count=0)
def test_join_unnamed_series_negative(left):
    right = native_pd.Series([1, 0, 2])
    eval_snowpark_pandas_result(
        pd.DataFrame(left),
        left,
        lambda df: df.join(pd.Series(right) if isinstance(df, pd.DataFrame) else right),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="Other Series must have a name",
    )


@sql_count_checker(query_count=0)
def test_join_unnamed_series_in_list_negative(left):
    right = pd.Series([1, 0, 2])
    with pytest.raises(ValueError, match="Other Series must have a name"):
        pd.DataFrame(left).join([right])


@sql_count_checker(query_count=2, join_count=4)
def test_join_list_mixed(left, right):
    # Join a DataFrame with a list containing both a Series and a DataFrame
    left, right = pd.DataFrame(left), pd.DataFrame(right)
    series = pd.Series([1, 2, 3], name="s")
    other = [right, series]
    result = left.join(other)
    expected = left.join(right).join(series)
    assert_frame_equal(result, expected)


@sql_count_checker(query_count=4, join_count=4)
def test_join_empty_rows(left, right, how):
    left, right = pd.DataFrame(left), pd.DataFrame(right)
    empty_df = pd.DataFrame(columns=["x", "y"])
    # empty on left
    result = left.join(empty_df, how=how)
    expected = left.merge(empty_df, how=how, left_index=True, right_index=True)
    assert_frame_equal(result, expected)
    # empty on right
    result = empty_df.join(right, how=how)
    expected = empty_df.merge(right, how=how, left_index=True, right_index=True)
    assert_frame_equal(result, expected)


@sql_count_checker(query_count=4, join_count=4)
def test_join_empty_columns(left, right, how):
    left, right = pd.DataFrame(left), pd.DataFrame(right)
    empty_df = pd.DataFrame(native_pd.Index([1, 2, 3]))
    # empty on left
    result = left.join(empty_df, how=how)
    expected = left.merge(empty_df, how=how, left_index=True, right_index=True)
    assert_frame_equal(result, expected)
    # empty on right
    result = empty_df.join(right, how=how)
    expected = empty_df.merge(right, how=how, left_index=True, right_index=True)
    assert_frame_equal(result, expected)


@sql_count_checker(query_count=0)
def test_join_different_levels_negative(left):
    # second dataframe
    columns = native_pd.MultiIndex.from_tuples([("b", ""), ("c", "c1")])
    right = pd.DataFrame(columns=columns, data=[[1, 33], [0, 44]])

    with pytest.raises(
        ValueError, match="Can not merge objects with different column levels"
    ):
        pd.DataFrame(left).join(right)


@sql_count_checker(query_count=2, join_count=2)
def test_cross_join(left, right):
    left, right = pd.DataFrame(left), pd.DataFrame(right)
    result = left.join(right, how="cross")
    expected = left.merge(right, how="cross")
    assert_frame_equal(result, expected)


@pytest.mark.parametrize(
    "lvalues, rvalues, validate",
    # 'one' should also validate as 'many'. If actual join is one-to-one
    # validation for '1:1', '1:m', 'm:1' and 'm:m' should succeed.
    # Similarly, if actual join is '1:m' validation for both '1:m' and 'm:m' should
    # succeed.
    [
        ([1, 2, 3], [4, 3, 1], "1:1"),  # 1:1 join
        ([1, 2, 3], [4, 3, 1], "1:m"),  # 1:1 join
        ([1, 2, 3], [4, 3, 1], "m:1"),  # 1:1 join
        ([1, 2, 3], [4, 3, 1], "m:m"),  # 1:1 join
        ([1, 2, 3], [1, 3, 1], "1:m"),  # 1:m join
        ([1, 2, 3], [1, 3, 1], "m:m"),  # 1:m join
        ([1, 2, 1], [2, 3, 1], "m:1"),  # m:1 join
        ([1, 2, 1], [2, 3, 1], "m:m"),  # m:1 join
        ([1, 2, 1], [2, 3, 2], "m:m"),  # m:m join
    ],
)
@sql_count_checker(query_count=0)
def test_join_validate(lvalues, rvalues, validate):
    left = pd.DataFrame({"A": [1, 1, 2]}, index=lvalues)
    right = pd.DataFrame({"B": [1, 4, 2]}, index=rvalues)
    msg = "Snowpark pandas merge API doesn't yet support 'validate' parameter"
    with pytest.raises(NotImplementedError, match=msg):
        left.join(right, validate=validate)


@pytest.mark.parametrize(
    "lvalues, rvalues, validate",
    [
        ([1, 2, 3], [1, 3, 1], "1:1"),  # 1:m join
        ([1, 2, 3], [1, 3, 1], "m:1"),  # 1:m join
        ([1, 2, 1], [2, 3, 1], "1:1"),  # m:1 join
        ([1, 2, 1], [2, 3, 1], "1:m"),  # m:1 join
        ([1, 2, 1], [2, 3, 2], "1:1"),  # m:m join
        ([1, 2, 1], [2, 3, 2], "1:m"),  # m:m join
        ([1, 2, 1], [2, 3, 2], "m:1"),  # m:m join
    ],
)
@sql_count_checker(query_count=0)
def test_join_validate_negative(lvalues, rvalues, validate):
    left = pd.DataFrame({"A": [1, 1, 2]}, index=lvalues)
    right = pd.DataFrame({"B": [1, 4, 2]}, index=rvalues)
    msg = "Snowpark pandas merge API doesn't yet support 'validate' parameter"
    with pytest.raises(NotImplementedError, match=msg):
        left.join(right, validate=validate)


@sql_count_checker(query_count=2, join_count=2)
def test_join_timedelta(left, right):
    right = right.astype("timedelta64[ns]")
    eval_snowpark_pandas_result(
        pd.DataFrame(left),
        left,
        lambda df: df.join(
            pd.DataFrame(right) if isinstance(df, pd.DataFrame) else right
        ),
    )
    left = left.astype("timedelta64[ns]")
    eval_snowpark_pandas_result(
        pd.DataFrame(left),
        left,
        lambda df: df.join(
            pd.DataFrame(right) if isinstance(df, pd.DataFrame) else right
        ),
    )
