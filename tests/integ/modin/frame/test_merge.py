#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from collections.abc import Hashable
from typing import Optional, Union

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas._typing import AnyArrayLike, IndexLabel
from pandas.errors import MergeError

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.utils import (
    assert_frame_equal,
    assert_snowpark_pandas_equal_to_pandas,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.fixture
def empty_df():
    return pd.DataFrame(columns=["A", "B"], dtype="int64", index=pd.Index([], name="i"))


@pytest.fixture
def left_df():
    return pd.DataFrame(
        {
            "A": [3, 2, 1, 4, 4],
            "B": [2, 3, 1, 2, 1],
            "left_c": [1.0, 2.0, 3.0, 4.0, np.nan],
            "left_d": [None, "d", "a", "c", "b"],
        },
        index=pd.Index([0, 1, 3, 2, 4], name="left_i"),
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


@pytest.fixture
def right_df():
    return pd.DataFrame(
        {
            "A": [4, 3, 1, 4, 4],
            "B": [3, 4, 2, 1, 1],
            "right_c": [2.0, 1.0, 4.0, 0.0, np.nan],
            "right_d": ["c", "d", "a", "b", None],
        },
        index=pd.Index([8, 4, 2, 9, 1], name="right_i"),
    )


def _merge_native_pandas_frames_on_index_on_both_sides(
    left: native_pd.DataFrame, right: native_pd.DataFrame, how: str, sort: bool
) -> native_pd.DataFrame:
    """
    Merge left and right on index.

    Args:
        left: Left DataFrame to join
        right: Right DataFrame or Series to join
        how: Type of merge to be performed.

    Returns:
        Merged dataframe.

    """
    native_res = left.merge(
        right, left_index=True, right_index=True, how=how, sort=sort
    )

    is_left_mi = left.index.nlevels > 1
    is_right_mi = right.index.nlevels > 1
    if sort:
        # When multi-index is involved on either (or both) side of frames, native pandas
        # doesn't respect 'sort' argument. It always behaves as sort=False.
        # We need to sort explicitly to compare results with Snowpark pandas result.
        if (is_left_mi and not is_right_mi) or (not is_left_mi and is_right_mi):
            join_key = left.index.name if is_right_mi else right.index.name
            native_res = (
                native_res.reset_index(level=join_key, drop=False)
                .sort_values(join_key, kind="stable")
                .set_index([join_key], append=True)
            )
        if (
            is_left_mi
            and is_right_mi
            and set(left.index.names).intersection(set(right.index.names))
        ):
            # sort on common index columns.
            levels = [name for name in left.index.names if name in right.index.names]
            native_res = native_res.sort_index(
                level=levels, sort_remaining=False, kind="stable"
            )

    # Index column name in merged frame is pretty inconsistent in native pandas.
    # In some cases it is inherited from left frame and in some cases its set to None.
    # In Snowpark pandas we provide consistent behavior by always populating index columns
    # names from left frame.
    # Fix index names before comparison.
    if not is_left_mi and not is_right_mi:
        native_res = native_res.rename_axis(left.index.names)
    return native_res


def _merge_native_pandas_frames_on_index_on_one_side(
    left: native_pd.DataFrame,
    right: native_pd.DataFrame,
    how: str,
    left_on: Optional[Union[IndexLabel, AnyArrayLike]] = None,
    right_on: Optional[Union[IndexLabel, AnyArrayLike]] = None,
    left_index: Optional[bool] = False,
    right_index: Optional[bool] = False,
    sort: Optional[bool] = False,
) -> native_pd.DataFrame:
    """
    Merge left and right where either left_index or right_index is True but not both.

    Args:
        left: Left DataFrame to join
        right: Right DataFrame or Series to join
        how: Type of merge to be performed.
        left_on: Columns to join on in left DataFrame.
        right_on: Columns ot join on in right DataFrame.
        left_index: If True, use index from left DataFrame as join keys.
        right_index: If True, use index from right DataFrame as join keys.

    Returns:
        Merged dataframe.
    """
    # Merging frames when 'left_index' or 'right_index' is True is full of bugs in
    # native pandas. Few of them are:
    # https://github.com/pandas-dev/pandas/issues/17257
    # Right/outer merge behavior on left column and right index is unexpected
    #
    # https://github.com/pandas-dev/pandas/issues/28243
    # Left join on index and column gives incorrect output
    #
    # https://github.com/pandas-dev/pandas/issues/33232
    # merge() outer with left_on column and right_index=True produces unexpected results
    #
    # So to compare results against Snowpark pandas, we merge native frames
    # by providing index column names explicitly.
    left_on = left.index.names if left_index else left_on
    right_on = right.index.names if right_index else right_on

    # Merging frames with 'left_on' and 'right_on' will reset the index. But with
    # either left_index or right_index being True expectation is that index values
    # should be inherited from left frame. So we move index columns to data columns
    # before merging the frames and recover index by calling set_index after merge.
    left_index_names = left.index.names
    left = left.reset_index(drop=False)

    native_res = left.merge(
        right,
        how=how,
        left_on=left_on,
        right_on=right_on,
        sort=sort,
    )

    # Recover index values.
    return native_res.set_index(left_index_names)


def _verify_merge(
    left: pd.DataFrame,
    right: Union[pd.DataFrame, pd.Series],
    how: str,
    *,
    on: Optional[IndexLabel] = None,
    left_on: Optional[Union[IndexLabel, AnyArrayLike]] = None,
    right_on: Optional[Union[IndexLabel, AnyArrayLike]] = None,
    left_index: Optional[bool] = False,
    right_index: Optional[bool] = False,
    force_output_column_order: Optional[list[Hashable]] = None,
    sort: Optional[bool] = False,
    indicator: Optional[Union[bool, str]] = False,
) -> None:
    """
    To avoid comparison failure due to bugs in Native pandas we perform some custom
    operation after merge.
    Some example bugs:

    https://github.com/pandas-dev/pandas/issues/46225
    outer join out of order when joining multiple DataFrames

    Args:
        left: Left DataFrame to join
        right: Right DataFrame or Series to join
        how: Type of merge to be performed.
        on: Columns to join on.
        left_on: Columns to join on in left DataFrame.
        right_on: Columns ot join on in right DataFrame.
        left_index: If True, use index from left DataFrame as join keys.
        right_index: If True, use index from right DataFrame as join keys.
        force_output_column_order: If provided, reorder native result using this list.
        indicator: If True, include indicator column.

    Returns:
        None
    """
    left_native = left.to_pandas()
    right_native = right.to_pandas()

    if left_index and right_index:
        native_res = _merge_native_pandas_frames_on_index_on_both_sides(
            left_native, right_native, how, sort=sort
        )
    elif left_index or right_index:
        native_res = _merge_native_pandas_frames_on_index_on_one_side(
            left_native,
            right_native,
            how,
            left_on,
            right_on,
            left_index,
            right_index,
            sort=sort,
        )
    else:
        native_res = left_native.merge(
            right_native,
            how=how,
            on=on,
            left_on=left_on,
            right_on=right_on,
            left_index=left_index,
            right_index=right_index,
            sort=sort,
            indicator=indicator,
        )

    if force_output_column_order:
        native_res = native_res.reindex(columns=force_output_column_order)

    snow_res = left.merge(
        right,
        how=how,
        on=on,
        left_on=left_on,
        right_on=right_on,
        left_index=left_index,
        right_index=right_index,
        sort=sort,
        indicator=indicator,
    )

    if indicator:
        # Native pandas returns categorical column. Snowflake backend doesn't support
        # categorical columns so snowpark pandas will create a column of string type.
        # Change native pandas indicator column type to string before comparison.
        indicator_col_label = native_res.columns[-1]
        native_res[indicator_col_label] = native_res[indicator_col_label].astype("str")
    assert_snowpark_pandas_equal_to_pandas(
        snow_res, native_res, check_dtype=False, check_index_type=False
    )


@pytest.mark.parametrize("on", ["A", "B", ["A", "B"], ("A", "B")])
@sql_count_checker(query_count=3, join_count=1)
def test_merge_on(left_df, right_df, on, how, sort):
    _verify_merge(left_df, right_df, how, on=on, sort=sort)


@pytest.mark.parametrize("on", ["left_i", "right_i"])
@sql_count_checker(query_count=3, join_count=1)
def test_merge_on_index_columns(left_df, right_df, how, on, sort):
    # Change left_df to: columns=["right_i", "B", "left_c", "left_d"] index=["left_i"]
    left_df = left_df.rename(columns={"A": "right_i"})
    # Change right_df to: columns=["left_i", "B", "right_c", "right_d"] index=["right_i"]
    right_df = right_df.rename(columns={"A": "left_i"})
    eval_snowpark_pandas_result(
        left_df,
        left_df.to_pandas(),
        lambda df: df.merge(
            right_df if isinstance(df, pd.DataFrame) else right_df.to_pandas(),
            on=on,
            how=how,
            sort=sort,
        ),
    )


@pytest.mark.parametrize("index1", [[3, 4], [1.5, 8.0], [None, None]])
@pytest.mark.parametrize("index2", [[7, 8], [1.5, 3.0], [None, None]])
@sql_count_checker(query_count=3, join_count=1)
def test_join_type_mismatch(index1, index2):
    df1 = pd.DataFrame({"A": [1, 2]}, index=index1)
    df2 = pd.DataFrame({"B": [3, 4]}, index=index2)
    _verify_merge(df1, df2, "outer", left_index=True, right_index=True)


@pytest.mark.parametrize(
    "index1, index2",
    [
        ([3, 4], ["a", "b"]),
        (["a", "b"], [1.5, 8.0]),
        ([True, False], ["a", "b"]),
    ],
)
@sql_count_checker(query_count=0)
def test_join_type_mismatch_negative(index1, index2):
    df1 = pd.DataFrame({"A": [1, 2]}, index=index1)
    df2 = pd.DataFrame({"B": [3, 4]}, index=index2)
    with pytest.raises(SnowparkSQLException, match="value 'a' is not recognized"):
        df1.merge(df2, how="outer", left_index=True, right_index=True).to_pandas()


@pytest.mark.parametrize(
    "index1, index2, expected_res",
    [
        # integer and boolean, Snowflake is able to convert integer to bool where everything > 0 is True, and
        # then perform the join. However, native pandas does not convert integer to bool, and produces a
        # different result.
        (
            [3, 4],
            [True, False],
            native_pd.DataFrame(
                {"A": [np.nan, 1.0, 2.0], "B": [4, 3, 3]},
                index=native_pd.Index([False, True, True]),
            ),
        ),
        # string and bool, Snowflake converts bool to string, and then performs the join. However, native pandas
        # doesn't covert bool to string.
        (
            ["a", "b"],
            [True, False],
            native_pd.DataFrame(
                {"A": [1.0, 2.0, np.nan, np.nan], "B": [np.nan, np.nan, 4.0, 3.0]},
                index=native_pd.Index(["a", "b", "false", "true"]),
            ),
        ),
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_join_type_mismatch_diff_with_native_pandas(index1, index2, expected_res):
    df1 = pd.DataFrame({"A": [1, 2]}, index=index1)
    df2 = pd.DataFrame({"B": [3, 4]}, index=index2)

    snow_res = df1.merge(df2, how="outer", left_index=True, right_index=True)
    assert_frame_equal(snow_res, expected_res, check_dtype=False)


@pytest.mark.parametrize("on", ["A", "B", "C"])
@sql_count_checker(query_count=3, join_count=1)
def test_merge_on_index_columns_with_multiindex(left_df, right_df, how, on, sort):
    # Change left_df to: columns = ["C", "left_d"] index = ["A", "B"]
    left_df = left_df.rename(columns={"left_c": "C"}).set_index(["A", "B"])
    # Change right_df to: columns = ["A", "B"] index = ["C", "right_d"]
    right_df = right_df.rename(columns={"right_c": "C"}).set_index(["C", "right_d"])
    _verify_merge(left_df, right_df, how, on=on, sort=sort)


@sql_count_checker(query_count=3, join_count=1)
def test_merge_on_multiindex_with_non_multiindex(left_df, right_df, how, sort):
    # Change left_df to: columns = ["A", "B"] index = ["left_c", "left_d"]
    left_df = left_df.set_index(["left_c", "left_d"])
    _verify_merge(left_df, right_df, how, on="A", sort=sort)


@pytest.mark.parametrize(
    "left_on, right_on",
    [
        ("B", "B"),
        ("left_c", "right_c"),
        ("left_d", "right_d"),
        (["A", "left_c"], ["B", "right_c"]),
        (("A", "left_c"), ("B", "right_c")),  # tuple
        (["left_d", "A"], ["right_d", "A"]),
        ("left_i", "right_i"),  # joining index on index
        ("left_i", "A"),  # joining index on column
        ("B", "right_i"),  # joining column on index
        (["A", "left_i"], ["B", "right_i"]),  # Mix of index and data join keys
    ],
)
@sql_count_checker(query_count=3, join_count=1)
def test_merge_left_on_right_on(left_df, right_df, how, left_on, right_on, sort):
    _verify_merge(left_df, right_df, how, left_on=left_on, right_on=right_on, sort=sort)


@pytest.mark.parametrize("left_on", ["left_i", "A", "B"])
@sql_count_checker(query_count=3, join_count=1)
def test_merge_left_on_right_index(left_df, right_df, how, left_on, sort):
    _verify_merge(left_df, right_df, how, left_on=left_on, right_index=True, sort=sort)


@pytest.mark.parametrize("right_on", ["right_i", "A", "B"])
@sql_count_checker(query_count=3, join_count=1)
def test_merge_left_index_right_on(left_df, right_df, how, right_on, sort):
    _verify_merge(left_df, right_df, how, left_index=True, right_on=right_on, sort=sort)


@sql_count_checker(query_count=3, join_count=1)
def test_merge_on_index_single_index(left_df, right_df, how, sort):
    _verify_merge(left_df, right_df, how, left_index=True, right_index=True, sort=sort)


@sql_count_checker(query_count=3, join_count=1)
def test_merge_on_index_multiindex_common_labels(left_df, right_df, how, sort):
    left_df = left_df.set_index("A", append=True)  # index columns ['left_i', 'A']
    right_df = right_df.set_index("A", append=True)  # index columns ['right_i', 'A']
    _verify_merge(
        left_df, right_df, how=how, left_index=True, right_index=True, sort=sort
    )


@pytest.mark.xfail(
    reason="pandas bug: https://github.com/pandas-dev/pandas/issues/58721",
    strict=True,
)
def test_merge_on_index_multiindex_common_labels_with_none(
    left_df, right_df, how, sort
):
    # index columns [None, 'A']
    with SqlCounter(query_count=4):
        left_df = left_df.set_index("A", append=True).rename_axis([None, "A"])

    # index columns ['A', None]
    with SqlCounter(query_count=4):
        right_df = right_df.set_index("A", append=True).rename_axis(["A", None])

    with SqlCounter(query_count=3, join_count=9):
        _verify_merge(
            left_df, right_df, how=how, left_index=True, right_index=True, sort=sort
        )


@sql_count_checker(query_count=3, join_count=1)
def test_merge_on_index_multiindex_equal_labels(left_df, right_df, how, sort):
    # index columns ['A', 'B]
    left_df = left_df.set_index(["A", "B"])
    # index columns ['A', 'B]
    right_df = right_df.set_index(["A", "B"])
    _verify_merge(
        left_df, right_df, how=how, left_index=True, right_index=True, sort=sort
    )


def test_merge_left_index_right_index_single_to_multi(left_df, right_df, how, sort):
    right_df = right_df.rename(columns={"A": "left_i"}).set_index(
        "left_i", append=True
    )  # index columns ['right_i', 'left_i']
    if how in ("inner", "right"):
        if how == "inner" and sort is False:
            pytest.skip("pandas bug: https://github.com/pandas-dev/pandas/issues/55774")
        else:
            with SqlCounter(query_count=3, join_count=1):
                _verify_merge(
                    left_df,
                    right_df,
                    how=how,
                    left_index=True,
                    right_index=True,
                    sort=sort,
                )
    else:  # left and outer join
        # When joining single index with multi index, in native pandas 'left' join
        # behaves as 'inner' join and 'outer' join behaves as 'right' join.
        # https://github.com/pandas-dev/pandas/issues/34292
        # https://github.com/pandas-dev/pandas/issues/49516

        # We verify index column names explicitly.
        snow_res = left_df.merge(
            right_df, how=how, left_index=True, right_index=True, sort=sort
        )
        assert snow_res.index.names == ["right_i", "left_i"]
        # Join pandas frames using 'on' to verify snow_res values.
        native_res = (
            left_df.to_pandas()
            .merge(right_df.to_pandas(), how=how, on="left_i", sort=sort)
            .reset_index(drop=True)
        )
        with SqlCounter(query_count=1, join_count=1):
            assert_snowpark_pandas_equal_to_pandas(
                snow_res.reset_index(drop=True), native_res
            )


def test_merge_left_index_right_index_multi_to_single(left_df, right_df, how, sort):
    left_df = left_df.rename(columns={"A": "right_i"}).set_index(
        "right_i", append=True
    )  # index columns ['left_i', 'right_i']
    if how in ("left", "inner"):
        with SqlCounter(query_count=3, join_count=1):
            _verify_merge(
                left_df, right_df, how=how, left_index=True, right_index=True, sort=sort
            )
    else:  # right and outer join
        # When joining multi index with single index, in native pandas 'right' join
        # behaves as 'inner' join and 'outer' join behaves as 'left' join.

        # We verify index column names explicitly.
        snow_res = left_df.merge(
            right_df, how=how, left_index=True, right_index=True, sort=sort
        )
        assert snow_res.index.names == ["left_i", "right_i"]
        # Join pandas frames using 'on' to verify snow_res values.
        native_res = (
            left_df.to_pandas()
            .merge(right_df.to_pandas(), how=how, on="right_i", sort=sort)
            .reset_index(drop=True)
        )
        with SqlCounter(query_count=1, join_count=1):
            assert_snowpark_pandas_equal_to_pandas(
                snow_res.reset_index(drop=True), native_res
            )


@sql_count_checker(query_count=2)
def test_merge_left_index_right_index_no_common_names_negative(left_df, right_df):
    left_df = left_df.set_index("B", append=True)  # index columns ['left_i', 'B']
    right_df = right_df.set_index("A", append=True)  # index columns ['right_i', 'A']
    eval_snowpark_pandas_result(
        left_df,
        left_df.to_pandas(),
        lambda df: df.merge(
            right_df if isinstance(df, pd.DataFrame) else right_df.to_pandas(),
            left_index=True,
            right_index=True,
        ),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="cannot join with no overlapping index names",
    )


@sql_count_checker(query_count=2)
def test_merge_left_index_right_index_none_as_common_label_negative(left_df, right_df):
    # index columns [None, 'B']
    left_df = left_df.reset_index(drop=True).set_index("B", append=True)
    # index columns [None, 'A']
    right_df = right_df.reset_index(drop=True).set_index("A", append=True)
    eval_snowpark_pandas_result(
        left_df,
        left_df.to_pandas(),
        lambda df: df.merge(
            right_df if isinstance(df, pd.DataFrame) else right_df.to_pandas(),
            left_index=True,
            right_index=True,
        ),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="cannot join with no overlapping index names",
    )


@sql_count_checker(query_count=3, join_count=1)
def test_merge_cross(left_df, right_df, sort):
    eval_snowpark_pandas_result(
        left_df,
        left_df.to_pandas(),
        lambda df: df.merge(
            right_df if isinstance(df, pd.DataFrame) else right_df.to_pandas(),
            how="cross",
            # There are no join keys in cross join. Sort param doesn't impact output
            # order.
            sort=sort,
        ),
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"on": "A"},
        {"left_on": "A", "right_on": "B"},
        {"left_on": "left_i", "right_index": True},
        {"left_index": True, "right_on": "A"},
    ],
)
@sql_count_checker(query_count=3, join_count=3)
def test_merge_non_empty_with_empty(left_df, empty_df, how, kwargs, sort):
    _verify_merge(left_df, empty_df, how, sort=sort, **kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"on": "A"},
        {"left_on": "A", "right_on": "B"},
        {"left_on": "i", "right_index": True},
        {"left_index": True, "right_on": "A"},
    ],
)
@sql_count_checker(query_count=3, join_count=3)
def test_merge_empty_with_non_empty(empty_df, right_df, how, kwargs, sort):
    # Native pandas returns incorrect column order when left frame is empty.
    # https://github.com/pandas-dev/pandas/issues/51929
    if "on" in kwargs:
        columns = ["A", "B_x", "B_y", "right_c", "right_d"]
    else:
        columns = ["A_x", "B_x", "A_y", "B_y", "right_c", "right_d"]
    _verify_merge(
        empty_df, right_df, how, sort=sort, **kwargs, force_output_column_order=columns
    )


@pytest.mark.parametrize(
    "on, left_on, right_on, left_index, right_index",
    [
        (None, None, None, True, False),  # Only left_index is set to True
        (None, None, None, False, True),  # Only right_index is set to True
        (None, "A", None, False, False),  # Only left_on is provided
        (None, None, "A", False, False),  # Only right_on is provided
        ("A", "A", None, False, False),  # on and left_on both provided
        ("A", None, "A", False, False),  # on and right_on both provided
        ("A", None, None, True, False),  # on and left_index both provided
        ("A", None, None, False, True),  # on and right_index both provided
        (None, "A", None, True, False),  # left_on and left_index both provided
        (None, None, "A", False, True),  # right_on and right_index both provided
        (None, ["A"], ["A", "B"], False, False),  # length mismatch
        (None, None, None, ["A"], True),  # left_index not a bool
        (None, None, None, True, ["A"]),  # right_index not a bool
        ("ABC", None, None, False, False),  # Unknown label in on
        (None, "ABC", None, False, True),  # Unknown label in left_on
        (None, None, "ABC", True, False),  # Unknown label in right_on
        (None, ["A", "B"], None, False, True),  # len(left_on) != right.num_index_levels
        (None, None, ["A", "B"], True, False),  # left.num_index_levels != len(right_on)
    ],
)
@sql_count_checker(query_count=2)
def test_merge_mis_specified_negative(
    left_df, right_df, on, left_on, right_on, left_index, right_index
):
    eval_snowpark_pandas_result(
        left_df,
        left_df.to_pandas(),
        lambda df: df.merge(
            right_df if isinstance(df, pd.DataFrame) else right_df.to_pandas(),
            on=on,
            left_on=left_on,
            right_on=right_on,
            left_index=left_index,
            right_index=right_index,
        ),
        expect_exception=True,
    )


@pytest.mark.parametrize(
    "on, left_on, right_on, left_index, right_index",
    [
        ("A", None, None, False, False),  # on provided
        (None, "A", None, False, False),  # left_on provided
        (None, None, "A", False, False),  # right_on provided
        (None, None, None, True, False),  # left_index is set to True
        (None, None, None, False, True),  # right_index is set to True
    ],
)
@sql_count_checker(query_count=2)
def test_merge_cross_mis_specified_negative(
    left_df, right_df, on, left_on, right_on, left_index, right_index
):
    eval_snowpark_pandas_result(
        left_df,
        left_df.to_pandas(),
        lambda df: df.merge(
            right_df if isinstance(df, pd.DataFrame) else right_df.to_pandas(),
            on=on,
            how="cross",
            left_on=left_on,
            right_on=right_on,
            left_index=left_index,
            right_index=right_index,
        ),
        expect_exception=True,
    )


@pytest.mark.parametrize(
    "left_col, right_col, kwargs",
    [
        (0, 0, {"suffixes": ("", "_dup")}),
        (0, 0, {"suffixes": (None, "_dup")}),
        (0, 0, {"suffixes": ("_x", "_y")}),
        (0, 0, {"suffixes": ["_x", "_y"]}),
        (0, 0, {"suffixes": ("_a", None)}),
        (0, 0, {}),
        ("b", "b", {"suffixes": (None, "_y")}),
        ("a", "a", {"suffixes": ("_x", None)}),
        ("a", "b", {"suffixes": ("_x", None)}),
        ("a", "a", {"suffixes": (None, "_x")}),
        ("a", "a", {}),
        ("a", 0, {"suffixes": (None, "_y")}),
        (0.0, 0.0, {"suffixes": ("_x", None)}),
    ],
)
@sql_count_checker(query_count=3, join_count=1)
def test_merge_suffix(left_df, right_df, left_col, right_col, kwargs):
    left_df = left_df.rename(columns={"A": left_col})
    right_df = right_df.rename(columns={"A": right_col})
    eval_snowpark_pandas_result(
        left_df,
        left_df.to_pandas(),
        lambda df: df.merge(
            right_df if isinstance(df, pd.DataFrame) else right_df.to_pandas(),
            on="B",
            how="left",
            **kwargs,
        ),
    )


@sql_count_checker(query_count=3, join_count=1)
def test_merge_duplicate_suffix(left_df, right_df):
    eval_snowpark_pandas_result(
        left_df,
        left_df.to_pandas(),
        lambda df: df.merge(
            right_df if isinstance(df, pd.DataFrame) else right_df.to_pandas(),
            on="A",
            how="left",
            suffixes=("_x", "_x"),
        ),
    )


@sql_count_checker(query_count=3, join_count=1)
def test_merge_label_conflict_with_suffix(left_df, right_df):
    # Test the behavior when adding suffix crates a conflict with another label.
    # Note: This raises a warning in pandas 2.0 and will raise an error in future
    # versions https://github.com/pandas-dev/pandas/issues/22818

    # Change left_df columns to ["A", "B", "C", "C_y"]
    left_df = left_df.rename(columns={"left_c": "C", "left_d": "C_y"})
    # Change right_df columns to ["A", "B", "C", "D"]
    right_df = right_df.rename(columns={"right_c": "C", "right_d": "D"})

    # During merge suffix '_y' is added to 'C' from right frame, but it conflicts with
    # last column in left frame.
    eval_snowpark_pandas_result(
        left_df,
        left_df.to_pandas(),
        lambda df: df.merge(
            right_df if isinstance(df, pd.DataFrame) else right_df.to_pandas(),
            on="A",
            how="left",
        ),
    )


@sql_count_checker(query_count=3, join_count=1)
def test_merge_non_str_suffix(left_df, right_df):
    eval_snowpark_pandas_result(
        left_df,
        left_df.to_pandas(),
        lambda df: df.merge(
            right_df if isinstance(df, pd.DataFrame) else right_df.to_pandas(),
            on="A",
            how="left",
            suffixes=(0, 2),
        ),
    )


@pytest.mark.parametrize(
    "suffixes",
    [(None, None), ("", None), (None, ""), ("", "")],
)
@sql_count_checker(query_count=2)
def test_merge_empty_suffix_negative(left_df, right_df, suffixes):
    eval_snowpark_pandas_result(
        left_df,
        left_df.to_pandas(),
        lambda df: df.merge(
            right_df if isinstance(df, pd.DataFrame) else right_df.to_pandas(),
            on="A",
            suffixes=suffixes,
        ),
        expect_exception=True,
    )


@pytest.mark.parametrize(
    "suffixes",
    [("a", "b", "c"), tuple("a")],
)
@sql_count_checker(query_count=2)
def test_merge_suffix_length_error_negative(left_df, right_df, suffixes):
    eval_snowpark_pandas_result(
        left_df,
        left_df.to_pandas(),
        lambda df: df.merge(
            right_df if isinstance(df, pd.DataFrame) else right_df.to_pandas(),
            on="A",
            suffixes=suffixes,
        ),
        expect_exception=True,
    )


@sql_count_checker(query_count=3, join_count=1)
def test_merge_duplicate_labels(left_df, right_df):
    # Change left_df columns to ["A", "B", "left_c", "left_c"]
    # 'left_c' is a duplicate label.
    left_df = left_df.rename(columns={"left_d": "left_c"})
    eval_snowpark_pandas_result(
        left_df,
        left_df.to_pandas(),
        lambda df: df.merge(
            right_df if isinstance(df, pd.DataFrame) else right_df.to_pandas(),
            on="A",
            how="left",
        ),
    )


@sql_count_checker(query_count=2)
def test_merge_duplicate_join_keys_negative(left_df, right_df):
    # Change left_df columns to ["A", "B", "left_c", "left_c"]
    # 'left_c' is a duplicate label. This can not be used as join key.
    left_df = left_df.rename(columns={"left_d": "left_c"})
    eval_snowpark_pandas_result(
        left_df,
        left_df.to_pandas(),
        lambda df: df.merge(
            right_df if isinstance(df, pd.DataFrame) else right_df.to_pandas(),
            on="left_c",
        ),
        expect_exception=True,
    )


@sql_count_checker(query_count=0)
def test_merge_invalid_how_negative(left_df, right_df):
    # native pandas raises UnboundLocalError: local variable 'lidx' referenced before assignment
    # In snowpark pandas we raise more meaningful error
    msg = "do not recognize join method full_outer_join"
    with pytest.raises(ValueError, match=msg):
        left_df.merge(right_df, on="A", how="full_outer_join")


@sql_count_checker(query_count=2, join_count=1)
def test_merge_with_self():
    snow_df = pd.DataFrame({"A": [1, 2, 3]})
    eval_snowpark_pandas_result(
        snow_df,
        snow_df.to_pandas(),
        lambda df: df.merge(df, on="A"),
        test_attrs=False,  # native pandas propagates attrs on self-merge, but we do not
    )


@pytest.mark.parametrize("on", ["A", "B"])
@sql_count_checker(query_count=4, join_count=1)
def test_merge_with_series(left_df, right_df, how, on, sort):
    native_series = right_df.to_pandas()[on]
    snow_series = pd.Series(native_series)
    _verify_merge(left_df, snow_series, how=how, on=on, sort=sort)


@sql_count_checker(query_count=1)
def test_merge_with_unnamed_series_negative(left_df):
    native_series = native_pd.Series([1, 2, 3])
    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(
        left_df,
        left_df.to_pandas(),
        lambda df: df.merge(
            snow_series if isinstance(df, pd.DataFrame) else native_series
        ),
        # Expect: ValueError: Cannot merge a Series without a name
        expect_exception=True,
    )


@sql_count_checker(query_count=3, join_count=1)
def test_merge_multiindex_columns():
    left = pd.DataFrame({("A", 1): [1, 2], ("A", 2): [3, 4], ("B", 3): [5, 6]})
    right = pd.DataFrame({("A", 1): [1, 2], ("B", 3): [4, 5], ("C", 4): [6, 7]})
    eval_snowpark_pandas_result(
        left,
        left.to_pandas(),
        lambda df: df.merge(
            right if isinstance(df, pd.DataFrame) else right.to_pandas(),
            on=[("B", 3)],
            how="left",
        ),
    )


@sql_count_checker(query_count=3, join_count=1)
def test_merge_series_multiindex_columns():
    left = pd.DataFrame({("A", 1): [1, 2], ("A", 2): [3, 4], ("B", 3): [5, 6]})
    right = pd.Series([1, 2], name=("B", 3))
    eval_snowpark_pandas_result(
        left,
        left.to_pandas(),
        lambda df: df.merge(
            right if isinstance(df, pd.DataFrame) else right.to_pandas(),
            on=[("B", 3)],
            how="left",
        ),
    )


@pytest.mark.parametrize("dtype", [None, "Int64"])
@sql_count_checker(query_count=6, join_count=2)
def test_merge_outer_with_nan(dtype):
    left = pd.DataFrame({"key": [1, 2], "col1": [1, 2]}, dtype=dtype)
    right = pd.DataFrame({"key": [np.nan, np.nan], "col2": [3, 4]}, dtype=dtype)
    _verify_merge(left, right, "outer", on="key")
    # switch left and right
    _verify_merge(right, left, "outer", on="key")


# Two extra queries to convert to native index for dataframe constructor when creating left and right
@sql_count_checker(query_count=5, join_count=1)
def test_merge_different_index_names():
    left = pd.DataFrame({"a": [1]}, index=pd.Index([1], name="c"))
    right = pd.DataFrame({"a": [1]}, index=pd.Index([1], name="d"))
    eval_snowpark_pandas_result(
        left,
        left.to_pandas(),
        lambda df: df.merge(
            right if isinstance(df, pd.DataFrame) else right.to_pandas(),
            left_on="c",
            right_on="d",
        ),
    )


@sql_count_checker(query_count=3, join_count=1)
def test_merge_no_join_keys(left_df, right_df, how, sort):
    _verify_merge(left_df, right_df, how, sort=sort)


@pytest.mark.parametrize("left_name, right_name", [("left_a", "right_a"), (1, "1")])
@sql_count_checker(query_count=2)
def test_merge_no_join_keys_negative(left_name, right_name, left_df, right_df):
    left_df = left_df.rename(columns={"A": left_name, "B": "left_b"})
    right_df = right_df.rename(columns={"A": right_name, "B": "right_b"})
    # Joining without any join keys and frames have no common columns. Expect exception
    eval_snowpark_pandas_result(
        left_df,
        left_df.to_pandas(),
        lambda df: df.merge(
            right_df if isinstance(df, pd.DataFrame) else right_df.to_pandas(),
        ),
        expect_exception=True,
        expect_exception_type=MergeError,
        expect_exception_match="No common columns to perform merge on",
    )


@sql_count_checker(query_count=2)
def test_merge_no_join_keys_common_index_negative(left_df, right_df):
    left_df = pd.DataFrame({"A": [1, 2, 3]}, native_pd.Index([7, 8, 9], name="KEY"))
    right_df = pd.DataFrame({"B": [1, 2, 3]}, native_pd.Index([7, 8, 9], name="KEY"))
    # Joining without any join keys and frames have no common data columns.
    # Common index column should not be used as join key.
    eval_snowpark_pandas_result(
        left_df,
        left_df.to_pandas(),
        lambda df: df.merge(
            right_df if isinstance(df, pd.DataFrame) else right_df.to_pandas(),
        ),
        expect_exception=True,
        expect_exception_type=MergeError,
        expect_exception_match="No common columns to perform merge on",
    )


@sql_count_checker(query_count=2)
def test_merge_no_join_keys_common_index_with_data_negative(left_df, right_df):
    left_df = left_df.rename(columns={"A": "left_a", "B": "left_b"})
    right_df = right_df.rename(columns={"A": "right_a", "B": "left_i"})
    # Joining without any join keys and frames have no common data columns.
    # Common column 'left_i' which is an index column in left frame and data column in
    # right frame, should not be used.
    # Expect exception
    # MergeError: No common columns to perform merge on...
    eval_snowpark_pandas_result(
        left_df,
        left_df.to_pandas(),
        lambda df: df.merge(
            right_df if isinstance(df, pd.DataFrame) else right_df.to_pandas(),
        ),
        expect_exception=True,
        expect_exception_type=MergeError,
        expect_exception_match="No common columns to perform merge on",
    )


@pytest.mark.parametrize(
    "left_on, right_on, expected_join_count",
    [
        (np.array(["a", "b", "c", "x", "y"]), "right_d", 2),
        ([np.array(["a", "b", "c", "x", "y"]), "A"], ["right_d", "A"], 2),
        ("left_d", np.array(["a", "b", "c", "x", "y"]), 2),
        (["left_d", "A"], [np.array(["a", "b", "c", "x", "y"]), "A"], 2),
        (["left_d", "A"], (np.array(["a", "b", "c", "x", "y"]), "A"), 2),  # tuple
        (
            np.array(["a", "b", "c", "x", "y"]),
            np.array(["x", "y", "c", "a", "b"]),
            3,
        ),
    ],
)
def test_merge_on_array_like_keys(
    left_df, right_df, left_on, right_on, how, expected_join_count
):
    with SqlCounter(query_count=3, join_count=expected_join_count):
        _verify_merge(left_df, right_df, how=how, left_on=left_on, right_on=right_on)


@sql_count_checker(query_count=2)
def test_merge_on_array_like_keys_conflict_negative(left_df, right_df):
    left_on = np.array(["a", "b", "c", "x", "y"])
    right_on = np.array(["x", "y", "c", "a", "b"])
    left_df = left_df.rename(columns={"A": "key_0"})

    eval_snowpark_pandas_result(
        left_df,
        left_df.to_pandas(),
        lambda df: df.merge(
            right_df if isinstance(df, pd.DataFrame) else right_df.to_pandas(),
            left_on=left_on,
            right_on=right_on,
        ),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="cannot insert key_0, already exists",
    )


@pytest.mark.parametrize(
    "left_on",
    [
        np.array(["a", "b", "c", "x"]),  # too short
        np.array(["a", "b", "c", "a", "b", "c"]),  # too long
    ],
)
@sql_count_checker(query_count=0)
def test_merge_on_array_like_keys_length_mismatch_negative(left_df, right_df, left_on):
    # Native pandas raises
    # ValueError: The truth value of an array with more than one element is ambiguous
    # Error message is not very helpful. So we instead raise error with
    # more helpful message.
    with pytest.raises(
        ValueError, match="array-like join key must be of same length as dataframe"
    ):
        left_df.merge(right_df, left_on=left_on, right_on="right_d")


@sql_count_checker(query_count=3, join_count=1)
def test_merge_with_indicator(left_df, right_df, how):
    _verify_merge(left_df, right_df, how, on="A", indicator=True)


@sql_count_checker(query_count=3, join_count=1)
def test_merge_with_indicator_cross_join(left_df, right_df):
    _verify_merge(left_df, right_df, how="cross", indicator=True)


@sql_count_checker(query_count=3, join_count=1)
def test_merge_with_indicator_explicit_name(left_df, right_df):
    _verify_merge(left_df, right_df, "outer", on="A", indicator="indicator_col")


@sql_count_checker(query_count=2)
def test_merge_with_invalid_indicator_type_negative(left_df, right_df):
    eval_snowpark_pandas_result(
        left_df,
        left_df.to_pandas(),
        lambda df: df.merge(
            right_df if isinstance(df, pd.DataFrame) else right_df.to_pandas(),
            on="A",
            indicator=1,
        ),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="indicator option can only accept boolean or string arguments",
    )


@sql_count_checker(query_count=2)
def test_merge_with_indicator_explicit_name_negative(left_df, right_df):
    left_df = left_df.rename(columns={"left_c": "_merge"})
    eval_snowpark_pandas_result(
        left_df,
        left_df.to_pandas(),
        lambda df: df.merge(
            right_df if isinstance(df, pd.DataFrame) else right_df.to_pandas(),
            on="A",
            indicator=True,
        ),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="Cannot use name of an existing column for indicator column",
    )


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
def test_merge_validate(lvalues, rvalues, validate):
    left = pd.DataFrame({"A": lvalues})
    right = pd.DataFrame({"B": rvalues})
    msg = "Snowpark pandas merge API doesn't yet support 'validate' parameter"
    with pytest.raises(NotImplementedError, match=msg):
        left.merge(right, left_on="A", right_on="B", validate=validate)


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
def test_merge_validate_negative(lvalues, rvalues, validate):
    left = pd.DataFrame({"A": lvalues})
    right = pd.DataFrame({"B": rvalues})
    msg = "Snowpark pandas merge API doesn't yet support 'validate' parameter"
    with pytest.raises(NotImplementedError, match=msg):
        left.merge(right, left_on="A", right_on="B", validate=validate)


@sql_count_checker(query_count=1, join_count=1)
def test_merge_timedelta_on():
    left_df = native_pd.DataFrame(
        {"lkey": ["foo", "bar", "baz", "foo"], "value": [1, 2, 3, 5]}
    ).astype({"value": "timedelta64[ns]"})
    right_df = native_pd.DataFrame(
        {"rkey": ["foo", "bar", "baz", "foo"], "value": [5, 6, 7, 8]}
    ).astype({"value": "timedelta64[ns]"})
    eval_snowpark_pandas_result(
        pd.DataFrame(left_df),
        left_df,
        lambda df: df.merge(
            pd.DataFrame(right_df) if isinstance(df, pd.DataFrame) else right_df,
            left_on="lkey",
            right_on="rkey",
        ),
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"how": "inner", "on": "a"},
        {"how": "right", "on": "a"},
        {"how": "right", "on": "b"},
        {"how": "left", "on": "c"},
        {"how": "cross"},
    ],
)
def test_merge_timedelta_how(kwargs):
    left_df = native_pd.DataFrame(
        {"a": ["foo", "bar"], "b": [1, 2], "c": [3, 5]}
    ).astype({"b": "timedelta64[ns]"})
    right_df = native_pd.DataFrame(
        {"a": ["foo", "baz"], "b": [1, 3], "c": [3, 4]}
    ).astype({"b": "timedelta64[ns]", "c": "timedelta64[ns]"})
    count = 1
    expect_exception = False
    if "c" == kwargs.get("on", None):  # merge timedelta with int exception
        expect_exception = True
        count = 0

    with SqlCounter(query_count=count, join_count=count):
        eval_snowpark_pandas_result(
            pd.DataFrame(left_df),
            left_df,
            lambda df: df.merge(
                pd.DataFrame(right_df) if isinstance(df, pd.DataFrame) else right_df,
                **kwargs,
            ),
            expect_exception=expect_exception,
            expect_exception_match="You are trying to merge on LongType and TimedeltaType columns for key 'c'. If you "
            "wish to proceed you should use pd.concat",
            expect_exception_type=ValueError,
            assert_exception_equal=False,  # pandas exception: You are trying to merge on int64 and timedelta64[ns]
            # columns for key 'c'. If you wish to proceed you should use pd.concat
        )
