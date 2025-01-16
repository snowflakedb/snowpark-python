#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas import Index, MultiIndex

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_frame_equal, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.fixture(scope="function")
def native_df():
    index = [1, 2, 3]
    columns = ["red", "green", "pink"]
    return native_pd.DataFrame(
        [[1, "one", True], [2, "two", True], [3, "three", False]],
        index=index,
        columns=columns,
    )


@pytest.fixture(scope="function")
def snow_df():
    index = [1, 2, 3]
    columns = ["red", "green", "pink"]
    return pd.DataFrame(
        [[1, "one", True], [2, "two", True], [3, "three", False]],
        index=index,
        columns=columns,
    )


@pytest.fixture(scope="function")
def multiindex_snow_df():
    index = MultiIndex.from_arrays([[1, 2, 3], [4, 5, 6]], names=["a", "b"])
    columns = MultiIndex.from_arrays([["red", "green"], [1, 2]], names=["x", "y"])
    return pd.DataFrame(
        [[1, "one"], [2, "two"], [3, "three"]], index=index, columns=columns
    )


@pytest.fixture(scope="function")
def multiindex_native_df():
    index = MultiIndex.from_arrays([[1, 2, 3], [4, 5, 6]], names=["a", "b"])
    columns = MultiIndex.from_arrays([["red", "green"], [1, 2]], names=["x", "y"])
    return native_pd.DataFrame(
        [[1, "one"], [2, "two"], [3, "three"]], index=index, columns=columns
    )


@pytest.fixture(params=[0, 1])
def axis(request):
    """
    axis argument to pass to drop.
    """
    return request.param


@pytest.mark.parametrize(
    "labels", [Index(["red", "green"]), np.array(["red", "green"])]
)
@sql_count_checker(query_count=1)
def test_drop_list_like(native_df, labels):
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.drop(labels, axis=1))


@pytest.mark.parametrize(
    "labels", [Index(["red", "green"]), np.array(["red", "green"])]
)
@sql_count_checker(query_count=1)
def test_drop_timedelta(native_df, labels):
    native_df_dt = native_df.astype({"red": "timedelta64[ns]"})
    snow_df = pd.DataFrame(native_df_dt)
    eval_snowpark_pandas_result(
        snow_df, native_df_dt, lambda df: df.drop(labels, axis=1)
    )


@pytest.mark.parametrize(
    "labels, axis, expected_query_count",
    [
        ([], 0, 1),
        (1, 0, 2),
        ([1, 2], 0, 3),
        ([3, 1, 2], 0, 4),
        ([1, 1], 0, 3),
        ([], 1, 1),
        ("red", 1, 1),
        (["red", "green"], 1, 1),
        (["green", "pink", "red"], 1, 1),
        (["red", "red"], 1, 1),
    ],
)
@pytest.mark.parametrize("inplace", [True, False])
def test_drop_axis(native_df, labels, axis, inplace, expected_query_count):
    snow_df = pd.DataFrame(native_df)
    with SqlCounter(query_count=expected_query_count):
        eval_snowpark_pandas_result(
            snow_df.copy(),
            native_df,
            lambda df: df.drop(labels, axis=axis, inplace=inplace),
            inplace=inplace,
        )


@pytest.mark.parametrize("labels", [[], "red", "green", ["red", "red"]])
@sql_count_checker(query_count=1)
def test_drop_duplicate_columns(native_df, labels):
    snow_df = pd.DataFrame(native_df)
    native_df = native_df.rename(columns={"pink": "red"})
    snow_df = snow_df.rename(columns={"pink": "red"})
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.drop(labels, axis=1))


@pytest.mark.parametrize(
    "labels, expected_query_count, expected_join_count",
    [([], 1, 1), (1, 2, 2), (2, 2, 2), ([1, 2], 3, 3)],
)
def test_drop_duplicate_row_labels(
    native_df, labels, expected_query_count, expected_join_count
):
    snow_df = pd.DataFrame(native_df)
    native_df = native_df.set_index([[1, 2, 1]])
    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
        snow_df = snow_df.set_index([[1, 2, 1]])
        eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.drop(labels))


@pytest.mark.parametrize(
    "msg, labels, level",
    [
        (r"labels \[1\] not found in level", 1, "x"),  # refer level by name
        (r"labels \['red'\] not found in level", "red", "y"),  # refer level by name
        (r"labels \[1\] not found in level", 1, 0),  # refer level by int position
        (
            r"labels \['red'\] not found in level",
            "red",
            1,
        ),  # refer level by int position
        (r"labels \[\(1,\)\] not found in level", (1,), 1),  # tuple label
        (r"labels \['blue'\] not found in axis", "blue", None),  # no level
        (r"labels \[3\] not found in axis", 3, None),  # no level
        (r"labels \[\(1, 2\)\] not found in axis", (1, 2), None),  # tuple label
        (r"labels \[1, 2, 3\] not found in axis", [1, 2, 3], None),  # list label
        (
            r"labels \['blue'\] not found in axis",
            ["red", "blue"],
            None,
        ),  # list with one missing
        (r"labels \[\(\)\] not found in level", [()], 0),  # empty tuple against level
    ],
)
@sql_count_checker(query_count=0)
def test_drop_invalid_labels_axis1_negative(
    msg, labels, level, multiindex_snow_df, multiindex_native_df
):
    with pytest.raises(KeyError, match=msg):
        multiindex_snow_df.drop(labels, level=level, axis=1)
    # Snowpark pandas message is slight different for consistency. Verify native pandas
    # also throw 'KeyError'
    with pytest.raises(KeyError):
        multiindex_native_df.drop(labels, level=level, axis=1)


@pytest.mark.parametrize(
    "msg, labels, level, expected_query_count",
    [
        (r"labels \[None\] not found in axis", [None], None, 1),  # None as label
        (r"labels \[4\] not found in level", 4, "a", 1),  # refer level by name
        (r"labels \[7\] not found in level", 7, "b", 1),  # refer level by name
        (r"labels \[4\] not found in level", 4, 0, 1),  # refer level by int position
        (r"labels \[7\] not found in level", 7, 1, 1),  # refer level by int position
        (r"labels \[4\] not found in axis", 4, None, 1),  # no level
        (r"labels \[\(1, 2\)\] not found in axis", (1, 2), None, 1),  # tuple label
        (r"labels \[4, 5, 7\] not found in axis", [4, 5, 7], None, 3),  # list label
        (r"labels \[7\] not found in axis", [1, 7], None, 2),  # list with one missing
    ],
)
def test_drop_invalid_labels_axis0_negative(
    msg, labels, level, expected_query_count, multiindex_snow_df, multiindex_native_df
):
    # Query is generated for each label dropped
    with SqlCounter(query_count=expected_query_count):
        with pytest.raises(KeyError, match=msg):
            multiindex_snow_df.drop(labels, level=level)
        # Snowpark pandas message is slight different for consistency. Verify native pandas
        # also throw 'KeyError'
        with pytest.raises(KeyError):
            multiindex_native_df.drop(labels, level=level, axis=1)


@pytest.mark.parametrize(
    "labels, level",
    [
        (1, "x"),  # refer level by name
        ("red", "y"),  # refer level by name
        (1, 0),  # refer level by int position
        ("red", 1),  # refer level by int position
        ("blue", None),  # no level
        (3, None),  # no level
        ((1, 2), None),  # tuple label
        ([1, 2], None),  # list label
        ([], None),  # empty labels
    ],
)
@sql_count_checker(query_count=2)
def test_drop_invalid_axis1_labels_errors_ignore(labels, level, multiindex_snow_df):
    result = multiindex_snow_df.drop(labels, level=level, axis=1, errors="ignore")
    assert_frame_equal(multiindex_snow_df, result)


@pytest.mark.parametrize(
    "labels, level",
    [
        (4, "a"),  # refer level by name
        (7, "b"),  # refer level by name
        (4, 0),  # refer level by int position
        (
            7,
            1,
        ),  # refer level by int position
        (4, None),  # no level
        ((1, 2), None),  # tuple label
        ([4, 5, 7], None),  # list label
        ([], None),  # empty labels
    ],
)
@sql_count_checker(query_count=2)
def test_drop_invalid_axis0_labels_errors_ignore(labels, level, multiindex_snow_df):
    result = multiindex_snow_df.drop(labels, level=level, errors="ignore")
    assert_frame_equal(multiindex_snow_df, result)


@sql_count_checker(query_count=0)
def test_empty_tuple_single_index_negative(native_df):
    msg = r"\[\(\)\] not found in axis"
    snow_df = pd.DataFrame(native_df)
    with pytest.raises(KeyError, match=msg):
        snow_df.drop((), axis=1)
    # Snowpark pandas message is slight different for consistency. Verify native pandas
    # also throw 'KeyError'
    with pytest.raises(KeyError):
        native_df.drop((), axis=1)


def test_empty_tuple_multiindex(multiindex_snow_df, axis):
    # Empty tuple should match with every label if column index is multiindex
    if axis == 1:
        with SqlCounter(query_count=1):
            result = multiindex_snow_df.drop((), axis=axis)
            assert len(result.columns) == 0
            assert len(result.index) == 3
    else:
        with SqlCounter(query_count=2):
            result = multiindex_snow_df.drop((), axis=axis)
            assert len(result.columns) == 2
            assert len(result.index) == 0


@sql_count_checker(query_count=2)
def test_drop_preserve_index_names(multiindex_snow_df):
    df_dropped_e = multiindex_snow_df.drop("red", axis=1)
    df_inplace_e = multiindex_snow_df.copy()
    return_value = df_inplace_e.drop("red", axis=1, inplace=True)
    assert return_value is None
    for obj in (df_dropped_e, df_inplace_e):
        assert obj.index.names == ["a", "b"]
        assert obj.columns.names == ["x", "y"]


@pytest.mark.parametrize(
    "op1, op2, expected_query_count",
    [
        (lambda df: df.drop("red", axis=1), lambda df: df.drop(columns="red"), 2),
        (
            lambda df: df.drop(labels="green", axis=1),
            lambda df: df.drop(columns="green"),
            2,
        ),
        (
            lambda df: df.drop("red", axis=1),
            lambda df: df.drop("red", axis="columns"),
            2,
        ),
        (lambda df: df.drop(1, axis=0), lambda df: df.drop(index=1), 4),
        (lambda df: df.drop(1), lambda df: df.drop(index=[1]), 4),
        (lambda df: df.drop(labels=2, axis="index"), lambda df: df.drop(index=[2]), 4),
        (
            lambda df: df.drop(index=1, columns="red"),
            lambda df: df.drop(index=1).drop(columns="red"),
            4,
        ),
    ],
)
def test_drop_api_equivalence(snow_df, op1, op2, expected_query_count):
    # equivalence of the labels/axis and index/columns API's
    # query count for each drop operation is higher when dropping from the index because an extra
    # query needed is needed to check whether the label(s) are present in the index
    with SqlCounter(query_count=expected_query_count):
        res1 = op1(snow_df)
        res2 = op2(snow_df)
        assert_frame_equal(res1, res2)


@sql_count_checker(query_count=0)
def test_misconfigured_input(snow_df):
    msg = "Cannot specify both 'labels' and 'index'/'columns'"
    with pytest.raises(ValueError, match=msg):
        snow_df.drop(labels="a", index="b")

    with pytest.raises(ValueError, match=msg):
        snow_df.drop(labels="a", columns="b")

    msg = "Need to specify at least one of 'labels', 'index' or 'columns'"
    with pytest.raises(ValueError, match=msg):
        snow_df.drop(axis=1)


@pytest.mark.parametrize(
    "labels",
    [
        "a",
        ("a", "", ""),
        ["top"],
        [("top", "OD", "wx"), ("top", "OD", "wy")],
        ("top", "OD", "wx"),
    ],
)
def test_mixed_depth_drop(labels, axis):
    arrays = [
        ["a", "top", "top", "routine1", "routine1", "routine2"],
        ["", "OD", "OD", "result1", "result2", "result1"],
        ["", "wx", "wy", "", "", ""],
    ]
    expected_query_count = 2
    tuples = sorted(zip(*arrays))
    mi = MultiIndex.from_tuples(tuples)
    if axis == 0:
        df = pd.DataFrame([1, 2, 3, 4, 5, 6], index=mi)
        expected_query_count = 3
        if labels == [("top", "OD", "wx"), ("top", "OD", "wy")]:
            expected_query_count = 4
    else:
        df = pd.DataFrame([[1, 2, 3, 4, 5, 6]], columns=mi)

    with SqlCounter(query_count=expected_query_count):
        eval_snowpark_pandas_result(
            df, df.to_pandas(), lambda d: d.drop(labels, axis=axis)
        )


@pytest.mark.parametrize(
    "labels, level", [("a", None), ("result1", 1), ("", 2), ("top", "a"), ("OD", "b")]
)
def test_drop_level(labels, level, axis):
    arrays = [
        ["a", "top", "top", "routine1", "routine1", "routine2"],
        ["", "OD", "OD", "result1", "result2", "result1"],
        ["", "wx", "wy", "", "", ""],
    ]

    tuples = sorted(zip(*arrays))
    mi = MultiIndex.from_tuples(tuples, names=["a", "b", "c"])
    if axis == 0:
        expected_query_count = 3
        df = pd.DataFrame([1, 2, 3, 4, 5, 6], index=mi)
    else:
        expected_query_count = 2
        df = pd.DataFrame([[1, 2, 3, 4, 5, 6]], columns=mi)
    with SqlCounter(query_count=expected_query_count):
        eval_snowpark_pandas_result(
            df, df.to_pandas(), lambda d: d.drop(labels, axis=axis, level=level)
        )
