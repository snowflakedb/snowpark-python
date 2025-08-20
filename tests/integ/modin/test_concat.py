#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from collections import deque
from collections.abc import Hashable, Iterable

import modin.pandas as pd
import pandas as native_pd
import pytest
from pandas import Index, MultiIndex

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.utils import (
    assert_frame_equal,
    assert_index_equal,
    assert_series_equal,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import TestFiles, multithreaded_run


@pytest.fixture(scope="function")
def df1():
    return native_pd.DataFrame(
        {
            "C": [1, 2, 3],
            "A": ["a", "b", "c"],
            "D": [3, 2, 1],
        },
        index=native_pd.Index([3, 1, 2], name="left_i"),
    )


@pytest.fixture(scope="function")
def df2():
    return native_pd.DataFrame(
        {
            "P": [3, 2, 1, 3],
            "A": ["a", "b", "c", "a"],
            "C": [1, 2, 3, 2],
        },
        index=native_pd.Index([2, 0, 3, 4], name="right_i"),
    )


@pytest.fixture(scope="function")
def df_single_col():
    return native_pd.DataFrame([1], columns=["A"])


@pytest.fixture(scope="function")
def zero_rows_df():
    return native_pd.DataFrame(columns=["A", "B"])


@pytest.fixture(scope="function")
def zero_columns_df():
    return native_pd.DataFrame(index=pd.Index([1, 2]))


@pytest.fixture(scope="function")
def empty_df():
    return native_pd.DataFrame()


@pytest.fixture(scope="function")
def series1():
    return native_pd.Series([1, 2])


@pytest.fixture(scope="function")
def series2():
    return native_pd.Series([2, 1])


@pytest.fixture(params=["inner", "outer"])
def join(request):
    """
    join argument to pass to concat.
    """
    return request.param


@pytest.fixture(params=[True, False])
def sort(request):
    """
    sort argument to pass to concat.
    """
    return request.param


@pytest.fixture(params=[True, False])
def ignore_index(request):
    """
    ignore_index argument to pass to concat.
    """
    return request.param


@pytest.fixture(params=[0, 1])
def axis(request):
    """
    ignore_index argument to pass to concat.
    """
    return request.param


def _concat_operation(snow_objs, native_objs, **kwargs):
    return (
        lambda x: pd.concat(snow_objs, **kwargs)
        if x == "pd"
        else native_pd.concat(native_objs, **kwargs)
    )


def test_concat_basic(df1, df2, join, sort, axis, ignore_index):
    expected_join_count = 1 if axis == 1 else 0
    native_objs = [df1, df2]
    snow_objs = [pd.DataFrame(obj) for obj in native_objs]
    with SqlCounter(query_count=1, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            "pd",
            "native_pd",
            _concat_operation(
                snow_objs,
                native_objs,
                axis=axis,
                join=join,
                sort=sort,
                ignore_index=ignore_index,
            ),
        )


@sql_count_checker(query_count=0)
def test_concat_no_items_negative():
    eval_snowpark_pandas_result(
        "pd",
        "native_pd",
        _concat_operation(snow_objs=[], native_objs=[]),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="No objects to concatenate",
    )


def test_concat_exclude_none(df1, df2, axis):
    expected_join_count = 2 if axis == 1 else 0
    with SqlCounter(query_count=2, join_count=expected_join_count):
        # Verify that none objects are simply ignored.
        df1, df2 = pd.DataFrame(df1), pd.DataFrame(df2)
        pieces = [df1, None, df2, None]
        result = pd.concat(pieces, axis=axis)
        expected = pd.concat([df1, df2], axis=axis)
        assert_frame_equal(result, expected)


@sql_count_checker(query_count=0)
def test_concat_all_none_negative():
    eval_snowpark_pandas_result(
        "pd",
        "native_pd",
        _concat_operation(snow_objs=[None, None], native_objs=[None, None]),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="All objects passed were None",
    )


def test_concat_mixed_objs(df1, df2, series1, series2, axis, join):
    expected_join_count = 1 if axis == 1 else 0
    expected_join_count_with_duplicates = 2 if axis == 1 else 0

    # Series and Dataframes
    native_objs = [df1, series1]
    snow_objs = [pd.DataFrame(df1), pd.Series(series1)]
    with SqlCounter(query_count=1, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            "pd",
            "native_pd",
            _concat_operation(snow_objs, native_objs, axis=axis, join=join),
        )

    # All dataframes
    native_objs = [df1, df2]
    snow_objs = [pd.DataFrame(df) for df in native_objs]
    with SqlCounter(query_count=1, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            "pd",
            "native_pd",
            _concat_operation(snow_objs, native_objs, axis=axis, join=join),
        )

    # All dataframes with duplicates
    native_objs = [df1, df2, df1]
    snow_objs = [pd.DataFrame(df) for df in native_objs]
    with SqlCounter(query_count=1, join_count=expected_join_count_with_duplicates):
        eval_snowpark_pandas_result(
            "pd",
            "native_pd",
            _concat_operation(snow_objs, native_objs, axis=axis, join=join),
        )

    # All series
    native_objs = [series1, series2]
    snow_objs = [pd.Series(series) for series in native_objs]
    with SqlCounter(query_count=1, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            "pd",
            "native_pd",
            _concat_operation(snow_objs, native_objs, axis=axis, join=join),
        )

    # All series with duplicates
    native_objs = [series1, series2, series1]
    snow_objs = [pd.Series(series) for series in native_objs]
    with SqlCounter(query_count=1, join_count=expected_join_count_with_duplicates):
        eval_snowpark_pandas_result(
            "pd",
            "native_pd",
            _concat_operation(snow_objs, native_objs, axis=axis, join=join),
        )


@pytest.mark.parametrize(
    "name1, name2, expected_columns",
    [
        (None, None, [0, 1]),
        ("foo", None, ["foo", 0]),
        (None, "bar", [0, "bar"]),
        ("foo", "bar", ["foo", "bar"]),
        ("foo", "foo", ["foo", "foo"]),
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_concat_series_names_axis1(series1, series2, name1, name2, expected_columns):
    native_series1 = series1.rename(name1)
    native_series2 = series2.rename(name2)
    snow_series1 = pd.Series(series1).rename(name1)
    snow_series2 = pd.Series(series2).rename(name2)
    # snow result
    snow_res = pd.concat([snow_series1, snow_series2], axis=1)
    native_res = native_pd.concat([native_series1, native_series2], axis=1)
    assert_frame_equal(snow_res, native_res)
    # Explicit check for column names
    assert snow_res.columns.tolist() == expected_columns


@pytest.mark.parametrize(
    "name1, name2, expected_name",
    [
        (None, None, None),
        ("foo", None, None),
        (None, "bar", None),
        ("foo", "bar", None),
        ("foo", "foo", "foo"),
    ],
)
@sql_count_checker(query_count=1, union_count=1)
def test_concat_series_names_axis0(series1, series2, name1, name2, expected_name):
    native_series1 = series1.rename(name1)
    native_series2 = series2.rename(name2)
    snow_series1 = pd.Series(series1).rename(name1)
    snow_series2 = pd.Series(series2).rename(name2)
    # snow result
    snow_res = pd.concat([snow_series1, snow_series2])
    native_res = native_pd.concat([native_series1, native_series2])
    assert_series_equal(snow_res, native_res)
    # Explicit check for column names
    assert snow_res.name == expected_name


@sql_count_checker(query_count=0)
def test_concat_invalid_join_negative(df1, df2):
    native_objs = [df1, df2]
    snow_objs = [pd.DataFrame(df) for df in native_objs]
    eval_snowpark_pandas_result(
        "pd",
        "native_pd",
        _concat_operation(snow_objs, native_objs, join="left"),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match=r"Only can inner \(intersect\) or outer \(union\) join the other axis",
    )


def test_concat_iterables(df1, df2, axis):
    # verify that concat works with tuples, list, deque, generators and custom iterables
    expected = native_pd.concat([df1, df2], axis=axis)
    df1, df2 = pd.DataFrame(df1), pd.DataFrame(df2)

    expected_join_count = 1 if axis == 1 else 0

    # list
    with SqlCounter(query_count=1, join_count=expected_join_count):
        assert_frame_equal(pd.concat([df1, df2], axis=axis), expected)

    # tuple
    with SqlCounter(query_count=1, join_count=expected_join_count):
        assert_frame_equal(pd.concat((df1, df2), axis=axis), expected)

    # generator
    with SqlCounter(query_count=1, join_count=expected_join_count):
        assert_frame_equal(pd.concat((df for df in (df1, df2)), axis=axis), expected)

    # deque
    with SqlCounter(query_count=1, join_count=expected_join_count):
        assert_frame_equal(pd.concat(deque((df1, df2)), axis=axis), expected)

    # custom iterator
    class CustomIterator1:
        def __init__(self, objs) -> None:
            self.objs = objs
            self.index = 0

        def __iter__(self):
            return self

        def __next__(self):
            if self.index < len(self.objs):
                self.index = self.index + 1
                return self.objs[self.index - 1]
            else:
                raise StopIteration

    with SqlCounter(query_count=1, join_count=expected_join_count):
        assert_frame_equal(pd.concat(CustomIterator1([df1, df2]), axis=axis), expected)

    # customer iterator with generator
    class CustomIterator2(Iterable):
        def __iter__(self):
            yield df1
            yield df2

    with SqlCounter(query_count=1, join_count=expected_join_count):
        assert_frame_equal(pd.concat(CustomIterator2(), axis=axis), expected)


@sql_count_checker(query_count=0)
def test_concat_non_iterables_negative():
    msg = (
        "first argument must be an iterable of pandas objects, "
        'you passed an object of type "str"'
    )
    eval_snowpark_pandas_result(
        "pd",
        "native_pd",
        _concat_operation(snow_objs="abc", native_objs="abc"),
        expect_exception=True,
        expect_exception_type=TypeError,
        expect_exception_match=msg,
    )


@pytest.mark.parametrize("obj", [native_pd.DataFrame(), native_pd.Series()])
@sql_count_checker(query_count=0)
def test_concat_native_object_negative(obj):
    msg = (
        f"{type(obj)} is not supported as 'value' argument. Please convert this to "
        r"Snowpark pandas objects by calling modin.pandas.Series\(\)/DataFrame\(\)"
    )
    # As top level object
    with pytest.raises(TypeError, match=msg):
        pd.concat(obj)
    # As list
    with pytest.raises(TypeError, match=msg):
        pd.concat([obj])
    # As dict
    with pytest.raises(TypeError, match=msg):
        pd.concat({"a": obj})


@sql_count_checker(query_count=0)
def test_concat_invalid_type_negative(df1):
    native_objs = [df1, "abc"]
    snow_objs = [pd.DataFrame(df1), "abc"]
    eval_snowpark_pandas_result(
        "pd",
        "native_pd",
        _concat_operation(snow_objs, native_objs),
        expect_exception=True,
        expect_exception_type=TypeError,
        expect_exception_match="cannot concatenate object of type '<class 'str'>'; only Series and DataFrame objs are valid",
    )


def _index(labels: list[Hashable]) -> Index:
    # Creates an index with single level
    return Index(labels, tupleize_cols=False)


def _multiindex(labels: list[tuple[Hashable, ...]]) -> MultiIndex:
    return MultiIndex.from_tuples(labels)


@pytest.mark.parametrize(
    "columns1, columns2, expected_cols",
    [
        (_index([1]), _index([1]), _index([1, 1])),
        (_index([1]), _index([(1, 2)]), _index([1, (1, 2)])),
        (_index([1]), _multiindex([(1,)]), _index([1, (1,)])),
        (_index([1]), _multiindex([(1, 2)]), _index([1, (1, 2)])),
        (_index([(1, 2)]), _multiindex([(1,)]), _index([(1, 2), (1,)])),
        (_index([(1, 2)]), _multiindex([(1, 2)]), _index([(1, 2), (1, 2)])),
        (_index([(1, 2)]), _multiindex([(1, 2, 3)]), _index([(1, 2), (1, 2, 3)])),
        (_multiindex([(1, 2)]), _index([1]), _index([(1, 2), 1])),
        (_multiindex([(1, 2)]), _index([(1, 2)]), _multiindex([(1, 2), (1, 2)])),
        (_multiindex([(1, 2)]), _index([(1, 2, 3)]), _index([(1, 2), (1, 2, 3)])),
        (_multiindex([(1, 2)]), _multiindex([(1,)]), _index([(1, 2), (1,)])),
        (_multiindex([(1, 2)]), _multiindex([(1, 2)]), _multiindex([(1, 2), (1, 2)])),
        (_multiindex([(1, 2)]), _multiindex([(1, 2, 3)]), _index([(1, 2), (1, 2, 3)])),
    ],
)
@sql_count_checker(query_count=0)
def test_concat_multiindex_columns_axis1(
    columns1, columns2, df_single_col, expected_cols
):
    df1, df2 = pd.DataFrame(df_single_col), pd.DataFrame(df_single_col)
    df1.columns = columns1
    df2.columns = columns2

    result_columns = pd.concat([df1, df2], axis=1).columns
    assert_index_equal(result_columns, expected_cols)


@pytest.mark.parametrize(
    "index1, index2, expected_index, expected_join_count",
    [
        (_index([1]), _index([1]), _index([1, 1]), 2),
        (_index([1]), _index([(1, 2)]), _index([1, (1, 2)]), 2),
        (_index([1]), _multiindex([(1,)]), _index([1, 1]), 2),
        (_index([1]), _multiindex([(1, 2)]), _index([1, (1, 2)]), 3),
        (_index([(1, 2)]), _multiindex([(1,)]), _index([(1, 2), 1]), 2),
        (_index([(1, 2)]), _multiindex([(1, 2)]), _index([(1, 2), (1, 2)]), 3),
        (_index([(1, 2)]), _multiindex([(1, 2, 3)]), _index([(1, 2), (1, 2, 3)]), 4),
        (_multiindex([(1, 2)]), _index([1]), _index([(1, 2), 1]), 3),
        (_multiindex([(1, 2)]), _index([(1, 2)]), _index([(1, 2), (1, 2)]), 3),
        (_multiindex([(1, 2)]), _index([(1, 2, 3)]), _index([(1, 2), (1, 2, 3)]), 3),
        (_multiindex([(1, 2)]), _multiindex([(1,)]), _index([(1, 2), 1]), 3),
        (
            _multiindex([(1, 2)]),
            _multiindex([(1, 2)]),
            _multiindex([(1, 2), (1, 2)]),
            4,
        ),
        (
            _multiindex([(1, 2)]),
            _multiindex([(1, 2, 3)]),
            _index([(1, 2), (1, 2, 3)]),
            5,
        ),
    ],
)
def test_concat_multiindex_row_labels_axis0(
    index1, index2, df_single_col, expected_index, expected_join_count
):
    df1, df2 = pd.DataFrame(df_single_col), pd.DataFrame(df_single_col)
    df1.index = index1
    df2.index = index2

    with SqlCounter(query_count=1, join_count=expected_join_count):
        res_index = pd.concat([df1, df2], axis=0).to_pandas().index
        assert isinstance(res_index, MultiIndex) == isinstance(
            expected_index, MultiIndex
        )

        # Snowflake backend doesn't support tuples datatype. Values returned are of
        # array type.
        if not isinstance(res_index, MultiIndex):
            expected_values = [
                list(v) if isinstance(v, tuple) else v for v in expected_index.tolist()
            ]
        else:
            expected_values = expected_index.tolist()
        assert res_index.tolist() == expected_values


@pytest.mark.parametrize(
    "index1, index2, expected_index, expected_join_count",
    [
        (_index([1]), _index([1]), _index([1]), 3),
        (_index([1]), _multiindex([(1,)]), _index([1]), 3),
        (_index([(1, 2)]), _multiindex([(1, 2)]), _index([(1, 2)]), 4),
        (_index([(1, 2)]), _multiindex([(1, 2, 3)]), _index([(1, 2), (1, 2, 3)]), 5),
        (_multiindex([(1, 2)]), _index([(1, 2)]), _index([(1, 2)]), 4),
        (_multiindex([(1, 2)]), _index([(1, 2, 3)]), _index([(1, 2), (1, 2, 3)]), 4),
        (_multiindex([(1, 2)]), _multiindex([(1, 2)]), _multiindex([(1, 2)]), 5),
        (
            _multiindex([(1, 2)]),
            _multiindex([(1, 2, 3)]),
            _index([(1, 2), (1, 2, 3)]),
            6,
        ),
    ],
)
def test_concat_multiindex_row_labels_axis1(
    index1, index2, df_single_col, expected_index, expected_join_count
):
    df1, df2 = pd.DataFrame(df_single_col), pd.DataFrame(df_single_col)
    df1.index = index1
    df2.index = index2

    with SqlCounter(query_count=1, join_count=expected_join_count):
        res_index = pd.concat([df1, df2], axis=1).to_pandas().index
        assert isinstance(res_index, MultiIndex) == isinstance(
            expected_index, MultiIndex
        )
        # Snowflake backend doesn't support tuples datatype. Values returned are of
        # array type.
        if not isinstance(res_index, MultiIndex):
            expected_values = [
                list(v) if isinstance(v, tuple) else v for v in expected_index.tolist()
            ]
        else:
            expected_values = expected_index.tolist()
        assert res_index.tolist() == expected_values


@pytest.mark.parametrize(
    "index1, index2",
    [
        # single index with integer and array
        (_index([1]), _index([(1, 2)])),
        # single index with integer and multiindex with array
        (_index([1]), _multiindex([(1, 2)])),
        # single index with array and multiindex with integer
        (_index([(1, 2)]), _multiindex([(1,)])),
        # multiindex with array and single index with integer
        (_multiindex([(1, 2)]), _index([1])),
        # multiindex with array and single index with integer
        (_multiindex([(1, 2)]), _multiindex([(1,)])),
    ],
)
def test_concat_multiindex_row_labels_axis1_negative(index1, index2, df_single_col):
    df1, df2 = pd.DataFrame(df_single_col), pd.DataFrame(df_single_col)
    df1.index = index1
    df2.index = index2

    # This behavior is different with Native pandas, where native pandas cast the index
    # to object and performs join successfully. In snowflake, join on columns between Number
    # and Array fails.
    with SqlCounter(query_count=0):
        with pytest.raises(SnowparkSQLException, match="Can not convert parameter"):
            pd.concat([df1, df2], axis=1).to_pandas()


@pytest.mark.parametrize(
    "columns1, columns2, expected_cols",
    [
        (_index([1]), _index([1]), _index([1])),
        (_index([1]), _index([(1, 2)]), _index([1, (1, 2)])),
        (_index([1]), _multiindex([(1,)]), _index([1, (1,)])),
        (_index([1]), _multiindex([(1, 2)]), _index([1, (1, 2)])),
        (_index([(1, 2)]), _multiindex([(1,)]), _index([(1, 2), (1,)])),
        (_index([(1, 2)]), _multiindex([(1, 2)]), _index([(1, 2)])),
        (_index([(1, 2)]), _multiindex([(1, 2, 3)]), _index([(1, 2), (1, 2, 3)])),
        (_multiindex([(1, 2)]), _index([1]), _index([(1, 2), 1])),
        (_multiindex([(1, 2)]), _index([(1, 2)]), _multiindex([(1, 2)])),
        (_multiindex([(1, 2)]), _index([(1, 2, 3)]), _index([(1, 2), (1, 2, 3)])),
        (_multiindex([(1, 2)]), _multiindex([(1,)]), _index([(1, 2), (1,)])),
        (_multiindex([(1, 2)]), _multiindex([(1, 2)]), _multiindex([(1, 2)])),
        (_multiindex([(1, 2)]), _multiindex([(1, 2, 3)]), _index([(1, 2), (1, 2, 3)])),
    ],
)
@sql_count_checker(query_count=0)
def test_concat_multiindex_columns_axis0(
    columns1, columns2, df_single_col, expected_cols
):
    df1, df2 = pd.DataFrame(df_single_col), pd.DataFrame(df_single_col)
    df1.columns = columns1
    df2.columns = columns2

    result_columns = pd.concat([df1, df2], axis=0).columns
    assert_index_equal(result_columns, expected_cols)


def test_concat_index_with_nulls(df1, df2):
    native_objs = [df1, df2]
    df1, df2 = pd.DataFrame(df1), pd.DataFrame(df2)
    df1.set_index([[None, "a", None]])
    df2.set_index([[4, 5, None, 1]])
    snow_objs = [df1, df2]
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            "pd", "native_pd", _concat_operation(snow_objs, native_objs)
        )


@pytest.mark.parametrize(
    "keys",
    [
        ["x", "y", "z"],  # length same as number of frames
        ["x"],  # too short
        ["x", "y", "z", "a"],  # too long
        ["x", "y", "x"],  # duplicate keys
        ["x", "x", "y"],  # duplicate keys
        [("x", 1), ("y", 2), ("z", 3)],  # keys as tuples
    ],
)
def test_concat_with_keys(df1, df2, series1, keys, axis):
    native_objs = [df1, df2, series1]
    snow_objs = [pd.DataFrame(df1), pd.DataFrame(df2), pd.Series(series1)]
    expected_join_count = 2 if axis == 1 and len(keys) > 1 else 0
    with SqlCounter(query_count=1, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            "pd",
            "native_pd",
            _concat_operation(snow_objs, native_objs, keys=keys, axis=axis),
        )


@pytest.mark.parametrize(
    "keys",
    [
        ["x", "y"],  # length same as number of frames
        ["x"],  # too short
        ["x", "y", "z"],  # too long
        ["x", "x"],  # duplicate keys
        [("x", 1), ("y", 2)],  # keys as tuples
    ],
)
def test_concat_same_frame_with_keys(df1, keys, axis):
    native_objs = [df1, df1]
    snow_objs = [pd.DataFrame(df) for df in native_objs]
    expected_join_count = 1 if axis == 1 and len(keys) > 1 else 0
    with SqlCounter(query_count=1, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            "pd",
            "native_pd",
            _concat_operation(snow_objs, native_objs, keys=keys, axis=axis),
        )


@pytest.mark.parametrize("nlevels", [2, 3])
@pytest.mark.parametrize("keys", [["x", "y"], [("x", 1), ("y", 2)]])
@sql_count_checker(query_count=1, join_count=1)
def test_concat_multiindex_columns_with_keys_axis1(df1, df2, nlevels, keys):
    df1, df2 = df1.copy(), df2.copy()
    df1.columns = MultiIndex.from_arrays([df1.columns.tolist()] * nlevels)
    df2.columns = MultiIndex.from_arrays([df2.columns.tolist()] * nlevels)
    native_objs = [df1, df2]
    snow_objs = [pd.DataFrame(df) for df in native_objs]
    eval_snowpark_pandas_result(
        "pd", "native_pd", _concat_operation(snow_objs, native_objs, axis=1, keys=keys)
    )


@sql_count_checker(query_count=1)
def test_concat_single_with_key(df1, axis):
    eval_snowpark_pandas_result(
        "pd",
        "native_pd",
        _concat_operation(
            snow_objs=[pd.DataFrame(df1)], native_objs=[df1], keys=["foo"], axis=axis
        ),
    )


@sql_count_checker(query_count=1)
def test_concat_keys_with_none(df1, df2, axis):
    native_objs = [df1, None, df2]
    snow_objs = [pd.DataFrame(df1), None, pd.DataFrame(df2)]
    eval_snowpark_pandas_result(
        "pd",
        "native_pd",
        _concat_operation(
            snow_objs,
            native_objs,
            keys=["x", "y"],
            axis=axis,
        ),
    )


@pytest.mark.parametrize(
    "names",
    [
        ["a", "b"],  # len same as number of levels
        ["a"],  # too short
        [],  # empty
        None,  # None,
    ],
)
@pytest.mark.parametrize(
    "name1, name2", [("one", "two"), ("one", None), (None, "two"), (None, None)]
)
def test_concat_with_keys_and_names(df1, df2, names, name1, name2, axis):
    df1 = df1.rename_axis(name1, axis=axis)
    df2 = df2.rename_axis(name2, axis=axis)
    native_objs = [df1, df2]
    snow_objs = [pd.DataFrame(df) for df in native_objs]

    # One extra query to convert index to native pandas when creating df
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            "pd",
            "native_pd",
            _concat_operation(
                snow_objs, native_objs, keys=["x", "y"], names=names, axis=axis
            ),
        )


@sql_count_checker(query_count=0)
def test_concat_with_keys_and_extra_names_negative(df1, df2, axis):
    native_objs = [df1, df2]
    snow_objs = [pd.DataFrame(df) for df in native_objs]
    eval_snowpark_pandas_result(
        "pd",
        "native_pd",
        _concat_operation(
            snow_objs, native_objs, keys=["x", "y"], names=["a", "b", "c"], axis=axis
        ),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="Length of names must match number of levels in MultiIndex",
    )


@sql_count_checker(query_count=0)
def test_concat_empty_keys_negative(df1, df2, axis):
    native_objs = [df1, df2]
    snow_objs = [pd.DataFrame(df) for df in native_objs]
    eval_snowpark_pandas_result(
        "pd",
        "native_pd",
        _concat_operation(snow_objs, native_objs, keys=[], axis=axis),
        expect_exception=True,
        expect_exception_type=ValueError,
    )


@pytest.mark.parametrize("dict_keys", [["x", "y"], ["y", "x"]])
def test_concat_dict(df1, df2, dict_keys, axis):
    native_objs = {dict_keys[0]: df1, dict_keys[1]: df2}
    snow_objs = {dict_keys[0]: pd.DataFrame(df1), dict_keys[1]: pd.DataFrame(df2)}
    expected_join_count = 1 if axis == 1 else 0
    with SqlCounter(query_count=1, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            "pd", "native_pd", _concat_operation(snow_objs, native_objs, axis=axis)
        )


@multithreaded_run()
@pytest.mark.parametrize("dict_keys", [["x", "y"], ["y", "x"]])
@pytest.mark.parametrize("keys", [["x", "y"], ["y", "x"], ["x"], ["y"]])
def test_concat_dict_with_keys(df1, df2, dict_keys, keys, axis):
    native_objs = {dict_keys[0]: df1, dict_keys[1]: df2}
    snow_objs = {dict_keys[0]: pd.DataFrame(df1), dict_keys[1]: pd.DataFrame(df2)}
    expected_join_count = 1 if axis == 1 and len(keys) > 1 else 0
    with SqlCounter(query_count=1, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            "pd",
            "native_pd",
            _concat_operation(snow_objs, native_objs, axis=axis, keys=keys),
        )


@sql_count_checker(query_count=0)
def test_concat_dict_with_invalid_keys_negative(df1, df2, axis):
    native_objs = {"x": df1, "y": df2}
    snow_objs = {"x": pd.DataFrame(df1), "y": pd.DataFrame(df2)}
    eval_snowpark_pandas_result(
        "pd",
        "native_pd",
        _concat_operation(snow_objs, native_objs, keys=["x", "z"], axis=axis),
        expect_exception=True,
        expect_exception_type=KeyError,
        expect_exception_match="z",
    )


@sql_count_checker(query_count=1, join_count=1)
def test_concat_with_mixed_tuples_as_column_labels(sort):
    # columns have mixed tuples
    df1 = native_pd.DataFrame({"A": "foo", ("B", 1): "bar"}, index=range(2))
    df2 = native_pd.DataFrame({"B": "foo", ("B", 1): "bar"}, index=range(2))
    native_objs = [df1, df2]
    snow_objs = [pd.DataFrame(df) for df in native_objs]
    eval_snowpark_pandas_result(
        "pd",
        "native_pd",
        _concat_operation(snow_objs, native_objs, axis=1, sort=sort),
    )


def test_concat_empty_df(df1, empty_df, zero_rows_df, zero_columns_df, axis):
    native_objs = [df1, empty_df, zero_columns_df, zero_rows_df]
    native_res = native_pd.concat(native_objs)

    snow_objs = [pd.DataFrame(obj) for obj in native_objs]
    snow_res = pd.concat(snow_objs)

    with SqlCounter(query_count=1):
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_res, native_res)


@pytest.mark.parametrize(
    "index1, index2",
    [
        (
            MultiIndex.from_tuples([(0, 0), (1, 2)]),
            MultiIndex.from_tuples([(0, 0), (1, 3)]),
        ),  # same levels for both frames
        (
            MultiIndex.from_tuples([(0, 0), (1, 2)], names=["a", "b"]),
            MultiIndex.from_tuples([(0, 0), (1, 3)], names=["c", "d"]),
        ),  # same levels, different names
        (
            MultiIndex.from_tuples([(0, 0), (1, 2)], names=["a", "b"]),
            MultiIndex.from_tuples([(0, 0), (1, 3)], names=["a", "c"]),
        ),  # same levels, one overlapping name
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_concat_multiindex(index1, index2):
    df1 = native_pd.DataFrame({"A": [0, 1]}, index=index1)
    df2 = native_pd.DataFrame({"B": [2, 3]}, index=index2)
    native_objs = [df1, df2]
    snow_objs = [pd.DataFrame(df) for df in native_objs]
    eval_snowpark_pandas_result(
        "pd", "native_pd", _concat_operation(snow_objs, native_objs, axis=1)
    )


@pytest.mark.parametrize(
    "type1, type2",
    [("df", "df"), ("series", "series"), ("df", "series")],
)
@pytest.mark.parametrize("col1, col2", [("A", None), ("A", "a"), (1, "1")])
@sql_count_checker(query_count=1, join_count=1)
def test_concat_verify_integrity_axis1(type1, type2, col1, col2):
    if type1 == "df":
        native_obj1 = native_pd.DataFrame([1, 2], columns=[col1])
        snow_obj1 = pd.DataFrame(native_obj1)
    else:
        native_obj1 = native_pd.Series([1, 2], name=col1)
        snow_obj1 = pd.Series(native_obj1)

    if type2 == "df":
        native_obj2 = native_pd.DataFrame([1, 2], columns=[col2])
        snow_obj2 = pd.DataFrame(native_obj2)
    else:
        native_obj2 = native_pd.Series([1, 2], name=col2)
        snow_obj2 = pd.Series(native_obj2)

    eval_snowpark_pandas_result(
        "pd",
        "native_pd",
        _concat_operation(
            snow_objs=[snow_obj1, snow_obj2],
            native_objs=[native_obj1, native_obj2],
            axis=1,
            verify_integrity=True,
        ),
    )


@pytest.mark.parametrize("obj2_type", ["df", "series"])
@sql_count_checker(query_count=0)
def test_concat_verify_integrity_axis1_negative(obj2_type):
    # obj1 is always a DataFrame.
    obj1 = pd.DataFrame([1, 2], columns=["A"])

    if obj2_type == "df":
        obj2 = pd.DataFrame([3, 4], columns=["A"])
    else:
        obj2 = pd.Series([3, 4], name="A")

    msg = "Columns have overlapping values"
    with pytest.raises(ValueError, match=msg):
        pd.concat([obj1, obj2], axis=1, verify_integrity=True)


@sql_count_checker(query_count=0)
def test_concat_all_series_verify_integrity_axis1_negative():
    # Native pandas has a bug, it doesn't apply integrity check when all input objects
    # are series.
    # Snowpark pandas apply integrity check irrespective of input object types.
    obj1 = pd.Series([1, 2], name="A")
    obj2 = pd.Series([3, 4], name="A")
    with pytest.raises(ValueError, match="Columns have overlapping values"):
        pd.concat([obj1, obj2], axis=1, verify_integrity=True)


@sql_count_checker(query_count=1, join_count=1)
def test_concat_verify_integrity_axis1_with_keys():
    # Even though original frames have duplicate columns, after adding keys to column
    # labels duplicates are resolved, hence no error.
    obj1 = native_pd.DataFrame([1, 2], columns=["A"])
    obj2 = native_pd.DataFrame([3, 4], columns=["A"])
    native_objs = [obj1, obj2]
    snow_objs = [pd.DataFrame(obj) for obj in native_objs]
    eval_snowpark_pandas_result(
        "pd",
        "native_pd",
        _concat_operation(
            snow_objs, native_objs, axis=1, verify_integrity=True, keys=["x", "y"]
        ),
    )


@pytest.mark.parametrize(
    "index1, index2",
    [
        ([0, 1], [5, 6]),
        (_multiindex([(1, 1), (1, 2)]), _multiindex([(2, 1), (2, 2)])),
    ],
)
@sql_count_checker(query_count=2, union_count=2)
def test_concat_verify_integrity_axis0(index1, index2):
    df1 = native_pd.DataFrame([1, 2], columns=["a"], index=index1)
    df2 = native_pd.DataFrame([1, 2], columns=["a"], index=index2)
    native_objs = [df1, df2]
    snow_objs = [pd.DataFrame(obj) for obj in native_objs]
    eval_snowpark_pandas_result(
        "pd",
        "native_pd",
        _concat_operation(snow_objs, native_objs, verify_integrity=True),
    )


@pytest.mark.parametrize(
    "index1, index2",
    [([0, 1], [0, 1]), (_multiindex([(1, 1), (1, 2)]), _multiindex([(2, 1), (1, 2)]))],
)
@sql_count_checker(query_count=2, union_count=2)
def test_concat_verify_integrity_axis0_with_keys(index1, index2):
    # Even though original frames have duplicate columns, after adding keys to column
    # labels duplicates are resolved, hence no error.
    df1 = native_pd.DataFrame([1, 2], columns=["a"], index=index1)
    df2 = native_pd.DataFrame([1, 2], columns=["a"], index=index2)
    native_objs = [df1, df2]
    snow_objs = [pd.DataFrame(obj) for obj in native_objs]
    eval_snowpark_pandas_result(
        "pd",
        "native_pd",
        _concat_operation(
            snow_objs, native_objs, verify_integrity=True, keys=["red", "green"]
        ),
    )


@pytest.mark.parametrize(
    "index1, index2",
    [([0, 1], [0, 1]), (_multiindex([(1, 1), (1, 2)]), _multiindex([(2, 1), (1, 2)]))],
)
@sql_count_checker(query_count=1, union_count=1)
def test_concat_verify_integrity_axis0_with_ignore_index(index1, index2):
    # Even though original frames have duplicate columns, ignore_index=True will
    # replace original index values with values 0 to n-1, hence no error.
    df1 = native_pd.DataFrame([1, 2], columns=["a"], index=index1)
    df2 = native_pd.DataFrame([1, 2], columns=["a"], index=index2)
    native_objs = [df1, df2]
    snow_objs = [pd.DataFrame(obj) for obj in native_objs]
    eval_snowpark_pandas_result(
        "pd",
        "native_pd",
        _concat_operation(
            snow_objs, native_objs, verify_integrity=True, ignore_index=True
        ),
    )


@pytest.mark.parametrize(
    "index1, index2",
    [
        ([0, 1], [0, 1]),
        (_multiindex([(1, 1), (1, 2)]), _multiindex([(2, 1), (1, 2)])),
        ([1, 1], [2, 3]),
    ],
)
@sql_count_checker(query_count=2, union_count=2)
def test_concat_verify_integrity_axis0_negative(index1, index2):
    df1 = native_pd.DataFrame([1, 2], columns=["a"], index=index1)
    df2 = native_pd.DataFrame([1, 2], columns=["a"], index=index2)
    native_objs = [df1, df2]
    snow_objs = [pd.DataFrame(obj) for obj in native_objs]
    eval_snowpark_pandas_result(
        "pd",
        "native_pd",
        _concat_operation(snow_objs, native_objs, verify_integrity=True),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="Indexes have overlapping values: ",
    )


@sql_count_checker(query_count=2, union_count=2)
def test_concat_verify_integrity_axis0_large_overlap_negative():
    df = pd.DataFrame(data=list(range(100)))
    msg = "Indexes have overlapping values. Few of them are: .* Please run "
    with pytest.raises(ValueError, match=msg):
        pd.concat([df, df], verify_integrity=True)


@sql_count_checker(query_count=0)
def test_concat_levels_negative(df1, df2):
    df1, df2 = pd.DataFrame(df1), pd.DataFrame(df2)
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas doesn't support 'levels' argument in concat API",
    ):
        pd.concat([df1, df2], keys=["x", "y"], names=["a", "b"], levels=["a", "b"])


def test_concat_sorted_frames():
    df1 = native_pd.DataFrame({"A": [5, 2, 7]})
    df2 = native_pd.DataFrame({"B": [3, 5, 6]})
    df3 = native_pd.DataFrame({"A": [2, 1, 7], "B": [3, 5, 4]})
    native_objs = [df1, df2, df3]
    snow_objs = [pd.DataFrame(obj) for obj in native_objs]
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            "pd", "native_pd", _concat_operation(snow_objs, native_objs)
        )
    native_objs = [
        df1.sort_values(by="A"),
        df2.sort_values(by="B"),
        df3.sort_values(by=["B", "A"]),
    ]
    snow_objs = [pd.DataFrame(obj) for obj in native_objs]
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            "pd", "native_pd", _concat_operation(snow_objs, native_objs)
        )


@pytest.mark.parametrize(
    "columns1, columns2, expected_rows, expected_cols",
    [
        (
            ["A", "C", "C"],
            ["A", "C", "C"],
            [[1, 2, 3], [4, 5, 6]],
            ["A", "C", "C"],
        ),  # same columns,
        (
            ["A", "C", "C"],
            ["C", "A", "C"],
            [[1, 2, 3], [5, 4, 6]],
            ["A", "C", "C"],
        ),  # same columns, different order
        (
            ["A", "C", "C"],
            ["A", "B", "C"],
            [[1, 2, 3, None], [4, 6, None, 5]],
            ["A", "C", "C", "B"],
        ),  # duplicate in frame1
        (
            ["A", "B", "C"],
            ["A", "C", "C"],
            [[1, 2, 3, None], [4, None, 5, 6]],
            ["A", "B", "C", "C"],
        ),  # duplicate in frame2
    ],
)
@sql_count_checker(query_count=2, union_count=1)
def test_concat_duplicate_columns(columns1, columns2, expected_rows, expected_cols):
    df1 = pd.DataFrame([[1, 2, 3]], columns=columns1)
    df2 = pd.DataFrame([[4, 5, 6]], columns=columns2)
    expected_df = pd.DataFrame(expected_rows, columns=expected_cols, index=[0, 0])
    assert_frame_equal(pd.concat([df1, df2]), expected_df, check_dtype=False)


@pytest.mark.parametrize("value1", [4, 1.5, True, "c", (1, 2), {"a": 1}])
@pytest.mark.parametrize("value2", [4, 1.5, True, "c", (1, 2), {"a": 1}])
@sql_count_checker(query_count=1, union_count=1)
def test_concat_type_mismatch(value1, value2):
    df1 = native_pd.DataFrame({"A": [value1]})
    df2 = native_pd.DataFrame({"A": [value2]})
    native_objs = [df1, df2]
    snow_objs = [pd.DataFrame(obj) for obj in native_objs]
    eval_snowpark_pandas_result(
        "pd",
        "native_pd",
        _concat_operation(snow_objs, native_objs),
    )


@pytest.mark.parametrize(
    "index1, index2",
    [
        (Index([0]), Index([1])),  # both None
        (Index([0]), Index([1], name="right")),  # first frame index name is None
        (Index([0], name="left"), Index([1])),  # second frame index name is None
        (MultiIndex.from_tuples([(0, 0)]), MultiIndex.from_tuples([(1, 1)])),
        (
            MultiIndex.from_tuples([(0, 0)], names=["left", None]),
            MultiIndex.from_tuples([(1, 1)], names=["left", "right"]),
        ),
    ],
)
@sql_count_checker(query_count=1, union_count=1)
def test_concat_none_index_name(index1, index2):
    df1 = native_pd.DataFrame([11], columns=["A"], index=index1)
    df2 = native_pd.DataFrame([22], columns=["B"], index=index2)
    native_objs = [df1, df2]
    snow_objs = [pd.DataFrame(obj) for obj in native_objs]
    eval_snowpark_pandas_result(
        "pd",
        "native_pd",
        _concat_operation(snow_objs, native_objs),
    )


@sql_count_checker(query_count=5, union_count=1)
def test_concat_from_file(resources_path):
    test_files = TestFiles(resources_path)
    df1 = native_pd.read_csv(test_files.test_concat_file1_csv)
    df2 = native_pd.read_csv(test_files.test_concat_file1_csv)
    native_objs = [df1, df2]
    df1 = pd.read_csv(test_files.test_concat_file1_csv)
    df2 = pd.read_csv(test_files.test_concat_file1_csv)
    snow_objs = [df1, df2]
    eval_snowpark_pandas_result(
        "pd",
        "native_pd",
        _concat_operation(snow_objs, native_objs),
    )


class TestConcatPositionalIndexWithOtherIndex_SNOW_2268219:
    @sql_count_checker(query_count=1, union_count=1, join_count=1)
    @pytest.mark.parametrize(
        "testing_dfs_from_read_snowflake",
        [native_pd.DataFrame({"col0": [1]})],
        indirect=True,
    )
    def test_positional_index_on_left(
        self,
        testing_dfs_from_read_snowflake,
    ):
        snow_df1, native_df1 = testing_dfs_from_read_snowflake
        snow_df2, native_df2 = snow_df1.copy(), native_df1.copy()
        snow_df2.index = [2]
        native_df2.index = [2]
        eval_snowpark_pandas_result(
            "pd",
            "native_pd",
            _concat_operation(
                snow_objs=[snow_df1, snow_df2], native_objs=[native_df1, native_df2]
            ),
        )

    @sql_count_checker(query_count=1, union_count=1, join_count=1)
    @pytest.mark.parametrize(
        "testing_dfs_from_read_snowflake",
        [native_pd.DataFrame({"col0": [1]})],
        indirect=True,
    )
    def test_positional_index_on_right(
        self,
        testing_dfs_from_read_snowflake,
    ):
        snow_df1, native_df1 = testing_dfs_from_read_snowflake
        snow_df2, native_df2 = snow_df1.copy(), native_df1.copy()
        snow_df1.index = [2]
        native_df1.index = [2]
        eval_snowpark_pandas_result(
            "pd",
            "native_pd",
            _concat_operation(
                snow_objs=[snow_df1, snow_df2], native_objs=[native_df1, native_df2]
            ),
        )


@sql_count_checker(query_count=1, join_count=2)
def test_concat_keys():
    native_data = {
        "one": native_pd.Series([1, 2, 3], index=["a", "b", "c"]),
        "two": native_pd.Series([2, 3, 4, 5], index=["a", "b", "c", "d"]),
        "three": native_pd.Series([3, 4, 5], index=["b", "c", "d"]),
    }
    native_df = native_pd.concat(native_data.values(), axis=1, keys=native_data.keys())

    data = {
        "one": pd.Series([1, 2, 3], index=["a", "b", "c"]),
        "two": pd.Series([2, 3, 4, 5], index=["a", "b", "c", "d"]),
        "three": pd.Series([3, 4, 5], index=["b", "c", "d"]),
    }
    snow_df = pd.concat(data.values(), axis=1, keys=data.keys())
    assert_frame_equal(snow_df, native_df, check_dtype=False)


@sql_count_checker(query_count=1, join_count=0)
def test_concat_object_with_same_index_with_dup(join):
    df = native_pd.DataFrame(
        {
            "C": [1, 2, 3],
            "A": ["a", "b", "c"],
            "D": [3, 2, 1],
        },
        index=native_pd.Index([2, 1, 2]),
    )
    native_objs = [df[["C", "A"]], df["D"], df["C"] + 1]
    snow_df = pd.DataFrame(df)
    snow_objs = [snow_df[["C", "A"]], snow_df["D"], snow_df["C"] + 1]
    eval_snowpark_pandas_result(
        "pd",
        "native_pd",
        _concat_operation(snow_objs, native_objs, join=join, axis=1),
    )


@sql_count_checker(query_count=1, join_count=0)
def test_concat_object_with_same_index_with_dup_sort(join):
    df = native_pd.DataFrame(
        {
            "C": [1, 2, 3],
            "A": ["a", "b", "c"],
            "D": [3, 2, 1],
        },
        index=native_pd.Index([2, 1, 2]),
    )
    snow_df = pd.DataFrame(df)
    snow_objs = [snow_df["D"], snow_df["C"] + 1]

    # Note this behavior is different from native pandas, native pandas
    # throws ValueError: cannot reindex on an axis with duplicate labels.
    # With snowpark pandas, the operation will be successful with an align
    # behavior.
    expected_result = native_pd.DataFrame(
        {
            "D": [2, 3, 1],
            "C": [3, 2, 4],
        },
        index=native_pd.Index([1, 2, 2]),
    )
    snow_res = pd.concat(snow_objs, join=join, axis=1, sort=True)
    assert_frame_equal(snow_res, expected_result)


@sql_count_checker(query_count=3, join_count=0)
def test_concat_series_from_same_df(join):
    num_cols = 4
    select_data = [f'{i} as "{i}"' for i in range(num_cols)]
    query = f"select {', '.join(select_data)}"

    # concat today uses join_on_index to concat all series, we use
    # read_snowflake here so that the default index is created and
    # managed by snowpark pandas, which is the same as row position
    # column. This creates a valid optimization scenario for join, where
    # join performed on the same row_position column doesn't require
    # actual join.
    # This can not be done with pd.DataFrame constructor because the index
    # and row position column is controlled by client side, which are
    # different columns.
    df = pd.read_snowflake(query)
    # Follow read_snowflake with a sort operation to ensure that ordering is stable and tests are not flaky.
    df = df.sort_values(df.columns.to_list())

    series = [df[col] for col in df.columns]
    final_df = pd.concat(series, join=join, axis=1)

    assert_frame_equal(df, final_df)


@sql_count_checker(query_count=3, join_count=0)
def test_df_creation_from_series_from_same_df():
    num_cols = 6
    select_data = [f'{i} as "{i}"' for i in range(num_cols)]
    query = f"select {', '.join(select_data)}"

    df = pd.read_snowflake(query)
    # Follow read_snowflake with a sort operation to ensure that ordering is stable and tests are not flaky.
    df = df.sort_values(df.columns.to_list())

    df_dict = {col: df[col] for col in df.columns}
    final_df = pd.DataFrame(df_dict)

    assert_frame_equal(df, final_df)


@sql_count_checker(query_count=0)
def test_concat_timedelta_not_implemented(df1):
    df1 = pd.DataFrame(df1)
    with pytest.raises(NotImplementedError):
        pd.concat([df1, df1, df1.astype({"C": "timedelta64[ns]"})])
