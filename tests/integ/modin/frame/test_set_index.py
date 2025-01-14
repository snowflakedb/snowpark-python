#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_frame_equal, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.fixture(
    scope="function",
    params=[
        param(
            {"a": [1, 2, 2], "b": [3, 4, 5], ("c", "d"): [0, 0, 1]}, id="frame_of_ints"
        ),
        param(
            {
                "a": native_pd.to_timedelta([1, 2, 2]),
                "b": [3, 4, 5],
                ("c", "d"): [0, 0, 1],
            },
            id="frame_of_ints_and_timedelta",
        ),
    ],
)
def native_df(request):
    return native_pd.DataFrame(request.param)


@pytest.fixture
def snow_df(native_df):
    return pd.DataFrame(native_df)


@pytest.fixture(params=[True, False])
def append(request):
    return request.param


@pytest.fixture(params=[True, False])
def drop(request):
    return request.param


@sql_count_checker(query_count=1, join_count=2)
@pytest.mark.parametrize(
    "pandas_index",
    [
        param(
            native_pd.MultiIndex.from_tuples([(5, 4), (4, 5), (5, 5)]), id="ints_index"
        ),
        param(
            native_pd.MultiIndex.from_tuples(
                [(pd.Timedelta(5), 4), (pd.Timedelta(4), 5), (pd.Timedelta(5), 5)]
            ),
            id="timedelta_and_int_index",
        ),
    ],
)
def test_set_index_multiindex(snow_df, native_df, pandas_index):
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.set_index(pandas_index),
    )


@sql_count_checker(query_count=1, join_count=1)
def test_set_index_timedelta_index(snow_df, native_df):
    values = native_pd.to_timedelta([1, 2, 3])
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.set_index(
            pd.TimedeltaIndex(values)
            if isinstance(df, pd.DataFrame)
            else native_pd.TimedeltaIndex(values)
        ),
    )


@sql_count_checker(query_count=2)
def test_set_index_empty_column():
    snow_df = pd.DataFrame(
        [
            {"a": 1, "p": 0},
            {"a": 2, "m": 10},
            {"a": 3, "m": 11, "p": 20},
            {"a": 4, "m": 12, "p": 21},
        ],
        # All values in columns 'x' are None.
        columns=["a", "m", "p", "x"],
    )

    eval_snowpark_pandas_result(
        snow_df, snow_df.to_pandas(), lambda df: df.set_index(["a", "x"])
    )


@sql_count_checker(query_count=4)
def test_set_index_empty_dataframe():
    # This data-types have been copied from native pandas test.
    snow_df = pd.DataFrame(
        {"a": pd.Series(dtype="datetime64[ns]"), "b": pd.Series(dtype="int64"), "c": []}
    )
    eval_snowpark_pandas_result(
        snow_df, snow_df.to_pandas(), lambda df: df.set_index(["a", "b"])
    )


@sql_count_checker(query_count=2)
def test_set_index_multiindex_columns(snow_df):
    columns = native_pd.MultiIndex.from_tuples([("foo", 1), ("foo", 2), ("bar", 1)])
    snow_df.columns = columns

    eval_snowpark_pandas_result(
        snow_df, snow_df.to_pandas(), lambda df: df.set_index(("foo", 1))
    )


@sql_count_checker(query_count=1, join_count=1)
def test_set_index_different_index_length():
    # pandas raises an error for this case, but we instead join the index to
    # the current dataframe on row position.
    snow_df = pd.DataFrame({"a": [1, 2, 2], "b": [3, 4, 5], ("c", "d"): [0, 0, 1]})
    index = pd.Index([1, 2])
    actual_df = snow_df.set_index(index)
    expected_df = native_pd.DataFrame(
        {"a": [1, 2, 2], "b": [3, 4, 5], ("c", "d"): [0, 0, 1]}, index=[1, 2, np.nan]
    )
    assert_frame_equal(actual_df, expected_df)


@sql_count_checker(query_count=1)
def test_set_index_dup_column_name():
    snow_df = pd.DataFrame([[1, 2, 3], [4, 5, 6]])
    snow_df.columns = native_pd.Index(["A", "A", "B"])
    eval_snowpark_pandas_result(
        snow_df,
        snow_df.to_pandas(),
        lambda df: df.set_index("A"),
        expect_exception=True,
        expect_exception_match="The column label 'A' is not unique",
        expect_exception_type=ValueError,
        assert_exception_equal=False  # we provide better error message than pandas: "ValueError: Index data must be
        # 1-dimensional"
    )


def test_set_index_names(snow_df):
    with SqlCounter(query_count=1):
        # Verify column names becomes index names.
        # multi index, native pandas automatically used
        assert snow_df.set_index(["a", "b"]).index.names == ["a", "b"]

    # Verify name from input index is set.
    index = pd.Index([1, 2, 0])
    index.names = ["iname"]
    with SqlCounter(query_count=0):
        assert snow_df.set_index(index).index.names == ["iname"]

    # Verify names from input multiindex are set.
    multi_index = native_pd.MultiIndex.from_arrays(
        [[1, 1, 2], [1, 2, 1]], names=["a", "b"]
    )
    with SqlCounter(query_count=1, join_count=2):
        assert snow_df.set_index(multi_index).index.names == ["a", "b"]

    with SqlCounter(query_count=2, join_count=4):
        # Verify that [MultiIndex, MultiIndex] yields a MultiIndex rather
        # than a pair of tuples
        multi_index2 = multi_index.rename(["C", "D"])
        eval_snowpark_pandas_result(
            snow_df,
            snow_df.to_pandas(),
            lambda df: df.set_index([multi_index, multi_index2]),
        )


@pytest.mark.parametrize("inplace", [True, False])
@pytest.mark.parametrize("keys", ["a", "b", ["a", "b"], ("c", "d")])
@sql_count_checker(query_count=1)
def test_set_index_drop_inplace(keys, drop, inplace, native_df):
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df.copy(),
        native_df,
        lambda df: df.set_index(keys, drop=drop, inplace=inplace),
        inplace=inplace,
    )


@pytest.mark.parametrize("keys", ["a", "b", ["a", "b"], ("c", "d")])
@sql_count_checker(query_count=1)
def test_set_index_append(keys, drop, native_df):
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.set_index(keys, drop=drop, append=True),
    )


@pytest.mark.parametrize("keys", ["a", "b", ["a", "b"]])
@sql_count_checker(query_count=1)
def test_set_index_append_to_multiindex(keys, drop, native_df):
    snow_df = pd.DataFrame(native_df)
    snow_df = snow_df.set_index([("c", "d")], append=True)
    native_df = native_df.set_index([("c", "d")], append=True)
    # append to existing multiindex
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.set_index(keys, drop=drop, append=True),
    )


@sql_count_checker(query_count=1)
def test_set_index_duplicate_label_in_dataframe_negative(drop):
    # note that we are not using the mixed-type dataframe from the snow_df
    # fixture because we want to avoid
    # https://github.com/pandas-dev/pandas/issues/30965
    snow_df = pd.DataFrame({"a": [1, 2, 2], "b": [3, 4, 5], ("c", "d"): [0, 0, 1]})
    # rename to create df with columns ['a', 'a', ('c', 'd')]
    snow_df = snow_df.rename(columns={"b": "a"})
    # Verify error for native pandas.
    with pytest.raises(ValueError, match="Index data must be 1-dimensional"):
        snow_df.to_pandas().set_index("a", drop=drop)
    # Verify error for snowpark pandas. We use difference error message.
    with pytest.raises(ValueError, match="The column label 'a' is not unique"):
        snow_df.set_index("a", drop=drop)


@sql_count_checker(query_count=1)
def test_set_index_duplicate_label_in_keys(native_df, drop, append):
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.set_index(["a", "a"], drop=drop, append=append),
    )


@pytest.mark.parametrize(
    "obj_type",
    [
        pd.Series,
        pd.Index,
        np.array,
        list,
        lambda x: [list(x)],
        lambda x: native_pd.MultiIndex.from_arrays([x]),
    ],
)
def test_set_index_pass_single_array(obj_type, drop, append, native_df):
    snow_df = pd.DataFrame(native_df)
    array = ["one", "two", "three"]
    key = obj_type(array)
    if obj_type == list:
        # list of strings gets interpreted as list of keys
        with SqlCounter(query_count=0):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                lambda df: df.set_index(key, drop=drop, append=append),
                expect_exception=True,
            )
    else:
        expected_query_count = 1
        if obj_type == pd.Series or obj_type == pd.Index:
            expected_query_count = 2
        with SqlCounter(query_count=expected_query_count, join_count=1):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                lambda df: df.set_index(
                    key.to_pandas()
                    if isinstance(df, native_pd.DataFrame)
                    and isinstance(key, (pd.Series, pd.Index))
                    else key,
                    drop=drop,
                    append=append,
                ),
            )


@pytest.mark.parametrize(
    "obj_type",
    [
        pd.Series,
        pd.Index,
        np.array,
        list,
        lambda x: native_pd.MultiIndex.from_arrays([x]),
    ],
)
def test_set_index_pass_arrays(obj_type, drop, append, native_df):
    snow_df = pd.DataFrame(native_df)
    array = ["one", "two", "three"]
    key = obj_type(array)
    keys = ["a", obj_type(array)]
    native_keys = [
        "a",
        key.to_pandas() if isinstance(key, (pd.Series, pd.Index)) else key,
    ]
    with SqlCounter(query_count=1, join_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.set_index(
                native_keys if isinstance(df, native_pd.DataFrame) else keys,
                drop=drop,
                append=append,
            ),
        )


@pytest.mark.parametrize(
    "obj_type2",
    [
        pd.Series,
        pd.Index,
        np.array,
        list,
        iter,
        lambda x: native_pd.MultiIndex.from_arrays([x]),
    ],
)
@pytest.mark.parametrize(
    "obj_type1",
    [
        pd.Series,
        pd.Index,
        np.array,
        list,
        iter,
        lambda x: native_pd.MultiIndex.from_arrays([x]),
    ],
)
def test_set_index_pass_arrays_duplicate(obj_type1, obj_type2, drop, append, native_df):
    snow_df = pd.DataFrame(native_df)
    array = ["one", "two", "three"]
    keys = [obj_type1(array), obj_type2(array)]
    if obj_type1 == pd.Series:
        obj_type1 = native_pd.Series
    elif obj_type1 == pd.Index:
        obj_type1 = native_pd.Index
    if obj_type2 == pd.Series:
        obj_type2 = native_pd.Series
    elif obj_type2 == pd.Index:
        obj_type2 = native_pd.Index
    native_keys = [obj_type1(array), obj_type2(array)]

    with SqlCounter(query_count=1, join_count=2):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.set_index(
                native_keys if isinstance(df, native_pd.DataFrame) else keys,
                drop=drop,
                append=append,
            ),
        )


@sql_count_checker(query_count=1, join_count=2)
def test_set_index_pass_multiindex(drop, append, native_df):
    snow_df = pd.DataFrame(native_df)
    index_data = [["one", "two", "three"], [9, 3, 7]]
    keys = native_pd.MultiIndex.from_arrays(index_data, names=["a", "b"])
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.set_index(keys, drop=drop, append=append),
    )


@pytest.mark.parametrize(
    "keys, expected_query_count",
    [
        (["a"], 3),
        ([[1, 6, 6]], 3),
    ],
)
def test_set_index_verify_integrity_negative(native_df, keys, expected_query_count):
    snow_df = pd.DataFrame(native_df)
    with SqlCounter(query_count=expected_query_count):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.set_index(keys, verify_integrity=True),
            expect_exception=True,
            expect_exception_type=ValueError,
            expect_exception_match="Index has duplicate keys",
        )


@pytest.mark.parametrize(
    "keys",
    [
        ["foo", "bar"],  # Column names are ['a', 'b', ('c', 'd')]
        [[1, 2, 3], "X"],  # Non-existent key 'X' in list with arrays
        (1, 2, 3),  # Tuple always raises KeyError
        [(1, 2, 3), "a"],  # Tuple in list also raises KeyError
    ],
)
@sql_count_checker(query_count=0)
def test_set_index_raise_keys_negative(keys, drop, append, native_df):
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.set_index(keys, drop=drop, append=append),
        expect_exception=True,
        expect_exception_type=KeyError,
    )


@pytest.mark.parametrize("keys", [{"abc"}, ["a", {"abc"}]])
@sql_count_checker(query_count=0)
def test_set_index_raise_on_invalid_type_set_negative(keys, drop, append, native_df):
    snow_df = pd.DataFrame(native_df)
    error_msg = (
        'The parameter "keys" may be a column key, one-dimensional array,'
        + " or a list containing only valid column keys and one-dimensional"
        + " arrays. Received column of type <class 'set'>"
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.set_index(keys, drop=drop, append=append),
        expect_exception=True,
        expect_exception_type=TypeError,
        expect_exception_match=error_msg,
        # Native pandas has a silly bug where it ends up adding an extra full stop in
        # error message. So we ignore matching with native pandas here.
        # https://github.com/pandas-dev/pandas/blob/1.5.x/pandas/core/frame.py#L6005
        assert_exception_equal=False,
    )


class TestSetIndexCustomLabelType:
    class CustomLabel:
        def __init__(self, name, color) -> None:
            self.name = name
            self.color = color

        def __str__(self) -> str:
            return f"<Thing {repr(self.name)}>"

        # necessary for pretty KeyError
        __repr__ = __str__

    class CustomLabelIterable(frozenset):
        # need to stabilize repr for KeyError (due to random order in sets)
        def __repr__(self) -> str:
            tmp = sorted(self)
            joined_reprs = ", ".join(map(repr, tmp))
            # double curly brace prints one brace in format string
            return f"frozenset({{{joined_reprs}}})"

    @pytest.mark.parametrize(
        "label_type",
        [
            CustomLabel,
            lambda x, y: TestSetIndexCustomLabelType.CustomLabelIterable([x, y]),
        ],
    )
    @sql_count_checker(query_count=6)
    def test_set_index_custom_label_type(self, label_type):
        label1 = label_type("One", "red")
        label2 = label_type("Two", "blue")
        snow_df = pd.DataFrame({label1: [0, 1], label2: [2, 3]})
        # use custom label directly
        eval_snowpark_pandas_result(
            snow_df,
            snow_df.to_pandas(),
            lambda df: df.set_index(label2),
        )

        # custom label wrapped in list
        eval_snowpark_pandas_result(
            snow_df,
            snow_df.to_pandas(),
            lambda df: df.set_index([label2]),
        )

        # missing key
        label3 = label_type("Three", "pink")
        msg = "None of .* are in the columns"
        # missing label directly
        eval_snowpark_pandas_result(
            snow_df,
            snow_df.to_pandas(),
            lambda df: df.set_index(label3),
            expect_exception=True,
            expect_exception_type=KeyError,
            expect_exception_match=msg,
        )
        # missing label in list
        eval_snowpark_pandas_result(
            snow_df,
            snow_df.to_pandas(),
            lambda df: df.set_index([label3]),
            expect_exception=True,
            expect_exception_type=KeyError,
            expect_exception_match=msg,
        )

    @sql_count_checker(query_count=0)
    def test_set_index_custom_label_type_raises(self):
        # purposefully inherit from something unhashable
        class UnhashableLabel(set):
            def __init__(self, name, color) -> None:
                self.name = name
                self.color = color

            def __str__(self) -> str:
                return f"<Thing {repr(self.name)}>"

        thing1 = UnhashableLabel("One", "red")
        thing2 = UnhashableLabel("Two", "blue")
        df = pd.DataFrame([[0, 2], [1, 3]], columns=[thing1, thing2])

        msg = 'The parameter "keys" may be a column key, .*'

        with pytest.raises(TypeError, match=msg):
            # use custom label directly
            df.set_index(thing2)

        with pytest.raises(TypeError, match=msg):
            # custom label wrapped in list
            df.set_index([thing2])

    @pytest.mark.parametrize(
        "sample",
        [
            native_pd.Index([1, 2, 3, 4], name="num"),
            native_pd.Series([1, 2, 3, 4], name="num"),
        ],
    )
    @sql_count_checker(query_count=1, join_count=1)
    def test_set_index_with_index_series_name(self, sample):
        df = native_pd.DataFrame(
            {
                "month": [1, 4, 7, 10],
                "year": [2012, 2014, 2013, 2014],
                "sale": [55, 40, 84, 31],
            }
        )

        eval_snowpark_pandas_result(
            pd.DataFrame(df),
            df,
            lambda df: df.set_index([sample])
            if isinstance(df, native_pd.DataFrame)
            else df.set_index(
                [sample] if isinstance(sample, pd.Index) else pd.Series(sample)
            ),
        )
