#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import random

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from modin.pandas.utils import is_scalar
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_frame_equal,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    create_test_dfs,
    eval_snowpark_pandas_result,
    try_cast_to_snowpark_pandas_series,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

# these tests are from original pandas and modified slightly to cover more scenarios.
# Original tests can be found in pandas/tests/copy_view/test_setitem.py.


SERIES_AND_LIST_LIKE_KEY_AND_ITEM_VALUES = [
    # series
    native_pd.Series([random.randint(0, 6) for _ in range(7)]),
    # list-like - not all cases are covered here, should be covered in series/test_loc.py
    native_pd.Index([random.randint(0, 6) for _ in range(7)]),
]

# Values that are scalars or behave like scalar keys and items.
SCALAR_LIKE_VALUES = [0, "xyz", None, 3.14]


@pytest.mark.parametrize(
    "key",
    [
        "a",
        [True, False, True],
        native_pd.Series([True, False, True]),
        ["a", "b"],
        ["b", "a"],
        native_pd.Series(["a", "b"]),
        native_pd.Series(["b", "a"]),
    ],
)
@pytest.mark.parametrize("dtype", ["int", "timedelta64[ns]"])
def test_df_setitem_df_value(key, dtype):
    data = {"a": [1, 2, 3], "b": [4, 5, 6]}
    snow_df, native_df = create_test_dfs(data, dtype=dtype)
    val = (
        native_pd.DataFrame({"a": [10, 20, 30]}, dtype=dtype)
        if is_scalar(key)
        else native_pd.DataFrame({"a": [10, 20, 30], "c": [40, 50, 60]}, dtype=dtype)
    )

    def setitem(df):
        if isinstance(df, pd.DataFrame):
            _key = pd.Series(key) if isinstance(key, native_pd.Series) else key
            df[_key] = pd.DataFrame(val)
        else:
            df[key] = val

    expected_query_count = 1
    expected_join_count = 1

    if isinstance(key, native_pd.Series) and key.dtype != bool:
        # need to pull key (column index) locally
        expected_query_count += 1

    if all(isinstance(i, bool) for i in key):
        expected_join_count += 1

    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
        eval_snowpark_pandas_result(snow_df, native_df, setitem, inplace=True)


@pytest.mark.parametrize(
    "key",
    [
        ["a", "a"],
        ["x", "x"],
        ["a", "x"],
    ],
)
@pytest.mark.parametrize("key_type", ["list", "series"])
def test_df_setitem_df_value_dedup_columns(key, key_type):
    if key_type == "series":
        key = native_pd.Series(key)
    data = {"a": [1, 2, 3], "b": [4, 5, 6]}
    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)
    val = native_pd.DataFrame({"a": [10, 20, 30], "c": [40, 50, 60]})

    def setitem(df):
        if isinstance(df, pd.DataFrame):
            _key = pd.Series(key) if isinstance(key, native_pd.Series) else key
            df[_key] = pd.DataFrame(val)
        else:
            df[key] = val

    expected_query_count = 1
    expected_join_count = 1

    if isinstance(key, native_pd.Series) and key.dtype != bool:
        # need to pull key (column index) locally
        expected_query_count += 1
    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
        eval_snowpark_pandas_result(snow_df, native_df, setitem, inplace=True)


@pytest.mark.parametrize(
    "key",
    [
        slice(0, 1),
        slice(0, 2),
        slice(0, -1),
        slice(0, -1, -2),
        slice("0", "3"),
        slice("0", "0"),
        slice("1", "2"),
        slice("2", "1"),
        slice("2", "0", -2),
        slice("-10", "100"),
    ],
)
@pytest.mark.parametrize("dtype", ["int", "timedelta64[ns]"])
def test_df_setitem_slice_key_df_value(key, dtype):
    data = {"a": [1, 2, 3], "b": [4, 5, 6]}
    index = ["0", "1", "2"]
    snow_df, native_df = create_test_dfs(data, index=index, dtype=dtype)
    val = native_pd.DataFrame(
        {"a": [10, 20, 30], "c": [40, 50, 60]}, index=index, dtype=dtype
    )
    val = val[key]

    def setitem(df):
        if isinstance(df, pd.DataFrame):
            _key = pd.Series(key) if isinstance(key, native_pd.Series) else key
            df[_key] = pd.DataFrame(val)
        else:
            df[key] = val

    expected_join_count = 3

    with SqlCounter(query_count=1, join_count=expected_join_count):
        eval_snowpark_pandas_result(snow_df, native_df, setitem, inplace=True)


@pytest.mark.parametrize(
    "val_index",
    [
        ["v"],
        ["x"],
    ],
)
@pytest.mark.parametrize(
    "val_columns",
    [
        ["A"],
        ["Z"],
    ],
)
@pytest.mark.parametrize(
    "key",
    [
        ["A"],
        "A",
    ],
)  # matching_item_columns_by_label is always True
@pytest.mark.parametrize("dtype", ["int", "timedelta64[ns]"])
def test_df_setitem_df_single_value(key, val_index, val_columns, dtype):
    native_df = native_pd.DataFrame(
        [[91, -2, 83, 74], [95, -6, 87, 78], [99, -10, 811, 712], [913, -14, 815, 716]],
        index=["x", "x", "z", "w"],
        columns=["A", "B", "C", "D"],
        dtype=dtype,
    )

    val = native_pd.DataFrame([100], columns=val_columns, index=val_index, dtype=dtype)

    def setitem(df):
        if isinstance(df, pd.DataFrame):
            df[key] = pd.DataFrame(val)
        else:
            df[key] = val

    with SqlCounter(query_count=1, join_count=1):
        eval_snowpark_pandas_result(
            pd.DataFrame(native_df), native_df, setitem, inplace=True
        )


@sql_count_checker(query_count=0)
def test_df_setitem_value_df_mismatch_num_col_negative():
    native_df = native_pd.DataFrame(
        [[91, -2, 83, 74], [95, -6, 87, 78], [99, -10, 811, 712], [913, -14, 815, 716]],
        index=["x", "x", "z", "w"],
        columns=["A", "B", "C", "D"],
    )

    key = "A"
    val = native_pd.DataFrame({"a": 100, "b": 101}, index=["x"])

    def setitem(df):
        if isinstance(df, pd.DataFrame):
            df[key] = pd.DataFrame(val)
        else:
            df[key] = val

    eval_snowpark_pandas_result(
        pd.DataFrame(native_df),
        native_df,
        setitem,
        inplace=True,
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="shape mismatch",
        assert_exception_equal=False,
    )


# matching_item_row_by_label is False here.
@sql_count_checker(query_count=1, join_count=2)
def test_df_setitem_array_value_duplicate_index():
    # Case: setting an array as a new column (df[col] = arr) where df's index
    # has duplicate values.
    native_df = native_pd.DataFrame(
        [[91, -2, 83, 74], [95, -6, 87, 78], [99, -10, 811, 712], [913, -14, 815, 716]],
        index=["x", "x", "z", "w"],
        columns=["A", "B", "C", "D"],
    )
    snow_df = pd.DataFrame(native_df)
    arr = np.array([1, 2, 3, 4], dtype="int64")

    def func_insert_new_column(df):
        df["X"] = arr

    eval_snowpark_pandas_result(
        snow_df, native_df, func_insert_new_column, inplace=True
    )


# matching_item_row_by_label is False here.
@sql_count_checker(query_count=2, join_count=6)
def test_df_setitem_array_value():
    # Case: setting an array as a new column (df[col] = arr) copies that data
    data = {"a": [1, 2, 3], "b": [4, 5, 6]}
    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)
    arr = np.array([7, 2, 3], dtype="int64")

    # test first adding a new column
    def func_insert_new_column(df):
        # this will trigger .insert(...) in SnowflakeQueryCompiler
        df["c"] = arr

    eval_snowpark_pandas_result(
        snow_df, native_df, func_insert_new_column, inplace=True
    )

    # then overwrite values of existing column
    def func_insert_new_column(df):
        # this will trigger .setitem(...) in SnowflakeQueryCompiler
        df["a"] = arr

    eval_snowpark_pandas_result(
        snow_df, native_df, func_insert_new_column, inplace=True
    )


@pytest.mark.parametrize(
    "native_df",
    [
        native_pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["A", "B", "C"]),
        native_pd.DataFrame(
            [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
            columns=["A", "B", "C"],
            index=["x", "y", "z"],
        ),
        native_pd.DataFrame(
            [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
            columns=["A", "B", "C"],
            index=["d", "d", "d"],
        ),
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_df_setitem_self_df_set_aligned_row_key(native_df):
    item = native_pd.DataFrame(
        [[10, 20, 30], [40, 50, 60], [70, 80, 90]],
        columns=["C", "A", "B"],
        index=native_df.index,
    )

    def setitem_helper(df):
        df[df["A"] > 1] = (
            item if isinstance(df, native_pd.DataFrame) else pd.DataFrame(item)
        )

    if native_df.index.has_duplicates:
        # pandas raises an error for duplicates while Snowpark pandas does not but can generate more rows
        with pytest.raises(
            ValueError, match="cannot reindex on an axis with duplicate labels"
        ):
            setitem_helper(native_df)
        snow = pd.DataFrame(native_df)
        setitem_helper(snow)
        assert_frame_equal(
            snow,
            native_pd.DataFrame(
                [[1, 2, 3], [40, 50, 60], [70, 80, 90]],
                columns=["A", "B", "C"],
                index=["d", "d", "d"],
            ),
            check_dtype=False,
        )
    else:
        eval_snowpark_pandas_result(
            pd.DataFrame(native_df), native_df, setitem_helper, inplace=True
        )


@pytest.mark.parametrize(
    "column",
    [
        native_pd.Series([3, 4, 5]),
        [3, 4, 5],
        native_pd.Series([42], index=[2]),
        native_pd.Series([-2, 2], name="abc", index=[2, 1]),
        native_pd.Series(
            index=[100, 101, 102]
        ),  # non-matching index will replace with NULLs
        native_pd.Series([]),
        param(["a", "c", "b"], id="string_type"),
        param(
            [pd.Timedelta(5), pd.Timedelta(6), pd.Timedelta(7)],
            id="timedelta_type",
        ),
        native_pd.Series(["x", "y", "z"], index=[2, 0, 1]),
        native_pd.RangeIndex(3),
        native_pd.Index(
            [datetime.datetime.now(), datetime.datetime.now(), datetime.datetime.now()]
        ),
    ],
)
@pytest.mark.parametrize(
    "key", ["a", "x"]
)  # replace existing column, or add new column
def test_df_setitem_replace_column_with_single_column(column, key):
    data = {"a": [1, 2, 3], "b": [4, 5, 6]}
    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)

    def func_insert_new_column(df, column):
        # this will trigger .setitem(...) in SnowflakeQueryCompiler

        # convert to snow objects when testing for Snowpark pandas
        if isinstance(df, pd.DataFrame):
            if isinstance(column, native_pd.Index) and not isinstance(
                column, native_pd.DatetimeIndex
            ):
                column = pd.Index(column)
            elif isinstance(column, native_pd.Series):
                column = try_cast_to_snowpark_pandas_series(column)

        df[key] = column

    expected_join_count = 2
    if isinstance(column, native_pd.Series):
        expected_join_count = 1
    elif isinstance(column, native_pd.Index) and not isinstance(
        column, native_pd.DatetimeIndex
    ):
        expected_join_count = 3

    if (
        key == "a"
        and isinstance(column, list)
        and column == [pd.Timedelta(5), pd.Timedelta(6), pd.Timedelta(7)]
    ):
        # failure because of SNOW-1738952. SNOW-1738952 only applies to this
        # case because we're replacing an existing column.
        with SqlCounter(query_count=0), pytest.raises(NotImplementedError):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                lambda df: func_insert_new_column(df, column),
                inplace=True,
            )
    else:
        # 2 extra queries, 1 for iter and 1 for tolist
        with SqlCounter(
            query_count=3
            if isinstance(column, native_pd.Index)
            and not isinstance(column, native_pd.DatetimeIndex)
            else 1,
            join_count=expected_join_count,
        ):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                lambda df: func_insert_new_column(df, column),
                inplace=True,
            )


@pytest.mark.parametrize("value", [[], [1, 2], np.array([4, 5, 6, 7])])
@pytest.mark.parametrize(
    "key", ["a", "x"]
)  # replace existing column, or add new column
def test_df_setitem_single_column_length_mismatch(key, value):
    data = {"a": [1, 2, 3], "b": [4, 5, 6]}
    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)

    def setitem_helper(df):
        df[key] = value

    if len(value) == 0:
        # both pandas and Snowpark pandas raise error when value is empty
        with SqlCounter(query_count=0):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                setitem_helper,
                inplace=True,
                expect_exception=True,
                expect_exception_type=ValueError,
                expect_exception_match="item to set is empty",
                assert_exception_equal=False,
            )

    else:
        # pandas raise error if the value length doesn't match with the column
        with pytest.raises(ValueError):
            setitem_helper(native_df.copy())
        # Snowpark instead will skip extra values or fill in the missing values using the last value
        with SqlCounter(query_count=1, join_count=2):
            setitem_helper(snow_df)
            if len(value) < len(native_df):
                value = value + [value[-1]] * (len(native_df) - len(value))
            else:
                value = value[: len(native_df)]
            setitem_helper(native_df)
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                snow_df, native_df
            )


@pytest.mark.parametrize(
    "index_values, other_index_values, expect_mismatch",
    [
        [["a", "b", "c", "d", "e"], ["a", "b", "c", "d", "e"], False],
        [["a", "b", "c", "d", "e"], ["x", "y", "z", "w", "u"], False],
        [["a", "b", "b", "d", "e"], ["a", "b", "c", "d", "e"], False],
        [["a", "b", "b", "d", "e"], ["a", "b", "b", "d", "e"], False],
        [["a", "b", "b", "d", "e"], ["b", "b", "a", "d", "e"], True],
        [["a", "b", "b", "d", "d"], ["a", "b", "c", "d", "e"], False],
        [["a", "b", "b", "d", "d"], ["a", "b", "c", "d", "e"], False],
        [["a", "b", "b", "d", "d"], ["a", "b", "b", "d", "d"], False],
        [["a", "b", "b", "d", "d"], ["b", "a", "d", "b", "d"], True],
        [["a", "b", "c", "d", "e"], ["a", "b", "b", "d", "e"], True],
        [["a", "b", "b", "d", "d"], ["a", "b", "b", "d", "e"], True],
        [["a", "b", "b", "d", "e"], ["x", "y", "z", "u", "u"], True],
    ],
)
# 2 extra queries to convert to native pandas when creating the two snowpark pandas dataframes
@sql_count_checker(query_count=3, join_count=1)
def test_df_setitem_with_unique_and_duplicate_index_values(
    index_values, other_index_values, expect_mismatch
):
    data = list(range(5))
    data1 = {"foo": data}
    data2 = {"bar": [val * 10 for val in data]}
    index = pd.Index(index_values, name="INDEX")
    other_index = pd.Index(other_index_values, name="INDEX")

    snow_df1 = pd.DataFrame(data1, index=index)
    snow_df2 = pd.DataFrame(data2, index=other_index)

    native_df1 = native_pd.DataFrame(
        data1, index=native_pd.Index(index_values, name="INDEX")
    )
    native_df2 = native_pd.DataFrame(
        data2, index=native_pd.Index(other_index_values, name="INDEX")
    )

    def setitem_op(df):
        df["foo2"] = (
            native_df2["bar"]
            if isinstance(df, native_pd.DataFrame)
            else snow_df2["bar"]
        )
        return df

    if not expect_mismatch:
        eval_snowpark_pandas_result(
            snow_df1,
            native_df1,
            lambda df: setitem_op(df),
        )
    else:
        # the Snowpark pandas behavior for setitem with non-unique index values is different
        # compare with native pandas. Native pandas raise an ValueError,
        # "cannot reindex on an axis with duplicate labels". In Snowpark pandas, we are not
        # able to perform such check and raises an error without trigger an eager evaluation,
        # and it exposes a left join behavior.
        snow_res = setitem_op(snow_df1)
        expected_res = native_df1.join(native_df2["bar"], how="left", sort=False)
        expected_res.columns = ["foo", "foo2"]
        assert_frame_equal(snow_res, expected_res, check_dtype=False)


OTHER_DF_2_MIXED_TYPES_COLUMNS = native_pd.DataFrame(
    {"d": [2.9, None, 4], "e": [4, 3, 4]}
)
OTHER_DF_3_COLUMNS = native_pd.DataFrame(
    {"d": [2, 3, 4], "e": [4, 3, 4], "z": ["a", "b", "c"]}
)


@pytest.mark.parametrize(
    "key,value",
    [
        (
            ["a", "a"],
            OTHER_DF_2_MIXED_TYPES_COLUMNS,
        ),  # for duplicates, the logic here is to lookup first key "a" and replace with corresponding series
        # from OTHER_DF_2_COLUMNS, then look up "a" again and replace again. I.e., [...] must have n columns
        # and each key of it addressed by the i-th column of the right side which must be a DataFrame
        # of n columns as well.
        (["a", "c"], OTHER_DF_2_MIXED_TYPES_COLUMNS),
        (
            [
                "b",
                "a",
            ],
            OTHER_DF_2_MIXED_TYPES_COLUMNS,
        ),
        (["a", "b", "a"], OTHER_DF_3_COLUMNS),
        (["b", "a", "c"], OTHER_DF_3_COLUMNS),
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_df_setitem_full_columns(key, value):
    data = {"a": [1, 2, 3], "b": [6, 5, 4], "c": [7, 8, 8]}
    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)

    def helper(df, key, value):
        # convert native data to Snowpark pandas
        if isinstance(value, native_pd.DataFrame) and isinstance(df, pd.DataFrame):
            value = pd.DataFrame(value)

        df[key] = value

    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: helper(df, key, value), inplace=True
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize(
    "data, comparison_value, set_value",
    [
        ({"a": [1, 2, 3], "b": [4, 5, 6]}, 2, 8),
        (
            {
                "a": native_pd.to_timedelta([1, 2, 3]),
                "b": native_pd.to_timedelta([4, 5, 6]),
            },
            pd.Timedelta(2),
            pd.Timedelta(8),
        ),
    ],
)
def test_df_setitem_lambda_dataframe(data, comparison_value, set_value):
    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)

    def masking_function(df):
        df[lambda x: x < comparison_value] = set_value

    eval_snowpark_pandas_result(snow_df, native_df, masking_function, inplace=True)


@sql_count_checker(query_count=1, join_count=0)
def test_setitem_lambda_series():
    data = {"a": 1, "b": 2, "c": 3}
    snow_ser = pd.Series(data)
    native_ser = native_pd.Series(data)

    def helper(ser):
        ser[lambda x: x < 2] = 8

    eval_snowpark_pandas_result(snow_ser, native_ser, helper, inplace=True)


@pytest.mark.parametrize("index", [True, False], ids=["with_index", "without_index"])
@pytest.mark.parametrize(
    "columns", [True, False], ids=["with_columns", "without_columns"]
)
def test_empty_df_setitem(index, columns):
    kwargs = {}
    if index:
        kwargs["index"] = [0, 1, 2]
    if columns:
        kwargs["columns"] = [0, 1, 2]
    native_df = native_pd.DataFrame(**kwargs)
    snow_df = pd.DataFrame(native_df)

    def set_col(df):
        df[0] = 1

    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            set_col,
            inplace=True,
            check_index_type=False,
        )

    # Check that `__setitem__` with a new column
    # when columns are specified results in correctly
    # adding a new column.
    if columns:
        native_df = native_pd.DataFrame(**kwargs)
        snow_df = pd.DataFrame(native_df)

        def set_col(df):
            df["newcol"] = 1

        with SqlCounter(query_count=1):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                set_col,
                inplace=True,
                check_index_type=False,
            )


def test_df_setitem_optimized():
    data = {"a": [1, 2, 3], "b": [6, 5, 4], "c": [7, 8, 8]}
    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)

    def helper(df):
        df["a"] = 10

    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(snow_df, native_df, helper, inplace=True)

    def helper(df):
        df["a"] = df["b"]

    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(snow_df, native_df, helper, inplace=True)

    def helper(df):
        df[df.a > 0] = df[df.b < 0]

    with SqlCounter(query_count=1, join_count=1):
        eval_snowpark_pandas_result(snow_df, native_df, helper, inplace=True)

    def helper(df):
        df["x"] = df.loc[df.b < 0, "b"]

    with SqlCounter(query_count=1, join_count=2):
        eval_snowpark_pandas_result(snow_df, native_df, helper, inplace=True)


@pytest.mark.parametrize(
    "df_key_shape_relative_to_self_shape",
    ["same", "more_cols", "less_cols", "more_rows", "less_rows"],
    ids=[
        "key_is_same_shape",
        "key_has_more_cols",
        "key_has_less_cols",
        "key_has_more_rows",
        "key_has_less_rows",
    ],
)
@pytest.mark.parametrize("is_df_key", [True, False], ids=["df_key", "array_key"])
class TestDFSetitemBool2DKey:
    @sql_count_checker(query_count=1, join_count=2)
    def test_df_setitem_bool_2d_key_array_value(
        self, df_key_shape_relative_to_self_shape, is_df_key
    ):
        data = {"a": [1, 2, 3], "b": [6, 5, 4], "c": [7, 8, 8]}
        snow_df = pd.DataFrame(data)
        native_df = native_pd.DataFrame(data)
        native_df_key = native_df % 2 == 0
        if "less" in df_key_shape_relative_to_self_shape:
            if df_key_shape_relative_to_self_shape == "less_rows":
                native_df_key = native_df_key.iloc[:-1, :]
            else:
                native_df_key = native_df_key.iloc[:, :-1]
        elif "more" in df_key_shape_relative_to_self_shape:
            if df_key_shape_relative_to_self_shape == "more_rows":
                native_df_key = native_pd.concat(
                    [
                        native_df_key,
                        native_pd.DataFrame(
                            [[False, True, False]],
                            index=native_pd.Index([3]),
                            columns=["a", "b", "c"],
                        ),
                    ]
                )
            else:
                native_df_key = native_pd.concat(
                    [
                        native_df_key,
                        native_pd.DataFrame(
                            [[False], [True], [False]],
                            columns=["d"],
                            index=native_df_key.index,
                        ),
                    ],
                    axis=1,
                )
        values = -1 * native_df.values
        if not is_df_key:
            native_df_key = native_df_key.values

        def setitem_helper(df):
            if isinstance(df, native_pd.DataFrame):
                key = native_df_key
                if df_key_shape_relative_to_self_shape != "same" and not is_df_key:
                    # pandas does not support array conditionals as
                    # setitem's key with a different shape than self
                    # but we do, thanks to lazy evaluation, so here,
                    # we must change the key that pandas gets.
                    key = native_df % 2 == 0
                    if "more" in df_key_shape_relative_to_self_shape:
                        if df_key_shape_relative_to_self_shape == "more_rows":
                            key = native_pd.concat(
                                [
                                    key,
                                    native_pd.DataFrame(
                                        [[False, True, False]],
                                        index=native_pd.Index([3]),
                                        columns=["a", "b", "c"],
                                    ),
                                ]
                            )
                        else:
                            key = native_pd.concat(
                                [
                                    key,
                                    native_pd.DataFrame(
                                        [[False], [True], [False]],
                                        columns=["d"],
                                        index=key.index,
                                    ),
                                ],
                                axis=1,
                            )
                    else:
                        if df_key_shape_relative_to_self_shape == "less_rows":
                            key = key.iloc[:-1, :]
                        else:
                            key = key.iloc[:, :-1]
                df[key] = values
            else:
                key = native_df_key
                if is_df_key:
                    key = pd.DataFrame(key)
                df[key] = values

        eval_snowpark_pandas_result(snow_df, native_df, setitem_helper, inplace=True)

    @sql_count_checker(query_count=1, join_count=1)
    def test_df_setitem_bool_2d_key_scalar_value(
        self, df_key_shape_relative_to_self_shape, is_df_key
    ):
        data = {"a": [1, 2, 3], "b": [6, 5, 4], "c": [7, 8, 8]}
        snow_df = pd.DataFrame(data)
        native_df = native_pd.DataFrame(data)
        native_df_key = native_df % 2 == 0
        if "less" in df_key_shape_relative_to_self_shape:
            if df_key_shape_relative_to_self_shape == "less_rows":
                native_df_key = native_df_key.iloc[:-1, :]
            else:
                native_df_key = native_df_key.iloc[:, :-1]
        elif "more" in df_key_shape_relative_to_self_shape:
            if df_key_shape_relative_to_self_shape == "more_rows":
                native_df_key = native_pd.concat(
                    [
                        native_df_key,
                        native_pd.DataFrame(
                            [[False, True, False]],
                            index=native_pd.Index([3]),
                            columns=["a", "b", "c"],
                        ),
                    ]
                )
            else:
                native_df_key = native_pd.concat(
                    [
                        native_df_key,
                        native_pd.DataFrame(
                            [[False], [True], [False]],
                            columns=["d"],
                            index=native_df_key.index,
                        ),
                    ],
                    axis=1,
                )
        value = -1
        if not is_df_key:
            native_df_key = native_df_key.values

        def setitem_helper(df):
            if isinstance(df, native_pd.DataFrame):
                key = native_df_key
                if df_key_shape_relative_to_self_shape != "same" and not is_df_key:
                    # pandas does not support array conditionals as
                    # setitem's key with a different shape than self
                    # but we do, thanks to lazy evaluation, so here,
                    # we must change the key that pandas gets.
                    key = native_df % 2 == 0
                    if "more" in df_key_shape_relative_to_self_shape:
                        if df_key_shape_relative_to_self_shape == "more_rows":
                            key = native_pd.concat(
                                [
                                    key,
                                    native_pd.DataFrame(
                                        [[False, True, False]],
                                        index=native_pd.Index([3]),
                                        columns=["a", "b", "c"],
                                    ),
                                ]
                            )
                        else:
                            key = native_pd.concat(
                                [
                                    key,
                                    native_pd.DataFrame(
                                        [[False], [True], [False]],
                                        columns=["d"],
                                        index=key.index,
                                    ),
                                ],
                                axis=1,
                            )
                    else:
                        if df_key_shape_relative_to_self_shape == "less_rows":
                            key = key.iloc[:-1, :]
                        else:
                            key = key.iloc[:, :-1]
                df[key] = value
            else:
                key = native_df_key
                if is_df_key:
                    key = pd.DataFrame(key)
                df[key] = value

        eval_snowpark_pandas_result(snow_df, native_df, setitem_helper, inplace=True)

    @sql_count_checker(query_count=1, join_count=2)
    def test_df_setitem_bool_2d_key_df_value_mismatched_column_labels(
        self, df_key_shape_relative_to_self_shape, is_df_key
    ):
        data = {"a": [1, 2, 3], "b": [6, 5, 4], "c": [7, 8, 8]}
        snow_df = pd.DataFrame(data)
        native_df = native_pd.DataFrame(data)
        native_df_key = native_df % 2 == 0
        if "less" in df_key_shape_relative_to_self_shape:
            if df_key_shape_relative_to_self_shape == "less_rows":
                native_df_key = native_df_key.iloc[:-1, :]
            else:
                native_df_key = native_df_key.iloc[:, :-1]
        elif "more" in df_key_shape_relative_to_self_shape:
            if df_key_shape_relative_to_self_shape == "more_rows":
                native_df_key = native_pd.concat(
                    [
                        native_df_key,
                        native_pd.DataFrame(
                            [[False, True, False]],
                            index=native_pd.Index([3]),
                            columns=["a", "b", "c"],
                        ),
                    ]
                )
            else:
                native_df_key = native_pd.concat(
                    [
                        native_df_key,
                        native_pd.DataFrame(
                            [[False], [True], [False]],
                            columns=["d"],
                            index=native_df_key.index,
                        ),
                    ],
                    axis=1,
                )
        if not is_df_key:
            native_df_key = native_df_key.values
        values = native_pd.DataFrame(
            -1 * native_df.values, columns=native_pd.Index(["x", "y", "z"])
        )

        def setitem_helper(df):
            if isinstance(df, native_pd.DataFrame):
                key = native_df_key
                if df_key_shape_relative_to_self_shape != "same" and not is_df_key:
                    # pandas does not support array conditionals as
                    # setitem's key with a different shape than self
                    # but we do, thanks to lazy evaluation, so here,
                    # we must change the key that pandas gets.
                    key = native_df % 2 == 0
                    if "more" in df_key_shape_relative_to_self_shape:
                        if df_key_shape_relative_to_self_shape == "more_rows":
                            key = native_pd.concat(
                                [
                                    key,
                                    native_pd.DataFrame(
                                        [[False, True, False]],
                                        index=native_pd.Index([3]),
                                        columns=["a", "b", "c"],
                                    ),
                                ]
                            )
                        else:
                            key = native_pd.concat(
                                [
                                    key,
                                    native_pd.DataFrame(
                                        [[False], [True], [False]],
                                        columns=["d"],
                                        index=key.index,
                                    ),
                                ],
                                axis=1,
                            )
                    else:
                        if df_key_shape_relative_to_self_shape == "less_rows":
                            key = key.iloc[:-1, :]
                        else:
                            key = key.iloc[:, :-1]
                df[key] = values
            else:
                key = native_df_key
                if is_df_key:
                    key = pd.DataFrame(key)
                df[key] = pd.DataFrame(values)

        eval_snowpark_pandas_result(snow_df, native_df, setitem_helper, inplace=True)

    @sql_count_checker(query_count=1, join_count=2)
    def test_df_setitem_bool_2d_key_df_value_mismatched_index_labels(
        self, df_key_shape_relative_to_self_shape, is_df_key
    ):
        data = {"a": [1, 2, 3], "b": [6, 5, 4], "c": [7, 8, 8]}
        snow_df = pd.DataFrame(data)
        native_df = native_pd.DataFrame(data)
        native_df_key = native_df % 2 == 0
        if "less" in df_key_shape_relative_to_self_shape:
            if df_key_shape_relative_to_self_shape == "less_rows":
                native_df_key = native_df_key.iloc[:-1, :]
            else:
                native_df_key = native_df_key.iloc[:, :-1]
        elif "more" in df_key_shape_relative_to_self_shape:
            if df_key_shape_relative_to_self_shape == "more_rows":
                native_df_key = native_pd.concat(
                    [
                        native_df_key,
                        native_pd.DataFrame(
                            [[False, True, False]],
                            index=native_pd.Index([3]),
                            columns=["a", "b", "c"],
                        ),
                    ]
                )
            else:
                native_df_key = native_pd.concat(
                    [
                        native_df_key,
                        native_pd.DataFrame(
                            [[False], [True], [False]],
                            columns=["d"],
                            index=native_df_key.index,
                        ),
                    ],
                    axis=1,
                )
        if not is_df_key:
            native_df_key = native_df_key.values
        values = native_pd.DataFrame(
            -1 * native_df.values, index=native_pd.Index([100, 101, 102])
        )

        def setitem_helper(df):
            if isinstance(df, native_pd.DataFrame):
                key = native_df_key
                if df_key_shape_relative_to_self_shape != "same" and not is_df_key:
                    # pandas does not support array conditionals as
                    # setitem's key with a different shape than self
                    # but we do, thanks to lazy evaluation, so here,
                    # we must change the key that pandas gets.
                    key = native_df % 2 == 0
                    if "more" in df_key_shape_relative_to_self_shape:
                        if df_key_shape_relative_to_self_shape == "more_rows":
                            key = native_pd.concat(
                                [
                                    key,
                                    native_pd.DataFrame(
                                        [[False, True, False]],
                                        index=native_pd.Index([3]),
                                        columns=["a", "b", "c"],
                                    ),
                                ]
                            )
                        else:
                            key = native_pd.concat(
                                [
                                    key,
                                    native_pd.DataFrame(
                                        [[False], [True], [False]],
                                        columns=["d"],
                                        index=key.index,
                                    ),
                                ],
                                axis=1,
                            )
                    else:
                        if df_key_shape_relative_to_self_shape == "less_rows":
                            key = key.iloc[:-1, :]
                        else:
                            key = key.iloc[:, :-1]
                df[key] = values
            else:
                key = native_df_key
                if is_df_key:
                    key = pd.DataFrame(key)
                df[key] = pd.DataFrame(values)

        eval_snowpark_pandas_result(snow_df, native_df, setitem_helper, inplace=True)

    @sql_count_checker(query_count=1, join_count=2)
    def test_df_setitem_bool_2d_key_df_value_matched_labels(
        self, df_key_shape_relative_to_self_shape, is_df_key
    ):
        data = {"a": [1, 2, 3], "b": [6, 5, 4], "c": [7, 8, 8]}
        snow_df = pd.DataFrame(data)
        native_df = native_pd.DataFrame(data)
        native_df_key = native_df % 2 == 0
        if "less" in df_key_shape_relative_to_self_shape:
            if df_key_shape_relative_to_self_shape == "less_rows":
                native_df_key = native_df_key.iloc[:-1, :]
            else:
                native_df_key = native_df_key.iloc[:, :-1]
        elif "more" in df_key_shape_relative_to_self_shape:
            if df_key_shape_relative_to_self_shape == "more_rows":
                native_df_key = native_pd.concat(
                    [
                        native_df_key,
                        native_pd.DataFrame(
                            [[False, True, False]],
                            index=native_pd.Index([3]),
                            columns=["a", "b", "c"],
                        ),
                    ]
                )
            else:
                native_df_key = native_pd.concat(
                    [
                        native_df_key,
                        native_pd.DataFrame(
                            [[False], [True], [False]],
                            columns=["d"],
                            index=native_df_key.index,
                        ),
                    ],
                    axis=1,
                )
        if not is_df_key:
            native_df_key = native_df_key.values
        values = -1 * native_df

        def setitem_helper(df):
            if isinstance(df, native_pd.DataFrame):
                key = native_df_key
                if df_key_shape_relative_to_self_shape != "same" and not is_df_key:
                    # pandas does not support array conditionals as
                    # setitem's key with a different shape than self
                    # but we do, thanks to lazy evaluation, so here,
                    # we must change the key that pandas gets.
                    key = native_df % 2 == 0
                    if "more" in df_key_shape_relative_to_self_shape:
                        if df_key_shape_relative_to_self_shape == "more_rows":
                            key = native_pd.concat(
                                [
                                    key,
                                    native_pd.DataFrame(
                                        [[False, True, False]],
                                        index=native_pd.Index([3]),
                                        columns=["a", "b", "c"],
                                    ),
                                ]
                            )
                        else:
                            key = native_pd.concat(
                                [
                                    key,
                                    native_pd.DataFrame(
                                        [[False], [True], [False]],
                                        columns=["d"],
                                        index=key.index,
                                    ),
                                ],
                                axis=1,
                            )
                    else:
                        if df_key_shape_relative_to_self_shape == "less_rows":
                            key = key.iloc[:-1, :]
                        else:
                            key = key.iloc[:, :-1]
                df[key] = native_pd.DataFrame(values)
            else:
                key = native_df_key
                if is_df_key:
                    key = pd.DataFrame(key)
                df[key] = pd.DataFrame(values)

        eval_snowpark_pandas_result(snow_df, native_df, setitem_helper, inplace=True)


@sql_count_checker(query_count=1, join_count=0)
def test_df_setitem_2d_bool_key_with_callable_value():
    data = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(native_df)

    class CallableValue:
        def __call__(self, df):
            return df**2

    def setitem_helper(df):
        if isinstance(df, pd.DataFrame):
            df[df % 2 == 0] = CallableValue()
        else:
            df[df % 2 == 0] = CallableValue()(df)

    eval_snowpark_pandas_result(snow_df, native_df, setitem_helper, inplace=True)


@sql_count_checker(query_count=1)
def test_df_setitem_2d_int_array_key_should_error():
    data = {"a": [1, 2, 3], "b": [6, 5, 4], "c": [7, 8, 8]}
    snow_df = pd.DataFrame(data)

    with pytest.raises(
        TypeError, match="Must pass DataFrame or 2-d ndarray with boolean values only"
    ):
        snow_df[snow_df.values] = 3


@sql_count_checker(query_count=1, join_count=1)
def test_df_setitem_2d_bool_key_short_array_value():
    data = {"a": [1, 2, 3], "b": [6, 5, 4], "c": [7, 8, 8]}
    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)

    # pandas does not support setitem when other is a NumPy array that does not match
    # self's shape.
    with pytest.raises(
        ValueError, match="other must be the same shape as self when an ndarray"
    ):
        native_df[native_df % 2 == 0] = -1 * native_df.iloc[:-1, :].values

    snow_df[snow_df % 2 == 0] = -1 * native_df.iloc[:-1, :].values
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        snow_df,
        native_pd.DataFrame(
            {"a": [1, -2, 3], "b": [-6, 5, np.nan], "c": [7, -8, np.nan]}
        ),
    )


@sql_count_checker(query_count=1, join_count=1)
def test_df_setitem_2d_bool_key_1d_array_value():
    data = {"a": [1, 2, 3], "b": [6, 5, 4], "c": [7, 8, 8]}
    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)

    # pandas does not support setitem when other is a NumPy array that does not match
    # self's shape.
    with pytest.raises(
        ValueError, match="other must be the same shape as self when an ndarray"
    ):
        native_df[native_df % 2 == 0] = np.array([1, 2, 3])

    snow_df[snow_df % 2 == 0] = np.array([1, 2, 3])
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        snow_df,
        native_pd.DataFrame(
            {"a": [1, 2, 3], "b": [np.nan, 5, np.nan], "c": [7, np.nan, np.nan]}
        ),
    )


@pytest.mark.parametrize("key", SERIES_AND_LIST_LIKE_KEY_AND_ITEM_VALUES)
@pytest.mark.parametrize("item", [range(7)])
@sql_count_checker(query_count=0)
def test_df_setitem_series_list_like_key_and_range_like_item_negative(
    key, item, default_index_snowpark_pandas_df
):
    # df[series/list-like key] = range-like item
    # ------------------------------------------
    # Ranges are treated like lists. This case is not implemented yet.
    # Example:
    # >>> df = pd.DataFrame({"A": list(range(4)), "B": list(range(4, 8)), "C": list(range(8, 12))})
    # >>> df
    #    A  B   C
    # 0  0  4   8
    # 1  1  5   9
    # 2  2  6  10
    # 3  3  7  11
    # >>> df[[1, 2, 3, 4]] = range(4)
    # >>> df
    #    A  B   C  1  2  3  4
    # 0  0  4   8  0  1  2  3
    # 1  1  5   9  0  1  2  3
    # 2  2  6  10  0  1  2  3
    # 3  3  7  11  0  1  2  3
    #
    # df[slice(None), [1, 2, 3, 4]] = range(4) raises a TypeError in native pandas: "unhashable type: 'slice'"

    snowpark_df = default_index_snowpark_pandas_df
    key = pd.Series(key) if isinstance(key, native_pd.Series) else key
    err_msg = "Currently do not support Series or list-like keys with range-like values"
    with pytest.raises(NotImplementedError, match=err_msg):
        snowpark_df[key] = item

    with pytest.raises(NotImplementedError, match=err_msg):
        snowpark_df[slice(None), key] = item


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("key", [1, 12, [1, 2, 3], native_pd.Series([0, 4, 5])])
def test_df_setitem_slice_item_negative(key, default_index_snowpark_pandas_df):
    # df[array-like/scalar key] = slice item
    # --------------------------------------
    # Here, slice is treated like a scalar object and assigned as itself to given key(s). This behavior is currently
    # not supported in Snowpark pandas.
    #
    # Example:
    # >>> df = pd.DataFrame({"A": list(range(4)), "B": list(range(4, 8)), "C": list(range(8, 12))})
    # >>> df
    #     #    A  B   C
    #     # 0  0  4   8
    #     # 1  1  5   9
    #     # 2  2  6  10
    #     # 3  3  7  11
    # >>> df[1] = slice(20, 30, 40)
    # >>> df
    #    A  B   C                  1
    # 0  0  4   8  slice(20, 30, 40)
    # 1  1  5   9  slice(20, 30, 40)
    # 2  2  6  10  slice(20, 30, 40)
    # 3  3  7  11  slice(20, 30, 40)

    snowpark_df = default_index_snowpark_pandas_df
    item = slice(20, 30, 40)
    err_msg = (
        "Currently do not support assigning a slice value as if it's a scalar value"
    )
    with pytest.raises(NotImplementedError, match=err_msg):
        snowpark_df[pd.Series(key) if isinstance(key, native_pd.Series) else key] = item


@pytest.mark.parametrize(
    "indexer",
    [
        ["A", "B"],
        ["B", "A"],
        slice(None),
        native_pd.Series(["A", "B"]),
        native_pd.Series(["B", "A"], index=["A", "B"]),
        slice(1, 3),
    ],
)
@pytest.mark.parametrize("item_type", ["numpy_array", "native_list"])
def test_df_setitem_2d_array(indexer, item_type):
    from math import prod

    expected_query_count = 1
    if isinstance(indexer, native_pd.Series):
        expected_query_count += 1

    expected_join_count = (
        3 if isinstance(indexer, slice) and indexer.start is not None else 1
    )

    native_df = native_pd.DataFrame(
        [[91, -2, 83, 74], [95, -6, 87, 78], [99, -10, 811, 712], [913, -14, 815, 716]],
        index=["x", "x", "z", "w"],
        columns=["A", "B", "C", "D"],
    )

    snow_df = pd.DataFrame(native_df)

    # Rather than re-shaping a NumPy array to ensure we get the correct types, we can just
    # use pandas' setitem behavior to get the right shape.
    item = np.arange(prod(native_df[indexer].shape)).reshape(native_df[indexer].shape)

    if item_type == "native_list":
        item = [list(i) for i in item]

    def setitem_helper(df):
        if isinstance(df, native_pd.DataFrame):
            df[indexer] = item
        else:
            if isinstance(indexer, native_pd.Series):
                df[pd.Series(indexer)] = item
            else:
                df[indexer] = item

    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            setitem_helper,
            inplace=True,
        )


@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="SNOW-1738952")
def test_df_setitem_2d_array_timedelta_negative():
    def setitem(df):
        df[[1]] = np.array([[pd.Timedelta(3)]])

    eval_snowpark_pandas_result(
        *create_test_dfs(native_pd.DataFrame([[pd.Timedelta(1), pd.Timedelta(2)]])),
        setitem,
        inplace=True
    )


def test_df_setitem_2d_array_row_length_no_match():
    native_df = native_pd.DataFrame(
        [[91, -2, 83, 74], [95, -6, 87, 78], [99, -10, 811, 712], [913, -14, 815, 716]],
        index=["x", "y", "z", "w"],
        columns=["A", "B", "C", "D"],
    )

    snow_df = pd.DataFrame(native_df)
    val = np.array(
        [
            [1, 2, 3, 4],
            [5, 6, 7, 8],
            [9, 10, 11, 12],
            [13, 14, 15, 16],
            [17, 18, 19, 20],
        ]
    )

    # When there are too many rows
    with pytest.raises(
        ValueError,
        match=r"setting an array element with a sequence.",
    ):
        native_df[:] = val

    def setitem_helper(df):
        if isinstance(df, native_pd.DataFrame):
            df[:] = val[:-1]
        else:
            df[:] = val

    with SqlCounter(query_count=1, join_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            setitem_helper,
            inplace=True,
        )

    # When there is exactly one row (pandas will broadcast, we ffill).
    native_df = native_pd.DataFrame(
        [[91, -2, 83, 74], [95, -6, 87, 78], [99, -10, 811, 712], [913, -14, 815, 716]],
        index=["x", "y", "z", "w"],
        columns=["A", "B", "C", "D"],
    )

    snow_df = pd.DataFrame(native_df)

    def setitem_helper(df):
        df[:] = val[:1]

    with SqlCounter(query_count=1, join_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            setitem_helper,
            inplace=True,
        )

    native_df = native_pd.DataFrame(
        [[91, -2, 83, 74], [95, -6, 87, 78], [99, -10, 811, 712], [913, -14, 815, 716]],
        index=["x", "y", "z", "w"],
        columns=["A", "B", "C", "D"],
    )

    snow_df = pd.DataFrame(native_df)

    def setitem_helper(df):
        if isinstance(df, native_pd.DataFrame):
            df[:] = [
                list(val[0]),
                list(val[1]),
                list(val[1]),
                list(val[1]),
            ]
        else:
            snow_df[:] = val[:2]

    with SqlCounter(query_count=1, join_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            setitem_helper,
            inplace=True,
        )


def test_df_setitem_2d_array_col_length_no_match():
    # pandas error message:
    # ValueError: could not broadcast input array from shape <VALUE_SHAPE> into shape <INDEXED_SELF_SHAPE>
    # Snowpark pandas error message:
    # ValueError: shape mismatch: the number of columns <NUM_VALUE_COLS> from the item does not
    # match with the number of columns <NUM_INDEXED_SELF_COLS> to set
    native_df = native_pd.DataFrame(
        [[91, -2, 83, 74], [95, -6, 87, 78], [99, -10, 811, 712], [913, -14, 815, 716]],
        index=["x", "y", "z", "w"],
        columns=["A", "B", "C", "D"],
    )

    snow_df = pd.DataFrame(native_df)
    val = np.array([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12], [13, 14, 15, 16]])

    # When there are too few cols
    def setitem_helper(df):
        df[:] = val[:, :-1]

    with SqlCounter(query_count=0):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            setitem_helper,
            inplace=True,
            expect_exception=True,
            expect_exception_type=ValueError,
            assert_exception_equal=False,  # Our error message is slightly different from pandas.
            expect_exception_match="shape mismatch: the number of columns 3 from the item does not match with the number of columns 4 to set",
        )

    # When there are too many cols
    native_df = native_pd.DataFrame(
        [[91, -2, 83, 74], [95, -6, 87, 78], [99, -10, 811, 712], [913, -14, 815, 716]],
        index=["x", "y", "z", "w"],
        columns=["A", "B", "C", "D"],
    )

    snow_df = pd.DataFrame(native_df)

    def setitem_helper(df):
        df[:] = np.hstack((val, val[:, :1]))

    with SqlCounter(query_count=0):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            setitem_helper,
            inplace=True,
            assert_exception_equal=False,  # Our error message is slightly different from pandas.
            expect_exception=True,
            expect_exception_type=ValueError,
            expect_exception_match="shape mismatch: the number of columns 5 from the item does not match with the number of columns 4 to set",  # Our error message is slightly different from pandas.
        )


@sql_count_checker(query_count=1, join_count=1)
def test_df_setitem_2d_array_with_explicit_na_values():
    native_df = native_pd.DataFrame(
        [[91, -2, 83, 74], [95, -6, 87, 78], [99, -10, 811, 712], [913, -14, 815, 716]],
        index=["x", "y", "z", "w"],
        columns=["A", "B", "C", "D"],
    )

    snow_df = pd.DataFrame(native_df)
    val = np.array(
        [
            [1, np.nan, 3, 4],
            [5, np.nan, 7, 8],
            [9, None, np.nan, 12],
            [np.nan, None, np.nan, 15],
        ]
    )

    def setitem_helper(df):
        df[:] = val

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        setitem_helper,
        inplace=True,
    )


@sql_count_checker(query_count=1, join_count=1)
def test_df_setitem_2d_array_with_ffill_na_values_negative():
    # Ideally, we want NA values to be propagated if they are the last
    # value present, but we currently do not support this.
    native_df = native_pd.DataFrame(
        [[91, -2, 83, 74], [95, -6, 87, 78], [99, -10, 811, 712], [913, -14, 815, 716]],
        index=["x", "y", "z", "w"],
        columns=["A", "B", "C", "D"],
    )

    snow_df = pd.DataFrame(native_df)
    val = np.array([[1, 2, 3, 4], [5, np.nan, 7, 8]])
    ffilled_vals = np.array(
        [[1, 2, 3, 4], [5, np.nan, 7, 8], [5, 2, 7, 8], [5, 2, 7, 8]]
    )

    def setitem_helper(df):
        if isinstance(df, native_pd.DataFrame):
            # This is what it would be if our ffilling would correctly propagate NA values.
            # df[:] = [list(val[0]), list(val[1]), list(val[1]), list(val[1])]
            # Instead, our ffill value picks the most recent *non-NA* value
            df[:] = ffilled_vals
        else:
            df[:] = val

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        setitem_helper,
        inplace=True,
    )


# TODO: SNOW-994624.  There are a mix of cases that differ from pandas in different ways, see jira/doc for more info.
# 1) Sometimes sets the wrong row rather than either expected values (ex, key=True)
# 2) Sometimes does not set any row rather than either expected values (ex, key=False)
# 3) Sometimes sets both expected index values (ex, key=0 or key=1)
@pytest.mark.xfail(reason="SNOW-994624 setitem boolean key is inconsistent")
@pytest.mark.parametrize("key", [True, False, 0, 1])
@pytest.mark.parametrize(
    "columns",
    [
        [0, 1, True, False, "x"],
        [0, 1, True, "x"],
        [1, True, False, "x"],
        [1, True, "x"],
        [0, 1, False, "x"],
        [0, False, "x"],
        [2, "x"],
    ],
)
@sql_count_checker(query_count=1, join_count=0)
def test_df_setitem_boolean_key(key, columns):
    item = 99
    index_len = 3

    data = [[i for i in range(len(columns))] for k in range(index_len)]

    native_df = native_pd.DataFrame(data, columns=columns)
    snow_df = pd.DataFrame(native_df)

    native_df[key] = item
    snow_df[key] = item

    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_df, native_df)


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("key_type", [np.array, "dataframe"])
def test_df_setitem_2D_key_series_value(key_type):
    def make_key(key, key_type, data_type):
        if key_type == np.array:
            return key_type(key)
        else:
            return data_type(key)

    native_df = native_pd.DataFrame([[1, 2, 3], [4, 5, 6]])
    with pytest.raises(ValueError, match="Must specify axis=0 or 1"):
        native_df[
            make_key([[True, True, True]] * 2, key_type, native_pd.DataFrame)
        ] = native_df[0]

    snow_df = pd.DataFrame(native_df)
    with pytest.raises(
        ValueError, match="setitem with a 2D key does not support Series values."
    ):
        snow_df[make_key([[True, True, True]] * 2, key_type, pd.DataFrame)] = snow_df[0]


@sql_count_checker(query_count=2, join_count=1)
def test_setitem_type_mismatch_SNOW_2157603():

    # First possible failure, set item with a date range on top of an int
    native_df = native_pd.DataFrame([[1, 2], [3, 4]], columns=["A", "B"])
    expected_df = native_df
    expected_df["A"] = native_pd.to_datetime(expected_df["A"])

    snow_df = pd.DataFrame(native_df)
    snow_df["A"] = pd.to_datetime(snow_df["A"])
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_df, expected_df)

    # Second possible failure, set item with a date range using an index
    native_index = native_pd.date_range(
        native_pd.to_datetime(0), native_pd.to_datetime(9999999999999), freq="5min"
    )
    native_df = native_pd.DataFrame(native_index)
    native_df[0] = native_pd.DataFrame(native_df[0])

    snow_index = pd.date_range(
        pd.to_datetime(0), pd.to_datetime(9999999999999), freq="5min"
    )
    snow_df = pd.DataFrame(snow_index)
    snow_df[0] = pd.DataFrame(native_df[0])
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_df, native_df)
