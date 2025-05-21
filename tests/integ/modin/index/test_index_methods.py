#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from numpy.testing import assert_equal
from pandas._libs import lib

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.index.conftest import (
    NATIVE_INDEX_SCALAR_TEST_DATA,
    NATIVE_INDEX_TEST_DATA,
    NATIVE_INDEX_UNIQUE_TEST_DATA,
    TEST_DFS,
)
from tests.integ.modin.utils import (
    assert_frame_equal,
    assert_index_equal,
    assert_series_equal,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@sql_count_checker(query_count=2)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_copy(native_index):
    snow_index = pd.Index(native_index)
    new_index = snow_index.copy()
    assert snow_index is not new_index
    assert_index_equal(snow_index, new_index)


@sql_count_checker(query_count=2)
def test_index_creation_from_lazy_index():
    i1 = pd.Index([1, 2, 3])
    i2 = pd.Index(i1)
    assert_index_equal(i1, i2)


@pytest.mark.parametrize("native_df", TEST_DFS)
@sql_count_checker(query_count=2)
def test_df_index_copy(native_df):
    snow_df = pd.DataFrame(native_df)
    new_index = snow_df.index.copy()
    new_columns = snow_df.columns.copy()

    assert snow_df.index is not new_index
    assert snow_df.columns is not new_columns

    assert_index_equal(snow_df.index, new_index)
    assert_index_equal(snow_df.columns, new_columns)


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA[2:])
def test_index_drop(native_index):
    snow_index = pd.Index(native_index)
    labels = [native_index[0]]
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Index.drop",
    ):
        assert_index_equal(snow_index.drop(labels), native_index.drop(labels))


@sql_count_checker(query_count=3, join_count=1)
@pytest.mark.parametrize("native_df", TEST_DFS)
def test_df_index_equals(native_df):
    snow_df = pd.DataFrame(native_df)
    assert native_df.columns.equals(native_df.columns)
    assert snow_df.columns.equals(snow_df.columns)
    assert native_df.columns.equals(snow_df.columns)
    assert snow_df.columns.equals(native_df.columns)

    assert native_df.index.equals(native_df.index)
    assert snow_df.index.equals(snow_df.index)
    assert native_df.index.equals(snow_df.index.to_pandas())
    assert snow_df.index.equals(native_df.index)


@sql_count_checker(query_count=0)
def test_index_union():
    idx1 = pd.Index([1, 2, 3, 4])
    idx2 = pd.Index([3, 4, 5, 6])
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Index.union",
    ):
        idx1.union(idx2)
    idx1 = pd.Index(["a", "b", "c", "d"])
    idx2 = pd.Index([1, 2, 3, 4])
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Index.union",
    ):
        idx1.union(idx2)


@sql_count_checker(query_count=0)
def test_index_difference():
    idx1 = pd.Index([2, 1, 3, 4])
    idx2 = pd.Index([3, 4, 5, 6])
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Index.difference",
    ):
        idx1.difference(idx2)


@sql_count_checker(query_count=4)
def test_index_intersection():
    idx1 = pd.Index([2, 1, 3, 4])
    idx2 = pd.Index([3, 4, 5, 6])
    diff = idx1.intersection(idx2)
    assert_index_equal(diff, pd.Index([3, 4], dtype="int64"))


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_get_level_values(native_index):
    snow_index = pd.Index(native_index)
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Index.get_level_values",
    ):
        assert_index_equal(snow_index.get_level_values(0), snow_index)


@sql_count_checker(query_count=0)
def test_slice_indexer():
    idx = pd.Index(list("abcd"))
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Index.slice_indexer",
    ):
        s = idx.slice_indexer(start="a", end="c")
        assert s != slice(1, 3, None)
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Index.slice_indexer",
    ):
        s = idx.slice_indexer(start="b", end="c")
        assert s == slice(1, 3, None)


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_summary(native_index):
    snow_index = pd.Index(native_index)
    assert snow_index._summary() == native_index._summary()


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_size(native_index):
    snow_index = pd.Index(native_index)
    assert snow_index.size == native_index.size


@pytest.mark.parametrize("native_df", TEST_DFS)
@sql_count_checker(query_count=0)
def test_df_index_size(native_df):
    snow_df = pd.DataFrame(native_df)
    assert snow_df.index.size == native_df.index.size
    assert snow_df.columns.size == native_df.columns.size


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_empty(native_index):
    snow_index = pd.Index(native_index)
    assert snow_index.empty == native_index.empty


@pytest.mark.parametrize("native_df", TEST_DFS)
@sql_count_checker(query_count=0)
def test_df_index_empty(native_df):
    snow_df = pd.DataFrame(native_df)
    assert snow_df.index.empty == native_df.index.empty
    assert snow_df.columns.empty == native_df.columns.empty


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_shape(native_index):
    snow_index = pd.Index(native_index)
    assert snow_index.shape == native_index.shape


@pytest.mark.parametrize("native_df", TEST_DFS)
@sql_count_checker(query_count=0)
def test_df_index_shape(native_df):
    snow_df = pd.DataFrame(native_df)
    assert snow_df.index.shape == native_df.index.shape
    assert snow_df.columns.shape == native_df.columns.shape


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_values(native_index):
    snow_index = pd.Index(native_index)
    assert_equal(snow_index.values, native_index.values)


@pytest.mark.parametrize("native_df", TEST_DFS)
@sql_count_checker(query_count=1)
def test_df_index_columns_values(native_df):
    snow_df = pd.DataFrame(native_df)
    assert_equal(snow_df.index.values, native_df.index.values)
    assert_equal(snow_df.columns.values, native_df.columns.values)


@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
@sql_count_checker(query_count=1)
def test_index_item(native_index):
    snow_index = pd.Index(native_index)
    if len(native_index) == 1:
        assert snow_index.item() == native_index.item()
    else:
        with pytest.raises(
            expected_exception=ValueError,
            match="can only convert an array of size 1 to a Python scalar",
        ):
            snow_index.item()


@pytest.mark.parametrize("native_df", TEST_DFS)
@sql_count_checker(query_count=1)
def test_df_index_columns_item(native_df):
    snow_df = pd.DataFrame(native_df)
    if len(native_df.index) == 1:
        assert snow_df.index.item() == native_df.index.item()
    else:
        with pytest.raises(
            expected_exception=ValueError,
            match="can only convert an array of size 1 to a Python scalar",
        ):
            snow_df.index.item()

    # no queries needed for columns
    if len(native_df.columns) == 1:
        assert snow_df.columns.item() == native_df.columns.item()
    else:
        with pytest.raises(
            expected_exception=ValueError,
            match="can only convert an array of size 1 to a Python scalar",
        ):
            snow_df.columns.item()


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_to_list(native_index):
    snow_index = pd.Index(native_index)
    assert_equal(native_index.to_list(), snow_index.to_list())


@pytest.mark.parametrize("native_df", TEST_DFS)
@sql_count_checker(query_count=1)
def test_df_index_columns_to_list(native_df):
    snow_df = pd.DataFrame(native_df)
    assert_equal(native_df.index.to_list(), snow_df.index.to_list())
    assert_equal(native_df.columns.to_list(), snow_df.columns.to_list())


@pytest.mark.parametrize("name", [None, "name", True, 1, lib.no_default])
@pytest.mark.parametrize("generate_extra_index", [True, False])
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_to_series(native_index, generate_extra_index, name):
    if generate_extra_index:
        index = range(len(native_index))
    else:
        index = None
    snow_index = pd.Index(native_index)
    with SqlCounter(query_count=1, join_count=1 if generate_extra_index else 0):
        assert_series_equal(
            snow_index.to_series(index=index, name=name),
            native_index.to_series(index=index, name=name),
            check_index_type=False,
        )


@pytest.mark.parametrize("name", [None, "name", True, 1, lib.no_default])
@pytest.mark.parametrize("generate_extra_index", [True, False])
@pytest.mark.parametrize("native_df", TEST_DFS)
def test_df_index_columns_to_series(native_df, generate_extra_index, name):
    snow_df = pd.DataFrame(native_df)

    # Snowpark pandas sets the dtype of df.columns for an empty df as object and native pandas sets it as int,
    # so we avoid this check on an empty df
    if native_df.empty:
        check_dtype = False
    else:
        check_dtype = True

    if generate_extra_index:
        row_index = range(len(native_df))
        col_index = range(len(native_df.columns))
    else:
        row_index = None
        col_index = None

    with SqlCounter(query_count=1, join_count=1 if generate_extra_index else 0):
        assert_series_equal(
            snow_df.index.to_series(index=row_index, name=name),
            native_df.index.to_series(index=row_index, name=name),
            check_index_type=False,
        )
        assert_series_equal(
            snow_df.columns.to_series(index=col_index),
            native_df.columns.to_series(index=col_index),
            check_dtype=check_dtype,
            check_index_type=False,
        )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("name", [None, "name", True, 1, lib.no_default])
@pytest.mark.parametrize("index", [True, False])
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_to_frame(native_index, name, index):
    snow_index = pd.Index(native_index)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        snow_index.to_frame(index=index, name=name),
        native_index.to_frame(index=index, name=name),
        check_index_type=False,
        check_column_type=False,
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("name", [None, "name", True, 1, lib.no_default])
@pytest.mark.parametrize("index", [True, False])
@pytest.mark.parametrize("native_df", TEST_DFS)
def test_df_index_to_frame(native_df, index, name):
    snow_df = pd.DataFrame(native_df)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        snow_df.index.to_frame(index=index, name=name),
        native_df.index.to_frame(index=index, name=name),
        check_index_type=False,
        check_column_type=False,
    )


@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_dtype(native_index):
    with SqlCounter(
        query_count=1 if getattr(native_index.dtype, "tz", None) is not None else 0
    ):
        snow_index = pd.Index(native_index)
        assert snow_index.dtype == native_index.dtype


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("native_df", TEST_DFS)
def test_df_index_columns_dtype(native_df):
    snow_df = pd.DataFrame(native_df)
    assert snow_df.index.dtype == native_df.index.dtype
    # do not check columns type for empty df
    if not native_df.empty:
        assert snow_df.columns.dtype == native_df.columns.dtype


@pytest.mark.parametrize("index", NATIVE_INDEX_UNIQUE_TEST_DATA)
def test_is_unique(index):
    with SqlCounter(query_count=1):
        snow_index = pd.Index(index)
        assert index.is_unique == snow_index.is_unique


@pytest.mark.parametrize("index", NATIVE_INDEX_UNIQUE_TEST_DATA)
def test_has_duplicates(index):
    with SqlCounter(query_count=1):
        snow_index = pd.Index(index)
        assert index.has_duplicates == snow_index.has_duplicates


@sql_count_checker(query_count=6)
def test_index_parent():
    """
    Check whether the parent field in Index is updated properly.
    """
    native_idx1 = native_pd.Index(["A", "B"], name="xyz")
    native_idx2 = native_pd.Index(["A", "B", "D", "E", "G", "H"], name="CFI")

    # DataFrame case.
    df = pd.DataFrame([[1, 2], [3, 4]], index=native_idx1)
    snow_idx = df.index
    assert_frame_equal(snow_idx._parent._parent, df)
    assert_index_equal(snow_idx, native_idx1)

    # Series case.
    s = pd.Series([1, 2, 4, 5, 6, 7], index=native_idx2, name="zyx")
    snow_idx = s.index
    assert_series_equal(snow_idx._parent._parent, s)
    assert_index_equal(snow_idx, native_idx2)


@sql_count_checker(query_count=0)
@pytest.mark.parametrize(
    "kwargs",
    [{"dtype": "str"}, {"copy": True}, {"name": "abc"}, {"tupleize_cols": False}],
)
def test_non_default_args(kwargs):
    idx = pd.Index([1, 2, 3, 4], name="name", dtype="int64")

    name = list(kwargs.keys())[0]
    value = list(kwargs.values())[0]
    msg = f"Non-default argument '{name}={value}' when constructing Index with query compiler"
    with pytest.raises(AssertionError, match=msg):
        pd.Index(query_compiler=idx._query_compiler, **kwargs)


@sql_count_checker(query_count=2)
def test_create_index_from_series():
    idx = pd.Index(pd.Series([5, 6]))
    assert_index_equal(idx, native_pd.Index([5, 6]))

    idx = pd.Index(pd.Series([5, 6], name="abc"))
    assert_index_equal(idx, native_pd.Index([5, 6], name="abc"))


@sql_count_checker(query_count=0)
def test_create_index_from_df_negative():
    with pytest.raises(ValueError):
        pd.Index(pd.DataFrame([[1, 2], [3, 4]]))
    with pytest.raises(ValueError):
        pd.DatetimeIndex(pd.DataFrame([[1, 2], [3, 4]]))


@sql_count_checker(query_count=3, join_count=3)
def test_index_identical():
    i1 = pd.Index(["a", "b", "c"])
    i2 = pd.Index(["a", "b", "c"])

    assert i1.identical(i2)

    i1 = i1.rename("foo")
    assert i1.equals(i2)
    assert not i1.identical(i2)

    i2 = i2.rename("foo")
    assert i1.identical(i2)

    i3 = pd.Index([("a", "a"), ("a", "b"), ("b", "a")])
    i4 = pd.Index([("a", "a"), ("a", "b"), ("b", "a")], tupleize_cols=False)
    assert not i3.identical(i4)


@pytest.mark.parametrize("native_index", NATIVE_INDEX_SCALAR_TEST_DATA)
@pytest.mark.parametrize("func", ["min", "max"])
@sql_count_checker(query_count=1)
def test_index_min_max(native_index, func):
    snow_index = pd.Index(native_index)
    snow_res = getattr(snow_index, func)()
    native_res = getattr(native_index, func)()
    # Snowpark pandas treats np.nan as None.
    native_res = None if native_res is np.nan else native_res
    assert snow_res == native_res


@pytest.mark.parametrize("func", ["min", "max"])
@pytest.mark.parametrize("axis", [1, "axis", 0.6, -1])
@sql_count_checker(query_count=0)
def test_index_min_max_wrong_axis_negative(func, axis):
    idx = pd.Index([1, 2, 3])
    with pytest.raises(ValueError, match="Axis must be None or 0 for Index objects"):
        getattr(idx, func)(axis=axis)


@pytest.mark.parametrize(
    "native_index",
    [
        native_pd.Index(["Apple", "Mango", "Watermelon"]),
        native_pd.Index(["Apple", "Mango", 2.0]),
        native_pd.Index([1, 2, 3, 4]),
        native_pd.Index([1.0, 2.0, 3.0, 4.0]),
        native_pd.Index([1.0, 2.0, np.nan, 4.0]),
        native_pd.Index([1, 2, 3, 4.0, np.nan]),
        native_pd.Index([1, 2, 3, 4.0, np.nan, "Apple"]),
        native_pd.Index([1, 2, 3, 4.0]),
        native_pd.Index([True, False, True]),
        native_pd.Index(["True", "False", "True"]),
        native_pd.Index([True, False, "True"]),
    ],
)
@pytest.mark.parametrize(
    "func", ["is_integer", "is_boolean", "is_floating", "is_numeric", "is_object"]
)
@sql_count_checker(query_count=0)
def test_index_is_type(native_index, func):
    snow_index = pd.Index(native_index)
    snow_res = getattr(snow_index, func)()
    native_res = getattr(native_index, func)()
    assert snow_res == native_res


@pytest.mark.parametrize("obj_type", ["df", "series"])
def test_df_series_set_index_and_reset_index(obj_type):
    obj = {"A": [1, 2, 3], "B": [4, 5, 6]}
    original_index = ["A", "B", "C"]
    assert_equal = assert_frame_equal if obj_type == "df" else assert_series_equal
    native_obj = (
        native_pd.DataFrame(obj, index=original_index)
        if obj_type == "df"
        else native_pd.Series(obj, index=original_index)
    )
    snow_obj = pd.DataFrame(native_obj) if obj_type == "df" else pd.Series(native_obj)

    # Index object to change obj's index to.
    native_idx = native_pd.Index([11, 22, 33])
    snow_idx = pd.Index(native_idx)

    # Test that df.index = new_index works with lazy index.
    with SqlCounter(query_count=1):
        native_obj.index = native_idx
        snow_obj.index = snow_idx
        assert_equal(snow_obj, native_obj)

    # Check if reset_index works with lazy index.
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(snow_obj, native_obj, lambda df: df.reset_index())
