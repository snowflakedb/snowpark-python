#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from numpy.testing import assert_equal

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import (
    assert_index_equal,
    assert_series_equal,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)

TEST_DFS = [
    native_pd.DataFrame(),
    native_pd.DataFrame({"col1": [1, 2, 3], "col2": [3, 4, 5], "col3": [5, 6, 7]}),
    native_pd.DataFrame(
        data={"col1": [1, 2, 3], "col2": [3, 4, 5]},
        index=native_pd.Index([[1, 2], [2, 3], [3, 4]]),
    ),
    native_pd.DataFrame([1]),
]

NATIVE_INDEX_TEST_DATA = [
    native_pd.Index([], dtype="object"),
    native_pd.Index([[1, 2], [2, 3], [3, 4]]),
    native_pd.Index([1, 2, 3]),
    native_pd.Index([3, np.nan, 5], name="my_index"),
    native_pd.Index([5, None, 7]),
    native_pd.Index([1]),
    native_pd.Index(["a", "b", 1, 2]),
]

NATIVE_NUMERIC_INDEX_TEST_DATA = [
    native_pd.Index([], dtype="int64"),
    native_pd.Index([1], dtype="float64"),
    native_pd.Index([1.1, 2.2, 1.0, 1, 1.1, 2.2, 1, 1, 1, 2, 2, 2, 2.2]),
    native_pd.Index([1, 3, 1, 1, 1, 3, 1, 1, 1, 2, 2, 2, 3], name="random name"),
    native_pd.Index(
        [True, False, True, False, True, False, True, False, True, True], dtype=bool
    ),
]

INDEX1 = native_pd.Series(
    [1, 2, 3, 2, 3, 5, 6, 7, 8, 4, 4, 5, 6, 7, 1, 2, 1, 2, 3, 4, 3, 4, 5, 6, 7]
)
INDEX2 = native_pd.Series(
    [
        "a",
        "b",
        "c",
        "b",
        "c",
        "e",
        "f",
        "g",
        "h",
        "d",
        "d",
        "e",
        "f",
        "g",
        "a",
        "b",
        "a",
        "b",
        "c",
        "d",
        "c",
        "d",
        "e",
        "f",
        "g",
    ]
)


@pytest.mark.parametrize(
    "native_series, expected, normalize, sort, ascending, dropna",
    [
        (
            INDEX1,
            native_pd.Series(
                [0.04, 0.12, 0.12, 0.12, 0.12, 0.16, 0.16, 0.16],
                index=[8, 1, 5, 6, 7, 2, 3, 4],
                name="proportion",
            ),
            True,
            True,
            True,
            True,
        ),
        (
            INDEX1,
            native_pd.Series(
                [0.04, 0.12, 0.12, 0.12, 0.12, 0.16, 0.16, 0.16],
                index=[8, 1, 5, 6, 7, 2, 3, 4],
                name="proportion",
            ),
            True,
            True,
            True,
            False,
        ),
        (
            INDEX1,
            native_pd.Series(
                [1, 3, 3, 3, 3, 4, 4, 4], index=[8, 1, 5, 6, 7, 2, 3, 4], name="count"
            ),
            False,
            True,
            True,
            True,
        ),
        (
            INDEX1,
            native_pd.Series(
                [1, 3, 3, 3, 3, 4, 4, 4], index=[8, 1, 5, 6, 7, 2, 3, 4], name="count"
            ),
            False,
            True,
            True,
            False,
        ),
        (
            INDEX2,
            native_pd.Series(
                [0.04, 0.12, 0.12, 0.12, 0.12, 0.16, 0.16, 0.16],
                index=["h", "a", "e", "f", "g", "b", "c", "d"],
                name="proportion",
            ),
            True,
            True,
            True,
            True,
        ),
        (
            INDEX2,
            native_pd.Series(
                [0.04, 0.12, 0.12, 0.12, 0.12, 0.16, 0.16, 0.16],
                index=["h", "a", "e", "f", "g", "b", "c", "d"],
                name="proportion",
            ),
            True,
            True,
            True,
            False,
        ),
        (
            INDEX2,
            native_pd.Series(
                [1, 3, 3, 3, 3, 4, 4, 4],
                index=["h", "a", "e", "f", "g", "b", "c", "d"],
                name="count",
            ),
            False,
            True,
            True,
            True,
        ),
        (
            INDEX2,
            native_pd.Series(
                [1, 3, 3, 3, 3, 4, 4, 4],
                index=["h", "a", "e", "f", "g", "b", "c", "d"],
                name="count",
            ),
            False,
            True,
            True,
            False,
        ),
    ],
)
@sql_count_checker(query_count=1)
def test_series_value_counts_non_deterministic_pandas_behavior(
    native_series, expected, normalize, sort, ascending, dropna
):
    # Native pandas produces different results locally and on Jenkins when the above data is used.
    # Therefore, explicitly compare the actual and expected results.

    # If `sort=True`, sort by the frequencies. If `sort=False`, maintain the original ordering.
    # `ascending` controls whether to sort the count in ascending or descending order.

    # When there is a tie between frequencies, the order is still deterministic, but
    # may be different from the result from native pandas. Snowpark pandas will
    # always respect the order of insertion during ties. Native pandas is not
    # deterministic since the original order/order of insertion is based on the
    # Python hashmap which may produce different results on different versions.
    # Refer to: https://github.com/pandas-dev/pandas/issues/15833
    snow_index = pd.Index(native_series)
    actual = snow_index.value_counts(
        normalize=normalize, sort=sort, ascending=ascending, dropna=dropna
    )
    assert_series_equal(actual, expected)


@pytest.mark.parametrize("native_index", NATIVE_NUMERIC_INDEX_TEST_DATA)
@pytest.mark.parametrize("normalize", [True, False])
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize("ascending", [True, False])
@pytest.mark.parametrize("dropna", [True, False])
@sql_count_checker(query_count=1)
def test_index_value_counts(native_index, normalize, sort, ascending, dropna):
    snow_index = pd.Index(native_index)
    eval_snowpark_pandas_result(
        snow_index,
        native_index,
        lambda idx: idx.value_counts(
            normalize=normalize, sort=sort, ascending=ascending, dropna=dropna
        ),
    )


@sql_count_checker(query_count=2)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_astype(native_index):
    # TODO: SNOW-1480906: Investigate astype failure for int to object conversion
    snow_index = pd.Index(native_index)
    snow_index = snow_index.astype("object")
    assert repr(snow_index) == repr(native_index)


@sql_count_checker(query_count=2)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_copy(native_index):
    snow_index = pd.Index(native_index)
    new_index = snow_index.copy()
    assert snow_index is not new_index
    assert_index_equal(snow_index, new_index)


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


@sql_count_checker(query_count=2)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA[2:])
def test_index_drop(native_index):
    snow_index = pd.Index(native_index)
    labels = [native_index[0]]
    assert_index_equal(snow_index.drop(labels), native_index.drop(labels))


@sql_count_checker(query_count=6)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_equals(native_index):
    snow_index = pd.Index(native_index)
    index_check = pd.Index([1, 2, 3, 4])
    assert not snow_index.equals(index_check)
    assert index_check.equals(pd.Index([1, 2, 3, 4]))

    new_index = snow_index.copy()
    assert snow_index.equals(new_index)


@sql_count_checker(query_count=4)
@pytest.mark.parametrize("native_df", TEST_DFS)
def test_df_index_equals(native_df):
    snow_df = pd.DataFrame(native_df)
    assert native_df.columns.equals(native_df.columns)
    assert snow_df.columns.equals(snow_df.columns)
    assert native_df.columns.equals(snow_df.columns.to_pandas())
    assert snow_df.columns.equals(native_df.columns)

    assert native_df.index.equals(native_df.index)
    assert snow_df.index.equals(snow_df.index)
    assert native_df.index.equals(snow_df.index.to_pandas())
    assert snow_df.index.equals(native_df.index)


@sql_count_checker(query_count=4)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA[:-1])
def test_index_sort_values(native_index):
    snow_index = pd.Index(native_index)
    with SqlCounter(query_count=4):
        assert_index_equal(snow_index.sort_values(), native_index.sort_values())
        native_tup = native_index.sort_values(return_indexer=True)
        snow_tup = snow_index.sort_values(return_indexer=True)
        assert_index_equal(native_tup[0], snow_tup[0])
        assert np.array_equal(native_tup[1], snow_tup[1])


@sql_count_checker(query_count=8)
def test_index_union():
    idx1 = pd.Index([1, 2, 3, 4])
    idx2 = pd.Index([3, 4, 5, 6])
    union = idx1.union(idx2)
    assert_index_equal(union, pd.Index([1, 2, 3, 4, 5, 6], dtype="int64"))
    idx1 = pd.Index(["a", "b", "c", "d"])
    idx2 = pd.Index([1, 2, 3, 4])
    union = idx1.union(idx2)
    assert_index_equal(
        union, pd.Index(["a", "b", "c", "d", 1, 2, 3, 4], dtype="object")
    )


@sql_count_checker(query_count=4)
def test_index_difference():
    idx1 = pd.Index([2, 1, 3, 4])
    idx2 = pd.Index([3, 4, 5, 6])
    diff = idx1.difference(idx2)
    assert_index_equal(diff, pd.Index([1, 2], dtype="int64"))


@sql_count_checker(query_count=4)
def test_index_intersection():
    idx1 = pd.Index([2, 1, 3, 4])
    idx2 = pd.Index([3, 4, 5, 6])
    diff = idx1.intersection(idx2)
    assert_index_equal(diff, pd.Index([3, 4], dtype="int64"))


@sql_count_checker(query_count=3)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_get_level_values(native_index):
    snow_index = pd.Index(native_index)
    assert_index_equal(snow_index.get_level_values(0), snow_index)


@sql_count_checker(query_count=2)
def test_slice_indexer():
    idx = pd.Index(list("abcd"))
    s = idx.slice_indexer(start="a", end="c")
    assert s != slice(1, 3, None)
    s = idx.slice_indexer(start="b", end="c")
    assert s == slice(1, 3, None)


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_summary(native_index):
    snow_index = pd.Index(native_index)
    assert snow_index._summary() == native_index._summary()


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_size(native_index):
    snow_index = pd.Index(native_index)
    assert snow_index.size == native_index.size


@pytest.mark.parametrize("native_df", TEST_DFS)
@sql_count_checker(query_count=1)
def test_df_index_size(native_df):
    snow_df = pd.DataFrame(native_df)
    assert snow_df.index.size == native_df.index.size
    assert snow_df.columns.size == native_df.columns.size


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_empty(native_index):
    snow_index = pd.Index(native_index)
    assert snow_index.empty == native_index.empty


@pytest.mark.parametrize("native_df", TEST_DFS)
@sql_count_checker(query_count=1)
def test_df_index_empty(native_df):
    snow_df = pd.DataFrame(native_df)
    assert snow_df.index.empty == native_df.index.empty
    assert snow_df.columns.empty == native_df.columns.empty


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_shape(native_index):
    snow_index = pd.Index(native_index)
    assert snow_index.shape == native_index.shape


@pytest.mark.parametrize("native_df", TEST_DFS)
@sql_count_checker(query_count=1)
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


@pytest.mark.parametrize("name", [None, "name", True, 1])
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


@pytest.mark.parametrize("name", [None, "name", True, 1])
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

    with SqlCounter(query_count=2, join_count=1 if generate_extra_index else 0):
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
@pytest.mark.parametrize("name", [None, "name", True, 1])
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


@sql_count_checker(query_count=2)
@pytest.mark.parametrize("name", [None, "name", True, 1])
@pytest.mark.parametrize("index", [True, False])
@pytest.mark.parametrize("native_df", TEST_DFS)
def test_df_index_columns_to_frame(native_df, index, name):
    snow_df = pd.DataFrame(native_df)
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        snow_df.index.to_frame(index=index, name=name),
        native_df.index.to_frame(index=index, name=name),
        check_index_type=False,
        check_column_type=False,
    )
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        snow_df.columns.to_frame(index=index, name=name),
        native_df.columns.to_frame(index=index, name=name),
        check_index_type=False,
        check_column_type=False,
    )


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_dtype(native_index):
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
