#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd

# This test file contains tests that execute a single underlying snowpark/snowflake pivot query.
import pytest
import numpy as np

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.pivot.pivot_utils import (
    pivot_table_test_helper,
    pivot_table_test_helper_expects_exception,
)
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equal_to_pandas,
    create_test_dfs,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@sql_count_checker(query_count=1)
def test_pivot_table_no_index_single_column_single_value(df_data):
    pivot_table_test_helper(
        df_data,
        {
            "index": None,
            "columns": "C",
            "values": "D",
        },
    )


@pytest.mark.parametrize(
    "aggfunc",
    (
        "mean",
        np.mean,
        "sum",
        np.sum,
        "min",
        np.min,
        min,
        "max",
        np.max,
        max,
        "count",
        "median",
        np.median,
        "skew",
        "std",
        np.std,
        "var",
        np.var,
    ),
)
@sql_count_checker(query_count=1)
def test_pivot_table_single_index_single_column_single_value(df_data, aggfunc):
    pivot_table_test_helper(
        df_data,
        {
            "index": "A",
            "columns": "C",
            "values": "D",
            "aggfunc": aggfunc,
        },
    )


@pytest.mark.parametrize(
    "aggfunc",
    [
        "count",
        "sum",
        "min",
        "max",
        "mean",
    ],
)
@sql_count_checker(query_count=1)
def test_pivot_table_multi_index_single_column_single_value(df_data, aggfunc):
    pivot_table_test_helper(
        df_data,
        {"index": ["A", "B"], "columns": "C", "values": "D", "aggfunc": aggfunc},
    )


@pytest.mark.parametrize(
    "aggfunc",
    [
        "count",
        "sum",
        "min",
        "max",
        "mean",
    ],
)
@sql_count_checker(query_count=1)
def test_pivot_table_no_index(df_data, aggfunc):
    pivot_table_test_helper(
        df_data,
        {"columns": "C", "values": "D", "aggfunc": aggfunc},
    )


@sql_count_checker(query_count=1)
def test_pivot_table_empty_table_with_index():
    # Cannot use pivot_table_test_helper since that checks the inferred types
    # on the resulting DataFrames' columns (which are empty), and the inferred type
    # on our DataFrame's columns is empty, while pandas has type floating.
    import pandas as native_pd

    native_df = native_pd.DataFrame({"A": [], "B": [], "C": [], "D": []})
    snow_df = pd.DataFrame(native_df)
    pivot_kwargs = {
        "index": ["A", "B"],
        "columns": "C",
        "values": "D",
        "aggfunc": "count",
    }

    snow_result = snow_df.pivot_table(**pivot_kwargs).to_pandas()
    native_result = native_df.pivot_table(**pivot_kwargs)

    assert native_result.empty == snow_result.empty and (native_result.empty is True)
    assert list(native_result.columns) == list(snow_result.columns)
    assert list(native_result.index) == list(snow_result.index)


@sql_count_checker(query_count=1)
def test_pivot_table_single_index_no_column_single_value(df_data):
    pivot_table_test_helper(
        df_data,
        {
            "index": "A",
            "columns": None,
            "values": "D",
        },
    )


@sql_count_checker(query_count=1)
def test_pivot_table_multi_index_no_column_single_value(df_data):
    pivot_table_test_helper(
        df_data,
        {
            "index": ["A", "B"],
            "columns": None,
            "values": "D",
        },
    )


@sql_count_checker(query_count=0)
def test_pivot_table_no_index_no_column_single_value(df_data):
    pivot_table_test_helper_expects_exception(
        df_data,
        {
            "index": None,
            "columns": None,
            "values": "D",
        },
        expect_exception_match=r"No group keys passed\!",
        expect_exception_type=ValueError,
        assert_exception_equal=True,
    )


@pytest.mark.xfail(
    strict=True,
    reason="SNOW-1201994: index contains ints coerced to string",
    # df_data_with_duplicates is wrapped in a call to `np.array` that coerces the
    # provided integer literals to str.
    # pandas 2.1 no longer allows the calculation of mean() on string columns, and now fails.
    # Though the data is now fixed to contain the appropriate mix of strings and ints, the test
    # now fails because the column names of the Snowpark pandas pivot_table result are coerced
    # to string instead of staying as int.
    # https://github.com/pandas-dev/pandas/issues/36703
    # https://github.com/pandas-dev/pandas/issues/44008
)
@sql_count_checker(query_count=1, join_count=1)
def test_pivot_table_with_duplicate_values(
    df_data_with_duplicates,
):
    pivot_table_test_helper(
        df_data_with_duplicates,
        {
            "index": "C",
            "columns": "E",
            "values": "D",
        },
        # Duplicates aren't handled currently for coercing and not needed for this particular test.
        coerce_to_float64=False,
    )


@pytest.mark.parametrize(
    "aggfunc",
    [
        "count",
        "sum",
    ],
)
@pytest.mark.parametrize(
    "values",
    [
        "DS",
        "FV",
    ],
)
@sql_count_checker(query_count=1)
def test_pivot_table_with_sum_and_count_null_and_empty_values_matching_behavior(
    df_data_small, aggfunc, values
):
    pivot_table_test_helper(
        df_data_small,
        {"index": ["ax", "AX"], "columns": "aX", "values": values, "aggfunc": aggfunc},
    )


@pytest.mark.skip(
    "SNOW-870145: This fails because nan values are not stored as null so we count/sum them differently"
)
@pytest.mark.parametrize(
    "aggfunc",
    [
        "count",
        "sum",
    ],
)
def test_pivot_table_with_sum_and_count_null_and_empty_values_matching_behavior_skipped(
    df_data_small, aggfunc, values
):
    pivot_table_test_helper(
        df_data_small,
        {"index": ["AX", "aX"], "columns": "ax", "values": "ET", "aggfunc": aggfunc},
    )


# One extra query to convert to native pandas in dataframe constructor when creating snow_df
@sql_count_checker(query_count=6, join_count=1)
def test_pivot_on_inline_data_using_temp_table():
    # Create a large dataframe of inlined data that will spill to a temporary table.
    snow_df = pd.DataFrame(
        {k: list(range(25)) for k in list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")},
        index=pd.Index(list(range(25)), name="index_no"),
    )

    snow_df = snow_df.pivot_table(
        index="index_no", values="A", columns="B", aggfunc=["sum", "count"]
    )

    # This would fail if the inlined data was not materialized first.
    row_count = snow_df._query_compiler.get_axis_len(0)

    assert row_count == 25


@pytest.mark.parametrize(
    "index, columns",
    [
        (None, "a"),
        (None, ["a"]),
        (None, ["a", "b"]),
        (None, ["a", "b", "c"]),
        ("d", "a"),
        ("d", ["a"]),
        ("d", ["a", "b"]),
        ("d", ["a", "b", "c"]),
        (["d"], "a"),
        (["d"], ["a"]),
        (["d"], ["a", "b"]),
        (["d"], ["a", "b", "c"]),
    ],
)
@pytest.mark.parametrize("margins", [True, False])
@pytest.mark.parametrize(
    "named_columns", [True, False], ids=["named_columns", "unnamed_columns"]
)
def test_pivot_empty_frame_snow_1013918(index, columns, margins, named_columns):
    cols = list("abcd")
    if named_columns:
        cols = pd.Index(cols, name="columns")
    snow_df, native_df = create_test_dfs(columns=cols)
    if named_columns:
        native_df.columns.names = snow_df.columns.names
    query_count = 2 if index is None or (len(columns) == 3 and margins) else 1
    join_count = 1 if index is not None and len(columns) == 1 else 0
    with SqlCounter(query_count=query_count, join_count=join_count):
        snow_df = snow_df.pivot_table(index=index, columns=columns, margins=margins)
        if margins and index is not None and len(list(index) + list(columns)) == 4:
            # When margins is True, and there are no values (i.e. all of the
            # columns in the DataFrame are passed in to either the `index`
            # or `columns` parameter), pandas errors out. We return an empty
            # DataFrame instead.
            with pytest.raises(TypeError, match="'str' object is not callable"):
                native_df.pivot_table(index=index, columns=columns, margins=margins)
            if isinstance(columns, list):
                levels = codes = [[]] * (len(columns) + 1)
                columns = [None if not named_columns else "columns"] + columns
            else:
                levels = codes = [[]]
            idx = pd.Index([], name=index[0])
            native_df = native_pd.DataFrame(
                index=idx,
                columns=pd.MultiIndex(levels=levels, codes=codes, names=columns),
            )
            native_df.index.name = index[0]
        else:
            native_df = native_df.pivot_table(
                index=index, columns=columns, margins=margins
            )
        assert_snowpark_pandas_equal_to_pandas(
            snow_df, native_df, check_index_type=False, check_column_type=False
        )


@pytest.mark.parametrize("margins", [True, False])
@pytest.mark.parametrize("named_columns", [True, False])
def test_pivot_empty_frame_no_values(margins, named_columns):
    columns = list("abcd")
    if named_columns:
        columns = pd.Index(columns, name="columns")
    snow_df, native_df = create_test_dfs(columns=columns)
    if named_columns:
        native_df.columns.names = snow_df.columns.names
    with SqlCounter(query_count=1):
        snow_df = snow_df.pivot_table(
            index=["c", "d"], columns=["a", "b"], margins=margins
        )
        if margins:
            # When margins is True, and there are no values (i.e. all of the
            # columns in the DataFrame are passed in to either the `index`
            # or `columns` parameter), pandas errors out. We return an empty
            # DataFrame instead.
            with pytest.raises(TypeError, match="'str' object is not callable"):
                native_df.pivot_table(
                    index=["c", "d"], columns=["a", "b"], margins=margins
                )
            levels = codes = [[], []]
            ind = pd.MultiIndex(levels=levels, codes=codes, names=["c", "d"])
            levels = codes = [[], [], []]
            cols = pd.MultiIndex(
                levels=levels,
                codes=codes,
                names=[None if not named_columns else "columns", "a", "b"],
            )
            native_df = native_pd.DataFrame(index=ind, columns=cols)
        else:
            native_df = native_df.pivot_table(
                index=["c", "d"], columns=["a", "b"], margins=margins
            )
        assert_snowpark_pandas_equal_to_pandas(
            snow_df, native_df, check_index_type=False, check_column_type=False
        )


@pytest.mark.skip(reason="No support for multi-indexes in pivot yet.")
@pytest.mark.parametrize(
    "index, columns",
    [
        (None, ("a", 1)),
        (None, [("a", 1)]),
        (None, [("a", 1), ("b", 2)]),
        (None, [("a", 1), ("b", 2), ("c", 3)]),
        (("d", 4), ("a", 1)),
        (("d", 4), [("a", 1)]),
        (("d", 4), [("a", 1), ("b", 2)]),
        (("d", 4), [("a", 1), ("b", 2), ("c", 3)]),
        ([("d", 4)], ("a", 1)),
        ([("d", 4)], [("a", 1)]),
        ([("d", 4)], [("a", 1), ("b", 2)]),
        ([("d", 4)], [("a", 1), ("b", 2), ("c", 3)]),
    ],
)
@pytest.mark.parametrize("margins", [True, False])
@pytest.mark.parametrize("named_columns", [True, False])
def test_pivot_empty_frame_snow_1013918_multiindex(
    index, columns, margins, named_columns
):
    cols = native_pd.MultiIndex.from_tuples(list(zip(list("abcd"), list("1234"))))
    if named_columns:
        cols.names = ["c1", "c2"]
    snow_df, native_df = create_test_dfs(columns=cols)
    query_count = 2 if index is None else 1
    join_count = 1 if index is not None and len(columns) == 1 else 0
    with SqlCounter(query_count=query_count, join_count=join_count):
        snow_df = snow_df.pivot_table(index=index, columns=columns, margins=margins)
        if margins and index is not None and len(list(index) + list(columns)) == 4:
            # When margins is True, and there are no values (i.e. all of the
            # columns in the DataFrame are passed in to either the `index`
            # or `columns` parameter), pandas errors out. We return an empty
            # DataFrame instead.
            with pytest.raises(TypeError, match="'str' object is not callable"):
                native_df.pivot_table(index=index, columns=columns, margins=margins)
            if isinstance(columns, list):
                levels = codes = [[]] * (len(columns) + 2)
                columns = ([None] if not named_columns else ["c1", "c2"]) + columns
            else:
                levels = codes = [[]]
            native_df = native_pd.DataFrame(
                index=pd.Index([], name=index if isinstance(index, str) else index[0]),
                columns=pd.MultiIndex(levels=levels, codes=codes, names=columns),
            )
        else:
            native_df = native_df.pivot_table(
                index=index, columns=columns, margins=margins
            )
        assert_snowpark_pandas_equal_to_pandas(
            snow_df, native_df, check_index_type=False, check_column_type=False
        )


@pytest.mark.skip(reason="No support for multi-indexes in pivot yet.")
@pytest.mark.parametrize("margins", [True, False])
@sql_count_checker(query_count=1)
@pytest.mark.parametrize("named_columns", [True, False])
def test_pivot_empty_frame_no_values_multiindex(margins, named_columns):
    columns = native_pd.MultiIndex(list(zip(list("abcd"), [1, 2, 3, 4])))
    if named_columns:
        columns.names = ["c1", "c2"]
    snow_df, native_df = create_test_dfs(columns=columns)
    snow_df = snow_df.pivot_table(
        index=[("c", 3), ("d", 4)], columns=[("a", 1), ("b", 2)], margins=margins
    )
    if margins:
        # When margins is True, and there are no values (i.e. all of the
        # columns in the DataFrame are passed in to either the `index`
        # or `columns` parameter), pandas errors out. We return an empty
        # DataFrame instead.
        with pytest.raises(TypeError, match="'str' object is not callable"):
            native_df.pivot_table(
                index=[("c", 3), ("d", 4)],
                columns=[("a", 1), ("b", 2)],
                margins=margins,
            )
        levels = codes = [[], []]
        ind = pd.MultiIndex(levels=levels, codes=codes, names=[("c", 3), ("d", 4)])
        levels = codes = [[], [], [], []]
        cols = pd.MultiIndex(
            levels=levels,
            codes=codes,
            names=([None] if not named_columns else ["c1", "c2"])
            + [("a", 1), ("b", 2)],
        )
        native_df = native_pd.DataFrame(index=ind, columns=cols)
    else:
        native_df = native_df.pivot_table(
            index=[("c", 3), ("d", 4)], columns=[("a", 1), ("b", 2)], margins=margins
        )
    assert_snowpark_pandas_equal_to_pandas(
        snow_df, native_df, check_index_type=False, check_column_type=False
    )
