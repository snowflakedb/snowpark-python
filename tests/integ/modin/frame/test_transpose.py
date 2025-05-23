#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401

from tests.integ.modin.utils import (
    assert_snowpark_pandas_equal_to_pandas,
    assert_snowpark_pandas_equals_to_pandas_with_coerce_to_float64,
    create_test_dfs,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

transpose_and_double_transpose_parameterize = pytest.mark.parametrize(
    "transpose_operation, expected_query_count",
    [(lambda df: df.T, 1), (lambda df: df.T.T, 1)],
)


def update_columns_index(df, new_columns_index):
    columns = pd.MultiIndex.from_tuples(new_columns_index)
    df.columns = columns
    return df


@transpose_and_double_transpose_parameterize
def test_dataframe_transpose_default_index(
    transpose_operation, expected_query_count, score_test_data
):
    snow_df = pd.DataFrame(score_test_data)
    native_df = native_pd.DataFrame(score_test_data)
    with SqlCounter(query_count=expected_query_count):
        eval_snowpark_pandas_result(snow_df, native_df, transpose_operation)


@transpose_and_double_transpose_parameterize
def test_dataframe_transpose_set_single_index(
    transpose_operation, expected_query_count, score_test_data
):
    snow_df = pd.DataFrame(score_test_data)
    native_df = native_pd.DataFrame(score_test_data)
    with SqlCounter(query_count=expected_query_count):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: transpose_operation(df.set_index(["name"])),
        )


@pytest.mark.parametrize(
    "operation",
    [
        param(lambda df: df.T, id="transpose_once"),
        param(
            lambda df: df.T.T,
            marks=pytest.mark.xfail(
                raises=NotImplementedError, strict=True, reason="SNOW-886400"
            ),
            id="transpose_twice",
        ),
    ],
)
@pytest.mark.parametrize(
    "index",
    [
        [[pd.Timedelta("-1 days"), pd.NaT, pd.Timedelta("2 days")]],
        native_pd.MultiIndex.from_tuples(
            [
                (pd.Timedelta("-1 days"), -1),
                (pd.NaT, None),
                (pd.Timedelta("2 days"), 2),
            ]
        ),
    ],
)
def test_dataframe_transpose_set_timedelta_index_SNOW_1652608(
    operation, score_test_data, index
):
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            *create_test_dfs(score_test_data), lambda df: operation(df.set_index(index))
        )


@transpose_and_double_transpose_parameterize
def test_dataframe_transpose_set_multi_index(
    transpose_operation, expected_query_count, score_test_data
):
    snow_df = pd.DataFrame(score_test_data)
    native_df = native_pd.DataFrame(score_test_data)
    with SqlCounter(query_count=expected_query_count):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: transpose_operation(df.set_index(["name", "score"])),
        )


@transpose_and_double_transpose_parameterize
def test_dataframe_transpose_set_columns_multi_index(
    transpose_operation, expected_query_count, score_test_data
):
    snow_df = pd.DataFrame(score_test_data)
    native_df = native_pd.DataFrame(score_test_data)

    with SqlCounter(query_count=expected_query_count):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: transpose_operation(
                update_columns_index(
                    df,
                    [
                        ("id", "name"),
                        ("id", "score"),
                        ("status", "employed"),
                        ("status", "kids"),
                    ],
                )
            ),
        )


@transpose_and_double_transpose_parameterize
def test_dataframe_transpose_set_columns_multi_index_mixed_types(
    transpose_operation, expected_query_count, score_test_data
):
    snow_df = pd.DataFrame(score_test_data)
    native_df = native_pd.DataFrame(score_test_data)

    with SqlCounter(query_count=expected_query_count):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: transpose_operation(
                update_columns_index(
                    df,
                    [
                        (123, False),
                        (456, True),
                        (789, None),
                        (0, True),
                    ],
                )
            ),
            comparator=assert_snowpark_pandas_equals_to_pandas_with_coerce_to_float64,
        )


@transpose_and_double_transpose_parameterize
def test_dataframe_transpose_both_multi_index(
    transpose_operation, expected_query_count, score_test_data
):
    native_df = native_pd.DataFrame(score_test_data)
    snow_df = pd.DataFrame(score_test_data)

    with SqlCounter(query_count=expected_query_count):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: transpose_operation(
                update_columns_index(
                    df.set_index(["name", "score"]),
                    [("status", "employed"), ("family", "kids")],
                )
            ),
        )


@transpose_and_double_transpose_parameterize
def test_dataframe_transpose_single_row(transpose_operation, expected_query_count):
    single_row_data = {"A": [1], "B": [2], "C": [3], "D": [4], "E": [5], "F": [6]}
    native_df = native_pd.DataFrame(single_row_data)
    snow_df = pd.DataFrame(single_row_data)

    with SqlCounter(query_count=expected_query_count):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            transpose_operation,
        )


@pytest.mark.parametrize(
    "single_column_data",
    [
        {"A": [1, 2, 3, 4, 5, 6]},
        {"B": [1, 1, 1, 1, 1]},  # value all same
        {"None": [None]},
        {"none": [None, None, None]},
        {"NaT": [pd.NaT, pd.NaT, pd.NaT]},
        {"nan": [6.0, 7.1, np.nan]},
        {"A": [None, 1, 2, 3]},
        {"None": [None, 1, 1, None]},
        {"float": [1.1, 1.0 / 7]},
        {"str": ["abc", None, ("a", "c")]},
        {123: ["a", "b", "c"]},
        {False: [1, 2, 3], True: ["4", "5", "6"]},
        # Note that if no label is provided, a default integer label is created.
        [1.0, np.nan, 2, 4],
        # Snowpark PIVOT(MIN(col)) doesn't work on ARRAY and MAP types: this must get explicitly cast
        # to VARIANT during our post-processing
        {"array": [[1, 2, 3]]},
        {"col": {"row": {"key": "value"}}},
    ],
)
@sql_count_checker(query_count=1, union_count=1)
def test_dataframe_transpose_single_column(single_column_data):
    native_df = native_pd.DataFrame(single_column_data)
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.T)


@sql_count_checker(query_count=1, union_count=1)
def test_dataframe_transpose_preserve_int_dtypes():
    bigint = 2**32 + 1
    data = {
        "A": [bigint, bigint + 1],
        "B": [bigint + 2, bigint + 3],
        "C": [bigint + 4, bigint + 5],
    }
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(data)

    # We do full comparison with dtypes (should be Int64) to validate they are preserved through transpose.
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.T,
        comparator=assert_snowpark_pandas_equal_to_pandas,
    )

    assert all([dtype == "int64" for dtype in snow_df.T.dtypes])


@sql_count_checker(query_count=1, union_count=1)
def test_dataframe_transpose_preserve_float_dtypes():
    data = {
        "A": [1.5, 2.3],
        "B": [3.14, 2.718],
        "C": [99.9, 101.1],
    }
    native_df = native_pd.DataFrame(data)
    snow_df = pd.DataFrame(data)

    # We do full comparison with dtypes (should be Int64) to validate they are preserved through transpose.
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.T,
        comparator=assert_snowpark_pandas_equal_to_pandas,
    )
    assert all([dtype == "float64" for dtype in snow_df.T.dtypes])


@sql_count_checker(query_count=1, union_count=1)
def test_dataframe_transpose_single_numeric_column():
    single_column_data = ({0: "A", 1: "B", 2: "C", 3: "D"},)
    native_df = native_pd.DataFrame(single_column_data, index=(0,))
    snow_df = pd.DataFrame(single_column_data, index=(0,))

    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.T)


@sql_count_checker(query_count=1, union_count=1)
def test_dataframe_all_missing():
    native_df = native_pd.DataFrame(
        {
            "nat": [native_pd.NaT, native_pd.NaT, native_pd.NaT],
            "nan": [np.nan, np.nan, np.nan],
            "none": [None, None, None],
        }
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.T)


@pytest.mark.parametrize(
    "test_args, expected_query_count",
    [
        ((None, None), 2),
        (([1, 2, 3], None), 2),
        ((None, ["a", "b", "c"]), 1),
    ],
)
def test_dataframe_transpose_empty(test_args, expected_query_count):
    index = test_args[0]
    columns = test_args[1]
    native_df = native_pd.DataFrame(index=index, columns=columns)
    snow_df = pd.DataFrame(index=index, columns=columns)
    with SqlCounter(query_count=expected_query_count):
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda df: df.T, check_column_type=False
        )


@pytest.mark.skip("SNOW-896260 Pivot with empty column values fails")
def test_dataframe_transpose_empty_with_failing_values():
    index = [1, 2, 3]
    columns = ["a", "b", "c"]
    native_df = native_pd.DataFrame(index=index, columns=columns)
    snow_df = pd.DataFrame(index=index, columns=columns)

    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.T)


# This will succeed in pandas but fail in snowpark pandas
@pytest.mark.xfail(
    reason="SNOW-896985: Support non-JSON serializable types in dataframe",
    strict=True,
)
@pytest.mark.parametrize(
    "col_label",
    [
        native_pd.Timestamp(year=2023, month=9, day=29),
        datetime.datetime(year=2023, month=9, day=29),
    ],
)
@sql_count_checker(query_count=0)
def test_dataframe_transpose_not_json_serializable_unimplemented(
    col_label, score_test_data
):
    test_data = score_test_data.copy()
    test_data[col_label] = test_data["name"]

    snow_df = pd.DataFrame(test_data)
    native_df = native_pd.DataFrame(test_data)

    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.T)


# This will succeed in pandas but fail in snowpark pandas
@pytest.mark.xfail(
    reason="SNOW-896985: Support non-JSON serializable types in dataframe"
)
@sql_count_checker(query_count=5)
def test_dataframe_transpose_object_data():
    class CustomObject:
        pass

    single_column_data = [123]
    columns = [CustomObject()]

    native_df = native_pd.DataFrame(single_column_data, columns=columns)
    snow_df = pd.DataFrame(single_column_data, columns=columns)

    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.T)


@sql_count_checker(query_count=1)
def test_dataframe_transpose_copy_warning_log(caplog, score_test_data):
    pd.DataFrame().transpose(copy=True)

    assert (
        "Single Warning: The argument `copy` of `transpose` has been ignored by Snowpark pandas API:\n"
        "Transpose ignore copy argument in Snowpark pandas API. was raised."
        in [r.msg for r in caplog.records]
    )


@sql_count_checker(query_count=1)
def test_dataframe_transpose_args_warning_log(caplog, score_test_data):
    pd.DataFrame().transpose("foo", "bar")

    assert (
        "The argument `args` of `transpose` has been ignored by Snowpark pandas API:\n"
        "Transpose ignores args in Snowpark pandas API."
        in [r.msg for r in caplog.records]
    )
