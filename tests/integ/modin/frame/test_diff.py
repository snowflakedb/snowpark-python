#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker

NUM_COLS_TALL_DF = 4
NUM_ROWS_TALL_DF = 32


@sql_count_checker(query_count=0)
def test_df_diff_invalid_periods_negative():
    native_df = native_pd.DataFrame()
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.diff(periods=0.5),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="periods must be an integer",
        assert_exception_equal=True,
    )

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.diff(periods="1"),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="periods must be an integer",
        assert_exception_equal=True,
    )


@pytest.mark.parametrize(
    "periods",
    [
        -1,
        0,
        1,
        NUM_ROWS_TALL_DF / 2,
        -1 * NUM_ROWS_TALL_DF / 2,
        NUM_ROWS_TALL_DF - 1,
        -1 * (NUM_ROWS_TALL_DF - 1),
        NUM_ROWS_TALL_DF,
        -1 * NUM_ROWS_TALL_DF,
    ],
    ids=[
        "with_row_after",
        "with_self",
        "with_row_before",
        "with_len(df)/2_rows_before",
        "with_len(df)/2_rows_after",
        "with_len(df)-1_rows_before",
        "with_len(df)-1_rows_after",
        "with_len(df)_rows_before",
        "with_len(df)_rows_after",
    ],
)
@sql_count_checker(query_count=1)
def test_row_wise_diff_all_int_df(periods):
    native_df = native_pd.DataFrame(
        np.arange(NUM_ROWS_TALL_DF * NUM_COLS_TALL_DF).reshape(
            (NUM_ROWS_TALL_DF, NUM_COLS_TALL_DF)
        ),
        columns=["A", "B", "C", "D"],
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.diff(periods),
    )


@pytest.mark.parametrize(
    "periods",
    [
        -1,
        0,
        1,
        NUM_COLS_TALL_DF / 2,
        -1 * NUM_COLS_TALL_DF / 2,
        NUM_COLS_TALL_DF - 1,
        -1 * (NUM_COLS_TALL_DF - 1),
        NUM_COLS_TALL_DF,
        -1 * NUM_COLS_TALL_DF,
    ],
    ids=[
        "with_col_after",
        "with_self",
        "with_col_before",
        "with_len(df.columns)/2_cols_before",
        "with_len(df.columns)/2_cols_after",
        "with_len(df.columns)-1_cols_before",
        "with_len(df.columns)-1_cols_after",
        "with_len(df.columns)_cols_before",
        "with_len(df.columns)_cols_after",
    ],
)
@sql_count_checker(query_count=1)
def test_col_wise_diff_all_int_df(periods):
    native_df = native_pd.DataFrame(
        np.arange(NUM_ROWS_TALL_DF * NUM_COLS_TALL_DF).reshape(
            (NUM_ROWS_TALL_DF, NUM_COLS_TALL_DF)
        ),
        columns=["A", "B", "C", "D"],
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.diff(periods, axis=1),
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("periods", [0, 1])
def test_df_diff_bool_df(periods):
    native_df = native_pd.DataFrame(
        np.arange(NUM_ROWS_TALL_DF * NUM_COLS_TALL_DF).reshape(
            (NUM_ROWS_TALL_DF, NUM_COLS_TALL_DF)
        ),
        columns=["A", "B", "C", "D"],
    )
    native_df = native_df.astype({"A": bool, "C": bool})
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.diff(periods=periods))


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("periods", [0, 1])
def test_df_diff_timedelta_df(periods):
    native_df = native_pd.DataFrame(
        np.arange(NUM_ROWS_TALL_DF * NUM_COLS_TALL_DF).reshape(
            (NUM_ROWS_TALL_DF, NUM_COLS_TALL_DF)
        ),
        columns=["A", "B", "C", "D"],
    )
    native_df = native_df.astype({"A": "timedelta64[ns]", "C": "timedelta64[ns]"})
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.diff(periods=periods))


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("periods", [-1, 0, 1])
@pytest.mark.parametrize("axis", [0, 1])
def test_df_diff_datetime_df(periods, axis):
    native_df = native_pd.DataFrame(
        np.arange(NUM_ROWS_TALL_DF * NUM_COLS_TALL_DF).reshape(
            (NUM_ROWS_TALL_DF, NUM_COLS_TALL_DF)
        ),
        columns=["A", "B", "C", "D"],
    )
    native_df = native_df.astype("datetime64[ns]")
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.diff(periods=periods, axis=axis)
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("periods", [0, 1])
def test_df_diff_int_and_bool_df(periods):
    native_df = native_pd.DataFrame(
        np.arange(NUM_ROWS_TALL_DF * NUM_COLS_TALL_DF).reshape(
            (NUM_ROWS_TALL_DF, NUM_COLS_TALL_DF)
        ),
        columns=["A", "B", "C", "D"],
    )
    native_df = native_df.astype({"A": bool, "C": bool})
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.diff(axis=1, periods=periods)
    )


@sql_count_checker(query_count=1)
def test_df_diff_bools_as_variants():
    native_df = native_pd.DataFrame([[True, False], [False, True]], dtype=object)
    snow_df = pd.DataFrame(native_df).astype(object)

    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.diff())


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("periods", [0, 1])
def test_df_diff_bools_as_variants_and_ints(periods):
    native_df = native_pd.DataFrame([[True, 1], [False, 2]])
    native_df = native_df.astype({0: object})
    snow_df = pd.DataFrame(native_df).astype({0: object})

    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.diff(axis=1, periods=periods)
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("periods", [0, 1])
def test_df_diff_bools_and_ints_as_variants(periods):
    native_df = native_pd.DataFrame([[True, 1], [False, 2]], dtype=object)
    snow_df = pd.DataFrame(native_df).astype(object)

    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.diff(axis=1, periods=periods)
    )


@sql_count_checker(query_count=0)
def test_df_diff_custom_variant_type_negative():
    class CustomObject(dict):
        def __rsub__(self, other):
            if isinstance(other, bool):
                return 3
            elif isinstance(other, type(self)):
                return "self"
            else:
                return 4

        def __sub__(self, other):
            if isinstance(other, bool):
                return -3
            elif isinstance(other, type(self)):
                return "self"
            else:
                return -4

    native_df = native_pd.DataFrame(
        [
            [True, CustomObject(), False],
            [False, CustomObject(), True],
            [True, CustomObject(), False],
        ]
    )
    snow_df = pd.DataFrame(native_df)
    with pytest.raises(
        SnowparkSQLException,
        match=r"Invalid argument types for function '-': \(OBJECT, OBJECT\)",
    ):
        snow_df.diff().to_pandas()


@pytest.mark.parametrize("axis", [0, 1])
@sql_count_checker(query_count=1)
def test_df_diff_mixed_variant_columns(axis):
    native_df = native_pd.DataFrame(
        [[True, 1, False], [1, np.nan, 3], [False, 4, None]]
    )
    snow_df = pd.DataFrame(native_df)

    if axis == 1:
        eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.diff(axis=axis))
    elif axis == 0:
        # When axis=0, pandas fails because we try to subtract
        # None and an int.
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snow_df.diff(axis=axis),
            native_pd.DataFrame(
                [[np.nan, np.nan, np.nan], [0.0, np.nan, 3.0], [-1.0, np.nan, np.nan]]
            ),
        )


@pytest.mark.parametrize("axis", [0, 1])
@sql_count_checker(query_count=1)
def test_df_diff_bool_df_with_missing_values(axis):
    native_df = native_pd.DataFrame(
        [[True, None, False], [None, False, True], [True, None, None]]
    )
    snow_df = pd.DataFrame(native_df)

    # When axis=0, pandas errors out because it attempts to subtract `None` from a bool type.
    # Snowpark pandas; however, computes the correct discrete difference (i.e. setting the
    # value to None.)
    if axis == 1:
        eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.diff(axis=axis))
    else:
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snow_df.diff(axis=0),
            native_pd.DataFrame(
                [[None, None, None], [None, None, True], [None, None, None]]
            ),
        )


@sql_count_checker(query_count=0)
def test_df_diff_string_type_negative():
    native_df = native_pd.DataFrame(
        [
            ["a", "b", "c"],
            ["d", "e", "f"],
        ]
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.diff(),
        expect_exception=True,
        expect_exception_match="Numeric value 'a' is not recognized",
        assert_exception_equal=False,
    )


@sql_count_checker(query_count=4)
def test_df_diff_strided_column_access():
    import string

    native_df = native_pd.DataFrame(
        np.arange(26 * 1000).reshape((1000, 26)), columns=list(string.ascii_lowercase)
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df[list(string.ascii_lowercase)[::2]].diff(axis=1),
    )


@sql_count_checker(query_count=4)
def test_df_diff_strided_row_access():
    import string

    native_df = native_pd.DataFrame(
        np.arange(26 * 1000).reshape((1000, 26)), columns=list(string.ascii_lowercase)
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.iloc[::2].diff())
