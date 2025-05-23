#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import Any, Callable, Optional, Union

import modin.pandas as pd
import numpy as np
import pandas as native_pd

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_with_coerce_to_float64,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)


def pivot_table_test_helper(
    df_data: Union[list, dict, np.array],
    pivot_table_kwargs: dict[Any, Any],
    preprocess_df: Optional[
        Callable[
            [Union[pd.DataFrame, native_pd.DataFrame]],
            Union[pd.DataFrame, native_pd.DataFrame],
        ]
    ] = None,
    coerce_to_float64: bool = True,
    expect_exception: bool = False,
    expect_exception_type: Optional[type[Exception]] = None,
    expect_exception_match: Optional[str] = None,
    assert_exception_equal: bool = True,
    named_columns: bool = False,
):
    """
    Helper for validating pivot_table tests, specifically this ensures the output is normalized to float64
    with acceptable precision so can compare the results if coerce_to_float64 is True.

    df_data: The raw data to be put in df as list of data/columns, dictionary of data values (col:series) or np.array
    preprocess_df: Optional callable to apply before the pivot_table operation on dataframe
    pivot_table_kwargs: Arguments to pass to pivot_table call
    coerce_to_float64: Coerce the results to float64 result since irrational numbers can result in object type
    expect_exception: Whether the call *should* raise an exception
    expect_exception_type: if not None, assert the exception type is expected
    expect_exception_match: if not None, assert the exception match the expected regex
    assert_exception_equal: bool. Whether to assert the exception from Snowpark pandas eqauls to pandas
    named_columns: bool. Whether to name the columns when making the test DataFrames.
    """
    if isinstance(df_data, tuple):
        native_df = native_pd.DataFrame(df_data[0], columns=df_data[1])
        snow_df = pd.DataFrame(df_data[0], columns=df_data[1])
    else:
        native_df = native_pd.DataFrame(df_data)
        snow_df = pd.DataFrame(df_data)

    if named_columns:
        native_df.columns.names = [f"c{i}" for i in range(len(native_df.columns.names))]
        snow_df.columns = native_df.columns
    if preprocess_df:
        native_df = preprocess_df(native_df)
        snow_df = preprocess_df(snow_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.pivot_table(**pivot_table_kwargs),
        comparator=assert_snowpark_pandas_equals_to_pandas_with_coerce_to_float64
        if coerce_to_float64
        else assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
        expect_exception=expect_exception,
        expect_exception_type=expect_exception_type,
        expect_exception_match=expect_exception_match,
        assert_exception_equal=assert_exception_equal,
    )


def pivot_table_test_helper_expects_exception(
    df_data: Union[list, dict, np.array],
    pivot_table_kwargs: dict[Any, Any],
    preprocess_df: Optional[
        Callable[
            [Union[pd.DataFrame, native_pd.DataFrame]],
            Union[pd.DataFrame, native_pd.DataFrame],
        ]
    ] = None,
    expect_exception_type: Optional[type[Exception]] = None,
    expect_exception_match: Optional[str] = None,
    assert_exception_equal: bool = True,
):
    """
    Helper for validating pivot_table tests, specifically this expects an exception to be raised.

    df_data: The raw data to be put in df as list of data/columns, dictionary of data values (col:series) or np.array
    pivot_table_kwargs: Arguments to pass to pivot_table call
    preprocess_df: Optional callable to apply before the pivot_table operation on dataframe
    expect_exception_type: if not None, assert the exception type is expected
    expect_exception_match: if not None, assert the exception match the expected regex
    assert_exception_equal: bool. Whether to assert the exception from Snowpark pandas eqauls to pandas
    """
    pivot_table_test_helper(
        df_data,
        pivot_table_kwargs,
        preprocess_df=preprocess_df,
        expect_exception=True,
        expect_exception_type=expect_exception_type,
        expect_exception_match=expect_exception_match,
        assert_exception_equal=assert_exception_equal,
    )


def df_pivot_table(df, *args, **kwargs):
    """
    Wrapper for pivot table calls in case we need to add other checks or pre-/post- processing steps.
    """
    return df.pivot_table(*args, **kwargs)
