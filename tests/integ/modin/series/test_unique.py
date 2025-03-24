#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from decimal import Decimal
from typing import Any

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_values_equal, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


def _make_nan_interleaved_float_series():
    ser = native_pd.Series([1.2345] * 100)
    ser[::2] = np.nan
    return ser


HOMOGENEOUS_INPUT_DATA_FOR_SERIES = [
    [],
    [2, 1, 3, 3],
    [1, 1, 1, 1],
    [12.0, 11.999999, 11.999999],
    ["A", "A", "C", "C", "A"],
    [None, "A", None, "B"],
    _make_nan_interleaved_float_series(),
    native_pd.Series([1, 2, 2**63, 2**63], dtype=np.uint64),
    pytest.param(
        native_pd.Series([1, 2, -(2**63) - 1, -(2**64)]),
        marks=pytest.mark.xfail(
            reason="Represent overflow using float instead of integer",
        ),
    ),
    pytest.param(
        native_pd.Series([Decimal(1.5), Decimal(2**64 - 1)], dtype=object),
        marks=pytest.mark.xfail(
            reason="Represent Decimal using float instead of integer as pandas does not recognize it",
        ),
    ),
]


@pytest.mark.parametrize("input_data", iter(HOMOGENEOUS_INPUT_DATA_FOR_SERIES))
@sql_count_checker(query_count=1)
def test_unique_homogeneous_data(input_data: list[Any]):

    snowpark_pandas_series = pd.Series(input_data)
    native_series = native_pd.Series(input_data)

    eval_snowpark_pandas_result(
        snowpark_pandas_series,
        native_series,
        lambda s: s.unique(),
        comparator=assert_values_equal,
    )


@sql_count_checker(query_count=1)
def test_unique_heterogeneous_data():
    input_data = ["A", 12, 56, "A"]
    snowpark_pandas_series = pd.Series(input_data)
    native_series = native_pd.Series(input_data)

    eval_snowpark_pandas_result(
        snowpark_pandas_series,
        native_series,
        lambda s: s.unique(),
        comparator=assert_values_equal,
    )


# sorting only works for homogeneous data, e.g. < is not defined for int and str
@pytest.mark.parametrize("input_data", HOMOGENEOUS_INPUT_DATA_FOR_SERIES)
@sql_count_checker(query_count=1)
def test_unique_post_sort_values(input_data: list[Any]):
    def pipeline(series):
        sorted_series = series.sort_values()
        return sorted_series.unique()

    snowpark_pandas_series = pd.Series(input_data)
    native_series = native_pd.Series(input_data)

    eval_snowpark_pandas_result(
        snowpark_pandas_series, native_series, pipeline, comparator=assert_values_equal
    )


@pytest.mark.xfail(
    reason="SNOW-1524901: Wrong result when index and a data column have the same name",
    strict=True,
)
@sql_count_checker(query_count=0)
def test_index_unique_data_columns_should_not_affect_index_column():
    # The index column and data columns in a DataFrame object can have
    # the same column names. We need to ensure that the correct column is
    # picked during access to df.index and df.col_name, and the results
    # for df.col_name.unique() and df.index.unique() are different.
    # In this test, both the index and a data column have the name "A".
    # Check that they produce different results with unique.
    # TODO: SNOW-1524901: snow_df.A.unique() does not produce the correct result here.
    #  Right now an empty result is returned: []. This only occurs when the index and
    #  a data column in a DataFrame share the same name.
    # The expected result is [11, 22, 33, 44, 55].
    native_df = native_pd.DataFrame(
        {
            "B": [10, 20, 30, 40, 50],
            "A": [11, 22, 33, 44, 55],
            "C": [60, 70, 80, 90, 100],
        },
        index=native_pd.Index([5, 4, 3, 2, 1], name="A"),
    )
    snow_df = pd.DataFrame(native_df)
    assert snow_df.A.unique() == native_df.A.unique()
