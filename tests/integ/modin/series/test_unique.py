#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from typing import Any

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import assert_values_equal, eval_snowpark_pandas_result


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
    pytest.param(
        native_pd.Series([1, 2, 2**63, 2**63], dtype=np.uint64),
        marks=pytest.mark.xfail(
            reason="SNOW-1356685: Dtype with unsigned int results in precision error"
        ),
    ),
    _make_nan_interleaved_float_series(),
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
