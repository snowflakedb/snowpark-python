#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#


import math

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.data import RAW_NA_DF_SERIES_TEST_CASES
from tests.integ.modin.utils import (
    eval_snowpark_pandas_result,
    update_none_in_series_data_test_cases,
)
from tests.integ.utils.sql_counter import sql_count_checker


def run_series_test_helper_on_df_data(series_input, operation=lambda df: df.isna()):
    eval_snowpark_pandas_result(
        pd.Series(series_input), native_pd.Series(series_input), operation
    )


@pytest.mark.parametrize("series_input, test_case_name", RAW_NA_DF_SERIES_TEST_CASES)
@sql_count_checker(query_count=1)
def test_dataframe_isna_with_none_values(series_input, test_case_name):
    run_series_test_helper_on_df_data(series_input)


@pytest.mark.parametrize("series_input, test_case_name", RAW_NA_DF_SERIES_TEST_CASES)
@sql_count_checker(query_count=1)
def test_dataframe_notna_with_none_values(series_input, test_case_name):
    run_series_test_helper_on_df_data(series_input, operation=lambda df: df.notna())


@pytest.mark.parametrize("series_input, test_case_name", RAW_NA_DF_SERIES_TEST_CASES)
@sql_count_checker(query_count=1)
def test_dataframe_isnull_with_none_values(series_input, test_case_name):
    run_series_test_helper_on_df_data(series_input, operation=lambda df: df.isnull())


@pytest.mark.parametrize("series_input, test_case_name", RAW_NA_DF_SERIES_TEST_CASES)
@sql_count_checker(query_count=1)
def test_dataframe_notnull_with_none_values(series_input, test_case_name):
    run_series_test_helper_on_df_data(series_input, operation=lambda df: df.notnull())


@pytest.mark.parametrize(
    "series_input, test_case_name",
    update_none_in_series_data_test_cases(
        RAW_NA_DF_SERIES_TEST_CASES, np.nan, "np.nan"
    ),
)
@sql_count_checker(query_count=1)
def test_dataframe_isna_with_np_nan_values(series_input, test_case_name):
    run_series_test_helper_on_df_data(series_input)


@pytest.mark.parametrize(
    "series_input, test_case_name",
    update_none_in_series_data_test_cases(RAW_NA_DF_SERIES_TEST_CASES, pd.NA, "pd.NA"),
)
@sql_count_checker(query_count=1)
def test_dataframe_isna_with_pd_na_values(series_input, test_case_name):
    run_series_test_helper_on_df_data(series_input)


@pytest.mark.parametrize(
    "series_input, test_case_name",
    update_none_in_series_data_test_cases(
        RAW_NA_DF_SERIES_TEST_CASES, pd.NaT, "pd.NaT"
    ),
)
@sql_count_checker(query_count=1)
def test_dataframe_isna_with_pd_nat_values(series_input, test_case_name):
    run_series_test_helper_on_df_data(
        series_input,
    )


@pytest.mark.parametrize(
    "series_input, test_case_name",
    update_none_in_series_data_test_cases(
        RAW_NA_DF_SERIES_TEST_CASES, math.nan, "math.NA"
    ),
)
@sql_count_checker(query_count=1)
def test_dataframe_isna_with_math_na_values(series_input, test_case_name):
    run_series_test_helper_on_df_data(series_input)


@pytest.mark.parametrize(
    "series_input, test_case_name",
    update_none_in_series_data_test_cases(
        RAW_NA_DF_SERIES_TEST_CASES,
        pd.array([1, None], dtype=pd.Int64Dtype())[-1],
        "pd.array(None)",
    ),
)
@sql_count_checker(query_count=1)
def test_dataframe_isna_with_pd_array_none_values(series_input, test_case_name):
    run_series_test_helper_on_df_data(series_input)


@pytest.mark.parametrize(
    "series_input, test_case_name",
    update_none_in_series_data_test_cases(
        RAW_NA_DF_SERIES_TEST_CASES, float("nan"), "float(nan)"
    ),
)
@sql_count_checker(query_count=1)
def test_dataframe_isna_with_float_nan_values(series_input, test_case_name):
    run_series_test_helper_on_df_data(series_input)
