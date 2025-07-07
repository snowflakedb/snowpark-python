#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import modin.pandas as pd
import pytest
from modin.pandas import DataFrame, Series

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)
from snowflake.snowpark.modin.plugin._internal.utils import MODIN_IS_AT_LEAST_0_33_0


def setup_mock_qc() -> SnowflakeQueryCompiler:
    mock_query_compiler = mock.create_autospec(SnowflakeQueryCompiler)
    mock_query_compiler.columnarize.return_value = mock_query_compiler

    # Hybrid engine switching methods
    # Actual values don't matter since we don't do any computation in unit tests, and AutoSwitchBackend
    # is disabled in conftest.py.
    if MODIN_IS_AT_LEAST_0_33_0:
        mock_query_compiler.get_backend.return_value = "Snowflake"
        mock_query_compiler.move_to_cost.return_value = 0
        mock_query_compiler.move_to_me_cost.return_value = 0
        mock_query_compiler.max_cost.return_value = 1000
        mock_query_compiler.stay_cost.return_value = 0
        mock_query_compiler._max_shape.return_value = (10, 10)
    return mock_query_compiler


@pytest.mark.parametrize(
    "io_method, kwargs",
    [
        ["read_gbq", {"query": ""}],
        ["read_clipboard", {}],
        ["read_hdf", {"path_or_buf": ""}],
        ["read_stata", {"filepath_or_buffer": ""}],
        ["read_sql", {"sql": "", "con": ""}],
        ["read_fwf", {"filepath_or_buffer": ""}],
        ["read_sql_table", {"table_name": "", "con": ""}],
        ["read_sql_query", {"sql": "", "con": ""}],
        ["to_pickle", {"filepath_or_buffer": "", "obj": ""}],
        ["read_spss", {"path": ""}],
        ["json_normalize", {"data": ""}],
    ],
)
def test_unsupported_io(io_method, kwargs):
    with pytest.raises(NotImplementedError):
        getattr(pd, io_method)(**kwargs)


@pytest.mark.parametrize(
    "general_method, kwargs",
    [
        ["merge_ordered", {"left": "", "right": ""}],
        ["value_counts", {"values": ""}],
        ["lreshape", {"data": "", "groups": ""}],
        ["wide_to_long", {"df": "", "stubnames": "", "i": "", "j": ""}],
    ],
)
def test_unsupported_general(general_method, kwargs):
    with pytest.raises(NotImplementedError):
        getattr(pd, general_method)(**kwargs)


@pytest.mark.parametrize(
    "df_method, kwargs",
    [
        ["asof", {"where": ""}],
        ["at_time", {"time": ""}],
        ["between_time", {"start_time": "", "end_time": ""}],
        ["bool", {}],
        ["boxplot", {}],
        ["clip", {}],
        ["combine", {"other": "", "func": ""}],
        ["combine_first", {"other": ""}],
        ["corrwith", {"other": ""}],
        ["cov", {}],
        ["dot", {"other": ""}],
        ["droplevel", {"level": ""}],
        ["eval", {"expr": "xxx"}],
        ["ewm", {}],
        ["clip", {}],
        ["combine", {"other": "", "func": ""}],
        ["combine_first", {"other": ""}],
        ["filter", {}],
        ["hist", {}],
        ["infer_objects", {}],
        ["interpolate", {}],
        ["isetitem", {"loc": "", "value": ""}],
        ["kurt", {}],
        ["kurtosis", {}],
        ["mode", {}],
        ["pipe", {"func": ""}],
        ["prod", {}],
        ["product", {}],
        ["query", {"expr": ""}],
        ["reindex_like", {"other": ""}],
        ["reorder_levels", {"order": ""}],
        ["sem", {}],
        ["set_flags", {}],
        ["swapaxes", {"axis1": "", "axis2": ""}],
        ["swaplevel", {}],
        ["to_clipboard", {}],
        ["to_feather", {"path": ""}],
        ["to_gbq", {"destination_table": ""}],
        ["to_hdf", {"path_or_buf": "", "key": ""}],
        ["to_json", {}],
        ["to_latex", {}],
        ["to_markdown", {}],
        ["to_orc", {}],
        ["to_parquet", {}],
        ["to_period", {}],
        ["to_pickle", {"path": ""}],
        ["to_records", {}],
        ["to_sql", {"name": "", "con": ""}],
        ["to_stata", {"path": ""}],
        ["to_timestamp", {}],
        ["to_xarray", {}],
        ["to_xml", {}],
        ["transform", {"func": [[], {}]}],
        ["truncate", {}],
        ["xs", {"key": ""}],
    ],
)
def test_unsupported_df(df_method, kwargs):
    mock_df = DataFrame(query_compiler=setup_mock_qc())

    with pytest.raises(NotImplementedError):
        getattr(mock_df, df_method)(**kwargs)


@pytest.mark.parametrize(
    "series_method, kwargs",
    [
        ["argmax", {}],
        ["argmin", {}],
        ["argsort", {}],
        ["array", {}],
        ["asof", {"where": ""}],
        ["at_time", {"time": ""}],
        ["autocorr", {}],
        ["between_time", {"start_time": "", "end_time": ""}],
        ["bool", {}],
        ["clip", {}],
        ["combine", {"other": "", "func": ""}],
        ["combine_first", {"other": ""}],
        ["corr", {"other": ""}],
        ["cov", {"other": ""}],
        ["divmod", {"other": ""}],
        ["dot", {"other": ""}],
        ["droplevel", {"level": ""}],
        ["ewm", {}],
        ["explode", {}],
        ["factorize", {}],
        ["filter", {}],
        ["infer_objects", {}],
        ["interpolate", {}],
        ["item", {}],
        ["kurt", {}],
        ["kurtosis", {}],
        ["mode", {}],
        ["nbytes", {}],
        ["pipe", {"func": ""}],
        ["prod", {}],
        ["ravel", {}],
        ["reindex_like", {"other": ""}],
        ["reorder_levels", {"order": ""}],
        ["repeat", {"repeats": ""}],
        ["rdivmod", {"other": ""}],
        ["searchsorted", {"value": ""}],
        ["sem", {}],
        ["set_flags", {}],
        ["swapaxes", {"axis1": "", "axis2": ""}],
        ["swaplevel", {}],
        ["to_clipboard", {}],
        ["to_hdf", {"path_or_buf": "", "key": ""}],
        ["to_json", {}],
        ["to_latex", {}],
        ["to_markdown", {}],
        ["to_period", {}],
        ["to_pickle", {"path": ""}],
        ["to_sql", {"name": "", "con": ""}],
        ["to_timestamp", {}],
        ["to_xarray", {}],
        ["transform", {"func": ""}],
        ["truncate", {}],
        ["view", {}],
        ["xs", {"key": ""}],
    ],
)
def test_unsupported_series(series_method, kwargs):
    mock_df = Series(query_compiler=setup_mock_qc())

    with pytest.raises(NotImplementedError):
        getattr(mock_df, series_method)(**kwargs)
