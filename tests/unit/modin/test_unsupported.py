#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from unittest import mock

import modin.pandas as pd
import pytest
from modin.pandas import DataFrame, Series

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)


@pytest.mark.parametrize(
    "io_method, kwargs",
    [
        ["read_xml", {"path_or_buffer": ""}],
        ["read_gbq", {"query": ""}],
        ["read_html", {"io": ""}],
        ["read_clipboard", {}],
        ["read_hdf", {"path_or_buf": ""}],
        ["read_feather", {"path": ""}],
        ["read_stata", {"filepath_or_buffer": ""}],
        ["read_sas", {"filepath_or_buffer": ""}],
        ["read_pickle", {"filepath_or_buffer": ""}],
        ["read_sql", {"sql": "", "con": ""}],
        ["read_fwf", {"filepath_or_buffer": ""}],
        ["read_sql_table", {"table_name": "", "con": ""}],
        ["read_sql_query", {"sql": "", "con": ""}],
        ["to_pickle", {"filepath_or_buffer": "", "obj": ""}],
        ["read_spss", {"path": ""}],
        ["json_normalize", {"data": ""}],
        ["read_orc", {"path": ""}],
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
        ["bool", {}],
        ["boxplot", {}],
        ["corrwith", {"other": ""}],
        ["cov", {}],
        ["dot", {"other": ""}],
        ["droplevel", {"level": ""}],
        ["eval", {"expr": "xxx"}],
        ["ewm", {}],
        ["filter", {}],
        ["from_dict", {"data": ""}],
        ["from_records", {"data": ""}],
        ["hist", {}],
        ["interpolate", {}],
        ["isetitem", {"loc": "", "value": ""}],
        ["pipe", {"func": ""}],
        ["pop", {"item": ""}],
        ["prod", {}],
        ["product", {}],
        ["query", {"expr": ""}],
        ["reorder_levels", {"order": ""}],
        ["set_flags", {}],
        ["style", {}],
        ["swapaxes", {"axis1": "", "axis2": ""}],
        ["swaplevel", {}],
        ["to_clipboard", {}],
        ["to_excel", {"excel_writer": ""}],
        ["to_feather", {"path": ""}],
        ["to_gbq", {"destination_table": ""}],
        ["to_hdf", {"path_or_buf": "", "key": ""}],
        ["to_html", {}],
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
        ["to_string", {}],
        ["to_timestamp", {}],
        ["to_xarray", {}],
        ["to_xml", {}],
        ["truncate", {}],
        ["xs", {"key": ""}],
        ["__dataframe__", {}],
    ],
)
def test_unsupported_df(df_method, kwargs):
    mock_query_compiler = mock.create_autospec(SnowflakeQueryCompiler)
    mock_query_compiler.columnarize.return_value = mock_query_compiler
    mock_df = DataFrame(query_compiler=mock_query_compiler)

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
        ["autocorr", {}],
        ["between", {"left": "", "right": ""}],
        ["bool", {}],
        ["corr", {"other": ""}],
        ["cov", {"other": ""}],
        ["divmod", {"other": ""}],
        ["dot", {"other": ""}],
        ["droplevel", {"level": ""}],
        ["ewm", {}],
        ["factorize", {}],
        ["filter", {}],
        ["hist", {}],
        ["interpolate", {}],
        ["item", {}],
        ["nbytes", {}],
        ["pipe", {"func": ""}],
        ["pop", {"item": ""}],
        ["prod", {}],
        ["ravel", {}],
        ["reorder_levels", {"order": ""}],
        ["searchsorted", {"value": ""}],
        ["set_flags", {}],
        ["swapaxes", {"axis1": "", "axis2": ""}],
        ["swaplevel", {}],
        ["to_clipboard", {}],
        ["to_excel", {"excel_writer": ""}],
        ["to_hdf", {"path_or_buf": "", "key": ""}],
        ["to_json", {}],
        ["to_latex", {}],
        ["to_markdown", {}],
        ["to_period", {}],
        ["to_pickle", {"path": ""}],
        ["to_sql", {"name": "", "con": ""}],
        ["to_string", {}],
        ["to_timestamp", {}],
        ["to_xarray", {}],
        ["truncate", {}],
        ["xs", {"key": ""}],
    ],
)
def test_unsupported_series(series_method, kwargs):
    mock_query_compiler = mock.create_autospec(SnowflakeQueryCompiler)
    mock_query_compiler.columnarize.return_value = mock_query_compiler
    mock_df = Series(query_compiler=mock_query_compiler)

    with pytest.raises(NotImplementedError):
        getattr(mock_df, series_method)(**kwargs)


@pytest.mark.parametrize(
    "series_method, kwargs",
    [["items", {}]],
)
def test_unsupported_series_generator(series_method, kwargs):
    mock_query_compiler = mock.create_autospec(SnowflakeQueryCompiler)
    mock_query_compiler.columnarize.return_value = mock_query_compiler
    mock_df = Series(query_compiler=mock_query_compiler)

    with pytest.raises(NotImplementedError):
        for x in getattr(mock_df, series_method)(**kwargs):
            x + 1
