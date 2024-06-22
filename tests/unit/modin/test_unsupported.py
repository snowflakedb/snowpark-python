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
        ["merge_asof", {"left": "", "right": ""}],
        ["pivot", {"data": ""}],
        ["value_counts", {"values": ""}],
        ["crosstab", {"index": "", "columns": ""}],
        ["lreshape", {"data": "", "groups": ""}],
        ["wide_to_long", {"df": "", "stubnames": "", "i": "", "j": ""}],
        ["to_timedelta", {"arg": ""}],
    ],
)
def test_unsupported_general(general_method, kwargs):
    with pytest.raises(NotImplementedError):
        getattr(pd, general_method)(**kwargs)


@pytest.mark.parametrize(
    "df_method, kwargs",
    [
        ["align", {"other": ""}],
        ["asfreq", {"freq": ""}],
        ["asof", {"where": ""}],
        ["at", {}],
        ["at_time", {"time": ""}],
        ["backfill", {}],
        ["between_time", {"start_time": "", "end_time": ""}],
        ["bfill", {}],
        ["bool", {}],
        ["boxplot", {}],
        ["clip", {}],
        ["combine", {"other": "", "func": ""}],
        ["combine_first", {"other": ""}],
        ["compare", {"other": ""}],
        ["corr", {}],
        ["corrwith", {"other": ""}],
        ["cov", {}],
        ["dot", {"other": ""}],
        ["droplevel", {"level": ""}],
        ["eval", {"expr": "xxx"}],
        ["ewm", {}],
        ["explode", {"column": ""}],
        ["filter", {}],
        ["from_dict", {"data": ""}],
        ["from_records", {"data": ""}],
        ["hist", {}],
        ["iat", {}],
        ["infer_objects", {}],
        ["interpolate", {}],
        ["isetitem", {"loc": "", "value": ""}],
        ["kurt", {}],
        ["kurtosis", {}],
        ["mode", {}],
        ["pipe", {"func": ""}],
        ["pivot", {}],
        ["plot", {}],
        ["pop", {"item": ""}],
        ["prod", {}],
        ["product", {}],
        ["query", {"expr": ""}],
        ["reindex_like", {"other": ""}],
        ["reorder_levels", {"order": ""}],
        ["sem", {}],
        ["set_flags", {}],
        ["stack", {}],
        ["style", {}],
        ["swapaxes", {"axis1": "", "axis2": ""}],
        ["swaplevel", {}],
        ["to_clipboard", {}],
        ["to_csv", {}],
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
        ["transform", {"func": [[], {}]}],
        ["truncate", {}],
        ["tz_convert", {"tz": ""}],
        ["tz_localize", {"tz": ""}],
        ["unstack", {}],
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
    "df_method, kwargs",
    [["items", {}], ["iteritems", {}]],
)
def test_unsupported_df_generator(df_method, kwargs):
    mock_query_compiler = mock.create_autospec(SnowflakeQueryCompiler)
    mock_query_compiler.columnarize.return_value = mock_query_compiler
    mock_df = DataFrame(query_compiler=mock_query_compiler)

    with pytest.raises(NotImplementedError):
        for x in getattr(mock_df, df_method)(**kwargs):
            x + 1


@pytest.mark.parametrize(
    "series_method, kwargs",
    [
        ["align", {"other": ""}],
        ["argmax", {}],
        ["argmin", {}],
        ["argsort", {}],
        ["array", {}],
        ["asfreq", {"freq": ""}],
        ["asof", {"where": ""}],
        ["at", {}],
        ["at_time", {"time": ""}],
        ["autocorr", {}],
        ["backfill", {}],
        ["between", {"left": "", "right": ""}],
        ["between_time", {"start_time": "", "end_time": ""}],
        ["bfill", {}],
        ["bool", {}],
        ["clip", {}],
        ["combine", {"other": "", "func": ""}],
        ["combine_first", {"other": ""}],
        ["compare", {"other": ""}],
        ["corr", {"other": ""}],
        ["cov", {"other": ""}],
        ["divmod", {"other": ""}],
        ["dot", {"other": ""}],
        ["droplevel", {"level": ""}],
        ["ewm", {}],
        ["explode", {}],
        ["factorize", {}],
        ["filter", {}],
        ["hist", {}],
        ["iat", {}],
        ["infer_objects", {}],
        ["interpolate", {}],
        ["item", {}],
        ["kurt", {}],
        ["kurtosis", {}],
        ["mode", {}],
        ["nbytes", {}],
        ["pipe", {"func": ""}],
        ["plot", {}],
        ["pop", {"item": ""}],
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
        ["to_csv", {}],
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
        ["transform", {"func": ""}],
        ["truncate", {}],
        ["tz_convert", {"tz": ""}],
        ["tz_localize", {"tz": ""}],
        ["unstack", {}],
        ["view", {}],
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
