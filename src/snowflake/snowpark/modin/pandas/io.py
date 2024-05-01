#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

# Licensed to Modin Development Team under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional information regarding
# copyright ownership.  The Modin Development Team licenses this file to you under the
# Apache License, Version 2.0 (the "License"); you may not use this file except in
# compliance with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific language
# governing permissions and limitations under the License.

# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the Modin project, under the Apache License,
# Version 2.0.

"""
Implement I/O public API as pandas does.

Almost all docstrings for public and magic methods should be inherited from pandas
for better maintability.
Manually add documentation for methods which are not presented in pandas.
"""

from __future__ import annotations

import csv
import inspect
import pathlib
import pickle
from collections import OrderedDict
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    AnyStr,
    Callable,
    Hashable,
    Iterable,
    Iterator,
    Literal,
    Pattern,
    Sequence,
)

import numpy as np
import pandas
from pandas._libs.lib import NoDefault, no_default
from pandas._typing import (
    CompressionOptions,
    ConvertersArg,
    CSVEngine,
    DtypeArg,
    DtypeBackend,
    FilePath,
    IndexLabel,
    IntStrT,
    ParseDatesArg,
    ReadBuffer,
    ReadCsvBuffer,
    StorageOptions,
    XMLParsers,
)
from pandas.io.parsers import TextFileReader
from pandas.io.parsers.readers import _c_parser_defaults

# add this line to enable doc tests to run
from snowflake.snowpark.modin import pandas as pd  # noqa: F401
from snowflake.snowpark.modin.config import ExperimentalNumPyAPI
from snowflake.snowpark.modin.plugin._internal.telemetry import (
    snowpark_pandas_telemetry_standalone_function_decorator,
)
from snowflake.snowpark.modin.plugin.utils.error_message import (
    ErrorMessage,
    pandas_module_level_function_not_implemented,
)
from snowflake.snowpark.modin.utils import (
    SupportsPrivateToNumPy,
    SupportsPrivateToPandas,
    SupportsPublicToNumPy,
    _inherit_docstrings,
    classproperty,
    expanduser_path_arg,
)

# below logic is to handle circular imports without errors
if TYPE_CHECKING:  # pragma: no cover
    from .dataframe import DataFrame
    from .series import Series

# TODO: SNOW-1265551: add inherit_docstrings decorators once docstring overrides are available


class ModinObjects:
    """Lazily import Modin classes and provide an access to them."""

    _dataframe = None

    @classproperty
    def DataFrame(cls):
        """Get ``modin.pandas.DataFrame`` class."""
        if cls._dataframe is None:
            from .dataframe import DataFrame

            cls._dataframe = DataFrame
        return cls._dataframe


def _read(
    **kwargs,
):  # pragma: no cover: our frontend currently overrides read_csv, so this is unused
    """
    Read csv file from local disk.

    Parameters
    ----------
    **kwargs : dict
        Keyword arguments in pandas.read_csv.

    Returns
    -------
    modin.pandas.DataFrame
    """
    from snowflake.snowpark.modin.core.execution.dispatching.factories.dispatcher import (
        FactoryDispatcher,
    )

    squeeze = kwargs.pop(
        "squeeze", False
    )  # pragma: no cover: this is a removed argument and should be removed upstream
    pd_obj = FactoryDispatcher.read_csv(**kwargs)
    # This happens when `read_csv` returns a TextFileReader object for iterating through
    if isinstance(pd_obj, TextFileReader):  # pragma: no cover
        reader = pd_obj.read
        pd_obj.read = lambda *args, **kwargs: ModinObjects.DataFrame(
            query_compiler=reader(*args, **kwargs)
        )
        return pd_obj
    result = ModinObjects.DataFrame(query_compiler=pd_obj)
    if squeeze:
        return result.squeeze(axis=1)
    return result


# TODO: SNOW-1265551: add inherit_docstrings decorators once docstring overrides are available
@expanduser_path_arg("path_or_buffer")
@snowpark_pandas_telemetry_standalone_function_decorator
@pandas_module_level_function_not_implemented()
def read_xml(
    path_or_buffer: FilePath | ReadBuffer[bytes] | ReadBuffer[str],
    *,
    xpath: str = "./*",
    namespaces: dict[str, str] | None = None,
    elems_only: bool = False,
    attrs_only: bool = False,
    names: Sequence[str] | None = None,
    dtype: DtypeArg | None = None,
    converters: ConvertersArg | None = None,
    parse_dates: ParseDatesArg | None = None,
    encoding: str | None = "utf-8",
    parser: XMLParsers = "lxml",
    stylesheet: FilePath | ReadBuffer[bytes] | ReadBuffer[str] | None = None,
    iterparse: dict[str, list[str]] | None = None,
    compression: CompressionOptions = "infer",
    storage_options: StorageOptions = None,
    dtype_backend: DtypeBackend | NoDefault = no_default,
) -> DataFrame:
    # TODO(https://github.com/modin-project/modin/issues/7104):
    # modin needs to remove defaults to pandas at API layer
    pass


@_inherit_docstrings(pandas.read_csv, apilink="pandas.read_csv")
@expanduser_path_arg("filepath_or_buffer")
@snowpark_pandas_telemetry_standalone_function_decorator
def read_csv(
    filepath_or_buffer: FilePath | ReadCsvBuffer[bytes] | ReadCsvBuffer[str],
    *,
    sep: str | None | NoDefault = no_default,
    delimiter: str | None | NoDefault = None,
    # Column and Index Locations and Names
    header: int | Sequence[int] | None | Literal["infer"] = "infer",
    names: Sequence[Hashable] | None | NoDefault = no_default,
    index_col: IndexLabel | Literal[False] | None = None,
    usecols=None,
    # General Parsing Configuration
    dtype: DtypeArg | None = None,
    engine: CSVEngine | None = None,
    converters=None,
    true_values=None,
    false_values=None,
    skipinitialspace: bool = False,
    skiprows=None,
    skipfooter: int = 0,
    nrows: int | None = None,
    # NA and Missing Data Handling
    na_values=None,
    keep_default_na: bool = True,
    na_filter: bool = True,
    verbose: bool = no_default,
    skip_blank_lines: bool = True,
    # Datetime Handling
    parse_dates=None,
    infer_datetime_format: bool = no_default,
    keep_date_col: bool = no_default,
    date_parser=no_default,
    date_format=None,
    dayfirst: bool = False,
    cache_dates: bool = True,
    # Iteration
    iterator: bool = False,
    chunksize: int | None = None,
    # Quoting, Compression, and File Format
    compression: CompressionOptions = "infer",
    thousands: str | None = None,
    decimal: str = ".",
    lineterminator: str | None = None,
    quotechar: str = '"',
    quoting: int = csv.QUOTE_MINIMAL,
    doublequote: bool = True,
    escapechar: str | None = None,
    comment: str | None = None,
    encoding: str | None = None,
    encoding_errors: str | None = "strict",
    dialect: str | csv.Dialect | None = None,
    # Error Handling
    on_bad_lines="error",
    # Internal
    delim_whitespace: bool = no_default,
    low_memory=_c_parser_defaults["low_memory"],
    memory_map: bool = False,
    float_precision: Literal["high", "legacy"] | None = None,
    storage_options: StorageOptions = None,
    dtype_backend: DtypeBackend | NoDefault = no_default,
) -> DataFrame | TextFileReader:  # pragma: no cover: this function is overridden by plugin/pd_overrides.py
    # ISSUE #2408: parse parameter shared with pandas read_csv and read_table and update with provided args
    _pd_read_csv_signature = {
        val.name for val in inspect.signature(pandas.read_csv).parameters.values()
    }
    _, _, _, f_locals = inspect.getargvalues(inspect.currentframe())
    kwargs = {k: v for k, v in f_locals.items() if k in _pd_read_csv_signature}
    return _read(**kwargs)


@_inherit_docstrings(pandas.read_table, apilink="pandas.read_table")
@snowpark_pandas_telemetry_standalone_function_decorator
@expanduser_path_arg("filepath_or_buffer")
def read_table(
    filepath_or_buffer: FilePath | ReadCsvBuffer[bytes] | ReadCsvBuffer[str],
    *,
    sep: str | None | NoDefault = no_default,
    delimiter: str | None | NoDefault = None,
    # Column and Index Locations and Names
    header: int | Sequence[int] | None | Literal["infer"] = "infer",
    names: Sequence[Hashable] | None | NoDefault = no_default,
    index_col: IndexLabel | Literal[False] | None = None,
    usecols=None,
    # General Parsing Configuration
    dtype: DtypeArg | None = None,
    engine: CSVEngine | None = None,
    converters=None,
    true_values=None,
    false_values=None,
    skipinitialspace: bool = False,
    skiprows=None,
    skipfooter: int = 0,
    nrows: int | None = None,
    # NA and Missing Data Handling
    na_values=None,
    keep_default_na: bool = True,
    na_filter: bool = True,
    verbose: bool = False,
    skip_blank_lines: bool = True,
    # Datetime Handling
    parse_dates=False,
    infer_datetime_format: bool = no_default,
    keep_date_col: bool = False,
    date_parser=no_default,
    date_format: str = None,
    dayfirst: bool = False,
    cache_dates: bool = True,
    # Iteration
    iterator: bool = False,
    chunksize: int | None = None,
    # Quoting, Compression, and File Format
    compression: CompressionOptions = "infer",
    thousands: str | None = None,
    decimal: str = ".",
    lineterminator: str | None = None,
    quotechar: str = '"',
    quoting: int = csv.QUOTE_MINIMAL,
    doublequote: bool = True,
    escapechar: str | None = None,
    comment: str | None = None,
    encoding: str | None = None,
    encoding_errors: str | None = "strict",
    dialect: str | csv.Dialect | None = None,
    # Error Handling
    on_bad_lines="error",
    # Internal
    delim_whitespace=False,
    low_memory=_c_parser_defaults["low_memory"],
    memory_map: bool = False,
    float_precision: str | None = None,
    storage_options: StorageOptions = None,
    dtype_backend: DtypeBackend | NoDefault = no_default,
) -> DataFrame | TextFileReader:  # pragma: no cover
    # ISSUE #2408: parse parameter shared with pandas read_csv and read_table and update with provided args
    _pd_read_table_signature = {
        val.name for val in inspect.signature(pandas.read_table).parameters.values()
    }
    _, _, _, f_locals = inspect.getargvalues(inspect.currentframe())
    if f_locals.get("sep", sep) is False or f_locals.get("sep", sep) is no_default:
        f_locals["sep"] = "\t"
    kwargs = {k: v for k, v in f_locals.items() if k in _pd_read_table_signature}
    return _read(**kwargs)


# TODO: SNOW-1265551: add inherit_docstrings decorators once docstring overrides are available
@snowpark_pandas_telemetry_standalone_function_decorator
@expanduser_path_arg("path")
def read_parquet(
    path,
    engine: str = "auto",
    columns: list[str] | None = None,
    storage_options: StorageOptions = None,
    use_nullable_dtypes: bool = no_default,
    dtype_backend=no_default,
    filesystem=None,
    filters=None,
    **kwargs,
) -> DataFrame:  # pragma: no cover: this function is overridden by plugin/pd_overrides.py
    from snowflake.snowpark.modin.core.execution.dispatching.factories.dispatcher import (
        FactoryDispatcher,
    )

    if engine == "fastparquet" and dtype_backend is not no_default:
        raise ValueError(
            "The 'dtype_backend' argument is not supported for the fastparquet engine"
        )

    return ModinObjects.DataFrame(
        query_compiler=FactoryDispatcher.read_parquet(
            path=path,
            engine=engine,
            columns=columns,
            storage_options=storage_options,
            use_nullable_dtypes=use_nullable_dtypes,
            dtype_backend=dtype_backend,
            filesystem=filesystem,
            filters=filters,
            **kwargs,
        )
    )


# TODO: SNOW-1265551: add inherit_docstrings decorators once docstring overrides are available
@snowpark_pandas_telemetry_standalone_function_decorator
@expanduser_path_arg("path_or_buf")
def read_json(
    path_or_buf,
    *,
    orient: str | None = None,
    typ: Literal["frame", "series"] = "frame",
    dtype: DtypeArg | None = None,
    convert_axes=None,
    convert_dates: bool | list[str] = True,
    keep_default_dates: bool = True,
    precise_float: bool = False,
    date_unit: str | None = None,
    encoding: str | None = None,
    encoding_errors: str | None = "strict",
    lines: bool = False,
    chunksize: int | None = None,
    compression: CompressionOptions = "infer",
    nrows: int | None = None,
    storage_options: StorageOptions = None,
    dtype_backend: DtypeBackend | NoDefault = no_default,
    engine="ujson",
) -> DataFrame | Series | pandas.io.json._json.JsonReader:  # pragma: no cover: this function is overridden by plugin/pd_overrides.py
    _, _, _, kwargs = inspect.getargvalues(inspect.currentframe())

    from snowflake.snowpark.modin.core.execution.dispatching.factories.dispatcher import (
        FactoryDispatcher,
    )

    return ModinObjects.DataFrame(query_compiler=FactoryDispatcher.read_json(**kwargs))


@_inherit_docstrings(pandas.read_gbq, apilink="pandas.read_gbq")
@snowpark_pandas_telemetry_standalone_function_decorator
def read_gbq(
    query: str,
    project_id: str | None = None,
    index_col: str | None = None,
    col_order: list[str] | None = None,
    reauth: bool = False,
    auth_local_webserver: bool = True,
    dialect: str | None = None,
    location: str | None = None,
    configuration: dict[str, Any] | None = None,
    credentials=None,
    use_bqstorage_api: bool | None = None,
    max_results: int | None = None,
    progress_bar_type: str | None = None,
) -> DataFrame:
    _, _, _, kwargs = inspect.getargvalues(inspect.currentframe())
    kwargs.update(kwargs.pop("kwargs", {}))

    from snowflake.snowpark.modin.core.execution.dispatching.factories.dispatcher import (
        FactoryDispatcher,
    )

    return ModinObjects.DataFrame(query_compiler=FactoryDispatcher.read_gbq(**kwargs))


@_inherit_docstrings(pandas.read_html, apilink="pandas.read_html")
@snowpark_pandas_telemetry_standalone_function_decorator
@expanduser_path_arg("io")
def read_html(
    io,
    *,
    match: str | Pattern = ".+",
    flavor: str | None = None,
    header: int | Sequence[int] | None = None,
    index_col: int | Sequence[int] | None = None,
    skiprows: int | Sequence[int] | slice | None = None,
    attrs: dict[str, str] | None = None,
    parse_dates: bool = False,
    thousands: str | None = ",",
    encoding: str | None = None,
    decimal: str = ".",
    converters: dict | None = None,
    na_values: Iterable[object] | None = None,
    keep_default_na: bool = True,
    displayed_only: bool = True,
    extract_links: Literal[None, "header", "footer", "body", "all"] = None,
    dtype_backend: DtypeBackend | NoDefault = no_default,
    storage_options: StorageOptions = None,
) -> list[DataFrame]:  # pragma: no cover  # noqa: PR01, RT01, D200
    """
    Read HTML tables into a ``DataFrame`` object.
    """
    _, _, _, kwargs = inspect.getargvalues(inspect.currentframe())

    from snowflake.snowpark.modin.core.execution.dispatching.factories.dispatcher import (
        FactoryDispatcher,
    )

    qcs = FactoryDispatcher.read_html(**kwargs)
    return [ModinObjects.DataFrame(query_compiler=qc) for qc in qcs]


@_inherit_docstrings(pandas.read_clipboard, apilink="pandas.read_clipboard")
@snowpark_pandas_telemetry_standalone_function_decorator
def read_clipboard(
    sep=r"\s+",
    dtype_backend: DtypeBackend | NoDefault = no_default,
    **kwargs,
):  # pragma: no cover  # noqa: PR01, RT01, D200
    """
    Read text from clipboard and pass to read_csv.
    """
    _, _, _, kwargs = inspect.getargvalues(inspect.currentframe())
    kwargs.update(kwargs.pop("kwargs", {}))

    from snowflake.snowpark.modin.core.execution.dispatching.factories.dispatcher import (
        FactoryDispatcher,
    )

    return ModinObjects.DataFrame(
        query_compiler=FactoryDispatcher.read_clipboard(**kwargs)
    )


@_inherit_docstrings(pandas.read_excel, apilink="pandas.read_excel")
@snowpark_pandas_telemetry_standalone_function_decorator
@expanduser_path_arg("io")
def read_excel(
    io,
    sheet_name: str | int | list[IntStrT] | None = 0,
    *,
    header: int | Sequence[int] | None = 0,
    names: list[str] | None = None,
    index_col: int | Sequence[int] | None = None,
    usecols: int
    | str
    | Sequence[int]
    | Sequence[str]
    | Callable[[str], bool]
    | None = None,
    dtype: DtypeArg | None = None,
    engine: Literal[("xlrd", "openpyxl", "odf", "pyxlsb")] | None = None,
    converters: dict[str, Callable] | dict[int, Callable] | None = None,
    true_values: Iterable[Hashable] | None = None,
    false_values: Iterable[Hashable] | None = None,
    skiprows: Sequence[int] | int | Callable[[int], object] | None = None,
    nrows: int | None = None,
    na_values=None,
    keep_default_na: bool = True,
    na_filter: bool = True,
    verbose: bool = False,
    parse_dates: list | dict | bool = False,
    date_parser: Callable | NoDefault = no_default,
    date_format=None,
    thousands: str | None = None,
    decimal: str = ".",
    comment: str | None = None,
    skipfooter: int = 0,
    storage_options: StorageOptions = None,
    dtype_backend: DtypeBackend | NoDefault = no_default,
    engine_kwargs: dict | None = None,
) -> DataFrame | dict[IntStrT, DataFrame]:  # pragma: no cover
    _, _, _, kwargs = inspect.getargvalues(inspect.currentframe())

    from snowflake.snowpark.modin.core.execution.dispatching.factories.dispatcher import (
        FactoryDispatcher,
    )

    intermediate = FactoryDispatcher.read_excel(**kwargs)
    if isinstance(intermediate, (OrderedDict, dict)):
        parsed = type(intermediate)()
        for key in intermediate.keys():
            parsed[key] = ModinObjects.DataFrame(query_compiler=intermediate.get(key))
        return parsed
    else:
        return ModinObjects.DataFrame(query_compiler=intermediate)


@_inherit_docstrings(pandas.read_hdf, apilink="pandas.read_hdf")
@snowpark_pandas_telemetry_standalone_function_decorator
@expanduser_path_arg("path_or_buf")
def read_hdf(
    path_or_buf,
    key=None,
    mode: str = "r",
    errors: str = "strict",
    where=None,
    start: int | None = None,
    stop: int | None = None,
    columns=None,
    iterator=False,
    chunksize: int | None = None,
    **kwargs,
):  # noqa: PR01, RT01, D200
    """
    Read data from the store into DataFrame.
    """
    _, _, _, kwargs = inspect.getargvalues(inspect.currentframe())
    kwargs.update(kwargs.pop("kwargs", {}))

    from snowflake.snowpark.modin.core.execution.dispatching.factories.dispatcher import (
        FactoryDispatcher,
    )

    return ModinObjects.DataFrame(query_compiler=FactoryDispatcher.read_hdf(**kwargs))


@_inherit_docstrings(pandas.read_feather, apilink="pandas.read_feather")
@snowpark_pandas_telemetry_standalone_function_decorator
@expanduser_path_arg("path")
def read_feather(
    path,
    columns: Sequence[Hashable] | None = None,
    use_threads: bool = True,
    storage_options: StorageOptions = None,
    dtype_backend: DtypeBackend | NoDefault = no_default,
):
    _, _, _, kwargs = inspect.getargvalues(inspect.currentframe())

    from snowflake.snowpark.modin.core.execution.dispatching.factories.dispatcher import (
        FactoryDispatcher,
    )

    return ModinObjects.DataFrame(
        query_compiler=FactoryDispatcher.read_feather(**kwargs)
    )


@_inherit_docstrings(pandas.read_stata)
@snowpark_pandas_telemetry_standalone_function_decorator
@expanduser_path_arg("filepath_or_buffer")
def read_stata(
    filepath_or_buffer,
    *,
    convert_dates: bool = True,
    convert_categoricals: bool = True,
    index_col: str | None = None,
    convert_missing: bool = False,
    preserve_dtypes: bool = True,
    columns: Sequence[str] | None = None,
    order_categoricals: bool = True,
    chunksize: int | None = None,
    iterator: bool = False,
    compression: CompressionOptions = "infer",
    storage_options: StorageOptions = None,
) -> DataFrame | pandas.io.stata.StataReader:
    _, _, _, kwargs = inspect.getargvalues(inspect.currentframe())

    from snowflake.snowpark.modin.core.execution.dispatching.factories.dispatcher import (
        FactoryDispatcher,
    )

    return ModinObjects.DataFrame(query_compiler=FactoryDispatcher.read_stata(**kwargs))


@_inherit_docstrings(pandas.read_sas, apilink="pandas.read_sas")
@snowpark_pandas_telemetry_standalone_function_decorator
@expanduser_path_arg("filepath_or_buffer")
def read_sas(
    filepath_or_buffer,
    *,
    format: str | None = None,
    index: Hashable | None = None,
    encoding: str | None = None,
    chunksize: int | None = None,
    iterator: bool = False,
    compression: CompressionOptions = "infer",
) -> DataFrame | pandas.io.sas.sasreader.ReaderBase:  # noqa: PR01, RT01, D200
    """
    Read SAS files stored as either XPORT or SAS7BDAT format files.
    """
    from snowflake.snowpark.modin.core.execution.dispatching.factories.dispatcher import (
        FactoryDispatcher,
    )

    return ModinObjects.DataFrame(
        query_compiler=FactoryDispatcher.read_sas(
            filepath_or_buffer=filepath_or_buffer,
            format=format,
            index=index,
            encoding=encoding,
            chunksize=chunksize,
            iterator=iterator,
            compression=compression,
        )
    )


@_inherit_docstrings(pandas.read_pickle, apilink="pandas.read_pickle")
@snowpark_pandas_telemetry_standalone_function_decorator
@expanduser_path_arg("filepath_or_buffer")
def read_pickle(
    filepath_or_buffer,
    compression: CompressionOptions = "infer",
    storage_options: StorageOptions = None,
):
    _, _, _, kwargs = inspect.getargvalues(inspect.currentframe())

    from snowflake.snowpark.modin.core.execution.dispatching.factories.dispatcher import (
        FactoryDispatcher,
    )

    return ModinObjects.DataFrame(
        query_compiler=FactoryDispatcher.read_pickle(**kwargs)
    )


@_inherit_docstrings(pandas.read_sql, apilink="pandas.read_sql")
@snowpark_pandas_telemetry_standalone_function_decorator
def read_sql(
    sql,
    con,
    index_col=None,
    coerce_float=True,
    params=None,
    parse_dates=None,
    columns=None,
    chunksize=None,
    dtype_backend: DtypeBackend | NoDefault = no_default,
    dtype=None,
):  # pragma: no cover  # noqa: PR01, RT01, D200
    """
    Read SQL query or database table into a DataFrame.
    """
    _, _, _, kwargs = inspect.getargvalues(inspect.currentframe())

    from snowflake.snowpark.modin.core.execution.dispatching.factories.dispatcher import (
        FactoryDispatcher,
    )

    if kwargs.get("chunksize") is not None:
        ErrorMessage.default_to_pandas("Parameters provided [chunksize]")
        df_gen = pandas.read_sql(**kwargs)
        return (
            ModinObjects.DataFrame(query_compiler=FactoryDispatcher.from_pandas(df))
            for df in df_gen
        )
    return ModinObjects.DataFrame(query_compiler=FactoryDispatcher.read_sql(**kwargs))


@_inherit_docstrings(pandas.read_fwf, apilink="pandas.read_fwf")
@snowpark_pandas_telemetry_standalone_function_decorator
@expanduser_path_arg("filepath_or_buffer")
def read_fwf(
    filepath_or_buffer: str | pathlib.Path | IO[AnyStr],
    *,
    colspecs="infer",
    widths=None,
    infer_nrows=100,
    dtype_backend: DtypeBackend | NoDefault = no_default,
    **kwds,
):  # pragma: no cover  # noqa: PR01, RT01, D200
    """
    Read a table of fixed-width formatted lines into DataFrame.
    """
    from pandas.io.parsers.base_parser import parser_defaults

    from snowflake.snowpark.modin.core.execution.dispatching.factories.dispatcher import (
        FactoryDispatcher,
    )

    _, _, _, kwargs = inspect.getargvalues(inspect.currentframe())
    kwargs.update(kwargs.pop("kwds", {}))
    target_kwargs = parser_defaults.copy()
    target_kwargs.update(kwargs)
    pd_obj = FactoryDispatcher.read_fwf(**target_kwargs)
    # When `read_fwf` returns a TextFileReader object for iterating through
    if isinstance(pd_obj, TextFileReader):
        reader = pd_obj.read
        pd_obj.read = lambda *args, **kwargs: ModinObjects.DataFrame(
            query_compiler=reader(*args, **kwargs)
        )
        return pd_obj
    return ModinObjects.DataFrame(query_compiler=pd_obj)


@_inherit_docstrings(pandas.read_sql_table, apilink="pandas.read_sql_table")
@snowpark_pandas_telemetry_standalone_function_decorator
def read_sql_table(
    table_name,
    con,
    schema=None,
    index_col=None,
    coerce_float=True,
    parse_dates=None,
    columns=None,
    chunksize=None,
    dtype_backend: DtypeBackend | NoDefault = no_default,
):  # noqa: PR01, RT01, D200
    """
    Read SQL database table into a DataFrame.
    """
    _, _, _, kwargs = inspect.getargvalues(inspect.currentframe())

    from snowflake.snowpark.modin.core.execution.dispatching.factories.dispatcher import (
        FactoryDispatcher,
    )

    return ModinObjects.DataFrame(
        query_compiler=FactoryDispatcher.read_sql_table(**kwargs)
    )


@_inherit_docstrings(pandas.read_sql_query, apilink="pandas.read_sql_query")
@snowpark_pandas_telemetry_standalone_function_decorator
def read_sql_query(
    sql,
    con,
    index_col: str | list[str] | None = None,
    coerce_float: bool = True,
    params: list[str] | dict[str, str] | None = None,
    parse_dates: list[str] | dict[str, str] | None = None,
    chunksize: int | None = None,
    dtype: DtypeArg | None = None,
    dtype_backend: DtypeBackend | NoDefault = no_default,
) -> DataFrame | Iterator[DataFrame]:
    _, _, _, kwargs = inspect.getargvalues(inspect.currentframe())

    from snowflake.snowpark.modin.core.execution.dispatching.factories.dispatcher import (
        FactoryDispatcher,
    )

    return ModinObjects.DataFrame(
        query_compiler=FactoryDispatcher.read_sql_query(**kwargs)
    )


@_inherit_docstrings(pandas.to_pickle)
@snowpark_pandas_telemetry_standalone_function_decorator
@expanduser_path_arg("filepath_or_buffer")
def to_pickle(
    obj: Any,
    filepath_or_buffer,
    compression: CompressionOptions = "infer",
    protocol: int = pickle.HIGHEST_PROTOCOL,
    storage_options: StorageOptions = None,
) -> None:
    from snowflake.snowpark.modin.core.execution.dispatching.factories.dispatcher import (
        FactoryDispatcher,
    )

    if isinstance(obj, ModinObjects.DataFrame):
        obj = obj._query_compiler
    return FactoryDispatcher.to_pickle(
        obj,
        filepath_or_buffer=filepath_or_buffer,
        compression=compression,
        protocol=protocol,
        storage_options=storage_options,
    )


@_inherit_docstrings(pandas.read_spss, apilink="pandas.read_spss")
@snowpark_pandas_telemetry_standalone_function_decorator
@expanduser_path_arg("path")
def read_spss(
    path: str | pathlib.Path,
    usecols: Sequence[str] | None = None,
    convert_categoricals: bool = True,
    dtype_backend: DtypeBackend | NoDefault = no_default,
):  # noqa: PR01, RT01, D200
    """
    Load an SPSS file from the file path, returning a DataFrame.
    """
    from snowflake.snowpark.modin.core.execution.dispatching.factories.dispatcher import (
        FactoryDispatcher,
    )

    return ModinObjects.DataFrame(
        query_compiler=FactoryDispatcher.read_spss(
            path=path,
            usecols=usecols,
            convert_categoricals=convert_categoricals,
            dtype_backend=dtype_backend,
        )
    )


@_inherit_docstrings(pandas.json_normalize, apilink="pandas.json_normalize")
@snowpark_pandas_telemetry_standalone_function_decorator
@pandas_module_level_function_not_implemented()
def json_normalize(
    data: dict | list[dict],
    record_path: str | list | None = None,
    meta: str | list[str | list[str]] | None = None,
    meta_prefix: str | None = None,
    record_prefix: str | None = None,
    errors: str | None = "raise",
    sep: str = ".",
    max_level: int | None = None,
) -> DataFrame:  # noqa: PR01, RT01, D200
    """
    Normalize semi-structured JSON data into a flat table.
    """
    # TODO(https://github.com/modin-project/modin/issues/7104):
    # modin needs to remove defaults to pandas at API layer
    pass


@_inherit_docstrings(pandas.read_orc, apilink="pandas.read_orc")
@snowpark_pandas_telemetry_standalone_function_decorator
@expanduser_path_arg("path")
@pandas_module_level_function_not_implemented()
def read_orc(
    path,
    columns: list[str] | None = None,
    dtype_backend: DtypeBackend | NoDefault = no_default,
    filesystem=None,
    **kwargs,
) -> DataFrame:  # noqa: PR01, RT01, D200
    """
    Load an ORC object from the file path, returning a DataFrame.
    """
    # TODO(https://github.com/modin-project/modin/issues/7104):
    # modin needs to remove defaults to pandas at API layer
    pass


@_inherit_docstrings(pandas.HDFStore)
@snowpark_pandas_telemetry_standalone_function_decorator
class HDFStore(pandas.HDFStore):  # pragma: no cover  # noqa: PR01, D200
    """
    Dict-like IO interface for storing pandas objects in PyTables.
    """

    _return_modin_dataframe = True

    def __getattribute__(self, item):
        default_behaviors = ["__init__", "__class__"]
        method = super().__getattribute__(item)
        if item not in default_behaviors:
            if callable(method):

                def return_handler(*args, **kwargs):
                    """
                    Replace the default behavior of methods with inplace kwarg.

                    Returns
                    -------
                    A Modin DataFrame in place of a pandas DataFrame, or the same
                    return type as pandas.HDFStore.

                    Notes
                    -----
                    This function will replace all of the arguments passed to
                    methods of HDFStore with the pandas equivalent. It will convert
                    Modin DataFrame to pandas DataFrame, etc. Currently, pytables
                    does not accept Modin DataFrame objects, so we must convert to
                    pandas.
                    """
                    # We don't want to constantly be giving this error message for
                    # internal methods.
                    if item[0] != "_":
                        ErrorMessage.default_to_pandas(f"`{item}`")
                    args = [
                        to_pandas(arg)
                        if isinstance(arg, ModinObjects.DataFrame)
                        else arg
                        for arg in args
                    ]
                    kwargs = {
                        k: to_pandas(v) if isinstance(v, ModinObjects.DataFrame) else v
                        for k, v in kwargs.items()
                    }
                    obj = super(HDFStore, self).__getattribute__(item)(*args, **kwargs)
                    if self._return_modin_dataframe and isinstance(
                        obj, pandas.DataFrame
                    ):
                        return ModinObjects.DataFrame(obj)
                    return obj

                # We replace the method with `return_handler` for inplace operations
                method = return_handler
        return method


@_inherit_docstrings(pandas.ExcelFile)
@snowpark_pandas_telemetry_standalone_function_decorator
class ExcelFile(pandas.ExcelFile):  # pragma: no cover # noqa: PR01, D200
    """
    Class for parsing tabular excel sheets into DataFrame objects.
    """

    _behave_like_pandas = False

    def _set_pandas_mode(self):  # noqa
        # disable Modin behavior to be able to pass object to `pandas.read_excel`
        # otherwise, Modin objects may be passed to the pandas context, resulting
        # in undefined behavior
        self._behave_like_pandas = True

    def __getattribute__(self, item):
        if item in ["_set_pandas_mode", "_behave_like_pandas"]:
            return object.__getattribute__(self, item)

        default_behaviors = ["__init__", "__class__"]
        method = super().__getattribute__(item)
        if not self._behave_like_pandas and item not in default_behaviors:
            if callable(method):

                def return_handler(*args, **kwargs):
                    """
                    Replace the default behavior of methods with inplace kwarg.

                    Returns
                    -------
                    A Modin DataFrame in place of a pandas DataFrame, or the same
                    return type as pandas.ExcelFile.

                    Notes
                    -----
                    This function will replace all of the arguments passed to
                    methods of ExcelFile with the pandas equivalent. It will convert
                    Modin DataFrame to pandas DataFrame, etc.
                    """
                    # We don't want to constantly be giving this error message for
                    # internal methods.
                    if item[0] != "_":
                        ErrorMessage.default_to_pandas(f"`{item}`")
                    args = [
                        to_pandas(arg)
                        if isinstance(arg, ModinObjects.DataFrame)
                        else arg
                        for arg in args
                    ]
                    kwargs = {
                        k: to_pandas(v) if isinstance(v, ModinObjects.DataFrame) else v
                        for k, v in kwargs.items()
                    }
                    obj = super(ExcelFile, self).__getattribute__(item)(*args, **kwargs)
                    if isinstance(obj, pandas.DataFrame):
                        return ModinObjects.DataFrame(obj)
                    return obj

                # We replace the method with `return_handler` for inplace operations
                method = return_handler
        return method


@snowpark_pandas_telemetry_standalone_function_decorator
def from_non_pandas(df, index, columns, dtype):  # pragma: no cover
    """
    Convert a non-pandas DataFrame into Modin DataFrame.

    Parameters
    ----------
    df : object
        Non-pandas DataFrame.
    index : object
        Index for non-pandas DataFrame.
    columns : object
        Columns for non-pandas DataFrame.
    dtype : type
        Data type to force.

    Returns
    -------
    modin.pandas.DataFrame
        Converted DataFrame.
    """
    from snowflake.snowpark.modin.core.execution.dispatching.factories.dispatcher import (
        FactoryDispatcher,
    )

    new_qc = FactoryDispatcher.from_non_pandas(df, index, columns, dtype)
    if new_qc is not None:
        return ModinObjects.DataFrame(query_compiler=new_qc)
    return new_qc


@snowpark_pandas_telemetry_standalone_function_decorator
def from_pandas(df):  # pragma: no cover
    """
    Convert a pandas DataFrame to a Modin DataFrame.

    Parameters
    ----------
    df : pandas.DataFrame
        The pandas DataFrame to convert.

    Returns
    -------
    modin.pandas.DataFrame
        A new Modin DataFrame object.
    """
    from snowflake.snowpark.modin.core.execution.dispatching.factories.dispatcher import (
        FactoryDispatcher,
    )

    return ModinObjects.DataFrame(query_compiler=FactoryDispatcher.from_pandas(df))


@snowpark_pandas_telemetry_standalone_function_decorator
def from_arrow(at):  # pragma: no cover
    """
    Convert an Arrow Table to a Modin DataFrame.

    Parameters
    ----------
    at : Arrow Table
        The Arrow Table to convert from.

    Returns
    -------
    DataFrame
        A new Modin DataFrame object.
    """
    from snowflake.snowpark.modin.core.execution.dispatching.factories.dispatcher import (
        FactoryDispatcher,
    )

    return ModinObjects.DataFrame(query_compiler=FactoryDispatcher.from_arrow(at))


@snowpark_pandas_telemetry_standalone_function_decorator
def from_dataframe(df):  # pragma: no cover
    """
    Convert a DataFrame implementing the dataframe exchange protocol to a Modin DataFrame.

    See more about the protocol in https://data-apis.org/dataframe-protocol/latest/index.html.

    Parameters
    ----------
    df : DataFrame
        The DataFrame object supporting the dataframe exchange protocol.

    Returns
    -------
    DataFrame
        A new Modin DataFrame object.
    """
    from snowflake.snowpark.modin.core.execution.dispatching.factories.dispatcher import (
        FactoryDispatcher,
    )

    return ModinObjects.DataFrame(query_compiler=FactoryDispatcher.from_dataframe(df))


@snowpark_pandas_telemetry_standalone_function_decorator
def to_pandas(modin_obj: SupportsPrivateToPandas) -> Any:  # pragma: no cover
    """
    Convert a Modin DataFrame/Series to a pandas DataFrame/Series.

    Parameters
    ----------
    modin_obj : modin.DataFrame, modin.Series
        The Modin DataFrame/Series to convert.

    Returns
    -------
    pandas.DataFrame or pandas.Series
        Converted object with type depending on input.
    """
    return modin_obj._to_pandas()


@snowpark_pandas_telemetry_standalone_function_decorator
def to_numpy(
    modin_obj: SupportsPrivateToNumPy | SupportsPublicToNumPy,
) -> np.ndarray:  # pragma: no cover
    """
    Convert a Modin object to a NumPy array.

    Parameters
    ----------
    modin_obj : modin.DataFrame, modin."Series", modin.numpy.array
        The Modin distributed object to convert.

    Returns
    -------
    numpy.array
        Converted object with type depending on input.
    """
    if isinstance(modin_obj, SupportsPrivateToNumPy):
        return modin_obj._to_numpy()
    array = modin_obj.to_numpy()
    if ExperimentalNumPyAPI.get():
        array = array._to_numpy()
    return array


__all__ = [
    "ExcelFile",
    "HDFStore",
    "json_normalize",
    "read_clipboard",
    "read_csv",
    "read_excel",
    "read_feather",
    "read_fwf",
    "read_gbq",
    "read_hdf",
    "read_html",
    "read_json",
    "read_orc",
    "read_parquet",
    "read_pickle",
    "read_sas",
    "read_spss",
    "read_sql",
    "read_sql_query",
    "read_sql_table",
    "read_stata",
    "read_table",
    "read_xml",
    "from_non_pandas",
    "from_pandas",
    "from_arrow",
    "from_dataframe",
    "to_pickle",
    "to_pandas",
    "to_numpy",
]
