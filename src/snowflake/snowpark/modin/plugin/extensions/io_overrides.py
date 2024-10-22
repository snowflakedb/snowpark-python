#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

import inspect
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Callable, Hashable, Iterable, Literal, Sequence

import modin.pandas as pd
import pandas as native_pd
from modin.pandas import DataFrame
from modin.pandas.api.extensions import register_pd_accessor
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
    StorageOptions,
    XMLParsers,
)

from snowflake.snowpark.modin.plugin.io.snow_io import (
    READ_CSV_DEFAULTS,
    PandasOnSnowflakeIO,
)
from snowflake.snowpark.modin.utils import _inherit_docstrings, expanduser_path_arg

if TYPE_CHECKING:  # pragma: no cover
    import csv

from snowflake.snowpark.modin.plugin.extensions.datetime_index import (  # noqa: F401
    DatetimeIndex,
)
from snowflake.snowpark.modin.plugin.extensions.index import Index  # noqa: F401
from snowflake.snowpark.modin.plugin.extensions.timedelta_index import (  # noqa: F401
    TimedeltaIndex,
)
from snowflake.snowpark.modin.plugin.utils.error_message import (
    pandas_module_level_function_not_implemented,
)

# TODO: SNOW-1265551: add inherit_docstrings decorators once docstring overrides are available


@register_pd_accessor("read_xml")
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
) -> pd.DataFrame:
    # TODO(https://github.com/modin-project/modin/issues/7104):
    # modin needs to remove defaults to pandas at API layer
    pass  # pragma: no cover


@_inherit_docstrings(native_pd.json_normalize, apilink="pandas.json_normalize")
@register_pd_accessor("json_normalize")
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
) -> pd.DataFrame:  # noqa: PR01, RT01, D200
    # TODO(https://github.com/modin-project/modin/issues/7104):
    # modin needs to remove defaults to pandas at API layer
    pass  # pragma: no cover


@_inherit_docstrings(native_pd.read_orc, apilink="pandas.read_orc")
@register_pd_accessor("read_orc")
@pandas_module_level_function_not_implemented()
def read_orc(
    path,
    columns: list[str] | None = None,
    dtype_backend: DtypeBackend | NoDefault = no_default,
    filesystem=None,
    **kwargs,
) -> pd.DataFrame:  # noqa: PR01, RT01, D200
    # TODO(https://github.com/modin-project/modin/issues/7104):
    # modin needs to remove defaults to pandas at API layer
    pass  # pragma: no cover


@_inherit_docstrings(native_pd.read_excel)
@register_pd_accessor("read_excel")
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
) -> pd.DataFrame | dict[IntStrT, pd.DataFrame]:
    # TODO this implementation is identical to modin, but we don't have a proper docstring override
    # mechanism yet
    _, _, _, kwargs = inspect.getargvalues(inspect.currentframe())
    from modin.core.execution.dispatching.factories.dispatcher import FactoryDispatcher

    intermediate = FactoryDispatcher.read_excel(**kwargs)
    if isinstance(intermediate, (OrderedDict, dict)):  # pragma: no cover
        parsed = type(intermediate)()
        for key in intermediate.keys():
            parsed[key] = pd.DataFrame(query_compiler=intermediate.get(key))
        return parsed
    else:
        return pd.DataFrame(query_compiler=intermediate)


@_inherit_docstrings(native_pd.read_csv, apilink="pandas.read_csv")
@register_pd_accessor("read_csv")
def read_csv(
    filepath_or_buffer: FilePath,
    *,
    sep: str | NoDefault | None = READ_CSV_DEFAULTS["sep"],
    delimiter: str | None = READ_CSV_DEFAULTS["delimiter"],
    header: int | Sequence[int] | Literal["infer"] | None = READ_CSV_DEFAULTS["header"],
    names: Sequence[Hashable] | NoDefault | None = READ_CSV_DEFAULTS["names"],
    index_col: IndexLabel | Literal[False] | None = READ_CSV_DEFAULTS["index_col"],
    usecols: list[Hashable] | Callable | None = READ_CSV_DEFAULTS["usecols"],
    dtype: DtypeArg | None = READ_CSV_DEFAULTS["dtype"],
    engine: CSVEngine | None = READ_CSV_DEFAULTS["engine"],
    converters: dict[Hashable, Callable] | None = READ_CSV_DEFAULTS["converters"],
    true_values: list[Any] | None = READ_CSV_DEFAULTS["true_values"],
    false_values: list[Any] | None = READ_CSV_DEFAULTS["false_values"],
    skipinitialspace: bool | None = READ_CSV_DEFAULTS["skipinitialspace"],
    skiprows: int | None = READ_CSV_DEFAULTS["skiprows"],
    skipfooter: int | None = READ_CSV_DEFAULTS["skipfooter"],
    nrows: int | None = READ_CSV_DEFAULTS["nrows"],
    na_values: Sequence[Hashable] | None = READ_CSV_DEFAULTS["na_values"],
    keep_default_na: bool | None = READ_CSV_DEFAULTS["keep_default_na"],
    na_filter: bool | None = READ_CSV_DEFAULTS["na_filter"],
    verbose: bool | None = READ_CSV_DEFAULTS["verbose"],
    skip_blank_lines: bool | None = READ_CSV_DEFAULTS["skip_blank_lines"],
    parse_dates: None
    | (
        bool | Sequence[int] | Sequence[Sequence[int]] | dict[str, Sequence[int]]
    ) = READ_CSV_DEFAULTS["parse_dates"],
    infer_datetime_format: bool | None = READ_CSV_DEFAULTS["infer_datetime_format"],
    keep_date_col: bool | None = READ_CSV_DEFAULTS["keep_date_col"],
    date_parser: Callable | None = READ_CSV_DEFAULTS["date_parser"],
    date_format: str | dict | None = READ_CSV_DEFAULTS["date_format"],
    dayfirst: bool | None = READ_CSV_DEFAULTS["dayfirst"],
    cache_dates: bool | None = READ_CSV_DEFAULTS["cache_dates"],
    iterator: bool = READ_CSV_DEFAULTS["iterator"],
    chunksize: int | None = READ_CSV_DEFAULTS["chunksize"],
    compression: Literal[
        "infer", "gzip", "bz2", "brotli", "zstd", "deflate", "raw_deflate", "none"
    ] = READ_CSV_DEFAULTS["compression"],
    thousands: str | None = READ_CSV_DEFAULTS["thousands"],
    decimal: str | None = READ_CSV_DEFAULTS["decimal"],
    lineterminator: str | None = READ_CSV_DEFAULTS["lineterminator"],
    quotechar: str = READ_CSV_DEFAULTS["quotechar"],
    quoting: int | None = READ_CSV_DEFAULTS["quoting"],
    doublequote: bool = READ_CSV_DEFAULTS["doublequote"],
    escapechar: str | None = READ_CSV_DEFAULTS["escapechar"],
    comment: str | None = READ_CSV_DEFAULTS["comment"],
    encoding: str | None = READ_CSV_DEFAULTS["encoding"],
    encoding_errors: str | None = READ_CSV_DEFAULTS["encoding_errors"],
    dialect: str | csv.Dialect | None = READ_CSV_DEFAULTS["dialect"],
    on_bad_lines: str = READ_CSV_DEFAULTS["on_bad_lines"],
    delim_whitespace: bool | None = READ_CSV_DEFAULTS["delim_whitespace"],
    low_memory: bool
    | None = READ_CSV_DEFAULTS[
        "low_memory"
    ],  # Different from default because we want better dtype detection
    memory_map: bool | None = READ_CSV_DEFAULTS["memory_map"],
    float_precision: Literal["high", "legacy"]
    | None = READ_CSV_DEFAULTS["float_precision"],
    storage_options: StorageOptions = READ_CSV_DEFAULTS["storage_options"],
    dtype_backend: DtypeBackend = READ_CSV_DEFAULTS["dtype_backend"],
) -> pd.DataFrame:
    _pd_read_csv_signature = {
        val.name for val in inspect.signature(native_pd.read_csv).parameters.values()
    }
    _, _, _, f_locals = inspect.getargvalues(inspect.currentframe())
    kwargs = {k: v for k, v in f_locals.items() if k in _pd_read_csv_signature}
    return pd.DataFrame(query_compiler=PandasOnSnowflakeIO.read_csv(**kwargs))


@_inherit_docstrings(native_pd.read_json, apilink="pandas.read_json")
@register_pd_accessor("read_json")
def read_json(
    path_or_buf: FilePath,
    *,
    orient: str | None = None,
    typ: Literal["frame", "series"] | None = "frame",
    dtype: DtypeArg | None = None,
    convert_axes: bool | None = None,
    convert_dates: bool | list[str] | None = None,
    keep_default_dates: bool | None = None,
    precise_float: bool | None = None,
    date_unit: str | None = None,
    encoding: str | None = None,
    encoding_errors: str | None = None,
    lines: bool | None = None,
    chunksize: int | None = None,
    compression: Literal[
        "infer", "gzip", "bz2", "brotli", "zstd", "deflate", "raw_deflate", "none"
    ] = "infer",
    nrows: int | None = None,
    storage_options: StorageOptions = None,
    dtype_backend: DtypeBackend = no_default,
    engine: Literal["ujson", "pyarrow"] | None = None,
) -> pd.DataFrame:
    _pd_read_json_signature = {
        val.name for val in inspect.signature(native_pd.read_json).parameters.values()
    }
    _, _, _, f_locals = inspect.getargvalues(inspect.currentframe())
    kwargs = {k: v for k, v in f_locals.items() if k in _pd_read_json_signature}
    return DataFrame(
        query_compiler=PandasOnSnowflakeIO.read_json(
            **kwargs,
        )
    )


@_inherit_docstrings(native_pd.read_parquet, apilink="pandas.read_parquet")
@register_pd_accessor("read_parquet")
def read_parquet(
    path: FilePath,
    engine: str | None = None,
    columns: list[str] | None = None,
    storage_options: StorageOptions = None,
    use_nullable_dtypes: bool | NoDefault = no_default,
    dtype_backend: DtypeBackend | NoDefault = no_default,
    filesystem: str = None,
    filters: list[tuple] | list[list[tuple]] | None = None,
    **kwargs,
):
    _pd_read_parquet_signature = {
        val.name
        for val in inspect.signature(native_pd.read_parquet).parameters.values()
    }
    _, _, _, f_locals = inspect.getargvalues(inspect.currentframe())
    kwargs = {k: v for k, v in f_locals.items() if k in _pd_read_parquet_signature}

    return pd.DataFrame(query_compiler=PandasOnSnowflakeIO.read_parquet(**kwargs))


@_inherit_docstrings(native_pd.read_sas, apilink="pandas.read_sas")
@register_pd_accessor("read_sas")
def read_sas(
    filepath_or_buffer,
    *,
    format: str | None = None,
    index: Hashable | None = None,
    encoding: str | None = None,
    chunksize: int | None = None,
    iterator: bool = False,
    compression: CompressionOptions = "infer",
) -> pd.DataFrame:
    _pd_read_sas_signature = {
        val.name for val in inspect.signature(native_pd.read_sas).parameters.values()
    }
    _, _, _, f_locals = inspect.getargvalues(inspect.currentframe())
    kwargs = {k: v for k, v in f_locals.items() if k in _pd_read_sas_signature}

    return pd.DataFrame(query_compiler=PandasOnSnowflakeIO.read_sas(**kwargs))
