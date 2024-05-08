#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

# this module houses classes for IO and interacting with Snowflake engine

import inspect
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Hashable,
    Iterable,
    Literal,
    Optional,
    Sequence,
    Union,
)

import pandas
from pandas._libs.lib import NoDefault, no_default
from pandas._typing import (
    CSVEngine,
    DtypeArg,
    DtypeBackend,
    FilePath,
    IndexLabel,
    StorageOptions,
)
from pandas.core.dtypes.common import is_list_like

from snowflake.snowpark.modin.core.execution.dispatching.factories.baseio import BaseIO
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)
from snowflake.snowpark.modin.plugin.utils.error_message import (
    pandas_module_level_function_not_implemented,
)
from snowflake.snowpark.modin.utils import (
    error_not_implemented_parameter,
    should_parse_header,
    translate_pandas_default,
    warn_not_supported_parameter,
)

if TYPE_CHECKING:  # pragma: no cover
    import csv


def _validate_read_csv_and_read_table_args(fn_name, **kwargs):
    """
    Helper function to error or warn on arguments that are unsupported by read_csv/read_table.
    """
    error_not_none_kwargs = [
        "verbose",
        "dayfirst",
        "date_parser",
        "date_format",
        "keep_date_col",
        "parse_dates",
        "iterator",
        "na_filter",
        "skipfooter",
        "nrows",
        "thousands",
        "decimal",
        "lineterminator",
        "dialect",
        "quoting",
        "doublequote",
        "encoding_errors",
        "comment",
        "converters",
        "true_values",
        "false_values",
        "keep_default_na",
        "delim_whitespace",
        "skipinitialspace",
        "on_bad_lines",
    ]
    for kw in error_not_none_kwargs:
        error_not_implemented_parameter(kw, kwargs.get(kw) is not None)
    warn_not_none_kwargs = [
        "engine",
        "cache_dates",
        "infer_datetime_format",
        "chunksize",
        "memory_map",
        "storage_options",
        "low_memory",
        "float_precision",
    ]
    for kw in warn_not_none_kwargs:
        warn_not_supported_parameter(kw, kwargs.get(kw) is not None, fn_name)
    warn_not_supported_parameter(
        "dtype_backend",
        kwargs.get("dtype_backend", no_default) is not no_default,
        fn_name,
    )


class PandasOnSnowflakeIO(BaseIO):
    """
    Factory providing methods for peforming I/O methods using pandas on Snowflake.

    Some methods are defined entirely in plugin/pd_overrides.py instead of here
    because Snowflake provides some different default argument values than pandas.
    """

    # frame_cls is the internal dataframe class used by Modin for other engines, but Snowflake
    # does not implement this interface
    frame_cls = None
    query_compiler_cls = SnowflakeQueryCompiler

    @classmethod
    def from_pandas(cls, df: pandas.DataFrame):
        """invoke construction from pandas DataFrame (io backup methods), df is a pandas.DataFrame living in main-memory
        Args:
            df: An existing (native) pandas DataFrame
        """
        return cls.query_compiler_cls.from_pandas(df, pandas.DataFrame)

    @classmethod
    def read_snowflake(
        cls,
        name_or_query: Union[str, Iterable[str]],
        index_col: Optional[Union[str, list[str]]] = None,
        columns: Optional[list[str]] = None,
    ):
        """
        See detailed docstring and examples in ``read_snowflake`` in frontend layer:
        src/snowflake/snowpark/modin/pandas/io.py
        """
        return cls.query_compiler_cls.from_snowflake(name_or_query, index_col, columns)

    @classmethod
    def to_snowflake(
        cls,
        name: Union[str, Iterable[str]],
        index: bool = True,
        overwrite: bool = False,
        **kwargs,
    ):
        """
        Stores DataFrame into table. Index must be range-index, else storage will be refused.
        Args:
            name: table name where to store table in Snowflake
            index: whether to store index in one (or more columns if Multiindex) column
            overwrite: whether to replace existing table, else fails with exception
            **kwargs: other optional arguments to be passed, ignored for now.
        """
        return cls.query_compiler_cls.to_snowflake(name, index, overwrite)

    @classmethod
    def to_snowpark(cls, index: bool = True, index_label: Optional[IndexLabel] = None):
        """
         Convert the Snowpark pandas Object(DataFrame or Series) to a Snowpark DataFrame.
         Note that once converted to a Snowpark Dataframe, no ordering information will be preserved. You can call
         reset_index to generate a default index column same as row position before call to_snowpark.

         Args:
             index: bool, default True.
                 Whether to keep the index columns in the result Snowpark DataFrame. If True, the index columns
                 will be the first set of columns. Otherwise, no index column will be included in the final Snowpark
                 DataFrame.
             index_label: IndexLabel, default None.
                 Column label(s) to use for the index column(s). If None is given (default) and index is True,
                 then the original index column labels are used. A sequence should be given if the Snowpark pandas
                 DataFrame or Series uses MultiIndex, and the length of the given sequence should be the same as
                 the number of index columns.

         Returns:
             :class:`~snowflake.snowpark.dataframe.DataFrame`
                 A Snowpark DataFrame contains the index columns if index=True and all data columns of the Snowpark pandas
                 DataFrame or Series.

        Note:
             The labels of the Snowpark pandas DataFrame/Series or index_label provided will be used as Normalized Snowflake
             Identifiers of the Snowpark DataFrame.
             For details about Normalized Snowflake Identifiers, please refer to the Note in :func:`~snowflake.snowpark.modin.pandas.io.read_snowflake`
        """
        return cls.query_compiler_cls.to_snowpark(
            index, index_label
        )  # pragma: no cover

    @classmethod
    def read_csv(
        cls,
        filepath_or_buffer: FilePath,
        *,
        sep: Optional[Union[str, NoDefault]] = no_default,
        delimiter: Optional[str] = None,
        header: Optional[Union[int, Sequence[int], Literal["infer"]]] = "infer",
        names: Optional[Union[Sequence[Hashable], NoDefault]] = no_default,
        index_col: Optional[Union[IndexLabel, Literal[False]]] = None,
        usecols: Optional[Union[list[Hashable], Callable]] = None,
        dtype: Optional[DtypeArg] = None,
        engine: Optional[CSVEngine] = None,
        converters: Optional[dict[Hashable, Callable]] = None,
        true_values: Optional[list[Any]] = None,
        false_values: Optional[list[Any]] = None,
        skipinitialspace: Optional[bool] = None,
        skiprows: Optional[int] = 0,
        skipfooter: Optional[int] = None,
        nrows: Optional[int] = None,
        na_values: Optional[Sequence[Hashable]] = None,
        keep_default_na: Optional[bool] = None,
        na_filter: Optional[bool] = None,
        verbose: Optional[bool] = None,
        skip_blank_lines: Optional[bool] = None,
        parse_dates: Optional[
            Union[
                bool, Sequence[int], Sequence[Sequence[int]], dict[str, Sequence[int]]
            ]
        ] = None,
        infer_datetime_format: Optional[bool] = None,
        keep_date_col: Optional[bool] = None,
        date_parser: Optional[Callable] = None,
        date_format: Optional[Union[str, dict]] = None,
        dayfirst: Optional[bool] = None,
        cache_dates: Optional[bool] = None,
        iterator: bool = None,
        chunksize: Optional[int] = None,
        compression: Literal[
            "infer", "gzip", "bz2", "brotli", "zstd", "deflate", "raw_deflate", "none"
        ] = "infer",
        thousands: Optional[str] = None,
        decimal: Optional[str] = None,
        lineterminator: Optional[str] = None,
        quotechar: str = '"',
        quoting: Optional[int] = None,
        doublequote: bool = None,
        escapechar: Optional[str] = None,
        comment: Optional[str] = None,
        encoding: Optional[str] = None,
        encoding_errors: Optional[str] = None,
        dialect: Optional[Union[str, "csv.Dialect"]] = None,
        on_bad_lines: str = None,
        delim_whitespace: Optional[bool] = None,
        low_memory: Optional[bool] = None,
        memory_map: Optional[bool] = None,
        float_precision: Optional[Literal["high", "legacy"]] = None,
        storage_options: StorageOptions = None,
        dtype_backend: DtypeBackend = no_default,
        **kwargs,
    ) -> SnowflakeQueryCompiler:
        """
        Validate arguments and perform I/O operation for read_csv and read_table.
        """
        # Copied from modin/pandas/io.py to allow read_csv and read_table to concisely share validation code
        _pd_read_csv_signature = {
            val.name for val in inspect.signature(pandas.read_csv).parameters.values()
        }
        _, _, _, f_locals = inspect.getargvalues(inspect.currentframe())
        kwargs = {k: v for k, v in f_locals.items() if k in _pd_read_csv_signature}
        _validate_read_csv_and_read_table_args("pd.read_csv", **kwargs)

        if not isinstance(filepath_or_buffer, str):
            raise NotImplementedError(
                "filepath_or_buffer must be a path to a file or folder stored locally or on a Snowflake stage."
            )

        sep = translate_pandas_default(sep)
        names = translate_pandas_default(names)

        if sep is not None and delimiter is not None:
            raise ValueError(
                "Specified a sep and a delimiter; you can only specify one."
            )

        if sep is None:
            sep = delimiter if delimiter is not None else ","

        # Distributed implementation is not supported for non-first row header or multi-index headers.
        if (
            isinstance(header, list)
            or (isinstance(header, int) and header != 0)
            or (skiprows != 0 and header is not None)
        ):
            error_not_implemented_parameter("header", header)

        parse_header = should_parse_header(header, names)

        if names is not None:
            if not is_list_like(names, allow_sets=False):
                raise ValueError("Names should be an ordered collection.")

            if len(set(names)) != len(names):
                raise ValueError("Duplicate names are not allowed.")

        if compression == "infer":
            compression = "auto"

        if usecols is not None:
            if not is_list_like(usecols) and not isinstance(usecols, Callable):
                raise ValueError(
                    "'usecols' must either be list-like of all strings, all integers or a callable."
                )
            elif is_list_like(usecols):
                if len(usecols) == 0:
                    return cls.query_compiler_cls.from_pandas(pandas.DataFrame())

                usecols_is_all_int = all(
                    [isinstance(column, int) for column in usecols]
                )
                usecols_is_all_str = all(
                    [isinstance(column, str) for column in usecols]
                )

                if not usecols_is_all_int and not usecols_is_all_str:
                    raise ValueError(
                        "'usecols' must either be list-like of all strings, all integers or a callable."
                    )
                usecols = list(usecols)

            # Case where usecols is Callable is handled in SnowflakeQueryCompiler.from_file.

        if index_col:
            if isinstance(index_col, (int, str)):
                index_col = [index_col]
            elif isinstance(index_col, (tuple, list)):
                for column in index_col:
                    if not isinstance(column, (int, str)):
                        raise TypeError(
                            f"list indices must be integers or slices, not {type(column).__name__}"
                        )
            else:
                raise TypeError(
                    f"list indices must be integers or slices, not {type(index_col).__name__}"
                )

        return cls.query_compiler_cls.from_file(
            "csv",
            filepath_or_buffer,
            field_delimiter=sep,
            skip_blank_lines=skip_blank_lines,
            null_if=na_values,
            compression=compression,
            escape=escapechar,
            skip_header=skiprows,
            encoding=encoding,
            field_optionally_enclosed_by=quotechar,
            parse_header=parse_header,
            names=names,
            index_col=index_col,
            usecols=usecols,
            dtype=dtype,
        )

    @classmethod
    def read_json(
        cls,
        path_or_buf: FilePath,
        *,
        orient: Optional[str] = None,
        typ: Optional[Literal["frame", "series"]] = None,
        dtype: Optional[DtypeArg] = None,
        convert_axes: Optional[bool] = None,
        convert_dates: Optional[Union[bool, list[str]]] = None,
        keep_default_dates: Optional[bool] = None,
        precise_float: Optional[bool] = None,
        date_unit: Optional[str] = None,
        encoding: Optional[str] = None,
        encoding_errors: Optional[str] = None,
        lines: Optional[bool] = None,
        chunksize: Optional[int] = None,
        compression: Literal[
            "infer", "gzip", "bz2", "brotli", "zstd", "deflate", "raw_deflate", "none"
        ] = "infer",
        nrows: Optional[int] = None,
        storage_options: StorageOptions = None,
        dtype_backend: DtypeBackend = no_default,
        engine: Optional[Literal["ujson", "pyarrow"]] = None,
        **kwargs: Any,
    ) -> "SnowflakeQueryCompiler":
        """
        Validate arguments and perform I/O operation for read_json.
        """
        if not isinstance(path_or_buf, str):
            raise NotImplementedError(
                "'path' must be a path to a file or folder stored locally or on a Snowflake stage."
            )

        error_not_implemented_parameter("orient", orient is not None)
        error_not_implemented_parameter("typ", typ is not None)
        error_not_implemented_parameter("dtype", dtype is not None)
        error_not_implemented_parameter("convert_axes", convert_axes is not None)
        error_not_implemented_parameter("convert_dates", convert_dates is not None)
        error_not_implemented_parameter(
            "keep_default_dates", keep_default_dates is not None
        )
        error_not_implemented_parameter("precise_float", precise_float is not None)
        error_not_implemented_parameter("date_unit", date_unit is not None)
        error_not_implemented_parameter("encoding_errors", encoding_errors is not None)
        error_not_implemented_parameter("lines", lines is not None)
        error_not_implemented_parameter("chunksize", chunksize is not None)
        error_not_implemented_parameter("nrows", nrows is not None)

        fn_name = "pd.read_json"
        warn_not_supported_parameter(
            "storage_options", storage_options is not None, fn_name
        )
        warn_not_supported_parameter("engine", engine is not None, fn_name)
        warn_not_supported_parameter(
            "dtype_backend", dtype_backend is not no_default, fn_name
        )

        if compression == "infer":
            compression = "auto"

        return cls.query_compiler_cls.from_file(
            "json",
            path_or_buf,
            compression=compression,
            encoding=encoding,
        )

    @classmethod
    def read_parquet(
        cls,
        path: FilePath,
        *,
        engine: Optional[str] = None,
        columns: Optional[list[str]] = None,
        storage_options: StorageOptions = None,
        use_nullable_dtypes: Union[bool, NoDefault] = no_default,
        dtype_backend: Union[DtypeBackend, NoDefault] = no_default,
        filesystem: str = None,
        filters: Optional[Union[list[tuple], list[list[tuple]]]] = None,
        **kwargs: Any,
    ) -> "SnowflakeQueryCompiler":
        """
        Validate arguments and perform I/O operation for read_parquet.
        """
        if not isinstance(path, str):
            raise NotImplementedError(
                "'path' must be a path to a file or folder stored locally or on a Snowflake stage."
            )

        error_not_implemented_parameter(
            "use_nullable_dtypes", use_nullable_dtypes is not no_default
        )

        fn_name = "pd.read_parquet"
        warn_not_supported_parameter("engine", engine is not None, fn_name)
        warn_not_supported_parameter(
            "storage_options", storage_options is not None, fn_name
        )
        warn_not_supported_parameter(
            "dtype_backend", dtype_backend is not no_default, fn_name
        )
        warn_not_supported_parameter("filesystem", filesystem is not None, fn_name)
        warn_not_supported_parameter("filters", filters is not None, fn_name)

        warn_not_supported_parameter("parquet_kwargs", len(kwargs) > 0, fn_name)

        if columns is not None:
            if not isinstance(columns, list):
                raise ValueError("'columns' must either be list of all strings.")

            columns_is_all_str = all([isinstance(column, str) for column in columns])

            if not columns_is_all_str:
                raise ValueError("'columns' must either be list of all strings.")
        return cls.query_compiler_cls.from_file("parquet", path, usecols=columns)

    @classmethod
    @pandas_module_level_function_not_implemented()
    def read_gbq(
        cls,
        query: str,
        project_id=None,
        index_col=None,
        col_order=None,
        reauth=False,
        auth_local_webserver=False,
        dialect=None,
        location=None,
        configuration=None,
        credentials=None,
        use_bqstorage_api=None,
        private_key=None,
        verbose=None,
        progress_bar_type=None,
        max_results=None,
    ):
        pass

    @classmethod
    @pandas_module_level_function_not_implemented()
    def read_html(
        cls,
        io,
        *,
        match=".+",
        flavor=None,
        header=None,
        index_col=None,
        skiprows=None,
        attrs=None,
        parse_dates=False,
        thousands=",",
        encoding=None,
        decimal=".",
        converters=None,
        na_values=None,
        keep_default_na=True,
        displayed_only=True,
        **kwargs,
    ):
        pass

    @classmethod
    @pandas_module_level_function_not_implemented()
    def read_clipboard(cls, sep=r"\s+", **kwargs):
        pass

    @classmethod
    @pandas_module_level_function_not_implemented()
    def read_excel(cls, **kwargs):
        pass

    @classmethod
    @pandas_module_level_function_not_implemented()
    def read_hdf(
        cls,
        path_or_buf,
        key=None,
        mode: str = "r",
        errors: str = "strict",
        where=None,
        start=None,
        stop=None,
        columns=None,
        iterator=False,
        chunksize=None,
        **kwargs,
    ):
        pass

    @classmethod
    @pandas_module_level_function_not_implemented()
    def read_feather(
        cls,
        path,
        **kwargs,
    ):
        pass

    @classmethod
    @pandas_module_level_function_not_implemented()
    def read_stata(
        cls,
        filepath_or_buffer,
        **kwargs,
    ):
        pass

    @classmethod
    @pandas_module_level_function_not_implemented()
    def read_sas(
        cls,
        filepath_or_buffer,
        *,
        format=None,
        index=None,
        encoding=None,
        chunksize=None,
        iterator=False,
        **kwargs,
    ):
        pass

    @classmethod
    @pandas_module_level_function_not_implemented()
    def read_pickle(
        cls,
        filepath_or_buffer,
        **kwargs,
    ):
        pass

    @classmethod
    @pandas_module_level_function_not_implemented()
    def read_sql(
        cls,
        sql,
        con,
        index_col=None,
        coerce_float=True,
        params=None,
        parse_dates=None,
        columns=None,
        chunksize=None,
        dtype_backend=no_default,
        dtype=None,
    ):
        pass

    @classmethod
    @pandas_module_level_function_not_implemented()
    def read_fwf(
        cls,
        filepath_or_buffer,
        *,
        colspecs="infer",
        widths=None,
        infer_nrows=100,
        dtype_backend=no_default,
        iterator=False,
        chunksize=None,
        **kwds,
    ):
        pass

    @classmethod
    @pandas_module_level_function_not_implemented()
    def read_sql_table(
        cls,
        table_name,
        con,
        schema=None,
        index_col=None,
        coerce_float=True,
        parse_dates=None,
        columns=None,
        chunksize=None,
        dtype_backend=no_default,
    ):
        pass

    @classmethod
    @pandas_module_level_function_not_implemented()
    def read_sql_query(
        cls,
        sql,
        con,
        **kwargs,
    ):
        pass

    @classmethod
    @pandas_module_level_function_not_implemented()
    def read_spss(cls, path, usecols, convert_categoricals, dtype_backend):
        pass

    @classmethod
    @pandas_module_level_function_not_implemented()
    def to_pickle(
        cls,
        obj,
        filepath_or_buffer,
        **kwargs,
    ):
        pass
