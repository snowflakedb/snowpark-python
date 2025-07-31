#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

# this module houses classes for IO and interacting with Snowflake engine

from contextlib import contextmanager
import os
import tempfile

import inspect
from collections import OrderedDict
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
from modin.core.io import BaseIO
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

from snowflake.snowpark._internal.utils import (
    STAGE_PREFIX,
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.mock._stage_registry import extract_stage_name_and_prefix
from snowflake.snowpark.modin.plugin._internal.io_utils import (
    is_local_filepath,
    is_snowflake_stage_path,
)
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)
from snowflake.snowpark.modin.plugin.utils.error_message import (
    pandas_module_level_function_not_implemented,
)
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from snowflake.snowpark.modin.utils import (
    error_not_implemented_parameter,
    should_parse_header,
    translate_pandas_default,
    warn_not_supported_parameter,
)

if TYPE_CHECKING:  # pragma: no cover
    import csv


READ_CSV_DEFAULTS = {
    "sep": no_default,
    "delimiter": None,
    "header": "infer",
    "names": no_default,
    "index_col": None,
    "usecols": None,
    "dtype": None,
    "engine": None,
    "converters": None,
    "true_values": None,
    "false_values": None,
    "skipinitialspace": False,
    "skiprows": None,
    "skipfooter": 0,
    "nrows": None,
    "na_values": None,
    "keep_default_na": True,
    "na_filter": True,
    "verbose": no_default,
    "skip_blank_lines": True,
    "parse_dates": None,
    "infer_datetime_format": no_default,
    "keep_date_col": no_default,
    "date_parser": no_default,
    "date_format": None,
    "dayfirst": False,
    "cache_dates": True,
    "iterator": False,
    "chunksize": None,
    "compression": "infer",
    "thousands": None,
    "decimal": ".",
    "lineterminator": None,
    "quotechar": '"',
    "quoting": 0,
    "doublequote": True,
    "escapechar": None,
    "comment": None,
    "encoding": None,
    "encoding_errors": "strict",
    "dialect": None,
    "on_bad_lines": "error",
    "delim_whitespace": no_default,
    "low_memory": True,
    "memory_map": False,
    "float_precision": None,
    "storage_options": None,
    "dtype_backend": no_default,
}


def _validate_read_staged_csv_and_read_table_args(fn_name, **kwargs):
    """
    Helper function to error or warn on arguments that are unsupported by read_csv/read_table.
    """

    error_not_default_kwargs = [
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
    for kw in error_not_default_kwargs:
        parameter_set = kwargs.get(kw) is not READ_CSV_DEFAULTS[kw]
        error_not_implemented_parameter(kw, parameter_set)
    warn_not_default_kwargs = [
        "engine",
        "cache_dates",
        "infer_datetime_format",
        "chunksize",
        "memory_map",
        "storage_options",
        "low_memory",
        "float_precision",
        "dtype_backend",
    ]
    for kw in warn_not_default_kwargs:
        parameter_set = kwargs.get(kw) is not READ_CSV_DEFAULTS[kw]
        warn_not_supported_parameter(kw, parameter_set, fn_name)


@contextmanager
def _file_from_stage(filepath_or_buffer):
    session = get_active_session()
    with tempfile.TemporaryDirectory() as local_temp_dir:
        session.file.get(filepath_or_buffer, local_temp_dir)
        _, stripped_filepath = extract_stage_name_and_prefix(filepath_or_buffer)
        yield os.path.join(local_temp_dir, os.path.basename(stripped_filepath))


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
    def json_normalize(cls, **kwargs):  # noqa: PR01
        """
        Normalize semi-structured JSON data into a query compiler representing a flat table.
        """
        return cls.from_pandas(pandas.json_normalize(**kwargs))

    @classmethod
    def _read_excel_locally(cls, **kwargs):
        try:
            intermediate = pandas.read_excel(**kwargs)
        except ImportError as e:
            raise ImportError(
                "Snowpark Pandas requires an additional package to read excel files such as openpyxl, pyxlsb, or xlrd",
                e,
            )
        if isinstance(intermediate, (OrderedDict, dict)):  # pragma: no cover
            parsed = type(intermediate)()
            for key in intermediate.keys():
                parsed[key] = cls.from_pandas(intermediate.get(key))
            return parsed
        else:
            return cls.from_pandas(intermediate)

    @classmethod
    def read_excel(cls, **kwargs):  # noqa: PR01
        """
        Read an excel file into a query compiler.

        Snowpark pandas has a slightly different error message from the upstream modin version.
        """
        io = kwargs["io"]
        if is_snowflake_stage_path(io):
            with _file_from_stage(io) as local_filepath:
                kwargs["io"] = local_filepath
                # We have to return here because the temp file is deleted
                # after exiting this block
                return cls._read_excel_locally(**kwargs)

        return cls._read_excel_locally(**kwargs)

    @classmethod
    def read_snowflake(
        cls,
        name_or_query: Union[str, Iterable[str]],
        index_col: Optional[Union[str, list[str]]] = None,
        columns: Optional[list[str]] = None,
        enforce_ordering: bool = False,
    ):
        """
        See detailed docstring and examples in ``read_snowflake`` in frontend layer:
        src/snowflake/snowpark/modin/plugin/pd_extensions.py
        """
        return cls.query_compiler_cls.from_snowflake(
            name_or_query, index_col, columns, enforce_ordering=enforce_ordering
        )

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
             For details about Normalized Snowflake Identifiers, please refer to the Note in :func:`~modin.pandas.read_snowflake`
        """
        return cls.query_compiler_cls.to_snowpark(
            index, index_label
        )  # pragma: no cover

    @classmethod
    def read_csv(
        cls,
        filepath_or_buffer: FilePath,
        *,
        sep: Optional[Union[str, NoDefault]] = READ_CSV_DEFAULTS["sep"],
        delimiter: Optional[str] = READ_CSV_DEFAULTS["delimiter"],
        header: Optional[
            Union[int, Sequence[int], Literal["infer"]]
        ] = READ_CSV_DEFAULTS["header"],
        names: Optional[Union[Sequence[Hashable], NoDefault]] = READ_CSV_DEFAULTS[
            "names"
        ],
        index_col: Optional[Union[IndexLabel, Literal[False]]] = READ_CSV_DEFAULTS[
            "index_col"
        ],
        usecols: Optional[Union[list[Hashable], Callable]] = READ_CSV_DEFAULTS[
            "usecols"
        ],
        dtype: Optional[DtypeArg] = READ_CSV_DEFAULTS["dtype"],
        engine: Optional[CSVEngine] = READ_CSV_DEFAULTS["engine"],
        converters: Optional[dict[Hashable, Callable]] = READ_CSV_DEFAULTS[
            "converters"
        ],
        true_values: Optional[list[Any]] = READ_CSV_DEFAULTS["true_values"],
        false_values: Optional[list[Any]] = READ_CSV_DEFAULTS["false_values"],
        skipinitialspace: Optional[bool] = READ_CSV_DEFAULTS["skipinitialspace"],
        skiprows: Optional[int] = READ_CSV_DEFAULTS["skiprows"],
        skipfooter: Optional[int] = READ_CSV_DEFAULTS["skipfooter"],
        nrows: Optional[int] = READ_CSV_DEFAULTS["nrows"],
        na_values: Optional[Sequence[Hashable]] = READ_CSV_DEFAULTS["na_values"],
        keep_default_na: Optional[bool] = READ_CSV_DEFAULTS["keep_default_na"],
        na_filter: Optional[bool] = READ_CSV_DEFAULTS["na_filter"],
        verbose: Optional[bool] = READ_CSV_DEFAULTS["verbose"],
        skip_blank_lines: Optional[bool] = READ_CSV_DEFAULTS["skip_blank_lines"],
        parse_dates: Optional[
            Union[
                bool, Sequence[int], Sequence[Sequence[int]], dict[str, Sequence[int]]
            ]
        ] = READ_CSV_DEFAULTS["parse_dates"],
        infer_datetime_format: Optional[bool] = READ_CSV_DEFAULTS[
            "infer_datetime_format"
        ],
        keep_date_col: Optional[bool] = READ_CSV_DEFAULTS["keep_date_col"],
        date_parser: Optional[Callable] = READ_CSV_DEFAULTS["date_parser"],
        date_format: Optional[Union[str, dict]] = READ_CSV_DEFAULTS["date_format"],
        dayfirst: Optional[bool] = READ_CSV_DEFAULTS["dayfirst"],
        cache_dates: Optional[bool] = READ_CSV_DEFAULTS["cache_dates"],
        iterator: bool = READ_CSV_DEFAULTS["iterator"],
        chunksize: Optional[int] = READ_CSV_DEFAULTS["chunksize"],
        compression: Literal[
            "infer", "gzip", "bz2", "brotli", "zstd", "deflate", "raw_deflate", "none"
        ] = READ_CSV_DEFAULTS["compression"],
        thousands: Optional[str] = READ_CSV_DEFAULTS["thousands"],
        decimal: Optional[str] = READ_CSV_DEFAULTS["decimal"],
        lineterminator: Optional[str] = READ_CSV_DEFAULTS["lineterminator"],
        quotechar: str = READ_CSV_DEFAULTS["quotechar"],
        quoting: Optional[int] = READ_CSV_DEFAULTS["quoting"],
        doublequote: bool = READ_CSV_DEFAULTS["doublequote"],
        escapechar: Optional[str] = READ_CSV_DEFAULTS["escapechar"],
        comment: Optional[str] = READ_CSV_DEFAULTS["comment"],
        encoding: Optional[str] = READ_CSV_DEFAULTS["encoding"],
        encoding_errors: Optional[str] = READ_CSV_DEFAULTS["encoding_errors"],
        dialect: Optional[Union[str, "csv.Dialect"]] = READ_CSV_DEFAULTS["dialect"],
        on_bad_lines: str = READ_CSV_DEFAULTS["on_bad_lines"],
        delim_whitespace: Optional[bool] = READ_CSV_DEFAULTS["delim_whitespace"],
        low_memory: Optional[bool] = READ_CSV_DEFAULTS[
            "low_memory"
        ],  # Different from default because we want better dtype detection
        memory_map: Optional[bool] = READ_CSV_DEFAULTS["memory_map"],
        float_precision: Optional[Literal["high", "legacy"]] = READ_CSV_DEFAULTS[
            "float_precision"
        ],
        storage_options: StorageOptions = READ_CSV_DEFAULTS["storage_options"],
        dtype_backend: DtypeBackend = READ_CSV_DEFAULTS["dtype_backend"],
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

        if not isinstance(filepath_or_buffer, str):
            raise NotImplementedError(
                "filepath_or_buffer must be a path to a file or folder stored locally or on a Snowflake stage."
            )

        if (
            filepath_or_buffer is not None
            and isinstance(filepath_or_buffer, str)
            and any(
                filepath_or_buffer.lower().startswith(prefix)
                for prefix in ["s3://", "s3china://", "s3gov://"]
            )
        ):
            session = get_active_session()
            temp_stage_name = random_name_for_temp_object(TempObjectType.STAGE)
            dirname = os.path.dirname(filepath_or_buffer)
            basename = os.path.basename(filepath_or_buffer)
            session.sql(
                f"CREATE OR REPLACE TEMPORARY STAGE {temp_stage_name} URL='{dirname}'"
            ).collect()
            filepath_or_buffer = (
                f"{STAGE_PREFIX}{os.path.join(temp_stage_name, basename)}"
            )

        if kwargs["engine"] != "snowflake" and is_local_filepath(filepath_or_buffer):
            return cls.query_compiler_cls.from_file_with_pandas("csv", **kwargs)

        WarningMessage.mismatch_with_pandas(
            "read_csv",
            "Staged files use the Snowflake CSV parser, which has different behavior than the pandas CSV parser",
        )
        _validate_read_staged_csv_and_read_table_args("pd.read_csv", **kwargs)

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
            or (skiprows != 0 and header is not READ_CSV_DEFAULTS["header"])
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

        return cls.query_compiler_cls.from_file_with_snowflake(
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
        typ: Optional[Literal["frame", "series"]] = "frame",
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
        error_not_implemented_parameter("typ", typ != "frame")
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

        return cls.query_compiler_cls.from_file_with_snowflake(
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
        return cls.query_compiler_cls.from_file_with_snowflake(
            "parquet", path, usecols=columns
        )

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
        pass  # pragma: no cover

    @classmethod
    def read_html(cls, **kwargs) -> list[SnowflakeQueryCompiler]:
        """
        Read HTML tables into a list of query compilers.
        """
        io = kwargs["io"]
        if is_snowflake_stage_path(io):
            with _file_from_stage(io) as local_filepath:
                kwargs["io"] = local_filepath
                # We have to return here because the temp file is deleted
                # after exiting this block
                return [cls.from_pandas(df) for df in pandas.read_html(**kwargs)]

        return [cls.from_pandas(df) for df in pandas.read_html(**kwargs)]

    @classmethod
    def read_xml(cls, **kwargs) -> SnowflakeQueryCompiler:
        """
        Read XML document into a query compiler.
        """
        path_or_buffer = kwargs["path_or_buffer"]
        if is_snowflake_stage_path(path_or_buffer):
            with _file_from_stage(path_or_buffer) as local_filepath:
                kwargs["path_or_buffer"] = local_filepath
                # We have to return here because the temp file is deleted
                # after exiting this block
                return cls.from_pandas(pandas.read_xml(**kwargs))

        return cls.from_pandas(pandas.read_xml(**kwargs))

    @classmethod
    @pandas_module_level_function_not_implemented()
    def read_clipboard(cls, sep=r"\s+", **kwargs):
        pass  # pragma: no cover

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
        pass  # pragma: no cover

    @classmethod
    def read_feather(cls, **kwargs) -> SnowflakeQueryCompiler:
        """
        Load a feather-format object from the file path into a query compiler.
        """
        path = kwargs["path"]
        if is_snowflake_stage_path(path):
            with _file_from_stage(path) as local_filepath:
                kwargs["path"] = local_filepath
                # We have to return here because the temp file is deleted
                # after exiting this block
                return cls.from_pandas(pandas.read_feather(**kwargs))

        return cls.from_pandas(pandas.read_feather(**kwargs))

    @classmethod
    def read_stata(cls, **kwargs) -> SnowflakeQueryCompiler:
        """
        Read Stata file into a query compiler.
        """
        filepath_or_buffer = kwargs["filepath_or_buffer"]
        if is_snowflake_stage_path(filepath_or_buffer):
            with _file_from_stage(filepath_or_buffer) as local_filepath:
                kwargs["filepath_or_buffer"] = local_filepath
                # We have to return here because the temp file is deleted
                # after exiting this block
                return cls.from_pandas(pandas.read_stata(**kwargs))

        return cls.from_pandas(pandas.read_stata(**kwargs))

    @classmethod
    def read_orc(cls, **kwargs) -> SnowflakeQueryCompiler:
        """
        Load an ORC object from the file path into a query compiler.
        """
        path = kwargs["path"]
        if is_snowflake_stage_path(path):
            with _file_from_stage(path) as local_filepath:
                kwargs["path"] = local_filepath
                # We have to return here because the temp file is deleted
                # after exiting this block
                return cls.from_pandas(pandas.read_orc(**kwargs))

        return cls.from_pandas(pandas.read_orc(**kwargs))

    @classmethod
    def read_sas(cls, **kwargs):  # noqa: PR01
        """
        Read SAS files stored as either XPORT or SAS7BDAT format files into a query compiler.
        """
        filepath_or_buffer = kwargs["filepath_or_buffer"]
        if is_snowflake_stage_path(filepath_or_buffer):
            with _file_from_stage(filepath_or_buffer) as local_filepath:
                kwargs["filepath_or_buffer"] = local_filepath
                # We have to return here because the temp file is deleted
                # after exiting this block
                return cls.from_pandas(pandas.read_sas(**kwargs))

        return cls.from_pandas(pandas.read_sas(**kwargs))

    @classmethod
    def read_pickle(cls, **kwargs) -> SnowflakeQueryCompiler:
        """
        Load pickled pandas object (or any object) from file into a query compiler.
        """
        filepath_or_buffer = kwargs["filepath_or_buffer"]
        if is_snowflake_stage_path(filepath_or_buffer):
            with _file_from_stage(filepath_or_buffer) as local_filepath:
                kwargs["filepath_or_buffer"] = local_filepath
                # We have to return here because the temp file is deleted
                # after exiting this block
                return cls.from_pandas(pandas.read_pickle(**kwargs))

        return cls.from_pandas(pandas.read_pickle(**kwargs))

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
        pass  # pragma: no cover

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
        pass  # pragma: no cover

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
        pass  # pragma: no cover

    @classmethod
    @pandas_module_level_function_not_implemented()
    def read_sql_query(
        cls,
        sql,
        con,
        **kwargs,
    ):
        pass  # pragma: no cover

    @classmethod
    @pandas_module_level_function_not_implemented()
    def read_spss(cls, path, usecols, convert_categoricals, dtype_backend):
        pass  # pragma: no cover

    @classmethod
    @pandas_module_level_function_not_implemented()
    def to_pickle(
        cls,
        obj,
        filepath_or_buffer,
        **kwargs,
    ):
        pass  # pragma: no cover

    @classmethod
    def to_csv(cls, obj, **kwargs) -> Optional[str]:
        """
        Write object to a comma-separated values (CSV) file using pandas.

        For parameters description please refer to pandas API.
        """
        path_or_buf = kwargs.get("path_or_buf", None)
        # Use snowflake DataFrameWriter if write location is snowflake stage.
        if is_snowflake_stage_path(path_or_buf) and isinstance(
            obj, SnowflakeQueryCompiler
        ):
            return obj.to_csv_with_snowflake(**kwargs)
        # Default to base implementation for local path.
        return BaseIO.to_csv(obj, **kwargs)
