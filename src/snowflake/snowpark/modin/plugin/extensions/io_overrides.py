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
    """
    Normalize semi-structured JSON data into a flat table.
    """
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
    """
    Load an ORC object from the file path, returning a DataFrame.
    """
    # TODO(https://github.com/modin-project/modin/issues/7104):
    # modin needs to remove defaults to pandas at API layer
    pass  # pragma: no cover


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
    """
    Read an Excel file into a Snowpark pandas DataFrame.
    """
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
    """
    Read csv file(s) into a Snowpark pandas DataFrame. This API can read
    files stored locally or on a Snowflake stage.

    Snowpark pandas stages files (unless they're already staged)
    and then reads them using Snowflake's CSV reader.

    Parameters
    ----------
    filepath_or_buffer : str
        Local file location or staged file location to read from. Staged file locations
        starts with a '@' symbol. To read a local file location with a name starting with `@`,
        escape it using a `\\@`. For more info on staged files, `read here
        <https://docs.snowflake.com/en/sql-reference/sql/create-stage>`_.
    sep : str, default ','
        Delimiter to use to separate fields in an input file. Delimiters can be
        multiple characters in Snowpark pandas.
    delimiter : str, default ','
        Alias for sep.
    header : int, list of int, None, default 'infer'
        Row number(s) to use as the column names, and the start of the
        data.  Default behavior is to infer the column names: if no names
        are passed the behavior is identical to ``header=0`` and column
        names are inferred from the first line of the file, if column
        names are passed explicitly then the behavior is identical to
        ``header=None``. Explicitly pass ``header=0`` to be able to
        replace existing names. If a non-zero integer or a list of integers is passed,
        a ``NotImplementedError`` will be raised.
    names : array-like, optional
        List of column names to use. If the file contains a header row,
        then you should explicitly pass ``header=0`` to override the column names.
        Duplicates in this list are not allowed.
    index_col: int, str, sequence of int / str, or False, optional, default ``None``
        Column(s) to use as the row labels of the ``DataFrame``, either given as
        string name or column index. If a sequence of int / str is given, a
        MultiIndex is used.
        Note: ``index_col=False`` can be used to force pandas to *not* use the first
        column as the index, e.g. when you have a malformed file with delimiters at
        the end of each line.
    usecols : list-like or callable, optional
        Return a subset of the columns. If list-like, all elements must either
        be positional (i.e. integer indices into the document columns) or strings
        that correspond to column names provided either by the user in `names` or
        inferred from the document header row(s). If ``names`` are given, the document
        header row(s) are not taken into account. For example, a valid list-like
        `usecols` parameter would be ``[0, 1, 2]`` or ``['foo', 'bar', 'baz']``.
        Element order is ignored, so ``usecols=[0, 1]`` is the same as ``[1, 0]``.
        To instantiate a DataFrame from ``data`` with element order preserved use
        ``pd.read_csv(data, usecols=['foo', 'bar'])[['foo', 'bar']]`` for columns
        in ``['foo', 'bar']`` order or
        ``pd.read_csv(data, usecols=['foo', 'bar'])[['bar', 'foo']]``
        for ``['bar', 'foo']`` order.

        If callable, the callable function will be evaluated against the column
        names, returning names where the callable function evaluates to True. An
        example of a valid callable argument would be ``lambda x: x.upper() in
        ['AAA', 'BBB', 'DDD']``.
    dtype : Type name or dict of column -> type, optional
        Data type for data or columns. E.g. {{'a': np.float64, 'b': np.int32,
        'c': 'Int64'}}
        Use `str` or `object` together with suitable `na_values` settings
        to preserve and not interpret dtype.
        If converters are specified, they will be applied INSTEAD
        of dtype conversion.
    engine : {{'c', 'python', 'pyarrow', 'snowflake'}}, optional
        Changes the parser for reading CSVs. 'snowflake' will use the parser
        from Snowflake itself, which matches the behavior of the COPY INTO
        command.
    converters : dict, optional
       This parameter is only supported on local files.
    true_values : list, optional
       This parameter is only supported on local files.
    false_values : list, optional
       This parameter is only supported on local files.
    skiprows: list-like, int or callable, optional
        Line numbers to skip (0-indexed) or number of lines to skip (int)
        at the start of the file.
    skipfooter : int, default 0
       This parameter is only supported on local files.
    nrows : int, optional
       This parameter is only supported on local files.
    na_values : scalar, str, list-like, or dict, optional
        Additional strings to recognize as NA/NaN.
    keep_default_na : bool, default True
       This parameter is only supported on local files.
    na_filter : bool, default True
       This parameter is only supported on local files.
    verbose : bool, default False
       This parameter is only supported on local files.
    skip_blank_lines : bool, default True
        If True, skip over blank lines rather than interpreting as NaN values.
    parse_dates : bool or list of int or names or list of lists or dict, default False
       This parameter is only supported on local files.
    infer_datetime_format : bool, default False
       This parameter is only supported on local files.
    keep_date_col : bool, default False
       This parameter is only supported on local files.
    date_parser : function, optional
       This parameter is only supported on local files.
    date_format : str or dict of column -> format, optional
       This parameter is only supported on local files.
    dayfirst : bool, default False
       This parameter is only supported on local files.
    cache_dates : bool, default True
        This parameter is not supported and will be ignored.
    iterator : bool, default False
        This parameter is not supported and will raise an error.
    chunksize : int, optional
        This parameter is not supported and will be ignored.
    compression: str, default 'infer'
        String (constant) that specifies the current compression algorithm for the
        data files to be loaded. Snowflake uses this option to detect how already-compressed
        data files were compressed so that the compressed data in the files
        can be extracted for loading.
        `List of Snowflake standard compressions
        <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table#format-type-options-formattypeoptions>`_ .
    thousands : str, optional
       This parameter is only supported on local files.
    decimal : str, default '.'
       This parameter is only supported on local files.
    lineterminator : str (length 1), optional
       This parameter is only supported on local files.
    quotechar : str (length 1), optional
        The character used to denote the start and end of a quoted item. Quoted
        items can include the delimiter and it will be ignored.
    quoting : int or csv.QUOTE_* instance, default 0
       This parameter is only supported on local files.
    doublequote : bool, default ``True``
       This parameter is only supported on local files.
    escapechar : str (length 1), optional
       This parameter is only supported on local files.
    comment : str, optional
       This parameter is only supported on local files.
    encoding : str, default 'utf-8'
        Encoding to use for UTF when reading/writing (ex. 'utf-8'). `List of Snowflake
        standard encodings <https://docs.snowflake.com/en/sql-reference/sql/copy-into-tables>`_ .
    encoding_errors : str, optional, default "strict"
       This parameter is only supported on local files.
    dialect : str or csv.Dialect, optional
       This parameter is only supported on local files.
    on_bad_lines : {{'error', 'warn', 'skip'}} or callable, default 'error'
       This parameter is only supported on local files.
    delim_whitespace : bool, default False
       This parameter is only supported on local files, not files which have been
       uploaded to a snowflake stage.
    low_memory : bool, default True
        This parameter is not supported and will be ignored.
    memory_map : bool, default False
        This parameter is not supported and will be ignored.
    float_precision : str, optional
        This parameter is not supported and will be ignored.
    dtype_backend : {'numpy_nullable', 'pyarrow'}, default 'numpy_nullable'
        This parameter is not supported and will be ignored.

    Returns
    -------
    Snowpark pandas DataFrame

    Raises
    ------
    NotImplementedError if a parameter is not supported.

    Notes
    -----
    Both local files and files staged on Snowflake can be passed into
    ``filepath_or_buffer``. A single file or a folder that matches
    a set of files can be passed into ``filepath_or_buffer``. Local files
    will be processed locally by default using the stand pandas parser
    before they are uploaded to a staging location as parquet files. This
    behavior can be overriden by explicitly using the snowflake engine
    with ``engine=snowflake``

    If parsing the file using Snowflake, certain parameters may not be supported
    and the order of rows in the dataframe may be different than the order of
    records in an input file. When reading multiple files, there is no
    deterministic order in which the files are read.

    Examples
    --------
    Read local csv file.

    >>> import csv
    >>> import tempfile
    >>> temp_dir = tempfile.TemporaryDirectory()
    >>> temp_dir_name = temp_dir.name
    >>> with open(f'{temp_dir_name}/data.csv', 'w') as f:
    ...     writer = csv.writer(f)
    ...     writer.writerows([['c1','c2','c3'], [1,2,3], [4,5,6], [7,8,9]])
    >>> import modin.pandas as pd
    >>> import snowflake.snowpark.modin.plugin
    >>> df = pd.read_csv(f'{temp_dir_name}/data.csv')
    >>> df
       c1  c2  c3
    0   1   2   3
    1   4   5   6
    2   7   8   9

    Read staged csv file.

    >>> _ = session.sql("create or replace temp stage mytempstage").collect()
    >>> _ = session.file.put(f'{temp_dir_name}/data.csv', '@mytempstage/myprefix')
    >>> df2 = pd.read_csv('@mytempstage/myprefix/data.csv')
    >>> df2
       c1  c2  c3
    0   1   2   3
    1   4   5   6
    2   7   8   9

    Read csv files from a local folder.

    >>> with open(f'{temp_dir_name}/data2.csv', 'w') as f:
    ...     writer = csv.writer(f)
    ...     writer.writerows([['c1','c2','c3'], [1,2,3], [4,5,6], [7,8,9]])
    >>> df3 = pd.read_csv(f'{temp_dir_name}/data2.csv')
    >>> df3
       c1  c2  c3
    0   1   2   3
    1   4   5   6
    2   7   8   9

    Read csv files from a staged location.

    >>> _ = session.file.put(f'{temp_dir_name}/data2.csv', '@mytempstage/myprefix')
    >>> df4 = pd.read_csv('@mytempstage/myprefix')
    >>> df4
       c1  c2  c3
    0   1   2   3
    1   4   5   6
    2   7   8   9
    3   1   2   3
    4   4   5   6
    5   7   8   9

    >>> temp_dir.cleanup()
    """
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
    """
    Read new-line delimited json file(s) into a Snowpark pandas DataFrame. This API can read
    files stored locally or on a Snowflake stage.

    Snowpark pandas first stages files (unless they're already staged)
    and then reads them using Snowflake's JSON reader.

    Parameters
    ----------
    path_or_buf : str
        Local file location or staged file location to read from. Staged file locations
        starts with a '@' symbol. To read a local file location with a name starting with `@`,
        escape it using a `\\@`. For more info on staged files, `read here
        <https://docs.snowflake.com/en/sql-reference/sql/create-stage>`_.

    orient : str
        This parameter is not supported and will raise an error.

    typ : {{'frame', 'series'}}, default 'frame'
        This parameter is not supported and will raise an error.

    dtype : bool or dict, default None
        This parameter is not supported and will raise an error.

    convert_axes : bool, default None
        This parameter is not supported and will raise an error.

    convert_dates : bool or list of str, default True
        This parameter is not supported and will raise an error.

    keep_default_dates : bool, default True
        This parameter is not supported and will raise an error.

    precise_float : bool, default False
        This parameter is not supported and will be ignored.

    date_unit : str, default None
        This parameter is not supported and will raise an error.

    encoding : str, default is 'utf-8'
        Encoding to use for UTF when reading/writing (ex. 'utf-8'). `List of Snowflake
        standard encodings <https://docs.snowflake.com/en/sql-reference/sql/copy-into-tables>`_ .

    encoding_errors : str, optional, default "strict"
        This parameter is not supported and will raise an error.

    lines : bool, default False
        This parameter is not supported and will raise an error.

    chunksize : int, optional
        This parameter is not supported and will raise an error.

    compression : str, default 'infer'
        String (constant) that specifies the current compression algorithm for the
        data files to be loaded. Snowflake uses this option to detect how already-compressed
        data files were compressed so that the compressed data in the files
        can be extracted for loading.
        `List of Snowflake standard compressions
        <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table#format-type-options-formattypeoptions>`_ .

    nrows : int, optional
        This parameter is not supported and will raise an error.

    storage_options : dict, optional
        This parameter is not supported and will be ignored.

    dtype_backend : {'numpy_nullable', 'pyarrow'}, default 'numpy_nullable'
        This parameter is not supported and will be ignored.

    engine : {'ujson', 'pyarrow'}, default 'ujson'
        This parameter is not supported and will be ignored.

    Returns
    -------
    Snowpark pandas DataFrame

    Raises
    ------
    NotImplementedError if a parameter is not supported.

    Notes
    -----
    Both local files and files staged on Snowflake can be passed into
    ``path_or_buf``. A single file or a folder that matches
    a set of files can be passed into ``path_or_buf``. There is no deterministic order
    in which the files are read.

    Examples
    --------

    Read local json file.

    >>> import tempfile
    >>> import json
    >>> temp_dir = tempfile.TemporaryDirectory()
    >>> temp_dir_name = temp_dir.name

    >>> data = {'A': "snowpark!", 'B': 3, 'C': [5, 6]}
    >>> with open(f'{temp_dir_name}/snowpark_pandas.json', 'w') as f:
    ...     json.dump(data, f)

    >>> import modin.pandas as pd
    >>> import snowflake.snowpark.modin.plugin
    >>> df = pd.read_json(f'{temp_dir_name}/snowpark_pandas.json')
    >>> df
               A  B       C
    0  snowpark!  3  [5, 6]

    Read staged json file.

    >>> _ = session.sql("create or replace temp stage mytempstage").collect()
    >>> _ = session.file.put(f'{temp_dir_name}/snowpark_pandas.json', '@mytempstage/myprefix')
    >>> df2 = pd.read_json('@mytempstage/myprefix/snowpark_pandas.json')
    >>> df2
               A  B       C
    0  snowpark!  3  [5, 6]

    Read json files from a local folder.

    >>> with open(f'{temp_dir_name}/snowpark_pandas2.json', 'w') as f:
    ...     json.dump(data, f)
    >>> df3 = pd.read_json(f'{temp_dir_name}')
    >>> df3
               A  B       C
    0  snowpark!  3  [5, 6]
    1  snowpark!  3  [5, 6]

    Read json files from a staged location.

    >>> _ = session.file.put(f'{temp_dir_name}/snowpark_pandas2.json', '@mytempstage/myprefix')
    >>> df4 = pd.read_json('@mytempstage/myprefix')
    >>> df4
               A  B       C
    0  snowpark!  3  [5, 6]
    1  snowpark!  3  [5, 6]
    """
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
    """
    Read parquet file(s) into a Snowpark pandas DataFrame. This API can read
    files stored locally or on a Snowflake stage.

    Snowpark pandas stages files (unless they're already staged)
    and then reads them using Snowflake's parquet reader.

    Parameters
    ----------
    path : str
        Local file location or staged file location to read from. Staged file locations
        starts with a '@' symbol. To read a local file location with a name starting with `@`,
        escape it using a `\\@`. For more info on staged files, `read here
        <https://docs.snowflake.com/en/sql-reference/sql/create-stage>`_.

    engine : {{'auto', 'pyarrow', 'fastparquet'}}, default None
        This parameter is not supported and will be ignored.

    storage_options : StorageOptions, default None
        This parameter is not supported and will be ignored.

    columns : list, default None
        If not None, only these columns will be read from the file.

    use_nullable_dtypes : bool, default False
        This parameter is not supported and will raise an error.

    dtype_backend : {'numpy_nullable', 'pyarrow'}, default 'numpy_nullable'
        This parameter is not supported and will be ignored.

    filesystem : fsspec or pyarrow filesystem, default None
        This parameter is not supported and will be ignored.

    filters : List[Tuple] or List[List[Tuple]], default None
        This parameter is not supported and will be ignored.

    **kwargs : Any, default None
        This parameter is not supported and will be ignored.

    Returns
    -------
    Snowpark pandas DataFrame

    Raises
    ------
    NotImplementedError if a parameter is not supported.

    Notes
    -----
    Both local files and files staged on Snowflake can be passed into
    ``path``. A single file or a folder that matches
    a set of files can be passed into ``path``. The order of rows in the
    dataframe may be different from the order of records in an input file. When reading
    multiple files, there is no deterministic order in which the files are read.

    Examples
    --------

    Read local parquet file.

    >>> import pandas as native_pd
    >>> import tempfile
    >>> temp_dir = tempfile.TemporaryDirectory()
    >>> temp_dir_name = temp_dir.name

    >>> df = native_pd.DataFrame(
    ...     {"foo": range(3), "bar": range(5, 8)}
    ...    )
    >>> df
       foo  bar
    0    0    5
    1    1    6
    2    2    7

    >>> _ = df.to_parquet(f'{temp_dir_name}/snowpark-pandas.parquet')
    >>> restored_df = pd.read_parquet(f'{temp_dir_name}/snowpark-pandas.parquet')
    >>> restored_df
       foo  bar
    0    0    5
    1    1    6
    2    2    7

    >>> restored_bar = pd.read_parquet(f'{temp_dir_name}/snowpark-pandas.parquet', columns=["bar"])
    >>> restored_bar
       bar
    0    5
    1    6
    2    7

    Read staged parquet file.

    >>> _ = session.sql("create or replace temp stage mytempstage").collect()
    >>> _ = session.file.put(f'{temp_dir_name}/snowpark-pandas.parquet', '@mytempstage/myprefix')
    >>> df2 = pd.read_parquet('@mytempstage/myprefix/snowpark-pandas.parquet')
    >>> df2
       foo  bar
    0    0    5
    1    1    6
    2    2    7

    Read parquet files from a local folder.

    >>> _ = df.to_parquet(f'{temp_dir_name}/snowpark-pandas2.parquet')
    >>> df3 = pd.read_parquet(f'{temp_dir_name}')
    >>> df3
       foo  bar
    0    0    5
    1    1    6
    2    2    7
    3    0    5
    4    1    6
    5    2    7

    Read parquet files from a staged location.

    >>> _ = session.file.put(f'{temp_dir_name}/snowpark-pandas2.parquet', '@mytempstage/myprefix')
    >>> df3 = pd.read_parquet('@mytempstage/myprefix')
    >>> df3
       foo  bar
    0    0    5
    1    1    6
    2    2    7
    3    0    5
    4    1    6
    5    2    7
    """
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
    """
    Read SAS files stored as either XPORT or SAS7BDAT format files.

    Parameters
    ----------

    filepath_or_buffer : str, path object, or file-like object
        String, path object (implementing os.PathLike[str]), or file-like object implementing a binary read() function. The string could be a URL. Valid URL schemes include http, ftp, s3, and file. For file URLs, a host is expected. A local file could be: file://localhost/path/to/table.sas7bdat.
    format : str {‘xport’, ‘sas7bdat’} or None
        If None, file format is inferred from file extension. If ‘xport’ or ‘sas7bdat’, uses the corresponding format.
    index : identifier of index column, defaults to None
        Identifier of column that should be used as index of the DataFrame.
    encoding : str, default is None
        Encoding for text data. If None, text data are stored as raw bytes.
    chunksize : int
        Read file chunksize lines at a time, returns iterator.
    iterator : bool, defaults to False
        If True, returns an iterator for reading the file incrementally.
    compression : str or dict, default ‘infer’
        For on-the-fly decompression of on-disk data. If ‘infer’ and ‘filepath_or_buffer’ is path-like, then detect compression from the following extensions: ‘.gz’, ‘.bz2’, ‘.zip’, ‘.xz’, ‘.zst’, ‘.tar’, ‘.tar.gz’, ‘.tar.xz’ or ‘.tar.bz2’ (otherwise no compression). If using ‘zip’ or ‘tar’, the ZIP file must contain only one data file to be read in. Set to None for no decompression. Can also be a dict with key 'method' set to one of {'zip', 'gzip', 'bz2', 'zstd', 'xz', 'tar'} and other key-value pairs are forwarded to zipfile.ZipFile, gzip.GzipFile, bz2.BZ2File, zstandard.ZstdDecompressor, lzma.LZMAFile or tarfile.TarFile, respectively. As an example, the following could be passed for Zstandard decompression using a custom compression dictionary: compression={'method': 'zstd', 'dict_data': my_compression_dict}.

    Returns
    -------
    DataFrame if iterator=False and chunksize=None, else SAS7BDATReader
    or XportReader

    Examples
    --------
    >>> df = pd.read_sas("sas_data.sas7bdat")  # doctest: +SKIP
    """
    _pd_read_sas_signature = {
        val.name for val in inspect.signature(native_pd.read_sas).parameters.values()
    }
    _, _, _, f_locals = inspect.getargvalues(inspect.currentframe())
    kwargs = {k: v for k, v in f_locals.items() if k in _pd_read_sas_signature}

    return pd.DataFrame(query_compiler=PandasOnSnowflakeIO.read_sas(**kwargs))
