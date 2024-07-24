#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import inspect
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Hashable,
    Literal,
    Optional,
    Sequence,
    Union,
)

import pandas as native_pd
from pandas._libs.lib import NoDefault, no_default
from pandas._typing import (
    CSVEngine,
    DtypeArg,
    DtypeBackend,
    FilePath,
    IndexLabel,
    StorageOptions,
)

import snowflake.snowpark.modin.pandas as pd
from snowflake.snowpark.modin.pandas import DataFrame
from snowflake.snowpark.modin.pandas.api.extensions import register_pd_accessor
from snowflake.snowpark.modin.plugin._internal.telemetry import (
    snowpark_pandas_telemetry_standalone_function_decorator,
)
from snowflake.snowpark.modin.plugin.io.snow_io import (
    READ_CSV_DEFAULTS,
    PandasOnSnowflakeIO,
)
from snowflake.snowpark.modin.utils import _inherit_docstrings

if TYPE_CHECKING:  # pragma: no cover
    import csv

from snowflake.snowpark.modin.plugin.extensions.index import Index  # noqa: F401

register_pd_accessor("Index")(Index)


@_inherit_docstrings(native_pd.read_csv, apilink="pandas.read_csv")
@register_pd_accessor("read_csv")
@snowpark_pandas_telemetry_standalone_function_decorator
def read_csv(
    filepath_or_buffer: FilePath,
    *,
    sep: Optional[Union[str, NoDefault]] = READ_CSV_DEFAULTS["sep"],
    delimiter: Optional[str] = READ_CSV_DEFAULTS["delimiter"],
    header: Optional[Union[int, Sequence[int], Literal["infer"]]] = READ_CSV_DEFAULTS[
        "header"
    ],
    names: Optional[Union[Sequence[Hashable], NoDefault]] = READ_CSV_DEFAULTS["names"],
    index_col: Optional[Union[IndexLabel, Literal[False]]] = READ_CSV_DEFAULTS[
        "index_col"
    ],
    usecols: Optional[Union[list[Hashable], Callable]] = READ_CSV_DEFAULTS["usecols"],
    dtype: Optional[DtypeArg] = READ_CSV_DEFAULTS["dtype"],
    engine: Optional[CSVEngine] = READ_CSV_DEFAULTS["engine"],
    converters: Optional[dict[Hashable, Callable]] = READ_CSV_DEFAULTS["converters"],
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
        Union[bool, Sequence[int], Sequence[Sequence[int]], dict[str, Sequence[int]]]
    ] = READ_CSV_DEFAULTS["parse_dates"],
    infer_datetime_format: Optional[bool] = READ_CSV_DEFAULTS["infer_datetime_format"],
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
) -> DataFrame:
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
    SnowparkPandasDataFrame

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
    >>> import snowflake.snowpark.modin.pandas as pd
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
@snowpark_pandas_telemetry_standalone_function_decorator
def read_json(
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
) -> DataFrame:
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

    >>> import snowflake.snowpark.modin.pandas as pd
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
@snowpark_pandas_telemetry_standalone_function_decorator
def read_parquet(
    path: FilePath,
    engine: Optional[str] = None,
    columns: Optional[list[str]] = None,
    storage_options: StorageOptions = None,
    use_nullable_dtypes: Union[bool, NoDefault] = no_default,
    dtype_backend: Union[DtypeBackend, NoDefault] = no_default,
    filesystem: str = None,
    filters: Optional[Union[list[tuple], list[list[tuple]]]] = None,
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
