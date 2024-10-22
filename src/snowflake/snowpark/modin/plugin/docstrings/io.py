#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""This module contains I/O top-level pandas docstrings that override modin's docstrings."""


def json_normalize():
    """
    Normalize semi-structured JSON data into a flat table.
    """


def read_orc():
    """
    Load an ORC object from the file path, returning a DataFrame.
    """


def read_excel():
    """
    Read an Excel file into a Snowpark pandas DataFrame.
    """


def read_csv():
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


def read_json():
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


def read_parquet():
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


def read_sas():
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
