#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""This module contains I/O top-level pandas docstrings that override modin's docstrings."""


def read_pickle():
    """
    Load pickled pandas object (or any object) from file and return unpickled object.

    This API can read files stored locally or on a Snowflake stage.

    Warning
    -------
    Loading pickled data received from untrusted sources can be unsafe. See `here <https://docs.python.org/3/library/pickle.html>`_.

    Parameters
    ----------
    filepath_or_buffer : str, path object, or file-like object
        String, path object (implementing os.PathLike[str]), or file-like object implementing a binary readlines() function. Also accepts URL. URL is not limited to S3 and GCS.
        Staged file locations start with the '@' symbol. To read a local file location with a name starting with `@`,
        escape it using a `\\@`. For more info on staged files, `read here
        <https://docs.snowflake.com/en/sql-reference/sql/create-stage>`_.
    compression : str or dict, default ‘infer’
        For on-the-fly decompression of on-disk data. If ‘infer’ and ‘filepath_or_buffer’ is path-like, then detect compression from the following extensions: ‘.gz’, ‘.bz2’, ‘.zip’, ‘.xz’, ‘.zst’, ‘.tar’, ‘.tar.gz’, ‘.tar.xz’ or ‘.tar.bz2’ (otherwise no compression). If using ‘zip’ or ‘tar’, the ZIP file must contain only one data file to be read in. Set to None for no decompression. Can also be a dict with key 'method' set to one of {'zip', 'gzip', 'bz2', 'zstd', 'xz', 'tar'} and other key-value pairs are forwarded to zipfile.ZipFile, gzip.GzipFile, bz2.BZ2File, zstandard.ZstdDecompressor, lzma.LZMAFile or tarfile.TarFile, respectively. As an example, the following could be passed for Zstandard decompression using a custom compression dictionary: compression={'method': 'zstd', 'dict_data': my_compression_dict}.
    storage_options : dict, optional
        Extra options that make sense for a particular storage connection, e.g. host, port, username, password, etc. For HTTP(S) URLs the key-value pairs are forwarded to urllib.request.Request as header options. For other URLs (e.g. starting with “s3://”, and “gcs://”) the key-value pairs are forwarded to fsspec.open. Please see fsspec and urllib for more details, and for more examples on storage options refer here.

    Returns
    -------
    object
        The unpickled pandas object (or any object) that was stored in file.

    See also
    --------
    DataFrame.to_pickle
        Pickle (serialize) DataFrame object to file.
    Series.to_pickle
        Pickle (serialize) Series object to file.
    read_hdf
        Read HDF5 file into a DataFrame.
    read_sql
        Read SQL query or database table into a DataFrame.
    read_parquet
        Load a parquet object, returning a DataFrame.

    Notes
    -----
    read_pickle is only guaranteed to be backwards compatible to pandas 1.0 provided the object was serialized with to_pickle.

    Examples
    --------
    >>> original_df = pd.DataFrame(
    ...     {"foo": range(5), "bar": range(5, 10)}
    ... )
    >>> original_df
       foo  bar
    0    0    5
    1    1    6
    2    2    7
    3    3    8
    4    4    9
    >>> pd.to_pickle(original_df, "./dummy.pkl")  # doctest: +SKIP

    >>> unpickled_df = pd.read_pickle("./dummy.pkl")  # doctest: +SKIP
    >>> unpickled_df  # doctest: +SKIP
       foo  bar
    0    0    5
    1    1    6
    2    2    7
    3    3    8
    4    4    9
    """


def read_html():
    """
    Read HTML tables into a list of DataFrame objects.

    This API can read files stored locally or on a Snowflake stage.

    Parameters
    ----------
    io : str, path object, or file-like object
        String, path object (implementing os.PathLike[str]), or file-like object implementing a string read() function. The string can represent a URL. Note that lxml only accepts the http, ftp and file url protocols. If you have a URL that starts with 'https' you might try removing the 's'.
        Staged file locations start with the '@' symbol. To read a local file location with a name starting with `@`,
        escape it using a `\\@`. For more info on staged files, `read here
        <https://docs.snowflake.com/en/sql-reference/sql/create-stage>`_.
    match : str or compiled regular expression, optional
        The set of tables containing text matching this regex or string will be returned. Unless the HTML is extremely simple you will probably need to pass a non-empty string here. Defaults to ‘.+’ (match any non-empty string). The default value will return all tables contained on a page. This value is converted to a regular expression so that there is consistent behavior between Beautiful Soup and lxml.
    flavor : {“lxml”, “html5lib”, “bs4”} or list-like, optional
        The parsing engine (or list of parsing engines) to use. ‘bs4’ and ‘html5lib’ are synonymous with each other, they are both there for backwards compatibility. The default of None tries to use lxml to parse and if that fails it falls back on bs4 + html5lib.
    header : int or list-like, optional
        The row (or list of rows for a MultiIndex) to use to make the columns headers.
    index_col : int or list-like, optional
        The column (or list of columns) to use to create the index.
    skiprows : int, list-like or slice, optional
        Number of rows to skip after parsing the column integer. 0-based. If a sequence of integers or a slice is given, will skip the rows indexed by that sequence. Note that a single element sequence means ‘skip the nth row’ whereas an integer means ‘skip n rows’.
    attrs : dict, optional
        This is a dictionary of attributes that you can pass to use to identify the table in the HTML. These are not checked for validity before being passed to lxml or Beautiful Soup. However, these attributes must be valid HTML table attributes to work correctly. For example,
        attrs = {"id": "table"}
        is a valid attribute dictionary because the ‘id’ HTML tag attribute is a valid HTML attribute for any HTML tag as per `this document <https://html.spec.whatwg.org/multipage/dom.html#global-attributes>`_.
        attrs = {"asdf": "table"}
        is not a valid attribute dictionary because ‘asdf’ is not a valid HTML attribute even if it is a valid XML attribute. Valid HTML 4.01 table attributes can be `found here
        <http://www.w3.org/TR/REC-html40/struct/tables.html#h-11.2>`_. A working draft of the HTML 5 spec can be found `here
        <https://html.spec.whatwg.org/multipage/tables.html>`_. It contains the latest information on table attributes for the modern web.
    parse_dates : bool, optional
        See read_csv() for more details.
    thousands : str, optional
        Separator to use to parse thousands. Defaults to ','.
    encoding : str, optional
        The encoding used to decode the web page. Defaults to ``None``.``None`` preserves the previous encoding behavior, which depends on the underlying parser library (e.g., the parser library will try to use the encoding provided by the document).
    decimal : str, default ‘.’
        Character to recognize as decimal point (e.g. use ‘,’ for European data).
    converters : dict, default None
        Dict of functions for converting values in certain columns. Keys can either be integers or column labels, values are functions that take one input argument, the cell (not column) content, and return the transformed content.
    na_values : iterable, default None
        Custom NA values.
    keep_default_na : bool, default True
        If na_values are specified and keep_default_na is False the default NaN values are overridden, otherwise they’re appended to.
    displayed_only : bool, default True
        Whether elements with “display: none” should be parsed.
    extract_links : {None, “all”, “header”, “body”, “footer”}
        Table elements in the specified section(s) with <a> tags will have their href extracted.
    dtype_backend : {‘numpy_nullable’, ‘pyarrow’}
        Back-end data type applied to the resultant DataFrame (still experimental). If not specified, the default behavior is to not use nullable data types. If specified, the behavior is as follows:
        - "numpy_nullable": returns nullable-dtype-backed DataFrame
        - "pyarrow": returns pyarrow-backed nullable ArrowDtype DataFrame
    storage_options : dict, optional
        Extra options that make sense for a particular storage connection, e.g. host, port, username, password, etc. For HTTP(S) URLs the key-value pairs are forwarded to urllib.request.Request as header options. For other URLs (e.g. starting with “s3://”, and “gcs://”) the key-value pairs are forwarded to fsspec.open. Please see fsspec and urllib for more details, and for more examples on storage options refer here.

    Returns
    -------
    dfs
        A list of DataFrames.

    See also
    --------
    read_csv
        Read a comma-separated values (csv) file into DataFrame.

    Notes
    -----
    Before using this function you should read the `gotchas about the HTML parsing libraries <https://pandas.pydata.org/docs/dev/user_guide/io.html#io-html-gotchas>`_.

    Expect to do some cleanup after you call this function. For example, you might need to manually assign column names if the column names are converted to NaN when you pass the header=0 argument. We try to assume as little as possible about the structure of the table and push the idiosyncrasies of the HTML contained in the table to the user.

    This function searches for <table> elements and only for <tr> and <th> rows and <td> elements within each <tr> or <th> element in the table. <td> stands for “table data”. This function attempts to properly handle colspan and rowspan attributes. If the function has a <thead> argument, it is used to construct the header, otherwise the function attempts to find the header within the body (by putting rows with only <th> elements into the header).

    Similar to read_csv() the header argument is applied after skiprows is applied.

    This function will always return a list of DataFrame or it will fail, i.e., it will not return an empty list, save for some rare cases. It might return an empty list in case of inputs with single row and <td> containing only whitespaces.

    Examples
    --------
        See the `read_html documentation in the IO section of the docs <https://pandas.pydata.org/docs/dev/user_guide/io.html#io-read-html>`_ for some examples of reading in HTML tables.
    """


def read_xml():
    r"""
    Read XML document into a DataFrame object.

    This API can read files stored locally or on a Snowflake stage.

    Parameters
    ----------
    path_or_buffer : str, path object, or file-like object
        String, path object (implementing ``os.PathLike[str]``), or file-like object implementing a ``read()`` function. The string can be a path. The string can further be a URL. Valid URL schemes include http, ftp, s3, and file.
        Staged file locations start with the '@' symbol. To read a local file location with a name starting with `@`,
        escape it using a `\\@`. For more info on staged files, `read here
        <https://docs.snowflake.com/en/sql-reference/sql/create-stage>`_.

    xpath : str, optional, default ‘./\*’
        The XPath to parse required set of nodes for migration to DataFrame.``XPath`` should return a collection of elements and not a single element. Note: The etree parser supports limited XPath expressions. For more complex XPath, use lxml which requires installation.
    namespaces : dict, optional
        The namespaces defined in XML document as dicts with key being namespace prefix and value the URI. There is no need to include all namespaces in XML, only the ones used in xpath expression. Note: if XML document uses default namespace denoted as xmlns=’<URI>’ without a prefix, you must assign any temporary namespace prefix such as ‘doc’ to the URI in order to parse underlying nodes and/or attributes.
    elems_only : bool, optional, default False
        Parse only the child elements at the specified xpath. By default, all child elements and non-empty text nodes are returned.
    attrs_only : bool, optional, default False
        Parse only the attributes at the specified xpath. By default, all attributes are returned.
    names : list-like, optional
        Column names for DataFrame of parsed XML data. Use this parameter to rename original element names and distinguish same named elements and attributes.
    dtype : Type name or dict of column -> type, optional
        Data type for data or columns. E.g. {‘a’: np.float64, ‘b’: np.int32, ‘c’: ‘Int64’} Use str or object together with suitable na_values settings to preserve and not interpret dtype. If converters are specified, they will be applied INSTEAD of dtype conversion.

    converters : dict, optional
        Dict of functions for converting values in certain columns. Keys can either be integers or column labels.

    parse_dates : bool or list of int or names or list of lists or dict, default False
        Identifiers to parse index or columns to datetime. The behavior is as follows:
        - boolean. If True -> try parsing the index.
        - list of int or names. e.g. If [1, 2, 3] -> try parsing columns 1, 2, 3 each as a separate date column.
        - list of lists. e.g. If [[1, 3]] -> combine columns 1 and 3 and parse as a single date column.
        - dict, e.g. {‘foo’ : [1, 3]} -> parse columns 1, 3 as date and call result ‘foo’

    encoding : str, optional, default ‘utf-8’
        Encoding of XML document.
    parser : {‘lxml’,’etree’}, default ‘lxml’
        Parser module to use for retrieval of data. Only ‘lxml’ and ‘etree’ are supported. With ‘lxml’ more complex XPath searches and ability to use XSLT stylesheet are supported.
    stylesheet : str, path object or file-like object
        A URL, file-like object, or a string path containing an XSLT script. This stylesheet should flatten complex, deeply nested XML documents for easier parsing. To use this feature you must have lxml module installed and specify ‘lxml’ as parser. The xpath must reference nodes of transformed XML document generated after XSLT transformation and not the original XML document. Only XSLT 1.0 scripts and not later versions is currently supported.
    iterparse : dict, optional
        The nodes or attributes to retrieve in iterparsing of XML document as a dict with key being the name of repeating element and value being list of elements or attribute names that are descendants of the repeated element. Note: If this option is used, it will replace xpath parsing and unlike xpath, descendants do not need to relate to each other but can exist any where in document under the repeating element. This memory- efficient method should be used for very large XML files (500MB, 1GB, or 5GB+). For example, {"row_element": ["child_elem", "attr", "grandchild_elem"]}.

    compression : str or dict, default ‘infer’
        For on-the-fly decompression of on-disk data. If ‘infer’ and ‘path_or_buffer’ is path-like, then detect compression from the following extensions: ‘.gz’, ‘.bz2’, ‘.zip’, ‘.xz’, ‘.zst’, ‘.tar’, ‘.tar.gz’, ‘.tar.xz’ or ‘.tar.bz2’ (otherwise no compression). If using ‘zip’ or ‘tar’, the ZIP file must contain only one data file to be read in. Set to None for no decompression. Can also be a dict with key 'method' set to one of {'zip', 'gzip', 'bz2', 'zstd', 'xz', 'tar'} and other key-value pairs are forwarded to zipfile.ZipFile, gzip.GzipFile, bz2.BZ2File, zstandard.ZstdDecompressor, lzma.LZMAFile or tarfile.TarFile, respectively. As an example, the following could be passed for Zstandard decompression using a custom compression dictionary: compression={'method': 'zstd', 'dict_data': my_compression_dict}.

    storage_options : dict, optional
        Extra options that make sense for a particular storage connection, e.g. host, port, username, password, etc. For HTTP(S) URLs the key-value pairs are forwarded to urllib.request.Request as header options. For other URLs (e.g. starting with “s3://”, and “gcs://”) the key-value pairs are forwarded to fsspec.open. Please see fsspec and urllib for more details, and for more examples on storage options refer here.
    dtype_backend : {‘numpy_nullable’, ‘pyarrow’}
        Back-end data type applied to the resultant DataFrame (still experimental). If not specified, the default behavior is to not use nullable data types. If specified, the behavior is as follows:
        - "numpy_nullable": returns nullable-dtype-backed DataFrame
        - "pyarrow": returns pyarrow-backed nullable ArrowDtype DataFrame

    Returns
    -------
    df
        A DataFrame.

    See also
    --------
    read_json
        Convert a JSON string to pandas object.
    read_html
        Read HTML tables into a list of DataFrame objects.

    Notes
    -----
    This method is best designed to import shallow XML documents in following format which is the ideal fit for the two-dimensions of a DataFrame (row by column). ::

            <root>
                <row>
                  <column1>data</column1>
                  <column2>data</column2>
                  <column3>data</column3>
                  ...
               </row>
               <row>
                  ...
               </row>
               ...
            </root>


    As a file format, XML documents can be designed any way including layout of elements and attributes as long as it conforms to W3C specifications. Therefore, this method is a convenience handler for a specific flatter design and not all possible XML structures.

    However, for more complex XML documents, stylesheet allows you to temporarily redesign original document with XSLT (a special purpose language) for a flatter version for migration to a DataFrame.

    This function will always return a single DataFrame or raise exceptions due to issues with XML document, xpath, or other parameters.

    See the read_xml documentation in the IO section of the docs for more information in using this method to parse XML files to DataFrames.

    Examples
    --------
    >>> from io import StringIO
    >>> xml = '''<?xml version='1.0' encoding='utf-8'?>
    ... <data xmlns="http://example.com">
    ... <row>
    ... <shape>square</shape>
    ... <degrees>360</degrees>
    ... <sides>4.0</sides>
    ... </row>
    ... <row>
    ... <shape>circle</shape>
    ... <degrees>360</degrees>
    ... <sides/>
    ... </row>
    ... <row>
    ... <shape>triangle</shape>
    ... <degrees>180</degrees>
    ... <sides>3.0</sides>
    ... </row>
    ... </data>'''

    >>> df = pd.read_xml(StringIO(xml))
    >>> df
          shape  degrees  sides
    0    square      360    4.0
    1    circle      360    NaN
    2  triangle      180    3.0

    >>> xml = '''<?xml version='1.0' encoding='utf-8'?>
    ... <data>
    ... <row shape="square" degrees="360" sides="4.0"/>
    ... <row shape="circle" degrees="360"/>
    ... <row shape="triangle" degrees="180" sides="3.0"/>
    ... </data>'''

    >>> df = pd.read_xml(StringIO(xml), xpath=".//row")
    >>> df
          shape  degrees  sides
    0    square      360    4.0
    1    circle      360    NaN
    2  triangle      180    3.0

    >>> xml = '''<?xml version='1.0' encoding='utf-8'?>
    ... <doc:data xmlns:doc="https://example.com">
    ... <doc:row>
    ...     <doc:shape>square</doc:shape>
    ...     <doc:degrees>360</doc:degrees>
    ...     <doc:sides>4.0</doc:sides>
    ... </doc:row>
    ... <doc:row>
    ...     <doc:shape>circle</doc:shape>
    ...     <doc:degrees>360</doc:degrees>
    ...     <doc:sides/>
    ... </doc:row>
    ... <doc:row>
    ...     <doc:shape>triangle</doc:shape>
    ...     <doc:degrees>180</doc:degrees>
    ...     <doc:sides>3.0</doc:sides>
    ... </doc:row>
    ... </doc:data>'''

    >>> df = pd.read_xml(
    ...     StringIO(xml),
    ...     xpath="//doc:row",
    ...     namespaces={"doc": "https://example.com"},
    ... )
    >>> df
          shape  degrees  sides
    0    square      360    4.0
    1    circle      360    NaN
    2  triangle      180    3.0

    >>> xml_data = '''
    ...         <data>
    ...         <row>
    ...             <index>0</index>
    ...             <a>1</a>
    ...             <b>2.5</b>
    ...             <c>True</c>
    ...             <d>a</d>
    ...             <e>2019-12-31 00:00:00</e>
    ...         </row>
    ...         <row>
    ...             <index>1</index>
    ...             <b>4.5</b>
    ...             <c>False</c>
    ...             <d>b</d>
    ...             <e>2019-12-31 00:00:00</e>
    ...         </row>
    ...         </data>
    ...         '''

    >>> df = pd.read_xml(
    ...     StringIO(xml_data), dtype_backend="numpy_nullable", parse_dates=["e"]
    ... )
    >>> df
       index    a    b      c  d          e
    0      0  1.0  2.5   True  a 2019-12-31
    1      1  NaN  4.5  False  b 2019-12-31
    """


def json_normalize():
    """
    Normalize semi-structured JSON data into a flat table.

    Parameters
    ----------
    data : dict or list of dicts
        Unserialized JSON objects.
    record_path : str or list of str, default None
        Path in each object to list of records. If not passed, data will be assumed to be an array of records.
    meta : list of paths (str or list of str), default None
        Fields to use as metadata for each record in resulting table.
    meta_prefix : str, default None
        If True, prefix records with dotted path, e.g. foo.bar.field if meta is [‘foo’, ‘bar’].
    record_prefix : str, default None
        If True, prefix records with dotted path, e.g. foo.bar.field if path to records is [‘foo’, ‘bar’].
    errors : {‘raise’, ‘ignore’}, default ‘raise’
        Configures error handling.
        - ‘ignore’ : will ignore KeyError if keys listed in meta are not always present.
        - ‘raise’ : will raise KeyError if keys listed in meta are not always present.
    sep : str, default ‘.’
        Nested records will generate names separated by sep. e.g., for sep=’.’, {‘foo’: {‘bar’: 0}} -> foo.bar.
    max_level : int, default None
        Max number of levels(depth of dict) to normalize. if None, normalizes all levels.

    Returns
    -------
    frame : DataFrame
    Normalize semi-structured JSON data into a flat table.

    Examples
    --------
    >>> data = [
    ...     {"id": 1, "name": {"first": "Coleen", "last": "Volk"}},
    ...     {"name": {"given": "Mark", "family": "Regner"}},
    ...     {"id": 2, "name": "Faye Raker"},
    ... ]
    >>> pd.json_normalize(data)
        id name.first name.last name.given name.family        name
    0  1.0     Coleen      Volk       None        None        None
    1  NaN       None      None       Mark      Regner        None
    2  2.0       None      None       None        None  Faye Raker

    >>> data = [
    ...     {
    ...         "id": 1,
    ...         "name": "Cole Volk",
    ...         "fitness": {"height": 130, "weight": 60},
    ...     },
    ...     {"name": "Mark Reg", "fitness": {"height": 130, "weight": 60}},
    ...     {
    ...         "id": 2,
    ...         "name": "Faye Raker",
    ...         "fitness": {"height": 130, "weight": 60},
    ...     },
    ... ]
    >>> pd.json_normalize(data, max_level=0)
        id        name                        fitness
    0  1.0   Cole Volk  {'height': 130, 'weight': 60}
    1  NaN    Mark Reg  {'height': 130, 'weight': 60}
    2  2.0  Faye Raker  {'height': 130, 'weight': 60}

    Normalizes nested data up to level 1.

    >>> data = [
    ...     {
    ...         "id": 1,
    ...         "name": "Cole Volk",
    ...         "fitness": {"height": 130, "weight": 60},
    ...     },
    ...     {"name": "Mark Reg", "fitness": {"height": 130, "weight": 60}},
    ...     {
    ...         "id": 2,
    ...         "name": "Faye Raker",
    ...         "fitness": {"height": 130, "weight": 60},
    ...     },
    ... ]
    >>> pd.json_normalize(data, max_level=1)
        id        name  fitness.height  fitness.weight
    0  1.0   Cole Volk             130              60
    1  NaN    Mark Reg             130              60
    2  2.0  Faye Raker             130              60

    >>> data = [
    ...     {
    ...         "state": "Florida",
    ...         "shortname": "FL",
    ...         "info": {"governor": "Rick Scott"},
    ...         "counties": [
    ...             {"name": "Dade", "population": 12345},
    ...             {"name": "Broward", "population": 40000},
    ...             {"name": "Palm Beach", "population": 60000},
    ...         ],
    ...     },
    ...     {
    ...         "state": "Ohio",
    ...         "shortname": "OH",
    ...         "info": {"governor": "John Kasich"},
    ...         "counties": [
    ...             {"name": "Summit", "population": 1234},
    ...             {"name": "Cuyahoga", "population": 1337},
    ...         ],
    ...     },
    ... ]
    >>> result = pd.json_normalize(
    ...     data, "counties", ["state", "shortname", ["info", "governor"]]
    ... )
    >>> result
             name  population    state shortname info.governor
    0        Dade       12345  Florida        FL    Rick Scott
    1     Broward       40000  Florida        FL    Rick Scott
    2  Palm Beach       60000  Florida        FL    Rick Scott
    3      Summit        1234     Ohio        OH   John Kasich
    4    Cuyahoga        1337     Ohio        OH   John Kasich

    >>> data = {"A": [1, 2]}
    >>> pd.json_normalize(data, "A", record_prefix="Prefix.")
       Prefix.0
    0         1
    1         2

    Returns normalized data with columns prefixed with the given string.
    """


def read_orc():
    """
    Load an ORC object from the file path, returning a DataFrame.

    This method reads an ORC (Optimized Row Columnar) file into a pandas DataFrame using the pyarrow.orc library. ORC is a columnar storage format that provides efficient compression and fast retrieval for analytical workloads. It allows reading specific columns, handling different filesystem types (such as local storage, cloud storage via fsspec, or pyarrow filesystem), and supports different data type backends, including numpy_nullable and pyarrow.

    It can read files stored locally or on a Snowflake stage.

    Parameters
    ----------
    path : str, path object, or file-like object
        String, path object (implementing os.PathLike[str]), or file-like object implementing a binary read() function. The string could be a URL. Valid URL schemes include http, ftp, s3, and file. For file URLs, a host is expected. A local file could be: file://localhost/path/to/table.orc.
        Staged file locations start with the '@' symbol. To read a local file location with a name starting with `@`,
        escape it using a `\\@`. For more info on staged files, `read here
        <https://docs.snowflake.com/en/sql-reference/sql/create-stage>`_.

    columns : list, default None
        If not None, only these columns will be read from the file. Output always follows the ordering of the file and not the columns list. This mirrors the original behaviour of pyarrow.orc.ORCFile.read().

    dtype_backend : {‘numpy_nullable’, ‘pyarrow’}
        Back-end data type applied to the resultant DataFrame (still experimental). If not specified, the default behavior is to not use nullable data types. If specified, the behavior is as follows:

        - "numpy_nullable": returns nullable-dtype-backed DataFrame

        - "pyarrow": returns pyarrow-backed nullable ArrowDtype DataFrame

    filesystem: fsspec or pyarrow filesystem, default None
        Filesystem object to use when reading the orc file.

    **kwargs
        Any additional kwargs are passed to pyarrow.

    Returns
    -------
    DataFrame
        DataFrame based on the ORC file.

    See also
    --------

    read_csv
        Read a comma-separated values (csv) file into a pandas DataFrame.

    read_excel
        Read an Excel file into a pandas DataFrame.

    read_spss
        Read an SPSS file into a pandas DataFrame.

    read_sas
        Load a SAS file into a pandas DataFrame.

    read_feather
        Load a feather-format object into a pandas DataFrame.

    Notes
    -----

    Before using this function you should read the user guide about ORC and install optional dependencies.

    If path is a URI scheme pointing to a local or remote file (e.g. “s3://”), a pyarrow.fs filesystem will be attempted to read the file. You can also pass a pyarrow or fsspec filesystem object into the filesystem keyword to override this behavior.

    Examples
    --------

    >>> result = pd.read_orc("example_pa.orc")  # doctest: +SKIP
    """


def read_excel():
    """
    Read an Excel file into a Snowpark pandas DataFrame.

    Supports xls, xlsx, xlsm, xlsb, odf, ods and odt file extensions read from a local filesystem or URL. Supports an option to read a single sheet or a list of sheets.

    This API can read files stored locally or on a Snowflake stage.

    Parameters
    ----------
    io : str, bytes, ExcelFile, xlrd.Book, path object, or file-like object
        Any valid string path is acceptable. The string could be a URL. Valid URL schemes include http, ftp, s3, and file. For file URLs, a host is expected. A local file could be: file://localhost/path/to/table.xlsx.
        If you want to pass in a path object, pandas accepts any os.PathLike.
        By file-like object, we refer to objects with a read() method, such as a file handle (e.g. via builtin open function) or StringIO.
        Staged file locations start with the '@' symbol. To read a local file location with a name starting with `@`,
        escape it using a `\\@`. For more info on staged files, `read here
        <https://docs.snowflake.com/en/sql-reference/sql/create-stage>`_.

        Deprecated: Passing byte strings is deprecated. To read from a byte string, wrap it in a BytesIO object.

    sheet_name : str, int, list, or None, default 0
        Strings are used for sheet names. Integers are used in zero-indexed sheet positions (chart sheets do not count as a sheet position). Lists of strings/integers are used to request multiple sheets. Specify None to get all worksheets.
        Available cases:
        - Defaults to 0: 1st sheet as a DataFrame
        - 1: 2nd sheet as a DataFrame
        - "Sheet1": Load sheet with name “Sheet1”
        - [0, 1, "Sheet5"]: Load first, second and sheet named “Sheet5” as a dict of DataFrame
        - None: All worksheets.
    header : int, list of int, default 0
        Row (0-indexed) to use for the column labels of the parsed DataFrame. If a list of integers is passed those row positions will be combined into a MultiIndex. Use None if there is no header.
    names : array-like, default None
        List of column names to use. If file contains no header row, then you should explicitly pass header=None.
    index_col : int, str, list of int, default None
        Column (0-indexed) to use as the row labels of the DataFrame. Pass None if there is no such column. If a list is passed, those columns will be combined into a MultiIndex. If a subset of data is selected with usecols, index_col is based on the subset.
        Missing values will be forward filled to allow roundtripping with to_excel for merged_cells=True. To avoid forward filling the missing values use set_index after reading the data instead of index_col.
    usecols : str, list-like, or callable, default None
        - If None, then parse all columns.
        - If str, then indicates comma separated list of Excel column letters and column ranges (e.g. “A:E” or “A,C,E:F”). Ranges are inclusive of both sides.
        - If list of int, then indicates list of column numbers to be parsed (0-indexed).
        - If list of string, then indicates list of column names to be parsed.
        - If callable, then evaluate each column name against it and parse the column if the callable returns True.

        Returns a subset of the columns according to behavior above.
    dtype : Type name or dict of column -> type, default None
        Data type for data or columns. E.g. {‘a’: np.float64, ‘b’: np.int32} Use object to preserve data as stored in Excel and not interpret dtype, which will necessarily result in object dtype. If converters are specified, they will be applied INSTEAD of dtype conversion. If you use None, it will infer the dtype of each column based on the data.
    engine : {‘openpyxl’, ‘calamine’, ‘odf’, ‘pyxlsb’, ‘xlrd’}, default None
        If io is not a buffer or path, this must be set to identify io. Engine compatibility :
        - openpyxl supports newer Excel file formats.
        - calamine supports Excel (.xls, .xlsx, .xlsm, .xlsb) and OpenDocument (.ods) file formats.
        - odf supports OpenDocument file formats (.odf, .ods, .odt).
        - pyxlsb supports Binary Excel files.
        - xlrd supports old-style Excel files (.xls).

        When engine=None, the following logic will be used to determine the engine:
        - If path_or_buffer is an OpenDocument format (.odf, .ods, .odt), then odf will be used.
        - Otherwise if path_or_buffer is an xls format, xlrd will be used.
        - Otherwise if path_or_buffer is in xlsb format, pyxlsb will be used.
        - Otherwise openpyxl will be used.
    converters : dict, default None
        Dict of functions for converting values in certain columns. Keys can either be integers or column labels, values are functions that take one input argument, the Excel cell content, and return the transformed content.
    true_values : list, default None
        Values to consider as True.
    false_values : list, default None
        Values to consider as False.
    skiprows : list-like, int, or callable, optional
        Line numbers to skip (0-indexed) or number of lines to skip (int) at the start of the file. If callable, the callable function will be evaluated against the row indices, returning True if the row should be skipped and False otherwise. An example of a valid callable argument would be lambda x: x in [0, 2].
    nrows : int, default None
        Number of rows to parse.
    na_values : scalar, str, list-like, or dict, default None
        Additional strings to recognize as NA/NaN. If dict passed, specific per-column NA values. By default the following values are interpreted as NaN: ‘’, ‘#N/A’, ‘#N/A N/A’, ‘#NA’, ‘-1.#IND’, ‘-1.#QNAN’, ‘-NaN’, ‘-nan’, ‘1.#IND’, ‘1.#QNAN’, ‘<NA>’, ‘N/A’, ‘NA’, ‘NULL’, ‘NaN’, ‘None’, ‘n/a’, ‘nan’, ‘null’.
    keep_default_na : bool, default True
        Whether or not to include the default NaN values when parsing the data. Depending on whether na_values is passed in, the behavior is as follows:
        - If keep_default_na is True, and na_values are specified, na_values is appended to the default NaN values used for parsing.
        - If keep_default_na is True, and na_values are not specified, only the default NaN values are used for parsing.
        - If keep_default_na is False, and na_values are specified, only the NaN values specified na_values are used for parsing.
        - If keep_default_na is False, and na_values are not specified, no strings will be parsed as NaN.

        Note that if na_filter is passed in as False, the keep_default_na and na_values parameters will be ignored.
    na_filter : bool, default True
        Detect missing value markers (empty strings and the value of na_values). In data without any NAs, passing na_filter=False can improve the performance of reading a large file.
    verbose : bool, default False
        Indicate number of NA values placed in non-numeric columns.
    parse_dates : bool, list-like, or dict, default False
        The behavior is as follows:
        - bool. If True -> try parsing the index.
        - list of int or names. e.g. If [1, 2, 3] -> try parsing columns 1, 2, 3 each as a separate date column.
        - list of lists. e.g. If [[1, 3]] -> combine columns 1 and 3 and parse as a single date column.
        - dict, e.g. {‘foo’ : [1, 3]} -> parse columns 1, 3 as date and call result ‘foo’

        If a column or index contains an unparsable date, the entire column or index will be returned unaltered as an object data type. If you don`t want to parse some cells as date just change their type in Excel to “Text”. For non-standard datetime parsing, use pd.to_datetime after pd.read_excel.
        Note: A fast-path exists for iso8601-formatted dates.
    date_parser : function, optional
        Function to use for converting a sequence of string columns to an array of datetime instances. The default uses dateutil.parser.parser to do the conversion. Pandas will try to call date_parser in three different ways, advancing to the next if an exception occurs: 1) Pass one or more arrays (as defined by parse_dates) as arguments; 2) concatenate (row-wise) the string values from the columns defined by parse_dates into a single array and pass that; and 3) call date_parser once for each row using one or more strings (corresponding to the columns defined by parse_dates) as arguments.

        Deprecated: Use date_format instead, or read in as object and then apply to_datetime() as-needed.

    date_format : str or dict of column -> format, default None
        If used in conjunction with parse_dates, will parse dates according to this format. For anything more complex, please read in as object and then apply to_datetime() as-needed.
    thousands : str, default None
        Thousands separator for parsing string columns to numeric. Note that this parameter is only necessary for columns stored as TEXT in Excel, any numeric columns will automatically be parsed, regardless of display format.
    decimal : str, default ‘.’
        Character to recognize as decimal point for parsing string columns to numeric. Note that this parameter is only necessary for columns stored as TEXT in Excel, any numeric columns will automatically be parsed, regardless of display format.(e.g. use ‘,’ for European data).
    comment : str, default None
        Comments out remainder of line. Pass a character or characters to this argument to indicate comments in the input file. Any data between the comment string and the end of the current line is ignored.
    skipfooter : int, default 0
        Rows at the end to skip (0-indexed).
    storage_options : dict, optional
        Extra options that make sense for a particular storage connection, e.g. host, port, username, password, etc. For HTTP(S) URLs the key-value pairs are forwarded to urllib.request.Request as header options. For other URLs (e.g. starting with “s3://”, and “gcs://”) the key-value pairs are forwarded to fsspec.open. Please see fsspec and urllib for more details, and for more examples on storage options refer here.
    dtype_backend : {‘numpy_nullable’, ‘pyarrow’}, default ‘numpy_nullable’
        Back-end data type applied to the resultant DataFrame (still experimental). Behaviour is as follows:
        - "numpy_nullable": returns nullable-dtype-backed DataFrame (default).
        - "pyarrow": returns pyarrow-backed nullable ArrowDtype DataFrame.
    engine_kwargs : dict, optional
        Arbitrary keyword arguments passed to excel engine.

    Returns
    -------
    DataFrame or dict of DataFrames
        DataFrame from the passed in Excel file. See notes in sheet_name argument for more information on when a dict of DataFrames is returned.

    See also
    --------
    DataFrame.to_excel
        Write DataFrame to an Excel file.
    DataFrame.to_csv
        Write DataFrame to a comma-separated values (csv) file.
    read_csv
        Read a comma-separated values (csv) file into DataFrame.
    read_fwf
        Read a table of fixed-width formatted lines into DataFrame.

    Notes
    -----
    For specific information on the methods used for each Excel engine, refer to the pandas `user guide <https://pandas.pydata.org/docs/user_guide/io.html#io-excel-reader>`_.

    Examples
    --------
    The file can be read using the file name as string or an open file object:

    >>> pd.read_excel('tmp.xlsx', index_col=0)  # doctest: +SKIP
           Name  Value
    0   string1      1
    1   string2      2
    2  #Comment      3

    >>> pd.read_excel(open('tmp.xlsx', 'rb'),
    ...               sheet_name='Sheet3')  # doctest: +SKIP
       Unnamed: 0      Name  Value
    0           0   string1      1
    1           1   string2      2
    2           2  #Comment      3

    The file can also be read from a stage:

    >>> pd.read_excel('@mystage/tmp.xlsx', index_col=0)  # doctest: +SKIP
           Name  Value
    0   string1      1
    1   string2      2
    2  #Comment      3

    Index and header can be specified via the index_col and header arguments

    >>> pd.read_excel('tmp.xlsx', index_col=None, header=None)  # doctest: +SKIP
         0         1      2
    0  NaN      Name  Value
    1  0.0   string1      1
    2  1.0   string2      2
    3  2.0  #Comment      3

    Column types are inferred but can be explicitly specified

    >>> pd.read_excel('tmp.xlsx', index_col=0,
    ...               dtype={'Name': str, 'Value': float})  # doctest: +SKIP
           Name  Value
    0   string1    1.0
    1   string2    2.0
    2  #Comment    3.0

    True, False, and NA values, and thousands separators have defaults, but can be explicitly specified, too. Supply the values you would like as strings or lists of strings!

    >>> pd.read_excel('tmp.xlsx', index_col=0,
    ...               na_values=['string1', 'string2'])  # doctest: +SKIP
           Name  Value
    0       NaN      1
    1       NaN      2
    2  #Comment      3

    Comment lines in the excel input file can be skipped using the comment kwarg.

    >>> pd.read_excel('tmp.xlsx', index_col=0, comment='#')  # doctest: +SKIP
          Name  Value
    0  string1    1.0
    1  string2    2.0
    2     None    NaN
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
        start with the '@' symbol. To read a local file location with a name starting with `@`,
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
        start with the '@' symbol. To read a local file location with a name starting with `@`,
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
        start with the '@' symbol. To read a local file location with a name starting with `@`,
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

    This API can read files stored locally or on a Snowflake stage.

    Parameters
    ----------

    filepath_or_buffer : str, path object, or file-like object
        String, path object (implementing os.PathLike[str]), or file-like object implementing a binary read() function. The string could be a URL. Valid URL schemes include http, ftp, s3, and file. For file URLs, a host is expected. A local file could be: file://localhost/path/to/table.sas7bdat.
        Staged file locations start with the '@' symbol. To read a local file location with a name starting with `@`,
        escape it using a `\\@`. For more info on staged files, `read here
        <https://docs.snowflake.com/en/sql-reference/sql/create-stage>`_.
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


def read_stata():
    """
    Read Stata file into DataFrame.

    This API can read files stored locally or on a Snowflake stage.

    Parameters
    ----------
    filepath_or_buffer : str, path object or file-like object
        Any valid string path is acceptable. The string could be a URL. Valid URL schemes include http, ftp, s3, and file. For file URLs, a host is expected. A local file could be: file://localhost/path/to/table.dta.

        If you want to pass in a path object, pandas accepts any os.PathLike.

        By file-like object, we refer to objects with a read() method, such as a file handle (e.g. via builtin open function) or StringIO.

        Staged file locations start with the '@' symbol. To read a local file location with a name starting with `@`,
        escape it using a `\\@`. For more info on staged files, `read here
        <https://docs.snowflake.com/en/sql-reference/sql/create-stage>`_.

    convert_dates : bool, default True
        Convert date variables to DataFrame time values.

    convert_categoricals : bool, default True
        Read value labels and convert columns to Categorical/Factor variables.

    index_col : str, optional
        Column to set as index.

    convert_missing : bool, default False
        Flag indicating whether to convert missing values to their Stata representations. If False, missing values are replaced with nan. If True, columns containing missing values are returned with object data types and missing values are represented by StataMissingValue objects.

    preserve_dtypes : bool, default True
        Preserve Stata datatypes. If False, numeric data are upcast to pandas default types for foreign data (float64 or int64).

    columns : list or None
        Columns to retain. Columns will be returned in the given order. None returns all columns.

    order_categoricals : bool, default True
        Flag indicating whether converted categorical data are ordered.

    chunksize : int, default None
        Return StataReader object for iterations, returns chunks with given number of lines.

    iterator : bool, default False
        Return StataReader object.

    compression : str or dict, default ‘infer’
        For on-the-fly decompression of on-disk data. If ‘infer’ and ‘filepath_or_buffer’ is path-like, then detect compression from the following extensions: ‘.gz’, ‘.bz2’, ‘.zip’, ‘.xz’, ‘.zst’, ‘.tar’, ‘.tar.gz’, ‘.tar.xz’ or ‘.tar.bz2’ (otherwise no compression). If using ‘zip’ or ‘tar’, the ZIP file must contain only one data file to be read in. Set to None for no decompression. Can also be a dict with key 'method' set to one of {'zip', 'gzip', 'bz2', 'zstd', 'xz', 'tar'} and other key-value pairs are forwarded to zipfile.ZipFile, gzip.GzipFile, bz2.BZ2File, zstandard.ZstdDecompressor, lzma.LZMAFile or tarfile.TarFile, respectively. As an example, the following could be passed for Zstandard decompression using a custom compression dictionary: compression={'method': 'zstd', 'dict_data': my_compression_dict}.

    storage_options : dict, optional
        Extra options that make sense for a particular storage connection, e.g. host, port, username, password, etc. For HTTP(S) URLs the key-value pairs are forwarded to urllib.request.Request as header options. For other URLs (e.g. starting with “s3://”, and “gcs://”) the key-value pairs are forwarded to fsspec.open. Please see fsspec and urllib for more details, and for more examples on storage options refer here.

    Returns
    -------
    DataFrame, pandas.api.typing.StataReader
        If iterator or chunksize, returns StataReader, else DataFrame.

    See also
    --------
    io.stata.StataReader
        Low-level reader for Stata data files.

    DataFrame.to_stata
        Export Stata data files.

    Notes
    -----
    Categorical variables read through an iterator may not have the same categories and dtype. This occurs when a variable stored in a DTA file is associated to an incomplete set of value labels that only label a strict subset of the values.

    Examples
    --------
    Creating a dummy stata for this example

    >>> df = pd.DataFrame({'animal': ['falcon', 'parrot', 'falcon', 'parrot'],
    ...                   'speed': [350, 18, 361, 15]})
    >>> df.to_stata('animals.dta')  # doctest: +SKIP

    Read a Stata dta file:

    >>> df = pd.read_stata('animals.dta')  # doctest: +SKIP

    Read a Stata dta file in 10,000 line chunks:

    >>> values = np.random.randint(0, 10, size=(20_000, 1), dtype="uint8")
    >>> df = pd.DataFrame(values, columns=["i"])
    >>> df.to_stata('filename.dta')  # doctest: +SKIP

    >>> with pd.read_stata('filename.dta', chunksize=10000) as itr:  # doctest: +SKIP
    ...     for chunk in itr:  # doctest: +SKIP
    ...         # Operate on a single chunk, e.g., chunk.mean()
    ...         pass
    """
