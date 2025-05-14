#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import functools
import os
import tempfile
import time
from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    as_completed,
)


import sys
from logging import getLogger
import queue
from typing import Any, Dict, List, Literal, Optional, Tuple, Union, Callable

from snowflake.snowpark._internal.data_source.datasource_partitioner import (
    DataSourcePartitioner,
)

import snowflake.snowpark
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import ReadFileNode
import snowflake.snowpark._internal.proto.generated.ast_pb2 as proto
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    create_file_format_statement,
    drop_file_format_if_exists_statement,
    infer_schema_statement,
    quote_name_without_upper_casing,
)
from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.analyzer.unary_expression import Alias
from snowflake.snowpark._internal.ast.utils import (
    build_expr_from_python_val,
    build_proto_from_struct_type,
    build_table_name,
    with_src_position,
)
from snowflake.snowpark._internal.data_source.datasource_typing import Connection
from snowflake.snowpark._internal.data_source.utils import (
    _upload_and_copy_into_table_with_retry,
    _task_fetch_data_from_source_with_retry,
    STATEMENT_PARAMS_DATA_SOURCE,
    DATA_SOURCE_SQL_COMMENT,
    DATA_SOURCE_DBAPI_SIGNATURE,
    add_unseen_files_to_process_queue,
)
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.telemetry import set_api_call_source
from snowflake.snowpark._internal.type_utils import (
    ColumnOrName,
    convert_sf_to_sp_type,
    convert_sp_to_sf_type,
)
from snowflake.snowpark._internal.udf_utils import get_types_from_type_hints
from snowflake.snowpark._internal.utils import (
    STAGE_PREFIX,
    XML_ROW_TAG_STRING,
    XML_ROW_DATA_COLUMN_NAME,
    XML_READER_FILE_PATH,
    XML_READER_API_SIGNATURE,
    XML_READER_SQL_COMMENT,
    INFER_SCHEMA_FORMAT_TYPES,
    SNOWFLAKE_PATH_PREFIXES,
    TempObjectType,
    get_aliased_option_name,
    get_copy_into_table_options,
    parse_positional_args_to_list_variadic,
    publicapi,
    get_temp_type_for_object,
    private_preview,
    random_name_for_temp_object,
    warning,
    is_in_stored_procedure,
)
from snowflake.snowpark.column import METADATA_COLUMN_TYPES, Column, _to_col_if_str
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.exceptions import (
    SnowparkSessionException,
    SnowparkDataframeReaderException,
)
from snowflake.snowpark.functions import sql_expr
from snowflake.snowpark.mock._connection import MockServerConnection
from snowflake.snowpark.table import Table
from snowflake.snowpark.types import (
    StructType,
    VariantType,
    StructField,
)

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

logger = getLogger(__name__)

LOCAL_TESTING_SUPPORTED_FILE_FORMAT = ("JSON",)
READER_OPTIONS_ALIAS_MAP = {
    "DELIMITER": "FIELD_DELIMITER",
    "HEADER": "PARSE_HEADER",
    "PATHGLOBFILTER": "PATTERN",
    "FILENAMEPATTERN": "PATTERN",
    "INFERSCHEMA": "INFER_SCHEMA",
    "SEP": "FIELD_DELIMITER",
    "LINESEP": "RECORD_DELIMITER",
    "QUOTE": "FIELD_OPTIONALLY_ENCLOSED_BY",
    "NULLVALUE": "NULL_IF",
    "DATEFORMAT": "DATE_FORMAT",
    "TIMESTAMPFORMAT": "TIMESTAMP_FORMAT",
}

_MAX_RETRY_TIME = 3


def _validate_stage_path(path: str) -> str:
    stripped_path = path.strip("\"'")
    if not any(stripped_path.startswith(prefix) for prefix in SNOWFLAKE_PATH_PREFIXES):
        raise ValueError(
            f"'{path}' is an invalid Snowflake stage location. DataFrameReader can only read files from stage locations."
        )
    return path


class DataFrameReader:
    """Provides methods to load data in various supported formats from a Snowflake
    stage to a :class:`DataFrame`. The paths provided to the DataFrameReader must refer
    to Snowflake stages.

    To use this object:

    1. Access an instance of a :class:`DataFrameReader` by using the
    :attr:`Session.read` property.

    2. Specify any `format-specific options <https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html#format-type-options-formattypeoptions>`_ and `copy options <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions>`_
    by calling the :func:`option` or :func:`options` method. These methods return a
    DataFrameReader that is configured with these options. (Note that although
    specifying copy options can make error handling more robust during the reading
    process, it may have an effect on performance.)

    3. Specify the schema of the data that you plan to load by constructing a
    :class:`types.StructType` object and passing it to the :func:`schema` method if the file format is CSV. Other file
    formats such as JSON, XML, Parquet, ORC, and AVRO don't accept a schema.
    This method returns a :class:`DataFrameReader` that is configured to read data that uses the specified schema.
    Currently, inferring schema is also supported for CSV and JSON formats as a preview feature open to all accounts.

    4. Specify the format of the data by calling the method named after the format
    (e.g. :func:`csv`, :func:`json`, etc.). These methods return a :class:`DataFrame`
    that is configured to load data in the specified format.

    5. Call a :class:`DataFrame` method that performs an action (e.g.
    :func:`DataFrame.collect`) to load the data from the file.

    The following examples demonstrate how to use a DataFrameReader.
            >>> # Create a temp stage to run the example code.
            >>> _ = session.sql("CREATE or REPLACE temp STAGE mystage").collect()

    Example 1:
        Loading the first two columns of a CSV file and skipping the first header line:

            >>> from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType, FloatType
            >>> _ = session.file.put("tests/resources/testCSV.csv", "@mystage", auto_compress=False)
            >>> # Define the schema for the data in the CSV file.
            >>> user_schema = StructType([StructField("a", IntegerType()), StructField("b", StringType()), StructField("c", FloatType())])
            >>> # Create a DataFrame that is configured to load data from the CSV file.
            >>> df = session.read.options({"field_delimiter": ",", "skip_header": 1}).schema(user_schema).csv("@mystage/testCSV.csv")
            >>> # Load the data into the DataFrame and return an array of rows containing the results.
            >>> df.collect()
            [Row(A=2, B='two', C=2.2)]

    Example 2:
        Loading a gzip compressed json file:

            >>> _ = session.file.put("tests/resources/testJson.json", "@mystage", auto_compress=True)
            >>> # Create a DataFrame that is configured to load data from the gzipped JSON file.
            >>> json_df = session.read.option("compression", "gzip").json("@mystage/testJson.json.gz")
            >>> # Load the data into the DataFrame and return an array of rows containing the results.
            >>> json_df.show()
            -----------------------
            |"$1"                 |
            -----------------------
            |{                    |
            |  "color": "Red",    |
            |  "fruit": "Apple",  |
            |  "size": "Large"    |
            |}                    |
            -----------------------
            <BLANKLINE>


    In addition, if you want to load only a subset of files from the stage, you can use the
    `pattern <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#loading-using-pattern-matching>`_
    option to specify a regular expression that matches the files that you want to load.

    Example 3:
        Loading only the CSV files from a stage location:

            >>> from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType
            >>> from snowflake.snowpark.functions import col
            >>> _ = session.file.put("tests/resources/*.csv", "@mystage", auto_compress=False)
            >>> # Define the schema for the data in the CSV files.
            >>> user_schema = StructType([StructField("a", IntegerType()), StructField("b", StringType()), StructField("c", FloatType())])
            >>> # Create a DataFrame that is configured to load data from the CSV files in the stage.
            >>> csv_df = session.read.option("pattern", ".*V[.]csv").schema(user_schema).csv("@mystage").sort(col("a"))
            >>> # Load the data into the DataFrame and return an array of rows containing the results.
            >>> csv_df.collect()
            [Row(A=1, B='one', C=1.2), Row(A=2, B='two', C=2.2), Row(A=3, B='three', C=3.3), Row(A=4, B='four', C=4.4)]

    To load Parquet, ORC and AVRO files, no schema is accepted because the schema will be automatically inferred.
    Inferring the schema can be disabled by setting option "infer_schema" to ``False``. Then you can use ``$1`` to access
    the column data as an OBJECT.

    Example 4:
        Loading a Parquet file with inferring the schema.
            >>> from snowflake.snowpark.functions import col
            >>> _ = session.file.put("tests/resources/test.parquet", "@mystage", auto_compress=False)
            >>> # Create a DataFrame that uses a DataFrameReader to load data from a file in a stage.
            >>> df = session.read.parquet("@mystage/test.parquet").where(col('"num"') == 2)
            >>> # Load the data into the DataFrame and return an array of rows containing the results.
            >>> df.collect()
            [Row(str='str2', num=2)]

    Example 5:
        Loading an ORC file and infer the schema:
            >>> from snowflake.snowpark.functions import col
            >>> _ = session.file.put("tests/resources/test.orc", "@mystage", auto_compress=False)
            >>> # Create a DataFrame that uses a DataFrameReader to load data from a file in a stage.
            >>> df = session.read.orc("@mystage/test.orc").where(col('"num"') == 2)
            >>> # Load the data into the DataFrame and return an array of rows containing the results.
            >>> df.collect()
            [Row(str='str2', num=2)]

    Example 6:
        Loading an AVRO file and infer the schema:
            >>> from snowflake.snowpark.functions import col
            >>> _ = session.file.put("tests/resources/test.avro", "@mystage", auto_compress=False)
            >>> # Create a DataFrame that uses a DataFrameReader to load data from a file in a stage.
            >>> df = session.read.avro("@mystage/test.avro").where(col('"num"') == 2)
            >>> # Load the data into the DataFrame and return an array of rows containing the results.
            >>> df.collect()
            [Row(str='str2', num=2)]

    Example 7:
        Loading a Parquet file without inferring the schema:
            >>> from snowflake.snowpark.functions import col
            >>> _ = session.file.put("tests/resources/test.parquet", "@mystage", auto_compress=False)
            >>> # Create a DataFrame that uses a DataFrameReader to load data from a file in a stage.
            >>> df = session.read.option("infer_schema", False).parquet("@mystage/test.parquet").where(col('$1')["num"] == 2)
            >>> # Load the data into the DataFrame and return an array of rows containing the results.
            >>> df.show()
            -------------------
            |"$1"             |
            -------------------
            |{                |
            |  "num": 2,      |
            |  "str": "str2"  |
            |}                |
            -------------------
            <BLANKLINE>

    Loading JSON and XML files doesn't support schema either. You also need to use ``$1`` to access the column data as an OBJECT.

    Example 8:
        Loading a JSON file:
            >>> from snowflake.snowpark.functions import col, lit
            >>> _ = session.file.put("tests/resources/testJson.json", "@mystage", auto_compress=False)
            >>> # Create a DataFrame that uses a DataFrameReader to load data from a file in a stage.
            >>> df = session.read.json("@mystage/testJson.json").where(col("$1")["fruit"] == lit("Apple"))
            >>> # Load the data into the DataFrame and return an array of rows containing the results.
            >>> df.show()
            -----------------------
            |"$1"                 |
            -----------------------
            |{                    |
            |  "color": "Red",    |
            |  "fruit": "Apple",  |
            |  "size": "Large"    |
            |}                    |
            |{                    |
            |  "color": "Red",    |
            |  "fruit": "Apple",  |
            |  "size": "Large"    |
            |}                    |
            -----------------------
            <BLANKLINE>

    Example 9:
        Loading an XML file:
            >>> _ = session.file.put("tests/resources/test.xml", "@mystage", auto_compress=False)
            >>> # Create a DataFrame that uses a DataFrameReader to load data from a file in a stage.
            >>> df = session.read.xml("@mystage/test.xml")
            >>> # Load the data into the DataFrame and return an array of rows containing the results.
            >>> df.show()
            ---------------------
            |"$1"               |
            ---------------------
            |<test>             |
            |  <num>1</num>     |
            |  <str>str1</str>  |
            |</test>            |
            |<test>             |
            |  <num>2</num>     |
            |  <str>str2</str>  |
            |</test>            |
            ---------------------
            <BLANKLINE>

    Example 10:
        Loading a CSV file with an already existing FILE_FORMAT:
            >>> from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType
            >>> _ = session.sql("create file format if not exists csv_format type=csv skip_header=1 null_if='none';").collect()
            >>> _ = session.file.put("tests/resources/testCSVspecialFormat.csv", "@mystage", auto_compress=False)
            >>> # Define the schema for the data in the CSV files.
            >>> schema = StructType([StructField("ID", IntegerType()),StructField("USERNAME", StringType()),StructField("FIRSTNAME", StringType()),StructField("LASTNAME", StringType())])
            >>> # Create a DataFrame that is configured to load data from the CSV files in the stage.
            >>> df = session.read.schema(schema).option("format_name", "csv_format").csv("@mystage/testCSVspecialFormat.csv")
            >>> # Load the data into the DataFrame and return an array of rows containing the results.
            >>> df.collect()
            [Row(ID=0, USERNAME='admin', FIRSTNAME=None, LASTNAME=None), Row(ID=1, USERNAME='test_user', FIRSTNAME='test', LASTNAME='user')]

    Example 11:
        Querying metadata for staged files:
            >>> from snowflake.snowpark.column import METADATA_FILENAME, METADATA_FILE_ROW_NUMBER
            >>> df = session.read.with_metadata(METADATA_FILENAME, METADATA_FILE_ROW_NUMBER.as_("ROW NUMBER")).schema(user_schema).csv("@mystage/testCSV.csv")
            >>> # Load the data into the DataFrame and return an array of rows containing the results.
            >>> df.show()
            --------------------------------------------------------
            |"METADATA$FILENAME"  |"ROW NUMBER"  |"A"  |"B"  |"C"  |
            --------------------------------------------------------
            |testCSV.csv          |1             |1    |one  |1.2  |
            |testCSV.csv          |2             |2    |two  |2.2  |
            --------------------------------------------------------
            <BLANKLINE>

    Example 12:
        Inferring schema for csv and json files (Preview Feature - Open):
            >>> # Read a csv file without a header
            >>> df = session.read.option("INFER_SCHEMA", True).csv("@mystage/testCSV.csv")
            >>> df.show()
            ----------------------
            |"c1"  |"c2"  |"c3"  |
            ----------------------
            |1     |one   |1.2   |
            |2     |two   |2.2   |
            ----------------------
            <BLANKLINE>

            >>> # Read a csv file with header and parse the header
            >>> _ = session.file.put("tests/resources/testCSVheader.csv", "@mystage", auto_compress=False)
            >>> df = session.read.option("INFER_SCHEMA", True).option("PARSE_HEADER", True).csv("@mystage/testCSVheader.csv")
            >>> df.show()
            ----------------------------
            |"id"  |"name"  |"rating"  |
            ----------------------------
            |1     |one     |1.2       |
            |2     |two     |2.2       |
            ----------------------------
            <BLANKLINE>

            >>> df = session.read.option("INFER_SCHEMA", True).json("@mystage/testJson.json")
            >>> df.show()
            ------------------------------
            |"color"  |"fruit"  |"size"  |
            ------------------------------
            |Red      |Apple    |Large   |
            |Red      |Apple    |Large   |
            ------------------------------
            <BLANKLINE>

    Example 13:
        Reading an XML file with a row tag
            >>> # Each XML record is extracted as a separate row,
            >>> # and each field within that record becomes a separate column of type VARIANT
            >>> _ = session.file.put("tests/resources/nested.xml", "@mystage", auto_compress=False)
            >>> df = session.read.option("rowTag", "tag").xml("@mystage/nested.xml")
            >>> df.show()
            -----------------------
            |"'test'"             |
            -----------------------
            |{                    |
            |  "num": "1",        |
            |  "obj": {           |
            |    "bool": "true",  |
            |    "str": "str2"    |
            |  },                 |
            |  "str": "str1"      |
            |}                    |
            -----------------------
            <BLANKLINE>

            >>> # Query nested fields using dot notation
            >>> from snowflake.snowpark.functions import col
            >>> df.select(
            ...     "'test'.num", "'test'.str", col("'test'.obj"), col("'test'.obj.bool")
            ... ).show()
            ------------------------------------------------------------------------------------------------------
            |\"\"\"'TEST'"":""NUM\"\"\"  |\"\"\"'TEST'"":""STR\"\"\"  |\"\"\"'TEST'"":""OBJ\"\"\"  |\"\"\"'TEST'"":""OBJ"".""BOOL\"\"\"  |
            ------------------------------------------------------------------------------------------------------
            |"1"                   |"str1"                |{                     |"true"                         |
            |                      |                      |  "bool": "true",     |                               |
            |                      |                      |  "str": "str2"       |                               |
            |                      |                      |}                     |                               |
            ------------------------------------------------------------------------------------------------------
            <BLANKLINE>
    """

    @publicapi
    def __init__(
        self, session: "snowflake.snowpark.session.Session", _emit_ast: bool = True
    ) -> None:
        self._session = session
        self._user_schema: Optional[StructType] = None
        self._cur_options: dict[str, Any] = {}
        self._file_path: Optional[str] = None
        self._file_type: Optional[str] = None
        self._metadata_cols: Optional[Iterable[ColumnOrName]] = None
        # Infer schema information
        self._infer_schema_transformations: Optional[
            List["snowflake.snowpark.column.Column"]
        ] = None
        self._infer_schema_target_columns: Optional[List[str]] = None
        self.__format: Optional[str] = None

        self._ast = None
        if _emit_ast:
            reader = proto.Expr()
            with_src_position(reader.dataframe_reader)
            self._ast = reader

    @property
    def _infer_schema(self):
        # let _cur_options to be the source of truth
        if self._file_type in INFER_SCHEMA_FORMAT_TYPES:
            return self._cur_options.get("INFER_SCHEMA", True)
        return False

    def _get_metadata_project_and_schema(self) -> Tuple[List[str], List[Attribute]]:
        if self._metadata_cols:
            metadata_project = [
                self._session._analyzer.analyze(col._expression, {})
                for col in self._metadata_cols
            ]
        else:
            metadata_project = []

        metadata_schema = []

        def _get_unaliased_name(unaliased: ColumnOrName) -> str:
            if isinstance(unaliased, Column):
                if isinstance(unaliased._expression, Alias):
                    return unaliased._expression.child.sql
                return unaliased._named().name
            return unaliased

        try:
            metadata_schema = [
                Attribute(
                    metadata_col._named().name,
                    METADATA_COLUMN_TYPES[_get_unaliased_name(metadata_col).upper()],
                )
                for metadata_col in self._metadata_cols or []
            ]
        except KeyError:
            raise ValueError(
                f"Metadata column name is not supported. Supported {METADATA_COLUMN_TYPES.keys()}, Got {metadata_project}"
            )

        return metadata_project, metadata_schema

    @publicapi
    def table(self, name: Union[str, Iterable[str]], _emit_ast: bool = True) -> Table:
        """Returns a Table that points to the specified table.

        This method is an alias of :meth:`~snowflake.snowpark.session.Session.table`.

        Args:
            name: Name of the table to use.
        """

        # AST.
        stmt = None
        if _emit_ast and self._ast is not None:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.read_table, stmt)
            ast.reader.CopyFrom(self._ast)
            build_table_name(ast.name, name)

        table = self._session.table(name, _emit_ast=False)

        if _emit_ast and stmt is not None:
            table._ast_id = stmt.uid

        return table

    @publicapi
    def schema(self, schema: StructType, _emit_ast: bool = True) -> "DataFrameReader":
        """Define the schema for CSV files that you want to read.

        Args:
            schema: Schema configuration for the CSV file to be read.

        Returns:
            a :class:`DataFrameReader` instance with the specified schema configuration for the data to be read.
        """

        # AST.
        if _emit_ast and self._ast is not None:
            build_proto_from_struct_type(schema, self._ast.dataframe_reader.schema)

        self._user_schema = schema
        return self

    @publicapi
    def with_metadata(
        self, *metadata_cols: Iterable[ColumnOrName], _emit_ast: bool = True
    ) -> "DataFrameReader":
        """Define the metadata columns that need to be selected from stage files.

        Returns:
            a :class:`DataFrameReader` instance with metadata columns to read.

        See Also:
            https://docs.snowflake.com/en/user-guide/querying-metadata
        """
        if isinstance(self._session._conn, MockServerConnection):
            self._session._conn.log_not_supported_error(
                external_feature_name="DataFrameReader.with_metadata",
                raise_error=NotImplementedError,
            )

        # AST.
        if _emit_ast and self._ast is not None:
            col_names, is_variadic = parse_positional_args_to_list_variadic(
                *metadata_cols
            )
            self._ast.dataframe_reader.metadata_columns.variadic = is_variadic
            for e in col_names:
                build_expr_from_python_val(
                    self._ast.dataframe_reader.metadata_columns.args.add(), e
                )

        self._metadata_cols = [
            _to_col_if_str(col, "DataFrameReader.with_metadata")
            for col in metadata_cols
        ]
        return self

    @property
    def _format(self) -> Optional[str]:
        return self.__format

    @_format.setter
    def _format(self, value: str) -> None:
        canon_format = value.strip().lower()
        allowed_formats = ["csv", "json", "avro", "parquet", "orc", "xml", "dbapi"]
        if canon_format not in allowed_formats:
            raise ValueError(
                f"Invalid format '{value}'. Supported formats are {allowed_formats}."
            )
        self.__format = canon_format

    @publicapi
    def format(
        self,
        format: Literal["csv", "json", "avro", "parquet", "orc", "xml"],
        _emit_ast: bool = True,
    ) -> "DataFrameReader":
        """Specify the format of the file(s) to load.

        Args:
            format: The format of the file(s) to load. Supported formats are csv, json, avro, parquet, orc, and xml.

        Returns:
            a :class:`DataFrameReader` instance that is set up to load data from the specified file format in a Snowflake stage.
        """
        self._format = format
        # AST.
        if _emit_ast and self._ast is not None:
            self._ast.dataframe_reader.format.value = self._format
        return self

    @publicapi
    def load(self, path: Optional[str] = None, _emit_ast: bool = True) -> DataFrame:
        """Specify the path of the file(s) to load.

        Args:
            path: The stage location of a file, or a stage location that has files.
             This parameter is required for all formats except dbapi.

        Returns:
            a :class:`DataFrame` that is set up to load data from the specified file(s) in a Snowflake stage.
        """
        if self._format is None:
            raise ValueError(
                "Please specify the format of the file(s) to load using the format() method."
            )

        format_str = self._format.lower()
        if format_str == "dbapi" and path is not None:
            raise ValueError(
                "The 'path' parameter is not supported for the dbapi format. Please omit this parameter when calling."
            )
        if format_str != "dbapi" and path is None:
            raise TypeError(
                "DataFrameReader.load() missing 1 required positional argument: 'path'"
            )
        if format_str == "dbapi":
            return self.dbapi(**{k.lower(): v for k, v in self._cur_options.items()})

        loader = getattr(self, self._format, None)
        if loader is not None:
            res = loader(path, _emit_ast=False)
            # AST.
            if _emit_ast and self._ast is not None:
                stmt = self._session._ast_batch.bind()
                ast = with_src_position(stmt.expr.read_load, stmt)
                ast.path = path
                ast.reader.CopyFrom(self._ast)
                res._ast_id = stmt.uid
            return res

        raise ValueError(f"Invalid format '{self._format}'.")

    @publicapi
    def csv(self, path: str, _emit_ast: bool = True) -> DataFrame:
        """Specify the path of the CSV file(s) to load.

        Args:
            path: The stage location of a CSV file, or a stage location that has CSV files.

        Returns:
            a :class:`DataFrame` that is set up to load data from the specified CSV file(s) in a Snowflake stage.
        """
        path = _validate_stage_path(path)
        self._file_path = path
        self._file_type = "CSV"

        schema_to_cast, transformations = None, None

        if not self._user_schema:
            if not self._infer_schema:
                raise SnowparkClientExceptionMessages.DF_MUST_PROVIDE_SCHEMA_FOR_READING_FILE()

            if isinstance(self._session._conn, MockServerConnection):
                self._session._conn.log_not_supported_error(
                    external_feature_name="Read option 'INFER_SCHEMA of value 'TRUE' for file format 'csv'",
                    internal_feature_name="DataFrameReader.csv",
                    parameters_info={
                        "format": "csv",
                        "option": "INFER_SCHEMA",
                        "option_value": "TRUE",
                    },
                    raise_error=NotImplementedError,
                )
            (
                schema,
                schema_to_cast,
                transformations,
                exception,
            ) = self._infer_schema_for_file_format(path, "CSV")
            if exception is not None:
                if isinstance(exception, FileNotFoundError):
                    raise exception
                # if infer schema query fails, use $1, VariantType as schema
                logger.warning(
                    f"Could not infer csv schema due to exception: {exception}. "
                    "\nUsing schema (C1, VariantType()) instead. Please use DataFrameReader.schema() "
                    "to specify user schema for the file."
                )
                schema = [Attribute('"C1"', VariantType(), True)]
                schema_to_cast = [("$1", "C1")]
                transformations = []
        else:
            self._cur_options["INFER_SCHEMA"] = False
            schema = self._user_schema._to_attributes()

        metadata_project, metadata_schema = self._get_metadata_project_and_schema()

        # AST.
        stmt = None
        if _emit_ast and self._ast is not None:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.read_csv, stmt)
            ast.path = path
            ast.reader.CopyFrom(self._ast)

        if self._session.sql_simplifier_enabled:
            df = DataFrame(
                self._session,
                self._session._analyzer.create_select_statement(
                    from_=self._session._analyzer.create_select_snowflake_plan(
                        ReadFileNode(
                            path,
                            self._file_type,
                            self._cur_options,
                            schema,
                            schema_to_cast=schema_to_cast,
                            transformations=transformations,
                            metadata_project=metadata_project,
                            metadata_schema=metadata_schema,
                        ),
                        analyzer=self._session._analyzer,
                    ),
                    analyzer=self._session._analyzer,
                ),
                _ast_stmt=stmt,
                _emit_ast=_emit_ast,
            )
        else:
            df = DataFrame(
                self._session,
                ReadFileNode(
                    path,
                    self._file_type,
                    self._cur_options,
                    schema,
                    schema_to_cast=schema_to_cast,
                    transformations=transformations,
                    metadata_project=metadata_project,
                    metadata_schema=metadata_schema,
                ),
                _ast_stmt=stmt,
                _emit_ast=_emit_ast,
            )
        df._reader = self
        set_api_call_source(df, "DataFrameReader.csv")

        return df

    @publicapi
    def json(self, path: str, _emit_ast: bool = True) -> DataFrame:
        """Specify the path of the JSON file(s) to load.

        Args:
            path: The stage location of a JSON file, or a stage location that has JSON files.

        Returns:
            a :class:`DataFrame` that is set up to load data from the specified JSON file(s) in a Snowflake stage.
        """
        # infer_schema is set to false by default for JSON
        if "INFER_SCHEMA" not in self._cur_options:
            self._cur_options["INFER_SCHEMA"] = False
        df = self._read_semi_structured_file(path, "JSON")

        # AST.
        if _emit_ast and self._ast is not None:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.read_json, stmt)
            ast.path = path
            ast.reader.CopyFrom(self._ast)
            df._ast_id = stmt.uid

        return df

    @publicapi
    def avro(self, path: str, _emit_ast: bool = True) -> DataFrame:
        """Specify the path of the AVRO file(s) to load.

        Args:
            path: The stage location of an AVRO file, or a stage location that has AVRO files.

        Note:
            When using :meth:`DataFrame.select`, quote the column names to select the desired columns.
            This is needed because converting from AVRO to `class`:`DataFrame` does not capitalize the
            column names from the original columns and a :meth:`DataFrame.select` without quote looks for
            capitalized column names.

        Returns:
            a :class:`DataFrame` that is set up to load data from the specified AVRO file(s) in a Snowflake stage.
        """
        df = self._read_semi_structured_file(path, "AVRO")

        # AST.
        if _emit_ast and self._ast is not None:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.read_avro, stmt)
            ast.path = path
            ast.reader.CopyFrom(self._ast)
            df._ast_id = stmt.uid

        return df

    @publicapi
    def parquet(self, path: str, _emit_ast: bool = True) -> DataFrame:
        """Specify the path of the PARQUET file(s) to load.

        Args:
            path: The stage location of a PARQUET file, or a stage location that has PARQUET files.

        Note:
            When using :meth:`DataFrame.select`, quote the column names to select the desired columns.
            This is needed because converting from PARQUET to `class`:`DataFrame` does not capitalize the
            column names from the original columns and a :meth:`DataFrame.select` without quote looks for
            capitalized column names.

        Returns:
            a :class:`DataFrame` that is set up to load data from the specified PARQUET file(s) in a Snowflake stage.
        """

        df = self._read_semi_structured_file(path, "PARQUET")

        # AST.
        if _emit_ast and self._ast is not None:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.read_parquet, stmt)
            ast.path = path
            ast.reader.CopyFrom(self._ast)
            df._ast_id = stmt.uid

        return df

    @publicapi
    def orc(self, path: str, _emit_ast: bool = True) -> DataFrame:
        """Specify the path of the ORC file(s) to load.

        Args:
            path: The stage location of a ORC file, or a stage location that has ORC files.

        Note:
            When using :meth:`DataFrame.select`, quote the column names to select the desired columns.
            This is needed because converting from ORC to `class`:`DataFrame` does not capitalize the
            column names from the original columns and a :meth:`DataFrame.select` without quote looks for
            capitalized column names.

        Returns:
            a :class:`DataFrame` that is set up to load data from the specified ORC file(s) in a Snowflake stage.
        """
        df = self._read_semi_structured_file(path, "ORC")

        # AST.
        if _emit_ast and self._ast is not None:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.read_orc, stmt)
            ast.path = path
            ast.reader.CopyFrom(self._ast)
            df._ast_id = stmt.uid

        return df

    @publicapi
    def xml(self, path: str, _emit_ast: bool = True) -> DataFrame:
        """Specify the path of the XML file(s) to load.

        Args:
            path: The stage location of an XML file, or a stage location that has XML files.

        Returns:
            a :class:`DataFrame` that is set up to load data from the specified XML file(s) in a Snowflake stage.

        Notes about reading XML files using a row tag:

            - We support reading XML by specifying the element tag that represents a single record using the ``rowTag``
              option. See Example 13 in :class:`DataFrameReader`.

            - Each XML record is flattened into a single row, with each XML element or attribute mapped to a column.
              All columns are represented with the variant type to accommodate heterogeneous or nested data. Therefore,
              every column value has a size limit due to the variant type.

            - The column names are derived from the XML element names. It will always be wrapped by single quotes.

            - To parse the nested XML under a row tag, you can use dot notation ``.`` to query the nested fields in
              a DataFrame. See Example 13 in :class:`DataFrameReader`.

            - When ``rowTag`` is specified, the following options are supported for reading XML files
              via :meth:`option()` or :meth:`options()`:

              + ``mode``: Specifies the mode for dealing with corrupt XML records. The default value is ``PERMISSIVE``. The supported values are:

                  - ``PERMISSIVE``: When it encounters a corrupt record, it sets all fields to null and includes a `columnNameOfCorruptRecord` column.

                  - ``DROPMALFORMED``: Ignores the whole record that cannot be parsed correctly.

                  - ``FAILFAST``: When it encounters a corrupt record, it raises an exception immediately.

              + ``columnNameOfCorruptRecord``: Specifies the name of the column that contains the corrupt record.
                The default value is '_corrupt_record'.
        """
        df = self._read_semi_structured_file(path, "XML")

        # AST.
        if _emit_ast and self._ast is not None:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.read_xml, stmt)
            ast.path = path
            ast.reader.CopyFrom(self._ast)
            df._ast_id = stmt.uid

        return df

    @publicapi
    def option(self, key: str, value: Any, _emit_ast: bool = True) -> "DataFrameReader":
        """Sets the specified option in the DataFrameReader.

        Use this method to configure any
        `format-specific options <https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html#format-type-options-formattypeoptions>`_
        and
        `copy options <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions>`_.
        (Note that although specifying copy options can make error handling more robust during the
        reading process, it may have an effect on performance.)

        Args:
            key: Name of the option (e.g. ``compression``, ``skip_header``, etc.).
            value: Value of the option.
        """

        # AST.
        if _emit_ast and self._ast is not None:
            t = self._ast.dataframe_reader.options.add()
            t._1 = key
            build_expr_from_python_val(t._2, value)

        aliased_key = get_aliased_option_name(key, READER_OPTIONS_ALIAS_MAP)
        self._cur_options[aliased_key] = value
        return self

    @publicapi
    def options(
        self, configs: Optional[Dict] = None, _emit_ast: bool = True, **kwargs
    ) -> "DataFrameReader":
        """Sets multiple specified options in the DataFrameReader.

        This method is the same as the :meth:`option` except that you can set multiple options in one call.

        Args:
            configs: Dictionary of the names of options (e.g. ``compression``,
                ``skip_header``, etc.) and their corresponding values.
        """
        if configs and kwargs:
            raise ValueError(
                "Cannot set options with both a dictionary and keyword arguments. Please use one or the other."
            )
        if configs is None:
            if not kwargs:
                raise ValueError("No options were provided")
            configs = kwargs

        for k, v in configs.items():
            self.option(k, v, _emit_ast=_emit_ast)
        return self

    def _infer_schema_for_file_format(
        self, path: str, format: str
    ) -> Tuple[List, List, List, Exception]:
        format_type_options, _ = get_copy_into_table_options(self._cur_options)

        temp_file_format_name = self._session.get_fully_qualified_name_if_possible(
            random_name_for_temp_object(TempObjectType.FILE_FORMAT)
        )
        drop_tmp_file_format_if_exists_query: Optional[str] = None
        use_temp_file_format = "FORMAT_NAME" not in self._cur_options
        file_format_name = self._cur_options.get("FORMAT_NAME", temp_file_format_name)
        infer_schema_options = self._cur_options.get("INFER_SCHEMA_OPTIONS", None)
        infer_schema_query = infer_schema_statement(
            path, file_format_name, infer_schema_options
        )
        try:
            if use_temp_file_format:
                self._session._conn.run_query(
                    create_file_format_statement(
                        file_format_name,
                        format,
                        format_type_options,
                        temp=True,
                        if_not_exist=True,
                        use_scoped_temp_objects=self._session._use_scoped_temp_objects,
                        is_generated=True,
                    ),
                    is_ddl_on_temp_object=True,
                )
                drop_tmp_file_format_if_exists_query = (
                    drop_file_format_if_exists_statement(file_format_name)
                )
            # SNOW-1628625: Schema inference should be done lazily
            results = self._session._conn.run_query(infer_schema_query)["data"]
            if len(results) == 0:
                raise FileNotFoundError(
                    f"Given path: '{path}' could not be found or is empty."
                )
            new_schema = []
            schema_to_cast = []
            transformations: List["snowflake.snowpark.column.Column"] = []
            read_file_transformations = None
            for r in results:
                # Columns for r [column_name, type, nullable, expression, filenames]
                name = quote_name_without_upper_casing(r[0])
                # Parse the type returned by infer_schema command to
                # pass to determine datatype for schema
                data_type_parts = r[1].split("(")
                parts_length = len(data_type_parts)
                if parts_length == 1:
                    data_type = r[1]
                    precision = 0
                    scale = 0
                else:
                    data_type = data_type_parts[0]
                    precision = int(data_type_parts[1].split(",")[0])
                    scale = int(data_type_parts[1].split(",")[1][:-1])
                new_schema.append(
                    Attribute(
                        name,
                        convert_sf_to_sp_type(
                            data_type,
                            precision,
                            scale,
                            0,
                            self._session._conn.max_string_size,
                        ),
                        r[2],
                    )
                )
                identifier = f"$1:{name}::{r[1]}" if format != "CSV" else r[3]
                schema_to_cast.append((identifier, r[0]))
                transformations.append(sql_expr(identifier))
            self._user_schema = StructType._from_attributes(new_schema)
            # If the user sets transformations, we should not override this
            self._infer_schema_transformations = transformations
            self._infer_schema_target_columns = self._user_schema.names
            read_file_transformations = [t._expression.sql for t in transformations]
        except Exception as e:
            return None, None, None, e
        finally:
            # Clean up the file format we created
            if drop_tmp_file_format_if_exists_query is not None:
                self._session._conn.run_query(
                    drop_tmp_file_format_if_exists_query, is_ddl_on_temp_object=True
                )

        return new_schema, schema_to_cast, read_file_transformations, None

    def _get_schema_from_user_input(
        self, user_schema: StructType
    ) -> Tuple[List, List, List]:
        """This function accept a user input structtype and return schemas needed for reading semi-structured file"""
        schema_to_cast = []
        transformations = []
        new_schema = []
        for field in user_schema.fields:
            name = quote_name_without_upper_casing(field._name)
            new_schema.append(
                Attribute(
                    name,
                    field.datatype,
                    field.nullable,
                )
            )
            identifier = f"$1:{name}::{convert_sp_to_sf_type(field.datatype)}"
            schema_to_cast.append((identifier, field._name))
            transformations.append(sql_expr(identifier))
        self._user_schema = StructType._from_attributes(new_schema)
        self._infer_schema_transformations = transformations
        self._infer_schema_target_columns = self._user_schema.names
        read_file_transformations = [t._expression.sql for t in transformations]
        return new_schema, schema_to_cast, read_file_transformations

    def _read_semi_structured_file(self, path: str, format: str) -> DataFrame:
        if isinstance(self._session._conn, MockServerConnection):
            if self._session._conn.is_closed():
                raise SnowparkSessionException(
                    "Cannot perform this operation because the session has been closed.",
                    error_code="1404",
                )
            if format not in LOCAL_TESTING_SUPPORTED_FILE_FORMAT:
                self._session._conn.log_not_supported_error(
                    external_feature_name=f"Read semi structured {format} file",
                    internal_feature_name="DataFrameReader._read_semi_structured_file",
                    parameters_info={"format": str(format)},
                    raise_error=NotImplementedError,
                )

        if self._user_schema and format.lower() != "json":
            raise ValueError(f"Read {format} does not support user schema")
        path = _validate_stage_path(path)
        self._file_path = path
        self._file_type = format

        schema = [Attribute('"$1"', VariantType())]
        read_file_transformations = None
        schema_to_cast = None
        use_user_schema = False

        if self._user_schema:
            (
                new_schema,
                schema_to_cast,
                read_file_transformations,
            ) = self._get_schema_from_user_input(self._user_schema)
            schema = new_schema
            self._cur_options["INFER_SCHEMA"] = False
            use_user_schema = True

        elif self._infer_schema:
            (
                new_schema,
                schema_to_cast,
                read_file_transformations,
                _,  # we don't check for error in case of infer schema failures. We use $1, Variant type
            ) = self._infer_schema_for_file_format(path, format)
            if new_schema:
                schema = new_schema

        metadata_project, metadata_schema = self._get_metadata_project_and_schema()

        if format == "XML" and XML_ROW_TAG_STRING in self._cur_options:
            warning(
                "rowTag",
                "rowTag for reading XML file is in private preview since 1.31.0. Do not use it in production.",
            )

            if is_in_stored_procedure():  # pragma: no cover
                # create a temp stage for udtf import files
                # we have to use "temp" object instead of "scoped temp" object in stored procedure
                # so we need to upload the file to the temp stage first to use register_from_file
                temp_stage = random_name_for_temp_object(TempObjectType.STAGE)
                sql_create_temp_stage = f"create temp stage if not exists {temp_stage} {XML_READER_SQL_COMMENT}"
                self._session.sql(sql_create_temp_stage, _emit_ast=False).collect(
                    _emit_ast=False
                )
                self._session._conn.upload_file(
                    XML_READER_FILE_PATH,
                    temp_stage,
                    compress_data=False,
                    overwrite=True,
                    skip_upload_on_content_match=True,
                )
                python_file_path = f"{STAGE_PREFIX}{temp_stage}/{os.path.basename(XML_READER_FILE_PATH)}"
            else:
                python_file_path = XML_READER_FILE_PATH

            # create udtf
            handler_name = "XMLReader"
            _, input_types = get_types_from_type_hints(
                (XML_READER_FILE_PATH, handler_name), TempObjectType.TABLE_FUNCTION
            )
            output_schema = StructType(
                [StructField(XML_ROW_DATA_COLUMN_NAME, VariantType(), True)]
            )
            xml_reader_udtf = self._session.udtf.register_from_file(
                python_file_path,
                handler_name,
                output_schema=output_schema,
                input_types=input_types,
                packages=["snowflake-snowpark-python"],
                replace=True,
            )
        else:
            xml_reader_udtf = None

        if self._session.sql_simplifier_enabled:
            df = DataFrame(
                self._session,
                self._session._analyzer.create_select_statement(
                    from_=self._session._analyzer.create_select_snowflake_plan(
                        ReadFileNode(
                            path,
                            format,
                            self._cur_options,
                            schema,
                            schema_to_cast=schema_to_cast,
                            transformations=read_file_transformations,
                            metadata_project=metadata_project,
                            metadata_schema=metadata_schema,
                            use_user_schema=use_user_schema,
                            xml_reader_udtf=xml_reader_udtf,
                        ),
                        analyzer=self._session._analyzer,
                    ),
                    analyzer=self._session._analyzer,
                ),
                _emit_ast=False,
            )
        else:
            df = DataFrame(
                self._session,
                ReadFileNode(
                    path,
                    format,
                    self._cur_options,
                    schema,
                    schema_to_cast=schema_to_cast,
                    transformations=read_file_transformations,
                    metadata_project=metadata_project,
                    metadata_schema=metadata_schema,
                    use_user_schema=use_user_schema,
                    xml_reader_udtf=xml_reader_udtf,
                ),
                _emit_ast=False,
            )
        df._reader = self
        if xml_reader_udtf:
            set_api_call_source(df, XML_READER_API_SIGNATURE)
        else:
            set_api_call_source(df, f"DataFrameReader.{format.lower()}")
        return df

    @private_preview(version="1.29.0")
    @publicapi
    def dbapi(
        self,
        create_connection: Callable[[], "Connection"],
        *,
        table: Optional[str] = None,
        query: Optional[str] = None,
        column: Optional[str] = None,
        lower_bound: Optional[Union[str, int]] = None,
        upper_bound: Optional[Union[str, int]] = None,
        num_partitions: Optional[int] = None,
        max_workers: Optional[int] = None,
        query_timeout: Optional[int] = 0,
        fetch_size: Optional[int] = 1000,
        custom_schema: Optional[Union[str, StructType]] = None,
        predicates: Optional[List[str]] = None,
        session_init_statement: Optional[Union[str, List[str]]] = None,
        udtf_configs: Optional[dict] = None,
        fetch_merge_count: int = 1,
        _emit_ast: bool = True,
    ) -> DataFrame:
        """
        Reads data from a database table or query into a DataFrame using a DBAPI connection,
        with support for optional partitioning, parallel processing, and query customization.

        There are multiple methods to partition data and accelerate ingestion.
        These methods can be combined to achieve optimal performance:

        1.Use column, lower_bound, upper_bound and num_partitions at the same time when you need to split large tables into smaller partitions for parallel processing.
        These must all be specified together, otherwise error will be raised.
        2.Set max_workers to a proper positive integer.
        This defines the maximum number of processes and threads used for parallel execution.
        3.Adjusting fetch_size can optimize performance by reducing the number of round trips to the database.
        4.Use predicates to defining WHERE conditions for partitions,
        predicates will be ignored if column is specified to generate partition.
        5.Set custom_schema to avoid snowpark infer schema, custom_schema must have a matched
        column name with table in external data source.

        Args:
            create_connection: A callable that takes no arguments and returns a DB-API compatible database connection.
                The callable must be picklable, as it will be passed to and executed in child processes.
            table: The name of the table in the external data source.
                This parameter cannot be used together with the `query` parameter.
            query: A valid SQL query to be used as the data source in the FROM clause.
                This parameter cannot be used together with the `table` parameter.
            column: The column name used for partitioning the table. Partitions will be retrieved in parallel.
                The column must be of a numeric type (e.g., int or float) or a date type.
                When specifying `column`, `lower_bound`, `upper_bound`, and `num_partitions` must also be provided.
            lower_bound: lower bound of partition, decide the stride of partition along with `upper_bound`.
                This parameter does not filter out data. It must be provided when `column` is specified.
            upper_bound: upper bound of partition, decide the stride of partition along with `lower_bound`.
                This parameter does not filter out data. It must be provided when `column` is specified.
            num_partitions: number of partitions to create when reading in parallel from multiple processes and threads.
                It must be provided when `column` is specified.
            max_workers: number of processes and threads used for parallelism.
            query_timeout: The timeout (in seconds) for each query execution. A default value of `0` means
                the query will never time out. The timeout behavior can also be configured within
                the `create_connection` method when establishing the database connection, depending on the capabilities
                of the DBMS and its driver.
            fetch_size: The number of rows to fetch per batch from the external data source.
                This determines how many rows are retrieved in each round trip,
                which can improve performance for drivers with a low default fetch size.
            custom_schema: a custom snowflake table schema to read data from external data source,
                the column names should be identical to corresponded column names external data source.
                This can be a schema string, for example: "id INTEGER, int_col INTEGER, text_col STRING",
                or StructType, for example: StructType([StructField("ID", IntegerType(), False), StructField("INT_COL", IntegerType(), False), StructField("TEXT_COL", StringType(), False)])
            predicates: A list of expressions suitable for inclusion in WHERE clauses, where each expression defines a partition.
                Partitions will be retrieved in parallel.
                If both `column` and `predicates` are specified, `column` takes precedence.
            session_init_statement: One or more SQL statements executed before fetching data from
                the external data source.
                This can be used for session initialization tasks such as setting configurations.
                For example, `"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"` can be used in SQL Server
                to avoid row locks and improve read performance.
                The `session_init_statement` is executed only once at the beginning of each partition read.
            udtf_configs: A dictionary containing configuration parameters for ingesting external data using a Snowflake UDTF.
                If this parameter is provided, the workload will be executed within a Snowflake UDTF context.

                The dictionary may include the following keys:

                - external_access_integration (str, required): The name of the external access integration,
                    which allows the UDTF to access external endpoints.

                - imports (List[str], optional): A list of stage file names to import into the UDTF.
                    Use this to include any private packages required by your `create_connection()` function.

                - packages (List[str], optional): A list of package names (with optional version numbers)
                    required as dependencies for your `create_connection()` function.
            fetch_merge_count: The number of fetched batches to merge into a single Parquet file
                before uploading it. This improves performance by reducing the number of
                small Parquet files. Defaults to 1, meaning each `fetch_size` batch is written to its own
                Parquet file and uploaded separately.

        Example::
            .. code-block:: python

                import oracledb
                def create_oracledb_connection():
                    connection = oracledb.connect(...)
                    return connection

                df = session.read.dbapi(create_oracledb_connection, table=...)
        """
        if (not table and not query) or (table and query):
            raise SnowparkDataframeReaderException(
                "Either 'table' or 'query' must be provided, but not both."
            )
        table_or_query = table or query
        statements_params_for_telemetry = {STATEMENT_PARAMS_DATA_SOURCE: "1"}
        start_time = time.perf_counter()
        if session_init_statement and isinstance(session_init_statement, str):
            session_init_statement = [session_init_statement]
        partitioner = DataSourcePartitioner(
            create_connection,
            table_or_query,
            column,
            lower_bound,
            upper_bound,
            num_partitions,
            query_timeout,
            fetch_size,
            custom_schema,
            predicates,
            session_init_statement,
            fetch_merge_count,
        )
        struct_schema = partitioner.schema
        partitioned_queries = partitioner.partitions

        if udtf_configs is not None:
            if "external_access_integration" not in udtf_configs:
                raise ValueError(
                    "external_access_integration cannot be None when udtf ingestion is used. Please refer to https://docs.snowflake.com/en/sql-reference/sql/create-external-access-integration to create external access integration"
                )
            partitions_table = random_name_for_temp_object(TempObjectType.TABLE)
            self._session.create_dataframe(
                [[query] for query in partitioned_queries], schema=["partition"]
            ).write.save_as_table(partitions_table, table_type="temp")
            df = partitioner.driver.udtf_ingestion(
                self._session,
                struct_schema,
                partitions_table,
                udtf_configs["external_access_integration"],
                fetch_size=fetch_size,
                imports=udtf_configs.get("imports", None),
                packages=udtf_configs.get("packages", None),
            )
            set_api_call_source(df, DATA_SOURCE_DBAPI_SIGNATURE)
            return df

        with tempfile.TemporaryDirectory() as tmp_dir:
            # create temp table
            snowflake_table_type = "TEMPORARY"
            snowflake_table_name = random_name_for_temp_object(TempObjectType.TABLE)
            create_table_sql = (
                "CREATE "
                f"{snowflake_table_type} "
                "TABLE "
                f"identifier(?) "
                f"""({" , ".join([f'{field.name} {convert_sp_to_sf_type(field.datatype)} {"NOT NULL" if not field.nullable else ""}' for field in struct_schema.fields])})"""
                f"""{DATA_SOURCE_SQL_COMMENT}"""
            )
            params = (snowflake_table_name,)
            logger.debug(f"Creating temporary Snowflake table: {snowflake_table_name}")
            self._session.sql(create_table_sql, params=params, _emit_ast=False).collect(
                statement_params=statements_params_for_telemetry, _emit_ast=False
            )
            # create temp stage
            snowflake_stage_name = random_name_for_temp_object(TempObjectType.STAGE)
            sql_create_temp_stage = (
                f"create {get_temp_type_for_object(self._session._use_scoped_temp_objects, True)} stage"
                f" if not exists {snowflake_stage_name} {DATA_SOURCE_SQL_COMMENT}"
            )
            self._session.sql(sql_create_temp_stage, _emit_ast=False).collect(
                statement_params=statements_params_for_telemetry, _emit_ast=False
            )

            try:
                with ProcessPoolExecutor(
                    max_workers=max_workers
                ) as process_executor, ThreadPoolExecutor(
                    max_workers=max_workers
                ) as thread_executor:
                    thread_pool_futures, process_pool_futures = [], []

                    def ingestion_thread_cleanup_callback(parquet_file_path, _):
                        # clean the local temp file after ingestion to avoid consuming too much temp disk space
                        os.remove(parquet_file_path)

                    # whether each partition should have its own reader is still under discussion
                    logger.debug("Starting to fetch data from the data source.")
                    for partition_idx, query in enumerate(partitioned_queries):
                        process_future = process_executor.submit(
                            _task_fetch_data_from_source_with_retry,
                            partitioner.reader(),
                            query,
                            partition_idx,
                            tmp_dir,
                        )
                        process_pool_futures.append(process_future)
                    # Monitor queue while tasks are running
                    parquet_file_queue = (
                        queue.Queue()
                    )  # maintain the queue of parquet files to process
                    set_of_files_already_added_in_queue = (
                        set()
                    )  # maintain file names we have already put into queue
                    while True:
                        try:
                            # each process and per fetch will create a parquet with a unique file name
                            # we add unseen files to process queue
                            add_unseen_files_to_process_queue(
                                tmp_dir,
                                set_of_files_already_added_in_queue,
                                parquet_file_queue,
                            )
                            file = parquet_file_queue.get_nowait()
                            logger.debug(f"Retrieved file from parquet queue: {file}")
                            thread_future = thread_executor.submit(
                                _upload_and_copy_into_table_with_retry,
                                self._session,
                                file,
                                snowflake_stage_name,
                                snowflake_table_name,
                                "abort_statement",
                                statements_params_for_telemetry,
                            )
                            thread_future.add_done_callback(
                                functools.partial(
                                    ingestion_thread_cleanup_callback, file
                                )
                            )
                            thread_pool_futures.append(thread_future)
                            logger.debug(
                                f"Submitted file {file} to thread executor for ingestion."
                            )
                        except queue.Empty:
                            all_job_done = True
                            unfinished_process_pool_futures = []
                            logger.debug(
                                "Parquet queue is empty, checking unfinished process pool futures."
                            )
                            for future in process_pool_futures:
                                if future.done():
                                    future.result()  # Throw error if the process failed
                                    logger.debug(
                                        "A process future completed successfully."
                                    )
                                else:
                                    unfinished_process_pool_futures.append(future)
                                    all_job_done = False
                            if (
                                all_job_done
                                and parquet_file_queue.empty()
                                and len(os.listdir(tmp_dir)) == 0
                            ):
                                # we finished all the fetch work based on the following 3 conditions:
                                # 1. all jod is done
                                # 2. parquet file queue is empty
                                # 3. no files in the temp work dir as they are all removed in thread future callback
                                # now we just need to wait for all ingestion threads to complete
                                logger.debug(
                                    "All jobs are done, and the parquet file queue is empty. Fetching work is complete."
                                )
                                break
                            process_pool_futures = unfinished_process_pool_futures
                            time.sleep(0.5)

                    for future in as_completed(thread_pool_futures):
                        future.result()  # Throw error if the thread failed
                        logger.debug("A thread future completed successfully.")

            except BaseException:
                # graceful shutdown
                process_executor.shutdown(wait=True)
                thread_executor.shutdown(wait=True)
                raise

            logger.debug(
                "All data has been successfully loaded into the Snowflake table."
            )
            self._session._conn._telemetry_client.send_data_source_perf_telemetry(
                DATA_SOURCE_DBAPI_SIGNATURE, time.perf_counter() - start_time
            )
            # Knowingly generating AST for `session.read.dbapi` calls as simply `session.read.table` calls
            # with the new name for the temporary table into which the external db data was ingressed.
            # Leaving this functionality as client-side only means capturing an AST specifically for
            # this API in a new entity is not valuable from a server-side execution or AST perspective.
            res_df = partitioner.driver.to_result_snowpark_df(
                self, snowflake_table_name, struct_schema, _emit_ast=_emit_ast
            )
            set_api_call_source(res_df, DATA_SOURCE_DBAPI_SIGNATURE)
            return res_df
