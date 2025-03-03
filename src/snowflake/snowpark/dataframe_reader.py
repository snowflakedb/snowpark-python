#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import datetime
import decimal
import functools
import multiprocessing as mp
import os
import shutil
import tempfile
import time
import traceback
from decimal import ROUND_HALF_EVEN, ROUND_HALF_UP
from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    as_completed,
)


import pytz
from dateutil import parser
import sys
from logging import getLogger
import queue
from typing import Any, Dict, List, Literal, Optional, Tuple, Union, Callable

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
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.telemetry import set_api_call_source
from snowflake.snowpark._internal.type_utils import (
    ColumnOrName,
    convert_sf_to_sp_type,
    convert_sp_to_sf_type,
    type_string_to_type_object,
)
from snowflake.snowpark._internal.data_source_utils import (
    data_source_data_to_pandas_df,
    Connection,
    infer_data_source_schema,
    generate_select_query,
    DATA_SOURCE_DBAPI_SIGNATURE,
    detect_dbms,
    DBMS_TYPE,
    STATEMENT_PARAMS_DATA_SOURCE,
    DATA_SOURCE_SQL_COMMENT,
    generate_sql_with_predicates,
    output_type_handler,
)
from snowflake.snowpark._internal.utils import (
    INFER_SCHEMA_FORMAT_TYPES,
    SNOWFLAKE_PATH_PREFIXES,
    TempObjectType,
    get_aliased_option_name,
    get_copy_into_table_options,
    parse_positional_args_to_list_variadic,
    publicapi,
    get_temp_type_for_object,
    normalize_local_file,
    private_preview,
    random_name_for_temp_object,
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
    DateType,
    DataType,
    _NumericType,
    TimestampType,
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
            with_src_position(reader.dataframe_reader_init)
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
        if _emit_ast:
            stmt = self._session._ast_batch.assign()
            ast = with_src_position(stmt.expr.read_table, stmt)
            ast.reader.CopyFrom(self._ast)
            build_table_name(ast.name, name)

        table = self._session.table(name)

        if _emit_ast:
            table._ast_id = stmt.var_id.bitfield1

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
        if _emit_ast:
            reader = proto.Expr()
            ast = with_src_position(reader.dataframe_reader_schema)
            ast.reader.CopyFrom(self._ast)
            build_proto_from_struct_type(schema, ast.schema)
            self._ast = reader

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
        if _emit_ast:
            reader = proto.Expr()
            ast = with_src_position(reader.dataframe_reader_with_metadata)
            ast.reader.CopyFrom(self._ast)
            col_names, is_variadic = parse_positional_args_to_list_variadic(
                *metadata_cols
            )
            ast.metadata_columns.variadic = is_variadic
            for e in col_names:
                build_expr_from_python_val(ast.metadata_columns.args.add(), e)
            self._ast = reader

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

    def format(
        self, format: Literal["csv", "json", "avro", "parquet", "orc", "xml"]
    ) -> "DataFrameReader":
        """Specify the format of the file(s) to load.

        Args:
            format: The format of the file(s) to load. Supported formats are csv, json, avro, parquet, orc, and xml.

        Returns:
            a :class:`DataFrameReader` instance that is set up to load data from the specified file format in a Snowflake stage.
        """
        self._format = format
        return self

    def load(self, path: Optional[str] = None) -> DataFrame:
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
            return loader(path)

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
            )
        df._reader = self
        set_api_call_source(df, "DataFrameReader.csv")

        # AST.
        if _emit_ast:
            stmt = self._session._ast_batch.assign()
            ast = with_src_position(stmt.expr.read_csv, stmt)
            ast.path = path
            ast.reader.CopyFrom(self._ast)
            df._ast_id = stmt.var_id.bitfield1

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
        if _emit_ast:
            stmt = self._session._ast_batch.assign()
            ast = with_src_position(stmt.expr.read_json, stmt)
            ast.path = path
            ast.reader.CopyFrom(self._ast)
            df._ast_id = stmt.var_id.bitfield1

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
        if _emit_ast:
            stmt = self._session._ast_batch.assign()
            ast = with_src_position(stmt.expr.read_avro, stmt)
            ast.path = path
            ast.reader.CopyFrom(self._ast)
            df._ast_id = stmt.var_id.bitfield1

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
        if _emit_ast:
            stmt = self._session._ast_batch.assign()
            ast = with_src_position(stmt.expr.read_parquet, stmt)
            ast.path = path
            ast.reader.CopyFrom(self._ast)
            df._ast_id = stmt.var_id.bitfield1

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
        if _emit_ast:
            stmt = self._session._ast_batch.assign()
            ast = with_src_position(stmt.expr.read_orc, stmt)
            ast.path = path
            ast.reader.CopyFrom(self._ast)
            df._ast_id = stmt.var_id.bitfield1

        return df

    @publicapi
    def xml(self, path: str, _emit_ast: bool = True) -> DataFrame:
        """Specify the path of the XML file(s) to load.

        Args:
            path: The stage location of an XML file, or a stage location that has XML files.

        Returns:
            a :class:`DataFrame` that is set up to load data from the specified XML file(s) in a Snowflake stage.
        """
        df = self._read_semi_structured_file(path, "XML")

        # AST.
        if _emit_ast:
            stmt = self._session._ast_batch.assign()
            ast = with_src_position(stmt.expr.read_xml, stmt)
            ast.path = path
            ast.reader.CopyFrom(self._ast)
            df._ast_id = stmt.var_id.bitfield1

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
        if _emit_ast:
            reader = proto.Expr()
            ast = with_src_position(reader.dataframe_reader_option)
            ast.reader.CopyFrom(self._ast)
            ast.key = key
            build_expr_from_python_val(ast.value, value)
            self._ast = reader

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

        # AST.
        if _emit_ast:
            reader = proto.Expr()
            ast = with_src_position(reader.dataframe_reader_options)
            ast.reader.CopyFrom(self._ast)
            for k, v in configs.items():
                t = ast.configs.add()
                t._1 = k
                build_expr_from_python_val(t._2, v)
            self._ast = reader

        for k, v in configs.items():
            self.option(k, v, _emit_ast=False)
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
                        ),
                        analyzer=self._session._analyzer,
                    ),
                    analyzer=self._session._analyzer,
                ),
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
                ),
            )
        df._reader = self
        set_api_call_source(df, f"DataFrameReader.{format.lower()}")
        return df

    @private_preview(version="1.29.0")
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
        fetch_size: Optional[int] = 0,
        custom_schema: Optional[Union[str, StructType]] = None,
        predicates: Optional[List[str]] = None,
        session_init_statement: Optional[str] = None,
    ) -> DataFrame:
        """Reads data from a database table using a DBAPI connection with optional partitioning, parallel processing, and query customization.
        By default, the function reads the entire table at a time without a query timeout.
        There are several ways to break data into small pieces and speed up ingestion, you can also combine them to acquire optimal performance:
            1.Use column, lower_bound, upper_bound and num_partitions at the same time when you need to split large tables into smaller partitions for parallel processing. These must all be specified together, otherwise error will be raised.
            2.Set max_workers to a proper positive integer. This defines the maximum number of processes and threads used for parallel execution.
            3.Adjusting fetch_size can optimize performance by reducing the number of round trips to the database.
            4.Use predicates to defining WHERE conditions for partitions, predicates will be ignored if column is specified to generate partition.
            5.Set custom_schema to avoid snowpark infer schema, custom_schema must have a matched column name with table in external data source.
        You can also use session_init_statement to perform any SQL that you want to execute on external data source before fetching data.
        Args:
            create_connection: a function that return a dbapi connection
            table: Specifies the name of the table in the external data source. This parameter cannot be set simultaneously with the query parameter.
            query: A valid SQL query to be used in the FROM clause. This parameter cannot be set simultaneously with the table parameter.
            column: column name used to create partition, the column type must be numeric like int type or float type, or Date type.
            lower_bound: lower bound of partition, decide the stride of partition along with upper_bound, this parameter does not filter out data.
            upper_bound: upper bound of partition, decide the stride of partition along with lower_bound, this parameter does not filter out data.
            num_partitions: number of partitions to create when reading in parallel from multiple processes and threads.
            max_workers: number of processes and threads used for parallelism.
            query_timeout: timeout(seconds) for each query, default value is 0, which means never timeout.
            fetch_size: batch size when fetching from external data source, which determine how many rows fetched per round trip. This improve performace for drivers that have a low default fetch size.
            custom_schema: a custom snowflake table schema to read data from external data source, the column names should be identical to corresponded column names external data source. This can be a schema string, for example: "id INTEGER, int_col INTEGER, text_col STRING", or StructType, for example: StructType([StructField("ID", IntegerType(), False)])
            predicates: a list of expressions suitable for inclusion in WHERE clauses, each defines a partition
            session_init_statement: session initiation statements for external data source, this statement will be executed before fetch data from external data source, for example: "insert into test_table values (1, 'sample_data')" will insert data into test_table before fetch data from it.
        Note:
            column, lower_bound, upper_bound and num_partitions must be specified if any one of them is specified.
        """
        if (not table and not query) or (table and query):
            raise SnowparkDataframeReaderException(
                "Either 'table' or 'query' must be provided, but not both."
            )
        table_or_query = table or query
        statements_params_for_telemetry = {STATEMENT_PARAMS_DATA_SOURCE: "1"}
        start_time = time.perf_counter()
        conn = create_connection()
        dbms_type, driver_info = detect_dbms(conn)
        logger.debug(f"Detected DBMS: {dbms_type}, Driver Info: {driver_info}")
        if custom_schema is None:
            struct_schema = infer_data_source_schema(
                conn, table_or_query, dbms_type, driver_info
            )
        else:
            if isinstance(custom_schema, str):
                struct_schema = type_string_to_type_object(custom_schema)
                if not isinstance(struct_schema, StructType):
                    raise ValueError(
                        f"Invalid schema string: {custom_schema}. "
                        f"You should provide a valid schema string representing a struct type."
                        'For example: "id INTEGER, int_col INTEGER, text_col STRING".'
                    )
            elif isinstance(custom_schema, StructType):
                struct_schema = custom_schema
            else:
                raise ValueError(
                    f"Invalid schema type: {type(custom_schema)}."
                    'The schema should be either a valid schema string, for example: "id INTEGER, int_col INTEGER, text_col STRING".'
                    'or a valid StructType, for example: StructType([StructField("ID", IntegerType(), False)])'
                )

        select_query = generate_select_query(
            table_or_query, struct_schema, dbms_type, driver_info
        )
        logger.debug(f"Generated select query: {select_query}")
        if column is None:
            if (
                lower_bound is not None
                or upper_bound is not None
                or num_partitions is not None
            ):
                raise ValueError(
                    "when column is not specified, lower_bound, upper_bound, num_partitions are expected to be None"
                )
            if predicates is None:
                partitioned_queries = [select_query]
            else:
                partitioned_queries = generate_sql_with_predicates(
                    select_query, predicates
                )
        else:
            if lower_bound is None or upper_bound is None or num_partitions is None:
                raise ValueError(
                    "when column is specified, lower_bound, upper_bound, num_partitions must be specified"
                )

            column_type = None
            for field in struct_schema.fields:
                if field.name.lower() == column.lower():
                    column_type = field.datatype
            if column_type is None:
                raise ValueError(f"Specified column {column} does not exist")

            if not isinstance(column_type, _NumericType) and not isinstance(
                column_type, DateType
            ):
                raise ValueError(
                    f"unsupported type {column_type}, column must be a numeric type like int and float, or date type"
                )
            partitioned_queries = self._generate_partition(
                select_query,
                column_type,
                column,
                lower_bound,
                upper_bound,
                num_partitions,
            )
        with tempfile.TemporaryDirectory() as tmp_dir:
            # create temp table
            snowflake_table_type = "TEMPORARY"
            snowflake_table_name = random_name_for_temp_object(TempObjectType.TABLE)
            create_table_sql = (
                "CREATE "
                f"{snowflake_table_type} "
                "TABLE "
                f"identifier(?) "
                f"""({" , ".join([f'"{field.name}" {convert_sp_to_sf_type(field.datatype)} {"NOT NULL" if not field.nullable else ""}' for field in struct_schema.fields])})"""
                f"""{DATA_SOURCE_SQL_COMMENT}"""
            )
            params = (snowflake_table_name,)
            logger.debug(f"Creating temporary Snowflake table: {snowflake_table_name}")
            self._session.sql(create_table_sql, params=params).collect(
                statement_params=statements_params_for_telemetry
            )
            # create temp stage
            snowflake_stage_name = random_name_for_temp_object(TempObjectType.STAGE)
            sql_create_temp_stage = (
                f"create {get_temp_type_for_object(self._session._use_scoped_temp_objects, True)} stage"
                f" if not exists {snowflake_stage_name} {DATA_SOURCE_SQL_COMMENT}"
            )
            self._session.sql(sql_create_temp_stage).collect(
                statement_params=statements_params_for_telemetry
            )

            try:
                with mp.Manager() as process_manager, ProcessPoolExecutor(
                    max_workers=max_workers
                ) as process_executor, ThreadPoolExecutor(
                    max_workers=max_workers
                ) as thread_executor:
                    thread_pool_futures, process_pool_futures = [], []
                    parquet_file_queue = process_manager.Queue()

                    def ingestion_thread_cleanup_callback(parquet_file_path, _):
                        # clean the local temp file after ingestion to avoid consuming too much temp disk space
                        shutil.rmtree(parquet_file_path, ignore_errors=True)

                    logger.debug("Starting to fetch data from the data source.")
                    for partition_idx, query in enumerate(partitioned_queries):
                        process_future = process_executor.submit(
                            DataFrameReader._task_fetch_from_data_source_with_retry,
                            parquet_file_queue,
                            create_connection,
                            query,
                            struct_schema,
                            partition_idx,
                            tmp_dir,
                            dbms_type,
                            query_timeout,
                            fetch_size,
                            session_init_statement,
                        )
                        process_pool_futures.append(process_future)
                    # Monitor queue while tasks are running
                    while True:
                        try:
                            file = parquet_file_queue.get_nowait()
                            logger.debug(f"Retrieved file from parquet queue: {file}")
                            thread_future = thread_executor.submit(
                                self._upload_and_copy_into_table_with_retry,
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
                            if all_job_done and parquet_file_queue.empty():
                                # all jod is done and parquet file queue is empty, we finished all the fetch work
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
            res_df = self.table(snowflake_table_name)
            set_api_call_source(res_df, DATA_SOURCE_DBAPI_SIGNATURE)
            return res_df

    def _generate_partition(
        self,
        select_query: str,
        column_type: DataType,
        column: Optional[str] = None,
        lower_bound: Optional[Union[str, int]] = None,
        upper_bound: Optional[Union[str, int]] = None,
        num_partitions: Optional[int] = None,
    ) -> List[str]:
        processed_lower_bound = self._to_internal_value(lower_bound, column_type)
        processed_upper_bound = self._to_internal_value(upper_bound, column_type)
        if processed_lower_bound > processed_upper_bound:
            raise ValueError("lower_bound cannot be greater than upper_bound")

        if processed_lower_bound == processed_upper_bound or num_partitions <= 1:
            return [select_query]

        if (processed_upper_bound - processed_lower_bound) >= num_partitions or (
            processed_upper_bound - processed_lower_bound
        ) < 0:
            actual_num_partitions = num_partitions
        else:
            actual_num_partitions = processed_upper_bound - processed_lower_bound
            logger.warning(
                "The number of partitions is reduced because the specified number of partitions is less than the difference between upper bound and lower bound."
            )

        # decide stride length
        upper_stride = (
            processed_upper_bound / decimal.Decimal(actual_num_partitions)
        ).quantize(decimal.Decimal("1e-18"), rounding=ROUND_HALF_EVEN)
        lower_stride = (
            processed_lower_bound / decimal.Decimal(actual_num_partitions)
        ).quantize(decimal.Decimal("1e-18"), rounding=ROUND_HALF_EVEN)
        preciseStride = upper_stride - lower_stride
        stride = int(preciseStride)

        lost_num_of_strides = (
            (preciseStride - decimal.Decimal(stride))
            * decimal.Decimal(actual_num_partitions)
            / decimal.Decimal(stride)
        )
        lower_bound_with_stride_alignment = processed_lower_bound + int(
            (lost_num_of_strides / 2 * decimal.Decimal(stride)).quantize(
                decimal.Decimal("1"), rounding=ROUND_HALF_UP
            )
        )

        current_value = lower_bound_with_stride_alignment

        partition_queries = []
        for i in range(actual_num_partitions):
            l_bound = (
                f"{column} >= '{self._to_external_value(current_value, column_type)}'"
                if i != 0
                else ""
            )
            current_value += stride
            u_bound = (
                f"{column} < '{self._to_external_value(current_value, column_type)}'"
                if i != actual_num_partitions - 1
                else ""
            )

            if u_bound == "":
                where_clause = l_bound
            elif l_bound == "":
                where_clause = f"{u_bound} OR {column} is null"
            else:
                where_clause = f"{l_bound} AND {u_bound}"

            partition_queries.append(select_query + f" WHERE {where_clause}")

        return partition_queries

    # this function is only used in data source API for SQL server
    def _to_internal_value(self, value: Union[int, str, float], column_type: DataType):
        if isinstance(column_type, _NumericType):
            return int(value)
        elif isinstance(column_type, (TimestampType, DateType)):
            # TODO: SNOW-1909315: support timezone
            dt = parser.parse(value)
            return int(dt.replace(tzinfo=pytz.UTC).timestamp())
        else:
            raise ValueError(
                f"unsupported type {column_type} for partition, column must be a numeric type like int and float, or date type"
            )

    # this function is only used in data source API for SQL server
    def _to_external_value(self, value: Union[int, str, float], column_type: DataType):
        if isinstance(column_type, _NumericType):
            return value
        elif isinstance(column_type, (TimestampType, DateType)):
            # TODO: SNOW-1909315: support timezone
            return datetime.datetime.fromtimestamp(value, tz=pytz.UTC)
        else:
            raise ValueError(
                f"unsupported type {column_type} for partition, column must be a numeric type like int and float, or date type"
            )

    @staticmethod
    def _retry_run(func: Callable, *args, **kwargs) -> Any:
        retry_count = 0
        last_error = None
        error_trace = ""
        func_name = func.__name__
        while retry_count < _MAX_RETRY_TIME:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_error = e
                error_trace = traceback.format_exc()
                retry_count += 1
                logger.debug(
                    f"[{func_name}] Attempt {retry_count}/{_MAX_RETRY_TIME} failed with {type(last_error).__name__}: {str(last_error)}. Retrying..."
                )
        error_message = (
            f"Function `{func_name}` failed after {_MAX_RETRY_TIME} attempts.\n"
            f"Last error: [{type(last_error).__name__}] {str(last_error)}\n"
            f"Traceback:\n{error_trace}"
        )
        final_error = SnowparkDataframeReaderException(message=error_message)
        raise final_error

    def _upload_and_copy_into_table(
        self,
        local_file: str,
        snowflake_stage_name: str,
        snowflake_table_name: Optional[str] = None,
        on_error: Optional[str] = "abort_statement",
        statements_params: Optional[Dict[str, str]] = None,
    ):
        file_name = os.path.basename(local_file)
        put_query = (
            f"PUT {normalize_local_file(local_file)} "
            f"@{snowflake_stage_name} OVERWRITE=TRUE {DATA_SOURCE_SQL_COMMENT}"
        )
        copy_into_table_query = f"""
        COPY INTO {snowflake_table_name} FROM @{snowflake_stage_name}/{file_name}
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
        PURGE=TRUE
        ON_ERROR={on_error}
        {DATA_SOURCE_SQL_COMMENT}
        """
        self._session.sql(put_query).collect(statement_params=statements_params)
        self._session.sql(copy_into_table_query).collect(
            statement_params=statements_params
        )

    def _upload_and_copy_into_table_with_retry(
        self,
        local_file: str,
        snowflake_stage_name: str,
        snowflake_table_name: str,
        on_error: Optional[str] = "abort_statement",
        statements_params: Optional[Dict[str, str]] = None,
    ):
        self._retry_run(
            self._upload_and_copy_into_table,
            local_file,
            snowflake_stage_name,
            snowflake_table_name,
            on_error,
            statements_params,
        )

    @staticmethod
    def _task_fetch_from_data_source(
        parquet_file_queue: queue.Queue,
        create_connection: Callable[[], "Connection"],
        query: str,
        schema: StructType,
        partition_idx: int,
        tmp_dir: str,
        dbms_type: DBMS_TYPE,
        query_timeout: int = 0,
        fetch_size: int = 0,
        session_init_statement: Optional[str] = None,
    ):
        def convert_to_parquet(fetched_data, fetch_idx):
            df = data_source_data_to_pandas_df(fetched_data, schema)
            if df.empty:
                logger.debug(
                    f"The DataFrame is empty, no parquet file is generated for partition {partition_idx} fetch {fetch_idx}."
                )
                return None
            path = os.path.join(
                tmp_dir, f"data_partition{partition_idx}_fetch{fetch_idx}.parquet"
            )
            df.to_parquet(path)
            return path

        conn = create_connection()
        # this is specified to pyodbc, need other way to manage timeout on other drivers
        if dbms_type == DBMS_TYPE.SQL_SERVER_DB:
            conn.timeout = query_timeout
        if dbms_type == DBMS_TYPE.ORACLE_DB:
            conn.outputtypehandler = output_type_handler
        cursor = conn.cursor()
        if session_init_statement:
            cursor.execute(session_init_statement)
        if fetch_size == 0:
            cursor.execute(query)
            result = cursor.fetchall()
            parquet_file_path = convert_to_parquet(result, 0)
            if parquet_file_path:
                parquet_file_queue.put(parquet_file_path)
        elif fetch_size > 0:
            cursor = cursor.execute(query)
            fetch_idx = 0
            while True:
                rows = cursor.fetchmany(fetch_size)
                if not rows:
                    break
                parquet_file_path = convert_to_parquet(rows, fetch_idx)
                if parquet_file_path:
                    parquet_file_queue.put(parquet_file_path)
                fetch_idx += 1
        else:
            raise ValueError("fetch size cannot be smaller than 0")

    @staticmethod
    def _task_fetch_from_data_source_with_retry(
        parquet_file_queue: queue.Queue,
        create_connection: Callable[[], "Connection"],
        query: str,
        schema: StructType,
        partition_idx: int,
        tmp_dir: str,
        dbms_type: DBMS_TYPE,
        query_timeout: int = 0,
        fetch_size: int = 0,
        session_init_statement: Optional[str] = None,
    ):
        DataFrameReader._retry_run(
            DataFrameReader._task_fetch_from_data_source,
            parquet_file_queue,
            create_connection,
            query,
            schema,
            partition_idx,
            tmp_dir,
            dbms_type,
            query_timeout,
            fetch_size,
            session_init_statement,
        )
