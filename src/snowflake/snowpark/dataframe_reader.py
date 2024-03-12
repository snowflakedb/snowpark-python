#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import sys
from logging import getLogger
from typing import Any, Dict, List, Optional, Tuple, Union

import snowflake.snowpark
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    create_file_format_statement,
    drop_file_format_if_exists_statement,
    infer_schema_statement,
    quote_name_without_upper_casing,
)
from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.analyzer.select_statement import (
    SelectSnowflakePlan,
    SelectStatement,
)
from snowflake.snowpark._internal.analyzer.unary_expression import Alias
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.telemetry import set_api_call_source
from snowflake.snowpark._internal.type_utils import ColumnOrName, convert_sf_to_sp_type
from snowflake.snowpark._internal.utils import (
    INFER_SCHEMA_FORMAT_TYPES,
    TempObjectType,
    get_copy_into_table_options,
    random_name_for_temp_object,
)
from snowflake.snowpark.column import METADATA_COLUMN_TYPES, Column, _to_col_if_str
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.functions import sql_expr
from snowflake.snowpark.table import Table
from snowflake.snowpark.types import StructType, VariantType

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

logger = getLogger(__name__)


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

    def __init__(self, session: "snowflake.snowpark.session.Session") -> None:
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

    def table(self, name: Union[str, Iterable[str]]) -> Table:
        """Returns a Table that points to the specified table.

        This method is an alias of :meth:`~snowflake.snowpark.session.Session.table`.

        Args:
            name: Name of the table to use.
        """
        return self._session.table(name)

    def schema(self, schema: StructType) -> "DataFrameReader":
        """Define the schema for CSV files that you want to read.

        Args:
            schema: Schema configuration for the CSV file to be read.

        Returns:
            a :class:`DataFrameReader` instance with the specified schema configuration for the data to be read.
        """
        self._user_schema = schema
        return self

    def with_metadata(
        self, *metadata_cols: Iterable[ColumnOrName]
    ) -> "DataFrameReader":
        """Define the metadata columns that need to be selected from stage files.

        Returns:
            a :class:`DataFrameReader` instance with metadata columns to read.

        See Also:
            https://docs.snowflake.com/en/user-guide/querying-metadata
        """
        self._metadata_cols = [
            _to_col_if_str(col, "DataFrameReader.with_metadata")
            for col in metadata_cols
        ]
        return self

    def csv(self, path: str) -> DataFrame:
        """Specify the path of the CSV file(s) to load.

        Args:
            path: The stage location of a CSV file, or a stage location that has CSV files.

        Returns:
            a :class:`DataFrame` that is set up to load data from the specified CSV file(s) in a Snowflake stage.
        """
        self._file_path = path
        self._file_type = "CSV"

        # infer schema is set to false by default
        if "INFER_SCHEMA" not in self._cur_options:
            self._cur_options["INFER_SCHEMA"] = False
        schema_to_cast, transformations = None, None

        if not self._user_schema:
            if not self._infer_schema:
                raise SnowparkClientExceptionMessages.DF_MUST_PROVIDE_SCHEMA_FOR_READING_FILE()

            (
                schema,
                schema_to_cast,
                transformations,
                exception,
            ) = self._infer_schema_for_file_format(path, "CSV")
            if exception is not None:
                # if infer schema query fails, use $1, VariantType as schema
                logger.warn(
                    f"Could not infer csv schema due to exception: {exception}. "
                    "\nUsing schema (C1, VariantType()) instead. Please use DataFrameReader.schema() "
                    "to specify user schema for the file."
                )
                schema = [Attribute('"C1"', VariantType(), True)]
                schema_to_cast = [("$1", "C1")]
                transformations = []
        else:
            schema = self._user_schema._to_attributes()

        metadata_project, metadata_schema = self._get_metadata_project_and_schema()

        if self._session.sql_simplifier_enabled:
            df = DataFrame(
                self._session,
                self._session._analyzer.create_select_statement(
                    from_=self._session._analyzer.create_select_snowflake_plan(
                        self._session._analyzer.plan_builder.read_file(
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
                self._session._plan_builder.read_file(
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
        return df

    def json(self, path: str) -> DataFrame:
        """Specify the path of the JSON file(s) to load.

        Args:
            path: The stage location of a JSON file, or a stage location that has JSON files.

        Returns:
            a :class:`DataFrame` that is set up to load data from the specified JSON file(s) in a Snowflake stage.
        """
        # infer_schema is set to false by default for JSON
        if "INFER_SCHEMA" not in self._cur_options:
            self._cur_options["INFER_SCHEMA"] = False
        return self._read_semi_structured_file(path, "JSON")

    def avro(self, path: str) -> DataFrame:
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
        return self._read_semi_structured_file(path, "AVRO")

    def parquet(self, path: str) -> DataFrame:
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
        return self._read_semi_structured_file(path, "PARQUET")

    def orc(self, path: str) -> DataFrame:
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
        return self._read_semi_structured_file(path, "ORC")

    def xml(self, path: str) -> DataFrame:
        """Specify the path of the XML file(s) to load.

        Args:
            path: The stage location of an XML file, or a stage location that has XML files.

        Returns:
            a :class:`DataFrame` that is set up to load data from the specified XML file(s) in a Snowflake stage.
        """
        return self._read_semi_structured_file(path, "XML")

    def option(self, key: str, value: Any) -> "DataFrameReader":
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
        self._cur_options[key.upper()] = value
        return self

    def options(self, configs: Dict) -> "DataFrameReader":
        """Sets multiple specified options in the DataFrameReader.

        This method is the same as the :meth:`option` except that you can set multiple options in one call.

        Args:
            configs: Dictionary of the names of options (e.g. ``compression``,
                ``skip_header``, etc.) and their corresponding values.
        """
        for k, v in configs.items():
            self.option(k, v)
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
        infer_schema_query = infer_schema_statement(path, file_format_name)
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
            results = self._session._conn.run_query(infer_schema_query)["data"]
            if len(results) == 0:
                raise FileNotFoundError("Given file not exist or give directory does not contain any csv files.")
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
                        convert_sf_to_sp_type(data_type, precision, scale, 0),
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

    def _read_semi_structured_file(self, path: str, format: str) -> DataFrame:
        from snowflake.snowpark.mock._connection import MockServerConnection

        if isinstance(self._session._conn, MockServerConnection):
            raise NotImplementedError(
                f"[Local Testing] Support for semi structured file {format} is not implemented."
            )

        if self._user_schema:
            raise ValueError(f"Read {format} does not support user schema")
        self._file_path = path
        self._file_type = format

        schema = [Attribute('"$1"', VariantType())]
        read_file_transformations = None
        schema_to_cast = None
        if self._infer_schema:
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
                SelectStatement(
                    from_=SelectSnowflakePlan(
                        self._session._plan_builder.read_file(
                            path,
                            format,
                            self._cur_options,
                            schema,
                            schema_to_cast=schema_to_cast,
                            transformations=read_file_transformations,
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
                self._session._plan_builder.read_file(
                    path,
                    format,
                    self._cur_options,
                    schema,
                    schema_to_cast=schema_to_cast,
                    transformations=read_file_transformations,
                    metadata_project=metadata_project,
                    metadata_schema=metadata_schema,
                ),
            )
        df._reader = self
        set_api_call_source(df, f"DataFrameReader.{format.lower()}")
        return df
