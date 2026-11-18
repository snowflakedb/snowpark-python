#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import json
import os
import re
import time
from collections import defaultdict
from collections.abc import Iterable
from logging import getLogger
from typing import Any, Dict, List, Literal, Optional, Tuple, Union, Callable
from datetime import datetime

import snowflake.snowpark
import snowflake.snowpark.context as context
import snowflake.snowpark._internal.proto.generated.ast_pb2 as proto
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    convert_value_to_sql_option,
    create_file_format_statement,
    drop_file_format_if_exists_statement,
    infer_schema_statement,
    quote_name_without_upper_casing,
    single_quote,
    unquote_if_quoted,
)
from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import ReadFileNode
from snowflake.snowpark._internal.analyzer.unary_expression import Alias
from snowflake.snowpark._internal.ast.utils import (
    build_expr_from_python_val,
    build_proto_from_struct_type,
    build_table_name,
    with_src_position,
)
from snowflake.snowpark._internal import document_reader
from snowflake.snowpark._internal.data_source import JDBC
from snowflake.snowpark._internal.data_source.datasource_partitioner import (
    DataSourcePartitioner,
)
from snowflake.snowpark._internal.data_source.datasource_typing import Connection
from snowflake.snowpark._internal.data_source.utils import (
    STATEMENT_PARAMS_DATA_SOURCE,
    DATA_SOURCE_DBAPI_SIGNATURE,
    create_data_source_table_and_stage,
    local_ingestion,
    STATEMENT_PARAMS_DATA_SOURCE_JDBC,
    DATA_SOURCE_JDBC_SIGNATURE,
    get_jdbc_dbms,
)
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.telemetry import TelemetryField, set_api_call_source
from snowflake.snowpark._internal.type_utils import (
    ColumnOrName,
    convert_sf_to_sp_type,
    convert_sp_to_sf_type,
    most_permissive_type,
    type_string_to_type_object,
    _parse_structured_type_str,
)
from snowflake.snowpark._internal.udf_utils import get_types_from_type_hints
from snowflake.snowpark._internal.xml_reader import DEFAULT_CHUNK_SIZE
from snowflake.snowpark._internal.xml_schema_inference import (
    merge_struct_types,
    canonicalize_type,
)
from snowflake.snowpark._internal.utils import (
    SNOWURL_PREFIX,
    STAGE_PREFIX,
    XML_ROW_TAG_STRING,
    XML_ROW_DATA_COLUMN_NAME,
    XML_READER_FILE_PATH,
    XML_SCHEMA_INFERENCE_FILE_PATH,
    XML_READER_API_SIGNATURE,
    INFER_SCHEMA_FORMAT_TYPES,
    SNOWFLAKE_PATH_PREFIXES,
    TempObjectType,
    get_aliased_option_name,
    get_copy_into_table_options,
    get_stage_parts,
    is_in_stored_procedure,
    parse_positional_args_to_list_variadic,
    private_preview,
    publicapi,
    random_name_for_temp_object,
    warning,
    experimental,
    IcebergChangesConfig,
    TimeTravelConfig,
)
from snowflake.snowpark.column import METADATA_COLUMN_TYPES, Column, _to_col_if_str
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.exceptions import (
    SnowparkSessionException,
    SnowparkDataframeReaderException,
)
from snowflake.snowpark.functions import sql_expr, col, concat, lit, to_file
from snowflake.snowpark.mock._connection import MockServerConnection
from snowflake.snowpark.table import Table
from snowflake.snowpark.types import (
    ArrayType,
    MapType,
    StringType,
    StructType,
    TimestampTimeZone,
    VariantType,
    StructField,
)

logger = getLogger(__name__)

_NOT_NULL_RE = re.compile(r"\s+NOT\s+NULL", re.IGNORECASE)


LOCAL_TESTING_SUPPORTED_FILE_FORMAT = ("JSON",)

# Time travel parameters that make a read a change-tracking (CHANGES) read.
_CHANGES_PARAMS = (
    "changes",
    "end_statement",
    "end_offset",
    "end_timestamp",
    "end_timestamp_type",
)

_TIME_TRAVEL_OPTIONS_PARAMS_MAP = {
    "TIME_TRAVEL_MODE": "time_travel_mode",
    "STATEMENT": "statement",
    "OFFSET": "offset",
    "TIMESTAMP": "timestamp",
    "TIMESTAMP_TYPE": "timestamp_type",
    "STREAM": "stream",
    **{param.upper(): param for param in _CHANGES_PARAMS},
}

READER_OPTIONS_ALIAS_MAP = {
    "DELIMITER": "FIELD_DELIMITER",
    "HEADER": "PARSE_HEADER",
    "PATHGLOBFILTER": "PATTERN",
    "FILENAMEPATTERN": "PATTERN",
    "INFERSCHEMA": "INFER_SCHEMA",
    "SAMPLINGRATIO": "SAMPLING_RATIO",
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


def _extract_time_travel_from_options(options: dict) -> dict:
    """
    Extract time travel options from DataFrameReader options.

    Special handling for 'AS-OF-TIMESTAMP' (PySpark compatibility):
    - Automatically sets time_travel_mode to 'at'
    - Cannot be used with time_travel_mode='before' (raises error)
    - Cannot be mixed with regular 'timestamp' option (raises error)

    Special handling for 'SNAPSHOT-ID' / 'SNAPSHOT_ID' (Spark Iceberg
    compatibility) — both aliases map to the internal ``version`` time
    travel parameter:
    - Automatically set time_travel_mode to 'at'
      (Iceberg snapshot ids only support ``AT(VERSION => N)``, not ``BEFORE``)
    - Cannot be used with time_travel_mode='before' (raises error)

    Special handling for 'VERSION_TAG' / 'VERSION-TAG' (Iceberg tag name) —
    both aliases map to the internal ``version_tag`` time travel parameter
    and emit ``AT(VERSION_REF => '<name>')`` on the Snowflake side.

    Special handling for 'VERSION_REF' / 'VERSION-REF' (Iceberg tag or
    branch name) — maps to the internal ``version_ref`` parameter.

    Special handling for 'BRANCH' (Spark Iceberg WAP branch read) — maps
    to the internal ``branch`` parameter and emits
    ``AT(VERSION_REF => '<name>')``.

    Special handling for 'TAG' (Spark Iceberg ``SparkReadOptions.TAG``) —
    maps to the internal ``version_tag`` parameter.
    """
    result = {}
    excluded_keys = set()

    # Handle PySpark as-of-timestamp special case
    if "AS-OF-TIMESTAMP" in options:
        if (
            "TIME_TRAVEL_MODE" in options
            and options["TIME_TRAVEL_MODE"].lower() == "before"
        ):
            raise ValueError(
                "Cannot use 'as-of-timestamp' option with time_travel_mode='before'"
            )
        if "TIMESTAMP" in options:
            raise ValueError(
                "Cannot use both 'as-of-timestamp' and 'timestamp' options."
            )
        result["time_travel_mode"] = "at"
        result["timestamp"] = options["AS-OF-TIMESTAMP"]
        excluded_keys.add("TIMESTAMP")

    # Handle Iceberg snapshot id (Spark ``snapshot-id`` / ``snapshot_id``).
    # Auto-sets mode='at' since ``AT(VERSION => N)`` is the only valid form.
    snapshot_id_value = options.get("SNAPSHOT-ID")
    snapshot_id_source = "snapshot-id"
    if snapshot_id_value is None:
        snapshot_id_value = options.get("SNAPSHOT_ID")
        snapshot_id_source = "snapshot_id"
    if snapshot_id_value is not None:
        if (
            "TIME_TRAVEL_MODE" in options
            and options["TIME_TRAVEL_MODE"].lower() == "before"
        ):
            raise ValueError(
                f"Cannot use '{snapshot_id_source}' option with "
                "time_travel_mode='before'. Iceberg snapshot id time travel "
                "only supports time_travel_mode='at'."
            )
        # Coerce string snapshot ids (Spark accepts both string and long
        # literals via .option(); we normalize to int so the SQL emits an
        # unquoted long).
        try:
            result["version"] = int(snapshot_id_value)
        except (TypeError, ValueError):
            raise ValueError(
                f"'{snapshot_id_source}' must be a 64-bit integer Iceberg "
                f"snapshot id, got {snapshot_id_value!r}."
            )
        result["time_travel_mode"] = "at"

    def _set_named_ref_option(
        result: dict,
        *,
        param_name: str,
        option_source: str,
        raw_value: Any,
    ) -> None:
        if (
            "TIME_TRAVEL_MODE" in options
            and options["TIME_TRAVEL_MODE"].lower() == "before"
        ):
            raise ValueError(
                f"Cannot use '{option_source}' option with "
                "time_travel_mode='before'. Iceberg named-ref time travel "
                "only supports time_travel_mode='at'."
            )
        result[param_name] = str(raw_value)
        result["time_travel_mode"] = "at"

    # Handle Iceberg tag (``version_tag`` / ``version-tag`` / Spark ``tag``).
    version_tag_value = options.get("VERSION_TAG")
    version_tag_source = "version_tag"
    if version_tag_value is None:
        version_tag_value = options.get("VERSION-TAG")
        version_tag_source = "version-tag"
    if version_tag_value is None:
        version_tag_value = options.get("TAG")
        version_tag_source = "tag"
    if version_tag_value is not None:
        _set_named_ref_option(
            result,
            param_name="version_tag",
            option_source=version_tag_source,
            raw_value=version_tag_value,
        )

    version_ref_value = options.get("VERSION_REF")
    version_ref_source = "version_ref"
    if version_ref_value is None:
        version_ref_value = options.get("VERSION-REF")
        version_ref_source = "version-ref"
    if version_ref_value is not None:
        _set_named_ref_option(
            result,
            param_name="version_ref",
            option_source=version_ref_source,
            raw_value=version_ref_value,
        )

    branch_value = options.get("BRANCH")
    if branch_value is not None:
        _set_named_ref_option(
            result,
            param_name="branch",
            option_source="branch",
            raw_value=branch_value,
        )

    for option_key, param_name in _TIME_TRAVEL_OPTIONS_PARAMS_MAP.items():
        if option_key in options and option_key not in excluded_keys:
            result[param_name] = options[option_key]

    return result


def _get_reader_option(options: dict, *keys: str):
    """Case-insensitive lookup for a reader option key."""
    for key in keys:
        for option_key, value in options.items():
            if option_key.upper() == key.upper():
                return value
    return None


def _extract_iceberg_changes_from_options(options: dict) -> dict:
    """Extract Spark Iceberg incremental-read options from a reader dict.

    Maps ``start-snapshot-id`` / ``end-snapshot-id`` (and underscore
    variants) to internal ``start_snapshot_id`` / ``end_snapshot_id``
    kwargs consumed by :meth:`Session.table`.
    """
    start = _get_reader_option(options, "start-snapshot-id", "start_snapshot_id")
    end = _get_reader_option(options, "end-snapshot-id", "end_snapshot_id")
    if start is None and end is None:
        return {}
    if start is None:
        raise ValueError(
            "Iceberg incremental read requires 'start-snapshot-id'; "
            "'end-snapshot-id' cannot be used alone."
        )
    start_id = IcebergChangesConfig._coerce_snapshot_id(start, "start-snapshot-id")
    end_id = None
    if end is not None:
        end_id = IcebergChangesConfig._coerce_snapshot_id(end, "end-snapshot-id")
    return {"start_snapshot_id": start_id, "end_snapshot_id": end_id}


_CHANGES_OPTION_KEYS = {param.upper() for param in _CHANGES_PARAMS}

# Iceberg selectors that default the time travel mode to 'at'.
_ICEBERG_TIME_TRAVEL_PARAMS = {"version", "version_tag", "version_ref", "branch"}

# Direct time travel arguments that do not select the direct-argument path of
# DataFrameReader.table on their own; without a mode they fall through to the
# reader options.
_POINT_IN_TIME_PARAMS = {"statement", "offset", "timestamp", "timestamp_type", "stream"}

_OTHER_TIME_TRAVEL_OPTION_KEYS = (
    "AS-OF-TIMESTAMP",
    "SNAPSHOT-ID",
    "SNAPSHOT_ID",
    "VERSION_TAG",
    "VERSION-TAG",
    "TAG",
    "VERSION_REF",
    "VERSION-REF",
    "BRANCH",
)


def _time_travel_option_keys(options: dict) -> list[str]:
    """Return reader option keys that configure time travel."""
    return [
        key
        for key in options
        if key.upper() in _TIME_TRAVEL_OPTIONS_PARAMS_MAP
        or key.upper() in _OTHER_TIME_TRAVEL_OPTION_KEYS
    ]


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
        self._data_source_format = ["jdbc", "dbapi"]
        self._xml_inferred_schema: Optional[StructType] = None

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
    def table(
        self,
        name: Union[str, Iterable[str]],
        _emit_ast: bool = True,
        *,
        time_travel_mode: Optional[Literal["at", "before"]] = None,
        statement: Optional[str] = None,
        offset: Optional[int] = None,
        timestamp: Optional[Union[str, datetime]] = None,
        timestamp_type: Optional[Union[str, TimestampTimeZone]] = None,
        stream: Optional[str] = None,
        changes: Optional[Literal["default", "append_only"]] = None,
        end_statement: Optional[str] = None,
        end_offset: Optional[int] = None,
        end_timestamp: Optional[Union[str, datetime]] = None,
        end_timestamp_type: Optional[Union[str, TimestampTimeZone]] = None,
        **kwargs,
    ) -> Table:
        """Returns a Table that points to the specified table.

        This method is an alias of :meth:`~snowflake.snowpark.session.Session.table` with
        additional support for setting time travel options via the :meth:`option` method.

        Args:
            name: Name of the table to use.
            time_travel_mode: Time travel mode, either 'at' or 'before'. Can also be set via
                ``option("time_travel_mode", "at")``.
            statement: Query ID for time travel. Can also be set via ``option("statement", "query_id")``.
            offset: Negative integer representing seconds in the past for time travel.
                Can also be set via ``option("offset", -60)``.
            timestamp: Timestamp string or datetime object for time travel.
                Can also be set via ``option("timestamp", "2023-01-01 12:00:00")`` or
                ``option("as-of-timestamp", "2023-01-01 12:00:00")``.
            timestamp_type: Type of timestamp interpretation ('NTZ', 'LTZ', or 'TZ').
                Can also be set via ``option("timestamp_type", "LTZ")``.
            stream: Stream name for time travel. Can also be set via ``option("stream", "stream_name")``.
            changes: ``'default'`` or ``'append_only'`` to read the changes of a table with change tracking
                enabled (``CHANGES (INFORMATION => ...)``). Requires ``time_travel_mode`` and exactly one of
                statement, offset, timestamp or stream. Can also be set via ``option("changes", "default")``.
                See :meth:`Session.table`.
            end_statement: Query ID that sets the end of the changes. Requires ``changes``.
                At most one of ``end_statement``, ``end_offset`` and ``end_timestamp`` can be given.
                Can also be set via ``option("end_statement", "query_id")``.
            end_offset: Zero or a negative integer representing seconds in the past that sets the end of the changes.
                Requires ``changes``. Can also be set via ``option("end_offset", -60)``.
            end_timestamp: Timestamp string or datetime object that sets the end of the changes.
                Requires ``changes``. Can also be set via ``option("end_timestamp", "2023-01-01 12:00:00")``.
                A string or naive datetime is not interpreted in the session time zone, so pass
                ``end_timestamp_type`` or a tz-aware datetime to be explicit.
            end_timestamp_type: Type of ``end_timestamp`` interpretation ('NTZ', 'LTZ', or 'TZ').
                Can also be set via ``option("end_timestamp_type", "LTZ")``.

        Note:
            Time travel options can be set either as direct parameters or via the
            :meth:`option` method, but NOT both. If any direct time travel parameter
            is provided, all time travel options will be ignored to avoid conflicts.
            The exception is ``changes`` and the ``end_*`` parameters: if any of them is
            used, mixing direct parameters and options raises a ``ValueError``.

            PySpark Compatibility: The ``as-of-timestamp`` option automatically sets
            ``time_travel_mode="at"`` and cannot be used with ``time_travel_mode="before"``.

        Examples::

            # Using direct parameters
            >>> table = session.read.table("my_table", time_travel_mode="at", offset=-3600)  # doctest: +SKIP

            # Using options (recommended for chaining)
            >>> table = (session.read  # doctest: +SKIP
            ...     .option("time_travel_mode", "at")
            ...     .option("offset", -3600)
            ...     .table("my_table"))

            # PySpark-style as-of-timestamp (automatically sets mode to "at")
            >>> table = session.read.option("as-of-timestamp", "2023-01-01 12:00:00").table("my_table")  # doctest: +SKIP

            # Read the changes of a table with change tracking enabled
            >>> table = (session.read  # doctest: +SKIP
            ...     .option("changes", "default")
            ...     .option("time_travel_mode", "at")
            ...     .option("statement", "01234567-abcd-1234-5678-123456789012")
            ...     .option("end_offset", -60)
            ...     .table("my_table"))

            # timestamp_type automatically set to "TZ" due to timezone info
            >>> import datetime, pytz  # doctest: +SKIP
            >>> tz_aware = datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=pytz.UTC)  # doctest: +SKIP
            >>> table1 = session.read.table("my_table", time_travel_mode="at", timestamp=tz_aware)  # doctest: +SKIP

            # timestamp_type remains "NTZ" (user's explicit choice respected)
            >>> table2 = session.read.table("my_table", time_travel_mode="at", timestamp=tz_aware, timestamp_type="NTZ")  # doctest: +SKIP

            # Mixing options and parameters (direct parameters completely override options)
            >>> table = (session.read  # doctest: +SKIP
            ...     .option("time_travel_mode", "before")  # This will be IGNORED
            ...     .option("offset", -60)                 # This will be IGNORED
            ...     .table("my_table", time_travel_mode="at", offset=-3600))  # Only this is used
        """
        # ``version`` (Iceberg snapshot id) and ``version_tag`` (Iceberg tag
        # name) are intentionally not in the public signature — they are
        # consumed by Snowpark Connect and may be removed once a first-class
        # API lands. Accept them through **kwargs so direct callers can
        # still pass them without us advertising the surface.
        version = kwargs.pop("version", None)
        version_tag = kwargs.pop("version_tag", None)
        start_snapshot_id = kwargs.pop("start_snapshot_id", None)
        end_snapshot_id = kwargs.pop("end_snapshot_id", None)
        version_ref = kwargs.pop("version_ref", None)
        branch = kwargs.pop("branch", None)
        if kwargs:
            raise TypeError(
                f"table() got unexpected keyword arguments: {sorted(kwargs)}"
            )

        # Reader options set to None count as unset.
        options = {k: v for k, v in self._cur_options.items() if v is not None}
        direct_time_travel_params = {
            "time_travel_mode": time_travel_mode,
            "statement": statement,
            "offset": offset,
            "timestamp": timestamp,
            "timestamp_type": timestamp_type,
            "stream": stream,
            "version": version,
            "version_tag": version_tag,
            "version_ref": version_ref,
            "branch": branch,
            "changes": changes,
            "end_statement": end_statement,
            "end_offset": end_offset,
            "end_timestamp": end_timestamp,
            "end_timestamp_type": end_timestamp_type,
        }
        direct_time_travel_args = [
            param
            for param, value in direct_time_travel_params.items()
            if value is not None
        ]
        time_travel_option_keys = _time_travel_option_keys(options)

        incremental_read_options = _extract_iceberg_changes_from_options(options)
        if (
            start_snapshot_id is not None
            or end_snapshot_id is not None
            or incremental_read_options
        ):
            # The incremental read path ignores time travel settings. Plain time
            # travel has always been ignored there, but dropping `changes` or an
            # `end_*` setting would silently turn a CHANGES read into a different
            # read, so reject those.
            conflicting = [
                arg for arg in direct_time_travel_args if arg in _CHANGES_PARAMS
            ] + [
                key.lower()
                for key in time_travel_option_keys
                if key.upper() in _CHANGES_OPTION_KEYS
            ]
            if conflicting:
                raise ValueError(
                    "Cannot combine an Iceberg incremental read (start/end snapshot "
                    "id) with 'changes' or 'end_*' time travel settings on the same "
                    f"read; found {conflicting!r}."
                )
            if start_snapshot_id is None:
                start_snapshot_id = incremental_read_options.get("start_snapshot_id")
            if end_snapshot_id is None:
                end_snapshot_id = incremental_read_options.get("end_snapshot_id")

        # Direct time travel arguments override reader options. For a CHANGES
        # read that would silently drop part of the settings, so all of them
        # must come from one source.
        if (
            direct_time_travel_args
            and time_travel_option_keys
            and any(
                arg_name.upper() in _CHANGES_OPTION_KEYS
                for arg_name in direct_time_travel_args + time_travel_option_keys
            )
        ):
            raise ValueError(
                "Cannot mix time travel arguments of table() "
                f"({', '.join(direct_time_travel_args)}) with time travel reader "
                f"options ({', '.join(k.lower() for k in time_travel_option_keys)}) "
                "when 'changes' or an 'end_*' setting is used. Pass all time travel "
                "settings either as arguments to table() or as reader options."
            )

        # AST.
        stmt = None
        if _emit_ast and self._ast is not None:
            # The AST stores end_offset as an int, so normalize integer strings
            # first. Invalid values raise the usual ValueError.
            end_offset = TimeTravelConfig._normalize_end_offset(end_offset)
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.read_table, stmt)
            ast.reader.CopyFrom(self._ast)
            build_table_name(ast.name, name)
            if time_travel_mode is not None:
                ast.time_travel_mode.value = time_travel_mode
            if statement is not None:
                ast.statement.value = statement
            if offset is not None:
                ast.offset.value = offset
            if timestamp is not None:
                build_expr_from_python_val(ast.timestamp, timestamp)
            if timestamp_type is not None:
                ast.timestamp_type.value = str(timestamp_type)
            if stream is not None:
                ast.stream.value = stream
            if changes is not None:
                ast.changes.value = changes
            if end_statement is not None:
                ast.end_statement.value = end_statement
            if end_offset is not None:
                ast.end_offset.value = end_offset
            if end_timestamp is not None:
                build_expr_from_python_val(ast.end_timestamp, end_timestamp)
            if end_timestamp_type is not None:
                ast.end_timestamp_type.value = str(end_timestamp_type)

        if (
            start_snapshot_id is not None
            or end_snapshot_id is not None
            or incremental_read_options
        ):
            table = self._session.table(
                name,
                _emit_ast=False,
                start_snapshot_id=start_snapshot_id,
                end_snapshot_id=end_snapshot_id,
            )
        elif set(direct_time_travel_args) - _POINT_IN_TIME_PARAMS:
            # If version / named-ref params are provided without mode,
            # default to 'at'.
            effective_mode = (
                time_travel_mode
                if time_travel_mode
                else (
                    "at"
                    if _ICEBERG_TIME_TRAVEL_PARAMS.intersection(direct_time_travel_args)
                    else None
                )
            )
            time_travel_params = {
                **direct_time_travel_params,
                "time_travel_mode": effective_mode,
            }
            table = self._session.table(name, _emit_ast=False, **time_travel_params)
        else:
            # if time_travel_mode is not provided, extract time travel config from options
            time_travel_params = _extract_time_travel_from_options(options)
            table = self._session.table(name, _emit_ast=False, **time_travel_params)

        if _emit_ast and stmt is not None:
            table._ast_id = stmt.uid

        return table

    @publicapi
    def schema(self, schema: StructType, _emit_ast: bool = True) -> "DataFrameReader":
        """Define the schema for CSV, JSON, Parquet, or XML files that you want to read.

        Args:
            schema: Schema configuration for the file to be read.

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
        allowed_formats = [
            "csv",
            "json",
            "avro",
            "parquet",
            "orc",
            "xml",
        ]
        allowed_formats.extend(self._data_source_format)
        if canon_format not in allowed_formats:
            raise ValueError(
                f"Invalid format '{value}'. Supported formats are {allowed_formats}."
            )
        self.__format = canon_format

    @publicapi
    def format(
        self,
        format: Literal[
            "csv", "json", "avro", "parquet", "orc", "xml", "dbapi", "jdbc"
        ],
        _emit_ast: bool = True,
    ) -> "DataFrameReader":
        """Specify the format of the file(s) to load.

        Args:
            format: The format of the file(s) to load. Supported formats are csv, json, avro, parquet, orc, and xml.
                Snowpark data source type is also supported, currently support dbapi and jdbc.

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
        if format_str in self._data_source_format and path is not None:
            raise ValueError(
                f"The 'path' parameter is not supported for the {format_str} format. Please omit this parameter when calling."
            )
        if format_str not in self._data_source_format and path is None:
            raise TypeError(
                "DataFrameReader.load() missing 1 required positional argument: 'path'"
            )
        if format_str == "dbapi":
            return self.dbapi(**{k.lower(): v for k, v in self._cur_options.items()})
        if format_str == "jdbc":
            return self.jdbc(**{k.lower(): v for k, v in self._cur_options.items()})

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

        Configure the reader with :meth:`option`, :meth:`options`, and
        :meth:`schema` before calling ``csv``. Options are not keyword
        arguments of this method. Common CSV format options include
        ``FIELD_DELIMITER``, ``SKIP_HEADER``, and
        ``FIELD_OPTIONALLY_ENCLOSED_BY``. See the
        `CSV format options
        <https://docs.snowflake.com/en/sql-reference/sql/create-file-format#type-csv>`_
        for their accepted values.

        Provide a schema explicitly or enable the reader's ``INFER_SCHEMA``
        option. Schema inference can issue queries while constructing the
        DataFrame and is not supported for CSV in local testing mode.

        Example (requires an existing stage containing a header row and two
        comma-separated columns)::

            >>> from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType
            >>> schema = StructType([StructField("id", IntegerType()), StructField("name", StringType())])
            >>> df = session.read.schema(schema).option("SKIP_HEADER", 1).csv("@my_stage/people.csv")  # doctest: +SKIP

        Args:
            path: The stage location of a CSV file, or a stage location that has CSV files.

        Returns:
            a :class:`DataFrame` that is set up to load data from the specified CSV file(s) in a Snowflake stage.
        """
        path = _validate_stage_path(path)
        self._file_path = path
        self._file_type = "CSV"

        schema_to_cast, transformations = None, None
        # this parameter determine whether generate schema_to_cast and transformations when user schema exist
        # schema_to_cast and transformations is needed to apply try_cast to data
        user_schema_with_try_cast = False

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
            try_cast = self._cur_options.get("TRY_CAST", False)
            (
                schema,
                schema_to_cast,
                transformations,
            ) = self._get_schema_from_csv_user_input(self._user_schema, try_cast)
            user_schema_with_try_cast = try_cast

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
                            use_user_schema=user_schema_with_try_cast,
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
                    use_user_schema=user_schema_with_try_cast,
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

            - ``compression`` is not supported for XML files when ``rowTag`` is specified.

            - When ``rowTag`` is specified, the following options are supported for reading XML files
              via :meth:`option()` or :meth:`options()`:

              + ``mode``: Specifies the mode for dealing with corrupt XML records. The default value is ``PERMISSIVE``. The supported values are:

                  - ``PERMISSIVE``: When it encounters a corrupt record, it sets all fields to null and includes a `columnNameOfCorruptRecord` column.

                  - ``DROPMALFORMED``: Ignores the whole record that cannot be parsed correctly.

                  - ``FAILFAST``: When it encounters a corrupt record, it raises an exception immediately.

              + ``columnNameOfCorruptRecord``: Specifies the name of the column that contains the corrupt record.
                The default value is '_corrupt_record'.

              + ``ignoreNamespace``: remove namespace prefixes from XML element names when constructing result column names.
                The default value is ``True``. Parsing uses recovery mode to tolerate malformed records (e.g., undefined
                namespace prefixes in attributes such as ``diffgr:id`` or ``msdata:rowOrder``). When this option is enabled,
                element name prefixes are stripped where resolvable; if a prefix isn't declared on the row tag element,
                it cannot be resolved and will be left intact (i.e. this setting is ignored for that element).
                For example, for the following XML data with a row tag ``abc:def``:
                ```
                <abc:def><abc:xyz>0</abc:xyz></abc:def>
                ```
                the result column name is ``abc:xyz`` where ``abc`` is not stripped.

              + ``attributePrefix``: The prefix to add to the attribute names. The default value is ``_``.

              + ``excludeAttributes``: Whether to exclude attributes from the XML element. The default value is ``False``.

              + ``valueTag``: The column name used for the value when there are attributes in an element that has no child elements.
                The default value is ``_VALUE``.

              + ``nullValue``: The value to treat as a null value. The default value is ``""``.

              + ``charset``: The character encoding of the XML file. The default value is ``utf-8``.

              + ``ignoreSurroundingWhitespace``: Whether or not whitespaces surrounding values should be skipped.
                The default value is ``False``.

              + ``rowValidationXSDPath``: The Snowflake stage path to the XSD file used for row validation.
                Rows that do not match the XSD schema will be considered corrupt records and handled according to
                the specified ``mode`` option.

              + ``cacheResult``: Whether to cache the result DataFrame of the XML reader to a temporary table after calling :meth:`xml`.
                When set to ``True`` (default), the result is cached and all subsequent operations on the DataFrame are performed on the cached data.
                This means the actual computation occurs before :meth:`DataFrame.collect` is called.
                When set to ``False``, the DataFrame is computed lazily and the actual computation occurs when :meth:`DataFrame.collect` is called.
        """
        df = self._read_semi_structured_file(path, "XML")
        # AST.
        if _emit_ast and self._ast is not None:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.read_xml, stmt)
            ast.path = path
            ast.reader.CopyFrom(self._ast)
            df._ast_id = stmt.uid

        # xml_reader returns VARIANT DataFrame, cast to custom or inferred schema type
        effective_schema = self._user_schema or self._xml_inferred_schema
        if effective_schema is not None:
            return self._apply_xml_schema(df, effective_schema)
        else:
            return df

    def _apply_xml_schema(self, df: DataFrame, schema: StructType) -> DataFrame:
        """Apply an XML schema to a VARIANT DataFrame"""
        cols = []
        for field in schema.fields:
            # TODO: SNOW-2923003: remove single quote after server side BCR is done
            col = df[single_quote(field._name)]
            if isinstance(field.datatype, (StructType, ArrayType, MapType)):
                # Complex types: keep as VARIANT to prevent structured cast errors
                cols.append(col.alias(quote_name_without_upper_casing(field._name)))
            else:
                # Primitive types: cast to the inferred datatype
                cols.append(
                    col.cast(field.datatype).alias(
                        quote_name_without_upper_casing(field._name)
                    )
                )

        # In PERMISSIVE mode, append the StringType column for the corrupt record if it
        # exists in the DataFrame emitted by the UDTF on malformed or corrupted records.
        mode = self._cur_options.get("MODE", "PERMISSIVE").upper()
        corrupt_col_name = self._cur_options.get(
            "COLUMNNAMEOFCORRUPTRECORD", "_corrupt_record"
        )
        if mode == "PERMISSIVE":
            df_columns = {c.strip('"').strip("'") for c in df.columns}
            if corrupt_col_name in df_columns:
                corrupt_ref = df[single_quote(corrupt_col_name)]
                cols.append(
                    corrupt_ref.cast(StringType()).alias(
                        quote_name_without_upper_casing(corrupt_col_name)
                    )
                )
                if self._xml_inferred_schema is not None:
                    self._xml_inferred_schema.fields.append(
                        StructField(corrupt_col_name, StringType())
                    )

        result = df.select(cols)
        return result

    @private_preview(version="1.56.0")
    @publicapi
    def documents(self, path: str, _emit_ast: bool = True) -> DataFrame:
        """Specify the path of the document(s) to read. Each document is read as text,
        or, with a ``schema`` or a ``prompt``, structured values or an answer are
        extracted from it.

        Options set with :meth:`option` or :meth:`options` (names are case-insensitive,
        any other option is ignored):

            - ``schema``: the fields to extract. A JSON Schema dict (with a ``properties``
              dict), a dict of ``{field: question}``, or a list of field names or
              ``"field: question"`` strings. The result has one column per field,
              named in upper case.
            - ``prompt``: instructions for the extraction. With a ``schema`` it guides
              how the fields are extracted. Without one, the answer to the prompt is
              returned as text.

        The returned DataFrame has one row per document, with these columns:

            - no ``schema`` or ``prompt``: ``SOURCE_FILE``, ``TOTAL_PAGES`` and
              ``CONTENT`` (the text of the whole document).
            - ``schema``: ``SOURCE_FILE``, one column per schema field, and
              ``EXTRACTION_SCORES``. ``EXTRACTION_SCORES`` is an object that maps each
              top-level schema field to a confidence score between 0 and 1 for its
              extracted value (one score for a whole array or object field). The
              scores are not calibrated probabilities, and a field without a score is
              left out.
            - ``prompt`` only: ``SOURCE_FILE`` and ``RESPONSE``.

        Every read also has a case-sensitive ``"_document_error"`` column (quote the name
        when you select it). A document that fails does not fail the read: its row holds
        an array of ``{stage, message}`` entries describing what went wrong, and
        ``NULL`` when the document succeeded. A read stops early with an error when most
        of its documents fail, or when document extraction is not available to the
        account.

        Note:
            When ``documents()`` is called from inside a stored procedure, that
            procedure must run with caller's rights (``execute_as="caller"``).

        Note:
            The read is lazy: the documents are read (and billed) when an action such as
            ``collect()`` or ``show()`` runs, and every action runs the whole read again.
            To use the result more than once, call ``df.cache_result()`` or write the
            DataFrame to a table first.

        Note:
            With a ``schema``, the supported file types are ``.txt``, ``.md``, ``.pdf``,
            ``.doc``, ``.docx``, ``.xls``, ``.xlsx``, ``.csv`` and ``.xhtml``, and the
            images ``.jpg``, ``.jpeg``, ``.png``, ``.gif`` and ``.webp``. Other files
            in the path are not read; each gets an error in its ``_document_error``. A
            very large file of a type other than PDF can fail as a whole, and an image
            over the model's size limit gets an error. With a ``prompt`` only, or with
            neither, every file in the path is read, and a file that cannot be read gets
            an error in its ``_document_error``. A read with neither also parses
            ``.jpg``, ``.jpeg``, ``.png`` and ``.tiff`` images.

        Args:
            path: The stage location of a document, or a stage location that has documents.

        Example::

            >>> # Parse every document in a stage to text.
            >>> df = session.read.documents("@mystage/docs/")  # doctest: +SKIP
            >>> df.select("SOURCE_FILE", "TOTAL_PAGES", "CONTENT").show()  # doctest: +SKIP

            >>> # Extract fields from every document in a stage.
            >>> schema = {  # doctest: +SKIP
            ...     "type": "object",
            ...     "properties": {
            ...         "invoice_number": {"type": "string"},
            ...         "total": {"type": "number"},
            ...     },
            ... }
            >>> df = session.read.option("schema", schema).documents("@mystage/invoices/")  # doctest: +SKIP
            >>> df.select("SOURCE_FILE", "INVOICE_NUMBER", "TOTAL", '"_document_error"').show()  # doctest: +SKIP
        """
        if isinstance(self._session._conn, MockServerConnection):
            self._session._conn.log_not_supported_error(
                external_feature_name="DataFrameReader.documents",
                raise_error=NotImplementedError,
            )
        path = _validate_stage_path(path)
        options = document_reader.DocumentReaderOptions.from_reader_options(
            self._cur_options
        )
        df = document_reader.read_documents(self._session, path, options)

        set_api_call_source(
            df,
            "DataFrameReader.documents",
            {TelemetryField.KEY_READ_TYPE.value: options.read_type},
        )
        return df

    def _documents(self, path: str) -> DataFrame:
        """Kept so callers of the earlier private name keep working; use
        :meth:`documents`."""
        return self.documents(path)

    @publicapi
    def option(self, key: str, value: Any, _emit_ast: bool = True) -> "DataFrameReader":
        """Sets the specified option in the DataFrameReader.

        Use this method to configure any
        `format-specific options <https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html#format-type-options-formattypeoptions>`_,
        `copy options <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#copy-options-copyoptions>`_,
        and `time travel options <https://docs.snowflake.com/en/sql-reference/constructs/at-before>`_.
        (Note that although specifying copy options can make error handling more robust during the
        reading process, it may have an effect on performance.)

        Time travel options can be used with ``table()`` method:
            - ``time_travel_mode``: Either "at" or "before"
            - ``statement``: Query ID for statement-based time travel
            - ``offset``: Seconds to go back in time (negative integer)
            - ``timestamp``: Specific timestamp for time travel
            - ``timestamp_type``: Type of timestamp interpretation ('NTZ', 'LTZ', or 'TZ').
            - ``stream``: Stream name for time travel.
            - ``changes``: ``'default'`` or ``'append_only'`` to read the changes of a table with
              change tracking enabled.
            - ``end_statement``: Query ID that sets the end of the changes.
            - ``end_offset``: Seconds to go back in time that set the end of the changes (zero or a negative integer).
            - ``end_timestamp``: Timestamp that sets the end of the changes.
            - ``end_timestamp_type``: Type of ``end_timestamp`` interpretation ('NTZ', 'LTZ', or 'TZ').

        Special PySpark compatibility option:
            - ``as-of-timestamp``: Automatically sets ``time_travel_mode`` to "at" and uses the
              provided timestamp. Cannot be used with ``time_travel_mode="before"``.

        Args:
            key: Name of the option (e.g. ``compression``, ``skip_header``, ``time_travel_mode``, etc.).
            value: Value of the option.
        """

        # AST.
        if _emit_ast and self._ast is not None:
            t = self._ast.dataframe_reader.options.add()
            t._1 = key
            build_expr_from_python_val(
                t._2, str(value) if isinstance(value, TimestampTimeZone) else value
            )

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
        self,
        path: str,
        format: str,
    ) -> Tuple[List, List, List, Exception]:
        format_type_options, _ = get_copy_into_table_options(self._cur_options)

        temp_file_format_name = self._session.get_fully_qualified_name_if_possible(
            random_name_for_temp_object(TempObjectType.FILE_FORMAT)
        )
        drop_tmp_file_format_if_exists_query: Optional[str] = None
        use_temp_file_format = "FORMAT_NAME" not in self._cur_options
        file_format_name = self._cur_options.get("FORMAT_NAME", temp_file_format_name)
        infer_schema_options = self._cur_options.get("INFER_SCHEMA_OPTIONS", {})

        # When use_relaxed_types is true then ignore length or precision of infered types.
        # Eg StringType(5) -> StringType or ShortType -> DoubleType
        use_relaxed_types = infer_schema_options.pop("USE_RELAXED_TYPES", False)

        # When try_cast is true attempt add try_cast to the schema_to_cast string so
        # that files aren't dropped if individual rows fail to parse.
        try_cast = self._cur_options.get("TRY_CAST", False)

        # When pattern is set we should only consider files that match the pattern during schema inference
        # If no files match or the filepaths are not parseable fallback to trying to read all files.
        # snow:// paths are not yet supported
        infer_path = path
        fallback_path = infer_path
        should_fallback = False
        if (
            (pattern := self._cur_options.get("PATTERN", None))
            and "FILES" not in infer_schema_options
            and not path.startswith(SNOWURL_PREFIX)
        ):
            # matches has schema (name, size, md5, last_modified)
            # Name is fully qualified with stage path
            # Use proper SQL escaping to prevent SQL injection
            escaped_pattern = convert_value_to_sql_option(pattern)
            matches = self._session._conn.run_query(
                f"list {path} pattern = {escaped_pattern}"
            )["data"]

            if len(matches):
                try:
                    # Use fully qualified stage name to describe if available
                    stage, _ = get_stage_parts(path)
                    fully_qualified_stage, _ = get_stage_parts(
                        path, return_full_stage_name=True
                    )

                    stage_description = self._session._conn.run_query(
                        f"describe stage {fully_qualified_stage}"
                    )["data"]

                    stage_locations_json = [
                        x for x in stage_description if x[0] == "STAGE_LOCATION"
                    ][0][3]

                    # If stage locations is available then the stage is external
                    # and stage prefixes will include CSP specific prefix information.
                    stage_locations = (
                        json.loads(stage_locations_json)
                        if stage_locations_json
                        else [stage]
                    )

                    # Compile prefixes are case insensitive regexes because
                    # casing is not consistent in bucket describe query.
                    regexes = [
                        re.compile(f"^{re.escape(location)}", re.IGNORECASE)
                        for location in stage_locations
                    ]

                    files = []
                    # Construct a list of file prefixes not including the stage
                    for match in matches:
                        prefix = match[0]
                        for regex in regexes:
                            prefix = regex.sub("", prefix)
                        files.append(prefix.lstrip("/"))
                    infer_schema_options["FILES"] = files

                    # Reconstruct path using just stage and any qualifiers
                    infer_path = (
                        path.partition(fully_qualified_stage)[0] + fully_qualified_stage
                    )
                    should_fallback = True
                except Exception as e:
                    warning(
                        "infer_schema_pattern_matching",
                        f"Failed to infer schema for provided pattern {pattern} with error:\n{e}",
                    )

        infer_schema_query = infer_schema_statement(
            infer_path, file_format_name, infer_schema_options or None
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

            try:
                # SNOW-1628625: Schema inference should be done lazily
                results = self._session._conn.run_query(infer_schema_query)["data"]
            except Exception as e:
                if should_fallback:
                    infer_schema_query = infer_schema_statement(
                        fallback_path, file_format_name, None
                    )
                    results = self._session._conn.run_query(infer_schema_query)["data"]
                else:
                    raise e

            if len(results) == 0:
                # Zero rows can mean the path is empty/missing, or that file
                # format options (PARSE_HEADER, SKIP_HEADER, ON_ERROR=CONTINUE)
                # silently filtered everything out.
                hints = [
                    f"{k}={format_type_options.get(k, infer_schema_options.get(k))}"
                    for k in ("PARSE_HEADER", "SKIP_HEADER", "ON_ERROR")
                    if k in format_type_options or k in infer_schema_options
                ]
                suffix = f" Applied options: {', '.join(hints)}." if hints else ""
                raise FileNotFoundError(
                    f"Given path: '{path}' returned no results from INFER_SCHEMA. "
                    "The path may be empty/missing, or file format options may "
                    "have filtered every row/header. Check the file contents and "
                    "file format options." + suffix
                )
            new_schema = []
            schema_to_cast = []
            transformations: List["snowflake.snowpark.column.Column"] = []
            read_file_transformations = None
            use_structured_type_infer_schema = (
                self._session._use_structured_type_infer_schema
            )
            for r in results:
                # Columns for r [column_name, type, nullable, expression, filenames, order_id]
                column_name, type, nullable, expression = r[0], r[1], r[2], r[3]
                name = quote_name_without_upper_casing(column_name)
                # Parse the type returned by infer_schema command.
                if use_structured_type_infer_schema:
                    # handles both simple types and structured
                    # types (OBJECT, MAP, ARRAY with inner details).
                    datatype = _parse_structured_type_str(
                        type, self._session._conn.max_string_size
                    )
                else:
                    data_type_parts = type.split("(")
                    parts_length = len(data_type_parts)
                    if parts_length == 1:
                        data_type = type
                        precision = 0
                        scale = 0
                    else:
                        data_type = data_type_parts[0]
                        precision = int(data_type_parts[1].split(",")[0])
                        scale = int(data_type_parts[1].split(",")[1][:-1])
                    datatype = convert_sf_to_sp_type(
                        data_type,
                        precision,
                        scale,
                        0,
                        self._session._conn.max_string_size,
                    )

                if use_relaxed_types:
                    datatype = most_permissive_type(datatype)
                new_schema.append(
                    Attribute(
                        name,
                        datatype,
                        nullable=nullable,
                    )
                )

                if try_cast:
                    id = expression.split(":")[0]
                    identifier = f"TRY_CAST({id} AS {convert_sp_to_sf_type(datatype)})"
                elif format == "CSV":
                    identifier = expression
                elif format == "PARQUET" and expression.upper().startswith(
                    "GET_IGNORE_CASE"
                ):
                    identifier = expression
                elif (
                    format.lower() in ("parquet", "json")
                    and use_structured_type_infer_schema
                ):
                    if isinstance(datatype, VariantType):
                        # Bare structured keyword was returned by the backend
                        # (no inner details).  Skip the cast — $1:{name}
                        # extracts as variant and lets callers handle
                        # structured-type discovery.
                        identifier = f"$1:{name}"
                    elif use_relaxed_types:
                        identifier = f"$1:{name}::{convert_sp_to_sf_type(datatype)}"
                    else:
                        # INFER_SCHEMA may return NOT NULL annotations in
                        # structured type strings (e.g.,
                        # "ARRAY(NUMBER(10,0) NOT NULL)"). Strip them before
                        # embedding in SQL — nullable info is already captured
                        # in the parsed DataType objects.
                        cast_type = _NOT_NULL_RE.sub("", type)
                        identifier = f"$1:{name}::{cast_type}"
                else:
                    identifier = f"$1:{name}::{convert_sp_to_sf_type(datatype) if use_relaxed_types else type}"

                schema_to_cast.append((identifier, column_name))
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

    def _get_schema_from_csv_user_input(
        self, user_schema: StructType, try_cast: bool
    ) -> Tuple[List, Optional[List], Optional[List]]:
        """
        This function accept a user input structtype and return schemas needed for reading CSV file.
        CSV files are processed differently than semi-structured file so need a different helper function.
        """
        schema_to_cast = []
        transformations = []
        new_schema = []
        for index, field in enumerate(user_schema.fields, start=1):
            new_schema.append(
                Attribute(
                    field.column_identifier.quoted_name,
                    field.datatype,
                    field.nullable,
                )
            )
            sf_type = convert_sp_to_sf_type(field.datatype)
            # TODO: SNOW-3324409 Support relaxed schema when read csv in copy mode
            if try_cast:
                identifier = f"TRY_CAST(${index} AS {sf_type})"
                schema_to_cast.append((identifier, unquote_if_quoted(field.name)))
                transformations.append(sql_expr(identifier))

        read_file_transformations = [t._expression.sql for t in transformations]
        # schema_to_cast and read_file_transformations should only exist when try_cast is True
        # this is meant to not break the current behavior
        if not try_cast:
            return new_schema, None, None
        return new_schema, schema_to_cast, read_file_transformations

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
        self._infer_schema_transformations = transformations
        self._infer_schema_target_columns = StructType._from_attributes(
            new_schema
        ).names
        read_file_transformations = [t._expression.sql for t in transformations]
        return new_schema, schema_to_cast, read_file_transformations

    def _register_xml_udtf(
        self,
        local_file_path: str,
        handler_name: str,
        output_schema: StructType,
    ):
        """Register a session-scoped XML UDTF with a deterministic fully-qualified
        name, cached per session and schema"""
        if is_in_stored_procedure():  # pragma: no cover
            # Use TEMPORARY instead of SCOPED TEMPORARY session stage
            import_stage_name = self._session.get_fully_qualified_name_if_possible(
                "SNOWPARK_TEMP_STAGE_XMLIMPORTS"
            )
            self._session._run_query(
                f"CREATE TEMPORARY STAGE IF NOT EXISTS {import_stage_name}",
                is_ddl_on_temp_object=True,
            )
            import_stage = f"{STAGE_PREFIX}{import_stage_name}"
            self._session._conn.upload_file(
                local_file_path,
                import_stage,
                compress_data=False,
                overwrite=True,
                skip_upload_on_content_match=True,
            )
            python_file_path = f"{import_stage}/{os.path.basename(local_file_path)}"
        else:
            python_file_path = local_file_path

        _, input_types = get_types_from_type_hints(
            (local_file_path, handler_name), TempObjectType.TABLE_FUNCTION
        )

        udtf_name = self._session.get_fully_qualified_name_if_possible(
            f"SNOWPARK_TEMP_TABLE_FUNCTION_{handler_name.upper()}"
        )

        return self._session.udtf.register_from_file(
            python_file_path,
            handler_name,
            name=udtf_name,
            output_schema=output_schema,
            input_types=input_types,
            packages=["snowflake-snowpark-python", "lxml<7"],
            if_not_exists=True,
            skip_upload_on_content_match=True,
            _suppress_local_package_warnings=True,
        )

    def _infer_schema_for_xml(
        self, path: str, string_types_only: bool = False
    ) -> Optional[StructType]:
        schema_udtf = self._register_xml_udtf(
            XML_SCHEMA_INFERENCE_FILE_PATH,
            "XMLSchemaInference",
            StructType([StructField("SCHEMA_VALUE", StringType(), True)]),
        )

        # Determine number of workers
        try:
            file_size = int(
                self._session.sql(f"ls {path}", _emit_ast=False).collect(
                    _emit_ast=False
                )[0]["size"]
            )
        except IndexError:
            raise ValueError(f"{path} does not exist")

        num_workers = min(16, file_size // DEFAULT_CHUNK_SIZE + 1)

        row_tag = self._cur_options[XML_ROW_TAG_STRING]
        sampling_ratio = float(self._cur_options.get("SAMPLING_RATIO", 1.0))
        if sampling_ratio <= 0:
            raise ValueError(
                f"samplingRatio ({sampling_ratio}) should be greater than 0"
            )
        ignore_namespace = self._cur_options.get("IGNORENAMESPACE", True)
        attribute_prefix = self._cur_options.get("ATTRIBUTEPREFIX", "_")
        exclude_attributes = self._cur_options.get("EXCLUDEATTRIBUTES", False)
        value_tag = self._cur_options.get("VALUETAG", "_VALUE")
        charset = self._cur_options.get("CHARSET", "utf-8")
        ignore_surrounding_whitespace = self._cur_options.get(
            "IGNORESURROUNDINGWHITESPACE", False
        )

        # Create range DataFrame and apply UDTF
        worker_col = "WORKER"
        df = self._session.range(num_workers).to_df(worker_col)
        df = df.select(
            schema_udtf(
                lit(path),
                lit(num_workers),
                lit(row_tag),
                col(worker_col),
                lit(sampling_ratio),
                lit(ignore_namespace),
                lit(attribute_prefix),
                lit(exclude_attributes),
                lit(value_tag),
                lit(charset),
                lit(ignore_surrounding_whitespace),
                lit(file_size),
                lit(string_types_only),
            )
        )

        # Collect and merge schema results from all workers
        results = df.collect(_emit_ast=False)
        merged_schema: Optional[StructType] = None

        for row in results:
            schema_str = row[0]
            if schema_str is None or schema_str == "":
                continue
            try:
                partial_schema = type_string_to_type_object(schema_str)
            except Exception:
                continue
            if not isinstance(partial_schema, StructType):
                partial_schema = StructType(
                    [StructField(value_tag, partial_schema, nullable=True)]
                )
            if merged_schema is None:
                merged_schema = partial_schema
            else:
                merged_schema = merge_struct_types(
                    merged_schema, partial_schema, value_tag
                )

        if merged_schema is None:
            return None

        # Canonicalize: NullType -> StringType, remove empty structs
        canonical = canonicalize_type(merged_schema)
        if canonical is None or not isinstance(canonical, StructType):
            return None

        def _strip_outer_quotes(name: str) -> str:
            """Strip one layer of double quotes added by _case_preserving_simple_string()"""
            if len(name) >= 2 and name[0] == '"' and name[-1] == '"':
                return name[1:-1]
            return name

        def _clean_schema_field_names(dt):
            """Strip outer double quotes from field names.

            _case_preserving_simple_string wraps colon-containing names in
            double quotes so the parser can split name:type correctly.
            type_string_to_type_object keeps those quotes as part of the name.
            Strip them here so downstream quote_name_without_upper_casing
            doesn't produce triple-quoted identifiers.
            """
            if isinstance(dt, StructType):
                return StructType(
                    [
                        StructField(
                            _strip_outer_quotes(f._name),
                            _clean_schema_field_names(f.datatype),
                            f.nullable,
                        )
                        for f in dt.fields
                    ]
                )
            if isinstance(dt, ArrayType):
                return ArrayType(
                    _clean_schema_field_names(dt.element_type), dt.contains_null
                )
            if isinstance(dt, MapType):
                return MapType(
                    _clean_schema_field_names(dt.key_type),
                    _clean_schema_field_names(dt.value_type),
                    dt.value_contains_null,
                )
            return dt

        inferred_schema = _clean_schema_field_names(canonical)
        return inferred_schema

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

        if self._user_schema and format.lower() not in ["json", "xml", "parquet"]:
            raise ValueError(f"Read {format} does not support user schema")
        if (
            self._user_schema
            and format.lower() == "xml"
            and XML_ROW_TAG_STRING not in self._cur_options
        ):
            raise ValueError("When reading XML with user schema, rowtag must be set.")
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
            if not isinstance(self._session._conn, MockServerConnection):
                (
                    new_schema,
                    schema_to_cast,
                    read_file_transformations,
                    infer_schema_exception,
                ) = self._infer_schema_for_file_format(path, format)
                if new_schema:
                    schema = new_schema
                elif infer_schema_exception is not None:
                    if isinstance(infer_schema_exception, FileNotFoundError):
                        raise infer_schema_exception
                    logger.warning(
                        f"Could not infer schema for {format} file due to exception: "
                        f"{infer_schema_exception}. "
                        "\nFalling back to $1 VARIANT schema. "
                        "Please use DataFrameReader.schema() to specify a user schema for the file."
                    )
                    self._cur_options["INFER_SCHEMA"] = False

        metadata_project, metadata_schema = self._get_metadata_project_and_schema()

        xml_inferred_schema = None
        if format == "XML" and XML_ROW_TAG_STRING in self._cur_options:
            # Internal flag set by SCOS to skip the inference pass when a user schema
            # is already present, maintaining 1-pass reading when user schema is provided.
            skip_inference = self._cur_options.get("_XML_SKIP_INFERENCE", False)
            if (
                context._is_snowpark_connect_compatible_mode
                and not self._user_schema
                and not skip_inference
            ):
                string_types_only = not self._cur_options.get("INFER_SCHEMA", True)
                xml_inferred_schema = self._infer_schema_for_xml(
                    path, string_types_only
                )
                if xml_inferred_schema is not None:
                    self._xml_inferred_schema = xml_inferred_schema
                    schema = [
                        Attribute(
                            quote_name_without_upper_casing(f._name),
                            f.datatype,
                            f.nullable,
                        )
                        for f in xml_inferred_schema.fields
                    ]
                    use_user_schema = True

            xml_reader_udtf = self._register_xml_udtf(
                XML_READER_FILE_PATH,
                "XMLReader",
                StructType(
                    [StructField(XML_ROW_DATA_COLUMN_NAME, VariantType(), True)]
                ),
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
            if self._cur_options.get("CACHERESULT", True):
                df = df.cache_result()
                # When schema is inferred or user-provided, columns are typed
                # (not all VARIANT), so don't enable the all-variant dot-notation mode.
                if xml_inferred_schema is None and not self._user_schema:
                    df._all_variant_cols = True
        else:
            set_api_call_source(df, f"DataFrameReader.{format.lower()}")
        return df

    @private_preview(version="1.38.0")
    @publicapi
    def jdbc(
        self,
        url: str,
        udtf_configs: dict,
        *,
        properties: Optional[dict] = None,
        table: Optional[str] = None,
        query: Optional[str] = None,
        column: Optional[str] = None,
        lower_bound: Optional[Union[str, int]] = None,
        upper_bound: Optional[Union[str, int]] = None,
        num_partitions: Optional[int] = None,
        query_timeout: Optional[int] = 0,
        fetch_size: Optional[int] = 0,
        custom_schema: Optional[Union[str, StructType]] = None,
        predicates: Optional[List[str]] = None,
        session_init_statement: Optional[Union[str, List[str]]] = None,
        _emit_ast: bool = True,
    ) -> DataFrame:
        """
        Reads an external database table or query into a DataFrame using JDBC in
        Snowflake-hosted Java user-defined table functions (UDTFs).

        A live Snowflake session, warehouse, current database and schema, staged JDBC
        driver, external access integration, and PASSWORD secret are required. The
        JDBC driver runs in Snowflake, not on the Python client; no local JVM is
        required. Local testing (mock sessions) is not supported. Java 17 is the
        default UDTF runtime. Snowflake supports Java 11, 17, and 21; select a
        runtime compatible with the driver JAR (see `Java UDF runtimes
        <https://docs.snowflake.com/en/developer-guide/udf/java/udf-java-introduction>`_).
        For example, ``ojdbc17`` requires Java 17 or later, whereas the SQL Server
        ``jre11`` JAR below can use Java 11.

        Calling this method immediately connects to the source to infer its schema
        and creates temporary Snowflake objects. Source rows are read when an action
        such as :meth:`DataFrame.collect` or :meth:`DataFrameWriter.save_as_table`
        executes. Repeated actions can read the source again and see changed data.
        Persist once when a reusable Snowflake copy is needed. Errors can occur
        during either schema inference or an action.

        Args:
            url: JDBC connection URL in the source driver's syntax. Never include
                credentials in this URL, even though common credential forms are
                stripped. URLs and properties can appear in generated SQL and logs.
            udtf_configs: Required dictionary with these keys:

                - ``external_access_integration`` (str, required): Snowflake
                  integration allowing the source endpoint and secret.
                - ``secret`` (str, required): Name of a Snowflake ``TYPE = PASSWORD``
                  secret containing the source username and password. Prefer a fully
                  qualified name, such as ``JDBC_DEMO.PUBLIC.ORACLE_CRED``.
                - ``imports`` (List[str], required): Staged driver JAR paths, such as
                  ``@JDBC_DEMO.PUBLIC.JDBC_DRIVERS/ojdbc17-23.9.0.25.07.jar``.
                - ``java_version`` (int, optional): Java UDTF runtime version;
                  defaults to ``17``.
                - ``packages`` (List[str], optional): Snowflake-supported Java
                  packages. Defaults to ``["com.snowflake:snowpark:latest"]``;
                  Snowpark is added if not supplied. This does not download the JDBC
                  driver: stage its JAR and include it in ``imports``.

            properties: Non-secret driver connection properties, passed as strings
                to ``DriverManager.getConnection``. Use the PASSWORD secret for
                authentication, never ``user``, ``password``, tokens, or trust-store
                passwords in this dictionary. Driver-specific options are not
                interchangeable between databases.
            table: Source table name, optionally qualified by the source schema or
                database. Specify exactly one of ``table`` and ``query``.
            query: Source-dialect SELECT statement that is valid inside a subquery
                in a FROM clause. String literals are supported; quote them using
                the source SQL dialect, for example ``'O''Brien'``. Trailing
                semicolons are stripped. This is SQL text, not a parameter-binding
                API: do not interpolate untrusted input. On SQL Server, a bare
                ``ORDER BY`` is invalid in this subquery; omit it and use
                :meth:`DataFrame.order_by` on the result, or use a source-supported
                ``TOP``/``OFFSET`` form when required by the query's semantics.
            column: Numeric, date, or timestamp result column used for range
                partitioning. Supply ``lower_bound``, ``upper_bound``, and
                ``num_partitions`` together with it. The column must be present in
                the source result. Range partitioning takes precedence over
                ``predicates``.
            lower_bound: Lower bound used to calculate partition stride, not a row
                filter. Use integers for numeric bounds and ISO date/timestamp
                strings for temporal bounds, such as ``"2026-01-01 00:00:00"``.
            upper_bound: Upper bound used to calculate stride, not a row filter.
                Must be greater than or equal to ``lower_bound``. The first and last
                partitions include values outside the bounds; NULLs go to the first
                partition. Put a WHERE clause in ``query`` to restrict rows.
            num_partitions: Positive integer specifying the requested number of
                source range queries. Small ranges can produce fewer partitions;
                equal bounds or ``1`` produce one unfiltered source query. Snowflake
                schedules the UDTF work; this is not a guaranteed connection or
                concurrency count. This method has no ``max_workers`` parameter.
            query_timeout: JDBC statement timeout in seconds, applied to schema
                inference, initialization statements, and source reads. ``0``
                (default) means no JDBC statement timeout, not unlimited execution:
                driver/network, Snowflake statement, and UDTF timeouts still apply.
                Enforcement depends on the driver. It is not a connection timeout.
            fetch_size: Hint passed to JDBC ``Statement.setFetchSize`` for source
                reads. ``0`` (default) leaves behavior to the driver. It is not a
                row limit or a guarantee of streaming. For Oracle, try ``10000`` as
                a starting point to reduce round trips, then tune for row width,
                memory, source load, and latency. See the streaming caveats below.
            custom_schema: Partial or complete overrides of the inferred Snowflake
                schema, as a schema string or :class:`~snowflake.snowpark.types.StructType`.
                Inference still runs and must succeed. Overrides match inferred
                names case-insensitively, replace matching fields, and keep source
                column order; unspecified fields retain their inferred types.
                Duplicate override names and names absent from the source result
                raise ``ValueError``. Overrides do not select/reorder source columns
                or change the JDBC getter; they control subsequent Snowflake casts.
                A cast can fail or lose precision if the value is incompatible.
            predicates: Source SQL WHERE expressions, one per partition query.
                Used only when ``column`` is absent. Predicates should be disjoint
                to avoid duplicates and collectively cover the desired rows; include
                ``IS NULL`` explicitly if needed. They can filter rows, unlike range
                bounds. Do not interpolate untrusted input.
            session_init_statement: Source SQL string or list of strings executed
                before schema inference and before each partition query. Connections
                can be reused across partition queries, so statements should be safe
                to repeat. Use session settings, not non-idempotent data changes.
                These statements configure the source session, not Snowflake.

        Example:
            **1. Provision access and upload drivers.** Replace all example hosts,
            source credentials, database/schema, role, warehouse, and local paths.
            The examples use reserved ``example.com`` hosts, not working endpoints.
            Provision only the sources you need. Have an administrator execute the
            following SQL using a role allowed to create the objects and grant
            privileges. The provisioning role needs ``CREATE NETWORK RULE``,
            ``CREATE SECRET``, and ``CREATE STAGE`` on the schema, plus ``CREATE
            INTEGRATION`` on the account and ``USAGE`` on referenced secrets and
            their schema. Creating the demonstration database also requires
            ``CREATE DATABASE``. ``JDBC_READER`` and ``JDBC_WH`` below are existing
            role and warehouse placeholders.

            .. code-block:: sql

                CREATE DATABASE IF NOT EXISTS JDBC_DEMO;
                USE DATABASE JDBC_DEMO;
                USE SCHEMA PUBLIC;
                CREATE STAGE IF NOT EXISTS JDBC_DRIVERS;

                CREATE NETWORK RULE ORACLE_NETWORK MODE = EGRESS TYPE = HOST_PORT
                  VALUE_LIST = ('oracle.example.com:1521');
                CREATE SECRET ORACLE_CRED TYPE = PASSWORD
                  USERNAME = '<oracle-read-user>' PASSWORD = '<oracle-password>';
                CREATE EXTERNAL ACCESS INTEGRATION ORACLE_ACCESS
                  ALLOWED_NETWORK_RULES = (JDBC_DEMO.PUBLIC.ORACLE_NETWORK)
                  ALLOWED_AUTHENTICATION_SECRETS = (JDBC_DEMO.PUBLIC.ORACLE_CRED)
                  ENABLED = TRUE;

                CREATE NETWORK RULE POSTGRES_NETWORK MODE = EGRESS TYPE = HOST_PORT
                  VALUE_LIST = ('postgres.example.com:5432');
                CREATE SECRET POSTGRES_CRED TYPE = PASSWORD
                  USERNAME = '<postgres-read-user>' PASSWORD = '<postgres-password>';
                CREATE EXTERNAL ACCESS INTEGRATION POSTGRES_ACCESS
                  ALLOWED_NETWORK_RULES = (JDBC_DEMO.PUBLIC.POSTGRES_NETWORK)
                  ALLOWED_AUTHENTICATION_SECRETS = (JDBC_DEMO.PUBLIC.POSTGRES_CRED)
                  ENABLED = TRUE;

                CREATE NETWORK RULE MYSQL_NETWORK MODE = EGRESS TYPE = HOST_PORT
                  VALUE_LIST = ('mysql.example.com:3306');
                CREATE SECRET MYSQL_CRED TYPE = PASSWORD
                  USERNAME = '<mysql-read-user>' PASSWORD = '<mysql-password>';
                CREATE EXTERNAL ACCESS INTEGRATION MYSQL_ACCESS
                  ALLOWED_NETWORK_RULES = (JDBC_DEMO.PUBLIC.MYSQL_NETWORK)
                  ALLOWED_AUTHENTICATION_SECRETS = (JDBC_DEMO.PUBLIC.MYSQL_CRED)
                  ENABLED = TRUE;

                CREATE NETWORK RULE SQLSERVER_NETWORK MODE = EGRESS TYPE = HOST_PORT
                  VALUE_LIST = ('sqlserver.example.com:1433');
                CREATE SECRET SQLSERVER_CRED TYPE = PASSWORD
                  USERNAME = '<sqlserver-read-user>' PASSWORD = '<sqlserver-password>';
                CREATE EXTERNAL ACCESS INTEGRATION SQLSERVER_ACCESS
                  ALLOWED_NETWORK_RULES = (JDBC_DEMO.PUBLIC.SQLSERVER_NETWORK)
                  ALLOWED_AUTHENTICATION_SECRETS = (JDBC_DEMO.PUBLIC.SQLSERVER_CRED)
                  ENABLED = TRUE;

                GRANT USAGE ON DATABASE JDBC_DEMO TO ROLE JDBC_READER;
                GRANT USAGE ON SCHEMA JDBC_DEMO.PUBLIC TO ROLE JDBC_READER;
                GRANT USAGE ON WAREHOUSE JDBC_WH TO ROLE JDBC_READER;
                GRANT CREATE FUNCTION ON SCHEMA JDBC_DEMO.PUBLIC TO ROLE JDBC_READER;
                GRANT CREATE TABLE ON SCHEMA JDBC_DEMO.PUBLIC TO ROLE JDBC_READER;
                GRANT READ ON STAGE JDBC_DEMO.PUBLIC.JDBC_DRIVERS TO ROLE JDBC_READER;
                GRANT READ ON SECRET JDBC_DEMO.PUBLIC.ORACLE_CRED TO ROLE JDBC_READER;
                GRANT READ ON SECRET JDBC_DEMO.PUBLIC.POSTGRES_CRED TO ROLE JDBC_READER;
                GRANT READ ON SECRET JDBC_DEMO.PUBLIC.MYSQL_CRED TO ROLE JDBC_READER;
                GRANT READ ON SECRET JDBC_DEMO.PUBLIC.SQLSERVER_CRED TO ROLE JDBC_READER;
                GRANT USAGE ON INTEGRATION ORACLE_ACCESS TO ROLE JDBC_READER;
                GRANT USAGE ON INTEGRATION POSTGRES_ACCESS TO ROLE JDBC_READER;
                GRANT USAGE ON INTEGRATION MYSQL_ACCESS TO ROLE JDBC_READER;
                GRANT USAGE ON INTEGRATION SQLSERVER_ACCESS TO ROLE JDBC_READER;

            The reader needs access to the stage, secrets, integration, and their
            database/schema. The grants above also allow saving a new permanent
            destination table. Appending to an existing table requires ``INSERT``
            on that table. The source account needs SELECT access to source tables.
            Network rules authorize egress but do not establish private routing or
            configure the source firewall. See `external access setup and privileges
            <https://docs.snowflake.com/en/developer-guide/external-network-access/creating-using-external-network-access>`_.

            Download compatible drivers from the database vendors. These filenames
            illustrate driver versions; use the exact names you upload in
            ``imports``. Run PUT from a client that supports local file uploads
            (for example, SnowSQL), as the stage owner or a role with stage
            ``READ`` and ``WRITE``. PUT paths are on that client, not the database
            server. In a Notebook, upload the JARs to this stage beforehand.

            .. code-block:: sql

                PUT file:///path/to/ojdbc17-23.9.0.25.07.jar
                  @JDBC_DEMO.PUBLIC.JDBC_DRIVERS AUTO_COMPRESS = FALSE;
                PUT file:///path/to/postgresql-42.7.7.jar
                  @JDBC_DEMO.PUBLIC.JDBC_DRIVERS AUTO_COMPRESS = FALSE;
                PUT file:///path/to/mysql-connector-j-9.5.0.jar
                  @JDBC_DEMO.PUBLIC.JDBC_DRIVERS AUTO_COMPRESS = FALSE;
                PUT file:///path/to/mssql-jdbc-13.2.1.jre11.jar
                  @JDBC_DEMO.PUBLIC.JDBC_DRIVERS AUTO_COMPRESS = FALSE;
                LIST @JDBC_DEMO.PUBLIC.JDBC_DRIVERS;

            **2. Select a Snowflake session.** In a Snowflake Notebook, use its
            active session. Ensure the Notebook's role has the grants above.
            Outside Snowflake, replace ``get_active_session()`` with
            ``Session.builder.config("connection_name", "my_connection").create()``
            after ``from snowflake.snowpark import Session``, using a configured
            Snowflake connection. Headless sessions also need an explicit current
            database/schema and warehouse.

            .. code-block:: python

                from snowflake.snowpark.context import get_active_session

                session = get_active_session()
                session.use_database("JDBC_DEMO")
                session.use_schema("PUBLIC")
                session.use_warehouse("JDBC_WH")

            The following Python blocks require the setup above and external
            resources; they are examples, not automatically executed doctests.

            **3. Oracle: bounded read with Native Network Encryption (NNE).**
            The service name ``FREEPDB1`` is a placeholder, not a SID. NNE options
            must agree with the database server's encryption and checksum policy.
            This example requires AES256 and SHA256; it is not TLS configuration.
            See `Oracle JDBC client-side security
            <https://docs.oracle.com/en/database/oracle/oracle-database/19/jjdbc/client-side-security.html>`_.

            .. code-block:: python

                oracle_url = "jdbc:oracle:thin:@//oracle.example.com:1521/FREEPDB1"
                oracle_config = {
                    "external_access_integration": "ORACLE_ACCESS",
                    "secret": "JDBC_DEMO.PUBLIC.ORACLE_CRED",
                    "imports": ["@JDBC_DEMO.PUBLIC.JDBC_DRIVERS/ojdbc17-23.9.0.25.07.jar"],
                    "java_version": 17,
                }
                oracle_properties = {
                    "oracle.net.encryption_client": "REQUIRED",
                    "oracle.net.encryption_types_client": "(AES256)",
                    "oracle.net.crypto_checksum_client": "REQUIRED",
                    "oracle.net.crypto_checksum_types_client": "(SHA256)",
                }
                df = session.read.jdbc(
                    url=oracle_url,
                    udtf_configs=oracle_config,
                    properties=oracle_properties,
                    query="SELECT 1 AS ID, 'O''Brien' AS NAME FROM DUAL",
                    fetch_size=10000,
                    query_timeout=60,
                    session_init_statement="ALTER SESSION SET TIME_ZONE = '+00:00'",
                )
                print(df.schema)
                df.show()

            **4. PostgreSQL, MySQL, and SQL Server: one-row connectivity checks.**
            These examples require TLS with server certificates matching the
            hostnames and chains trusted by the Java runtime's default trust store.
            The PostgreSQL example selects the Java SSL factory explicitly rather
            than relying on a client-local ``~/.postgresql/root.crt``. Configure
            trust for your deployment; do not put trust-store passwords in
            ``properties``. For PostgreSQL, see
            `SSL configuration <https://jdbc.postgresql.org/documentation/ssl/>`_.
            For MySQL, see `Connector/J properties
            <https://dev.mysql.com/doc/connector-j/en/connector-j-reference-configuration-properties.html>`_.

            .. code-block:: python

                postgres_df = session.read.jdbc(
                    url="jdbc:postgresql://postgres.example.com:5432/appdb",
                    udtf_configs={
                        "external_access_integration": "POSTGRES_ACCESS",
                        "secret": "JDBC_DEMO.PUBLIC.POSTGRES_CRED",
                        "imports": ["@JDBC_DEMO.PUBLIC.JDBC_DRIVERS/postgresql-42.7.7.jar"],
                        "java_version": 17,
                    },
                    properties={
                        "sslmode": "verify-full",
                        "sslfactory": "org.postgresql.ssl.DefaultJavaSSLFactory",
                        "ApplicationName": "snowpark-jdbc",
                    },
                    query="SELECT 1 AS id, 'hello' AS greeting",
                    query_timeout=60,
                )
                postgres_df.show()

                mysql_df = session.read.jdbc(
                    url="jdbc:mysql://mysql.example.com:3306/appdb",
                    udtf_configs={
                        "external_access_integration": "MYSQL_ACCESS",
                        "secret": "JDBC_DEMO.PUBLIC.MYSQL_CRED",
                        "imports": ["@JDBC_DEMO.PUBLIC.JDBC_DRIVERS/mysql-connector-j-9.5.0.jar"],
                        "java_version": 17,
                    },
                    properties={"sslMode": "VERIFY_IDENTITY", "useCursorFetch": "true"},
                    query="SELECT 1 AS id, 'hello' AS greeting",
                    fetch_size=1000,
                    query_timeout=60,
                )
                mysql_df.show()

                sqlserver_df = session.read.jdbc(
                    url=("jdbc:sqlserver://sqlserver.example.com:1433;databaseName=appdb;"
                         "encrypt=true;trustServerCertificate=false;"),
                    udtf_configs={
                        "external_access_integration": "SQLSERVER_ACCESS",
                        "secret": "JDBC_DEMO.PUBLIC.SQLSERVER_CRED",
                        "imports": ["@JDBC_DEMO.PUBLIC.JDBC_DRIVERS/mssql-jdbc-13.2.1.jre11.jar"],
                        "java_version": 11,
                    },
                    properties={"responseBuffering": "adaptive"},
                    query="SELECT 1 AS id, 'hello' AS greeting",
                    query_timeout=60,
                )
                sqlserver_df.show()

            SQL Server ``trustServerCertificate=true`` bypasses certificate
            validation and is only appropriate for explicitly trusted test
            environments; keep it ``false`` for authenticated TLS. See `Microsoft
            JDBC encryption
            <https://learn.microsoft.com/en-us/sql/connect/jdbc/connecting-with-ssl-encryption>`_.

            **5. Partition, override types, and persist.** Reuse the Oracle session
            and configuration above. This five-row source query needs no user table.
            Bounds of 2 and 4 still include IDs 1 and 5: bounds determine stride,
            not filtering. Use a WHERE clause in the source query to restrict rows.

            .. code-block:: python

                source_query = (
                    "SELECT LEVEL AS ID, 'demo' AS NAME "
                    "FROM DUAL CONNECT BY LEVEL <= 5"
                )
                partitioned_df = session.read.jdbc(
                    url=oracle_url,
                    udtf_configs=oracle_config,
                    properties=oracle_properties,
                    query=source_query,
                    column="ID",
                    lower_bound=2,
                    upper_bound=4,
                    num_partitions=2,
                    custom_schema="ID NUMBER(10, 0)",
                    fetch_size=10000,
                    query_timeout=60,
                )
                print(partitioned_df.schema)
                # Default error-if-exists mode avoids overwriting an existing table.
                partitioned_df.write.save_as_table("JDBC_DEMO.PUBLIC.ORACLE_SAMPLE")
                session.table("JDBC_DEMO.PUBLIC.ORACLE_SAMPLE").order_by("ID").show()

            The equivalent type override using ``StructType`` is:

            .. code-block:: python

                from snowflake.snowpark.types import DecimalType, StructField, StructType

                schema_override = StructType([StructField("ID", DecimalType(10, 0))])
                predicate_df = session.read.jdbc(
                    url=oracle_url,
                    udtf_configs=oracle_config,
                    properties=oracle_properties,
                    query=source_query,
                    predicates=["ID < 3 OR ID IS NULL", "ID >= 3"],
                    custom_schema=schema_override,
                    fetch_size=10000,
                    query_timeout=60,
                )
                predicate_df.order_by("ID").show()

            For a table read, replace ``query`` with ``table``. This example uses
            Oracle's existing one-row ``DUAL`` table. For application tables, use
            a qualified name such as ``APP.ORDERS`` and ensure source SELECT access.

            .. code-block:: python

                table_df = session.read.jdbc(
                    url=oracle_url,
                    udtf_configs=oracle_config,
                    properties=oracle_properties,
                    table="DUAL",
                    query_timeout=60,
                )
                table_df.show()

                # The format/options interface accepts the same JDBC options.
                option_df = (
                    session.read.format("jdbc")
                    .option("url", oracle_url)
                    .option("udtf_configs", oracle_config)
                    .option("properties", oracle_properties)
                    .option("query", source_query)
                    .option("query_timeout", 60)
                    .load()
                )
                option_df.show()

        Note:
            **Type mapping.** Inference uses JDBC type, source type name, Java
            class, precision, and scale reported by the driver. Inspect ``df.schema``
            with your driver version and connection properties before persisting.
            Most values are fetched as strings and cast in Snowflake; this is not
            identical to every DB-API driver's conversion behavior.

            - JDBC integer types map to integral Snowflake types; character and
              CLOB types map to STRING; floating-point types map to floating-point
              Snowflake types. Declared numeric precision/scale is retained when
              valid for Snowflake. Unconstrained Oracle NUMBER and PostgreSQL
              NUMERIC use NUMBER(38, 18), leaving 20 integer digits. Oracle FLOAT
              with floating-point metadata uses DOUBLE. Precision beyond 38 is
              capped; negative scales use NUMBER(38, 0). Out-of-range values can
              fail or round; use an explicit source cast or suitable override.
            - JDBC BOOLEAN, PostgreSQL boolean, and SQL Server BIT map to BOOLEAN.
              Other JDBC BIT results remain STRING, including MySQL BIT and the
              default MySQL BOOLEAN/TINYINT(1) metadata. TINYINT(1) can contain values
              other than 0 and 1, so it is not safe to convert all such columns to
              BOOLEAN. Driver properties can change the reported metadata. Validate
              the data before choosing an explicit cast or override.
            - JDBC TIMESTAMP without timezone maps to TIMESTAMP_NTZ. Oracle DATE
              reported as TIMESTAMP also uses NTZ, retaining its time of day.
              Oracle TIMESTAMP WITH TIME ZONE, PostgreSQL TIMESTAMPTZ, SQL Server
              DATETIMEOFFSET, and JDBC TIMESTAMP_WITH_TIMEZONE use TIMESTAMP_TZ
              via Java ``OffsetDateTime``. The instant and returned offset are
              represented, not the original timezone-region name. PostgreSQL's
              driver returns these values in UTC; the original input offset is
              not recoverable from TIMESTAMPTZ.
            - Oracle TIMESTAMP WITH LOCAL TIME ZONE uses TIMESTAMP_LTZ via a Java
              timestamp converted to an instant. Display depends on Snowflake's
              session timezone; source/driver timezone settings affect interpretation.
              TIME WITH TIME ZONE (including PostgreSQL TIMETZ) remains STRING
              because Snowflake TIME cannot represent its offset. Ordinary JDBC
              DATE and TIME map to DATE and TIME.
            - Unmapped JDBC types fall back to a recognized Java-class mapping,
              then STRING if neither is recognized. JDBC OTHER also maps to STRING
              unless a specific timezone mapping applies. This is not a general
              VARIANT/JSON parser or a guarantee of lossless conversion.
            - Binary values currently use driver string conversion, not a lossless
              binary transfer. Do not rely on RAW, BINARY, VARBINARY, BYTEA, or BLOB
              round-tripping arbitrary bytes. Project an explicit source-side hex
              or base64 encoding as text when byte fidelity is required.

            **Streaming and tuning.** The read UDTF fetches rows lazily, but a JDBC
            driver can buffer a full result set. ``fetch_size`` alone does not bound
            memory. PostgreSQL cursor fetching requires autocommit to be disabled;
            this reader does not call ``Connection.setAutoCommit(false)``, so a
            positive fetch size alone does not enable it. See `pgJDBC cursor
            requirements <https://jdbc.postgresql.org/documentation/query/>`_.
            MySQL cursor fetching requires driver configuration such as
            ``useCursorFetch=true`` as well as a positive fetch size. SQL Server's
            adaptive response buffering is driver-specific. Measure your driver
            and workload; use selective source queries and balanced partitions,
            rather than assuming all drivers stream identically. Separate queries
            do not guarantee a transactionally consistent snapshot of a changing
            source. A Snowpark ``limit()`` after ingestion is not a source row limit.

            **Limits and troubleshooting.**

            - Java UDTFs have a server limit of 500 output columns. The separate
              error ``Function name (including parameter and return type) too long``
              concerns total signature length, not row-value size or a universal
              240-column threshold. This reader uses short positional UDTF names
              and restores source names afterward, reducing signature length; it
              does not remove server limits. Project fewer source columns or split
              a wide read using a stable key. See `Java UDTF limits
              <https://docs.snowflake.com/en/developer-guide/udf/java/udf-java-tabular-functions>`_.
            - A blocking Java UDTF call can hit the server's default 600-second
              handler timeout, including a slow first fetch or a later stalled
              fetch. Streaming for more than 600 seconds in total is not itself
              the same condition. ``query_timeout=0`` and a larger fetch size do
              not disable that limit. Reduce time to first row, tune source SQL,
              or split the read. Contact Snowflake Support for persistent handler
              timeouts; there is no JDBC option here that overrides server limits.
            - For a missing integration/secret or authorization error, check all
              three required ``udtf_configs`` keys, fully qualified secret name,
              ``ENABLED = TRUE``, ``ALLOWED_AUTHENTICATION_SECRETS``, and the grants
              above. A connection failure also warrants checking the allowed
              host/port, routing, source firewall, credentials, and TLS trust.
            - For ``No suitable driver`` or Java class-version errors, verify the
              exact stage/JAR path, stage READ privilege, uncompressed upload, JDBC
              URL prefix, and compatibility with ``java_version``.
            - Oracle ``ORA-12660`` indicates incompatible encryption requirements.
              Coordinate the NNE encryption/checksum settings and supported
              algorithms with the source administrator; do not disable required
              encryption to work around the error.
            - A SQL Server subquery ``ORDER BY`` error requires changing the source
              query, not Snowflake's sort settings. Remove the source sort and
              order the returned DataFrame, or use a valid source TOP/OFFSET query.
            - Inference or cast errors with ``custom_schema`` are not bypassed by
              supplying more fields. Check source access, reported names/types,
              and value ranges; override only existing result fields.
        """
        if (not table and not query) or (table and query):
            raise SnowparkDataframeReaderException(
                "Either 'table' or 'query' must be provided, but not both."
            )
        external_access_integration = udtf_configs.get(
            "external_access_integration", None
        )
        imports = udtf_configs.get("imports", None)
        # Copy so the caller's udtf_configs is never mutated.
        packages = list(udtf_configs.get("packages", ["com.snowflake:snowpark:latest"]))
        java_version = udtf_configs.get("java_version", 17)
        secret = udtf_configs.get("secret", None)

        if external_access_integration is None or imports is None or secret is None:
            raise ValueError(
                "external_access_integration, secret and imports must be specified in udtf configs"
            )
        if column is not None and (
            isinstance(num_partitions, bool)
            or not isinstance(num_partitions, int)
            or num_partitions < 1
        ):
            raise ValueError(
                f"num_partitions must be a positive integer, got {num_partitions!r}"
            )

        telemetry_json_string = defaultdict()
        telemetry_json_string["function_name"] = DATA_SOURCE_JDBC_SIGNATURE
        telemetry_json_string["ingestion_mode"] = "udtf_ingestion"
        telemetry_json_string["dbms_type"] = get_jdbc_dbms(url)
        telemetry_json_string["imports"] = imports
        statements_params_for_telemetry = {STATEMENT_PARAMS_DATA_SOURCE_JDBC: "1"}

        start_time = time.perf_counter()
        logger.debug(f"ingestion start at: {start_time}")

        if session_init_statement and isinstance(session_init_statement, str):
            session_init_statement = [session_init_statement]

        jdbc_client = JDBC(
            session=self._session,
            url=url,
            external_access_integration=external_access_integration,
            properties=properties,
            imports=imports,
            packages=packages,
            java_version=java_version,
            table_or_query=table or query,
            is_query=True if query is not None else False,
            secret=secret,
            column=column,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            num_partitions=num_partitions,
            query_timeout=query_timeout,
            fetch_size=fetch_size,
            custom_schema=custom_schema,
            predicates=predicates,
            session_init_statement=session_init_statement,
            _emit_ast=_emit_ast,
        )

        partitions = jdbc_client.partitions

        partitions_table = random_name_for_temp_object(TempObjectType.TABLE)
        self._session.create_dataframe(
            [[query] for query in partitions], schema=["partition"]
        ).write.save_as_table(
            partitions_table,
            table_type="temp",
            statement_params=statements_params_for_telemetry,
        )

        df = jdbc_client.read(partitions_table)
        res_df = jdbc_client.to_result_snowpark_df(
            df, jdbc_client.schema, _emit_ast=_emit_ast
        )
        end_time = time.perf_counter()
        telemetry_json_string["end_to_end_duration"] = end_time - start_time
        telemetry_json_string["schema"] = res_df.schema.simple_string()
        self._session._conn._telemetry_client.send_data_source_perf_telemetry(
            telemetry_json_string
        )
        set_api_call_source(res_df, DATA_SOURCE_JDBC_SIGNATURE)
        return res_df

    @publicapi
    def dbapi(
        self,
        create_connection: Callable[..., "Connection"],
        *,
        table: Optional[str] = None,
        query: Optional[str] = None,
        column: Optional[str] = None,
        lower_bound: Optional[Union[str, int]] = None,
        upper_bound: Optional[Union[str, int]] = None,
        num_partitions: Optional[int] = None,
        max_workers: Optional[int] = None,
        query_timeout: Optional[int] = 0,
        fetch_size: Optional[int] = 100000,
        custom_schema: Optional[Union[str, StructType]] = None,
        predicates: Optional[List[str]] = None,
        session_init_statement: Optional[Union[str, List[str]]] = None,
        udtf_configs: Optional[dict] = None,
        fetch_merge_count: int = 1,
        fetch_with_process: bool = False,
        connection_parameters: Optional[dict] = None,
        _emit_ast: bool = True,
    ) -> DataFrame:
        """
        Reads data from a database table or query into a DataFrame using a DBAPI connection,
        with support for optional partitioning, parallel processing, and query customization.

        Usage Notes:
            - Ingestion performance tuning:
                - **Partitioning**: Use ``column``, ``lower_bound``, ``upper_bound``, and ``num_partitions``
                  together to split large tables into smaller partitions for parallel processing.
                  All four parameters must be specified together, otherwise an error will be raised.
                - **Parallel execution**: Set ``max_workers`` to control the maximum number of processes
                  and threads used for parallel execution.
                - **Fetch optimization**: Adjust ``fetch_size`` to optimize performance by reducing
                  the number of round trips to the database.
                - **Partition filtering**: Use ``predicates`` to define WHERE conditions for partitions.
                  Note that ``predicates`` will be ignored if ``column`` is specified for partitioning.
                - **Schema specification**: Set ``custom_schema`` to skip schema inference. The custom schema
                  must have matching column names with the table in the external data source.
            - Execution timing and error handling:
                - **UDTF Ingestion**: Uses lazy evaluation. Errors are reported as ``SnowparkSQLException``
                  during DataFrame actions (e.g., ``DataFrame.collect()``).
                - **Local Ingestion**: Uses eager execution. Errors are reported immediately as
                  ``SnowparkDataFrameReaderException`` when this method is called.

        Args:
            create_connection: A callable that returns a DB-API compatible database connection.
                The callable can optionally accept keyword arguments via `**kwargs`.
                If connection_parameters is provided, those will be passed as keyword arguments to this callable.
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
            fetch_with_process: Whether to use multiprocessing for data fetching and Parquet file generation in local ingestion.
                Default to `False`, which means multithreading is used to fetch data in parallel.
                Setting this to `True` enables multiprocessing, which may improve performance for CPU-bound tasks
                like Parquet file generation. When using multiprocessing, guard your script with
                `if __name__ == "__main__":` and call `multiprocessing.freeze_support()` on Windows if needed.
                This parameter has no effect in UDFT ingestion.
            connection_parameters: Optional dictionary of parameters to pass to the create_connection callable.
                If provided, these parameters will be unpacked and passed as keyword arguments
                to create_connection(`**connection_parameters`).
                This allows for flexible connection configuration without hardcoding values in the callable.
                Example: {"timeout": 30, "isolation_level": "READ_UNCOMMITTED"}

        Example::
            .. code-block:: python

                import oracledb
                def create_oracledb_connection():
                    connection = oracledb.connect(...)
                    return connection

                df = session.read.dbapi(create_oracledb_connection, table=...)

        Example::
            .. code-block:: python

                import oracledb
                def create_oracledb_connection():
                    connection = oracledb.connect(...)
                    return connection

                df = session.read.dbapi(create_oracledb_connection, table=..., fetch_with_process=True)

        Example::
            .. code-block:: python

                import sqlite3
                def create_sqlite_connection(timeout=5.0, isolation_level=None, **kwargs):
                    connection = sqlite3.connect(
                        database=":memory:",
                        timeout=timeout,
                        isolation_level=isolation_level
                    )
                    return connection

                connection_params = {"timeout": 30.0, "isolation_level": "DEFERRED"}
                df = session.read.dbapi(
                    create_sqlite_connection,
                    table=...,
                    connection_parameters=connection_params
                )

        Example::
            .. code-block:: python

                import oracledb
                def create_oracledb_connection():
                    connection = oracledb.connect(...)
                    return connection

                # pull data from target table with parallelism using partition column
                df_local_par_column = session.read.dbapi(
                    create_oracledb_connection,
                    table="target_table",
                    fetch_size=100000,
                    num_partitions=4,
                    column="ID",  # swap with the column you want your partition based on
                    upper_bound=10000,
                    lower_bound=0
                )

        Example::
            .. code-block:: python

                import oracledb
                def create_oracledb_connection():
                    connection = oracledb.connect(...)
                    return connection

                # pull data from target table with parallelism using predicates
                df_local_predicates = session.read.dbapi(
                    create_oracledb_connection,
                    table="target_table",
                    fetch_size=100000,
                    predicates=[
                        "ID < 3",
                        "ID >= 3"
                    ]
                )

        Example::
            .. code-block:: python

                import oracledb
                def create_oracledb_connection():
                    connection = oracledb.connect(...)
                    return connection
                udtf_configs = {
                    "external_access_integration": "<your external access integration>"
                }

                # pull data from target table with udtf ingestion

                df_udtf_basic = session.read.dbapi(
                    create_oracledb_connection,
                    table="target_table",
                    udtf_configs=udtf_configs
                )

        Example::
            .. code-block:: python

                import oracledb
                def create_oracledb_connection():
                    connection = oracledb.connect(...)
                    return connection
                udtf_configs = {
                    "external_access_integration": "<your external access integration>"
                }

                # pull data from target table with udtf ingestion with parallelism using partition column

                df_udtf_par_column = session.read.dbapi(
                    create_oracledb_connection,
                    table="target_table",
                    udtf_configs=udtf_configs,
                    fetch_size=100000,
                    num_partitions=4,
                    column="ID",  # swap with the column you want your partition based on
                    upper_bound=10000,
                    lower_bound=0
                )

        Example::
            .. code-block:: python

                import oracledb
                def create_oracledb_connection():
                    connection = oracledb.connect(...)
                    return connection
                udtf_configs = {
                    "external_access_integration": "<your external access integration>"
                }

                # pull data from target table with udtf ingestion with parallelism using partition column

                df_udtf_predicates = session.read.dbapi(
                    create_oracledb_connection,
                    table="target_table",
                    udtf_configs=udtf_configs,
                    fetch_size=100000,
                    predicates=[
                        "ID < 3",
                        "ID >= 3"
                    ]
                )

        """
        if (not table and not query) or (table and query):
            raise SnowparkDataframeReaderException(
                "Either 'table' or 'query' must be provided, but not both."
            )

        telemetry_json_string = defaultdict()
        telemetry_json_string["function_name"] = DATA_SOURCE_DBAPI_SIGNATURE
        telemetry_json_string["ingestion_mode"] = (
            "udtf_ingestion" if udtf_configs is not None else "local_ingestion"
        )

        table_or_query = table or query
        is_query = True if table is None else False
        statements_params_for_telemetry = {STATEMENT_PARAMS_DATA_SOURCE: "1"}
        start_time = time.perf_counter()
        logger.debug(f"ingestion start at: {start_time}")
        if session_init_statement and isinstance(session_init_statement, str):
            session_init_statement = [session_init_statement]
        partitioner = DataSourcePartitioner(
            create_connection,
            table_or_query,
            is_query,
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
            connection_parameters,
        )
        struct_schema = partitioner.schema
        partitioned_queries = partitioner.partitions
        telemetry_json_string["dbms_type"] = partitioner.dbms_type.value
        telemetry_json_string["driver_type"] = partitioner.driver_type.value

        # udtf ingestion
        if udtf_configs is not None:
            if "external_access_integration" not in udtf_configs:
                raise ValueError(
                    "external_access_integration cannot be None when udtf ingestion is used. Please refer to https://docs.snowflake.com/en/sql-reference/sql/create-external-access-integration to create external access integration"
                )
            partitions_table = random_name_for_temp_object(TempObjectType.TABLE)
            self._session.create_dataframe(
                [[query] for query in partitioned_queries], schema=["partition"]
            ).write.save_as_table(
                partitions_table,
                table_type="temp",
                statement_params=statements_params_for_telemetry,
            )
            df = partitioner._udtf_ingestion(
                self._session,
                struct_schema,
                partitions_table,
                udtf_configs["external_access_integration"],
                fetch_size=fetch_size,
                imports=udtf_configs.get("imports", None),
                packages=udtf_configs.get("packages", None),
                artifact_repository=udtf_configs.get("artifact_repository", None),
                session_init_statement=session_init_statement,
                query_timeout=query_timeout,
                statement_params=statements_params_for_telemetry,
                _emit_ast=False,  # internal API, no need to emit AST
            )
            end_time = time.perf_counter()
            telemetry_json_string["end_to_end_duration"] = end_time - start_time
            logger.debug(
                f"ingestion end at: {end_time}, total time: {end_time - start_time}"
            )

            self._session._conn._telemetry_client.send_data_source_perf_telemetry(
                telemetry_json_string
            )
            set_api_call_source(df, DATA_SOURCE_DBAPI_SIGNATURE)
            return df

        # create table and stage for data source
        snowflake_table_name = random_name_for_temp_object(TempObjectType.TABLE)
        snowflake_stage_name = random_name_for_temp_object(TempObjectType.STAGE)
        create_data_source_table_and_stage(
            session=self._session,
            schema=struct_schema,
            snowflake_table_name=snowflake_table_name,
            snowflake_stage_name=snowflake_stage_name,
            statements_params_for_telemetry=statements_params_for_telemetry,
        )

        # parquet ingestion, ingestion external data source into temporary table

        local_ingestion(
            session=self._session,
            partitioner=partitioner,
            partitioned_queries=partitioned_queries,
            max_workers=max_workers,
            fetch_with_process=fetch_with_process,
            snowflake_stage_name=snowflake_stage_name,
            snowflake_table_name=snowflake_table_name,
            statements_params_for_telemetry=statements_params_for_telemetry,
            telemetry_json_string=telemetry_json_string,
            _emit_ast=_emit_ast,
        )

        end_time = time.perf_counter()
        telemetry_json_string["end_to_end_duration"] = end_time - start_time

        # Knowingly generating AST for `session.read.dbapi` calls as simply `session.read.table` calls
        # with the new name for the temporary table into which the external db data was ingressed.
        # Leaving this functionality as client-side only means capturing an AST specifically for
        # this API in a new entity is not valuable from a server-side execution or AST perspective.
        res_df = partitioner.driver.to_result_snowpark_df(
            self, snowflake_table_name, struct_schema, _emit_ast=_emit_ast
        )
        telemetry_json_string["schema"] = res_df.schema.simple_string()
        self._session._conn._telemetry_client.send_data_source_perf_telemetry(
            telemetry_json_string
        )
        logger.debug(
            f"ingestion end at: {end_time}, total time: {end_time - start_time}"
        )
        logger.debug(f"recorded telemetry: {json.dumps(telemetry_json_string)}")
        set_api_call_source(res_df, DATA_SOURCE_DBAPI_SIGNATURE)
        return res_df

    @publicapi
    @experimental(version="1.37.0")
    def file(self, path: str, _emit_ast: bool = True) -> DataFrame:
        """
        Returns a DataFrame with a single column ``FILE`` containing a list of files
        in the specified Snowflake stage location (either internal or external).

        Args:
            path: The stage location to list files from (e.g., "@mystage", "@mystage/path/").

        Example::

            >>> # Create a temp stage and upload some test files
            >>> _ = session.sql("CREATE OR REPLACE TEMP STAGE mystage").collect()
            >>> _ = session.file.put("tests/resources/testCSV.csv", "@mystage", auto_compress=False)
            >>> _ = session.file.put("tests/resources/testCSVheader.csv", "@mystage", auto_compress=False)
            >>> _ = session.file.put("tests/resources/testJson.json", "@mystage", auto_compress=False)

            >>> # List all files in the stage
            >>> df = session.read.file("@mystage")
            >>> df.count()
            3

            >>> # List files matching a pattern (only CSV files)
            >>> df = session.read.option("pattern", ".*\\.csv").file("@mystage")
            >>> df.count()
            2

            >>> # List files with a more specific pattern
            >>> df = session.read.option("pattern", ".*header.*").file("@mystage")
            >>> df.count()
            1
        """
        path = _validate_stage_path(path)

        # Build the LIST command
        list_query = f"LIST {path}"

        # Add pattern option if specified
        if "PATTERN" in self._cur_options:
            pattern = self._cur_options["PATTERN"]
            # Use proper SQL escaping to prevent SQL injection
            escaped_pattern = convert_value_to_sql_option(pattern)
            list_query += f" PATTERN = {escaped_pattern}"

        # Execute the LIST command and create DataFrame
        # LIST returns columns: name, size, md5, last_modified
        # We only want the 'name' column renamed to 'FILE'
        df = self._session.sql(list_query, _emit_ast=_emit_ast).select(
            to_file(concat(lit("@"), col('"name"'))).alias("file")
        )

        set_api_call_source(df, "DataFrameReader.file")
        return df

    def directory(self, stage_name: str, _emit_ast: bool = True) -> DataFrame:
        """
        Returns a DataFrame representing the results of a directory table query on the specified stage.

        This method is an alias of :meth:`~snowflake.snowpark.session.Session.directory`.

        Args:
            stage_name: The name of the stage to query. The stage name should not include the '@' prefix
                as it will be added automatically.

        Returns:
            A DataFrame containing metadata about files in the stage with the following columns:

            - ``RELATIVE_PATH``: Path to the files to access using the file URL
            - ``SIZE``: Size of the file in bytes
            - ``LAST_MODIFIED``: Timestamp when the file was last updated in the stage
            - ``MD5``: MD5 checksum for the file
            - ``ETAG``: ETag header for the file
            - ``FILE_URL``: Snowflake file URL to access the file

        """
        # AST.
        stmt = None
        if _emit_ast and self._ast is not None:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.read_directory, stmt)
            ast.reader.CopyFrom(self._ast)
            ast.stage_name = stage_name

        dataframe = self._session.directory(stage_name, _emit_ast=False)

        if _emit_ast and stmt is not None:
            dataframe._ast_id = stmt.uid

        return dataframe
