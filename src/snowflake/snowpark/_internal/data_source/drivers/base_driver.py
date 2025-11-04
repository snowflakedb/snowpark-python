#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from enum import Enum
import datetime
from typing import Dict, List, Callable, Any, Optional, TYPE_CHECKING
from snowflake.connector.options import pandas as pd

from snowflake.snowpark._internal.analyzer.analyzer_utils import unquote_if_quoted
from snowflake.snowpark._internal.data_source.datasource_typing import (
    Connection,
    Cursor,
)
from snowflake.snowpark._internal.server_connection import MAX_STRING_SIZE
from snowflake.snowpark._internal.utils import (
    get_sorted_key_for_version,
    measure_time,
    random_name_for_temp_object,
    TempObjectType,
)
from snowflake.snowpark.exceptions import SnowparkDataframeReaderException
from snowflake.snowpark.types import (
    StructType,
    StructField,
    VariantType,
    TimestampType,
    IntegerType,
    BinaryType,
    DateType,
    BooleanType,
    StringType,
)
import snowflake.snowpark
import logging

PARTITION_TABLE_COLUMN_NAME = "partition"

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from snowflake.snowpark.session import Session
    from snowflake.snowpark.dataframe import DataFrame


class BaseDriver:
    def __init__(
        self,
        create_connection: Callable[..., "Connection"],
        dbms_type: Enum,
        connection_parameters: Optional[dict] = None,
    ) -> None:
        self.create_connection = create_connection
        self.dbms_type = dbms_type
        self.connection_parameters = connection_parameters
        self.raw_schema = None

    def _call_create_connection(self) -> "Connection":
        """Call create_connection with connection_parameters if provided."""
        if self.connection_parameters:
            return self.create_connection(**self.connection_parameters)
        return self.create_connection()

    def to_snow_type(self, schema: List[Any]) -> StructType:
        raise NotImplementedError(
            f"{self.__class__.__name__} has not implemented to_snow_type function"
        )

    def non_retryable_error_checker(self, error: Exception) -> bool:
        return False

    @staticmethod
    def prepare_connection(
        conn: "Connection",
        query_timeout: int = 0,
    ) -> "Connection":
        return conn

    @staticmethod
    def generate_infer_schema_sql(
        table_or_query: str, is_query: bool, query_input_alias: str
    ):
        return (
            f"SELECT * FROM ({table_or_query}) {query_input_alias} WHERE 1 = 0"
            if is_query
            else f"SELECT * FROM {table_or_query} WHERE 1 = 0"
        )

    def get_raw_schema(
        self,
        table_or_query: str,
        cursor: "Cursor",
        is_query: bool,
        query_input_alias: str,
    ) -> None:
        cursor.execute(
            self.generate_infer_schema_sql(table_or_query, is_query, query_input_alias)
        )
        self.raw_schema = cursor.description

    def infer_schema_from_description(
        self,
        table_or_query: str,
        cursor: "Cursor",
        is_query: bool,
        query_input_alias: str,
    ) -> StructType:
        self.get_raw_schema(table_or_query, cursor, is_query, query_input_alias)
        generated_schema = self.to_snow_type(self.raw_schema)
        # snowflake will default string length to 128MB in the bundle which will be enabled in 2026-01
        # https://docs.snowflake.com/en/release-notes/bcr-bundles/2025_07_bundle
        # here we prematurely make the change to default string to
        # 1. align the string length with UDTF based ingestion
        # 2. avoid the BCR impact to dbapi feature
        for field in generated_schema.fields:
            if isinstance(field.datatype, StringType) and field.datatype.length is None:
                field.datatype.length = MAX_STRING_SIZE
        return generated_schema

    def infer_schema_from_description_with_error_control(
        self, table_or_query: str, is_query: bool, query_input_alias: str
    ) -> StructType:
        conn = self._call_create_connection()
        cursor = conn.cursor()
        try:
            return self.infer_schema_from_description(
                table_or_query, cursor, is_query, query_input_alias
            )

        except Exception as exc:
            raise SnowparkDataframeReaderException(
                "Auto infer schema failure:"
                f"{exc!r}."
                "A query:"
                f"{self.generate_infer_schema_sql(table_or_query, is_query, query_input_alias)}"
                "is used to infer Snowpark DataFrame schema from"
                f"{table_or_query}"
                "But it failed with above exception"
            ) from exc
        finally:
            # Best effort to close cursor and connection; failures are non-critical and can be ignored.
            try:
                cursor.close()
            except BaseException as exc:
                logger.debug(
                    f"Failed to close cursor after inferring schema from description due to error: {exc!r}"
                )

            try:
                conn.close()
            except BaseException as exc:
                logger.debug(
                    f"Failed to close connection after inferring schema from description due to error: {exc!r}"
                )

    def udtf_ingestion(
        self,
        session: "snowflake.snowpark.Session",
        schema: StructType,
        partition_table: str,
        external_access_integrations: str,
        fetch_size: int = 1000,
        imports: Optional[List[str]] = None,
        packages: Optional[List[str]] = None,
        session_init_statement: Optional[List[str]] = None,
        query_timeout: Optional[int] = 0,
        statement_params: Optional[Dict[str, str]] = None,
        _emit_ast: bool = True,
    ) -> "snowflake.snowpark.DataFrame":
        from snowflake.snowpark._internal.data_source.utils import UDTF_PACKAGE_MAP

        udtf_name = random_name_for_temp_object(TempObjectType.FUNCTION)
        with measure_time() as udtf_register_time:
            session.udtf.register(
                self.udtf_class_builder(
                    fetch_size=fetch_size,
                    schema=schema,
                    session_init_statement=session_init_statement,
                    query_timeout=query_timeout,
                ),
                name=udtf_name,
                output_schema=StructType(
                    [
                        StructField(field.name, VariantType(), field.nullable)
                        for field in schema.fields
                    ]
                ),
                external_access_integrations=[external_access_integrations],
                packages=packages or UDTF_PACKAGE_MAP.get(self.dbms_type),
                imports=imports,
                statement_params=statement_params,
                _emit_ast=_emit_ast,  # internal function call, _emit_ast will be set to False by the caller
            )
        logger.debug(f"register ingestion udtf takes: {udtf_register_time()} seconds")
        call_udtf_sql = f"""
            select * from {partition_table}, table({udtf_name}({PARTITION_TABLE_COLUMN_NAME}))
            """
        res = session.sql(call_udtf_sql, _emit_ast=_emit_ast)
        return BaseDriver.keep_nullable_attributes(
            self.to_result_snowpark_df_udtf(res, schema, _emit_ast=_emit_ast),
            schema,
        )

    def udtf_class_builder(
        self,
        fetch_size: int = 1000,
        schema: StructType = None,
        session_init_statement: List[str] = None,
        query_timeout: int = 0,
    ) -> type:
        create_connection = self.create_connection
        prepare_connection = self.prepare_connection
        connection_parameters = self.connection_parameters

        class UDTFIngestion:
            def process(self, query: str):
                conn_result = (
                    create_connection(**connection_parameters)
                    if connection_parameters
                    else create_connection()
                )
                conn = prepare_connection(conn_result, query_timeout)
                cursor = conn.cursor()
                if session_init_statement is not None:
                    for statement in session_init_statement:
                        cursor.execute(statement)
                cursor.execute(query)
                while True:
                    rows = cursor.fetchmany(fetch_size)
                    if not rows:
                        break
                    yield from rows

        return UDTFIngestion

    @staticmethod
    def validate_numeric_precision_scale(
        precision: Optional[int], scale: Optional[int]
    ) -> bool:
        if precision is not None:
            if not (0 <= precision <= 38):
                return False
            if scale is not None and not (0 <= scale <= precision):
                return False
        elif scale is not None:
            return False
        return True

    # convert timestamp and date to string to work around SNOW-1911989
    # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.map.html
    # 'map' is introduced in pandas 2.1.0, before that it is 'applymap'
    @staticmethod
    def df_map_method(pandas_df):
        return (
            pandas_df.applymap
            if get_sorted_key_for_version(str(pd.__version__)) < (2, 1, 0)
            else pandas_df.map
        )

    @staticmethod
    def data_source_data_to_pandas_df(
        data: List[Any], schema: StructType
    ) -> "pd.DataFrame":
        # unquote column name because double quotes stored in parquet file create column mismatch during copy into table
        columns = [unquote_if_quoted(col.name) for col in schema.fields]
        # this way handles both list of object and list of tuples and avoid implicit pandas type conversion
        df = pd.DataFrame([list(row) for row in data], columns=columns, dtype=object)

        for field in schema.fields:
            name = unquote_if_quoted(field.name)
            if isinstance(field.datatype, IntegerType):
                # 'Int64' is a pandas dtype while 'int64' is a numpy dtype, as stated here:
                # https://github.com/pandas-dev/pandas/issues/27731
                # https://pandas.pydata.org/docs/reference/api/pandas.Int64Dtype.html
                # https://numpy.org/doc/stable/reference/arrays.scalars.html#numpy.int64
                df[name] = df[name].astype("Int64")
            elif isinstance(field.datatype, (TimestampType, DateType)):
                df[name] = df[name].map(
                    lambda x: x.isoformat()
                    if isinstance(x, (datetime.datetime, datetime.date))
                    else x
                )
            # astype below is meant to address copy into failure when the column contain only None value,
            # pandas would infer wrong type for that column in that situation, thus we convert them to corresponding type.
            elif isinstance(field.datatype, BinaryType):
                # we convert all binary to hex, so it is safe to astype to string
                df[name] = (
                    df[name]
                    .map(lambda x: x.hex() if isinstance(x, (bytearray, bytes)) else x)
                    .astype("string")
                )
            elif isinstance(field.datatype, BooleanType):
                df[name] = df[name].astype("boolean")
        return df

    @staticmethod
    def to_result_snowpark_df(
        session: "Session", table_name: str, schema: StructType, _emit_ast: bool = True
    ) -> "DataFrame":
        return session.table(table_name, _emit_ast=_emit_ast)

    @staticmethod
    def keep_nullable_attributes(
        selected_df: "DataFrame", schema: StructType
    ) -> "DataFrame":
        for attr, source_field in zip(selected_df._plan.attributes, schema.fields):
            attr.nullable = source_field.nullable
        return selected_df

    @staticmethod
    def to_result_snowpark_df_udtf(
        res_df: "DataFrame",
        schema: StructType,
        _emit_ast: bool = True,
    ):
        cols = [
            res_df[field.name].cast(field.datatype).alias(field.name)
            for field in schema.fields
        ]
        return res_df.select(cols, _emit_ast=_emit_ast)

    def get_server_cursor_if_supported(self, conn: "Connection") -> "Cursor":
        """
        This method is used to get a server cursor if the driver and the DBMS supports it.
        It can be overridden by the driver to return a server cursor if supported.
        Otherwise, it will return the default cursor supported by the driver and the DBMS.

        - databricks-sql-connector: no concept of client/server cursor, no need to override
        - python-oracledb: default to the server cursor, no need to override
        - psycopg2: default to the client cursor which needs to be overridden to return the server cursor
        - pymysql: default to the client cursor which needs to be overridden to return the server cursor

        TODO:
        - pyodbc: This is a Python wrapper on top of ODBC drivers, the ODBC driver and the DBMS may or may not support server cursor
         and if they do support, the way to get the server cursor may vary across different DBMS. we need to document pyodbc.
        """
        return conn.cursor()
