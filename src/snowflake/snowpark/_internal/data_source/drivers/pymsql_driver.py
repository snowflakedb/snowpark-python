#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from enum import Enum
from decimal import Decimal
from datetime import date, datetime, timedelta
from typing import List, Any, Type, TYPE_CHECKING
import logging
from snowflake.snowpark._internal.data_source.drivers import BaseDriver
from snowflake.snowpark._internal.data_source.datasource_typing import (
    Connection,
    Cursor,
)
from snowflake.snowpark._internal.type_utils import NoneType
from snowflake.snowpark.functions import to_variant, parse_json, column
from snowflake.snowpark.types import (
    StructType,
    StringType,
    DecimalType,
    DateType,
    TimestampType,
    FloatType,
    BinaryType,
    StructField,
    TimeType,
    IntegerType,
    TimestampTimeZone,
    VariantType,
)

if TYPE_CHECKING:
    from snowflake.snowpark.session import Session  # pragma: no cover
    from snowflake.snowpark.dataframe import DataFrame  # pragma: no cover

_MYSQL_INFER_TYPE_SAMPLE_LIMIT = 1000

logger = logging.getLogger(__name__)


class PymysqlTypeCode(Enum):
    DECIMAL = (0, Decimal)
    NEWDECIMAL = (246, Decimal)
    INT = (3, int)
    TINYINT = (1, int)
    SMALLINT = (2, int)
    MEDIUMINT = (9, int)
    BIGINT = (8, int)
    YEAR = (13, int)
    FLOAT = (4, float)
    DOUBLE = (5, float)
    CHAR = (254, str)
    VARCHAR = (253, str)
    TINYTEXT = (252, str)
    TEXT = (252, str)
    MEDIUMTEXT = (252, str)
    LONGTEXT = (252, str)
    ENUM = (254, str)
    SET = (254, str)
    BIT = (16, bytes)
    BINARY = (254, bytes)
    VARBINARY = (253, bytes)
    TINYBLOB = (252, bytes)
    BLOB = (252, bytes)
    MEDIUMBLOB = (252, bytes)
    LONGBLOB = (252, bytes)
    DATE = (10, date)
    DATETIME = (12, datetime)
    TIMESTAMP = (7, datetime)
    TIME = (11, timedelta)
    JSON = (245, str)


BASE_PYMYSQL_TYPE_TO_SNOW_TYPE = {
    PymysqlTypeCode.DECIMAL: DecimalType,
    PymysqlTypeCode.NEWDECIMAL: DecimalType,
    PymysqlTypeCode.INT: IntegerType,
    PymysqlTypeCode.TINYINT: IntegerType,
    PymysqlTypeCode.SMALLINT: IntegerType,
    PymysqlTypeCode.MEDIUMINT: IntegerType,
    PymysqlTypeCode.BIGINT: IntegerType,
    PymysqlTypeCode.YEAR: IntegerType,
    PymysqlTypeCode.FLOAT: FloatType,
    PymysqlTypeCode.DOUBLE: FloatType,
    PymysqlTypeCode.CHAR: StringType,
    PymysqlTypeCode.VARCHAR: StringType,
    PymysqlTypeCode.TINYTEXT: StringType,
    PymysqlTypeCode.TEXT: StringType,
    PymysqlTypeCode.MEDIUMTEXT: StringType,
    PymysqlTypeCode.LONGTEXT: StringType,
    PymysqlTypeCode.ENUM: StringType,
    PymysqlTypeCode.SET: StringType,
    PymysqlTypeCode.BIT: StringType,
    PymysqlTypeCode.BINARY: BinaryType,
    PymysqlTypeCode.VARBINARY: BinaryType,
    PymysqlTypeCode.TINYBLOB: BinaryType,
    PymysqlTypeCode.BLOB: BinaryType,
    PymysqlTypeCode.MEDIUMBLOB: BinaryType,
    PymysqlTypeCode.LONGBLOB: BinaryType,
    PymysqlTypeCode.DATE: DateType,
    PymysqlTypeCode.DATETIME: TimestampType,
    PymysqlTypeCode.TIMESTAMP: TimestampType,
    PymysqlTypeCode.TIME: TimeType,
    PymysqlTypeCode.JSON: VariantType,
}


class PymysqlDriver(BaseDriver):
    @staticmethod
    def generate_infer_schema_sql(
        table_or_query: str, is_query: bool, query_input_alias: str
    ):
        return (
            f"SELECT * FROM ({table_or_query}) {query_input_alias} LIMIT {_MYSQL_INFER_TYPE_SAMPLE_LIMIT}"
            if is_query
            else f"SELECT * FROM `{table_or_query}` LIMIT {_MYSQL_INFER_TYPE_SAMPLE_LIMIT}"
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
        data = cursor.fetchall()
        raw_schema = cursor.description
        raw_types = self.infer_type_from_data(data, len(raw_schema))

        processed_raw_schema = []
        for type, col in zip(raw_types, raw_schema):
            new_col = list(col)
            new_col[1] = PymysqlTypeCode((new_col[1], type))
            processed_raw_schema.append(new_col)

        self.raw_schema = processed_raw_schema

    def to_snow_type(self, schema: List[Any]) -> StructType:
        """
        pymysql mysql type to type code mapping:
        https://github.com/PyMySQL/PyMySQL/blob/main/pymysql/constants/FIELD_TYPE.py

        mysql type to snowflake type mapping:
        https://other-docs.snowflake.com/en/connectors/mysql6/view-data#mysql-to-snowflake-data-type-mapping
        """
        fields = []
        for col in schema:
            (
                name,
                type_code,
                display_size,
                internal_size,
                precision,
                scale,
                null_ok,
            ) = col
            snow_type = BASE_PYMYSQL_TYPE_TO_SNOW_TYPE.get(type_code, StringType)
            if type_code in (PymysqlTypeCode.DECIMAL, PymysqlTypeCode.NEWDECIMAL):
                # we did -2 here because what driver returned is precision + 2, mysql store + 2 precision internally
                precision -= 2
                if not self.validate_numeric_precision_scale(precision, scale):
                    logger.debug(
                        f"Snowpark does not support column"
                        f" {name} of type {type_code} with precision {precision} and scale {scale}. "
                        "The maximum number of digits in DECIMAL format for MySQL is 65. "
                        "For Snowflake, the maximum is 38."
                        "Supported up to the maximum allowed digits in Snowflake. When exceeded, precision is lost."
                    )
                    precision, scale = None, None
                data_type = snow_type(
                    precision if precision is not None else 38,
                    scale if scale is not None else 0,
                )
            elif type_code == PymysqlTypeCode.TIMESTAMP:
                data_type = snow_type(TimestampTimeZone.TZ)
            elif type_code == PymysqlTypeCode.DATETIME:
                data_type = snow_type(TimestampTimeZone.NTZ)
            else:
                data_type = snow_type()
            fields.append(StructField(name, data_type, null_ok))
        return StructType(fields)

    def non_retryable_error_checker(self, error: Exception) -> bool:
        import pymysql

        if isinstance(error, pymysql.err.ProgrammingError):
            syntax_error_codes = [
                "1064",  # syntax error
            ]
            for error_code in syntax_error_codes:
                if error_code in str(error):
                    return True
        return False

    def udtf_class_builder(
        self,
        fetch_size: int = 1000,
        schema: StructType = None,
        session_init_statement: List[str] = None,
        query_timeout: int = 0,
    ) -> type:
        create_connection = self.create_connection
        connection_parameters = self.connection_parameters

        class UDTFIngestion:
            def process(self, query: str):
                import pymysql

                conn = (
                    create_connection(**connection_parameters)
                    if connection_parameters
                    else create_connection()
                )
                cursor = pymysql.cursors.SSCursor(conn)
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
    def infer_type_from_data(data: List[tuple], number_of_columns: int) -> List[Type]:
        # TODO: SNOW-2112938 investigate whether different types can be fit into one column
        #  (eg. if int and float both fit into decimal column)
        raw_data_types_set = [set() for _ in range(number_of_columns)]
        for row in data:
            for i, col in enumerate(row):
                if type(col) != NoneType:
                    raw_data_types_set[i].add(type(col))
        types = [
            type_set.pop() if len(type_set) == 1 else str
            for type_set in raw_data_types_set
        ]
        return types

    @staticmethod
    def to_result_snowpark_df(
        session: "Session", table_name, schema, _emit_ast: bool = True
    ) -> "DataFrame":
        project_columns = []
        for field in schema.fields:
            if isinstance(field.datatype, VariantType):
                project_columns.append(
                    to_variant(parse_json(column(field.name))).as_(field.name)
                )
            else:
                project_columns.append(column(field.name))
        return session.table(table_name, _emit_ast=_emit_ast).select(
            project_columns, _emit_ast=_emit_ast
        )

    @staticmethod
    def to_result_snowpark_df_udtf(
        res_df: "DataFrame",
        schema: StructType,
        _emit_ast: bool = True,
    ):
        cols = []
        for field in schema.fields:
            if isinstance(field.datatype, VariantType):
                cols.append(to_variant(parse_json(column(field.name))).as_(field.name))
            else:
                cols.append(res_df[field.name].cast(field.datatype).alias(field.name))
        return res_df.select(cols, _emit_ast=_emit_ast)

    def get_server_cursor_if_supported(self, conn: "Connection") -> "Cursor":
        import pymysql

        return pymysql.cursors.SSCursor(conn)
