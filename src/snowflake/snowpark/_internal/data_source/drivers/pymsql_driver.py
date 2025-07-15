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
from snowflake.snowpark._internal.utils import (
    random_name_for_temp_object,
    TempObjectType,
)
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
    def infer_schema_from_description(
        self, table_or_query: str, cursor: "Cursor", is_query: bool
    ) -> StructType:
        if is_query:
            random_table_alias = random_name_for_temp_object(TempObjectType.TABLE)
            cursor.execute(
                f"select {random_table_alias}.* from ({table_or_query}) {random_table_alias} limit {_MYSQL_INFER_TYPE_SAMPLE_LIMIT}"
            )
        else:
            cursor.execute(
                f"select * from `{table_or_query}` limit {_MYSQL_INFER_TYPE_SAMPLE_LIMIT}"
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
        return self.to_snow_type(processed_raw_schema)

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
            snow_type = BASE_PYMYSQL_TYPE_TO_SNOW_TYPE.get(type_code, None)
            if snow_type is None:
                raise NotImplementedError(f"mysql type not supported: {type_code}")
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

    def udtf_class_builder(
        self, fetch_size: int = 1000, schema: StructType = None
    ) -> type:
        create_connection = self.create_connection

        class UDTFIngestion:
            def process(self, query: str):

                conn = create_connection()
                cursor = conn.cursor()
                cursor.execute(query)
                while True:
                    rows = cursor.fetchmany(fetch_size)
                    if not rows:
                        break
                    yield from rows

        return UDTFIngestion

    def prepare_connection(
        self,
        conn: "Connection",
        query_timeout: int = 0,
    ) -> "Connection":
        conn.read_timeout = query_timeout if query_timeout != 0 else None
        return conn

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
