#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import logging
from enum import Enum
from typing import List, Any, TYPE_CHECKING

from snowflake.snowpark._internal.data_source.datasource_typing import Connection
from snowflake.snowpark._internal.data_source.drivers import BaseDriver
from snowflake.snowpark._internal.utils import generate_random_alphanumeric
from snowflake.snowpark.functions import to_variant, parse_json, column
from snowflake.snowpark.types import (
    StructType,
    IntegerType,
    StringType,
    DecimalType,
    BooleanType,
    DateType,
    DoubleType,
    TimestampType,
    VariantType,
    FloatType,
    BinaryType,
    TimeType,
    TimestampTimeZone,
    StructField,
)

if TYPE_CHECKING:
    from snowflake.snowpark.session import Session  # pragma: no cover
    from snowflake.snowpark.dataframe import DataFrame  # pragma: no cover
    from snowflake.snowpark._internal.data_source.datasource_typing import (
        Cursor,
    )  # pragma: no cover


logger = logging.getLogger(__name__)


# The following Enum Class is generated from the following two docs:
# 1. https://github.com/psycopg/psycopg2/blob/master/psycopg/pgtypes.h
# 2. https://www.postgresql.org/docs/current/datatype.html
# pgtypes.h includes a broad range of type codes, but some newer type codes are missing.
# We will focus on the overlapping types that appear in both the documentation and the results from our Postgres tests.
class Psycopg2TypeCode(Enum):
    BOOLOID = 16
    BYTEAOID = 17
    CHAROID = 18
    # NAMEOID = 19 # Not listed in the Postgres doc.
    INT8OID = 20
    INT2OID = 21
    # INT2VECTOROID = 22  # Not listed in the Postgres doc.
    INT4OID = 23
    # REGPROCOID = 24  # Not listed in the Postgres doc.
    TEXTOID = 25
    # OIDOID = 26  # Not listed in the Postgres doc.
    # TIDOID = 27  # Not listed in the Postgres doc.
    # XIDOID = 28  # Not listed in the Postgres doc.
    # CIDOID = 29  # Not listed in the Postgres doc.
    # OIDVECTOROID = 30  # Not listed in the Postgres doc.
    # PG_TYPE_RELTYPE_OID = 71  # Not listed in the Postgres doc.
    # PG_ATTRIBUTE_RELTYPE_OID = 75  # Not listed in the Postgres doc.
    # PG_PROC_RELTYPE_OID = 81  # Not listed in the Postgres doc.
    # PG_CLASS_RELTYPE_OID = 83  # Not listed in the Postgres doc.
    JSON = 114  # Not listed in the pgtypes.h
    XML = 142  # Not listed in the pgtypes.h
    POINTOID = 600
    LSEGOID = 601
    PATHOID = 602
    BOXOID = 603
    POLYGONOID = 604
    LINEOID = 628
    FLOAT4OID = 700
    FLOAT8OID = 701
    # ABSTIMEOID = 702  # Not listed in the Postgres doc.
    # RELTIMEOID = 703  # Not listed in the Postgres doc.
    # TINTERVALOID = 704  # Not listed in the Postgres doc.
    # UNKNOWNOID = 705  # Not listed in the Postgres doc.
    CIRCLEOID = 718
    MACADDR8 = 774  # Not listed in the pgtypes.h
    CASHOID = 790  # MONEY
    MACADDROID = 829
    CIDROID = 650
    INETOID = 869
    INT4ARRAYOID = 1007  # Not listed in the Postgres doc.
    ACLITEMOID = 1033  # Not listed in the Postgres doc.
    BPCHAROID = 1042
    VARCHAROID = 1043
    DATEOID = 1082
    TIMEOID = 1083
    TIMESTAMPOID = 1114
    TIMESTAMPTZOID = 1184
    INTERVALOID = 1186
    TIMETZOID = 1266
    BITOID = 1560
    VARBITOID = 1562
    NUMERICOID = 1700
    # REFCURSOROID = 1790  # Not listed in the Postgres doc.
    # REGPROCEDUREOID = 2202  # Not listed in the Postgres doc.
    # REGOPEROID = 2203  # Not listed in the Postgres doc.
    # REGOPERATOROID = 2204  # Not listed in the Postgres doc.
    # REGCLASSOID = 2205  # Not listed in the Postgres doc.
    # REGTYPEOID = 2206  # Not listed in the Postgres doc.
    # RECORDOID = 2249  # Not listed in the Postgres doc.
    # CSTRINGOID = 2275  # Not listed in the Postgres doc.
    # ANYOID = 2276  # Not listed in the Postgres doc.
    # ANYARRAYOID = 2277  # Not listed in the Postgres doc.
    # VOIDOID = 2278  # Not listed in the Postgres doc.
    # TRIGGEROID = 2279  # Not listed in the Postgres doc.
    # LANGUAGE_HANDLEROID = 2280  # Not listed in the Postgres doc.
    # INTERNALOID = 2281  # Not listed in the Postgres doc.
    # OPAQUEOID = 2282  # Not listed in the Postgres doc.
    # ANYELEMENTOID = 2283  # Not listed in the Postgres doc.
    UUID = 2950  # Not listed in the pgtypes.h
    TXID_SNAPSHOT = 2970  # Not listed in the pgtypes.h
    PG_LSN = 3220  # Not listed in the pgtypes.h
    TSVECTOR = 3614  # Not listed in the pgtypes.h
    TSQUERY = 3615  # Not listed in the pgtypes.h
    JSONB = 3802  # Not listed in the pgtypes.h
    PG_SNAPSHOT = 5038  # Not listed in the pgtypes.h


# https://other-docs.snowflake.com/en/connectors/postgres6/view-data#postgresql-to-snowflake-data-type-mapping
BASE_POSTGRES_TYPE_TO_SNOW_TYPE = {
    Psycopg2TypeCode.BOOLOID: BooleanType,
    Psycopg2TypeCode.BYTEAOID: BinaryType,
    Psycopg2TypeCode.CHAROID: StringType,
    Psycopg2TypeCode.INT8OID: IntegerType,
    Psycopg2TypeCode.INT2OID: IntegerType,
    Psycopg2TypeCode.INT4OID: IntegerType,
    Psycopg2TypeCode.TEXTOID: StringType,
    Psycopg2TypeCode.POINTOID: StringType,
    Psycopg2TypeCode.LSEGOID: StringType,
    Psycopg2TypeCode.PATHOID: StringType,
    Psycopg2TypeCode.BOXOID: StringType,
    Psycopg2TypeCode.POLYGONOID: StringType,
    Psycopg2TypeCode.LINEOID: StringType,
    Psycopg2TypeCode.FLOAT4OID: FloatType,
    Psycopg2TypeCode.FLOAT8OID: DoubleType,
    Psycopg2TypeCode.CIRCLEOID: StringType,
    Psycopg2TypeCode.CASHOID: VariantType,
    Psycopg2TypeCode.MACADDROID: StringType,
    Psycopg2TypeCode.CIDROID: StringType,
    Psycopg2TypeCode.INETOID: StringType,
    Psycopg2TypeCode.BPCHAROID: StringType,
    Psycopg2TypeCode.VARCHAROID: StringType,
    Psycopg2TypeCode.DATEOID: DateType,
    Psycopg2TypeCode.TIMEOID: TimeType,
    Psycopg2TypeCode.TIMESTAMPOID: TimestampType,
    Psycopg2TypeCode.TIMESTAMPTZOID: TimestampType,
    Psycopg2TypeCode.INTERVALOID: StringType,
    Psycopg2TypeCode.TIMETZOID: TimeType,
    Psycopg2TypeCode.BITOID: StringType,
    Psycopg2TypeCode.VARBITOID: StringType,
    Psycopg2TypeCode.NUMERICOID: DecimalType,
    Psycopg2TypeCode.JSON: VariantType,
    Psycopg2TypeCode.JSONB: VariantType,
    Psycopg2TypeCode.MACADDR8: StringType,
    Psycopg2TypeCode.UUID: StringType,
    Psycopg2TypeCode.XML: StringType,
    Psycopg2TypeCode.TSVECTOR: StringType,
    Psycopg2TypeCode.TSQUERY: StringType,
    Psycopg2TypeCode.TXID_SNAPSHOT: StringType,
    Psycopg2TypeCode.PG_LSN: StringType,
    Psycopg2TypeCode.PG_SNAPSHOT: StringType,
}


class Psycopg2Driver(BaseDriver):
    def to_snow_type(self, schema: List[Any]) -> StructType:
        # The psycopg2 spec is defined in the following links:
        # https://www.psycopg.org/docs/cursor.html#cursor.description
        # https://www.psycopg.org/docs/extensions.html#psycopg2.extensions.Column
        fields = []
        for (
            name,
            type_code,
            _display_size,
            _internal_size,
            precision,
            scale,
            _null_ok,
        ) in schema:
            try:
                type_code = Psycopg2TypeCode(type_code)
            except ValueError:
                # not supported type is now handled as string type in below code
                type_code = None
            snow_type = BASE_POSTGRES_TYPE_TO_SNOW_TYPE.get(type_code, StringType)
            if type_code == Psycopg2TypeCode.NUMERICOID:
                if not self.validate_numeric_precision_scale(precision, scale):
                    logger.debug(
                        f"Snowpark does not support column"
                        f" {name} of type {type_code} with precision {precision} and scale {scale}. "
                        "The default Numeric precision and scale will be used."
                    )
                    precision, scale = None, None
                data_type = snow_type(
                    precision if precision is not None else 38,
                    scale if scale is not None else 0,
                )
            elif type_code == Psycopg2TypeCode.TIMESTAMPTZOID:
                data_type = snow_type(TimestampTimeZone.TZ)
            else:
                data_type = snow_type()
            fields.append(StructField(name, data_type, True))
        return StructType(fields)

    def non_retryable_error_checker(self, error: Exception) -> bool:
        import psycopg2

        if isinstance(error, psycopg2.errors.SyntaxError):
            syntax_error_codes = [
                "42601",  # syntax error
            ]
            for error_code in syntax_error_codes:
                if error_code == str(error.pgcode):
                    return True
        return False

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

    @staticmethod
    def prepare_connection(
        conn: "Connection",
        query_timeout: int = 0,
    ) -> "Connection":
        if query_timeout:
            # https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-STATEMENT-TIMEOUT
            # postgres default uses milliseconds
            conn.cursor().execute(f"SET STATEMENT_TIMEOUT = {query_timeout * 1000}")
        return conn

    def udtf_class_builder(
        self,
        fetch_size: int = 1000,
        schema: StructType = None,
        session_init_statement: List[str] = None,
        query_timeout: int = 0,
    ) -> type:
        create_connection = self.create_connection
        connection_parameters = self.connection_parameters

        # TODO: SNOW-2101485 use class method to prepare connection
        # ideally we should use the same function as prepare_connection
        # however, since we introduce new module for new driver support and initially the new module is not available in the backend
        # so if registering UDTF which uses the class method, cloudpickle will pickle the class method along with
        # the new module -- this leads to not being able to find the new module when unpickling on the backend.
        # once the new module is available in the backend, we can use the class method.
        def prepare_connection_in_udtf(
            conn: "Connection",
            query_timeout: int = 0,
        ) -> "Connection":
            if query_timeout:
                # https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-STATEMENT-TIMEOUT
                # postgres default uses milliseconds
                conn.cursor().execute(f"SET STATEMENT_TIMEOUT = {query_timeout * 1000}")
            return conn

        class UDTFIngestion:
            def process(self, query: str):
                conn_result = (
                    create_connection(**connection_parameters)
                    if connection_parameters
                    else create_connection()
                )
                conn = prepare_connection_in_udtf(conn_result, query_timeout)
                cursor = conn.cursor(
                    f"SNOWPARK_CURSOR_{generate_random_alphanumeric(5)}"
                )
                if session_init_statement is not None:
                    session_init_cur = conn.cursor()
                    for statement in session_init_statement:
                        session_init_cur.execute(statement)
                        session_init_cur.fetchall()
                cursor.execute(query)
                while True:
                    rows = cursor.fetchmany(fetch_size)
                    if not rows:
                        break
                    yield from rows

        return UDTFIngestion

    def get_server_cursor_if_supported(self, conn: "Connection") -> "Cursor":
        return conn.cursor(f"SNOWPARK_CURSOR_{generate_random_alphanumeric(5)}")
