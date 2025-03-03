#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import decimal
import logging
from enum import Enum
from typing import List, Any, Tuple, Protocol, Optional
from snowflake.connector.options import pandas as pd

from snowflake.snowpark.exceptions import SnowparkDataframeReaderException
from snowflake.snowpark.types import (
    StringType,
    VariantType,
    BinaryType,
    TimestampType,
    DecimalType,
    FloatType,
    BooleanType,
    DateType,
    TimestampTimeZone,
    StructType,
    StructField,
    DoubleType,
    VectorType,
    TimeType,
)

_logger = logging.getLogger(__name__)

# https://learn.microsoft.com/en-us/sql/machine-learning/python/python-libraries-and-data-types?view=sql-server-ver16
SQL_SERVER_TYPE_TO_SNOW_TYPE = {
    int: DecimalType,
    float: FloatType,
    decimal.Decimal: DecimalType,
    datetime.datetime: TimestampType,
    bool: BooleanType,
    str: StringType,
    bytes: BinaryType,
    datetime.date: DateType,
    datetime.time: TimeType,
    bytearray: BinaryType,
}


STATEMENT_PARAMS_DATA_SOURCE = "SNOWPARK_PYTHON_DATASOURCE"
DATA_SOURCE_DBAPI_SIGNATURE = "DataFrameReader.dbapi"
DATA_SOURCE_SQL_COMMENT = (
    f"/* Python:snowflake.snowpark.{DATA_SOURCE_DBAPI_SIGNATURE} */"
)


class DBMS_TYPE(Enum):
    SQL_SERVER_DB = "SQL_SERVER_DB"
    ORACLE_DB = "ORACLE_DB"
    SQLITE_DB = "SQLITE3_DB"
    UNKNOWN = "UNKNOWN"


def detect_dbms(dbapi2_conn) -> Tuple[DBMS_TYPE, str]:
    """Detects the DBMS type from a DBAPI2 connection."""

    # Get the Python driver name
    python_driver_name = type(dbapi2_conn).__module__.lower()

    # Dictionary-based lookup for known DBMS
    dbms_mapping = {
        "pyodbc": detect_dbms_pyodbc,
        "oracledb": lambda conn: DBMS_TYPE.ORACLE_DB,
        "sqlite3": lambda conn: DBMS_TYPE.SQLITE_DB,
    }

    if python_driver_name in dbms_mapping:
        return dbms_mapping[python_driver_name](dbapi2_conn), python_driver_name

    _logger.debug(f"Unsupported database driver: {python_driver_name}")
    return DBMS_TYPE.UNKNOWN, python_driver_name


def detect_dbms_pyodbc(dbapi2_conn):
    """Detects the DBMS type for a pyodbc connection."""
    # pyodbc.SQL_DBMS_NAME is a constant used to get the DBMS name by calling dbapi2_conn.getinfo(pyodbc.SQL_DBMS_NAME)
    # and according to the ODBC spec, SQL_DBMS_NAME is an integer value 17
    # https://github.com/microsoft/ODBC-Specification/blob/4dda95986bda5d3b55d7749315d3e5a0951c1e50/Windows/inc/sql.h#L467
    # here we are using pyodbc_conn.getinfo(17) to get the DBMS name to avoid importing pyodbc
    # which helps our test while achieving the same goal
    dbms_name = dbapi2_conn.getinfo(17).lower()  # pyodbc.SQL_DBMS_NAME = 17

    # Set-based lookup for SQL Server
    sqlserver_keywords = {"sql server", "mssql", "sqlserver"}
    if any(keyword in dbms_name for keyword in sqlserver_keywords):
        return DBMS_TYPE.SQL_SERVER_DB

    _logger.debug(f"Unsupported DBMS for pyodbc: {dbms_name}")
    return DBMS_TYPE.UNKNOWN


class Connection(Protocol):
    """External datasource connection created from user-input create_connection function."""

    def cursor(self) -> "Cursor":
        pass

    def close(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass


class Cursor(Protocol):
    """Cursor created from external datasource connection"""

    def execute(self, sql: str, *params: Any) -> "Cursor":
        pass

    def fetchall(self) -> List[Tuple]:
        pass

    def fetchone(self):
        pass

    def close(self):
        pass


def validate(precision: Optional[int], scale: Optional[int]) -> bool:
    if precision is not None:
        if not (0 <= precision <= 38):
            return False
        if scale is not None and not (0 <= scale <= precision):
            return False
    elif scale is not None:
        return False
    return True


def sql_server_to_snowpark_type(schema: List[tuple]) -> StructType:
    """
    SQLServer to Python datatype mapping
    https://peps.python.org/pep-0249/#description returns the following spec
    name, type_code, display_size, internal_size, precision, scale, null_ok

    SQLServer supported types in Python (outdated):
    https://learn.microsoft.com/en-us/sql/machine-learning/python/python-libraries-and-data-types?view=sql-server-ver16
    """
    fields = []
    for column in schema:
        name, type_code, display_size, internal_size, precision, scale, null_ok = column
        snow_type = SQL_SERVER_TYPE_TO_SNOW_TYPE.get(type_code, None)
        if snow_type is None:
            raise NotImplementedError(f"sql server type not supported: {type_code}")
        if type_code in (int, decimal.Decimal):
            if not validate(precision, scale):
                _logger.debug(
                    f"Snowpark does not support column"
                    f" {name} of type {type_code} with precision {precision} and scale {scale}. "
                    "The default Numeric precision and scale will be used."
                )
                precision, scale = None, None
            data_type = snow_type(
                precision if precision is not None else 38,
                scale if scale is not None else 0,
            )
        else:
            data_type = snow_type()
        fields.append(StructField(name, data_type, null_ok))
    return StructType(fields)


def oracledb_to_snowpark_type(schema: List[Any]) -> StructType:
    """
    OracleDB to Python datatype mapping
    Oracle also follows PEP 249 spec, it returns a fetch info object containing the following memvar and more:
    name, type_code, display_size, internal_size, precision, scale, null_ok

    oracledb fetch info reference:
    https://python-oracledb.readthedocs.io/en/latest/api_manual/fetch_info.html#api-fetchinfo-objects

    oracledb supported types in Python:
    https://python-oracledb.readthedocs.io/en/latest/user_guide/appendix_a.html#supported-oracle-database-data-types
    # TODO: SNOW-1922043 Investigation on handling number type in oracle db
    """
    import oracledb

    convert_map_to_use = {
        oracledb.DB_TYPE_VARCHAR: StringType,
        oracledb.DB_TYPE_NVARCHAR: StringType,
        oracledb.DB_TYPE_NUMBER: DecimalType,
        oracledb.DB_TYPE_DATE: DateType,
        oracledb.DB_TYPE_BOOLEAN: BooleanType,
        oracledb.DB_TYPE_BINARY_DOUBLE: DoubleType,
        oracledb.DB_TYPE_BINARY_FLOAT: FloatType,
        oracledb.DB_TYPE_TIMESTAMP: TimestampType,
        oracledb.DB_TYPE_TIMESTAMP_TZ: TimestampType,
        oracledb.DB_TYPE_TIMESTAMP_LTZ: TimestampType,
        oracledb.DB_TYPE_INTERVAL_YM: VariantType,
        oracledb.DB_TYPE_INTERVAL_DS: VariantType,
        oracledb.DB_TYPE_RAW: BinaryType,
        oracledb.DB_TYPE_LONG: StringType,
        oracledb.DB_TYPE_LONG_RAW: BinaryType,
        oracledb.DB_TYPE_ROWID: StringType,
        oracledb.DB_TYPE_UROWID: StringType,
        oracledb.DB_TYPE_CHAR: StringType,
        oracledb.DB_TYPE_BLOB: BinaryType,
        oracledb.DB_TYPE_CLOB: StringType,
        oracledb.DB_TYPE_NCHAR: StringType,
        oracledb.DB_TYPE_NCLOB: StringType,
        oracledb.DB_TYPE_LONG_NVARCHAR: StringType,
        oracledb.DB_TYPE_BFILE: BinaryType,
        oracledb.DB_TYPE_JSON: VariantType,
        oracledb.DB_TYPE_BINARY_INTEGER: DecimalType,
        oracledb.DB_TYPE_XMLTYPE: StringType,
        oracledb.DB_TYPE_OBJECT: VariantType,
        oracledb.DB_TYPE_VECTOR: VectorType,
        oracledb.DB_TYPE_CURSOR: None,  # NOT SUPPORTED
    }

    fields = []
    for column in schema:
        name = column.name
        type_code = column.type_code
        precision = column.precision
        scale = column.scale
        null_ok = column.null_ok
        snow_type = convert_map_to_use.get(type_code, None)
        if snow_type is None:
            # TODO: SNOW-1912068 support types that we don't have now
            raise NotImplementedError(f"oracledb type not supported: {type_code}")
        if type_code == oracledb.DB_TYPE_TIMESTAMP_TZ:
            data_type = snow_type(TimestampTimeZone.TZ)
        elif type_code == oracledb.DB_TYPE_TIMESTAMP_LTZ:
            data_type = snow_type(TimestampTimeZone.LTZ)
        elif snow_type == DecimalType:
            if not validate(precision, scale):
                _logger.debug(
                    f"Snowpark does not support column"
                    f" {name} of type {type_code} with precision {precision} and scale {scale}. "
                    "The default Numeric precision and scale will be used."
                )
                precision, scale = None, None
            data_type = snow_type(
                precision if precision is not None else 38,
                scale if scale is not None else 0,
            )
        else:
            data_type = snow_type()
        fields.append(StructField(name, data_type, null_ok))

    return StructType(fields)


def infer_data_source_schema(
    conn: Connection, table: str, dbms_type: DBMS_TYPE, driver_info: str
) -> StructType:
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table} WHERE 1 = 0")
        raw_schema = cursor.description
        if dbms_type == DBMS_TYPE.SQL_SERVER_DB:
            return sql_server_to_snowpark_type(raw_schema)
        elif dbms_type == DBMS_TYPE.ORACLE_DB:
            return oracledb_to_snowpark_type(raw_schema)
        else:
            raise NotImplementedError(
                f"Failed to infer Snowpark DataFrame schema from source '{table}'. "
                f"Currently supported drivers are 'pyodbc' and 'oracledb', but got: '{driver_info}'. "
                "To avoid auto inference, you can manually specify the Snowpark DataFrame schema using 'custom_schema' in DataFrameReader.dbapi."
            )

    except Exception as exc:
        cursor.close()
        raise SnowparkDataframeReaderException(
            f"Failed to infer Snowpark DataFrame schema from table '{table}'."
            f" To avoid auto inference, you can manually specify the Snowpark DataFrame schema using 'custom_schema' in DataFrameReader.dbapi."
            f" Please check the stack trace for more details."
        ) from exc


def data_source_data_to_pandas_df(data: List[Any], schema: StructType) -> pd.DataFrame:
    columns = [col.name for col in schema.fields]
    df = pd.DataFrame.from_records(data, columns=columns)

    # convert timestamp and date to string to work around SNOW-1911989
    df = df.map(
        lambda x: x.isoformat()
        if isinstance(x, (datetime.datetime, datetime.date))
        else x
    )
    # convert binary type to object type to work around SNOW-1912094
    df = df.map(lambda x: x.hex() if isinstance(x, (bytearray, bytes)) else x)
    return df


def generate_select_query(
    table: str, schema: StructType, dbms: DBMS_TYPE, driver_info: str
) -> str:
    if dbms == DBMS_TYPE.ORACLE_DB:
        cols = []
        for field in schema.fields:
            if (
                isinstance(field.datatype, TimestampType)
                and field.datatype.tz == TimestampTimeZone.TZ
            ):
                cols.append(
                    f"""TO_CHAR({field.name}, 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM')"""
                )
            elif (
                isinstance(field.datatype, TimestampType)
                and field.datatype.tz == TimestampTimeZone.LTZ
            ):
                cols.append(
                    f"""TO_CHAR({field.name} AT TIME ZONE SESSIONTIMEZONE, 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM')"""
                )
            else:
                cols.append(field.name)
        return f"""select {" , ".join(cols)} from {table}"""
    else:
        return f"select * from {table}"


def generate_sql_with_predicates(select_query: str, predicates: List[str]):
    return [select_query + f" WHERE {predicate}" for predicate in predicates]


# move into oracle driver class in the future
def output_type_handler(cursor, metadata):
    import oracledb

    if metadata.type_code in (oracledb.DB_TYPE_CLOB, oracledb.DB_TYPE_NCLOB):
        return cursor.var(oracledb.DB_TYPE_LONG, arraysize=cursor.arraysize)
    elif metadata.type_code == oracledb.DB_TYPE_BLOB:
        return cursor.var(oracledb.DB_TYPE_RAW, arraysize=cursor.arraysize)
