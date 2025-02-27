#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import logging
from enum import Enum
from typing import List, Any, Tuple, Protocol
from snowflake.connector.options import pandas as pd

from snowflake.snowpark.exceptions import SnowparkDataframeReaderException
from snowflake.snowpark.types import (
    StringType,
    GeographyType,
    VariantType,
    BinaryType,
    GeometryType,
    TimestampType,
    DecimalType,
    FloatType,
    ShortType,
    IntegerType,
    BooleanType,
    LongType,
    TimeType,
    ByteType,
    DateType,
    TimestampTimeZone,
    StructType,
    StructField,
)

_logger = logging.getLogger(__name__)

SQL_SERVER_TYPE_TO_SNOW_TYPE = {
    "bigint": LongType,
    "bit": BooleanType,
    "decimal": DecimalType,
    "float": FloatType,
    "int": IntegerType,
    "money": DecimalType,
    "real": FloatType,
    "smallint": ShortType,
    "smallmoney": DecimalType,
    "tinyint": ByteType,
    "numeric": DecimalType,
    "date": DateType,
    "datetime2": TimestampType,
    "datetime": TimestampType,
    "datetimeoffset": TimestampType,
    "smalldatetime": TimestampType,
    "time": TimeType,
    "timestamp": TimestampType,
    "char": StringType,
    "text": StringType,
    "varchar": StringType,
    "nchar": StringType,
    "ntext": StringType,
    "nvarchar": StringType,
    "binary": BinaryType,
    "varbinary": BinaryType,
    "image": BinaryType,
    "sql_variant": VariantType,
    "geography": GeographyType,
    "geometry": GeometryType,
    "uniqueidentifier": StringType,
    "xml": StringType,
    "sysname": StringType,
}
ORACLEDB_TYPE_TO_SNOW_TYPE = {
    "bfile": BinaryType,
    "varchar2": StringType,
    "varchar": StringType,
    "nvarchar2": StringType,
    # TODO: SNOW-1922043 Investigation on handling number type in oracle db
    "number": DecimalType,
    "float": FloatType,
    "long": LongType,
    "date": DateType,
    "intervalyeartomonth": StringType,
    "intervaldaytosecond": StringType,
    "json": VariantType,
    "binary_float": FloatType,
    "binary_double": FloatType,
    "timestamp": TimestampType,
    "longraw": BinaryType,
    "raw": BinaryType,
    "clob": StringType,
    "nclob": StringType,
    "blob": BinaryType,
    "char": StringType,
    "nchar": StringType,
    "rowid": StringType,
    "sys.anydata": VariantType,
    "uritype": VariantType,
    "urowid": StringType,
    "xmltype": VariantType,
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
        "cx_oracle": lambda conn: DBMS_TYPE.ORACLE_DB,
        "oracledb": lambda conn: DBMS_TYPE.ORACLE_DB,
        "sqlite3": lambda conn: DBMS_TYPE.SQLITE_DB,
    }

    if python_driver_name in dbms_mapping:
        return dbms_mapping[python_driver_name](dbapi2_conn), python_driver_name

    _logger.debug(f"Unsupported database driver: {python_driver_name}")
    return DBMS_TYPE.UNKNOWN, ""


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


def sql_server_to_snowpark_type(schema: List[tuple]) -> StructType:
    """
    This is used to convert sql server raw schema to snowpark structtype.
    Each tuple in the list represent a column and values are as follows:
    column name: str
    data type: str
    precision: int
    scale: int
    nullable: bool
    """
    fields = []
    for column in schema:
        snow_type = SQL_SERVER_TYPE_TO_SNOW_TYPE.get(column[1].lower(), None)
        if snow_type is None:
            # TODO: SNOW-1912068 support types that we don't have now
            raise NotImplementedError(f"sql server type not supported: {column[1]}")
        if column[1].lower() in ["datetime2", "datetime", "smalldatetime"]:
            data_type = snow_type(TimestampTimeZone.NTZ)
        elif column[1].lower() == "datetimeoffset":
            data_type = snow_type(TimestampTimeZone.LTZ)
        elif snow_type == DecimalType:
            data_type = snow_type(
                column[2] if column[2] is not None else 38,
                column[3] if column[3] is not None else 0,
            )
        else:
            data_type = snow_type()
        fields.append(StructField(column[0], data_type, column[4]))

    return StructType(fields)


def oracledb_to_snowpark_type(schema: List[tuple]) -> StructType:
    """
    This is used to convert oracledb raw schema to snowpark structtype.
    Each tuple in the list represent a column and values are as follows:
    column name: str
    data type: str
    precision: int
    scale: int
    nullable: str
    """
    fields = []
    for column in schema:
        remove_space_column_name = column[1].lower().replace(" ", "")
        processed_column_name = (
            "timestamp"
            if remove_space_column_name.startswith("timestamp")
            else remove_space_column_name
        )
        snow_type = ORACLEDB_TYPE_TO_SNOW_TYPE.get(processed_column_name, None)
        if snow_type is None:
            # TODO: SNOW-1912068 support types that we don't have now
            raise NotImplementedError(f"oracledb type not supported: {column[1]}")
        if "withtimezone" in remove_space_column_name:
            data_type = snow_type(TimestampTimeZone.TZ)
        elif "withlocaltimezone" in remove_space_column_name:
            data_type = snow_type(TimestampTimeZone.LTZ)
        elif snow_type == DecimalType:
            data_type = snow_type(
                column[2] if column[2] is not None else 38,
                column[3] if column[3] is not None else 0,
            )
        else:
            data_type = snow_type()
        fields.append(StructField(column[0], data_type, bool(column[4].lower() == "y")))

    return StructType(fields)


def infer_data_source_schema(
    conn: Connection, table: str, dbms_type: DBMS_TYPE, driver_info: str
) -> StructType:
    try:
        cursor = conn.cursor()
        if dbms_type == DBMS_TYPE.SQL_SERVER_DB:
            query = f"""
                    SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{table}'
                    """
            cursor.execute(query)
            raw_schema = cursor.fetchall()
            return sql_server_to_snowpark_type(raw_schema)
        elif dbms_type == DBMS_TYPE.ORACLE_DB:
            query = f"""
                    SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE, NULLABLE
                    FROM USER_TAB_COLUMNS
                    WHERE TABLE_NAME = '{table}'
                    """
            cursor.execute(query)
            raw_schema = cursor.fetchall()
            return oracledb_to_snowpark_type(raw_schema)
        else:
            raise NotImplementedError(
                f"Failed to infer Snowpark DataFrame schema from source '{table}'. "
                f"Currently supported drivers are 'pyodbc' and 'oracledb', but got: '{driver_info}'. "
                "To avoid auto inference, you can manually specify the Snowpark DataFrame schema using 'custom_schema' in DataFrameReader.dbapi."
            )

    except Exception as exc:
        raise SnowparkDataframeReaderException(
            f"Failed to infer Snowpark DataFrame schema from table '{table}'. To avoid auto inference, you can manually specify the Snowpark DataFrame schema using 'custom_schema' in DataFrameReader.dbapi."
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
