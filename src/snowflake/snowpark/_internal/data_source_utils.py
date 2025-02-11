#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import logging
from enum import Enum
from typing import List, Any, Tuple, Protocol, Union
from snowflake.connector.options import pandas as pd
from dateutil import parser

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
    DoubleType,
)

_logger = logging.getLogger("snowflake.snowpark")

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
    "varchar2": StringType,
    "varchar": StringType,
    "nvarchar2": StringType,
    # TODO: SNOW-1922043 Investigation on handling number type in oracle db
    "number": DecimalType,
    "float": FloatType,
    "long": LongType,
    "date": TimestampType,
    "binary_float": FloatType,
    "binary_double": DoubleType,
    "timestamp": TimestampType,
    "raw": BinaryType,
    "clob": StringType,
    "nclob": StringType,
    "blob": BinaryType,
    "char": StringType,
    "nchar": StringType,
}


STATEMENT_PARAMS_DATA_SOURCE = "SNOWPARK_PYTHON_DATASOURCE"
DATA_SOURCE_DBAPI_SIGNATURE = "DataFrameReader.dbapi"
DATA_SOURCE_SQL_COMMENT = (
    f"/* Python:snowflake.snowpark.{DATA_SOURCE_DBAPI_SIGNATURE} */"
)


class DBMS_TYPE(Enum):
    SQL_SERVER_DB = "SQL_SERVER_DB"
    ORACLE_DB = "ORACLE_DB"


def detect_dbms(dbapi2_conn):
    """Detects the DBMS type from a DBAPI2 connection."""

    # Get the Python driver name
    python_driver_name = type(dbapi2_conn).__module__.lower()

    # Dictionary-based lookup for known DBMS
    dbms_mapping = {
        "pyodbc": detect_dbms_pyodbc,
        "cx_oracle": lambda conn: DBMS_TYPE.ORACLE_DB,
        "oracledb": lambda conn: DBMS_TYPE.ORACLE_DB,
    }

    if python_driver_name in dbms_mapping:
        return dbms_mapping[python_driver_name](dbapi2_conn)

    _logger.debug(f"Unsupported database driver: {python_driver_name}")
    return None


def detect_dbms_pyodbc(dbapi2_conn):
    """Detects the DBMS type for a pyodbc connection."""
    import pyodbc

    dbms_name = dbapi2_conn.getinfo(pyodbc.SQL_DBMS_NAME).lower()

    # Set-based lookup for SQL Server
    sqlserver_keywords = {"sql server", "mssql", "sqlserver"}
    if any(keyword in dbms_name for keyword in sqlserver_keywords):
        return DBMS_TYPE.SQL_SERVER_DB

    _logger.debug(f"Unsupported DBMS for pyodbc: {dbms_name}")
    return None


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
            raise NotImplementedError(f"sql server type not supported: {column[1]}")
        if "withtimezone" in remove_space_column_name:
            data_type = snow_type(TimestampTimeZone.TZ)
        elif "withlocaltimezone" in remove_space_column_name:
            data_type = snow_type(TimestampTimeZone.LTZ)
        elif (
            remove_space_column_name.startswith("timestamp")
            or remove_space_column_name == "date"
        ):
            data_type = snow_type(TimestampTimeZone.NTZ)
        elif snow_type == DecimalType:
            data_type = snow_type(
                column[2] if column[2] is not None else 38,
                column[3] if column[3] is not None else 0,
            )
        else:
            data_type = snow_type()
        fields.append(
            StructField(
                column[0], data_type, True if column[4].lower() == "y" else False
            )
        )

    return StructType(fields)


def infer_data_source_schema(
    conn: Connection, table: str
) -> Tuple[StructType, List[Tuple[str, Any, int, int, Union[str, bool]]]]:
    try:
        cursor = conn.cursor()
        if detect_dbms(conn) == DBMS_TYPE.SQL_SERVER_DB:
            query = f"""
                    SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{table}'
                    """
            cursor.execute(query)
            raw_schema = cursor.fetchall()
            return sql_server_to_snowpark_type(raw_schema), raw_schema
        elif detect_dbms(conn) == DBMS_TYPE.ORACLE_DB:
            query = f"""
                    SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE, NULLABLE
                    FROM USER_TAB_COLUMNS
                    WHERE TABLE_NAME = '{table}'
                    """
            cursor.execute(query)
            raw_schema = cursor.fetchall()
            return oracledb_to_snowpark_type(raw_schema), raw_schema
        else:
            raise NotImplementedError(
                f"currently supported drivers are pyodbc and oracledb, got: {detect_dbms(conn)}"
            )
    except Exception as exc:
        raise SnowparkDataframeReaderException(
            f"Unable to infer schema from table {table}"
        ) from exc


def data_source_data_to_pandas_df(
    data: List[Any], schema: Tuple[Any], conn: Connection
) -> pd.DataFrame:
    columns = [col[0] for col in schema]
    df = pd.DataFrame.from_records(data, columns=columns)

    # convert timestamp and date to string to work around SNOW-1911989
    df = df.map(
        lambda x: x.isoformat()
        if isinstance(x, (datetime.datetime, datetime.date))
        else x
    )
    # convert binary type to object type to work around SNOW-1912094
    df = df.map(lambda x: x.hex() if isinstance(x, (bytearray, bytes)) else x)
    if detect_dbms(conn) == DBMS_TYPE.SQL_SERVER_DB:
        return df
    elif detect_dbms(conn) == DBMS_TYPE.ORACLE_DB:
        clob_data = []
        tz_data = []
        for col in schema:
            if col[1].lower() in ["clob", "nclob"]:
                clob_data.append(col[0])
            if "time zone" in col[1].lower():
                tz_data.append(col[0])
        for column in clob_data:
            df[column] = df[column].apply(lambda x: x.read())
        for column in tz_data:
            df[column] = df[column].apply(lambda x: parser.parse(x))

    else:
        raise NotImplementedError(
            f"currently supported drivers are pyodbc and oracledb, got: {detect_dbms(conn)}"
        )
    return df


def generate_select_query(table: str, schema: StructType, connection_type: str) -> str:
    if "pyodbc" == connection_type.lower():
        return f"select * from {table}"
    elif "oracledb" == connection_type.lower():
        cols = []
        for field in schema.fields:
            if (
                isinstance(field.datatype, TimestampType)
                and field.datatype.tz == TimestampTimeZone.TZ
            ):
                cols.append(
                    f"""TO_CHAR({field.name}, 'YYYY-MM-DD"T"HH24:MI:SS.FF6 TZH:TZM')"""
                )
            elif (
                isinstance(field.datatype, TimestampType)
                and field.datatype.tz == TimestampTimeZone.LTZ
            ):
                cols.append(
                    f"""TO_CHAR({field.name} AT TIME ZONE SESSIONTIMEZONE, 'YYYY-MM-DD"T"HH24:MI:SS.FF6 TZH:TZM')"""
                )
            else:
                cols.append(field.name)
        return f"""select {" , ".join(cols)} from {table}"""
    else:
        raise NotImplementedError(
            f"currently supported drivers are pyodbc and oracledb, got: {connection_type}"
        )
