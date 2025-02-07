#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import datetime
from typing import List, Any, Tuple, Protocol, Union
from snowflake.connector.options import pandas as pd

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
    "number": DecimalType,
    "float": FloatType,
    "long": LongType,
    "date": DateType,
    "binary_float": FloatType,
    "binary_double": DoubleType,
    "timestamp": TimestampType,
    "raw": BinaryType,
    # TODO: test if longraw can be convert to binary
    "longraw": BinaryType,
    "clob": StringType,
    "nclob": StringType,
    "blob": BinaryType,
    "char": StringType,
    "nchar": StringType,
}


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
            data_type = snow_type(column[2], column[3])
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
        elif remove_space_column_name.startswith("timestamp"):
            data_type = snow_type(TimestampTimeZone.NTZ)
        elif snow_type == DecimalType:
            data_type = snow_type(column[2], column[3])
        else:
            data_type = snow_type()
        fields.append(
            StructField(
                column[0], data_type, True if column[4].lower() == "y" else False
            )
        )

    return StructType(fields)


def infer_data_source_schema(
    conn: Connection, table: str, connection_type: str
) -> Tuple[StructType, List[Tuple[str, Any, int, int, Union[str, bool]]]]:
    # TODO: SNOW-1893781 change implementation if oracle has different way of infer schema
    cursor = conn.cursor()
    if "pyodbc" == connection_type.lower():
        query = f"""
                SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{table}'
                """
        cursor.execute(query)
        raw_schema = cursor.fetchall()
        return sql_server_to_snowpark_type(raw_schema), raw_schema
    elif "oracledb" == connection_type.lower():
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
            f"currently supported drivers are pyodbc and oracledb, got: {connection_type}"
        )


def data_source_data_to_pandas_df(
    data: List[Any], schema: Tuple[Any], connection_type: str
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
    if "pyodbc" == connection_type.lower():
        return df
    elif "oracledb" == connection_type.lower():
        clob_data = []
        for i, col in enumerate(schema):
            if col[1].lower() in ["clob", "nclob"]:
                clob_data.append(i)
        for column in clob_data:
            df[column] = df[column].apply(lambda x: x.read())
    else:
        raise NotImplementedError(
            f"currently supported drivers are pyodbc and oracledb, got: {connection_type}"
        )
    return df
