#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import oracledb
import random
import datetime
import uuid

from base_db_setup import TestDBABC
from tests.perf.data_source.scripts.base_db_setup import ONE_MILLION


class TestOracleDB(TestDBABC):
    def __init__(
        self,
        username="SYSTEM",
        password="test",
        host="localhost",
        port=1521,
        service_name="FREEPDB1",
        table_name="ALL_TYPE_TABLE",
        insert_batch_size=10000,
    ) -> None:
        self.USERNAME = username
        self.PASSWORD = password
        self.HOST = host
        self.PORT = port
        self.SERVICE_NAME = service_name
        self.TABLE_NAME = table_name
        self.insert_batch_size = insert_batch_size
        self._connection = self.create_connection()

    def create_connection(self):
        dsn = f"{self.HOST}:{self.PORT}/{self.SERVICE_NAME}"
        connection = oracledb.connect(
            user=self.USERNAME, password=self.PASSWORD, dsn=dsn
        )
        return connection

    def create_table(self, table_name=None, replace=True):
        to_create_table = table_name or self.TABLE_NAME
        with self.connection.cursor() as cursor:
            if replace:
                try:
                    cursor.execute(f"DROP TABLE {to_create_table}")
                    print("Table dropped successfully.")
                except oracledb.DatabaseError:
                    pass
            cursor.execute(
                f"""
                CREATE TABLE {to_create_table} (
                    id NUMBER GENERATED AS IDENTITY PRIMARY KEY,
                    number_col NUMBER(10,2),
                    binary_float_col BINARY_FLOAT,
                    binary_double_col BINARY_DOUBLE,
                    varchar2_col VARCHAR2(50),
                    char_col CHAR(10),
                    clob_col CLOB,
                    nchar_col NCHAR(10),
                    nvarchar2_col NVARCHAR2(50),
                    nclob_col NCLOB,
                    date_col DATE,
                    timestamp_col TIMESTAMP,
                    timestamp_tz_col TIMESTAMP WITH TIME ZONE,
                    timestamp_ltz_col TIMESTAMP WITH LOCAL TIME ZONE,
                    blob_col BLOB,
                    raw_col RAW(16),
                    guid_col RAW(16) DEFAULT SYS_GUID()
                )
            """
            )
            print("Table created successfully.")

    @staticmethod
    def generate_random_data():
        return (
            round(random.uniform(1, 10000), 2),
            random.uniform(1, 10000),
            random.uniform(1, 10000),
            TestOracleDB.generate_random_string(50),
            TestOracleDB.generate_random_string(10).ljust(10),
            TestOracleDB.generate_random_string(1000),  # Simulating large CLOB text
            TestOracleDB.generate_random_string(10).ljust(10),
            TestOracleDB.generate_random_string(50),
            TestOracleDB.generate_random_string(1000),  # Simulating large NCLOB text
            datetime.datetime(
                2024, random.randint(1, 12), random.randint(1, 28)
            ).date(),
            datetime.datetime(
                2024,
                random.randint(1, 12),
                random.randint(1, 28),
                random.randint(0, 23),
                random.randint(0, 59),
            ),
            TestOracleDB.random_datetime_with_timezone(),
            TestOracleDB.random_datetime_with_timezone(),
            bytes(random.getrandbits(8) for _ in range(16)),
            uuid.uuid4().bytes,
        )

    def insert_data(self, num_rows=1_000_000, table_name=None):
        to_insert_table = table_name or self.TABLE_NAME
        # List of columns for better maintainability
        columns = [
            "number_col",
            "binary_float_col",
            "binary_double_col",
            "varchar2_col",
            "char_col",
            "clob_col",
            "nchar_col",
            "nvarchar2_col",
            "nclob_col",
            "date_col",
            "timestamp_col",
            "timestamp_tz_col",
            "timestamp_ltz_col",
            "blob_col",
            "raw_col",
        ]

        # Generate column names and placeholders dynamically
        column_list = ", ".join(columns)
        placeholders = ", ".join([f":{i + 1}" for i in range(len(columns))])

        # Use f-string formatting for clarity
        insert_sql = f"""
            INSERT INTO {to_insert_table} (
                {column_list}
            ) VALUES (
                {placeholders}
            )
        """

        self._insert_data_with_sql(insert_sql, num_rows)


if __name__ == "__main__":
    # for setup
    test = TestOracleDB()
    table_name = "ALL_TYPE_TABLE"
    test.create_table(table_name=table_name, replace=True)
    test.insert_data(ONE_MILLION, table_name=table_name)
    ret = (
        test.connection.cursor()
        .execute(f"select count(*) from {table_name}")
        .fetchall()
    )
    print(ret)
    test.close_connection()
