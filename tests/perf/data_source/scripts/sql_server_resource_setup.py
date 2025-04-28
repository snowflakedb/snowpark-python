#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

# UNSUPPORTED TYPES in pyodbc:
# 1. datetimeoffset_col DATETIMEOFFSET
# 2. geography_col GEOGRAPHY
# 3. geometry_col GEOMETRY
# workaround: https://github.com/mkleehammer/pyodbc/wiki/Using-an-Output-Converter-function
# TODO: SNOW-1945100

import pyodbc
import random
import datetime

from base_db_setup import TestDBABC, ONE_MILLION


class TestSQLServerDB(TestDBABC):
    def __init__(
        self,
        *,
        host="127.0.0.1",
        port=1433,
        database="msdb",
        username="sa",
        password="Test12345()",
        table_name="ALL_TYPE_TABLE",
        insert_batch_size=10000,
    ) -> None:
        self.HOST = host
        self.PORT = port
        self.DATABASE = database
        self.USERNAME = username
        self.PASSWORD = password
        self.TABLE_NAME = table_name
        self.insert_batch_size = insert_batch_size or 10000
        self._connection = self.create_connection()

    def create_connection(self):
        connection_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={self.HOST},{self.PORT};"
            f"DATABASE={self.DATABASE};"
            f"UID={self.USERNAME};"
            f"PWD={self.PASSWORD};"
            "TrustServerCertificate=yes"
        )
        connection = pyodbc.connect(connection_str)
        return connection

    def create_table(self, table_name=None, replace=True):
        to_create_table = table_name or self.TABLE_NAME
        with self.connection.cursor() as cursor:
            if replace:
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {to_create_table}")
                    print("Table dropped successfully.")
                except pyodbc.DatabaseError:
                    pass
            cursor.execute(
                f"""
                CREATE TABLE {to_create_table} (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    bigint_col BIGINT,
                    bit_col BIT,
                    decimal_col DECIMAL(18, 2),
                    float_col FLOAT,
                    int_col INT,
                    money_col MONEY,
                    real_col REAL,
                    smallint_col SMALLINT,
                    smallmoney_col SMALLMONEY,
                    tinyint_col TINYINT,
                    numeric_col NUMERIC(18, 2),
                    date_col DATE,
                    datetime2_col DATETIME2,
                    datetime_col DATETIME,
                    smalldatetime_col SMALLDATETIME,
--                     datetimeoffset_col DATETIMEOFFSET,
                    time_col TIME,
                    char_col CHAR(10),
                    text_col TEXT,
                    varchar_col VARCHAR(50),
                    nchar_col NCHAR(10),
                    ntext_col NTEXT,
                    nvarchar_col NVARCHAR(50),
                    binary_col BINARY(16),
                    varbinary_col VARBINARY(16),
                    image_col IMAGE,
                    sql_variant_col SQL_VARIANT,
--                     geography_col GEOGRAPHY,
--                     geometry_col GEOMETRY,
                    uniqueidentifier_col UNIQUEIDENTIFIER DEFAULT NEWID(),
                    xml_col XML,
                    sysname_col SYSNAME
                )
            """
            )
            print("Table created successfully.")

    @staticmethod
    def generate_random_data():
        return (
            random.randint(1, 1e18),  # bigint
            random.choice([0, 1]),  # bit
            round(random.uniform(1, 10000), 2),  # decimal
            random.uniform(1, 10000),  # float
            random.randint(1, 10000),  # int
            round(random.uniform(1, 10000), 2),  # money
            random.uniform(1, 10000),  # real
            random.randint(1, 32767),  # smallint
            round(random.uniform(1, 1000), 2),  # smallmoney
            random.randint(0, 255),  # tinyint
            round(random.uniform(1, 10000), 2),  # numeric
            datetime.date.today(),  # date
            datetime.datetime.now(),  # datetime2
            datetime.datetime.now(),  # datetime
            # datetime.datetime.now(datetime.timezone.utc),  # datetimeoffset
            datetime.datetime.now(),  # smalldatetime
            datetime.datetime.now().time(),  # time
            TestSQLServerDB.generate_random_string(10).ljust(10),  # char
            TestSQLServerDB.generate_random_string(1000),  # text
            TestSQLServerDB.generate_random_string(50),  # varchar
            TestSQLServerDB.generate_random_string(10).ljust(10),  # nchar
            TestSQLServerDB.generate_random_string(1000),  # ntext
            TestSQLServerDB.generate_random_string(50),  # nvarchar
            bytes(random.getrandbits(8) for _ in range(16)),  # binary
            bytes(random.getrandbits(8) for _ in range(16)),  # varbinary
            bytes(random.getrandbits(8) for _ in range(16)),  # image
            bytes(
                random.getrandbits(8) for _ in range(16)
            ),  # sql_variant (using uniqueidentifier as a mock variant)
            # 'POINT(1 1)',  # geography (mock string for simplicity)
            # 'POINT(1 1)',  # geometry (mock string for simplicity)
            bytes(random.getrandbits(8) for _ in range(16)),  # uniqueidentifier
            "<root><element>Test</element></root>",  # xml
            "sysname_test",  # sysname
        )

    def insert_null_data(self, num_rows=1, table_name=None):
        to_insert_table = table_name or self.TABLE_NAME
        # Define the column names as a list for better maintainability
        columns = [
            "bigint_col",
            "bit_col",
            "decimal_col",
            "float_col",
            "int_col",
            "money_col",
            "real_col",
            "smallint_col",
            "smallmoney_col",
            "tinyint_col",
            "numeric_col",
            "date_col",
            "datetime2_col",
            "datetime_col",
            "smalldatetime_col",
            # "datetimeoffset_col",
            "time_col",
            "char_col",
            "text_col",
            "varchar_col",
            "nchar_col",
            "ntext_col",
            "nvarchar_col",
            "binary_col",
            "varbinary_col",
            "image_col",
            "sql_variant_col",
            # "geography_col",
            # "geometry_col",
            "uniqueidentifier_col",
            "xml_col",
            "sysname_col",
        ]

        # Dynamically construct the column string and placeholders
        column_str = ", ".join(columns)
        placeholders = ", ".join(["?"] * len(columns))

        # Construct the SQL statement dynamically
        insert_sql = f"""
            INSERT INTO {to_insert_table} (
                {column_str}
            ) VALUES ({placeholders})
        """
        with self.connection.cursor() as cursor:
            for _ in range(num_rows):
                # Generate a tuple with None for each column
                null_data = [None] * (len(columns) - 1)
                null_data.append("sysname_test")  # sysname_col does not allow null
                cursor.execute(insert_sql, tuple(null_data))
            self.connection.commit()
            print(f"Inserted {num_rows} rows with NULL values successfully.")

    def insert_data(self, num_rows=1_000_000, table_name=None):
        to_insert_table = table_name or self.TABLE_NAME
        # Define the column names as a list for better maintainability
        columns = [
            "bigint_col",
            "bit_col",
            "decimal_col",
            "float_col",
            "int_col",
            "money_col",
            "real_col",
            "smallint_col",
            "smallmoney_col",
            "tinyint_col",
            "numeric_col",
            "date_col",
            "datetime2_col",
            "datetime_col",
            "smalldatetime_col",
            # "datetimeoffset_col",
            "time_col",
            "char_col",
            "text_col",
            "varchar_col",
            "nchar_col",
            "ntext_col",
            "nvarchar_col",
            "binary_col",
            "varbinary_col",
            "image_col",
            "sql_variant_col",
            # "geography_col",
            # "geometry_col",
            "uniqueidentifier_col",
            "xml_col",
            "sysname_col",
        ]

        # Dynamically construct the column string and placeholders
        column_str = ", ".join(columns)
        placeholders = ", ".join(["?"] * len(columns))

        # Construct the SQL statement dynamically
        insert_sql = f"""
            INSERT INTO {to_insert_table} (
                {column_str}
            ) VALUES ({placeholders})
        """
        self._insert_data_with_sql(insert_sql, num_rows)


if __name__ == "__main__":
    # for setup
    test = TestSQLServerDB()
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
