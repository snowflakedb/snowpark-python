#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import string
import time

import oracledb
import random
import datetime
import uuid

from pyspark.sql import SparkSession

from snowflake.snowpark import Session

# Oracle DB connection parameters



def create_table(conn):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE all_type_table_10m_row (
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
            )"""
        )
        print("Table all_type_table_1m_row created successfully.")


def insert_data(conn, num_rows=10000000):
    with conn.cursor() as cursor:
        for _ in range(num_rows):
            cursor.execute(
                """
                INSERT INTO ALL_TYPE_TABLE_10M_ROW (
                    number_col, binary_float_col, binary_double_col,
                    varchar2_col, char_col, clob_col, nchar_col, nvarchar2_col, nclob_col,
                    date_col, timestamp_col, timestamp_tz_col, timestamp_ltz_col,
                    blob_col, raw_col
                ) VALUES (
                    :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15
                )""",
                (
                    round(random.uniform(1, 10000), 2),
                    random.uniform(1, 10000),
                    random.uniform(1, 10000),
                    f"Sample{random.randint(1, 10000)}",
                    f"Char{random.randint(1, 10)}".ljust(10),
                    f"Large text {random.randint(1, 10000)}",
                    f"NChar{random.randint(1, 10)}".ljust(10),
                    f"NVarchar{random.randint(1, 10000)}",
                    f"Large Unicode text {random.randint(1, 10000)}",
                    datetime.datetime(
                        2024, random.randint(1, 12), random.randint(1, 28)
                    ),
                    datetime.datetime(
                        2024,
                        random.randint(1, 12),
                        random.randint(1, 28),
                        random.randint(0, 23),
                        random.randint(0, 59),
                    ),
                    datetime.datetime(
                        2024,
                        random.randint(1, 12),
                        random.randint(1, 28),
                        random.randint(0, 23),
                        random.randint(0, 59),
                    ),
                    datetime.datetime(
                        2024,
                        random.randint(1, 12),
                        random.randint(1, 28),
                        random.randint(0, 23),
                        random.randint(0, 59),
                    ),
                    bytes(random.randbytes(16)),
                    uuid.uuid4().bytes,
                ),
            )
        conn.commit()
        print(f"Inserted {num_rows} rows successfully.")


def random_data_table(conn):
    cursor = conn.cursor()
    cursor.execute(
        """
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE RANDOM_DATA_100COL_10M';
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;
    """
    )
    # Create table with 100 columns (50 numeric, 50 string)
    col = [f"num_col{i}" if i % 2 == 0 else f"str_col{i}" for i in range(1, 100)]

    column_definitions = [
        f"{c} NUMBER(10,2)" if i % 2 == 1 else f"{c} VARCHAR2(20)"
        for i, c in enumerate(col)
    ]
    sql_create_table = f"""
        CREATE TABLE RANDOM_DATA_100COL_10M (
            id NUMBER GENERATED AS IDENTITY PRIMARY KEY,
            {', '.join(column_definitions)}
        )
    """
    cursor.execute(sql_create_table)

    # Function to generate random data
    def generate_random_row():
        row = [
            round(random.uniform(1, 10000), 2)
            if i % 2 == 0
            else "".join(random.choices(string.ascii_letters, k=10))
            for i in range(1, 100)
        ]
        return row

    # Insert 1 million rows in batches
    batch_size = 100000
    insert_sql = f"""
        INSERT INTO RANDOM_DATA_100COL_10M ({', '.join(c for c in col)})
        VALUES ({', '.join([':' + str(i) for i in range(1, 100)])})
    """

    for _ in range(100):  # 100 batches of 10,000 rows each
        batch_data = [generate_random_row() for _ in range(batch_size)]
        cursor.executemany(insert_sql, batch_data)
        conn.commit()

    print("Table created and 10 million rows inserted successfully.")

    # Close connection
    cursor.close()


schema = """
SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE, NULLABLE
FROM USER_TAB_COLUMNS
WHERE table_name = 'ALL_TYPE_TABLE_1M_ROW'
"""
aaa = """select count(*) from RANDOM_DATA_100COL_10M"""


def dbapi_end_to_end():
    def create_connection():
        username = "SYSTEM"
        password = "Oracle123"
        # Create connection
        dsn = "localhost:1521/FREEPDB1"
        connection = oracledb.connect(user=username, password=password, dsn=dsn)
        return connection

    session = Session.builder.configs(CONNECTION_PARAMETERS).create()
    start = time.time()
    df = session.read.dbapi(create_connection, table="ALL_TYPE_TABLE_1M_ROW")
    df.write.save_as_table("ALL_TYPE_TABLE_1M_ROW")
    end = time.time()
    print("dbapi end to end ", end - start, " seconds")

    session.close()


def pyspark_end_to_end():
    spark = (
        SparkSession.builder.appName("Simple Application")
        .config(
            "spark.driver.extraClassPath",
            "/Users/yuwang/Downloads/parquet-avro-1.10.1.jar:/Users/yuwang/Downloads/spark-snowflake_2.12-3.1.0.jar:/Users/yuwang/Downloads/connector-3.1.0/snowflake-jdbc-3.19.0.jar:/Users/yuwang/Downloads/mssql-jdbc-12.8.1.jre11.jar:/Users/yuwang/Downloads/ojdbc11.jar",
        )
        .config("parquet.avro.write-old-list-structure", "false")
        .getOrCreate()
    )
    oracle_url = "jdbc:oracle:thin:@//localhost:1521/FREEPDB1"
    # oracle_properties = {
    #     "user": "SYSTEM",
    #     "password": "Oracle123",
    #     "driver": "oracle.jdbc.driver.OracleDriver",
    #     "fetchsize": "10000",
    # }
    timestamp1 = time.time()
    # df = spark.read.jdbc(
    #     url=oracle_url,
    #     table="RANDOM_DATA_100COL_1M",
    #     column="ID",
    #     numPartitions=10,
    #     lowerBound=0,
    #     upperBound=1000000,
    #     properties=oracle_properties
    # )
    df = (
        spark.read.format("jdbc")
        .option("url", oracle_url)
        .option("user", "SYSTEM")
        .option("password", "Oracle123")
        .option("driver", "oracle.jdbc.driver.OracleDriver")
        .option("fetchsize", "100000")
        .option("partitionColumn", "ID")
        .option("numPartitions", "10")
        .option("lowerBound", "0")
        .option("upperBound", "10000000")
        .option(
            "dbtable",
            "(SELECT ID,NUMBER_COL,BINARY_FLOAT_COL,BINARY_DOUBLE_COL,VARCHAR2_COL,CHAR_COL,CLOB_COL,NCHAR_COL,NVARCHAR2_COL,NCLOB_COL,DATE_COL,TIMESTAMP_COL,TIMESTAMP_TZ_COL,TO_CHAR(TIMESTAMP_LTZ_COL),BLOB_COL,RAW_COL,GUID_COL FROM ALL_TYPE_TABLE_10M_ROW)",
        )
        .load()
    )
    # df.collect()
    timestamp2 = time.time()
    print("external data source to local time: ", timestamp2 - timestamp1, " seconds")

    (
        df.write.format("net.snowflake.spark.snowflake")
        .option("sfUrl", "sfctest0.snowflakecomputing.com")
        .option("sfUser", CONNECTION_PARAMETERS["user"])
        .option("sfPassword", CONNECTION_PARAMETERS["password"])
        .option("sfDatabase", CONNECTION_PARAMETERS["database"])
        .option("sfSchema", CONNECTION_PARAMETERS["schema"])
        .option("sfWarehouse", CONNECTION_PARAMETERS["warehouse"])
        .option("use_parquet_in_write", "true")
        .option("dbtable", "pyspark_dbapi_perf_test")
        .mode("overwrite")
        .save()
    )
    timestamp3 = time.time()
    print("local to snowflake time: ", timestamp3 - timestamp2, " seconds")

    print("end to end time: ", timestamp3 - timestamp1, " seconds")


pyspark_end_to_end()
