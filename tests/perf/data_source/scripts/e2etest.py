#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import time
from copy import copy
from typing import Dict, List, Tuple
from datetime import datetime
import pandas as pd
import logging

from snowflake.snowpark import Session
from dbtest_config import (
    DatabaseTestConfig,
    create_sql_server_config,
    create_oracle_config,
    DEFAULT_PYSPARK_CONFIG,
    DEFAULT_ORACLE_JDBC_CONFIG,
    DEFAULT_SQLSERVER_JDBC_CONFIG,
)

from parameters import SNOWFLAKE_CONNECTION

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class PerformanceMetrics:
    def __init__(self) -> None:
        self.start_time = None
        self.read_time = 0
        self.write_time = 0
        self.total_time = 0


class DBPerformanceTest:
    def __init__(self, snowflake_connection: Dict) -> None:
        self.snowflake_connection = snowflake_connection
        self.results = []

    def time_operation(
        self, operation_name: str, func, *args, **kwargs
    ) -> Tuple[float, any]:
        """Execute operation and measure time"""
        dbapi_parameters = kwargs.pop("dbapi_parameters", {})
        start = time.time()
        result = func(*args, **kwargs, **dbapi_parameters)
        duration = time.time() - start
        logger.info(f"{operation_name} took {duration:.2f} seconds")
        return duration, result

    def get_table_size(self, source_db, table_name: str) -> int:
        """Get row count from existing table"""
        with source_db.connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            return cursor.fetchone()[0]

    def run_test_for_db(self, config: DatabaseTestConfig) -> List[Dict]:
        """Run performance tests for a specific database type"""
        print(
            f"Starting Database Test:\n"
            f"- Database Class: {config.db_class.__name__}\n"
            f"- Insert Row Count: {config.insert_row_count}\n"
            if config.insert_row_count
            else "" f"- Existing Table: {config.existing_table or 'None'}\n"
            if config.existing_table
            else "" f"- DBAPI Parameters: {config.dbapi_parameters or 'None'}"
        )
        test_results = []

        # Initialize source database
        source_db = config.db_class(**config.connection_params)

        try:
            # Determine table name and size
            if config.existing_table:
                table_name = config.existing_table
                size = self.get_table_size(source_db, table_name)
                logger.info(f"Using existing table {table_name} with {size} rows")
            else:
                size = config.insert_row_count
                table_name = f"{source_db.TABLE_NAME}_{size}"
                # Setup test data
                source_db.create_table(table_name=table_name, replace=True)
                source_db.insert_data(num_rows=size, table_name=table_name)

            logger.info(
                f"\nRunning test with {size} rows for {config.db_class.__name__}"
            )
            metrics = PerformanceMetrics()

            # Create Snowflake session
            session = Session.builder.configs(self.snowflake_connection).create()
            snowflake_table = f"{table_name}_PERF_TEST"

            try:
                # Measure read.dbapi() performance
                metrics.read_time, df = self.time_operation(
                    "read.dbapi",
                    session.read.dbapi,
                    source_db.create_connection,
                    table=table_name,
                    dbapi_parameters=config.dbapi_parameters,
                )

                # Measure write.save_as_table() performance
                metrics.write_time, _ = self.time_operation(
                    "write.save_as_table",
                    df.write.save_as_table,
                    snowflake_table,
                    mode="overwrite",
                )

                # Validate data
                self._validate_data(source_db, session, table_name, snowflake_table)

                # Calculate total time
                metrics.total_time = metrics.read_time + metrics.write_time

                # Store results
                test_results.append(
                    {
                        "timestamp": datetime.now(),
                        "database_type": config.db_class.__name__,
                        "dbapi_parameters": config.dbapi_parameters,
                        "data_size": size,
                        "fetch_size": config.dbapi_parameters.get("fetch_size", 0),
                        "num_partitions": config.dbapi_parameters.get(
                            "num_partitions", 0
                        ),
                        "read_time": metrics.read_time,
                        "write_time": metrics.write_time,
                        "total_time": metrics.total_time,
                    }
                )

            finally:
                session.close()

        finally:
            source_db.close_connection()

        return test_results

    def run_test_for_pyspark_jdbc(self, spark_config: Dict, jdbc_config: Dict) -> None:
        snowflake_table_name = "pyspark_dbapi_perf_test"
        snowflake_session = Session.builder.configs(self.snowflake_connection).create()
        snowflake_session.sql(f"drop table if exists {snowflake_table_name}").collect()
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("PySparkJDBCTest")
        for k, v in spark_config.items():
            spark = spark.config(k, v)
        spark = spark.getOrCreate()
        logger.info("Running PySpark JDBC performance test.")
        metrics = PerformanceMetrics()
        metrics.start_time = time.time()

        try:
            logger.info("Reading data from JDBC source.")
            df = spark.read.format("jdbc").options(**jdbc_config).load()
            metrics.read_time = time.time() - metrics.start_time
            # Writing data back to Snowflake
            logger.info("Writing data back to Snowflake.")
            write_start = time.time()
            (
                df.write.format("net.snowflake.spark.snowflake")
                .option("sfUrl", self.snowflake_connection["host"])
                .option("sfUser", self.snowflake_connection["user"])
                .option("sfPassword", self.snowflake_connection["password"])
                .option("sfDatabase", self.snowflake_connection["database"])
                .option("sfSchema", self.snowflake_connection["schema"])
                .option("sfWarehouse", self.snowflake_connection["warehouse"])
                .option("use_parquet_in_write", "true")
                .option("dbtable", snowflake_table_name)
                .mode("overwrite")
                .save()
            )
            metrics.write_time = time.time() - write_start
            metrics.total_time = time.time() - metrics.start_time

        except Exception as e:
            logger.error(f"Error during PySpark JDBC test: {e}")
            snowflake_session.sql(
                f"drop table if exists {snowflake_table_name}"
            ).collect()
            raise

        logger.info(f"Test completed in {metrics.total_time} seconds.")
        self.results.append(
            {
                "database_type": jdbc_config.get("driver"),
                "data_size": df.count(),
                "read_time": metrics.read_time,
                "write_time": metrics.write_time,
                "total_time": metrics.total_time,
            }
        )

    def _validate_data(
        self,
        source_db,
        session,
        source_table: str,
        target_table: str,
    ):
        """Validate data between source and Snowflake"""
        # Get count from source
        with source_db.connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {source_table}")
            assert cursor.fetchone()[0] == session.table(target_table).count()

    def save_results(self, filename: str = "performance_results.csv"):
        """Save test results to CSV"""
        df = pd.DataFrame(self.results)
        df.to_csv(filename, index=False)
        logger.info(f"\nResults saved to {filename}")


def pyspark_perf_test():
    # here the assumption is that source database is already set up
    perf_test = DBPerformanceTest(SNOWFLAKE_CONNECTION)

    oracle_jdbc_config = copy(DEFAULT_ORACLE_JDBC_CONFIG)
    oracle_jdbc_config[
        "dbtable"
    ] = "(SELECT ID,NUMBER_COL,BINARY_FLOAT_COL,BINARY_DOUBLE_COL,VARCHAR2_COL,CHAR_COL,CLOB_COL,NCHAR_COL,NVARCHAR2_COL,NCLOB_COL,DATE_COL,TIMESTAMP_COL,TIMESTAMP_TZ_COL,TO_CHAR(TIMESTAMP_LTZ_COL),BLOB_COL,RAW_COL,GUID_COL FROM ALL_TYPE_TABLE)"

    sqlserver_jdbc_config = copy(DEFAULT_SQLSERVER_JDBC_CONFIG)
    sqlserver_jdbc_config[
        "dbtable"
    ] = "(SELECT id,bigint_col,bit_col,decimal_col,float_col,int_col,money_col,real_col,smallint_col,smallmoney_col,tinyint_col,numeric_col,date_col,datetime2_col,datetime_col,smalldatetime_col,time_col,char_col,text_col,varchar_col,nchar_col,ntext_col,nvarchar_col,binary_col,varbinary_col,image_col,uniqueidentifier_col,xml_col,sysname_col FROM ALL_TYPE_TABLE) as tmp"

    perf_test.run_test_for_pyspark_jdbc(DEFAULT_PYSPARK_CONFIG, oracle_jdbc_config)

    perf_test.run_test_for_pyspark_jdbc(DEFAULT_PYSPARK_CONFIG, sqlserver_jdbc_config)

    # Save all results
    perf_test.save_results("pyspark_performance_results.csv")

    # Print summary
    print("\nTest Summary:")
    for result in perf_test.results:
        print(
            f"\nDatabase: {result['database_type']}, Size: {result['data_size']} rows:"
        )
        print(f"Read time: {result['read_time']:.2f} seconds")
        print(f"Write time: {result['write_time']:.2f} seconds")
        print(f"Total time: {result['total_time']:.2f} seconds")


def snowpark_perf_test():
    # Initialize and run tests
    perf_test = DBPerformanceTest(SNOWFLAKE_CONNECTION)

    # SAMPLE
    sql_sever_config = create_sql_server_config(insert_row_count=10000, fetch_size=1000)
    oracle_config = create_oracle_config(insert_row_count=10000, fetch_size=1000)

    # UPDATE THIS
    TEST_MATRIX = [sql_sever_config, oracle_config]

    # Run tests for each database type
    for db_config in TEST_MATRIX:
        results = perf_test.run_test_for_db(db_config)
        perf_test.results.extend(results)

    # Save all results
    perf_test.save_results()

    # Print summary
    print("\nTest Summary:")
    for result in perf_test.results:
        print(
            f"\nDatabase: {result['database_type']}, Size: {result['data_size']} rows:"
        )
        print(f"Read time: {result['read_time']:.2f} seconds")
        print(f"Write time: {result['write_time']:.2f} seconds")
        print(f"Total time: {result['total_time']:.2f} seconds")
        print(f"DBAPI parameters: {result['dbapi_parameters']}")


if __name__ == "__main__":
    pyspark_perf_test()
    snowpark_perf_test()
