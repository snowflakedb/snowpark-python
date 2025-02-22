#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import time
from typing import Dict, List, Tuple
from datetime import datetime
import pandas as pd
import logging

from snowflake.snowpark import Session
from parameters import SNOWFLAKE_CONNECTION
from dbtest_config import (
    DatabaseTestConfig,
    create_sql_server_config,
    create_oracle_config,
)

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
        self.source_count = 0
        self.target_count = 0
        self.validation_passed = False


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
                    table_name,
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
                metrics = self._validate_data(
                    source_db, session, table_name, snowflake_table, metrics
                )

                # Calculate total time
                metrics.total_time = metrics.read_time + metrics.write_time

                # Store results
                test_results.append(
                    {
                        "timestamp": datetime.now(),
                        "database_type": config.db_class.__name__,
                        "dbapi_parameters": config.dbapi_parameters,
                        "data_size": size,
                        "read_time": metrics.read_time,
                        "write_time": metrics.write_time,
                        "total_time": metrics.total_time,
                        "source_count": metrics.source_count,
                        "target_count": metrics.target_count,
                        "validation_passed": metrics.validation_passed,
                    }
                )

            finally:
                session.close()

        finally:
            source_db.close_connection()

        return test_results

    def _validate_data(
        self,
        source_db,
        session,
        source_table: str,
        target_table: str,
        metrics: PerformanceMetrics,
    ) -> PerformanceMetrics:
        """Validate data between source and Snowflake"""
        # Get count from source
        with source_db.connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {source_table}")
            metrics.source_count = cursor.fetchone()[0]

        # Get count from Snowflake
        metrics.target_count = session.table(target_table).count()

        # Validate row counts
        metrics.validation_passed = metrics.source_count == metrics.target_count
        logger.info(f"Data validation - Row counts match: {metrics.validation_passed}")
        logger.info(
            f"Source count: {metrics.source_count}, Snowflake count: {metrics.target_count}"
        )

        return metrics

    def save_results(self, filename: str = "performance_results.csv"):
        """Save test results to CSV"""
        df = pd.DataFrame(self.results)
        df.to_csv(filename, index=False)
        logger.info(f"\nResults saved to {filename}")


if __name__ == "__main__":
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
        print(f"Data validation: {'✓' if result['validation_passed'] else '✗'}")
        print(f"DBAPI parameters: {result['dbapi_parameters']}")
