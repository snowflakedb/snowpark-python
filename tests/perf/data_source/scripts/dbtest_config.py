#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import Dict, Type

from tests.perf.data_source.scripts.oracle_resource_setup import TestOracleDB
from tests.perf.data_source.scripts.sql_server_resource_setup import TestSQLServerDB


class DatabaseTestConfig:
    def __init__(
        self,
        db_class: Type,
        connection_params: Dict = None,
        insert_row_count: int = None,
        existing_table: str = None,
        dbapi_parameters: Dict = None,
    ) -> None:
        if insert_row_count and existing_table:
            raise ValueError(
                "insert_row_count and existing_table can not be used at the same time,"
                "when insert_row_count, a new table will be created"
            )
        self.db_class = db_class
        self.connection_params = connection_params or {}
        self.insert_row_count = insert_row_count or 1_000_000
        self.existing_table = existing_table
        self.dbapi_parameters = dbapi_parameters or {}


def create_oracle_config(
    connection_params: Dict = None,
    insert_row_count: int = None,
    existing_table: str = None,
    fetch_size: int = None,
) -> DatabaseTestConfig:
    """
    Helper method to create Oracle test configuration with default values.

    Args:
        connection_params: Optional connection parameters, will use defaults if not provided
        insert_row_count: Number of rows to insert if creating new table
        existing_table: Name of existing table to use
        fetch_size: DBAPI fetch_size parameter
    """
    default_connection = {
        "username": "SYSTEM",
        "password": "test",
        "host": "localhost",
        "port": 1521,
        "service_name": "FREEPDB1",
    }

    config = DatabaseTestConfig(
        db_class=TestOracleDB,
        connection_params=connection_params or default_connection,
        insert_row_count=insert_row_count,
        existing_table=existing_table,
        dbapi_parameters={"fetch_size": fetch_size} if fetch_size else None,
    )
    return config


def create_sql_server_config(
    connection_params: Dict = None,
    insert_row_count: int = None,
    existing_table: str = None,
    fetch_size: int = None,
) -> DatabaseTestConfig:
    """
    Helper method to create SQL Server test configuration with default values.

    Args:
        connection_params: Optional connection parameters, will use defaults if not provided
        insert_row_count: Number of rows to insert if creating new table
        existing_table: Name of existing table to use
        fetch_size: DBAPI fetch_size parameter
    """
    if existing_table and insert_row_count:
        raise ValueError(
            "existing_table and insert_row_count can not be used at the same time,"
            "when insert_row_count, a new table will be created"
        )
    default_connection = {
        "host": "127.0.0.1",
        "port": 1433,
        "database": "msdb",
        "username": "sa",
        "password": "Test12345()",
    }

    config = DatabaseTestConfig(
        db_class=TestSQLServerDB,
        connection_params=connection_params or default_connection,
        insert_row_count=insert_row_count,
        existing_table=existing_table,
        dbapi_parameters={"fetch_size": fetch_size or 0},
    )
    return config


def create_pyspark_session_config(driver_extra_class_path, master="local", **kwargs):
    """
    class_path is dir where the java jar, snowflake jar, parquet avro target db driver jar is placed
    """
    return {
        "spark.driver.extraClassPath": str(driver_extra_class_path),
        "spark.master": master,
        **kwargs,
    }


def create_jdbc_config(
    jdbc_url,
    user,
    password,
    driver,
    fetch_size=None,
    partition_column=None,
    num_partitions=None,
    lower_bound=None,
    upper_bound=None,
):
    config = {
        "url": jdbc_url,
        "user": user,
        "password": password,
        "driver": driver,
    }
    if fetch_size:
        config["fetchsize"] = fetch_size
    if partition_column:
        config["partitionColumn"] = partition_column
    if num_partitions:
        config["numPartitions"] = num_partitions
    if lower_bound:
        config["lowerBound"] = lower_bound
    if upper_bound:
        config["upperBound"] = upper_bound
    return config


DEFAULT_ORACLE_CONFIGS = [
    create_oracle_config(existing_table="ALL_TYPE_TABLE", fetch_size=fetch_size)
    for fetch_size in [1000, 10000, 10000]
]
DEFAULT_SQLSERVER_CONFIGS = [
    create_sql_server_config(existing_table="ALL_TYPE_TABLE", fetch_size=fetch_size)
    for fetch_size in [1000, 10000, 10000]
]


DEFAULT_PYSPARK_CONFIG = create_pyspark_session_config(
    driver_extra_class_path="./jdbc_drivers/*"
)

DEFAULT_ORACLE_JDBC_CONFIG = create_jdbc_config(
    jdbc_url="jdbc:oracle:thin:@//localhost:1521/FREEPDB1",
    user="SYSTEM",
    password="test",
    driver="oracle.jdbc.driver.OracleDriver",
)

DEFAULT_SQLSERVER_JDBC_CONFIG = create_jdbc_config(
    jdbc_url="jdbc:sqlserver://127.0.0.1:1433;TrustServerCertificate=true;databaseName=msdb",
    user="sa",
    password="Test12345()",
    driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",
)
