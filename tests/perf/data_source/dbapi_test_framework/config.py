#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
Simple configuration for DBAPI ingestion tests.

Define your test matrix here by specifying:
- dbms: which database to test
- table: existing table name to read from
- ingestion_method: 'local', 'udtf', 'local_sproc', 'udtf_sproc'
- fetch_size, max_workers: DBAPI parameters
"""

import os

# Load environment variables from .env file in the same directory if it exists and dotenv is installed
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# Snowflake connection parameters
SNOWFLAKE_PARAMS = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "host": os.getenv("SNOWFLAKE_HOST"),
    "port": int(os.getenv("SNOWFLAKE_PORT", 443)),
    "protocol": os.getenv("SNOWFLAKE_PROTOCOL", "https"),
}

# Source database connection parameters
MYSQL_PARAMS = {
    "host": os.getenv("MYSQL_HOST"),
    "port": int(os.getenv("MYSQL_PORT", 3306)),
    "user": os.getenv("MYSQL_USERNAME"),  # Connection function expects 'user'
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE"),
}

POSTGRES_PARAMS = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "database": os.getenv("POSTGRES_DBNAME"),  # Connection function expects 'database'
}

MSSQL_PARAMS = {
    "host": os.getenv("MSSQL_SERVER"),
    "port": int(os.getenv("MSSQL_PORT", 1433)),
    "user": os.getenv("MSSQL_UID"),
    "password": os.getenv("MSSQL_PWD"),
    "database": os.getenv("MSSQL_DATABASE", "test_db"),  # Default to test_db
    "driver": os.getenv("MSSQL_DRIVER", "{ODBC Driver 18 for SQL Server}"),
}

ORACLE_PARAMS = {
    "host": os.getenv("ORACLEDB_HOST"),
    "port": int(os.getenv("ORACLEDB_PORT", 1521)),
    "user": os.getenv("ORACLEDB_USERNAME"),  # Connection function expects 'user'
    "password": os.getenv("ORACLEDB_PASSWORD"),
    "service_name": os.getenv("ORACLEDB_SERVICE_NAME"),
}

DATABRICKS_PARAMS = {
    "server_hostname": os.getenv("DATABRICKS_SERVER_HOSTNAME"),
    "http_path": os.getenv("DATABRICKS_HTTP_PATH"),
    "access_token": os.getenv("DATABRICKS_ACCESS_TOKEN"),
}

# DBAPI ingestion parameters
DBAPI_PARAMS = {}

# Cleanup configuration
# Set to False to keep target tables for debugging
CLEANUP_TARGET_TABLES = False

# Show target table info before cleanup (first row + count)
# Set to False to skip showing table info
SHOW_TARGET_TABLE_INFO = True

# Package requirements for stored procedures by DBMS type
SPROC_PACKAGES = {
    "mysql": ["pymysql"],
    "postgres": ["psycopg2"],
    "mssql": ["pyodbc", "msodbcsql"],
    "oracle": ["oracledb"],
    "databricks": ["databricks-sql-connector"],
}

# UDTF configuration (for udtf and udtf_sproc methods)
# Each DBMS needs its own external access integration
# Names match the existing test integrations in tests/resources/test_data_source_dir/
UDTF_CONFIGS = {
    "mysql": {
        "external_access_integration": "snowpark_dbapi_mysql_test_integration",
    },
    "postgres": {
        "external_access_integration": "snowpark_dbapi_postgres_test_integration",
    },
    "mssql": {
        "external_access_integration": "snowpark_dbapi_sql_server_test_integration",
    },
    "oracle": {
        "external_access_integration": "snowpark_dbapi_oracledb_test_integration",
    },
    "databricks": {
        "external_access_integration": "snowpark_dbapi_databricks_test_integration",
    },
}

# Test matrix - define which tests to run
# Each test config format:
# {
#     "dbms": "mysql",
#     "source": {"type": "table|query", "value": "..."},
#     "ingestion_method": "local|udtf|local_sproc|udtf_sproc"
# }
_DBMS_LIST = ["mysql", "postgres", "mssql", "oracle", "databricks"]
# _DBMS_LIST = ['mysql']
_METHODS = ["local", "udtf"]
_METHODS = ["local_sproc", "udtf_sproc"]

# Generate test matrix: table-based and query-based tests
TEST_MATRIX = [
    # Table-based tests
    *[
        {
            "dbms": dbms,
            "source": {"type": "table", "value": "DBAPI_TEST_TABLE"},
            "ingestion_method": method,
        }
        for dbms in _DBMS_LIST
        for method in _METHODS
    ],
    # Query-based tests
    *[
        {
            "dbms": dbms,
            "source": {"type": "query", "value": "SELECT * FROM DBAPI_TEST_TABLE"},
            "ingestion_method": method,
        }
        for dbms in _DBMS_LIST
        for method in _METHODS
    ],
]

# Simple single test config (used by main.py if TEST_MATRIX is not used)
SINGLE_TEST_CONFIG = {
    "dbms": "mssql",
    "source": {"type": "query", "value": "SELECT * FROM DBAPI_TEST_TABLE"},
    "ingestion_method": "local_sproc",
}
