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

# Snowflake connection parameters
SNOWFLAKE_PARAMS = {}

# Source database connection parameters
MYSQL_PARAMS = {}

POSTGRES_PARAMS = {}

MSSQL_PARAMS = {}

ORACLE_PARAMS = {}

DATABRICKS_PARAMS = {}

# DBAPI ingestion parameters
DBAPI_PARAMS = {}

# UDTF configuration (for udtf and udtf_sproc methods)
# Each DBMS needs its own external access integration
UDTF_CONFIGS = {
    "mysql": {
        "external_access_integration": "MYSQL_EXTERNAL_ACCESS_INTEGRATION",
    },
    "postgres": {
        "external_access_integration": "POSTGRES_EXTERNAL_ACCESS_INTEGRATION",
    },
    "mssql": {
        "external_access_integration": "MSSQL_EXTERNAL_ACCESS_INTEGRATION",
    },
    "oracle": {
        "external_access_integration": "ORACLE_EXTERNAL_ACCESS_INTEGRATION",
    },
    "databricks": {
        "external_access_integration": "DATABRICKS_EXTERNAL_ACCESS_INTEGRATION",
    },
}

# Test matrix - define which tests to run
# Each test config is a dict with: dbms, table, ingestion_method, and optional params
TEST_MATRIX = [
    # MySQL tests
    {
        "dbms": "mysql",
        "table": "test_table",
        "ingestion_method": "local",
    },
    {
        "dbms": "mysql",
        "table": "test_table",
        "ingestion_method": "udtf",
    },
    # Postgres tests
    {
        "dbms": "postgres",
        "table": "test_table",
        "ingestion_method": "local",
    },
    # Add more test configurations as needed
]

# Simple single test config (used by main.py if TEST_MATRIX is not used)
SINGLE_TEST_CONFIG = {
    "dbms": "mysql",
    "table": "test_table",
    "ingestion_method": "local",
}
