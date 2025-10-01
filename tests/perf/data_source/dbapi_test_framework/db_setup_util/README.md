# Database Setup Utilities

This directory contains scripts to set up identical test tables across multiple database systems for DBAPI ingestion testing.

## Overview

All scripts create a table named `DBAPI_TEST_TABLE` with 15 identical columns and insert 10,000 rows of deterministic test data.

### Table Schema

| Column Name    | Type              | Description                          |
|----------------|-------------------|--------------------------------------|
| id             | AUTO_INCREMENT PK | Primary key (auto-generated)         |
| int_col        | INTEGER           | Integer values                       |
| bigint_col     | BIGINT            | Large integer values                 |
| smallint_col   | SMALLINT          | Small integer values                 |
| float_col      | FLOAT/DOUBLE      | Floating point values                |
| decimal_col    | DECIMAL(10,2)     | Fixed precision decimal              |
| boolean_col    | BOOLEAN/BIT       | True/False values                    |
| varchar_col    | VARCHAR(100)      | Variable length strings              |
| char_col       | CHAR(10)          | Fixed length strings                 |
| text_col       | TEXT/CLOB         | Large text content                   |
| date_col       | DATE              | Date values                          |
| timestamp_col  | TIMESTAMP         | Timestamp values                     |
| binary_col     | BINARY(16)        | Binary data (16 bytes)               |
| json_col       | JSON/STRING       | JSON formatted strings               |
| uuid_col       | VARCHAR(36)       | UUID strings                         |

## Prerequisites

### Python Packages

Install required database drivers:

```bash
pip install pymysql                  # MySQL
pip install psycopg2-binary          # PostgreSQL  
pip install pyodbc                   # SQL Server
pip install oracledb                 # Oracle
pip install databricks-sql-connector # Databricks
pip install python-dotenv            # For .env support
```

### Environment Setup

Create a `.env` file in the `dbapi_test_framework/` directory with your database credentials:

```env
# MySQL
MYSQL_HOST=your-mysql-host
MYSQL_PORT=3306
MYSQL_USERNAME=your-username
MYSQL_PASSWORD=your-password
MYSQL_DATABASE=your-database

# PostgreSQL
POSTGRES_HOST=your-postgres-host
POSTGRES_PORT=5432
POSTGRES_USER=your-username
POSTGRES_PASSWORD=your-password
POSTGRES_DBNAME=your-database

# SQL Server
MSSQL_SERVER=your-sqlserver-host
MSSQL_PORT=1433
MSSQL_DATABASE=test_db
MSSQL_UID=your-username
MSSQL_PWD=your-password
MSSQL_DRIVER={ODBC Driver 18 for SQL Server}

# Oracle
ORACLEDB_HOST=your-oracle-host
ORACLEDB_PORT=1521
ORACLEDB_SERVICE_NAME=your-service-name
ORACLEDB_USERNAME=your-username
ORACLEDB_PASSWORD=your-password

# Databricks
DATABRICKS_SERVER_HOSTNAME=your-workspace.databricks.net
DATABRICKS_HTTP_PATH=sql/protocolv1/o/...
DATABRICKS_ACCESS_TOKEN=your-access-token
```

## Architecture

The setup utilities follow a clean, modular design:

- **`common_schema.py`**: Defines the unified schema, type mappings for all DBMS, and deterministic data generator
- **`base_setup.py`**: Base `DatabaseSetup` class with common logic (create table, insert data, etc.)
- **Individual setup scripts** (mysql_setup.py, etc.): Thin wrappers that handle connection and delegate to base class
- **`setup_all.py`**: Orchestrator to run all setups

This design eliminates code duplication and uses proper Python imports (no `sys.path` manipulation).

## Usage

### Setup Individual Database

Run as Python modules from the project root:

```bash
# From project root
python3 -m tests.perf.data_source.dbapi_test_framework.db_setup_util.mysql_setup
python3 -m tests.perf.data_source.dbapi_test_framework.db_setup_util.postgres_setup
python3 -m tests.perf.data_source.dbapi_test_framework.db_setup_util.oracle_setup
python3 -m tests.perf.data_source.dbapi_test_framework.db_setup_util.mssql_setup
python3 -m tests.perf.data_source.dbapi_test_framework.db_setup_util.databricks_setup
```

Or from the db_setup_util directory:

```bash
cd tests/perf/data_source/dbapi_test_framework/db_setup_util
python3 mysql_setup.py
# etc.
```

### Setup All Databases

Run all setups at once:

```bash
python3 -m tests.perf.data_source.dbapi_test_framework.db_setup_util.setup_all
```

This will attempt to set up all databases and provide a summary report.

## Data Determinism

All scripts use the same seed (42) to generate deterministic data. This means:
- Row #0 in MySQL has **exactly** the same values as row #0 in PostgreSQL, Oracle, etc.
- Row #1 in all databases has the same values
- And so on...

This is crucial for testing data consistency across different DBAPI implementations.

## Large Query Generation

For performance testing with larger datasets, see the `large_query_generation/` subdirectory.

These utilities generate SQL queries that multiply the 10k base table by factor k without storing additional data:
- k=100 → 1M rows
- k=1000 → 10M rows
- k=100000 → 1B rows

Pre-built templates available for: 100k, 1M, 10M, 100M, 1B, 10B rows.

See `large_query_generation/README.md` for details.

## Customization

### Change Number of Rows

Edit the script or modify `DEFAULT_ROWS` in `common_schema.py`:

```python
from db_setup_util.common_schema import DEFAULT_ROWS

# In individual script
insert_data(conn, num_rows=50000)  # Insert 50k rows instead
```

### Change Table Name

Modify `TABLE_NAME` in `common_schema.py`:

```python
TABLE_NAME = "MY_CUSTOM_TABLE"
```

## Verification

After running setup, verify the table:

```sql
-- Check row count
SELECT COUNT(*) FROM DBAPI_TEST_TABLE;
-- Should return: 10000

-- Sample data
SELECT * FROM DBAPI_TEST_TABLE LIMIT 5;
```

## Troubleshooting

### Connection Issues

- Verify `.env` file exists and has correct credentials
- Check network connectivity to database servers
- Ensure database drivers are installed

### Permission Issues

- Ensure user has CREATE TABLE and INSERT privileges
- For Oracle, may need additional tablespace permissions

### Data Type Issues

- Some databases (Oracle) use alternative types (NUMBER for boolean)
- Scripts handle these conversions automatically
