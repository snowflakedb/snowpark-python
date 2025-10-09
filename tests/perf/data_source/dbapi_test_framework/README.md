# DBAPI Test Framework

Comprehensive framework for testing DBAPI ingestion performance across different databases and ingestion methods.

## Structure

```
dbapi_test_framework/
├── config.py              # Configuration (loads from .env, test matrix)
├── connections.py         # Connection factories for each DBMS
├── runner.py              # Test runner with 4 ingestion methods
├── main.py                # Entry point
├── .env                   # Environment variables (credentials - not in git)
└── db_setup_util/         # Database setup scripts
    ├── common_schema.py   # Unified schema + data generator
    ├── base_setup.py      # Base setup class
    ├── mysql_setup.py     # MySQL table setup
    ├── postgres_setup.py  # PostgreSQL table setup
    ├── oracle_setup.py    # Oracle table setup
    ├── mssql_setup.py     # SQL Server table setup
    ├── databricks_setup.py # Databricks table setup
    └── setup_all.py       # Run all setups at once
```

## Quick Start

### 1. Set Up Environment Variables

Create a `.env` file in the `dbapi_test_framework/` directory:

```env
# Snowflake
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USER=your-user
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_DATABASE=your-database
SNOWFLAKE_SCHEMA=your-schema
SNOWFLAKE_WAREHOUSE=your-warehouse
SNOWFLAKE_ROLE=your-role
SNOWFLAKE_HOST=your-account.snowflakecomputing.com
SNOWFLAKE_PORT=443
SNOWFLAKE_PROTOCOL=https

# MySQL
MYSQL_HOST=your-mysql-host
MYSQL_PORT=3306
MYSQL_USERNAME=your-user
MYSQL_PASSWORD=your-password
MYSQL_DATABASE=your-database

# PostgreSQL
POSTGRES_HOST=your-postgres-host
POSTGRES_PORT=5432
POSTGRES_USER=your-user
POSTGRES_PASSWORD=your-password
POSTGRES_DBNAME=your-database

# SQL Server
MSSQL_SERVER=your-sqlserver-host
MSSQL_PORT=1433
MSSQL_UID=your-user
MSSQL_PWD=your-password
MSSQL_DATABASE=test_db
MSSQL_DRIVER={ODBC Driver 18 for SQL Server}

# Oracle
ORACLEDB_HOST=your-oracle-host
ORACLEDB_PORT=1521
ORACLEDB_USERNAME=your-user
ORACLEDB_PASSWORD=your-password
ORACLEDB_SERVICE_NAME=your-service

# Databricks
DATABRICKS_SERVER_HOSTNAME=your-workspace.databricks.net
DATABRICKS_HTTP_PATH=sql/protocolv1/o/...
DATABRICKS_ACCESS_TOKEN=your-token
```

### 2. Set Up Test Data

Create test tables with identical data across all databases:

```bash
cd db_setup_util

# Setup individual database
python3 mysql_setup.py
python3 postgres_setup.py
python3 oracle_setup.py
python3 mssql_setup.py
python3 databricks_setup.py

# Or setup all at once
python3 setup_all.py
```

This creates `DBAPI_TEST_TABLE` with 10,000 rows of deterministic test data in each database.

### 3. Run Tests

```bash
# From the dbapi_test_framework directory
cd tests/perf/data_source/dbapi_test_framework

# Run a single test
python3 main.py

# Run full test matrix
python3 main.py --matrix
```

Or as a module from project root:
```bash
python3 -m tests.perf.data_source.dbapi_test_framework.main
python3 -m tests.perf.data_source.dbapi_test_framework.main --matrix
```

## Ingestion Methods

The framework tests 7 different ingestion approaches:

### DBAPI Methods (Python drivers)

1. **local** - Local ingestion using `session.read.dbapi()`
   - Data fetched locally and uploaded to Snowflake
   - No external access integration needed

2. **udtf** - UDTF ingestion using `session.read.dbapi(udtf_configs=...)`
   - Data fetched via UDTF running on Snowflake
   - Requires external access integration

3. **local_sproc** - Local ingestion inside a stored procedure
   - Local ingestion logic runs inside a Snowflake stored procedure
   - Requires external access integration + packages

4. **udtf_sproc** - UDTF ingestion inside a stored procedure
   - UDTF ingestion logic runs inside a Snowflake stored procedure
   - Requires external access integration + packages

### JDBC Methods (Java drivers)

5. **jdbc** - JDBC ingestion using `session.read.jdbc()`
   - Data fetched via JDBC UDTF running on Snowflake
   - Requires JDBC driver JAR, Snowflake secret, and external access integration
   - **Only supports UDTF-based ingestion** (no local mode)
   - Driver JARs are automatically uploaded to stage on first run

6. **jdbc_sproc** - JDBC ingestion inside a stored procedure
   - JDBC ingestion logic runs inside a Snowflake stored procedure
   - Requires JDBC driver JAR, Snowflake secret, and external access integration
   - Driver JARs are automatically uploaded to stage on first run

### PySpark Methods (Spark + JDBC)

7. **pyspark** - PySpark JDBC ingestion
   - Data fetched via PySpark JDBC running on local Spark session
   - Written to Snowflake using Snowflake-Spark connector
   - Requires PySpark, JDBC driver JARs in `drivers/` directory, and Snowflake-Spark connector
   - Uses plain credentials from `.env` (not Snowflake secrets)
   - Runs on local machine (not on Snowflake servers)

## Supported Databases

All databases support DBAPI, JDBC, and PySpark methods:

- **MySQL** (DBAPI: `pymysql`, JDBC: `mysql-connector-j`)
- **PostgreSQL** (DBAPI: `psycopg2`, JDBC: `postgresql`)
- **MS SQL Server** (DBAPI: `pyodbc`, JDBC: `mssql-jdbc`)
- **Oracle** (DBAPI: `oracledb`, JDBC: `ojdbc`)
- **Databricks** (DBAPI: `databricks-sql-connector`, JDBC: `DatabricksJDBC42`)

**Note**: PySpark method uses the same JDBC drivers as the JDBC methods.

## Configuration

### Test Matrix

The test matrix in `config.py` supports two source types:

```python
# Test config format
{
    "dbms": "mysql",
    "source": {
        "type": "table|query",  # Type: table or query
        "value": "DBAPI_TEST_TABLE"  # Table name or SQL query
    },
    "ingestion_method": "local|udtf|local_sproc|udtf_sproc|jdbc|jdbc_sproc"
}
```

### Generate Test Matrix

Use list comprehensions for compact configuration:

```python
_DBMS_LIST = ["mysql", "postgres", "mssql", "oracle", "databricks"]
_METHODS = ["local", "udtf"]

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
```

This generates 20 tests (5 DBMS × 2 methods × 2 source types).

### External Access Integrations

Required for UDTF and stored procedure methods:

```python
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
```

### Stored Procedure Packages

Automatically configured per DBMS:

```python
SPROC_PACKAGES = {
    "mysql": ["pymysql"],
    "postgres": ["psycopg2"],
    "mssql": ["pyodbc", "msodbcsql"],
    "oracle": ["oracledb"],
    "databricks": ["databricks-sql-connector"],
}
```

### Runtime Options

```python
# Show table info (row count + first row) before cleanup
SHOW_TARGET_TABLE_INFO = True  # Default

# Cleanup target tables after tests
CLEANUP_TARGET_TABLES = False  # Set to True in production

# DBAPI parameters (optional)
DBAPI_PARAMS = {
    "fetch_size": 10000,
    "max_workers": 4,
}
```

## Example Configurations

### Table-Based Ingestion
```python
{
    "dbms": "mysql",
    "source": {"type": "table", "value": "DBAPI_TEST_TABLE"},
    "ingestion_method": "local"
}
```

### Query-Based Ingestion
```python
{
    "dbms": "postgres",
    "source": {
        "type": "query",
        "value": "SELECT id, varchar_col FROM DBAPI_TEST_TABLE WHERE id < 5000"
    },
    "ingestion_method": "udtf"
}
```

### Stored Procedure Ingestion
```python
{
    "dbms": "oracle",
    "source": {"type": "table", "value": "DBAPI_TEST_TABLE"},
    "ingestion_method": "udtf_sproc"  # Runs inside Snowflake stored procedure
}
```

### JDBC Ingestion
```python
{
    "dbms": "mysql",
    "source": {"type": "table", "value": "DBAPI_TEST_TABLE"},
    "ingestion_method": "jdbc"  # Uses JDBC driver instead of Python DBAPI
}
```

### JDBC in Stored Procedure
```python
{
    "dbms": "postgres",
    "source": {
        "type": "query",
        "value": "SELECT * FROM DBAPI_TEST_TABLE WHERE id BETWEEN 1 AND 1000"
    },
    "ingestion_method": "jdbc_sproc"  # JDBC inside stored procedure
}
```

### PySpark Ingestion
```python
{
    "dbms": "mysql",
    "source": {"type": "table", "value": "DBAPI_TEST_TABLE"},
    "ingestion_method": "pyspark",  # PySpark JDBC on local Spark session
    "dbapi_params": {
        "fetchsize": 10000,
        "numPartitions": 10,
        "partitionColumn": "id",
        "lowerBound": 0,
        "upperBound": 100000
    }
}
```

## Output

### During Test Execution

```
############################################################
TEST: MYSQL - LOCAL
Source Type: TABLE
Source Value: DBAPI_TEST_TABLE
############################################################

============================================================
Running: LOCAL INGESTION
============================================================
✓ Completed in 12.45 seconds

============================================================
TARGET TABLE INFO
============================================================
Row count: 10000

First row:
---------------------------------------------------------
|"ID"  |"INT_COL"  |"BIGINT_COL"  |"VARCHAR_COL"  |...  |
---------------------------------------------------------
|1     |83811      |478163328     |varchar_0_...  |...  |
---------------------------------------------------------

✓ Cleaned up target table: TEST_MYSQL_LOCAL_1234567890

Test completed: success
```

### Test Summary (Matrix Mode)

```
================================================================================
TEST SUMMARY
================================================================================
 Status      DBMS         Method        Source        Value                Time    
--------------------------------------------------------------------------------
   ✓        mysql         local         table      DBAPI_TEST_TABLE        12.34s  
   ✓        mysql         udtf          table      DBAPI_TEST_TABLE        15.67s  
   ✓        postgres      local         table      DBAPI_TEST_TABLE        11.23s  
   ✓        postgres      udtf          table      DBAPI_TEST_TABLE        14.56s  
   ✓        mysql         local         query      SELECT * FROM DBAPI...  10.89s  
   ✓        mysql         udtf          query      SELECT * FROM DBAPI...  13.45s  
--------------------------------------------------------------------------------
Total: 6 | Success: 6 | Failed: 0
```

## Prerequisites

### Python Packages

```bash
pip install python-dotenv      # For .env support
pip install pymysql            # MySQL
pip install psycopg2-binary    # PostgreSQL
pip install pyodbc             # SQL Server
pip install oracledb           # Oracle
pip install databricks-sql-connector  # Databricks
pip install pyspark            # For PySpark ingestion method
```

**Note for PySpark**: You also need the Snowflake-Spark connector and Snowflake JDBC driver:
- **Snowflake Spark Connector**: `spark-snowflake_2.13-3.1.0.jar` (Scala 2.13, recommended)
  - Download: https://mvnrepository.com/artifact/net.snowflake/spark-snowflake_2.13/3.1.0
  - **Important**: Use version 3.1.0 - version 3.1.1+ has issues with Oracle BLOB types
- **Snowflake JDBC**: `snowflake-jdbc-3.19.0.jar` or later
  - Download: https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc
- Place both JARs in the `drivers/` directory

See `drivers/jdbc_drivers_readme.md` for more details and known issues.

### Snowflake Setup

For UDTF and stored procedure methods, create external access integrations:

```sql
-- Example for MySQL
CREATE OR REPLACE NETWORK RULE snowpark_dbapi_mysql_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('your-mysql-host:3306');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION snowpark_dbapi_mysql_test_integration
  ALLOWED_NETWORK_RULES = (snowpark_dbapi_mysql_network_rule)
  ENABLED = TRUE;
```

Repeat for each DBMS you want to test with UDTF/stored procedure methods.

### JDBC Setup (for jdbc and jdbc_sproc methods)

#### 1. Download JDBC Drivers

Download the required JDBC driver JARs and place them in the `drivers/` directory:

```bash
cd tests/perf/data_source/dbapi_test_framework
mkdir -p drivers
# Download drivers from official sources (see drivers/README.md)
```

See `drivers/README.md` for download links and specific versions.

#### 2. Create Snowflake Secrets

JDBC methods require Snowflake secrets to store database credentials:

```sql
-- Example for MySQL
CREATE OR REPLACE SECRET snowpark_dbapi_mysql_test_cred
  TYPE = USERNAME_PASSWORD
  USERNAME = 'your_mysql_user'
  PASSWORD = 'your_mysql_password';

-- Grant usage to the role
GRANT USAGE ON SECRET snowpark_dbapi_mysql_test_cred TO ROLE your_role;
```

Repeat for each database you want to test with JDBC.

**Secret naming convention:**
- MySQL: `ADMIN.PUBLIC.SNOWPARK_DBAPI_MYSQL_TEST_CRED`
- PostgreSQL: `ADMIN.PUBLIC.SNOWPARK_DBAPI_POSTGRES_TEST_CRED`
- SQL Server: `ADMIN.PUBLIC.SNOWPARK_DBAPI_SQL_SERVER_TEST_CRED`
- Oracle: `ADMIN.PUBLIC.SNOWPARK_DBAPI_ORACLEDB_TEST_CRED`
- Databricks: `ADMIN.PUBLIC.SNOWPARK_DBAPI_DATABRICKS_TEST_CRED`

You can customize secret names via environment variables (e.g., `MYSQL_SECRET`).

#### 3. Update External Access Integration (if needed)

The JDBC methods use the same external access integrations as DBAPI UDTF methods. No additional setup required if you already have UDTF working.

#### 4. Automatic Driver Upload

When you run JDBC tests:
- The framework automatically checks if the driver is on the stage
- If not found, it uploads from your local `drivers/` directory
- Subsequent runs skip the upload (fast)
- Each test only uploads the specific driver it needs

### PySpark Setup (for pyspark method)

#### 1. JDBC Drivers

PySpark uses the same JDBC drivers as the JDBC methods. Ensure drivers are in the `drivers/` directory (see JDBC Setup above).

#### 2. Snowflake-Spark Connector

Download the Snowflake-Spark connector JAR:
- Maven Central: `net.snowflake:spark-snowflake_2.12` or `spark-snowflake_2.13`
- Place in `drivers/` directory alongside JDBC drivers
- Or configure `spark.jars` in `config.PYSPARK_SESSION_CONFIG`

#### 3. Configuration

Customize PySpark session settings in `config.py`:

```python
PYSPARK_SESSION_CONFIG = {
    "spark.master": "local[*]",
    "spark.driver.extraClassPath": "./drivers/*",
    # Optional: Tune for your machine
    "spark.sql.shuffle.partitions": 16,
    "spark.default.parallelism": 16,
    "spark.executor.cores": 8,
    "spark.executor.memory": "16g",
}
```

#### 4. Credentials

PySpark uses plain credentials from `.env` (not Snowflake secrets):
- Reads directly from `MYSQL_USERNAME`, `MYSQL_PASSWORD`, etc.
- No external access integration required
- Runs entirely on local machine

## Database Setup Utilities

The `db_setup_util/` directory contains scripts to create identical test tables across all databases.

### Features
- Same table name: `DBAPI_TEST_TABLE`
- Same 15 columns with compatible types
- Deterministic data (row #N is identical across all DBMS)
- 10,000 rows by default (configurable)

### Usage

See `db_setup_util/README.md` for detailed documentation.

## Troubleshooting

### Connection Issues
- Verify `.env` file exists and has correct credentials
- Check that `python-dotenv` is installed
- Test connection to source databases independently

### Permission Issues
- Ensure Snowflake user has CREATE TABLE privileges
- Ensure source database users have SELECT privileges
- For stored procedures, ensure external access integrations are granted

### Package Issues
- Install all required Python packages
- For SQL Server, ensure ODBC driver is installed (`msodbcsql` or `ODBC Driver 18 for SQL Server`)
- Check package versions are compatible

### Table Not Found
- Run the setup scripts in `db_setup_util/` first
- Verify table name matches config (`DBAPI_TEST_TABLE` by default)

## Advanced Usage

### Custom DBAPI Parameters

```python
DBAPI_PARAMS = {
    "fetch_size": 5000,
    "max_workers": 8,
}
```

### Mixed Test Matrix

```python
TEST_MATRIX = [
    # Table with local method
    {"dbms": "mysql", "source": {"type": "table", "value": "DBAPI_TEST_TABLE"}, "ingestion_method": "local"},
    
    # Query with UDTF
    {"dbms": "postgres", "source": {"type": "query", "value": "SELECT * FROM DBAPI_TEST_TABLE WHERE id < 5000"}, "ingestion_method": "udtf"},
    
    # Stored procedure
    {"dbms": "oracle", "source": {"type": "table", "value": "DBAPI_TEST_TABLE"}, "ingestion_method": "local_sproc"},
]
```

### Keep Tables for Debugging

```python
# In config.py
CLEANUP_TARGET_TABLES = False  # Tables persist after tests
SHOW_TARGET_TABLE_INFO = True  # Show row count + first row
```

## Architecture Notes

- **Lazy evaluation**: Connection parameters are loaded from `.env` via `config.py`
- **Clean imports**: Uses try/except pattern for both direct and module execution
- **Extensible**: Easy to add new DBMS or ingestion methods
- **DRY principle**: Common logic in base classes, DBMS-specific logic isolated
- **Configurable**: Runtime behavior controlled via config variables
