# DBAPI Test Framework

Simple framework for testing DBAPI ingestion performance across different databases and ingestion methods.

## Structure

```
dbapi_test_framework/
├── config.py          # Configuration (connection params, test matrix)
├── connections.py     # Connection factories for each DBMS
├── runner.py          # Test runner with 4 ingestion methods
├── main.py           # Entry point
└── dbms/             # Placeholder directory (logic in connections.py)
```

## Quick Start

### 1. Configure

Edit `config.py`:
- Set your Snowflake connection parameters
- Set source database connection parameters (MySQL, Postgres, etc.)
- Define test matrix or single test config

### 2. Run

```bash
# Run a single test
python -m tests.perf.data_source.dbapi_test_framework.main

# Run full test matrix
python -m tests.perf.data_source.dbapi_test_framework.main --matrix
```

## Ingestion Methods

1. **local** - Local ingestion using `session.read.dbapi()`
2. **udtf** - UDTF ingestion using `session.read.dbapi(udtf_configs=...)`
3. **local_sproc** - Local ingestion inside a stored procedure
4. **udtf_sproc** - UDTF ingestion inside a stored procedure

## Supported Databases

- MySQL
- PostgreSQL
- MS SQL Server
- Oracle
- Databricks

## Configuration

### External Access Integrations

Each DBMS needs its own external access integration for UDTF ingestion. Configure in `config.py`:

```python
UDTF_CONFIGS = {
    'mysql': {
        'external_access_integration': 'MYSQL_EXTERNAL_ACCESS_INTEGRATION',
    },
    'postgres': {
        'external_access_integration': 'POSTGRES_EXTERNAL_ACCESS_INTEGRATION',
    },
    # ... etc for mssql, oracle, databricks
}
```

### Example Test Config

```python
TEST_MATRIX = [
    {
        'dbms': 'mysql',
        'table': 'test_table',
        'ingestion_method': 'local',
    },
    {
        'dbms': 'postgres',
        'table': 'test_table',
        'ingestion_method': 'udtf',  # Will use POSTGRES_EXTERNAL_ACCESS_INTEGRATION
    },
]
```

## Output

Tests print timing information to console:

```
############################################################
TEST: MYSQL - LOCAL
Source Table: test_table
############################################################

============================================================
Running: LOCAL INGESTION
============================================================
✓ Completed in 12.45 seconds
```
