# Large Query Generation

Utilities for generating and validating large result sets from small tables for performance testing.

## Overview

These utilities help you generate SQL queries that multiply a 10k row table by factor `k` to create large result sets for testing, without actually storing large amounts of data.

## Files

- **`generate_large_queries.py`** - Core query generator with DBMS-specific syntax
- **`query_templates.py`** - Pre-built templates for common sizes (100k, 1M, 10M, 100M, 1B, 10B)
- **`validate_queries.py`** - Validation tool to verify queries work correctly
- **`__init__.py`** - Package exports

## Quick Start

### Generate a Query

```python
from large_query_generation import generate_large_query

# Generate 1M row query for PostgreSQL
query = generate_large_query("postgres", "DBAPI_TEST_TABLE", k=100)

# Use in test config
test_config = {
    "dbms": "postgres",
    "source": {"type": "query", "value": query},
    "ingestion_method": "local"
}
```

### Use Pre-built Templates

```python
from large_query_generation import get_large_query

# Get 1M row template
query_1m = get_large_query("postgres", "1m")

# Available sizes: "100k", "1m", "10m", "100m", "1b", "10b"
```

### Validate Queries

```bash
# Validate all DBMS with k=10 (100k rows each)
python3 validate_queries.py -k 10

# Validate specific DBMS
python3 validate_queries.py --dbms postgres -k 100
```

## How It Works

Each DBMS uses optimized syntax to generate a number sequence and CROSS JOIN with the base table:

| DBMS | Method | Query Pattern |
|------|--------|---------------|
| MySQL | Recursive CTE | `WITH RECURSIVE numbers AS (...) SELECT t.* FROM table t CROSS JOIN numbers` |
| PostgreSQL | generate_series() | `SELECT t.* FROM table t CROSS JOIN generate_series(1, k)` |
| SQL Server | VALUES + Factorization | `SELECT t.* FROM table t CROSS JOIN (VALUES ...) × CROSS JOIN (VALUES ...)` |
| Oracle | CONNECT BY | `SELECT t.* FROM table t CROSS JOIN (SELECT LEVEL FROM DUAL CONNECT BY LEVEL <= k)` |
| Databricks | explode(sequence) | `SELECT t.* FROM table t CROSS JOIN (SELECT explode(sequence(1, k)))` |

**Note:** SQL Server uses smart factorization (e.g., k=1000 = 100×10) to avoid CTEs, making queries subquery-compatible.

## Multiplication Factors

| k Value | Result Size | Template | Use Case |
|---------|-------------|----------|----------|
| 10 | 100,000 | "100k" | Quick test |
| 100 | 1,000,000 | "1m" | Small test |
| 1,000 | 10,000,000 | "10m" | Medium test |
| 10,000 | 100,000,000 | "100m" | Large test |
| 100,000 | 1,000,000,000 | "1b" | Stress test |
| 1,000,000 | 10,000,000,000 | "10b" | Extreme test |

## Examples

### Example 1: Dynamic Generation

```python
from db_setup_util.large_query_generation import generate_large_query

# Different sizes for different DBMS
TEST_MATRIX = [
    {
        "dbms": "mysql",
        "source": {
            "type": "query",
            "value": generate_large_query("mysql", k=100)  # 1M rows
        },
        "ingestion_method": "local"
    },
    {
        "dbms": "postgres",
        "source": {
            "type": "query",
            "value": generate_large_query("postgres", k=1000)  # 10M rows
        },
        "ingestion_method": "udtf"
    },
]
```

### Example 2: Using Templates

```python
from db_setup_util.large_query_generation import get_large_query

# Simple template usage
TEST_MATRIX = [
    {"dbms": "postgres", "source": {"type": "query", "value": get_large_query("postgres", "1m")}, "ingestion_method": "local"},
    {"dbms": "mysql", "source": {"type": "query", "value": get_large_query("mysql", "10m")}, "ingestion_method": "udtf"},
]
```

## Validation

The validation script checks:
- ✅ Query executes without errors
- ✅ Returns expected schema (same columns as base table)
- ✅ Can be wrapped in subquery (SELECT * FROM (query) AS subquery)
- ✅ Expected row count is calculated (10k × k)

```bash
python3 validate_queries.py -k 100
```

Output:
```
VALIDATION SUMMARY
======================================================================
DBMS         Expected     Syntax  Columns  Subquery  Status
----------------------------------------------------------------------
mysql       1,000,000        ✓       ✓        ✓      ✓ PASS
postgres    1,000,000        ✓       ✓        ✓      ✓ PASS
mssql       1,000,000        ✓       ✓        ✓      ✓ PASS
oracle      1,000,000        ✓       ✓        ✓      ✓ PASS
databricks  1,000,000        ✓       ✓        ✓      ✓ PASS
----------------------------------------------------------------------
Overall: ✓ ALL PASSED
```

## Notes

- **Efficient**: Database generates rows on-the-fly, no storage needed
- **Deterministic**: Same base table = same multiplied results
- **Scalable**: Can generate up to 100M+ rows from 10k base
- **Flexible**: Customize k value for any test size
