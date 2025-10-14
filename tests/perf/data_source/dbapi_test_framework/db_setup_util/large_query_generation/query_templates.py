#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
Query templates for generating large result sets for performance testing.

Each query multiplies the base 10k rows by factor k using DBMS-specific syntax.
"""

try:
    from .generate_large_queries import generate_large_query
except ImportError:
    from generate_large_queries import generate_large_query


# Pre-generated queries for common test sizes
LARGE_QUERY_TEMPLATES = {
    # 100k rows (k=10)
    "100k": {
        "mysql": generate_large_query("mysql", k=10),
        "postgres": generate_large_query("postgres", k=10),
        "mssql": generate_large_query("mssql", k=10),
        "oracle": generate_large_query("oracle", k=10),
        "databricks": generate_large_query("databricks", k=10),
    },
    # 1M rows (k=100)
    "1m": {
        "mysql": generate_large_query("mysql", k=100),
        "postgres": generate_large_query("postgres", k=100),
        "mssql": generate_large_query("mssql", k=100),
        "oracle": generate_large_query("oracle", k=100),
        "databricks": generate_large_query("databricks", k=100),
    },
    # 10M rows (k=1000)
    "10m": {
        "mysql": generate_large_query("mysql", k=1000),
        "postgres": generate_large_query("postgres", k=1000),
        "mssql": generate_large_query("mssql", k=1000),
        "oracle": generate_large_query("oracle", k=1000),
        "databricks": generate_large_query("databricks", k=1000),
    },
    # 100M rows (k=10000)
    "100m": {
        "mysql": generate_large_query("mysql", k=10000),
        "postgres": generate_large_query("postgres", k=10000),
        "mssql": generate_large_query("mssql", k=10000),
        "oracle": generate_large_query("oracle", k=10000),
        "databricks": generate_large_query("databricks", k=10000),
    },
    # 1B rows (k=100000)
    "1b": {
        "mysql": generate_large_query("mysql", k=100000),
        "postgres": generate_large_query("postgres", k=100000),
        "mssql": generate_large_query("mssql", k=100000),
        "oracle": generate_large_query("oracle", k=100000),
        "databricks": generate_large_query("databricks", k=100000),
    },
    # 10B rows (k=1000000)
    "10b": {
        "mysql": generate_large_query("mysql", k=1000000),
        "postgres": generate_large_query("postgres", k=1000000),
        "mssql": generate_large_query("mssql", k=1000000),
        "oracle": generate_large_query("oracle", k=1000000),
        "databricks": generate_large_query("databricks", k=1000000),
    },
}


def get_large_query(dbms, size="1m"):
    """
    Get a pre-generated large query template.

    Args:
        dbms: Database type (mysql, postgres, mssql, oracle, databricks)
        size: Query size ('100k', '1m', '10m')

    Returns:
        SQL query string

    Example:
        query = get_large_query("postgres", "1m")
        # Returns query that produces 1M rows from 10k base table
    """
    size = size.lower()
    dbms = dbms.lower()

    if size not in LARGE_QUERY_TEMPLATES:
        raise ValueError(
            f"Unknown size: {size}. Use '100k', '1m', '10m', '100m', '1b', or '10b'"
        )

    if dbms not in LARGE_QUERY_TEMPLATES[size]:
        raise ValueError(f"Unknown DBMS: {dbms}")

    return LARGE_QUERY_TEMPLATES[size][dbms]


if __name__ == "__main__":
    # Example usage
    print("Available query templates:")
    print("=" * 70)
    row_counts = {
        "100k": "100,000",
        "1m": "1,000,000",
        "10m": "10,000,000",
        "100m": "100,000,000",
        "1b": "1,000,000,000",
        "10b": "10,000,000,000",
    }
    for size, queries in LARGE_QUERY_TEMPLATES.items():
        row_count = row_counts[size]
        print(f"\n{size.upper()} ({row_count} rows):")
        for dbms in queries.keys():
            print(f"  - {dbms}")

    print("\n\nExample usage in config.py:")
    print("=" * 70)
    print(
        """
from query_templates import get_large_query

# Get 1M row query for PostgreSQL
postgres_1m_query = get_large_query("postgres", "1m")

TEST_MATRIX = [
    {
        "dbms": "postgres",
        "source": {"type": "query", "value": postgres_1m_query},
        "ingestion_method": "local"
    }
]
"""
    )
