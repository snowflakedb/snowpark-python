#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
Generate SQL queries that multiply 10k rows by k factor for performance testing.
Each query returns the same 10k rows repeated k times.
"""


def generate_large_query(dbms, table_name="DBAPI_TEST_TABLE", k=100):
    """
    Generate a SQL query that returns 10k rows repeated k times.

    Args:
        dbms: Database type (mysql, postgres, mssql, oracle, databricks)
        table_name: Base table name
        k: Multiplication factor (1 = 10k rows, 100 = 1M rows, 1000 = 10M rows)

    Returns:
        SQL query string that returns 10k * k rows
    """
    dbms = dbms.lower()

    if dbms == "mysql":
        # MySQL: Use recursive CTE to generate numbers, then CROSS JOIN
        query = f"""
WITH RECURSIVE numbers AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM numbers WHERE n < {k}
)
SELECT t.*
FROM {table_name} t
CROSS JOIN numbers
"""

    elif dbms == "postgres" or dbms == "postgresql":
        # PostgreSQL: Use generate_series()
        query = f"""
SELECT t.*
FROM {table_name} t
CROSS JOIN generate_series(1, {k}) AS multiplier(n)
"""

    elif dbms == "mssql" or dbms == "sqlserver":
        # SQL Server: Use VALUES with CROSS JOIN for multiplication (works in subqueries, no CTE)
        # Decompose k into smaller factors to avoid huge VALUES clauses
        # e.g., k=100 = 10 × 10, k=1000 = 10 × 10 × 10, k=10000 = 100 × 100
        # Decompose k into factors (prefer 10s and 100s for readability)
        factors = []
        remaining = k

        while remaining > 1:
            if remaining % 100 == 0 and remaining >= 100:
                factors.append(100)
                remaining //= 100
            elif remaining % 10 == 0 and remaining >= 10:
                factors.append(10)
                remaining //= 10
            else:
                # For non-round numbers, just use the remainder
                factors.append(remaining)
                remaining = 1

        # If no factors or just 1, use simple VALUES
        if not factors or (len(factors) == 1 and factors[0] <= 100):
            factor = factors[0] if factors else k
            values_list = ", ".join([f"({i})" for i in range(1, factor + 1)])
            query = f"""
SELECT t.*
FROM {table_name} t
CROSS JOIN (VALUES {values_list}) AS multiplier(n)
"""
        else:
            # Generate CROSS JOINs
            crosses = []
            for i, factor in enumerate(factors):
                values_list = ", ".join([f"({j})" for j in range(1, factor + 1)])
                crosses.append(f"CROSS JOIN (VALUES {values_list}) AS n{i}(v{i})")

            query = f"""
SELECT t.*
FROM {table_name} t
{' '.join(crosses)}
"""

    elif dbms == "oracle":
        # Oracle: Use CONNECT BY LEVEL (no alias for inline view in CROSS JOIN)
        query = f"""
SELECT t.*
FROM {table_name} t
CROSS JOIN (SELECT LEVEL AS n FROM DUAL CONNECT BY LEVEL <= {k}) multiplier
"""

    elif dbms == "databricks" or dbms == "dbx":
        # Databricks: Use explode with sequence
        query = f"""
SELECT t.*
FROM {table_name} t
CROSS JOIN (SELECT explode(sequence(1, {k})) AS n)
"""

    else:
        raise ValueError(f"Unsupported DBMS: {dbms}")

    return query.strip()


def generate_validation_query(dbms, table_name="DBAPI_TEST_TABLE"):
    """
    Generate a query to get baseline schema and row count.

    Args:
        dbms: Database type
        table_name: Base table name

    Returns:
        Dict with count query and sample query
    """
    return {
        "count": f"SELECT COUNT(*) as row_count FROM {table_name}",
        "sample": f"SELECT * FROM {table_name} LIMIT 1",
        "columns": f"SELECT * FROM {table_name} WHERE 1=0",  # Get schema only
    }


def print_all_queries(k=100):
    """Print queries for all DBMS types."""
    dbms_list = ["mysql", "postgres", "mssql", "oracle", "databricks"]

    print("=" * 80)
    print(f"LARGE QUERY GENERATION (k={k}, expected rows={10_000 * k:,})")
    print("=" * 80)

    for dbms in dbms_list:
        query = generate_large_query(dbms, k=k)
        print(f"\n{dbms.upper()}:")
        print("-" * 80)
        print(query)
        print("-" * 80)

    print(f"\n\n{'='*80}")
    print("VALIDATION QUERIES")
    print("=" * 80)
    print("\nUse these to validate the multiplied query returns correct results:")
    print("\n1. Row count should be: 10,000 * k")
    print("2. Column names should match the base table")
    print("3. Data should be duplicated k times")
    print("\nBaseline query for all DBMS:")
    print("  SELECT COUNT(*) FROM DBAPI_TEST_TABLE  -- Should return 10,000")


if __name__ == "__main__":
    # Example usage
    print("\n" + "=" * 80)
    print("EXAMPLE: Small test (k=10, 100k rows)")
    print("=" * 80)
    print_all_queries(k=10)

    print("\n\n" + "=" * 80)
    print("EXAMPLE: Medium test (k=100, 1M rows)")
    print("=" * 80)
    print_all_queries(k=100)

    print("\n\n" + "=" * 80)
    print("USAGE IN CONFIG")
    print("=" * 80)
    print(
        """
# Example config for large query test:
{
    "dbms": "postgres",
    "source": {
        "type": "query",
        "value": '''
            SELECT t.*
            FROM DBAPI_TEST_TABLE t
            CROSS JOIN generate_series(1, 100) AS multiplier(n)
        '''
    },
    "ingestion_method": "local"
}

# This will ingest 1M rows (10k * 100) from PostgreSQL
"""
    )
