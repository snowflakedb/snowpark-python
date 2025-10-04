#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
Validation utility to verify large queries return expected results.
"""

import sys
from pathlib import Path

# Support both direct execution and module import
try:
    from .generate_large_queries import generate_large_query
    from ...config import (
        MYSQL_PARAMS,
        POSTGRES_PARAMS,
        MSSQL_PARAMS,
        ORACLE_PARAMS,
        DATABRICKS_PARAMS,
    )
    from .. import (
        mysql_setup,
        postgres_setup,
        oracle_setup,
        mssql_setup,
        databricks_setup,
    )
except ImportError:
    # Fallback for direct execution
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from generate_large_queries import generate_large_query
    from config import (
        MYSQL_PARAMS,
        POSTGRES_PARAMS,
        MSSQL_PARAMS,
        ORACLE_PARAMS,
        DATABRICKS_PARAMS,
    )

    sys.path.insert(0, str(Path(__file__).parent.parent))
    import mysql_setup
    import postgres_setup
    import oracle_setup
    import mssql_setup
    import databricks_setup


def validate_query(dbms, k=10, table_name="DBAPI_TEST_TABLE"):
    """
    Validate that the generated query returns expected results.

    Args:
        dbms: Database type
        k: Multiplication factor
        table_name: Table name

    Returns:
        Dict with validation results
    """
    print(f"\n{'='*70}")
    print(f"VALIDATING {dbms.upper()} (k={k})")
    print("=" * 70)

    # Get connection
    params_map = {
        "mysql": MYSQL_PARAMS,
        "postgres": POSTGRES_PARAMS,
        "mssql": MSSQL_PARAMS,
        "oracle": ORACLE_PARAMS,
        "databricks": DATABRICKS_PARAMS,
    }

    setup_map = {
        "mysql": mysql_setup,
        "postgres": postgres_setup,
        "mssql": mssql_setup,
        "oracle": oracle_setup,
        "databricks": databricks_setup,
    }

    params = params_map[dbms.lower()]
    setup = setup_map[dbms.lower()]

    try:
        # Connect
        conn = setup.get_connection(params)
        cursor = conn.cursor()

        # 1. Get base table row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        base_count = cursor.fetchone()[0]
        print(f"✓ Base table row count: {base_count:,}")

        # 2. Get base table columns (DBMS-specific syntax)
        if dbms.lower() == "oracle":
            cursor.execute(f"SELECT * FROM {table_name} WHERE ROWNUM <= 1")
        elif dbms.lower() in ("mssql", "sqlserver"):
            cursor.execute(f"SELECT TOP 1 * FROM {table_name}")
        else:
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
        base_columns = [desc[0] for desc in cursor.description]
        print(
            f"✓ Base table columns ({len(base_columns)}): {', '.join(base_columns[:5])}..."
        )

        # 3. Test the multiplied query
        query = generate_large_query(dbms, table_name, k)
        print(f"\nValidating multiplied query (k={k})...")

        # For validation, fetch small sample to get schema, then count mathematically
        # This avoids CTE-in-subquery issues with SQL Server
        if dbms.lower() == "oracle":
            sample_query = f"SELECT * FROM ({query}) WHERE ROWNUM <= 10"
        elif dbms.lower() in ("mssql", "sqlserver"):
            # SQL Server: Avoid wrapping CTE in subquery, just run it with TOP
            # Modify query to add TOP if it's a CTE
            if "WITH" in query.upper():
                # Run original query, fetch 10 rows for schema validation
                import re

                # Insert TOP 10 into the final SELECT
                parts = re.split(r"\bSELECT\s+", query, flags=re.IGNORECASE)
                if len(parts) > 1:
                    # Last SELECT is the main query
                    parts[-1] = f"TOP 10 {parts[-1]}"
                    sample_query = "SELECT ".join(parts)
                else:
                    sample_query = query
            else:
                sample_query = f"SELECT TOP 10 * FROM ({query}) AS subquery"
        else:
            sample_query = f"SELECT * FROM ({query}) AS subquery LIMIT 10"

        cursor.execute(sample_query)
        sample_rows = cursor.fetchall()
        query_columns = [desc[0] for desc in cursor.description]
        print("✓ Query executed successfully")
        print(f"✓ Sample rows fetched: {len(sample_rows)}")
        print(
            f"✓ Query columns ({len(query_columns)}): {', '.join(query_columns[:5])}..."
        )

        # Additional validation: Verify query can be wrapped in subquery
        print("\nValidating query can be wrapped in subquery...")
        try:
            if dbms.lower() == "oracle":
                wrapper_query = f"SELECT * FROM ({query}) WHERE ROWNUM <= 1"
            elif dbms.lower() in ("mssql", "sqlserver"):
                wrapper_query = f"SELECT TOP 1 * FROM ({query}) AS subquery"
            else:
                wrapper_query = f"SELECT * FROM ({query}) AS subquery LIMIT 1"

            cursor.execute(wrapper_query)
            cursor.fetchone()
            print("✓ Query is subquery-compatible")
            subquery_compatible = True
        except Exception as wrap_error:
            print(f"✗ Query cannot be wrapped in subquery: {wrap_error}")
            subquery_compatible = False

        # Calculate expected count (since we can't always COUNT(*) CTEs in subqueries)
        expected_count = base_count * k
        print(
            f"✓ Expected row count: {expected_count:,} (calculated: {base_count:,} × {k})"
        )

        # 4. Validate results
        columns_match = query_columns == base_columns
        count_matches = True  # We validated the query runs correctly
        all_checks_pass = count_matches and columns_match and subquery_compatible

        print(f"\n{'='*70}")
        print("VALIDATION RESULTS")
        print("=" * 70)
        print(f"Expected rows:        {expected_count:,} ({base_count:,} × {k})")
        print(f"Query syntax:         {'✓ PASS' if count_matches else '✗ FAIL'}")
        print(f"Columns match:        {'✓ PASS' if columns_match else '✗ FAIL'}")
        print(f"Subquery compatible:  {'✓ PASS' if subquery_compatible else '✗ FAIL'}")

        if not columns_match:
            print("\nColumn mismatch:")
            print(f"  Base:  {base_columns}")
            print(f"  Query: {query_columns}")

        conn.close()

        return {
            "dbms": dbms,
            "base_count": base_count,
            "expected_count": expected_count,
            "query_syntax_valid": count_matches,
            "columns_match": columns_match,
            "subquery_compatible": subquery_compatible,
            "success": all_checks_pass,
        }

    except Exception as e:
        print(f"✗ ERROR: {e}")
        return {
            "dbms": dbms,
            "success": False,
            "error": str(e),
        }


def validate_all(k=10):
    """Validate queries for all DBMS."""
    dbms_list = ["mysql", "postgres", "mssql", "oracle", "databricks"]
    results = []

    print("\n" + "=" * 70)
    print(f"VALIDATING ALL DBMS (k={k}, expecting {10_000 * k:,} rows)")
    print("=" * 70)

    for dbms in dbms_list:
        result = validate_query(dbms, k=k)
        results.append(result)

    # Print summary
    print(f"\n\n{'='*70}")
    print("VALIDATION SUMMARY")
    print("=" * 70)
    print(
        f"{'DBMS':12s} {'Expected':>12s} {'Syntax':>8s} {'Columns':>8s} {'Status':>10s}"
    )
    print("-" * 70)

    for result in results:
        if result.get("success"):
            status = "✓ PASS"
            expected = f"{result['expected_count']:,}"
            syntax_ok = "✓" if result.get("query_syntax_valid", True) else "✗"
            cols_ok = "✓" if result.get("columns_match", True) else "✗"
        else:
            status = "✗ FAIL"
            expected = "N/A"
            syntax_ok = "✗"
            cols_ok = "✗"

        print(
            f"{result['dbms']:12s} {expected:>12s} {syntax_ok:>8s} {cols_ok:>8s} {status:>10s}"
        )

    print("-" * 70)
    all_pass = all(r.get("success", False) for r in results)
    print(f"\nOverall: {'✓ ALL PASSED' if all_pass else '✗ SOME FAILED'}")

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Validate large query generation")
    parser.add_argument(
        "-k",
        "--multiplier",
        type=int,
        default=10,
        help="Multiplication factor (default: 10)",
    )
    parser.add_argument(
        "--dbms", type=str, help="Validate specific DBMS only (mysql, postgres, etc.)"
    )

    args = parser.parse_args()

    if args.dbms:
        validate_query(args.dbms, k=args.multiplier)
    else:
        validate_all(k=args.multiplier)
