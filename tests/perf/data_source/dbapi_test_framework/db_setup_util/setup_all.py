#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
Run all database setups at once.
Creates DBAPI_TEST_TABLE with identical data in all configured databases.
"""

# Support both direct execution and module import
try:
    from . import (
        mysql_setup,
        postgres_setup,
        oracle_setup,
        mssql_setup,
        databricks_setup,
    )
except ImportError:
    import mysql_setup
    import postgres_setup
    import oracle_setup
    import mssql_setup
    import databricks_setup


def run_all_setups():
    """Run setup for all databases."""
    setups = [
        ("MySQL", mysql_setup),
        ("PostgreSQL", postgres_setup),
        ("Oracle", oracle_setup),
        ("SQL Server", mssql_setup),
        ("Databricks", databricks_setup),
    ]

    results = {}

    print("\n" + "=" * 70)
    print("RUNNING ALL DATABASE SETUPS")
    print("=" * 70 + "\n")

    for name, setup_module in setups:
        try:
            print(f"\n{'='*70}")
            print(f"Setting up {name}...")
            print("=" * 70)
            setup_module.main()
            results[name] = "✓ SUCCESS"
        except Exception as e:
            results[name] = f"✗ FAILED: {str(e)}"
            print(f"Error setting up {name}: {e}")

    # Print summary
    print("\n" + "=" * 70)
    print("SETUP SUMMARY")
    print("=" * 70)
    for name, result in results.items():
        print(f"{name:20s} {result}")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    run_all_setups()
