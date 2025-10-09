#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Microsoft SQL Server test table setup."""

import pyodbc

# Support both direct execution and module import
try:
    from .base_setup import DatabaseSetup
    from .common_schema import DEFAULT_ROWS
except ImportError:
    from base_setup import DatabaseSetup
    from common_schema import DEFAULT_ROWS


def get_connection(params):
    """Create SQL Server connection."""
    driver = params.get("driver", "{ODBC Driver 18 for SQL Server}")
    connection_string = (
        f"DRIVER={driver};"
        f"SERVER={params['host']},{params.get('port', 1433)};"
        f"DATABASE={params.get('database', 'test_db')};"
        f"UID={params['user']};"
        f"PWD={params['password']};"
        f"TrustServerCertificate=yes;"
        f"Encrypt=yes;"
    )
    return pyodbc.connect(connection_string)


def main(params=None):
    """Main setup function."""
    if params is None:
        try:
            from ..config import MSSQL_PARAMS as params
        except ImportError:
            import sys
            from pathlib import Path

            sys.path.insert(0, str(Path(__file__).parent.parent))
            from config import MSSQL_PARAMS as params

    print("=" * 60)
    print("SQL Server Database Setup")
    print("=" * 60)

    print(f"Connecting to SQL Server at {params['host']}...")
    conn = get_connection(params)
    print("Connected!")

    # Use base setup with SQL Server-specific settings
    setup = DatabaseSetup(conn, dbms_type="mssql", placeholder_style="?")
    setup.run_setup(num_rows=DEFAULT_ROWS)

    conn.close()
    print("Done!")


if __name__ == "__main__":
    main()
