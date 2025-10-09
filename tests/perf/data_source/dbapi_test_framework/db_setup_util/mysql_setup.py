#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""MySQL test table setup."""

import pymysql

# Support both direct execution and module import
try:
    from .base_setup import DatabaseSetup
    from .common_schema import DEFAULT_ROWS
except ImportError:
    from base_setup import DatabaseSetup
    from common_schema import DEFAULT_ROWS


def get_connection(params):
    """Create MySQL connection."""
    return pymysql.connect(
        host=params["host"],
        port=params.get("port", 3306),
        user=params["user"],
        password=params["password"],
        database=params["database"],
    )


def main(params=None):
    """Main setup function."""
    if params is None:
        try:
            from ..config import MYSQL_PARAMS as params
        except ImportError:
            import sys
            from pathlib import Path

            sys.path.insert(0, str(Path(__file__).parent.parent))
            from config import MYSQL_PARAMS as params

    print("=" * 60)
    print("MySQL Database Setup")
    print("=" * 60)

    print(f"Connecting to MySQL at {params['host']}...")
    conn = get_connection(params)
    print("Connected!")

    # Use base setup with MySQL-specific settings
    setup = DatabaseSetup(conn, dbms_type="mysql", placeholder_style="%s")
    setup.run_setup(num_rows=DEFAULT_ROWS)

    conn.close()
    print("Done!")


if __name__ == "__main__":
    main()
