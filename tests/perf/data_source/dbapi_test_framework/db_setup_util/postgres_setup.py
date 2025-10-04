#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""PostgreSQL test table setup."""

import psycopg2

# Support both direct execution and module import
try:
    from .base_setup import DatabaseSetup
    from .common_schema import DEFAULT_ROWS
except ImportError:
    from base_setup import DatabaseSetup
    from common_schema import DEFAULT_ROWS


def get_connection(params):
    """Create PostgreSQL connection."""
    return psycopg2.connect(
        host=params["host"],
        port=params["port"],
        user=params["user"],
        password=params["password"],
        dbname=params.get("database", params.get("dbname")),  # Accept both
    )


def main(params=None):
    """Main setup function."""
    if params is None:
        try:
            from ..config import POSTGRES_PARAMS as params
        except ImportError:
            import sys
            from pathlib import Path

            sys.path.insert(0, str(Path(__file__).parent.parent))
            from config import POSTGRES_PARAMS as params

    print("=" * 60)
    print("PostgreSQL Database Setup")
    print("=" * 60)

    print(f"Connecting to PostgreSQL at {params['host']}...")
    conn = get_connection(params)
    print("Connected!")

    # Use base setup with PostgreSQL-specific settings
    setup = DatabaseSetup(conn, dbms_type="postgres", placeholder_style="%s")
    setup.run_setup(num_rows=DEFAULT_ROWS)

    conn.close()
    print("Done!")


if __name__ == "__main__":
    main()
