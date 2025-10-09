#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Oracle test table setup."""

import oracledb

# Support both direct execution and module import
try:
    from .base_setup import DatabaseSetup
    from .common_schema import DEFAULT_ROWS
except ImportError:
    from base_setup import DatabaseSetup
    from common_schema import DEFAULT_ROWS


def get_connection(params):
    """Create Oracle connection."""
    dsn = f"{params['host']}:{params['port']}/{params['service_name']}"
    return oracledb.connect(
        user=params["user"],
        password=params["password"],
        dsn=dsn,
    )


def main(params=None):
    """Main setup function."""
    if params is None:
        try:
            from ..config import ORACLE_PARAMS as params
        except ImportError:
            import sys
            from pathlib import Path

            sys.path.insert(0, str(Path(__file__).parent.parent))
            from config import ORACLE_PARAMS as params

    print("=" * 60)
    print("Oracle Database Setup")
    print("=" * 60)

    print(f"Connecting to Oracle at {params['host']}...")
    conn = get_connection(params)
    print("Connected!")

    # Use base setup with Oracle-specific settings
    setup = DatabaseSetup(conn, dbms_type="oracle", placeholder_style=":")
    setup.run_setup(num_rows=DEFAULT_ROWS)

    conn.close()
    print("Done!")


if __name__ == "__main__":
    main()
