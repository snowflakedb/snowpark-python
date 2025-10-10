#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Databricks test table setup."""

from databricks import sql

# Support both direct execution and module import
try:
    from .base_setup import DatabaseSetup
    from .common_schema import TABLE_NAME, COLUMN_NAMES, generate_row_data, DEFAULT_ROWS
except ImportError:
    from base_setup import DatabaseSetup
    from common_schema import TABLE_NAME, COLUMN_NAMES, generate_row_data, DEFAULT_ROWS


class DatabricksSetup(DatabaseSetup):
    """Databricks-specific setup with custom insert logic."""

    def insert_data(
        self, num_rows=DEFAULT_ROWS, table_name=TABLE_NAME, batch_size=1000
    ):
        """Insert data using multi-row INSERT (Databricks doesn't support executemany well)."""
        cursor = self.connection.cursor()
        columns = ", ".join(COLUMN_NAMES)

        for batch_start in range(0, num_rows, batch_size):
            batch_end = min(batch_start + batch_size, num_rows)

            # Build multi-row INSERT
            values_list = []
            for i in range(batch_start, batch_end):
                row = generate_row_data(i)
                values = []
                for val in row:
                    if val is None:
                        values.append("NULL")
                    elif isinstance(val, bool):
                        values.append("TRUE" if val else "FALSE")
                    elif isinstance(val, (int, float)):
                        values.append(str(val))
                    elif isinstance(val, bytes):
                        values.append(f"X'{val.hex()}'")
                    elif isinstance(val, str):
                        escaped = val.replace("'", "''")
                        values.append(f"'{escaped}'")
                    else:
                        values.append(f"'{str(val)}'")
                values_list.append(f"({', '.join(values)})")

            insert_sql = (
                f"INSERT INTO {table_name} ({columns}) VALUES {', '.join(values_list)}"
            )
            cursor.execute(insert_sql)
            print(f"Inserted rows {batch_start} to {batch_end-1}")

        # Verify count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"Total rows in {table_name}: {count}")
        cursor.close()


def get_connection(params):
    """Create Databricks connection."""
    return sql.connect(
        server_hostname=params["server_hostname"],
        http_path=params["http_path"],
        access_token=params["access_token"],
    )


def main(params=None):
    """Main setup function."""
    if params is None:
        try:
            from ..config import DATABRICKS_PARAMS as params
        except ImportError:
            import sys
            from pathlib import Path

            sys.path.insert(0, str(Path(__file__).parent.parent))
            from config import DATABRICKS_PARAMS as params

    print("=" * 60)
    print("Databricks Database Setup")
    print("=" * 60)

    print(f"Connecting to Databricks at {params['server_hostname']}...")
    conn = get_connection(params)
    print("Connected!")

    # Use Databricks-specific setup
    setup = DatabricksSetup(conn, dbms_type="databricks")
    setup.run_setup(num_rows=DEFAULT_ROWS)

    conn.close()
    print("Done!")


if __name__ == "__main__":
    main()
