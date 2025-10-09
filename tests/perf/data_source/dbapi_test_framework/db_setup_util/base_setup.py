#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Base class for database setup with common logic."""

# Support both direct execution and module import
try:
    from .common_schema import (
        TABLE_NAME,
        COLUMN_NAMES,
        TYPE_MAPPINGS,
        generate_row_data,
        DEFAULT_ROWS,
    )
except ImportError:
    from common_schema import (
        TABLE_NAME,
        COLUMN_NAMES,
        TYPE_MAPPINGS,
        generate_row_data,
        DEFAULT_ROWS,
    )


class DatabaseSetup:
    """Base class handling common setup logic for all DBMS."""

    def __init__(self, connection, dbms_type, placeholder_style="?") -> None:
        """
        Initialize database setup.

        Args:
            connection: DBAPI 2.0 connection object
            dbms_type: Type of DBMS (mysql, postgres, oracle, mssql, databricks)
            placeholder_style: Placeholder style ('?', '%s', or ':1' for positional)
        """
        self.connection = connection
        self.dbms_type = dbms_type.lower()
        self.placeholder_style = placeholder_style
        self.type_mapping = TYPE_MAPPINGS[self.dbms_type]

    def generate_create_table_sql(self, table_name=TABLE_NAME):
        """Generate CREATE TABLE SQL for this DBMS."""
        columns = ["id " + self.type_mapping["id"]]
        for col_name in COLUMN_NAMES:
            columns.append(f"{col_name} {self.type_mapping[col_name]}")

        columns_str = ",\n    ".join(columns)
        return f"CREATE TABLE {table_name} (\n    {columns_str}\n)"

    def create_table(self, table_name=TABLE_NAME, drop_if_exists=True):
        """Create test table."""
        cursor = self.connection.cursor()

        if drop_if_exists:
            self._drop_table(cursor, table_name)

        create_sql = self.generate_create_table_sql(table_name)
        cursor.execute(create_sql)
        self.connection.commit()
        print(f"Created table {table_name}")
        cursor.close()

    def _drop_table(self, cursor, table_name):
        """Drop table if exists (DBMS-specific syntax)."""
        try:
            if self.dbms_type == "oracle":
                cursor.execute(f"DROP TABLE {table_name}")
            else:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            print(f"Dropped existing table {table_name}")
        except Exception:
            pass  # Table doesn't exist

    def prepare_row_data(self, row):
        """Prepare row data for insertion (handle DBMS-specific conversions)."""
        if self.dbms_type == "oracle":
            # Convert boolean to 1/0
            row = list(row)
            row[5] = 1 if row[5] else 0  # boolean_col is at index 5
            return tuple(row)
        return row

    def generate_placeholders(self):
        """Generate placeholder string based on DBMS style."""
        if self.placeholder_style == ":":
            # Oracle style :1, :2, :3
            return ", ".join([f":{i+1}" for i in range(len(COLUMN_NAMES))])
        elif self.placeholder_style == "%s":
            # PostgreSQL/MySQL style
            return ", ".join(["%s"] * len(COLUMN_NAMES))
        else:
            # Standard ? style (pyodbc)
            return ", ".join(["?"] * len(COLUMN_NAMES))

    def insert_data(
        self, num_rows=DEFAULT_ROWS, table_name=TABLE_NAME, batch_size=1000
    ):
        """Insert deterministic test data."""
        cursor = self.connection.cursor()

        columns = ", ".join(COLUMN_NAMES)
        placeholders = self.generate_placeholders()
        insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Insert in batches
        for batch_start in range(0, num_rows, batch_size):
            batch_end = min(batch_start + batch_size, num_rows)
            batch_data = [
                self.prepare_row_data(generate_row_data(i))
                for i in range(batch_start, batch_end)
            ]
            cursor.executemany(insert_sql, batch_data)
            self.connection.commit()
            print(f"Inserted rows {batch_start} to {batch_end-1}")

        # Verify count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"Total rows in {table_name}: {count}")
        cursor.close()

    def run_setup(self, num_rows=DEFAULT_ROWS, table_name=TABLE_NAME):
        """Run complete setup (create table + insert data)."""
        self.create_table(table_name)
        self.insert_data(num_rows, table_name)
