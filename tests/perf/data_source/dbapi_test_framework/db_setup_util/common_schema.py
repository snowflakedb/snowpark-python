#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
Common schema and deterministic data generator for DBAPI test tables.
All DBMS will have the same table with identical data.
"""

import random
import datetime
import uuid

# Table configuration
TABLE_NAME = "DBAPI_TEST_TABLE"
DEFAULT_ROWS = 10_000
SEED = 42

# 15 columns that all DBMS support (excluding auto-increment id)
COLUMN_NAMES = [
    "int_col",
    "bigint_col",
    "smallint_col",
    "float_col",
    "decimal_col",
    "boolean_col",
    "varchar_col",
    "char_col",
    "text_col",
    "date_col",
    "timestamp_col",
    "binary_col",
    "json_col",
    "uuid_col",
]

# Type mappings for each DBMS
TYPE_MAPPINGS = {
    "mysql": {
        "id": "INT AUTO_INCREMENT PRIMARY KEY",
        "int_col": "INT",
        "bigint_col": "BIGINT",
        "smallint_col": "SMALLINT",
        "float_col": "FLOAT",
        "decimal_col": "DECIMAL(10, 2)",
        "boolean_col": "BOOLEAN",
        "varchar_col": "VARCHAR(100)",
        "char_col": "CHAR(10)",
        "text_col": "TEXT",
        "date_col": "DATE",
        "timestamp_col": "TIMESTAMP",
        "binary_col": "BINARY(16)",
        "json_col": "JSON",
        "uuid_col": "VARCHAR(36)",
    },
    "postgres": {
        "id": "SERIAL PRIMARY KEY",
        "int_col": "INTEGER",
        "bigint_col": "BIGINT",
        "smallint_col": "SMALLINT",
        "float_col": "DOUBLE PRECISION",
        "decimal_col": "DECIMAL(10, 2)",
        "boolean_col": "BOOLEAN",
        "varchar_col": "VARCHAR(100)",
        "char_col": "CHAR(10)",
        "text_col": "TEXT",
        "date_col": "DATE",
        "timestamp_col": "TIMESTAMP",
        "binary_col": "BYTEA",
        "json_col": "JSONB",
        "uuid_col": "VARCHAR(36)",
    },
    "oracle": {
        "id": "NUMBER GENERATED AS IDENTITY PRIMARY KEY",
        "int_col": "NUMBER(10)",
        "bigint_col": "NUMBER(19)",
        "smallint_col": "NUMBER(5)",
        "float_col": "BINARY_DOUBLE",
        "decimal_col": "NUMBER(10, 2)",
        "boolean_col": "NUMBER(1)",
        "varchar_col": "VARCHAR2(100)",
        "char_col": "CHAR(10)",
        "text_col": "CLOB",
        "date_col": "DATE",
        "timestamp_col": "TIMESTAMP",
        "binary_col": "RAW(16)",
        "json_col": "CLOB",
        "uuid_col": "VARCHAR2(36)",
    },
    "mssql": {
        "id": "INT IDENTITY(1,1) PRIMARY KEY",
        "int_col": "INT",
        "bigint_col": "BIGINT",
        "smallint_col": "SMALLINT",
        "float_col": "FLOAT",
        "decimal_col": "DECIMAL(10, 2)",
        "boolean_col": "BIT",
        "varchar_col": "VARCHAR(100)",
        "char_col": "CHAR(10)",
        "text_col": "TEXT",
        "date_col": "DATE",
        "timestamp_col": "DATETIME2",
        "binary_col": "BINARY(16)",
        "json_col": "NVARCHAR(MAX)",
        "uuid_col": "VARCHAR(36)",
    },
    "databricks": {
        "id": "BIGINT GENERATED ALWAYS AS IDENTITY",  # No PRIMARY KEY (requires Unity Catalog)
        "int_col": "INT",
        "bigint_col": "BIGINT",
        "smallint_col": "SMALLINT",
        "float_col": "DOUBLE",
        "decimal_col": "DECIMAL(10, 2)",
        "boolean_col": "BOOLEAN",
        "varchar_col": "STRING",
        "char_col": "STRING",
        "text_col": "STRING",
        "date_col": "DATE",
        "timestamp_col": "TIMESTAMP",
        "binary_col": "BINARY",
        "json_col": "STRING",
        "uuid_col": "STRING",
    },
}


def generate_row_data(row_num):
    """
    Generate deterministic test data for a given row number.
    Same row_num always produces same values across all runs.

    Args:
        row_num: Row number (0-based)

    Returns:
        Tuple of 14 values (excluding auto-increment id)
    """
    # Set seed based on row number for deterministic data
    rng = random.Random(SEED + row_num)

    # Generate exact same data for this row_num every time
    int_col = rng.randint(1, 100000)
    bigint_col = rng.randint(1, 9999999999)
    smallint_col = rng.randint(1, 30000)
    float_col = round(rng.uniform(1.0, 10000.0), 4)
    decimal_col = round(rng.uniform(1.0, 10000.0), 2)
    boolean_col = rng.choice([True, False])

    # String data
    varchar_col = f"varchar_{row_num}_{rng.randint(1000, 9999)}"
    char_col = f"char{row_num % 1000}".ljust(10)[:10]
    text_col = f"Text content for row {row_num}. " + "Lorem ipsum " * rng.randint(5, 15)

    # Date/time data
    date_col = datetime.date(2024, (row_num % 12) + 1, (row_num % 28) + 1)
    timestamp_col = datetime.datetime(
        2024,
        (row_num % 12) + 1,
        (row_num % 28) + 1,
        row_num % 24,
        row_num % 60,
        row_num % 60,
    )

    # Binary data - deterministic 16 bytes
    binary_col = bytes(((row_num + i) * 17) % 256 for i in range(16))

    # JSON data as string (some DBMS need string, others support JSON)
    json_col = f'{{"row": {row_num}, "value": {rng.randint(1, 1000)}, "key": "test_{row_num}"}}'

    # UUID - deterministic based on row_num
    uuid_bytes = bytes(((row_num + i) * 13) % 256 for i in range(16))
    uuid_col = str(uuid.UUID(bytes=uuid_bytes))

    return (
        int_col,
        bigint_col,
        smallint_col,
        float_col,
        decimal_col,
        boolean_col,
        varchar_col,
        char_col,
        text_col,
        date_col,
        timestamp_col,
        binary_col,
        json_col,
        uuid_col,
    )


def print_sample_data():
    """Print sample data for verification."""
    print(f"Sample data for {TABLE_NAME}:")
    print(f"Columns: {', '.join(COLUMN_NAMES)}")
    print("\nFirst 3 rows:")
    for i in range(3):
        data = generate_row_data(i)
        print(f"Row {i}: {data[:5]}...")  # Print first 5 values


if __name__ == "__main__":
    print_sample_data()
