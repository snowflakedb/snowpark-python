#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""Tests for issue #4076: Filter on empty dataframe results in schemaless dataframe."""

from snowflake.snowpark.functions import col, lit
from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType


def test_filter_empty_dataframe_preserves_schema(session):
    """Filter on empty dataframe should preserve schema (issue #4076)."""
    empty_schema = StructType([
        StructField("entry", StringType(), True),
        StructField("file_path", StringType(), True),
    ])
    mock_encounters_df = session.create_dataframe([], schema=empty_schema)

    # Before filter: should have 2 columns
    assert len(mock_encounters_df.schema.fields) == 2
    assert [f.name for f in mock_encounters_df.schema.fields] == ["ENTRY", "FILE_PATH"]

    # After filter: should preserve the same schema
    filtered_df = mock_encounters_df.filter(col("entry").is_null())
    assert len(filtered_df.schema.fields) == 2, (
        "Filtered empty dataframe should retain schema (ENTRY, FILE_PATH)"
    )
    assert [f.name for f in filtered_df.schema.fields] == ["ENTRY", "FILE_PATH"]

    # Both should be empty
    assert mock_encounters_df.count() == 0
    assert filtered_df.count() == 0


def test_filter_empty_dataframe_various_conditions(session):
    """Various filter conditions on empty dataframe should all preserve schema."""
    empty_schema = StructType([
        StructField("entry", StringType(), True),
        StructField("file_path", StringType(), True),
    ])
    df = session.create_dataframe([], schema=empty_schema)

    # is_null
    assert len(df.filter(col("entry").is_null()).schema.fields) == 2

    # is_not_null
    assert len(df.filter(col("entry").is_not_null()).schema.fields) == 2

    # equals
    assert len(df.filter(col("entry") == lit("x")).schema.fields) == 2

    # not equals
    assert len(df.filter(col("entry") != lit("x")).schema.fields) == 2

    # Chain filters
    chained = df.filter(col("entry").is_null()).filter(col("file_path").is_null())
    assert len(chained.schema.fields) == 2


def test_select_lit_filter_preserves_projection_not_source(session):
    """select(lit(1)).filter() must keep projected columns only, not original schema."""
    schema = StructType([
        StructField("a", IntegerType()),
        StructField("b", StringType()),
    ])
    df = session.create_dataframe([], schema=schema)
    result = df.select(lit(1).alias("x")).filter(col("x") > 0)
    assert len(result.schema.fields) == 1
    assert result.schema.fields[0].name == "X"


def test_filter_empty_dataframe_with_three_columns(session):
    """Filter on empty dataframe with more columns preserves schema."""
    schema = StructType([
        StructField("a", IntegerType()),
        StructField("b", StringType()),
        StructField("c", IntegerType()),
    ])
    df = session.create_dataframe([], schema=schema)
    filtered = df.filter(col("a") > 0)
    assert len(filtered.schema.fields) == 3
    assert [f.name for f in filtered.schema.fields] == ["A", "B", "C"]
