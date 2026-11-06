#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Tests for explode() and flatten() table functions in local testing mode.

This addresses GitHub issue #3565 (SNOW-2213161) where explode() with local
testing fails with AttributeError: 'MockSelectStatement' object has no
attribute 'snowflake_plan'.
"""

import json

import pytest

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, explode, explode_outer, flatten
from snowflake.snowpark.types import VariantType


def parse_variant(value):
    """Parse a VARIANT value - it may be JSON encoded or raw."""
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value
    return value


@pytest.fixture(scope="module")
def session():
    session = Session.builder.config("local_testing", True).create()
    yield session
    session.close()


class TestExplodeBasic:
    """Test basic explode functionality with arrays."""

    def test_explode_simple_array(self, session):
        """Test the exact reproduction case from issue #3565."""
        data = [{"foo": "bar", "my_array": ["one", "two"]}]
        df = session.create_dataframe(data, schema=["foo", "my_array"])
        result = df.select("foo", explode("my_array")).collect()

        assert len(result) == 2
        assert result[0]["FOO"] == "bar"
        # VALUE is VARIANT type, so may be JSON-encoded
        assert parse_variant(result[0]["VALUE"]) == "one"
        assert result[1]["FOO"] == "bar"
        assert parse_variant(result[1]["VALUE"]) == "two"

    def test_explode_multiple_rows(self, session):
        """Test explode with multiple input rows."""
        data = [
            {"idx": 1, "arr": ["a", "b"]},
            {"idx": 2, "arr": ["x", "y", "z"]},
        ]
        df = session.create_dataframe(data, schema=["idx", "arr"])
        result = df.select("idx", explode("arr")).collect()

        assert len(result) == 5
        # Check we get correct combinations
        values_by_idx = {}
        for row in result:
            idx = row["IDX"]
            if idx not in values_by_idx:
                values_by_idx[idx] = []
            values_by_idx[idx].append(parse_variant(row["VALUE"]))

        assert sorted(values_by_idx[1]) == ["a", "b"]
        assert sorted(values_by_idx[2]) == ["x", "y", "z"]

    def test_explode_with_integers(self, session):
        """Test explode with integer arrays."""
        data = [{"id": 1, "nums": [10, 20, 30]}]
        df = session.create_dataframe(data, schema=["id", "nums"])
        result = df.select("id", explode("nums")).collect()

        assert len(result) == 3
        values = [parse_variant(row["VALUE"]) for row in result]
        assert sorted(values) == [10, 20, 30]

    def test_explode_schema_has_value_column(self, session):
        """Test that explode result has correct schema."""
        data = [{"arr": ["a", "b"]}]
        df = session.create_dataframe(data, schema=["arr"])
        result_df = df.select(explode("arr"))

        # Should have VALUE column of VARIANT type
        field_names = [f.name for f in result_df.schema.fields]
        assert "VALUE" in field_names

        # VALUE should be VARIANT type
        value_field = next(f for f in result_df.schema.fields if f.name == "VALUE")
        assert isinstance(value_field.datatype, VariantType)


class TestExplodeWithMaps:
    """Test explode functionality with maps/dictionaries."""

    def test_explode_simple_map(self, session):
        """Test explode with a dictionary/map column."""
        data = [{"name": "Alice", "scores": {"math": 90, "science": 85}}]
        df = session.create_dataframe(data, schema=["name", "scores"])
        result = df.select("name", explode("scores")).collect()

        assert len(result) == 2
        keys = {row["KEY"] for row in result}
        assert keys == {"math", "science"}

        for row in result:
            assert row["NAME"] == "Alice"
            if row["KEY"] == "math":
                assert parse_variant(row["VALUE"]) == 90
            elif row["KEY"] == "science":
                assert parse_variant(row["VALUE"]) == 85


class TestExplodeOuter:
    """Test explode_outer which handles empty/null arrays."""

    def test_explode_outer_with_empty_array(self, session):
        """Test explode_outer produces NULL for empty arrays."""
        data = [
            {"idx": 1, "arr": [1, 2]},
            {"idx": 2, "arr": []},
            {"idx": 3, "arr": [3]},
        ]
        df = session.create_dataframe(data, schema=["idx", "arr"])
        result = df.select("idx", explode_outer("arr")).sort("idx").collect()

        # idx=1 should have 2 rows, idx=2 should have 1 NULL row, idx=3 should have 1 row
        idx_counts = {}
        for row in result:
            idx = row["IDX"]
            idx_counts[idx] = idx_counts.get(idx, 0) + 1

        assert idx_counts[1] == 2
        assert idx_counts[2] == 1  # NULL row for empty array
        assert idx_counts[3] == 1

    def test_explode_outer_with_null_array(self, session):
        """Test explode_outer produces NULL for NULL arrays."""
        data = [
            {"idx": 1, "arr": ["a"]},
            {"idx": 2, "arr": None},
        ]
        df = session.create_dataframe(data, schema=["idx", "arr"])
        result = df.select("idx", explode_outer("arr")).sort("idx").collect()

        assert len(result) == 2
        # First row should have value
        assert result[0]["IDX"] == 1
        assert parse_variant(result[0]["VALUE"]) == "a"
        # Second row should have NULL value
        assert result[1]["IDX"] == 2
        assert result[1]["VALUE"] is None


# Note: Alias tests are skipped as alias handling for table functions requires
# additional work in the mock framework.


class TestExplodeWithOriginalColumn:
    """Test that explode preserves other columns from the original dataframe."""

    def test_explode_preserves_original_columns(self, session):
        """Ensure explode joins correctly with original dataframe columns."""
        data = [
            {"id": 1, "name": "Alice", "items": ["a", "b"]},
            {"id": 2, "name": "Bob", "items": ["x"]},
        ]
        df = session.create_dataframe(data, schema=["id", "name", "items"])
        result = df.select("id", "name", explode("items")).collect()

        # Should have 3 rows total
        assert len(result) == 3

        # Check columns are present
        assert "ID" in result[0].as_dict()
        assert "NAME" in result[0].as_dict()
        assert "VALUE" in result[0].as_dict()

        # Check values are correctly joined
        alice_rows = [r for r in result if r["NAME"] == "Alice"]
        bob_rows = [r for r in result if r["NAME"] == "Bob"]

        assert len(alice_rows) == 2
        assert len(bob_rows) == 1


class TestFlattenDirect:
    """Test the flatten() function directly."""

    def test_flatten_basic(self, session):
        """Test basic flatten functionality."""
        data = [{"arr": [1, 2, 3]}]
        df = session.create_dataframe(data, schema=["arr"])
        result = df.select(flatten(col("arr"))).select("value").collect()

        values = [parse_variant(row["VALUE"]) for row in result]
        assert sorted(values) == [1, 2, 3]

    def test_flatten_with_outer(self, session):
        """Test flatten with outer=True."""
        data = [
            {"arr": [1]},
            {"arr": []},
        ]
        df = session.create_dataframe(data, schema=["arr"])
        result = df.select(flatten(col("arr"), outer=True)).select("value").collect()

        # Should have 2 rows: one for the value, one NULL for empty array
        assert len(result) == 2


class TestFlattenUnsupportedParameters:
    """Test that unsupported FLATTEN parameters raise NotImplementedError."""

    def test_flatten_with_path_raises_error(self, session):
        """Test that path parameter raises NotImplementedError."""
        data = [{"obj": {"nested": {"value": 1}}}]
        df = session.create_dataframe(data, schema=["obj"])

        with pytest.raises(NotImplementedError) as exc_info:
            df.select(flatten(col("obj"), path="nested")).collect()

        assert "PATH parameter is not supported" in str(exc_info.value)
        assert "local testing" in str(exc_info.value)

    def test_flatten_with_recursive_raises_error(self, session):
        """Test that recursive=True raises NotImplementedError."""
        data = [{"arr": [[1, 2], [3, 4]]}]
        df = session.create_dataframe(data, schema=["arr"])

        with pytest.raises(NotImplementedError) as exc_info:
            df.select(flatten(col("arr"), recursive=True)).collect()

        assert "RECURSIVE=True is not supported" in str(exc_info.value)
        assert "local testing" in str(exc_info.value)


class TestUDTFNotAffected:
    """Test that non-flatten UDTFs still work correctly after the fix."""

    def test_custom_udtf_still_works(self, session):
        """Verify that custom UDTFs are not affected by the flatten fix.

        This test ensures the early-return pattern for flatten doesn't
        break the existing UDTF code path.
        """
        from snowflake.snowpark.functions import lit
        from snowflake.snowpark.types import IntegerType, StructField, StructType

        # Define a simple UDTF
        class Repeater:
            def process(self, value, times):
                for _ in range(times):
                    yield (value,)

        output_schema = StructType([StructField("REPEATED", IntegerType())])

        # Register the UDTF
        repeater_udtf = session.udtf.register(
            Repeater,
            output_schema=output_schema,
            input_types=[IntegerType(), IntegerType()],
            name="test_repeater",
        )

        # Call with literals (supported pattern in mock)
        result = session.table_function(repeater_udtf(lit(42), lit(3))).collect()

        assert len(result) == 3
        for row in result:
            assert parse_variant(row["REPEATED"]) == 42
