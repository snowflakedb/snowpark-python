#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
from snowflake.snowpark.functions import col, fl_get_file_type
from snowflake.snowpark.types import FileType
from tests.utils import Utils, TestFiles


pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="list command not supported in local testing mode",
    ),
]


def test_file_basic(session, resources_path):
    """Test basic file listing functionality."""
    test_stage = Utils.random_stage_name()
    Utils.create_stage(session, test_stage, is_temporary=True)

    try:
        # Upload test files
        test_files = TestFiles(resources_path)
        session.file.put(
            test_files.test_file_csv, f"@{test_stage}", auto_compress=False
        )
        session.file.put(
            test_files.test_file_json, f"@{test_stage}", auto_compress=False
        )
        df = session.read.file(f"@{test_stage}")

        # Verify column name and type
        assert len(df.schema.fields) == 1
        assert df.schema.fields[0].name == "FILE"
        assert isinstance(df.schema.fields[0].datatype, FileType)

        # Check that we got both files
        result = df.collect()
        assert len(result) == 2
        file_paths = [row["FILE"] for row in result]
        # FILE column contains full stage paths
        assert any("testCSV.csv" in path for path in file_paths)
        assert any("testJson.json" in path for path in file_paths)
    finally:
        Utils.drop_stage(session, test_stage)


def test_file_with_pattern(session, resources_path):
    """Test file listing with pattern matching."""
    test_stage = Utils.random_stage_name()
    Utils.create_stage(session, test_stage, is_temporary=True)

    try:
        # Upload various file types
        test_files = TestFiles(resources_path)
        session.file.put(
            test_files.test_file_csv, f"@{test_stage}", auto_compress=False
        )
        session.file.put(
            test_files.test_file_csv_header, f"@{test_stage}", auto_compress=False
        )
        session.file.put(
            test_files.test_file_json, f"@{test_stage}", auto_compress=False
        )
        session.file.put(
            test_files.test_file_xml, f"@{test_stage}", auto_compress=False
        )

        # Test CSV pattern
        df_csv = session.read.option("pattern", r".*\.csv").file(f"@{test_stage}")
        csv_files = df_csv.collect()
        assert len(csv_files) == 2
        for row in csv_files:
            assert ".csv" in row["FILE"]
            assert ".json" not in row["FILE"]
            assert ".xml" not in row["FILE"]

        # Test JSON pattern
        df_json = session.read.option("pattern", r".*\.json").file(f"@{test_stage}")
        json_files = df_json.collect()
        assert len(json_files) == 1
        assert ".json" in json_files[0]["FILE"]

        # Test header pattern
        df_header = session.read.option("pattern", ".*header.*").file(f"@{test_stage}")
        header_files = df_header.collect()
        assert len(header_files) == 1
        assert "header" in header_files[0]["FILE"]
    finally:
        Utils.drop_stage(session, test_stage)


def test_file_with_subdirectories(session, resources_path):
    """Test file listing with subdirectories."""
    test_stage = Utils.random_stage_name()
    Utils.create_stage(session, test_stage, is_temporary=True)

    try:
        # Upload files to subdirectories
        test_files = TestFiles(resources_path)
        session.file.put(
            test_files.test_file_csv, f"@{test_stage}/data/", auto_compress=False
        )
        session.file.put(
            test_files.test_file_json, f"@{test_stage}/data/", auto_compress=False
        )
        session.file.put(
            test_files.test_file_xml, f"@{test_stage}/config/", auto_compress=False
        )

        # List all files in stage
        df_all = session.read.file(f"@{test_stage}")
        all_files = df_all.collect()
        assert len(all_files) == 3

        # List files in specific subdirectory
        df_data = session.read.file(f"@{test_stage}/data/")
        data_files = df_data.collect()
        assert len(data_files) == 2
        # FILE column contains stage paths like @stage/data/file.csv
        for row in data_files:
            file_path = row["FILE"]
            assert "data/" in file_path or "/data/" in file_path

        # List files in config subdirectory
        df_config = session.read.file(f"@{test_stage}/config/")
        config_files = df_config.collect()
        assert len(config_files) == 1
        file_path = config_files[0]["FILE"]
        assert "config/" in file_path or "/config/" in file_path
    finally:
        Utils.drop_stage(session, test_stage)


def test_file_empty_stage(session):
    """Test file listing on an empty stage."""
    empty_stage = Utils.random_stage_name()
    Utils.create_stage(session, empty_stage, is_temporary=True)

    try:
        df = session.read.file(f"@{empty_stage}")
        result = df.collect()
        assert len(result) == 0
    finally:
        Utils.drop_stage(session, empty_stage)


def test_file_compressed_files(session, resources_path):
    """Test file listing with compressed files."""
    test_stage = Utils.random_stage_name()
    Utils.create_stage(session, test_stage, is_temporary=True)

    try:
        # Upload with compression
        test_files = TestFiles(resources_path)
        session.file.put(test_files.test_file_csv, f"@{test_stage}", auto_compress=True)
        session.file.put(
            test_files.test_file_json, f"@{test_stage}", auto_compress=True
        )

        df = session.read.file(f"@{test_stage}")
        result = df.collect()

        # Compressed files should have .gz extension
        assert len(result) == 2
        for row in result:
            assert ".gz" in row["FILE"]
    finally:
        Utils.drop_stage(session, test_stage)


def test_file_invalid_stage_path(session):
    """Test error handling for invalid stage paths."""
    with pytest.raises(ValueError, match="invalid Snowflake stage location"):
        session.read.file("/invalid/path")

    with pytest.raises(ValueError, match="invalid Snowflake stage location"):
        session.read.file("not_a_stage")

    # Valid prefixes should not raise errors
    try:
        session.read.file("@mystage")  # This will fail later if stage doesn't exist
    except ValueError:
        pytest.fail("Valid stage prefix should not raise ValueError")


def test_file_pattern_no_matches(session, resources_path):
    """Test pattern that matches no files."""
    test_stage = Utils.random_stage_name()
    Utils.create_stage(session, test_stage, is_temporary=True)

    try:
        # Upload some files
        test_files = TestFiles(resources_path)
        session.file.put(
            test_files.test_file_csv, f"@{test_stage}", auto_compress=False
        )
        session.file.put(
            test_files.test_file_json, f"@{test_stage}", auto_compress=False
        )

        # Pattern that matches nothing
        df = session.read.option("pattern", r".*\.txt").file(f"@{test_stage}")
        result = df.collect()
        assert len(result) == 0
    finally:
        Utils.drop_stage(session, test_stage)


def test_file_complex_patterns(session, resources_path):
    """Test various complex regex patterns."""
    # Create a fresh stage for this test
    test_stage = Utils.random_stage_name()
    Utils.create_stage(session, test_stage, is_temporary=True)

    try:
        # Upload files with different naming patterns
        test_files = TestFiles(resources_path)
        files_to_upload = [
            test_files.test_file_csv,
            test_files.test_file_csv_header,
            test_files.test_file2_csv,
            test_files.test_file_json,
        ]

        for file_path in files_to_upload:
            session.file.put(file_path, f"@{test_stage}", auto_compress=False)

        # Pattern: files ending with ".csv"
        df1 = session.read.option("pattern", r".*\.csv").file(f"@{test_stage}")
        result1 = df1.collect()
        assert len(result1) == 3  # testCSV.csv, testCSVheader.csv, test2CSV.csv

        # Pattern: files containing "Json" (case sensitive)
        df2 = session.read.option("pattern", ".*Json.*").file(f"@{test_stage}")
        result2 = df2.collect()
        assert len(result2) == 1  # testJson.json

        # Pattern: files containing "header"
        df3 = session.read.option("pattern", ".*header.*").file(f"@{test_stage}")
        result3 = df3.collect()
        assert len(result3) == 1  # testCSVheader.csv
    finally:
        Utils.drop_stage(session, test_stage)


def test_file_chained_operations(session, resources_path):
    """Test chaining operations after file listing."""
    test_stage = Utils.random_stage_name()
    Utils.create_stage(session, test_stage, is_temporary=True)

    try:
        # Upload files
        test_files = TestFiles(resources_path)
        session.file.put(
            test_files.test_file_csv, f"@{test_stage}", auto_compress=False
        )

        # Test selecting and aliasing
        df_csv = session.read.file(f"@{test_stage}")
        df_alias = df_csv.select(
            col("FILE").alias("CSV_FILE_PATH"),
            fl_get_file_type("FILE").alias("FILE_TYPE"),
        )
        result = df_alias.collect()
        assert "testCSV.csv" in result[0]["CSV_FILE_PATH"]
        assert result[0]["FILE_TYPE"] == "document"
    finally:
        Utils.drop_stage(session, test_stage)


def test_file_special_characters_in_filenames(session, resources_path):
    """Test handling files with special characters in names."""
    special_stage = Utils.random_stage_name()
    Utils.create_stage(session, special_stage, is_temporary=True)

    try:
        # Upload a file with special characters if available
        test_files = TestFiles(resources_path)
        session.file.put(
            test_files.test_file_with_special_characters_parquet,
            f"@{special_stage}",
            auto_compress=False,
        )

        df = session.read.file(f"@{special_stage}")
        result = df.collect()

        assert len(result) == 1
        assert "special_characters" in result[0]["FILE"]
    finally:
        Utils.drop_stage(session, special_stage)


@pytest.mark.parametrize("stage_prefix", ["@", "@~", "@%"])
def test_file_different_stage_prefixes(session, resources_path, stage_prefix):
    """Test file listing with different stage prefix formats."""
    if stage_prefix == "@":
        # For regular stage, use the temp_stage fixture
        stage_name = Utils.random_stage_name()
        Utils.create_stage(session, stage_name, is_temporary=True)
        stage_path = f"@{stage_name}"

        try:
            test_files = TestFiles(resources_path)
            session.file.put(test_files.test_file_json, stage_path, auto_compress=False)
            df = session.read.file(stage_path)
            result = df.collect()
            assert len(result) >= 1
        finally:
            Utils.drop_stage(session, stage_name)
    else:
        # For user/table stages, just verify the path validation works
        try:
            # This may fail if the stage doesn't exist, but shouldn't fail validation
            session.read.file(f"{stage_prefix}mystage")
        except Exception as e:
            # Should not be a ValueError about invalid stage location
            assert "invalid Snowflake stage location" not in str(e)


def test_file_pattern_escape_single_quotes(session, resources_path):
    """Test that patterns with single quotes are properly escaped to prevent SQL injection."""
    test_stage = Utils.random_stage_name()
    Utils.create_stage(session, test_stage, is_temporary=True)

    try:
        # Upload a test file
        test_files = TestFiles(resources_path)
        session.file.put(
            test_files.test_file_csv, f"@{test_stage}", auto_compress=False
        )

        # Test patterns with single quotes that could cause SQL injection if not escaped
        dangerous_patterns = [
            "'; DROP TABLE users; --",
            "test' OR '1'='1",
            ".*'.csv",
        ]

        for pattern in dangerous_patterns:
            # This should not raise SQL syntax errors - quotes should be escaped
            df = session.read.option("pattern", pattern).file(f"@{test_stage}")
            result = df.collect()
            # These patterns won't match our test file, but shouldn't cause SQL errors
            assert len(result) == 0

        # Verify a normal pattern still works
        df = session.read.option("pattern", r".*\.csv").file(f"@{test_stage}")
        result = df.collect()
        assert len(result) == 1
        assert "testCSV.csv" in result[0]["FILE"]

    finally:
        Utils.drop_stage(session, test_stage)
