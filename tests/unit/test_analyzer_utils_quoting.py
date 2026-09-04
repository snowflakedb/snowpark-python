#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Tests for single-quote handling in analyzer_utils SQL builders.

These verify that single quotes embedded in user-provided values are doubled
so the value stays a single, well-formed SQL string literal, and that values
which are already single-quoted are passed through unchanged.
"""


from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    copy_into_location,
    escape_location_literal,
    infer_schema_statement,
    single_quote,
)


class TestSingleQuote:
    """single_quote() wraps values and doubles embedded single quotes."""

    def test_embedded_single_quote_is_doubled(self):
        """A value containing ' produces '' inside the quoted string."""
        result = single_quote("it's")
        assert result == "'it''s'"

    def test_embedded_quote_stays_inside_literal(self):
        """Embedded quotes are doubled so the value remains one literal."""
        value = "a') b"
        result = single_quote(value)
        assert result == "'a'') b'"
        # Every single quote inside the wrapped value is doubled.
        inner = result[1:-1]
        assert "'" not in inner or "''" in inner

    def test_already_quoted_value_is_returned_as_is(self):
        """A value that is already single-quoted is returned unchanged."""
        value = "'already quoted'"
        assert single_quote(value) == value


class TestInferSchemaStatementQuoting:
    """Stage path and file format name in INFER_SCHEMA are quoted."""

    def test_path_with_single_quote_is_doubled(self):
        """A stage path containing ' stays inside the SQL string literal."""
        result = infer_schema_statement(
            path="@stage/o'brien",
            file_format_name="my_format",
        )
        assert "LOCATION  => '@stage/o''brien'," in result

    def test_format_name_with_single_quote_is_doubled(self):
        """file_format_name containing ' stays inside the literal."""
        result = infer_schema_statement(
            path="@stage/data",
            file_format_name="my'fmt",
        )
        assert "FILE_FORMAT  => 'my'fmt'" not in result
        assert "FILE_FORMAT  => 'my''fmt'" in result

    def test_already_quoted_path_is_not_double_escaped(self):
        """Regression: a path that is already a valid single-quoted SQL literal
        must be passed through verbatim, not re-escaped.

        normalize_path() wraps stage paths in single quotes and escapes embedded
        quotes (e.g. ``'@stage/o\\'brien'``). Stripping and re-quoting such a
        value would double-escape the already-escaped quote and corrupt the path.
        """
        # Backslash-escaped form produced by utils.normalize_path()
        backslash_quoted = "'@stage/o\\'brien'"
        result = infer_schema_statement(
            path=backslash_quoted,
            file_format_name="my_format",
        )
        assert "LOCATION  => '@stage/o\\'brien'," in result

        # SQL-doubled form is likewise preserved as-is
        doubled_quoted = "'@stage/o''brien'"
        result = infer_schema_statement(
            path=doubled_quoted,
            file_format_name="my_format",
        )
        assert "LOCATION  => '@stage/o''brien'," in result

        # A plain quoted path (e.g. used for spaces) is preserved as-is
        spaced_quoted = "'@stage/my data'"
        result = infer_schema_statement(
            path=spaced_quoted,
            file_format_name="my_format",
        )
        assert "LOCATION  => '@stage/my data'," in result


class TestCopyFilesQuoting:
    """COPY FILES target and source locations are quoted."""

    def test_stage_location_with_single_quote_is_doubled(self):
        """A target location containing ' is quoted in COPY FILES."""
        result = file_operation_statement_copy_files(stage_location="@stage/o'brien")
        assert "'@stage/o''brien'" in result

    def test_source_file_name_with_single_quote_is_doubled(self):
        """A source location containing ' is quoted in COPY FILES."""
        result = file_operation_statement_copy_files(
            stage_location="@dest_stage",
            file_name="@source/o'brien",
        )
        assert "'@source/o''brien'" in result


class TestEscapeLocationLiteral:
    """escape_location_literal() passes through well-formed literals and
    fully escapes anything else (including look-alike quoted strings)."""

    def test_plain_quoted_value_is_preserved(self):
        assert escape_location_literal("'@stage/data'") == "'@stage/data'"

    def test_value_with_spaces_is_preserved(self):
        assert escape_location_literal("'@stage/my data'") == "'@stage/my data'"

    def test_doubled_interior_quote_is_preserved(self):
        assert escape_location_literal("'@stage/o''brien'") == "'@stage/o''brien'"

    def test_backslash_escaped_interior_quote_is_preserved(self):
        # Form produced by utils.normalize_path()
        assert escape_location_literal("'@stage/o\\'brien'") == "'@stage/o\\'brien'"

    def test_empty_literal_is_preserved(self):
        assert escape_location_literal("''") == "''"

    def test_literal_containing_single_quote_is_preserved(self):
        assert escape_location_literal("''''") == "''''"

    def test_unescaped_interior_quote_breakout_is_escaped(self):
        # Starts and ends with a quote, but the interior quote terminates the
        # literal early -- it must be treated as unquoted and fully escaped.
        assert (
            escape_location_literal("'@x' trailing text --'")
            == "'''@x'' trailing text --'''"
        )

    def test_unterminated_literal_is_escaped(self):
        # Trailing backslash escapes the final quote, so the literal never closes.
        assert escape_location_literal("'abc\\'") == "'''abc\\'''"

    def test_unquoted_value_is_escaped(self):
        assert escape_location_literal("@stage/data") == "'@stage/data'"

    def test_too_short_value_is_escaped(self):
        assert escape_location_literal("'") == "''''"


class TestCopyIntoLocationQuoting:
    """stage_location in copy_into_location is quoted/escaped."""

    def test_embedded_single_quote_is_doubled(self):
        result = copy_into_location(
            query="SELECT * FROM source_table",
            stage_location="@stage/o'brien",
        )
        assert "'@stage/o''brien'" in result

    def test_already_quoted_location_is_preserved(self):
        result = copy_into_location(
            query="SELECT * FROM source_table",
            stage_location="'@stage/out/'",
        )
        assert " INTO '@stage/out/' FROM " in result

    def test_quote_wrapped_breakout_is_neutralized(self):
        """A location that would break out of the literal stays inside it."""
        value = "'s3://bucket/out/' FROM (SELECT * FROM other_table) --'"
        result = copy_into_location(
            query="SELECT * FROM source_table",
            stage_location=value,
        )
        # The whole value is wrapped into a single literal; the trailing
        # FROM/comment never escapes to top-level SQL.
        assert (
            "INTO '''s3://bucket/out/'' FROM (SELECT * FROM other_table) --'''"
            in result
        )


# Helper to call file_operation_statement for COPY FILES
def file_operation_statement_copy_files(
    stage_location: str = "@target_stage",
    file_name: str = "@source_stage",
) -> str:
    """Helper that calls file_operation_statement with command='copy_files'."""
    from snowflake.snowpark._internal.analyzer.analyzer_utils import (
        file_operation_statement,
    )

    return file_operation_statement(
        command="copy_files",
        file_name=file_name,
        stage_location=stage_location,
        options={},
    )
