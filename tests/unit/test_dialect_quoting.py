#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""Tests for identifier quoting in dialect generate_select_query().

Column names come from ``cursor.description`` and are interpolated into the
generated SELECT statements. These tests verify that embedded identifier-quote
characters in those names are doubled, so each name stays a single, well-formed
quoted identifier and any embedded ``;`` remains inside the identifier.
"""

import re


from snowflake.snowpark._internal.data_source.dbms_dialects.base_dialect import (
    BaseDialect,
)
from snowflake.snowpark._internal.data_source.dbms_dialects.databricks_dialect import (
    DatabricksDialect,
)
from snowflake.snowpark._internal.data_source.dbms_dialects.mysql_dialect import (
    MysqlDialect,
)
from snowflake.snowpark._internal.data_source.dbms_dialects.oracledb_dialect import (
    OracledbDialect,
)
from snowflake.snowpark._internal.data_source.dbms_dialects.postgresql_dialect import (
    PostgresDialect,
)
from snowflake.snowpark._internal.data_source.dbms_dialects.sqlserver_dialect import (
    SqlServerDialect,
)
from snowflake.snowpark._internal.data_source.drivers.psycopg2_driver import (
    Psycopg2TypeCode,
)
from snowflake.snowpark.types import StringType, StructField, StructType


def _semicolons_outside_identifiers(query: str) -> bool:
    """Return True if there are semicolons outside double-quoted identifiers.

    A properly quoted identifier is "..." where internal " are doubled. If the
    quoting is broken (unescaped "), an embedded semicolon leaks out.
    """
    # Strip all properly-balanced double-quoted identifiers
    stripped = re.sub(r'"(?:[^"]|"")*"', "IDENT", query)
    return ";" in stripped


def _semicolons_outside_backtick_identifiers(query: str) -> bool:
    """Return True if there are semicolons outside of backtick-quoted identifiers."""
    stripped = re.sub(r"`(?:[^`]|``)*`", "IDENT", query)
    return ";" in stripped


class TestPostgresDialectQuoting:
    """Embedded double-quotes in column names must be doubled (PostgreSQL)."""

    EMBEDDED_QUOTE_COL = 'y"; x'

    def _build_schema_and_raw(self):
        schema = StructType(
            [
                StructField("x", StringType()),
                StructField("y", StringType()),
            ]
        )
        # Simulate cursor.description: (name, type_code, ...)
        raw_schema = [
            ("x", Psycopg2TypeCode.VARCHAROID.value),
            (self.EMBEDDED_QUOTE_COL, Psycopg2TypeCode.VARCHAROID.value),
        ]
        return schema, raw_schema

    def test_embedded_quote_keeps_semicolon_inside_identifier(self):
        """The generated query must NOT have semicolons outside quoted identifiers."""
        schema, raw_schema = self._build_schema_and_raw()

        query = PostgresDialect.generate_select_query(
            table_or_query="schema1.table1",
            schema=schema,
            raw_schema=raw_schema,
            is_query=False,
            query_input_alias="t",
        )

        assert not _semicolons_outside_identifiers(
            query
        ), f"Generated query has semicolons outside identifier boundaries:\n{query}"

    def test_embedded_quote_in_subquery_mode(self):
        """is_query=True path (alias emission) must also stay well-formed."""
        schema, raw_schema = self._build_schema_and_raw()

        query = PostgresDialect.generate_select_query(
            table_or_query="SELECT * FROM schema1.table1",
            schema=schema,
            raw_schema=raw_schema,
            is_query=True,
            query_input_alias="t",
        )

        assert not _semicolons_outside_identifiers(
            query
        ), f"Generated query has semicolons outside identifier boundaries:\n{query}"

    def test_embedded_double_quote_is_escaped(self):
        """Embedded double-quotes in column names must be doubled."""
        schema, raw_schema = self._build_schema_and_raw()

        query = PostgresDialect.generate_select_query(
            table_or_query="schema1.table1",
            schema=schema,
            raw_schema=raw_schema,
            is_query=False,
            query_input_alias="t",
        )

        # The column has an embedded " — it must appear as "" in output
        assert (
            '""' in query
        ), f"Embedded double-quote not escaped in identifier:\n{query}"


class TestBaseDialectQuoting:
    """BaseDialect uses backtick quoting."""

    EMBEDDED_QUOTE_COL = "y`; x"

    def _build_raw(self):
        return [("x", 0), (self.EMBEDDED_QUOTE_COL, 0)]

    def test_embedded_quote_is_query_false(self):
        schema = StructType(
            [StructField("x", StringType()), StructField("y", StringType())]
        )
        query = BaseDialect.generate_select_query(
            "my_table", schema, self._build_raw(), False, "t"
        )
        assert not _semicolons_outside_backtick_identifiers(
            query
        ), f"Semicolon outside identifier:\n{query}"

    def test_embedded_quote_is_query_true(self):
        schema = StructType(
            [StructField("x", StringType()), StructField("y", StringType())]
        )
        query = BaseDialect.generate_select_query(
            "SELECT * FROM my_table", schema, self._build_raw(), True, "t"
        )
        assert not _semicolons_outside_backtick_identifiers(
            query
        ), f"Semicolon outside identifier:\n{query}"


class TestMysqlDialectQuoting:
    """MySQL uses backtick quoting."""

    EMBEDDED_QUOTE_COL = "y`; x"

    def _build_schema_and_raw(self):
        schema = StructType(
            [StructField("x", StringType()), StructField("y", StringType())]
        )
        raw_schema = [("x", 0), (self.EMBEDDED_QUOTE_COL, 0)]
        return schema, raw_schema

    def test_embedded_quote_is_query_false(self):
        schema, raw_schema = self._build_schema_and_raw()
        dialect = MysqlDialect()
        query = dialect.generate_select_query(
            "my_table", schema, raw_schema, False, "t"
        )
        assert not _semicolons_outside_backtick_identifiers(
            query
        ), f"Semicolon outside identifier:\n{query}"

    def test_embedded_quote_is_query_true(self):
        schema, raw_schema = self._build_schema_and_raw()
        dialect = MysqlDialect()
        query = dialect.generate_select_query(
            "SELECT * FROM my_table", schema, raw_schema, True, "t"
        )
        assert not _semicolons_outside_backtick_identifiers(
            query
        ), f"Semicolon outside identifier:\n{query}"


class TestDatabricksDialectQuoting:
    """Databricks uses backtick quoting."""

    EMBEDDED_QUOTE_COL = "y`; x"

    def _build_schema_and_raw(self):
        schema = StructType(
            [StructField("x", StringType()), StructField("y", StringType())]
        )
        raw_schema = [("x", 0), (self.EMBEDDED_QUOTE_COL, 0)]
        return schema, raw_schema

    def test_embedded_quote_is_query_false(self):
        schema, raw_schema = self._build_schema_and_raw()
        dialect = DatabricksDialect()
        query = dialect.generate_select_query(
            "my_table", schema, raw_schema, False, "t"
        )
        assert not _semicolons_outside_backtick_identifiers(
            query
        ), f"Semicolon outside identifier:\n{query}"

    def test_embedded_quote_is_query_true(self):
        schema, raw_schema = self._build_schema_and_raw()
        dialect = DatabricksDialect()
        query = dialect.generate_select_query(
            "SELECT * FROM my_table", schema, raw_schema, True, "t"
        )
        assert not _semicolons_outside_backtick_identifiers(
            query
        ), f"Semicolon outside identifier:\n{query}"


class TestOracledbDialectQuoting:
    """Oracle uses quote_name (double-quote) for identifiers."""

    EMBEDDED_QUOTE_COL = 'y"; x'

    def _build_schema_and_raw(self):
        schema = StructType(
            [StructField("x", StringType()), StructField("y", StringType())]
        )
        raw_schema = [("x", 0), (self.EMBEDDED_QUOTE_COL, 0)]
        return schema, raw_schema

    def test_embedded_quote_is_query_true(self):
        schema, raw_schema = self._build_schema_and_raw()
        dialect = OracledbDialect()
        query = dialect.generate_select_query(
            "SELECT * FROM my_table", schema, raw_schema, True, "t"
        )
        assert not _semicolons_outside_identifiers(
            query
        ), f"Semicolon outside identifier:\n{query}"


class TestSqlServerDialectQuoting:
    """SQL Server uses quote_name (double-quote) for identifiers."""

    EMBEDDED_QUOTE_COL = 'y"; x'

    def _build_schema_and_raw(self):
        schema = StructType(
            [StructField("x", StringType()), StructField("y", StringType())]
        )
        raw_schema = [("x", 0), (self.EMBEDDED_QUOTE_COL, 0)]
        return schema, raw_schema

    def test_embedded_quote_is_query_true(self):
        schema, raw_schema = self._build_schema_and_raw()
        dialect = SqlServerDialect()
        query = dialect.generate_select_query(
            "SELECT * FROM my_table", schema, raw_schema, True, "t"
        )
        assert not _semicolons_outside_identifiers(
            query
        ), f"Semicolon outside identifier:\n{query}"
