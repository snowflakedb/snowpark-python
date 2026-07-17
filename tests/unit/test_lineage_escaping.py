#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
Regression tests for string escaping in Lineage.trace(): values passed via
object_domain and object_name must be properly escaped before being embedded
in the single-quoted SQL string literal passed to SYSTEM$DGQL.
"""

import pytest

from snowflake.snowpark.lineage import _DGQLQueryBuilder, LineageDirection


def _sql_string_literal_ends_early(inner: str) -> bool:
    """Return True if ``inner`` (the content between the outer quotes of
    ``SYSTEM$DGQL('<inner>')``) contains an unescaped single quote that would
    terminate the SQL string literal prematurely.

    Snowflake honors two escape forms inside single-quoted literals:
      * backslash escapes the next character (``\\'`` is a literal quote,
        ``\\\\`` is a literal backslash), and
      * a doubled quote (``''``) is a literal quote.

    A lone ``'`` that is neither preceded by a backslash nor part of a ``''``
    pair terminates the literal early, leaving the remainder as bare SQL.
    """
    i = 0
    n = len(inner)
    while i < n:
        c = inner[i]
        if c == "\\":
            i += 2  # backslash escapes the next character; skip both
        elif c == "'":
            if i + 1 < n and inner[i + 1] == "'":
                i += 2  # '' is an escaped quote; skip the pair
            else:
                return True  # lone quote terminates the literal
        else:
            i += 1
    return False


class TestBuildQueryStringEscaping:
    """Verify that object_domain and object_name values containing single
    quotes or backslashes are properly escaped in the generated SQL so the
    SYSTEM$DGQL string literal is always well-formed."""

    PREFIX = "select SYSTEM$DGQL('"
    SUFFIX = "')"

    # Values with single quotes, backslashes, and combinations that require
    # both backslash-first and quote escaping to produce a valid SQL literal.
    SPECIAL_DOMAINS = [
        "TABLE' OR 1=1--",
        "TABLE'; DROP TABLE users;--",
        "TABLE' UNION SELECT * FROM information_schema.tables--",
        "VIEW') OR ('1'='1",
        # backslash followed by quote -- requires backslash-first escaping:
        "TABLE\\' || (select current_user()) --",
        "TABLE\\\\' OR 1=1--",
        "\\' UNION SELECT password_hash FROM internal.users --",
    ]

    SPECIAL_NAMES = [
        "db.schema.t' || (select current_user()) --",
        "db.schema.t\\' || (select current_user()) --",
    ]

    @pytest.mark.parametrize("domain", SPECIAL_DOMAINS)
    def test_object_domain_is_escaped(self, domain):
        """object_domain values with special characters must produce a
        well-formed SQL string literal."""
        sql = _DGQLQueryBuilder.build_query(
            object_domain=domain,
            object_name="db.schema.my_table",
            edge_directions=[LineageDirection.DOWNSTREAM],
        )
        assert sql.startswith(self.PREFIX), f"Unexpected SQL prefix: {sql[:50]}"
        assert sql.endswith(self.SUFFIX), f"Unexpected SQL suffix: {sql[-10:]}"

        inner = sql[len(self.PREFIX) : -len(self.SUFFIX)]
        assert not _sql_string_literal_ends_early(inner), (
            f"object_domain={domain!r} produced an unterminated SQL string "
            f"literal. Inner query: {inner!r}"
        )

    @pytest.mark.parametrize("name", SPECIAL_NAMES)
    def test_object_name_is_escaped(self, name):
        """object_name values with special characters must produce a
        well-formed SQL string literal."""
        sql = _DGQLQueryBuilder.build_query(
            object_domain="TABLE",
            object_name=name,
            edge_directions=[LineageDirection.DOWNSTREAM],
        )
        inner = sql[len(self.PREFIX) : -len(self.SUFFIX)]
        assert not _sql_string_literal_ends_early(inner), (
            f"object_name={name!r} produced an unterminated SQL string "
            f"literal. Inner query: {inner!r}"
        )

    def test_detector_catches_insufficient_escaping(self):
        """Sanity-check the detector: quote-doubling alone (without escaping
        the preceding backslash) must be flagged as producing an unterminated
        literal, and a correctly backslash-first-escaped value must pass."""
        # Quote-doubling only: backslash leaves the following '' open
        assert _sql_string_literal_ends_early("{V(domain: \\'' ...")
        # Backslash-first then quote: correctly escaped
        assert not _sql_string_literal_ends_early("{V(domain: \\\\'' ...")
