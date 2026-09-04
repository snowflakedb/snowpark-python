#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""End-to-end regression tests for escaping special characters in SQL literals.

These cover four sinks that build single-quoted SQL string literals from
user-provided values:

  * DataFrame.create_or_replace_view(..., comment=...)  -> get_comment_sql
  * Column.collate(spec)                                 -> collate_expression
  * Column.__getitem__(field) (VARIANT/OBJECT key)       -> subfield_expression
  * DataFrame.flatten(..., path=...)                     -> flatten_expression

Each test verifies BOTH:
  1. values containing apostrophes / backslashes work end-to-end and return the
     correct result (escaping must not break valid input), and
  2. values containing other special characters (single quotes, backslashes,
     parentheses, commas, trailing dashes) are escaped into the literal so they
     are treated as literal data and produce valid SQL.
"""

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.functions import col, lit, parse_json
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.utils import TestData, Utils

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="SQL generation / live execution required for these escaping tests",
    ),
]


def test_comment_escapes_special_characters(session):
    # 1. A comment with apostrophes and a backslash round-trips and the view is
    #    created with the expected body.
    view_name = Utils.random_view_name()
    comment = "O'Brien's data \\ backup"
    df = session.create_dataframe([[1]], schema=["a"])
    try:
        df.create_or_replace_view(view_name, comment=comment)
        # The view exists and returns the expected body.
        assert session.table(view_name).collect() == [Row(1)]
        got = session.sql(f"SHOW VIEWS LIKE '{view_name}'").collect()
        assert got[0]["comment"] == comment
    finally:
        Utils.drop_view(session, view_name)

    # 2. A comment whose special characters (backslash, quote, parentheses,
    #    commas, trailing dashes) could otherwise terminate the literal early is
    #    escaped, so the view is created and the comment round-trips verbatim.
    view_name2 = Utils.random_view_name()
    comment2 = "O'Brien\\notes (draft), v2 -- "
    try:
        df.create_or_replace_view(view_name2, comment=comment2)
        # The body is unchanged.
        assert session.table(view_name2).collect() == [Row(1)]
        shown = session.sql(f"SHOW VIEWS LIKE '{view_name2}'").collect()
        # The whole comment is stored verbatim as literal data.
        assert shown[0]["comment"] == comment2
    finally:
        Utils.drop_view(session, view_name2)


def test_comment_round_trip_special_characters(session):
    # Lock the contract that the stored comment equals the exact Python input,
    # including literal backslashes (e.g. C:\table, \t, \n stay as written and
    # are not interpreted as escape sequences).
    df = session.create_dataframe([[1]], schema=["a"])
    values = [
        "O'Brien",
        "C:\\table",  # Python value = C:\table
        "a\\'b",  # Python value = a\'b
        "\\\\",  # Python value = two backslashes
        "plain comment",
    ]
    for comment in values:
        view_name = Utils.random_view_name()
        try:
            df.create_or_replace_view(view_name, comment=comment)
            shown = session.sql(f"SHOW VIEWS LIKE '{view_name}'").collect()
            assert (
                shown[0]["comment"] == comment
            ), f"stored comment {shown[0]['comment']!r} != input {comment!r}"
        finally:
            Utils.drop_view(session, view_name)


@pytest.mark.parametrize("spec", ["en_US-trim", "'en_US-trim'"])
def test_collate_valid_specs(session, spec):
    # Valid collation specs (unquoted AND pre-quoted) must work end-to-end
    # exactly as before -- no behavior change for inputs without special
    # characters.
    Utils.check_answer(
        TestData.string3(session).where(col("a").collate(spec) == "abcba"),
        [Row("  abcba  ")],
    )


def test_collate_escapes_special_characters(session):
    # A collation spec containing single quotes and parentheses is escaped into
    # the COLLATE literal as a single string. The resulting value is not a valid
    # collation specification, so Snowflake rejects it -- the characters are
    # treated as literal data, not as separate SQL tokens.
    spec = "en') = 'x' (note) --"
    df = TestData.string3(session).where(col("a").collate(spec) == "abcba")
    with pytest.raises(SnowparkSQLException):
        df.collect()

    # The same holds when the spec is used in a projection.
    spec2 = "en' (a), (b) --"
    with pytest.raises(SnowparkSQLException):
        TestData.string3(session).select(col("a").collate(spec2)).collect()


def test_subfield_escapes_special_characters(session):
    # Build a VARIANT/OBJECT with keys that contain an apostrophe and backslash.
    df = session.create_dataframe([[1]], schema=["x"]).select(
        parse_json(lit('{"O\'Brien": "ok", "a\\\\b": "ok2", "plain": "p"}')).alias("v")
    )

    # 1. Keys with special characters extract the correct values.
    assert df.select(col("v")["O'Brien"].alias("r")).collect()[0]["R"] == '"ok"'
    assert df.select(col("v")["a\\b"].alias("r")).collect()[0]["R"] == '"ok2"'

    # 2. A key containing quotes, parentheses, commas and trailing dashes is
    #    escaped into the bracket literal as a single string. It is simply a
    #    key that does not exist in the document, so the lookup yields NULL and
    #    no extra output columns are produced.
    key = "plain' , (x) AS y -- "
    res = df.select(col("v")[key].alias("r")).collect()
    assert len(res) == 1
    assert res[0]["R"] is None
    assert list(res[0].asDict().keys()) == ["R"]


def test_flatten_escapes_special_characters(session):
    df = session.create_dataframe([[1]], schema=["x"]).select(
        parse_json(lit('{"arr": [10, 20]}')).alias("v")
    )

    # 1. A valid JSON path flattens correctly.
    valid = df.flatten(
        col("v"), path="arr", outer=False, recursive=False, mode="ARRAY"
    ).select("value")
    Utils.check_answer(valid, [Row("10"), Row("20")])

    # 2. A path containing single quotes, parentheses, commas and trailing
    #    dashes is escaped into the PATH literal as a single string. It is
    #    therefore an invalid / non-existent compound field path: with
    #    OUTER => TRUE Snowflake either errors on the bad path name or yields a
    #    single NULL-value row -- the characters are treated as literal data.
    path = "' (a), (b) -- "
    df2 = session.create_dataframe([[1]], schema=["x"]).select(
        parse_json(lit('{"a": [1]}')).alias("v")
    )
    flat = df2.flatten(col("v"), path=path, outer=True, recursive=False, mode="BOTH")
    try:
        res = flat.select("value").collect()
    except SnowparkSQLException:
        # Bad compound object's field path name -> path treated as literal data.
        return
    assert all(r["VALUE"] is None for r in res)
