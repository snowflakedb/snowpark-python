#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""DataFrame trace of the sources of a dataframe: tables, SQL, created dataframes and readers.

Each test builds a dataframe whose action fails on the server, typically with a final ``select``
of a missing column, and checks the debug output of the error: the lineage trace, the SQL error
to Python source mapping and the missing or existing object context. ``src`` gives the expected
entry of a call from the source of the test, so every position is checked exactly.
"""

import pytest

from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import lit, parse_json, seq4
from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType
from tests.integ.df_trace_utils import (
    assert_debug_output,
    BASE,
    base,
    fail,
    FAIL,
    src,
)

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="SnowparkSQLException is not raised in local testing mode",
        run=False,
    ),
]


_csv_schema = StructType(
    [StructField("ID", IntegerType()), StructField("NAME", StringType())]
)


def test_table(session, trace_settings, df_trace_table):
    df = fail(session.table(df_trace_table))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [src("session.table(df_trace_table)"), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex, trace=[FAIL, src("session.table(df_trace_table)")], mapped=mapped
    )


def test_read_table(session, trace_settings, df_trace_table):
    df = fail(session.read.table(df_trace_table))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [src("session.read.table(df_trace_table)"), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex, trace=[FAIL, src("session.read.table(df_trace_table)")], mapped=mapped
    )


def test_sql_select(session, trace_settings):
    df = fail(session.sql("select 1 as id"))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [src('session.sql("select 1 as id")'), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex, trace=[FAIL, src('session.sql("select 1 as id")')], mapped=mapped
    )


def test_sql_with_params(session, trace_settings):
    df = fail(session.sql("select ? as id", params=[1]))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [src('session.sql("select ? as id", params=[1])'), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[FAIL, src('session.sql("select ? as id", params=[1])')],
        mapped=mapped,
    )


def test_sql_non_select(session, trace_settings):
    df = fail(session.sql("show parameters like 'TIMEZONE'"))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [src("session.sql(\"show parameters like 'TIMEZONE'\")"), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[FAIL, src("session.sql(\"show parameters like 'TIMEZONE'\")")],
        mapped=mapped,
    )


def test_create_dataframe(session, trace_settings):
    df = fail(base(session))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [BASE, FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(ex, trace=[FAIL, BASE], mapped=mapped)


def test_create_dataframe_large(session, trace_settings):
    df = fail(
        session.create_dataframe(
            [(i, str(i)) for i in range(20000)], schema=["ID", "NAME"]
        )
    )
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [src("session.create_dataframe..."), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex, trace=[FAIL, src("session.create_dataframe...")], mapped=mapped
    )


def test_create_dataframe_dicts_and_createDataFrame(session, trace_settings):
    df = fail(session.createDataFrame([{"ID": 1, "NAME": "a"}]))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [src('session.createDataFrame([{"ID": 1, "NAME": "a"}])'), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[FAIL, src('session.createDataFrame([{"ID": 1, "NAME": "a"}])')],
        mapped=mapped,
    )


def test_range(session, trace_settings):
    df = fail(session.range(5))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [src("session.range(5)"), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(ex, trace=[FAIL, src("session.range(5)")], mapped=mapped)


def test_generator(session, trace_settings):
    df = fail(session.generator(seq4().alias("A"), rowcount=3))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [src('session.generator(seq4().alias("A"), rowcount=3)'), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[FAIL, src('session.generator(seq4().alias("A"), rowcount=3)')],
        mapped=mapped,
    )


def test_table_function(session, trace_settings):
    df = fail(session.table_function("split_to_table", lit("a b"), lit(" ")))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [
        src('session.table_function("split_to_table", lit("a b"), lit(" "))'),
        FAIL,
    ]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('session.table_function("split_to_table", lit("a b"), lit(" "))'),
        ],
        mapped=mapped,
    )


def test_flatten(session, trace_settings):
    df = fail(session.flatten(parse_json(lit("[1,2]"))))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[FAIL, src('session.flatten(parse_json(lit("[1,2]")))')],
        mapped=mapped,
    )


def test_read_csv(session, trace_settings, temp_stage):
    df = fail(session.read.schema(_csv_schema).csv(f"@{temp_stage}/a.csv"))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [src('session.read.schema(_csv_schema).csv(f"@{temp_stage}/a.csv")'), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('session.read.schema(_csv_schema).csv(f"@{temp_stage}/a.csv")'),
        ],
        mapped=mapped,
    )


def test_read_json_options(session, trace_settings, temp_stage):
    df = fail(
        session.read.option("strip_outer_array", True)
        .schema(_csv_schema)
        .json(f"@{temp_stage}/a.json")
    )
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [
        src(
            'session.read.option("strip_outer_array", True).schema(_csv_schema) .json...'
        ),
        FAIL,
    ]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src(
                'session.read.option("strip_outer_array", True).schema(_csv_schema) .json...'
            ),
        ],
        mapped=mapped,
    )


def test_read_parquet(session, trace_settings, temp_stage):
    df = fail(session.read.schema(_csv_schema).parquet(f"@{temp_stage}/a.parquet"))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [
        src('session.read.schema(_csv_schema).parquet(f"@{temp_stage}/a.parquet")'),
        FAIL,
    ]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('session.read.schema(_csv_schema).parquet(f"@{temp_stage}/a.parquet")'),
        ],
        mapped=mapped,
    )


def test_read_format_load_options(session, trace_settings, temp_stage):
    df = fail(
        session.read.format("csv")
        .options({"field_delimiter": ","})
        .schema(_csv_schema)
        .load(f"@{temp_stage}/a.csv")
    )
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [
        src(
            'session.read.format("csv") .options({"field_delimiter": ","}) .schema(_csv_schema) .load...'
        ),
        FAIL,
    ]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src(
                'session.read.format("csv") .options({"field_delimiter": ","}) .schema(_csv_schema) .load...'
            ),
        ],
        mapped=mapped,
    )
