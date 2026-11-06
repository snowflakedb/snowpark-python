#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""DataFrame trace of actions, of the kinds of SQL errors and of the object context of missing and existing objects.

Each test builds a dataframe whose action fails on the server, typically with a final ``select``
of a missing column, and checks the debug output of the error: the lineage trace, the SQL error
to Python source mapping and the missing or existing object context. ``src`` gives the expected
entry of a call from the source of the test, so every position is checked exactly.
"""

import pytest

from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import col, lit
from tests.integ.df_trace_utils import (
    assert_debug_output,
    BASE,
    base,
    fail,
    FAIL,
    random_name,
    src,
)

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="SnowparkSQLException is not raised in local testing mode",
        run=False,
    ),
]


def test_typing_with_error_line(session, trace_settings):
    df = base(session).with_column("BAD", col("NAME") + lit(1)).filter(col("ID") > 0)
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    assert_debug_output(
        ex,
        trace=[
            src('base(session).with_column("BAD", col("NAME") + lit(1)).filter...'),
            src('base(session).with_column("BAD", col("NAME") + lit(1))'),
            BASE,
        ],
    )


def test_typing_in_flattened_select(session, trace_settings):
    df = base(session).select("ID", (col("NAME") + lit(1)).alias("X")).select("X")
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    assert_debug_output(
        ex,
        trace=[
            src(
                'base(session).select("ID", (col("NAME") + lit(1)).alias("X")).select...'
            ),
            src('base(session).select("ID", (col("NAME") + lit(1)).alias("X"))'),
            BASE,
        ],
    )


def test_invalid_identifier_after_rename(session, trace_settings):
    df = base(session).select(col("ID").alias("RENAMED")).select("ID")
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [
        BASE,
        src('base(session).select(col("ID").alias("RENAMED")).select("ID")'),
    ]
    if not trace_settings.simplifier:
        mapped = [src('base(session).select(col("ID").alias("RENAMED")).select("ID")')]
    assert_debug_output(
        ex,
        trace=[
            src('base(session).select(col("ID").alias("RENAMED")).select("ID")'),
            src('base(session).select(col("ID").alias("RENAMED"))'),
            BASE,
        ],
        mapped=mapped,
    )


def test_missing_table(session, trace_settings):
    with pytest.raises(SnowparkSQLException) as ex:
        df = session.table("DF_TRACE_NO_SUCH_TABLE").select("A")
        df.collect()
    trace = [src('session.table("DF_TRACE_NO_SUCH_TABLE")')]
    if not trace_settings.simplifier:
        trace = [
            src('session.table("DF_TRACE_NO_SUCH_TABLE").select("A")'),
            src('session.table("DF_TRACE_NO_SUCH_TABLE")'),
        ]
    assert_debug_output(
        ex,
        trace=trace,
        missing_object=(
            "DF_TRACE_NO_SUCH_TABLE",
            src('session.table("DF_TRACE_NO_SUCH_TABLE")'),
        ),
    )


def test_missing_table_in_join(session, trace_settings):
    with pytest.raises(SnowparkSQLException) as ex:
        df = base(session).join(session.table("DF_TRACE_NO_SUCH_TABLE"), on="ID")
        df.collect()
    assert_debug_output(
        ex,
        trace=[src('session.table("DF_TRACE_NO_SUCH_TABLE")')],
        missing_object=(
            "DF_TRACE_NO_SUCH_TABLE",
            src('session.table("DF_TRACE_NO_SUCH_TABLE")'),
        ),
    )


def test_count(session, trace_settings):
    df = fail(base(session))
    with pytest.raises(SnowparkSQLException) as ex:
        df.count()
    mapped = [BASE, FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(ex, trace=[src("df.count()"), FAIL, BASE], mapped=mapped)


def test_show(session, trace_settings):
    df = fail(base(session))
    with pytest.raises(SnowparkSQLException) as ex:
        df.show()
    mapped = [BASE, FAIL, src("df.show()")]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(ex, trace=[src("df.show()"), FAIL, BASE], mapped=mapped)


def test_first(session, trace_settings):
    df = fail(base(session))
    with pytest.raises(SnowparkSQLException) as ex:
        df.first()
    mapped = [BASE, FAIL, src("df.first()")]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(ex, trace=[src("df.first()"), FAIL, BASE], mapped=mapped)


def test_to_pandas(session, trace_settings):
    df = fail(base(session))
    with pytest.raises(SnowparkSQLException) as ex:
        df.to_pandas()
    mapped = [BASE, FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(ex, trace=[FAIL, BASE], mapped=mapped)


def test_to_pandas_batches(session, trace_settings):
    df = fail(base(session))
    with pytest.raises(SnowparkSQLException) as ex:
        list(df.to_pandas_batches())
    mapped = [BASE, FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(ex, trace=[FAIL, BASE], mapped=mapped)


def test_to_local_iterator(session, trace_settings):
    df = fail(base(session))
    with pytest.raises(SnowparkSQLException) as ex:
        list(df.to_local_iterator())
    mapped = [BASE, FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(ex, trace=[FAIL, BASE], mapped=mapped)


def test_cache_result(session, trace_settings):
    df = fail(base(session))
    with pytest.raises(SnowparkSQLException) as ex:
        df.cache_result()
    mapped = [BASE, FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(ex, trace=[FAIL, BASE], mapped=mapped)


def test_print_schema(session, trace_settings):
    df = fail(base(session))
    with pytest.raises(SnowparkSQLException) as ex:
        df.print_schema()
    mapped = [BASE, FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(ex, trace=[FAIL, BASE], mapped=mapped)


def test_stat_corr_cov(session, trace_settings):
    df = fail(base(session))
    with pytest.raises(SnowparkSQLException) as ex:
        df.stat.corr("ID", "V")
    mapped = [BASE, FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex, trace=[src('df.stat.corr("ID", "V")'), FAIL, BASE], mapped=mapped
    )


def test_stat_approx_quantile(session, trace_settings):
    df = fail(base(session))
    with pytest.raises(SnowparkSQLException) as ex:
        df.stat.approx_quantile("ID", [0.5])
    mapped = [BASE, FAIL, src('df.stat.approx_quantile("ID", [0.5])')]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[src('df.stat.approx_quantile("ID", [0.5])'), FAIL, BASE],
        mapped=mapped,
    )


def test_create_or_replace_view(session, trace_settings):
    df = fail(base(session))
    with pytest.raises(SnowparkSQLException) as ex:
        df.create_or_replace_view(random_name("DF_TRACE_V"))
    assert_debug_output(ex, trace=[])


def test_create_or_replace_temp_view(session, trace_settings):
    df = fail(base(session))
    with pytest.raises(SnowparkSQLException) as ex:
        df.create_or_replace_temp_view(random_name("DF_TRACE_V"))
    assert_debug_output(ex, trace=[])


def test_create_temp_view(session, trace_settings):
    df = fail(base(session))
    with pytest.raises(SnowparkSQLException) as ex:
        df.create_temp_view(random_name("DF_TRACE_V"))
    assert_debug_output(ex, trace=[])


def test_write_save_as_table(session, trace_settings):
    df = fail(base(session))
    with pytest.raises(SnowparkSQLException) as ex:
        df.write.mode("overwrite").save_as_table(random_name("DF_TRACE_T"))
    mapped = [BASE, FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(ex, trace=[FAIL, BASE], mapped=mapped)


def test_write_copy_into_location(session, trace_settings):
    df = fail(base(session))
    with pytest.raises(SnowparkSQLException) as ex:
        (
            df.write.copy_into_location(
                "@~/df_trace_out/", file_format_type="csv", overwrite=True
            )
        )
    assert_debug_output(
        ex,
        trace=[src("df.write.copy_into_location..."), FAIL, BASE],
        mapped=[src("df.write.copy_into_location...")],
    )


def test_write_csv_json_parquet(session, trace_settings):
    df = fail(base(session))
    with pytest.raises(SnowparkSQLException) as ex:
        df.write.option("overwrite", True).csv("@~/df_trace_out_csv/")
    assert_debug_output(
        ex,
        trace=[
            src('df.write.option("overwrite", True).csv("@~/df_trace_out_csv/")'),
            FAIL,
            BASE,
        ],
        mapped=[src('df.write.option("overwrite", True).csv("@~/df_trace_out_csv/")')],
    )


def test_write_json(session, trace_settings):
    df = fail(base(session))
    with pytest.raises(SnowparkSQLException) as ex:
        df.write.json("@~/df_trace_out_json/", overwrite=True)
    assert_debug_output(
        ex,
        trace=[
            src('df.write.json("@~/df_trace_out_json/", overwrite=True)'),
            FAIL,
            BASE,
        ],
        mapped=[src('df.write.json("@~/df_trace_out_json/", overwrite=True)')],
    )


def test_write_parquet(session, trace_settings):
    df = fail(base(session))
    with pytest.raises(SnowparkSQLException) as ex:
        df.write.parquet("@~/df_trace_out_pq/", overwrite=True)
    assert_debug_output(
        ex,
        trace=[
            src('df.write.parquet("@~/df_trace_out_pq/", overwrite=True)'),
            FAIL,
            BASE,
        ],
        mapped=[src('df.write.parquet("@~/df_trace_out_pq/", overwrite=True)')],
    )


def test_write_partition_by_format_save(session, trace_settings):
    df = fail(base(session))
    with pytest.raises(SnowparkSQLException) as ex:
        (
            df.write.format("csv")
            .partition_by("ID")
            .save("@~/df_trace_out_p/", mode="overwrite")
        )
    assert_debug_output(
        ex,
        trace=[
            src(
                'df.write.format("csv").partition_by("ID").save("@~/df_trace_out_p/", mode="overwrite")'
            ),
            FAIL,
            BASE,
        ],
    )


def test_write_options(session, trace_settings):
    df = fail(base(session))
    with pytest.raises(SnowparkSQLException) as ex:
        df.write.options({"overwrite": True}).csv("@~/df_trace_out_opts/")
    assert_debug_output(
        ex,
        trace=[
            src('df.write.options({"overwrite": True}).csv("@~/df_trace_out_opts/")'),
            FAIL,
            BASE,
        ],
        mapped=[
            src('df.write.options({"overwrite": True}).csv("@~/df_trace_out_opts/")')
        ],
    )


def test_write_insert_into(session, trace_settings, df_trace_table):
    df = fail(base(session))
    with pytest.raises(SnowparkSQLException) as ex:
        df.write.insert_into(df_trace_table)
    mapped = [BASE, FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(ex, trace=[FAIL, BASE], mapped=mapped)


def test_exists_sql(session, trace_settings):
    name = random_name("DF_TRACE_EX")
    session.sql(f"create or replace temp table {name} (a int)").collect()
    df = session.sql(f"create temp table {name} (a int)")
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    assert_debug_output(
        ex,
        trace=[src('session.sql(f"create temp table {name} (a int)")')],
        existing_object=(
            "DF_TRACE_EX_[0-9A-F]{12}",
            src('session.sql(f"create or replace temp table {name} (a int)")'),
        ),
    )


def test_exists_temp_view_twice(session, trace_settings):
    with pytest.raises(SnowparkSQLException) as ex:
        name = random_name("DF_TRACE_EXV")
        d = base(session).select("ID")
        d.create_temp_view(name)
        d.create_temp_view(name)
    assert_debug_output(
        ex,
        trace=[],
        existing_object=(
            "DF_TRACE_EXV_[0-9A-F]{12}",
            src("d.create_temp_view(name)", nth=1),
        ),
    )


def test_exists_save_as_table_errorifexists(session, trace_settings):
    with pytest.raises(SnowparkSQLException) as ex:
        name = random_name("DF_TRACE_EXT")
        d = base(session).select("ID")
        d.write.mode("overwrite").save_as_table(name, table_type="temp")
        d.write.mode("errorifexists").save_as_table(name, table_type="temp")
    assert_debug_output(
        ex,
        trace=[],
        existing_object=(
            "DF_TRACE_EXT_[0-9A-F]{12}",
            src('d.write.mode("overwrite").save_as_table(name, table_type="temp")'),
        ),
    )
