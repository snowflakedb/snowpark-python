#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""DataFrame trace of the dataframe operations: projections, row operations, aggregations, joins and set operations.

Each test builds a dataframe whose action fails on the server, typically with a final ``select``
of a missing column, and checks the debug output of the error: the lineage trace, the SQL error
to Python source mapping and the missing or existing object context. ``src`` gives the expected
entry of a call from the source of the test, so every position is checked exactly.
"""

import pytest

from snowflake.snowpark import Window
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import col, lit, parse_json, sum as sum_
from tests.integ.df_trace_utils import (
    assert_debug_output,
    BASE,
    base,
    fail,
    FAIL,
    other,
    OTHER,
    src,
)

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="SnowparkSQLException is not raised in local testing mode",
        run=False,
    ),
]


def test_select(session, trace_settings):
    df = fail(base(session).select("ID", col("V") + 1))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [BASE, src('base(session).select("ID", col("V") + 1)'), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[FAIL, src('base(session).select("ID", col("V") + 1)'), BASE],
        mapped=mapped,
    )


def test_select_expr(session, trace_settings):
    df = fail(base(session).select_expr("ID", "V + 1").selectExpr("ID"))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('base(session).select_expr("ID", "V + 1").selectExpr("ID")'),
            src('base(session).select_expr("ID", "V + 1")'),
            BASE,
        ],
        mapped=mapped,
    )


def test_with_column(session, trace_settings):
    df = fail(
        base(session).with_column("X", col("ID") + 1).withColumn("Y", col("X") * 2)
    )
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [
        src('base(session).with_column("X", col("ID") + 1).withColumn...'),
        FAIL,
    ]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('base(session).with_column("X", col("ID") + 1).withColumn...'),
            src('base(session).with_column("X", col("ID") + 1)'),
            BASE,
        ],
        mapped=mapped,
    )


def test_with_columns(session, trace_settings):
    df = fail(base(session).with_columns(["X", "Y"], [col("ID") + 1, col("V") * 2]))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [
        BASE,
        src('base(session).with_columns(["X", "Y"], [col("ID") + 1, col("V") * 2])'),
        FAIL,
    ]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src(
                'base(session).with_columns(["X", "Y"], [col("ID") + 1, col("V") * 2])'
            ),
            BASE,
        ],
        mapped=mapped,
    )


def test_with_column_renamed(session, trace_settings):
    df = fail(
        base(session).with_column_renamed("ID", "I2").withColumnRenamed("I2", "I3")
    )
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [
        src('base(session).with_column_renamed("ID", "I2").withColumnRenamed...'),
        FAIL,
    ]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('base(session).with_column_renamed("ID", "I2").withColumnRenamed...'),
            src('base(session).with_column_renamed("ID", "I2")'),
            BASE,
        ],
        mapped=mapped,
    )


def test_rename_to_df(session, trace_settings):
    df = fail(base(session).to_df(["A", "B", "C"]).toDF("X", "Y", "Z"))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [
        src('base(session).to_df(["A", "B", "C"]).toDF("X", "Y", "Z")'),
        FAIL,
    ]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('base(session).to_df(["A", "B", "C"]).toDF("X", "Y", "Z")'),
            src('base(session).to_df(["A", "B", "C"])'),
            BASE,
        ],
        mapped=mapped,
    )


def test_rename(session, trace_settings):
    df = fail(base(session).rename("V", "W"))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [BASE, src('base(session).rename("V", "W")'), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex, trace=[FAIL, src('base(session).rename("V", "W")'), BASE], mapped=mapped
    )


def test_drop(session, trace_settings):
    df = fail(base(session).drop("V"))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [BASE, src('base(session).drop("V")'), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex, trace=[FAIL, src('base(session).drop("V")'), BASE], mapped=mapped
    )


def test_alias(session, trace_settings):
    df = fail(base(session).alias("T").select("T.ID"))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [BASE, src('base(session).alias("T").select("T.ID")')]
    if not trace_settings.simplifier:
        mapped = [src('base(session).alias("T").select("T.ID")')]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('base(session).alias("T").select("T.ID")'),
            src('base(session).alias("T")'),
            BASE,
        ],
        mapped=mapped,
    )


def test_col_ilike(session, trace_settings):
    if not trace_settings.simplifier:
        pytest.skip("the API requires the SQL simplifier")
    df = fail(base(session).col_ilike("i%"))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex, trace=[FAIL, src('base(session).col_ilike("i%")'), BASE], mapped=mapped
    )


def test_filter_where(session, trace_settings):
    df = fail(base(session).filter(col("ID") > 1).where("V > 1"))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('base(session).filter(col("ID") > 1).where("V > 1")'),
            src('base(session).filter(col("ID") > 1)'),
            BASE,
        ],
        mapped=mapped,
    )


def test_sort(session, trace_settings):
    df = fail(base(session).sort("ID").orderBy("V").order_by(col("NAME")))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [
        BASE,
        src('base(session).sort("ID").orderBy("V").order_by(col("NAME"))'),
        FAIL,
    ]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('base(session).sort("ID").orderBy("V").order_by(col("NAME"))'),
            src('base(session).sort("ID").orderBy("V")'),
            src('base(session).sort("ID")'),
            BASE,
        ],
        mapped=mapped,
    )


def test_limit(session, trace_settings):
    df = fail(base(session).limit(2, offset=1))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [BASE, src("base(session).limit(2, offset=1)"), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex, trace=[FAIL, src("base(session).limit(2, offset=1)"), BASE], mapped=mapped
    )


def test_table_sample(session, trace_settings, df_trace_table):
    df = fail(session.table(df_trace_table).sample(0.5))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [src("session.table(df_trace_table).sample(0.5)"), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src("session.table(df_trace_table).sample(0.5)"),
            src("session.table(df_trace_table)"),
        ],
        mapped=mapped,
    )


def test_distinct(session, trace_settings):
    df = fail(base(session).distinct())
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex, trace=[FAIL, src("base(session).distinct()"), BASE], mapped=mapped
    )


def test_drop_duplicates(session, trace_settings):
    df = fail(base(session).drop_duplicates("ID").dropDuplicates("NAME"))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [
        src('base(session).drop_duplicates("ID").dropDuplicates("NAME")'),
        FAIL,
    ]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('base(session).drop_duplicates("ID").dropDuplicates("NAME")'),
            src('base(session).drop_duplicates("ID")'),
            BASE,
        ],
        mapped=mapped,
    )


def test_sample(session, trace_settings):
    df = fail(base(session).sample(0.5))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex, trace=[FAIL, src("base(session).sample(0.5)"), BASE], mapped=mapped
    )


def test_sample_by(session, trace_settings):
    d = base(session).sample_by(col("NAME"), {"a": 0.5}).sampleBy("NAME", {"b": 0.5})
    df = fail(d)
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('base(session).sample_by(col("NAME"), {"a": 0.5}).sampleBy...'),
            src('base(session).sample_by(col("NAME"), {"a": 0.5})'),
            BASE,
        ],
        mapped=mapped,
    )


def test_dropna_fillna_replace(session, trace_settings):
    df = fail(base(session).dropna().fillna(0).replace(1, 5))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [src("base(session).dropna().fillna(0).replace(1, 5)"), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src("base(session).dropna().fillna(0).replace(1, 5)"),
            src("base(session).dropna().fillna(0)"),
            src("base(session).dropna()"),
            BASE,
        ],
        mapped=mapped,
    )


def test_na_stat_functions(session, trace_settings):
    df = fail(
        base(session)
        .na.drop()
        .na.fill(0)
        .na.replace(1, 5)
        .stat.sample_by("NAME", {"a": 0.5})
    )
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src(
                "base(session).na.drop().na.fill(0).na.replace(1, 5).stat.sample_by(..."
            ),
            src("base(session) .na.drop() .na.fill(0) .na.replace(1, 5)"),
            src("base(session) .na.drop() .na.fill(0)"),
            src("base(session) .na.drop()"),
            BASE,
        ],
        mapped=mapped,
    )


def test_crosstab(session, trace_settings):
    df = fail(base(session).stat.crosstab("ID", "NAME"))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[FAIL, src('base(session).stat.crosstab("ID", "NAME")'), BASE],
        mapped=mapped,
    )


def test_describe(session, trace_settings):
    df = fail(base(session).describe())
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [src("base(session).describe()"), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex, trace=[FAIL, src("base(session).describe()"), BASE], mapped=mapped
    )


def test_cache_result(session, trace_settings):
    df = fail(base(session).filter(col("ID") > 0).cache_result().select("ID"))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [
        src('base(session).filter(col("ID") > 0).cache_result().select("ID")'),
        FAIL,
    ]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('base(session).filter(col("ID") > 0).cache_result().select("ID")'),
            src('base(session).filter(col("ID") > 0).cache_result()'),
            src('base(session).filter(col("ID") > 0)'),
            BASE,
        ],
        mapped=mapped,
    )


def test_group_by_agg(session, trace_settings):
    df = fail(base(session).group_by("NAME").agg(sum_("V").alias("S")))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('base(session).group_by("NAME").agg(sum_("V").alias("S"))'),
            src('base(session).group_by("NAME")'),
            BASE,
        ],
        mapped=mapped,
    )


def test_groupBy_functions(session, trace_settings):
    g = base(session).groupBy("NAME")
    d = g.count().union_all(g.sum("V").select("NAME", "SUM(V)")).select("NAME", "COUNT")
    df = fail(d)
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [
        src('g.count().union_all(g.sum("V").select("NAME", "SUM(V)")).select...'),
        FAIL,
    ]
    if trace_settings.cte:
        mapped = [
            src('g.count().union_all(g.sum("V").select("NAME", "SUM(V)")).select...')
        ]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('g.count().union_all(g.sum("V").select("NAME", "SUM(V)")).select...'),
            src('g.count().union_all(g.sum("V").select("NAME", "SUM(V)"))'),
            src("g.count()"),
            src('g.sum("V").select("NAME", "SUM(V)")'),
            src('base(session).groupBy("NAME")'),
            src('g.sum("V")'),
            BASE,
        ],
        mapped=mapped,
    )


def test_grouped_min_max_avg_median(session, trace_settings):
    g = base(session).group_by("NAME")
    df = fail(
        g.min("V")
        .join(g.max("V"), on="NAME")
        .join(g.avg("V"), on="NAME")
        .join(g.mean("V"), on="NAME")
        .join(g.median("V"), on="NAME")
    )
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    if trace_settings.cte:
        mapped = [
            src('g.min("V") .join(g.max("V"), on="NAME") .join(g.avg("V"), on="NAME")')
        ]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src(
                'g.min("V") .join(g.max("V"), on="NAME") .join(g.avg("V"), on="NAME") .join(g.mean("V"), on="NAME") .join...'
            ),
            src(
                'g.min("V") .join(g.max("V"), on="NAME") .join(g.avg("V"), on="NAME") .join(g.mean("V"), on="NAME")'
            ),
            src('g.median("V")'),
            src('g.min("V") .join(g.max("V"), on="NAME") .join(g.avg("V"), on="NAME")'),
            src('g.mean("V")'),
            src('base(session).group_by("NAME")'),
            src('g.min("V") .join(g.max("V"), on="NAME")'),
            src('g.avg("V")'),
            BASE,
            src('g.min("V")'),
            src('g.max("V")'),
        ],
        mapped=mapped,
    )


def test_rollup(session, trace_settings):
    df = fail(base(session).rollup("NAME").agg(sum_("V")))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('base(session).rollup("NAME").agg(sum_("V"))'),
            src('base(session).rollup("NAME")'),
            BASE,
        ],
        mapped=mapped,
    )


def test_cube(session, trace_settings):
    df = fail(base(session).cube("NAME", "ID").agg(sum_("V")))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('base(session).cube("NAME", "ID").agg(sum_("V"))'),
            src('base(session).cube("NAME", "ID")'),
            BASE,
        ],
        mapped=mapped,
    )


def test_grouping_sets(session, trace_settings):
    from snowflake.snowpark import GroupingSets

    df = fail(
        base(session)
        .group_by_grouping_sets(GroupingSets([col("NAME")], [col("ID")]))
        .agg(sum_("V"))
    )
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src(
                'base(session) .group_by_grouping_sets(GroupingSets([col("NAME")], [col("ID")])) .agg...'
            ),
            src(
                'base(session) .group_by_grouping_sets(GroupingSets([col("NAME")], [col("ID")]))'
            ),
            BASE,
        ],
        mapped=mapped,
    )


def test_df_agg(session, trace_settings):
    df = fail(base(session).agg(sum_("V")))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex, trace=[FAIL, src('base(session).agg(sum_("V"))'), BASE], mapped=mapped
    )


def test_pivot(session, trace_settings):
    df = fail(
        base(session).select("ID", "NAME", "V").pivot("NAME", ["a", "b"]).sum("V")
    )
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src(
                'base(session).select("ID", "NAME", "V").pivot("NAME", ["a", "b"]).sum...'
            ),
            src('base(session).select("ID", "NAME", "V").pivot("NAME", ["a", "b"])'),
            src('base(session).select("ID", "NAME", "V")'),
            BASE,
        ],
        mapped=mapped,
    )


def test_grouped_pivot(session, trace_settings):
    df = fail(base(session).group_by("ID").pivot("NAME", ["a", "b"]).agg(sum_("V")))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src(
                'base(session).group_by("ID").pivot("NAME", ["a", "b"]).agg(sum_("V"))'
            ),
            src('base(session).group_by("ID").pivot("NAME", ["a", "b"])'),
            src('base(session).group_by("ID")'),
            BASE,
        ],
        mapped=mapped,
    )


def test_unpivot(session, trace_settings):
    with pytest.raises(SnowparkSQLException) as ex:
        df = fail(base(session).unpivot("VAL", "COL", ["ID", "V"]))
        df.collect()
    trace = [src('base(session).unpivot("VAL", "COL", ["ID", "V"])'), BASE]
    if not trace_settings.simplifier:
        trace = [FAIL, src('base(session).unpivot("VAL", "COL", ["ID", "V"])'), BASE]
    assert_debug_output(ex, trace=trace)


def test_window(session, trace_settings):
    w = Window.partition_by("NAME").order_by("ID")
    df = fail(base(session).with_column("RN", sum_("V").over(w)))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [BASE, src('base(session).with_column("RN", sum_("V").over(w))'), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[FAIL, src('base(session).with_column("RN", sum_("V").over(w))'), BASE],
        mapped=mapped,
    )


def test_join_inner(session, trace_settings):
    df = fail(base(session).join(other(session), on="ID", how="inner"))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('base(session).join(other(session), on="ID", how="inner")'),
            BASE,
            OTHER,
        ],
        mapped=mapped,
    )


def test_join_condition(session, trace_settings):
    a, b = base(session), other(session)
    df = fail(a.join(b, a["ID"] == b["ID"], "left"))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[FAIL, src('a.join(b, a["ID"] == b["ID"], "left")'), BASE, OTHER],
        mapped=mapped,
    )


def test_join_variants(session, trace_settings):
    a, b = base(session), other(session)
    df = fail(
        a.join(b, on="ID", how="right")
        .join(b, on="ID", how="full")
        .join(b, on="ID", how="semi")
    )
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    if trace_settings.cte:
        # Under CTE optimization the query that fails is the rewritten one, whose lines are not
        # described by the line intervals of the plan, so all of its operations are reported.
        mapped = [
            src(
                'a.join(b, on="ID", how="right") .join(b, on="ID", how="full") .join...'
            ),
            FAIL,
        ]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src(
                'a.join(b, on="ID", how="right") .join(b, on="ID", how="full") .join...'
            ),
            OTHER,
            src('a.join(b, on="ID", how="right") .join(b, on="ID", how="full")'),
            src('a.join(b, on="ID", how="right")'),
            BASE,
        ],
        mapped=mapped,
    )


def test_cross_join(session, trace_settings):
    df = fail(
        base(session)
        .cross_join(other(session).select(col("TAG").alias("T2")))
        .crossJoin(other(session).select(col("TAG").alias("T3")))
    )
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    if trace_settings.cte:
        # Under CTE optimization the query that fails is the rewritten one, whose lines are not
        # described by the line intervals of the plan, so all of its operations are reported.
        mapped = [
            src(
                'base(session) .cross_join(other(session).select(col("TAG").alias("T2"))) .crossJoin...'
            ),
            FAIL,
        ]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src(
                'base(session) .cross_join(other(session).select(col("TAG").alias("T2"))) .crossJoin...'
            ),
            src(
                'base(session) .cross_join(other(session).select(col("TAG").alias("T2")))'
            ),
            src('other(session).select(col("TAG").alias("T3"))'),
            BASE,
            src('other(session).select(col("TAG").alias("T2"))'),
            OTHER,
        ],
        mapped=mapped,
    )


def test_natural_join(session, trace_settings):
    df = fail(base(session).natural_join(other(session)))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[FAIL, src("base(session).natural_join(other(session))"), BASE, OTHER],
        mapped=mapped,
    )


def test_self_join(session, trace_settings):
    a = base(session)
    df = fail(a.join(a.select(col("ID").alias("ID2")), col("ID") == col("ID2")))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    if trace_settings.cte:
        # Under CTE optimization the query that fails is the rewritten one, whose lines are not
        # described by the line intervals of the plan, so all of its operations are reported.
        mapped = [
            src('a.join(a.select(col("ID").alias("ID2")), col("ID") == col("ID2"))'),
            FAIL,
        ]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('a.join(a.select(col("ID").alias("ID2")), col("ID") == col("ID2"))'),
            BASE,
            src('a.select(col("ID").alias("ID2"))'),
        ],
        mapped=mapped,
    )


def test_join_table_function(session, trace_settings):
    df = session.create_dataframe([("a b",)], schema=["T"])
    df = fail(df.join_table_function("split_to_table", col("T"), lit(" ")))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('df.join_table_function("split_to_table", col("T"), lit(" "))'),
            src('session.create_dataframe([("a b",)], schema=["T"])'),
        ],
        mapped=mapped,
    )


def test_lateral_join(session, trace_settings):
    a, b = base(session), other(session).select(col("TAG"))
    df = fail(a.lateral_join(b))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src("a.lateral_join(b)"),
            BASE,
            src('other(session).select(col("TAG"))'),
            OTHER,
        ],
        mapped=mapped,
    )


def test_df_flatten(session, trace_settings):
    df = session.create_dataframe([("[1,2]",)], schema=["T"]).select(
        parse_json(col("T")).alias("J")
    )
    df = fail(df.flatten(col("J")))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('df.flatten(col("J"))'),
            src('session.create_dataframe([("[1,2]",)], schema=["T"]).select...'),
            src('session.create_dataframe([("[1,2]",)], schema=["T"])'),
        ],
        mapped=mapped,
    )


def test_union_family(session, trace_settings):
    a, b = other(session), other(session)
    df = fail(a.union(b).union_all(b).unionAll(b))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [src("a.union(b).union_all(b).unionAll(b)"), FAIL]
    if trace_settings.cte:
        mapped = [OTHER]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src("a.union(b).union_all(b).unionAll(b)"),
            OTHER,
            src("a.union(b).union_all(b)"),
            src("a.union(b)"),
        ],
        mapped=mapped,
    )


def test_union_by_name_family(session, trace_settings):
    a, b = other(session), other(session)
    d = a.union_by_name(b).unionByName(b).union_all_by_name(b).unionAllByName(b)
    df = fail(d)
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [
        src("a.union_by_name(b).unionByName(b).union_all_by_name(b).unionAllByName(b)"),
        FAIL,
    ]
    if trace_settings.cte:
        mapped = [OTHER]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src(
                "a.union_by_name(b).unionByName(b).union_all_by_name(b).unionAllByName(b)"
            ),
            OTHER,
            src("a.union_by_name(b).unionByName(b).union_all_by_name(b)"),
            src("a.union_by_name(b).unionByName(b)"),
            src("a.union_by_name(b)"),
        ],
        mapped=mapped,
    )


def test_intersect_except(session, trace_settings):
    a, b = other(session), other(session)
    df = fail(a.intersect(b).except_(b).minus(b).subtract(b))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [src("a.intersect(b).except_(b).minus(b).subtract(b)"), FAIL]
    if trace_settings.cte:
        mapped = [OTHER]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src("a.intersect(b).except_(b).minus(b).subtract(b)"),
            OTHER,
            src("a.intersect(b).except_(b).minus(b)"),
            src("a.intersect(b).except_(b)"),
            src("a.intersect(b)"),
        ],
        mapped=mapped,
    )


def test_random_split_first(session, trace_settings):
    parts = base(session).random_split([0.5, 0.5])
    df = fail(parts[0])
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [src("base(session).random_split([0.5, 0.5])"), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[FAIL, src("base(session).random_split([0.5, 0.5])"), BASE],
        mapped=mapped,
    )


def test_to_df_then_union_with_self(session, trace_settings):
    a = other(session).filter(col("ID") > 0)
    df = fail(a.union_all(a))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [src("a.union_all(a)"), FAIL]
    if trace_settings.cte:
        mapped = [OTHER, src('other(session).filter(col("ID") > 0)')]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src("a.union_all(a)"),
            src('other(session).filter(col("ID") > 0)'),
            OTHER,
        ],
        mapped=mapped,
    )
