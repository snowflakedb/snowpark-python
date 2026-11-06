#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""DataFrame trace of the shapes of user code (call sites, loops, multi-line calls) and of plans reused under CTE optimization.

Each test builds a dataframe whose action fails on the server, typically with a final ``select``
of a missing column, and checks the debug output of the error: the lineage trace, the SQL error
to Python source mapping and the missing or existing object context. ``src`` gives the expected
entry of a call from the source of the test, so every position is checked exactly.
"""

import pytest

from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import col, lit, udf
from snowflake.snowpark.types import IntegerType
from tests.integ.df_trace_utils import (
    assert_debug_output,
    BASE,
    base,
    fail,
    FAIL,
    other,
    OTHER,
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


_UDTF_SOURCE = "class Echo:\n    def process(self, x):\n        yield (x,)\n"

_UDAF_SOURCE = (
    "class Summer:\n"
    "    def __init__(self):\n        self._s = 0\n"
    "    @property\n    def aggregate_state(self):\n        return self._s\n"
    "    def accumulate(self, x):\n        self._s += x\n"
    "    def merge(self, other):\n        self._s += other\n"
    "    def finish(self):\n        return self._s\n"
)


def _source_file(name, text):
    import os
    import tempfile

    path = os.path.join(tempfile.mkdtemp(), name)
    with open(path, "w") as f:
        f.write(text)
    return path


def test_chain_50(session, trace_settings):
    df = base(session)
    for i in range(50):
        df = df.with_column(f"C{i}", col("ID") + i)
    df = fail(df)
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [BASE, src('df.with_column(f"C{i}", col("ID") + i)'), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[FAIL, src('df.with_column(f"C{i}", col("ID") + i)'), BASE],
        mapped=mapped,
    )


def test_loop_reassign(session, trace_settings):
    df = base(session)
    for i in range(5):
        df = df.with_column("NEW", df["ID"] + i)
    df = fail(df)
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [BASE, src('df.with_column("NEW", df["ID"] + i)'), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[FAIL, src('df.with_column("NEW", df["ID"] + i)'), BASE],
        mapped=mapped,
    )


def test_diamond(session, trace_settings):
    root = base(session).filter(col("ID") > 0)
    left = root.select("ID", "NAME")
    right = root.select(col("ID").alias("ID2"), "V")
    df = fail(left.join(right, col("ID") == col("ID2")))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    if trace_settings.cte:
        # Under CTE optimization the query that fails is the rewritten one, whose lines are not
        # described by the line intervals of the plan, so all of its operations are reported.
        mapped = [src('left.join(right, col("ID") == col("ID2"))'), FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('left.join(right, col("ID") == col("ID2"))'),
            src('root.select("ID", "NAME")'),
            src('root.select(col("ID").alias("ID2"), "V")'),
            src('base(session).filter(col("ID") > 0)'),
            BASE,
        ],
        mapped=mapped,
    )


def test_same_df_twice_in_expression(session, trace_settings):
    a = base(session)
    df = fail(a.filter(col("ID").isin(a.select("ID").filter(col("ID") > 1))))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('a.filter(col("ID").isin(a.select("ID").filter(col("ID") > 1)))'),
            BASE,
            src('a.select("ID").filter(col("ID") > 1)'),
            src('a.select("ID")'),
        ],
        mapped=mapped,
    )


def test_multiline_call(session, trace_settings):
    df = (
        base(session)
        .select(
            "ID",
            "NAME",
        )
        .with_column(
            "X",
            col("ID") + 1,
        )
    )
    df = fail(df)
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [
        BASE,
        src('base(session) .select( "ID", "NAME", )'),
        src('base(session) .select( "ID", "NAME", ) .with_column...'),
        FAIL,
    ]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('base(session) .select( "ID", "NAME", ) .with_column...'),
            src('base(session) .select( "ID", "NAME", )'),
            BASE,
        ],
        mapped=mapped,
    )


def test_many_calls_one_line(session, trace_settings):
    d = base(session).filter(col("ID") > 0).select("ID", "NAME").sort("ID").limit(2)
    df = fail(d)
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [
        BASE,
        src(
            'base(session).filter(col("ID") > 0).select("ID", "NAME").sort("ID").limit...'
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
                'base(session).filter(col("ID") > 0).select("ID", "NAME").sort("ID").limit...'
            ),
            src('base(session).filter(col("ID") > 0).select("ID", "NAME").sort("ID")'),
            src('base(session).filter(col("ID") > 0).select("ID", "NAME")'),
            src('base(session).filter(col("ID") > 0)'),
            BASE,
        ],
        mapped=mapped,
    )


def test_lambda_call_site(session, trace_settings):
    f = lambda d: d.filter(col("ID") > 0).select("ID")  # noqa: E731
    df = fail(f(base(session)))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [BASE, src('d.filter(col("ID") > 0).select("ID")'), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('d.filter(col("ID") > 0).select("ID")'),
            src('d.filter(col("ID") > 0)'),
            BASE,
        ],
        mapped=mapped,
    )


def test_helper_functions(session, trace_settings):
    def add(d):
        return d.with_column("X", col("ID") + 1)

    def mid(d):
        return add(add(d)).filter(col("X") > 0)

    df = fail(mid(base(session)))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [
        src('add(add(d)).filter(col("X") > 0)'),
        FAIL,
    ]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('add(add(d)).filter(col("X") > 0)'),
            src('d.with_column("X", col("ID") + 1)'),
            BASE,
        ],
        mapped=mapped,
    )


def test_comprehension_call_site(session, trace_settings):
    dfs = [base(session).with_column("K", lit(i)) for i in range(3)]
    out = dfs[0]
    for d in dfs[1:]:
        out = out.union_all(d)
    df = fail(out)
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [src("out.union_all(d)"), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src("out.union_all(d)"),
            src('base(session).with_column("K", lit(i))'),
            BASE,
        ],
        mapped=mapped,
    )


def test_wide_projection(session, trace_settings):
    df = base(session)
    df = fail(df.select(*[(col("ID") + i).alias(f"W{i}") for i in range(200)]))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [
        BASE,
        src('df.select(*[(col("ID") + i).alias(f"W{i}") for i in range(200)])'),
        FAIL,
    ]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('df.select(*[(col("ID") + i).alias(f"W{i}") for i in range(200)])'),
            BASE,
        ],
        mapped=mapped,
    )


def test_udf_in_pipeline(session, trace_settings):
    f = udf(lambda x: x + 1, return_type=IntegerType(), input_types=[IntegerType()])
    df = fail(base(session).with_column("U", f(col("ID"))))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [BASE, src('base(session).with_column("U", f(col("ID")))'), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('base(session).with_column("U", f(col("ID")))'),
            src("udf(..."),
            BASE,
        ],
        mapped=mapped,
    )


def test_union_all_self(session, trace_settings):
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


def test_self_join_reused(session, trace_settings):
    a = base(session).filter(col("ID") > 0)
    df = fail(a.join(a.select(col("ID").alias("I2")), col("ID") == col("I2")))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    if trace_settings.cte:
        # Under CTE optimization the query that fails is the rewritten one, whose lines are not
        # described by the line intervals of the plan, so all of its operations are reported.
        mapped = [
            src('a.join(a.select(col("ID").alias("I2")), col("ID") == col("I2"))'),
            FAIL,
        ]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('a.join(a.select(col("ID").alias("I2")), col("ID") == col("I2"))'),
            src('base(session).filter(col("ID") > 0)'),
            src('a.select(col("ID").alias("I2"))'),
            BASE,
        ],
        mapped=mapped,
    )


def test_describe_reused(session, trace_settings):
    d = base(session).describe()
    df = fail(d.union_all(d))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [src("d.union_all(d)"), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[FAIL, src("d.union_all(d)"), src("base(session).describe()"), BASE],
        mapped=mapped,
    )


def test_large_create_dataframe_reused(session, trace_settings):
    big = session.create_dataframe(
        [(i, str(i)) for i in range(20000)], schema=["ID", "NAME"]
    )
    df = fail(big.union_all(big.filter(col("ID") > 5)))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [src('big.union_all(big.filter(col("ID") > 5))'), FAIL]
    if trace_settings.cte:
        mapped = [src("session.create_dataframe...")]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('big.union_all(big.filter(col("ID") > 5))'),
            src("session.create_dataframe..."),
            src('big.filter(col("ID") > 5)'),
        ],
        mapped=mapped,
    )


def test_three_way_reuse(session, trace_settings):
    a = other(session).with_column("K", lit(1))
    df = fail(a.union_all(a).union_all(a))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [src("a.union_all(a).union_all(a)"), FAIL]
    if trace_settings.cte:
        mapped = [OTHER, src('other(session).with_column("K", lit(1))')]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src("a.union_all(a).union_all(a)"),
            src('other(session).with_column("K", lit(1))'),
            src("a.union_all(a)"),
            OTHER,
        ],
        mapped=mapped,
    )


def test_crosstab_reused(session, trace_settings):
    x = base(session).stat.crosstab("ID", "NAME")
    df = fail(x.union_all(x))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [src("x.union_all(x)"), FAIL]
    if trace_settings.cte:
        mapped = [src('base(session).stat.crosstab("ID", "NAME")')]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src("x.union_all(x)"),
            src('base(session).stat.crosstab("ID", "NAME")'),
            BASE,
        ],
        mapped=mapped,
    )


def test_udf_register_from_file(session, trace_settings):
    import os
    import tempfile

    path = os.path.join(tempfile.mkdtemp(), "df_trace_udf.py")
    with open(path, "w") as f:
        f.write("def plus_one(x):\n    return x + 1\n")
    f = session.udf.register_from_file(
        path, "plus_one", return_type=IntegerType(), input_types=[IntegerType()]
    )
    df = fail(base(session).with_column("U", f(col("ID"))))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [BASE, src('base(session).with_column("U", f(col("ID")))'), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('base(session).with_column("U", f(col("ID")))'),
            src("session.udf.register_from_file..."),
            BASE,
        ],
        mapped=mapped,
    )


def test_udf_registration_describe(session, trace_settings):
    f = session.udf.register(
        lambda x: x + 1,
        return_type=IntegerType(),
        input_types=[IntegerType()],
        name=random_name("DF_TRACE_UDF_D"),
        is_permanent=False,
        replace=True,
    )
    df = fail(session.udf.describe(f))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [src("session.udf.describe(f)"), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(ex, trace=[FAIL, src("session.udf.describe(f)")], mapped=mapped)


def test_udtf_register_from_file_and_describe(session, trace_settings):
    from snowflake.snowpark.types import StructField, StructType

    t = session.udtf.register_from_file(
        _source_file("df_trace_udtf.py", _UDTF_SOURCE),
        "Echo",
        output_schema=StructType([StructField("E", IntegerType())]),
        input_types=[IntegerType()],
        name=random_name("DF_TRACE_UDTF"),
        is_permanent=False,
        replace=True,
    )
    df = fail(base(session).join_table_function(t(col("ID"))))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('base(session).join_table_function(t(col("ID")))'),
            BASE,
            src('t(col("ID"))'),
            src("session.udtf.register_from_file..."),
        ],
        mapped=mapped,
    )


def test_udaf_register_from_file_and_describe(session, trace_settings):
    agg = session.udaf.register_from_file(
        _source_file("df_trace_udaf.py", _UDAF_SOURCE),
        "Summer",
        return_type=IntegerType(),
        input_types=[IntegerType()],
        name=random_name("DF_TRACE_UDAF"),
        is_permanent=False,
        replace=True,
    )
    session.udaf.describe(agg).collect()
    df = fail(base(session).group_by("NAME").agg(agg(col("ID")).alias("S")))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('base(session).group_by("NAME").agg(agg(col("ID")).alias("S"))'),
            src("session.udaf.register_from_file..."),
            src('base(session).group_by("NAME")'),
            BASE,
        ],
        mapped=mapped,
    )
