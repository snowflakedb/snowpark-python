#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""DataFrame trace of dataframes used in column expressions and of the UDFs, UDTFs and UDAFs used by an operation.

Each test builds a dataframe whose action fails on the server, typically with a final ``select``
of a missing column, and checks the debug output of the error: the lineage trace, the SQL error
to Python source mapping and the missing or existing object context. ``src`` gives the expected
entry of a call from the source of the test, so every position is checked exactly.
"""

import pytest

try:
    import pandas  # noqa: F401

    is_pandas_available = True
except ImportError:
    is_pandas_available = False

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


def test_isin_subquery_in_filter(session, trace_settings):
    a, b = base(session), other(session).select("ID")
    df = fail(a.filter(col("ID").isin(b)))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('a.filter(col("ID").isin(b))'),
            BASE,
            src('other(session).select("ID")'),
            OTHER,
        ],
        mapped=mapped,
    )


def test_isin_nested_in_boolean(session, trace_settings):
    a, b = base(session), other(session).select("ID")
    df = fail(a.filter((col("ID").isin(b)) & (col("V") > 0) | (col("ID") == 3)))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('a.filter((col("ID").isin(b)) & (col("V") > 0) | (col("ID") == 3))'),
            BASE,
            src('other(session).select("ID")'),
            OTHER,
        ],
        mapped=mapped,
    )


def test_isin_in_when_otherwise(session, trace_settings):
    from snowflake.snowpark.functions import when

    a, b = base(session), other(session).select("ID")
    df = fail(a.with_column("F", when(col("ID").isin(b), lit(1)).otherwise(lit(0))))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [
        src('a.with_column("F", when(col("ID").isin(b), lit(1)).otherwise(lit(0)))'),
        FAIL,
    ]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src(
                'a.with_column("F", when(col("ID").isin(b), lit(1)).otherwise(lit(0)))'
            ),
            BASE,
            src('other(session).select("ID")'),
            OTHER,
        ],
        mapped=mapped,
    )


def test_isin_in_select(session, trace_settings):
    a, b = base(session), other(session).select("ID")
    df = fail(a.select("ID", col("ID").isin(b).alias("IN_B")))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [src('a.select("ID", col("ID").isin(b).alias("IN_B"))'), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('a.select("ID", col("ID").isin(b).alias("IN_B"))'),
            BASE,
            src('other(session).select("ID")'),
            OTHER,
        ],
        mapped=mapped,
    )


def test_isin_in_join_condition(session, trace_settings):
    a, b, sub = (
        base(session),
        other(session),
        other(session).select(col("ID").alias("X")),
    )
    df = fail(a.join(b, (a["ID"] == b["ID"]) & a["ID"].isin(sub)))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('a.join(b, (a["ID"] == b["ID"]) & a["ID"].isin(sub))'),
            BASE,
            OTHER,
            src('other(session).select(col("ID").alias("X"))'),
        ],
        mapped=mapped,
    )


def test_subquery_reused_in_later_operation(session, trace_settings):
    a, b = base(session), other(session).select("ID")
    filtered = a.filter(col("ID").isin(b))
    df = fail(filtered.select("ID", "NAME").sort("ID"))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [
        src('filtered.select("ID", "NAME").sort("ID")'),
        FAIL,
    ]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('filtered.select("ID", "NAME").sort("ID")'),
            src('filtered.select("ID", "NAME")'),
            src('a.filter(col("ID").isin(b))'),
            BASE,
            src('other(session).select("ID")'),
            OTHER,
        ],
        mapped=mapped,
    )


def test_same_dataframe_as_receiver_and_subquery(session, trace_settings):
    a = base(session)
    df = fail(a.filter(col("ID").isin(a.select("ID"))))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('a.filter(col("ID").isin(a.select("ID")))'),
            BASE,
            src('a.select("ID")'),
        ],
        mapped=mapped,
    )


def test_two_subqueries_one_call(session, trace_settings):
    a = base(session)
    b, d = other(session).select("ID"), other(session).select(col("ID").alias("ID"))
    df = fail(a.filter(col("ID").isin(b) | col("ID").isin(d)))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('a.filter(col("ID").isin(b) | col("ID").isin(d))'),
            BASE,
            src('other(session).select("ID")'),
            src('other(session).select(col("ID").alias("ID"))'),
            OTHER,
        ],
        mapped=mapped,
    )


def test_isin_in_sort(session, trace_settings):
    a, b = base(session), other(session).select("ID")
    df = fail(a.sort(col("ID").isin(b)))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('a.sort(col("ID").isin(b))'),
            BASE,
            src('other(session).select("ID")'),
            OTHER,
        ],
        mapped=mapped,
    )


def test_udf_lambda(session, trace_settings):
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


def test_udf_decorator(session, trace_settings):
    @udf(return_type=IntegerType(), input_types=[IntegerType()])
    def plus_one(x):
        return x + 1

    df = fail(base(session).select(plus_one(col("ID")).alias("U")))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [BASE, src('base(session).select(plus_one(col("ID")).alias("U"))'), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('base(session).select(plus_one(col("ID")).alias("U"))'),
            src("udf(return_type=IntegerType(), input_types=[IntegerType()])"),
            BASE,
        ],
        mapped=mapped,
    )


def test_udf_register_named(session, trace_settings):
    f = session.udf.register(
        lambda x: x * 2,
        return_type=IntegerType(),
        input_types=[IntegerType()],
        name=random_name("DF_TRACE_UDF"),
        is_permanent=False,
        replace=True,
    )
    df = fail(base(session).filter(f(col("ID")) > 0))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [BASE, src('base(session).filter(f(col("ID")) > 0)'), FAIL]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('base(session).filter(f(col("ID")) > 0)'),
            src("session.udf.register..."),
            BASE,
        ],
        mapped=mapped,
    )


@pytest.mark.skipif(
    not is_pandas_available, reason="pandas is required to register vectorized UDFs"
)
def test_udf_vectorized(session, trace_settings):
    from snowflake.snowpark.functions import pandas_udf
    from snowflake.snowpark.types import PandasSeriesType

    f = pandas_udf(
        lambda x: x + 1,
        return_type=PandasSeriesType(IntegerType()),
        input_types=[PandasSeriesType(IntegerType())],
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
            src("pandas_udf(..."),
            BASE,
        ],
        mapped=mapped,
    )


def test_udf_used_twice(session, trace_settings):
    f = udf(lambda x: x + 1, return_type=IntegerType(), input_types=[IntegerType()])
    df = base(session).with_column("U", f(col("ID")))
    df = fail(df.filter(f(col("U")) > 0))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [
        src('df.filter(f(col("U")) > 0)'),
        FAIL,
    ]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('df.filter(f(col("U")) > 0)'),
            src("udf(..."),
            src('base(session).with_column("U", f(col("ID")))'),
            BASE,
        ],
        mapped=mapped,
    )


def test_udf_nested_in_expression(session, trace_settings):
    f = udf(lambda x: x + 1, return_type=IntegerType(), input_types=[IntegerType()])
    df = fail(base(session).with_column("U", (f(col("ID")) * 2 + 1).cast("int")))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [
        BASE,
        src('base(session).with_column("U", (f(col("ID")) * 2 + 1).cast("int"))'),
        FAIL,
    ]
    if not trace_settings.simplifier:
        mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('base(session).with_column("U", (f(col("ID")) * 2 + 1).cast("int"))'),
            src("udf(..."),
            BASE,
        ],
        mapped=mapped,
    )


def test_udtf_join_table_function(session, trace_settings):
    from snowflake.snowpark.functions import udtf
    from snowflake.snowpark.types import StructField, StructType

    class Echo:
        def process(self, x):
            yield (x,)

    t = udtf(
        Echo,
        output_schema=StructType([StructField("E", IntegerType())]),
        input_types=[IntegerType()],
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
            src("udtf(..."),
        ],
        mapped=mapped,
    )


def test_udtf_session_table_function(session, trace_settings):
    from snowflake.snowpark.functions import udtf
    from snowflake.snowpark.types import StructField, StructType

    class Count:
        def process(self, n):
            for i in range(n):
                yield (i,)

    t = udtf(
        Count,
        output_schema=StructType([StructField("I", IntegerType())]),
        input_types=[IntegerType()],
    )
    df = fail(session.table_function(t(lit(3))))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src("session.table_function(t(lit(3)))"),
            src("t(lit(3))"),
            src("udtf(..."),
        ],
        mapped=mapped,
    )


def test_udaf_in_agg(session, trace_settings):
    from snowflake.snowpark.functions import udaf

    class Summer:
        def __init__(self) -> None:
            self._s = 0

        @property
        def aggregate_state(self):
            return self._s

        def accumulate(self, x):
            self._s += x

        def merge(self, other):
            self._s += other

        def finish(self):
            return self._s

    agg = udaf(Summer, return_type=IntegerType(), input_types=[IntegerType()])
    df = fail(base(session).group_by("NAME").agg(agg(col("ID")).alias("S")))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    mapped = [FAIL]
    assert_debug_output(
        ex,
        trace=[
            FAIL,
            src('base(session).group_by("NAME").agg(agg(col("ID")).alias("S"))'),
            src("udaf(Summer, return_type=IntegerType(), input_types=[IntegerType()])"),
            src('base(session).group_by("NAME")'),
            BASE,
        ],
        mapped=mapped,
    )
