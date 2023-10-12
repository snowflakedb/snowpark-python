#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import math

import pytest

from snowflake.snowpark import Window
from snowflake.snowpark._internal.analyzer.analyzer_utils import quote_name
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.functions import (
    any_value,
    call_udf,
    col,
    count_distinct,
    first_value,
    in_,
    lag,
    last_value,
    lead,
    listagg,
    lit,
    rank,
    upper,
    when,
)
from tests.utils import Utils

pytestmark = pytest.mark.xfail(
    condition="config.getvalue('local_testing_mode')",
    raises=NotImplementedError,
    strict=False,
)


def get_metadata_names(session, df):
    description = session._conn._cursor.describe(df.queries["queries"][-1])
    return [quote_name(metadata.name) for metadata in description]


def local_testing_get_metadata_names(session, df):
    return [col.name for col in session._conn.get_result_and_metadata(df._plan)[1]]


@pytest.mark.localtest
def test_like(session, local_testing_mode):
    df1 = (
        session.create_dataframe(["v"], schema=["c"])
        if local_testing_mode
        else session.sql("select 'v' as c")
    )

    df2 = df1.select(df1["c"].like(lit("v%")))
    metadata_names = (
        local_testing_get_metadata_names(session, df2)
        if local_testing_mode
        else get_metadata_names(session, df2)
    )

    assert (
        df2._output[0].name
        == df2.columns[0]
        == metadata_names[0]
        == '"""C"" LIKE \'V%\'"'
    )

    df1 = (
        session.create_dataframe(["v"], schema=['"c c"'])
        if local_testing_mode
        else session.sql("select 'v' as \"c c\"")
    )

    df2 = df1.select(df1["c c"].like(lit("v%")))
    metadata_names = (
        local_testing_get_metadata_names(session, df2)
        if local_testing_mode
        else get_metadata_names(session, df2)
    )

    assert (
        df2._output[0].name
        == df2.columns[0]
        == metadata_names[0]
        == '"""C C"" LIKE \'V%\'"'
    )


def test_regexp(session):
    df1 = session.sql("select 'v' as c")
    df2 = df1.select(df1["c"].regexp(lit("v%")))
    assert (
        df2._output[0].name
        == df2.columns[0]
        == get_metadata_names(session, df2)[0]
        == '"""C"" REGEXP \'V%\'"'
    )

    df1 = session.sql("select 'v' as \"c c\"")
    df2 = df1.select(df1["c c"].regexp(lit("v%")))
    assert (
        df2._output[0].name
        == df2.columns[0]
        == get_metadata_names(session, df2)[0]
        == '"""C C"" REGEXP \'V%\'"'
    )


def test_collate(session):
    df1 = session.sql("select 'v' as c")
    df2 = df1.select(df1["c"].collate("en"))
    assert (
        df2._output[0].name
        == df2.columns[0]
        == get_metadata_names(session, df2)[0]
        == '"""C"" COLLATE \'EN\'"'
    )

    df1 = session.sql("select 'v' as \"c c\"")
    df2 = df1.select(df1["c c"].collate("en"))
    assert (
        df2._output[0].name
        == df2.columns[0]
        == get_metadata_names(session, df2)[0]
        == '"""C C"" COLLATE \'EN\'"'
    )


def test_subfield(session):
    df1 = session.sql('select [1, 2, 3] as c, parse_json(\'{"a": "b"}\') as "c c"')
    df2 = df1.select(df1["C"][0], df1["c c"]["a"])
    assert (
        [x.name for x in df2._output]
        == df2.columns
        == get_metadata_names(session, df2)
        == ['"""C""[0]"', '"""C C""[\'A\']"']
    )


def test_case_when(session):
    df1 = session.sql('select 1 as c, 2 as "c c"')
    df2 = df1.select(when(df1["c"] == 1, lit(True)).when(df1["c"] == 2, lit("abc")))
    assert (
        df2._output[0].name
        == df2.columns[0]
        == get_metadata_names(session, df2)[0]
        == '"CASE  WHEN (""C"" = 1 :: INT) THEN TRUE :: BOOLEAN WHEN (""C"" = 2 :: INT) THEN \'ABC\' ELSE NULL END"'
    )


def test_multiple_expression(session):
    df1 = session.sql("select 1 as c, 'v' as \"c c\"")
    df2 = df1.select(in_(["c", "c c"], [[lit(1), lit("v")]]))
    assert (
        df2._output[0].name
        == df2.columns[0]
        == get_metadata_names(session, df2)[0]
        == '"(""C"", ""C C"") IN ((1 :: INT, \'V\'))"'
    )


def test_in_expression(session):
    df1 = session.sql("select 1 as c, 'v' as \"c c\"")
    df2 = df1.select(df1["c"].in_(1, 2, 3), df1["c c"].in_("v"))
    assert (
        [x.name for x in df2._output]
        == df2.columns
        == get_metadata_names(session, df2)
        == ['"""C"" IN (1 :: INT, 2 :: INT, 3 :: INT)"', '"""C C"" IN (\'V\')"']
    )


@pytest.mark.skip("df2.columns has wrong result. Bug needs to be fixed.")
def test_scalar_subquery(session):
    df1 = session.sql("select 1 as c, 'v' as \"c c\"")
    df2 = df1.select(df1["c c"].in_(session.sql("select 'v'")))
    assert (
        df2._output[0].name
        == df2.columns[0]
        # wrong result was returned for df2.columns both with and without sql simplifier.
        == '"""C C"" IN ((SELECT \'V\'))"'
    )


def test_specified_window_frame(session):
    df1 = session.sql("select 'v' as \" a\"")
    assert df1._output[0].name == '" a"'
    assert df1.columns[0] == '" a"'
    df2 = df1.select(rank().over(Window.order_by('" a"').range_between(1, 2)) - 1)
    assert (
        df2._output[0].name
        == df2.columns[0]
        == get_metadata_names(session, df2)[0]
        == '"(RANK() OVER (  ORDER BY "" A"" ASC NULLS FIRST  RANGE BETWEEN 1 FOLLOWING  AND 2 FOLLOWING  ) - 1 :: INT)"'
    )


def test_cast(session):
    df1 = session.sql("select 1 as a, 'v' as \" a\"")
    df2 = df1.select(
        df1["a"].cast("string(23)"),
        df1[" a"].try_cast("integer"),
        upper(df1[" a"]).cast("string"),
    )
    assert (
        [x.name for x in df2._output]
        == df2.columns
        == get_metadata_names(session, df2)
        == [
            '"CAST (""A"" AS STRING(23))"',
            '"TRY_CAST ("" A"" AS INT)"',
            '"CAST (UPPER("" A"") AS STRING)"',
        ]
    )


def test_unspecified_frame(session):
    df1 = session.sql("select 'v' as \" a\"")
    assert (
        df1._output[0].name
        == df1.columns[0]
        == get_metadata_names(session, df1)[0]
        == '" a"'
    )
    df2 = df1.select(any_value(df1[" a"]).over())
    assert (
        df2._output[0].name
        == df2.columns[0]
        == get_metadata_names(session, df2)[0]
        == '"ANY_VALUE("" A"") OVER (  )"'
    )


def test_special_frame_boundry(session):
    df1 = session.sql("select 'v' as \" a\"")
    assert df1._output[0].name == '" a"'
    assert df1.columns[0] == '" a"'
    df2 = df1.select(
        rank().over(
            Window.order_by('" a"').range_between(
                Window.UNBOUNDED_PRECEDING, Window.UNBOUNDED_FOLLOWING
            )
        )
        - 1
    )
    assert (
        df2._output[0].name
        == df2.columns[0]
        == get_metadata_names(session, df2)[0]
        == '"(RANK() OVER (  ORDER BY "" A"" ASC NULLS FIRST  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) - 1 :: INT)"'
    )


def test_rank_related_function_expression(session):
    "Lag, Lead, FirstValue, LastValue"
    df1 = session.sql("select 1 as a, 'v' as \" a\"")
    window = Window.order_by(" a")
    df2 = df1.select(
        lag(df1[" a"]).over(window),
        lead(df1[" a"]).over(window),
        first_value(df1[" a"]).over(window),
        last_value(df1[" a"]).over(window),
    )
    assert (
        [x.name for x in df2._output]
        == df2.columns
        == get_metadata_names(session, df2)
        == [
            '"LAG("" A"", 1, NULL) OVER (  ORDER BY "" A"" ASC NULLS FIRST )"',
            '"LEAD("" A"", 1, NULL) OVER (  ORDER BY "" A"" ASC NULLS FIRST )"',
            '"FIRST_VALUE("" A"") OVER (  ORDER BY "" A"" ASC NULLS FIRST )"',
            '"LAST_VALUE("" A"") OVER (  ORDER BY "" A"" ASC NULLS FIRST )"',
        ]
    )
    df3 = df1.select(
        lag(df1["a"]).over(window),
        lead(df1["a"]).over(window),
        first_value(df1["a"]).over(window),
        last_value(df1["a"]).over(window),
    )
    assert (
        [x.name for x in df3._output]
        == df3.columns
        == get_metadata_names(session, df3)
        == [
            '"LAG(""A"", 1, NULL) OVER (  ORDER BY "" A"" ASC NULLS FIRST )"',
            '"LEAD(""A"", 1, NULL) OVER (  ORDER BY "" A"" ASC NULLS FIRST )"',
            '"FIRST_VALUE(""A"") OVER (  ORDER BY "" A"" ASC NULLS FIRST )"',
            '"LAST_VALUE(""A"") OVER (  ORDER BY "" A"" ASC NULLS FIRST )"',
        ]
    )


@pytest.mark.skipif(
    condition="config.getvalue('local_testing_mode')", reason="Relies on DUAL table"
)
def test_literal(session):
    df1 = session.table("dual")
    df2 = df1.select(lit("a"), lit(1), lit(True), lit([1]))
    assert (
        [x.name for x in df2._output]
        == df2.columns
        == get_metadata_names(session, df2)
        == [
            "\"'A'\"",
            '"1 :: INT"',
            '"TRUE :: BOOLEAN"',
            "\"PARSE_JSON('[1]') :: ARRAY\"",
        ]
    )


@pytest.mark.localtest
def test_attribute(session, local_testing_mode):
    df1 = (
        session.create_dataframe([[1, 2]], schema=[" a", "a"])
        if local_testing_mode
        else session.sql('select 1 as " a", 2 as a')
    )

    df2 = df1.select(df1[" a"], df1["a"])
    metadata_names = (
        local_testing_get_metadata_names(session, df2)
        if local_testing_mode
        else get_metadata_names(session, df2)
    )

    assert [x.name for x in df2._output] == metadata_names == ['" a"', '"A"']
    assert df2.columns == [
        '" a"',
        "A",
    ]  # In class ColumnIdentifier, the "" is removed for '"A"'.


@pytest.mark.localtest
def test_unresolved_attribute(session, local_testing_mode):
    df1 = (
        session.create_dataframe([[1, 2]], schema=[" a", "a"])
        if local_testing_mode
        else session.sql('select 1 as " a", 2 as a')
    )

    df2 = df1.select(" a", "a")
    metadata_names = (
        local_testing_get_metadata_names(session, df2)
        if local_testing_mode
        else get_metadata_names(session, df2)
    )

    assert [x.name for x in df2._output] == metadata_names == ['" a"', '"A"']
    assert df2.columns == [
        '" a"',
        "A",
    ]  # In class ColumnIdentifier, the "" is removed for '"A"'.


def test_star(session):
    df1 = session.sql('select 1 as " a", 2 as a')
    df2 = df1.select(df1["*"])
    assert (
        [x.name for x in df2._output]
        == get_metadata_names(session, df2)
        == ['" a"', '"A"']
    )
    assert df2.columns == [
        '" a"',
        "A",
    ]  # In class ColumnIdentifier, the "" is removed for '"A"'.
    df3 = df1.select("*")
    assert (
        [x.name for x in df3._output]
        == get_metadata_names(session, df3)
        == ['" a"', '"A"']
    )
    assert df3.columns == [
        '" a"',
        "A",
    ]  # In class ColumnIdentifier, the "" is removed for '"A"'.


def test_function_expression(session):
    df1 = session.sql("select 'a' as a")
    df2 = df1.select(upper(df1["A"]))
    assert (
        df2._output[0].name
        == df2.columns[0]
        == get_metadata_names(session, df2)[0]
        == '"UPPER(""A"")"'
    )

    df3 = df1.select(count_distinct("a"))
    assert (
        df3._output[0].name
        == df3.columns[0]
        == get_metadata_names(session, df3)[0]
        == '"COUNT( DISTINCT ""A"")"'
    )


@pytest.mark.udf
@pytest.mark.parametrize("use_qualified_name", [True, False])
def test_udf(session, use_qualified_name):
    def add_one(x: int) -> int:
        return x + 1

    special_chars = "quoted_name"
    temp_func_name = (
        f'"{Utils.random_name_for_temp_object(TempObjectType.FUNCTION)}{special_chars}"'
    )
    perm_func_name = (
        f'"{special_chars}{Utils.random_name_for_temp_object(TempObjectType.FUNCTION)}"'
    )
    stage_name = Utils.random_stage_name()
    try:
        Utils.create_stage(session, stage_name, is_temporary=False)
        df = session.create_dataframe([1, 2], schema=["a"])
        session.udf.register(add_one, name=temp_func_name, is_permanent=False)
        session.udf.register(
            add_one, name=perm_func_name, is_permanent=True, stage_location=stage_name
        )
        if use_qualified_name:
            database_name = session.get_current_database()
            schema_name = session.get_current_schema()
            full_temp_func_name = f"{database_name}.{schema_name}.{temp_func_name}"
            df_temp = df.select(call_udf(full_temp_func_name, col("a")))
            assert (
                df_temp._output[0].name
                == get_metadata_names(session, df_temp)[0]
                == f'""{database_name}"."{schema_name}"."{temp_func_name.upper()}"(""A"")"'
            )
            assert (
                df_temp.columns[0]
                == get_metadata_names(session, df_temp)[0]
                == f'""{database_name}"."{schema_name}"."{temp_func_name.upper()}"(""A"")"'
            )

            full_perm_func_name = f"{session.get_current_database()}.{session.get_current_schema()}.{perm_func_name}"
            df_perm = df.select(call_udf(full_perm_func_name, col("a")))
            assert (
                df_perm._output[0].name
                == get_metadata_names(session, df_perm)[0]
                == f'""{database_name}"."{schema_name}"."{perm_func_name.upper()}"(""A"")"'
            )
            assert (
                df_perm.columns[0]
                == get_metadata_names(session, df_perm)[0]
                == f'""{database_name}"."{schema_name}"."{perm_func_name.upper()}"(""A"")"'
            )
        else:
            df_temp = df.select(call_udf(temp_func_name, col("a")))
            assert (
                df_temp._output[0].name
                == df_temp.columns[0]
                == get_metadata_names(session, df_temp)[0]
                == f'""{temp_func_name.upper()}"(""A"")"'
            )

            df_perm = df.select(call_udf(perm_func_name, col("a")))
            assert (
                df_perm._output[0].name
                == df_perm.columns[0]
                == get_metadata_names(session, df_perm)[0]
                == f'""{perm_func_name.upper()}"(""A"")"'
            )
    finally:
        session._run_query(f"drop function if exists {temp_func_name}(int)")
        session._run_query(f"drop function if exists {perm_func_name}(int)")
        Utils.drop_stage(session, stage_name)


def test_unary_expression(session):
    """Alias, UnresolvedAlias, Cast, UnaryMinus, IsNull, IsNotNull, IsNaN, Not"""
    df1 = session.sql('select 1 as " a", 2 as a')
    df2 = df1.select(
        df1[" a"].cast("string"),
        df1[" a"].alias(" b"),
        -df1[" a"],
        df1[" a"].is_null(),
        df1[" a"].is_not_null(),
        df1[" a"].equal_nan(),
        ~(df1[" a"] == 1),
    )
    assert (
        [x.name for x in df2._output]
        == df2.columns
        == get_metadata_names(session, df2)
        == [
            '"CAST ("" A"" AS STRING)"',
            '" b"',
            '"- "" A"""',
            '""" A"" IS NULL"',
            '""" A"" IS NOT NULL"',
            '""" A"" = \'NAN\'"',
            '"NOT ("" A"" = 1 :: INT)"',
        ]
    )

    df3 = df1.select(
        df1["a"].cast("string(87)"),
        df1["a"].alias("b"),
        -df1["a"],
        df1["a"].is_null(),
        df1["a"].is_not_null(),
        df1["a"].equal_nan(),
        ~(df1["a"] == 1),
    )
    assert (
        [x.name for x in df3._output]
        == get_metadata_names(session, df3)
        == [
            '"CAST (""A"" AS STRING(87))"',
            '"B"',
            '"- ""A"""',
            '"""A"" IS NULL"',
            '"""A"" IS NOT NULL"',
            '"""A"" = \'NAN\'"',
            '"NOT (""A"" = 1 :: INT)"',
        ]
    )
    assert df3.columns == [
        '"CAST (""A"" AS STRING(87))"',
        "B",
        '"- ""A"""',
        '"""A"" IS NULL"',
        '"""A"" IS NOT NULL"',
        '"""A"" = \'NAN\'"',
        '"NOT (""A"" = 1 :: INT)"',
    ]  # In class ColumnIdentifier, the "" is removed for '"B"'.


def test_list_agg_within_group_sort_order(session):
    df1 = session.sql(
        'select c as "a b" from (select c from values((1), (2), (3)) as t(c))'
    )
    df2 = df1.select(
        listagg(df1["a b"], "a", is_distinct=True)
        .within_group(df1["a b"].asc())
        .over(Window.partition_by(df1["a b"]))
    )
    assert (
        df2._output[0].name
        == df2.columns[0]
        == get_metadata_names(session, df2)[0]
        == '"LISTAGG ( DISTINCT ""A B"", \'A\') WITHIN GROUP ( ORDER BY ""A B"" ASC NULLS FIRST) OVER (PARTITION BY ""A B""  )"'
    )


def test_binary_expression(session):
    """=, !=, >, <, >=, <=, EQUAL_NULL, AND, OR, +, -, *, /, %, POWER, BITAND, BITOR, BITXOR"""
    df1 = session.sql("select 1 as \" a\", 'x' as \" b\", 1 as a, 'x' as b")
    df2 = df1.select(
        df1[" a"] == "x",
        df1[" a"] != "x",
        df1[" a"] > "x",
        df1[" a"] <= "x",
        df1[" a"].equal_null(df1[" b"]),
        (df1[" b"] == "x") & (df1[" a"] == 1),
        (df1[" b"] == "x") | (df1[" a"] == 1),
        df1[" b"].bitand(lit(1)),
        df1[" b"].bitor(lit(1)),
        df1[" b"].bitxor(lit(1)),
        pow(df1[" b"], 2),
        df1[" a"] + df1[" a"],
        df1[" a"] - df1[" a"],
        df1[" a"] * df1[" a"],
        df1[" a"] / df1[" a"],
        df1[" a"] % df1[" a"],
    )
    assert (
        [x.name for x in df2._output]
        == df2.columns
        == get_metadata_names(session, df2)
        == [
            '"("" A"" = \'X\')"',
            '"("" A"" != \'X\')"',
            '"("" A"" > \'X\')"',
            '"("" A"" <= \'X\')"',
            '"EQUAL_NULL("" A"", "" B"")"',
            '"(("" B"" = \'X\') AND ("" A"" = 1 :: INT))"',
            '"(("" B"" = \'X\') OR ("" A"" = 1 :: INT))"',
            '"BITAND(1 :: INT, "" B"")"',
            '"BITOR(1 :: INT, "" B"")"',
            '"BITXOR(1 :: INT, "" B"")"',
            '"POWER("" B"", 2 :: INT)"',
            '"("" A"" + "" A"")"',
            '"("" A"" - "" A"")"',
            '"("" A"" * "" A"")"',
            '"("" A"" / "" A"")"',
            '"("" A"" % "" A"")"',
        ]
    )
    df3 = df1.select(
        df1["a"] == "x",
        df1["a"] != "x",
        df1["a"] > "x",
        df1["a"] <= "x",
        df1["a"].equal_null(df1["b"]),
        (df1["b"] == "x") & (df1["a"] == 1),
        (df1["b"] == "x") | (df1["a"] == 1),
        df1["b"].bitand(lit(1)),
        df1["b"].bitor(lit(1)),
        df1["b"].bitxor(lit(1)),
        pow(df1["b"], 2),
        df1["a"] + df1["a"],
        df1["a"] - df1["a"],
        df1["a"] * df1["a"],
        df1["a"] / df1["a"],
        df1["a"] % df1["a"],
    )
    assert (
        [x.name for x in df3._output]
        == df3.columns
        == [
            '"(""A"" = \'X\')"',
            '"(""A"" != \'X\')"',
            '"(""A"" > \'X\')"',
            '"(""A"" <= \'X\')"',
            '"EQUAL_NULL(""A"", ""B"")"',
            '"((""B"" = \'X\') AND (""A"" = 1 :: INT))"',
            '"((""B"" = \'X\') OR (""A"" = 1 :: INT))"',
            '"BITAND(1 :: INT, ""B"")"',
            '"BITOR(1 :: INT, ""B"")"',
            '"BITXOR(1 :: INT, ""B"")"',
            '"POWER(""B"", 2 :: INT)"',
            '"(""A"" + ""A"")"',
            '"(""A"" - ""A"")"',
            '"(""A"" * ""A"")"',
            '"(""A"" / ""A"")"',
            '"(""A"" % ""A"")"',
        ]
    )


def test_cast_nan_column_name(session):
    df1 = session.sql("select 'a' as a")
    df2 = df1.select(df1["A"] == math.nan)
    assert (
        df2._output[0].name
        == df2.columns[0]
        == get_metadata_names(session, df2)[0]
        == '"(""A"" = \'NAN\' :: FLOAT)"'
    )


def test_inf_column_name(session):
    df1 = session.sql("select 'inf'")
    df2 = df1.select(df1["'INF'"] == math.inf)
    assert (
        df2._output[0].name
        == df2.columns[0]
        == get_metadata_names(session, df2)[0]
        == '"(""\'INF\'"" = \'INF\' :: FLOAT)"'
    )


@pytest.mark.skip("grougping sets doesn't use local column name inference yet.")
def test_grouping_sets(session):
    ...


@pytest.mark.skip("table function doesn't use local inferred column names")
def test_table_function():
    ...
