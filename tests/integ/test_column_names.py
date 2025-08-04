#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import math
from typing import List, Optional

import pytest

from snowflake.snowpark import Row, Session, Window
from snowflake.snowpark._internal.utils import TempObjectType, quote_name
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.functions import (
    any_value,
    avg,
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
    make_interval,
    rank,
    upper,
    when,
)
from snowflake.snowpark.mock._connection import MockServerConnection
from snowflake.snowpark.types import (
    ArrayType,
    BooleanType,
    DataType,
    DecimalType,
    DoubleType,
    LongType,
    StringType,
    TimestampTimeZone,
    TimestampType,
    VariantType,
)
from tests.utils import IS_IN_STORED_PROC, Utils

paramList = [True, False]


@pytest.fixture(params=paramList, autouse=True)
def setup(request, session):
    is_eliminate_numeric_sql_value_cast_enabled = (
        session.eliminate_numeric_sql_value_cast_enabled
    )
    session.eliminate_numeric_sql_value_cast_enabled = request.param
    yield
    session.eliminate_numeric_sql_value_cast_enabled = (
        is_eliminate_numeric_sql_value_cast_enabled
    )


def get_metadata_names(session: Session, df: DataFrame):
    if isinstance(session._conn, MockServerConnection):
        return [col.name for col in session._conn.get_result_and_metadata(df._plan)[1]]

    description = session._conn._cursor.describe(df.queries["queries"][-1])
    return [quote_name(metadata.name) for metadata in description]


def verify_column_result(
    session: Session,
    df: DataFrame,
    expected_column_names: List[str],
    expected_dtypes: List[DataType],
    expected_rows: Optional[List[Row]],
):
    metadata_column_names = get_metadata_names(session, df)
    metadata_column_dtypes = [col.datatype for col in df.schema]
    output_names = [output.name for output in df._output]
    assert output_names == df.columns == metadata_column_names == expected_column_names
    for (datatype, expected_type) in zip(metadata_column_dtypes, expected_dtypes):
        if isinstance(expected_type, StringType):
            assert isinstance(datatype, StringType)
        elif isinstance(expected_type, TimestampType):
            assert isinstance(datatype, TimestampType)
        else:
            assert datatype == expected_type

    if expected_rows is not None:
        res = df.collect()
        assert res == expected_rows


def test_nested_alias(session):
    df = session.create_dataframe(["v"], schema=["c"])
    df2 = df.select(df.c.alias("foo").alias("bar"))
    rows = df.collect()
    assert df2.columns == ["BAR"]
    assert rows == [Row(BAR="v")]


def test_like(session):
    df1 = session.create_dataframe(["v"], schema=["c"])
    df2 = df1.select(df1["c"].like(lit("v%")))

    verify_column_result(
        session, df2, ['"""C"" LIKE \'V%\'"'], [BooleanType()], [Row(True)]
    )

    df1 = session.create_dataframe(["v"], schema=['"c c"'])
    df2 = df1.select(df1["c c"].like(lit("v%")))

    verify_column_result(
        session, df2, ['"""C C"" LIKE \'V%\'"'], [BooleanType()], [Row(True)]
    )


def test_regexp(session):
    df1 = session.create_dataframe(["v"], schema=["c"])
    df2 = df1.select(df1["c"].regexp(lit("v%")))

    verify_column_result(
        session, df2, ['"""C"" REGEXP \'V%\'"'], [BooleanType()], [Row(False)]
    )

    df1 = session.create_dataframe(["v"], schema=['"c c"'])
    df2 = df1.select(df1['"c c"'].regexp(lit("v%")))

    verify_column_result(
        session, df2, ['"""C C"" REGEXP \'V%\'"'], [BooleanType()], [Row(False)]
    )

    df1 = session.create_dataframe(["v"], schema=["c"])
    df2 = df1.select(df1["c"].regexp(lit("v%"), "c"))

    verify_column_result(
        session, df2, ['"RLIKE(""C"", \'V%\', \'C\')"'], [BooleanType()], [Row(False)]
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1346957: Collation not supported in Local Testing",
)
def test_collate(session):
    df1 = session.sql("select 'v' as c")
    df2 = df1.select(df1["c"].collate("en"))

    verify_column_result(
        session, df2, ['"""C"" COLLATE \'EN\'"'], [StringType(1)], [Row("v")]
    )

    df1 = session.sql("select 'v' as \"c c\"")
    df2 = df1.select(df1["c c"].collate("en"))

    verify_column_result(
        session, df2, ['"""C C"" COLLATE \'EN\'"'], [StringType(1)], [Row("v")]
    )


def test_subfield(session):
    df1 = session.create_dataframe(
        data=[[[1, 2, 3], {"a": "b"}]], schema=["c", '"c c"']
    )
    df2 = df1.select(df1["C"][0], df1["c c"]["a"])

    verify_column_result(
        session,
        df2,
        ['"""C""[0]"', '"""C C""[\'A\']"'],
        [VariantType(), VariantType()],
        None,
    )


def test_case_when(session):
    df1 = session.create_dataframe([[1, 2]], schema=["c", '"c c"'])
    df2 = df1.select(when(df1["c"] == 1, lit(True)).when(df1["c"] == 2, lit("abc")))
    expected_columns = [
        '"CASE  WHEN (""C"" = 1 :: INT) THEN TRUE :: BOOLEAN WHEN (""C"" = 2 :: INT) THEN \'ABC\' ELSE NULL END"'
    ]
    if session.eliminate_numeric_sql_value_cast_enabled:
        expected_columns = [
            '"CASE  WHEN (""C"" = 1) THEN TRUE :: BOOLEAN WHEN (""C"" = 2) THEN \'ABC\' ELSE NULL END"'
        ]
    verify_column_result(session, df2, expected_columns, [BooleanType()], [Row(True)])


def test_multiple_expression(session):
    df1 = session.create_dataframe([[1, "v"]], schema=["c", '"c c"'])
    df2 = df1.select(in_(["c", "c c"], [[lit(1), lit("v")]]))
    expected_columns = ['"(""C"", ""C C"") IN ((1 :: INT, \'V\'))"']
    if session.eliminate_numeric_sql_value_cast_enabled:
        expected_columns = ['"(""C"", ""C C"") IN ((1, \'V\'))"']
    verify_column_result(session, df2, expected_columns, [BooleanType()], [Row(True)])


def test_in_expression(session):
    df1 = session.create_dataframe([[1, "v"]], schema=["c", '"c c"'])
    df2 = df1.select(df1["c"].in_(1, 2, 3), df1["c c"].in_("v"))
    expected_columns = [
        '"""C"" IN (1 :: INT, 2 :: INT, 3 :: INT)"',
        '"""C C"" IN (\'V\')"',
    ]
    if session.eliminate_numeric_sql_value_cast_enabled:
        expected_columns = ['"""C"" IN (1, 2, 3)"', '"""C C"" IN (\'V\')"']
    verify_column_result(
        session,
        df2,
        expected_columns,
        [BooleanType(), BooleanType()],
        [Row(True, True)],
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


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-982770: Rank is not supported in Local Testing",
)
def test_specified_window_frame(session):
    df1 = session.sql("select 'v' as \" a\"")
    assert df1._output[0].name == '" a"'
    assert df1.columns[0] == '" a"'
    df2 = df1.select(rank().over(Window.order_by('" a"').rows_between(1, 2)) - 1)
    expected_columns = [
        '"(RANK() OVER (  ORDER BY \n    "" A"" ASC NULLS FIRST  ROWS BETWEEN 1 FOLLOWING  AND 2 FOLLOWING  ) - 1 :: INT)"'
    ]
    if session.eliminate_numeric_sql_value_cast_enabled:
        expected_columns = [
            '"(RANK() OVER (  ORDER BY \n    "" A"" ASC NULLS FIRST  ROWS BETWEEN 1 FOLLOWING  AND 2 FOLLOWING  ) - 1)"'
        ]
    verify_column_result(session, df2, expected_columns, [LongType()], [Row(0)])


def test_cast(session):

    df1 = session.create_dataframe([[1, "v"]], schema=["a", '" a"'])
    df2 = df1.select(
        df1["a"].cast("string(23)"),
        df1[" a"].try_cast("integer"),
        upper(df1[" a"]).cast("string"),
    )
    verify_column_result(
        session,
        df2,
        [
            '"CAST (""A"" AS STRING(23))"',
            '"TRY_CAST ("" A"" AS INT)"',
            '"CAST (UPPER("" A"") AS STRING)"',
        ],
        [StringType(), LongType(), StringType()],
        [Row("1", None, "V")],
    )


def test_unspecified_frame(session):
    df1 = session.create_dataframe([("v",)], schema=[" a"])
    verify_column_result(session, df1, ['" a"'], [StringType()], [Row("v")])
    df2 = df1.select(any_value(df1[" a"]).over())
    verify_column_result(
        session, df2, ['"ANY_VALUE("" A"") OVER (  )"'], [StringType()], [Row("v")]
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-982770: Rank is not supported in Local Testing",
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
    expected_columns = [
        '"(RANK() OVER (  ORDER BY \n    "" A"" ASC NULLS FIRST  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) - 1 :: INT)"'
    ]
    if session.eliminate_numeric_sql_value_cast_enabled:
        expected_columns = [
            '"(RANK() OVER (  ORDER BY \n    "" A"" ASC NULLS FIRST  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) - 1)"'
        ]

    verify_column_result(session, df2, expected_columns, [LongType()], [Row(0)])


def test_rank_related_function_expression(session, local_testing_mode):
    df1 = session.create_dataframe([[1, "v"]], schema=["a", '" a"'])
    window = Window.order_by(" a")
    df2 = df1.select(
        lag(df1[" a"]).over(window),
        lead(df1[" a"]).over(window),
        first_value(df1[" a"]).over(window),
        last_value(df1[" a"]).over(window),
    )

    verify_column_result(
        session,
        df2,
        [
            '"LAG("" A"", 1, NULL) OVER (  ORDER BY \n    "" A"" ASC NULLS FIRST )"',
            '"LEAD("" A"", 1, NULL) OVER (  ORDER BY \n    "" A"" ASC NULLS FIRST )"',
            '"FIRST_VALUE("" A"") OVER (  ORDER BY \n    "" A"" ASC NULLS FIRST )"',
            '"LAST_VALUE("" A"") OVER (  ORDER BY \n    "" A"" ASC NULLS FIRST )"',
        ],
        [StringType(), StringType(), StringType(), StringType()],
        [Row(None, None, "v", "v")],
    )

    df3 = df1.select(
        lag(df1["a"]).over(window),
        lead(df1["a"]).over(window),
        first_value(df1["a"]).over(window),
        last_value(df1["a"]).over(window),
    )

    verify_column_result(
        session,
        df3,
        [
            '"LAG(""A"", 1, NULL) OVER (  ORDER BY \n    "" A"" ASC NULLS FIRST )"',
            '"LEAD(""A"", 1, NULL) OVER (  ORDER BY \n    "" A"" ASC NULLS FIRST )"',
            '"FIRST_VALUE(""A"") OVER (  ORDER BY \n    "" A"" ASC NULLS FIRST )"',
            '"LAST_VALUE(""A"") OVER (  ORDER BY \n    "" A"" ASC NULLS FIRST )"',
        ],
        [
            # SNOW-1527199 lag and lead is not returning the correct type under local testing mode
            LongType() if not local_testing_mode else StringType(),
            LongType() if not local_testing_mode else StringType(),
            LongType(),
            LongType(),
        ],
        [Row(None, None, 1, 1)],
    )


def test_literal(session, local_testing_mode):
    df1 = session.create_dataframe([[]])
    df2 = df1.select(lit("a"), lit(1), lit(True), lit([1]))

    expected_dtypes = [
        StringType(),
        LongType(),
        BooleanType(),
        # snowflake doesn't enforce the inner type of ArrayType, so it is expected that
        # it returns StringType() as inner type.
        ArrayType(LongType()) if local_testing_mode else ArrayType(),
    ]
    verify_column_result(
        session,
        df2,
        [
            "\"'A'\"",
            '"1 :: INT"',
            '"TRUE :: BOOLEAN"',
            "\"PARSE_JSON('[1]') :: ARRAY\"",
        ],
        expected_dtypes,
        [Row("a", 1, True, "[\n  1\n]")],
    )


def test_interval(session):
    df1 = session.create_dataframe(
        [
            [datetime.datetime(2010, 1, 1), datetime.datetime(2011, 1, 1)],
            [datetime.datetime(2012, 1, 1), datetime.datetime(2013, 1, 1)],
        ],
        schema=["a", "b"],
    )
    df2 = df1.select(
        df1["a"]
        + make_interval(
            quarters=1,
            months=1,
            weeks=2,
            days=2,
            hours=2,
            minutes=3,
            seconds=3,
            milliseconds=3,
            microseconds=4,
            nanoseconds=4,
        )
    )
    verify_column_result(
        session,
        df2,
        [
            '"(""A"" + INTERVAL \'1 QUARTER,1 MONTH,2 WEEK,2 DAY,2 HOUR,3 MINUTE,3 SECOND,3 MILLISECOND,4 MICROSECOND,4 NANOSECOND\')"',
        ],
        [TimestampType(timezone=TimestampTimeZone.NTZ)],
        None,
    )


def test_attribute(session):
    df1 = session.create_dataframe([[1, 2]], schema=[" a", "a"])
    df2 = df1.select(df1[" a"], df1["a"])

    assert (
        [x.name for x in df2._output]
        == get_metadata_names(session, df2)
        == ['" a"', '"A"']
    )
    assert df2.columns == [
        '" a"',
        "A",
    ]  # In class ColumnIdentifier, the "" is removed for '"A"'.


def test_unresolved_attribute(session):
    df1 = session.create_dataframe([[1, 2]], schema=[" a", "a"])

    df2 = df1.select(" a", "a")

    assert (
        [x.name for x in df2._output]
        == get_metadata_names(session, df2)
        == ['" a"', '"A"']
    )
    assert df2.columns == [
        '" a"',
        "A",
    ]  # In class ColumnIdentifier, the "" is removed for '"A"'.


def test_star(session):
    df1 = session.create_dataframe([[1, 2]], schema=[" a", "a"])
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


def test_function_expression(session, local_testing_mode):
    df1 = session.create_dataframe(["a"], schema=["a"])
    if not local_testing_mode:
        # local testing does not support upper
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


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="Temp function not supported in stored proc environment"
)
@pytest.mark.udf
@pytest.mark.parametrize("use_qualified_name", [True, False])
def test_udf(session, use_qualified_name, local_testing_mode):
    def add_one(x: int) -> int:
        return x + 1

    special_chars = "quoted_name"
    temp_func_name = (
        f'"{Utils.random_name_for_temp_object(TempObjectType.FUNCTION)}{special_chars}"'
    )
    perm_func_name = (
        f'"{special_chars}{Utils.random_name_for_temp_object(TempObjectType.FUNCTION)}"'
    )
    database_name = session.get_current_database()
    schema_name = session.get_current_schema()
    full_temp_func_name = f"{database_name}.{schema_name}.{temp_func_name}"

    stage_name = Utils.random_stage_name()
    if not local_testing_mode:
        Utils.create_stage(session, stage_name, is_temporary=False)

    df = session.create_dataframe([1, 2], schema=["a"])

    try:
        if not local_testing_mode:
            # Local Testing does not support permanent registration of extension functions
            session.udf.register(
                add_one,
                name=perm_func_name,
                is_permanent=True,
                stage_location=stage_name,
            )

            if use_qualified_name:
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
                df_perm = df.select(call_udf(perm_func_name, col("a")))
                assert (
                    df_perm._output[0].name
                    == df_perm.columns[0]
                    == get_metadata_names(session, df_perm)[0]
                    == f'""{perm_func_name.upper()}"(""A"")"'
                )

        session.udf.register(add_one, name=temp_func_name, is_permanent=False)

        if use_qualified_name:
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
        else:
            df_temp = df.select(call_udf(temp_func_name, col("a")))
            assert (
                df_temp._output[0].name
                == df_temp.columns[0]
                == get_metadata_names(session, df_temp)[0]
                == f'""{temp_func_name.upper()}"(""A"")"'
            )
    finally:
        if not local_testing_mode:
            session._run_query(f"drop function if exists {temp_func_name}(int)")
            session._run_query(f"drop function if exists {perm_func_name}(int)")
            Utils.drop_stage(session, stage_name)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1359104: Column alias creates incorrect column name.",
)
def test_unary_expression(session):
    """Alias, UnresolvedAlias, Cast, UnaryMinus, IsNull, IsNotNull, IsNaN, Not"""
    df1 = session.create_dataframe([[1, 2]], schema=['" a"', "a"])
    df2 = df1.select(
        df1[" a"].cast("string"),
        df1[" a"].alias(" b"),
        -df1[" a"],
        df1[" a"].is_null(),
        df1[" a"].is_not_null(),
        df1[" a"].equal_nan(),
        ~(df1[" a"] == 1),
    )

    expected_columns = [
        '"CAST ("" A"" AS STRING)"',
        '" b"',
        '"- "" A"""',
        '""" A"" IS NULL"',
        '""" A"" IS NOT NULL"',
        '""" A"" = \'NAN\'"',
        '"NOT ("" A"" = 1)"'
        if session.eliminate_numeric_sql_value_cast_enabled
        else '"NOT ("" A"" = 1 :: INT)"',
    ]
    verify_column_result(
        session,
        df2,
        expected_columns,
        [
            StringType(),
            LongType(),
            LongType(),
            BooleanType(),
            BooleanType(),
            BooleanType(),
            BooleanType(),
        ],
        # Not able to check result, collect failed due to 'Numeric value 'NAN' is not recognized'
        None,
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
            '"NOT (""A"" = 1)"'
            if session.eliminate_numeric_sql_value_cast_enabled
            else '"NOT (""A"" = 1 :: INT)"',
        ]
    )
    assert df3.columns == [
        '"CAST (""A"" AS STRING(87))"',
        "B",
        '"- ""A"""',
        '"""A"" IS NULL"',
        '"""A"" IS NOT NULL"',
        '"""A"" = \'NAN\'"',
        '"NOT (""A"" = 1)"'
        if session.eliminate_numeric_sql_value_cast_enabled
        else '"NOT (""A"" = 1 :: INT)"',
    ]  # In class ColumnIdentifier, the "" is removed for '"B"'.


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Window function ListAgg is not supported",
)
def test_list_agg_within_group_sort_order(session):
    df1 = session.sql(
        'select c as "a b" from (select c from values((1), (2), (3)) as t(c))'
    )
    df2 = df1.select(
        listagg(df1["a b"], "a", is_distinct=True)
        .within_group(df1["a b"].asc())
        .over(Window.partition_by(df1["a b"]))
    )

    verify_column_result(
        session,
        df2,
        [
            '"LISTAGG ( DISTINCT ""A B"", \'A\') WITHIN GROUP ( ORDER BY \n    ""A B"" ASC NULLS FIRST) OVER (PARTITION BY ""A B""  )"'
        ],
        [StringType()],
        [Row("1")],
    )


def test_binary_expression(session, local_testing_mode):
    """=, !=, >, <, >=, <=, EQUAL_NULL, AND, OR, +, -, *, /, %, POWER, BITAND, BITOR, BITXOR"""
    df1 = session.create_dataframe(
        [[1, "x", 1, "x"]], schema=['" a"', '" b"', "a", "b"]
    )
    df2 = df1.select(
        df1[" b"] == "x",
        df1[" b"] != "x",
        df1[" b"] > "x",
        df1[" b"] <= "x",
        df1[" a"].equal_null(df1[" b"]),
        (df1[" b"] == "x") & (df1[" a"] == 1),
        (df1[" b"] == "x") | (df1[" a"] == 1),
        df1[" a"].bitand(lit(1)),
        df1[" a"].bitor(lit(1)),
        df1[" a"].bitxor(lit(1)),
        pow(df1[" a"], 2),
        df1[" a"] + df1[" a"],
        df1[" a"] - df1[" a"],
        df1[" a"] * df1[" a"],
        df1[" a"] / df1[" a"],
        df1[" a"] % df1[" a"],
    )

    verify_column_result(
        session,
        df2,
        [
            '"("" B"" = \'X\')"',
            '"("" B"" != \'X\')"',
            '"("" B"" > \'X\')"',
            '"("" B"" <= \'X\')"',
            '"EQUAL_NULL("" A"", "" B"")"',
            '"(("" B"" = \'X\') AND ("" A"" = 1))"'
            # when eliminate_numeric_sql_value_cast_enabled the binary expression
            # will not have cast expression for integer values like INT
            if session.eliminate_numeric_sql_value_cast_enabled
            else '"(("" B"" = \'X\') AND ("" A"" = 1 :: INT))"',
            '"(("" B"" = \'X\') OR ("" A"" = 1))"'
            if session.eliminate_numeric_sql_value_cast_enabled
            else '"(("" B"" = \'X\') OR ("" A"" = 1 :: INT))"',
            '"BITAND(1, "" A"")"'
            if session.eliminate_numeric_sql_value_cast_enabled
            else '"BITAND(1 :: INT, "" A"")"',
            '"BITOR(1, "" A"")"'
            if session.eliminate_numeric_sql_value_cast_enabled
            else '"BITOR(1 :: INT, "" A"")"',
            '"BITXOR(1, "" A"")"'
            if session.eliminate_numeric_sql_value_cast_enabled
            else '"BITXOR(1 :: INT, "" A"")"',
            '"POWER("" A"", 2)"'
            if session.eliminate_numeric_sql_value_cast_enabled
            else '"POWER("" A"", 2 :: INT)"',
            '"("" A"" + "" A"")"',
            '"("" A"" - "" A"")"',
            '"("" A"" * "" A"")"',
            '"("" A"" / "" A"")"',
            '"("" A"" % "" A"")"',
        ],
        [
            BooleanType(),
            BooleanType(),
            BooleanType(),
            BooleanType(),
            BooleanType(),
            BooleanType(),
            BooleanType(),
            # SNOW-1524633 local_testing_mode returns BooleanType() for bitwise operator
            #   instead of LongType()
            LongType() if not local_testing_mode else BooleanType(),
            LongType() if not local_testing_mode else BooleanType(),
            LongType() if not local_testing_mode else BooleanType(),
            DoubleType(),
            LongType(),
            LongType(),
            LongType(),
            DecimalType(38, 6),
            LongType(),
        ],
        # Not able to collect result, failed with error "Numeric value 'x' is not recognized"
        None,
    )
    df3 = df1.select(
        df1["b"] == "x",
        df1["b"] != "x",
        df1["b"] > "x",
        df1["b"] <= "x",
        df1["a"].equal_null(df1["b"]),
        (df1["b"] == "x") & (df1["a"] == 1),
        (df1["b"] == "x") | (df1["a"] == 1),
        df1["a"].bitand(lit(1)),
        df1["a"].bitor(lit(1)),
        df1["a"].bitxor(lit(1)),
        pow(df1["a"], 2),
        df1["a"] + df1["a"],
        df1["a"] - df1["a"],
        df1["a"] * df1["a"],
        df1["a"] / df1["a"],
        df1["a"] % df1["a"],
    )
    verify_column_result(
        session,
        df3,
        [
            '"(""B"" = \'X\')"',
            '"(""B"" != \'X\')"',
            '"(""B"" > \'X\')"',
            '"(""B"" <= \'X\')"',
            '"EQUAL_NULL(""A"", ""B"")"',
            '"((""B"" = \'X\') AND (""A"" = 1))"'
            if session.eliminate_numeric_sql_value_cast_enabled
            else '"((""B"" = \'X\') AND (""A"" = 1 :: INT))"',
            '"((""B"" = \'X\') OR (""A"" = 1))"'
            if session.eliminate_numeric_sql_value_cast_enabled
            else '"((""B"" = \'X\') OR (""A"" = 1 :: INT))"',
            '"BITAND(1, ""A"")"'
            if session.eliminate_numeric_sql_value_cast_enabled
            else '"BITAND(1 :: INT, ""A"")"',
            '"BITOR(1, ""A"")"'
            if session.eliminate_numeric_sql_value_cast_enabled
            else '"BITOR(1 :: INT, ""A"")"',
            '"BITXOR(1, ""A"")"'
            if session.eliminate_numeric_sql_value_cast_enabled
            else '"BITXOR(1 :: INT, ""A"")"',
            '"POWER(""A"", 2)"'
            if session.eliminate_numeric_sql_value_cast_enabled
            else '"POWER(""A"", 2 :: INT)"',
            '"(""A"" + ""A"")"',
            '"(""A"" - ""A"")"',
            '"(""A"" * ""A"")"',
            '"(""A"" / ""A"")"',
            '"(""A"" % ""A"")"',
        ],
        [
            BooleanType(),
            BooleanType(),
            BooleanType(),
            BooleanType(),
            BooleanType(),
            BooleanType(),
            BooleanType(),
            # SNOW-1524633 local testing mode returns BooleanType() for bitwise operator
            LongType() if not local_testing_mode else BooleanType(),
            LongType() if not local_testing_mode else BooleanType(),
            LongType() if not local_testing_mode else BooleanType(),
            DoubleType(),
            LongType(),
            LongType(),
            LongType(),
            DecimalType(38, 6),
            LongType(),
        ],
        # Not able to collect result, failed with error "Numeric value 'x' is not recognized"
        None,
    )


def test_cast_nan_column_name(session):
    df1 = session.create_dataframe([["a"]], schema=["a"])
    df2 = df1.select(df1["A"] == math.nan)

    verify_column_result(
        session,
        df2,
        ['"(""A"" = \'NAN\' :: FLOAT)"'],
        [BooleanType()],
        # Not able to collect, failed due to "Numeric value 'a' is not recognized"
        None,
    )


def test_inf_column_name(session, local_testing_mode):
    df1 = session.create_dataframe([["inf"]], schema=["'INF'"])
    df2 = df1.select(df1["'INF'"] == math.inf)

    # there is a behavior difference between local testing mode and snowflake.
    # The column 'INF' of df1 is actually mapped to string columns. In snowflake,
    # string column can be casted to numeric column when possible. However, in python,
    # string to numeric cast is not possible. Therefore, local testing mode will return
    # False for df2 result, but Snowflake returns True.
    # SNOW-1524637 fixing the INF comparison for local testing mode
    expected_rows = None if [Row(False)] else [Row(True)]
    verify_column_result(
        session,
        df2,
        ['"(""\'INF\'"" = \'INF\' :: FLOAT)"'],
        [BooleanType()],
        expected_rows,
    )


@pytest.mark.skip("grougping sets doesn't use local column name inference yet.")
def test_grouping_sets(session):
    ...


@pytest.mark.skip("table function doesn't use local inferred column names")
def test_table_function():
    ...


def test_str_column_name_no_quotes(session, local_testing_mode):
    decimal_string = "1.500000"
    df = session.create_dataframe([1, 2], schema=["a"])
    assert str(df.select(col("a")).collect()) == "[Row(A=1), Row(A=2)]"
    assert (
        str(df.select(avg(col("a"))).collect())
        == f"""[Row(AVG("A")=Decimal('{decimal_string}'))]"""
    )

    # column name with quotes
    df = session.create_dataframe([1, 2], schema=['"a"'])
    assert str(df.select(col('"a"')).collect()) == "[Row(a=1), Row(a=2)]"
    assert (
        str(df.select(avg(col('"a"'))).collect())
        == f"""[Row(AVG("A")=Decimal('{decimal_string}'))]"""
    )


def test_show_column_name_with_quotes(session, local_testing_mode):
    df = session.create_dataframe([1, 2], schema=["a"])
    assert (
        df.select(col("a"))._show_string(_emit_ast=session.ast_enabled)
        == """\
-------
|"A"  |
-------
|1    |
|2    |
-------
"""
    )
    assert (
        df.select(avg(col("a")))._show_string(_emit_ast=session.ast_enabled)
        == """\
----------------
|"AVG(""A"")"  |
----------------
|1.500000      |
----------------
"""
    )

    # column name with quotes
    df = session.create_dataframe([1, 2], schema=['"a"'])
    assert (
        df.select(col('"a"'))._show_string(_emit_ast=session.ast_enabled)
        == """\
-------
|"a"  |
-------
|1    |
|2    |
-------
"""
    )
    assert (
        df.select(avg(col('"a"')))._show_string(_emit_ast=session.ast_enabled)
        == """\
----------------
|"AVG(""A"")"  |
----------------
|1.500000      |
----------------
"""
    )
