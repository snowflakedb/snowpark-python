#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import itertools
import sys
from typing import Tuple

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark._internal.analyzer.select_statement import (
    SET_EXCEPT,
    SET_INTERSECT,
    SET_UNION,
    SET_UNION_ALL,
)
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import (
    avg,
    col,
    iff,
    lit,
    min as min_,
    object_construct_keep_null,
    row_number,
    seq1,
    sql_expr,
    sum as sum_,
    table_function,
    udtf,
)
from tests.utils import TestData, Utils

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

pytestmark = [
    pytest.mark.xfail(
        "config.getoption('local_testing_mode', default=False)",
        reason="This is a SQL test suite",
        run=False,
    )
]


@pytest.fixture(scope="module", autouse=True)
def skip(pytestconfig):
    if pytestconfig.getoption("disable_sql_simplifier"):
        pytest.skip(
            "Disable sql simplifier test when simplifier is disabled",
            allow_module_level=True,
        )


boolParamList = [True, False]


@pytest.fixture(params=boolParamList)
def setup_reduce_cast(request, session):
    is_eliminate_numeric_sql_value_cast_enabled = (
        session.eliminate_numeric_sql_value_cast_enabled
    )
    session.eliminate_numeric_sql_value_cast_enabled = request.param
    yield
    session.eliminate_numeric_sql_value_cast_enabled = (
        is_eliminate_numeric_sql_value_cast_enabled
    )


@pytest.fixture(scope="module")
def simplifier_table(session) -> None:
    table_name = Utils.random_table_name()
    Utils.create_table(session, table_name, "a int, b int")
    session._conn.run_query(f"insert into {table_name}(a, b) values (1, 2)")
    yield table_name
    Utils.drop_table(session, table_name)


@pytest.fixture(scope="module")
def large_simplifier_table(session) -> None:
    table_name = Utils.random_table_name()
    df = session.create_dataframe(
        [[1, 2, 3, 4], [5, 6, 7, 8]], schema=["a", "b", "c", "d"]
    )
    df.write.save_as_table(table_name, table_type="temp", mode="overwrite")
    yield table_name
    Utils.drop_table(session, table_name)


@pytest.fixture(scope="module")
def distinct_table(session):
    table_name = Utils.random_table_name()
    data = [
        [5, "a"],
        [3, "b"],
        [5, "a"],
        [1, "c"],
        [3, "c"],
    ]
    session.create_dataframe(data, schema=["a", "b"]).write.save_as_table(
        table_name, table_type="temp", mode="overwrite"
    )
    yield table_name
    Utils.drop_table(session, table_name)


@pytest.mark.parametrize(
    "set_operator", [SET_UNION, SET_UNION_ALL, SET_EXCEPT, SET_INTERSECT]
)
def test_set_same_operator(session, set_operator):
    df1 = session.sql("SELECT 1 as a, 2 as b")
    df2 = session.sql("SELECT 2 as a, 2 as b")
    df3 = session.sql("SELECT 3 as a, 2 as b")
    df4 = session.sql("SELECT 4 as a, 2 as b")
    if SET_UNION == set_operator:
        result1 = df1.union(df2).union(df3.union(df4))
        Utils.check_answer(
            result1, [Row(1, 2), Row(2, 2), Row(3, 2), Row(4, 2)], sort=True
        )
    elif SET_UNION_ALL == set_operator:
        result1 = df1.union_all(df2).union_all(df3.union_all(df4))
        Utils.check_answer(
            result1, [Row(1, 2), Row(2, 2), Row(3, 2), Row(4, 2)], sort=True
        )
    elif SET_EXCEPT == set_operator:
        result1 = df1.except_(df2).except_(df3.except_(df4))
        Utils.check_answer(result1, [Row(1, 2)], sort=False)
    else:
        result1 = df1.intersect(df2).intersect(df3.intersect(df4))
        Utils.check_answer(result1, [], sort=False)

    query1 = Utils.normalize_sql(result1._plan.queries[-1].sql)
    assert query1 == Utils.normalize_sql(
        f"( SELECT 1 as a, 2 as b ){set_operator}( SELECT 2 as a, 2 as b ){set_operator}( ( SELECT 3 as a, 2 as b ){set_operator}( SELECT 4 as a, 2 as b ) )"
    )


@pytest.mark.parametrize(
    "operator,action",
    [
        (SET_UNION, lambda df1, df2: df1.union(df2)),
        (SET_UNION_ALL, lambda df1, df2: df1.union_all(df2)),
        (SET_EXCEPT, lambda df1, df2: df1.except_(df2)),
        (SET_INTERSECT, lambda df1, df2: df1.intersect(df2)),
    ],
)
def test_distinct_set_operator(session, distinct_table, action, operator):
    try:
        original = session.conf.get("use_simplified_query_generation")
        session.conf.set("use_simplified_query_generation", True)
        df1 = session.table(distinct_table)
        df2 = session.table(distinct_table)

        df = action(df1, df2.distinct())
        assert Utils.normalize_sql(df.queries["queries"][0]) == Utils.normalize_sql(
            f"""( SELECT * FROM {distinct_table}){operator}( SELECT DISTINCT * FROM {distinct_table})"""
        )

        df = action(df1.distinct(), df2)
        assert Utils.normalize_sql(df.queries["queries"][0]) == Utils.normalize_sql(
            f"""( SELECT DISTINCT * FROM {distinct_table}){operator}( SELECT * FROM {distinct_table})"""
        )

        df = action(df1, df2).distinct()
        assert Utils.normalize_sql(df.queries["queries"][0]) == Utils.normalize_sql(
            f"""SELECT DISTINCT * FROM ( ( SELECT * FROM {distinct_table} ){operator}( SELECT * FROM {distinct_table} ) )"""
        )

        df = action(df1, df2.distinct()).distinct()
        assert Utils.normalize_sql(df.queries["queries"][0]) == Utils.normalize_sql(
            f"""SELECT DISTINCT * FROM ( ( SELECT * FROM {distinct_table}){operator}( SELECT DISTINCT * FROM {distinct_table}) )"""
        )

        df = action(df1.distinct(), df2).distinct()
        assert Utils.normalize_sql(df.queries["queries"][0]) == Utils.normalize_sql(
            f"""SELECT DISTINCT * FROM ( ( SELECT DISTINCT * FROM {distinct_table}){operator}( SELECT * FROM {distinct_table}) )"""
        )

        df = action(df1.distinct(), df2.distinct()).distinct()
        assert Utils.normalize_sql(df.queries["queries"][0]) == Utils.normalize_sql(
            f"""SELECT DISTINCT * FROM ( ( SELECT DISTINCT * FROM {distinct_table}){operator}( SELECT DISTINCT * FROM {distinct_table}) )"""
        )
    finally:
        session.conf.set("use_simplified_query_generation", original)


@pytest.mark.parametrize("set_operator", [SET_UNION_ALL, SET_EXCEPT, SET_INTERSECT])
def test_union_and_other_operators(session, set_operator):
    df1 = session.sql("SELECT 1 as a")
    df2 = session.sql("SELECT 2 as a")
    df3 = session.sql("SELECT 3 as a")

    if SET_UNION_ALL == set_operator:
        result1 = df1.union(df2).union_all(df3)
        result2 = df1.union(df2.union_all(df3))
        assert Utils.normalize_sql(
            result1._plan.queries[-1].sql
        ) == Utils.normalize_sql(
            f"( SELECT 1 as a ) UNION ( SELECT 2 as a ){set_operator}( ( SELECT 3 as a ) )"
        )
        assert Utils.normalize_sql(
            result2._plan.queries[-1].sql
        ) == Utils.normalize_sql(
            f"( SELECT 1 as a ) UNION ( ( SELECT 2 as a ){set_operator}( SELECT 3 as a ) )"
        )
    elif SET_EXCEPT == set_operator:
        result1 = df1.union(df2).except_(df3)
        result2 = df1.union(df2.except_(df3))
        assert Utils.normalize_sql(
            result1._plan.queries[-1].sql
        ) == Utils.normalize_sql(
            f"( SELECT 1 as a ) UNION ( SELECT 2 as a ){set_operator}( ( SELECT 3 as a ) )"
        )
        assert Utils.normalize_sql(
            result2._plan.queries[-1].sql
        ) == Utils.normalize_sql(
            f"( SELECT 1 as a ) UNION ( ( SELECT 2 as a ){set_operator}( SELECT 3 as a ) )"
        )
    else:  # intersect
        # intersect has higher precedence than union and other set operators
        result1 = df1.union(df2).intersect(df3)
        result2 = df1.union(df2.intersect(df3))
        assert Utils.normalize_sql(
            result1._plan.queries[-1].sql
        ) == Utils.normalize_sql(
            f"( ( SELECT 1 as a ) UNION ( SELECT 2 as a ) ){set_operator}( ( SELECT 3 as a ) )"
        )
        assert Utils.normalize_sql(
            result2._plan.queries[-1].sql
        ) == Utils.normalize_sql(
            f"( SELECT 1 as a ) UNION ( ( SELECT 2 as a ){set_operator}( SELECT 3 as a ) )"
        )


def test_union_by_name(session):
    df1 = session.create_dataframe([[1, 2, 11], [3, 4, 33]], schema=["a", "b", "c"])
    df2 = session.create_dataframe([[5, 6, 55], [3, 4, 33]], schema=["a", "b", "c"])

    # test flattening union_by_name works with basic example
    df = df1.union_by_name(df2)
    Utils.check_answer(df, [Row(1, 2, 11), Row(3, 4, 33), Row(5, 6, 55)])
    assert df.queries["queries"][-1].count("SELECT") == 4

    # test two layer select result is same as one layer select result
    df_l1 = df.select(df.a, df.b)
    df_l2 = df.select(df.a, df.b).select(df.a, df.b)
    Utils.check_answer(df_l1, [Row(1, 2), Row(3, 4), Row(5, 6)])
    assert df_l1.queries["queries"][-1].count("SELECT") == df_l2.queries["queries"][
        -1
    ].count("SELECT")

    # test we don't flatten in case of selecting dropped columns
    df3 = df.select((col("a") + 1).as_("d"))
    df4 = df3.select(df.b)
    assert df3.queries["queries"][-1].count("SELECT") + 1 == df4.queries["queries"][
        -1
    ].count("SELECT")

    # test we don't flatten when it is not possible to flatten (expression eval)
    df5 = df3.select((col("d") + 1).as_("a"))
    assert df3.queries["queries"][-1].count("SELECT") + 1 == df5.queries["queries"][
        -1
    ].count("SELECT")

    def get_max_nesting_depth(query):
        max_depth, curr_depth = 0, 0
        for char in query:
            if char == "(":
                curr_depth += 1
            elif char == ")":
                curr_depth -= 1
            max_depth = max(max_depth, curr_depth)
        return max_depth

    # multiple unions
    df6 = session.create_dataframe([[7, 8, 88], [8, 9, 99]], schema=["a", "b", "c"])
    df_n1 = df1.union_by_name(df2)
    df_n2 = df1.union_by_name(df2).union_by_name(df6)
    assert get_max_nesting_depth(df_n1.queries["queries"][-1]) == get_max_nesting_depth(
        df_n2.queries["queries"][-1]
    )


def test_union_with_cache_result(session):
    """Created to test regression in SNOW-876321"""
    df = session.sql("select 1 as A, 2 as B")
    df1 = df.select(lit("foo").alias("lit_col"))
    df2 = df.select(lit("eggs").alias("lit_col"))
    df3 = df1.union(df2)

    df4 = df3.cache_result()
    Utils.check_answer(df4, [Row(LIT_COL="foo"), Row(LIT_COL="eggs")])


def test_set_after_set(session):
    df = session.createDataFrame([(1, "one"), (2, "two"), (3, "one"), (4, "two")])
    df2 = session.createDataFrame([(3, "one"), (4, "two")])

    df_new = df.subtract(df2).with_column("NEW_COLUMN", lit(True))
    df_new_2 = df.subtract(df2).with_column("NEW_COLUMN", lit(True))
    df_union = df_new.union_all(df_new_2)
    Utils.check_answer(
        df_union,
        [
            Row(1, "one", True),
            Row(1, "one", True),
            Row(2, "two", True),
            Row(2, "two", True),
        ],
        sort=True,
    )
    assert df_union.columns == ["_1", "_2", "NEW_COLUMN"]


def test_select_new_columns(session, simplifier_table):
    """The query adds columns that reference columns unchanged in the subquery."""
    df = session.table(simplifier_table)

    # Add a new column c by using existing columns
    df1 = df.select("a", "b", (col("a") + col("b")).as_("c"))
    Utils.check_answer(df1, [Row(1, 2, 3)])
    assert df1.columns == ["A", "B", "C"]
    assert df1.queries["queries"][-1].count("SELECT") == 1

    df2 = df.select("a", "b", lit(3).as_("c"))
    Utils.check_answer(df2, [Row(1, 2, 3)])
    assert df2.columns == ["A", "B", "C"]
    assert df2.queries["queries"][-1].count("SELECT") == 1


def test_select_subquery_with_same_level_dependency(session, simplifier_table):
    df = session.table(simplifier_table)
    # select columns. subquery has same level column dependency. No flatten.
    df12 = df.select(df.a, df.b, (df.a + 10).as_("c"), (col("c") + 10).as_("d"))
    df13 = df12.select("a", "b", "d", "c")
    Utils.check_answer(df13, [Row(1, 2, 21, 11)])
    assert df13.queries["queries"][-1].count("SELECT") == 2

    # select columns. subquery has no same level column. Flatten.
    df14 = df.select(df.a, df.b, (df.a + 10).as_("c"), (col("b") + 10).as_("d"))
    df15 = df14.select("a", "b", "d", "c")
    Utils.check_answer(df15, [Row(1, 2, 12, 11)])
    assert df15.queries["queries"][-1].count("SELECT") == 1

    # select columns, change a column that reference to a same-level column. Flatten.
    df16 = df.select(df.a, (df.a + 10).as_("c"), (col("c") + 10).as_("b"))
    Utils.check_answer(df16, [Row(1, 11, 21)])
    assert df16.queries["queries"][-1].count("SELECT") == 1


def test_select_change_columns_reference_unchanged(session, simplifier_table):
    """The query changes columns that reference only unchanged columns in the parent."""
    df = session.table(simplifier_table)

    # depend on same column name. flatten
    df1 = df.select("a", (col("b") + 1).as_("b"))
    Utils.check_answer(df1, [Row(1, 3)])
    assert df1.columns == ["A", "B"]
    assert df1.queries["queries"][-1].count("SELECT") == 1

    # depend on same column names for both columns. flatten
    df2 = df.select((col("a") + 1).as_("a"), (col("b") + 1).as_("b"))
    Utils.check_answer(df2, [Row(2, 3)])
    assert df2.columns == ["A", "B"]
    assert df2.queries["queries"][-1].count("SELECT") == 1

    # depend on same column names for both columns.
    df3 = df.select("a", (col("b") + 1).as_("b")).select((col("a") + 1).as_("a"), "b")
    Utils.check_answer(df3, [Row(2, 3)])
    assert df3.columns == ["A", "B"]
    assert df3.queries["queries"][-1].count("SELECT") == 1


def test_select_change_columns_reference_a_changed_column(session, simplifier_table):
    df = session.table(simplifier_table)
    df1 = df.select((col("a") + 1).as_("a"), "b")

    # b depends on a, which is changed in the subquery, so no flatten
    df2 = df1.select("a", (col("a") + 1).as_("b"))
    Utils.check_answer(df2, [Row(2, 3)])
    assert df2.queries["queries"][-1].count("SELECT") == 2

    # b doesn't depend on a or any other changed column. flatten.
    df3 = df1.select("a", lit(1).as_("b"))
    Utils.check_answer(df3, [Row(2, 1)])
    assert df3.queries["queries"][-1].count("SELECT") == 1


def test_select_subquery_has_columns_changed(session, simplifier_table):
    """The query select columns that reference columns new or changed in the subquery."""
    df = session.table(simplifier_table)

    # Add a new column c by using existing columns
    df1 = df.select("a", "b", (col("a") + col("b")).as_("c"))

    # Add a new column d that doesn't use c after c was added previously. Flatten safely.
    df2 = df1.select("a", "b", "c", (col("a") + col("b") + 1).as_("d"))
    Utils.check_answer(df2, [Row(1, 2, 3, 4)])
    assert df2.columns == ["A", "B", "C", "D"]
    assert df2.queries["queries"][-1].count("SELECT") == 1

    # select all columns including the newly added c. Flatten.
    df3 = df.select("a", "b", lit(100).as_("c"))
    df4 = df3.select("a", "b", "c")
    Utils.check_answer(df4, [Row(1, 2, 100)])
    assert df4.queries["queries"][-1].count("SELECT") == 1

    # Add a new column d that uses c, which was new in the subquery.
    # d is placed before c. This shouldn't be flattened because the sql would be like
    # `select a, b, a + c as d, a + b as c from test_table`. column d references to c which is defined after d.
    df5 = df1.select("a", "b", (col("a") + col("c")).as_("d"), "c")
    Utils.check_answer(df5, [Row(1, 2, 4, 3)])
    assert df5.columns == ["A", "B", "D", "C"]
    assert df5.queries["queries"][-1].count("SELECT") == 2

    # Add a new column d that uses c, which was new in the subquery.
    # In theory it can be flattened because d uses c and is placed after c.
    # The sql would be like `select a, b, a + b as c, a + c as d from test_table`
    # But it's not flattened because we don't detect the same level column cross-reference.
    df5 = df1.select("a", "b", "c", (col("a") + col("c")).as_("d"))
    Utils.check_answer(df5, [Row(1, 2, 3, 4)])
    assert df5.columns == ["A", "B", "C", "D"]
    assert df5.queries["queries"][-1].count("SELECT") == 2

    # query and subquery change the same column, no flatten.
    df6 = df.select("a", (col("b") + 1).as_("b"))
    df7 = df6.select("a", (col("b") + 1).as_("b"))
    Utils.check_answer(df7, [Row(1, 4)])
    assert df7.queries["queries"][-1].count("SELECT") == 2

    # query changes a column, which was newly created in the subquery. No flatten.
    df8 = df.select("a", (col("b") + 1).as_("c"))
    df9 = df8.select("a", (col("c") + 1).as_("c"))
    Utils.check_answer(df9, [Row(1, 4)])
    assert df9.queries["queries"][-1].count("SELECT") == 2

    # query changes a column, which was newly created in the subquery. No flatten.
    df10 = df.select("a", (col("b") + 1).as_("c"))
    df11 = df10.select("a", (col("c") + 1).as_("d"))
    Utils.check_answer(df11, [Row(1, 4)])
    assert df11.queries["queries"][-1].count("SELECT") == 2


def test_select_expr(session, simplifier_table):
    df = session.table(simplifier_table)

    df1 = df.select_expr("a", "b")
    Utils.check_answer(df1, [Row(1, 2)])
    assert df1.queries["queries"][-1].count("SELECT") == 2

    df2 = df.select_expr("a + 1 as a", "b + 1 as b")
    Utils.check_answer(df2, [Row(2, 3)])
    assert df2.queries["queries"][-1].count("SELECT") == 2

    # query again after sql_expr. No flatten.
    df3 = df2.select("a", "b")
    Utils.check_answer(df3, [Row(2, 3)])
    assert df3.queries["queries"][-1].count("SELECT") == 3

    """ query has no new columns. subquery has new, changed or dropped columns."""
    # a new column in the subquery. sql text column doesn't know the dependency, to be safe, no flatten
    df4 = df.select("a", "b", (col("a") + col("b")).as_("c"))
    df5 = df4.select_expr("a + 1 as a", "b + 1 as b", "c + 1 as c")
    Utils.check_answer(df5, [Row(2, 3, 4)])
    assert df5.queries["queries"][-1].count("SELECT") == 2

    # a changed column in the subquery. sql text column doesn't know the dependency, to be safe, no flatten
    df6 = df.select("a", lit(10).as_("b"))
    df7 = df6.select_expr("a + 1 as a", "b + 1 as b")
    Utils.check_answer(df7, [Row(2, 11)])
    assert df7.queries["queries"][-1].count("SELECT") == 2

    # a dropped column in the subquery. sql text column doesn't know whether it references the dropped column, to be safe, no flatten
    df8 = df.select("a")
    df9 = df8.select_expr("a + 1 as a")
    Utils.check_answer(df9, [Row(2)])
    assert df9.queries["queries"][-1].count("SELECT") == 2

    df10 = df.select("a", "b")
    df11 = df10.select_expr("a + 1 as a")
    Utils.check_answer(df11, [Row(2)])
    assert df11.queries["queries"][-1].count("SELECT") == 2

    """ query has new columns. subquery has new, changed or dropped columns."""
    # a new column in the subquery. sql text column doesn't know the dependency, to be safe, no flatten
    df4 = df.select("a", "b", (col("a") + col("b")).as_("c"))
    df5 = df4.select_expr("a + b as d")
    Utils.check_answer(df5, [Row(3)])
    assert df5.queries["queries"][-1].count("SELECT") == 2

    # a changed column in the subquery. sql text column doesn't know the dependency, to be safe, no flatten
    df6 = df.select("a", lit(10).as_("b"))
    df7 = df6.select_expr("a + b as d")
    Utils.check_answer(df7, [Row(11)])
    assert df7.queries["queries"][-1].count("SELECT") == 2

    # a dropped column in the subquery. sql text column doesn't know whether it references the dropped column, to be safe, no flatten
    df8 = df.select("a")
    df9 = df8.select_expr("a + b as d")
    with pytest.raises(SnowparkSQLException, match="invalid identifier"):
        df9.collect()

    df10 = df.select("a", "b")
    df11 = df10.select_expr("a + 1 as d")
    Utils.check_answer(df11, [Row(2)])
    assert df11.queries["queries"][-1].count("SELECT") == 2


@pytest.mark.udf
def test_select_with_table_function_join(session):
    # setup
    df = session.create_dataframe(
        [[1, 2, "one o two"], [2, 3, "two o three"]], schema=["a", "b", "c"]
    )
    split_to_table = table_function("split_to_table")

    @udtf(output_schema=["two_x", "three_x"])
    class multiplier_udtf:
        def process(self, n: int) -> Iterable[Tuple[int, int]]:
            yield (2 * n, 3 * n)

    df1 = df.select(col("a"), split_to_table("c", lit(" ")))
    df2 = df.select(col("a"), multiplier_udtf(df.b))
    # test multiple selects are flattened
    expected = [Row(1), Row(1), Row(1), Row(2), Row(2), Row(2)]
    df3 = df1.select("a", "seq").select("a").select("a")
    Utils.check_answer(expected, df3)
    assert df1.queries["queries"][-1].count("SELECT") == df3.queries["queries"][
        -1
    ].count("SELECT")

    expected = [Row(1), Row(2)]
    df4 = df2.select("a", "two_x").select("a").select("a")
    Utils.check_answer(expected, df4)
    assert df2.queries["queries"][-1].count("SELECT") == df4.queries["queries"][
        -1
    ].count("SELECT")

    # test aliasing does not add extra layers
    df5 = df.select("a", split_to_table("c", lit(" ")).as_("seq", "idx", "val"))
    assert df1.queries["queries"][-1].count("SELECT") == df5.queries["queries"][
        -1
    ].count("SELECT")

    df6 = df.select("a", multiplier_udtf("b").as_("x2", "x3"))
    assert df2.queries["queries"][-1].count("SELECT") == df6.queries["queries"][
        -1
    ].count("SELECT")

    # test dropped columns are not flattened
    df7 = df1.select("a", "b")
    assert df7.queries["queries"][-1].count("SELECT") == df1.queries["queries"][
        -1
    ].count("SELECT")

    df8 = df2.select("a", "c")
    assert df8.queries["queries"][-1].count("SELECT") == df2.queries["queries"][
        -1
    ].count("SELECT")

    # test expressions are not flattened
    expected = [Row(3), Row(3), Row(3), Row(4), Row(4), Row(4)]
    df9 = df1.select((col("a") + 1).as_("a")).select((col("a") + 1).as_("a"))
    Utils.check_answer(expected, df9)
    assert (
        df9.queries["queries"][-1].count("SELECT")
        == df1.queries["queries"][-1].count("SELECT") + 1
    )

    expected = [Row(3), Row(4)]
    df10 = df2.select((col("a") + 1).as_("a")).select((col("a") + 1).as_("a"))
    Utils.check_answer(expected, df10)
    assert (
        df10.queries["queries"][-1].count("SELECT")
        == df2.queries["queries"][-1].count("SELECT") + 1
    )


@pytest.mark.udf
def test_join_table_function(session):
    # setup
    df = session.create_dataframe(
        [[1, 2, "one o two"], [2, 3, "two o three"]], schema=["a", "b", "c"]
    )
    split_to_table = table_function("split_to_table")

    @udtf(output_schema=["two_x", "three_x"])
    class multiplier_udtf:
        def process(self, n: int) -> Iterable[Tuple[int, int]]:
            yield (2 * n, 3 * n)

    df1 = df.join_table_function(split_to_table("c", lit(" ")))
    df2 = df.join_table_function(multiplier_udtf("b"))

    # test column flattens
    expected = [Row(1), Row(1), Row(1), Row(2), Row(2), Row(2)]
    df3 = df1.select("a", "b", "seq")
    df4 = df3.select("a", "seq").select("a").select("a")
    Utils.check_answer(expected, df4)
    assert df3.queries["queries"][-1].count("SELECT") == df4.queries["queries"][
        -1
    ].count("SELECT")

    expected = [Row(1), Row(2)]
    df5 = df2.select("a", "b", "two_x")
    df6 = df5.select("a", "two_x").select("a").select("a")
    Utils.check_answer(expected, df6)
    assert df5.queries["queries"][-1].count("SELECT") == df6.queries["queries"][
        -1
    ].count("SELECT")

    # test column renames flatten for built-in fns
    expected = [Row(2), Row(2), Row(2), Row(3), Row(3), Row(3)]
    df7 = df1.select("a", "seq")
    df8 = df7.select((col("a") + 1).as_("a"), "seq").select("a").select("a")
    Utils.check_answer(expected, df8)
    assert df7.queries["queries"][-1].count("SELECT") == df8.queries["queries"][
        -1
    ].count("SELECT")

    # test column rename flatten for user defined fns
    expected = [Row(2), Row(3)]
    df9 = df2.select("a", "two_x")
    df10 = df9.select((col("a") + 1).as_("a"), "two_x").select("a").select("a")
    Utils.check_answer(expected, df10)
    assert df9.queries["queries"][-1].count("SELECT") == df10.queries["queries"][
        -1
    ].count("SELECT")

    # test flattening works for aliases fns also
    df11 = df.join_table_function(
        split_to_table("c", lit(" ")).as_("seq", "val", "idx")
    )
    df12 = df.join_table_function(multiplier_udtf("b").as_("x2", "x3"))

    expected = [Row(1), Row(1), Row(1), Row(2), Row(2), Row(2)]
    df13 = df11.select("a", "b", "idx")
    df14 = df13.select("a", "idx").select("a").select("a")
    Utils.check_answer(expected, df14)
    assert df13.queries["queries"][-1].count("SELECT") == df14.queries["queries"][
        -1
    ].count("SELECT")

    expected = [Row(1), Row(2)]
    df15 = df12.select("a", "b", "x2")
    df16 = df15.select("a", "x2").select("a").select("a")
    Utils.check_answer(expected, df16)
    assert df15.queries["queries"][-1].count("SELECT") == df16.queries["queries"][
        -1
    ].count("SELECT")


def test_with_column(session, simplifier_table):
    df = session.table(simplifier_table)
    new_df = df
    for i in range(10):
        new_df = new_df.with_column(f"c{i}", lit(i))
    assert new_df._plan.queries[-1].sql.count("SELECT") == 1

    new_df = df
    for i in range(10):
        new_df = new_df.with_column(f"c{i}", col("a"))
    assert new_df._plan.queries[-1].sql.count("SELECT") == 1

    new_df = df.with_column("x", df["a"]).with_column("y", df["b"])
    assert new_df._plan.queries[-1].sql.count("SELECT") == 1


def test_table_function(session):
    split_to_table = table_function("split_to_table")
    df = session.table_function(split_to_table(lit("one two three four"), lit(" ")))

    # flatten when possible
    df1 = (
        df.select("seq", "index").select("index").select((col("index") - 1).as_("IDX"))
    )
    Utils.check_answer(df1, [Row(0), Row(1), Row(2), Row(3)])
    assert df1.queries["queries"][-1].count("SELECT") == 2

    # cases when flatten is not possible
    df2 = df.select((col("seq") + 1).as_("a"), (col("index") - 1).as_("b")).select(
        col("a") + 1, col("b") + 7
    )
    Utils.check_answer(df2, [Row(3, 7), Row(3, 8), Row(3, 9), Row(3, 10)])
    assert df2.queries["queries"][-1].count("SELECT") == 3


def test_drop_columns(session, simplifier_table):
    df = session.table(simplifier_table)

    # drop a column not referenced by other same-level columns
    df1 = df.select("a", "b", (col("a") + 1).as_("c"))
    df2 = df1.select("a", "b")
    Utils.check_answer(df2, [Row(1, 2)])
    assert df2._plan.queries[-1].sql.count("SELECT") == 1

    # drop a column referenced by other same-level columns
    df1 = df.select("a", "b", (col("a") + 1).as_("c"), (col("c") + 1).as_("d"))
    df2 = df1.select("a", "b", "d")
    Utils.check_answer(df2, [Row(1, 2, 3)])
    assert df2._plan.queries[-1].sql.count("SELECT") == 2
    df2 = df1.select("a", "b")
    Utils.check_answer(df2, [Row(1, 2)])
    assert df2._plan.queries[-1].sql.count("SELECT") == 2

    # drop the column d, which isn't referenced by other columns. Flatten
    df3 = df1.select("a", "b", "c")
    Utils.check_answer(df3, [Row(1, 2, 2)])
    assert df3._plan.queries[-1].sql.count("SELECT") == 1

    # subquery has sql text so unable to figure out same-level dependency, so assuming d depends on c. No flatten.
    df4 = df.select("a", "b", lit(3).as_("c"), sql_expr("1 + 1 as d"))
    df5 = df4.select("a", "b", "d")
    Utils.check_answer(df5, [Row(1, 2, 2)])
    assert df5._plan.queries[-1].sql.count("SELECT") == 3

    df4 = df.select("a", "b", lit(3).as_("c"), sql_expr("1 + 1 as d"))
    df5 = df4.select("a", "b", "c")
    Utils.check_answer(df5, [Row(1, 2, 3)])
    assert df5._plan.queries[-1].sql.count("SELECT") == 3


def test_reference_non_exist_columns(session, simplifier_table):
    df = session.table(simplifier_table)
    with pytest.raises(
        SnowparkSQLException,
        match="invalid identifier 'C'",
    ):
        df.select(col("c") + 1).collect()


def test_order_by(setup_reduce_cast, session, simplifier_table):
    df = session.table(simplifier_table)

    # flatten
    df1 = df.sort("a", col("b") + 1)
    integer_literal_postfix = (
        "" if session.eliminate_numeric_sql_value_cast_enabled else " :: INT"
    )
    assert Utils.normalize_sql(df1.queries["queries"][-1]) == Utils.normalize_sql(
        f'SELECT * FROM {simplifier_table} ORDER BY "A" ASC NULLS FIRST, ("B" + 1{integer_literal_postfix}) ASC NULLS FIRST'
    )

    # flatten
    df2 = df.select("a", "b").sort("a", "b")
    assert Utils.normalize_sql(df2.queries["queries"][-1]) == Utils.normalize_sql(
        f'SELECT "A", "B" FROM {simplifier_table} ORDER BY "A" ASC NULLS FIRST, "B" ASC NULLS FIRST'
    )

    # no flatten because c is a new column
    df3 = df.select("a", "b", (col("a") - col("b")).as_("c")).sort("a", "b", "c")
    assert Utils.normalize_sql(df3.queries["queries"][-1]) == Utils.normalize_sql(
        f'SELECT * FROM ( SELECT "A", "B", ("A" - "B") AS "C" FROM {simplifier_table} ) ORDER BY "A" ASC NULLS FIRST, "B" ASC NULLS FIRST, "C" ASC NULLS FIRST'
    )

    # no flatten because a and be are changed
    df4 = df.select((col("a") + 1).as_("a"), ((col("b") + 1).as_("b"))).sort("a", "b")
    assert Utils.normalize_sql(df4.queries["queries"][-1]) == Utils.normalize_sql(
        f'SELECT * FROM ( SELECT ("A" + 1{integer_literal_postfix}) AS "A", ("B" + 1{integer_literal_postfix}) AS "B" FROM {simplifier_table} ) ORDER BY "A" ASC NULLS FIRST, "B" ASC NULLS FIRST'
    )

    # subquery has sql text so unable to figure out same-level dependency, so assuming d depends on c. No flatten.
    df5 = df.select("a", "b", lit(3).as_("c"), sql_expr("1 + 1 as d")).sort("a", "b")
    assert Utils.normalize_sql(df5.queries["queries"][-1]) == Utils.normalize_sql(
        f'SELECT * FROM ( SELECT "A", "B", 3 :: INT AS "C", 1 + 1 as d FROM ( SELECT * FROM {simplifier_table} ) ) ORDER BY "A" ASC NULLS FIRST, "B" ASC NULLS FIRST'
    )


def test_filter(setup_reduce_cast, session, simplifier_table):
    df = session.table(simplifier_table)
    integer_literal_postfix = (
        "" if session.eliminate_numeric_sql_value_cast_enabled else " :: INT"
    )

    # flatten
    df1 = df.filter((col("a") > 1) & (col("b") > 2))
    assert Utils.normalize_sql(df1.queries["queries"][-1]) == Utils.normalize_sql(
        f'SELECT * FROM {simplifier_table} WHERE (("A" > 1{integer_literal_postfix}) AND ("B" > 2{integer_literal_postfix}))'
    )

    # flatten
    df2 = df.select("a", "b").filter((col("a") > 1) & (col("b") > 2))
    assert Utils.normalize_sql(df2.queries["queries"][-1]) == Utils.normalize_sql(
        f'SELECT "A", "B" FROM {simplifier_table} WHERE (("A" > 1{integer_literal_postfix}) AND ("B" > 2{integer_literal_postfix}))'
    )

    # no flatten because c is a new column
    df3 = df.select("a", "b", (col("a") - col("b")).as_("c")).filter(
        (col("a") > 1) & (col("b") > 2) & (col("c") < 1)
    )
    assert Utils.normalize_sql(df3.queries["queries"][-1]) == Utils.normalize_sql(
        f'SELECT * FROM ( SELECT "A", "B", ("A" - "B") AS "C" FROM {simplifier_table} ) WHERE ((("A" > 1{integer_literal_postfix}) AND ("B" > 2{integer_literal_postfix})) AND ("C" < 1{integer_literal_postfix}))'
    )

    # no flatten because a and be are changed
    df4 = df.select((col("a") + 1).as_("a"), (col("b") + 1).as_("b")).filter(
        (col("a") > 1) & (col("b") > 2)
    )
    assert Utils.normalize_sql(df4.queries["queries"][-1]) == Utils.normalize_sql(
        f'SELECT * FROM ( SELECT ("A" + 1{integer_literal_postfix}) AS "A", ("B" + 1{integer_literal_postfix}) AS "B" FROM {simplifier_table} ) WHERE (("A" > 1{integer_literal_postfix}) AND ("B" > 2{integer_literal_postfix}))'
    )

    df5 = df4.select("a")
    assert Utils.normalize_sql(df5.queries["queries"][-1]) == Utils.normalize_sql(
        f'SELECT "A" FROM ( SELECT ("A" + 1{integer_literal_postfix}) AS "A", ("B" + 1{integer_literal_postfix}) AS "B" FROM {simplifier_table} ) WHERE (("A" > 1{integer_literal_postfix}) AND ("B" > 2{integer_literal_postfix}))'
    )

    # subquery has sql text so unable to figure out same-level dependency, so assuming d depends on c. No flatten.
    df6 = df.select("a", "b", lit(3).as_("c"), sql_expr("1 + 1 as d")).filter(
        col("a") > 1
    )
    assert Utils.normalize_sql(df6.queries["queries"][-1]) == Utils.normalize_sql(
        f'SELECT * FROM ( SELECT "A", "B", 3 :: INT AS "C", 1 + 1 as d FROM ( SELECT * FROM {simplifier_table} ) ) WHERE ("A" > 1{integer_literal_postfix})'
    )


def test_limit(setup_reduce_cast, session, simplifier_table):
    df = session.table(simplifier_table)
    df = df.limit(10)
    assert Utils.normalize_sql(
        df.queries["queries"][-1]
    ).lower() == Utils.normalize_sql(
        f"select * from {simplifier_table.lower()} limit 10"
    )

    df = session.sql(f"select * from {simplifier_table}")
    df = df.limit(10)
    # we don't know if the original sql already has top/limit clause using a subquery is necessary.
    #  or else there will be SQL compile error.
    assert Utils.normalize_sql(
        df.queries["queries"][-1]
    ).lower() == Utils.normalize_sql(
        f"select * from ( select * from {simplifier_table.lower()} ) limit 10"
    )

    df = session.sql(f"select * from {simplifier_table}")
    df = df.limit(0)
    assert Utils.normalize_sql(
        df.queries["queries"][-1]
    ).lower() == Utils.normalize_sql(
        f"select * from ( select * from {simplifier_table.lower()} ) limit 0"
    )

    # test for non-select sql statement
    temp_table_name = Utils.random_table_name()
    query = f"create or replace temporary table {temp_table_name} (bar string)"
    df = session.sql(query).limit(1)
    assert df.queries["queries"][-2] == query
    assert df.collect()[0][0] == f"Table {temp_table_name} successfully created."


def test_filter_order_limit_together(setup_reduce_cast, session, simplifier_table):
    df = session.table(simplifier_table)
    df1 = df.select("a", "b").filter(col("b") > 1).sort("a").limit(5)
    integer_literal_postfix = (
        "" if session.eliminate_numeric_sql_value_cast_enabled else " :: INT"
    )

    assert Utils.normalize_sql(df1.queries["queries"][-1]) == Utils.normalize_sql(
        f'SELECT "A", "B" FROM {simplifier_table} WHERE ("B" > 1{integer_literal_postfix}) ORDER BY "A" ASC NULLS FIRST LIMIT 5'
    )

    df2 = df1.select("a")
    assert Utils.normalize_sql(df2.queries["queries"][-1]) == Utils.normalize_sql(
        f'SELECT "A" FROM {simplifier_table} WHERE ("B" > 1{integer_literal_postfix}) ORDER BY "A" ASC NULLS FIRST LIMIT 5'
    )


def test_order_limit_filter(setup_reduce_cast, session, simplifier_table):
    df = session.table(simplifier_table)
    df1 = df.select("a", "b").sort("a").limit(1).filter(col("b") > 1)
    integer_literal_postfix = (
        "" if session.eliminate_numeric_sql_value_cast_enabled else " :: INT"
    )

    assert Utils.normalize_sql(df1.queries["queries"][-1]) == Utils.normalize_sql(
        f'SELECT * FROM ( SELECT "A", "B" FROM {simplifier_table} ORDER BY "A" ASC NULLS FIRST LIMIT 1 ) WHERE ("B" > 1{integer_literal_postfix})'
    )

    df2 = df1.select("a")
    assert Utils.normalize_sql(df2.queries["queries"][-1]) == Utils.normalize_sql(
        f'SELECT "A" FROM ( SELECT "A", "B" FROM {simplifier_table} ORDER BY "A" ASC NULLS FIRST LIMIT 1 ) WHERE ("B" > 1{integer_literal_postfix})'
    )


def test_limit_window(session, simplifier_table):
    df = session.table(simplifier_table)
    df1 = df.select("a", "b").limit(1).select("a", "b", row_number().over())
    assert Utils.normalize_sql(df1.queries["queries"][-1]) == Utils.normalize_sql(
        f'SELECT "A", "B", row_number() OVER ( ) FROM ( SELECT "A", "B" FROM {simplifier_table} LIMIT 1 )'
    )

    df2 = df1.select("a")
    assert Utils.normalize_sql(df2.queries["queries"][-1]) == Utils.normalize_sql(
        f'SELECT "A" FROM ( SELECT "A", "B" FROM {simplifier_table} LIMIT 1 )'
    )


def test_limit_offset(session, simplifier_table):
    df = session.table(simplifier_table)
    df = df.limit(10, offset=1)
    assert Utils.normalize_sql(df.queries["queries"][-1]) == Utils.normalize_sql(
        f"SELECT * FROM {simplifier_table} LIMIT 10 OFFSET 1"
    )

    df2 = df.limit(6)
    assert Utils.normalize_sql(df2.queries["queries"][-1]) == Utils.normalize_sql(
        f"SELECT * FROM {simplifier_table} LIMIT 6 OFFSET 1"
    )

    df3 = df.limit(5, offset=2)
    assert Utils.normalize_sql(df3.queries["queries"][-1]) == Utils.normalize_sql(
        f"SELECT * FROM ( SELECT * FROM {simplifier_table} LIMIT 10 OFFSET 1 ) LIMIT 5 OFFSET 2"
    )

    df4 = session.sql(f"select * from {simplifier_table}")
    df4 = df4.limit(10)
    # we don't know if the original sql already has top/limit clause using a subquery is necessary.
    #  or else there will be SQL compile error.
    assert Utils.normalize_sql(df4.queries["queries"][-1]) == Utils.normalize_sql(
        f"SELECT * FROM ( select * from {simplifier_table} ) LIMIT 10"
    )

    df5 = df4.limit(5, offset=20)
    assert Utils.normalize_sql(df5.queries["queries"][-1]) == Utils.normalize_sql(
        f"SELECT * FROM ( SELECT * FROM ( select * from {simplifier_table} ) LIMIT 10 ) LIMIT 5 OFFSET 20"
    )


def test_agg(session, simplifier_table):
    df = session.table(simplifier_table)
    df1 = df.agg([avg("a")]).select("AVG(A)").select("AVG(A)").select("AVG(A)")
    Utils.check_answer(df1, [Row(1)])
    assert df1.queries["queries"][0].count("SELECT") == 3
    df1 = (
        df.select("a")
        .select("a")
        .select("a")
        .agg([avg("a")])
        .select("AVG(A)")
        .select("AVG(A)")
        .select("AVG(A)")
    )
    Utils.check_answer(df1, [Row(1)])
    assert df1.queries["queries"][0].count("SELECT") == 3
    df1 = df.group_by("a", "b").agg([avg("a")]).select("a").select("a").select("a")
    Utils.check_answer(df1, [Row(1)])
    assert df1.queries["queries"][0].count("SELECT") == 3


def test_pivot(session):
    df = (
        TestData.monthly_sales(session)
        .pivot("month", ["JAN", "FEB", "MAR", "APR"])
        .agg(sum_(col("amount")))
        .select("EMPID")
        .select("EMPID")
        .select("EMPID")
    )
    assert df.queries["queries"][0].count("SELECT") == 4
    df = (
        TestData.monthly_sales(session)
        .select("EMPID", "month", "amount")
        .select("EMPID", "month", "amount")
        .pivot("month", ["JAN", "FEB", "MAR", "APR"])
        .agg(sum_(col("amount")))
        .select("EMPID")
        .select("EMPID")
        .select("EMPID")
    )
    assert df.queries["queries"][0].count("SELECT") == 4


def test_group_by_pivot(session):
    df = (
        TestData.monthly_sales_with_team(session)
        .group_by("empid")
        .pivot("month", ["JAN", "FEB", "MAR", "APR"])
        .agg(sum_(col("amount")))
        .select("EMPID")
        .select("EMPID")
        .select("EMPID")
    )
    assert df.queries["queries"][0].count("SELECT") == 5
    df = (
        TestData.monthly_sales_with_team(session)
        .select("EMPID", "team", "month", "amount")
        .select("EMPID", "team", "month", "amount")
        .group_by("empid")
        .pivot("month", ["JAN", "FEB", "MAR", "APR"])
        .agg(sum_(col("amount")))
        .select("EMPID")
        .select("EMPID")
        .select("EMPID")
    )
    assert df.queries["queries"][0].count("SELECT") == 5


@pytest.mark.parametrize("func_name", ["cube", "rollup"])
def test_cube_rollup(session, func_name):
    df = session.create_dataframe(
        [
            ("country A", "state A", 50),
            ("country A", "state A", 50),
            ("country A", "state B", 5),
            ("country A", "state B", 5),
            ("country B", "state A", 100),
            ("country B", "state A", 100),
            ("country B", "state B", 10),
            ("country B", "state B", 10),
        ]
    ).to_df(["country", "state", "value"])
    func = getattr(df, func_name)
    df1 = (
        func("country")
        .agg(sum_(col("value")))
        .select("country")
        .select("country")
        .select("country")
    )
    assert df1.queries["queries"][0].count("SELECT") == 4
    func = getattr(
        df.select("country", "state", "value").select("country", "state", "value"),
        func_name,
    )
    df1 = (
        func("country")
        .agg(sum_(col("value")))
        .select("country")
        .select("country")
        .select("country")
    )
    assert df1.queries["queries"][0].count("SELECT") == 4


def test_use_sql_simplifier(session, simplifier_table):
    sql_simplifier_enabled_original = session.sql_simplifier_enabled
    try:
        session.sql_simplifier_enabled = False
        df1 = (
            session.sql(f"SELECT * from {simplifier_table}")
            .select("*")
            .select("a")
            .select("a")
            .filter(col("a") == 1)
            .sort("a")
        )
        session.sql_simplifier_enabled = True
        df2 = (
            session.sql(f"SELECT * from {simplifier_table}")
            .select("*")
            .select("a")
            .select("a")
            .filter(col("a") == 1)
            .sort("a")
        )
        assert df1.queries["queries"][0].count("SELECT") == 6
        assert df2.queries["queries"][0].count("SELECT") == 2
        Utils.check_answer(df1, df2, sort=True)

        session.sql_simplifier_enabled = False
        df3 = (
            session.table(simplifier_table)
            .select("*")
            .select("a")
            .select("a")
            .filter(col("a") == 1)
            .sort("a")
        )

        session.sql_simplifier_enabled = True
        df4 = (
            session.table(simplifier_table)
            .select("*")
            .select("a")
            .select("a")
            .filter(col("a") == 1)
            .sort("a")
        )
        assert df3.queries["queries"][0].count("SELECT") == 6
        assert df4.queries["queries"][0].count("SELECT") == 1
        Utils.check_answer(df3, df4, sort=True)
    finally:
        session.sql_simplifier_enabled = sql_simplifier_enabled_original


def test_join_dataframes(session, simplifier_table):
    df_left = session.create_dataframe([[1, 2]]).to_df("a", "b")
    df_right = session.create_dataframe([[3, 4]]).to_df("c", "d")

    df = df_left.join(df_right)
    df1 = df.select("a").select("a").select("a")
    assert df1.queries["queries"][0].count("SELECT") == 8

    df2 = (
        df.select((col("a") + 1).as_("a"))
        .select((col("a") + 1).as_("a"))
        .select((col("a") + 1).as_("a"))
    )
    assert df2.queries["queries"][0].count("SELECT") == 10

    df3 = df.with_column("x", df_left.a).with_column("y", df_right.d)
    assert '"A" AS "X", "D" AS "Y"' in Utils.normalize_sql(df3.queries["queries"][0])
    Utils.check_answer(df3, [Row(1, 2, 3, 4, 1, 4)])

    # the following can't be flattened
    df4 = df_right.to_df("e", "f")
    df5 = df_left.join(df4)
    df6 = df5.with_column("x", df_right.c).with_column("y", df4.f)
    assert df6.queries["queries"][0].count("SELECT") == 10
    Utils.check_answer(df6, [Row(1, 2, 3, 4, 3, 4)])


def test_sample(session, simplifier_table):
    df = session.table(simplifier_table)
    df_table_row_sample = session.table(simplifier_table).sample(n=3)
    assert Utils.normalize_sql(
        df_table_row_sample.queries["queries"][-1]
    ) == Utils.normalize_sql(f"SELECT * FROM {simplifier_table} SAMPLE (3 ROWS)")

    df_table_sample = df.sample(
        0.5, sampling_method="BERNOULLI", seed=1
    )  # SQL is generated from Table's sample method.
    df1 = df_table_sample.select("a").select("a").select("a")
    assert df1.queries["queries"][-1].count("SELECT") == 2
    df2 = (
        df_table_sample.select((col("a") + 1).as_("a"))
        .select((col("a") + 1).as_("a"))
        .select((col("a") + 1).as_("a"))
    )
    assert df2.queries["queries"][-1].count("SELECT") == 4

    df_query_sample = df.sample(
        0.5
    )  # SQL is generated from DataFrame's sample method..
    df3 = df_query_sample.select("a").select("a").select("a")
    assert df3.queries["queries"][-1].count("SELECT") == 2

    df4 = (
        df_query_sample.select((col("a") + 1).as_("a"))
        .select((col("a") + 1).as_("a"))
        .select((col("a") + 1).as_("a"))
    )
    assert df4.queries["queries"][-1].count("SELECT") == 4


def test_unpivot(session, simplifier_table):
    column_list = ["jan", "feb", "mar", "apr"]
    df = TestData.monthly_sales_flat(session).unpivot("sales", "month", column_list)
    df1 = df.select("sales").select("sales").select("sales")
    assert df1.queries["queries"][-1].count("SELECT") == 4

    df2 = (
        df.select((col("sales") + 1).as_("sales"))
        .select((col("sales") + 1).as_("sales"))
        .select((col("sales") + 1).as_("sales"))
    )
    assert df2.queries["queries"][-1].count("SELECT") == 6


def test_select_star(session, simplifier_table):
    df = session.table(simplifier_table)
    df1 = df.select("*")
    assert Utils.normalize_sql(df1.queries["queries"][0]) == Utils.normalize_sql(
        f"SELECT * FROM {simplifier_table}"
    )

    df2 = df.select(df["*"])
    assert Utils.normalize_sql(df2.queries["queries"][0]) == Utils.normalize_sql(
        f'SELECT "A", "B" FROM {simplifier_table}'
    )

    df3 = df.select("*", "a")
    assert Utils.normalize_sql(df3.queries["queries"][0]) == Utils.normalize_sql(
        f'SELECT *, "A" FROM ( SELECT * FROM {simplifier_table} )'
    )

    df4 = df.select(df["*"], "a")
    assert Utils.normalize_sql(df4.queries["queries"][0]) == Utils.normalize_sql(
        f'SELECT "A","B", "A" FROM ( SELECT * FROM {simplifier_table} )'
    )

    df5 = df3.select("b")
    assert Utils.normalize_sql(df5.queries["queries"][0]) == Utils.normalize_sql(
        f'SELECT "B" FROM ( SELECT *, "A" FROM ( SELECT * FROM {simplifier_table} ) )'
    )

    df6 = df4.select("b")
    assert Utils.normalize_sql(df6.queries["queries"][0]) == Utils.normalize_sql(
        f'SELECT "B" FROM ( SELECT "A","B", "A" FROM ( SELECT * FROM {simplifier_table} ) )'
    )

    with pytest.raises(SnowparkSQLException, match="ambiguous column name 'A'"):
        df3.select("a").collect()

    with pytest.raises(SnowparkSQLException, match="ambiguous column name 'A'"):
        df4.select("a").collect()


def test_session_range(session, simplifier_table):
    df = session.range(0, 5, 1)
    df1 = df.select("id").select("id").select("id")
    assert df1.queries["queries"][0].count("SELECT") == 2

    df2 = (
        df.select((col("id") + 1).as_("id"))
        .select((col("id") + 1).as_("id"))
        .select((col("id") + 1).as_("id"))
    )
    assert df2.queries["queries"][0].count("SELECT") == 4


def test_natural_join(session, simplifier_table):
    df1 = session.table(simplifier_table)
    df2 = session.table(simplifier_table)
    df = df1.natural_join(df2)
    df = df.select("a").select("a").select("a")
    df.collect()
    assert Utils.normalize_sql(df.queries["queries"][0]).count('SELECT "A"') == 1


def test_rename_to_dropped_column_name(session):
    session.sql_simplifier_enabled = True
    df1 = session.create_dataframe([[1, 2, 3]], schema=["a", "b", "c"])
    df2 = df1.drop("a").drop("b")
    df3 = df2.withColumn("a", df2["c"])
    df4 = df3.withColumn("b", sql_expr("1"))
    assert df4.columns == ["C", "A", "B"]
    Utils.check_answer(df4, [Row(3, 3, 1)])


def test_rename_to_existing_column_column(session):
    session.sql_simplifier_enabled = True
    df1 = session.create_dataframe([[1, 2, 3]], schema=["a", "b", "c"])
    # df2 = df1.drop("a").drop("b")
    df3 = df1.withColumn("a", df1["c"])
    df4 = df3.withColumn("b", sql_expr("1"))
    assert df4.columns == ["C", "A", "B"]
    Utils.check_answer(df4, [Row(3, 3, 1)])


@pytest.mark.parametrize("df_from", ["values", "table", "sql"])
def test_drop_using_exclude(session, large_simplifier_table, df_from):
    if df_from == "values":
        df = session.create_dataframe([[1, 2, 3, 4]], schema=["a", "b", "c", "d"])
        drop_a_query = 'SELECT * EXCLUDE ("A") FROM ( SELECT "A", "B", "C", "D" FROM ( SELECT $1 AS "A", $2 AS "B", $3 AS "C", $4 AS "D" FROM VALUES (1 :: INT, 2 :: INT, 3 :: INT, 4 :: INT) ) )'
        drop_ab_query = 'SELECT * EXCLUDE ("A", "B") FROM ( SELECT "A", "B", "C", "D" FROM ( SELECT $1 AS "A", $2 AS "B", $3 AS "C", $4 AS "D" FROM VALUES (1 :: INT, 2 :: INT, 3 :: INT, 4 :: INT) ) )'
    elif df_from == "table":
        df = session.table(large_simplifier_table)
        drop_a_query = f'SELECT * EXCLUDE ("A") FROM {large_simplifier_table}'
        drop_ab_query = f'SELECT * EXCLUDE ("A", "B") FROM {large_simplifier_table}'
    elif df_from == "sql":
        df = session.sql(f"select * from {large_simplifier_table}")
        drop_a_query = (
            f'SELECT * EXCLUDE ("A") FROM ( select * from {large_simplifier_table} )'
        )
        drop_ab_query = f'SELECT * EXCLUDE ("A", "B") FROM ( select * from {large_simplifier_table} )'
    else:
        raise ValueError("Invalid df_from value")
    drop_a_query = Utils.normalize_sql(drop_a_query)
    drop_ab_query = Utils.normalize_sql(drop_ab_query)

    original = session.conf.get("use_simplified_query_generation")
    try:
        session.conf.set("use_simplified_query_generation", True)

        # test we generate correct select * exclude query
        df1 = df.drop("a")
        assert Utils.normalize_sql(df1.queries["queries"][0]) == Utils.normalize_sql(
            drop_a_query
        ), df1.queries["queries"][0]

        # repeated drop should not generate new query
        df2 = df.drop("a").drop("a")
        assert Utils.normalize_sql(df2.queries["queries"][0]) == Utils.normalize_sql(
            drop_a_query
        ), df2.queries["queries"][0]

        # dropping non-existing column should not generate new query
        df3 = df.drop("a").drop("f").drop("g")
        assert Utils.normalize_sql(df3.queries["queries"][0]) == Utils.normalize_sql(
            drop_a_query
        ), df3.queries["queries"][0]

        # dropping multiple columns are flattened
        df4 = df.drop("a", "b")
        assert Utils.normalize_sql(df4.queries["queries"][0]) == Utils.normalize_sql(
            drop_ab_query
        ), df4.queries["queries"][0]
        df5 = df.drop("a").drop("b")
        assert Utils.normalize_sql(df5.queries["queries"][0]) == Utils.normalize_sql(
            drop_ab_query
        ), df5.queries["queries"][0]
    finally:
        session.conf.set("use_simplified_query_generation", original)


def test_flattening_for_exclude(session, large_simplifier_table):
    original = session.conf.get("use_simplified_query_generation")
    try:
        session.conf.set("use_simplified_query_generation", True)
        df = session.table(large_simplifier_table)

        # select
        df1 = df.select("a", "b", "c").drop("a")
        assert Utils.normalize_sql(df1.queries["queries"][0]) == Utils.normalize_sql(
            f'SELECT * EXCLUDE ("A") FROM ( SELECT "A", "B", "C" FROM {large_simplifier_table} )'
        )
        df2 = df.drop("a").select("b", "c").drop("b")
        assert Utils.normalize_sql(df2.queries["queries"][0]) == Utils.normalize_sql(
            f'SELECT * EXCLUDE ("B") FROM ( SELECT "B", "C" FROM ( SELECT * EXCLUDE ("A") FROM {large_simplifier_table} ) )'
        )

        # filter
        df3 = df.filter(col("a") > 1).drop("a")
        assert Utils.normalize_sql(df3.queries["queries"][0]) == Utils.normalize_sql(
            f'SELECT * EXCLUDE ("A") FROM {large_simplifier_table} WHERE ("A" > 1)'
        )
        df4 = df.drop("a").filter(col("a") > 1).drop("b")
        assert Utils.normalize_sql(df4.queries["queries"][0]) == Utils.normalize_sql(
            f'SELECT * EXCLUDE ("A", "B") FROM {large_simplifier_table} WHERE ("A" > 1)'
        )

        # sort
        df5 = df.sort("a").drop("a")
        assert Utils.normalize_sql(df5.queries["queries"][0]) == Utils.normalize_sql(
            f'SELECT * EXCLUDE ("A") FROM {large_simplifier_table} ORDER BY "A" ASC NULLS FIRST'
        )
        df6 = df.drop("b").sort("a").drop("a")
        assert Utils.normalize_sql(df6.queries["queries"][0]) == Utils.normalize_sql(
            f'SELECT * EXCLUDE ("A", "B") FROM {large_simplifier_table} ORDER BY "A" ASC NULLS FIRST'
        )

        # limit
        df7 = df.limit(10).drop("a")
        assert Utils.normalize_sql(df7.queries["queries"][0]) == Utils.normalize_sql(
            f'SELECT * EXCLUDE ("A") FROM {large_simplifier_table} LIMIT 10'
        )
        df8 = df.drop("a").limit(10).drop("b")
        assert Utils.normalize_sql(df8.queries["queries"][0]) == Utils.normalize_sql(
            f'SELECT * EXCLUDE ("A", "B") FROM {large_simplifier_table} LIMIT 10'
        )

        # distinct
        df9 = df.distinct().drop("a")
        assert Utils.normalize_sql(df9.queries["queries"][0]) == Utils.normalize_sql(
            f'SELECT DISTINCT * EXCLUDE ("A") FROM {large_simplifier_table}'
        )
        df10 = df.drop("a").distinct().drop("b")
        assert Utils.normalize_sql(df10.queries["queries"][0]) == Utils.normalize_sql(
            f'SELECT DISTINCT * EXCLUDE ("A", "B") FROM {large_simplifier_table}'
        )

    finally:
        session.conf.set("use_simplified_query_generation", original)


@pytest.mark.parametrize("select_cols", [["d"], ["a", "b", "c"]])
def test_chained_operation(session, large_simplifier_table, select_cols):
    actions = [
        lambda df: df.select(select_cols),
        lambda df: df.filter(col("b") > 1).sort("c"),
        lambda df: df.drop("a"),
        lambda df: df.distinct(),
    ]
    original = session.conf.get("use_simplified_query_generation")

    def get_df_or_error(action_permutation):
        try:
            df = session.table(large_simplifier_table)
            for action in action_permutation:
                df = action(df)
            assert len(df.queries["queries"]) == 1
            df.collect()  # trigger query generation and catch errors
            return df, None
        except Exception as e:
            return df, e

    try:
        for action_permutation in itertools.permutations(actions):
            session.conf.set("use_simplified_query_generation", True)
            df_enabled, error_enabled = get_df_or_error(action_permutation)

            session.conf.set("use_simplified_query_generation", False)
            df_disabled, error_disabled = get_df_or_error(action_permutation)

            if error_enabled or error_disabled:
                if error_disabled is None:
                    raise AssertionError(
                        "Raised error for simplified query generation that is not raised for non-simplified query generation",
                        error_enabled,
                        df_enabled._plan.api_calls,
                    )
                if error_enabled is None:
                    raise AssertionError(
                        "Raised error for non-simplified query generation that is not raised for simplified query generation",
                        error_disabled,
                        df_disabled._plan.api_calls,
                    )
                assert type(error_enabled) == type(error_disabled)

                if hasattr(error_enabled, "error_code"):
                    assert error_enabled.error_code == error_disabled.error_code
            else:
                Utils.check_answer(df_enabled, df_disabled)
                Utils.check_answer(df_enabled.limit(2), df_disabled.limit(2))

    finally:
        session.conf.set("use_simplified_query_generation", original)


def test_chained_sort(session):
    session.sql_simplifier_enabled = False
    df1 = session.create_dataframe([[1, 2], [4, 3]], schema=["a", "b"])

    session.sql_simplifier_enabled = True
    df2 = session.create_dataframe([[1, 2], [4, 3]], schema=["a", "b"])

    Utils.check_answer(df1.sort("a").sort("b"), df2.sort("a").sort("b"), sort=False)
    assert (
        df2.sort("a").sort("b").queries["queries"][0]
        == df2.sort("b", "a").queries["queries"][0]
    )


@pytest.mark.parametrize(
    "operation,simplified_query",
    [
        # Flattened
        (
            lambda df: df.filter(col("A") > 1).select(col("B") + 1),
            'SELECT ("B" + 1{POSTFIX}) FROM ( SELECT $1 AS "A", $2 AS "B" FROM VALUES (1 :: INT, -2 :: INT), (3 :: INT, -4 :: INT) ) WHERE ("A" > 1{POSTFIX})',
        ),
        # Flattened, if there are duplicate column names across the parent/child, WHERE is evaluated on subquery first, so we could flatten in this case
        (
            lambda df: df.filter(col("A") > 1).select((col("B") + 1).alias("A")),
            'SELECT ("B" + 1{POSTFIX}) AS "A" FROM ( SELECT $1 AS "A", $2 AS "B" FROM VALUES (1 :: INT, -2 :: INT), (3 :: INT, -4 :: INT) ) WHERE ("A" > 1{POSTFIX})',
        ),
        # Flattened
        (
            lambda df: df.filter(col("A") > 1)
            .select(col("A"), col("B"), lit(12).alias("TWELVE"))
            .filter(col("A") > 2),
            'SELECT "A", "B", 12 :: INT AS "TWELVE" FROM ( SELECT $1 AS "A", $2 AS "B" FROM VALUES (1 :: INT, -2 :: INT), (3 :: INT, -4 :: INT) ) WHERE (("A" > 1{POSTFIX}) AND ("A" > 2{POSTFIX}))',
        ),
        # Not fully flattened, since col("A") > 1 and col("A") > 2 are referring to different columns
        (
            lambda df: df.filter(col("A") > 1)
            .select((col("B") + 1).alias("A"))
            .filter(col("A") > 2),
            'SELECT * FROM ( SELECT ("B" + 1{POSTFIX}) AS "A" FROM ( SELECT $1 AS "A", $2 AS "B" FROM VALUES (1 :: INT, -2 :: INT), (3 :: INT, -4 :: INT) ) WHERE ("A" > 1{POSTFIX}) ) WHERE ("A" > 2{POSTFIX})',
        ),
        # Not flattened, since A is updated in the select after filter.
        (
            lambda df: df.filter(col("A") > 1).select("A", seq1(0)),
            'SELECT "A", seq1(0) FROM ( SELECT "A", "B" FROM ( SELECT $1 AS "A", $2 AS "B" FROM VALUES (1 :: INT, -2 :: INT), (3 :: INT, -4 :: INT) ) WHERE ("A" > 1{POSTFIX}) )',
        ),
        # Not flattened, since we cannot detect dependent columns from sql_expr
        (
            lambda df: df.filter(sql_expr("A > 1")).select(col("B"), col("A")),
            'SELECT "B", "A" FROM ( SELECT "A", "B" FROM ( SELECT $1 AS "A", $2 AS "B" FROM VALUES (1 :: INT, -2 :: INT), (3 :: INT, -4 :: INT) ) WHERE A > 1 )',
        ),
        # Not flattened, since we cannot flatten when the subquery uses positional parameter ($1)
        (
            lambda df: df.filter(col("$1") > 1).select(col("B"), col("A")),
            'SELECT "B", "A" FROM ( SELECT * FROM ( SELECT "A", "B" FROM ( SELECT $1 AS "A", $2 AS "B" FROM VALUES (1 :: INT, -2 :: INT), (3 :: INT, -4 :: INT) ) ) WHERE ("$1" > 1{POSTFIX}) )',
        ),
    ],
)
def test_select_after_filter(setup_reduce_cast, session, operation, simplified_query):
    session.sql_simplifier_enabled = False
    df1 = session.create_dataframe([[1, -2], [3, -4]], schema=["a", "b"])

    session.sql_simplifier_enabled = True
    df2 = session.create_dataframe([[1, -2], [3, -4]], schema=["a", "b"])

    # replace 'POSTFIX' with the expected integer literal postfix to get the final expected simplified query
    integer_literal_postfix = (
        "" if session.eliminate_numeric_sql_value_cast_enabled else " :: INT"
    )
    simplified_query = simplified_query.format_map({"POSTFIX": integer_literal_postfix})
    simplified_query = Utils.normalize_sql(simplified_query)

    Utils.check_answer(operation(df1), operation(df2))
    assert Utils.normalize_sql(
        operation(df2).queries["queries"][0]
    ) == Utils.normalize_sql(simplified_query)


@pytest.mark.parametrize(
    "operation,simplified_query,execute_sql",
    [
        # Flattened
        (
            lambda df: df.order_by(col("A")).select(col("B") + 1),
            'SELECT ("B" + 1{POSTFIX}) FROM ( SELECT $1 AS "A", $2 AS "B" FROM VALUES (1 :: INT, -2 :: INT), (3 :: INT, -4 :: INT) ) ORDER BY "A" ASC NULLS FIRST',
            True,
        ),
        # Not flattened because SEQ1() is a data generator.
        (
            lambda df: df.order_by(col("A")).select(seq1(0)),
            'SELECT seq1(0) FROM ( SELECT "A", "B" FROM ( SELECT $1 AS "A", $2 AS "B" FROM VALUES (1 :: INT, -2 :: INT), (3 :: INT, -4 :: INT) ) ORDER BY "A" ASC NULLS FIRST )',
            True,
        ),
        # Not flattened, unlike filter, current query takes precendence when there are duplicate column names from a ORDERBY clause
        (
            lambda df: df.order_by(col("A")).select((col("B") + 1).alias("A")),
            'SELECT ("B" + 1{POSTFIX}) AS "A" FROM ( SELECT "A", "B" FROM ( SELECT $1 AS "A", $2 AS "B" FROM VALUES (1 :: INT, -2 :: INT), (3 :: INT, -4 :: INT) ) ORDER BY "A" ASC NULLS FIRST )',
            True,
        ),
        # Not flattened, since we cannot detect dependent columns from sql_expr
        (
            lambda df: df.order_by(sql_expr("A")).select(col("B")),
            'SELECT "B" FROM ( SELECT "A", "B" FROM ( SELECT $1 AS "A", $2 AS "B" FROM VALUES (1 :: INT, -2 :: INT), (3 :: INT, -4 :: INT) ) ORDER BY A ASC NULLS FIRST )',
            True,
        ),
        # Not flattened, since we cannot flatten when the subquery uses positional parameter ($1)
        (
            lambda df: df.order_by(col("$1")).select(col("B")),
            'SELECT "B" FROM ( SELECT * FROM ( SELECT "A", "B" FROM ( SELECT $1 AS "A", $2 AS "B" FROM VALUES (1 :: INT, -2 :: INT), (3 :: INT, -4 :: INT) ) ) ORDER BY "$1" ASC NULLS FIRST )',
            True,
        ),
        # Not flattened, skip execution since this would result in SnowparkSQLException
        (
            lambda df: df.order_by(col("C")).select((col("A") + col("B")).alias("C")),
            'SELECT ("A" + "B") AS "C" FROM ( SELECT "A", "B" FROM ( SELECT $1 AS "A", $2 AS "B" FROM VALUES (1 :: INT, -2 :: INT), (3 :: INT, -4 :: INT) ) ORDER BY "C" ASC NULLS FIRST )',
            False,
        ),
        # Flattened
        (
            lambda df: df.order_by(col("A"))
            .select(col("B"), col("A"))
            .order_by(col("B"))
            .select(col("A")),
            'SELECT "A" FROM ( SELECT $1 AS "A", $2 AS "B" FROM VALUES (1 :: INT, -2 :: INT), (3 :: INT, -4 :: INT) ) ORDER BY "B" ASC NULLS FIRST, "A" ASC NULLS FIRST',
            True,
        ),
    ],
)
def test_select_after_orderby(
    setup_reduce_cast, session, operation, simplified_query, execute_sql
):
    session.sql_simplifier_enabled = False
    df1 = session.create_dataframe([[1, -2], [3, -4]], schema=["a", "b"])

    session.sql_simplifier_enabled = True
    df2 = session.create_dataframe([[1, -2], [3, -4]], schema=["a", "b"])

    integer_literal_postfix = (
        "" if session.eliminate_numeric_sql_value_cast_enabled else " :: INT"
    )
    simplified_query = simplified_query.format_map({"POSTFIX": integer_literal_postfix})
    simplified_query = Utils.normalize_sql(simplified_query)

    assert Utils.normalize_sql(
        operation(df2).queries["queries"][0]
    ) == Utils.normalize_sql(simplified_query)
    if execute_sql:
        Utils.check_answer(operation(df1), operation(df2))


def test_window_with_filter(session):
    df = session.create_dataframe([[0], [1]], schema=["A"])
    df = (
        df.with_column("B", iff(df.A == 0, 10, 11))
        .with_column("C", min_("B").over())
        .filter(df.A == 1)
    )
    Utils.check_answer(df, [Row(1, 11, 10)], sort=False)


def test_data_generator_with_filter(session):
    df = session.create_dataframe([[0], [1]], schema=["a"])
    df = (
        df.with_column("B", seq1()).with_column("C", min_("B").over()).filter(df.A == 1)
    )
    Utils.check_answer(df, [Row(1, 1, 0)])


def test_star_column(session):
    # convert to a table
    df = session.create_dataframe(
        [[0, "a"], [1, "b"]], schema=["a", "b"]
    ).cache_result()
    # select a column and rename it twice
    df1 = df.select(col("a").as_("x"), "b").select(col("x").as_("y"), "b")
    df2 = df1.select(object_construct_keep_null("*"))
    # expect that no subquery is flattened
    query = df2.queries["queries"][0]
    assert query.count("SELECT") == 3
    Utils.check_answer(
        df2, [Row('{\n  "B": "a",\n  "Y": 0\n}'), Row('{\n  "B": "b",\n  "Y": 1\n}')]
    )


def test_select_limit_orderby(session):
    # convert to a table
    df = session.create_dataframe([[5, "a"], [3, "b"]], schema=["a", "b"])
    # call sort after limit
    df1 = df.select("a", "b").limit(2).sort(col("a"))
    Utils.check_answer(df1, [Row(3, "b"), Row(5, "a")])
    # sql simplification is not applied, and order by clause is attached at end
    expected_query = """SELECT * FROM ( SELECT "A", "B" FROM ( SELECT $1 AS "A", $2 AS "B" FROM VALUES (5 :: INT, 'a' :: STRING), (3 :: INT, 'b' :: STRING) ) LIMIT 2 ) ORDER BY "A" ASC NULLS FIRST"""
    assert Utils.normalize_sql(df1.queries["queries"][0]) == Utils.normalize_sql(
        expected_query
    )

    df2 = df.select("a", "b").sort(col("a")).limit(2)
    Utils.check_answer(df2, [Row(3, "b"), Row(5, "a")])
    # sql simplification is applied, order by is in front of the limit
    expected_query = """SELECT "A", "B" FROM ( SELECT $1 AS "A", $2 AS "B" FROM VALUES (5 :: INT, 'a' :: STRING), (3 :: INT, 'b' :: STRING) ) ORDER BY "A" ASC NULLS FIRST LIMIT 2"""
    assert Utils.normalize_sql(df2.queries["queries"][0]) == Utils.normalize_sql(
        expected_query
    )

    df3 = df.select("a", "b").limit(2, offset=1).sort(col("a"))
    expected_query = """SELECT * FROM ( SELECT "A", "B" FROM ( SELECT $1 AS "A", $2 AS "B" FROM VALUES (5 :: INT, 'a' :: STRING), (3 :: INT, 'b' :: STRING) ) LIMIT 2 OFFSET 1 ) ORDER BY "A" ASC NULLS FIRST"""
    assert Utils.normalize_sql(df3.queries["queries"][0]) == Utils.normalize_sql(
        expected_query
    )


@pytest.mark.parametrize(
    "operation,expected_query,expected_result,sort_results",
    [
        # df.select().distinct() != df.distinct().select()
        (
            lambda df: df.select("a", "b").distinct().select("a"),
            lambda table: f"""SELECT "A" FROM ( SELECT DISTINCT "A", "B" FROM {table} )""",
            [Row(5), Row(3), Row(1), Row(3)],
            True,
        ),
        (
            lambda df: df.select("a", "b").select("a").distinct(),
            lambda table: f"""SELECT DISTINCT "A" FROM {table}""",
            [Row(5), Row(3), Row(1)],
            True,
        ),
        # df.select().distinct().limit() != df.distinct().select().limit() for optimization
        (
            lambda df: df.select("a", "b").distinct().limit(4),
            lambda table: f"""SELECT DISTINCT "A", "B" FROM {table} LIMIT 4""",
            [Row(5, "a"), Row(3, "b"), Row(1, "c"), Row(3, "c")],
            True,
        ),
        (
            lambda df: df.select("a", "b").limit(4).distinct(),
            lambda table: f"""SELECT DISTINCT * FROM ( SELECT "A", "B" FROM {table} LIMIT 4 )""",
            None,
            True,
        ),
        # df.distinct().filter() = df.filter().distinct()
        (
            lambda df: df.select("a", "b").distinct().filter(col("a") > 1),
            lambda table: f"""SELECT DISTINCT "A", "B" FROM {table} WHERE ("A" > 1)""",
            [Row(5, "a"), Row(3, "b"), Row(3, "c")],
            True,
        ),
        (
            lambda df: df.select("a", "b").filter(col("a") > 1).distinct(),
            lambda table: f"""SELECT DISTINCT "A", "B" FROM {table} WHERE ("A" > 1)""",
            [Row(5, "a"), Row(3, "b"), Row(3, "c")],
            True,
        ),
        # df.distinct().sort() = df.sort().distinct()
        (
            lambda df: df.select("a", "b").distinct().sort(col("a"), col("b")),
            lambda table: f"""SELECT DISTINCT "A", "B" FROM {table} ORDER BY "A" ASC NULLS FIRST, "B" ASC NULLS FIRST""",
            [Row(1, "c"), Row(3, "b"), Row(3, "c"), Row(5, "a")],
            False,
        ),
        (
            lambda df: df.sort(col("a"), col("b")).distinct(),
            lambda table: f"""SELECT DISTINCT * FROM {table} ORDER BY "A" ASC NULLS FIRST, "B" ASC NULLS FIRST""",
            [Row(1, "c"), Row(3, "b"), Row(3, "c"), Row(5, "a")],
            True,
        ),
        (
            lambda df: df.select("a", "b").sort(col("a"), col("b")).distinct(),
            lambda table: f"""SELECT DISTINCT * FROM ( SELECT "A", "B" FROM {table} ORDER BY "A" ASC NULLS FIRST, "B" ASC NULLS FIRST )""",
            [Row(1, "c"), Row(3, "b"), Row(3, "c"), Row(5, "a")],
            True,
        ),
        # df.sort(A).select(B).distinct()
        (
            lambda df: df.sort(col("a")).select("b").distinct(),
            lambda table: f"""SELECT DISTINCT * FROM ( SELECT "B" FROM {table} ORDER BY "A" ASC NULLS FIRST )""",
            [Row("a"), Row("b"), Row("c")],
            True,
        ),
        # df.sort(A).distinct().select(B)
        (
            lambda df: df.sort(col("a")).distinct().select("b"),
            lambda table: f"""SELECT "B" FROM ( SELECT DISTINCT * FROM {table} ORDER BY "A" ASC NULLS FIRST )""",
            [Row("a"), Row("b"), Row("c"), Row("c")],
            True,
        ),
        # df.filter(A).select(B).distinct()
        (
            lambda df: df.filter(col("a") > 1).select("b").distinct(),
            lambda table: f"""SELECT DISTINCT "B" FROM {table} WHERE ("A" > 1)""",
            [Row("a"), Row("b"), Row("c")],
            True,
        ),
        # df.filter(A).distinct().select(B)
        (
            lambda df: df.filter(col("a") > 1).distinct().select("b"),
            lambda table: f"""SELECT "B" FROM ( SELECT DISTINCT * FROM {table} WHERE ("A" > 1) )""",
            [Row("a"), Row("b"), Row("c")],
            True,
        ),
    ],
)
def test_select_distinct(
    session, distinct_table, operation, expected_query, expected_result, sort_results
):
    try:
        original = session.conf.get("use_simplified_query_generation")
        session.conf.set("use_simplified_query_generation", True)
        df = session.table(distinct_table)
        df1 = operation(df)
        if expected_result is not None:
            Utils.check_answer(df1, expected_result, sort=sort_results)
        assert Utils.normalize_sql(df1.queries["queries"][0]) == Utils.normalize_sql(
            expected_query(distinct_table)
        )
    finally:
        session.conf.set("use_simplified_query_generation", original)
