#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark import Row, context
from snowflake.snowpark._internal.analyzer.select_statement import (
    SET_EXCEPT,
    SET_INTERSECT,
    SET_UNION,
    SET_UNION_ALL,
)
from snowflake.snowpark.context import _use_sql_simplifier
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import col, lit, sql_expr
from tests.utils import Utils

if not _use_sql_simplifier:
    pytest.skip(
        "Disable sql simplifier test when simplifier is disabled",
        allow_module_level=True,
    )


@pytest.fixture(scope="module")
def simplifier_table(session) -> None:
    table_name = Utils.random_table_name()
    Utils.create_table(session, table_name, "a int, b int")
    session._conn.run_query(f"insert into {table_name}(a, b) values (1, 2)")
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
            result1, [Row(1, 2), Row(2, 2), Row(3, 2), Row(4, 2)], sort=False
        )
    elif SET_UNION_ALL == set_operator:
        result1 = df1.union_all(df2).union_all(df3.union_all(df4))
        Utils.check_answer(
            result1, [Row(1, 2), Row(2, 2), Row(3, 2), Row(4, 2)], sort=False
        )
    elif SET_EXCEPT == set_operator:
        result1 = df1.except_(df2).except_(df3.except_(df4))
        Utils.check_answer(result1, [Row(1, 2)], sort=False)
    else:
        result1 = df1.intersect(df2).intersect(df3.intersect(df4))
        Utils.check_answer(result1, [], sort=False)

    query1 = result1._plan.queries[-1].sql
    assert (
        query1
        == f"(SELECT 1 as a, 2 as b){set_operator}(SELECT 2 as a, 2 as b){set_operator}((SELECT 3 as a, 2 as b){set_operator}(SELECT 4 as a, 2 as b))"
    )


@pytest.mark.parametrize("set_operator", [SET_UNION_ALL, SET_EXCEPT, SET_INTERSECT])
def test_union_and_other_operators(session, set_operator):
    df1 = session.sql("SELECT 1 as a")
    df2 = session.sql("SELECT 2 as a")
    df3 = session.sql("SELECT 3 as a")

    if SET_UNION_ALL == set_operator:
        result1 = df1.union(df2).union_all(df3)
        result2 = df1.union(df2.union_all(df3))
        assert (
            result1._plan.queries[-1].sql
            == f"(SELECT 1 as a) UNION (SELECT 2 as a){set_operator}((SELECT 3 as a))"
        )
        assert (
            result2._plan.queries[-1].sql
            == f"(SELECT 1 as a) UNION ((SELECT 2 as a){set_operator}(SELECT 3 as a))"
        )
    elif SET_EXCEPT == set_operator:
        result1 = df1.union(df2).except_(df3)
        result2 = df1.union(df2.except_(df3))
        assert (
            result1._plan.queries[-1].sql
            == f"(SELECT 1 as a) UNION (SELECT 2 as a){set_operator}((SELECT 3 as a))"
        )
        assert (
            result2._plan.queries[-1].sql
            == f"(SELECT 1 as a) UNION ((SELECT 2 as a){set_operator}(SELECT 3 as a))"
        )
    else:  # intersect
        # intersect has higher precedence than union and other set operators
        result1 = df1.union(df2).intersect(df3)
        result2 = df1.union(df2.intersect(df3))
        assert (
            result1._plan.queries[-1].sql
            == f"((SELECT 1 as a) UNION (SELECT 2 as a)){set_operator}((SELECT 3 as a))"
        )
        assert (
            result2._plan.queries[-1].sql
            == f"(SELECT 1 as a) UNION ((SELECT 2 as a){set_operator}(SELECT 3 as a))"
        )


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

    """ query has no new columns. subquery has new, chnged or dropped columns."""
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

    """ query has new columns. subquery has new, chnged or dropped columns."""
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


def test_with_column(session, simplifier_table):
    df = session.table(simplifier_table)
    new_df = df
    for i in range(10):
        new_df = new_df.with_column(f"c{i}", lit(i))

    assert new_df._plan.queries[-1].sql.count("SELECT") == 1


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


def test_order_by(session, simplifier_table):
    df = session.table(simplifier_table)

    # flatten
    df1 = df.sort("a", col("b") + 1)
    assert (
        df1.queries["queries"][-1]
        == f'SELECT  *  FROM {simplifier_table} ORDER BY "A" ASC NULLS FIRST, ("B" + 1 :: INT) ASC NULLS FIRST'
    )

    # flatten
    df2 = df.select("a", "b").sort("a", "b")
    assert (
        df2.queries["queries"][-1]
        == f'SELECT "A", "B" FROM {simplifier_table} ORDER BY "A" ASC NULLS FIRST, "B" ASC NULLS FIRST'
    )

    # no flatten because c is a new column
    df3 = df.select("a", "b", (col("a") - col("b")).as_("c")).sort("a", "b", "c")
    assert (
        df3.queries["queries"][-1]
        == f'SELECT  *  FROM ( SELECT "A", "B", ("A" - "B") AS "C" FROM {simplifier_table}) ORDER BY "A" ASC NULLS FIRST, "B" ASC NULLS FIRST, "C" ASC NULLS FIRST'
    )

    # no flatten because a and be are changed
    df4 = df.select((col("a") + 1).as_("a"), ((col("b") + 1).as_("b"))).sort("a", "b")
    assert (
        df4.queries["queries"][-1]
        == f'SELECT  *  FROM ( SELECT ("A" + 1 :: INT) AS "A", ("B" + 1 :: INT) AS "B" FROM {simplifier_table}) ORDER BY "A" ASC NULLS FIRST, "B" ASC NULLS FIRST'
    )


def test_filter(session, simplifier_table):
    df = session.table(simplifier_table)

    # flatten
    df1 = df.filter((col("a") > 1) & (col("b") > 2))
    assert (
        df1.queries["queries"][-1]
        == f'SELECT  *  FROM {simplifier_table} WHERE (("A" > 1 :: INT) AND ("B" > 2 :: INT))'
    )

    # flatten
    df2 = df.select("a", "b").filter((col("a") > 1) & (col("b") > 2))
    assert (
        df2.queries["queries"][-1]
        == f'SELECT "A", "B" FROM {simplifier_table} WHERE (("A" > 1 :: INT) AND ("B" > 2 :: INT))'
    )

    # no flatten because c is a new column
    df3 = df.select("a", "b", (col("a") - col("b")).as_("c")).filter(
        (col("a") > 1) & (col("b") > 2) & (col("c") < 1)
    )
    assert (
        df3.queries["queries"][-1]
        == f'SELECT  *  FROM ( SELECT "A", "B", ("A" - "B") AS "C" FROM {simplifier_table}) WHERE ((("A" > 1 :: INT) AND ("B" > 2 :: INT)) AND ("C" < 1 :: INT))'
    )

    # no flatten because a and be are changed
    df4 = df.select((col("a") + 1).as_("a"), (col("b") + 1).as_("b")).filter(
        (col("a") > 1) & (col("b") > 2)
    )
    assert (
        df4.queries["queries"][-1]
        == f'SELECT  *  FROM ( SELECT ("A" + 1 :: INT) AS "A", ("B" + 1 :: INT) AS "B" FROM {simplifier_table}) WHERE (("A" > 1 :: INT) AND ("B" > 2 :: INT))'
    )

    df5 = df4.select("a")
    print(df5.queries["queries"][-1])


def test_limit(session, simplifier_table):
    df = session.table(simplifier_table)
    df = df.limit(10)
    assert df.queries["queries"][-1] == f"SELECT  *  FROM {simplifier_table} LIMIT 10"

    df = session.sql(f"select * from {simplifier_table}")
    df = df.limit(10)
    # we don't know if the original sql already has top/limit clause using a subquery is necessary.
    #  or else there will be SQL compile error.
    assert (
        df.queries["queries"][-1]
        == f"SELECT  *  FROM (select * from {simplifier_table}) LIMIT 10"
    )


def test_filter_order_limit_together(session, simplifier_table):
    df = session.table(simplifier_table)
    df1 = df.select("a", "b").filter(col("b") > 1).sort("a").limit(5)
    assert (
        df1.queries["queries"][-1]
        == f'SELECT "A", "B" FROM {simplifier_table} WHERE ("B" > 1 :: INT) ORDER BY "A" ASC NULLS FIRST LIMIT 5'
    )

    df2 = df1.select("a")
    assert (
        df2.queries["queries"][-1]
        == f'SELECT "A" FROM ( SELECT "A", "B" FROM {simplifier_table} WHERE ("B" > 1 :: INT) ORDER BY "A" ASC NULLS FIRST LIMIT 5)'
    )


def test_use_sql_simplifier(session, simplifier_table):
    try:
        context._use_sql_simplifier = False
        df1 = (
            session.sql(f"SELECT * from {simplifier_table}")
            .select("*")
            .select("a")
            .select("a")
            .filter(col("a") == 1)
            .sort("a")
        )
        context._use_sql_simplifier = True
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

        context._use_sql_simplifier = False
        df3 = (
            session.table(simplifier_table)
            .select("*")
            .select("a")
            .select("a")
            .filter(col("a") == 1)
            .sort("a")
        )
        context._use_sql_simplifier = True
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
        context._use_sql_simplifier = True


def test_select_star(session, simplifier_table):
    df = session.table(simplifier_table)
    df1 = df.select("*")
    assert df1.queries["queries"][0] == f"SELECT  *  FROM {simplifier_table}"

    df2 = df.select(df["*"])  # no flatten
    assert (
        df2.queries["queries"][0]
        == f'SELECT "A","B" FROM ( SELECT  *  FROM {simplifier_table})'
    )

    df3 = df.select("*", "a")
    assert (
        df3.queries["queries"][0]
        == f'SELECT *, "A" FROM ( SELECT  *  FROM {simplifier_table})'
    )

    df4 = df.select(df["*"], "a")
    assert (
        df4.queries["queries"][0]
        == f'SELECT "A","B", "A" FROM ( SELECT  *  FROM {simplifier_table})'
    )

    df5 = df3.select("b")
    assert (
        df5.queries["queries"][0]
        == f'SELECT "B" FROM ( SELECT *, "A" FROM ( SELECT  *  FROM {simplifier_table}))'
    )

    with pytest.raises(SnowparkSQLException, match="ambiguous column name 'A'"):
        df3.select("a").collect()

    with pytest.raises(SnowparkSQLException, match="ambiguous column name 'A'"):
        df4.select("a").collect()

    # tests/integ/scala/test_dataframe_join_suite.py has tests of join and select *. We don't repeat those tests here.
