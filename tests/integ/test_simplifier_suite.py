#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.functions import col, lit
from tests.utils import Utils


@pytest.fixture(scope="module")
def simplifier_table(session) -> None:
    table_name = Utils.random_table_name()
    Utils.create_table(session, table_name, "a int, b int")
    session._conn.run_query(f"insert into {table_name}(a, b) values (1, 2)")
    yield table_name
    Utils.drop_table(session, table_name)


def test_union_union(session):
    df1 = session.create_dataframe([[1, 2]], schema=["a", "b"])
    df2 = session.create_dataframe([[2, 2]], schema=["a", "b"])
    df3 = session.create_dataframe([[3, 2]], schema=["a", "b"])
    df4 = session.create_dataframe([[4, 2]], schema=["a", "b"])

    result1 = df1.union(df2).union(df3.union(df4))
    Utils.check_answer(
        result1, [Row(1, 2), Row(2, 2), Row(3, 2), Row(4, 2)], sort=False
    )
    query1 = result1._plan.queries[-1].sql
    assert len(query1.split("UNION (")) == 4

    result2 = df1.union(df2).union(df3)
    Utils.check_answer(result2, [Row(1, 2), Row(2, 2), Row(3, 2)], sort=False)
    query2 = result2._plan.queries[-1].sql
    assert len(query2.split("UNION (")) == 3

    # mix union and union all
    result3 = df1.union(df2).union(df3.union_all(df4))
    Utils.check_answer(
        result3, [Row(1, 2), Row(2, 2), Row(3, 2), Row(4, 2)], sort=False
    )
    query3 = result1._plan.queries[-1].sql
    assert len(query3.split("UNION (")) == 4
    assert len(query3.split("UNION ( SELECT")) == 2
    assert len(query3.split("UNION (( SELECT")) == 2
    assert len(query3.split("UNION ALL ( SELECT")) == 2


def union_all_plus_union_all(session):
    ...


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
    # select columns and change sequence from columns that have same level column dependency. No flatten.
    df12 = df.select(df.a, df.b, (df.a + 10).as_("c"), (col("c") + 10).as_("d"))
    df13 = df12.select("a", "b", "d", "c")
    Utils.check_answer(df13, [Row(1, 2, 21, 11)])
    assert df13.queries["queries"][-1].count("SELECT") == 2

    # select columns and change sequence from columns that have no same level column. Flatten.
    df14 = df.select(df.a, df.b, (df.a + 10).as_("c"), (col("b") + 10).as_("d"))
    df15 = df14.select("a", "b", "d", "c")
    Utils.check_answer(df15, [Row(1, 2, 12, 11)])
    assert df15.queries["queries"][-1].count("SELECT") == 1

    # select columns, change a column that reference to the a same-level column. Flatten.
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
    # use column name as sql text
    df1 = df.select_expr("a", "b")
    Utils.check_answer(df1, [Row(1, 2)])
    assert df1.queries["queries"][-1].count("SELECT") == 1

    # use an sql expression. Flatten.
    df2 = df.select_expr("a + 1 as a", "b + 1 as b")
    Utils.check_answer(df2, [Row(2, 3)])
    assert df2.queries["queries"][-1].count("SELECT") == 1

    # query again after sql_expr. No flatten.
    df3 = df2.select("a", "b")
    Utils.check_answer(df3, [Row(2, 3)])
    assert df3.queries["queries"][-1].count("SELECT") == 2


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

    # drop a column not referenced by other same-level columns, but the subquery has other same-level reference
    # we don't flatten even though in theory it can be flattened. It's not expected to happen very often that a user drops a column they defined previously.
    # This saves some memory that would store which columns reference to a column
    df3 = df1.select("a", "b", "c")
    Utils.check_answer(df3, [Row(1, 2, 2)])
    assert df3._plan.queries[-1].sql.count("SELECT") == 1


def test_reference_non_exist_columns(session, simplifier_table):
    df = session.table(simplifier_table)
    with pytest.raises(
        ValueError,
        match="""Column name "C" used in a column expression but it doesn't exist""",
    ):
        df.select(col("c") + 1).collect()
