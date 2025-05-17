#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark._internal.utils import set_ast_state, AstFlagSource
from snowflake.snowpark.functions import col, explode, sum as sum_, split
from tests.utils import Utils


@pytest.fixture(autouse=True)
def setup(request, session):
    original = session.ast_enabled
    set_ast_state(AstFlagSource.TEST, True)
    yield
    set_ast_state(AstFlagSource.TEST, original)


@pytest.fixture(scope="module")
def test_table(session):
    table_name = Utils.random_table_name()
    df = session.create_dataframe(
        [[":", "colon:separated:values", 3], [":", "more:colon:separated:values", 6]],
        schema=["a", "b", "c"],
    )
    df.write.mode("overwrite").save_as_table(table_name, table_type="temp")

    yield table_name

    Utils.drop_table(session, table_name)


def verify_ast_id_consistency(df):
    assert df._ast_id is not None
    assert df._ast_id == df._plan.df_ast_id
    assert df._select_statement.df_ast_ids is not None
    assert df._select_statement.df_ast_ids[-1] == df._ast_id


@pytest.mark.parametrize(
    "op",
    [
        lambda df, _: df.select("a", "b"),
        lambda df, _: df.filter("a > 1"),
        lambda df, _: df.sort("a"),
        lambda df, _: df.limit(10),
        lambda df, _: df.distinct(),
        lambda df, _: df.to_df("a1", "b1", "c1"),
        lambda df, _: df.group_by("a", "b").agg(sum_("c")),
        lambda df, _: df.with_column("a1", col("a") + 1),
        lambda df, df2: df.join(df2, on="a", how="left"),
        lambda df, df2: df.union(df2),
        lambda df, df2: df.intersect(df2),
        lambda df, df2: df.except_(df2),
    ],
)
def test_df_ast_id(session, local_testing_mode, op, test_table):
    if not session.sql_simplifier_enabled:
        pytest.skip("sql simplifier is not enabled")
    df1 = session.table(test_table)
    df2 = session.create_dataframe(
        [
            [",", "some,comma,separated,values", 33],
            [",", "some,more,comma,separated,values", 66],
        ],
        schema=["a", "b", "c"],
    )

    verify_ast_id_consistency(df1)
    verify_ast_id_consistency(df2)

    df = op(df1, df2)

    verify_ast_id_consistency(df)

    df = df.select(col("$1").as_("a1"), col("$2").as_("b1")).filter(col("a1") > 1)
    verify_ast_id_consistency(df)

    if not local_testing_mode:
        df = df.select(
            col("a1"), split(col("b1"), col("a1")).alias("array_col")
        ).select("a1", explode(col("array_col")).alias("exploded_col"))
        verify_ast_id_consistency(df)
