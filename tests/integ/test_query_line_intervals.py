#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark._internal.utils import get_plan_from_line_numbers
from snowflake.snowpark import functions as F
from tests.utils import Utils

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="Tracking query line intervals requires SQL generation",
        run=False,
    ),
]


@pytest.fixture(scope="module")
def test_data(session):
    df1 = session.create_dataframe([[1, "A", 100], [2, "B", 200]]).to_df(
        ["id", "name", "value"]
    )
    df2 = session.create_dataframe([[3, "C", 300], [4, "D", 400]]).to_df(
        ["id", "name", "value"]
    )
    df3 = session.create_dataframe([[5, "E", 500], [6, "F", 600]]).to_df(
        ["id", "name", "value"]
    )
    df_join1 = session.create_dataframe([[1, "A"], [2, "B"]], schema=["id", "name"])
    df_join2 = session.create_dataframe([[1, 10], [2, 20]], schema=["id", "value"])

    return {
        "df1": df1,
        "df2": df2,
        "df3": df3,
        "df_join1": df_join1,
        "df_join2": df_join2,
    }


@pytest.mark.parametrize(
    "op,sql_simplifier,line_to_expected_sql",
    [
        (
            lambda data: data["df1"].union(data["df2"]),
            True,
            {
                0: '( SELECT "_1" AS "ID", "_2" AS "NAME", "_3" AS "VALUE" FROM ( SELECT $1 AS "_1", $2 AS "_2", $3 AS "_3" FROM VALUES (1 :: INT, \'A\' :: STRING, 100 :: INT), (2 :: INT, \'B\' :: STRING, 200 :: INT) ) ) UNION ( SELECT "_1" AS "ID", "_2" AS "NAME", "_3" AS "VALUE" FROM ( SELECT $1 AS "_1", $2 AS "_2", $3 AS "_3" FROM VALUES (3 :: INT, \'C\' :: STRING, 300 :: INT), (4 :: INT, \'D\' :: STRING, 400 :: INT) ) )',
                6: 'SELECT $1 AS "_1", $2 AS "_2", $3 AS "_3" FROM VALUES (1 :: INT, \'A\' :: STRING, 100 :: INT), (2 :: INT, \'B\' :: STRING, 200 :: INT)',
                10: 'SELECT "_1" AS "ID", "_2" AS "NAME", "_3" AS "VALUE" FROM ( SELECT $1 AS "_1", $2 AS "_2", $3 AS "_3" FROM VALUES (3 :: INT, \'C\' :: STRING, 300 :: INT), (4 :: INT, \'D\' :: STRING, 400 :: INT) )',
            },
        ),
        (
            lambda data: data["df_join1"].join(
                data["df_join2"], data["df_join1"].id == data["df_join2"].id
            ),
            True,
            {
                2: 'SELECT * FROM ( ( SELECT "ID" AS "l_0000_ID", "NAME" AS "NAME" FROM ( SELECT $1 AS "ID", $2 AS "NAME" FROM VALUES (1 :: INT, \'A\' :: STRING), (2 :: INT, \'B\' :: STRING) ) ) AS SNOWPARK_LEFT INNER JOIN ( SELECT "ID" AS "r_0001_ID", "VALUE" AS "VALUE" FROM ( SELECT $1 AS "ID", $2 AS "VALUE" FROM VALUES (1 :: INT, 10 :: INT), (2 :: INT, 20 :: INT) ) ) AS SNOWPARK_RIGHT ON ("l_0000_ID" = "r_0001_ID") )',
                7: "SELECT $1 AS \"ID\", $2 AS \"NAME\" FROM  VALUES (1 :: INT, 'A' :: STRING), (2 :: INT, 'B' :: STRING)",
                14: 'SELECT "ID" AS "r_0001_ID", "VALUE" AS "VALUE" FROM ( SELECT $1 AS "ID", $2 AS "VALUE" FROM VALUES (1 :: INT, 10 :: INT), (2 :: INT, 20 :: INT) )',
            },
        ),
        (
            lambda data: data["df1"].filter(data["df1"].value > 150),
            True,
            {
                8: 'SELECT $1 AS "_1", $2 AS "_2", $3 AS "_3" FROM VALUES (1 :: INT, \'A\' :: STRING, 100 :: INT), (2 :: INT, \'B\' :: STRING, 200 :: INT)',
            },
        ),
        (
            lambda data: data["df1"].drop("VALUE"),
            True,
            {
                1: 'SELECT "_1" AS "ID", "_2" AS "NAME" FROM ( SELECT $1 AS "_1", $2 AS "_2", $3 AS "_3" FROM VALUES (1 :: INT, \'A\' :: STRING, 100 :: INT), (2 :: INT, \'B\' :: STRING, 200 :: INT) )',
                4: 'SELECT $1 AS "_1", $2 AS "_2", $3 AS "_3" FROM  VALUES (1 :: INT, \'A\' :: STRING, 100 :: INT), (2 :: INT, \'B\' :: STRING, 200 :: INT)',
            },
        ),
        (
            lambda data: data["df1"].pivot(F.col("name")).sum(F.col("value")),
            True,
            {
                0: 'SELECT * FROM ( SELECT "_1" AS "ID", "_2" AS "NAME", "_3" AS "VALUE" FROM ( SELECT $1 AS "_1", $2 AS "_2", $3 AS "_3" FROM VALUES (1 :: INT, \'A\' :: STRING, 100 :: INT), (2 :: INT, \'B\' :: STRING, 200 :: INT) ) ) PIVOT ( sum("VALUE") FOR "NAME" IN ( ANY ) )',
                6: 'SELECT $1 AS "_1", $2 AS "_2", $3 AS "_3" FROM VALUES (1 :: INT, \'A\' :: STRING, 100 :: INT), (2 :: INT, \'B\' :: STRING, 200 :: INT)',
                9: 'SELECT * FROM ( SELECT "_1" AS "ID", "_2" AS "NAME", "_3" AS "VALUE" FROM ( SELECT $1 AS "_1", $2 AS "_2", $3 AS "_3" FROM VALUES (1 :: INT, \'A\' :: STRING, 100 :: INT), (2 :: INT, \'B\' :: STRING, 200 :: INT) ) ) PIVOT ( sum("VALUE") FOR "NAME" IN ( ANY ) )',
            },
        ),
    ],
)
def test_get_plan_from_line_numbers_sql_content(
    session, op, sql_simplifier, line_to_expected_sql, test_data
):
    session.sql_simplifier_enabled = sql_simplifier
    df = op(test_data)

    for line_num, expected_sql in line_to_expected_sql.items():
        plan = get_plan_from_line_numbers(df._plan, line_num)
        print(plan.queries[-1].sql)
        assert (
            plan is not None
        ), f"get_plan_from_line_numbers returned None for line {line_num}"

        plan_sql = None
        if hasattr(plan, "queries") and plan.queries:
            plan_sql = plan.queries[-1].sql
        elif hasattr(plan, "sql_query") and plan.sql_query:
            plan_sql = plan.sql_query

        assert (
            plan_sql is not None
        ), f"Could not extract SQL from plan for line {line_num}"

        assert Utils.normalize_sql(expected_sql) == Utils.normalize_sql(
            plan_sql
        ), f"Line {line_num}: Expected SQL '{expected_sql}' not equal to plan sql:\n{plan_sql}"


@pytest.mark.parametrize(
    "line_num,expected_error",
    [
        (6, "Line number 6 does not fall within any interval"),
    ],
)
def test_get_plan_from_line_numbers_error_cases(session, line_num, expected_error):
    """Test get_plan_from_line_numbers error cases with specific inputs."""
    session.sql_simplifier_enabled = False

    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])

    with pytest.raises(ValueError, match=expected_error):
        get_plan_from_line_numbers(df._plan, line_num)
