#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark._internal.utils import set_ast_state, AstFlagSource
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import (
    col,
    sum,
)
from snowflake.snowpark.window import Window

pytestmark = [
    pytest.mark.xfail(
        "config.getoption('local_testing_mode', default=False)",
        reason="This is a SQL test suite",
        run=False,
    ),
]


@pytest.fixture(autouse=True)
def setup(request, session):
    original = session.ast_enabled
    set_ast_state(AstFlagSource.TEST, True)
    yield
    set_ast_state(AstFlagSource.TEST, original)


def test_python_source_location_in_sql_error(session):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    df1 = df.select(col("a").alias("x"), col("b").alias("y"))
    df2 = df1.select(col("x") + col("a"))
    with pytest.raises(SnowparkSQLException) as ex:
        df2.collect()
    assert "SQL compilation error corresponds to Python source at lines 35" in str(
        ex.value.debug_context
    )


def test_python_source_location_in_session_sql(session):
    df3 = session.sql(
        "SELECT a, b, c + '5' AS invalid_operation FROM (select 1 as a, 2 as b, array_construct(1, 2, 3) as c) WHERE a > 0"
    )
    with pytest.raises(SnowparkSQLException) as ex:
        df3.show()
    assert "SQL compilation error corresponds to Python source at lines 44-46" in str(
        ex.value.debug_context
    )


def test_join_ambiguous_column_error(session):
    df1 = session.create_dataframe([[1, "a"], [2, "b"]], schema=["id", "value"])
    df2 = session.create_dataframe([[1, "x"], [2, "y"]], schema=["id", "data"])
    joined = df1.join(df2, df1["id"] == df2["id"])
    df_error = joined.select(col("id"))
    with pytest.raises(SnowparkSQLException) as ex:
        df_error.collect()
    assert "SQL compilation error corresponds to Python source at lines 58" in str(
        ex.value.debug_context
    )


def test_window_function_error(session):
    df = session.create_dataframe(
        [[1, 10], [2, 20], [3, 30]], schema=["group_id", "value"]
    )
    window_spec = Window.partition_by("nonexistent_column").order_by("value")
    df_error = df.select(col("value"), sum(col("value")).over(window_spec))
    with pytest.raises(SnowparkSQLException) as ex:
        df_error.collect()
    assert "SQL compilation error corresponds to Python source at lines 71" in str(
        ex.value.debug_context
    )


def test_invalid_identifier_error_message(session):
    # we reuse tests from test_snowflake_plan_suite.py since AST is not enabled in that test suite
    df = session.create_dataframe([[1, 2, 3]], schema=['"abc"', '"abd"', '"def"'])
    with pytest.raises(SnowparkSQLException) as ex:
        df.select("abc").collect()
    assert ex.value.sql_error_code == 904
    assert "invalid identifier 'ABC'" in str(ex.value)
    assert (
        "There are existing quoted column identifiers: ['\"abc\"', '\"abd\"', '\"def\"']"
        in str(ex.value)
    )
    assert "Do you mean '\"abc\"'?" in str(ex.value)
    assert "SQL compilation error corresponds to Python source at lines 83" in str(
        ex.value
    )

    with pytest.raises(SnowparkSQLException) as ex:
        df.select("_ab").collect()
    assert "invalid identifier '_AB'" in str(ex.value)
    assert (
        "There are existing quoted column identifiers: ['\"abc\"', '\"abd\"', '\"def\"']"
        in str(ex.value)
    )
    assert "Do you mean '\"abd\"' or '\"abc\"'?" in str(ex.value)
    assert "SQL compilation error corresponds to Python source at lines 96" in str(
        ex.value
    )

    with pytest.raises(SnowparkSQLException) as ex:
        df.select('"abC"').collect()
    assert "invalid identifier '\"abC\"'" in str(ex.value)
    assert (
        "There are existing quoted column identifiers: ['\"abc\"', '\"abd\"', '\"def\"']"
        in str(ex.value)
    )
    assert "Do you mean" not in str(ex.value)
    assert "SQL compilation error corresponds to Python source at lines 108" in str(
        ex.value
    )

    df = session.create_dataframe([list(range(20))], schema=[str(i) for i in range(20)])
    with pytest.raises(
        SnowparkSQLException, match="There are existing quoted column identifiers:*..."
    ) as ex:
        df.select("20").collect()
    assert "SQL compilation error corresponds to Python source at lines 123" in str(
        ex.value
    )

    df = session.create_dataframe([1, 2, 3], schema=["A"])
    with pytest.raises(
        SnowparkSQLException, match="There are existing quoted column identifiers:*..."
    ) as ex:
        df.select("B").schema
    assert "There are existing quoted column identifiers: ['\"A\"']" in str(ex.value)
    assert "SQL compilation error corresponds to Python source at lines 132" in str(
        ex.value
    )
