#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark._internal.utils import TempObjectType
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
)
from tests.integ.utils.sql_counter import SqlCounter
from tests.utils import Utils


@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_read_snowflake_query_basic_cte(session, enforce_ordering):
    expected_query_count = 6 if enforce_ordering else 4
    expected_union_count = 2 if enforce_ordering else 6
    with SqlCounter(query_count=expected_query_count, union_count=expected_union_count):
        # create table
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        session.create_dataframe(
            native_pd.DataFrame([[1, 2], [3, 7], [6, 7]], columns=["A", "B"])
        ).write.save_as_table(table_name, table_type="temp")
        SQL_QUERY = f"""WITH CTE1 AS (SELECT SQUARE(A) AS A2, SQUARE(B) AS B2 FROM {table_name} WHERE A % 2 = 1),
        CTE2 AS (SELECT SQUARE(A2) as A2, SQUARE(B2) AS B4 FROM CTE1 WHERE B2 % 2 = 0) (SELECT * FROM {table_name})
        UNION ALL (SELECT * FROM CTE1) UNION ALL (SELECT * FROM CTE2)"""
        df = pd.read_snowflake(SQL_QUERY, enforce_ordering=enforce_ordering)
        pdf = native_pd.DataFrame(
            [[1, 2], [3, 7], [6, 7], [1, 4], [9, 49], [1, 16]], columns=["A", "B"]
        )
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(df, pdf)


@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_read_snowflake_query_recursive_cte(enforce_ordering):
    expected_query_count = 5 if enforce_ordering else 3
    expected_union_count = 1 if enforce_ordering else 3
    with SqlCounter(query_count=expected_query_count, union_count=expected_union_count):
        SQL_QUERY = """WITH RECURSIVE current_f (current_val, previous_val) AS
                        (
                        SELECT 0, 1
                        UNION ALL
                        SELECT current_val + previous_val, current_val FROM current_f
                        WHERE current_val + previous_val < 100
                        )
                    SELECT current_val FROM current_f ORDER BY current_val"""
        df = pd.read_snowflake(SQL_QUERY, enforce_ordering=enforce_ordering)
        native_df = (
            native_pd.DataFrame(
                [[0], [1], [1], [2], [3], [5], [8], [13], [21], [34], [55], [89]],
                columns=["CURRENT_VAL"],
            )
            .sort_values("CURRENT_VAL")
            .reset_index(drop=True)
        )
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            df.sort_values("CURRENT_VAL").reset_index(drop=True), native_df
        )


@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_read_snowflake_query_complex_recursive_cte(session, enforce_ordering):
    expected_query_count = 6 if enforce_ordering else 4
    expected_join_count = 1 if enforce_ordering else 3
    expected_union_count = 1 if enforce_ordering else 3
    with SqlCounter(
        query_count=expected_query_count,
        join_count=expected_join_count,
        union_count=expected_union_count,
    ):
        # create table
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        session.sql(
            f"""
                    -- The components of a car.
                    CREATE OR REPLACE TEMPORARY TABLE {table_name} (
                        description VARCHAR,
                        component_ID INTEGER,
                        quantity INTEGER,
                        parent_component_ID INTEGER
                        )
                    """
        ).collect()
        session.sql(
            f"""
                    INSERT INTO {table_name} (description, quantity, component_ID, parent_component_ID) VALUES
                        ('car', 1, 1, 0),
                        ('wheel', 4, 11, 1),
                            ('tire', 1, 111, 11),
                            ('#112 bolt', 5, 112, 11),
                            ('brake', 1, 113, 11),
                                ('brake pad', 1, 1131, 113),
                        ('engine', 1, 12, 1),
                            ('piston', 4, 121, 12),
                            ('cylinder block', 1, 122, 12),
                            ('#112 bolt', 16, 112, 12)   -- Can use same type of bolt in multiple places
                    """
        )
        SQL_QUERY = f"""
                        WITH RECURSIVE current_layer (indent, layer_ID, parent_component_ID, component_id, description, sort_key) AS (
                        SELECT
                            '...',
                            1,
                            parent_component_ID,
                            component_id,
                            description,
                            '0001'
                            FROM {table_name} WHERE component_id = 1
                        UNION ALL
                        SELECT indent || '...',
                            layer_ID + 1,
                            {table_name}.parent_component_ID,
                            {table_name}.component_id,
                            {table_name}.description,
                            sort_key || SUBSTRING('000' || {table_name}.component_ID, -4)
                            FROM current_layer JOIN {table_name}
                            ON ({table_name}.parent_component_id = current_layer.component_id)
                        )
                        SELECT
                        -- The indentation gives us a sort of "side-ways tree" view, with
                        -- sub-{table_name} indented under their respective {table_name}.
                        indent || description AS description,
                        component_id,
                        parent_component_ID,
                        sort_key
                        -- The layer_ID and sort_key are useful for debugging, but not
                        -- needed in the report.
                        --  , layer_ID, sort_key
                        FROM current_layer
                        ORDER BY sort_key
                    """
        cur = session.connection.cursor()
        cur.execute(SQL_QUERY)
        native_df = (
            cur.fetch_pandas_all().sort_values("SORT_KEY").reset_index(drop=True)
        )
        snow_df = (
            pd.read_snowflake(SQL_QUERY, enforce_ordering=enforce_ordering)
            .sort_values("SORT_KEY")
            .reset_index(drop=True)
        )
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_df, native_df)


@pytest.mark.parametrize(
    "enforce_ordering",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skip(
                "Queries with CALL statements raise a SQL compilation "
                "error when enforce_ordering=False"
            ),
        ),
    ],
)
def test_read_snowflake_query_cte_with_cross_language_sproc(session, enforce_ordering):
    expected_query_count = 7 if enforce_ordering else 3
    with SqlCounter(query_count=expected_query_count, sproc_count=1):
        # create table name
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        SPROC_CREATION = """
                    WITH filter_by_role AS PROCEDURE (table_name VARCHAR, role VARCHAR)
                    RETURNS TABLE("id" NUMBER, "name" VARCHAR, "role" VARCHAR)
                    LANGUAGE SCALA
                    RUNTIME_VERSION = '2.12'
                    PACKAGES = ('com.snowflake:snowpark:latest')
                    HANDLER = 'Filter.filterByRole'
                    AS
                    $$
                    import com.snowflake.snowpark.functions._
                    import com.snowflake.snowpark._

                    object Filter {
                        def filterByRole(session: Session, tableName: String, role: String): DataFrame = {
                            val table = session.table(tableName)
                            val filteredRows = table.filter(col("\\"role\\"") === role)
                            return filteredRows
                        }
                    }
                    $$
                    """
        SQL_QUERY = f"{SPROC_CREATION} CALL filter_by_role('{table_name}', 'op')"
        native_df = native_pd.DataFrame(
            [[1, "Alice", "op"], [2, "Bob", "dev"], [3, "Cindy", "dev"]],
            columns=["id", "name", "role"],
        )
        session.create_dataframe(native_df).write.save_as_table(
            table_name, table_type="temp"
        )
        native_df = native_df.iloc[0:1]
        snow_df = pd.read_snowflake(SQL_QUERY, enforce_ordering=enforce_ordering)
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_df, native_df)


@pytest.mark.modin_sp_precommit
@pytest.mark.parametrize(
    "enforce_ordering",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skip(
                "Queries with CALL statements raise a SQL compilation "
                "error when enforce_ordering=False"
            ),
        ),
    ],
)
def test_read_snowflake_query_cte_with_python_anonymous_sproc(
    session, enforce_ordering
):
    expected_query_count = 7 if enforce_ordering else 3
    with SqlCounter(query_count=expected_query_count, sproc_count=1):
        # create table name
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        SPROC_CREATION = """
                WITH filterByRole AS PROCEDURE (tableName VARCHAR, role VARCHAR)
                RETURNS TABLE("id" NUMBER, "name" VARCHAR, "role" VARCHAR)
                LANGUAGE PYTHON
                RUNTIME_VERSION = '3.9'
                PACKAGES = ('snowflake-snowpark-python')
                HANDLER = 'filter_by_role'
                AS $$from snowflake.snowpark.functions import col

def filter_by_role(session, table_name, role):
    df = session.table(table_name)
    return df.filter(col('"role"') == role)
                $$
                """
        SQL_QUERY = f"{SPROC_CREATION} CALL filterByRole('{table_name}', 'op')"
        native_df = native_pd.DataFrame(
            [[1, "Alice", "op"], [2, "Bob", "dev"], [3, "Cindy", "dev"]],
            columns=["id", "name", "role"],
        )
        session.create_dataframe(native_df).write.save_as_table(
            table_name, table_type="temp"
        )
        native_df = native_df.iloc[0:1]
        snow_df = pd.read_snowflake(SQL_QUERY, enforce_ordering=enforce_ordering)
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_df, native_df)
