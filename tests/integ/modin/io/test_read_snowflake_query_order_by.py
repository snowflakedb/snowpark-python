#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.modin.plugin.utils.warning_message import (
    ORDER_BY_IN_SQL_QUERY_NOT_GUARANTEED_WARNING,
    WarningMessage,
)
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
)
from tests.integ.utils.sql_counter import SqlCounter
from tests.utils import Utils


@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_select_star_with_order_by(session, caplog, enforce_ordering):
    expected_query_count = 6 if enforce_ordering else 4
    with SqlCounter(query_count=expected_query_count):
        # This test ensures that the presence of an ORDER BY causes us not to take the fastpath
        # of select * from table, where we just do `pd.read_snowflake("table")` instead.
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        # Want random permutation, but need to make sure that there are no duplicates in the sorting column
        # as otherwise ties may be broken differently between us and vanilla pandas.
        native_df = native_pd.DataFrame(
            np.random.choice(10_000, size=(1_000, 10), replace=False),
            columns=[f"col{i}" for i in range(10)],
        )
        session.create_dataframe(native_df).write.save_as_table(
            table_name, table_type="temp"
        )
        caplog.clear()
        WarningMessage.printed_warnings = set()
        with caplog.at_level(logging.DEBUG):
            snow_df = pd.read_snowflake(
                f'SELECT * FROM {table_name} ORDER BY "col8"',
                enforce_ordering=enforce_ordering,
            )
        assert ORDER_BY_IN_SQL_QUERY_NOT_GUARANTEED_WARNING in caplog.text
        native_df = native_df.reset_index(drop=True)
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snow_df.sort_values("col0").reset_index(drop=True),
            native_df.sort_values("col0").reset_index(drop=True),
        )


@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_no_order_by_but_column_name_shadows(session, caplog, enforce_ordering):
    expected_query_count = 5 if enforce_ordering else 3
    with SqlCounter(query_count=expected_query_count):
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        session.create_dataframe(
            native_pd.DataFrame(
                [[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["A", "B", "ORDER BY"]
            )
        ).write.save_as_table(table_name, table_type="temp")
        caplog.clear()
        WarningMessage.printed_warnings = set()
        with caplog.at_level(logging.DEBUG):
            df = pd.read_snowflake(
                f'SELECT A, B, "ORDER BY" FROM {table_name}',
                enforce_ordering=enforce_ordering,
            )
        # verify no temporary table is materialized for regular table
        assert "Materialize temporary table" not in caplog.text
        assert ORDER_BY_IN_SQL_QUERY_NOT_GUARANTEED_WARNING not in caplog.text
        assert df.columns.tolist() == ["A", "B", "ORDER BY"]


@pytest.mark.parametrize("order_by_col", [1, '"ORDER BY"', '"ORDER BY 1"', "A"])
@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_order_by_and_column_name_shadows(
    session, caplog, order_by_col, enforce_ordering
):
    expected_query_count = 6 if enforce_ordering else 4
    with SqlCounter(query_count=expected_query_count):
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        # Want random permutation, but need to make sure that there are no duplicates in the sorting column
        # as otherwise ties may be broken differently between us and vanilla pandas.
        native_df = native_pd.DataFrame(
            np.random.choice(3_000, size=(1_000, 3), replace=False),
            columns=["ORDER BY", "A", "ORDER BY 1"],
        )
        session.create_dataframe(native_df).write.save_as_table(
            table_name, table_type="temp"
        )
        caplog.clear()
        WarningMessage.printed_warnings = set()
        with caplog.at_level(logging.DEBUG):
            snow_df = pd.read_snowflake(
                f'SELECT "ORDER BY", A, "ORDER BY 1" FROM {table_name} ORDER BY {order_by_col}',
                enforce_ordering=enforce_ordering,
            )
        # verify warning issued since we are sorting.
        assert ORDER_BY_IN_SQL_QUERY_NOT_GUARANTEED_WARNING in caplog.text
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snow_df.sort_values("A").reset_index(drop=True),
            native_df.sort_values("A").reset_index(drop=True),
        )


@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_order_by_as_column_name_should_not_warn_negative(
    session, caplog, enforce_ordering
):
    expected_query_count = 6 if enforce_ordering else 4
    with SqlCounter(query_count=expected_query_count):
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        session.create_dataframe(
            native_pd.DataFrame(
                [[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["A", "B", " ORDER BY "]
            )
        ).write.save_as_table(table_name, table_type="temp")
        caplog.clear()
        WarningMessage.printed_warnings = set()
        with caplog.at_level(logging.DEBUG):
            df = pd.read_snowflake(
                f'SELECT " ORDER BY " FROM {table_name}',
                enforce_ordering=enforce_ordering,
            )
            df.to_pandas()  # Force materialization of snowpark dataframe backing this dataframe.
        # In this case, there is no ORDER BY, but since we use string matching, we will get a false
        # positive here.
        assert ORDER_BY_IN_SQL_QUERY_NOT_GUARANTEED_WARNING in caplog.text
        assert df.columns.tolist() == [" ORDER BY "]


@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_inner_order_by_should_be_ignored_and_no_outer_order_by_negative(
    session, caplog, enforce_ordering
):
    expected_query_count = 6 if enforce_ordering else 4
    with SqlCounter(query_count=expected_query_count):
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        session.create_dataframe(
            native_pd.DataFrame(
                [[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=["A", "B", "ORDER BY"]
            )
        ).write.save_as_table(table_name, table_type="temp")
        caplog.clear()
        WarningMessage.printed_warnings = set()
        with caplog.at_level(logging.DEBUG):
            df = pd.read_snowflake(
                f"SELECT * FROM (SELECT * FROM {table_name} ORDER BY 1)",
                enforce_ordering=enforce_ordering,
            )
            df.to_pandas()  # Force materialization of snowpark dataframe backing this dataframe.
        # Ideally, in this case, we would optimize away the ORDER BY, since it has no bearing
        # on the final result; however, we use the logical plan of a SQL Query to determine if
        # its got an ORDER BY, and the logical plan bubbles up nested inner ORDER BY's
        # (for context as to why, see Thierry's message here:
        # https://snowflake.slack.com/archives/C02BTC3HY/p1708032327090439?thread_ts=1708025496.641369&cid=C02BTC3HY)
        # so we still include the sort in our code.
        assert ORDER_BY_IN_SQL_QUERY_NOT_GUARANTEED_WARNING in caplog.text
        assert df.columns.tolist() == ["A", "B", "ORDER BY"]


@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_order_by_with_no_limit_but_colname_shadows(session, caplog, enforce_ordering):
    expected_query_count = 6 if enforce_ordering else 4
    with SqlCounter(query_count=expected_query_count):
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        native_df = native_pd.DataFrame(
            [[1, 2, 4], [4, 5, 6], [7, 8, 3]], columns=["A", "B", "LIMIT 1"]
        )
        session.create_dataframe(native_df).write.save_as_table(
            table_name, table_type="temp"
        )
        WarningMessage.printed_warnings = set()
        caplog.clear()
        with caplog.at_level(logging.DEBUG):
            df = pd.read_snowflake(
                f'SELECT * FROM {table_name} ORDER BY "LIMIT 1"',
                enforce_ordering=enforce_ordering,
            )
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                df.sort_values("A").reset_index(drop=True),
                native_df.sort_values("A").reset_index(drop=True),
            )
        assert ORDER_BY_IN_SQL_QUERY_NOT_GUARANTEED_WARNING in caplog.text


@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_order_by_with_limit_and_name_shadows(session, caplog, enforce_ordering):
    expected_query_count = 5 if enforce_ordering else 3
    with SqlCounter(query_count=expected_query_count):
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        native_df = native_pd.DataFrame(
            [[1, 2, 4], [4, 5, 6], [7, 8, 3]], columns=["A", "B", "LIMIT 1"]
        )
        session.create_dataframe(native_df).write.save_as_table(
            table_name, table_type="temp"
        )
        WarningMessage.printed_warnings = set()
        with caplog.at_level(logging.DEBUG):
            df = pd.read_snowflake(
                f'SELECT * FROM {table_name} ORDER BY "LIMIT 1" LIMIT 2',
                enforce_ordering=enforce_ordering,
            )
            assert len(df) == 2
        assert ORDER_BY_IN_SQL_QUERY_NOT_GUARANTEED_WARNING in caplog.text


@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_read_snowflake_query_complex_query_with_join_and_order_by(
    session, caplog, enforce_ordering
):
    expected_query_count = 7 if enforce_ordering else 5
    expected_join_count = 1 if enforce_ordering else 3
    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
        # create table
        table_name1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        session.create_dataframe(
            native_pd.DataFrame(
                [[10, "car"], [3, "bus"], [6, "train"]],
                columns=["price to consumer", "mode of transportation"],
            )
        ).write.save_as_table(table_name1, table_type="temp")
        table_name2 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        session.create_dataframe(
            native_pd.DataFrame(
                [[5, "car"], [0.5, "bus"], [2, "train"]],
                columns=["cost to operator", "mode of transportation"],
            )
        ).write.save_as_table(table_name2, table_type="temp")
        WarningMessage.printed_warnings = set()
        with caplog.at_level(logging.DEBUG):
            df = pd.read_snowflake(
                f'SELECT "price to consumer" - "cost to operator" as "profit", "mode of transportation" FROM {table_name1} NATURAL JOIN {table_name2} ORDER BY "profit"',
                enforce_ordering=enforce_ordering,
            )
        assert ORDER_BY_IN_SQL_QUERY_NOT_GUARANTEED_WARNING in caplog.text
        pdf = native_pd.DataFrame(
            [[5, "car"], [2.5, "bus"], [4, "train"]],
            columns=["profit", "mode of transportation"],
        )
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            df.sort_values("profit").reset_index(drop=True),
            pdf.sort_values("profit").reset_index(drop=True),
        )


@pytest.mark.parametrize("ordinal", [1, 2, 28])
@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_order_by_with_position_key(session, ordinal, caplog, enforce_ordering):
    expected_query_count = 6 if enforce_ordering else 4
    with SqlCounter(query_count=expected_query_count):
        column_order = [
            "col12",
            "col1",
            "col10",
            "col11",
            "col16",
            "col24",
            "col22",
            "col20",
            "col28",
            "col26",
            "col13",
            "col15",
            "col23",
            "col14",
            "col5",
            "col18",
            "col3",
            "col6",
            "col2",
            "col4",
            "col19",
            "col0",
            "col7",
            "col8",
            "col27",
            "col29",
            "col17",
            "col9",
            "col25",
            "col21",
        ]
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        # Want random permutation, but need to make sure that there are no duplicates in the sorting column
        # as otherwise ties may be broken differently between us and vanilla pandas.
        native_df = native_pd.DataFrame(
            np.arange(60).reshape((2, 30)),
            columns=[f"col{i}" for i in range(30)],
        )
        native_df[column_order[ordinal]].iloc[0] = np.nan
        session.create_dataframe(native_df).write.save_as_table(
            table_name, table_type="temp"
        )
        columns = ", ".join([f'"{col_name}"' for col_name in column_order])
        WarningMessage.printed_warnings = set()
        with caplog.at_level(logging.DEBUG):
            snow_df = pd.read_snowflake(
                f"SELECT * from (SELECT {columns} FROM {table_name}) ORDER BY {ordinal + 1} ASC NULLS LAST",
                enforce_ordering=enforce_ordering,
            )
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                snow_df.sort_values("col12").reset_index(drop=True),
                native_df[column_order].sort_values("col12").reset_index(drop=True),
            )
        assert ORDER_BY_IN_SQL_QUERY_NOT_GUARANTEED_WARNING in caplog.text
