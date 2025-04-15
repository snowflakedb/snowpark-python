#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.session import Session
from tests.integ.modin.utils import BASIC_TYPE_DATA1, BASIC_TYPE_DATA2
from tests.integ.utils.sql_counter import sql_count_checker
from tests.utils import Utils


@sql_count_checker(query_count=7)
def test_to_dynamic_table_enforce_ordering_raises(session) -> None:
    try:
        # create table
        table_name = Utils.random_table_name()
        session.create_dataframe(
            [BASIC_TYPE_DATA1, BASIC_TYPE_DATA2]
        ).write.save_as_table(table_name)

        # create series with enforce_ordering enabled
        snow_series = pd.read_snowflake(
            f"(((SELECT * FROM {table_name})))", enforce_ordering=True
        ).iloc[:, 0]

        # creating dynamic_table fails when enforce_ordering is enabled
        # because it cannot depend on a temp table
        dynamic_table_name = Utils.random_name_for_temp_object(
            TempObjectType.DYNAMIC_TABLE
        )
        with pytest.raises(
            SnowparkSQLException,
            match="Dynamic Tables cannot depend on a temporary object",
        ):
            snow_series.to_dynamic_table(
                name=dynamic_table_name,
                warehouse=session.get_current_warehouse(),
                lag="1000 minutes",
            )
    finally:
        # cleanup
        Utils.drop_dynamic_table(session, dynamic_table_name)
        Utils.drop_table(session, table_name)


@sql_count_checker(query_count=6)
def test_to_dynamic_table_no_enforce_ordering(session) -> None:
    try:
        # create table
        table_name = Utils.random_table_name()
        session.create_dataframe(
            [BASIC_TYPE_DATA1, BASIC_TYPE_DATA2]
        ).write.save_as_table(table_name)

        # create series with enforce_ordering disabled
        snow_series = pd.read_snowflake(
            f"(((SELECT * FROM {table_name})))", enforce_ordering=False
        ).iloc[:, 0]

        # creating dynamic_table succeeds when enforce_ordering is disabled
        dynamic_table_name = Utils.random_name_for_temp_object(
            TempObjectType.DYNAMIC_TABLE
        )
        assert (
            "successfully created"
            in snow_series.to_dynamic_table(
                name=dynamic_table_name,
                warehouse=session.get_current_warehouse(),
                lag="1000 minutes",
            )[0]["status"]
        )

        # accessing the created dynamic_table in the same session also succeeds
        res = session.sql(f"select * from {dynamic_table_name}").collect()
        assert len(res) == 2
    finally:
        # cleanup
        Utils.drop_dynamic_table(session, dynamic_table_name)
        Utils.drop_table(session, table_name)


@sql_count_checker(query_count=5)
def test_to_dynamic_table_multiple_sessions_no_enforce_ordering(
    session,
    db_parameters,
) -> None:
    try:
        # create table
        table_name = Utils.random_table_name()
        session.create_dataframe(
            [BASIC_TYPE_DATA1, BASIC_TYPE_DATA2]
        ).write.save_as_table(table_name)

        # create series with enforce_ordering disabled
        snow_series = pd.read_snowflake(
            f"(((SELECT * FROM {table_name})))", enforce_ordering=False
        ).iloc[:, 0]

        # creating dynamic_table succeeds when enforce_ordering is disabled
        dynamic_table_name = Utils.random_name_for_temp_object(
            TempObjectType.DYNAMIC_TABLE
        )
        assert (
            "successfully created"
            in snow_series.to_dynamic_table(
                name=dynamic_table_name,
                warehouse=session.get_current_warehouse(),
                lag="1000 minutes",
            )[0]["status"]
        )

        # another session
        new_session = Session.builder.configs(db_parameters).create()
        pd.session = new_session

        # accessing the created dynamic_table in another session also succeeds
        res = new_session.sql(f"select * from {dynamic_table_name}").collect()
        assert len(res) == 2
        new_session.close()
    finally:
        # cleanup
        Utils.drop_dynamic_table(session, dynamic_table_name)
        Utils.drop_table(session, table_name)
        pd.session = session


@pytest.mark.parametrize("index", [True, False])
@pytest.mark.parametrize("index_labels", [None, ["my_index"]])
@sql_count_checker(query_count=6)
def test_to_dynamic_table_index(session, index, index_labels):
    try:
        # create table
        table_name = Utils.random_table_name()
        session.create_dataframe(
            [BASIC_TYPE_DATA1, BASIC_TYPE_DATA2]
        ).write.save_as_table(table_name)

        # create series with enforce_ordering disabled
        snow_series = pd.read_snowflake(
            f"(((SELECT * FROM {table_name})))", enforce_ordering=False
        ).iloc[:, 0]

        dynamic_table_name = Utils.random_name_for_temp_object(
            TempObjectType.DYNAMIC_TABLE
        )
        snow_series.to_dynamic_table(
            name=dynamic_table_name,
            warehouse=session.get_current_warehouse(),
            lag="1000 minutes",
            index=index,
            index_label=index_labels,
        )
        expected_columns = []
        if index:
            # if index is retained in the result, add it as the first expected column
            expected_index = ["index"]
            if index_labels:
                expected_index = index_labels
            expected_columns = expected_columns + expected_index
        # add the expected data columns
        expected_columns = expected_columns + ["_1"]

        # verify columns
        actual = pd.read_snowflake(
            dynamic_table_name,
            enforce_ordering=False,
        ).columns
        assert actual.tolist() == expected_columns
    finally:
        # cleanup
        Utils.drop_dynamic_table(session, dynamic_table_name)
        Utils.drop_table(session, table_name)


@sql_count_checker(query_count=6)
def test_to_dynamic_table_multiindex(session):
    try:
        # create table
        table_name = Utils.random_table_name()
        session.create_dataframe(
            [BASIC_TYPE_DATA1, BASIC_TYPE_DATA2]
        ).write.save_as_table(table_name)

        # create dataframe with enforce_ordering disabled
        snow_dataframe = pd.read_snowflake(
            f"(((SELECT * FROM {table_name})))", enforce_ordering=False
        )

        # make sure dataframe has a multi-index
        snow_dataframe = snow_dataframe.set_index(["_1", "_2"])

        # create series
        snow_series = snow_dataframe.iloc[:, 0]

        dynamic_table_name = Utils.random_name_for_temp_object(
            TempObjectType.DYNAMIC_TABLE
        )
        snow_series.to_dynamic_table(
            name=dynamic_table_name,
            warehouse=session.get_current_warehouse(),
            lag="1000 minutes",
            index=True,
        )

        # verify columns
        actual = pd.read_snowflake(
            dynamic_table_name,
            enforce_ordering=False,
        ).columns
        assert actual.tolist() == ["_1", "_2", "_3"]

        with pytest.raises(
            ValueError, match="Length of 'index_label' should match number of levels"
        ):
            snow_series.to_dynamic_table(
                name=dynamic_table_name,
                warehouse=session.get_current_warehouse(),
                lag="1000 minutes",
                index=True,
                index_label=["a"],
            )
    finally:
        # cleanup
        Utils.drop_dynamic_table(session, dynamic_table_name)
        Utils.drop_table(session, table_name)
