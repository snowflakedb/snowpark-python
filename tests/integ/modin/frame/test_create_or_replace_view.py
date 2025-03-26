#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.session import Session
from tests.integ.modin.utils import BASIC_TYPE_DATA1, BASIC_TYPE_DATA2
from tests.integ.utils.sql_counter import sql_count_checker
from tests.utils import Utils


@pytest.fixture(scope="function")
def native_pandas_df_basic():
    native_df = native_pd.DataFrame(
        {
            "ID": [1, 2, 3],
            "FOOT_SIZE": [32.0, 27.0, 40.0],
            "SHOE_MODEL": ["medium", "small", "large"],
        }
    )
    native_df = native_df.set_index("ID")
    return native_df


@sql_count_checker(query_count=2)
def test_create_or_replace_view_basic(session, native_pandas_df_basic) -> None:
    view_name = Utils.random_view_name()
    try:
        snow_dataframe = pd.DataFrame(native_pandas_df_basic)

        assert (
            "successfully created"
            in snow_dataframe.create_or_replace_view(name=view_name)[0]["status"]
        )
    finally:
        Utils.drop_view(session, view_name)


@sql_count_checker(query_count=6)
def test_create_or_replace_view_multiple_sessions_no_relaxed_ordering_raises(
    session, native_pandas_df_basic
) -> None:
    try:
        # create table
        table_name = Utils.random_table_name()
        session.create_dataframe(
            [BASIC_TYPE_DATA1, BASIC_TYPE_DATA2]
        ).write.save_as_table(table_name)

        # create dataframe with relaxed_ordering disabled
        snow_dataframe = pd.read_snowflake(
            f"(((SELECT * FROM {table_name})))", relaxed_ordering=False
        )

        # create view
        view_name = Utils.random_view_name()
        assert (
            "successfully created"
            in snow_dataframe.create_or_replace_view(name=view_name)[0]["status"]
        )

        # another session
        new_session = Session.builder.create()
        pd.session = session

        # accessing the created view in another session fails when relaxed_ordering is disabled
        with pytest.raises(
            SnowparkSQLException,
            match="Object 'VIEW_NAME' does not exist or not authorized",
        ):
            new_session.sql("select * from view_name").collect()
        new_session.close()
    finally:
        # cleanup
        Utils.drop_view(session, view_name)
        Utils.drop_table(session, table_name)
        pd.session = session


@sql_count_checker(query_count=4)
def test_create_or_replace_view_multiple_sessions_relaxed_ordering(
    session, native_pandas_df_basic
) -> None:
    try:
        # create table
        table_name = Utils.random_table_name()
        session.create_dataframe(
            [BASIC_TYPE_DATA1, BASIC_TYPE_DATA2]
        ).write.save_as_table(table_name)

        # create dataframe with relaxed_ordering enabled
        snow_dataframe = pd.read_snowflake(
            f"(((SELECT * FROM {table_name})))", relaxed_ordering=True
        )

        # create view
        view_name = Utils.random_view_name()
        assert (
            "successfully created"
            in snow_dataframe.create_or_replace_view(name=view_name)[0]["status"]
        )

        # another session
        new_session = Session.builder.create()
        pd.session = new_session

        # accessing the created view in another session succeeds when relaxed_ordering is enabled
        res = new_session.sql(f"select * from {view_name}").collect()
        assert len(res) == 2
        new_session.close()
    finally:
        # cleanup
        Utils.drop_view(session, view_name)
        Utils.drop_table(session, table_name)
        pd.session = session
