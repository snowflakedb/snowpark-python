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
def native_pandas_ser_basic():
    native_ser = native_pd.Series([4, 7, 4, 2], name="A")
    return native_ser


@sql_count_checker(query_count=2)
def test_create_or_replace_view_basic(session, native_pandas_ser_basic) -> None:
    view_name = Utils.random_view_name()
    try:
        snow_series = pd.Series(native_pandas_ser_basic)

        assert (
            "successfully created"
            in snow_series.create_or_replace_view(name=view_name)[0]["status"]
        )
    finally:
        Utils.drop_view(session, view_name)


@sql_count_checker(query_count=6)
def test_create_or_replace_view_multiple_sessions_no_relaxed_ordering_raises(
    session,
) -> None:
    try:
        # create table
        table_name = Utils.random_table_name()
        session.create_dataframe(
            [BASIC_TYPE_DATA1, BASIC_TYPE_DATA2]
        ).write.save_as_table(table_name)

        # create series with relaxed_ordering disabled
        snow_series = pd.read_snowflake(
            f"(((SELECT * FROM {table_name})))", relaxed_ordering=False
        ).iloc[:, 0]

        # create view
        view_name = Utils.random_view_name()
        assert (
            "successfully created"
            in snow_series.create_or_replace_view(name=view_name)[0]["status"]
        )

        # another session
        new_session = Session.builder.create()
        pd.session = new_session

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


@sql_count_checker(query_count=2)
def test_create_or_replace_view_multiple_sessions_relaxed_ordering(session) -> None:
    try:
        # create table
        table_name = Utils.random_table_name()
        session.create_dataframe(
            [BASIC_TYPE_DATA1, BASIC_TYPE_DATA2]
        ).write.save_as_table(table_name)

        # create series with relaxed_ordering enabled
        snow_series = pd.read_snowflake(
            f"(((SELECT * FROM {table_name})))", relaxed_ordering=True
        ).iloc[:, 0]

        # create view
        view_name = Utils.random_view_name()
        assert (
            "successfully created"
            in snow_series.create_or_replace_view(name=view_name)[0]["status"]
        )

        # another session
        new_session = Session.builder.create()
        pd.session = new_session

        # accessing the created view in another session succeeds when relaxed_ordering is enabled
        res = new_session.sql(f"select * from {view_name}").collect()
        assert len(res) == 2
    finally:
        # cleanup
        Utils.drop_view(new_session, view_name)
        Utils.drop_table(new_session, table_name)
        new_session.close()
        pd.session = session
