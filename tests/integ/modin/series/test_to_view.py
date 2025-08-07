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


@pytest.fixture(
    params=[
        pytest.param(
            lambda obj, *args, **kwargs: obj.to_view(*args, **kwargs),
            id="method",
        ),
        pytest.param(pd.to_view, id="function"),
    ]
)
def to_view(request):
    return request.param


@sql_count_checker(query_count=2)
def test_to_view_basic(session, native_pandas_ser_basic, to_view) -> None:
    view_name = Utils.random_view_name()
    try:
        snow_series = pd.Series(native_pandas_ser_basic)

        result = to_view(snow_series, name=view_name)

        assert "successfully created" in result[0]["status"]
    finally:
        Utils.drop_view(session, view_name)


@sql_count_checker(query_count=6)
def test_to_view_multiple_sessions_enforce_ordering_raises(
    session,
    db_parameters,
    to_view,
) -> None:
    try:
        # create table
        table_name = Utils.random_table_name()
        session.create_dataframe(
            [BASIC_TYPE_DATA1, BASIC_TYPE_DATA2]
        ).write.save_as_table(table_name)

        # create series with enforce_ordering enabled
        snow_series = pd.read_snowflake(
            f"SELECT * FROM {table_name}", enforce_ordering=True
        ).iloc[:, 0]

        # create view
        view_name = Utils.random_view_name()
        result = to_view(snow_series, name=view_name)

        assert "successfully created" in result[0]["status"]

        # another session
        new_session = Session.builder.configs(db_parameters).create()
        pd.session = new_session

        # accessing the created view in another session fails when enforce_ordering is enabled
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


@sql_count_checker(query_count=5)
def test_to_view_multiple_sessions_no_enforce_ordering(
    session,
    db_parameters,
    to_view,
) -> None:
    try:
        # create table
        table_name = Utils.random_table_name()
        session.create_dataframe(
            [BASIC_TYPE_DATA1, BASIC_TYPE_DATA2]
        ).write.save_as_table(table_name)

        # create series with enforce_ordering disabled
        snow_series = pd.read_snowflake(
            f"SELECT * FROM {table_name}", enforce_ordering=False
        ).iloc[:, 0]

        # create view
        view_name = Utils.random_view_name()
        result = to_view(snow_series, name=view_name)

        assert "successfully created" in result[0]["status"]

        # another session
        new_session = Session.builder.configs(db_parameters).create()
        pd.session = new_session

        # accessing the created view in another session succeeds when enforce_ordering is disabled
        res = new_session.sql(f"select * from {view_name}").collect()
        assert len(res) == 2
        new_session.close()

    finally:
        # cleanup
        Utils.drop_view(session, view_name)
        Utils.drop_table(session, table_name)
        pd.session = session


@pytest.mark.parametrize(
    "index, index_labels, expected_index_columns",
    [
        (True, None, ["index"]),
        (True, ["my_index"], ["my_index"]),
        (False, None, []),
        (False, ["my_index"], []),
    ],
)
@sql_count_checker(query_count=6)
def test_to_view_index(session, index, index_labels, expected_index_columns, to_view):
    try:
        # create table
        table_name = Utils.random_table_name()
        session.create_dataframe(
            [BASIC_TYPE_DATA1, BASIC_TYPE_DATA2]
        ).write.save_as_table(table_name)

        # create series with enforce_ordering disabled
        snow_series = pd.read_snowflake(
            f"SELECT * FROM {table_name}", enforce_ordering=False
        ).iloc[:, 0]

        view_name = Utils.random_view_name()
        to_view(
            snow_series,
            name=view_name,
            index=index,
            index_label=index_labels,
        )

        # add the expected data columns
        expected_columns = expected_index_columns + ["_1"]

        # verify columns
        actual = pd.read_snowflake(
            view_name,
            enforce_ordering=False,
        ).columns
        assert actual.tolist() == expected_columns
    finally:
        # cleanup
        Utils.drop_view(session, view_name)
        Utils.drop_table(session, table_name)


@sql_count_checker(query_count=6)
def test_to_view_multiindex(session, to_view):
    try:
        # create table
        table_name = Utils.random_table_name()
        session.create_dataframe(
            [BASIC_TYPE_DATA1, BASIC_TYPE_DATA2]
        ).write.save_as_table(table_name)

        # create dataframe with enforce_ordering disabled
        snow_dataframe = pd.read_snowflake(
            f"SELECT * FROM {table_name}", enforce_ordering=False
        )

        # make sure dataframe has a multi-index
        snow_dataframe = snow_dataframe.set_index(["_1", "_2"])

        # create series
        snow_series = snow_dataframe.iloc[:, 0]

        view_name = Utils.random_view_name()
        to_view(
            snow_series,
            name=view_name,
            index=True,
        )

        # verify columns
        actual = pd.read_snowflake(
            view_name,
            enforce_ordering=False,
        ).columns
        assert actual.tolist() == ["_1", "_2", "_3"]

    finally:
        # cleanup
        Utils.drop_view(session, view_name)
        Utils.drop_table(session, table_name)


@sql_count_checker(query_count=4)
def test_to_view_multiindex_length_mismatch_raises(session, to_view):
    try:
        # create table
        table_name = Utils.random_table_name()
        session.create_dataframe(
            [BASIC_TYPE_DATA1, BASIC_TYPE_DATA2]
        ).write.save_as_table(table_name)

        # create dataframe with enforce_ordering disabled
        snow_dataframe = pd.read_snowflake(
            f"SELECT * FROM {table_name}", enforce_ordering=False
        )

        # make sure dataframe has a multi-index
        snow_dataframe = snow_dataframe.set_index(["_1", "_2"])

        # create series
        snow_series = snow_dataframe.iloc[:, 0]

        view_name = Utils.random_view_name()
        with pytest.raises(
            ValueError,
            match="Length of 'index_label' should match number of levels",
        ):
            to_view(
                snow_series,
                name=view_name,
                index=True,
                index_label=["a"],
            )
    finally:
        # cleanup
        Utils.drop_view(session, view_name)
        Utils.drop_table(session, table_name)
