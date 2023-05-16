#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import json
import os
from typing import Optional
from unittest import mock
from unittest.mock import MagicMock

import pytest

import snowflake.snowpark.session
from snowflake.connector import ProgrammingError, SnowflakeConnection
from snowflake.connector.options import pandas
from snowflake.snowpark import Session
from snowflake.snowpark._internal.server_connection import ServerConnection
from snowflake.snowpark.exceptions import SnowparkInvalidObjectNameException
from snowflake.snowpark.session import _PYTHON_SNOWPARK_USE_SCOPED_TEMP_OBJECTS_STRING
from snowflake.snowpark.types import StructField, StructType


def test_aliases():
    assert Session.createDataFrame == Session.create_dataframe


@pytest.mark.parametrize(
    "account, role,database,schema,warehouse",
    [("ACCOUNT", "ADMIN", "DB", "SCHEMA", "WH"), (None, None, None, None, None)],
)
def test_str(account, role, database, schema, warehouse):
    mock_sf_connection = mock.create_autospec(
        SnowflakeConnection,
        account=account,
        role=role,
        database=database,
        schema=schema,
        warehouse=warehouse,
        _telemetry=None,
        _session_parameters={},
    )
    mock_sf_connection.is_closed.return_value = False
    mock_server_connection = ServerConnection({}, mock_sf_connection)

    def quoted(s):
        return f'"{s}"' if s else s

    assert (
        str(Session(mock_server_connection))
        == f"<snowflake.snowpark.session.Session: account={quoted(account)}, role={quoted(role)}, database={quoted(database)}, schema={quoted(schema)}, warehouse={quoted(warehouse)}>"
    )


def test_used_scoped_temp_object():
    fake_connection = mock.create_autospec(ServerConnection)
    fake_connection._conn = mock.Mock()

    # by default module level config is on
    fake_connection._conn._session_parameters = None
    assert Session(fake_connection)._use_scoped_temp_objects is True

    fake_connection._conn._session_parameters = {}
    assert Session(fake_connection)._use_scoped_temp_objects is True

    fake_connection._conn._session_parameters = {
        _PYTHON_SNOWPARK_USE_SCOPED_TEMP_OBJECTS_STRING: True
    }
    assert Session(fake_connection)._use_scoped_temp_objects is True

    fake_connection._conn._session_parameters = {
        _PYTHON_SNOWPARK_USE_SCOPED_TEMP_OBJECTS_STRING: False
    }
    assert Session(fake_connection)._use_scoped_temp_objects is False

    # turn off module level config
    snowflake.snowpark.session._use_scoped_temp_objects = False

    fake_connection._conn._session_parameters = {}
    assert Session(fake_connection)._use_scoped_temp_objects is False

    fake_connection._conn._session_parameters = {
        _PYTHON_SNOWPARK_USE_SCOPED_TEMP_OBJECTS_STRING: True
    }
    assert Session(fake_connection)._use_scoped_temp_objects is False

    fake_connection._conn._session_parameters = {
        _PYTHON_SNOWPARK_USE_SCOPED_TEMP_OBJECTS_STRING: False
    }
    assert Session(fake_connection)._use_scoped_temp_objects is False


def test_close_exception():
    fake_connection = mock.create_autospec(ServerConnection)
    fake_connection._conn = mock.Mock()
    fake_connection.is_closed = MagicMock(return_value=False)
    exception_msg = "Mock exception for session.cancel_all"
    fake_connection.run_query = MagicMock(side_effect=Exception(exception_msg))
    with pytest.raises(
        Exception, match=f"Failed to close this session. The error is: {exception_msg}"
    ):
        session = Session(fake_connection)
        session.close()


def test_resolve_import_path_ignore_import_path(tmp_path_factory):
    fake_connection = mock.create_autospec(ServerConnection)
    fake_connection._conn = mock.Mock()
    session = Session(fake_connection)

    tmp_path = tmp_path_factory.mktemp("session_test")
    a_temp_file = tmp_path / "file.txt"
    a_temp_file.write_text("any text is good")
    try:
        # import_path is ignored because file is not a .py file
        abs_path, _, leading_path = session._resolve_import_path(
            str(a_temp_file), import_path="a.b"
        )
        assert abs_path == str(a_temp_file)
        assert leading_path is None
    finally:
        os.remove(a_temp_file)


@pytest.mark.parametrize("has_current_database", (True, False))
def test_resolve_package_current_database(has_current_database):
    def mock_get_current_parameter(param: str, quoted: bool = True) -> Optional[str]:
        return "db" if has_current_database else None

    def mock_get_information_schema_packages(table_name: str):
        if has_current_database:
            assert table_name == "information_schema.packages"
        else:
            assert table_name == "snowflake.information_schema.packages"

        result = MagicMock()
        result.filter().group_by().agg()._internal_collect_with_tag.return_value = [
            ("random_package_name", json.dumps(["1.0.0"]))
        ]
        return result

    fake_connection = mock.create_autospec(ServerConnection)
    fake_connection._conn = mock.Mock()
    fake_connection._get_current_parameter = mock_get_current_parameter
    session = Session(fake_connection)
    session.table = MagicMock(name="session.table")
    session.table.side_effect = mock_get_information_schema_packages

    session._resolve_packages(
        ["random_package_name"], validate_package=True, include_pandas=False
    )


def test_resolve_package_terms_not_accepted():
    fake_connection = mock.create_autospec(ServerConnection)
    fake_connection._conn = mock.Mock()
    session = Session(fake_connection)

    def get_information_schema_packages(table_name: str):
        if table_name == "information_schema.packages":
            result = MagicMock()
            result.filter().group_by().agg()._internal_collect_with_tag.return_value = (
                []
            )
            return result

    def run_query(sql: str):
        if sql == "select system$are_anaconda_terms_acknowledged()":
            return [[False]]

    session.table = MagicMock(name="session.table")
    session.table.side_effect = get_information_schema_packages
    session._run_query = MagicMock(name="session._run_query")
    session._run_query.side_effect = run_query
    with pytest.raises(
        ValueError,
        match="Cannot add package random_package_name because Anaconda terms must be accepted by ORGADMIN to use "
        "Anaconda 3rd party packages. Please follow the instructions at "
        "https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html"
        "#using-third-party-packages-from-anaconda.",
    ):
        session._resolve_packages(
            ["random_package_name"], validate_package=True, include_pandas=False
        )


def test_write_pandas_wrong_table_type():
    fake_connection = mock.create_autospec(ServerConnection)
    fake_connection._conn = mock.Mock()
    session = Session(fake_connection)
    with pytest.raises(ValueError, match="Unsupported table type."):
        session.write_pandas(
            mock.create_autospec(pandas.DataFrame), table_name="t", table_type="aaa"
        )


def test_create_dataframe_empty_schema():
    fake_connection = mock.create_autospec(ServerConnection)
    fake_connection._conn = mock.Mock()
    session = Session(fake_connection)
    with pytest.raises(
        ValueError,
        match="The provided schema or inferred schema cannot be None or empty",
    ):
        session.create_dataframe([[1]], schema=StructType([]))


def test_create_dataframe_wrong_type():
    fake_connection = mock.create_autospec(ServerConnection)
    fake_connection._conn = mock.Mock()
    session = Session(fake_connection)
    with pytest.raises(
        TypeError, match=r"Cannot cast <class 'int'>\(1\) to <class 'str'>."
    ):
        session.create_dataframe([[1]], schema=StructType([StructField("a", str)]))


def test_table_exists_invalid_table_name():
    fake_connection = mock.create_autospec(ServerConnection)
    fake_connection._conn = mock.Mock()
    session = Session(fake_connection)
    with pytest.raises(
        SnowparkInvalidObjectNameException,
        match="The object name 'a.b.c.d' is invalid.",
    ):
        session._table_exists(["a", "b", "c", "d"])


def test_explain_query_error():
    fake_connection = mock.create_autospec(ServerConnection)
    fake_connection._conn = mock.Mock()
    session = Session(fake_connection)
    session._run_query = MagicMock()
    session._run_query.side_effect = ProgrammingError("Can't explain.")
    assert session._explain_query("select 1") is None
