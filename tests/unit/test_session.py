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

try:
    import pandas

    is_pandas_available = True
except ImportError:
    is_pandas_available = False

from snowflake.snowpark import Session
from snowflake.snowpark._internal.server_connection import ServerConnection
from snowflake.snowpark._internal.utils import parse_table_name
from snowflake.snowpark.exceptions import (
    SnowparkInvalidObjectNameException,
    SnowparkSessionException,
)
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

    fake_connection._get_client_side_session_parameter = (
        lambda x, y: ServerConnection._get_client_side_session_parameter(
            fake_connection, x, y
        )
    )

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
        SnowparkSessionException,
        match=f"Failed to close this session. The error is: {exception_msg}",
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
        RuntimeError,
        match="Cannot add package random_package_name because Anaconda terms must be accepted by ORGADMIN to use "
        "Anaconda 3rd party packages. Please follow the instructions at "
        "https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html"
        "#using-third-party-packages-from-anaconda.",
    ):
        session._resolve_packages(
            ["random_package_name"], validate_package=True, include_pandas=False
        )


def test_infer_is_return_table():
    fake_connection = mock.create_autospec(ServerConnection)
    fake_connection._conn = mock.Mock()
    session = Session(fake_connection)

    def run_query(sql: str, log_on_exception=None):
        if "TABLE" in sql:
            return [[], [[], "TABLE"]]
        return [[], [[], "STRING"]]

    session._run_query = MagicMock(name="session._run_query")
    session._run_query.side_effect = run_query

    assert Session._infer_is_return_table(session, "system$send_email") is False
    assert (
        Session._infer_is_return_table(session, "  ASSOCIATE_SEMANTIC_CATEGORY_TAGS")
        is False
    )
    assert Session._infer_is_return_table(session, "Store_CLAssification") is False
    assert Session._infer_is_return_table(session, "my_sproc") is False
    assert Session._infer_is_return_table(session, "my_table_sproc") is True


@pytest.mark.skipif(not is_pandas_available, reason="requires pandas for write_pandas")
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


def test_parse_table_name():
    # test no double quotes
    assert parse_table_name("a") == ["a"]
    assert parse_table_name("a.b") == ["a", "b"]
    assert parse_table_name("a.b.c") == ["a", "b", "c"]
    assert parse_table_name("_12$opOW") == ["_12$opOW"]
    assert parse_table_name("qwE123.z$xC") == ["qwE123", "z$xC"]
    assert parse_table_name("Wo_89$.d9$dC.z_1Z$") == ["Wo_89$", "d9$dC", "z_1Z$"]

    # test double quotes
    assert parse_table_name('"a"') == ['"a"']
    assert parse_table_name('"a.b"') == ['"a.b"']
    assert parse_table_name('"a..b"') == ['"a..b"']
    assert parse_table_name('"a.b".b.c') == ['"a.b"', "b", "c"]
    assert parse_table_name('"a.b"."b.c"') == ['"a.b"', '"b.c"']
    assert parse_table_name('"a.b"."b".c') == ['"a.b"', '"b"', "c"]
    assert parse_table_name('"a.b"."b.b"."c.c"') == ['"a.b"', '"b.b"', '"c.c"']

    assert parse_table_name('"@#$!23XM"') == ['"@#$!23XM"']
    assert parse_table_name('"@#$!23XM._!Mcs"') == ['"@#$!23XM._!Mcs"']
    assert parse_table_name('"@#$!23XM.._!Mcs"') == ['"@#$!23XM.._!Mcs"']
    assert parse_table_name('"@#$!23XM._!Mcs".qwE123.z$xC') == [
        '"@#$!23XM._!Mcs"',
        "qwE123",
        "z$xC",
    ]
    assert parse_table_name('"@#$!23XM._!Mcs".".39Qw$5.c"') == [
        '"@#$!23XM._!Mcs"',
        '".39Qw$5.c"',
    ]
    assert parse_table_name('"@#$!23XM._!Mcs".".39Qw$5.c".z$xC') == [
        '"@#$!23XM._!Mcs"',
        '".39Qw$5.c"',
        "z$xC",
    ]
    assert parse_table_name('"@#$!23XM._!Mcs".".39Qw$5.c"."2^.z$xC"') == [
        '"@#$!23XM._!Mcs"',
        '".39Qw$5.c"',
        '"2^.z$xC"',
    ]

    # test escape double quotes
    assert parse_table_name('"""a.""b"."b.c"') == ['"""a.""b"', '"b.c"']
    assert parse_table_name('"""a.""b"."b.c".d') == ['"""a.""b"', '"b.c"', "d"]
    assert parse_table_name('"""a.""b"."b.c"."d"""""') == [
        '"""a.""b"',
        '"b.c"',
        '"d"""""',
    ]
    assert parse_table_name('"""@#$!23XM._!Mcs""b.39Qw$5.c"."2^.z$xC""%cx_.z"') == [
        '"""@#$!23XM._!Mcs""b.39Qw$5.c"',
        '"2^.z$xC""%cx_.z"',
    ]
    assert parse_table_name(
        '"""@#$!23XM._!Mcs""b.39Qw$5.c"."2^.z$xC""%cx_.z".z$xC'
    ) == ['"""@#$!23XM._!Mcs""b.39Qw$5.c"', '"2^.z$xC""%cx_.z"', "z$xC"]
    assert parse_table_name(
        '"""@#$!23XM._!Mcs""b.39Qw$5.c"."2^.z$xC""%cx_.z"."_12$D""""""d"""""'
    ) == [
        '"""@#$!23XM._!Mcs""b.39Qw$5.c"',
        '"2^.z$xC""%cx_.z"',
        '"_12$D""""""d"""""',
    ]

    # test no identifier for schema
    assert parse_table_name("a..b") == ["a", "", "b"]
    assert parse_table_name('"a.b"..b') == ['"a.b"', "", "b"]
    assert parse_table_name('"a.b".."b.b"') == ['"a.b"', "", '"b.b"']

    assert parse_table_name("d9$dC..z$xC") == ["d9$dC", "", "z$xC"]
    assert parse_table_name('"""@#$!23XM._!Mcs""b.39Qw$5.c"..z$xC') == [
        '"""@#$!23XM._!Mcs""b.39Qw$5.c"',
        "",
        "z$xC",
    ]
    assert parse_table_name('"""@#$!23XM._!Mcs""b.39Qw$5.c".."_12$D""""""d"""""') == [
        '"""@#$!23XM._!Mcs""b.39Qw$5.c"',
        "",
        '"_12$D""""""d"""""',
    ]

    # negative cases
    with pytest.raises(SnowparkInvalidObjectNameException):
        assert parse_table_name("12~3")  # ~ unsupported in unquoted id
    with pytest.raises(SnowparkInvalidObjectNameException):
        assert parse_table_name("123")  # can not start with num in unquoted id
    with pytest.raises(SnowparkInvalidObjectNameException):
        assert parse_table_name("$dab")  # can not start with $ in unquoted id
    with pytest.raises(SnowparkInvalidObjectNameException):
        assert parse_table_name("")  # empty not allowed in unquoted id
    with pytest.raises(SnowparkInvalidObjectNameException):
        assert parse_table_name("   ")  # space not allowed in unquoted id
    with pytest.raises(SnowparkInvalidObjectNameException):
        assert parse_table_name("a...b")  # unsupported semantic
    with pytest.raises(SnowparkInvalidObjectNameException):
        assert parse_table_name("a.b.")  # unsupported semantic
    with pytest.raises(SnowparkInvalidObjectNameException):
        assert parse_table_name(".b.")  # unsupported semantic
    with pytest.raises(SnowparkInvalidObjectNameException):
        assert parse_table_name("a.b.c.d")  # 4 unquoted ids
    with pytest.raises(SnowparkInvalidObjectNameException):
        assert parse_table_name('"a"."b"."c"."d"')  # 4 quoted ids
    with pytest.raises(SnowparkInvalidObjectNameException):
        assert parse_table_name('"abc"abc')  # id after ending quotes
    with pytest.raises(SnowparkInvalidObjectNameException):
        assert parse_table_name('"abc""abc')  # no ending quotes
    with pytest.raises(SnowparkInvalidObjectNameException):
        assert parse_table_name('&*%."abc"')  # unsupported chars in unquoted ids
    with pytest.raises(SnowparkInvalidObjectNameException):
        assert parse_table_name('"abc"."abc')  # missing double quotes in the end
    with pytest.raises(SnowparkInvalidObjectNameException):
        assert parse_table_name('"abc".!123~#')  # unsupported chars in unquoted ids
    with pytest.raises(SnowparkInvalidObjectNameException):
        assert parse_table_name('*&^."abc".abc')  # unsupported chars in unquoted ids
    with pytest.raises(SnowparkInvalidObjectNameException):
        assert parse_table_name('."abc".')  # unsupported semantic


def test_session_id():
    fake_server_connection = mock.create_autospec(ServerConnection)
    fake_server_connection.get_session_id = mock.Mock(return_value=123456)
    session = Session(fake_server_connection)

    assert session.session_id == 123456


def test_connection():
    fake_snowflake_connection = mock.create_autospec(SnowflakeConnection)
    fake_snowflake_connection._telemetry = mock.Mock()
    fake_snowflake_connection._session_parameters = mock.Mock()
    fake_snowflake_connection.is_closed = mock.Mock(return_value=False)
    fake_options = {"": ""}
    server_connection = ServerConnection(fake_options, fake_snowflake_connection)
    session = Session(server_connection)

    assert session.connection == session._conn._conn
    assert session.connection == fake_snowflake_connection
