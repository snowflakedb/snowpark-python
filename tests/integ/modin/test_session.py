#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
from typing import Optional
import warnings

import modin.pandas as pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.connector.constants import CONFIG_FILE, CONNECTIONS_FILE
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSessionException
from snowflake.snowpark.session import (
    _add_session,
    _get_active_sessions,
    _remove_session,
)
from tests.integ.modin.utils import (
    create_test_dfs,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker
from tests.utils import IS_IN_STORED_PROC, running_on_jenkins, running_on_public_ci

NO_ACTIVE_SESSION_ERROR_PATTERN = (
    r"Snowpark pandas requires an active snowpark session, but there is none. "
    + r"Please create one by following the instructions here: "
)
MULTIPLE_ACTIVE_SESSIONS_ERROR_PATTERN = (
    r"There are multiple active snowpark sessions, but you need to choose one "
    + r"for Snowpark pandas. Please assign one to Snowpark pandas with a "
    + r"statement like `modin.pandas.session = session`."
)


class SetPandasSession:
    """
    Context manager that sets the pandas sesion on enter, and resets it on exit.

    Attributes:
        _new_session: An Optional[Session] new pandas session
        _old_session: The Optional[Session] old pandas session.
    """

    def __init__(self, new_session: Optional[Session]) -> "SetPandasSession":
        self._new_session = new_session

    def __enter__(self) -> None:
        """Save the old pandas session and set the session to the new one."""
        try:
            self._old_session = pd.session
        except SnowparkSessionException:
            # Accessing pd.session may raise an error if it hasn't been assigned a
            # value yet and there is no unique active snowpark session. In that case,
            # use None as a sentinel value for missing session. We can use None
            # as a sentinel value because pd.session is never None, even if there
            # is no active Snowpark session.
            self._old_session = None
        pd.session = self._new_session

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Restore the old pands session, if it existed."""
        if self._old_session is not None:
            pd.session = self._old_session
        return None


@pytest.fixture(
    # run this fixture before every test case in this module so that global
    # session state doesn't affect these tests.
    autouse=True
)
def remove_all_active_sessions():
    """Remove all active Snowpark sessions, then restore them when the test finishes."""
    try:
        sessions = list(_get_active_sessions())
    except SnowparkSessionException:
        # we get a SnowparkSessionException if there are no active sessions.
        sessions = []
    for session in sessions:
        _remove_session(session)
    yield
    for session in sessions:
        _add_session(session)


@pytest.fixture(
    # run this fixture before every test case in this module so that global
    # pd.session state doesn't affect these tests.
    autouse=True
)
def clear_pandas_session():
    """Clear the Snowpark pandas session, if it exists, then reset it when the test finishes."""
    with SetPandasSession(None):
        yield


@pytest.fixture
def new_session(db_parameters):
    new_session = Session.builder.configs(db_parameters).create()
    yield new_session
    new_session.close()


# Skip the SQL counter checks for tests in this module.

# SqlCounter will get or create a session and count the queries that that session
# executes. Some of these tests need to start with the session removed, so they
# can't use SqlCounter. Some of the tests use multiple sessions, so the query
# counts from SqlCounter aren't meaningful. These tests are about session
# management, so we don't care about the SQL query counts. We require integration
# tests to have run a SqlCounter of some sort, so each test case will create a
# SQL counter with no_check=True.


@sql_count_checker(no_check=True)
def test_snowpark_pandas_session_is_global_session(new_session):
    assert new_session is pd.session


@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="SHOW PARAMETERS statement is not supported in stored proc environment",
)
@sql_count_checker(no_check=True)
def test_warning_if_quoted_identifiers_ignore_case_is_set(new_session):
    assert new_session is pd.session
    pd.session.sql("ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = True").collect()
    warning_msg = "Snowflake parameter 'QUOTED_IDENTIFIERS_IGNORE_CASE' is set to True"
    with warnings.catch_warnings(record=True) as w:
        warnings.filterwarnings("always")
        pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        assert len(w) == 1
        assert warning_msg in str(w[-1].message)


@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="SHOW PARAMETERS statement is not supported in stored proc environment",
)
@sql_count_checker(no_check=True)
def test_no_warning_if_quoted_identifiers_ignore_case_is_unset(new_session):
    assert new_session is pd.session
    pd.session.sql("ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = False").collect()
    with warnings.catch_warnings(record=True) as w:
        warnings.filterwarnings("always")
        pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        assert len(w) == 0


@sql_count_checker(no_check=True)
def test_cannot_create_dataframe_without_session():
    with pytest.raises(SnowparkSessionException, match=NO_ACTIVE_SESSION_ERROR_PATTERN):
        pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})


@sql_count_checker(no_check=True)
def test_cannot_access_pandas_session_without_session():
    with pytest.raises(SnowparkSessionException, match=NO_ACTIVE_SESSION_ERROR_PATTERN):
        pd.session


@sql_count_checker(no_check=True)
def test_cannot_access_session_attribute_without_active_session():
    with pytest.raises(SnowparkSessionException, match=NO_ACTIVE_SESSION_ERROR_PATTERN):
        pd.session


@pytest.mark.skipif(
    running_on_public_ci() or running_on_jenkins(),
    reason="cannot create config file on CI due to permission issue. "
    "TODO SNOW-918497: investigate the failure and enable it in jenkins",
)
@sql_count_checker(no_check=True)
def test_automatically_create_session_from_config_file(db_parameters):
    if os.path.exists(CONNECTIONS_FILE) or os.path.exists(CONFIG_FILE):
        pytest.fail("Please remove the existing config file to run this test")

    try:
        os.makedirs(os.path.dirname(CONNECTIONS_FILE), exist_ok=True)
        with open(CONNECTIONS_FILE, "w") as f:
            f.write('default_connection_name = "default"\n\n')
            f.write("[default]\n")
            for k, v in db_parameters.items():
                f.write(f'{k} = "{v}"\n')
        with pytest.raises(
            SnowparkSessionException, match=NO_ACTIVE_SESSION_ERROR_PATTERN
        ):
            pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    finally:
        try:
            os.remove(CONNECTIONS_FILE)
        except FileNotFoundError:
            pass


@sql_count_checker(no_check=True)
def test_automatically_create_session_from_config_env_var(db_parameters, monkeypatch):
    if os.path.exists(CONNECTIONS_FILE) or os.path.exists(CONFIG_FILE):
        pytest.fail("Please remove the existing config file to run this test")

    import tomlkit

    doc = tomlkit.document()
    default_con = tomlkit.table()
    for k, v in db_parameters.items():
        default_con[k] = v
    doc["default"] = default_con
    with monkeypatch.context() as m:
        m.setenv("SNOWFLAKE_CONNECTIONS", tomlkit.dumps(doc))
        m.setenv("SNOWFLAKE_DEFAULT_CONNECTION_NAME", "default")
        with pytest.raises(
            SnowparkSessionException, match=NO_ACTIVE_SESSION_ERROR_PATTERN
        ):
            pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})


@sql_count_checker(no_check=True)
def test_multiple_session(db_parameters, new_session):
    _ = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    assert pd.session is new_session

    with Session.builder.configs(db_parameters).create() as second_new_session:
        for curr_session in [second_new_session, new_session]:
            pd.session = curr_session
            df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
            assert len(df) == 3
            assert (
                df._query_compiler._modin_frame.ordered_dataframe.session
                is curr_session
            )

        # when unset, it will not work
        pd.session = None
        with pytest.raises(
            SnowparkSessionException, match=MULTIPLE_ACTIVE_SESSIONS_ERROR_PATTERN
        ):
            pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})


@sql_count_checker(no_check=True)
def test_snowpark_pandas_session_class_does_not_exist_snow_1022098():
    with pytest.raises(AttributeError):
        pd.Session


@pytest.mark.parametrize(
    "operation",
    [
        lambda df: df.apply(lambda x: x + 1, axis=1),
        lambda df: df[0].apply(lambda x: x + 1),
        lambda df: df.groupby(0).apply(lambda x: x + 1),
    ],
)
class TestApplyLikeMethodsWithMultipleSessionsSnow1216902:
    """
    Test some cases from SNOW-1216902

    Apply-like methods were trying to find and use the unique active Snowpark
    session instead of using the session that the dataframe was already using.
    """

    @sql_count_checker(no_check=True)
    def test_with_pandas_session(self, new_session, db_parameters, operation):
        # in the beginning, new_session is the only active session.
        # create a Snowpark dataframe using that session.
        dfs = create_test_dfs([1])
        # Then, create a new session. Note that even though we don't assign
        # this session to a variable, it stays in the global variable
        # containing all active sessions.
        with Session.builder.configs(db_parameters).create():
            # Even though there are multiple sessions, dfs[0] is associated
            # with just one of them, and we'll use that one to perform
            # `operation`.
            eval_snowpark_pandas_result(*dfs, operation)
        # The extra session's context manager will close it on exit.

    @sql_count_checker(no_check=True)
    def test_with_no_pandas_session(self, new_session, db_parameters, operation):
        # in the beginning, new_session is the only active session.
        # create a Snowpark dataframe using that session.
        dfs = create_test_dfs([1])
        # Enter the following context managers in the order listed:
        # 1) Create a new session with Session.builder.configs().create(). Note that even
        #    though we don't assign this session to a variable, it stays in the global variable
        #    containing all active sessions.
        # 2) Set pd.session to None.
        # Since we have already created the dataframe with the original session, we
        # can call apply() on it and use that session to execute the query.
        with Session.builder.configs(db_parameters).create(), SetPandasSession(None):
            eval_snowpark_pandas_result(*dfs, operation)
        # We then exit the managers in the opposite order:
        # 1) Reset pd.session to the original session
        # 2) Close the extra session that we created in 1)
