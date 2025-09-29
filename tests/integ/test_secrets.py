#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
from snowflake.snowpark.secrets import (
    get_generic_secret_string,
    get_username_password,
    get_secret_type,
    get_cloud_provider_token,
    get_oauth_access_token,
)
from snowflake.snowpark.types import BooleanType, StringType
from tests.utils import IS_NOT_ON_GITHUB, RUNNING_ON_JENKINS, IS_IN_STORED_PROC, Utils
from snowflake.snowpark import Row
from snowflake.snowpark.exceptions import SnowparkSQLException


@pytest.mark.skipif(
    IS_NOT_ON_GITHUB or not RUNNING_ON_JENKINS,
    reason="Secret API is only supported on Snowflake server environment",
)
def test_get_generic_secret_string_sproc(session, db_parameters):
    def get_secret_string(session_):
        if get_generic_secret_string("cred") == "replace-with-your-api-key":
            return True
        return False

    try:
        get_secret_string_sp = session.sproc.register(
            get_secret_string,
            return_type=BooleanType(),
            packages=["snowflake-snowpark-python"],
            external_access_integrations=[
                db_parameters["external_access_integration1"]
            ],
            secrets={"cred": f"{db_parameters['external_access_key1']}"},
        )
        result = get_secret_string_sp()
        assert result
    except KeyError:
        pytest.skip("External Access Integration is not supported on the deployment.")


@pytest.mark.skipif(
    IS_NOT_ON_GITHUB or not RUNNING_ON_JENKINS,
    reason="Secret API is only supported on Snowflake server environment",
)
def test_get_generic_secret_string_udf(session, db_parameters):
    def get_secret_string():
        if get_generic_secret_string("cred") == "replace-with-your-api-key_2":
            return True
        return False

    try:
        get_secret_string_udf = session.udf.register(
            get_secret_string,
            return_type=BooleanType(),
            packages=["snowflake-snowpark-python"],
            external_access_integrations=[
                db_parameters["external_access_integration2"]
            ],
            secrets={"cred": f"{db_parameters['external_access_key2']}"},
        )
        df = session.create_dataframe([[1], [2]]).to_df("x")
        Utils.check_answer(df.select(get_secret_string_udf()), [Row(True), Row(True)])
    except KeyError:
        pytest.skip("External Access Integration is not supported on the deployment.")


@pytest.mark.skipif(
    IS_NOT_ON_GITHUB or not RUNNING_ON_JENKINS,
    reason="Secret API is only supported on Snowflake server environment",
)
def test_get_username_password_udf(session, db_parameters):
    def get_secret_username_password():
        creds = get_username_password("cred")
        if (
            creds.username == "replace-with-your-username"
            and creds.password == "replace-with-your-password"
        ):
            return True
        return False

    try:
        get_secret_udf = session.udf.register(
            get_secret_username_password,
            return_type=BooleanType(),
            packages=["snowflake-snowpark-python"],
            external_access_integrations=[
                db_parameters["external_access_integration2"]
            ],
            secrets={"cred": f"{db_parameters['external_access_key2']}"},
        )
        df = session.create_dataframe([[1], [2]]).to_df("x")
        Utils.check_answer(df.select(get_secret_udf()), [Row(True), Row(True)])
    except KeyError:
        pytest.skip("External Access Integration is not supported on the deployment.")


@pytest.mark.skipif(
    IS_NOT_ON_GITHUB or not RUNNING_ON_JENKINS,
    reason="Secret API is only supported on Snowflake server environment",
)
def test_get_secret_type_sproc(session, db_parameters):
    def get_type(session_):
        t_str = get_secret_type("cred_str")
        t_pwd = get_secret_type("cred_pwd")
        return f"{t_str},{t_pwd}"

    try:
        get_type_sp = session.sproc.register(
            get_type,
            return_type=StringType(),
            packages=["snowflake-snowpark-python"],
            external_access_integrations=[
                db_parameters["external_access_integration1"],
                db_parameters["external_access_integration3"],
            ],
            secrets={
                "cred_str": f"{db_parameters['external_access_key1']}",
                "cred_pwd": f"{db_parameters['external_access_key3']}",
            },
            anonymous=True,
        )
        result = get_type_sp()
        parts = result.split(",")
        assert parts[0] == "GENERIC_STRING"
        assert parts[1] == "PASSWORD"
    except KeyError:
        pytest.skip("External Access Integration is not supported on the deployment.")


@pytest.mark.skipif(
    IS_NOT_ON_GITHUB or not RUNNING_ON_JENKINS,
    reason="Secret API is only supported on Snowflake server environment",
)
def test_get_nonexistent_secret(session, db_parameters):
    def get_secret():
        get_generic_secret_string("cred")
        return False

    with pytest.raises(SnowparkSQLException):
        session.udf.register(
            get_secret,
            return_type=BooleanType(),
            packages=["snowflake-snowpark-python"],
            external_access_integrations=[
                db_parameters["external_access_integration2"]
            ],
            secrets={"cred": "nonexistent_secret"},
        )


@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="Run only outside Snowflake server to validate NotImplementedError",
)
def test_secrets_import_error():
    # [SNOW-2324796] Phase 1 relies on Snowflake server environment
    # Remove this test once secrets local development support is complete
    with pytest.raises(NotImplementedError):
        get_generic_secret_string("s1")
    with pytest.raises(NotImplementedError):
        get_username_password("p1")
    with pytest.raises(NotImplementedError):
        get_secret_type("t1")
    with pytest.raises(NotImplementedError):
        get_cloud_provider_token("c1")
    with pytest.raises(NotImplementedError):
        get_oauth_access_token("o1")
