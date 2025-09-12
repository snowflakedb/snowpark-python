#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
from snowflake.snowpark.secrets import get_generic_secret_string, get_username_password
from snowflake.snowpark.types import BooleanType
from tests.utils import IS_NOT_ON_GITHUB, RUNNING_ON_JENKINS


@pytest.mark.skipif(
    IS_NOT_ON_GITHUB,
    reason="Secret API is only supported on Snowflake server environment",
)
@pytest.mark.skipif(
    not RUNNING_ON_JENKINS,
    reason="Secret API is only supported on Snowflake server environment",
)
def test_get_generic_secret_string_sproc(session, db_parameters):
    def get_secret_string():
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
    IS_NOT_ON_GITHUB,
    reason="Secret API is only supported on Snowflake server environment",
)
@pytest.mark.skipif(
    not RUNNING_ON_JENKINS,
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
        result = get_secret_string_udf()
        assert result
    except KeyError:
        pytest.skip("External Access Integration is not supported on the deployment.")


@pytest.mark.skipif(
    IS_NOT_ON_GITHUB,
    reason="Secret API is only supported on Snowflake server environment",
)
@pytest.mark.skipif(
    not RUNNING_ON_JENKINS,
    reason="Secret API is only supported on Snowflake server environment",
)
def test_get_username_password_udf(session, db_parameters):
    def get_secret_username_password():
        object = get_username_password("cred")
        if (
            object["username"] == "replace-with-your-username"
            and object["password"] == "replace-with-your-password"
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
        result = get_secret_udf()
        assert result
    except KeyError:
        pytest.skip("External Access Integration is not supported on the deployment.")
