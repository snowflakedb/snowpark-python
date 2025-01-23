#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import os
from typing import Dict

import pytest

import snowflake.connector
from snowflake.snowpark import Session
from snowflake.snowpark._internal.utils import set_ast_state, AstFlagSource
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.mock._connection import MockServerConnection
from tests.ast.ast_test_utils import (
    close_full_ast_validation_mode,
    setup_full_ast_validation_mode,
)
from tests.parameters import CONNECTION_PARAMETERS
from tests.utils import (
    TEST_SCHEMA,
    TestFiles,
    Utils,
    running_on_jenkins,
    running_on_public_ci,
)

RUNNING_ON_GH = os.getenv("GITHUB_ACTIONS") == "true"
RUNNING_ON_JENKINS = "JENKINS_HOME" in os.environ

test_dir = os.path.dirname(__file__)
test_data_dir = os.path.join(test_dir, "cassettes")

SNOWFLAKE_CREDENTIAL_HEADER_FIELDS = [
    "Authorization",
    "x-amz-server-side-encryption-customer-key-MD5",
    "x-amz-server-side-encryption-customer-key-md5",
    "x-amz-server-side-encryption-customer-key",
    "x-amz-server-side-encryption-customer-algorithm",
    "x-amz-id-2",
    "x-amz-request-id",
    "x-amz-version-id",
]


def print_help() -> None:
    print(
        """Connection parameter must be specified in parameters.py,
    for example:
CONNECTION_PARAMETERS = {
    'account': 'testaccount',
    'user': 'user1',
    'password': 'test',
    'database': 'testdb',
    'schema': 'public',
}
"""
    )


def set_up_external_access_integration_resources(
    session, rule1, rule2, key1, key2, integration1, integration2
):
    try:
        # IMPORTANT SETUP NOTES: the test role needs to be granted the creation privilege
        # log into the admin account and run the following sql to grant the privilege
        # GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE <test_role>;
        # prepare external access resource
        session.sql(
            f"""
    CREATE OR REPLACE NETWORK RULE {rule1}
      MODE = EGRESS
      TYPE = HOST_PORT
      VALUE_LIST = ('www.google.com');
    """
        ).collect()
        session.sql(
            f"""
    CREATE OR REPLACE NETWORK RULE {rule2}
      MODE = EGRESS
      TYPE = HOST_PORT
      VALUE_LIST = ('www.microsoft.com');
    """
        ).collect()
        session.sql(
            f"""
    CREATE OR REPLACE SECRET {key1}
      TYPE = GENERIC_STRING
      SECRET_STRING = 'replace-with-your-api-key';
    """
        ).collect()
        session.sql(
            f"""
    CREATE OR REPLACE SECRET {key2}
      TYPE = GENERIC_STRING
      SECRET_STRING = 'replace-with-your-api-key_2';
    """
        ).collect()
        session.sql(
            f"""
    CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION {integration1}
      ALLOWED_NETWORK_RULES = ({rule1})
      ALLOWED_AUTHENTICATION_SECRETS = ({key1})
      ENABLED = true;
    """
        ).collect()
        session.sql(
            f"""
    CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION {integration2}
      ALLOWED_NETWORK_RULES = ({rule2})
      ALLOWED_AUTHENTICATION_SECRETS = ({key2})
      ENABLED = true;
    """
        ).collect()
        CONNECTION_PARAMETERS["external_access_rule1"] = rule1
        CONNECTION_PARAMETERS["external_access_rule2"] = rule2
        CONNECTION_PARAMETERS["external_access_key1"] = key1
        CONNECTION_PARAMETERS["external_access_key2"] = key2
        CONNECTION_PARAMETERS["external_access_integration1"] = integration1
        CONNECTION_PARAMETERS["external_access_integration2"] = integration2
    except SnowparkSQLException:
        # GCP currently does not support external access integration
        # we can remove the exception once the integration is available on GCP
        pass


def clean_up_external_access_integration_resources(
    session, rule1, rule2, key1, key2, integration1, integration2
):
    session.sql(f"drop network rule if exists {rule1}").collect()
    session.sql(f"drop network rule if exists {rule2}").collect()
    session.sql(f"drop secret if exists {key1}").collect()
    session.sql(f"drop secret if exists {key2}").collect()
    session.sql(f"drop integration if exists {integration1}").collect()
    session.sql(f"drop integration if exists {integration2}").collect()
    CONNECTION_PARAMETERS.pop("external_access_rule1", None)
    CONNECTION_PARAMETERS.pop("external_access_rule2", None)
    CONNECTION_PARAMETERS.pop("external_access_key1", None)
    CONNECTION_PARAMETERS.pop("external_access_key2", None)
    CONNECTION_PARAMETERS.pop("external_access_integration1", None)
    CONNECTION_PARAMETERS.pop("external_access_integration2", None)


@pytest.fixture(scope="session")
def db_parameters(local_testing_mode) -> Dict[str, str]:
    # If its running on our public CI or Jenkins, replace the schema
    if running_on_public_ci() or running_on_jenkins():
        # tests related to external access integration requires secrets, network rule to be created ahead
        # we keep the information of the existing schema to refer to those objects
        CONNECTION_PARAMETERS["schema_with_secret"], CONNECTION_PARAMETERS["schema"] = (
            CONNECTION_PARAMETERS["schema"],
            TEST_SCHEMA,
        )
    else:
        CONNECTION_PARAMETERS["schema_with_secret"] = CONNECTION_PARAMETERS["schema"]
    CONNECTION_PARAMETERS["local_testing"] = local_testing_mode
    return CONNECTION_PARAMETERS


@pytest.fixture(scope="session")
def resources_path() -> str:
    return os.path.normpath(os.path.join(os.path.dirname(__file__), "../resources"))


@pytest.fixture(scope="session")
def connection(db_parameters, local_testing_mode):
    if local_testing_mode:
        yield MockServerConnection(options={"disable_local_testing_telemetry": True})
    else:
        _keys = [
            "user",
            "password",
            "private_key_file",
            "host",
            "port",
            "database",
            "schema",
            "account",
            "protocol",
            "role",
            "warehouse",
        ]
        with snowflake.connector.connect(
            **{
                k: db_parameters[k]
                for k in _keys
                if k in db_parameters and db_parameters[k] is not None
            }
        ) as con:
            yield con


@pytest.fixture(scope="session")
def is_sample_data_available(connection, local_testing_mode) -> bool:
    if local_testing_mode:
        return False
    return (
        len(
            connection.cursor()
            .execute("show databases like 'SNOWFLAKE_SAMPLE_DATA'")
            .fetchall()
        )
        > 0
    )


@pytest.fixture(scope="session", autouse=True)
def test_schema(connection, local_testing_mode) -> None:
    """Set up and tear down the test schema. This is automatically called per test session."""
    if local_testing_mode:
        yield
    else:
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA}")
            # This is needed for test_get_schema_database_works_after_use_role in test_session_suite
            cursor.execute(
                f"GRANT ALL PRIVILEGES ON SCHEMA {TEST_SCHEMA} TO ROLE PUBLIC"
            )
            yield
            cursor.execute(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA}")


@pytest.fixture(scope="module")
def session(
    db_parameters,
    resources_path,
    sql_simplifier_enabled,
    local_testing_mode,
    cte_optimization_enabled,
    ast_enabled,
    validate_ast,
    unparser_jar,
):
    set_ast_state(AstFlagSource.TEST, ast_enabled)
    rule1 = f"rule1{Utils.random_alphanumeric_str(10)}"
    rule2 = f"rule2{Utils.random_alphanumeric_str(10)}"
    key1 = f"key1{Utils.random_alphanumeric_str(10)}"
    key2 = f"key2{Utils.random_alphanumeric_str(10)}"
    integration1 = f"integration1{Utils.random_alphanumeric_str(10)}"
    integration2 = f"integration2{Utils.random_alphanumeric_str(10)}"

    session = (
        Session.builder.configs(db_parameters)
        .config("local_testing", local_testing_mode)
        .create()
    )
    session.sql_simplifier_enabled = sql_simplifier_enabled
    session._cte_optimization_enabled = cte_optimization_enabled
    session.ast_enabled = ast_enabled

    if os.getenv("GITHUB_ACTIONS") == "true" and not local_testing_mode:
        set_up_external_access_integration_resources(
            session, rule1, rule2, key1, key2, integration1, integration2
        )

    if validate_ast:
        full_ast_validation_listener = setup_full_ast_validation_mode(
            session, db_parameters, unparser_jar
        )

    yield session

    if validate_ast:
        close_full_ast_validation_mode(full_ast_validation_listener)

    if os.getenv("GITHUB_ACTIONS") == "true" and not local_testing_mode:
        clean_up_external_access_integration_resources(
            session, rule1, rule2, key1, key2, integration1, integration2
        )
    session.close()


@pytest.fixture(scope="function")
def profiler_session(
    db_parameters,
    resources_path,
    sql_simplifier_enabled,
    local_testing_mode,
    cte_optimization_enabled,
):
    rule1 = f"rule1{Utils.random_alphanumeric_str(10)}"
    rule2 = f"rule2{Utils.random_alphanumeric_str(10)}"
    key1 = f"key1{Utils.random_alphanumeric_str(10)}"
    key2 = f"key2{Utils.random_alphanumeric_str(10)}"
    integration1 = f"integration1{Utils.random_alphanumeric_str(10)}"
    integration2 = f"integration2{Utils.random_alphanumeric_str(10)}"
    session = (
        Session.builder.configs(db_parameters)
        .config("local_testing", local_testing_mode)
        .create()
    )
    session.sql_simplifier_enabled = sql_simplifier_enabled
    session._cte_optimization_enabled = cte_optimization_enabled
    if os.getenv("GITHUB_ACTIONS") == "true" and not local_testing_mode:
        set_up_external_access_integration_resources(
            session, rule1, rule2, key1, key2, integration1, integration2
        )
    yield session
    if os.getenv("GITHUB_ACTIONS") == "true" and not local_testing_mode:
        clean_up_external_access_integration_resources(
            session, rule1, rule2, key1, key2, integration1, integration2
        )
    session.close()


@pytest.fixture(scope="function")
def temp_schema(connection, session, local_testing_mode) -> None:
    """Set up and tear down a temp schema for cross-schema test.
    This is automatically called per test module."""
    temp_schema_name = Utils.get_fully_qualified_temp_schema(session)
    if local_testing_mode:
        yield temp_schema_name
    else:
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {temp_schema_name}")
            # This is needed for test_get_schema_database_works_after_use_role in test_session_suite
            cursor.execute(
                f"GRANT ALL PRIVILEGES ON SCHEMA {temp_schema_name} TO ROLE PUBLIC"
            )
            yield temp_schema_name
            cursor.execute(f"DROP SCHEMA IF EXISTS {temp_schema_name}")


@pytest.fixture(scope="module")
def temp_stage(session, resources_path, local_testing_mode):
    tmp_stage_name = Utils.random_stage_name()
    test_files = TestFiles(resources_path)

    if not local_testing_mode:
        Utils.create_stage(session, tmp_stage_name, is_temporary=True)
    Utils.upload_to_stage(
        session, tmp_stage_name, test_files.test_file_parquet, compress=False
    )
    yield tmp_stage_name
    if not local_testing_mode:
        Utils.drop_stage(session, tmp_stage_name)


@pytest.fixture(scope="function", autouse=True)
def clear_session_ast_batch_on_validate_ast(session, validate_ast):
    """
    After each test, flush the AST batch so it does not pollute the next test validation.
    """
    yield
    if validate_ast:
        session._ast_batch.flush()
