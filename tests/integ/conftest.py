#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import gzip
import json
import linecache
import logging
import os
import uuid
from typing import Dict

import pytest
from pytest import fail

import _vendored.vcrpy as vcr
import snowflake.connector
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.mock._connection import MockServerConnection
from snowflake.snowpark.query_history import QueryListener, QueryRecord
from tests.parameters import CONNECTION_PARAMETERS
from tests.utils import Utils

RUNNING_ON_GH = os.getenv("GITHUB_ACTIONS") == "true"
RUNNING_ON_JENKINS = "JENKINS_HOME" in os.environ
TEST_SCHEMA = f"GH_JOB_{(str(uuid.uuid4()).replace('-', '_'))}"
if RUNNING_ON_JENKINS:
    TEST_SCHEMA = f"JENKINS_JOB_{(str(uuid.uuid4()).replace('-', '_'))}"


test_dir = os.path.dirname(__file__)
test_data_dir = os.path.join(test_dir, "cassettes")

SNOWFLAKE_CREDENTIAL_HEADER_FIELDS = [
    "Authorization",
    "x-amz-server-side-encryption-customer-key-MD5",
    "x-amz-server-side-encryption-customer-key-md5",
    "x-amz-server-side-encryption-customer-key",
    "x-amz-server-side-encryption-customer-algorithm",
    "x-amz-id-2",
    # "x-amz-request-id",
    # "x-amz-version-id",
]


def _process_request_recording(request):
    """Invoked before request is processed"""
    return request


def _process_response_recording(response):
    """Process response recording"""
    # Remove Snowflake credentials.
    for key in SNOWFLAKE_CREDENTIAL_HEADER_FIELDS:
        response["headers"].pop(key, None)

    return response


vcr.default_vcr = vcr.VCR(
    cassette_library_dir=test_data_dir,
    before_record_request=_process_request_recording,
    before_record_response=_process_response_recording,
    filter_headers=SNOWFLAKE_CREDENTIAL_HEADER_FIELDS,
    record_mode="all",
)

vcr.use_cassette = vcr.default_vcr.use_cassette


def running_on_public_ci() -> bool:
    """Whether or not tests are currently running on one of our public CIs."""
    return RUNNING_ON_GH


def running_on_jenkins() -> bool:
    """Whether or not tests are currently running on a Jenkins node."""
    return RUNNING_ON_JENKINS


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
            "host",
            "port",
            "database",
            "account",
            "protocol",
            "role",
        ]
        with snowflake.connector.connect(
            **{k: db_parameters[k] for k in _keys if k in db_parameters}
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


class TracebackHistory(QueryListener):
    def __init__(self) -> None:
        self.queries = []
        self.tracebacks = []
        self.request_ids = []

    def _notify(self, query_record: QueryRecord, *args, **kwargs) -> None:

        # get traceback
        import traceback

        formatted_lines = traceback.format_stack()

        # exclude all the wrapper code, i.e. start with files in snowflake/snowpark (or tests)
        idx = next(
            i
            for i, line in enumerate(formatted_lines)
            if "snowflake/snowpark" in line or "tests/" in line
        )
        formatted_lines = formatted_lines[idx:]

        # remove traceback related to _internal/server_connection.py
        idx = next(
            i
            for i, line in enumerate(formatted_lines)
            if "_internal/server_connection.py" in line
        )

        # cleanup traceback from telemetry, because there's no added value in displaying it.
        formatted_lines = formatted_lines[
            : idx - 1
        ]  # -1 to also exclude the call of session._conn.execute

        formatted_lines = [
            line for line in formatted_lines if "telemetry.py" not in line
        ]

        request_id = kwargs["requestId"]

        self.request_ids.append(request_id)
        self.queries.append(query_record)
        self.tracebacks.append(formatted_lines)


@pytest.fixture(autouse=True)
def check_ast_encode_invoked(request, session):
    # In code later the pytest request will be shadowed, save here.
    test_request = request

    # Store for each test a separate yaml file for inspection.
    test_file, test_line_no, test_name = test_request.node.location
    cassette_locator = test_file.replace("tests/", "").replace(".py", "")
    cassette_file_path = os.path.join(cassette_locator, test_name + ".yaml")

    # Can not extract tracebacks directly, therefore store them using query listener
    query_history = TracebackHistory()
    session._conn.add_query_listener(query_history)

    # Remove casette file if exists to prevent repeated execution problems with VCR.
    if os.path.isfile(cassette_file_path):
        os.remove(cassette_file_path)

    do_check = "modin" not in request.node.location[0]

    if not do_check:
        yield
    else:
        # Disable VCR logger here to be less verbose.
        logging.basicConfig()
        vcr_log = logging.getLogger("_vendored.vcrpy")
        vcr_log.setLevel(logging.WARNING)

        with vcr.use_cassette(cassette_file_path, match_on=[]) as tape:

            # Execute test by yielding
            yield

            # Match with query history.
            query_dict = {}
            for record, request_id, traceback in zip(
                query_history.queries,
                query_history.request_ids,
                query_history.tracebacks,
            ):
                query_dict[request_id] = {
                    "sfqid": record.query_id,
                    "sqlText": record.sql_text,
                    "traceback": traceback,
                }

            for request, response in zip(tape.requests, tape.responses):
                # Failed requests have no body, skip them here.
                if request.body and request.method in [
                    "POST",
                    "GET",
                ]:  # Skip PUT requests.
                    request_dict_body = json.loads(
                        gzip.decompress(request.body).decode("UTF-8")
                    )

                    request_id = dict(request.query).get("requestId")

                    # Some requests do not correspond to queries.
                    # Only log here requests that belong to queries.
                    if request_id and request_id in query_dict.keys():
                        query_dict[request_id]["request"] = request_dict_body
                        query_dict[request_id]["response"] = response

            # Check now for all queries whether "dataframeAst" is contained in request. If not, need to add AST to APIs.
            # Collect information on test/API to display to user in fail message.
            REST_AST_KEY = "dataframeAst"

            line_numbers_without_ast_request = set()

            for q in query_dict.values():
                if "request" in q.keys():
                    if REST_AST_KEY not in q["request"].keys():
                        # TODO: can show full traceback here.

                        # Should be single line b.c. of traceback uniqueness
                        line = [
                            line
                            for line in q["traceback"]
                            if test_file in line and test_name in line
                        ][0]
                        lineno = int(line.split(",")[1].replace("line", "").strip())

                        line_numbers_without_ast_request.add(lineno)
                else:
                    # TODO: can also get line number for this.
                    logging.error(f"No REST request found for Query sfqid={q['sfqid']}")

        session._conn.remove_query_listener(query_history)

        if (
            do_check
            # check only tests that passed.
            # and test_request.node.rep_call.passed
            and len(line_numbers_without_ast_request) > 0
        ):

            # Format lines together with content for user display
            line_numbers_without_ast_request = sorted(
                list(line_numbers_without_ast_request)
            )

            error_message = (
                "Following lines did not encode AST as part of the REST request:\n"
            )

            for line_no in line_numbers_without_ast_request:
                line = linecache.getline(filename=test_file, lineno=line_no).strip()
                error_message += f"{test_file}:{line_no} {line}\n"

            fail(
                reason=f"Dataframe ast encoding missing in test '{test_name}' "
                + f"\n\nTest file: {test_file}\nTest name: {test_name}\nLine no: {test_line_no}\n\n"
                + f"Please add AST encoding for the APIs used in:\n{error_message}",
                pytrace=False,
            )
