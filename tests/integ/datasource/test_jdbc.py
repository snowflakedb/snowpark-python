#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import pytest

from snowflake.snowpark._internal.data_source import JDBC
from tests.resources.test_data_source_dir.test_jdbc import (
    URL,
    EXTERNAL_ACCESS_INTEGRATION,
)


@pytest.fixture(scope="module", autouse=True)
def setup(session, resources_path):
    stage_name = session.get_session_stage()
    session.file.put(
        resources_path + "/test_data_source_dir/mongodb-jdbc-2.3.0-all.jar", stage_name
    )
    yield


def test_basic_jdbc(session):
    pass


def test_partitions(session):
    # no partitions
    stage_name = session.get_session_stage()
    jar_path = stage_name + "/mongodb-jdbc-2.3.0-all.jar"
    client = JDBC(
        session,
        URL,
        properties={"database": "VirtualDatabase0"},
        table_or_query='SELECT _id, "double", "string", "object", "array", "binary","objectId", "bool", "date", "int32", "long", "decimal128", "timestamp", "code" FROM test_collection',
        external_access_integration=EXTERNAL_ACCESS_INTEGRATION,
        imports=[jar_path],
        is_query=True,
    )
    client.partitions

    # partition column

    # predicates
    pass


def test_custom_schema(session):
    pass


def test_unsupported_dbms_type(session):
    pass


def test_timestamp_type(session):
    pass


def test_infer_schema(session):
    stage_name = session.get_session_stage()
    jar_path = stage_name + "/mongodb-jdbc-2.3.0-all.jar"
    client = JDBC(
        session,
        URL,
        properties={"database": "VirtualDatabase0"},
        table_or_query='SELECT _id, "double", "string", "object", "array", "binary","objectId", "bool", "date", "int32", "long", "decimal128", "timestamp", "code" FROM test_collection',
        external_access_integration=EXTERNAL_ACCESS_INTEGRATION,
        imports=[jar_path],
        is_query=True,
    )
    client.secret_detector()
