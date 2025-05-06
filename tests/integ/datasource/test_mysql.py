#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
from tests.utils import RUNNING_ON_JENKINS
from tests.parameters import MYSQL_CONNECTION_PARAMETERS

DEPENDENCIES_PACKAGE_UNAVAILABLE = True
try:
    import databricks  # noqa: F401
    import pandas  # noqa: F401

    DEPENDENCIES_PACKAGE_UNAVAILABLE = False
except ImportError:
    pass


pytestmark = [
    pytest.mark.skipif(DEPENDENCIES_PACKAGE_UNAVAILABLE, reason="Missing 'databricks'"),
    pytest.mark.skipif(
        RUNNING_ON_JENKINS, reason="cannot access external datasource from jenkins"
    ),
]

TEST_TABLE_NAME = "ALL_TYPES_TABLE"


def create_connection_mysql():
    import pymysql

    conn = pymysql.connect(
        user=MYSQL_CONNECTION_PARAMETERS["username"],
        password=MYSQL_CONNECTION_PARAMETERS["password"],
        host=MYSQL_CONNECTION_PARAMETERS["host"],
        database=MYSQL_CONNECTION_PARAMETERS["database"],
    )
    return conn


def test_mysql_end_to_end(session):
    df = session.read.dbapi(create_connection_mysql, table=TEST_TABLE_NAME)
    df.show()
