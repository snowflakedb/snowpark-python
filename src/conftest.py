#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import os
import uuid

import pytest

from snowflake.snowpark import Session

RUNNING_ON_GH = os.getenv("GITHUB_ACTIONS") == "true"
TEST_SCHEMA = "GH_JOB_{}".format(str(uuid.uuid4()).replace("-", "_"))


@pytest.fixture(
    autouse=True, scope="class"
)  # scope session is pytest session, not the snowpark session.
def add_snowpark_session(doctest_namespace):
    with open("tests/parameters.py", encoding="utf-8") as f:
        exec(f.read(), globals())
    with Session.builder.configs(
        globals()["CONNECTION_PARAMETERS"]
    ).create() as session:
        if RUNNING_ON_GH:
            session.sql("CREATE SCHEMA IF NOT EXISTS {}".format(TEST_SCHEMA)).collect()
            # This is needed for test_get_schema_database_works_after_use_role in test_session_suite
            session.sql(
                "GRANT ALL PRIVILEGES ON SCHEMA {} TO ROLE PUBLIC".format(TEST_SCHEMA)
            ).collect()
            session.use_schema(TEST_SCHEMA)
        doctest_namespace["session"] = session
        yield
        if RUNNING_ON_GH:
            session.sql("DROP SCHEMA IF EXISTS {}".format(TEST_SCHEMA)).collect()
