#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session, get_active_sessions


def test_get_active_session(session):
    assert session == get_active_session()


def test_get_active_sessions(session, db_parameters):
    with Session.builder.configs(db_parameters).create() as session2:
        assert {session, session2} == get_active_sessions()
