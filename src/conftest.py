#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import pytest

from snowflake.snowpark import Session


@pytest.fixture(
    autouse=True, scope="session"
)  # scope session is pytest session, not the snowpark session.
def add_snowpark_session(doctest_namespace):
    with open("tests/parameters.py", encoding="utf-8") as f:
        exec(f.read(), globals())
    with Session.builder.configs(
        globals()["CONNECTION_PARAMETERS"]
    ).create() as session:
        doctest_namespace["session"] = session
        yield
