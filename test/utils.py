#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import uuid
from src.snowflake.snowpark.session import Session


class Utils:
    @staticmethod
    def random_name() -> str:
        return "SN_TEST_OBJECT_{}".format(str(uuid.uuid4()).replace("-", "_"))

    @staticmethod
    def create_table(session: 'Session', name: str, schema: str):
        session._run_query("create or replace table {name} ({schema})".format(name=name, schema=schema))

    @staticmethod
    def drop_table(session: 'Session', name: str):
        session._run_query("drop table if exists {name}".format(name=name))

    @staticmethod
    def equals_ignore_case(a: str, b: str) -> bool:
        return a.lower() == b.lower()
