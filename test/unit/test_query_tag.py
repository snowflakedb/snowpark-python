#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

from snowflake.snowpark.internal.utils import Utils as InternalUtils


def test_generate_query_tag():
    query_tag = InternalUtils.create_statement_query_tag()
    assert "test_query_tag" in query_tag  # file name in query tag
    assert "test_generate_query_tag" in query_tag  # calling function name in query tag


def test_generate_query_tag_skip_1_level():
    query_tag = InternalUtils.create_statement_query_tag(skip_levels=1)
    assert "test_query_tag" in query_tag  # file name in query tag
    assert (
        "test_generate_query_tag_skip_1_level" in query_tag
    )  # calling function name in query tag


def test_generate_query_tag_skip_2_levels():
    query_tag = InternalUtils.create_statement_query_tag(skip_levels=2)
    assert "test_query_tag" not in query_tag  # skipped last call stack
    assert (
        "test_generate_query_tag_skip_2_level" not in query_tag
    )  # skipped last call stack
