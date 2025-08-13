#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import pytest


@pytest.fixture(scope="module", autouse=True)
def setup(session):
    yield


def test_basic_jdbc(session):
    pass


def test_partitions(session):
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
    pass
