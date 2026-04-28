#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""Shared pytest fixtures for catalog integration tests (see ``test_catalog*.py``)."""

import uuid

import pytest

from snowflake.snowpark.session import Session
from snowflake.snowpark.types import IntegerType

CATALOG_TEMP_OBJECT_PREFIX = "SP_CATALOG_TEMP"


def get_temp_name(type: str) -> str:
    return f"{CATALOG_TEMP_OBJECT_PREFIX}_{type}_{uuid.uuid4().hex[:6]}".upper()


def create_temp_db(session) -> str:
    original_db = session.get_current_database()
    temp_db = get_temp_name("DB")
    session._run_query(f"create or replace database {temp_db}")
    session.use_database(original_db)
    return temp_db


@pytest.fixture(scope="module")
def temp_db1(session):
    temp_db = create_temp_db(session)
    yield temp_db
    session._run_query(f"drop database if exists {temp_db}")


@pytest.fixture(scope="module")
def temp_db2(session):
    temp_db = create_temp_db(session)
    yield temp_db
    session._run_query(f"drop database if exists {temp_db}")


def create_temp_schema(session, db: str) -> str:
    original_db = session.get_current_database()
    original_schema = session.get_current_schema()
    temp_schema = get_temp_name("SCHEMA")
    session._run_query(f"create or replace schema {db}.{temp_schema}")

    session.use_database(original_db)
    session.use_schema(original_schema)
    return temp_schema


@pytest.fixture(scope="module")
def temp_schema1(session, temp_db1):
    temp_schema = create_temp_schema(session, temp_db1)
    yield temp_schema
    session._run_query(f"drop schema if exists {temp_db1}.{temp_schema}")


@pytest.fixture(scope="module")
def temp_schema2(session, temp_db1):
    temp_schema = create_temp_schema(session, temp_db1)
    yield temp_schema
    session._run_query(f"drop schema if exists {temp_db1}.{temp_schema}")


def create_temp_table(session, db: str, schema: str) -> str:
    temp_table = get_temp_name("TABLE")
    session._run_query(
        f"create or replace temp table {db}.{schema}.{temp_table} (a int, b string)"
    )
    return temp_table


@pytest.fixture(scope="module")
def temp_table1(session, temp_db1, temp_schema1):
    temp_table = create_temp_table(session, temp_db1, temp_schema1)
    yield temp_table
    session._run_query(f"drop table if exists {temp_db1}.{temp_schema1}.{temp_table}")


@pytest.fixture(scope="module")
def temp_table2(session, temp_db1, temp_schema1):
    temp_table = create_temp_table(session, temp_db1, temp_schema1)
    yield temp_table
    session._run_query(f"drop table if exists {temp_db1}.{temp_schema1}.{temp_table}")


def create_temp_view(session, db: str, schema: str) -> str:
    temp_schema = get_temp_name("VIEW")
    session._run_query(
        f"create or replace temp view {db}.{schema}.{temp_schema} as select 1 as a, '2' as b"
    )
    return temp_schema


@pytest.fixture(scope="module")
def temp_view1(session, temp_db1, temp_schema1):
    temp_view = create_temp_view(session, temp_db1, temp_schema1)
    yield temp_view
    session._run_query(f"drop view if exists {temp_db1}.{temp_schema1}.{temp_view}")


@pytest.fixture(scope="module")
def temp_view2(session, temp_db1, temp_schema1):
    temp_view = create_temp_view(session, temp_db1, temp_schema1)
    yield temp_view
    session._run_query(f"drop view if exists {temp_db1}.{temp_schema1}.{temp_view}")


def create_temp_procedure(session: Session, db, schema) -> str:
    temp_procedure = get_temp_name("PROCEDURE")
    session.sproc.register(
        lambda _, x: x + 1,
        return_type=IntegerType(),
        input_types=[IntegerType()],
        name=f"{db}.{schema}.{temp_procedure}",
        packages=["snowflake-snowpark-python"],
    )
    return temp_procedure


@pytest.fixture(scope="module")
def temp_procedure1(session, temp_db1, temp_schema1):
    temp_procedure = create_temp_procedure(session, temp_db1, temp_schema1)
    yield temp_procedure
    session._run_query(
        f"drop procedure if exists {temp_db1}.{temp_schema1}.{temp_procedure}(int)"
    )


@pytest.fixture(scope="module")
def temp_procedure2(session, temp_db1, temp_schema1):
    temp_procedure = create_temp_procedure(session, temp_db1, temp_schema1)
    yield temp_procedure
    session._run_query(
        f"drop procedure if exists {temp_db1}.{temp_schema1}.{temp_procedure}(int)"
    )


def create_temp_udf(session: Session, db, schema) -> str:
    temp_udf = get_temp_name("UDF")
    session.udf.register(
        lambda x: x + 1,
        return_type=IntegerType(),
        input_types=[IntegerType()],
        name=f"{db}.{schema}.{temp_udf}",
    )
    return temp_udf


@pytest.fixture(scope="module")
def temp_udf1(session, temp_db1, temp_schema1):
    temp_udf = create_temp_udf(session, temp_db1, temp_schema1)
    yield temp_udf
    session._run_query(
        f"drop function if exists {temp_db1}.{temp_schema1}.{temp_udf}(int)"
    )


@pytest.fixture(scope="module")
def temp_udf2(session, temp_db1, temp_schema1):
    temp_udf = create_temp_udf(session, temp_db1, temp_schema1)
    yield temp_udf
    session._run_query(
        f"drop function if exists {temp_db1}.{temp_schema1}.{temp_udf}(int)"
    )


DOES_NOT_EXIST_PATTERN = "does_not_exist_.*"
