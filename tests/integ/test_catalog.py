#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import uuid
import pytest

from snowflake.snowpark.catalog import Catalog, Column
from snowflake.snowpark.types import LongType, StringType

pytestmark = [
    pytest.mark.xfail(
        "config.getoption('local_testing_mode', default=False)",
        reason="deepcopy is not supported and required by local testing",
        run=False,
    )
]

CATALOG_TEMP_OBJECT_PREFIX = "SP_CATALOG_TEMP"


def get_temp_name(type: str) -> str:
    return f"{CATALOG_TEMP_OBJECT_PREFIX}_{type}_{uuid.uuid4().hex[:6]}".upper()


def create_temp_db(session) -> str:
    original_db = session.get_current_database()
    temp_db = get_temp_name("DB")
    session._run_query(f"create or replace database {temp_db}")
    session.use_database(original_db)
    return temp_db


@pytest.fixture(scope="function")
def temp_db1(session):
    temp_db = create_temp_db(session)
    yield temp_db
    session._run_query(f"drop database if exists {temp_db}")


@pytest.fixture(scope="function")
def temp_db2(session):
    temp_db = create_temp_db(session)
    yield temp_db
    session._run_query(f"drop database if exists {temp_db}")


def create_temp_schema(session, db: str) -> str:
    original_schema = session.get_current_schema()
    temp_schema = get_temp_name("SCHEMA")
    session._run_query(f"create or replace schema {db}.{temp_schema}")
    session.use_schema(original_schema)
    return temp_schema


@pytest.fixture(scope="function")
def temp_schema1(session, temp_db1):
    temp_schema = create_temp_schema(session, temp_db1)
    yield temp_schema
    session._run_query(f"drop schema if exists {temp_db1}.{temp_schema}")


@pytest.fixture(scope="function")
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


@pytest.fixture(scope="function")
def temp_table1(session, temp_db1, temp_schema1):
    temp_table = create_temp_table(session, temp_db1, temp_schema1)
    yield temp_table
    session._run_query(f"drop table if exists {temp_db1}.{temp_schema1}.{temp_table}")


@pytest.fixture(scope="function")
def temp_table2(session, temp_db1, temp_schema1):
    temp_table = create_temp_table(session, temp_db1, temp_schema1)
    yield temp_table
    session._run_query(f"drop table if exists {temp_db1}.{temp_schema1}.{temp_table}")


@pytest.fixture(scope="function")
def temp_view1(session):
    pass


@pytest.fixture(scope="function")
def temp_view2(session):
    pass


@pytest.fixture(scope="function")
def temp_procedure1(session):
    pass


@pytest.fixture(scope="function")
def temp_procedure2(session):
    pass


@pytest.fixture(scope="function")
def temp_udf1(session):
    pass


@pytest.fixture(scope="function")
def temp_udf2(session):
    pass


def test_list_db(session, temp_db1, temp_db2):
    catalog: Catalog = session.catalog
    db_list = catalog.list_databases(pattern=f"{CATALOG_TEMP_OBJECT_PREFIX}_DB_*")
    assert {db.name for db in db_list} == {temp_db1, temp_db2}


def test_list_schema(session, temp_db1, temp_schema1, temp_schema2):
    catalog: Catalog = session.catalog
    assert (
        len(catalog.list_databases(pattern=f"{CATALOG_TEMP_OBJECT_PREFIX}_SCHEMA_*"))
        == 0
    )
    schema_list = catalog.list_schemas(
        pattern=f"{CATALOG_TEMP_OBJECT_PREFIX}_SCHEMA_*", database=temp_db1
    )
    assert {schema.name for schema in schema_list} == {temp_schema1, temp_schema2}


def test_list_tables(session, temp_db1, temp_schema1, temp_table1, temp_table2):
    catalog: Catalog = session.catalog

    assert len(catalog.list_tables(pattern="does_not_exist_*")) == 0
    assert (
        len(
            catalog.list_tables(
                pattern="does_not_exist_*", database=temp_db1, schema=temp_schema1
            )
        )
        == 0
    )

    print(f"list tables {temp_db1} {temp_schema1}")
    table_list = catalog.list_tables(database=temp_db1, schema=temp_schema1)
    assert {table.name for table in table_list} == {temp_table1, temp_table2}

    cols = catalog.list_columns(temp_table1, database=temp_db1, schema=temp_schema1)
    assert cols == [Column("A", LongType(), True), Column("B", StringType(), True)]


def test_get_methods(session):
    pass


def test_set_methods(session):
    pass


def test_exists_methods(session):
    pass


def test_drop_methods(session):
    pass
