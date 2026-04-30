#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""Catalog integration tests and shared fixtures.

Mode-agnostic tests (same behavior for SQL and REST catalog backends) live in
this module. Backend-specific tests are in ``test_catalog_sql_mode.py`` and
``test_catalog_rest_mode.py``, which reuse the fixtures defined here via
``pytest_plugins`` in ``conftest.py``.
"""

import uuid
from unittest.mock import patch

import pytest

from snowflake.snowpark.catalog import Catalog
from snowflake.snowpark.context import _DEFAULT_ARTIFACT_REPOSITORY
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import IntegerType

CATALOG_TEMP_OBJECT_PREFIX = "SP_CATALOG_TEMP"
DOES_NOT_EXIST_PATTERN = "does_not_exist_.*"


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
    session.sql(
        f"ALTER SCHEMA SET DEFAULT_PYTHON_ARTIFACT_REPOSITORY = {_DEFAULT_ARTIFACT_REPOSITORY}"
    ).collect()

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


pytestmark = [
    pytest.mark.xfail(
        "config.getoption('local_testing_mode', default=False)",
        reason="deepcopy is not supported and required by local testing",
        run=False,
    ),
]


def test_list_tables(session, temp_db1, temp_schema1, temp_table1, temp_table2):
    catalog: Catalog = session.catalog

    assert len(catalog.list_tables(pattern=DOES_NOT_EXIST_PATTERN)) == 0
    assert (
        len(
            catalog.list_tables(
                pattern=DOES_NOT_EXIST_PATTERN, database=temp_db1, schema=temp_schema1
            )
        )
        == 0
    )

    table_list = catalog.list_tables(database=temp_db1, schema=temp_schema1)
    assert {table.name for table in table_list} == {temp_table1, temp_table2}

    table_list = catalog.list_tables(
        database=temp_db1,
        schema=temp_schema1,
        like=f"{CATALOG_TEMP_OBJECT_PREFIX}_TABLE_%",
    )
    assert {table.name for table in table_list} == {temp_table1, temp_table2}

    cols = catalog.list_columns(temp_table1, database=temp_db1, schema=temp_schema1)
    assert len(cols) == 2
    assert cols[0].name == "A"
    assert cols[0].datatype == "NUMBER(38,0)"
    assert cols[0].nullable is True
    assert cols[1].name == "B"
    # 2025_07/bcr-2118 changes the default string length from 16777216 to 134217728
    assert (
        cols[1].datatype == "VARCHAR(16777216)"
        or cols[1].datatype == "VARCHAR(134217728)"
    )
    assert cols[1].nullable is True


def test_list_views(session, temp_db1, temp_schema1, temp_view1, temp_view2):
    catalog: Catalog = session.catalog

    assert len(catalog.list_views(pattern=DOES_NOT_EXIST_PATTERN)) == 0
    assert (
        len(
            catalog.list_views(
                pattern=DOES_NOT_EXIST_PATTERN, database=temp_db1, schema=temp_schema1
            )
        )
        == 0
    )

    view_list = catalog.list_views(database=temp_db1, schema=temp_schema1)
    assert {view.name for view in view_list} >= {temp_view1, temp_view2}

    view_list = catalog.list_views(
        database=temp_db1,
        schema=temp_schema1,
        pattern=f"{CATALOG_TEMP_OBJECT_PREFIX}_VIEW.*",
    )
    assert {view.name for view in view_list} >= {temp_view1, temp_view2}

    view_list = catalog.list_views(
        database=temp_db1,
        schema=temp_schema1,
        like=f"{CATALOG_TEMP_OBJECT_PREFIX}_VIEW%",
    )
    assert {view.name for view in view_list} >= {temp_view1, temp_view2}


@pytest.mark.udf
def test_list_procedures(
    session, temp_db1, temp_schema1, temp_procedure1, temp_procedure2
):
    catalog: Catalog = session.catalog
    session.sql(
        f"ALTER SCHEMA SET DEFAULT_PYTHON_ARTIFACT_REPOSITORY = {_DEFAULT_ARTIFACT_REPOSITORY}"
    ).collect()
    assert len(catalog.list_procedures(pattern=DOES_NOT_EXIST_PATTERN)) == 0
    assert (
        len(
            catalog.list_procedures(
                pattern=DOES_NOT_EXIST_PATTERN, database=temp_db1, schema=temp_schema1
            )
        )
        == 0
    )

    procedure_list = catalog.list_procedures(
        database=temp_db1,
        schema=temp_schema1,
        pattern=f"{CATALOG_TEMP_OBJECT_PREFIX}_PROCEDURE_.*",
    )
    assert {procedure.name for procedure in procedure_list} >= {
        temp_procedure1,
        temp_procedure2,
    }

    procedure_list = catalog.list_procedures(
        database=temp_db1,
        schema=temp_schema1,
        like=f"{CATALOG_TEMP_OBJECT_PREFIX}_PROCEDURE_%",
    )
    assert {procedure.name for procedure in procedure_list} >= {
        temp_procedure1,
        temp_procedure2,
    }


@pytest.mark.xfail(reason="SNOW-1787268: Bug in snowflake api functions iter")
def test_list_udfs(session, temp_db1, temp_schema1, temp_udf1, temp_udf2):
    catalog: Catalog = session.catalog

    assert len(catalog.list_functions(pattern=DOES_NOT_EXIST_PATTERN)) == 0
    assert (
        len(
            catalog.list_functions(
                pattern=DOES_NOT_EXIST_PATTERN, database=temp_db1, schema=temp_schema1
            )
        )
        == 0
    )
    udf_list = catalog.list_functions(
        database=temp_db1,
        schema=temp_schema1,
        pattern=f"{CATALOG_TEMP_OBJECT_PREFIX}_UDF_.*",
    )
    assert {udf.name for udf in udf_list} >= {temp_udf1, temp_udf2}


def test_set_db_schema(session, temp_db1, temp_db2, temp_schema1, temp_schema2):
    catalog = session.catalog

    original_db = session.get_current_database()
    original_schema = session.get_current_schema()
    try:
        catalog.set_current_database(temp_db1)
        catalog.set_current_schema(temp_schema1)
        assert session.get_current_database() == f'"{temp_db1}"'
        assert session.get_current_schema() == f'"{temp_schema1}"'

        catalog.set_current_schema(temp_schema2)
        assert session.get_current_schema() == f'"{temp_schema2}"'

        catalog.set_current_database(temp_db2)
        assert session.get_current_database() == f'"{temp_db2}"'
    finally:
        session.use_database(original_db)
        session.use_schema(original_schema)


def test_parse_names_negative(session):
    catalog = session.catalog
    with pytest.raises(
        ValueError,
        match="Unexpected type. Expected str or Database, got '<class 'int'>'",
    ):
        catalog.database_exists(123)

    with pytest.raises(
        ValueError, match="Unexpected type. Expected str or Schema, got '<class 'int'>'"
    ):
        catalog.schema_exists(123)

    with pytest.raises(
        ValueError,
        match="arg_types must be provided when function/procedure is a string",
    ):
        catalog.procedure_exists("proc")

    with patch.object(session, "get_current_database", return_value=None):
        for db in (None, ""):
            with pytest.raises(
                ValueError,
                match="No database detected. Please provide database to proceed.",
            ):
                catalog._parse_database(database=db)

    with patch.object(session, "get_current_schema", return_value=None):
        for schema in (None, ""):
            with pytest.raises(
                ValueError,
                match="No schema detected. Please provide schema to proceed.",
            ):
                catalog._parse_schema(schema=schema)
