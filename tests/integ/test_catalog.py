#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import uuid
import pytest

from snowflake.snowpark.catalog import Catalog, Column
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import IntegerType, LongType, StringType

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
    original_schema = session.get_current_schema()
    temp_schema = get_temp_name("SCHEMA")
    session._run_query(f"create or replace schema {db}.{temp_schema}")
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
    temp_schema = get_temp_name("SCHEMA")
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


def test_list_db(session, temp_db1, temp_db2):
    catalog: Catalog = session.catalog
    db_list = catalog.list_databases(pattern=f"{CATALOG_TEMP_OBJECT_PREFIX}_DB_*")
    assert {db.name for db in db_list} >= {temp_db1, temp_db2}


def test_list_schema(session, temp_db1, temp_schema1, temp_schema2):
    catalog: Catalog = session.catalog
    assert (
        len(catalog.list_databases(pattern=f"{CATALOG_TEMP_OBJECT_PREFIX}_SCHEMA_*"))
        == 0
    )
    schema_list = catalog.list_schemas(
        pattern=f"{CATALOG_TEMP_OBJECT_PREFIX}_SCHEMA_*", database=temp_db1
    )
    assert {schema.name for schema in schema_list} >= {temp_schema1, temp_schema2}


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


def test_list_views(session, temp_db1, temp_schema1, temp_view1, temp_view2):
    catalog: Catalog = session.catalog

    assert len(catalog.list_views(pattern="does_not_exist_*")) == 0
    assert (
        len(
            catalog.list_views(
                pattern="does_not_exist_*", database=temp_db1, schema=temp_schema1
            )
        )
        == 0
    )

    view_list = catalog.list_views(database=temp_db1, schema=temp_schema1)
    assert {view.name for view in view_list} >= {temp_view1, temp_view2}


@pytest.mark.xfail(reason="SNOW-1787268: Bug in snowflake api functions iter")
def test_list_functions(session):
    catalog: Catalog = session.catalog

    assert len(catalog.list_functions(pattern="does_not_exist_*")) == 0
    seq_functions = catalog.list_functions(pattern="seq_*")
    assert {func.name for func in seq_functions} == {"seq0", "seq2"}


def test_list_procedures(
    session, temp_db1, temp_schema1, temp_procedure1, temp_procedure2
):
    catalog: Catalog = session.catalog

    assert len(catalog.list_procedures(pattern="does_not_exist_*")) == 0
    assert (
        len(
            catalog.list_procedures(
                pattern="does_not_exist_*", database=temp_db1, schema=temp_schema1
            )
        )
        == 0
    )

    procedure_list = catalog.list_procedures(
        database=temp_db1,
        schema=temp_schema1,
        pattern=f"{CATALOG_TEMP_OBJECT_PREFIX}_PROCEDURE_*",
    )
    assert {procedure.name for procedure in procedure_list} >= {
        temp_procedure1,
        temp_procedure2,
    }


@pytest.mark.xfail(reason="SNOW-1787268: Bug in snowflake api functions iter")
def test_list_udfs(session, temp_db1, temp_schema1, temp_udf1, temp_udf2):
    catalog: Catalog = session.catalog

    assert len(catalog.list_functions(pattern="does_not_exist_*")) == 0
    assert (
        len(
            catalog.list_functions(
                pattern="does_not_exist_*", database=temp_db1, schema=temp_schema1
            )
        )
        == 0
    )
    udf_list = catalog.list_functions(
        database=temp_db1,
        schema=temp_schema1,
        pattern=f"{CATALOG_TEMP_OBJECT_PREFIX}_UDF_*",
    )
    assert {udf.name for udf in udf_list} >= {temp_udf1, temp_udf2}


def test_get_db_schema(session):
    catalog: Catalog = session.catalog
    current_db = session.get_current_database()
    current_schema = session.get_current_schema()
    assert catalog.get_current_database().name == current_db
    assert catalog.get_current_schema().name == current_schema


def test_get_table_view(session, temp_db1, temp_schema1, temp_table1, temp_view1):
    catalog: Catalog = session.catalog
    table = catalog.get_table(temp_table1, database=temp_db1, schema=temp_schema1)
    assert table.name == temp_table1
    assert table.database_name == temp_db1
    assert table.schema_name == temp_schema1

    view = catalog.get_view(temp_view1, database=temp_db1, schema=temp_schema1)
    assert view.name == temp_view1
    assert view.database_name == temp_db1
    assert view.schema_name == temp_schema1


def test_get_function_procedure_udf(
    session, temp_db1, temp_schema1, temp_procedure1, temp_udf1
):
    catalog: Catalog = session.catalog

    # function = catalog.get_function("seq1", [])
    # assert function.name == "seq1"

    procedure = catalog.get_procedure(
        temp_procedure1, [IntegerType()], database=temp_db1, schema=temp_schema1
    )
    assert procedure.name == temp_procedure1
    assert procedure.database_name == temp_db1
    assert procedure.schema_name == temp_schema1

    udf = catalog.get_user_defined_function(
        temp_udf1, [IntegerType()], database=temp_db1, schema=temp_schema1
    )
    assert udf.name == temp_udf1
    assert udf.database_name == temp_db1
    assert udf.schema_name == temp_schema1


def test_set_db_schema(session, temp_db1, temp_db2, temp_schema1, temp_schema2):
    catalog = session.catalog

    original_db = session.get_current_database()
    original_schema = session.get_current_schema()
    try:
        catalog.set_current_database(temp_db1)
        catalog.set_current_schema(temp_schema1)
        assert session.get_current_database() == f'"{temp_db1}"'
        assert session.get_current_schema() == f'"{temp_schema1}"'

        catalog.set_current_database(temp_db2)
        catalog.set_current_schema(temp_schema2)
        assert session.get_current_database() == f'"{temp_db2}"'
        assert session.get_current_schema() == f'"{temp_schema2}"'
    finally:
        session.use_database(original_db)
        session.use_schema(original_schema)


def test_exists_db_schema(session, temp_db1, temp_schema1):
    catalog = session.catalog
    assert catalog.database_exists(temp_db1)
    assert not catalog.database_exists("does_not_exist")

    assert catalog.schema_exists(temp_schema1, database=temp_db1)
    assert not catalog.schema_exists(temp_schema1, database="does_not_exist")


def test_exists_table_view(session, temp_db1, temp_schema1, temp_table1, temp_view1):
    catalog = session.catalog
    assert catalog.table_exists(temp_table1, database=temp_db1, schema=temp_schema1)
    assert not catalog.table_exists(
        "does_not_exist", database=temp_db1, schema=temp_schema1
    )

    assert catalog.view_exists(temp_view1, database=temp_db1, schema=temp_schema1)
    assert not catalog.view_exists(
        "does_not_exist", database=temp_db1, schema=temp_schema1
    )


def test_exists_function_procedure_udf(
    session, temp_db1, temp_schema1, temp_procedure1, temp_udf1
):
    catalog = session.catalog
    # assert catalog.function_exists("seq1", [])
    # assert not catalog.function_exists("does_not_exist", [])

    assert catalog.procedure_exists(
        temp_procedure1, [IntegerType()], database=temp_db1, schema=temp_schema1
    )
    assert not catalog.procedure_exists(
        "does_not_exist", [], database=temp_db1, schema=temp_schema1
    )

    assert catalog.user_defined_function_exists(
        temp_udf1, [IntegerType()], database=temp_db1, schema=temp_schema1
    )
    assert not catalog.user_defined_function_exists(
        "does_not_exist", [], database=temp_db1, schema=temp_schema1
    )


def test_drop(session):
    catalog = session.catalog

    original_db = session.get_current_database()
    original_schema = session.get_current_schema()
    try:
        temp_db = create_temp_db(session)
        temp_schema = create_temp_schema(session, temp_db)
        temp_table = create_temp_table(session, temp_db, temp_schema)
        temp_view = create_temp_view(session, temp_db, temp_schema)

        assert catalog.database_exists(temp_db)
        assert catalog.schema_exists(temp_schema, database=temp_db)
        assert catalog.table_exists(temp_table, database=temp_db, schema=temp_schema)
        assert catalog.view_exists(temp_view, database=temp_db, schema=temp_schema)

        catalog.drop_table(temp_table, database=temp_db, schema=temp_schema)
        catalog.drop_view(temp_view, database=temp_db, schema=temp_schema)

        assert not catalog.table_exists(
            temp_table, database=temp_db, schema=temp_schema
        )
        assert not catalog.view_exists(temp_view, database=temp_db, schema=temp_schema)

        catalog.drop_schema(temp_schema, database=temp_db)
        assert not catalog.schema_exists(temp_schema, database=temp_db)

        catalog.drop_database(temp_db)
        assert not catalog.database_exists(temp_db)
    finally:
        session.use_database(original_db)
        session.use_schema(original_schema)
