#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""Catalog integration tests with ``context._is_snowpark_connect_compatible_mode`` True (SQL backend).

Keep this file separate from ``test_catalog_rest_mode.py`` so removing one backend path
deletes only the matching test module.
"""

import pytest

from snowflake.core.exceptions import NotFoundError as CoreNotFoundError
from snowflake.core.view import View
from snowflake.snowpark import context
from snowflake.snowpark._internal.analyzer.analyzer_utils import unquote_if_quoted
from snowflake.snowpark.catalog import Catalog
from snowflake.snowpark.exceptions import NotFoundError
from snowflake.snowpark.types import IntegerType
from tests.integ.test_catalog import (
    CATALOG_TEMP_OBJECT_PREFIX,
    create_temp_db,
    create_temp_schema,
    create_temp_table,
    create_temp_view,
)

pytestmark = [
    pytest.mark.xfail(
        "config.getoption('local_testing_mode', default=False)",
        reason="deepcopy is not supported and required by local testing",
        run=False,
    ),
]


@pytest.fixture(autouse=True, scope="module")
def _catalog_sql_backend_mode(session):
    mp = pytest.MonkeyPatch()
    mp.setattr(context, "_is_snowpark_connect_compatible_mode", True)
    mp.setattr(session, "_catalog", None)
    try:
        yield
    finally:
        mp.undo()
        session._catalog = None


def test_list_db_sql_mode(session, temp_db1, temp_db2):
    catalog: Catalog = session.catalog
    db_list = catalog.list_databases(pattern=f"{CATALOG_TEMP_OBJECT_PREFIX}_DB_*")
    assert {db.name for db in db_list} >= {temp_db1, temp_db2}

    db_list = catalog.list_databases(like=f"{CATALOG_TEMP_OBJECT_PREFIX}_DB_%")
    assert {db.name for db in db_list} >= {temp_db1, temp_db2}


def test_list_schema_sql_mode(session, temp_db1, temp_schema1, temp_schema2):
    catalog: Catalog = session.catalog
    assert (
        len(catalog.list_databases(pattern=f"{CATALOG_TEMP_OBJECT_PREFIX}_SCHEMA_.*"))
        == 0
    )

    schema_list = catalog.list_schemas(
        pattern=f"{CATALOG_TEMP_OBJECT_PREFIX}_SCHEMA_.*", database=temp_db1
    )
    assert {schema.name for schema in schema_list} >= {temp_schema1, temp_schema2}

    schema_list = catalog.list_schemas(
        like=f"{CATALOG_TEMP_OBJECT_PREFIX}_SCHEMA_%", database=temp_db1
    )
    assert {schema.name for schema in schema_list} >= {temp_schema1, temp_schema2}


def test_get_db_schema_sql_mode(session):
    catalog: Catalog = session.catalog
    current_db = session.get_current_database()
    current_schema = session.get_current_schema()
    assert catalog.get_database(current_db).name == unquote_if_quoted(current_db)
    assert catalog.get_schema(current_schema).name == unquote_if_quoted(current_schema)


def test_get_database_missing_raises_snowpark_not_found_sql_mode(session):
    catalog: Catalog = session.catalog
    with pytest.raises(NotFoundError, match="could not be found"):
        catalog.get_database("NONEXISTENT_DB_XYZ_12345")


def test_compat_mode_with_sql_base_disabled_uses_rest_backend(session):
    original_use_sql_base = session._use_sql_base
    try:
        session._use_sql_base = False
        session._catalog = None
        catalog: Catalog = session.catalog
        assert type(catalog._backend).__name__ == "_RestCatalogBackend"
        with pytest.raises(CoreNotFoundError):
            catalog.get_database("NONEXISTENT_DB_XYZ_12345")
    finally:
        session._use_sql_base = original_use_sql_base
        session._catalog = None


def test_compat_mode_with_sql_base_enabled_uses_sql_backend(session):
    original_use_sql_base = session._use_sql_base
    try:
        session._use_sql_base = True
        session._catalog = None
        catalog: Catalog = session.catalog
        assert type(catalog._backend).__name__ == "_SqlCatalogBackend"
    finally:
        session._use_sql_base = original_use_sql_base
        session._catalog = None


def test_get_table_resolves_view_sql_mode(session, temp_db1, temp_schema1, temp_view1):
    catalog: Catalog = session.catalog
    obj = catalog.get_table(temp_view1, database=temp_db1, schema=temp_schema1)
    assert isinstance(obj, View)
    assert obj.name == temp_view1


def test_table_exists_true_for_view_name_sql_mode(
    session, temp_db1, temp_schema1, temp_view1
):
    catalog: Catalog = session.catalog
    assert catalog.table_exists(temp_view1, database=temp_db1, schema=temp_schema1)


@pytest.mark.udf
def test_get_procedure_sql_mode(session, temp_db1, temp_schema1, temp_procedure1):
    catalog: Catalog = session.catalog
    procedure = catalog.get_procedure(
        temp_procedure1, [IntegerType()], database=temp_db1, schema=temp_schema1
    )
    assert procedure.name == temp_procedure1
    assert procedure.database_name == temp_db1
    assert procedure.schema_name == temp_schema1


@pytest.mark.udf
def test_get_user_defined_function_sql_mode(session, temp_db1, temp_schema1, temp_udf1):
    catalog: Catalog = session.catalog
    udf = catalog.get_user_defined_function(
        temp_udf1, [IntegerType()], database=temp_db1, schema=temp_schema1
    )
    assert udf.name == temp_udf1
    assert udf.database_name == temp_db1
    assert udf.schema_name == temp_schema1


def test_database_exists_sql_mode(session, temp_db1):
    catalog: Catalog = session.catalog
    assert catalog.database_exists(temp_db1)
    assert not catalog.database_exists("does_not_exist")


def test_get_table_view_sql_mode(
    session, temp_db1, temp_schema1, temp_table1, temp_view1
):
    catalog: Catalog = session.catalog
    table = catalog.get_table(temp_table1, database=temp_db1, schema=temp_schema1)
    assert table.name == temp_table1
    assert table.database_name == temp_db1
    assert table.schema_name == temp_schema1

    view = catalog.get_view(temp_view1, database=temp_db1, schema=temp_schema1)
    assert view.name == temp_view1
    assert view.database_name == temp_db1
    assert view.schema_name == temp_schema1


def test_exists_db_schema_sql_mode(session, temp_db1, temp_schema1):
    catalog = session.catalog
    assert catalog.database_exists(temp_db1)
    assert not catalog.database_exists("does_not_exist")

    assert catalog.schema_exists(temp_schema1, database=temp_db1)
    assert not catalog.schema_exists(temp_schema1, database="does_not_exist")


def test_exists_table_view_sql_mode(
    session, temp_db1, temp_schema1, temp_table1, temp_view1
):
    catalog = session.catalog
    db1_obj = catalog.get_database(temp_db1)
    schema1_obj = catalog.get_schema(database=temp_db1, schema=temp_schema1)

    assert catalog.table_exists(temp_table1, database=temp_db1, schema=temp_schema1)
    assert catalog.table_exists(temp_table1, database=db1_obj, schema=schema1_obj)
    table = catalog.get_table(temp_table1, database=temp_db1, schema=temp_schema1)
    assert catalog.table_exists(table)
    assert not catalog.table_exists(
        "does_not_exist", database=temp_db1, schema=temp_schema1
    )

    assert catalog.view_exists(temp_view1, database=temp_db1, schema=temp_schema1)
    assert catalog.view_exists(temp_view1, database=db1_obj, schema=schema1_obj)
    view = catalog.get_view(temp_view1, database=temp_db1, schema=temp_schema1)
    assert catalog.view_exists(view)
    assert not catalog.view_exists(
        "does_not_exist", database=temp_db1, schema=temp_schema1
    )


@pytest.mark.udf
def test_exists_function_procedure_udf_sql_mode(
    session, temp_db1, temp_schema1, temp_procedure1, temp_udf1
):
    catalog = session.catalog
    db1_obj = catalog.get_database(temp_db1)
    schema1_obj = catalog.get_schema(temp_schema1, database=temp_db1)

    assert catalog.procedure_exists(
        temp_procedure1, [IntegerType()], database=temp_db1, schema=temp_schema1
    )
    assert catalog.procedure_exists(
        temp_procedure1, [IntegerType()], database=db1_obj, schema=schema1_obj
    )
    proc = catalog.get_procedure(
        temp_procedure1, [IntegerType()], database=temp_db1, schema=temp_schema1
    )
    assert catalog.procedure_exists(proc)
    assert not catalog.procedure_exists(
        "does_not_exist", [], database=temp_db1, schema=temp_schema1
    )

    assert catalog.user_defined_function_exists(
        temp_udf1, [IntegerType()], database=temp_db1, schema=temp_schema1
    )
    assert catalog.user_defined_function_exists(
        temp_udf1, [IntegerType()], database=db1_obj, schema=schema1_obj
    )
    udf = catalog.get_user_defined_function(
        temp_udf1, [IntegerType()], database=temp_db1, schema=temp_schema1
    )
    assert catalog.user_defined_function_exists(udf)
    assert not catalog.user_defined_function_exists(
        "does_not_exist", [], database=temp_db1, schema=temp_schema1
    )


@pytest.mark.parametrize("use_object", [True, False])
def test_drop_sql_mode(session, use_object):
    catalog = session.catalog

    original_db = session.get_current_database()
    original_schema = session.get_current_schema()
    try:
        temp_db = create_temp_db(session)
        temp_schema = create_temp_schema(session, temp_db)
        temp_table = create_temp_table(session, temp_db, temp_schema)
        temp_view = create_temp_view(session, temp_db, temp_schema)
        if use_object:
            temp_schema = catalog.get_schema(temp_schema, database=temp_db)
            temp_db = catalog.get_database(temp_db)

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
