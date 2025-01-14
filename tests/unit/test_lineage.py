#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

from snowflake.snowpark.exceptions import SnowparkFetchDataException
from snowflake.snowpark.lineage import (
    Lineage,
    LineageDirection,
    _DGQLQueryBuilder,
    _ObjectField,
    _SnowflakeDomain,
    _UserDomain,
)
from snowflake.snowpark.session import Session


def test_get_name_and_version():
    fake_session = mock.create_autospec(Session, _session_id=123456)
    fake_session._analyzer = mock.Mock()

    graph_entity = {
        _ObjectField.USER_DOMAIN: _UserDomain.MODEL,
        _ObjectField.DB: "db1",
        _ObjectField.SCHEMA: "schema1",
        _ObjectField.PROPERTIES: {_ObjectField.PARENT_NAME: "name1"},
        _ObjectField.NAME: "version1",
    }
    name, version = Lineage(fake_session)._get_name_and_version(graph_entity)
    assert name == "db1.schema1.name1"
    assert version == "version1"

    graph_entity = {
        _ObjectField.USER_DOMAIN: _SnowflakeDomain.DATASET,
        _ObjectField.DB: "db1",
        _ObjectField.SCHEMA: "schema1",
        _ObjectField.PROPERTIES: {_ObjectField.PARENT_NAME: "name1"},
        _ObjectField.NAME: "version1",
    }
    name, version = Lineage(fake_session)._get_name_and_version(graph_entity)
    assert name == "db1.schema1.name1"
    assert version == "version1"

    graph_entity = {
        _ObjectField.USER_DOMAIN: _SnowflakeDomain.TABLE,
        _ObjectField.DB: "db1",
        _ObjectField.SCHEMA: "schema1",
        _ObjectField.PROPERTIES: {_ObjectField.PARENT_NAME: "whatever"},
        _ObjectField.NAME: "name1",
    }
    name, version = Lineage(fake_session)._get_name_and_version(graph_entity)
    assert name == "db1.schema1.name1"
    assert version is None

    graph_entity = {
        _ObjectField.USER_DOMAIN: _UserDomain.FEATURE_VIEW,
        _ObjectField.DB: "db1",
        _ObjectField.SCHEMA: "schema1",
        _ObjectField.PROPERTIES: {_ObjectField.PARENT_NAME: "whatever"},
        _ObjectField.NAME: "name1$v1",
    }
    name, version = Lineage(fake_session)._get_name_and_version(graph_entity)
    assert name == "db1.schema1.name1"
    assert version == "v1"

    graph_entity = {
        _ObjectField.USER_DOMAIN: _UserDomain.FEATURE_VIEW,
        _ObjectField.DB: "db1",
        _ObjectField.SCHEMA: "schema1",
        _ObjectField.PROPERTIES: {_ObjectField.PARENT_NAME: "whatever"},
        _ObjectField.NAME: '"name1$v1"',
    }
    name, version = Lineage(fake_session)._get_name_and_version(graph_entity)
    assert name == 'db1.schema1."name1"'
    assert version == "v1"

    graph_entity = {
        _ObjectField.USER_DOMAIN: _UserDomain.FEATURE_VIEW,
        _ObjectField.DB: "db1",
        _ObjectField.SCHEMA: "schema1",
        _ObjectField.PROPERTIES: {_ObjectField.PARENT_NAME: "whatever"},
        _ObjectField.NAME: "name1$name2$v1",
    }
    name, version = Lineage(fake_session)._get_name_and_version(graph_entity)
    assert name == "db1.schema1.name1$name2"
    assert version == "v1"

    graph_entity = {
        _ObjectField.USER_DOMAIN: _UserDomain.FEATURE_VIEW,
        _ObjectField.DB: "db1",
        _ObjectField.SCHEMA: "schema1",
        _ObjectField.PROPERTIES: {_ObjectField.PARENT_NAME: "whatever"},
        _ObjectField.NAME: "name1v1",
    }
    with pytest.raises(SnowparkFetchDataException) as exc:
        Lineage(fake_session)._get_name_and_version(graph_entity)
    assert f"unexpected {_UserDomain.FEATURE_VIEW} name format." in str(exc)

    graph_entity = {
        _ObjectField.USER_DOMAIN: _UserDomain.MODEL,
        _ObjectField.DB: "db1",
        _ObjectField.SCHEMA: "schema1",
        _ObjectField.NAME: "name1",
    }
    with pytest.raises(SnowparkFetchDataException) as exc:
        Lineage(fake_session)._get_name_and_version(graph_entity)
    assert (
        f"missing name/version field for domain {graph_entity[_ObjectField.USER_DOMAIN]}."
        in str(exc)
    )


def test_get_user_entity():
    fake_session = mock.create_autospec(Session, _session_id=123456)
    fake_session._analyzer = mock.Mock()

    graph_entity = {
        _ObjectField.USER_DOMAIN: _UserDomain.MODEL,
        _ObjectField.DB: "db1",
        _ObjectField.SCHEMA: "schema1",
        _ObjectField.PROPERTIES: {_ObjectField.PARENT_NAME: "name1"},
        _ObjectField.NAME: "version1",
        _ObjectField.USER_DOMAIN: _UserDomain.MODEL,
        _ObjectField.CREATED_ON: "123455",
        _ObjectField.STATUS: "Active",
    }

    user_entity = Lineage(fake_session)._get_user_entity(graph_entity)
    assert len(user_entity) == 5
    assert _ObjectField.NAME in user_entity
    assert user_entity[_ObjectField.NAME] == "db1.schema1.name1"
    assert _ObjectField.VERSION in user_entity
    assert user_entity[_ObjectField.VERSION] == "version1"
    assert _ObjectField.DOMAIN in user_entity
    assert user_entity[_ObjectField.DOMAIN] == _UserDomain.MODEL
    assert _ObjectField.CREATED_ON in user_entity

    graph_entity = {
        _ObjectField.USER_DOMAIN: _UserDomain.MODEL,
        _ObjectField.DB: "db1",
        _ObjectField.SCHEMA: "schema1",
        _ObjectField.PROPERTIES: {_ObjectField.PARENT_NAME: "name1"},
        _ObjectField.NAME: "version1",
        _ObjectField.USER_DOMAIN: _UserDomain.MODEL,
        _ObjectField.CREATED_ON: "123455",
    }

    with pytest.raises(SnowparkFetchDataException) as exc:
        Lineage(fake_session)._get_user_entity(graph_entity)
    assert f"missing {_ObjectField.STATUS} property." in str(exc)

    # Tests the work around.
    graph_entity = {
        _ObjectField.USER_DOMAIN: _SnowflakeDomain.TABLE,
        _ObjectField.DB: "db1",
        _ObjectField.SCHEMA: "schema1",
        _ObjectField.REFINED_DOMAIN: _SnowflakeDomain.VIEW,
        _ObjectField.CREATED_ON: "123455",
        _ObjectField.STATUS: "Active",
        _ObjectField.NAME: "name1",
    }

    user_entity = Lineage(fake_session)._get_user_entity(graph_entity)
    assert len(user_entity) == 4
    assert _ObjectField.NAME in user_entity
    assert user_entity[_ObjectField.NAME] == "db1.schema1.name1"
    assert _ObjectField.DOMAIN in user_entity
    assert user_entity[_ObjectField.DOMAIN] == _SnowflakeDomain.VIEW
    assert _ObjectField.CREATED_ON in user_entity

    graph_entity = {
        _ObjectField.USER_DOMAIN: _UserDomain.FEATURE_VIEW,
        _ObjectField.DB: "db1",
        _ObjectField.SCHEMA: "schema1",
        _ObjectField.REFINED_DOMAIN: _SnowflakeDomain.VIEW,
        _ObjectField.CREATED_ON: "123455",
        _ObjectField.STATUS: "Active",
        _ObjectField.NAME: "name1$v1",
    }

    user_entity = Lineage(fake_session)._get_user_entity(graph_entity)
    assert len(user_entity) == 5
    assert _ObjectField.NAME in user_entity
    assert user_entity[_ObjectField.NAME] == "db1.schema1.name1"
    assert _ObjectField.DOMAIN in user_entity
    assert user_entity[_ObjectField.DOMAIN] == _UserDomain.FEATURE_VIEW
    assert _ObjectField.CREATED_ON in user_entity

    graph_entity = {
        _ObjectField.USER_DOMAIN: _SnowflakeDomain.COLUMN,
        _ObjectField.DB: "db1",
        _ObjectField.SCHEMA: "schema1",
        _ObjectField.REFINED_DOMAIN: _SnowflakeDomain.COLUMN,
        _ObjectField.PROPERTIES: {
            _ObjectField.PARENT_NAME: "name1",
            _ObjectField.TABLE_TYPE: "TABLE",
        },
        _ObjectField.CREATED_ON: "123455",
        _ObjectField.STATUS: "Active",
        _ObjectField.NAME: "col1",
    }

    user_entity = Lineage(fake_session)._get_user_entity(graph_entity)
    assert len(user_entity) == 5
    assert _ObjectField.NAME in user_entity
    assert user_entity[_ObjectField.NAME] == "db1.schema1.name1.col1"
    assert _ObjectField.DOMAIN in user_entity
    assert user_entity[_ObjectField.DOMAIN] == _SnowflakeDomain.COLUMN
    assert _ObjectField.CREATED_ON in user_entity
    assert _ObjectField.TYPE in user_entity
    assert user_entity[_ObjectField.TYPE] == "TABLE"


def test_split_fully_qualified_name():
    test_cases_valid = [
        "my_database.public.sales_table",
        '"my_database".public."sales_table"',
        'database."schema"."object"',
        '"database".schema."object"',
        '"database"."schema".object',
        '"data.base".schema.object',
    ]
    for each in test_cases_valid:
        assert 3 == len(_DGQLQueryBuilder.split_fully_qualified_name(each))


def test_is_valid_object_name():
    fake_session = mock.create_autospec(Session, _session_id=123456)
    fake_session._analyzer = mock.Mock()

    test_cases_valid = [
        "my_database.public.sales_table",
        '"my_database".public."sales_table"',
        'database."schema"."object"',
        '"database".schema."object"',
        '"database"."schema".object',
        '"data.base".schema.object',
    ]

    test_cases_invalid = [
        '"database.schema.object"',
        '"my_database"."public.sales_table"',
        '"databa"se".schema."object"',
    ]

    # Assert checks for valid cases
    for case in test_cases_valid:
        Lineage(fake_session)._check_valid_object_name(case, _SnowflakeDomain.TABLE)

    # Assert checks for invalid cases
    for case in test_cases_invalid:
        with pytest.raises(ValueError) as exc:
            Lineage(fake_session)._check_valid_object_name(case, _SnowflakeDomain.TABLE)
        assert "Invalid object name:" in str(exc)


def test_get_feature_view_name():
    test_cases = [
        ("my_database.public.sales_table", "v1", 'my_database.public."SALES_TABLE$v1"'),
        (
            'my_database.public."sales_table"',
            "v1",
            'my_database.public."sales_table$v1"',
        ),
        ('database."schema"."object"', "v1", 'database."schema"."object$v1"'),
        ('"database".schema."object"', "v1", '"database".schema."object$v1"'),
        ('"database"."schema".object', "v1", '"database"."schema"."OBJECT$v1"'),
        ('"database".schema.object', "v1", '"database".schema."OBJECT$v1"'),
        ('"data.base".schema."obj.ect"', "v1", '"data.base".schema."obj.ect$v1"'),
    ]

    for name, version, expected_output in test_cases:
        assert (
            _DGQLQueryBuilder._get_feature_view_name(name, version) == expected_output
        )


def test_build_query():
    query = "select SYSTEM$DGQL('{V(domain: TABLE, name:\"db.sch.name1\") {downstream: E(edgeType:[DATA_LINEAGE, OBJECT_DEPENDENCY],direction:OUT){S {domain, refinedDomain, userDomain, name, properties, schema, db, status, createdOn, id}, T {domain, refinedDomain, userDomain, name, properties, schema, db, status, createdOn, id}}}}')"
    assert query == _DGQLQueryBuilder.build_query(
        _SnowflakeDomain.TABLE,
        [LineageDirection.DOWNSTREAM],
        object_name="db.sch.name1",
    )

    query = 'select SYSTEM$DGQL(\'{V(domain: MODULE, name:"v1", parentName:"db.sch.name1") {downstream: E(edgeType:[DATA_LINEAGE, OBJECT_DEPENDENCY],direction:OUT){S {domain, refinedDomain, userDomain, name, properties, schema, db, status, createdOn, id}, T {domain, refinedDomain, userDomain, name, properties, schema, db, status, createdOn, id}}}}\')'
    assert query == _DGQLQueryBuilder.build_query(
        _UserDomain.MODEL,
        [LineageDirection.DOWNSTREAM],
        object_name="db.sch.name1",
        object_version="v1",
    )

    query = 'select SYSTEM$DGQL(\'{V(domain: TABLE, name:"db.sch.\\\\"NAME1$v1\\\\"") {downstream: E(edgeType:[DATA_LINEAGE, OBJECT_DEPENDENCY],direction:OUT){S {domain, refinedDomain, userDomain, name, properties, schema, db, status, createdOn, id}, T {domain, refinedDomain, userDomain, name, properties, schema, db, status, createdOn, id}}}}\')'
    assert query == _DGQLQueryBuilder.build_query(
        _UserDomain.FEATURE_VIEW,
        [LineageDirection.DOWNSTREAM],
        object_name="db.sch.name1",
        object_version="v1",
    )

    query = (
        'select SYSTEM$DGQL(\'{V(domain: MODULE, name:"\\\\"v1\\\\"", parentName:"\\\\"db\\\\".\\\\"sch\\\\".\\\\"name1\\\\""'
        ") {downstream: E(edgeType:[DATA_LINEAGE, OBJECT_DEPENDENCY],direction:OUT){S {domain, refinedDomain, userDomain, name, "
        "properties, schema, db, status, createdOn, id}, T {domain, refinedDomain, userDomain, name, properties, schema, db, "
        "status, createdOn, id}}}}')"
    )
    assert query == _DGQLQueryBuilder.build_query(
        _UserDomain.MODEL,
        [LineageDirection.DOWNSTREAM],
        object_name='"db"."sch"."name1"',
        object_version='"v1"',
    )

    query = (
        'select SYSTEM$DGQL(\'{V(domain: SNOWSERVICE_INSTANCE, name:"\\\\"db\\\\".\\\\"sch\\\\".\\\\"name1\\\\""'
        ") {downstream: E(edgeType:[DATA_LINEAGE, OBJECT_DEPENDENCY],direction:OUT){S {domain, refinedDomain, userDomain, name, "
        "properties, schema, db, status, createdOn, id}, T {domain, refinedDomain, userDomain, name, properties, schema, db, "
        "status, createdOn, id}}}}')"
    )
    assert query == _DGQLQueryBuilder.build_query(
        _UserDomain.SERVICE,
        [LineageDirection.DOWNSTREAM],
        object_name='"db"."sch"."name1"',
    )

    query = "select SYSTEM$DGQL('{V(domain: TABLE, id:\"12345\") {downstream: E(edgeType:[DATA_LINEAGE, OBJECT_DEPENDENCY],direction:OUT){S {domain, refinedDomain, userDomain, name, properties, schema, db, status, createdOn, id}, T {domain, refinedDomain, userDomain, name, properties, schema, db, status, createdOn, id}}}}')"
    assert query == _DGQLQueryBuilder.build_query(
        _SnowflakeDomain.TABLE, [LineageDirection.DOWNSTREAM], object_id="12345"
    )

    query = 'select SYSTEM$DGQL(\'{V(domain: TABLE, id:"12345", parentId:"6789") {downstream: E(edgeType:[DATA_LINEAGE, OBJECT_DEPENDENCY],direction:OUT){S {domain, refinedDomain, userDomain, name, properties, schema, db, status, createdOn, id}, T {domain, refinedDomain, userDomain, name, properties, schema, db, status, createdOn, id}}}}\')'
    assert query == _DGQLQueryBuilder.build_query(
        _SnowflakeDomain.TABLE,
        [LineageDirection.DOWNSTREAM],
        object_id="12345",
        parent_id="6789",
    )

    with pytest.raises(ValueError) as exc:
        _DGQLQueryBuilder.build_query(
            _SnowflakeDomain.TABLE,
            [LineageDirection.DOWNSTREAM],
            object_id="12345",
            object_name="db.sch.name1",
        )
    assert "Either object_name or object_id must be provided" in str(exc)

    with pytest.raises(ValueError) as exc:
        _DGQLQueryBuilder.build_query(
            _SnowflakeDomain.TABLE, [LineageDirection.DOWNSTREAM]
        )
    assert "Either object_name or object_id must be provided" in str(exc)
