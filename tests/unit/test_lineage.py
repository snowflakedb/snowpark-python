#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

from snowflake.snowpark.exceptions import SnowparkFetchDataException
from snowflake.snowpark.lineage import (
    Lineage,
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
    print(name, version)
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
    print(name, version)
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
    print(name, version)
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
    print(name, version)
    assert name == "db1.schema1.name1"
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
