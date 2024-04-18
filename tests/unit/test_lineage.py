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
        _ObjectField.USER_DOMAIN: _UserDomain.MODEL.value,
        _ObjectField.DB: "db1",
        _ObjectField.SCHEMA: "schema1",
        _ObjectField.PARENT_NAME: "name1",
        _ObjectField.NAME: "version1",
    }
    name, version = Lineage(fake_session)._get_name_and_version(graph_entity)
    print(name, version)
    assert name == "db1.schema1.name1"
    assert version == "version1"

    graph_entity = {
        _ObjectField.USER_DOMAIN: _SnowflakeDomain.DATASET.value,
        _ObjectField.DB: "db1",
        _ObjectField.SCHEMA: "schema1",
        _ObjectField.PARENT_NAME: "name1",
        _ObjectField.NAME: "version1",
    }
    name, version = Lineage(fake_session)._get_name_and_version(graph_entity)
    print(name, version)
    assert name == "db1.schema1.name1"
    assert version == "version1"

    graph_entity = {
        _ObjectField.USER_DOMAIN: _SnowflakeDomain.TABLE.value,
        _ObjectField.DB: "db1",
        _ObjectField.SCHEMA: "schema1",
        _ObjectField.PARENT_NAME: "whatever",
        _ObjectField.NAME: "name1",
    }
    name, version = Lineage(fake_session)._get_name_and_version(graph_entity)
    print(name, version)
    assert name == "db1.schema1.name1"
    assert version is None

    graph_entity = {
        _ObjectField.USER_DOMAIN: _UserDomain.FEATURE_VIEW.value,
        _ObjectField.DB: "db1",
        _ObjectField.SCHEMA: "schema1",
        _ObjectField.PARENT_NAME: "whatever",
        _ObjectField.NAME: "name1$v1",
    }
    name, version = Lineage(fake_session)._get_name_and_version(graph_entity)
    print(name, version)
    assert name == "db1.schema1.name1"
    assert version == "v1"

    graph_entity = {
        _ObjectField.USER_DOMAIN: _UserDomain.FEATURE_VIEW.value,
        _ObjectField.DB: "db1",
        _ObjectField.SCHEMA: "schema1",
        _ObjectField.PARENT_NAME: "whatever",
        _ObjectField.NAME: "name1v1",
    }
    with pytest.raises(SnowparkFetchDataException) as exc:
        Lineage(fake_session)._get_name_and_version(graph_entity)
    assert f"unexpected {_UserDomain.FEATURE_VIEW.value} name format." in str(exc)

    graph_entity = {
        _ObjectField.USER_DOMAIN: _UserDomain.MODEL.value,
        _ObjectField.DB: "db1",
        _ObjectField.SCHEMA: "schema1",
        _ObjectField.NAME: "name1",
    }
    with pytest.raises(SnowparkFetchDataException) as exc:
        Lineage(fake_session)._get_name_and_version(graph_entity)
    assert (
        f"missing version field for domain {graph_entity[_ObjectField.USER_DOMAIN]}."
        in str(exc)
    )
