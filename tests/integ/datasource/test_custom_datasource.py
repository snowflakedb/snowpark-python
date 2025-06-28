#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from dataclasses import dataclass

import pytest

from snowflake.snowpark.types import StructType
from tests.utils import RUNNING_ON_JENKINS

from snowflake.snowpark.data_source import InputPartition, DataSourceReader, DataSource


DEPENDENCIES_PACKAGE_UNAVAILABLE = True
try:
    import pymongo  # noqa: F401
    import pandas  # noqa: F401

    DEPENDENCIES_PACKAGE_UNAVAILABLE = False
except ImportError:
    pass


pytestmark = [
    pytest.mark.skipif(DEPENDENCIES_PACKAGE_UNAVAILABLE, reason="Missing 'pymysql'"),
    pytest.mark.skipif(
        RUNNING_ON_JENKINS, reason="cannot access external datasource from jenkins"
    ),
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="feature not available in local testing",
    ),
]


# custom data source definition
class MongoDbFakeDataSourceReader(DataSourceReader):
    def __init__(self, schema) -> None:
        super().__init__(schema)
        self.schema: StructType = schema

    def partitions(self):
        return [AgeInputPartition(25), AgeInputPartition(35)]

    def read(self, partition):
        from pymongo.mongo_client import MongoClient
        from pymongo.server_api import ServerApi

        username = ""
        password = ""
        uri = f"{username}{password}"

        client = MongoClient(uri, server_api=ServerApi("1"))
        res = []
        collection = client["my_test_db"]["my_collection"]
        document = collection.find({"age": partition.age})
        for doc in document:
            res.append((doc["name"], doc["age"], doc["city"]))

        yield res


@dataclass
class AgeInputPartition(InputPartition):
    age: int


class MongoDbFakeDataSource(DataSource):
    """
    An example data source for batch query using the `faker` library.
    """

    @classmethod
    def name(cls):
        return "mongodb_test"

    def schema(self):
        return "name string, age int, city string"

    def reader(self, schema: StructType):
        return MongoDbFakeDataSourceReader(schema)


@pytest.mark.skip("test resource not setup")
def test_custom_datasource(session):

    reader = session.read
    reader.register_custom_data_source(MongoDbFakeDataSource)
    df = reader.format("mongodb_test").load()
    df.show()
