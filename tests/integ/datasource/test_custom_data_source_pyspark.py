#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""Data sources written with PySpark, read by Snowpark without changes."""
import datetime
import decimal
import json

import pytest

from snowflake.snowpark import Row as SnowparkRow
from snowflake.snowpark.types import (
    ArrayType,
    DecimalType,
    LongType,
    MapType,
    TimestampTimeZone,
    TimestampType,
    VariantType,
)

try:
    import pyarrow as pa
except ImportError:
    pa = None

pyspark_datasource = pytest.importorskip("pyspark.sql.datasource")

from pyspark.sql import Row  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    ArrayType as SparkArrayType,
    DecimalType as SparkDecimalType,
    IntegerType as SparkIntegerType,
    MapType as SparkMapType,
    StringType as SparkStringType,
    StructField as SparkStructField,
    StructType as SparkStructType,
    TimestampType as SparkTimestampType,
    VariantType as SparkVariantType,
    VariantVal,
)

pytestmark = [
    pytest.mark.skipif(pa is None, reason="pyarrow is not installed"),
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="server-side ingestion is not supported in local testing",
    ),
]

TS = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)


class SparkUsersDataSource(pyspark_datasource.DataSource):
    """A PySpark data source returning a DDL schema with mixed-case names."""

    @classmethod
    def name(cls):
        return "spark_users"

    def schema(self):
        return "userId INT, userName STRING, createdAt TIMESTAMP"

    def reader(self, schema):
        assert isinstance(schema, SparkStructType)
        return SparkUsersReader(schema, int(self.options.get("n", 4)))


class SparkUsersReader(pyspark_datasource.DataSourceReader):
    def __init__(self, schema, n) -> None:
        self.field_names = schema.fieldNames()
        self.n = n

    def partitions(self):
        return [pyspark_datasource.InputPartition(i) for i in range(0, self.n, 2)]

    def read(self, partition):
        # not an assert, which pytest rewrites to depend on _pytest in the UDTF
        if self.field_names != ["userId", "userName", "createdAt"]:
            raise ValueError(f"unexpected schema {self.field_names}")
        for i in range(partition.value, min(partition.value + 2, self.n)):
            yield Row(userName=f"user {i}", createdAt=TS, userId=i)


class SparkTypesDataSource(pyspark_datasource.DataSource):
    """A PySpark data source returning a StructType schema, with the default partitions()."""

    def schema(self):
        return SparkStructType(
            [
                SparkStructField("id", SparkIntegerType()),
                SparkStructField("price", SparkDecimalType(10, 2)),
                SparkStructField("tags", SparkArrayType(SparkStringType())),
                SparkStructField(
                    "attrs", SparkMapType(SparkStringType(), SparkIntegerType())
                ),
                SparkStructField("v", SparkVariantType()),
                SparkStructField("ts", SparkTimestampType()),
            ]
        )

    def reader(self, schema):
        return SparkTypesReader()


class SparkTypesReader(pyspark_datasource.DataSourceReader):
    def read(self, partition):
        yield (
            1,
            decimal.Decimal("1.25"),
            ["a", "b"],
            {"k": 1},
            VariantVal.parseJson('{"nested": [1, 2]}'),
            TS,
        )
        yield Row(
            id=2,
            price=None,
            tags=None,
            attrs=None,
            v=VariantVal.parseJson("null"),
            ts=None,
        )


@pytest.fixture(scope="module")
def external_access_integration(session):
    integrations = session.sql("show external access integrations").collect()
    if not integrations:
        pytest.skip("no external access integration is available")
    return integrations[0]["name"]


def _expected_users(n):
    return [SnowparkRow(USERID=i, USERNAME=f"user {i}", CREATEDAT=TS) for i in range(n)]


def _check_types(df):
    assert [type(f.datatype) for f in df.schema.fields] == [
        LongType,
        DecimalType,
        ArrayType,
        MapType,
        VariantType,
        TimestampType,
    ]
    assert df.schema.fields[1].datatype == DecimalType(10, 2)
    assert df.schema.fields[5].datatype == TimestampType(TimestampTimeZone.LTZ)
    rows = df.sort("id").collect()
    assert rows[0]["ID"] == 1
    assert rows[0]["PRICE"] == decimal.Decimal("1.25")
    assert json.loads(rows[0]["TAGS"]) == ["a", "b"]
    assert json.loads(rows[0]["ATTRS"]) == {"k": 1}
    assert json.loads(rows[0]["V"]) == {"nested": [1, 2]}
    assert rows[0]["TS"] == TS
    assert list(rows[1]) == [2, None, None, None, None, None]


def _read_users(session, **options):
    session.data_source.register(SparkUsersDataSource)
    try:
        df = session.read.format("spark_users").options(options).load()
        assert df.schema.names == ["USERID", "USERNAME", "CREATEDAT"]
        assert df.schema.fields[2].datatype == TimestampType(TimestampTimeZone.LTZ)
        assert df.sort("userId").collect() == _expected_users(4)
    finally:
        session.data_source.unregister("spark_users")


def test_pyspark_data_source_local_ingestion(session):
    _read_users(session)


def test_pyspark_data_source_types_local_ingestion(session):
    _check_types(session.read.format(SparkTypesDataSource).load())


def test_pyspark_user_schema(session):
    schema = SparkStructType(
        [
            SparkStructField("userId", SparkIntegerType()),
            SparkStructField("userName", SparkStringType()),
            SparkStructField("createdAt", SparkTimestampType()),
        ]
    )
    df = session.read.format(SparkUsersDataSource).schema(schema).load()
    assert df.sort("userId").collect() == _expected_users(4)


def test_pyspark_data_source_udtf_ingestion(session, external_access_integration):
    _read_users(
        session,
        **{"snowpark.external_access_integration": external_access_integration},
    )


def test_pyspark_data_source_types_udtf_ingestion(session, external_access_integration):
    _check_types(
        session.read.format(SparkTypesDataSource)
        .option("snowpark.external_access_integration", external_access_integration)
        .load()
    )
