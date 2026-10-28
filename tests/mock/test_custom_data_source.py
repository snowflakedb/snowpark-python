#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.datasource import DataSource, DataSourceReader, InputPartition
from snowflake.snowpark.exceptions import SnowparkDataframeReaderException
from snowflake.snowpark.types import LongType, StringType, StructField, StructType


class RangeDataSource(DataSource):
    @classmethod
    def name(cls):
        return "range_source"

    def schema(self):
        return "id INT, label STRING"

    def reader(self, schema):
        return RangeReader(int(self.options.get("n", 3)), self.options.get("fail"))


class RangeReader(DataSourceReader):
    def __init__(self, n, fail) -> None:
        self.n = n
        self.fail = fail

    def partitions(self):
        return [InputPartition(i) for i in range(self.n)]

    def read(self, partition):
        if self.fail is not None and partition.value == int(self.fail):
            raise ConnectionError("boom")
        yield {"label": f"row {partition.value}", "ID": partition.value}


def test_read_registered_name(session):
    session.data_source.register(RangeDataSource)
    df = session.read.format("RANGE_SOURCE").option("N", 3).load()
    assert df.schema == StructType(
        [StructField("ID", LongType()), StructField("LABEL", StringType())]
    )
    assert df.sort("id").collect() == [
        Row(ID=0, LABEL="row 0"),
        Row(ID=1, LABEL="row 1"),
        Row(ID=2, LABEL="row 2"),
    ]


def test_read_class_and_user_schema(session):
    df = (
        session.read.format(RangeDataSource)
        .schema("id INT, label STRING NOT NULL")
        .load()
    )
    assert df.schema.fields[1].nullable is False
    assert df.count() == 3


def test_read_no_partitions(session):
    df = session.read.format(RangeDataSource).option("n", 0).load()
    assert df.collect() == []
    assert df.columns == ["ID", "LABEL"]


def test_read_failure(session):
    with pytest.raises(SnowparkDataframeReaderException, match="partition 1"):
        session.read.format(RangeDataSource).option("fail", 1).load()


def test_unsupported_in_local_testing(session):
    with pytest.raises(NotImplementedError, match="local testing"):
        session.read.format(RangeDataSource).option(
            "snowpark.external_access_integration", "EAI"
        ).load()


def test_unknown_format(session):
    with pytest.raises(ValueError, match="Invalid format 'unknown_source'"):
        session.read.format("unknown_source")
