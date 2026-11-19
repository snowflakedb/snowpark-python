#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import json

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


def test_server_side_reads_are_not_supported(session):
    with pytest.raises(NotImplementedError, match="server-side reads"):
        session.read.format(RangeDataSource).option(
            "snowpark.external_access_integration", "EAI"
        ).load()


def test_unknown_format(session):
    with pytest.raises(ValueError, match="Invalid format 'unknown_source'"):
        session.read.format("unknown_source")


class ValueReader(DataSourceReader):
    def __init__(self, rows) -> None:
        self.rows = rows

    def read(self, partition):
        yield from self.rows


def _value_source(rows):
    class ValueSource(DataSource):
        @classmethod
        def name(cls):
            return "value_source"

        def schema(self):
            return "id INT, price DECIMAL(10, 2)"

        def reader(self, schema):
            return ValueReader(rows)

    return ValueSource


def test_mock_values_are_converted_like_local_ingestion(session):
    from decimal import Decimal

    df = session.read.format(_value_source([(2.0, Decimal("1.005"))])).load()
    assert df.collect() == [Row(ID=2, PRICE=Decimal("1.01"))]
    with pytest.raises(SnowparkDataframeReaderException, match="not an integer"):
        session.read.format(_value_source([(1.7, None)])).load()


@pytest.fixture
def ast_session(session):
    from snowflake.snowpark._internal.utils import AstFlagSource, set_ast_state

    original = session.ast_enabled
    set_ast_state(AstFlagSource.TEST, True)
    session.ast_enabled = True
    try:
        yield session
    finally:
        set_ast_state(AstFlagSource.TEST, original)
        session.ast_enabled = original


def test_mock_read_keeps_ast(ast_session):
    df = ast_session.read.format(RangeDataSource).load()
    assert df._ast_id is not None
    # transformations fail on a DataFrame without an AST when the AST is enabled
    assert df.filter(df.id > 0).sort("id").count() == 2


def test_semi_structured_projection_keeps_ast(ast_session):
    from snowflake.snowpark._internal.data_source.custom.conversion import (
        parse_semi_structured_columns,
    )
    from snowflake.snowpark.types import TimestampType, VariantType

    base = ast_session.create_dataframe(
        [[1, '{"a": 1}', "2024-01-02 03:04:05"]], schema=["id", "v", "ts"]
    )
    schema = StructType(
        [
            StructField("ID", LongType()),
            StructField("V", VariantType()),
            StructField("TS", TimestampType()),
        ]
    )
    df = parse_semi_structured_columns(base, schema, True)
    assert df._ast_id is not None
    assert df.select("id")._ast_id is not None
    # without the AST, it is not emitted
    assert parse_semi_structured_columns(base, schema)._ast_id is None


def test_mock_unpicklable_reader_fails_like_live_reads(session):
    import threading

    class LockReader(ValueReader):
        def __init__(self) -> None:
            super().__init__([(1, None)])
            self.lock = threading.Lock()

    class LockSource(DataSource):
        @classmethod
        def name(cls):
            return "lock_source"

        def schema(self):
            return "id INT"

        def reader(self, schema):
            return LockReader()

    with pytest.raises(TypeError, match="must be picklable"):
        session.read.format(LockSource).load()


def test_mock_null_in_not_null_column(session):
    source = _value_source([(1, None)])
    df = session.read.format(source).schema("id INT NOT NULL, price DECIMAL(10, 2)")
    assert df.load().count() == 1
    with pytest.raises(SnowparkDataframeReaderException, match="NOT NULL column ID"):
        session.read.format(_value_source([(None, None)])).schema(
            "id INT NOT NULL, price DECIMAL(10, 2)"
        ).load()


def test_mock_semi_structured_values_are_converted_like_udtf_ingestion(session):
    from snowflake.snowpark.types import ArrayType, MapType, VariantType

    schema = StructType(
        [
            StructField("V", VariantType()),
            StructField("A", ArrayType()),
            StructField("M", MapType(StringType(), LongType())),
        ]
    )

    class SemiSource(DataSource):
        @classmethod
        def name(cls):
            return "semi_source"

        def schema(self):
            return schema

        def reader(self, schema):
            return ValueReader([(Row(a=1), Row(1, 2), Row(k=1))])

    rows = session.read.format(SemiSource).load().collect()
    assert json.loads(rows[0]["V"]) == {"a": 1}
    assert json.loads(rows[0]["A"]) == [1, 2]
    assert json.loads(rows[0]["M"]) == {"k": 1}


def test_mock_boolean_values(session):
    class BoolSource(DataSource):
        @classmethod
        def name(cls):
            return "bool_source"

        def schema(self):
            return "b BOOLEAN"

        def reader(self, schema):
            return ValueReader([("true",)])

    with pytest.raises(SnowparkDataframeReaderException, match="not a boolean"):
        session.read.format(BoolSource).load()


def test_last_option_wins_regardless_of_case(session):
    df = (
        session.read.format(RangeDataSource)
        .option("n", 1)
        .option("N", 2)
        .option("n", 3)
    )
    assert df._raw_options == {"n": 3}
    assert df.load().count() == 3


def test_class_with_reserved_name_can_be_read_directly(session):
    class Json(RangeDataSource):
        @classmethod
        def name(cls):
            return cls.__name__

    assert Json.name().lower() == "json"
    assert session.read.format(Json).load().count() == 3
    with pytest.raises(ValueError, match="reserved"):
        session.data_source.register(Json)


def test_schema_string_of_a_data_source_is_the_same_as_reader_schema(session):
    ddl = 'camelCase STRING, "my col" INT, "QuotedCase" INT'
    received = []

    class DdlSource(DataSource):
        @classmethod
        def name(cls):
            return "ddl_source"

        def schema(self):
            return ddl

        def reader(self, schema):
            received.append(schema)
            return ValueReader([("a", 1, 2)])

    df = session.read.format(DdlSource).load()
    expected = session.read.schema(ddl)._user_schema
    assert received == [expected]
    assert [(f._name, f.name) for f in received[0].fields] == [
        (f._name, f.name) for f in expected.fields
    ]
    assert df.columns == ["CAMELCASE", '"my col"', '"QuotedCase"']
