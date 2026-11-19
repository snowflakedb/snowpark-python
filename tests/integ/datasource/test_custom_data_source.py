#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import datetime
import decimal
import json

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.datasource import DataSource, DataSourceReader, InputPartition
from snowflake.snowpark.exceptions import (
    SnowparkDataframeReaderException,
    SnowparkSQLException,
)
from snowflake.snowpark.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampTimeZone,
    TimestampType,
    TimeType,
    VariantType,
)

try:
    import pyarrow as pa
except ImportError:
    pa = None

pytestmark = [
    pytest.mark.skipif(pa is None, reason="pyarrow is not installed"),
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="tested in tests/mock/test_custom_data_source.py",
    ),
]

TZ = datetime.timezone(datetime.timedelta(hours=-8))
AWARE = datetime.datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=TZ)
NAIVE = datetime.datetime(2024, 1, 2, 3, 4, 5, 123456)


class RangeDataSource(DataSource):
    @classmethod
    def name(cls):
        return "range_source"

    def schema(self):
        return "id INT, label STRING"

    def reader(self, schema):
        return RangeReader(
            int(self.options.get("n", 10)),
            int(self.options.get("step", 3)),
            self.options.get("fail"),
        )


class RangeReader(DataSourceReader):
    def __init__(self, n, step, fail) -> None:
        self.n = n
        self.step = step
        self.fail = fail

    def partitions(self):
        return [InputPartition(i) for i in range(0, self.n, self.step)]

    def read(self, partition):
        for i in range(partition.value, min(partition.value + self.step, self.n)):
            if self.fail is not None and i == int(self.fail):
                raise ValueError(f"cannot read {i}")
            yield (i, f"row {i}")


def _check_range_schema(df):
    assert [(f.name, type(f.datatype)) for f in df.schema.fields] == [
        ("ID", LongType),
        ("LABEL", StringType),
    ]


def _expected_range(n):
    return [Row(ID=i, LABEL=f"row {i}") for i in range(n)]


class ArrowDataSource(DataSource):
    def schema(self):
        return StructType(
            [
                StructField("ID", LongType(), False),
                StructField('"Mixed Case"', StringType()),
            ]
        )

    def reader(self, schema):
        return ArrowReader()


class ArrowReader(DataSourceReader):
    def partitions(self):
        return [InputPartition(0), InputPartition(100)]

    def read(self, partition):
        start = partition.value
        yield pa.record_batch(
            {
                "Mixed Case": pa.array([f"a{start}", f"b{start}"]),
                "id": pa.array([start, start + 1], pa.int32()),
            }
        )
        yield pa.table({"id": [start + 2], "Mixed Case": [None]})
        yield {"ID": start + 3, "Mixed Case": "dict"}


ALL_TYPES_SCHEMA = (
    "i BIGINT, d DECIMAL(10,2), f DOUBLE, b BOOLEAN, bin BINARY, dt DATE, t TIME, "
    "ntz TIMESTAMP_NTZ, ltz TIMESTAMP_LTZ, tz TIMESTAMP_TZ, a ARRAY<INT>, "
    'm MAP<STRING,INT>, o STRUCT<x: INT>, v VARIANT, "Quoted" STRING'
)


class AllTypesDataSource(DataSource):
    def schema(self):
        return ALL_TYPES_SCHEMA

    def reader(self, schema):
        return AllTypesReader()


class AllTypesReader(DataSourceReader):
    def partitions(self):
        return [InputPartition("rows"), InputPartition("arrow")]

    def read(self, partition):
        if partition.value == "rows":
            yield (
                1,
                decimal.Decimal("1.234"),
                1.5,
                True,
                b"\x01\x02",
                datetime.date(2024, 1, 2),
                datetime.time(1, 2, 3),
                NAIVE,
                AWARE,
                AWARE,
                [1, 2],
                {"k": 1},
                Row(x=1),
                {"nested": [1, {"a": None}]},
                "q",
            )
            yield {"I": 2, "Quoted": "only quoted"}
        else:
            yield pa.record_batch(
                {
                    "i": pa.array([3]),
                    "d": pa.array([1.0]),
                    "f": pa.array([2]),
                    "b": pa.array([False]),
                    "bin": pa.array([b"x"]),
                    "dt": pa.array([datetime.date(2024, 1, 1)]),
                    "t": pa.array([datetime.time(1)]),
                    "ntz": pa.array([NAIVE]),
                    "ltz": pa.array([AWARE]),
                    "tz": pa.array([AWARE.isoformat()]),
                    "a": pa.array([[3]]),
                    "m": pa.array([[("a", 1)]], pa.map_(pa.string(), pa.int64())),
                    "o": pa.array([{"x": 3}]),
                    "v": pa.array(["s"]),
                    "Quoted": pa.array(["q3"]),
                }
            )


def _check_all_types(df):
    assert [f.name for f in df.schema.fields] == [
        "I",
        "D",
        "F",
        "B",
        "BIN",
        "DT",
        "T",
        "NTZ",
        "LTZ",
        "TZ",
        "A",
        "M",
        "O",
        "V",
        '"Quoted"',
    ]
    types = [f.datatype for f in df.schema.fields]
    assert types[:8] == [
        LongType(),
        DecimalType(10, 2),
        DoubleType(),
        BooleanType(),
        BinaryType(),
        DateType(),
        TimeType(),
        TimestampType(TimestampTimeZone.NTZ),
    ]
    assert types[8] == TimestampType(TimestampTimeZone.LTZ)
    assert types[9] == TimestampType(TimestampTimeZone.TZ)
    assert types[13] == VariantType()

    rows = df.sort("I").collect()
    assert len(rows) == 3
    r1, r2, r3 = rows
    assert r1["D"] == decimal.Decimal("1.23")
    assert r1["F"] == 1.5 and r1["B"] is True and bytes(r1["BIN"]) == b"\x01\x02"
    assert r1["DT"] == datetime.date(2024, 1, 2)
    assert r1["T"] == datetime.time(1, 2, 3)
    assert r1["NTZ"] == NAIVE
    assert r1["LTZ"] == AWARE
    assert r1["TZ"] == AWARE and r1["TZ"].utcoffset() == datetime.timedelta(hours=-8)
    assert json.loads(r1["A"]) == [1, 2]
    assert json.loads(r1["M"]) == {"k": 1}
    assert json.loads(r1["O"]) == {"x": 1}
    assert json.loads(r1["V"]) == {"nested": [1, {"a": None}]}
    assert r1["Quoted"] == "q"

    assert r2["Quoted"] == "only quoted"
    assert all(v is None for k, v in r2.as_dict().items() if k not in ("I", "Quoted"))

    assert r3["D"] == decimal.Decimal("1.00") and r3["F"] == 2.0
    assert r3["NTZ"] == NAIVE
    assert r3["LTZ"] == AWARE
    assert r3["TZ"] == AWARE
    assert json.loads(r3["A"]) == [3]
    assert json.loads(r3["M"]) == {"a": 1}
    assert json.loads(r3["O"]) == {"x": 3}
    assert json.loads(r3["V"]) == "s"


@pytest.fixture(scope="module")
def external_access_integration(session):
    integrations = session.sql("show external access integrations").collect()
    if not integrations:
        pytest.skip("no external access integration is available")
    return integrations[0]["name"]


@pytest.fixture(autouse=True)
def register_range_source(session):
    session.data_source.register(RangeDataSource)
    yield
    if "range_source" in session.data_source.registered_names():
        session.data_source.unregister("range_source")


@pytest.mark.parametrize("fetch_with_process", [False, True])
def test_local_ingestion(session, fetch_with_process):
    df = (
        session.read.format("range_source")
        .option("n", 10)
        .option("snowpark.max_workers", 2)
        .option("snowpark.fetch_with_process", fetch_with_process)
        .load()
    )
    _check_range_schema(df)
    assert df.sort("id").collect() == _expected_range(10)


def test_local_ingestion_small_batches(session):
    df = (
        session.read.format(RangeDataSource)
        .options({"n": 7, "step": 7, "snowpark.batch_size": 2})
        .load()
    )
    assert df.sort("id").collect() == _expected_range(7)


def test_load_path_option(session):
    class PathDataSource(DataSource):
        def schema(self):
            return "path STRING"

        def reader(self, schema):
            return PathReader(self.options["path"])

    class PathReader(DataSourceReader):
        def __init__(self, path) -> None:
            self.path = path

        def read(self, partition):
            yield (self.path,)

    assert session.read.format(PathDataSource).load("/some/path").collect() == [
        Row(PATH="/some/path")
    ]


def test_user_schema(session):
    df = (
        session.read.format("range_source")
        .schema(
            StructType(
                [
                    StructField("ID", LongType(), False),
                    StructField("LABEL", StringType()),
                ]
            )
        )
        .option("n", 3)
        .load()
    )
    assert df.schema.fields[0].nullable is False
    assert df.sort("id").collect() == _expected_range(3)


def test_no_partitions(session):
    df = session.read.format("range_source").option("n", 0).load()
    assert df.collect() == []
    assert df.columns == ["ID", "LABEL"]


def test_arrow_data(session):
    df = session.read.format(ArrowDataSource).load()
    assert df.columns == ["ID", '"Mixed Case"']
    assert df.schema.fields[0].nullable is False
    assert df.sort("id").collect() == [
        Row(0, "a0"),
        Row(1, "b0"),
        Row(2, None),
        Row(3, "dict"),
        Row(100, "a100"),
        Row(101, "b100"),
        Row(102, None),
        Row(103, "dict"),
    ]


def test_all_types_local_ingestion(session):
    _check_all_types(session.read.format(AllTypesDataSource).load())


def test_failing_partition(session):
    with pytest.raises(SnowparkDataframeReaderException, match="cannot read 4"):
        session.read.format("range_source").option("n", 10).option("fail", 4).load()


def test_type_mismatch(session):
    class BadDataSource(DataSource):
        def schema(self):
            return "id INT"

        def reader(self, schema):
            return BadReader()

    class BadReader(DataSourceReader):
        def read(self, partition):
            yield ("not a number",)

    with pytest.raises(
        SnowparkDataframeReaderException, match="column ID .* partition 0"
    ):
        session.read.format(BadDataSource).load()


def test_unpicklable_reader(session):
    import threading

    class LockDataSource(DataSource):
        def schema(self):
            return "id INT"

        def reader(self, schema):
            return LockReader()

    class LockReader(DataSourceReader):
        def __init__(self) -> None:
            self.lock = threading.Lock()

        def read(self, partition):
            yield (1,)

    with pytest.raises(TypeError, match="must be picklable"):
        session.read.format(LockDataSource).load()


def test_invalid_engine_option(session):
    with pytest.raises(ValueError, match="Unknown option 'snowpark.foo'"):
        session.read.format("range_source").option("snowpark.foo", 1).load()


def test_udtf_ingestion(session, external_access_integration):
    df = (
        session.read.format("range_source")
        .option("n", 10)
        .option("snowpark.external_access_integration", external_access_integration)
        .load()
    )
    _check_range_schema(df)
    assert df.sort("id").collect() == _expected_range(10)


def test_all_types_udtf_ingestion(session, external_access_integration):
    df = (
        session.read.format(AllTypesDataSource)
        .option("snowpark.external_access_integration", external_access_integration)
        .option("snowpark.packages", ["pyarrow"])
        .load()
    )
    _check_all_types(df)


def test_udtf_ingestion_failing_partition(session, external_access_integration):
    df = (
        session.read.format("range_source")
        .options(
            {
                "n": 10,
                "fail": 4,
                "snowpark.external_access_integration": external_access_integration,
            }
        )
        .load()
    )
    with pytest.raises(SnowparkSQLException, match="partition 1 .*cannot read 4"):
        df.collect()


class OptionsDataSource(DataSource):
    def schema(self):
        return "key STRING, value STRING"

    def reader(self, schema):
        return OptionsReader(self.options)


class OptionsReader(DataSourceReader):
    def __init__(self, options) -> None:
        # a common pattern: the reader keeps the options, a Snowpark CaseInsensitiveDict
        self.options = options

    def read(self, partition):
        yield ("prefix", self.options["PREFIX"])


def test_udtf_ingestion_reader_keeps_options(session, external_access_integration):
    df = (
        session.read.format(OptionsDataSource)
        .option("prefix", "abc")
        .option("snowpark.external_access_integration", external_access_integration)
        .load()
    )
    assert df.collect() == [Row(KEY="prefix", VALUE="abc")]
