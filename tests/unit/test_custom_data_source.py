#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import datetime
import decimal
import pickle
import threading
from unittest import mock

import cloudpickle
import pytest

from snowflake.snowpark._internal.data_source.custom import conversion
from snowflake.snowpark._internal.data_source.custom.conversion import (
    FieldMatcher,
    normalize_items,
    staging_schema,
    to_row_tuple,
    to_udtf_row,
    udtf_output_schema,
    validate_schema,
)
from snowflake.snowpark._internal.data_source.custom.ingestion import (
    CustomPartitionReader,
    _dumps,
    _local_modules_of,
    _pickle_by_value,
    _resolve_udtf_packages,
)
from snowflake.snowpark._internal.data_source.custom.options import (
    DEFAULT_BATCH_SIZE,
    CaseInsensitiveDict,
    split_options,
)
from snowflake.snowpark.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceRegistration,
    InputPartition,
)
from snowflake.snowpark.exceptions import SnowparkDataframeReaderException
from snowflake.snowpark.row import Row
from snowflake.snowpark.types import (
    ArrayType,
    DecFloatType,
    GeographyType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampTimeZone,
    TimestampType,
    VariantType,
)

try:
    import pyarrow as pa

    is_pyarrow_available = True
except ImportError:
    is_pyarrow_available = False

requires_pyarrow = pytest.mark.skipif(
    not is_pyarrow_available, reason="pyarrow is not installed"
)

SCHEMA = StructType(
    [
        StructField("ID", LongType()),
        StructField('"Name"', StringType()),
    ]
)


class SimpleDataSource(DataSource):
    def schema(self):
        return "id INT"

    def reader(self, schema):
        return SimpleReader()


class SimpleReader(DataSourceReader):
    def read(self, partition):
        yield (1,)


def test_case_insensitive_dict():
    d = CaseInsensitiveDict({"Foo": "1"})
    assert d["FOO"] == "1"
    assert "foo" in d
    assert d.get("fOO") == "1"
    d["BAR"] = "2"
    assert list(d.keys()) == ["foo", "bar"]
    assert d.pop("Bar") == "2"
    assert isinstance(d.copy(), CaseInsensitiveDict)
    assert pickle.loads(pickle.dumps(d))["FOO"] == "1"


def test_split_options():
    user, engine = split_options(
        {
            "Path": "/a",
            "flag": True,
            "num": 5,
            "none": None,
            "snowpark.External_Access_Integration": "EAI",
            "snowpark.packages": "requests, pyarrow",
            "snowpark.imports": '["@stage/a.py"]',
            "snowpark.secrets": {"cred": "my_secret"},
            "snowpark.max_workers": "4",
            "snowpark.fetch_with_process": "true",
            "snowpark.batch_size": 10,
        }
    )
    assert dict(user) == {"path": "/a", "flag": "true", "num": "5", "none": None}
    assert engine.external_access_integration == "EAI"
    assert engine.use_udtf_ingestion
    assert engine.packages == ["requests", "pyarrow"]
    assert engine.imports == ["@stage/a.py"]
    assert engine.secrets == {"cred": "my_secret"}
    assert engine.max_workers == 4
    assert engine.fetch_with_process is True
    assert engine.batch_size == 10


def test_split_options_defaults():
    user, engine = split_options({})
    assert dict(user) == {}
    assert not engine.use_udtf_ingestion
    assert engine.max_workers >= 1
    assert engine.fetch_with_process is False
    assert engine.batch_size == DEFAULT_BATCH_SIZE


@pytest.mark.parametrize(
    "options, error, match",
    [
        ({"snowpark.unknown": 1}, ValueError, "Unknown option 'snowpark.unknown'"),
        ({"snowpark.max_workers": 0}, ValueError, "positive integer"),
        ({"snowpark.max_workers": "abc"}, TypeError, "must be an integer"),
        ({"snowpark.batch_size": True}, TypeError, "must be an integer"),
        ({"snowpark.fetch_with_process": "yes"}, TypeError, "must be a boolean"),
        ({"snowpark.packages": 1}, TypeError, "list of strings"),
        ({"snowpark.packages": [1]}, TypeError, "list of strings"),
        ({"snowpark.secrets": {"a": 1}}, TypeError, "dictionary of strings"),
        ({1: "a"}, TypeError, "Option names must be strings"),
    ],
)
def test_split_options_invalid(options, error, match):
    with pytest.raises(error, match=match):
        split_options(options)


def test_input_partition():
    assert InputPartition(1) == InputPartition(1)
    assert InputPartition(1) != InputPartition(2)
    assert repr(InputPartition(1)) == "InputPartition(value=1)"
    assert SimpleReader().partitions() == [InputPartition(None)]


def test_data_source_defaults():
    class NoSchema(DataSource):
        pass

    ds = NoSchema({"a": "1"})
    assert ds.options == {"a": "1"}
    assert NoSchema.name() == "NoSchema"
    with pytest.raises(NotImplementedError, match="schema"):
        ds.schema()
    with pytest.raises(NotImplementedError, match="reader"):
        ds.reader(SCHEMA)


def test_registration():
    registration = DataSourceRegistration(mock.MagicMock())
    registration.register(SimpleDataSource)
    assert registration.registered_names() == ["simpledatasource"]
    assert registration._get(" SIMPLEDATASOURCE ") is SimpleDataSource
    assert registration._get("other") is None
    registration.unregister("SimpleDataSource")
    assert registration.registered_names() == []
    with pytest.raises(KeyError, match="not registered"):
        registration.unregister("SimpleDataSource")


def test_registration_invalid():
    registration = DataSourceRegistration(mock.MagicMock())

    class Csv(SimpleDataSource):
        @classmethod
        def name(cls):
            return "CSV"

    class Empty(SimpleDataSource):
        @classmethod
        def name(cls):
            return " "

    with pytest.raises(TypeError, match="subclass"):
        registration.register(SimpleReader)
    with pytest.raises(TypeError, match="subclass"):
        registration.register(SimpleDataSource({}))
    with pytest.raises(ValueError, match="reserved"):
        registration.register(Csv)
    with pytest.raises(ValueError, match="non-empty string"):
        registration.register(Empty)


def test_registration_thread_safe():
    registration = DataSourceRegistration(mock.MagicMock())
    classes = [type(f"Source{i}", (SimpleDataSource,), {}) for i in range(50)]
    threads = [
        threading.Thread(target=registration.register, args=(c,)) for c in classes
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(registration.registered_names()) == 50


def test_validate_schema():
    validate_schema(SCHEMA)
    with pytest.raises(ValueError, match="at least one field"):
        validate_schema(StructType([]))
    with pytest.raises(NotImplementedError, match="GeographyType"):
        validate_schema(StructType([StructField("G", GeographyType())]))
    with pytest.raises(NotImplementedError, match="DecFloat"):
        validate_schema(StructType([StructField("D", DecFloatType())]))


def test_staging_and_udtf_schema():
    schema = StructType(
        [
            StructField("A", ArrayType(LongType())),
            StructField("M", MapType(StringType(), LongType())),
            StructField("V", VariantType()),
            StructField("T", TimestampType(TimestampTimeZone.TZ), False),
            StructField("I", LongType()),
        ]
    )
    assert [f.datatype for f in staging_schema(schema).fields] == [
        StringType(),
        StringType(),
        StringType(),
        TimestampType(TimestampTimeZone.TZ),
        LongType(),
    ]
    udtf_schema = udtf_output_schema(schema)
    assert [f.datatype for f in udtf_schema.fields] == [
        ArrayType(LongType()),
        MapType(StringType(), LongType()),
        VariantType(),
        StringType(),
        LongType(),
    ]
    assert udtf_schema.fields[3].nullable is False


def test_field_matcher():
    matcher = FieldMatcher(SCHEMA)
    assert matcher.names == ["ID", "Name"]
    assert matcher.match(["name", "id"]) == [1, None]
    assert matcher.match(["Name", "id"]) == [1, 0]
    assert matcher.match(["ID", "id", "Name"]) == [0, 2]


def test_to_row_tuple():
    matcher = FieldMatcher(SCHEMA)
    assert to_row_tuple((1, "a"), matcher) == (1, "a")
    assert to_row_tuple([1, "a"], matcher) == (1, "a")
    assert to_row_tuple(Row(1, "a"), matcher) == (1, "a")
    assert to_row_tuple(Row(Name="a", id=1), matcher) == (1, "a")
    assert to_row_tuple({"Name": "a"}, matcher) == (None, "a")
    assert to_row_tuple({"id": 1, "Name": "a", "extra": 0}, matcher) == (1, "a")
    with pytest.raises(ValueError, match="2 fields"):
        to_row_tuple((1,), matcher)
    with pytest.raises(ValueError, match="without the schema fields"):
        to_row_tuple(Row(id=1, name="a"), matcher)
    with pytest.raises(TypeError, match="must yield"):
        to_row_tuple(1, matcher)


def test_to_row_tuple_pyspark_row_like():
    class FakeSparkRow(tuple):
        def __new__(cls, **kwargs):
            row = tuple.__new__(cls, kwargs.values())
            row.__fields__ = list(kwargs.keys())
            return row

    matcher = FieldMatcher(SCHEMA)
    assert to_row_tuple(FakeSparkRow(Name="a", ID=1), matcher) == (1, "a")


def test_to_udtf_row():
    ts = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)
    assert to_udtf_row((1, ts, None), [1, 2]) == (
        1,
        "2024-01-02T03:04:05+00:00",
        None,
    )
    assert to_udtf_row((1,), []) == (1,)
    assert to_udtf_row((Row(a=Row(1, 2)), None), [], [0, 1]) == ({"a": [1, 2]}, None)


@requires_pyarrow
def test_rows_to_arrow_table():
    schema = StructType(
        [
            StructField("I", LongType()),
            StructField("D", conversion.DecimalType(5, 2)),
            StructField("DT", conversion.DateType()),
            StructField("TS", TimestampType()),
            StructField("V", VariantType()),
            StructField("A", ArrayType(LongType())),
        ]
    )
    matcher = FieldMatcher(schema)
    ts = datetime.datetime(2024, 1, 2, 3, 4, 5)
    table = conversion.rows_to_arrow_table(
        [
            (1, 1.235, "2024-01-02", ts, {"a": Row(x=1)}, [1, 2]),
            (None, None, None, None, None, None),
        ],
        matcher,
        0,
    )
    assert table.column_names == ["I", "D", "DT", "TS", "V", "A"]
    assert table.to_pylist()[0] == {
        "I": 1,
        "D": decimal.Decimal("1.24"),
        "DT": datetime.date(2024, 1, 2),
        "TS": "2024-01-02T03:04:05",
        "V": '{"a": {"x": 1}}',
        "A": "[1, 2]",
    }
    assert set(table.to_pylist()[1].values()) == {None}


@requires_pyarrow
def test_rows_to_arrow_table_conversion_error():
    matcher = FieldMatcher(SCHEMA)
    with pytest.raises(
        SnowparkDataframeReaderException, match="column ID .* partition 3"
    ):
        conversion.rows_to_arrow_table([("x", "a")], matcher, 3)


@requires_pyarrow
def test_arrow_to_staged_table():
    schema = StructType(
        [
            StructField("ID", LongType()),
            StructField("M", MapType(StringType(), LongType())),
            StructField("TS", TimestampType()),
            StructField("S", StringType()),
        ]
    )
    matcher = FieldMatcher(schema)
    batch = pa.record_batch(
        {
            "s": pa.array([1]),
            "m": pa.array([[("a", 1)]], pa.map_(pa.string(), pa.int64())),
            "ts": pa.array([datetime.date(2024, 1, 2)]),
            "id": pa.array([1], pa.int32()),
        }
    )
    table = conversion.arrow_to_staged_table(batch, matcher, 0)
    assert table.column_names == ["ID", "M", "TS", "S"]
    assert table.schema.field("ID").type == pa.int64()
    assert table.schema.field("TS").type == pa.timestamp("us")
    assert table.to_pylist() == [
        {"ID": 1, "M": '{"a": 1}', "TS": datetime.datetime(2024, 1, 2), "S": "1"}
    ]
    # positional fallback when the column count matches
    positional = pa.record_batch(
        [pa.array([1]), pa.array([None], pa.null()), pa.array(["x"]), pa.array(["s"])],
        names=["a", "b", "c", "d"],
    )
    assert conversion.arrow_to_staged_table(positional, matcher, 0).to_pylist() == [
        {"ID": 1, "M": None, "TS": "x", "S": "s"}
    ]
    with pytest.raises(SnowparkDataframeReaderException, match="don't include"):
        conversion.arrow_to_staged_table(
            pa.record_batch({"id": pa.array([1])}), matcher, 0
        )


@requires_pyarrow
def test_normalize_items():
    matcher = FieldMatcher(SCHEMA)
    items = [
        (1, "a"),
        pa.record_batch({"Name": pa.array(["b", "c"]), "id": pa.array([2, 3])}),
        {"id": 4},
    ]
    assert list(normalize_items(items, matcher)) == [
        (1, "a"),
        (2, "b"),
        (3, "c"),
        (4, None),
    ]


class _BatchReader(DataSourceReader):
    def __init__(self, items, fail_after=None) -> None:
        self.items = items
        self.fail_after = fail_after

    def read(self, partition):
        for i, item in enumerate(self.items):
            if self.fail_after is not None and i == self.fail_after:
                raise ConnectionError("boom")
            yield item


def _partition_reader(reader, batch_size=2):
    return CustomPartitionReader(cloudpickle.dumps(reader), SCHEMA, batch_size)


@requires_pyarrow
def test_custom_partition_reader_batches():
    reader = _partition_reader(
        _BatchReader(
            [
                (1, "a"),
                (2, "b"),
                (3, "c"),
                pa.record_batch({"id": pa.array([4]), "Name": pa.array(["d"])}),
                (5, "e"),
            ]
        )
    )
    tables = list(reader.read((0, cloudpickle.dumps(InputPartition(None)))))
    assert [t.num_rows for t in tables] == [2, 1, 1, 1]
    assert [r["ID"] for t in tables for r in t.to_pylist()] == [1, 2, 3, 4, 5]
    assert reader.data_to_parquet_bytesio(tables[0]) is not None
    assert reader.data_to_parquet_bytesio(tables[0].slice(0, 0)) is None


@requires_pyarrow
def test_custom_partition_reader_errors():
    # failures before any output are retryable
    reader = _partition_reader(_BatchReader([(1, "a")], fail_after=0))
    with pytest.raises(ConnectionError):
        list(reader.read((0, cloudpickle.dumps(None))))
    # failures after output are not, to avoid duplicates
    reader = _partition_reader(
        _BatchReader([(1, "a"), (2, "b"), (3, "c")], fail_after=2)
    )
    with pytest.raises(
        SnowparkDataframeReaderException, match="partition 7 .* after part of it"
    ):
        list(reader.read((7, cloudpickle.dumps(None))))
    # invalid data is not retryable
    reader = _partition_reader(_BatchReader([(1,)]))
    with pytest.raises(SnowparkDataframeReaderException, match="partition 1"):
        list(reader.read((1, cloudpickle.dumps(None))))


def test_custom_partition_reader_pickle():
    reader = _partition_reader(_BatchReader([(1, "a")]))
    reader._reader = threading.Lock()  # an unpicklable cached reader
    restored = pickle.loads(pickle.dumps(reader))
    assert restored._reader is None
    assert restored.batch_size == 2


def test_dumps_unpicklable():
    class LockReader(DataSourceReader):
        def __init__(self) -> None:
            self.lock = threading.Lock()

        def read(self, partition):
            yield (1,)

    with pytest.raises(TypeError, match="must be picklable"):
        _dumps(LockReader(), "DataSourceReader")


def test_pickle_by_value():
    with _pickle_by_value():
        registered = cloudpickle.list_registry_pickle_by_value()
        assert "snowflake.snowpark.datasource" in registered
        assert conversion.__name__ in registered
        assert CaseInsensitiveDict.__module__ in registered
        data = cloudpickle.dumps(InputPartition(1))
    assert "snowflake.snowpark.datasource" not in (
        cloudpickle.list_registry_pickle_by_value()
    )
    assert cloudpickle.loads(data).value == 1


def test_resolve_udtf_packages():
    assert _resolve_udtf_packages(None) == ["snowflake-snowpark-python"]
    assert _resolve_udtf_packages(["requests"]) == [
        "requests",
        "snowflake-snowpark-python",
    ]
    assert _resolve_udtf_packages(["snowflake-snowpark-python==1.40.0"]) == [
        "snowflake-snowpark-python==1.40.0"
    ]
    assert _resolve_udtf_packages(["Snowflake-Snowpark-Python>=1.40"]) == [
        "Snowflake-Snowpark-Python>=1.40"
    ]


def test_local_modules_of():
    import sys
    import types

    local_module = types.ModuleType("my_local_source")
    local_module.__file__ = "/tmp/my_project/my_local_source.py"
    exec(
        "from snowflake.snowpark.datasource import DataSourceReader\n"
        "class LocalReader(DataSourceReader):\n"
        "    def read(self, partition):\n"
        "        yield (1,)\n",
        local_module.__dict__,
    )
    with mock.patch.dict(sys.modules, {"my_local_source": local_module}):
        reader = local_module.LocalReader()
        assert _local_modules_of([reader, reader, InputPartition(1)]) == [local_module]
    # Snowpark, the standard library and builtins are pickled by reference
    assert _local_modules_of([InputPartition(1), decimal.Decimal(1), 1]) == []
