#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import datetime
import decimal
import pickle
import queue
import threading
from unittest import mock

import cloudpickle
import pytest

from snowflake.snowpark._internal.data_source.custom import conversion
from snowflake.snowpark._internal.data_source.custom.conversion import (
    FieldMatcher,
    convert_row,
    normalize_items,
    staging_schema,
    to_row_tuple,
    udtf_output_schema,
    udtf_row_converters,
    validate_schema,
)
from snowflake.snowpark._internal.data_source.custom.ingestion import (
    CustomPartitionReader,
    _build_udtf_handler,
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
    BooleanType,
    DateType,
    DecFloatType,
    DecimalType,
    DoubleType,
    GeographyType,
    LongType,
    MapType,
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


def test_convert_row():
    ts = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)
    schema = StructType(
        [
            StructField("I", LongType()),
            StructField("TS", TimestampType()),
            StructField("N", StringType()),
        ]
    )
    converters = udtf_row_converters(schema)
    assert convert_row((1, ts, None), converters) == (
        1,
        "2024-01-02T03:04:05+00:00",
        None,
    )
    assert convert_row((1,), [None]) == (1,)
    schema = StructType(
        [StructField("V", VariantType()), StructField("A", ArrayType())]
    )
    assert convert_row((Row(a=Row(1, 2)), None), udtf_row_converters(schema)) == (
        {"a": [1, 2]},
        None,
    )


def test_convert_row_date_time_decimal():
    # the same values as in local ingestion
    schema = StructType(
        [
            StructField("D", DateType()),
            StructField("T", TimeType()),
            StructField("N", DecimalType(10, 2)),
            StructField("F", DecimalType(10, 2)),
        ]
    )
    row = convert_row(
        ("2024-01-02", "03:04:05", decimal.Decimal("1.235"), 2.5),
        udtf_row_converters(schema),
    )
    assert row == (
        datetime.date(2024, 1, 2),
        datetime.time(3, 4, 5),
        decimal.Decimal("1.24"),
        decimal.Decimal("2.50"),
    )
    assert convert_row(
        (datetime.datetime(2024, 1, 2, 3), None, None, None),
        udtf_row_converters(schema),
    ) == (datetime.date(2024, 1, 2), None, None, None)


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
    # timestamps are staged as ISO strings, like those of rows
    assert table.schema.field("TS").type == pa.string()
    assert table.to_pylist() == [
        {"ID": 1, "M": '{"a": 1}', "TS": "2024-01-02T00:00:00", "S": "1"}
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
def test_arrow_partial_name_match_does_not_fall_back_to_positions():
    schema = StructType(
        [StructField("A", StringType()), StructField("B", StringType())]
    )
    matcher = FieldMatcher(schema)
    batch = pa.record_batch({"b": ["B"], "x": ["X"]})
    with pytest.raises(SnowparkDataframeReaderException, match=r"include.*\['A'\]"):
        conversion.arrow_to_staged_table(batch, matcher, 0)
    with pytest.raises(SnowparkDataframeReaderException, match="don't include"):
        list(normalize_items([batch], matcher))


@requires_pyarrow
def test_arrow_timestamp_keeps_offset():
    schema = StructType(
        [
            StructField("TS", TimestampType(TimestampTimeZone.TZ)),
            StructField("I", LongType()),
        ]
    )
    ts = datetime.datetime(
        2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone(datetime.timedelta(hours=8))
    )
    batch = pa.record_batch(
        {
            "ts": pa.array([ts, None], pa.timestamp("us", tz="Asia/Shanghai")),
            "i": [1, 2],
        }
    )
    table = conversion.arrow_to_staged_table(batch, FieldMatcher(schema), 0)
    assert table.column("TS").to_pylist() == ["2024-01-02T03:04:05+08:00", None]


@requires_pyarrow
def test_arrow_decimal_rounds_half_up():
    schema = StructType([StructField("N", DecimalType(10, 2))])
    matcher = FieldMatcher(schema)
    for values in (
        pa.array([decimal.Decimal("1.234"), decimal.Decimal("1.235"), None]),
        pa.array([1.234, 1.235, None]),
        pa.array([1, 2, None]),
    ):
        table = conversion.arrow_to_staged_table(
            pa.record_batch({"n": values}), matcher, 0
        )
        assert table.schema.field("N").type == pa.decimal128(10, 2)
    table = conversion.arrow_to_staged_table(
        pa.record_batch({"n": pa.array([decimal.Decimal("1.235")])}), matcher, 0
    )
    assert table.column("N").to_pylist() == [decimal.Decimal("1.24")]
    table = conversion.arrow_to_staged_table(
        pa.record_batch({"n": pa.array([1.999])}), matcher, 0
    )
    assert table.column("N").to_pylist() == [decimal.Decimal("2.00")]


@requires_pyarrow
def test_integral_columns_reject_fractions():
    schema = StructType([StructField("I", LongType())])
    matcher = FieldMatcher(schema)
    table = conversion.rows_to_arrow_table([(2.0,), (3,), (None,)], matcher, 0)
    assert table.column("I").to_pylist() == [2, 3, None]
    for bad in (1.7, float("nan")):
        with pytest.raises(SnowparkDataframeReaderException, match="column I"):
            conversion.rows_to_arrow_table([(bad,)], matcher, 0)
    # the same as for Arrow data
    with pytest.raises(SnowparkDataframeReaderException, match="column I"):
        conversion.arrow_to_staged_table(pa.record_batch({"i": [1.7]}), matcher, 0)


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


@requires_pyarrow
@pytest.mark.parametrize(
    "item, match",
    [
        ({1: "a"}, "must be strings"),
        (pa.array([1, 2]), "Wrap a pyarrow.Array"),
        (object(), "must yield"),
    ],
)
def test_custom_partition_reader_invalid_items_are_not_retried(item, match):
    reader = _partition_reader(_BatchReader([item]))
    # not a ValueError or TypeError, which would be retried
    with pytest.raises(SnowparkDataframeReaderException, match=match):
        list(reader.read((0, cloudpickle.dumps(None))))


class _FlakyReader(DataSourceReader):
    def __init__(self, error, fail_times, fail_after_output) -> None:
        self.error = error
        self.fail_times = fail_times
        self.fail_after_output = fail_after_output
        self.calls = 0

    def read(self, partition):
        self.calls += 1
        if self.fail_after_output:
            # a full batch is output before the failure
            yield (1, "a")
            yield (3, "c")
        if self.calls <= self.fail_times:
            raise self.error
        yield (2, "b")


@requires_pyarrow
@pytest.mark.parametrize("error", [TypeError("bad"), ValueError("bad"), KeyError("k")])
def test_custom_partition_reader_user_errors_before_output_are_retried(error):
    from snowflake.snowpark._internal.data_source.utils import (
        _task_fetch_data_from_source_with_retry,
    )

    def run(fail_times, fail_after_output=False):
        reader = _partition_reader(_FlakyReader(error, fail_times, fail_after_output))
        results = queue.Queue()
        _task_fetch_data_from_source_with_retry(
            reader, (0, cloudpickle.dumps(None)), 0, results
        )
        return reader, results

    # a failure before any output is retried, and all the rows are loaded
    reader, results = run(fail_times=1)
    assert reader._reader.calls == 2
    assert results.qsize() == 2  # a parquet file and the completion signal
    # failing every attempt raises after 3 attempts
    with pytest.raises(SnowparkDataframeReaderException, match="3 attempts"):
        run(fail_times=10)
    # a failure after output is not retried, to avoid duplicate rows
    with pytest.raises(SnowparkDataframeReaderException, match="after part of it"):
        run(fail_times=1, fail_after_output=True)


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


def test_pickle_by_value_requires_cloudpickle_2(monkeypatch):
    monkeypatch.delattr(cloudpickle, "register_pickle_by_value")
    with pytest.raises(ImportError, match=r"require cloudpickle>=2\.0"):
        with _pickle_by_value():
            pass


def test_udtf_handler_module_not_found_message():
    schema = StructType([StructField("I", LongType())])

    class Missing:
        def __reduce__(self):
            return (__import__, ("not_installed_module_xyz",))

    handler = _build_udtf_handler(pickle.dumps(Missing()), schema)
    with pytest.raises(ModuleNotFoundError) as exc_info:
        handler()
    message = str(exc_info.value)
    assert "snowpark.packages" in message
    assert "snowpark.imports" in message


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


def test_local_modules_of_collects_imported_local_modules(tmp_path):
    import sys

    (tmp_path / "cds_helper_leaf.py").write_text("def leaf():\n    return 1\n")
    (tmp_path / "cds_helper_mid.py").write_text(
        "from cds_helper_leaf import leaf\n" "def mid():\n    return leaf()\n"
    )
    (tmp_path / "cds_helper_cls.py").write_text("class Helper:\n    pass\n")
    (tmp_path / "cds_source.py").write_text(
        "import cds_helper_mid\n"
        "import snowflake.snowpark.datasource\n"
        "from cds_helper_cls import Helper\n"
        "from snowflake.snowpark.datasource import DataSourceReader\n"
        "class SourceReader(DataSourceReader):\n"
        "    def read(self, partition):\n        yield (1,)\n"
    )
    names = ["cds_helper_leaf", "cds_helper_mid", "cds_helper_cls", "cds_source"]
    sys.path.insert(0, str(tmp_path))
    try:
        import cds_source

        modules = _local_modules_of([cds_source.SourceReader()])
        assert sorted(m.__name__ for m in modules) == sorted(names)
    finally:
        sys.path.remove(str(tmp_path))
        for name in names:
            sys.modules.pop(name, None)


def test_local_modules_of_scans_main_globals(tmp_path):
    import sys
    import types

    (tmp_path / "cds_main_helper.py").write_text("def helper():\n    return 1\n")
    sys.path.insert(0, str(tmp_path))
    main = types.ModuleType("__main__")
    try:
        import cds_main_helper

        exec(
            "import cds_main_helper\n"
            "from snowflake.snowpark.datasource import DataSourceReader\n"
            "class MainReader(DataSourceReader):\n"
            "    def read(self, partition):\n        yield (cds_main_helper.helper(),)\n",
            main.__dict__,
        )
        main.MainReader.__module__ = "__main__"
        with mock.patch.dict(sys.modules, {"__main__": main}):
            modules = _local_modules_of([main.MainReader()])
        # __main__ itself is pickled by value by cloudpickle
        assert modules == [cds_main_helper]
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.pop("cds_main_helper", None)


def test_decimal_converter_with_38_digits():
    convert = conversion._decimal_converter(2)
    assert convert(decimal.Decimal("1" * 36 + ".005")) == decimal.Decimal(
        "1" * 36 + ".01"
    )
    assert conversion._decimal_converter(10)(
        decimal.Decimal("12345678901234567890123456.1234567890")
    ) == decimal.Decimal("12345678901234567890123456.1234567890")
    schema = StructType([StructField("N", DecimalType(38, 0))])
    # the UDTF
    handler = _build_udtf_handler(
        cloudpickle.dumps(_BatchReader([(10**35,)])), schema
    )
    assert list(handler().process(0, cloudpickle.dumps(None))) == [(10**35,)]


@requires_pyarrow
def test_decimal_38_digits_rows_and_arrow():
    schema = StructType([StructField("N", DecimalType(38, 2))])
    matcher = FieldMatcher(schema)
    value = decimal.Decimal("1" * 36 + ".005")
    table = conversion.rows_to_arrow_table([(value,)], matcher, 0)
    assert table.column("N").to_pylist() == [decimal.Decimal("1" * 36 + ".01")]
    batch = pa.record_batch(
        {"n": pa.array([decimal.Decimal("1" * 33 + ".1234")], pa.decimal128(38, 4))}
    )
    table = conversion.arrow_to_staged_table(batch, matcher, 0)
    assert table.column("N").to_pylist() == [decimal.Decimal("1" * 33 + ".12")]


def test_udtf_value_parity():
    schema = StructType(
        [
            StructField("I", LongType()),
            StructField("N", DecimalType(10, 2)),
        ]
    )
    handler = _build_udtf_handler(
        cloudpickle.dumps(_BatchReader([(2.0, decimal.Decimal("1.005"))])), schema
    )
    assert list(handler().process(0, cloudpickle.dumps(None))) == [
        (2, decimal.Decimal("1.01"))
    ]
    bad = _build_udtf_handler(cloudpickle.dumps(_BatchReader([(1.7, None)])), schema)
    with pytest.raises(RuntimeError, match="not an integer"):
        list(bad().process(0, cloudpickle.dumps(None)))


@pytest.mark.parametrize(
    "value, expected",
    [
        (3, 3),
        (True, 1),
        (2.0, 2),
        (decimal.Decimal("2"), 2),
        (decimal.Decimal("2.0"), 2),
        (-0.0, 0),
    ],
)
def test_to_integer_accepts_integral_values(value, expected):
    result = conversion._to_integer(value)
    assert result == expected
    assert isinstance(result, int)


@pytest.mark.parametrize(
    "value",
    [
        1.7,
        decimal.Decimal("1.5"),
        float("nan"),
        float("inf"),
        -float("inf"),
        decimal.Decimal("NaN"),
        decimal.Decimal("Infinity"),
    ],
)
def test_to_integer_rejects_non_integral_values(value):
    with pytest.raises(ValueError, match="not an integer"):
        conversion._to_integer(value)


def test_to_integer_numpy():
    np = pytest.importorskip("numpy")
    assert conversion._to_integer(np.float32(2.0)) == 2
    assert conversion._to_integer(np.int64(5)) == 5
    with pytest.raises(ValueError, match="not an integer"):
        conversion._to_integer(np.float32(1.5))
    with pytest.raises(ValueError, match="not an integer"):
        conversion._to_integer(np.float64("nan"))


def test_ddl_names_are_matched_like_in_schemas():
    from snowflake.snowpark._internal.data_source.datasource_partitioner import (
        DataSourcePartitioner,
    )

    schema = DataSourcePartitioner.formatting_custom_schema(
        'camelCase STRING, "my col" INT, "QuotedCase" INT, plain tinyint'
    )
    matcher = FieldMatcher(schema)
    assert to_row_tuple(
        {"camelCase": "x", "my col": 1, "QuotedCase": 3, "plain": 2}, matcher
    ) == ("x", 1, 3, 2)
    # unquoted names are matched case-insensitively, and quoted names exactly
    assert to_row_tuple(
        {"CAMELcase": "x", "MY COL": 1, "quotedcase": 3, "PLAIN": 2}, matcher
    ) == ("x", None, None, 2)


@pytest.mark.parametrize("value", ["5", b"5"])
def test_to_integer_rejects_strings(value):
    with pytest.raises(ValueError, match="not an integer"):
        conversion._to_integer(value)


def test_to_float():
    assert conversion._to_float(decimal.Decimal("1.5")) == 1.5
    assert isinstance(conversion._to_float(True), float)
    assert conversion._to_float(2) == 2.0
    assert conversion._to_float(1.5) == 1.5
    with pytest.raises(ValueError, match="not a number"):
        conversion._to_float("1.5")


@requires_pyarrow
def test_rows_to_arrow_table_value_parity():
    schema = StructType([StructField("F", DoubleType()), StructField("I", LongType())])
    matcher = FieldMatcher(schema)
    table = conversion.rows_to_arrow_table([(decimal.Decimal("1.5"), True)], matcher, 0)
    assert table.to_pylist() == [{"F": 1.5, "I": 1}]
    with pytest.raises(SnowparkDataframeReaderException, match="column I"):
        conversion.rows_to_arrow_table([(1.0, "5")], matcher, 0)
    # the same values as in the UDTF and in local testing
    converters = conversion.value_converters(schema)
    assert conversion.convert_row((decimal.Decimal("1.5"), True), converters) == (
        1.5,
        1,
    )
    with pytest.raises(ValueError):
        conversion.convert_row((1.0, "5"), converters)


@requires_pyarrow
def test_not_null_columns_reject_nulls():
    schema = StructType([StructField("I", LongType(), nullable=False)])
    matcher = FieldMatcher(schema)
    with pytest.raises(SnowparkDataframeReaderException, match="NOT NULL column"):
        conversion.rows_to_arrow_table([(1,), (None,)], matcher, 0)
    with pytest.raises(SnowparkDataframeReaderException, match="NOT NULL column"):
        conversion.arrow_to_staged_table(
            pa.record_batch({"i": pa.array([1, None])}), matcher, 0
        )
    with pytest.raises(ValueError, match="NOT NULL column I"):
        conversion.check_not_null((None,), matcher)
    conversion.check_not_null((1,), matcher)


def test_local_modules_of_skips_imported_modules(tmp_path):
    import sys

    (tmp_path / "cds_uploaded.py").write_text("class Up:\n    pass\n")
    (tmp_path / "cds_not_uploaded.py").write_text("class NotUp:\n    pass\n")
    sys.path.insert(0, str(tmp_path))
    try:
        import cds_not_uploaded
        import cds_uploaded

        objects = [cds_uploaded.Up(), cds_not_uploaded.NotUp()]
        assert len(_local_modules_of(objects)) == 2
        for imports in (
            [str(tmp_path / "cds_uploaded.py")],
            [(str(tmp_path / "cds_uploaded.py"), None)],
            ["@stage/x.py", str(tmp_path / "cds_uploaded.py")],
        ):
            assert _local_modules_of(objects, imports) == [cds_not_uploaded]
        # an uploaded directory
        assert _local_modules_of(objects, [str(tmp_path)]) == []
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.pop("cds_uploaded", None)
        sys.modules.pop("cds_not_uploaded", None)


def test_dumps_error_mentions_module_level_variables():
    class Unpicklable:
        def __reduce__(self):
            raise TypeError("nope")

    with pytest.raises(TypeError, match="module-level variables"):
        _dumps(Unpicklable(), "DataSourceReader")


def test_udtf_ingestion_falls_back_to_pickling_modules_by_reference(tmp_path):
    import sys

    from snowflake.snowpark._internal.data_source.custom import ingestion
    from snowflake.snowpark._internal.data_source.custom.options import EngineOptions

    (tmp_path / "cds_lock_mod.py").write_text(
        "import threading\n"
        "from snowflake.snowpark.datasource import DataSourceReader\n"
        "_lock = threading.Lock()\n"
        "class LockReader(DataSourceReader):\n"
        "    def read(self, partition):\n"
        "        with _lock:\n"
        "            yield (1,)\n"
    )
    sys.path.insert(0, str(tmp_path))
    try:
        import cds_lock_mod

        reader = cds_lock_mod.LockReader()
        # the module-level lock can't be pickled by value
        with pytest.raises(TypeError, match="module-level variables"):
            with _pickle_by_value(_local_modules_of([reader])):
                _dumps(reader, "DataSourceReader")

        used_modules = []
        original = ingestion._pickle_by_value

        def spy(modules=()):
            used_modules.append(list(modules))
            return original(modules)

        session = mock.MagicMock()
        schema = StructType([StructField("I", LongType())])
        with mock.patch.object(ingestion, "_pickle_by_value", spy), mock.patch.object(
            ingestion.BaseDriver, "keep_nullable_attributes"
        ), mock.patch.object(ingestion, "cast_udtf_output_columns"):
            ingestion._udtf_ingestion(
                session, reader, [InputPartition(0)], schema, EngineOptions(), {}
            )
        assert [m.__name__ for m in used_modules[0]] == ["cds_lock_mod"]
        # then by reference, for the pickling and for registering the UDTF
        assert used_modules[1:] == [[], []]
        session.udtf.register.assert_called_once()
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.pop("cds_lock_mod", None)


def test_udtf_handler_rejects_nulls_in_not_null_columns():
    schema = StructType([StructField("I", LongType(), nullable=False)])
    handler = _build_udtf_handler(cloudpickle.dumps(_BatchReader([(None,)])), schema)
    with pytest.raises(RuntimeError, match="NOT NULL column I"):
        list(handler().process(0, cloudpickle.dumps(None)))


@requires_pyarrow
@pytest.mark.parametrize(
    "datatype, column",
    [
        (TimestampType(), pa.array([1700000000])),
        (TimestampType(), pa.array([1.5])),
        (DateType(), pa.array([19000])),
        (TimeType(), pa.array([3600])),
        (LongType(), pa.array(["5"])),
        (LongType(), pa.array([b"5"])),
        (DoubleType(), pa.array(["1.5"])),
        (BooleanType(), pa.array(["true"])),
        (BooleanType(), pa.array([1])),
    ],
)
def test_arrow_rejects_conversions_that_rows_reject(datatype, column):
    matcher = FieldMatcher(StructType([StructField("C", datatype)]))
    with pytest.raises(
        SnowparkDataframeReaderException, match="column C .* partition 4"
    ):
        conversion.arrow_to_staged_table(pa.record_batch({"c": column}), matcher, 4)


@requires_pyarrow
def test_arrow_accepts_compatible_types():
    schema = StructType(
        [
            StructField("B", BooleanType()),
            StructField("D", DecimalType(10, 2)),
            StructField("DT", DateType()),
            StructField("I", LongType()),
        ]
    )
    batch = pa.record_batch(
        {
            "b": pa.array([True, None]),
            "d": pa.array(["1.005", None]),
            "dt": pa.array(["2024-01-02", None]),
            "i": pa.array([None, None], pa.null()),
        }
    )
    table = conversion.arrow_to_staged_table(batch, FieldMatcher(schema), 0)
    assert table.to_pylist()[0] == {
        "B": True,
        "D": decimal.Decimal("1.01"),
        "DT": datetime.date(2024, 1, 2),
        "I": None,
    }


@pytest.mark.parametrize("value", [True, False])
def test_to_bool_accepts_booleans(value):
    assert conversion._to_bool(value) is value


@pytest.mark.parametrize("value", ["true", 1, 0, 1.0, object()])
def test_to_bool_rejects_other_values(value):
    with pytest.raises(ValueError, match="not a boolean"):
        conversion._to_bool(value)


def test_to_bool_numpy():
    np = pytest.importorskip("numpy")
    assert conversion._to_bool(np.bool_(True)) is True


@requires_pyarrow
def test_boolean_values_are_converted_like_in_all_modes():
    schema = StructType([StructField("B", BooleanType())])
    matcher = FieldMatcher(schema)
    assert conversion.rows_to_arrow_table(
        [(True,), (None,)], matcher, 0
    ).to_pylist() == [
        {"B": True},
        {"B": None},
    ]
    with pytest.raises(SnowparkDataframeReaderException, match="column B"):
        conversion.rows_to_arrow_table([("true",)], matcher, 0)
    handler = _build_udtf_handler(cloudpickle.dumps(_BatchReader([("true",)])), schema)
    with pytest.raises(RuntimeError, match="not a boolean"):
        list(handler().process(0, cloudpickle.dumps(None)))


def test_local_modules_of_follows_instance_state(tmp_path):
    import sys

    (tmp_path / "cds_state_cfg.py").write_text("class Cfg:\n    pass\n")
    (tmp_path / "cds_state_slot.py").write_text("class Slot:\n    pass\n")
    (tmp_path / "cds_state_part.py").write_text(
        "class Part:\n    def __init__(self, v):\n        self.v = v\n"
    )
    (tmp_path / "cds_state_reader.py").write_text(
        "from snowflake.snowpark.datasource import DataSourceReader\n"
        "class Reader(DataSourceReader):\n"
        "    __slots__ = ('slot',)\n"
        "    def __init__(self, cfgs, slot):\n"
        "        self.cfgs = cfgs\n"
        "        self.slot = slot\n"
        "    def read(self, partition):\n        yield (1,)\n"
    )
    names = [
        "cds_state_cfg",
        "cds_state_slot",
        "cds_state_part",
        "cds_state_reader",
    ]
    sys.path.insert(0, str(tmp_path))
    try:
        import cds_state_cfg
        import cds_state_part
        import cds_state_reader
        import cds_state_slot

        cyclic = {"cfg": [cds_state_cfg.Cfg()]}
        cyclic["self"] = cyclic
        reader = cds_state_reader.Reader(cyclic, cds_state_slot.Slot())
        partition = InputPartition(cds_state_part.Part(1))
        modules = _local_modules_of([reader, partition])
        assert sorted(m.__name__ for m in modules) == sorted(names)
        # objects of installed packages are not searched
        assert _local_modules_of([InputPartition({"a": [decimal.Decimal(1)]})]) == []
    finally:
        sys.path.remove(str(tmp_path))
        for name in names:
            sys.modules.pop(name, None)


def test_udtf_ingestion_fallback_logs_a_warning(tmp_path, caplog):
    import logging
    import sys

    from snowflake.snowpark._internal.data_source.custom import ingestion
    from snowflake.snowpark._internal.data_source.custom.options import EngineOptions

    (tmp_path / "cds_warn_mod.py").write_text(
        "import threading\n"
        "from snowflake.snowpark.datasource import DataSourceReader\n"
        "_lock = threading.Lock()\n"
        "class WarnReader(DataSourceReader):\n"
        "    def read(self, partition):\n"
        "        with _lock:\n"
        "            yield (1,)\n"
    )
    sys.path.insert(0, str(tmp_path))
    try:
        import cds_warn_mod

        with mock.patch.object(
            ingestion.BaseDriver, "keep_nullable_attributes"
        ), mock.patch.object(ingestion, "cast_udtf_output_columns"), caplog.at_level(
            logging.WARNING, logger=ingestion.logger.name
        ):
            ingestion._udtf_ingestion(
                mock.MagicMock(),
                cds_warn_mod.WarnReader(),
                [InputPartition(0)],
                StructType([StructField("I", LongType())]),
                EngineOptions(),
                {},
            )
        assert "cds_warn_mod" in caplog.text
        assert "snowpark.imports" in caplog.text
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.pop("cds_warn_mod", None)
