#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import inspect
import json
from unittest import mock

import pytest

from snowflake.snowpark._internal.data_source.custom import pyspark_compat
from snowflake.snowpark._internal.data_source.custom.conversion import (
    _json_dumps,
    to_udtf_row,
)
from snowflake.snowpark._internal.data_source.custom.ingestion import (
    _get_partitions,
    _resolve_udtf_packages,
    read_custom_data_source,
)
from snowflake.snowpark._internal.data_source.custom.pyspark_compat import (
    is_pyspark_class,
    is_pyspark_struct_type,
    snowpark_type_to_spark_json,
    spark_json_to_snowpark_schema,
    spark_json_to_snowpark_type,
    uses_pyspark,
    with_spark_timestamp_semantics,
)
from snowflake.snowpark._internal.data_source.datasource_partitioner import (
    DataSourcePartitioner,
)
from snowflake.snowpark.datasource import (
    DataSource,
    DataSourceLike,
    DataSourceReader,
    DataSourceReaderLike,
    DataSourceRegistration,
    _validate_data_source_class,
)
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    GeographyType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampTimeZone,
    TimestampType,
    TimeType,
    VariantType,
)

# Fake classes that look like PySpark's to Snowpark, so that these tests don't need PySpark.
_PYSPARK_MODULE = "pyspark.sql.datasource"


class FakeSparkDataSource:
    def __init__(self, options) -> None:
        self.options = options

    @classmethod
    def name(cls):
        return cls.__name__

    def schema(self):
        raise NotImplementedError

    def reader(self, schema):
        raise NotImplementedError


class FakeSparkReader:
    def partitions(self):
        raise NotImplementedError  # like PySpark 4.0

    def read(self, partition):
        raise NotImplementedError


class FakeSparkStructType:
    def __init__(self, json_value) -> None:
        self._json_value = json_value

    def jsonValue(self):
        return self._json_value


class FakeVariantVal:
    def __init__(self, json_string) -> None:
        self.json_string = json_string

    def toJson(self, zone_id="UTC"):
        return self.json_string


FakeSparkDataSource.__module__ = _PYSPARK_MODULE
FakeSparkReader.__module__ = _PYSPARK_MODULE
FakeSparkStructType.__module__ = "pyspark.sql.types"
FakeSparkStructType.__name__ = "StructType"
FakeVariantVal.__name__ = "VariantVal"


class UserSparkSource(FakeSparkDataSource):
    @classmethod
    def name(cls):
        return "user_spark_source"

    def schema(self):
        return "userId INT, ts TIMESTAMP"

    def reader(self, schema):
        return UserSparkReader(schema)


class UserSparkReader(FakeSparkReader):
    def __init__(self, schema) -> None:
        self.schema = schema

    def read(self, partition):
        yield (1, None)


SPARK_SCHEMA = {
    "type": "struct",
    "fields": [
        {"name": "userId", "type": "integer", "nullable": False, "metadata": {}},
        {"name": "Mixed Case", "type": "string", "nullable": True, "metadata": {}},
        {"name": "price", "type": "decimal(12,2)", "nullable": True, "metadata": {}},
        {"name": "ts", "type": "timestamp", "nullable": True, "metadata": {}},
        {"name": "ts_ntz", "type": "timestamp_ntz", "nullable": True, "metadata": {}},
        {
            "name": "tags",
            "type": {"type": "array", "elementType": "string", "containsNull": True},
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "attrs",
            "type": {
                "type": "map",
                "keyType": "string",
                "valueType": "long",
                "valueContainsNull": True,
            },
            "nullable": True,
            "metadata": {},
        },
        {
            "name": "nested",
            "type": {
                "type": "struct",
                "fields": [
                    {"name": "x", "type": "double", "nullable": True, "metadata": {}}
                ],
            },
            "nullable": True,
            "metadata": {},
        },
        {"name": "v", "type": "variant", "nullable": True, "metadata": {}},
    ],
}


def test_no_pyspark_import():
    source = inspect.getsource(pyspark_compat)
    assert "import pyspark" not in source
    assert "from pyspark" not in source


def test_detection():
    assert is_pyspark_class(FakeSparkDataSource)
    assert is_pyspark_class(UserSparkSource)
    assert not is_pyspark_class(DataSource)
    assert not is_pyspark_class(UserSparkSource({}))
    assert uses_pyspark([1, UserSparkReader(None)])
    assert not uses_pyspark([1, "a", None])
    assert is_pyspark_struct_type(FakeSparkStructType({}))
    assert not is_pyspark_struct_type(StructType([]))


@pytest.mark.parametrize(
    "spark_type, expected",
    [
        ("string", StringType()),
        ("varchar(10)", StringType(10)),
        ("char(3)", StringType(3)),
        ("binary", BinaryType()),
        ("boolean", BooleanType()),
        ("date", DateType()),
        ("timestamp", TimestampType(TimestampTimeZone.LTZ)),
        ("timestamp_ntz", TimestampType(TimestampTimeZone.NTZ)),
        ("double", DoubleType()),
        ("float", FloatType()),
        ("byte", ByteType()),
        ("short", ShortType()),
        ("integer", IntegerType()),
        ("long", LongType()),
        ("decimal", DecimalType(10, 0)),
        ("decimal(38, 18)", DecimalType(38, 18)),
        ("variant", VariantType()),
        ("time(6)", TimeType()),
        ({"type": "udt", "sqlType": "binary"}, BinaryType()),
    ],
)
def test_spark_json_to_snowpark_type(spark_type, expected):
    assert spark_json_to_snowpark_type(spark_type) == expected


@pytest.mark.parametrize(
    "spark_type",
    ["interval day to second", "void", "calendarinterval", {"type": "udt"}, 1],
)
def test_spark_json_to_snowpark_type_unsupported(spark_type):
    with pytest.raises(NotImplementedError, match="not supported"):
        spark_json_to_snowpark_type(spark_type)


def test_spark_json_to_snowpark_schema():
    schema = spark_json_to_snowpark_schema(SPARK_SCHEMA)
    assert [f.name for f in schema.fields] == [
        "USERID",
        '"Mixed Case"',
        "PRICE",
        "TS",
        "TS_NTZ",
        "TAGS",
        "ATTRS",
        "NESTED",
        "V",
    ]
    assert schema.fields[0].nullable is False
    assert schema.fields[2].datatype == DecimalType(12, 2)
    assert schema.fields[5].datatype == ArrayType(StringType())
    assert schema.fields[6].datatype == MapType(StringType(), LongType())
    assert isinstance(schema.fields[7].datatype, StructType)
    with pytest.raises(TypeError, match="struct"):
        spark_json_to_snowpark_schema("string")


def test_round_trip_keeps_names():
    schema = spark_json_to_snowpark_schema(SPARK_SCHEMA)
    assert snowpark_type_to_spark_json(schema) == SPARK_SCHEMA


def test_snowpark_type_to_spark_json():
    schema = DataSourcePartitioner.formatting_custom_schema(
        'userId INT, "Mixed" STRING, t TIME, ts TIMESTAMP_TZ'
    )
    schema.fields.extend(
        [
            StructField("a", ArrayType()),
            StructField("m", MapType(StringType(), LongType())),
        ]
    )
    spark = snowpark_type_to_spark_json(schema)
    assert [(f["name"], f["type"]) for f in spark["fields"]] == [
        ("userId", "integer"),
        ("Mixed", "string"),
        ("t", "time(6)"),
        ("ts", "timestamp"),
        (
            "a",
            {"type": "array", "elementType": "string", "containsNull": True},
        ),
        (
            "m",
            {
                "type": "map",
                "keyType": "string",
                "valueType": "long",
                "valueContainsNull": True,
            },
        ),
    ]
    with pytest.raises(NotImplementedError, match="GeographyType"):
        snowpark_type_to_spark_json(GeographyType())


def test_to_pyspark_struct_type():
    fake_types = mock.MagicMock()
    with mock.patch.dict("sys.modules", {"pyspark.sql.types": fake_types}):
        pyspark_compat.to_pyspark_struct_type(
            StructType([StructField("userId", LongType())])
        )
    fake_types.StructType.fromJson.assert_called_once_with(
        {
            "type": "struct",
            "fields": [
                {"name": "userId", "type": "long", "nullable": True, "metadata": {}}
            ],
        }
    )
    fake_types.StructType.fromJson.side_effect = ValueError("unknown type time(6)")
    with mock.patch.dict("sys.modules", {"pyspark.sql.types": fake_types}):
        with pytest.raises(NotImplementedError, match="time"):
            pyspark_compat.to_pyspark_struct_type(
                StructType([StructField("t", TimeType())])
            )


def test_with_spark_timestamp_semantics():
    schema = DataSourcePartitioner.formatting_custom_schema(
        "userId INT, a TIMESTAMP, b TIMESTAMP_NTZ, c TIMESTAMP_TZ"
    )
    converted = with_spark_timestamp_semantics(schema)
    assert [f.datatype for f in converted.fields] == [
        IntegerType(),
        TimestampType(TimestampTimeZone.LTZ),
        TimestampType(TimestampTimeZone.NTZ),
        TimestampType(TimestampTimeZone.TZ),
    ]
    assert converted.fields[0]._name == "userId"


def test_protocols():
    assert isinstance(FakeSparkDataSource({}), DataSourceLike)
    assert isinstance(FakeSparkReader(), DataSourceReaderLike)
    assert not isinstance(object(), DataSourceReaderLike)


def test_register_duck_typed_data_source():
    registration = DataSourceRegistration(mock.MagicMock())
    registration.register(UserSparkSource)
    assert registration._get("USER_SPARK_SOURCE") is UserSparkSource
    assert _validate_data_source_class(FakeSparkDataSource) == "fakesparkdatasource"
    with pytest.raises(TypeError, match="pyspark.sql.datasource.DataSource"):
        registration.register(FakeSparkReader)


def test_get_partitions():
    class NoPartitions:
        def read(self, partition):
            yield (1,)

    assert _get_partitions(FakeSparkReader()) == [None]
    assert _get_partitions(NoPartitions()) == [None]
    assert _get_partitions(type("R", (DataSourceReader,), {"read": None})()) == [
        DataSourceReader.partitions(None)[0]
    ]


def test_resolve_udtf_packages_pyspark():
    assert _resolve_udtf_packages(None, requires_pyspark=True) == [
        "snowflake-snowpark-python",
        "pyspark",
    ]
    assert _resolve_udtf_packages(["pyspark==4.2.0"], requires_pyspark=True) == [
        "pyspark==4.2.0",
        "snowflake-snowpark-python",
    ]


def test_variant_and_spark_row_to_json():
    class FakeSparkRow(tuple):
        def __new__(cls, **kwargs):
            row = tuple.__new__(cls, kwargs.values())
            row.__fields__ = list(kwargs.keys())
            return row

    value = {"v": FakeVariantVal('{"a": [1, null]}'), "r": FakeSparkRow(x=1)}
    assert json.loads(_json_dumps(value)) == {"v": {"a": [1, None]}, "r": {"x": 1}}
    assert to_udtf_row((FakeVariantVal("null"),), [], [0]) == (None,)
    assert _json_dumps(FakeVariantVal("null")) is None


def _read_with_mock_session(data_source_cls, user_schema=None):
    session = mock.MagicMock()
    fake_types = mock.MagicMock()
    fake_types.StructType.fromJson.side_effect = FakeSparkStructType
    with mock.patch.dict("sys.modules", {"pyspark.sql.types": fake_types}), mock.patch(
        "snowflake.snowpark._internal.data_source.custom.ingestion._local_ingestion"
    ) as local_ingestion, mock.patch(
        "snowflake.snowpark._internal.data_source.custom.ingestion.set_api_call_source"
    ):
        read_custom_data_source(session, data_source_cls, {}, user_schema)
    return local_ingestion.call_args.args


def test_read_pyspark_data_source_schema():
    _, reader, partitions, schema, *_ = _read_with_mock_session(UserSparkSource)
    assert partitions == [None]
    assert schema.fields[1].datatype == TimestampType(TimestampTimeZone.LTZ)
    # reader() receives a pyspark.sql.types.StructType with the original field names
    assert isinstance(reader.schema, FakeSparkStructType)
    assert [f["name"] for f in reader.schema.jsonValue()["fields"]] == [
        "userId",
        "ts",
    ]


def test_read_pyspark_data_source_struct_type_schema():
    spark_schema = FakeSparkStructType(SPARK_SCHEMA)

    class StructSchemaSource(UserSparkSource):
        def schema(self):
            return spark_schema

    _, reader, _, schema, *_ = _read_with_mock_session(StructSchemaSource)
    # the StructType returned by schema() is passed to reader() as is
    assert reader.schema is spark_schema
    assert schema == spark_json_to_snowpark_schema(SPARK_SCHEMA)


def test_read_pyspark_data_source_not_implemented():
    with pytest.raises(NotImplementedError, match="only supports batch reads"):
        _read_with_mock_session(FakeSparkDataSource, "id INT")
