#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import inspect

import pytest

from snowflake.snowpark._internal import spark_schema
from snowflake.snowpark._internal.data_source.datasource_partitioner import (
    DataSourcePartitioner,
)
from snowflake.snowpark._internal.spark_schema import (
    is_pyspark_struct_type,
    pyspark_struct_type_to_snowpark,
    spark_ddl_identifiers_to_snowflake as translate,
    spark_json_to_snowpark_schema,
    spark_json_to_snowpark_type,
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


# A class that looks like PySpark's StructType to Snowpark, so that these tests don't need PySpark.
class FakeSparkStructType:
    def __init__(self, json_value) -> None:
        self._json_value = json_value

    def jsonValue(self):
        return self._json_value


FakeSparkStructType.__module__ = "pyspark.sql.types"
FakeSparkStructType.__name__ = "StructType"

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
    source = inspect.getsource(spark_schema)
    assert "import pyspark" not in source
    assert "from pyspark" not in source


def test_is_pyspark_struct_type():
    assert is_pyspark_struct_type(FakeSparkStructType({}))
    assert not is_pyspark_struct_type(StructType([]))
    assert not is_pyspark_struct_type("a INT")


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


def test_pyspark_struct_type_to_snowpark():
    schema = pyspark_struct_type_to_snowpark(FakeSparkStructType(SPARK_SCHEMA))
    assert schema == spark_json_to_snowpark_schema(SPARK_SCHEMA)


def test_spark_ddl_identifiers_to_snowflake():
    assert translate("a INT, b STRING") == "a INT, b STRING"
    # a name that doesn't need quoting is the same as without backticks
    assert translate("`a` INT, `b`: STRING") == "a INT, b: STRING"
    assert translate("`my col` INT, `x-y`: STRING") == '"my col": INT, "x-y": STRING'
    assert translate("it's INT, `a b` INT") == 'it\'s INT, "a b": INT'
    assert translate('`we``ird"` INT') == '"we`ird""": INT'
    # quoted strings are not changed
    assert (
        translate("a INT COMMENT 'has `ticks`', `b` INT")
        == "a INT COMMENT 'has `ticks`', b INT"
    )
    assert translate('"x`y" INT') == '"x`y" INT'

    schema = DataSourcePartitioner.formatting_custom_schema(
        translate("`my col` INT, `camelCase` STRING, plain tinyint")
    )
    assert [f.name for f in schema.fields] == ['"my col"', "CAMELCASE", "PLAIN"]


def test_schema_accepts_ddl_strings(session):
    reader = session.read.schema(
        "id tinyint, `my col` STRING NOT NULL, `Name` char(3) COMMENT 'it''s, a name'"
    )
    assert reader._user_schema == StructType(
        [
            StructField("ID", ByteType()),
            StructField('"my col"', StringType(), nullable=False),
            StructField("NAME", StringType(3)),
        ]
    )
    with pytest.raises(ValueError, match="Invalid schema string"):
        session.read.schema("int")


def test_schema_accepts_struct_types(session):
    schema = StructType([StructField("ID", LongType())])
    assert session.read.schema(schema)._user_schema is schema


def test_schema_accepts_pyspark_struct_types(session):
    reader = session.read.schema(FakeSparkStructType(SPARK_SCHEMA))
    assert reader._user_schema == spark_json_to_snowpark_schema(SPARK_SCHEMA)
    assert reader._user_schema.fields[0].name == "USERID"
