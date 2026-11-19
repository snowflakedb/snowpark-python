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


def test_spark_ddl_identifiers_to_snowflake_spacing_and_errors():
    assert translate("`a`INT, `b c`INT") == 'a INT, "b c":INT'
    assert translate("`a`: INT") == "a: INT"
    with pytest.raises(ValueError, match="Unterminated backtick-quoted identifier"):
        translate("`abc INT")
    with pytest.raises(ValueError, match="Unterminated backtick-quoted identifier"):
        translate("a INT, `b")


DDL = (
    'id INT, `my col` STRING NOT NULL, `camelCase` STRING, "CaseSens" INT, '
    "s STRUCT<a: INT, `b c`: STRING>, arr ARRAY<STRUCT<`d e`: INT>>"
)


def _equivalent_struct_type():
    return StructType(
        [
            StructField("id", IntegerType()),
            StructField("my col", StringType(), nullable=False),
            StructField("camelCase", StringType()),
            StructField("CaseSens", IntegerType()),
            StructField(
                "s",
                StructType(
                    [
                        StructField("a", IntegerType()),
                        StructField("b c", StringType()),
                    ]
                ),
            ),
            StructField(
                "arr", ArrayType(StructType([StructField("d e", IntegerType())]))
            ),
        ]
    )


def _names(schema):
    result = []
    for field in schema.fields:
        result.append(field._name)
        datatype = field.datatype
        if isinstance(datatype, ArrayType):
            datatype = datatype.element_type
        if isinstance(datatype, StructType):
            result.append(_names(datatype))
    return result


def test_ddl_schema_has_the_field_names_of_the_equivalent_struct_type(session):
    schema = session.read.schema(DDL)._user_schema
    expected = _equivalent_struct_type()
    assert _names(schema) == _names(expected)
    assert _names(schema) == [
        "id",
        "my col",
        "camelCase",
        "CaseSens",
        "s",
        ["a", "b c"],
        "arr",
        ["d e"],
    ]
    assert [f.nullable for f in schema.fields] == [f.nullable for f in expected.fields]
    assert [f.datatype for f in schema.fields[:3]] == [
        f.datatype for f in expected.fields[:3]
    ]
    # a quoted name stays case-sensitive, and the others are not
    assert [f.name for f in schema.fields[:4]] == [
        "ID",
        '"my col"',
        "CAMELCASE",
        '"CaseSens"',
    ]


def test_ddl_schema_is_read_like_the_equivalent_struct_type(session):
    ddl_reader = session.read.schema(DDL)
    expected = _equivalent_struct_type()
    # JSON, Parquet, ...
    ddl_result = ddl_reader._get_schema_from_user_input(ddl_reader._user_schema)
    expected_result = ddl_reader._get_schema_from_user_input(expected)
    assert [(a.name, a.datatype, a.nullable) for a in ddl_result[0]] == [
        (a.name, a.datatype, a.nullable) for a in expected_result[0]
    ]
    assert ddl_result[1] == expected_result[1]
    assert ddl_result[2] == expected_result[2]
    assert '$1:"my col"::' in ddl_result[2][1]
    assert '"""' not in "".join(ddl_result[2])


def test_ddl_schema_is_applied_to_xml_like_the_equivalent_struct_type():
    from unittest import mock

    from snowflake.snowpark.dataframe_reader import DataFrameReader

    def applied_columns(schema):
        df = mock.MagicMock()
        df.columns = []
        reader = DataFrameReader(mock.MagicMock())
        reader._apply_xml_schema(df, schema)
        return [call.args for call in df.__getitem__.call_args_list]

    reader = DataFrameReader(mock.MagicMock())
    schema = reader.schema(DDL)._user_schema
    assert applied_columns(schema) == applied_columns(_equivalent_struct_type())
    assert ("'my col'",) in applied_columns(schema)


def test_ddl_schema_ast_is_the_same_as_for_the_equivalent_struct_type(session):
    from snowflake.snowpark._internal.utils import AstFlagSource, set_ast_state

    original = session.ast_enabled
    set_ast_state(AstFlagSource.TEST, True)
    session.ast_enabled = True
    try:
        ddl_ast = session.read.schema(
            "id INT, `camelCase` STRING NOT NULL, s STRUCT<a: INT>"
        )._ast.dataframe_reader.schema
        struct_ast = session.read.schema(
            StructType(
                [
                    StructField("id", IntegerType()),
                    StructField("camelCase", StringType(), nullable=False),
                    StructField("s", StructType([StructField("a", IntegerType())])),
                ]
            )
        )._ast.dataframe_reader.schema
        assert ddl_ast.ByteSize() > 0
        assert ddl_ast == struct_ast
        # a quoted name is a quoted identifier in the AST, which is how it stays case-sensitive
        quoted_ast = session.read.schema('"CaseSens" INT')._ast.dataframe_reader.schema
        assert quoted_ast.fields[0].column_identifier.column_name.name == '"CaseSens"'
    finally:
        set_ast_state(AstFlagSource.TEST, original)
        session.ast_enabled = original


def test_ddl_schema_is_read_from_csv_like_the_equivalent_struct_type(session):
    reader = session.read.schema("id INT, `my col` STRING NOT NULL, `camelCase` INT")
    expected = StructType(
        [
            StructField("id", IntegerType()),
            StructField("my col", StringType(), nullable=False),
            StructField("camelCase", IntegerType()),
        ]
    )
    result = reader._get_schema_from_csv_user_input(reader._user_schema, True)
    expected_result = reader._get_schema_from_csv_user_input(expected, True)
    assert [(a.name, a.datatype, a.nullable) for a in result[0]] == [
        (a.name, a.datatype, a.nullable) for a in expected_result[0]
    ]
    assert result[1] == expected_result[1]
    assert result[2] == expected_result[2]
