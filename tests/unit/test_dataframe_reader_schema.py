#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from unittest import mock

import pytest

from snowflake.snowpark._internal.utils import AstFlagSource, set_ast_state
from snowflake.snowpark.dataframe_reader import DataFrameReader
from snowflake.snowpark.types import (
    ArrayType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

DDL = (
    'id INT, "CaseSens" STRING NOT NULL, "my col" INT, "a""b" INT, '
    's STRUCT<x: INT, "Y z": STRING>, arr ARRAY<STRUCT<"Q q": INT>>, '
    'm MAP<STRING, STRUCT<"K k": INT>>'
)


def _equivalent_struct_type():
    return StructType(
        [
            StructField("id", IntegerType()),
            StructField('"CaseSens"', StringType(), nullable=False),
            StructField('"my col"', IntegerType()),
            StructField('"a""b"', IntegerType()),
            StructField(
                "s",
                StructType(
                    [
                        StructField("x", IntegerType()),
                        StructField('"Y z"', StringType()),
                    ]
                ),
            ),
            StructField(
                "arr",
                ArrayType(StructType([StructField('"Q q"', IntegerType())])),
            ),
            StructField(
                "m",
                MapType(
                    StringType(), StructType([StructField('"K k"', IntegerType())])
                ),
            ),
        ]
    )


def _states(schema):
    result = []
    for field in schema.fields:
        datatype = field.datatype
        nested = []
        if isinstance(datatype, ArrayType):
            datatype = datatype.element_type
        if isinstance(datatype, MapType):
            datatype = datatype.value_type
        if isinstance(datatype, StructType):
            nested = _states(datatype)
        result.append(
            (
                field._name,
                field.name,
                field.original_column_identifier,
                field.nullable,
                nested,
            )
        )
    return result


def _xml_columns(schema):
    df = mock.MagicMock()
    df.columns = []
    DataFrameReader(mock.MagicMock())._apply_xml_schema(df, schema)
    keys = [call.args for call in df.__getitem__.call_args_list]
    value = df.__getitem__.return_value
    aliases = [call.args for call in value.alias.call_args_list] + [
        call.args for call in value.cast.return_value.alias.call_args_list
    ]
    return keys, sorted(aliases)


def test_ddl_schema_is_the_equivalent_struct_type(session):
    schema = session.read.schema(DDL)._user_schema
    expected = _equivalent_struct_type()
    assert schema == expected
    assert _states(schema) == _states(expected)
    assert schema.simple_string() == expected.simple_string()
    assert [f._name for f in schema.fields[:4]] == [
        "id",
        '"CaseSens"',
        '"my col"',
        '"a""b"',
    ]
    # unquoted names are case-insensitive and quoted names are case-sensitive
    assert [f.name for f in schema.fields[:3]] == ["ID", '"CaseSens"', '"my col"']
    assert (
        schema != session.read.schema("id INT, CaseSens STRING NOT NULL")._user_schema
    )


def test_ddl_schema_ast_is_the_same_as_for_the_equivalent_struct_type(session):
    original = session.ast_enabled
    set_ast_state(AstFlagSource.TEST, True)
    session.ast_enabled = True
    try:
        ddl_ast = session.read.schema(DDL)._ast.dataframe_reader.schema
        struct_ast = session.read.schema(
            _equivalent_struct_type()
        )._ast.dataframe_reader.schema
        assert ddl_ast.ByteSize() > 0
        assert ddl_ast == struct_ast
        assert [f.column_identifier.column_name.name for f in ddl_ast.fields[:4]] == [
            "id",
            '"CaseSens"',
            '"my col"',
            '"a""b"',
        ]
    finally:
        set_ast_state(AstFlagSource.TEST, original)
        session.ast_enabled = original


def test_ddl_schema_is_read_like_the_equivalent_struct_type(session):
    reader = session.read.schema(DDL)
    expected = _equivalent_struct_type()

    # JSON and Parquet
    result = reader._get_schema_from_user_input(reader._user_schema)
    expected_result = reader._get_schema_from_user_input(expected)
    assert [(a.name, a.datatype, a.nullable) for a in result[0]] == [
        (a.name, a.datatype, a.nullable) for a in expected_result[0]
    ]
    assert result[1] == expected_result[1]
    assert result[2] == expected_result[2]
    assert [a.name for a in result[0][:4]] == [
        '"id"',
        '"CaseSens"',
        '"my col"',
        '"a""b"',
    ]
    assert result[2][0].startswith('$1:"id"::')
    assert result[2][1].startswith('$1:"CaseSens"::')
    assert result[2][2].startswith('$1:"my col"::')
    assert result[2][3].startswith('$1:"a""b"::')
    assert [name for _, name in result[1][:4]] == ["id", "CaseSens", "my col", 'a"b']

    # CSV
    csv_result = reader._get_schema_from_csv_user_input(reader._user_schema, True)
    csv_expected = reader._get_schema_from_csv_user_input(expected, True)
    assert [(a.name, a.datatype) for a in csv_result[0]] == [
        (a.name, a.datatype) for a in csv_expected[0]
    ]
    assert csv_result[1:] == csv_expected[1:]

    # XML
    assert _xml_columns(reader._user_schema) == _xml_columns(expected)
    keys, aliases = _xml_columns(reader._user_schema)
    assert keys[:4] == [("'id'",), ("'CaseSens'",), ("'my col'",), ("'a\"b'",)]
    assert ('"CaseSens"',) in aliases and ('"id"',) in aliases


def test_unquoted_field_names_are_read_as_before():
    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("Mixed", StringType(), nullable=False),
            StructField("UP", IntegerType()),
        ]
    )
    reader = DataFrameReader(mock.MagicMock())
    attributes, to_cast, transformations = reader._get_schema_from_user_input(schema)
    assert [a.name for a in attributes] == ['"id"', '"Mixed"', '"UP"']
    assert to_cast == [
        ('$1:"id"::INT', "id"),
        ('$1:"Mixed"::STRING', "Mixed"),
        ('$1:"UP"::INT', "UP"),
    ]
    assert transformations == [expr for expr, _ in to_cast]
    keys, aliases = _xml_columns(schema)
    assert keys == [("'id'",), ("'Mixed'",), ("'UP'",)]
    assert aliases == [('"Mixed"',), ('"UP"',), ('"id"',)]


def test_schema_rejects_invalid_ddl_strings(session):
    with pytest.raises(ValueError, match="Invalid schema string"):
        session.read.schema("int")
