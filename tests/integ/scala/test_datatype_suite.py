#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import uuid

# Many of the tests have been moved to unit/scala/test_datattype_suite.py
from decimal import Decimal

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.functions import lit
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
    GeometryType,
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
    VectorType,
)
from tests.utils import Utils


def test_verify_datatypes_reference(session):
    schema = StructType(
        [
            StructField("var", VariantType()),
            StructField("geography", GeographyType()),
            StructField("geometry", GeometryType()),
            StructField("date", DateType()),
            StructField("time", TimeType()),
            StructField("timestamp", TimestampType(TimestampTimeZone.NTZ)),
            StructField("string", StringType(19)),
            StructField("boolean", BooleanType()),
            StructField("binary", BinaryType()),
            StructField("byte", ByteType()),
            StructField("short", ShortType()),
            StructField("int", IntegerType()),
            StructField("long", LongType()),
            StructField("float", FloatType()),
            StructField("double", DoubleType()),
            StructField("decimal", DecimalType(10, 2)),
            StructField("array", ArrayType(IntegerType())),
            StructField("map", MapType(ByteType(), TimeType())),
        ]
    )

    df = session.create_dataframe(
        [
            [
                None,
                None,
                None,
                None,
                None,
                None,
                "a",
                True,
                None,
                1,
                2,
                3,
                4,
                5.0,
                6.0,
                Decimal(123),
                None,
                None,
            ]
        ],
        schema,
    )

    expected_schema = StructType(
        [
            StructField("VAR", VariantType()),
            StructField("GEOGRAPHY", GeographyType()),
            StructField("GEOMETRY", GeometryType()),
            StructField("DATE", DateType()),
            StructField("TIME", TimeType()),
            StructField("TIMESTAMP", TimestampType(TimestampTimeZone.NTZ)),
            StructField("STRING", StringType(19)),
            StructField("BOOLEAN", BooleanType()),
            StructField("BINARY", BinaryType()),
            StructField("BYTE", LongType()),
            StructField("SHORT", LongType()),
            StructField("INT", LongType()),
            StructField("LONG", LongType()),
            StructField("FLOAT", DoubleType()),
            StructField("DOUBLE", DoubleType()),
            StructField("DECIMAL", DecimalType(10, 2)),
            StructField("ARRAY", ArrayType(StringType())),
            StructField("MAP", MapType(StringType(), StringType())),
        ]
    )
    Utils.is_schema_same(df.schema, expected_schema, case_sensitive=False)


@pytest.mark.localtest
def test_verify_datatypes_reference2(session):
    d1 = DecimalType(2, 1)
    d2 = DecimalType(2, 1)
    assert d1 == d2

    df = session.range(1).select(
        lit(0.05).cast(DecimalType(5, 2)).as_("a"),
        lit(0.07).cast(DecimalType(7, 2)).as_("b"),
    )

    assert df.collect() == [Row(Decimal("0.05"), Decimal("0.07"))]
    assert (
        str(df.schema.fields)
        == "[StructField('A', DecimalType(5, 2), nullable=False), "
        "StructField('B', DecimalType(7, 2), nullable=False)]"
    )


@pytest.mark.xfail(reason="SNOW-974852 vectors are not yet rolled out", strict=False)
def test_verify_datatypes_reference_vector(session):
    schema = StructType(
        [
            StructField("int_vector", VectorType(int, 3)),
            StructField("float_vector", VectorType(float, 3)),
        ]
    )
    df = session.create_dataframe(
        [
            [
                None,
                None,
            ]
        ],
        schema,
    )

    expected_schema = StructType(
        [
            StructField("INT_VECTOR", VectorType(int, 3)),
            StructField("FLOAT_VECTOR", VectorType(float, 3)),
        ]
    )
    Utils.is_schema_same(df.schema, expected_schema)


def test_dtypes(session):
    schema = StructType(
        [
            StructField("var", VariantType()),
            StructField("geography", GeographyType()),
            StructField("geometry", GeometryType()),
            StructField("date", DateType()),
            StructField("time", TimeType()),
            StructField("timestamp", TimestampType()),
            StructField("string", StringType(22)),
            StructField("boolean", BooleanType()),
            StructField("binary", BinaryType()),
            StructField("byte", ByteType()),
            StructField("short", ShortType()),
            StructField("int", IntegerType()),
            StructField("long", LongType()),
            StructField("float", FloatType()),
            StructField("double", DoubleType()),
            StructField("decimal", DecimalType(10, 2)),
            StructField("array", ArrayType(IntegerType())),
            StructField("map", MapType(ByteType(), TimeType())),
        ]
    )

    df = session.create_dataframe(
        [
            [
                None,
                None,
                None,
                None,
                None,
                None,
                "a",
                True,
                None,
                1,
                2,
                3,
                4,
                5.0,
                6.0,
                Decimal(123),
                None,
                None,
            ]
        ],
        schema,
    )

    assert df.dtypes == [
        ("VAR", "variant"),
        ("GEOGRAPHY", "geography"),
        ("GEOMETRY", "geometry"),
        ("DATE", "date"),
        ("TIME", "time"),
        ("TIMESTAMP", "timestamp"),
        ("STRING", "string(22)"),
        ("BOOLEAN", "boolean"),
        ("BINARY", "binary"),
        ("BYTE", "bigint"),
        ("SHORT", "bigint"),
        ("INT", "bigint"),
        ("LONG", "bigint"),
        ("FLOAT", "double"),
        ("DOUBLE", "double"),
        ("DECIMAL", "decimal(10,2)"),
        ("ARRAY", "array<string>"),
        ("MAP", "map<string,string>"),
    ]


@pytest.mark.parametrize(
    "structured_types_enabled,expected_dtypes,expected_schema",
    [
        (
            True,
            [
                ("VEC", "vector<int,5>"),
                ("MAP", "map<string(16777216),bigint>"),
                ("OBJ", "struct<string(16777216),double>"),
                ("ARR", "array<double>"),
            ],
            StructType(
                [
                    StructField("VEC", VectorType(int, 5), nullable=True),
                    StructField(
                        "MAP", MapType(StringType(), LongType()), nullable=True
                    ),
                    StructField(
                        "OBJ",
                        StructType(
                            [
                                StructField("A", StringType(), nullable=True),
                                StructField("B", DoubleType(), nullable=True),
                            ]
                        ),
                        nullable=True,
                    ),
                    StructField("ARR", ArrayType(DoubleType()), nullable=True),
                ]
            ),
        ),
        (
            False,
            [
                ("VEC", "vector<int,5>"),
                ("MAP", "map<string,string>"),
                ("OBJ", "map<string,string>"),
                ("ARR", "array<string>"),
            ],
            StructType(
                [
                    StructField("VEC", VectorType(int, 5), nullable=True),
                    StructField(
                        "MAP", MapType(StringType(), StringType()), nullable=True
                    ),
                    StructField(
                        "OBJ", MapType(StringType(), StringType()), nullable=True
                    ),
                    StructField("ARR", ArrayType(StringType()), nullable=True),
                ]
            ),
        ),
    ],
)
def test_structured_dtypes(
    session, structured_types_enabled, expected_dtypes, expected_schema
):
    session.structured_types_enabled = structured_types_enabled
    table_name = f"snowpark_structured_dtypes_{uuid.uuid4().hex[:5]}"

    value = "true" if structured_types_enabled else "false"
    session.sql(
        f"alter session set ENABLE_STRUCTURED_TYPES_IN_CLIENT_RESPONSE={value}"
    ).collect()
    session.sql(
        f"alter session set IGNORE_CLIENT_VESRION_IN_STRUCTURED_TYPES_RESPONSE={value}"
    ).collect()
    session.sql(
        f"alter session set FORCE_ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT={value}"
    ).collect()
    try:
        session.sql(
            f"""
        create table if not exists {table_name} (
          vec vector(int, 5),
          map map(varchar, int),
          obj object(a varchar, b float),
          arr array(float)
        );
        """
        ).collect()
        session.sql(
            f"""
        insert into
          {table_name}
        select
          [1,2,3,4,5] :: vector(int, 5),
          object_construct('k1', 1) :: map(varchar, int),
          object_construct('a', 'foo', 'b', 0.05) :: object(a varchar, b float),
          [1.0, 3.1, 4.5] :: array(float)
         ;
        """
        ).collect()
        df = session.table(table_name)
        assert df.schema == expected_schema
        assert df.dtypes == expected_dtypes
    finally:
        pass
        session.sql(f"drop table if exists {table_name}")
    session.structured_types_enabled = True


@pytest.mark.xfail(reason="SNOW-974852 vectors are not yet rolled out", strict=False)
def test_dtypes_vector(session):
    schema = StructType(
        [
            StructField("int_vector", VectorType(int, 3)),
            StructField("float_vector", VectorType(float, 3)),
        ]
    )
    df = session.create_dataframe(
        [
            [
                None,
                None,
            ]
        ],
        schema,
    )

    assert df.dtypes == [
        ("INT_VECTOR", "vector<int,3>"),
        ("FLOAT_VECTOR", "vector<float,3>"),
    ]
