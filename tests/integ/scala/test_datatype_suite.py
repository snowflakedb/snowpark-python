#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import uuid

# Many of the tests have been moved to unit/scala/test_datattype_suite.py
from decimal import Decimal

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import (
    array_construct,
    col,
    lit,
    object_construct,
    udf,
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
from tests.utils import IS_ICEBERG_SUPPORTED, IS_STRUCTURED_TYPES_SUPPORTED, Utils

# Map of structured type enabled state to test params


# make sure dataframe creation is the same as _create_test_dataframe
_STRUCTURE_DATAFRAME_QUERY = """
select
  object_construct('k1', 1) :: map(varchar, int) as map,
  object_construct('A', 'foo', 'B', 0.05) :: object(A varchar, B float) as obj,
  [1.0, 3.1, 4.5] :: array(float) as arr
"""


# make sure dataframe creation is the same as _STRUCTURE_DATAFRAME_QUERY
def _create_test_dataframe(s):
    df = s.create_dataframe([1], schema=["a"]).select(
        object_construct(lit("k1"), lit(1))
        .cast(MapType(StringType(), IntegerType(), structured=True))
        .alias("map"),
        object_construct(lit("A"), lit("foo"), lit("B"), lit(0.05))
        .cast(
            StructType(
                [StructField("A", StringType()), StructField("B", DoubleType())],
                structured=True,
            )
        )
        .alias("obj"),
        array_construct(lit(1.0), lit(3.1), lit(4.5))
        .cast(ArrayType(FloatType(), structured=True))
        .alias("arr"),
    )
    return df


STRUCTURED_TYPES_EXAMPLES = {
    True: pytest.param(
        _STRUCTURE_DATAFRAME_QUERY,
        [
            ("MAP", "map<string(16777216),bigint>"),
            ("OBJ", "struct<string(16777216),double>"),
            ("ARR", "array<double>"),
        ],
        StructType(
            [
                StructField(
                    "MAP",
                    MapType(StringType(16777216), LongType(), structured=True),
                    nullable=True,
                ),
                StructField(
                    "OBJ",
                    StructType(
                        [
                            StructField("A", StringType(16777216), nullable=True),
                            StructField("B", DoubleType(), nullable=True),
                        ],
                        structured=True,
                    ),
                    nullable=True,
                ),
                StructField(
                    "ARR", ArrayType(DoubleType(), structured=True), nullable=True
                ),
            ]
        ),
        id="structured-types-enabled",
    ),
    False: pytest.param(
        _STRUCTURE_DATAFRAME_QUERY,
        [
            ("MAP", "map<string,string>"),
            ("OBJ", "map<string,string>"),
            ("ARR", "array<string>"),
        ],
        StructType(
            [
                StructField("MAP", MapType(StringType(), StringType()), nullable=True),
                StructField("OBJ", MapType(StringType(), StringType()), nullable=True),
                StructField("ARR", ArrayType(StringType()), nullable=True),
            ]
        ),
        id="legacy",
    ),
}


@pytest.mark.skipif(
    "config.getvalue('local_testing_mode')",
    reason="FEAT: function to_geography not supported",
)
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


@pytest.mark.skipif(
    "config.getvalue('local_testing_mode')",
    reason="FEAT: function to_geography not supported",
)
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


@pytest.mark.skipif(
    "config.getvalue('local_testing_mode')",
    reason="FEAT: Cast to StructType not supported",
)
@pytest.mark.parametrize(
    "query,expected_dtypes,expected_schema",
    [STRUCTURED_TYPES_EXAMPLES[IS_STRUCTURED_TYPES_SUPPORTED]],
)
def test_structured_dtypes(session, query, expected_dtypes, expected_schema):
    df = _create_test_dataframe(session)
    assert df.schema == expected_schema
    assert df.dtypes == expected_dtypes


@pytest.mark.skipif(
    "config.getvalue('local_testing_mode')",
    reason="FEAT: Cast to StructType not supported",
)
@pytest.mark.parametrize(
    "query,expected_dtypes,expected_schema",
    [STRUCTURED_TYPES_EXAMPLES[IS_STRUCTURED_TYPES_SUPPORTED]],
)
def test_structured_dtypes_select(session, query, expected_dtypes, expected_schema):
    df = _create_test_dataframe(session)
    flattened_df = df.select(
        df.map["k1"].alias("value1"),
        df.obj["A"].alias("a"),
        col("obj")["B"].alias("b"),
        df.arr[0].alias("value2"),
        df.arr[1].alias("value3"),
        col("arr")[2].alias("value4"),
    )
    assert flattened_df.schema == StructType(
        [
            StructField("VALUE1", LongType(), nullable=True),
            StructField("A", StringType(16777216), nullable=True),
            StructField("B", DoubleType(), nullable=True),
            StructField("VALUE2", DoubleType(), nullable=True),
            StructField("VALUE3", DoubleType(), nullable=True),
            StructField("VALUE4", DoubleType(), nullable=True),
        ]
    )
    assert flattened_df.dtypes == [
        ("VALUE1", "bigint"),
        ("A", "string(16777216)"),
        ("B", "double"),
        ("VALUE2", "double"),
        ("VALUE3", "double"),
        ("VALUE4", "double"),
    ]
    assert flattened_df.collect() == [
        Row(VALUE1=1, A="foo", B=0.05, VALUE2=1.0, VALUE3=3.1, VALUE4=4.5)
    ]


@pytest.mark.skipif(
    "config.getvalue('local_testing_mode')",
    reason="FEAT: Cast to StructType not supported",
)
@pytest.mark.parametrize(
    "query,expected_dtypes,expected_schema",
    [STRUCTURED_TYPES_EXAMPLES[IS_STRUCTURED_TYPES_SUPPORTED]],
)
def test_structured_dtypes_pandas(session, query, expected_dtypes, expected_schema):
    pdf = _create_test_dataframe(session).to_pandas()
    if IS_STRUCTURED_TYPES_SUPPORTED:
        assert (
            pdf.to_json()
            == '{"MAP":{"0":[["k1",1.0]]},"OBJ":{"0":{"A":"foo","B":0.05}},"ARR":{"0":[1.0,3.1,4.5]}}'
        )
    else:
        assert (
            pdf.to_json()
            == '{"MAP":{"0":"{\\n  \\"k1\\": 1\\n}"},"OBJ":{"0":"{\\n  \\"A\\": \\"foo\\",\\n  \\"B\\": 5.000000000000000e-02\\n}"},"ARR":{"0":"[\\n  1.000000000000000e+00,\\n  3.100000000000000e+00,\\n  4.500000000000000e+00\\n]"}}'
        )


@pytest.mark.skip(
    "SNOW-1356851: Skipping until iceberg testing infrastructure is added."
)
@pytest.mark.skipif(
    not (IS_STRUCTURED_TYPES_SUPPORTED and IS_ICEBERG_SUPPORTED),
    reason="Test requires iceberg support and structured type support.",
)
@pytest.mark.parametrize(
    "query,expected_dtypes,expected_schema",
    [STRUCTURED_TYPES_EXAMPLES[IS_STRUCTURED_TYPES_SUPPORTED]],
)
def test_structured_dtypes_iceberg(session, query, expected_dtypes, expected_schema):
    table_name = f"snowpark_structured_dtypes_{uuid.uuid4().hex[:5]}"
    try:
        session.sql(
            f"""
        create iceberg table if not exists {table_name} (
          map map(varchar, int),
          obj object(a varchar, b float),
          arr array(float)
        )
        CATALOG = 'SNOWFLAKE'
        EXTERNAL_VOLUME = 'python_connector_iceberg_exvol'
        BASE_LOCATION = 'python_connector_merge_gate';
        """
        ).collect()
        session.sql(
            f"""
        insert into {table_name}
        {query}
        """
        ).collect()
        df = session.table(table_name)
        assert df.schema == expected_schema
        assert df.dtypes == expected_dtypes
    finally:
        session.sql(f"drop table if exists {table_name}")


@pytest.mark.skip(
    "SNOW-1356851: Skipping until iceberg testing infrastructure is added."
)
@pytest.mark.skipif(
    not (IS_STRUCTURED_TYPES_SUPPORTED and IS_ICEBERG_SUPPORTED),
    reason="Test requires iceberg support and structured type support.",
)
@pytest.mark.parametrize(
    "query,expected_dtypes,expected_schema",
    [STRUCTURED_TYPES_EXAMPLES[IS_STRUCTURED_TYPES_SUPPORTED]],
)
def test_structured_dtypes_iceberg_udf(
    session, query, expected_dtypes, expected_schema
):
    table_name = f"snowpark_structured_dtypes_udf_test{uuid.uuid4().hex[:5]}"

    def nop(x):
        return x

    (map_type, object_type, array_type) = expected_schema
    nop_map_udf = udf(
        nop, return_type=map_type.datatype, input_types=[map_type.datatype]
    )
    nop_object_udf = udf(
        nop, return_type=object_type.datatype, input_types=[object_type.datatype]
    )
    nop_array_udf = udf(
        nop, return_type=array_type.datatype, input_types=[array_type.datatype]
    )

    try:
        session.sql(
            f"""
        create iceberg table if not exists {table_name} (
          map map(varchar, int),
          obj object(A varchar, B float),
          arr array(float)
        )
        CATALOG = 'SNOWFLAKE'
        EXTERNAL_VOLUME = 'python_connector_iceberg_exvol'
        BASE_LOCATION = 'python_connector_merge_gate';
        """
        ).collect()
        session.sql(
            f"""
        insert into {table_name}
        {query}
        """
        ).collect()

        df = session.table(table_name)
        working = df.select(
            nop_object_udf(col("obj")).alias("obj"),
            nop_array_udf(col("arr")).alias("arr"),
        )
        assert working.schema == StructType([object_type, array_type])

        with pytest.raises(SnowparkSQLException):
            # SNOW-XXXXXXX: Map not supported as a udf return type.
            df.select(
                nop_map_udf(col("map")).alias("map"),
            ).collect()
    finally:
        session.sql(f"drop table if exists {table_name}")


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
