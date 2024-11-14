#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import uuid

# Many of the tests have been moved to unit/scala/test_datattype_suite.py
from decimal import Decimal

import pytest

from snowflake.connector.options import installed_pandas
from snowflake.snowpark import Row
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import (
    any_value,
    array_construct,
    array_sort,
    col,
    lit,
    object_construct,
    sum_distinct,
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
from tests.utils import (
    Utils,
    iceberg_supported,
    structured_types_enabled_session,
    structured_types_supported,
)

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
    True: (
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
    ),
    False: (
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
    ),
}

ICEBERG_CONFIG = {
    "catalog": "SNOWFLAKE",
    "external_volume": "python_connector_iceberg_exvol",
    "base_location": "python_connector_merge_gate",
}


@pytest.fixture(scope="module")
def structured_type_support(session, local_testing_mode):
    yield structured_types_supported(session, local_testing_mode)


@pytest.fixture(scope="module")
def examples(structured_type_support):
    yield STRUCTURED_TYPES_EXAMPLES[structured_type_support]


@pytest.fixture(scope="module")
def structured_type_session(session, structured_type_support):
    if structured_type_support:
        with structured_types_enabled_session(session) as sess:
            yield sess
    else:
        yield session


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
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
    "config.getoption('local_testing_mode', default=False)",
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
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: SNOW-1372813 Cast to StructType not supported",
)
def test_structured_dtypes(structured_type_session, examples):
    query, expected_dtypes, expected_schema = examples
    df = _create_test_dataframe(structured_type_session)
    assert df.schema == expected_schema
    assert df.dtypes == expected_dtypes


@pytest.mark.skipif(
    "config.getoption('disable_sql_simplifier', default=False)",
    reason="without sql_simplifier returned types are all variants",
)
@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: SNOW-1372813 Cast to StructType not supported",
)
def test_structured_dtypes_select(structured_type_session, examples):
    query, expected_dtypes, expected_schema = examples
    df = _create_test_dataframe(structured_type_session)
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


@pytest.mark.skipif(not installed_pandas, reason="Pandas required for this test.")
@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: SNOW-1372813 Cast to StructType not supported",
)
def test_structured_dtypes_pandas(structured_type_session, structured_type_support):
    pdf = _create_test_dataframe(structured_type_session).to_pandas()
    if structured_type_support:
        assert (
            pdf.to_json()
            == '{"MAP":{"0":[["k1",1.0]]},"OBJ":{"0":{"A":"foo","B":0.05}},"ARR":{"0":[1.0,3.1,4.5]}}'
        )
    else:
        assert (
            pdf.to_json()
            == '{"MAP":{"0":"{\\n  \\"k1\\": 1\\n}"},"OBJ":{"0":"{\\n  \\"A\\": \\"foo\\",\\n  \\"B\\": 5.000000000000000e-02\\n}"},"ARR":{"0":"[\\n  1.000000000000000e+00,\\n  3.100000000000000e+00,\\n  4.500000000000000e+00\\n]"}}'
        )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="local testing does not fully support structured types yet.",
)
def test_structured_dtypes_iceberg(
    structured_type_session, local_testing_mode, structured_type_support
):
    if not (
        structured_type_support
        and iceberg_supported(structured_type_session, local_testing_mode)
    ):
        pytest.skip("Test requires iceberg support and structured type support.")
    query, expected_dtypes, expected_schema = STRUCTURED_TYPES_EXAMPLES[True]

    table_name = f"snowpark_structured_dtypes_{uuid.uuid4().hex[:5]}".upper()
    dynamic_table_name = f"snowpark_dynamic_iceberg_{uuid.uuid4().hex[:5]}".upper()
    try:
        create_df = structured_type_session.create_dataframe([], schema=expected_schema)
        create_df.write.save_as_table(table_name, iceberg_config=ICEBERG_CONFIG)
        structured_type_session.sql(
            f"""
        insert into {table_name}
        {query}
        """
        ).collect()
        df = structured_type_session.table(table_name)
        assert df.schema == expected_schema
        assert df.dtypes == expected_dtypes

        save_ddl = structured_type_session._run_query(
            f"select get_ddl('table', '{table_name}')"
        )
        assert save_ddl[0][0] == (
            f"create or replace ICEBERG TABLE {table_name.upper()} (\n\t"
            "MAP MAP(STRING, LONG),\n\tOBJ OBJECT(A STRING, B DOUBLE),\n\tARR ARRAY(DOUBLE)\n)\n "
            "EXTERNAL_VOLUME = 'PYTHON_CONNECTOR_ICEBERG_EXVOL'\n CATALOG = 'SNOWFLAKE'\n "
            "BASE_LOCATION = 'python_connector_merge_gate/';"
        )

        # Try saving as dynamic table
        dyn_df = structured_type_session.table(table_name)
        warehouse = structured_type_session.get_current_warehouse().strip('"')
        dyn_df.create_or_replace_dynamic_table(
            dynamic_table_name,
            warehouse=warehouse,
            lag="1000 minutes",
            mode="errorifexists",
            iceberg_config=ICEBERG_CONFIG,
        )

        dynamic_ddl = structured_type_session._run_query(
            f"select get_ddl('table', '{dynamic_table_name}')"
        )
        assert dynamic_ddl[0][0] == (
            f"create or replace dynamic table {dynamic_table_name}(\n\tMAP,\n\tOBJ,\n\tARR\n) "
            f"target_lag = '16 hours, 40 minutes' refresh_mode = AUTO initialize = ON_CREATE "
            f"warehouse = {warehouse}\n as  SELECT  *  FROM ( SELECT  *  FROM {table_name});"
        )

    finally:
        Utils.drop_table(structured_type_session, table_name)
        Utils.drop_dynamic_table(structured_type_session, dynamic_table_name)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="local testing does not fully support structured types yet.",
)
def test_iceberg_nested_fields(
    structured_type_session, local_testing_mode, structured_type_support
):
    if not (
        structured_type_support
        and iceberg_supported(structured_type_session, local_testing_mode)
    ):
        pytest.skip("Test requires iceberg support and structured type support.")

    table_name = Utils.random_table_name()
    transformed_table_name = Utils.random_table_name()

    expected_schema = StructType(
        [
            StructField(
                "NESTED_DATA",
                StructType(
                    [
                        StructField('"camelCase"', StringType(), nullable=True),
                        StructField('"snake_case"', StringType(), nullable=True),
                        StructField('"PascalCase"', StringType(), nullable=True),
                        StructField(
                            '"nested_map"',
                            MapType(
                                StringType(),
                                StructType(
                                    [
                                        StructField(
                                            '"inner_camelCase"',
                                            StringType(),
                                            nullable=True,
                                        ),
                                        StructField(
                                            '"inner_snake_case"',
                                            StringType(),
                                            nullable=True,
                                        ),
                                        StructField(
                                            '"inner_PascalCase"',
                                            StringType(),
                                            nullable=True,
                                        ),
                                    ],
                                    structured=True,
                                ),
                                structured=True,
                            ),
                            nullable=True,
                        ),
                    ],
                    structured=True,
                ),
                nullable=True,
            )
        ],
        structured=False,
    )

    try:
        structured_type_session.sql(
            f"""
        CREATE OR REPLACE ICEBERG TABLE {table_name} (
            "NESTED_DATA" OBJECT(
                camelCase STRING,
                snake_case STRING,
                PascalCase STRING,
                nested_map MAP(
                    STRING,
                    OBJECT(
                        inner_camelCase STRING,
                        inner_snake_case STRING,
                        inner_PascalCase STRING
                    )
                )
            )
        ) EXTERNAL_VOLUME = 'python_connector_iceberg_exvol' CATALOG = 'SNOWFLAKE' BASE_LOCATION = 'python_connector_merge_gate';
        """
        ).collect()
        df = structured_type_session.table(table_name)
        assert df.schema == expected_schema

        # Round tripping will fail if the inner fields has incorrect names.
        df.write.mode("overwrite").save_as_table(
            table_name=transformed_table_name, iceberg_config=ICEBERG_CONFIG
        )
        assert (
            structured_type_session.table(transformed_table_name).schema
            == expected_schema
        )
    finally:
        Utils.drop_table(structured_type_session, table_name)
        Utils.drop_table(structured_type_session, transformed_table_name)


@pytest.mark.skip(
    reason="SNOW-1748140: Need to handle structured types in datatype_mapper"
)
def test_struct_dtype_iceberg_lqb(
    structured_type_session, local_testing_mode, structured_type_support
):
    if not (
        structured_type_support
        and iceberg_supported(structured_type_session, local_testing_mode)
    ):
        pytest.skip("Test requires iceberg support and structured type support.")

    read_table = f"snowpark_structured_dtypes_lqb_read_{uuid.uuid4().hex[:5]}"
    write_table = f"snowpark_structured_dtypes_lqb_write_{uuid.uuid4().hex[:5]}"
    query = """select
                [1, 2, 3] :: array(bigint) as arr,
                object_construct('k1', 1, 'k2', 2) :: map(varchar, bigint) as map,
                1 as a,
                2 as b
    """
    expected_dtypes = [
        ("ARR", "array<bigint>"),
        ("MAP", "map<string(16777216),bigint>"),
        ("A", "bigint"),
        ("B", "bigint"),
    ]
    expected_schema = StructType(
        [
            StructField("ARR", ArrayType(LongType(), structured=True), nullable=True),
            StructField(
                "MAP",
                MapType(StringType(), LongType(), structured=True),
                nullable=True,
            ),
            StructField("A", LongType(), nullable=True),
            StructField("B", LongType(), nullable=True),
        ]
    )
    is_query_compilation_stage_enabled = (
        structured_type_session._query_compilation_stage_enabled
    )
    is_large_query_breakdown_enabled = (
        structured_type_session._large_query_breakdown_enabled
    )
    original_bounds = structured_type_session._large_query_breakdown_complexity_bounds
    try:
        structured_type_session._query_compilation_stage_enabled = True
        structured_type_session._large_query_breakdown_enabled = True
        structured_type_session._large_query_breakdown_complexity_bounds = (300, 600)

        create_df = structured_type_session.create_dataframe([], schema=expected_schema)
        create_df.write.save_as_table(read_table, iceberg_config=ICEBERG_CONFIG)
        structured_type_session.sql(
            f"""
        insert into {read_table}
        {query}
        """
        ).collect()

        base_df = structured_type_session.table(read_table)
        assert base_df.schema == expected_schema
        assert base_df.dtypes == expected_dtypes

        df1 = base_df.with_column("A", col("A") + lit(1))
        df2 = base_df.with_column("B", col("B") + lit(1))

        for i in range(6):
            df1 = df1.with_column("A", col("A") + lit(i) + col("A"))
            df2 = df2.with_column("B", col("B") + lit(i) + col("B"))

        df1 = df1.group_by(col("A")).agg(
            sum_distinct(col("B")).alias("B"),
            any_value(col("ARR")).alias("ARR"),
            any_value(col("MAP")).alias("MAP"),
        )
        df2 = df2.group_by(col("B")).agg(
            sum_distinct(col("A")).alias("A"),
            any_value(col("ARR")).alias("ARR"),
            any_value(col("MAP")).alias("MAP"),
        )
        union_df = df1.union_all(df2)
        union_df = union_df.select(
            array_sort("ARR", sort_ascending=False).alias("ARR"), "MAP", "A", "B"
        )

        assert union_df.schema == expected_schema

        union_df.write.save_as_table(
            write_table,
            column_order="name",
            mode="overwrite",
            iceberg_config=ICEBERG_CONFIG,
        )

        queries = union_df.queries
        # assert that the queries are broken down into 2 queries and 1 post action
        assert len(queries["queries"]) == 2, queries["queries"]
        assert len(queries["post_actions"]) == 1
        final_df = structured_type_session.table(write_table)

        # assert that
        assert final_df.schema == expected_schema
        assert final_df.dtypes == expected_dtypes
    finally:
        structured_type_session._query_compilation_stage_enabled = (
            is_query_compilation_stage_enabled
        )
        structured_type_session._large_query_breakdown_enabled = (
            is_large_query_breakdown_enabled
        )
        structured_type_session._large_query_breakdown_complexity_bounds = (
            original_bounds
        )
        Utils.drop_table(structured_type_session, read_table)
        Utils.drop_table(structured_type_session, write_table)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="local testing does not fully support structured types yet.",
)
def test_structured_dtypes_iceberg_create_from_values(
    structured_type_session, local_testing_mode, structured_type_support
):
    if not (
        structured_type_support
        and iceberg_supported(structured_type_session, local_testing_mode)
    ):
        pytest.skip("Test requires iceberg support and structured type support.")

    _, __, expected_schema = STRUCTURED_TYPES_EXAMPLES[True]
    table_name = f"snowpark_structured_dtypes_{uuid.uuid4().hex[:5]}"
    data = [
        ({"x": 1}, {"A": "a", "B": 1}, [1, 1, 1]),
        ({"x": 2}, {"A": "b", "B": 2}, [2, 2, 2]),
    ]
    try:
        create_df = structured_type_session.create_dataframe(
            data, schema=expected_schema
        )
        create_df.write.save_as_table(table_name, iceberg_config=ICEBERG_CONFIG)
        assert structured_type_session.table(table_name).order_by(
            col("ARR"), ascending=True
        ).collect() == [Row(*d) for d in data]
    finally:
        Utils.drop_table(structured_type_session, table_name)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="local testing does not fully support structured types yet.",
)
def test_structured_dtypes_iceberg_udf(
    structured_type_session, local_testing_mode, structured_type_support
):
    if not (
        structured_type_support
        and iceberg_supported(structured_type_session, local_testing_mode)
    ):
        pytest.skip("Test requires iceberg support and structured type support.")
    query, expected_dtypes, expected_schema = STRUCTURED_TYPES_EXAMPLES[True]

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
        create_df = structured_type_session.create_dataframe([], schema=expected_schema)
        create_df.write.save_as_table(table_name, iceberg_config=ICEBERG_CONFIG)
        structured_type_session.sql(
            f"""
        insert into {table_name}
        {query}
        """
        ).collect()

        df = structured_type_session.table(table_name)
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
        Utils.drop_table(structured_type_session, table_name)


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


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: SNOW-1372813 Cast to StructType not supported",
)
def test_structured_dtypes_cast(structured_type_session, structured_type_support):
    if not structured_type_support:
        pytest.skip("Test requires structured type support.")
    expected_semi_schema = StructType(
        [
            StructField("ARR", ArrayType(StringType()), nullable=True),
            StructField("MAP", MapType(StringType(), StringType()), nullable=True),
            StructField("OBJ", MapType(StringType(), StringType()), nullable=True),
        ]
    )
    expected_structured_schema = StructType(
        [
            StructField("ARR", ArrayType(LongType(), structured=True), nullable=True),
            StructField(
                "MAP",
                MapType(StringType(100), LongType(), structured=True),
                nullable=True,
            ),
            StructField(
                "OBJ",
                StructType(
                    [
                        StructField("A", DoubleType(), nullable=True),
                        StructField("B", StringType(100), nullable=True),
                    ],
                    structured=True,
                ),
                nullable=True,
            ),
        ]
    )
    df = structured_type_session.create_dataframe(
        [[[1, 2, 3], {"k1": 1, "k2": 2}, {"A": 1.0, "B": "foobar"}]],
        schema=StructType(
            [
                StructField("arr", ArrayType()),
                StructField("map", MapType()),
                StructField("obj", MapType()),
            ]
        ),
    )
    assert df.schema == expected_semi_schema
    assert df.collect() == [
        Row(
            "[\n  1,\n  2,\n  3\n]",
            '{\n  "k1": 1,\n  "k2": 2\n}',
            '{\n  "A": 1,\n  "B": "foobar"\n}',
        )
    ]

    cast_df = df.select(
        df.arr.cast(ArrayType(IntegerType(), structured=True)).alias("arr"),
        df.map.cast(MapType(StringType(100), IntegerType(), structured=True)).alias(
            "map"
        ),
        df.obj.cast(
            StructType(
                [StructField("A", FloatType()), StructField("B", StringType(100))],
                structured=True,
            )
        ).alias("obj"),
    )
    assert cast_df.schema == expected_structured_schema
    assert cast_df.collect() == [
        Row([1, 2, 3], {"k1": 1, "k2": 2}, {"A": 1.0, "B": "foobar"})
    ]
