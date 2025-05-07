#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import uuid

# Many of the tests have been moved to unit/scala/test_datattype_suite.py
from decimal import Decimal

import logging
import pytest

import snowflake.snowpark.context as context
from snowflake.connector.options import installed_pandas
from snowflake.snowpark import Row
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import (
    any_value,
    array_construct,
    array_sort,
    col,
    lit,
    object_construct,
    sum_distinct,
    udaf,
    udf,
    to_file,
)
from snowflake.snowpark.session import Session
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
    FileType,
)
from tests.utils import (
    TempObjectType,
    TestFiles,
    Utils,
    iceberg_supported,
    structured_types_enabled_session,
    structured_types_supported,
)

# Map of structured type enabled state to test params


# make sure dataframe creation is the same as _create_test_dataframe
_STRUCTURED_DATAFRAME_QUERY = """
select
  object_construct('k1', 1) :: map(varchar, int) as map,
  object_construct('A', 'foo', 'b', 0.05) :: object(A varchar, b float) as obj,
  [1.0, 3.1, 4.5] :: array(float) as arr
"""


# make sure dataframe creation is the same as _STRUCTURED_DATAFRAME_QUERY
def _create_test_dataframe(s, structured_type_support):
    nested_field_name = "b" if structured_type_support else "B"
    df = s.create_dataframe([1], schema=["a"]).select(
        object_construct(lit("k1"), lit(1))
        .cast(MapType(StringType(), IntegerType(), structured=structured_type_support))
        .alias("map"),
        object_construct(lit("A"), lit("foo"), lit(nested_field_name), lit(0.05))
        .cast(
            StructType(
                [
                    StructField("A", StringType()),
                    StructField(nested_field_name, DoubleType()),
                ],
                structured=structured_type_support,
            )
        )
        .alias("obj"),
        array_construct(lit(1.0), lit(3.1), lit(4.5))
        .cast(ArrayType(FloatType(), structured=structured_type_support))
        .alias("arr"),
    )
    return df


ICEBERG_CONFIG = {
    "catalog": "SNOWFLAKE",
    "external_volume": "python_connector_iceberg_exvol",
    "base_location": "python_connector_merge_gate",
}

# When creating tables the max string size remain 16mb regardless of lob setting
MAX_TABLE_STRING_SIZE = 2**24


def _create_example(structured_types_enabled, max_string):
    if structured_types_enabled:
        return (
            _STRUCTURED_DATAFRAME_QUERY,
            [
                ("MAP", f"map<string({max_string}),bigint>"),
                ("OBJ", f"struct<string({max_string}),double>"),
                ("ARR", "array<double>"),
            ],
            StructType(
                [
                    StructField(
                        "MAP",
                        MapType(StringType(max_string), LongType(), structured=True),
                        nullable=True,
                    ),
                    StructField(
                        "OBJ",
                        StructType(
                            [
                                StructField("A", StringType(max_string), nullable=True),
                                StructField("b", DoubleType(), nullable=True),
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
        )
    else:
        return (
            _STRUCTURED_DATAFRAME_QUERY,
            [
                ("MAP", "map<string,string>"),
                ("OBJ", "map<string,string>"),
                ("ARR", "array<string>"),
            ],
            StructType(
                [
                    StructField(
                        "MAP", MapType(StringType(), StringType()), nullable=True
                    ),
                    StructField(
                        "OBJ", MapType(StringType(), StringType()), nullable=True
                    ),
                    StructField("ARR", ArrayType(StringType()), nullable=True),
                ]
            ),
        )


@pytest.fixture(scope="module")
def structured_type_support(session, local_testing_mode):
    yield structured_types_supported(session, local_testing_mode)


@pytest.fixture(scope="module")
def examples(structured_type_session, structured_type_support, max_string):
    yield _create_example(structured_type_support, max_string)


@pytest.fixture(scope="module")
def structured_type_session(session, structured_type_support, local_testing_mode):
    if structured_type_support:
        with structured_types_enabled_session(session) as sess:
            yield sess
    else:
        yield session


@pytest.fixture(scope="module")
def max_string(structured_type_session):
    # SNOW-1938099: When creating tables the default string size is 16mb regardless of
    # what the lob parameters are set to. Iceberg and select statements use max sized strings.
    return structured_type_session._conn.max_string_size


@pytest.fixture(scope="module")
def server_side_max_string(structured_type_session):
    # SNOW-1938099: SFCTEST0 seems to have an unstable value returned when creating session so for now
    # derive the max string size from account parameters rather than session parameters.

    non_default_value = None
    try:
        enabled = structured_type_session.sql(
            "show parameters like 'ENABLE_LARGE_VARCHAR_AND_BINARY_IN_RESULT'"
        ).collect()
        if enabled[0].value == "true":
            value = structured_type_session.sql(
                "show parameters like 'MAX_LOB_SIZE_IN_MEMORY'"
            ).collect()
            non_default_value = int(value[0].value)
    except Exception:
        pass
    return non_default_value or structured_type_session._conn.max_string_size


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
def test_structured_dtypes(structured_type_session, examples, structured_type_support):
    query, expected_dtypes, expected_schema = examples
    df = _create_test_dataframe(structured_type_session, structured_type_support)
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
def test_structured_dtypes_select(
    structured_type_session, examples, structured_type_support, max_string
):
    query, expected_dtypes, expected_schema = examples
    df = _create_test_dataframe(structured_type_session, structured_type_support)
    nested_field_name = "b" if context._should_use_structured_type_semantics() else "B"
    flattened_df = df.select(
        df.map["k1"].alias("value1"),
        df.obj["A"].alias("a"),
        col("obj")[nested_field_name].alias("b"),
        df.arr[0].alias("value2"),
        df.arr[1].alias("value3"),
        col("arr")[2].alias("value4"),
    )

    # Semi structured schemas can't extract inner types
    override_type = None if structured_type_support else VariantType()
    override_dtype = None if structured_type_support else "variant"

    assert flattened_df.schema == StructType(
        [
            StructField("VALUE1", override_type or LongType(), nullable=True),
            StructField("A", override_type or StringType(max_string), nullable=True),
            StructField(
                nested_field_name, override_type or DoubleType(), nullable=True
            ),
            StructField("VALUE2", override_type or DoubleType(), nullable=True),
            StructField("VALUE3", override_type or DoubleType(), nullable=True),
            StructField("VALUE4", override_type or DoubleType(), nullable=True),
        ]
    )
    assert flattened_df.dtypes == [
        ("VALUE1", override_dtype or "bigint"),
        ("A", override_dtype or f"string({max_string})"),
        ("B", override_dtype or "double"),
        ("VALUE2", override_dtype or "double"),
        ("VALUE3", override_dtype or "double"),
        ("VALUE4", override_dtype or "double"),
    ]

    if structured_type_support:
        expected_row = Row(
            VALUE1=1, A="foo", B=0.05, VALUE2=1.0, VALUE3=3.1, VALUE4=4.5
        )
    else:
        expected_row = Row(
            VALUE1="1", A='"foo"', B="0.05", VALUE2="1", VALUE3="3.1", VALUE4="4.5"
        )
    assert flattened_df.collect() == [expected_row]


@pytest.mark.skipif(not installed_pandas, reason="Pandas required for this test.")
@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: SNOW-1372813 Cast to StructType not supported",
)
def test_structured_dtypes_pandas(structured_type_session, structured_type_support):
    pdf = _create_test_dataframe(
        structured_type_session, structured_type_support
    ).to_pandas()
    if structured_type_support:
        assert (
            pdf.to_json()
            == '{"MAP":{"0":[["k1",1.0]]},"OBJ":{"0":{"A":"foo","b":0.05}},"ARR":{"0":[1.0,3.1,4.5]}}'
        )
    else:
        assert (
            pdf.to_json()
            == '{"MAP":{"0":"{\\n  \\"k1\\": 1\\n}"},"OBJ":{"0":"{\\n  \\"A\\": \\"foo\\",\\n  \\"B\\": 0.05\\n}"},"ARR":{"0":"[\\n  1,\\n  3.1,\\n  4.5\\n]"}}'
        )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="local testing does not fully support structured types yet.",
)
def test_structured_dtypes_iceberg(
    structured_type_session,
    local_testing_mode,
    structured_type_support,
    server_side_max_string,
):

    if not (
        structured_type_support
        and iceberg_supported(structured_type_session, local_testing_mode)
    ):
        pytest.skip("Test requires iceberg support and structured type support.")
    query, expected_dtypes, expected_schema = _create_example(
        True, server_side_max_string
    )

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
            "MAP MAP(STRING, LONG),\n\tOBJ OBJECT(A STRING, b DOUBLE),\n\tARR ARRAY(DOUBLE)\n)\n "
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
        formatted_table_name = (
            table_name
            if structured_type_session.sql_simplifier_enabled
            else f"({table_name})"
        )

        assert dynamic_ddl[0][0] == (
            f"create or replace dynamic iceberg table {dynamic_table_name}(\n\tMAP,\n\tOBJ,\n\tARR\n)"
            " target_lag = '16 hours, 40 minutes' refresh_mode = AUTO initialize = ON_CREATE "
            f"warehouse = {warehouse} external_volume = 'PYTHON_CONNECTOR_ICEBERG_EXVOL'  "
            "catalog = 'SNOWFLAKE'  base_location = 'python_connector_merge_gate/' \n as  "
            f"SELECT  *  FROM ( SELECT  *  FROM {formatted_table_name});"
        )

    finally:
        Utils.drop_table(structured_type_session, table_name)
        Utils.drop_dynamic_table(structured_type_session, dynamic_table_name)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="local testing does not fully support structured types yet.",
)
def test_structured_dtypes_negative(structured_type_session, structured_type_support):
    if not structured_type_support:
        pytest.skip("Test requires structured type support.")

    with pytest.raises(ValueError, match="MapType requires key and value type be set."):
        MapType()

    with pytest.raises(ValueError, match="MapType requires key and value type be set."):
        MapType(StringType())


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="local testing does not fully support structured types yet.",
)
@pytest.mark.skipif(
    "config.getoption('enable_ast', default=False)",
    reason="SNOW-1862700: AST does not support new structured type semantics yet.",
)
def test_udaf_structured_map_downcast(
    structured_type_session, structured_type_support, caplog
):
    if not structured_type_support:
        pytest.skip("Test requires structured type support.")

    with caplog.at_level(logging.WARNING):

        @udaf(return_type=MapType(StringType(), StringType(), structured=True))
        class MapCollector:
            def __init__(self) -> None:
                self._agg_state = dict()

            @property
            def aggregate_state(self) -> dict:
                return self._agg_state

            def accumulate(self, int_: int) -> None:
                self._agg_state[int_] = self._agg_state.get(int_, 0) + 1

            def merge(self, other_state: int) -> None:
                self._agg_state = {**self._agg_state, **other_state}

            def finish(self) -> dict:
                return self._agg_state

        assert (
            "Snowflake does not support structured maps as return type for UDAFs. Downcasting to semi-structured object."
            in caplog.text
        )
        assert MapCollector._return_type == StructType()


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="local testing does not fully support structured types yet.",
)
def test_structured_type_infer(structured_type_session, structured_type_support):
    if not structured_type_support:
        pytest.skip("Test requires structured type support.")

    struct = Row(f1="v1", f2=2)
    df = structured_type_session.create_dataframe(
        [
            ({"key": "value"}, [1, 2, 3], struct),
        ],
        schema=["map", "array", "obj"],
    )

    assert df.schema == StructType(
        [
            StructField(
                "MAP",
                MapType(StringType(), StringType(), structured=True),
                nullable=True,
            ),
            StructField("ARRAY", ArrayType(LongType(), structured=True), nullable=True),
            StructField(
                "OBJ",
                StructType(
                    [
                        StructField("f1", StringType(), nullable=True),
                        StructField("f2", LongType(), nullable=True),
                    ],
                    structured=True,
                ),
                nullable=True,
            ),
        ],
        structured=True,
    )
    df.collect()


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="local testing does not fully support structured types yet.",
)
def test_iceberg_nested_fields(
    structured_type_session,
    local_testing_mode,
    structured_type_support,
    server_side_max_string,
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
                        StructField(
                            "camelCase",
                            StringType(server_side_max_string),
                            nullable=True,
                        ),
                        StructField(
                            "snake_case",
                            StringType(server_side_max_string),
                            nullable=True,
                        ),
                        StructField(
                            "PascalCase",
                            StringType(server_side_max_string),
                            nullable=True,
                        ),
                        StructField(
                            "nested_map",
                            MapType(
                                StringType(server_side_max_string),
                                StructType(
                                    [
                                        StructField(
                                            "inner_camelCase",
                                            StringType(server_side_max_string),
                                            nullable=True,
                                        ),
                                        StructField(
                                            "inner_snake_case",
                                            StringType(server_side_max_string),
                                            nullable=True,
                                        ),
                                        StructField(
                                            "inner_PascalCase",
                                            StringType(server_side_max_string),
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
        structured=True,
    )

    try:
        structured_type_session.sql(
            f"""
        CREATE OR REPLACE ICEBERG TABLE {table_name} (
            "NESTED_DATA" OBJECT(
                camelCase STRING({server_side_max_string}),
                snake_case STRING({server_side_max_string}),
                PascalCase STRING({server_side_max_string}),
                nested_map MAP(
                    STRING({server_side_max_string}),
                    OBJECT(
                        inner_camelCase STRING({server_side_max_string}),
                        inner_snake_case STRING({server_side_max_string}),
                        inner_PascalCase STRING({server_side_max_string})
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


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="local testing does not fully support structured types yet.",
    run=False,
)
@pytest.mark.parametrize("cte_enabled", [True, False])
def test_struct_dtype_iceberg_lqb(
    structured_type_session,
    local_testing_mode,
    structured_type_support,
    cte_enabled,
    server_side_max_string,
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
        ("MAP", f"map<string({server_side_max_string}),bigint>"),
        ("A", "bigint"),
        ("B", "bigint"),
    ]
    expected_schema = StructType(
        [
            StructField("ARR", ArrayType(LongType(), structured=True), nullable=True),
            StructField(
                "MAP",
                MapType(
                    StringType(server_side_max_string), LongType(), structured=True
                ),
                nullable=True,
            ),
            StructField("A", LongType(), nullable=True),
            StructField("B", LongType(), nullable=True),
        ]
    )
    is_query_compilation_stage_enabled = (
        structured_type_session._query_compilation_stage_enabled
    )
    is_cte_optimization_enabled = structured_type_session._cte_optimization_enabled
    is_large_query_breakdown_enabled = (
        structured_type_session._large_query_breakdown_enabled
    )
    original_bounds = structured_type_session._large_query_breakdown_complexity_bounds
    try:
        structured_type_session._query_compilation_stage_enabled = True
        structured_type_session._cte_optimization_enabled = cte_enabled
        structured_type_session._large_query_breakdown_enabled = True
        structured_type_session._large_query_breakdown_complexity_bounds = (
            (300, 600) if structured_type_session.sql_simplifier_enabled else (50, 80)
        )

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
        structured_type_session._cte_optimization_enabled = is_cte_optimization_enabled
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
    structured_type_session,
    local_testing_mode,
    structured_type_support,
    server_side_max_string,
):
    if not (
        structured_type_support
        and iceberg_supported(structured_type_session, local_testing_mode)
    ):
        pytest.skip("Test requires iceberg support and structured type support.")

    _, __, expected_schema = _create_example(True, server_side_max_string)
    table_name = f"snowpark_structured_dtypes_{uuid.uuid4().hex[:5]}"
    data = [
        ({"x": 1}, Row(A="a", b=1), [1, 1, 1]),
        ({"x": 2}, Row(A="b", b=2), [2, 2, 2]),
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
    structured_type_session,
    local_testing_mode,
    structured_type_support,
    server_side_max_string,
):
    if not (
        structured_type_support
        and iceberg_supported(structured_type_session, local_testing_mode)
    ):
        pytest.skip("Test requires iceberg support and structured type support.")
    query, expected_dtypes, expected_schema = _create_example(
        True, server_side_max_string
    )

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
@pytest.mark.skipif(
    "config.getoption('enable_ast', default=False)",
    reason="SNOW-1862700: AST does not support new structured type semantics yet.",
)
def test_structured_dtypes_cast(structured_type_session, structured_type_support):
    if not structured_type_support:
        pytest.skip("Test requires structured type support.")
    expected_semi_schema = StructType(
        [
            StructField("ARR", ArrayType(), nullable=True),
            StructField("MAP", StructType(), nullable=True),
            StructField("OBJ", StructType(), nullable=True),
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
                StructField("map", StructType()),
                StructField("obj", StructType()),
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
        Row([1, 2, 3], {"k1": 1, "k2": 2}, Row(A=1.0, B="foobar"))
    ]


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="local testing does not fully support structured types yet.",
)
def test_structured_type_print_schema(
    structured_type_session, local_testing_mode, structured_type_support, capsys
):
    if not (
        structured_type_support
        and iceberg_supported(structured_type_session, local_testing_mode)
    ):
        pytest.skip("Test requires iceberg support and structured type support.")

    schema = StructType(
        [
            StructField(
                "map",
                MapType(
                    StringType(),
                    ArrayType(
                        StructType(
                            [
                                StructField("Field1", StringType()),
                                StructField("Field2", IntegerType()),
                            ],
                            structured=True,
                        ),
                        structured=True,
                    ),
                    structured=True,
                ),
            )
        ],
        structured=True,
    )

    df = structured_type_session.create_dataframe([], schema=schema)
    df.printSchema()
    captured = capsys.readouterr()
    assert captured.out == (
        "root\n"
        ' |-- "MAP": MapType (nullable = True)\n'
        " |   |-- key: StringType()\n"
        " |   |-- value: ArrayType\n"
        " |   |   |-- element: StructType\n"
        ' |   |   |   |-- "Field1": StringType() (nullable = True)\n'
        ' |   |   |   |-- "Field2": LongType() (nullable = True)\n'
    )

    # Test that depth works as expected
    assert df._format_schema(1) == ('root\n |-- "MAP": MapType (nullable = True)')
    assert df._format_schema(2) == (
        "root\n"
        ' |-- "MAP": MapType (nullable = True)\n'
        " |   |-- key: StringType()\n"
        " |   |-- value: ArrayType"
    )
    assert df._format_schema(3) == (
        "root\n"
        ' |-- "MAP": MapType (nullable = True)\n'
        " |   |-- key: StringType()\n"
        " |   |-- value: ArrayType\n"
        " |   |   |-- element: StructType"
    )

    # Check that column names can be translated
    assert (
        df._format_schema(1, translate_columns={'"MAP"': '"map"'})
        == 'root\n |-- "map": MapType (nullable = True)'
    )

    # Check that column types can be translated
    assert (
        df._format_schema(
            2,
            translate_types={
                "MapType": "dict",
                "StringType": "str",
                "ArrayType": "list",
            },
        )
        == 'root\n |-- "MAP": dict (nullable = True)\n |   |-- key: str\n |   |-- value: list'
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="local testing does not fully support structured types yet.",
)
def test_structured_array_contains_null(
    structured_type_session, structured_type_support
):
    if not structured_type_support:
        pytest.skip("Test requires structured type support.")

    array_df = structured_type_session.sql(
        "select [1, 2, 3] :: ARRAY(INT NOT NULL) as A, [1, 2, 3] :: ARRAY(INT) as A_N"
    )
    expected_schema = StructType(
        [
            StructField(
                "A", ArrayType(LongType(), structured=True, contains_null=False)
            ),
            StructField(
                "A_N", ArrayType(LongType(), structured=True, contains_null=True)
            ),
        ]
    )
    assert array_df.schema == expected_schema

    table_name = (
        f"snowpark_structured_dtypes_contains_null_{uuid.uuid4().hex[:5]}".upper()
    )
    try:
        # Create table from select statement
        non_null_df = structured_type_session.create_dataframe(
            [
                ([1, 2, 3],),
            ],
            schema=StructType(
                [
                    StructField(
                        "A",
                        ArrayType(IntegerType(), contains_null=False),
                        nullable=False,
                    )
                ]
            ),
        )
        non_null_df.write.save_as_table(table_name)
        save_ddl = structured_type_session._run_query(
            f"select get_ddl('table', '{table_name}')"
        )
        # Not null dropped because dataframe created from select cannot maintain nullability
        assert save_ddl[0][0] == (
            f"create or replace TABLE {table_name.upper()} (\n\tA ARRAY(NUMBER(38,0))\n);"
        )
    finally:
        Utils.drop_table(structured_type_session, table_name)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="local testing does not fully support structured types yet.",
)
def test_structured_map_value_contains_null(
    structured_type_session, structured_type_support, max_string
):
    if not structured_type_support:
        pytest.skip("Test requires structured type support.")

    array_df = structured_type_session.sql(
        "select {'test' : 'test'} :: MAP(STRING, STRING NOT NULL) AS M, {'test' : 'test'} :: MAP(STRING, STRING) AS M_N"
    )
    expected_schema = StructType(
        [
            StructField(
                "M",
                MapType(
                    StringType(),
                    StringType(),
                    structured=True,
                    value_contains_null=False,
                ),
            ),
            StructField(
                "M_N",
                MapType(
                    StringType(),
                    StringType(),
                    structured=True,
                    value_contains_null=True,
                ),
            ),
        ]
    )
    assert array_df.schema == expected_schema

    table_name = f"snowpark_structured_map_contains_null_{uuid.uuid4().hex[:5]}".upper()
    try:
        # Create table from select statement
        non_null_df = structured_type_session.create_dataframe(
            [
                ({"a": "b"},),
            ],
            schema=StructType(
                [
                    StructField(
                        "A",
                        MapType(StringType(), StringType(), value_contains_null=False),
                        nullable=False,
                    )
                ]
            ),
        )
        non_null_df.write.save_as_table(table_name)
        save_ddl = structured_type_session._run_query(
            f"select get_ddl('table', '{table_name}')"
        )
        # Not null dropped because dataframe created from select cannot maintain nullability
        assert save_ddl[0][0] == (
            f"create or replace TABLE {table_name.upper()} (\n\tA MAP(VARCHAR({max_string}), VARCHAR({max_string}))\n);"
        )
    finally:
        Utils.drop_table(structured_type_session, table_name)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="local testing does not fully support structured types yet.",
)
def test_structured_type_schema_expression(
    structured_type_session, local_testing_mode, structured_type_support, max_string
):
    # Test does not require iceberg support, but does require FDN table structured type support
    # which is enabled in the same accounts as iceberg.
    if not (
        structured_type_support
        and iceberg_supported(structured_type_session, local_testing_mode)
    ):
        pytest.skip("Test requires structured type support.")

    table_name = f"snowpark_schema_expresion_test_{uuid.uuid4().hex[:5]}".upper()
    non_null_table_name = (
        f"snowpark_schema_expresion_nonnull_test_{uuid.uuid4().hex[:5]}".upper()
    )
    nested_table_name = (
        f"snowpark_schema_expresion_nested_test_{uuid.uuid4().hex[:5]}".upper()
    )

    expected_schema = StructType(
        [
            StructField(
                "MAP",
                MapType(StringType(), DoubleType(), structured=True),
                nullable=True,
            ),
            StructField("ARR", ArrayType(DoubleType(), structured=True), nullable=True),
            StructField(
                "OBJ",
                StructType(
                    [
                        StructField("FIELD1", StringType(), nullable=True),
                        StructField("FIELD2", DoubleType(), nullable=True),
                    ],
                    structured=True,
                ),
                nullable=True,
            ),
        ]
    )

    expected_non_null_schema = StructType(
        [
            StructField(
                "MAP",
                MapType(StringType(), DoubleType(), structured=True),
                nullable=False,
            ),
            StructField(
                "ARR", ArrayType(DoubleType(), structured=True), nullable=False
            ),
            StructField(
                "OBJ",
                StructType(
                    [
                        StructField("FIELD1", StringType(), nullable=False),
                        StructField("FIELD2", DoubleType(), nullable=False),
                    ],
                    structured=True,
                ),
                nullable=False,
            ),
        ]
    )

    expected_nested_schema = StructType(
        [
            StructField(
                "MAP",
                MapType(
                    StringType(),
                    StructType(
                        [StructField("ARR", ArrayType(DoubleType(), structured=True))],
                        structured=True,
                    ),
                    structured=True,
                ),
            )
        ]
    )

    try:
        # SNOW-1819428: Nullability doesn't seem to be respected when creating
        # a structured type dataframe so use a table instead.
        structured_type_session.sql(
            f"create table {table_name} (MAP MAP(STRING({max_string}), DOUBLE), ARR ARRAY(DOUBLE), "
            f"OBJ OBJECT(FIELD1 STRING({max_string}), FIELD2 DOUBLE))"
        ).collect()
        structured_type_session.sql(
            f"create table {non_null_table_name} (MAP MAP(STRING({max_string}), DOUBLE) NOT NULL, "
            f"ARR ARRAY(DOUBLE) NOT NULL, OBJ OBJECT(FIELD1 STRING({max_string}) NOT NULL, FIELD2 "
            "DOUBLE NOT NULL) NOT NULL)"
        ).collect()
        structured_type_session.sql(
            f"create table {nested_table_name} (MAP MAP(STRING({max_string}), OBJECT(ARR ARRAY(DOUBLE))))"
        ).collect()

        table = structured_type_session.table(table_name)
        non_null_table = structured_type_session.table(non_null_table_name)
        nested_table = structured_type_session.table(nested_table_name)

        assert table.schema == expected_schema
        assert non_null_table.schema == expected_non_null_schema
        assert nested_table.schema == expected_nested_schema

        # Dataframe.union forces a schema_expression call
        assert table.union(table).schema == expected_schema
        # Functions used in schema generation don't respect nested nullability so compare query string instead
        non_null_union = non_null_table.union(non_null_table)
        expected_schema = (
            f"""( SELECT object_construct_keep_null('a' ::  STRING ({max_string}), NULL :: DOUBLE) :: """
            f"""MAP(STRING({max_string}), DOUBLE) AS "MAP", to_array(NULL :: DOUBLE) :: ARRAY(DOUBLE) AS "ARR", """
            f"""object_construct_keep_null('FIELD1', 'a' ::  STRING ({max_string}), 'FIELD2', 0 :: DOUBLE) :: """
            f"""OBJECT("FIELD1" STRING({max_string}), "FIELD2" DOUBLE) AS "OBJ") UNION ( """
            f"""SELECT object_construct_keep_null('a' ::  STRING ({max_string}), NULL :: DOUBLE) :: """
            f"""MAP(STRING({max_string}), DOUBLE) AS "MAP", to_array(NULL :: DOUBLE) :: ARRAY(DOUBLE) """
            f"""AS "ARR", object_construct_keep_null('FIELD1', 'a' ::  STRING ({max_string}), 'FIELD2', """
            f"""0 :: DOUBLE) :: OBJECT("FIELD1" STRING({max_string}), "FIELD2" DOUBLE) AS "OBJ")"""
        )
        assert non_null_union._plan.schema_query == expected_schema

        assert nested_table.union(nested_table).schema == expected_nested_schema
    finally:
        Utils.drop_table(structured_type_session, table_name)
        Utils.drop_table(structured_type_session, non_null_table_name)
        Utils.drop_table(structured_type_session, nested_table_name)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Structured types are not supported in Local Testing",
)
def test_stored_procedure_with_structured_returns(
    structured_type_session,
    structured_type_support,
    local_testing_mode,
    resources_path,
    max_string,
):
    if not structured_type_support:
        pytest.skip("Structured types not enabled in this account.")

    test_files = TestFiles(resources_path)
    tmp_stage_name = Utils.random_stage_name()
    if not local_testing_mode:
        Utils.create_stage(structured_type_session, tmp_stage_name, is_temporary=True)
        structured_type_session.add_packages("snowflake-snowpark-python")
    Utils.upload_to_stage(
        structured_type_session,
        tmp_stage_name,
        test_files.test_sp_py_file,
        compress=False,
    )

    expected_dtypes = [
        ("VEC", "vector<int,5>"),
        ("MAP", f"map<string({max_string}),bigint>"),
        ("OBJ", f"struct<string({max_string}),double>"),
        ("ARR", "array<double>"),
    ]
    expected_schema = StructType(
        [
            StructField("VEC", VectorType(int, 5), nullable=True),
            StructField(
                "MAP",
                MapType(StringType(max_string), LongType(), structured=True),
                nullable=True,
            ),
            StructField(
                "OBJ",
                StructType(
                    [
                        StructField("a", StringType(max_string), nullable=True),
                        StructField("b", DoubleType(), nullable=True),
                    ],
                    structured=True,
                ),
                nullable=True,
            ),
            StructField("ARR", ArrayType(DoubleType(), structured=True), nullable=True),
        ]
    )

    sproc_name = Utils.random_name_for_temp_object(TempObjectType.PROCEDURE)

    def test_sproc(_session: Session) -> DataFrame:
        return _session.sql(
            """
        select
          [1,2,3,4,5] :: vector(int, 5) as vec,
          object_construct('k1', 1) :: map(varchar, int) as map,
          object_construct('a', 'foo', 'b', 0.05) :: object(a varchar, b float) as obj,
          [1.0, 3.1, 4.5] :: array(float) as arr
         ;
        """
        )

    structured_type_session.sproc.register(
        test_sproc,
        name=sproc_name,
        replace=True,
    )
    df = structured_type_session.call(sproc_name)
    assert df.schema == expected_schema
    assert df.dtypes == expected_dtypes


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Structured types are not supported in Local Testing",
)
def test_cast_structtype_rename(structured_type_session, structured_type_support):
    if not structured_type_support:
        pytest.skip("Test requires structured type support.")
    data = [
        ({"firstname": "James", "middlename": "", "lastname": "Smith"}, "1991-04-01")
    ]
    schema = StructType(
        [
            StructField(
                "name",
                StructType(
                    [
                        StructField("firstname", StringType(), True),
                        StructField("middlename", StringType(), True),
                        StructField("lastname", StringType(), True),
                    ]
                ),
            ),
            StructField("dob", StringType(), True),
        ]
    )

    schema2 = StructType(
        [
            StructField("fname", StringType()),
            StructField("middlename", StringType()),
            StructField("lname", StringType()),
        ]
    )

    df = structured_type_session.create_dataframe(data, schema)
    Utils.check_answer(
        df.select(
            col("name").cast(schema2, rename_fields=True).as_("new_name"), col("dob")
        ),
        [
            Row(
                NEW_NAME=Row(fname="James", middlename="", lname="Smith"),
                DOB="1991-04-01",
            )
        ],
    )
    with pytest.raises(
        ValueError, match="is_add and is_rename cannot be set to True at the same time"
    ):
        df.select(
            col("name")
            .cast(schema2, rename_fields=True, add_fields=True)
            .as_("new_name"),
            col("dob"),
        )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Structured types are not supported in Local Testing",
)
def test_cast_structtype_add(structured_type_session, structured_type_support):
    if not structured_type_support:
        pytest.skip("Test requires structured type support.")
    data = [
        ({"firstname": "James", "middlename": "", "lastname": "Smith"}, "1991-04-01")
    ]
    schema = StructType(
        [
            StructField(
                "name",
                StructType(
                    [
                        StructField("firstname", StringType(), True),
                        StructField("middlename", StringType(), True),
                        StructField("lastname", StringType(), True),
                    ]
                ),
            ),
            StructField("dob", StringType(), True),
        ]
    )

    schema2 = StructType(
        [
            StructField("firstname", StringType()),
            StructField("middlename", StringType()),
            StructField("lastname", StringType()),
            StructField("extra", StringType()),
        ]
    )

    df = structured_type_session.create_dataframe(data, schema)
    Utils.check_answer(
        df.select(
            col("name").cast(schema2, add_fields=True).as_("new_name"), col("dob")
        ),
        [
            Row(
                NEW_NAME=Row(fname="James", middlename="", lname="Smith", extra=None),
                DOB="1991-04-01",
            )
        ],
    )
    with pytest.raises(
        ValueError, match="is_add and is_rename cannot be set to True at the same time"
    ):
        df.select(
            col("name")
            .cast(schema2, rename_fields=True, add_fields=True)
            .as_("new_name"),
            col("dob"),
        )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Structured types are not supported in Local Testing",
)
def test_non_nullable_schema(structured_type_session, structured_type_support):
    if not structured_type_support:
        pytest.skip("Test requires structured type support.")

    schema = StructType(
        [
            StructField(
                "struct",
                StructType(
                    [
                        StructField("name", StringType(), True),
                        StructField("age", IntegerType(), True),
                    ]
                ),
                False,
            )
        ]
    )
    df = structured_type_session.createDataFrame(
        [({"name": "Alice", "age": 2},), ({"name": "Bob", "age": 5},)], schema
    )
    assert df._format_schema() == (
        "root\n"
        ' |-- "STRUCT": StructType (nullable = True)\n'
        ' |   |-- "name": StringType() (nullable = True)\n'
        ' |   |-- "age": LongType() (nullable = True)'
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="File type is not supported in Local Testing",
)
def test_file_type(session, resources_path):
    stage_name = Utils.random_name_for_temp_object(TempObjectType.STAGE)
    _ = session.sql(f"create or replace temp stage {stage_name}").collect()
    test_files = TestFiles(resources_path)
    _ = session.file.put(
        test_files.test_file_csv, f"@{stage_name}", auto_compress=False, overwrite=True
    )
    df = session.range(1).select(to_file(f"@{stage_name}/testCSV.csv").alias("file"))
    assert df.schema == StructType([StructField("file", FileType(), False)])
    df = session.range(1).select(
        lit(f"@{stage_name}/testCSV.csv", datatype=FileType()).alias("file")
    )
    assert df.schema == StructType([StructField("file", FileType(), False)])
    df = session.range(1).select(lit(None, datatype=FileType()).alias("file"))
    assert df.schema == StructType([StructField("file", FileType(), True)])


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="local testing does not fully support structured types yet.",
)
def test_nest_struct_field_names(structured_type_session, structured_type_support):
    if not structured_type_support:
        pytest.skip("Test requires structured type support.")
    schema = StructType(
        [
            StructField(
                "A", StructType([StructField("field with space", StringType(), True)])
            )
        ]
    )
    df = structured_type_session.create_dataframe(
        [{"A": {"field with space": "value"}}], schema
    )
    Utils.check_answer(df, [Row(A=Row(**{"field with space": "value"}))])
