#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import datetime
import random
import string

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import SnowparkClientException
from snowflake.snowpark.functions import call_udf, col, lit, max, min, udf
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    GeographyType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    TimestampType,
    TimeType,
    VariantType,
)
from tests.utils import TestData, TestFiles, Utils

pytestmark = pytest.mark.udf

tmp_stage_name = Utils.random_stage_name()
tmp_table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
table1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
table2 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
semi_structured_table = Utils.random_name_for_temp_object(TempObjectType.TABLE)
view1 = f'"{Utils.random_name_for_temp_object(TempObjectType.VIEW)}"'
view2 = f'"{Utils.random_name_for_temp_object(TempObjectType.VIEW)}"'


@pytest.fixture(scope="module", autouse=True)
def setup(session, resources_path):
    test_files = TestFiles(resources_path)

    Utils.create_stage(session, tmp_stage_name, is_temporary=True)
    Utils.upload_to_stage(
        session, tmp_stage_name, test_files.test_file_parquet, compress=False
    )
    Utils.create_table(session, table1, "a int")
    session._run_query(f"insert into {table1} values(1),(2),(3)")
    Utils.create_table(session, table2, "a int, b int")
    session._run_query(f"insert into {table2} values(1, 2),(2, 3),(3, 4)")
    yield
    Utils.drop_table(session, tmp_table_name)
    Utils.drop_table(session, table1)
    Utils.drop_table(session, table2)
    Utils.drop_table(session, semi_structured_table)
    Utils.drop_view(session, view1)
    Utils.drop_view(session, view2)
    Utils.drop_stage(session, tmp_stage_name)


def test_basic_udf_function(session):
    df = session.table(table1)
    double_udf = udf(
        lambda x: x + x, return_type=IntegerType(), input_types=[IntegerType()]
    )
    Utils.check_answer(df.select(double_udf("a")).collect(), [Row(2), Row(4), Row(6)])


def test_udf_with_arrays(session):
    Utils.create_table(session, semi_structured_table, "a1 array")
    session._run_query(
        f"insert into {semi_structured_table} "
        f"select (array_construct('1', '2', '3'))"
    )
    session._run_query(
        f"insert into {semi_structured_table} "
        f"select (array_construct('4', '5', '6'))"
    )
    df = session.table(semi_structured_table)
    list_udf = udf(
        lambda x: ",".join(x),
        return_type=StringType(),
        input_types=[ArrayType(StringType())],
    )
    Utils.check_answer(
        df.select(list_udf("a1")).collect(), [Row("1,2,3"), Row("4,5,6")]
    )


def test_udf_with_map_input(session):
    Utils.create_table(session, semi_structured_table, "o1 object")
    session._run_query(
        f"insert into {semi_structured_table} "
        f"select (object_construct('1', 'one', '2', 'two'))"
    )
    session._run_query(
        f"insert into {semi_structured_table} "
        f"select (object_construct('10', 'ten', '20', 'twenty'))"
    )
    df = session.table(semi_structured_table)
    map_keys_udf = udf(
        lambda x: list(x.keys()),
        return_type=ArrayType(StringType()),
        input_types=[MapType(StringType(), StringType())],
    )
    array_sum_udf = udf(
        lambda x: sum(int(i) for i in x),
        return_type=IntegerType(),
        input_types=[ArrayType(StringType())],
    )
    Utils.check_answer(
        df.select(array_sum_udf(map_keys_udf("o1"))).collect(), [Row(3), Row(30)]
    )


def test_udf_with_map_return(session):
    Utils.create_table(session, semi_structured_table, "a1 array")
    session._run_query(
        f"insert into {semi_structured_table} "
        f"select (array_construct('1', '2', '3'))"
    )
    session._run_query(
        f"insert into {semi_structured_table} "
        f"select (array_construct('4', '5', '6'))"
    )
    df = session.table(semi_structured_table)
    map_udf = udf(
        lambda x: {i: f"convert_to_map{i}" for i in x},
        return_type=MapType(StringType(), StringType()),
        input_types=[ArrayType(StringType())],
    )
    res = Utils.get_sorted_rows(df.select(map_udf("a1")).collect())
    assert len(res) == 2
    for i in [1, 2, 3]:
        assert f"convert_to_map{i}" in res[0][0]
    for i in [4, 5, 6]:
        assert f"convert_to_map{i}" in res[1][0]


def test_udf_with_multiple_args_of_map_array(session):
    Utils.create_table(
        session, semi_structured_table, "o1 object, o2 object, id varchar"
    )
    session._run_query(
        f"insert into {semi_structured_table} "
        f"(select object_construct('1', 'one', '2', 'two'), "
        f"object_construct('one', '10', 'two', '20'), "
        f"'ID1')"
    )
    session._run_query(
        f"insert into {semi_structured_table} "
        f"(select object_construct('3', 'three', '4', 'four'), "
        f"object_construct('three', '30', 'four', '40'), "
        f"'ID2')"
    )
    df = session.table(semi_structured_table)

    def f(map1, map2, id):
        values = [map2[key] for key in map1.values()]
        res = sum(int(v) for v in values)
        return {id: str(res)}

    map_udf = udf(
        f,
        return_type=MapType(StringType(), StringType()),
        input_types=[
            MapType(StringType(), StringType()),
            MapType(StringType(), StringType()),
            StringType(),
        ],
    )
    res = Utils.get_sorted_rows(df.select(map_udf("o1", "o2", "id")).collect())
    assert len(res) == 2
    assert '"ID1": "30"' in res[0][0]
    assert '"ID2": "70"' in res[1][0]


def test_filter_on_top_of_udf(session):
    df = session.table(table1)
    double_udf = udf(
        lambda x: x + x, return_type=IntegerType(), input_types=[IntegerType()]
    )
    Utils.check_answer(
        df.select(double_udf("a")).filter(col("$1") > 4).collect(), [Row(6)]
    )


def test_compose_on_dataframe_reader(session, resources_path):
    df = (
        session.read.option("INFER_SCHEMA", False)
        .parquet(f"@{tmp_stage_name}/test.parquet")
        .to_df("a")
    )
    replace_udf = udf(
        lambda elem: elem.replace("num", "id"),
        return_type=StringType(),
        input_types=[StringType()],
    )
    Utils.check_answer(
        df.select(replace_udf("a")).collect(),
        [
            Row('{"id":1,"str":"str1"}'),
            Row('{"id":2,"str":"str2"}'),
        ],
    )


def test_view_with_udf(session):
    TestData.column_has_special_char(session).create_or_replace_view(view1)
    df1 = session.sql(f"select * from {view1}")
    udf1 = udf(
        lambda x, y: x + y,
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
    )
    df1.with_column(
        '"col #"', udf1(col('"col %"'), col('"col *"'))
    ).create_or_replace_view(view2)
    Utils.check_answer(
        session.sql(f"select * from {view2}").collect(),
        [
            Row(1, 2, 3),
            Row(3, 4, 7),
        ],
    )


def test_string_return_type(session):
    df = session.table(table1)
    prefix = "Hello"
    string_udf = udf(
        lambda x: f"{prefix}{x}",
        return_type=StringType(),
        input_types=[IntegerType()],
    )
    Utils.check_answer(
        df.select("a", string_udf("a")).collect(),
        [
            Row(1, "Hello1"),
            Row(2, "Hello2"),
            Row(3, "Hello3"),
        ],
    )


def test_large_closure(session):
    df = session.table(table1)
    factor = 64
    long_string = "".join(random.choices(string.ascii_letters, k=factor * 1024))
    string_udf = udf(
        lambda x: f"{long_string}{x}",
        return_type=StringType(),
        input_types=[IntegerType()],
    )
    rows = df.select(string_udf("a")).collect()
    assert rows[1][0].startswith(long_string)


def test_udf_function_with_multiple_columns(session):
    df = session.table(table2)
    sum_udf = udf(
        lambda x, y: x + y,
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
    )
    Utils.check_answer(
        df.with_column("c", sum_udf("a", "b")).collect(),
        [
            Row(1, 2, 3),
            Row(2, 3, 5),
            Row(3, 4, 7),
        ],
    )


def test_incorrect_number_of_args(session):
    df = session.table(table2)
    string_udf = udf(
        lambda x: f"Hello{x}", return_type=StringType(), input_types=[IntegerType()]
    )
    with pytest.raises(ValueError) as ex_info:
        assert df.with_column("c", string_udf("a", "b"))
    assert "Incorrect number of arguments passed to the UDF" in str(ex_info)


def test_call_udf_api(session):
    df = session.table(table1)
    function_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)
    session.udf.register(
        lambda x: x + x,
        return_type=IntegerType(),
        input_types=[IntegerType()],
        name=function_name,
    )
    Utils.check_answer(
        df.with_column(
            "c",
            call_udf(
                f"{session.get_fully_qualified_current_schema()}.{function_name}",
                col("a"),
            ),
        ).collect(),
        [Row(1, 2), Row(2, 4), Row(3, 6)],
    )


def test_long_type(session):
    df = session.create_dataframe([1, 2, 3]).to_df("a")
    long_udf = udf(lambda x: x + x, return_type=LongType(), input_types=[LongType()])
    Utils.check_answer(
        df.select("a", long_udf("a")).collect(),
        [
            Row(1, 2),
            Row(2, 4),
            Row(3, 6),
        ],
    )


def test_short_type(session):
    df = session.create_dataframe([1, 2, 3]).to_df("a")
    short_udf = udf(lambda x: x + x, return_type=ShortType(), input_types=[ShortType()])
    Utils.check_answer(
        df.select("a", short_udf("a")).collect(),
        [
            Row(1, 2),
            Row(2, 4),
            Row(3, 6),
        ],
    )


def test_float_type(session):
    df = session.create_dataframe([1.1, 2.2, 3.3]).to_df("a")
    float_udf = udf(lambda x: x + x, return_type=FloatType(), input_types=[FloatType()])
    Utils.check_answer(
        df.select("a", float_udf("a")).collect(),
        [
            Row(1.1, 2.2),
            Row(2.2, 4.4),
            Row(3.3, 6.6),
        ],
    )


def test_double_type(session):
    df = session.create_dataframe([1.01, 2.01, 3.01]).to_df("a")
    double_udf = udf(
        lambda x: x + x, return_type=DoubleType(), input_types=[DoubleType()]
    )
    Utils.check_answer(
        df.select("a", double_udf("a")).collect(),
        [
            Row(1.01, 2.02),
            Row(2.01, 4.02),
            Row(3.01, 6.02),
        ],
    )


def test_boolean_type(session):
    df = session.create_dataframe([[1, 1], [2, 2], [3, 4]]).to_df("a", "b")
    boolean_udf = udf(
        lambda x, y: x == y,
        return_type=BooleanType(),
        input_types=[IntegerType(), IntegerType()],
    )
    Utils.check_answer(
        df.select(boolean_udf("a", "b")).collect(),
        [
            Row(True),
            Row(True),
            Row(False),
        ],
    )


def test_binary_type(session):
    data = ["Hello", "World"]
    bytes_data = [bytes(s, "utf8") for s in data]
    df1 = session.create_dataframe(data).to_df("a")
    df2 = session.create_dataframe(bytes_data).to_df("a")
    to_binary = udf(
        lambda x: bytes(x, "utf8"),
        return_type=BinaryType(),
        input_types=[StringType()],
    )
    from_binary = udf(
        lambda x: x.decode("utf8"),
        return_type=StringType(),
        input_types=[BinaryType()],
    )
    Utils.check_answer(
        df1.select(to_binary("a")).collect(), [Row(s) for s in bytes_data]
    )
    Utils.check_answer(df2.select(from_binary("a")).collect(), [Row(s) for s in data])


def test_date_and_timestamp_type(session):
    data = [
        [datetime.date(2019, 1, 1), datetime.datetime(2019, 1, 1)],
        [datetime.date(2020, 1, 1), datetime.datetime(2020, 1, 1)],
        [None, None],
    ]

    def to_timestamp(d):
        return datetime.datetime(d.year, d.month, d.day) if d else None

    def to_date(t):
        return t.date() if t else None

    out = [Row(to_timestamp(d), to_date(t)) for d, t in data]
    df = session.create_dataframe(data).to_df("date", "timestamp")
    to_timestamp_udf = udf(
        to_timestamp, return_type=TimestampType(), input_types=[DateType()]
    )
    to_date_udf = udf(to_date, return_type=DateType(), input_types=[TimestampType()])
    Utils.check_answer(
        df.select(to_timestamp_udf("date"), to_date_udf("timestamp")).collect(), out
    )


def test_time_and_timestamp_type(session):
    Utils.create_table(session, tmp_table_name, "time time, timestamp timestamp")
    session._run_query(
        f"insert into {tmp_table_name} select to_time(a), to_timestamp(b) "
        f"from values('01:02:03','1970-01-01 01:02:03'), "
        f"(null, null) as T(a, b)"
    )

    def to_timestamp(t):
        return datetime.datetime(1970, 1, 1, t.hour, t.minute, t.second) if t else None

    def to_time(t):
        return t.time() if t else None

    df = session.table(tmp_table_name)
    to_timestamp_udf = udf(
        to_timestamp, return_type=TimestampType(), input_types=[TimeType()]
    )
    to_time_udf = udf(to_time, return_type=TimeType(), input_types=[TimestampType()])
    res = Utils.get_sorted_rows(
        df.select(to_timestamp_udf("time"), to_time_udf("timestamp")).collect()
    )
    assert res[0] == Row(None, None)
    assert str(res[1][0]) == "1970-01-01 01:02:03"
    assert str(res[1][1]) == "01:02:03"


def test_time_date_timestamp_type_with_snowflake_timezone(session):
    df = session.sql("select '00:00:00' :: time as col1")

    add_udf = udf(
        lambda x: datetime.time(x.hour, x.minute, x.second + 5),
        return_type=TimeType(),
        input_types=[TimeType()],
    )
    res = df.select(add_udf("col1")).collect()
    assert len(res) == 1
    assert str(res[0][0]) == "00:00:05"

    df = session.sql("select '2020-1-1' :: date as col1")
    add_udf = udf(
        lambda x: datetime.date(x.year, x.month, x.day + 1),
        return_type=DateType(),
        input_types=[DateType()],
    )
    res = df.select(add_udf("col1")).collect()
    assert len(res) == 1
    assert str(res[0][0]) == "2020-01-02"

    df = session.sql("select '2020-1-1 00:00:00' :: date as col1")
    add_udf = udf(
        lambda x: datetime.datetime(
            x.year, x.month, x.day + 1, x.hour, x.minute, x.second + 5
        ),
        return_type=TimestampType(),
        input_types=[TimestampType()],
    )
    res = df.select(add_udf("col1")).collect()
    assert len(res) == 1
    assert str(res[0][0]) == "2020-01-02 00:00:05"


def test_geography_type(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, "g geography", is_temporary=True)
    session._run_query(
        f"insert into {table_name} values ('POINT(30 10)'), ('POINT(50 60)'), (null)"
    )
    df = session.table(table_name)

    def geography(g):
        if not g:
            return None
        else:
            g_str = str(g)
            if "[50, 60]" in g_str and "Point" in g_str:
                return g_str
            else:
                return g_str.replace("0", "")

    geography_udf = udf(
        geography, return_type=StringType(), input_types=[GeographyType()]
    )

    Utils.check_answer(
        df.select(geography_udf(col("g"))),
        [
            Row("{'coordinates': [3, 1], 'type': 'Point'}"),
            Row("{'coordinates': [50, 60], 'type': 'Point'}"),
            Row(None),
        ],
    )


def test_variant_string_input(session):
    @udf(return_type=StringType(), input_types=[VariantType()])
    def variant_string_input_udf(v):
        return v.lower()

    Utils.check_answer(
        TestData.variant1(session).select(variant_string_input_udf("str1")).collect(),
        [Row("x")],
    )


@pytest.mark.skip(
    "SNOW-447601: enable this test after the server has "
    "a full type mapping for variant data"
)
def test_variant_binary_input(session):
    @udf(return_type=BinaryType(), input_types=[VariantType()])
    def variant_binary_input_udf(v):
        return v

    Utils.check_answer(
        TestData.variant1(session).select(variant_binary_input_udf("bin1")).collect(),
        [Row(bytes("snow", "utf8"))],
    )


def test_variant_boolean_input(session):
    @udf(return_type=BooleanType(), input_types=[VariantType()])
    def variant_boolean_input_udf(v):
        return v

    Utils.check_answer(
        TestData.variant1(session).select(variant_boolean_input_udf("bool1")).collect(),
        [Row(True)],
    )


def test_variant_number_input(session):
    @udf(return_type=IntegerType(), input_types=[VariantType()])
    def variant_number_input_udf(v):
        return v + 20

    Utils.check_answer(
        TestData.variant1(session).select(variant_number_input_udf("num1")).collect(),
        [Row(35)],
    )


@pytest.mark.skip(
    "SNOW-447601: enable this test after the server has "
    "a full type mapping for variant data"
)
def test_variant_timestamp_input(session):
    @udf(return_type=TimestampType(), input_types=[VariantType()])
    def variant_timestamp_udf(v):
        if not v:
            return None
        return v + datetime.timedelta(seconds=5)

    Utils.check_answer(
        TestData.variant1(session)
        .select(variant_timestamp_udf("timestamp_ntz1"))
        .collect(),
        [
            Row(
                datetime.datetime.strptime(
                    "2017-02-24 12:00:05.456", "%Y-%m-%d %H:%M:%S.%f"
                )
            )
        ],
    )


@pytest.mark.skip(
    "SNOW-447601: enable this test after the server has "
    "a full type mapping for variant data"
)
def test_variant_time_input(session):
    @udf(return_type=TimeType(), input_types=[VariantType()])
    def variant_time_udf(v):
        if not v:
            return None
        return datetime.time(v.hour, v.minute, v.second + 5)

    Utils.check_answer(
        TestData.variant1(session).select(variant_time_udf("time1")).collect(),
        [Row(datetime.datetime.strptime("20:57:06", "%H:%M:%S").time())],
    )


@pytest.mark.skip(
    "SNOW-447601: enable this test after the server has "
    "a full type mapping for variant data"
)
def test_variant_date_input(session):
    @udf(return_type=DateType(), input_types=[VariantType()])
    def variant_date_udf(v):
        if not v:
            return None
        return datetime.date(v.year, v.month, v.day + 1)

    Utils.check_answer(
        TestData.variant1(session).select(variant_date_udf("date1")).collect(),
        [Row(datetime.datetime.strptime("2017-02-25", "%Y-%m-%d").date())],
    )


def test_variant_null(session):
    @udf(return_type=StringType(), input_types=[VariantType()])
    def variant_null_output_udf(_):
        return None

    Utils.check_answer(
        session.sql("select 1 as a").select(variant_null_output_udf("a")).collect(),
        [Row(None)],
    )

    # when NullType is specified, StringType is used
    @udf(return_type=NullType(), input_types=[VariantType()])
    def variant_null_output_udf1(_):
        return None

    Utils.check_answer(
        session.sql("select 1 as a").select(variant_null_output_udf1("a")).collect(),
        [Row(None)],
    )

    @udf(return_type=VariantType(), input_types=[VariantType()])
    def variant_null_output_udf2(_):
        return None

    Utils.check_answer(
        session.sql("select 1 as a").select(variant_null_output_udf2("a")).collect(),
        [Row("null")],
    )

    @udf(return_type=StringType(), input_types=[VariantType()])
    def variant_null_input_udf(v):
        # we need to parse sqlNullWrapper on the server side
        return None if hasattr(v, "is_sql_null") else v["a"]

    Utils.check_answer(
        TestData.null_json1(session).select(variant_null_input_udf("v")).collect(),
        [Row(None), Row("foo"), Row(None)],
    )


def test_variant_string_output(session):
    @udf(return_type=VariantType(), input_types=[VariantType()])
    def variant_string_output_udf(_):
        return "foo"

    Utils.check_answer(
        TestData.variant1(session).select(variant_string_output_udf("num1")).collect(),
        [Row('"foo"')],
    )


# The behavior of Variant("null") in Python UDF is different from the one in Java UDF
# Given a string "null", Python UDF will just a string "null", instead of NULL value
def test_variant_null_string_output(session):
    @udf(return_type=VariantType(), input_types=[VariantType()])
    def variant_null_string_output_udf(_):
        return "null"

    Utils.check_answer(
        TestData.variant1(session)
        .select(variant_null_string_output_udf("num1"))
        .collect(),
        [Row('"null"')],
    )


def test_variant_number_output(session):
    @udf(return_type=VariantType(), input_types=[VariantType()])
    def variant_int_output_udf(_):
        return 1

    Utils.check_answer(
        TestData.variant1(session).select(variant_int_output_udf("num1")).collect(),
        [Row("1")],
    )

    @udf(return_type=VariantType(), input_types=[VariantType()])
    def variant_float_output_udf(_):
        return 1.1

    Utils.check_answer(
        TestData.variant1(session).select(variant_float_output_udf("num1")).collect(),
        [Row("1.1")],
    )

    # @udf(
    #     return_type=VariantType(),
    #     input_types=[VariantType()],
    # )
    # def variant_decimal_output_udf(_):
    #     import decimal
    #
    #     return decimal.Decimal(1.1)
    #
    # assert TestData.variant1(session).select(
    #     variant_decimal_output_udf("num1")
    # ).collect() == [Row("1.1")]


def test_variant_boolean_output(session):
    @udf(return_type=VariantType(), input_types=[VariantType()])
    def variant_boolean_output_udf(_):
        return True

    Utils.check_answer(
        TestData.variant1(session).select(variant_boolean_output_udf("num1")).collect(),
        [Row("true")],
    )


@pytest.mark.skip(
    "SNOW-447601: enable this test after the server has "
    "a full type mapping for variant data"
)
def test_variant_binary_output(session):
    @udf(return_type=VariantType(), input_types=[VariantType()])
    def variant_binary_output_udf(_):
        return bytes("snow", "utf8")

    Utils.check_answer(
        TestData.variant1(session).select(variant_binary_output_udf("num1")).collect(),
        [Row('"736E6F77"')],
    )


def test_variant_dict_output(session):
    @udf(return_type=VariantType(), input_types=[VariantType()])
    def variant_dict_output_udf(_):
        return {"a": "foo"}

    Utils.check_answer(
        TestData.variant1(session).select(variant_dict_output_udf("num1")).collect(),
        [Row('{\n  "a": "foo"\n}')],
    )


def test_variant_list_output(session):
    @udf(return_type=VariantType(), input_types=[VariantType()])
    def variant_list_output_udf(_):
        return [1, 2, 3]

    Utils.check_answer(
        TestData.variant1(session).select(variant_list_output_udf("num1")).collect(),
        [Row("[\n  1,\n  2,\n  3\n]")],
    )


@pytest.mark.skip(
    "SNOW-447601: enable this test after the server has "
    "a full type mapping for variant data"
)
def test_variant_timestamp_output(session):
    @udf(return_type=VariantType(), input_types=[VariantType()])
    def variant_timestamp_output_udf(_):
        return datetime.datetime.strptime("2020-10-10 01:02:03", "%Y-%m-%d %H:%M:%S")

    Utils.check_answer(
        TestData.variant1(session)
        .select(variant_timestamp_output_udf("num1"))
        .collect(),
        [Row('"2020-10-10 01:02:03.000"')],
    )


@pytest.mark.skip(
    "SNOW-447601: enable this test after the server has "
    "a full type mapping for variant data"
)
def test_variant_time_output(session):
    @udf(return_type=VariantType(), input_types=[VariantType()])
    def variant_time_output_udf(_):
        return datetime.datetime.strptime("01:02:03", "%H:%M:%S").time()

    Utils.check_answer(
        TestData.variant1(session).select(variant_time_output_udf("num1")).collect(),
        [Row('"01:02:03"')],
    )


@pytest.mark.skip(
    "SNOW-447601: enable this test after the server has "
    "a full type mapping for variant data"
)
def test_variant_date_output(session):
    @udf(return_type=VariantType(), input_types=[VariantType()])
    def variant_date_output_udf(_):
        return datetime.datetime.strptime("2020-10-10", "%Y-%m-%d").date()

    Utils.check_answer(
        TestData.variant1(session).select(variant_date_output_udf("num1")).collect(),
        [Row('"2020-10-10"')],
    )


def test_array_variant(session):
    @udf(return_type=ArrayType(VariantType()), input_types=[ArrayType(VariantType())])
    def variant_udf(v):
        return v + [1]

    Utils.check_answer(
        TestData.variant1(session).select(variant_udf("arr1")).collect(),
        [Row('[\n  "Example",\n  1\n]')],
    )

    @udf(return_type=ArrayType(VariantType()), input_types=[ArrayType(VariantType())])
    def variant_udf_none(v):
        return v + [None]

    Utils.check_answer(
        TestData.variant1(session).select(variant_udf_none("arr1")).collect(),
        [Row('[\n  "Example",\n  null\n]')],
    )

    @udf(return_type=ArrayType(VariantType()), input_types=[ArrayType(VariantType())])
    def variant_udf_none_if_true(_):
        return None if True else [1]

    Utils.check_answer(
        TestData.variant1(session).select(variant_udf_none_if_true("arr1")).collect(),
        [Row(None)],
    )


def test_map_variant(session):
    @udf(
        return_type=MapType(StringType(), VariantType()),
        input_types=[MapType(StringType(), VariantType())],
    )
    def variant_udf(v):
        return {**v, "a": 1}

    Utils.check_answer(
        TestData.variant1(session).select(variant_udf("obj1")).collect(),
        [Row('{\n  "Tree": "Pine",\n  "a": 1\n}')],
    )

    @udf(
        return_type=MapType(StringType(), VariantType()),
        input_types=[MapType(StringType(), VariantType())],
    )
    def variant_udf_none(v):
        return {**v, "a": None}

    Utils.check_answer(
        TestData.variant1(session).select(variant_udf_none("obj1")).collect(),
        [Row('{\n  "Tree": "Pine",\n  "a": null\n}')],
    )

    @udf(
        return_type=MapType(StringType(), VariantType()),
        input_types=[MapType(StringType(), VariantType())],
    )
    def variant_udf_none_if_true(_):
        return None if True else {"a": 1}

    Utils.check_answer(
        TestData.variant1(session).select(variant_udf_none_if_true("obj1")).collect(),
        [Row(None)],
    )


def test_negative_test_to_input_invalid_func_name(session):
    func_name = "negative test invalid name"
    with pytest.raises(SnowparkClientException) as ex_info:
        udf(
            lambda x: x + x,
            return_type=IntegerType(),
            input_types=[IntegerType()],
            name=func_name,
        )
    assert "The object name 'negative test invalid name' is invalid." in str(ex_info)


def test_empty_argument_function(session):
    udf1 = udf(lambda: 100, return_type=IntegerType())
    df = session.create_dataframe([1]).to_df("col")
    Utils.check_answer(df.select(udf1()).collect(), [Row(100)])


def test_repro_snow_415682(session, is_sample_data_available):
    if not is_sample_data_available:
        pytest.skip("SNOWFLAKE_SAMPLE_DATA is not available in this deployment")

    # Define a one-day slice of the Web_Sales table in TPCDS
    df = session.table(["SNOWFLAKE_SAMPLE_DATA", "TPCDS_SF10TCL", "WEB_SALES"])
    # Add below 2 extra filters to make the result row count is 10
    df = (
        df.filter(df["WS_SOLD_DATE_SK"] == 2451952)
        .filter(df["WS_SOLD_TIME_SK"] == 35874)
        .filter(df["WS_BILL_CUSTOMER_SK"] == 10530912)
    )

    # Get the list of column descriptions
    cols = df.schema

    # Derive the subset of columns containing decimal values, based on data type
    metric_cols = [col(c.name) for c in cols.fields if c.datatype == DecimalType(7, 2)]

    # Define a set of aggregates representing the min and max of each metric column
    metric_aggs = [e for c in metric_cols for e in (max(c), min(c))]

    # Get the results
    my_aggs = df.select(metric_aggs).collect()

    # construct a set of tuples containing column name, min and max value
    my_agg_tuples = [
        (e, my_aggs[0][2 * i], my_aggs[0][2 * i + 1]) for i, e in enumerate(metric_cols)
    ]

    # Define a <overly simple> function that normalizes
    # an incoming value based on min and max for that column.
    # Build a UDF for that function
    @udf
    def norm_udf(my_val: float, my_max: float, my_min: float) -> float:
        return (my_val - my_min) / (my_max - my_min)

    # Define the set of columns represening normalized metrics values,
    # by calling the UDF on each metrics column along with the precomputed min and max.
    # Note new column names are constructed for the results
    metrics_normalized = [
        norm_udf(c, lit(col_min), lit(col_max)).as_(f"norm_{c.getName()[1:-1]}")
        for (c, col_min, col_max) in my_agg_tuples
    ]

    # Now query the table retrieving normalized column values instead of absolute values
    # NORM_WS_EXT_DISCOUNT_AMT has a BigDecimal value with (precision = 1, scale = 2)
    my_norms = (
        df.select(metrics_normalized)
        .select("NORM_WS_EXT_DISCOUNT_AMT")
        .sort("NORM_WS_EXT_DISCOUNT_AMT")
    )

    Utils.check_answer(
        my_norms.collect(),
        [
            Row(0.0),
            Row(0.003556988004215603),
            Row(0.005327891585434567),
            Row(0.031172106954869112),
            Row(0.03785634836528609),
            Row(0.06832313005602315),
            Row(0.1572793596020284),
            Row(0.24924957011943236),
            Row(0.5399685289472378),
            Row(1.0),
        ],
    )
