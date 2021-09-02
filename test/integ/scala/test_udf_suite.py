#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import datetime
import random
import string
from test.utils import TestFiles, Utils

import pytest

from snowflake.snowpark.functions import col, udf
from snowflake.snowpark.row import Row
from snowflake.snowpark.types.sf_types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    TimestampType,
    TimeType,
    Variant,
    VariantType,
)

tmp_stage_name = Utils.random_stage_name()
tmp_table_name = Utils.random_name()
table1 = Utils.random_name()
table2 = Utils.random_name()
semi_structured_table = Utils.random_name()
view1 = f'"{Utils.random_name()}"'
view2 = f'"{Utils.random_name()}"'


@pytest.fixture(scope="module", autouse=True)
def setup(session_cnx, resources_path):
    test_files = TestFiles(resources_path)
    with session_cnx() as session:
        Utils.create_stage(session, tmp_stage_name, is_temporary=True)
        Utils.upload_to_stage(
            session, tmp_stage_name, test_files.test_file_parquet, compress=False
        )
        Utils.create_table(session, table1, "a int")
        session._run_query(f"insert into {table1} values(1),(2),(3)")
        Utils.create_table(session, table2, "a int, b int")
        session._run_query(f"insert into {table2} values(1, 2),(2, 3),(3, 4)")
        # TODO: remove this snippet (don't need to set this parameter for Python UDF)
        #  after prpr
        current_sf_version = float(
            session._run_query("select current_version()")[0][0][:4]
        )
        if current_sf_version >= 5.35:
            session._run_query(
                "alter session set PYTHON_UDF_X86_PRPR_TOP_LEVEL_PACKAGES_FROZEN_SOLVE_VERSIONS='{}'"
            )
        yield
        Utils.drop_table(session, tmp_table_name)
        Utils.drop_table(session, table1)
        Utils.drop_table(session, table2)
        Utils.drop_table(session, semi_structured_table)
        Utils.drop_table(session, view1)
        Utils.drop_table(session, view2)
        Utils.drop_stage(session, tmp_stage_name)


def test_basic_udf_function(session_cnx):
    with session_cnx() as session:
        df = session.table(table1)
        double_udf = udf(
            lambda x: x + x, return_type=IntegerType(), input_types=[IntegerType()]
        )
        assert df.select(double_udf("a")).collect() == [Row(2), Row(4), Row(6)]


def test_udf_with_arrays(session_cnx):
    with session_cnx() as session:
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
        assert df.select(list_udf("a1")).collect() == [Row("1,2,3"), Row("4,5,6")]


def test_udf_with_map_input(session_cnx):
    with session_cnx() as session:
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
            lambda x: sum([int(i) for i in x]),
            return_type=IntegerType(),
            input_types=[ArrayType(StringType())],
        )
        assert df.select(array_sum_udf(map_keys_udf("o1"))).collect() == [
            Row(3),
            Row(30),
        ]


def test_udf_with_map_return(session_cnx):
    with session_cnx() as session:
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
        res = df.select(map_udf("a1")).collect()
        assert len(res) == 2
        for i in [1, 2, 3]:
            assert f"convert_to_map{i}" in res[0].get_string(0)
        for i in [4, 5, 6]:
            assert f"convert_to_map{i}" in res[1].get_string(0)


def test_udf_with_multiple_args_of_map_array(session_cnx):
    with session_cnx() as session:
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
            res = sum([int(v) for v in values])
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
        res = df.select(map_udf("o1", "o2", "id")).collect()
        assert len(res) == 2
        assert '"ID1": "30"' in res[0].get_string(0)
        assert '"ID2": "70"' in res[1].get_string(0)


def test_filter_on_top_of_udf(session_cnx):
    with session_cnx() as session:
        df = session.table(table1)
        double_udf = udf(
            lambda x: x + x, return_type=IntegerType(), input_types=[IntegerType()]
        )
        assert df.select(double_udf("a")).filter(col("$1") > 4).collect() == [Row(6)]


def test_compose_on_dataframe_reader(session_cnx, resources_path):
    with session_cnx() as session:
        df = session.read.parquet(f"@{tmp_stage_name}/test.parquet").toDF("a")
        replace_udf = udf(
            lambda elem: elem.replace("num", "id"),
            return_type=StringType(),
            input_types=[StringType()],
        )
        assert df.select(replace_udf("a")).collect() == [
            Row('{"id":1,"str":"str1"}'),
            Row('{"id":2,"str":"str2"}'),
        ]


def test_large_closure(session_cnx):
    with session_cnx() as session:
        df = session.table(table1)
        factor = 64
        long_string = "".join(random.choices(string.ascii_letters, k=factor * 1024))
        string_udf = udf(
            lambda x: f"{long_string}{x}",
            return_type=StringType(),
            input_types=[IntegerType()],
        )
        rows = df.select(string_udf("a")).collect()
        assert rows[1].get_string(0).startswith(long_string)


def test_string_return_type(session_cnx):
    with session_cnx() as session:
        df = session.table(table1)
        prefix = "Hello"
        string_udf = udf(
            lambda x: f"{prefix}{x}",
            return_type=StringType(),
            input_types=[IntegerType()],
        )
        assert df.select("a", string_udf("a")).collect() == [
            Row([1, "Hello1"]),
            Row([2, "Hello2"]),
            Row([3, "Hello3"]),
        ]


def test_long_type(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([1, 2, 3]).toDF("a")
        long_udf = udf(
            lambda x: x + x, return_type=LongType(), input_types=[LongType()]
        )
        assert df.select("a", long_udf("a")).collect() == [
            Row([1, 2]),
            Row([2, 4]),
            Row([3, 6]),
        ]


def test_short_type(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([1, 2, 3]).toDF("a")
        short_udf = udf(
            lambda x: x + x, return_type=ShortType(), input_types=[ShortType()]
        )
        assert df.select("a", short_udf("a")).collect() == [
            Row([1, 2]),
            Row([2, 4]),
            Row([3, 6]),
        ]


def test_float_type(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([1.1, 2.2, 3.3]).toDF("a")
        float_udf = udf(
            lambda x: x + x, return_type=FloatType(), input_types=[FloatType()]
        )
        assert df.select("a", float_udf("a")).collect() == [
            Row([1.1, 2.2]),
            Row([2.2, 4.4]),
            Row([3.3, 6.6]),
        ]


def test_double_type(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([1.01, 2.01, 3.01]).toDF("a")
        double_udf = udf(
            lambda x: x + x, return_type=DoubleType(), input_types=[DoubleType()]
        )
        assert df.select("a", double_udf("a")).collect() == [
            Row([1.01, 2.02]),
            Row([2.01, 4.02]),
            Row([3.01, 6.02]),
        ]


def test_boolean_type(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame([[1, 1], [2, 2], [3, 4]]).toDF("a", "b")
        boolean_udf = udf(
            lambda x, y: x == y,
            return_type=BooleanType(),
            input_types=[IntegerType(), IntegerType()],
        )
        assert df.select(boolean_udf("a", "b")).collect() == [
            Row(True),
            Row(True),
            Row(False),
        ]


def test_binary_type(session_cnx):
    with session_cnx() as session:
        data = ["Hello", "World"]
        bytes_data = [bytes(s, "utf8") for s in data]
        df1 = session.createDataFrame(data).toDF("a")
        df2 = session.createDataFrame(bytes_data).toDF("a")
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
        assert df1.select(to_binary("a")).collect() == [Row(s) for s in bytes_data]
        assert df2.select(from_binary("a")).collect() == [Row(s) for s in data]


def test_date_and_timestamp_type(session_cnx):
    with session_cnx() as session:
        data = [
            [datetime.date(2019, 1, 1), datetime.datetime(2019, 1, 1)],
            [datetime.date(2020, 1, 1), datetime.datetime(2020, 1, 1)],
            [None, None],
        ]

        def to_timestamp(d):
            return datetime.datetime(d.year, d.month, d.day) if d else None

        def to_date(t):
            return t.date() if t else None

        out = [Row([to_timestamp(d), to_date(t)]) for d, t in data]
        df = session.createDataFrame(data).toDF("date", "timestamp")
        to_timestamp_udf = udf(
            to_timestamp, return_type=TimestampType(), input_types=[DateType()]
        )
        to_date_udf = udf(
            to_date, return_type=DateType(), input_types=[TimestampType()]
        )
        assert (
            df.select(to_timestamp_udf("date"), to_date_udf("timestamp")).collect()
            == out
        )


def test_time_and_timestamp_type(session_cnx):
    with session_cnx() as session:
        Utils.create_table(session, tmp_table_name, "time time, timestamp timestamp")
        session._run_query(
            f"insert into {tmp_table_name} select to_time(a), to_timestamp(b) "
            f"from values('01:02:03','1970-01-01 01:02:03'), "
            f"(null, null) as T(a, b)"
        )

        def to_timestamp(t):
            return (
                datetime.datetime(1970, 1, 1, t.hour, t.minute, t.second) if t else None
            )

        def to_time(t):
            return t.time() if t else None

        df = session.table(tmp_table_name)
        to_timestamp_udf = udf(
            to_timestamp, return_type=TimestampType(), input_types=[TimeType()]
        )
        to_time_udf = udf(
            to_time, return_type=TimeType(), input_types=[TimestampType()]
        )
        res = df.select(to_timestamp_udf("time"), to_time_udf("timestamp")).collect()
        assert res[0].get_string(0) == "1970-01-01 01:02:03"
        assert res[0].get_string(1) == "01:02:03"
        assert res[1] == Row([None, None])


def test_time_date_timestamp_type_with_snowflake_timezone(session_cnx):
    with session_cnx() as session:
        df = session.sql("select '00:00:00' :: time as col1")

        add_udf = udf(
            lambda x: datetime.time(x.hour, x.minute, x.second + 5),
            return_type=TimeType(),
            input_types=[TimeType()],
        )
        assert df.select(add_udf("col1")).collect()[0].get_string(0) == "00:00:05"

        df = session.sql("select '2020-1-1' :: date as col1")
        add_udf = udf(
            lambda x: datetime.date(x.year, x.month, x.day + 1),
            return_type=DateType(),
            input_types=[DateType()],
        )
        assert df.select(add_udf("col1")).collect()[0].get_string(0) == "2020-01-02"

        df = session.sql("select '2020-1-1 00:00:00' :: date as col1")
        add_udf = udf(
            lambda x: datetime.datetime(
                x.year, x.month, x.day + 1, x.hour, x.minute, x.second + 5
            ),
            return_type=TimestampType(),
            input_types=[TimestampType()],
        )
        assert (
            df.select(add_udf("col1")).collect()[0].get_string(0)
            == "2020-01-02 00:00:05"
        )
