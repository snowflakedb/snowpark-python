#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import os
import re
import tempfile
from datetime import datetime
from decimal import Decimal
from test.utils import IS_WINDOWS, TestData, TestFiles, Utils

import pytest

from snowflake import connector
from snowflake.snowpark.functions import col, lit, max, mean, min, sum
from snowflake.snowpark.row import Row
from snowflake.snowpark.session import Session
from snowflake.snowpark.snowpark_client_exception import SnowparkClientException
from snowflake.snowpark.types.sf_types import (
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
    TimestampType,
    TimeType,
    VariantType,
)

SAMPLING_DEVIATION = 0.4


def test_null_data_in_tables(session):
    table_name = Utils.random_name()
    try:
        Utils.create_table(session, table_name, "num int")
        session.sql(f"insert into {table_name} values(null),(null),(null)").collect()
        res = session.table(table_name).collect()
        assert res == [Row(None), Row(None), Row(None)]
    finally:
        Utils.drop_table(session, table_name)


def test_null_data_in_local_relation_with_filters(session):
    df = session.createDataFrame([[1, None], [2, "NotNull"], [3, None]]).toDF(
        ["a", "b"]
    )
    assert df.collect() == [Row(1, None), Row(2, "NotNull"), Row(3, None)]
    df2 = session.createDataFrame([[1, None], [2, "NotNull"], [3, None]]).toDF(
        ["a", "b"]
    )
    assert df.collect() == df2.collect()

    assert df.filter(col("b").is_null()).collect() == [
        Row(1, None),
        Row(3, None),
    ]
    assert df.filter(col("b").is_not_null()).collect() == [Row(2, "NotNull")]
    assert df.sort(col("b").asc_nulls_last()).collect() == [
        Row(2, "NotNull"),
        Row(1, None),
        Row(3, None),
    ]


def test_project_null_values(session):
    """Tests projecting null values onto different columns in a dataframe"""
    df = session.createDataFrame([1, 2]).toDF("a").withColumn("b", lit(None))
    assert df.collect() == [Row(1, None), Row(2, None)]

    df2 = session.createDataFrame([1, 2]).toDF("a").select(lit(None))
    assert len(df2.schema.fields) == 1
    assert df2.schema.fields[0].datatype == StringType()
    assert df2.collect() == [Row(None), Row(None)]


def test_write_null_data_to_table(session):
    table_name = Utils.random_name()
    df = session.createDataFrame([(1, None), (2, None), (3, None)]).toDF("a", "b")
    try:
        df.write.saveAsTable(table_name)
        Utils.check_answer(session.table(table_name), df, True)
    finally:
        Utils.drop_table(session, table_name)


def test_createOrReplaceView_with_null_data(session):
    df = session.createDataFrame([[1, None], [2, "NotNull"], [3, None]]).toDF(
        ["a", "b"]
    )
    view_name = Utils.random_name()
    try:
        df.createOrReplaceView(view_name)

        res = session.sql(f"select * from {view_name}").collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1, None), Row(2, "NotNull"), Row(3, None)]
    finally:
        Utils.drop_view(session, view_name)


def test_adjust_column_width_of_show(session):
    df = session.createDataFrame([[1, None], [2, "NotNull"]]).toDF("a", "b")
    # run show(), make sure no error is reported
    df.show(10, 4)

    res = df._DataFrame__show_string(10, 4)
    assert (
        res
        == """
--------------
|"A"  |"B"   |
--------------
|1    |NULL  |
|2    |N...  |
--------------\n""".lstrip()
    )


def test_show_with_null_data(session):
    df = session.createDataFrame([[1, None], [2, "NotNull"]]).toDF("a", "b")
    # run show(), make sure no error is reported
    df.show(10)

    res = df._DataFrame__show_string(10)
    assert (
        res
        == """
-----------------
|"A"  |"B"      |
-----------------
|1    |NULL     |
|2    |NotNull  |
-----------------\n""".lstrip()
    )


def test_show_multi_lines_row(session):
    df = session.createDataFrame(
        [
            ("line1\nline2", None),
            ("single line", "NotNull\none more line\nlast line"),
        ]
    ).toDF("a", "b")

    res = df._DataFrame__show_string(2)
    assert (
        res
        == """
-------------------------------
|"A"          |"B"            |
-------------------------------
|line1        |NULL           |
|line2        |               |
|single line  |NotNull        |
|             |one more line  |
|             |last line      |
-------------------------------\n""".lstrip()
    )


def test_show(session):
    TestData.test_data1(session).show()

    res = TestData.test_data1(session)._DataFrame__show_string(10)
    assert (
        res
        == """
--------------------------
|"NUM"  |"BOOL"  |"STR"  |
--------------------------
|1      |True    |a      |
|2      |False   |b      |
--------------------------\n""".lstrip()
    )

    # make sure show runs with sql
    session.sql("show tables").show()

    session.sql("drop table if exists test_table_123").show()

    # truncate result, no more than 50 characters
    res = session.sql("drop table if exists test_table_123")._DataFrame__show_string(1)

    assert (
        res
        == """
------------------------------------------------------
|"status"                                            |
------------------------------------------------------
|Drop statement executed successfully (TEST_TABL...  |
------------------------------------------------------\n""".lstrip()
    )


def test_non_select_query_composition(session):
    table_name = Utils.random_name()
    try:
        session.sql(
            f"create or replace temporary table {table_name} (num int)"
        ).collect()
        df = (
            session.sql("show tables")
            .select('"name"')
            .filter(col('"name"') == table_name)
        )
        assert len(df.collect()) == 1
        schema = df.schema
        assert len(schema.fields) == 1
        assert type(schema.fields[0].datatype) is StringType
        assert schema.fields[0].name == '"name"'
    finally:
        Utils.drop_table(session, table_name)


def test_non_select_query_composition_union(session):
    table_name = Utils.random_name()
    try:
        session.sql(
            f"create or replace temporary table {table_name} (num int)"
        ).collect()
        df1 = session.sql("show tables")
        df2 = session.sql("show tables")

        df = df1.union(df2).select('"name"').filter(col('"name"') == table_name)
        res = df.collect()
        assert len(res) == 2

    finally:
        Utils.drop_table(session, table_name)


def test_non_select_query_composition_self_union(session):
    table_name = Utils.random_name()
    try:
        session.sql(
            f"create or replace temporary table {table_name} (num int)"
        ).collect()
        df = session.sql("show tables")

        union = df.union(df).select('"name"').filter(col('"name"') == table_name)

        assert len(union.collect()) == 2
        assert len(union._DataFrame__plan.queries) == 3
    finally:
        Utils.drop_table(session, table_name)


def test_only_use_result_scan_when_composing_queries(session):
    df = session.sql("show tables")
    assert len(df._DataFrame__plan.queries) == 1
    assert df._DataFrame__plan.queries[0].sql == "show tables"

    df2 = df.select('"name"')
    assert len(df2._DataFrame__plan.queries) == 2
    assert "RESULT_SCAN" in df2._DataFrame__plan.queries[-1].sql


def test_joins_on_result_scan(session):
    df1 = session.sql("show tables").select(['"name"', '"kind"'])
    df2 = session.sql("show tables").select(['"name"', '"rows"'])

    result = df1.join(df2, '"name"')
    result.collect()  # no error
    assert len(result.schema.fields) == 3


def test_select_star(session):
    double2 = TestData.double2(session)
    expected = TestData.double2(session).collect()
    assert double2.select("*").collect() == expected
    assert double2.select(double2.col("*")).collect() == expected


def test_first(session):
    assert TestData.integer1(session).first() == Row(1)
    assert TestData.null_data1(session).first() == Row(None)
    assert TestData.integer1(session).filter(col("a") < 0).first() is None

    res = TestData.integer1(session).first(2)
    assert sorted(res, key=lambda x: x[0]) == [Row(1), Row(2)]

    # return all elements
    res = TestData.integer1(session).first(3)
    assert sorted(res, key=lambda x: x[0]) == [Row(1), Row(2), Row(3)]

    res = TestData.integer1(session).first(10)
    assert sorted(res, key=lambda x: x[0]) == [Row(1), Row(2), Row(3)]

    res = TestData.integer1(session).first(-10)
    assert sorted(res, key=lambda x: x[0]) == [Row(1), Row(2), Row(3)]


def test_sample_with_row_count(session):
    """Tests sample using n (row count)"""
    row_count = 10000
    df = session.range(row_count)
    assert df.sample(n=0).count() == 0
    assert len(df.sample(n=0).collect()) == 0
    row_count_10_percent = int(row_count / 10)
    assert df.sample(n=row_count_10_percent).count() == row_count_10_percent
    assert df.sample(n=row_count).count() == row_count
    assert df.sample(n=row_count + 10).count() == row_count
    assert len(df.sample(n=row_count_10_percent).collect()) == row_count_10_percent
    assert len(df.sample(n=row_count).collect()) == row_count
    assert len(df.sample(n=row_count + 10).collect()) == row_count


def test_sample_with_frac(session):
    """Tests sample using frac"""
    row_count = 10000
    df = session.range(row_count)
    assert df.sample(frac=0.0).count() == 0
    half_row_count = row_count * 0.5
    assert (
        abs(df.sample(frac=0.5).count() - half_row_count)
        < half_row_count * SAMPLING_DEVIATION
    )
    assert df.sample(frac=1.0).count() == row_count
    assert len(df.sample(frac=0.0).collect()) == 0
    half_row_count = row_count * 0.5
    assert (
        abs(len(df.sample(frac=0.5).collect()) - half_row_count)
        < half_row_count * SAMPLING_DEVIATION
    )
    assert len(df.sample(frac=1.0).collect()) == row_count


def test_sample_negative(session):
    """Tests negative test cases for sample"""
    row_count = 10000
    df = session.range(row_count)
    with pytest.raises(ValueError):
        df.sample(n=-1)
    with pytest.raises(connector.errors.ProgrammingError):
        df.sample(n=1000001).count()
    with pytest.raises(ValueError):
        df.sample(frac=-0.01)
    with pytest.raises(ValueError):
        df.sample(frac=1.01)


def test_sample_on_join(session):
    """Tests running sample on a join statement"""
    row_count = 10000
    df1 = session.range(row_count).withColumn('"name"', lit("value1"))
    df2 = session.range(row_count).withColumn('"name"', lit("value2"))

    result = df1.join(df2, '"ID"')
    sample_row_count = int(result.count() / 10)

    assert result.sample(n=sample_row_count).count() == sample_row_count
    assert (
        abs(result.sample(frac=0.1).count() - sample_row_count)
        < sample_row_count * SAMPLING_DEVIATION
    )


def test_sample_on_union(session):
    """Tests running sample on union statements"""
    row_count = 10000
    df1 = session.range(row_count).withColumn('"name"', lit("value1"))
    df2 = session.range(5000, 5000 + row_count).withColumn('"name"', lit("value2"))

    # Test union
    result = df1.union(df2)
    sample_row_count = int(result.count() / 10)
    assert result.sample(n=sample_row_count).count() == sample_row_count
    assert (
        abs(result.sample(frac=0.1).count() - sample_row_count)
        < sample_row_count * SAMPLING_DEVIATION
    )
    # Test unionAll
    result = df1.unionAll(df2)
    sample_row_count = int(result.count() / 10)
    assert result.sample(n=sample_row_count).count() == sample_row_count
    assert (
        abs(result.sample(frac=0.1).count() - sample_row_count)
        < sample_row_count * SAMPLING_DEVIATION
    )


def test_toDf(session):
    # toDF(*str) with 1 column
    df1 = session.createDataFrame([1, 2, 3]).toDF("a")
    assert (
        df1.count() == 3
        and len(df1.schema.fields) == 1
        and df1.schema.fields[0].name == "A"
    )
    df1.show()
    # toDF([str]) with 1 column
    df2 = session.createDataFrame([1, 2, 3]).toDF(["a"])
    assert (
        df2.count() == 3
        and len(df2.schema.fields) == 1
        and df2.schema.fields[0].name == "A"
    )
    df2.show()

    # toDF(*str) with 2 columns
    df3 = session.createDataFrame([(1, None), (2, "NotNull"), (3, None)]).toDF("a", "b")
    assert df3.count() == 3 and len(df3.schema.fields) == 2
    assert df3.schema.fields[0].name == "A" and df3.schema.fields[-1].name == "B"
    # toDF([str]) with 2 columns
    df4 = session.createDataFrame([(1, None), (2, "NotNull"), (3, None)]).toDF(
        ["a", "b"]
    )
    assert df4.count() == 3 and len(df4.schema.fields) == 2
    assert df4.schema.fields[0].name == "A" and df4.schema.fields[-1].name == "B"

    # toDF(*str) with 3 columns
    df5 = session.createDataFrame(
        [(1, None, "a"), (2, "NotNull", "a"), (3, None, "a")]
    ).toDF("a", "b", "c")
    assert df5.count() == 3 and len(df5.schema.fields) == 3
    assert df5.schema.fields[0].name == "A" and df5.schema.fields[-1].name == "C"
    # toDF([str]) with 3 columns
    df6 = session.createDataFrame(
        [(1, None, "a"), (2, "NotNull", "a"), (3, None, "a")]
    ).toDF(["a", "b", "c"])
    assert df6.count() == 3 and len(df6.schema.fields) == 3
    assert df6.schema.fields[0].name == "A" and df6.schema.fields[-1].name == "C"


def test_toDF_negative_test(session):
    values = session.createDataFrame([[1, None], [2, "NotNull"], [3, None]])

    # toDF(*str) with invalid args count
    with pytest.raises(ValueError) as ex_info:
        values.toDF()
    assert "The number of columns doesn't match" in ex_info.value.args[0]
    with pytest.raises(ValueError):
        values.toDF("a")
    assert "The number of columns doesn't match" in ex_info.value.args[0]
    with pytest.raises(ValueError):
        values.toDF("a", "b", "c")
    assert "The number of columns doesn't match" in ex_info.value.args[0]

    # toDF([str]) with invalid args count
    with pytest.raises(ValueError):
        values.toDF([])
    assert "The number of columns doesn't match" in ex_info.value.args[0]
    with pytest.raises(ValueError):
        values.toDF(["a"])
    assert "The number of columns doesn't match" in ex_info.value.args[0]
    with pytest.raises(ValueError):
        values.toDF(["a", "b", "c"])
    assert "The number of columns doesn't match" in ex_info.value.args[0]


def test_sort(session):
    df = session.createDataFrame(
        [(1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (2, 3), (3, 1), (3, 2), (3, 3)]
    ).toDF("a", "b")

    # order ASC with 1 column
    sorted_rows = df.sort(col("a").asc()).collect()
    assert [
        sorted_rows[i][0] <= sorted_rows[i + 1][0] for i in range(len(sorted_rows) - 1)
    ]

    # order DESC with 1 column
    sorted_rows = df.sort(col("a").desc()).collect()
    assert [
        sorted_rows[i][0] >= sorted_rows[i + 1][0] for i in range(len(sorted_rows) - 1)
    ]

    # order ASC with 2 columns
    sorted_rows = df.sort(col("a").asc(), col("b").asc()).collect()
    assert [
        sorted_rows[i][0] <= sorted_rows[i + 1][0]
        or (
            sorted_rows[i][0] == sorted_rows[i + 1][0]
            and sorted_rows[i][1] <= sorted_rows[i + 1][1]
        )
        for i in range(len(sorted_rows) - 1)
    ]

    # order DESC with 2 columns
    sorted_rows = df.sort(col("a").desc(), col("b").desc()).collect()
    assert [
        sorted_rows[i][0] > sorted_rows[i + 1][0]
        or (
            sorted_rows[i][0] == sorted_rows[i + 1][0]
            and sorted_rows[i][1] >= sorted_rows[i + 1][1]
        )
        for i in range(len(sorted_rows) - 1)
    ]

    # Negative test: sort() needs at least one sort expression
    with pytest.raises(ValueError) as ex_info:
        df.sort([])
    assert "sort() needs at least one sort expression" in ex_info.value.args[0]


def test_select(session):
    df = session.createDataFrame([(1, "a", 10), (2, "b", 20), (3, "c", 30)]).toDF(
        ["a", "b", "c"]
    )

    # select(String, String*) with 1 column
    expected_result = [Row(1), Row(2), Row(3)]
    assert df.select("a").collect() == expected_result
    # select(Seq[String]) with 1 column
    assert df.select(["a"]).collect() == expected_result
    # select(Column, Column*) with 1 column
    assert df.select(col("a")).collect() == expected_result
    # select(Seq[Column]) with 1 column
    assert df.select([col("a")]).collect() == expected_result

    expected_result = [Row(1, "a", 10), Row(2, "b", 20), Row(3, "c", 30)]
    # select(String, String*) with 3 columns
    assert df.select(["a", "b", "c"]).collect() == expected_result
    # select(Seq[String]) with 3 column
    assert df.select(["a", "b", "c"]).collect() == expected_result
    # select(Column, Column*) with 3 column
    assert df.select(col("a"), col("b"), col("c")).collect() == expected_result
    # select(Seq[Column]) with 3 column
    assert df.select([col("a"), col("b"), col("c")]).collect() == expected_result

    # test col("a") + col("c")
    expected_result = [Row("a", 11), Row("b", 22), Row("c", 33)]
    # select(Column, Column*) with col("a") + col("b")
    assert df.select(col("b"), col("a") + col("c")).collect() == expected_result
    # select(Seq[Column]) with col("a") + col("b")
    assert df.select([col("b"), col("a") + col("c")]).collect() == expected_result


def test_select_negative_select(session):
    df = session.createDataFrame([(1, "a", 10), (2, "b", 20), (3, "c", 30)]).toDF(
        ["a", "b", "c"]
    )

    # Select with empty sequences
    with pytest.raises(TypeError) as ex_info:
        df.select()
    assert "select() input cannot be empty" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select([])
    assert "select() input cannot be empty" in str(ex_info)

    # select columns which don't exist
    with pytest.raises(connector.errors.ProgrammingError) as ex_info:
        df.select("not_exists_column").collect()
    assert "SQL compilation error" in str(ex_info)

    with pytest.raises(connector.errors.ProgrammingError) as ex_info:
        df.select(["not_exists_column"]).collect()
    assert "SQL compilation error" in str(ex_info)

    with pytest.raises(connector.errors.ProgrammingError) as ex_info:
        df.select(col("not_exists_column")).collect()
    assert "SQL compilation error" in str(ex_info)

    with pytest.raises(connector.errors.ProgrammingError) as ex_info:
        df.select([col("not_exists_column")]).collect()
    assert "SQL compilation error" in str(ex_info)


def test_drop_and_dropcolumns(session):
    df = session.createDataFrame([(1, "a", 10), (2, "b", 20), (3, "c", 30)]).toDF(
        ["a", "b", "c"]
    )

    expected_result = [Row(1, "a", 10), Row(2, "b", 20), Row(3, "c", 30)]

    # drop non-exist-column (do nothing)
    assert df.drop("not_exist_column").collect() == expected_result
    assert df.drop(["not_exist_column"]).collect() == expected_result
    assert df.drop(col("not_exist_column")).collect() == expected_result
    assert df.drop([col("not_exist_column")]).collect() == expected_result

    # drop 1st column
    expected_result = [Row("a", 10), Row("b", 20), Row("c", 30)]
    assert df.drop("a").collect() == expected_result
    assert df.drop(["a"]).collect() == expected_result
    assert df.drop(col("a")).collect() == expected_result
    assert df.drop([col("a")]).collect() == expected_result

    # drop 2nd column
    expected_result = [Row(1, 10), Row(2, 20), Row(3, 30)]
    assert df.drop("b").collect() == expected_result
    assert df.drop(["b"]).collect() == expected_result
    assert df.drop(col("b")).collect() == expected_result
    assert df.drop([col("b")]).collect() == expected_result

    # drop 2nd and 3rd column
    expected_result = [Row(1), Row(2), Row(3)]
    assert df.drop("b", "c").collect() == expected_result
    assert df.drop(["b", "c"]).collect() == expected_result
    assert df.drop(col("b"), col("c")).collect() == expected_result
    assert df.drop([col("b"), col("c")]).collect() == expected_result

    # drop all columns (negative test)
    with pytest.raises(SnowparkClientException) as ex_info:
        df.drop("a", "b", "c")
    assert "Cannot drop all column" in str(ex_info)

    with pytest.raises(SnowparkClientException) as ex_info:
        df.drop(["a", "b", "c"])
    assert "Cannot drop all column" in str(ex_info)

    with pytest.raises(SnowparkClientException) as ex_info:
        df.drop(col("a"), col("b"), col("c"))
    assert "Cannot drop all column" in str(ex_info)

    with pytest.raises(SnowparkClientException) as ex_info:
        df.drop([col("a"), col("b"), col("c")])
    assert "Cannot drop all column" in str(ex_info)


def test_dataframe_agg(session):
    df = session.createDataFrame([(1, "One"), (2, "Two"), (3, "Three")]).toDF(
        "empid", "name"
    )

    # Agg() on 1 column
    assert df.agg(max(col("empid"))).collect() == [Row(3)]
    assert df.agg([min(col("empid"))]).collect() == [Row(1)]
    assert df.agg({"empid": "max"}).collect() == [Row(3)]
    assert df.agg(("empid", "max")).collect() == [Row(3)]
    assert df.agg([("empid", "max")]).collect() == [Row(3)]
    assert df.agg({"empid": "avg"}).collect() == [Row(2.0)]
    assert df.agg(("empid", "avg")).collect() == [Row(2.0)]
    assert df.agg([("empid", "avg")]).collect() == [Row(2.0)]

    # Agg() on 2 columns
    assert df.agg([max(col("empid")), max(col("name"))]).collect() == [Row(3, "Two")]
    assert df.agg([min(col("empid")), min("name")]).collect() == [Row(1, "One")]
    assert df.agg({"empid": "max", "name": "max"}).collect() == [Row(3, "Two")]
    assert df.agg([("empid", "max"), ("name", "max")]).collect() == [Row(3, "Two")]
    assert df.agg([("empid", "max"), ("name", "max")]).collect() == [Row(3, "Two")]
    assert df.agg({"empid": "min", "name": "min"}).collect() == [Row(1, "One")]
    assert df.agg([("empid", "min"), ("name", "min")]).collect() == [Row(1, "One")]
    assert df.agg([("empid", "min"), ("name", "min")]).collect() == [Row(1, "One")]


def test_groupby(session):
    df = session.createDataFrame(
        [
            ("country A", "state A", 50),
            ("country A", "state A", 50),
            ("country A", "state B", 5),
            ("country A", "state B", 5),
            ("country B", "state A", 100),
            ("country B", "state A", 100),
            ("country B", "state B", 10),
            ("country B", "state B", 10),
        ]
    ).toDF(["country", "state", "value"])

    # groupBy without column
    assert df.groupBy().agg(max(col("value"))).collect() == [Row(100)]
    assert df.groupBy([]).agg(sum(col("value"))).collect() == [Row(330)]
    assert df.groupBy().agg([sum(col("value"))]).collect() == [Row(330)]

    # groupBy() on 1 column
    expected_res = [Row("country A", 110), Row("country B", 220)]
    assert df.groupBy("country").agg(sum(col("value"))).collect() == expected_res
    assert df.groupBy(["country"]).agg(sum(col("value"))).collect() == expected_res
    assert df.groupBy(col("country")).agg(sum(col("value"))).collect() == expected_res
    assert df.groupBy([col("country")]).agg(sum(col("value"))).collect() == expected_res

    # groupBy() on 2 columns
    expected_res = [
        Row("country A", "state B", 10),
        Row("country B", "state B", 20),
        Row("country A", "state A", 100),
        Row("country B", "state A", 200),
    ]

    res = df.groupBy(["country", "state"]).agg(sum(col("value"))).collect()
    assert sorted(res, key=lambda x: x[2]) == expected_res

    res = df.groupBy([col("country"), col("state")]).agg(sum(col("value"))).collect()
    assert sorted(res, key=lambda x: x[2]) == expected_res


def test_createDataFrame_with_given_schema(session):
    schema = StructType(
        [
            StructField("string", StringType()),
            StructField("byte", ByteType()),
            StructField("short", ShortType()),
            StructField("int", IntegerType()),
            StructField("long", LongType()),
            StructField("float", FloatType()),
            StructField("double", DoubleType()),
            StructField("number", DecimalType(10, 3)),
            StructField("boolean", BooleanType()),
            StructField("binary", BinaryType()),
            StructField("timestamp", TimestampType()),
            StructField("date", DateType()),
        ]
    )

    data = [
        Row(
            "a",
            1,
            2,
            3,
            4,
            1.1,
            1.2,
            Decimal("1.2"),
            True,
            bytearray([1, 2]),
            datetime.strptime("2017-02-24 12:00:05.456", "%Y-%m-%d %H:%M:%S.%f"),
            datetime.strptime("2017-02-25", "%Y-%m-%d").date(),
        ),
        Row(None, None, None, None, None, None, None, None, None, None, None, None),
    ]

    result = session.createDataFrame(data, schema)
    schema_str = str(result.schema)
    assert (
        schema_str == "StructType[StructField(STRING, String, Nullable=True), "
        "StructField(BYTE, Long, Nullable=True), "
        "StructField(SHORT, Long, Nullable=True), "
        "StructField(INT, Long, Nullable=True), "
        "StructField(LONG, Long, Nullable=True), "
        "StructField(FLOAT, Double, Nullable=True), "
        "StructField(DOUBLE, Double, Nullable=True), "
        "StructField(NUMBER, Decimal(10,3), Nullable=True), "
        "StructField(BOOLEAN, Boolean, Nullable=True), "
        "StructField(BINARY, Binary, Nullable=True), "
        "StructField(TIMESTAMP, Timestamp, Nullable=True), "
        "StructField(DATE, Date, Nullable=True)]"
    )
    Utils.check_answer(result, data, sort=False)


def test_createDataFrame_with_given_schema_time(session):
    schema = StructType(
        [
            StructField("time", TimeType()),
        ]
    )

    data = [Row(datetime.strptime("20:57:06", "%H:%M:%S").time()), Row(None)]
    df = session.createDataFrame(data, schema)
    schema_str = str(df.schema)
    assert schema_str == "StructType[StructField(TIME, Time, Nullable=True)]"
    assert df.collect() == data


def test_show_collect_with_misc_commands(session, resources_path, tmpdir):
    object_name = Utils.random_name()
    stage_name = Utils.random_stage_name()
    # In scala, they create a temp JAR file, here we just upload an existing CSV file
    filepath = TestFiles(resources_path).test_file_csv
    escaped_filepath = Utils.escape_path(filepath)

    canonical_dir_path = tmpdir.strpath + os.path.sep
    escaped_temp_dir = Utils.escape_path(canonical_dir_path)

    misc_commands = [
        f"create or replace temp stage {stage_name}",
        f"put file://{escaped_filepath} @{stage_name}",
        f"get @{stage_name} file://{escaped_temp_dir}",
        f"list @{stage_name}",
        f"remove @{stage_name}",
        f"remove @{stage_name}",  # second REMOVE returns 0 rows.
        f"create temp table {object_name} (c1 int)",
        f"drop table {object_name}",
        f"create temp view {object_name} (string) as select current_version()",
        f"drop view {object_name}",
        f"show tables",
        f"drop stage {stage_name}",
    ]

    # Misc commands with show
    for command in misc_commands:
        session.sql(command).show()

    # Misc commands with collect()
    for command in misc_commands:
        session.sql(command).collect()

    # Misc commands with session.conn.getResultAndMetadata
    for command in misc_commands:
        rows, meta = session.conn.get_result_and_metadata(
            session.sql(command)._DataFrame__plan
        )
        assert len(rows) == 0 or len(rows[0]) == len(meta)


def test_createDataFrame_with_given_schema_array_map_variant(session):
    schema = StructType(
        [
            StructField("array", ArrayType(None)),
            StructField("map", MapType(None, None)),
            StructField("variant", VariantType()),
            # StructField("geography", GeographyType()),
        ]
    )
    data = [Row(["'", 2], {"'": 1}, 1), Row(None, None, None)]
    df = session.createDataFrame(data, schema)
    assert (
        str(df.schema)
        == "StructType[StructField(ARRAY, ArrayType[String], Nullable=True), "
        "StructField(MAP, MapType[String,String], Nullable=True), "
        "StructField(VARIANT, Variant, Nullable=True)]"
    )
    df.show()
    expected = [
        Row('[\n  "\'",\n  2\n]', '{\n  "\'": 1\n}', "1"),
        Row(None, None, None),
    ]
    Utils.check_answer(df, expected, sort=False)


def test_variant_in_array_and_map(session):
    schema = StructType(
        [StructField("array", ArrayType(None)), StructField("map", MapType(None, None))]
    )
    data = [Row([1, "\"'"], {"a": "\"'"})]
    df = session.createDataFrame(data, schema)
    Utils.check_answer(df, [Row('[\n  1,\n  "\\"\'"\n]', '{\n  "a": "\\"\'"\n}')])


def test_escaped_character(session):
    df = session.createDataFrame(["'", "\\", "\n"]).toDF("a")
    res = df.collect()
    assert res == [Row("'"), Row("\\"), Row("\n")]


def test_create_or_replace_temporary_view(session, db_parameters):
    view_name = Utils.random_name()
    view_name1 = f'"{view_name}%^11"'
    view_name2 = f'"{view_name}"'

    try:
        df = session.createDataFrame([1, 2, 3]).toDF("a")
        df.createOrReplaceTempView(view_name)
        res = session.table(view_name).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]

        # test replace
        df2 = session.createDataFrame(["a", "b", "c"]).toDF("b")
        df2.createOrReplaceTempView(view_name)
        res = session.table(view_name).collect()
        assert res == [Row("a"), Row("b"), Row("c")]

        # view name has special char
        df.createOrReplaceTempView(view_name1)
        res = session.table(view_name1).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]

        # view name has quote
        df.createOrReplaceTempView(view_name2)
        res = session.table(view_name2).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]

        # Get a second session object
        session2 = Session.builder.configs(db_parameters).create()
        assert session is not session2
        with pytest.raises(connector.errors.ProgrammingError) as ex_info:
            session2.table(view_name).collect()
        assert "does not exist or not authorized" in str(ex_info)
    finally:
        Session._set_active_session(session)
        Utils.drop_view(session, view_name)
        Utils.drop_view(session, view_name1)
        Utils.drop_view(session, view_name2)
        session2.close()


def test_createDataFrame_with_schema_inference(session):
    df1 = session.createDataFrame([1, 2, 3]).toDF("int")
    Utils.check_answer(df1, [Row(1), Row(2), Row(3)])
    schema1 = df1.schema
    assert len(schema1.fields) == 1
    assert schema1.fields[0].name == "INT"
    assert schema1.fields[0].datatype == LongType()

    # tuple
    df2 = session.createDataFrame([(True, "a"), (False, "b")]).toDF("boolean", "string")
    Utils.check_answer(df2, [Row(True, "a"), Row(False, "b")], False)

    # TODO needs Variant class and Geography
    # case class


def test_create_nullable_dataframe_with_schema_inference(session):
    df = session.createDataFrame([(1, 1, None), (2, 3, True)]).toDF("a", "b", "c")
    assert (
        str(df.schema) == "StructType[StructField(A, Long, Nullable=False), "
        "StructField(B, Long, Nullable=False), "
        "StructField(C, Boolean, Nullable=True)]"
    )
    Utils.check_answer(df, [Row(1, 1, None), Row(2, 3, True)])


def test_schema_inference_binary_type(session):
    df = session.createDataFrame(
        [
            [(1).to_bytes(1, byteorder="big"), (2).to_bytes(1, byteorder="big")],
            [(3).to_bytes(1, byteorder="big"), (4).to_bytes(1, byteorder="big")],
            [None, b""],
        ]
    )
    assert (
        str(df.schema) == "StructType[StructField(_1, Binary, Nullable=True), "
        "StructField(_2, Binary, Nullable=False)]"
    )


def test_primitive_array(session):
    schema = StructType([StructField("arr", ArrayType(None))])
    df = session.createDataFrame([Row([1])], schema)
    Utils.check_answer(df, Row("[\n  1\n]"))


def test_time_date_and_timestamp_test(session):
    assert str(session.sql("select '00:00:00' :: Time").collect()[0][0]) == "00:00:00"
    assert (
        str(session.sql("select '1970-1-1 00:00:00' :: Timestamp").collect()[0][0])
        == "1970-01-01 00:00:00"
    )
    assert str(session.sql("select '1970-1-1' :: Date").collect()[0][0]) == "1970-01-01"


def test_quoted_column_names(session):
    normalName = "NORMAL_NAME"
    lowerCaseName = '"lower_case"'
    quoteStart = '"""quote_start"'
    quoteEnd = '"quote_end"""'
    quoteMiddle = '"quote_""_mid"'
    quoteAllCases = '"""quote_""_start"""'

    table_name = Utils.random_name()
    try:
        Utils.create_table(
            session,
            table_name,
            f"{normalName} int, {lowerCaseName} int, {quoteStart} int,"
            f"{quoteEnd} int, {quoteMiddle} int, {quoteAllCases} int",
        )
        session.sql(f"insert into {table_name} values(1, 2, 3, 4, 5, 6)").collect()

        # test select()
        df1 = session.table(table_name).select(
            normalName,
            lowerCaseName,
            quoteStart,
            quoteEnd,
            quoteMiddle,
            quoteAllCases,
        )
        schema1 = df1.schema

        assert len(schema1.fields) == 6
        assert schema1.fields[0].name == normalName
        assert schema1.fields[1].name == lowerCaseName
        assert schema1.fields[2].name == quoteStart
        assert schema1.fields[3].name == quoteEnd
        assert schema1.fields[4].name == quoteMiddle
        assert schema1.fields[5].name == quoteAllCases

        assert df1.collect() == [Row(1, 2, 3, 4, 5, 6)]

        # test select() + cacheResult() + select()
        # TODO uncomment cacheResult when available
        df2 = session.table(table_name).select(
            normalName,
            lowerCaseName,
            quoteStart,
            quoteEnd,
            quoteMiddle,
            quoteAllCases,
        )
        # df2 = df2.cacheResult().select(normalName,
        #                               lowerCaseName, quoteStart, quoteEnd,
        #                               quoteMiddle, quoteAllCases)
        schema2 = df2.schema

        assert len(schema2.fields) == 6
        assert schema2.fields[0].name == normalName
        assert schema2.fields[1].name == lowerCaseName
        assert schema2.fields[2].name == quoteStart
        assert schema2.fields[3].name == quoteEnd
        assert schema2.fields[4].name == quoteMiddle
        assert schema2.fields[5].name == quoteAllCases

        assert df1.collect() == [Row(1, 2, 3, 4, 5, 6)]

        # Test drop()
        df3 = session.table(table_name).drop(
            lowerCaseName, quoteStart, quoteEnd, quoteMiddle, quoteAllCases
        )
        schema3 = df3.schema
        assert len(schema3.fields) == 1
        assert schema3.fields[0].name == normalName
        assert df3.collect() == [Row(1)]

        # Test select() + cacheResult() + drop()
        # TODO uncomment cacheResult when available
        df4 = session.table(table_name).select(
            normalName,
            lowerCaseName,
            quoteStart,
            quoteEnd,
            quoteMiddle,
            quoteAllCases,
        )  # df4 = df4.cacheResult()
        df4 = df4.drop(lowerCaseName, quoteStart, quoteEnd, quoteMiddle, quoteAllCases)

        schema4 = df4.schema
        assert len(schema4.fields) == 1
        assert schema4.fields[0].name == normalName
        assert df4.collect() == [Row(1)]

    finally:
        Utils.drop_table(session, table_name)


def test_column_names_without_surrounding_quote(session):
    normalName = "NORMAL_NAME"
    lowerCaseName = '"lower_case"'
    quoteStart = '"""quote_start"'
    quoteEnd = '"quote_end"""'
    quoteMiddle = '"quote_""_mid"'
    quoteAllCases = '"""quote_""_start"""'

    table_name = Utils.random_name()
    try:
        Utils.create_table(
            session,
            table_name,
            f"{normalName} int, {lowerCaseName} int, {quoteStart} int,"
            f"{quoteEnd} int, {quoteMiddle} int, {quoteAllCases} int",
        )
        session.sql(f"insert into {table_name} values(1, 2, 3, 4, 5, 6)").collect()

        quoteStart2 = '"quote_start'
        quoteEnd2 = 'quote_end"'
        quoteMiddle2 = 'quote_"_mid'

        df1 = session.table(table_name).select(quoteStart2, quoteEnd2, quoteMiddle2)

        # Even if the input format can be simplified format,
        # the returned column is the same.
        schema1 = df1.schema
        assert len(schema1.fields) == 3
        assert schema1.fields[0].name == quoteStart
        assert schema1.fields[1].name == quoteEnd
        assert schema1.fields[2].name == quoteMiddle
        assert df1.collect() == [Row(3, 4, 5)]

    finally:
        Utils.drop_table(session, table_name)


def test_negative_test_for_user_input_invalid_quoted_name(session):
    df = session.createDataFrame([1, 2, 3]).toDF("a")
    with pytest.raises(SnowparkClientException) as ex_info:
        df.where(col('"A" = "A" --"') == 2).collect()
    assert "invalid identifier" in str(ex_info)


def test_clone_with_union_dataframe(session):
    table_name = Utils.random_name()
    try:
        Utils.create_table(session, table_name, "c1 int, c2 int")

        session.sql(f"insert into {table_name} values(1, 1),(2, 2)").collect()
        df = session.table(table_name)

        union_df = df.union(df)
        cloned_union_df = union_df.clone()
        res = cloned_union_df.collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1, 1), Row(1, 1), Row(2, 2), Row(2, 2)]
    finally:
        Utils.drop_table(session, table_name)


def test_dataframe_show_with_new_line(session):
    df = session.createDataFrame(
        ["line1\nline1.1\n", "line2", "\n", "line4", "\n\n", None]
    ).toDF("a")
    assert (
        df._DataFrame__show_string(10)
        == """
-----------
|"A"      |
-----------
|line1    |
|line1.1  |
|         |
|line2    |
|         |
|         |
|line4    |
|         |
|         |
|         |
|NULL     |
-----------\n""".lstrip()
    )

    df2 = session.createDataFrame(
        [
            ("line1\nline1.1\n", 1),
            ("line2", 2),
            ("\n", 3),
            ("line4", 4),
            ("\n\n", 5),
            (None, 6),
        ]
    ).toDF("a", "b")
    assert (
        df2._DataFrame__show_string(10)
        == """
-----------------
|"A"      |"B"  |
-----------------
|line1    |1    |
|line1.1  |     |
|         |     |
|line2    |2    |
|         |3    |
|         |     |
|line4    |4    |
|         |5    |
|         |     |
|         |     |
|NULL     |6    |
-----------------\n""".lstrip()
    )


def test_negative_test_to_input_invalid_table_name_for_saveAsTable(session):
    df = session.createDataFrame([(1, None), (2, "NotNull"), (3, None)]).toDF("a", "b")
    with pytest.raises(SnowparkClientException) as ex_info:
        df.write.saveAsTable("negative test invalid table name")
    assert re.compile("The object name .* is invalid.").match(ex_info.value.message)


def test_negative_test_to_input_invalid_view_name_for_createOrReplaceView(session):
    df = session.createDataFrame([[2, "NotNull"]]).toDF(["a", "b"])
    with pytest.raises(SnowparkClientException) as ex_info:
        df.createOrReplaceView("negative test invalid table name")
    assert re.compile("The object name .* is invalid.").match(ex_info.value.message)


def test_toDF_with_array_schema(session):
    df = session.createDataFrame([[1, "a"]]).toDF("a", "b")
    schema = df.schema
    assert len(schema.fields) == 2
    assert schema.fields[0].name == "A"
    assert schema.fields[1].name == "B"


def test_sort_with_array_arg(session):
    df = session.createDataFrame([(1, 1, 1), (2, 0, 4), (1, 2, 3)]).toDF(
        "col1", "col2", "col3"
    )
    df_sorted = df.sort([col("col1").asc(), col("col2").desc(), col("col3")])
    Utils.check_answer(df_sorted, [Row(1, 2, 3), Row(1, 1, 1), Row(2, 0, 4)], False)


def test_select_with_array_args(session):
    df = session.createDataFrame([[1, 2]]).toDF("col1", "col2")
    df_selected = df.select(df.col("col1"), lit("abc"), df.col("col1") + df.col("col2"))
    Utils.check_answer(df_selected, Row(1, "abc", 3))


def test_select_string_with_array_args(session):
    df = session.createDataFrame([[1, 2, 3]]).toDF("col1", "col2", "col3")
    df_selected = df.select(["col1", "col2"])
    Utils.check_answer(df_selected, [Row(1, 2)])


def test_drop_string_with_array_args(session):
    df = session.createDataFrame([[1, 2, 3]]).toDF("col1", "col2", "col3")
    Utils.check_answer(df.drop(["col3"]), [Row(1, 2)])


def test_drop_with_array_args(session):
    df = session.createDataFrame([[1, 2, 3]]).toDF("col1", "col2", "col3")
    Utils.check_answer(df.drop([df["col3"]]), [Row(1, 2)])


def test_agg_with_array_args(session):
    df = session.createDataFrame([[1, 2], [4, 5]]).toDF("col1", "col2")
    Utils.check_answer(df.agg([max(col("col1")), mean(col("col2"))]), [Row(4, 3.5)])


def test_groupby_with_array_args(session):
    df = session.createDataFrame(
        [
            ("country A", "state A", 50),
            ("country A", "state A", 50),
            ("country A", "state B", 5),
            ("country A", "state B", 5),
            ("country B", "state A", 100),
            ("country B", "state A", 100),
            ("country B", "state B", 10),
            ("country B", "state B", 10),
        ]
    ).toDF(["country", "state", "value"])

    expected = [
        Row("country A", "state A", 100),
        Row("country A", "state B", 10),
        Row("country B", "state A", 200),
        Row("country B", "state B", 20),
    ]

    Utils.check_answer(
        df.groupBy([col("country"), col("state")]).agg(sum(col("value"))), expected
    )


def test_groupby_string_with_array_args(session):
    df = session.createDataFrame(
        [
            ("country A", "state A", 50),
            ("country A", "state A", 50),
            ("country A", "state B", 5),
            ("country A", "state B", 5),
            ("country B", "state A", 100),
            ("country B", "state A", 100),
            ("country B", "state B", 10),
            ("country B", "state B", 10),
        ]
    ).toDF(["country", "state", "value"])

    expected = [
        Row("country A", "state A", 100),
        Row("country A", "state B", 10),
        Row("country B", "state A", 200),
        Row("country B", "state B", 20),
    ]

    Utils.check_answer(
        df.groupBy(["country", "state"]).agg(sum(col("value"))), expected
    )
