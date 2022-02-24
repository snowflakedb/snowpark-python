#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import copy
import math
import os
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Iterator

import pytest

import snowflake.connector
from snowflake.connector import ProgrammingError
from snowflake.snowpark import Row, Session
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import (
    SnowparkColumnException,
    SnowparkDataframeException,
    SnowparkInvalidObjectNameException,
    SnowparkPlanException,
)
from snowflake.snowpark.functions import (
    as_integer,
    col,
    datediff,
    get,
    lit,
    max,
    mean,
    min,
    parse_json,
    sum,
    to_timestamp,
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
from tests.utils import TestData, TestFiles, Utils

SAMPLING_DEVIATION = 0.4


def test_null_data_in_tables(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        Utils.create_table(session, table_name, "num int")
        session.sql(f"insert into {table_name} values(null),(null),(null)").collect()
        res = session.table(table_name).collect()
        assert res == [Row(None), Row(None), Row(None)]
    finally:
        Utils.drop_table(session, table_name)


def test_null_data_in_local_relation_with_filters(session):
    df = session.create_dataframe([[1, None], [2, "NotNull"], [3, None]]).to_df(
        ["a", "b"]
    )
    assert df.collect() == [Row(1, None), Row(2, "NotNull"), Row(3, None)]
    df2 = session.create_dataframe([[1, None], [2, "NotNull"], [3, None]]).to_df(
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
    df = session.create_dataframe([1, 2]).to_df("a").with_column("b", lit(None))
    assert df.collect() == [Row(1, None), Row(2, None)]

    df2 = session.create_dataframe([1, 2]).to_df("a").select(lit(None))
    assert len(df2.schema.fields) == 1
    assert df2.schema.fields[0].datatype == StringType()
    assert df2.collect() == [Row(None), Row(None)]


def test_write_null_data_to_table(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    df = session.create_dataframe([(1, None), (2, None), (3, None)]).to_df("a", "b")
    try:
        df.write.save_as_table(table_name)
        Utils.check_answer(session.table(table_name), df, True)
    finally:
        Utils.drop_table(session, table_name)


def test_createOrReplaceView_with_null_data(session):
    df = session.create_dataframe([[1, None], [2, "NotNull"], [3, None]]).to_df(
        ["a", "b"]
    )
    view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
    try:
        df.create_or_replace_view(view_name)

        res = session.sql(f"select * from {view_name}").collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1, None), Row(2, "NotNull"), Row(3, None)]
    finally:
        Utils.drop_view(session, view_name)


def test_adjust_column_width_of_show(session):
    df = session.create_dataframe([[1, None], [2, "NotNull"]]).to_df("a", "b")
    # run show(), make sure no error is reported
    df.show(10, 4)

    res = df._show_string(10, 4)
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
    df = session.create_dataframe([[1, None], [2, "NotNull"]]).to_df("a", "b")
    # run show(), make sure no error is reported
    df.show(10)

    res = df._show_string(10)
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
    df = session.create_dataframe(
        [
            ("line1\nline2", None),
            ("single line", "NotNull\none more line\nlast line"),
        ]
    ).to_df("a", "b")

    res = df._show_string(2)
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

    res = TestData.test_data1(session)._show_string(10)
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
    res = session.sql("drop table if exists test_table_123")._show_string(1)

    assert (
        res
        == """
------------------------------------------------------
|"status"                                            |
------------------------------------------------------
|Drop statement executed successfully (TEST_TABL...  |
------------------------------------------------------\n""".lstrip()
    )


def test_cache_result(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session._run_query(f"create temp table {table_name} (num int)")
    session._run_query(f"insert into {table_name} values(1),(2)")

    df = session.table(table_name)
    Utils.check_answer(df, [Row(1), Row(2)])

    session._run_query(f"insert into {table_name} values (3)")
    Utils.check_answer(df, [Row(1), Row(2), Row(3)])

    df1 = df.cache_result()
    session._run_query(f"insert into {table_name} values (4)")
    Utils.check_answer(df1, [Row(1), Row(2), Row(3)])
    Utils.check_answer(df, [Row(1), Row(2), Row(3), Row(4)])

    df2 = df1.where(col("num") > 2)
    Utils.check_answer(df2, [Row(3)])

    df3 = df.where(col("num") > 2)
    Utils.check_answer(df3, [Row(3), Row(4)])

    df4 = df1.cache_result()
    Utils.check_answer(df4, [Row(1), Row(2), Row(3)])

    session._run_query(f"drop table {table_name}")
    Utils.check_answer(df1, [Row(1), Row(2), Row(3)])
    Utils.check_answer(df2, [Row(3)])


def test_cache_result_with_show(session):
    table_name1 = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        session._run_query(f"create temp table {table_name1} (name string)")
        session._run_query(f"insert into {table_name1} values('{table_name1}')")
        table = session.table(table_name1)

        # SHOW TABLES
        df1 = session.sql("show tables").cache_result()
        table_names = [tn[1] for tn in df1.collect()]
        assert table_name1 in table_names

        # SHOW TABLES + SELECT
        df2 = session.sql("show tables").select('"created_on"', '"name"').cache_result()
        table_names = [tn[1] for tn in df2.collect()]
        assert table_name1 in table_names

        # SHOW TABLES + SELECT + Join
        df3 = session.sql("show tables").select('"created_on"', '"name"')
        df3.show()
        df4 = df3.join(table, df3['"name"'] == table["name"]).cache_result()
        table_names = [tn[0] for tn in df4.select("name").collect()]
        assert table_name1 in table_names
    finally:
        session._run_query(f"drop table {table_name1}")


def test_non_select_query_composition(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
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
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        session.sql(
            f"create or replace temporary table {table_name} (num int)"
        ).collect()
        df1 = session.sql("show tables")
        df2 = session.sql("show tables")

        df = df1.union(df2).select('"name"').filter(col('"name"') == table_name)
        res = df.collect()
        assert len(res) == 1

    finally:
        Utils.drop_table(session, table_name)


def test_non_select_query_composition_unionall(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        session.sql(
            f"create or replace temporary table {table_name} (num int)"
        ).collect()
        df1 = session.sql("show tables")
        df2 = session.sql("show tables")

        df = df1.union_all(df2).select('"name"').filter(col('"name"') == table_name)
        res = df.collect()
        assert len(res) == 2

    finally:
        Utils.drop_table(session, table_name)


def test_non_select_query_composition_self_union(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        session.sql(
            f"create or replace temporary table {table_name} (num int)"
        ).collect()
        df = session.sql("show tables")

        union = df.union(df).select('"name"').filter(col('"name"') == table_name)

        assert len(union.collect()) == 1
        assert len(union._plan.queries) == 3
    finally:
        Utils.drop_table(session, table_name)


def test_non_select_query_composition_self_unionall(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        session.sql(
            f"create or replace temporary table {table_name} (num int)"
        ).collect()
        df = session.sql("show tables")

        union = df.union_all(df).select('"name"').filter(col('"name"') == table_name)

        assert len(union.collect()) == 2
        assert len(union._plan.queries) == 3
    finally:
        Utils.drop_table(session, table_name)


def test_only_use_result_scan_when_composing_queries(session):
    df = session.sql("show tables")
    assert len(df._plan.queries) == 1
    assert df._plan.queries[0].sql == "show tables"

    df2 = df.select('"name"')
    assert len(df2._plan.queries) == 2
    assert "RESULT_SCAN" in df2._plan.queries[-1].sql


def test_joins_on_result_scan(session):
    df1 = session.sql("show tables").select(['"name"', '"kind"'])
    df2 = session.sql("show tables").select(['"name"', '"rows"'])

    result = df1.join(df2, '"name"')
    result.collect()  # no error
    assert len(result.schema.fields) == 3


def test_df_stat_corr(session):
    with pytest.raises(ProgrammingError) as exec_info:
        TestData.string1(session).stat.corr("a", "b")
    assert "Numeric value 'a' is not recognized" in str(exec_info)

    assert TestData.null_data2(session).stat.corr("a", "b") is None
    assert TestData.double4(session).stat.corr("a", "b") is None
    assert math.isnan(TestData.double3(session).stat.corr("a", "b"))
    math.isclose(TestData.double2(session).stat.corr("a", "b"), 0.9999999999999991)


def test_df_stat_cov(session):
    with pytest.raises(ProgrammingError) as exec_info:
        TestData.string1(session).stat.cov("a", "b")
    assert "Numeric value 'a' is not recognized" in str(exec_info)

    assert TestData.null_data2(session).stat.cov("a", "b") == 0
    assert TestData.double4(session).stat.cov("a", "b") is None
    assert math.isnan(TestData.double3(session).stat.cov("a", "b"))
    math.isclose(TestData.double2(session).stat.cov("a", "b"), 0.010000000000000037)


def test_df_stat_approxQuantile(session):
    assert TestData.approx_numbers(session).stat.approx_quantile("a", [0.5]) == [4.5]
    assert TestData.approx_numbers(session).stat.approx_quantile(
        "a", [0, 0.1, 0.4, 0.6, 1]
    ) == [-0.5, 0.5, 3.5, 5.5, 9.5]

    with pytest.raises(ProgrammingError) as exec_info:
        TestData.approx_numbers(session).stat.approx_quantile("a", [-1])
    assert "Invalid value [-1.0] for function 'APPROX_PERCENTILE_ESTIMATE'" in str(
        exec_info
    )

    with pytest.raises(ProgrammingError) as exec_info:
        TestData.string1(session).stat.approx_quantile("a", [0.5])
    assert "Numeric value 'test1' is not recognized" in str(exec_info)

    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, "num int")
    try:
        assert session.table(table_name).stat.approx_quantile("num", [0.5])[0] is None

        res = TestData.double2(session).stat.approx_quantile(["a", "b"], [0, 0.1, 0.6])
        Utils.assert_rows(
            res, [[0.05, 0.15000000000000002, 0.25], [0.45, 0.55, 0.6499999999999999]]
        )

        # ApproxNumbers2 contains a column called T, which conflicts with tmpColumnName.
        # This test demos that the query still works.
        assert (
            TestData.approx_numbers2(session).stat.approx_quantile("a", [0.5])[0] == 4.5
        )
        assert (
            TestData.approx_numbers2(session).stat.approx_quantile("t", [0.5])[0] == 3.0
        )

        assert TestData.double2(session).stat.approx_quantile("a", []) == []
        assert TestData.double2(session).stat.approx_quantile([], []) == []
        assert TestData.double2(session).stat.approx_quantile([], [0.5]) == []
    finally:
        Utils.drop_table(session, table_name)


def test_df_stat_crosstab(session):
    cross_tab = (
        TestData.monthly_sales(session).stat.crosstab("empid", "month").collect()
    )
    assert (
        cross_tab[0]["EMPID"] == 1
        and cross_tab[0]["'JAN'"] == 2
        and cross_tab[0]["'FEB'"] == 2
        and cross_tab[0]["'MAR'"] == 2
        and cross_tab[0]["'APR'"] == 2
    )
    assert (
        cross_tab[1]["EMPID"] == 2
        and cross_tab[1]["'JAN'"] == 2
        and cross_tab[1]["'FEB'"] == 2
        and cross_tab[1]["'MAR'"] == 2
        and cross_tab[1]["'APR'"] == 2
    )

    cross_tab_2 = (
        TestData.monthly_sales(session).stat.crosstab("month", "empid").collect()
    )
    assert (
        cross_tab_2[0]["MONTH"] == "JAN"
        and cross_tab_2[0]["CAST(1 AS NUMBER(38,0))"] == 2
        and cross_tab_2[1]["CAST(2 AS NUMBER(38,0))"] == 2
    )
    assert (
        cross_tab_2[1]["MONTH"] == "FEB"
        and cross_tab_2[1]["CAST(1 AS NUMBER(38,0))"] == 2
        and cross_tab_2[1]["CAST(2 AS NUMBER(38,0))"] == 2
    )
    assert (
        cross_tab_2[2]["MONTH"] == "MAR"
        and cross_tab_2[2]["CAST(1 AS NUMBER(38,0))"] == 2
        and cross_tab_2[2]["CAST(2 AS NUMBER(38,0))"] == 2
    )
    assert (
        cross_tab_2[3]["MONTH"] == "APR"
        and cross_tab_2[3]["CAST(1 AS NUMBER(38,0))"] == 2
        and cross_tab_2[3]["CAST(2 AS NUMBER(38,0))"] == 2
    )

    cross_tab_3 = TestData.date1(session).stat.crosstab("a", "b").collect()
    assert (
        cross_tab_3[0]["A"] == date(2020, 8, 1)
        and cross_tab_3[0]["CAST(1 AS NUMBER(38,0))"] == 1
        and cross_tab_3[0]["CAST(2 AS NUMBER(38,0))"] == 0
    )
    assert (
        cross_tab_3[1]["A"] == date(2010, 12, 1)
        and cross_tab_3[1]["CAST(1 AS NUMBER(38,0))"] == 0
        and cross_tab_3[1]["CAST(2 AS NUMBER(38,0))"] == 1
    )

    cross_tab_4 = TestData.date1(session).stat.crosstab("b", "a").collect()
    assert (
        cross_tab_4[0]["B"] == 1
        and cross_tab_4[0]["TO_DATE('2020-08-01')"] == 1
        and cross_tab_4[0]["TO_DATE('2010-12-01')"] == 0
    )
    assert (
        cross_tab_4[1]["B"] == 2
        and cross_tab_4[1]["TO_DATE('2020-08-01')"] == 0
        and cross_tab_4[1]["TO_DATE('2010-12-01')"] == 1
    )

    cross_tab_5 = TestData.string7(session).stat.crosstab("a", "b").collect()
    assert (
        cross_tab_5[0]["A"] == "str"
        and cross_tab_5[0]["CAST(1 AS NUMBER(38,0))"] == 1
        and cross_tab_5[0]["CAST(2 AS NUMBER(38,0))"] == 0
    )
    assert (
        cross_tab_5[1]["A"] is None
        and cross_tab_5[1]["CAST(1 AS NUMBER(38,0))"] == 0
        and cross_tab_5[1]["CAST(2 AS NUMBER(38,0))"] == 1
    )

    cross_tab_6 = TestData.string7(session).stat.crosstab("b", "a").collect()
    assert (
        cross_tab_6[0]["B"] == 1
        and cross_tab_6[0]["'str'"] == 1
        and cross_tab_6[0]["NULL"] == 0
    )
    assert (
        cross_tab_6[1]["B"] == 2
        and cross_tab_6[1]["'str'"] == 0
        and cross_tab_6[1]["NULL"] == 0
    )


def test_df_stat_sampleBy(session):
    sample_by = (
        TestData.monthly_sales(session)
        .stat.sample_by(col("empid"), {1: 0.0, 2: 1.0})
        .collect()
    )
    expected_data = [
        [2, 4500, "JAN"],
        [2, 35000, "JAN"],
        [2, 200, "FEB"],
        [2, 90500, "FEB"],
        [2, 2500, "MAR"],
        [2, 9500, "MAR"],
        [2, 800, "APR"],
        [2, 4500, "APR"],
    ]
    assert len(sample_by) == len(expected_data)
    for i, row in enumerate(sample_by):
        assert (
            row["EMPID"] == expected_data[i][0]
            and row["AMOUNT"] == expected_data[i][1]
            and row["MONTH"] == expected_data[i][2]
        )

    sample_by_2 = (
        TestData.monthly_sales(session)
        .stat.sample_by(col("month"), {"JAN": 1.0})
        .collect()
    )
    expected_data_2 = [
        [1, 10000, "JAN"],
        [1, 400, "JAN"],
        [2, 4500, "JAN"],
        [2, 35000, "JAN"],
    ]
    assert len(sample_by_2) == len(expected_data_2)
    for i, row in enumerate(sample_by_2):
        assert (
            row["EMPID"] == expected_data_2[i][0]
            and row["AMOUNT"] == expected_data_2[i][1]
            and row["MONTH"] == expected_data_2[i][2]
        )

    sample_by_3 = TestData.monthly_sales(session).stat.sample_by(col("month"), {})
    schema_names = sample_by_3.schema.names
    assert (
        schema_names[0] == "EMPID"
        and schema_names[1] == "AMOUNT"
        and schema_names[2] == "MONTH"
    )
    assert len(sample_by_3.collect()) == 0


def test_df_stat_crosstab_max_column_test(session):
    df1 = session.create_dataframe(
        [
            [Utils.random_alphanumeric_str(230), Utils.random_alphanumeric_str(230)]
            for _ in range(1000)
        ],
        schema=["a", "b"],
    )
    assert df1.stat.crosstab("a", "b").count() == 1000

    df2 = session.create_dataframe(
        [
            [Utils.random_alphanumeric_str(230), Utils.random_alphanumeric_str(230)]
            for _ in range(1001)
        ],
        schema=["a", "b"],
    )
    with pytest.raises(SnowparkDataframeException) as exec_info:
        df2.stat.crosstab("a", "b").collect()
    assert (
        "The number of distinct values in the second input column (1001) exceeds the maximum number of distinct values allowed (1000)"
        in str(exec_info)
    )

    df3 = session.create_dataframe([[1, 1] for _ in range(1000)], schema=["a", "b"])
    res_3 = df3.stat.crosstab("a", "b").collect()
    assert len(res_3) == 1
    assert res_3[0]["A"] == 1 and res_3[0]["CAST(1 AS NUMBER(38,0))"] == 1000

    df4 = session.create_dataframe([[1, 1] for _ in range(1001)], schema=["a", "b"])
    res_4 = df4.stat.crosstab("a", "b").collect()
    assert len(res_4) == 1
    assert res_4[0]["A"] == 1 and res_4[0]["CAST(1 AS NUMBER(38,0))"] == 1001


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


def test_sample_with_seed(session):
    row_count = 10000
    temp_table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.range(row_count).write.save_as_table(
        temp_table_name, create_temp_table=True
    )
    df = session.table(temp_table_name)
    try:
        sample1 = df.sample(frac=0.1, seed=1).collect()
        sample2 = df.sample(frac=0.1, seed=1).collect()
        Utils.check_answer(sample1, sample2, sort=True)
    finally:
        Utils.drop_table(session, temp_table_name)


def test_sample_with_sampling_method(session):
    """sampling method actually has no impact on result. It has impact on performance."""
    row_count = 10000
    temp_table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.range(row_count).write.save_as_table(
        temp_table_name, create_temp_table=True
    )
    df = session.table(temp_table_name)
    try:
        assert df.sample(frac=0.0, sampling_method="BLOCK").count() == 0
        half_row_count = row_count * 0.5
        assert (
            abs(df.sample(frac=0.5, sampling_method="BLOCK").count() - half_row_count)
            < half_row_count * SAMPLING_DEVIATION
        )
        assert df.sample(frac=1.0, sampling_method="BLOCK").count() == row_count
        assert len(df.sample(frac=0.0, sampling_method="BLOCK").collect()) == 0
        half_row_count = row_count * 0.5
        assert (
            abs(
                len(df.sample(frac=0.5, sampling_method="BLOCK").collect())
                - half_row_count
            )
            < half_row_count * SAMPLING_DEVIATION
        )
        assert len(df.sample(frac=1.0, sampling_method="BLOCK").collect()) == row_count
    finally:
        Utils.drop_table(session, temp_table_name)


def test_sample_negative(session):
    """Tests negative test cases for sample"""
    row_count = 10000
    df = session.range(row_count)
    with pytest.raises(ValueError):
        df.sample(n=-1)
    with pytest.raises(snowflake.connector.errors.ProgrammingError):
        df.sample(n=1000001).count()
    with pytest.raises(ValueError):
        df.sample(frac=-0.01)
    with pytest.raises(ValueError):
        df.sample(frac=1.01)

    table = session.table("non_existing_table")
    with pytest.raises(ValueError):
        table.sample(sampling_method="InvalidValue")


def test_sample_on_join(session):
    """Tests running sample on a join statement"""
    row_count = 10000
    df1 = session.range(row_count).with_column('"name"', lit("value1"))
    df2 = session.range(row_count).with_column('"name"', lit("value2"))

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
    df1 = session.range(row_count).with_column('"name"', lit("value1"))
    df2 = session.range(5000, 5000 + row_count).with_column('"name"', lit("value2"))

    # Test union
    result = df1.union(df2)
    sample_row_count = int(result.count() / 10)
    assert result.sample(n=sample_row_count).count() == sample_row_count
    assert (
        abs(result.sample(frac=0.1).count() - sample_row_count)
        < sample_row_count * SAMPLING_DEVIATION
    )
    # Test union_all
    result = df1.union_all(df2)
    sample_row_count = int(result.count() / 10)
    assert result.sample(n=sample_row_count).count() == sample_row_count
    assert (
        abs(result.sample(frac=0.1).count() - sample_row_count)
        < sample_row_count * SAMPLING_DEVIATION
    )


def test_toDf(session):
    # to_df(*str) with 1 column
    df1 = session.create_dataframe([1, 2, 3]).to_df("a")
    assert (
        df1.count() == 3
        and len(df1.schema.fields) == 1
        and df1.schema.fields[0].name == "A"
    )
    df1.show()
    # to_df([str]) with 1 column
    df2 = session.create_dataframe([1, 2, 3]).to_df(["a"])
    assert (
        df2.count() == 3
        and len(df2.schema.fields) == 1
        and df2.schema.fields[0].name == "A"
    )
    df2.show()

    # to_df(*str) with 2 columns
    df3 = session.create_dataframe([(1, None), (2, "NotNull"), (3, None)]).to_df(
        "a", "b"
    )
    assert df3.count() == 3 and len(df3.schema.fields) == 2
    assert df3.schema.fields[0].name == "A" and df3.schema.fields[-1].name == "B"
    # to_df([str]) with 2 columns
    df4 = session.create_dataframe([(1, None), (2, "NotNull"), (3, None)]).to_df(
        ["a", "b"]
    )
    assert df4.count() == 3 and len(df4.schema.fields) == 2
    assert df4.schema.fields[0].name == "A" and df4.schema.fields[-1].name == "B"

    # to_df(*str) with 3 columns
    df5 = session.create_dataframe(
        [(1, None, "a"), (2, "NotNull", "a"), (3, None, "a")]
    ).to_df("a", "b", "c")
    assert df5.count() == 3 and len(df5.schema.fields) == 3
    assert df5.schema.fields[0].name == "A" and df5.schema.fields[-1].name == "C"
    # to_df([str]) with 3 columns
    df6 = session.create_dataframe(
        [(1, None, "a"), (2, "NotNull", "a"), (3, None, "a")]
    ).to_df(["a", "b", "c"])
    assert df6.count() == 3 and len(df6.schema.fields) == 3
    assert df6.schema.fields[0].name == "A" and df6.schema.fields[-1].name == "C"


def test_toDF_negative_test(session):
    values = session.create_dataframe([[1, None], [2, "NotNull"], [3, None]])

    # to_df(*str) with invalid args count
    with pytest.raises(ValueError) as ex_info:
        values.to_df()
    assert "The number of columns doesn't match" in ex_info.value.args[0]
    with pytest.raises(ValueError):
        values.to_df("a")
    assert "The number of columns doesn't match" in ex_info.value.args[0]
    with pytest.raises(ValueError):
        values.to_df("a", "b", "c")
    assert "The number of columns doesn't match" in ex_info.value.args[0]

    # to_df([str]) with invalid args count
    with pytest.raises(ValueError):
        values.to_df([])
    assert "The number of columns doesn't match" in ex_info.value.args[0]
    with pytest.raises(ValueError):
        values.to_df(["a"])
    assert "The number of columns doesn't match" in ex_info.value.args[0]
    with pytest.raises(ValueError):
        values.to_df(["a", "b", "c"])
    assert "The number of columns doesn't match" in ex_info.value.args[0]


def test_sort(session):
    df = session.create_dataframe(
        [(1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (2, 3), (3, 1), (3, 2), (3, 3)]
    ).to_df("a", "b")

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
    df = session.create_dataframe([(1, "a", 10), (2, "b", 20), (3, "c", 30)]).to_df(
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
    df = session.create_dataframe([(1, "a", 10), (2, "b", 20), (3, "c", 30)]).to_df(
        ["a", "b", "c"]
    )

    # Select with empty sequences
    with pytest.raises(ValueError) as ex_info:
        df.select()
    assert "The input of select() cannot be empty" in str(ex_info)

    with pytest.raises(ValueError) as ex_info:
        df.select([])
    assert "The input of select() cannot be empty" in str(ex_info)

    # select columns which don't exist
    with pytest.raises(snowflake.connector.errors.ProgrammingError) as ex_info:
        df.select("not_exists_column").collect()
    assert "SQL compilation error" in str(ex_info)

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as ex_info:
        df.select(["not_exists_column"]).collect()
    assert "SQL compilation error" in str(ex_info)

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as ex_info:
        df.select(col("not_exists_column")).collect()
    assert "SQL compilation error" in str(ex_info)

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as ex_info:
        df.select([col("not_exists_column")]).collect()
    assert "SQL compilation error" in str(ex_info)


def test_drop_and_dropcolumns(session):
    df = session.create_dataframe([(1, "a", 10), (2, "b", 20), (3, "c", 30)]).to_df(
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
    with pytest.raises(SnowparkColumnException) as ex_info:
        df.drop("a", "b", "c")
    assert "Cannot drop all column" in str(ex_info)

    with pytest.raises(SnowparkColumnException) as ex_info:
        df.drop(["a", "b", "c"])
    assert "Cannot drop all column" in str(ex_info)

    with pytest.raises(SnowparkColumnException) as ex_info:
        df.drop(col("a"), col("b"), col("c"))
    assert "Cannot drop all column" in str(ex_info)

    with pytest.raises(SnowparkColumnException) as ex_info:
        df.drop([col("a"), col("b"), col("c")])
    assert "Cannot drop all column" in str(ex_info)


def test_dataframe_agg(session):
    df = session.create_dataframe([(1, "One"), (2, "Two"), (3, "Three")]).to_df(
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


def test_rollup(session):
    df = session.create_dataframe(
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
    ).to_df(["country", "state", "value"])

    # At least one column needs to be provided ( negative test )
    with pytest.raises(snowflake.connector.errors.ProgrammingError) as ex_info:
        df.rollup(list()).agg(sum(col("value"))).show()

    assert "001003 (42000): " in str(ex_info) and "SQL compilation error" in str(
        ex_info
    )

    # rollup() on 1 column
    expected_result = [
        Row("country A", 110),
        Row("country B", 220),
        Row(None, 330),
    ]
    Utils.check_answer(df.rollup("country").agg(sum(col("value"))), expected_result)
    Utils.check_answer(df.rollup(["country"]).agg(sum(col("value"))), expected_result)
    Utils.check_answer(
        df.rollup(col("country")).agg(sum(col("value"))), expected_result
    )
    Utils.check_answer(
        df.rollup([col("country")]).agg(sum(col("value"))), expected_result
    )

    # rollup() on 2 columns
    expected_result = [
        Row(None, None, 330),
        Row("country A", None, 110),
        Row("country A", "state A", 100),
        Row("country A", "state B", 10),
        Row("country B", None, 220),
        Row("country B", "state A", 200),
        Row("country B", "state B", 20),
    ]
    Utils.check_answer(
        df.rollup("country", "state")
        .agg(sum(col("value")))
        .sort(col("country"), col("state")),
        expected_result,
        False,
    )
    Utils.check_answer(
        df.rollup(["country", "state"])
        .agg(sum(col("value")))
        .sort(col("country"), col("state")),
        expected_result,
        False,
    )
    Utils.check_answer(
        df.rollup(col("country"), col("state"))
        .agg(sum(col("value")))
        .sort(col("country"), col("state")),
        expected_result,
        False,
    )
    Utils.check_answer(
        df.rollup([col("country"), col("state")])
        .agg(sum(col("value")))
        .sort(col("country"), col("state")),
        expected_result,
        False,
    )


def test_groupby(session):
    df = session.create_dataframe(
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
    ).to_df(["country", "state", "value"])

    # group_by without column
    assert df.group_by().agg(max(col("value"))).collect() == [Row(100)]
    assert df.group_by([]).agg(sum(col("value"))).collect() == [Row(330)]
    assert df.group_by().agg([sum(col("value"))]).collect() == [Row(330)]

    # group_by() on 1 column
    expected_res = [Row("country A", 110), Row("country B", 220)]
    assert df.group_by("country").agg(sum(col("value"))).collect() == expected_res
    assert df.group_by(["country"]).agg(sum(col("value"))).collect() == expected_res
    assert df.group_by(col("country")).agg(sum(col("value"))).collect() == expected_res
    assert (
        df.group_by([col("country")]).agg(sum(col("value"))).collect() == expected_res
    )

    # group_by() on 2 columns
    expected_res = [
        Row("country A", "state B", 10),
        Row("country B", "state B", 20),
        Row("country A", "state A", 100),
        Row("country B", "state A", 200),
    ]

    res = df.group_by(["country", "state"]).agg(sum(col("value"))).collect()
    assert sorted(res, key=lambda x: x[2]) == expected_res

    res = df.group_by([col("country"), col("state")]).agg(sum(col("value"))).collect()
    assert sorted(res, key=lambda x: x[2]) == expected_res


def test_cube(session):
    df = session.create_dataframe(
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
    ).to_df(["country", "state", "value"])

    # At least one column needs to be provided ( negative test )
    with pytest.raises(snowflake.connector.errors.ProgrammingError) as ex_info:
        df.cube(list()).agg(sum(col("value"))).show()

    assert "001003 (42000): " in str(ex_info) and "SQL compilation error" in str(
        ex_info
    )

    # cube() on 1 column
    expected_result = [
        Row("country A", 110),
        Row("country B", 220),
        Row(None, 330),
    ]
    Utils.check_answer(df.cube("country").agg(sum(col("value"))), expected_result)
    Utils.check_answer(df.cube(["country"]).agg(sum(col("value"))), expected_result)
    Utils.check_answer(df.cube(col("country")).agg(sum(col("value"))), expected_result)
    Utils.check_answer(
        df.cube([col("country")]).agg(sum(col("value"))), expected_result
    )

    # cube() on 2 columns
    expected_result = [
        Row(None, None, 330),
        Row(None, "state A", 300),  # This is an extra row comparing with rollup().
        Row(None, "state B", 30),  # This is an extra row comparing with rollup().
        Row("country A", None, 110),
        Row("country A", "state A", 100),
        Row("country A", "state B", 10),
        Row("country B", None, 220),
        Row("country B", "state A", 200),
        Row("country B", "state B", 20),
    ]
    Utils.check_answer(
        df.cube("country", "state")
        .agg(sum(col("value")))
        .sort(col("country"), col("state")),
        expected_result,
        False,
    )
    Utils.check_answer(
        df.cube(["country", "state"])
        .agg(sum(col("value")))
        .sort(col("country"), col("state")),
        expected_result,
        False,
    )
    Utils.check_answer(
        df.cube(col("country"), col("state"))
        .agg(sum(col("value")))
        .sort(col("country"), col("state")),
        expected_result,
        False,
    )
    Utils.check_answer(
        df.cube([col("country"), col("state")])
        .agg(sum(col("value")))
        .sort(col("country"), col("state")),
        expected_result,
        False,
    )


def test_flatten(session):
    table = session.sql("select parse_json(a) as a from values('[1,2]') as T(a)")
    Utils.check_answer(table.flatten(table["a"]).select("value"), [Row("1"), Row("2")])

    # conflict column names
    table1 = session.sql(
        "select parse_json(value) as value from values('[1,2]') as T(value)"
    )
    flatten = table1.flatten(
        table1["value"], "", outer=False, recursive=False, mode="both"
    )
    Utils.check_answer(
        flatten.select(table1["value"], flatten["value"]),
        [Row("[\n  1,\n  2\n]", "1"), Row("[\n  1,\n  2\n]", "2")],
        sort=False,
    )

    # multiple flatten
    flatten1 = flatten.flatten(
        table1["value"], "[0]", outer=True, recursive=True, mode="array"
    )
    Utils.check_answer(
        flatten1.select(table1["value"], flatten["value"], flatten1["value"]),
        [Row("[\n  1,\n  2\n]", "1", "1"), Row("[\n  1,\n  2\n]", "2", "1")],
        sort=False,
    )

    # wrong mode
    with pytest.raises(ValueError) as ex_info:
        flatten.flatten(col("value"), "", outer=False, recursive=False, mode="wrong")
    assert "mode must be one of ('OBJECT', 'ARRAY', 'BOTH')" in str(ex_info)

    # contains multiple query
    df = session.sql("show schemas").limit(1)
    # scala uses `show tables`. But there is no table in python test. `show schemas` guarantees result is not empty.
    df1 = df.with_column("value", lit("[1,2]")).select(
        parse_json(col("value")).as_("value")
    )
    flatten2 = df1.flatten(df1["value"])
    Utils.check_answer(
        flatten2.select(flatten2["value"]), [Row("1"), Row("2")], sort=False
    )

    # flatten with object traversing
    table2 = session.sql("select * from values('{\"a\":[1,2]}') as T(a)").select(
        parse_json(col("a")).as_("a")
    )

    flatten3 = table2.flatten(table2["a"]["a"])
    Utils.check_answer(
        flatten3.select(flatten3["value"]), [Row("1"), Row("2")], sort=False
    )

    # join
    df2 = table.flatten(table["a"]).select(col("a"), col("value"))
    df3 = table2.flatten(table2["a"]["a"]).select(col("a"), col("value"))

    Utils.check_answer(
        df2.join(df3, df2["value"] == df3["value"]).select(df3["value"]),
        [Row("1"), Row("2")],
        sort=False,
    )

    # union
    Utils.check_answer(
        df2.union(df3).select(col("value")),
        [Row("1"), Row("2"), Row("1"), Row("2")],
        sort=False,
    )


def test_flatten_in_session(session):
    Utils.check_answer(
        session.flatten(parse_json(lit("""["a","'"]"""))).select(col("value")),
        [Row('"a"'), Row('"\'"')],
        sort=False,
    )

    Utils.check_answer(
        session.flatten(
            parse_json(lit("""{"a":[1,2]}""")),
            "a",
            outer=True,
            recursive=True,
            mode="ARRAY",
        ).select("value"),
        [Row("1"), Row("2")],
    )

    with pytest.raises(ValueError):
        session.flatten(
            parse_json(lit("[1]")), "", outer=False, recursive=False, mode="wrong"
        )

    df1 = session.flatten(parse_json(lit("[1,2]")))
    df2 = session.flatten(
        parse_json(lit("""{"a":[1,2]}""")),
        "a",
        outer=False,
        recursive=False,
        mode="BOTH",
    )

    # union
    Utils.check_answer(
        df1.union(df2).select("path"),
        [Row("[0]"), Row("[1]"), Row("a[0]"), Row("a[1]")],
        sort=False,
    )

    # join
    Utils.check_answer(
        df1.join(df2, df1["value"] == df2["value"]).select(
            df1["path"].as_("path1"), df2["path"].as_("path2")
        ),
        [Row("[0]", "a[0]"), Row("[1]", "a[1]")],
        sort=False,
    )


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

    result = session.create_dataframe(data, schema)
    schema_str = str(result.schema)
    assert (
        schema_str == "StructType[StructField(STRING, String, Nullable=True), "
        "StructField(BYTE, Long, Nullable=True), "
        "StructField(SHORT, Long, Nullable=True), "
        "StructField(INT, Long, Nullable=True), "
        "StructField(LONG, Long, Nullable=True), "
        "StructField(FLOAT, Double, Nullable=True), "
        "StructField(DOUBLE, Double, Nullable=True), "
        "StructField(NUMBER, Decimal(10, 3), Nullable=True), "
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
    df = session.create_dataframe(data, schema)
    schema_str = str(df.schema)
    assert schema_str == "StructType[StructField(TIME, Time, Nullable=True)]"
    assert df.collect() == data


def test_show_collect_with_misc_commands(session, resources_path, tmpdir):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
    stage_name = Utils.random_name_for_temp_object(TempObjectType.STAGE)
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
        f"create temp table {table_name} (c1 int)",
        f"drop table {table_name}",
        f"create temp view {view_name} (string) as select current_version()",
        f"drop view {view_name}",
        f"show tables",
        f"drop stage {stage_name}",
    ]

    # Misc commands with show
    for command in misc_commands:
        session.sql(command).show()

    # Misc commands with collect()
    for command in misc_commands:
        session.sql(command).collect()

    # Misc commands with session._conn.getResultAndMetadata
    for command in misc_commands:
        rows, meta = session._conn.get_result_and_metadata(session.sql(command)._plan)
        assert len(rows) == 0 or len(rows[0]) == len(meta)


def test_createDataFrame_with_given_schema_array_map_variant(session):
    schema = StructType(
        [
            StructField("array", ArrayType(None)),
            StructField("map", MapType(None, None)),
            StructField("variant", VariantType()),
            StructField("geography", GeographyType()),
        ]
    )
    data = [
        Row(["'", 2], {"'": 1}, 1, "POINT(30 10)"),
        Row(None, None, None, None),
    ]
    df = session.create_dataframe(data, schema)
    assert (
        str(df.schema)
        == "StructType[StructField(ARRAY, ArrayType[String], Nullable=True), "
        "StructField(MAP, MapType[String, String], Nullable=True), "
        "StructField(VARIANT, Variant, Nullable=True), "
        "StructField(GEOGRAPHY, Geography, Nullable=True)]"
    )
    df.show()
    geography_string = """{
  "coordinates": [
    30,
    10
  ],
  "type": "Point"
}"""
    expected = [
        Row('[\n  "\'",\n  2\n]', '{\n  "\'": 1\n}', "1", geography_string),
        Row(None, None, None, None),
    ]
    Utils.check_answer(df, expected, sort=False)


def test_variant_in_array_and_map(session):
    schema = StructType(
        [StructField("array", ArrayType(None)), StructField("map", MapType(None, None))]
    )
    data = [Row([1, "\"'"], {"a": "\"'"})]
    df = session.create_dataframe(data, schema)
    Utils.check_answer(df, [Row('[\n  1,\n  "\\"\'"\n]', '{\n  "a": "\\"\'"\n}')])


def test_escaped_character(session):
    df = session.create_dataframe(["'", "\\", "\n"]).to_df("a")
    res = df.collect()
    assert res == [Row("'"), Row("\\"), Row("\n")]


def test_create_or_replace_temporary_view(session, db_parameters):
    view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
    view_name1 = f'"{view_name}%^11"'
    view_name2 = f'"{view_name}"'

    try:
        df = session.create_dataframe([1, 2, 3]).to_df("a")
        df.create_or_replace_temp_view(view_name)
        res = session.table(view_name).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]

        # test replace
        df2 = session.create_dataframe(["a", "b", "c"]).to_df("b")
        df2.create_or_replace_temp_view(view_name)
        res = session.table(view_name).collect()
        assert res == [Row("a"), Row("b"), Row("c")]

        # view name has special char
        df.create_or_replace_temp_view(view_name1)
        res = session.table(view_name1).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]

        # view name has quote
        df.create_or_replace_temp_view(view_name2)
        res = session.table(view_name2).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(2), Row(3)]

        # Get a second session object
        session2 = Session.builder.configs(db_parameters).create()
        with session2:
            assert session is not session2
            with pytest.raises(snowflake.connector.errors.ProgrammingError) as ex_info:
                session2.table(view_name).collect()
            assert "does not exist or not authorized" in str(ex_info)
    finally:
        Utils.drop_view(session, view_name)
        Utils.drop_view(session, view_name1)
        Utils.drop_view(session, view_name2)


def test_createDataFrame_with_schema_inference(session):
    df1 = session.create_dataframe([1, 2, 3]).to_df("int")
    Utils.check_answer(df1, [Row(1), Row(2), Row(3)])
    schema1 = df1.schema
    assert len(schema1.fields) == 1
    assert schema1.fields[0].name == "INT"
    assert schema1.fields[0].datatype == LongType()

    # tuple
    df2 = session.create_dataframe([(True, "a"), (False, "b")]).to_df(
        "boolean", "string"
    )
    Utils.check_answer(df2, [Row(True, "a"), Row(False, "b")], False)

    # TODO needs Variant class and Geography
    # case class


def test_create_nullable_dataframe_with_schema_inference(session):
    df = session.create_dataframe([(1, 1, None), (2, 3, True)]).to_df("a", "b", "c")
    assert (
        str(df.schema) == "StructType[StructField(A, Long, Nullable=False), "
        "StructField(B, Long, Nullable=False), "
        "StructField(C, Boolean, Nullable=True)]"
    )
    Utils.check_answer(df, [Row(1, 1, None), Row(2, 3, True)])


def test_schema_inference_binary_type(session):
    df = session.create_dataframe(
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
    df = session.create_dataframe([Row([1])], schema)
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

    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
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

    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
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
    df = session.create_dataframe([1, 2, 3]).to_df("a")
    with pytest.raises(SnowparkPlanException) as ex_info:
        df.where(col('"A" = "A" --"') == 2).collect()
    assert "Invalid identifier" in str(ex_info)


def test_clone_with_union_dataframe(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        Utils.create_table(session, table_name, "c1 int, c2 int")

        session.sql(f"insert into {table_name} values(1, 1),(2, 2)").collect()
        df = session.table(table_name)

        union_df = df.union(df)
        cloned_union_df = copy.copy(union_df)
        res = cloned_union_df.collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1, 1), Row(2, 2)]
    finally:
        Utils.drop_table(session, table_name)


def test_clone_with_unionall_dataframe(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        Utils.create_table(session, table_name, "c1 int, c2 int")

        session.sql(f"insert into {table_name} values(1, 1),(2, 2)").collect()
        df = session.table(table_name)

        union_df = df.union_all(df)
        cloned_union_df = copy.copy(union_df)
        res = cloned_union_df.collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1, 1), Row(1, 1), Row(2, 2), Row(2, 2)]
    finally:
        Utils.drop_table(session, table_name)


def test_dataframe_show_with_new_line(session):
    df = session.create_dataframe(
        ["line1\nline1.1\n", "line2", "\n", "line4", "\n\n", None]
    ).to_df("a")
    assert (
        df._show_string(10)
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

    df2 = session.create_dataframe(
        [
            ("line1\nline1.1\n", 1),
            ("line2", 2),
            ("\n", 3),
            ("line4", 4),
            ("\n\n", 5),
            (None, 6),
        ]
    ).to_df("a", "b")
    assert (
        df2._show_string(10)
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
    df = session.create_dataframe([(1, None), (2, "NotNull"), (3, None)]).to_df(
        "a", "b"
    )
    with pytest.raises(SnowparkInvalidObjectNameException) as ex_info:
        df.write.save_as_table("negative test invalid table name")
    assert re.compile("The object name .* is invalid.").match(ex_info.value.message)


def test_negative_test_to_input_invalid_view_name_for_createOrReplaceView(session):
    df = session.create_dataframe([[2, "NotNull"]]).to_df(["a", "b"])
    with pytest.raises(SnowparkInvalidObjectNameException) as ex_info:
        df.create_or_replace_view("negative test invalid table name")
    assert re.compile("The object name .* is invalid.").match(ex_info.value.message)


def test_toDF_with_array_schema(session):
    df = session.create_dataframe([[1, "a"]]).to_df("a", "b")
    schema = df.schema
    assert len(schema.fields) == 2
    assert schema.fields[0].name == "A"
    assert schema.fields[1].name == "B"


def test_sort_with_array_arg(session):
    df = session.create_dataframe([(1, 1, 1), (2, 0, 4), (1, 2, 3)]).to_df(
        "col1", "col2", "col3"
    )
    df_sorted = df.sort([col("col1").asc(), col("col2").desc(), col("col3")])
    Utils.check_answer(df_sorted, [Row(1, 2, 3), Row(1, 1, 1), Row(2, 0, 4)], False)


def test_select_with_array_args(session):
    df = session.create_dataframe([[1, 2]]).to_df("col1", "col2")
    df_selected = df.select(df.col("col1"), lit("abc"), df.col("col1") + df.col("col2"))
    Utils.check_answer(df_selected, Row(1, "abc", 3))


def test_select_string_with_array_args(session):
    df = session.create_dataframe([[1, 2, 3]]).to_df("col1", "col2", "col3")
    df_selected = df.select(["col1", "col2"])
    Utils.check_answer(df_selected, [Row(1, 2)])


def test_drop_string_with_array_args(session):
    df = session.create_dataframe([[1, 2, 3]]).to_df("col1", "col2", "col3")
    Utils.check_answer(df.drop(["col3"]), [Row(1, 2)])


def test_drop_with_array_args(session):
    df = session.create_dataframe([[1, 2, 3]]).to_df("col1", "col2", "col3")
    Utils.check_answer(df.drop([df["col3"]]), [Row(1, 2)])


def test_agg_with_array_args(session):
    df = session.create_dataframe([[1, 2], [4, 5]]).to_df("col1", "col2")
    Utils.check_answer(df.agg([max(col("col1")), mean(col("col2"))]), [Row(4, 3.5)])


def test_rollup_with_array_args(session):
    df = session.create_dataframe(
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
    ).to_df(["country", "state", "value"])

    expected_result = [
        Row(None, None, 330),
        Row("country A", None, 110),
        Row("country A", "state A", 100),
        Row("country A", "state B", 10),
        Row("country B", None, 220),
        Row("country B", "state A", 200),
        Row("country B", "state B", 20),
    ]

    Utils.check_answer(
        df.rollup([col("country"), col("state")])
        .agg(sum(col("value")))
        .sort(col("country"), col("state")),
        expected_result,
        sort=False,
    )


def test_rollup_string_with_array_args(session):
    df = session.create_dataframe(
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
    ).to_df(["country", "state", "value"])

    expected_result = [
        Row(None, None, 330),
        Row("country A", None, 110),
        Row("country A", "state A", 100),
        Row("country A", "state B", 10),
        Row("country B", None, 220),
        Row("country B", "state A", 200),
        Row("country B", "state B", 20),
    ]

    Utils.check_answer(
        df.rollup(["country", "state"])
        .agg(sum("value"))
        .sort(col("country"), col("state")),
        expected_result,
        sort=False,
    )


def test_groupby_with_array_args(session):
    df = session.create_dataframe(
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
    ).to_df(["country", "state", "value"])

    expected = [
        Row("country A", "state A", 100),
        Row("country A", "state B", 10),
        Row("country B", "state A", 200),
        Row("country B", "state B", 20),
    ]

    Utils.check_answer(
        df.group_by([col("country"), col("state")]).agg(sum(col("value"))), expected
    )


def test_groupby_string_with_array_args(session):
    df = session.create_dataframe(
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
    ).to_df(["country", "state", "value"])

    expected = [
        Row("country A", "state A", 100),
        Row("country A", "state B", 10),
        Row("country B", "state A", 200),
        Row("country B", "state B", 20),
    ]

    Utils.check_answer(
        df.group_by(["country", "state"]).agg(sum(col("value"))), expected
    )


def test_rename_basic(session):
    df = session.create_dataframe([[1, 2]], schema=["a", "b"])
    df2 = df.with_column_renamed("b", "b1")
    assert df2.schema.names[1] == "B1"
    Utils.check_answer(df2, [Row(1, 2)])


def test_rename_join_dataframe(session):
    df_left = session.create_dataframe([[1, 2]], schema=["a", "b"])
    df_right = session.create_dataframe([[3, 4]], schema=["a", "c"])
    df_join = df_left.join(df_right)

    # rename left df columns including ambiguous columns
    df1 = df_join.rename(df_left.a, "left_a").rename(df_left.b, "left_b")
    assert df1.schema.names[0] == "LEFT_A" and df1.schema.names[1] == "LEFT_B"
    Utils.check_answer(df1, [Row(1, 2, 3, 4)])

    df2 = df1.rename(df_right.a, "right_a").rename(df_right.c, "right_c")
    assert df2.schema.names == ["LEFT_A", "LEFT_B", "RIGHT_A", "RIGHT_C"]
    Utils.check_answer(df2, [Row(1, 2, 3, 4)])

    # Get columns for right DF's columns
    df3 = df2.select(df_right["a"], df_right["c"])
    assert df3.schema.names == ["RIGHT_A", "RIGHT_C"]
    Utils.check_answer(df3, [Row(3, 4)])


def test_rename_to_df_and_joined_dataframe(session):
    df1 = session.create_dataframe([[1, 2]]).to_df("a", "b")
    df2 = session.create_dataframe([[1, 2]]).to_df("a", "b")
    df3 = df1.to_df("a1", "b1")
    df4 = df3.join(df2)
    df5 = df4.rename(df1.a, "a2")
    assert df5.schema.names == ["A2", "B1", "A", "B"]
    Utils.check_answer(df5, [Row(1, 2, 1, 2)])


def test_rename_negative_test(session):
    df = session.create_dataframe([[1, 2]], schema=["a", "b"])

    # rename un-qualified column
    with pytest.raises(ValueError) as exec_info:
        df.rename(lit("c"), "c")
    assert f"Unable to rename column {lit('c')} because it doesn't exist." in str(
        exec_info
    )

    # rename non-existent column
    with pytest.raises(ValueError) as exec_info:
        df.rename("not_exist_column", "c")
    assert (
        'Unable to rename column "not_exist_column" because it doesn\\\'t exist.'
        in str(exec_info)
    )

    df2 = session.sql("select 1 as A, 2 as A, 3 as A")
    with pytest.raises(SnowparkColumnException) as col_exec_info:
        df2.rename("A", "B")
    assert (
        f'Unable to rename the column "A" as "B" because this DataFrame has 3 columns named "A".'
        in str(col_exec_info)
    )


def test_with_columns_keep_order(session):
    data = {
        "STARTTIME": 0,
        "ENDTIME": 10000,
        "START_STATION_ID": 2,
        "END_STATION_ID": 3,
    }
    df = session.create_dataframe([Row(1, data)]).to_df(["TRIPID", "V"])

    result = df.with_columns(
        ["starttime", "endtime", "duration", "start_station_id", "end_station_id"],
        [
            to_timestamp(get(col("V"), lit("STARTTIME"))),
            to_timestamp(get(col("V"), lit("ENDTIME"))),
            datediff("minute", col("STARTTIME"), col("ENDTIME")),
            as_integer(get(col("V"), lit("START_STATION_ID"))),
            as_integer(get(col("V"), lit("END_STATION_ID"))),
        ],
    )

    Utils.check_answer(
        [
            Row(
                TRIPID=1,
                V='{\n  "ENDTIME": 10000,\n  "END_STATION_ID": 3,\n  "STARTTIME": 0,\n  "START_STATION_ID": 2\n}',
                STARTTIME=datetime(1969, 12, 31, 16, 0, 0),
                ENDTIME=datetime(1969, 12, 31, 18, 46, 40),
                DURATION=166,
                START_STATION_ID=2,
                END_STATION_ID=3,
            )
        ],
        result,
    )


def test_with_columns_input_doesnt_match_each_other(session):
    df = session.create_dataframe([Row(1, 2, 3)]).to_df(["a", "b", "c"])
    with pytest.raises(ValueError) as ex_info:
        df.with_columns(["e", "f"], [lit(1)])
    assert (
        "The size of column names (2) is not equal to the size of columns (1)"
        in str(ex_info)
    )


def test_with_columns_replace_existing(session):
    df = session.create_dataframe([Row(1, 2, 3)]).to_df(["a", "b", "c"])
    replaced = df.with_columns(["b", "d"], [lit(5), lit(6)])
    Utils.check_answer(replaced, [Row(A=1, C=3, B=5, D=6)])

    with pytest.raises(ValueError) as ex_info:
        df.with_columns(["d", "b", "d"], [lit(4), lit(5), lit(6)])
    assert (
        "The same column name is used multiple times in the col_names parameter."
        in str(ex_info)
    )

    with pytest.raises(ValueError) as ex_info:
        df.with_columns(["d", "b", "D"], [lit(4), lit(5), lit(6)])
    assert (
        "The same column name is used multiple times in the col_names parameter."
        in str(ex_info)
    )


def test_drop_duplicates(session):
    df = session.create_dataframe(
        [[1, 1, 1, 1], [1, 1, 1, 2], [1, 1, 2, 3], [1, 2, 3, 4], [1, 2, 3, 4]],
        schema=["a", "b", "c", "d"],
    )
    Utils.check_answer(
        df.dropDuplicates(),
        [Row(1, 1, 1, 1), Row(1, 1, 1, 2), Row(1, 1, 2, 3), Row(1, 2, 3, 4)],
    )

    result1 = df.dropDuplicates(["a"])
    assert result1.count() == 1
    row1 = result1.collect()[0]
    # result is non-deterministic.
    assert row1 in [Row(1, 1, 1, 1), Row(1, 1, 1, 2), Row(1, 1, 2, 3), Row(1, 2, 3, 4)]

    result2 = df.dropDuplicates(["a", "b"])
    assert result2.count() == 2
    Utils.check_answer(result2.where(col("b") == lit(2)), [Row(1, 2, 3, 4)])
    row2 = result2.where(col("b") == lit(1)).collect()[0]
    # result is non-deterministic.
    assert row2 in [Row(1, 1, 1, 1), Row(1, 1, 1, 2), Row(1, 1, 2, 3)]

    result3 = df.dropDuplicates(["a", "b", "c"])
    assert result3.count() == 3
    Utils.check_answer(result3.where(col("c") == lit(2)), [Row(1, 1, 2, 3)])
    Utils.check_answer(result3.where(col("c") == lit(3)), [Row(1, 2, 3, 4)])
    row3 = result3.where(col("c") == lit(1)).collect()[0]
    # result is non-deterministic.
    assert row2 in [Row(1, 1, 1, 1), Row(1, 1, 1, 2)]

    Utils.check_answer(
        df.dropDuplicates(["a", "b", "c", "d"]),
        [Row(1, 1, 1, 1), Row(1, 1, 1, 2), Row(1, 1, 2, 3), Row(1, 2, 3, 4)],
    )
    Utils.check_answer(
        df.dropDuplicates("a", "b", "c", "d", "d"),
        [Row(1, 1, 1, 1), Row(1, 1, 1, 2), Row(1, 1, 2, 3), Row(1, 2, 3, 4)],
    )

    with pytest.raises(SnowparkColumnException) as exec_info:
        df.dropDuplicates("e").collect()
    assert "The DataFrame does not contain the column named e." in str(exec_info)


def test_consecutively_drop_duplicates(session):
    df = session.create_dataframe(
        [[1, 1, 1, 1], [1, 1, 1, 2], [1, 1, 2, 3], [1, 2, 3, 4], [1, 2, 3, 4]],
        schema=["a", "b", "c", "d"],
    )
    df1 = (
        df.drop_duplicates()
        .drop_duplicates(["a", "b", "c"])
        .drop_duplicates(["a", "b"])
        .drop_duplicates(["a"])
    )
    assert df1.count() == 1
    row1 = df1.collect()[0]
    # result is non-deterministic.
    assert row1 in [Row(1, 1, 1, 1), Row(1, 1, 1, 2), Row(1, 1, 2, 3), Row(1, 2, 3, 4)]


def test_dropna(session):
    Utils.check_answer(
        TestData.double3(session).na.drop(thresh=1, subset=["a"]),
        [Row(1.0, 1), Row(4.0, None)],
    )

    res = TestData.double3(session).na.drop(thresh=1, subset=["a", "b"]).collect()
    assert res[0] == Row(1.0, 1)
    assert math.isnan(res[1][0])
    assert res[1][1] == 2
    assert res[2] == Row(None, 3)
    assert res[3] == Row(4.0, None)

    assert TestData.double3(session).na.drop(thresh=0, subset=["a"]).count() == 6
    assert TestData.double3(session).na.drop(thresh=3, subset=["a", "b"]).count() == 0
    assert TestData.double3(session).na.drop(thresh=1, subset=[]).count() == 6

    # wrong column name
    with pytest.raises(SnowparkColumnException) as ex_info:
        TestData.double3(session).na.drop(thresh=1, subset=["c"])
    assert "The DataFrame does not contain the column named" in str(ex_info)


def test_fillna(session):
    Utils.check_answer(
        TestData.null_data3(session).na.fill(
            {"flo": 12.3, "int": 11, "boo": False, "str": "f"}
        ),
        [
            Row(1.0, 1, True, "a"),
            Row(12.3, 2, False, "b"),
            Row(12.3, 3, False, "f"),
            Row(4.0, 11, False, "d"),
            Row(12.3, 11, False, "f"),
            Row(12.3, 11, False, "f"),
        ],
        sort=False,
    )
    Utils.check_answer(
        TestData.null_data3(session).na.fill(
            {"flo": 22.3, "int": 22, "boo": False, "str": "f"}
        ),
        [
            Row(1.0, 1, True, "a"),
            Row(22.3, 2, False, "b"),
            Row(22.3, 3, False, "f"),
            Row(4.0, 22, False, "d"),
            Row(22.3, 22, False, "f"),
            Row(22.3, 22, False, "f"),
        ],
        sort=False,
    )
    # wrong type
    Utils.check_answer(
        TestData.null_data3(session).na.fill(
            {"flo": 12.3, "int": "11", "boo": False, "str": 1}
        ),
        [
            Row(1.0, 1, True, "a"),
            Row(12.3, 2, False, "b"),
            Row(12.3, 3, False, None),
            Row(4.0, None, False, "d"),
            Row(12.3, None, False, None),
            Row(12.3, None, False, None),
        ],
        sort=False,
    )
    # wrong column name
    with pytest.raises(SnowparkColumnException) as ex_info:
        TestData.null_data3(session).na.fill({"wrong": 11})
    assert "The DataFrame does not contain the column named" in str(ex_info)


def test_replace(session):
    res = (
        TestData.null_data3(session)
        .na.replace({2: 300, 1: 200}, subset=["flo"])
        .collect()
    )
    assert res[0] == Row(200.0, 1, True, "a")
    assert math.isnan(res[1][0])
    assert res[1][1:] == Row(2, None, "b")
    assert res[2:-1] == [
        Row(None, 3, False, None),
        Row(4.0, None, None, "d"),
        Row(None, None, None, None),
    ]
    assert math.isnan(res[-1][0])
    assert res[-1][1:] == Row(None, None, None)

    # replace null
    res = (
        TestData.null_data3(session).na.replace({None: True}, subset=["boo"]).collect()
    )
    assert res[0] == Row(1.0, 1, True, "a")
    assert math.isnan(res[1][0])
    assert res[1][1:] == Row(2, True, "b")
    assert res[2:-1] == [
        Row(None, 3, False, None),
        Row(4.0, None, True, "d"),
        Row(None, None, True, None),
    ]
    assert math.isnan(res[-1][0])
    assert res[-1][1:] == Row(None, True, None)

    # replace NaN
    Utils.check_answer(
        TestData.null_data3(session).na.replace({float("nan"): 11}, subset=["flo"]),
        [
            Row(1.0, 1, True, "a"),
            Row(11, 2, None, "b"),
            Row(None, 3, False, None),
            Row(4.0, None, None, "d"),
            Row(None, None, None, None),
            Row(11, None, None, None),
        ],
        sort=False,
    )

    # incompatible type (skip that replacement and do nothing)
    res = (
        TestData.null_data3(session).na.replace({None: "aa"}, subset=["flo"]).collect()
    )
    assert res[0] == Row(1.0, 1, True, "a")
    assert math.isnan(res[1][0])
    assert res[1][1:] == Row(2, None, "b")
    assert res[2:-1] == [
        Row(None, 3, False, None),
        Row(4.0, None, None, "d"),
        Row(None, None, None, None),
    ]
    assert math.isnan(res[-1][0])
    assert res[-1][1:] == Row(None, None, None)

    # replace NaN with None
    Utils.check_answer(
        TestData.null_data3(session).na.replace({float("nan"): None}, subset=["flo"]),
        [
            Row(1.0, 1, True, "a"),
            Row(None, 2, None, "b"),
            Row(None, 3, False, None),
            Row(4.0, None, None, "d"),
            Row(None, None, None, None),
            Row(None, None, None, None),
        ],
        sort=False,
    )


def test_explain(session):
    df = TestData.column_has_special_char(session)
    df.explain()
    explain_string = df._explain_string()
    assert "Query List" in explain_string
    assert df._plan.queries[0].sql.strip() in explain_string
    assert "Logical Execution Plan" in explain_string

    # can't analyze multiple queries
    explain_string = session.create_dataframe([1] * 20000)._explain_string()
    assert "CREATE" in explain_string
    assert "\n---\n" in explain_string
    assert "SELECT" in explain_string
    assert "Logical Execution Plan" not in explain_string


def test_to_local_iterator(session):
    df = session.create_dataframe([1, 2, 3]).toDF("a")
    iterator = df.to_local_iterator()
    assert isinstance(iterator, Iterator)

    index = 0
    array = df.collect()
    for row in iterator:
        assert row == array[index]
        index += 1

    for row in df.to_local_iterator():
        assert row == array[0]
        break
