#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import datetime

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import col, lit, max, table_function
from snowflake.snowpark.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from tests.integ.scala.test_dataframe_suite import SAMPLING_DEVIATION
from tests.utils import IS_IN_STORED_PROC, TestFiles, Utils

pytestmark = pytest.mark.xfail(
    condition="config.getvalue('local_testing_mode')",
    raises=NotImplementedError,
    strict=True,
)


def test_basic_query(session):
    df1 = session.sql("select * from values (?, ?), (?, ?)", params=[1, "a", 2, "b"])
    Utils.check_answer(df1, [Row(1, "a"), Row(2, "b")])

    df2 = session.sql(
        "create or replace table identifier(?) (id int)",
        params=["bind_variable_test_table"],
    )
    Utils.check_answer(
        df2, [Row(status="Table BIND_VARIABLE_TEST_TABLE successfully created.")]
    )
    Utils.check_answer(
        df2.select("*"),
        [Row(status="Table BIND_VARIABLE_TEST_TABLE successfully created.")],
    )


def test_statement_params(session):
    df = session.sql(
        "select column1::DATE from values (?), (?)", params=["01-01-1970", "12-31-2000"]
    )
    statement_params = {
        "DATE_INPUT_FORMAT": "MM-DD-YYYY",
        "SF_PARTNER": "FAKE_PARTNER",
    }
    Utils.check_answer(
        df.collect(statement_params=statement_params),
        [Row(datetime.date(1970, 1, 1)), Row(datetime.date(2000, 12, 31))],
    )


@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="Async job is not supported in stored proc today",
)
def test_async(session, resources_path):
    # Test single query
    df = session.sql(
        "select column1::INT as a, column2 as b from values (?, ?), (?, ?)",
        params=[1, "a", 2, "b"],
    )
    Utils.check_answer(df.collect(block=False).result(), [Row(1, "a"), Row(2, "b")])

    # Test multi queries
    user_schema = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", StringType()),
            StructField("c", DoubleType()),
        ]
    )
    test_files = TestFiles(resources_path)
    stage_name = Utils.random_stage_name()
    Utils.create_stage(session, stage_name, is_temporary=True)
    Utils.upload_to_stage(
        session, "@" + stage_name, test_files.test_file_csv, compress=False
    )
    df_read = session.read.schema(user_schema).csv(
        f"@{stage_name}/{test_files.test_file_csv}"
    )
    with pytest.raises(
        NotImplementedError,
        match="Async multi-query dataframe using bind variable is not supported yet",
    ):
        df.join(df_read, on="a").collect_nowait().result()


def test_to_local_iterator(session):
    df = session.sql("select * from values (?, ?), (?, ?)", params=[1, "a", 2, "b"])
    Utils.check_answer(list(df.to_local_iterator()), [Row(1, "a"), Row(2, "b")])


def test_to_pandas(session):
    pd_df = session.sql(
        "select * from values (?, ?), (?, ?)", params=[1, "a", 2, "b"]
    ).to_pandas()
    Utils.check_answer(session.create_dataframe(pd_df), [Row(1, "a"), Row(2, "b")])


def test_select(session):
    df = session.sql("select * from values (?, ?), (?, ?)", params=[1, "a", 2, "b"])
    Utils.check_answer(df.select("column1"), [Row(1), Row(2)])
    Utils.check_answer(df.select("column1", "column1"), [Row(1, 1), Row(2, 2)])
    Utils.check_answer(df.select("$1"), [Row(1), Row(2)])
    Utils.check_answer(df.select("*"), [Row(1, "a"), Row(2, "b")])
    Utils.check_answer(df.select(df["*"]), [Row(1, "a"), Row(2, "b")])
    Utils.check_answer(df.filter("column1 > 1").select("column2"), [Row("b")])


def test_filter(session):
    df = session.sql("select * from values (?, ?), (?, ?)", params=[1, "a", 2, "b"])
    Utils.check_answer(df.filter("column1 > 1"), [Row(2, "b")])

    # Test no flattening case
    Utils.check_answer(df.select("$1", "$2").filter("$1 > 1"), [Row(2, "b")])


def test_sort(session):
    df = session.sql("select * from values (?, ?), (?, ?)", params=[1, "a", 2, "b"])
    Utils.check_answer(df.sort("column1"), [Row(1, "a"), Row(2, "b")], sort=False)
    Utils.check_answer(
        df.sort("column1", ascending=False), [Row(2, "b"), Row(1, "a")], sort=False
    )

    # Test no flattening case
    Utils.check_answer(
        df.select("$1", "$2").sort("$1"), [Row(1, "a"), Row(2, "b")], sort=False
    )
    Utils.check_answer(
        df.select("$1", "$2").sort("$1", ascending=False),
        [Row(2, "b"), Row(1, "a")],
        sort=False,
    )


def test_set_operation(session):
    # Test simple set operations
    df1 = session.sql("select * from values (?, ?), (?, ?)", params=[1, "a", 2, "b"])
    df2 = session.sql("select * from values (?, ?), (?, ?)", params=[1, "c", 3, "d"])
    df3 = session.sql("select * from values (?, ?), (?, ?)", params=[1, "a", 4, "e"])
    Utils.check_answer(
        df1.union(df2), [Row(1, "a"), Row(1, "c"), Row(2, "b"), Row(3, "d")]
    )
    Utils.check_answer(
        df1.union_all(df3), [Row(1, "a"), Row(1, "a"), Row(2, "b"), Row(4, "e")]
    )
    Utils.check_answer(df1.intersect(df3), [Row(1, "a")])
    Utils.check_answer(df1.except_(df3), [Row(2, "b")])

    # Test nested set operations
    Utils.check_answer(
        df1.union(df2).union(df1), [Row(1, "a"), Row(1, "c"), Row(2, "b"), Row(3, "d")]
    )
    Utils.check_answer(df1.union(df2).intersect(df3), [Row(1, "a")])
    Utils.check_answer(
        df1.intersect(df3).union(df2), [Row(1, "a"), Row(1, "c"), Row(3, "d")]
    )


def test_limit(session):
    df = session.sql("select * from values (?, ?), (?, ?)", params=[1, "a", 2, "b"])
    Utils.check_answer(df.sort("column1").limit(1), [Row(1, "a")])


def test_table_function(session):
    df = session.sql(
        "select ? as name, ? as addresses",
        params=["James", "address1 address2 address3"],
    )
    split_to_table = table_function("split_to_table")
    joined_df = df.join_table_function(
        split_to_table(col("addresses"), lit(" ")).alias("seq", "idx", "val")
    )
    Utils.check_answer(
        joined_df,
        [
            Row(
                name="James",
                addresses="address1 address2 address3",
                seq=1,
                idx=1,
                val="address1",
            ),
            Row(
                name="James",
                addresses="address1 address2 address3",
                seq=1,
                idx=2,
                val="address2",
            ),
            Row(
                name="James",
                addresses="address1 address2 address3",
                seq=1,
                idx=3,
                val="address3",
            ),
        ],
    )


def test_in(session):
    df = session.sql(
        "select * from values (?, ?), (?, ?), (?, ?)", params=[1, "a", 2, "b", 3, "c"]
    )
    Utils.check_answer(
        df.filter(df["column1"].in_(lit(1), lit(2))), [Row(1, "a"), Row(2, "b")]
    )

    df_for_in = session.create_dataframe([[1], [2]], schema=["col1"])
    Utils.check_answer(
        df.filter(df["column1"].in_(df_for_in)), [Row(1, "a"), Row(2, "b")]
    )
    Utils.check_answer(
        df.select(df["column1"], df["column1"].in_(lit(1), lit(2))),
        [Row(1, True), Row(2, True), Row(3, False)],
    )


def test_cache_result(session):
    df = session.sql(
        "select column1::INT, column2 from values (?, ?), (?, ?)",
        params=[1, "a", 2, "b"],
    )
    cached_df = df.cache_result()
    Utils.check_answer(cached_df, [Row(1, "a"), Row(2, "b")])


def test_join(session):
    df1 = session.sql("select * from values (?, ?), (?, ?)", params=[1, "a", 2, "b"])
    df2 = session.sql("select * from values (?, ?), (?, ?)", params=[1, "c", 3, "d"])
    Utils.check_answer(df1.join(df2, on="column1"), [Row(1, "a", "c")])
    Utils.check_answer(df2.join(df1, on="column1"), [Row(1, "c", "a")])
    Utils.check_answer(
        df1.join(df2, on="column1", lsuffix="left", rsuffix="right"),
        [Row(column1=1, column2_left="a", column2_right="c")],
    )


def test_aggregation(session):
    df = session.sql(
        "select * from values (?, ?), (?, ?), (?, ?), (?, ?)",
        params=[1, "a", 2, "b", 1, "c", 2, "d"],
    )
    Utils.check_answer(
        df.group_by("column1").agg(max(col("column2"))), [Row(1, "c"), Row(2, "d")]
    )
    Utils.check_answer(
        df.rollup(col("column1")).agg(max(col("column2"))),
        [Row(None, "d"), Row(1, "c"), Row(2, "d")],
    )
    Utils.check_answer(
        df.cube(col("column1")).agg(max(col("column2"))),
        [Row(None, "d"), Row(1, "c"), Row(2, "d")],
    )
    Utils.check_answer(
        df.pivot(col("column1"), [1, 2]).agg(max(col("column2"))), [Row("c", "d")]
    )


def test_pivot_unpivot(session):
    df1 = session.sql(
        "select column1::INT as empid, column2::INT as amount, column3 as month from values (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?)",
        params=[1, 10000, "JAN", 1, 400, "JAN", 1, 5000, "FEB", 2, 3000, "FEB"],
    )
    Utils.check_answer(
        df1.pivot(col("month"), ["JAN", "FEB"]).sum(col("amount")),
        [Row(1, 10400, 5000), Row(2, None, 3000)],
    )

    df2 = session.sql(
        "select column1::INT as empid, column2 as dept, column3::INT as jan, column4::INT as feb from values (?, ?, ?, ?), (?, ?, ?, ?)",
        params=[1, "electronics", 100, 200, 2, "clothes", 100, 300],
    )
    Utils.check_answer(
        df2.unpivot("sales", "month", ["jan", "feb"]),
        [
            Row(1, "electronics", "JAN", 100),
            Row(1, "electronics", "FEB", 200),
            Row(2, "clothes", "JAN", 100),
            Row(2, "clothes", "FEB", 300),
        ],
    )


def test_sample(session):
    row_count = 10000
    df = session.sql(
        f"select * from values {', '.join(['(?)'] * row_count)}",
        params=list(range(row_count)),
    )
    assert df.sample(n=row_count // 10).count() == row_count // 10
    assert (
        abs(df.sample(frac=0.5).count() - row_count // 2)
        < row_count // 2 * SAMPLING_DEVIATION
    )


def test_write(session):
    table_name = Utils.random_table_name()
    try:
        df = session.sql(
            "select column1::INT, column2 from values (?, ?), (?, ?)",
            params=[1, "a", 2, "b"],
        )
        df.write.save_as_table(table_name, mode="append")
        Utils.check_answer(session.table(table_name), [Row(1, "a"), Row(2, "b")])
        df.write.save_as_table(table_name, mode="append")
        Utils.check_answer(
            session.table(table_name),
            [Row(1, "a"), Row(1, "a"), Row(2, "b"), Row(2, "b")],
        )

        df.write.save_as_table(table_name, mode="overwrite")
        Utils.check_answer(session.table(table_name), [Row(1, "a"), Row(2, "b")])
        df.write.save_as_table(table_name, mode="ignore")
        Utils.check_answer(session.table(table_name), [Row(1, "a"), Row(2, "b")])
        with pytest.raises(SnowparkSQLException, match="already exists"):
            df.write.save_as_table(table_name, mode="errorifexists")
            Utils.check_answer(session.table(table_name), [Row(1, "a"), Row(2, "b")])
    finally:
        session.sql(f"drop table if exists {table_name}")


def test_view(session):
    df = session.sql(
        "select * from values (?, ?), (?, ?)",
        params=[1, "a", 2, "b"],
    )
    view_name = Utils.random_view_name()
    with pytest.raises(
        SnowparkSQLException,
        match=".*Bind variables not allowed in view and UDF definitions.*",
    ):
        df.create_or_replace_view(view_name)

    with pytest.raises(
        SnowparkSQLException,
        match=".*Bind variables not allowed in view and UDF definitions.*",
    ):
        df.create_or_replace_temp_view(view_name)


def test_first(session):
    df = session.sql(
        "select * from values (?, ?), (?, ?), (?, ?), (?, ?)",
        params=[1, "a", 2, "b", 3, "c", 4, "d"],
    )
    Utils.check_answer(df.sort("column1").first(), [Row(1, "a")])
    Utils.check_answer(df.sort("column1").first(block=False).result(), [Row(1, "a")])
    Utils.check_answer(
        df.sort("column1").first(3), [Row(1, "a"), Row(2, "b"), Row(3, "c")]
    )
    Utils.check_answer(
        df.sort("column1").first(-1),
        [Row(1, "a"), Row(2, "b"), Row(3, "c"), Row(4, "d")],
    )


def test_na(session):
    df = session.sql(
        "select column1::INT as column1, column2 from values (?, ?), (?, ?), (NULL, NULL)",
        params=[1, "a", 2, "b"],
    )
    Utils.check_answer(df.na.drop(), [Row(1, "a"), Row(2, "b")])
    Utils.check_answer(
        df.na.fill({"column1": 3, "column2": "c"}),
        [Row(1, "a"), Row(2, "b"), Row(3, "c")],
    )
    Utils.check_answer(
        df.na.replace({1: 3}), [Row(3, "a"), Row(2, "b"), Row(None, None)]
    )


def test_describe(session):
    df = session.sql(
        "select column1::INT as column1, column2 from values (?, ?), (?, ?)",
        params=[1, "a", 2, "b"],
    )
    Utils.check_answer(
        df.describe(),
        [
            Row("count", 2, "2"),
            Row("mean", 1.5, None),
            Row("stddev", 0.7071067811865476, None),
            Row("min", 1, "a"),
            Row("max", 2, "b"),
        ],
    )


def test_column_rename(session):
    df = session.sql(
        "select * from values (?, ?), (?, ?)",
        params=[1, "a", 2, "b"],
    )
    Utils.check_answer(
        df.with_column_renamed("column1", "column3"),
        [Row(column3=1, column2="a"), Row(colum3=2, colum2="b")],
    )


def test_random_split(session):
    df = session.sql(
        "select * from values (?, ?), (?, ?), (?, ?), (?, ?)",
        params=[1, "a", 2, "b", 3, "c", 4, "d"],
    )
    weights = [0.2, 0.8]
    parts = df.random_split(weights)
    assert len(parts) == len(weights)
    part_counts = [p.count() for p in parts]
    assert sum(part_counts) == 4


def test_explain(session):
    df = session.sql(
        "select * from values (?, ?), (?, ?)",
        params=[1, "a", 2, "b"],
    )
    df.explain()


@pytest.mark.skipif(
    condition="config.getvalue('local_testing_mode')",
    reason="Testing stored proc only feature",
)
def test_stored_proc_not_supported(session):
    import snowflake.snowpark._internal.utils as internal_utils

    original_platform = internal_utils.PLATFORM
    internal_utils.PLATFORM = "XP"
    try:
        with pytest.raises(
            NotImplementedError,
            match=".*Bind variable in stored procedure is not supported yet.*",
        ):
            session.sql(
                "select * from values (?, ?), (?, ?)",
                params=[1, "a", 2, "b"],
            )
    finally:
        internal_utils.PLATFORM = original_platform
