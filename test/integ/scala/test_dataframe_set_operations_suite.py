#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import random
from datetime import date, datetime
from decimal import Decimal
from test.utils import TestData, Utils
from typing import List

from snowflake.snowpark.column import Column
from snowflake.snowpark.functions import col, lit, min, sum
from snowflake.snowpark.row import Row
from snowflake.snowpark.types.sf_types import IntegerType


def test_union_with_filters(session_cnx):
    """Tests union queries with a filter added"""
    with session_cnx() as session:

        def check(new_col: Column, cfilter: Column, result: List[Row]):
            df1 = (
                session.createDataFrame([[1, 1]])
                .toDF(["a", "b"])
                .withColumn("c", new_col)
            )
            df2 = df1.union(df1).withColumn("d", lit(100)).filter(cfilter)

            Utils.check_answer(df2, result)

        check(
            lit(None).cast(IntegerType()),
            col("c").is_null(),
            [Row([1, 1, None, 100]), Row([1, 1, None, 100])],
        )
        check(lit(None).cast(IntegerType()), col("c").is_not_null(), list())
        check(lit(2).cast(IntegerType()), col("c").is_null(), list())
        check(
            lit(2).cast(IntegerType()),
            col("c").is_not_null(),
            [Row([1, 1, 2, 100]), Row([1, 1, 2, 100])],
        )
        check(
            lit(2).cast(IntegerType()),
            col("c") == 2,
            [Row([1, 1, 2, 100]), Row([1, 1, 2, 100])],
        )
        check(lit(2).cast(IntegerType()), col("c") != 2, list())


def test_union_all(session_cnx):
    with session_cnx() as session:
        td4 = TestData.test_data4(session)
        union_df = td4.union(td4).union(td4).union(td4).union(td4)

        res = union_df.agg([min(col("key")), sum(col("key"))]).collect()
        assert res == [Row([1, 25250])]

        # unionAll is an alias of union
        union_all_df = td4.unionAll(td4).unionAll(td4).unionAll(td4).unionAll(td4)
        res1 = union_df.collect()
        res2 = union_all_df.collect()
        # don't sort
        assert res1 == res2


def test_intersect_nullability(session_cnx):
    with session_cnx() as session:
        non_nullable_ints = session.createDataFrame([[1], [3]]).toDF("a")
        null_ints = TestData.null_ints(session)

        assert all(not i.nullable for i in non_nullable_ints.schema.fields)

        assert all(i.nullable for i in null_ints.schema.fields)

        df1 = non_nullable_ints.intersect(null_ints)
        res = df1.collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(3)]
        assert all(not i.nullable for i in df1.schema.fields)

        df2 = null_ints.intersect(non_nullable_ints)
        res = df2.collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(3)]
        assert all(i.nullable for i in df2.schema.fields)

        df3 = null_ints.intersect(null_ints)
        res = df3.collect()
        res.sort(key=lambda x: x[0] or 0)
        assert res == [Row(None), Row(1), Row(2), Row(3)]
        assert all(i.nullable for i in df3.schema.fields)

        df4 = non_nullable_ints.intersect(non_nullable_ints)
        res = df4.collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row(1), Row(3)]
        assert all(not i.nullable for i in df4.schema.fields)


def test_spark_17123_performing_set_ops_on_non_native_types(session_cnx):
    with session_cnx() as session:
        dates = session.createDataFrame(
            [
                [date(1, 1, 1), Decimal(1), datetime(1, 1, 1, microsecond=2000)],
                [date(1, 1, 3), Decimal(4), datetime(1, 1, 1, microsecond=5000)],
            ]
        ).toDF("date", "decimal", "timestamp")

        widen_typed_rows = session.createDataFrame(
            [
                [
                    datetime(1, 1, 1, microsecond=5000),
                    Decimal(10.5),
                    datetime(1, 1, 1, microsecond=10000),
                ]
            ]
        ).toDF("date", "decimal", "timestamp")

        dates.union(widen_typed_rows).collect()
        dates.intersect(widen_typed_rows).collect()
        dates.except_(widen_typed_rows).collect()


def test_intersect(session_cnx):
    with session_cnx() as session:
        lcd = TestData.lower_case_data(session)
        res = lcd.intersect(lcd).collect()
        res.sort(key=lambda x: x[0])
        assert res == [Row([1, "a"]), Row([2, "b"]), Row([3, "c"]), Row([4, "d"])]

        assert lcd.intersect(TestData.upper_case_data(session)).collect() == []

        # check null equality
        df = TestData.null_ints(session).intersect(TestData.null_ints(session))
        res = df.collect()
        res.sort(key=lambda x: x[0] or 0)
        assert res == [Row([None]), Row([1]), Row([2]), Row([3])]

        # check if values are de-duplicated
        df = TestData.all_nulls(session).intersect(TestData.all_nulls(session))
        assert df.collect() == [Row(None)]

        # check if values are de-duplicated
        df = session.createDataFrame(
            [("id1", 1), ("id1", 1), ("id", 1), ("id1", 2)]
        ).toDF("id", "value")
        res = df.intersect(df).collect()
        res.sort(key=lambda x: (x[0], x[1]))
        assert res == [Row(["id", 1]), Row(["id1", 1]), Row(["id1", 2])]


def test_project_should_not_be_pushed_down_through_intersect_or_except(session_cnx):
    with session_cnx() as session:
        df1 = session.createDataFrame([[i] for i in range(1, 101)]).toDF("i")
        df2 = session.createDataFrame([[i] for i in range(1, 31)]).toDF("i")

        assert df1.intersect(df2).count() == 30
        assert df1.except_(df2).count() == 70


def test_mix_set_operator(session_cnx):
    with session_cnx() as session:
        df1 = session.createDataFrame([1]).toDF("a")
        df2 = session.createDataFrame([2]).toDF("a")
        df3 = session.createDataFrame([3]).toDF("a")

        res = df1.union(df2).intersect(df2.union(df3)).collect()
        expected = df2.collect()
        assert res == expected

        res1 = df1.union(df2).intersect(df2.union(df3)).union(df3).collect()
        res2 = df2.union(df3).collect()
        assert res1 == res2

        res = df1.union(df2).except_(df2.union(df3).intersect(df1.union(df2))).collect()
        expected = df1.collect()
        assert res == expected


def test_except(session_cnx):
    with session_cnx() as session:
        lower_case_data = TestData.lower_case_data(session)
        upper_case_data = TestData.upper_case_data(session)
        Utils.check_answer(
            lower_case_data.except_(upper_case_data),
            [
                Row((1, "a")),
                Row((2, "b")),
                Row((3, "c")),
                Row((4, "d")),
            ],
        )
        Utils.check_answer(lower_case_data.except_(lower_case_data), [])
        Utils.check_answer(upper_case_data.except_(upper_case_data), [])

        # check null equality
        null_ints = TestData.null_ints(session)
        Utils.check_answer(
            null_ints.except_(null_ints.filter(lit(0) == 1)), null_ints.collect()
        )
        Utils.check_answer(null_ints.except_(null_ints), [])

        # check all null equality and de-duplication
        all_nulls = TestData.all_nulls(session)
        Utils.check_answer(
            all_nulls.except_(all_nulls.filter(lit(0) == 1)), [Row(None)]
        )
        Utils.check_answer(all_nulls.except_(all_nulls), [])

        # check if values are de-duplicated
        df = session.createDataFrame(
            (("id1", 1), ("id1", 1), ("id", 1), ("id1", 2))
        ).toDF("id", "value")
        Utils.check_answer(
            df.except_(df.filter(lit(0) == 1)),
            [Row(("id", 1)), Row(("id1", 1)), Row(("id1", 2))],
        )

        # check if the empty set on the left side works
        Utils.check_answer(all_nulls.filter(lit(0) == 1).except_(all_nulls), [])


def test_except_between_two_projects_without_references_used_in_filter(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame(((1, 2, 4), (1, 3, 5), (2, 2, 3), (2, 4, 5))).toDF(
            "a", "b", "c"
        )
        df1 = df.filter(Column("a") == 1)
        df2 = df.filter(Column("a") == 2)
        Utils.check_answer(df1.select("b").except_(df2.select("b")), Row(3))
        Utils.check_answer(df1.select("b").except_(df2.select("c")), Row(2))


def test_nondeterministic_expressions_should_not_be_pushed_down(session_cnx):
    with session_cnx() as session:
        df1 = session.createDataFrame([(i,) for i in range(1, 21)]).toDF("i")
        df2 = session.createDataFrame([(i,) for i in range(1, 11)]).toDF("i")

        # Checks that the random filter is not pushed down and
        # so will return the same result when run again

        union = df1.union(df2).filter(Column("i") < lit(random.randint(1, 7)))
        Utils.check_answer(union.collect(), union.collect())

        intersect = df1.intersect(df2).filter(Column("i") < lit(random.randint(1, 7)))
        Utils.check_answer(intersect.collect(), intersect.collect())

        except_ = df1.except_(df2).filter(Column("i") < lit(random.randint(1, 7)))
        Utils.check_answer(except_.collect(), except_.collect())


def test_except_nullability(session_cnx):
    with session_cnx() as session:
        non_nullable_ints = session.createDataFrame(((11,), (3,))).toDF("a")
        for attribute in non_nullable_ints.schema.to_attributes():
            assert not attribute.nullable

        null_ints = TestData.null_ints(session)
        df1 = non_nullable_ints.except_(null_ints)
        Utils.check_answer(df1, Row(11))
        for attribute in df1.schema.to_attributes():
            assert not attribute.nullable

        df2 = null_ints.except_(non_nullable_ints)
        Utils.check_answer(df2, [Row(1), Row(2), Row(None)])
        for attribute in df2.schema.to_attributes():
            assert attribute.nullable

        df3 = null_ints.except_(null_ints)
        Utils.check_answer(df3, [])
        for attribute in df3.schema.to_attributes():
            assert attribute.nullable

        df4 = non_nullable_ints.except_(non_nullable_ints)
        Utils.check_answer(df4, [])
        for attribute in df4.schema.to_attributes():
            assert not attribute.nullable


def test_except_distinct_sql_compliance(session_cnx):
    with session_cnx() as session:
        df_left = session.createDataFrame([(1,), (2,), (2,), (3,), (3,), (4,)]).toDF(
            "id"
        )
        df_right = session.createDataFrame([(1,), (3,)]).toDF("id")
        Utils.check_answer(df_left.except_(df_right), [Row(2), Row(4)])
