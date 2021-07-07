#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import uuid
from decimal import Decimal
from typing import NamedTuple, Optional

from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.internal.analyzer.analyzer_package import AnalyzerPackage
from snowflake.snowpark.session import Session


class Utils:
    @staticmethod
    def random_name() -> str:
        return "SN_TEST_OBJECT_{}".format(str(uuid.uuid4()).replace("-", "_")).upper()

    @staticmethod
    def create_table(session: "Session", name: str, schema: str):
        session._run_query(f"create or replace table {name} ({schema})")

    @staticmethod
    def create_stage(session: "Session", name: str, is_temporary: bool = True):
        session._run_query(
            f"create or replace {'temporary' if is_temporary else ''} stage {name}"
        )

    @staticmethod
    def drop_stage(session: "Session", name: str):
        session._run_query(f"drop stage if exists {AnalyzerPackage.quote_name(name)}")

    @staticmethod
    def drop_table(session: "Session", name: str):
        session._run_query(f"drop table if exists {AnalyzerPackage.quote_name(name)}")

    @staticmethod
    def drop_view(session: "Session", name: str):
        session._run_query(f"drop view if exists {AnalyzerPackage.quote_name(name)}")

    @staticmethod
    def equals_ignore_case(a: str, b: str) -> bool:
        return a.lower() == b.lower()


class TestData:
    Data = NamedTuple("Data", [("num", int), ("bool", bool), ("str", str)])
    Data2 = NamedTuple("Data2", [("a", int), ("b", int)])
    Data3 = NamedTuple("Data3", [("a", int), ("b", Optional[int])])
    Data4 = NamedTuple("Data4", [("key", int), ("value", str)])
    LowerCaseData = NamedTuple("LowerCaseData", [("n", int), ("l", str)])
    UpperCaseData = NamedTuple("UpperCaseData", [("N", int), ("L", str)])
    NullInt = NamedTuple("NullInts", [("a", Optional[int])])
    Number2 = NamedTuple("Number2", [("x", int), ("y", int), ("z", int)])

    @classmethod
    def test_data1(cls, session: "Session") -> DataFrame:
        return session.createDataFrame(
            [cls.Data(1, True, "a"), cls.Data(2, False, "b")]
        )

    @classmethod
    def test_data2(cls, session: "Session") -> DataFrame:
        return session.createDataFrame(
            [
                cls.Data2(1, 1),
                cls.Data2(1, 2),
                cls.Data2(2, 1),
                cls.Data2(2, 2),
                cls.Data2(3, 1),
                cls.Data2(3, 2),
            ]
        )

    @classmethod
    def test_data3(cls, session: "Session") -> DataFrame:
        return session.createDataFrame([cls.Data3(1, None), cls.Data3(2, 2)])

    @classmethod
    def test_data4(cls, session: "Session") -> DataFrame:
        return session.createDataFrame([cls.Data4(i, str(i)) for i in range(1, 101)])

    @classmethod
    def lower_case_data(cls, session: "Session") -> DataFrame:
        return session.createDataFrame([[1, "a"], [2, "b"], [3, "c"], [4, "d"]])

    @classmethod
    def upper_case_data(cls, session: "Session") -> DataFrame:
        return session.createDataFrame(
            [[1, "A"], [2, "B"], [3, "C"], [4, "D"], [5, "E"], [6, "F"]]
        )

    @classmethod
    def null_ints(cls, session: "Session") -> DataFrame:
        return session.createDataFrame(
            [cls.NullInt(1), cls.NullInt(2), cls.NullInt(3), cls.NullInt(None)]
        )

    @classmethod
    def all_nulls(cls, session: "Session") -> DataFrame:
        return session.createDataFrame(
            [cls.NullInt(None), cls.NullInt(None), cls.NullInt(None), cls.NullInt(None)]
        )

    @classmethod
    def null_data1(cls, session: "Session") -> DataFrame:
        return session.sql("select * from values(null),(2),(1),(3),(null) as T(a)")

    @classmethod
    def integer1(cls, session: "Session") -> DataFrame:
        return session.sql("select * from values(1),(2),(3) as T(a)")

    @classmethod
    def double1(cls, session: "Session") -> DataFrame:
        return session.sql("select * from values(1.111),(2.222),(3.333) as T(a)")

    @classmethod
    def double2(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select * from values(0.1, 0.5),(0.2, 0.6),(0.3, 0.7) as T(a,b)"
        )

    @classmethod
    def duplicated_numbers(cls, session: "Session") -> DataFrame:
        return session.sql("select * from values(3),(2),(1),(3),(2) as T(a)")

    @classmethod
    def string3(cls, session: "Session") -> DataFrame:
        return session.sql("select * from values('  abcba  '), (' a12321a   ') as T(a)")

    @classmethod
    def string4(cls, session: "Session") -> DataFrame:
        return session.sql("select * from values('apple'),('banana'),('peach') as T(a)")

    @classmethod
    def array2(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select array_construct(a,b,c) as arr1, d, e, f from"
            " values(1,2,3,2,'e1','[{a:1}]'),(6,7,8,1,'e2','[{a:1},{b:2}]') as T(a,b,c,d,e,f)"
        )

    @classmethod
    def variant2(cls, session: "Session") -> DataFrame:
        return session.sql(
            """
            select parse_json(column1) as src
            from values
            ('{
                "date with '' and ." : "2017-04-28",
                "salesperson" : {
                  "id": "55",
                  "name": "Frank Beasley"
                },
                "customer" : [
                  {"name": "Joyce Ridgely", "phone": "16504378889", "address": "San Francisco, CA"}
                ],
                "vehicle" : [
                  {"make": "Honda", "extras":["ext warranty", "paint protection"]}
                ]
            }')
            """
        )

    @classmethod
    def null_json1(cls, session: "Session") -> DataFrame:
        return session.sql(
            'select parse_json(column1) as v from values (\'{"a": null}\'), (\'{"a": "foo"}\'),'
            " (null)"
        )

    @classmethod
    def decimal_data(cls, session: "Session") -> DataFrame:
        return session.createDataFrame(
            [
                [Decimal(1), Decimal(1)],
                [Decimal(1), Decimal(2)],
                [Decimal(2), Decimal(1)],
                [Decimal(2), Decimal(2)],
                [Decimal(3), Decimal(1)],
                [Decimal(3), Decimal(2)],
            ]
        ).toDF(["a", "b"])

    @classmethod
    def xyz(cls, session: "Session") -> DataFrame:
        return session.createDataFrame(
            [
                cls.Number2(1, 2, 1),
                cls.Number2(1, 2, 3),
                cls.Number2(2, 1, 10),
                cls.Number2(2, 2, 1),
                cls.Number2(2, 2, 3),
            ]
        )

    @classmethod
    def column_has_special_char(cls, session: "Session") -> DataFrame:
        return session.createDataFrame([[1, 2], [3, 4]]).toDF(['"col %"', '"col *"'])
