#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import uuid
from decimal import Decimal
from typing import NamedTuple, Optional

from src.snowflake.snowpark.dataframe import DataFrame
from src.snowflake.snowpark.session import Session


class Utils:
    @staticmethod
    def random_name() -> str:
        return "SN_TEST_OBJECT_{}".format(str(uuid.uuid4()).replace("-", "_")).upper()

    @staticmethod
    def create_table(session: "Session", name: str, schema: str):
        session._run_query(f"create or replace table {name} ({schema})")

    @staticmethod
    def drop_table(session: "Session", name: str):
        session._run_query(f"drop table if exists {name}")

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
        # TODO: SNOW-367306 swap order of rows after finishing inferring schema
        return session.createDataFrame([cls.Data3(2, 2), cls.Data3(1, None)])

    @classmethod
    def test_data4(cls, session: "Session") -> DataFrame:
        return session.createDataFrame([cls.Data4(i, str(i)) for i in range(1, 101)])

    @classmethod
    def test_lower_case_data(cls, session: "Session") -> DataFrame:
        return session.createDataFrame([[1, "a"], [2, "b"], [3, "c"], [4, "d"]])

    @classmethod
    def test_upper_case_data(cls, session: "Session") -> DataFrame:
        return session.createDataFrame(
            [[1, "A"], [2, "B"], [3, "C"], [4, "D"], [5, "E"], [6, "F"]]
        )

    @classmethod
    def null_data1(cls, session: "Session") -> DataFrame:
        return session.sql("select * from values(null),(2),(1),(3),(null) as T(a)")

    @classmethod
    def integer1(cls, session: "Session") -> DataFrame:
        return session.sql("select * from values(1),(2),(3) as T(a)")

    @classmethod
    def double2(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select * from values(0.1, 0.5),(0.2, 0.6),(0.3, 0.7) as T(a,b)"
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
