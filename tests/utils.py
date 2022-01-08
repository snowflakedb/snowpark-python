#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import functools
import math
import os
import platform
import random
import string
import uuid
from decimal import Decimal
from typing import List, NamedTuple, Optional, Union

from snowflake.snowpark import DataFrame, Row, Session
from snowflake.snowpark._internal.analyzer.analyzer_package import AnalyzerPackage

IS_WINDOWS = platform.system() == "Windows"
IS_MACOS = platform.system() == "Darwin"
IS_LINUX = platform.system() == "Linux"
IS_UNIX = IS_LINUX or IS_MACOS


class Utils:
    @staticmethod
    def escape_path(path):
        if IS_WINDOWS:
            return path.replace("\\", "\\\\")
        else:
            return path

    @staticmethod
    def random_name() -> str:
        return "SN_TEST_OBJECT_{}".format(str(uuid.uuid4()).replace("-", "_")).upper()

    @staticmethod
    def random_alphanumeric_str(n: int):
        return "".join(
            random.choice(
                string.ascii_uppercase + string.ascii_lowercase + string.digits
            )
            for _ in range(n)
        )

    @staticmethod
    def random_stage_name() -> str:
        return f"SN_TEST_Stage_{abs(random.randint(0, 2 ** 31))}".upper()

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
    def unset_query_tag(session: "Session"):
        session.query_tag = None

    @staticmethod
    def upload_to_stage(
        session: "Session", stage_name: str, filename: str, compress: bool
    ):
        session._conn.upload_file(
            stage_location=stage_name, path=filename, compress_data=compress
        )

    @staticmethod
    def equals_ignore_case(a: str, b: str) -> bool:
        return a.lower() == b.lower()

    @classmethod
    def random_temp_schema(cls):
        return f"SCHEMA_{cls.random_name()}"

    @classmethod
    def get_fully_qualified_temp_schema(cls, session: Session):
        return f"{session.getCurrentDatabase()}.{cls.random_temp_schema()}"

    @staticmethod
    def assert_rows(actual_rows, expected_rows):
        assert len(actual_rows) == len(
            expected_rows
        ), f"row count is different. Expected {len(expected_rows)}. Actual {len(actual_rows)}"
        for row_index in range(0, len(expected_rows)):
            expected_row = expected_rows[row_index]
            actual_row = actual_rows[row_index]
            assert len(actual_row) == len(
                expected_row
            ), f"column count for row {row_index + 1} is different. Expected {len(expected_row)}. Actual {len(actual_row)}"
            for column_index in range(0, len(expected_row)):
                expected_value = expected_row[column_index]
                actual_value = actual_row[column_index]
                if isinstance(expected_value, float):
                    if math.isnan(expected_value):
                        assert math.isnan(
                            actual_value
                        ), f"Expected NaN. Actual {actual_value}"
                    else:
                        assert math.isclose(
                            actual_value, expected_value
                        ), f"Expected {expected_value}. Actual {actual_value}"
                else:
                    assert (
                        actual_value == expected_value
                    ), f"Expected {expected_value}. Actual {actual_value}"

    @staticmethod
    def check_answer(
        actual: Union[Row, List[Row], DataFrame],
        expected: Union[Row, List[Row], DataFrame],
        sort=True,
    ):
        def get_rows(input_data: Union[Row, List[Row], DataFrame]):
            if type(input_data) == list:
                rows = input_data
            elif type(input_data) == DataFrame:
                rows = input_data.collect()
            elif type(input_data) == Row:
                rows = [input_data]
            else:
                raise TypeError(
                    "input_data must be a DataFrame, a list of Row objects or a Row object"
                )
            return rows

        actual_rows = get_rows(actual)
        expected_rows = get_rows(expected)
        if sort:

            def compare_rows(row1, row2):
                assert len(row1) == len(
                    row2
                ), "rows1 and row2 have different length so they're not comparable."
                for value1, value2 in zip(row1, row2):
                    if value1 == value2:
                        continue
                    if value1 is None:
                        return -1
                    elif value2 is None:
                        return 1
                    elif value1 > value2:
                        return 1
                    elif value1 < value2:
                        return -1
                return 0

            sort_key = functools.cmp_to_key(compare_rows)
            sorted_expected_rows = sorted(expected_rows, key=sort_key)
            sorted_actual_rows = sorted(actual_rows, key=sort_key)
            Utils.assert_rows(sorted_actual_rows, sorted_expected_rows)
        else:
            Utils.assert_rows(actual_rows, expected_rows)


class TestData:
    Data = NamedTuple("Data", [("num", int), ("bool", bool), ("str", str)])
    Data2 = NamedTuple("Data2", [("a", int), ("b", int)])
    Data3 = NamedTuple("Data3", [("a", int), ("b", Optional[int])])
    Data4 = NamedTuple("Data4", [("key", int), ("value", str)])
    LowerCaseData = NamedTuple("LowerCaseData", [("n", int), ("l", str)])
    UpperCaseData = NamedTuple("UpperCaseData", [("N", int), ("L", str)])
    NullInt = NamedTuple("NullInts", [("a", Optional[int])])
    Number1 = NamedTuple("Number1", [("K", int), ("v1", float), ("v2", float)])
    Number2 = NamedTuple("Number2", [("x", int), ("y", int), ("z", int)])
    MonthlySales = NamedTuple(
        "MonthlySales", [("empid", int), ("amount", int), ("month", str)]
    )

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
    def null_data2(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select * from values(1,2,3),(null,2,3),(null,null,3),(null,null,null),"
            "(1,null,3),(1,null,null),(1,2,null) as T(a,b,c)"
        )

    @classmethod
    def null_data3(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select * from values(1.0, 1, true, 'a'),('NaN'::Double, 2, null, 'b'),"
            "(null, 3, false, null), (4.0, null, null, 'd'), (null, null, null, null),"
            "('NaN'::Double, null, null, null) as T(flo, int, boo, str)"
        )

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
    def double3(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select * from values(1.0, 1),('NaN'::Double, 2),(null, 3),"
            "(4.0, null), (null, null), ('NaN'::Double, null) as T(a, b)"
        )

    @classmethod
    def nan_data1(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select * from values(1.2),('NaN'::Double),(null),(2.3) as T(a)"
        )

    @classmethod
    def double4(cls, session: "Session") -> DataFrame:
        return session.sql("select * from values(1.0, 1) as T(a, b)")

    @classmethod
    def duplicated_numbers(cls, session: "Session") -> DataFrame:
        return session.sql("select * from values(3),(2),(1),(3),(2) as T(a)")

    @classmethod
    def approx_numbers(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select * from values(1),(2),(3),(4),(5),(6),(7),(8),(9),(0) as T(a)"
        )

    @classmethod
    def approx_numbers2(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select * from values(1, 1),(2, 1),(3, 3),(4, 3),(5, 3),(6, 3),(7, 3),"
            + "(8, 5),(9, 5),(0, 5) as T(a, T)"
        )

    @classmethod
    def string1(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select * from values('test1', 'a'),('test2', 'b'),('test3', 'c') as T(a, b)"
        )

    @classmethod
    def string2(cls, session: "Session") -> DataFrame:
        return session.sql("select * from values('asdFg'),('qqq'),('Qw') as T(a)")

    @classmethod
    def string3(cls, session: "Session") -> DataFrame:
        return session.sql("select * from values('  abcba  '), (' a12321a   ') as T(a)")

    @classmethod
    def string4(cls, session: "Session") -> DataFrame:
        return session.sql("select * from values('apple'),('banana'),('peach') as T(a)")

    @classmethod
    def string5(cls, session: "Session") -> DataFrame:
        return session.sql("select * from values('1,2,3,4,5') as T(a)")

    @classmethod
    def string6(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select * from values('1,2,3,4,5', ','),('1 2 3 4 5', ' ') as T(a, b)"
        )

    @classmethod
    def string7(cls, session: "Session") -> DataFrame:
        return session.sql("select * from values('str', 1),(null, 2) as T(a, b)")

    @classmethod
    def array1(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select array_construct(a,b,c) as arr1, array_construct(d,e,f) as arr2 "
            "from values(1,2,3,3,4,5),(6,7,8,9,0,1) as T(a,b,c,d,e,f)"
        )

    @classmethod
    def array2(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select array_construct(a,b,c) as arr1, d, e, f from"
            " values(1,2,3,2,'e1','[{a:1}]'),(6,7,8,1,'e2','[{a:1},{b:2}]') as T(a,b,c,d,e,f)"
        )

    @classmethod
    def array3(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select array_construct(a,b,c) as arr1, d, e, f "
            "from values(1,2,3,1,2,','),(4,5,6,1,-1,', '),(6,7,8,0,2,';') as T(a,b,c,d,e,f)"
        )

    @classmethod
    def object1(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select key, to_variant(value) as value from "
            "values('age', 21),('zip', 94401) as T(key,value)"
        )

    @classmethod
    def object2(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select object_construct(a,b,c,d,e,f) as obj, k, v, flag from "
            "values('age', 21, 'zip', 21021, 'name', 'Joe', 'age', 0, true),"
            "('age', 26, 'zip', 94021, 'name', 'Jay', 'key', 0, false) as T(a,b,c,d,e,f,k,v,flag)"
        )

    @classmethod
    def object3(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select key, to_variant(value) as value from "
            "values(null, 21),('zip', null) as T(key,value)"
        )

    @classmethod
    def null_array1(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select array_construct(a,b,c) as arr1, array_construct(d,e,f) as arr2 "
            "from values(1,null,3,3,null,5),(6,null,8,9,null,1) as T(a,b,c,d,e,f)"
        )

    @classmethod
    def zero1(cls, session: "Session") -> DataFrame:
        return session.sql("select * from values(0) as T(a)")

    @classmethod
    def variant1(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select to_variant(to_array('Example')) as arr1,"
            + ' to_variant(to_object(parse_json(\'{"Tree": "Pine"}\'))) as obj1, '
            + " to_variant(to_binary('snow', 'utf-8')) as bin1,"
            + " to_variant(true) as bool1,"
            + " to_variant('X') as str1, "
            + " to_variant(to_date('2017-02-24')) as date1, "
            + " to_variant(to_time('20:57:01.123456789+07:00')) as time1, "
            + " to_variant(to_timestamp_ntz('2017-02-24 12:00:00.456')) as timestamp_ntz1, "
            + " to_variant(to_timestamp_ltz('2017-02-24 13:00:00.123 +01:00')) as timestamp_ltz1, "
            + " to_variant(to_timestamp_tz('2017-02-24 13:00:00.123 +01:00')) as timestamp_tz1, "
            + " to_variant(1.23::decimal(6, 3)) as decimal1, "
            + " to_variant(3.21::double) as double1, "
            + " to_variant(15) as num1 "
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
    def valid_json1(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select parse_json(column1) as v, column2 as k from values ('{\"a\": null}','a'), "
            "('{\"a\": \"foo\"}','a'), ('{\"a\": \"foo\"}','b'), (null,'a')"
        )

    @classmethod
    def invalid_json1(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select (column1) as v from values ('{\"a\": null'), ('{\"a: \"foo\"}'), ('{\"a:')"
        )

    @classmethod
    def null_xml1(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select (column1) as v from values ('<t1>foo<t2>bar</t2><t3></t3></t1>'), "
            "('<t1></t1>'), (null), ('')"
        )

    @classmethod
    def valid_xml1(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select parse_xml(a) as v, b as t2, c as t3, d as instance from values"
            + "('<t1>foo<t2>bar</t2><t3></t3></t1>','t2','t3',0),('<t1></t1>','t2','t3',0),"
            + "('<t1><t2>foo</t2><t2>bar</t2></t1>','t2','t3',1) as T(a,b,c,d)"
        )

    @classmethod
    def invalid_xml1(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select (column1) as v from values ('<t1></t>'), ('<t1><t1>'), ('<t1</t1>')"
        )

    @classmethod
    def date1(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select * from values('2020-08-01'::Date, 1),('2010-12-01'::Date, 2) as T(a,b)"
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
    def number1(cls, session) -> DataFrame:
        return session.createDataFrame(
            [
                cls.Number1(1, 10.0, 0.0),
                cls.Number1(2, 10.0, 11.0),
                cls.Number1(2, 20.0, 22.0),
                cls.Number1(2, 25.0, 0.0),
                cls.Number1(2, 30.0, 35.0),
            ]
        )

    @classmethod
    def number2(cls, session):
        return session.createDataFrame(
            [cls.Number2(1, 2, 3), cls.Number2(0, -1, 4), cls.Number2(-5, 0, -9)]
        )

    @classmethod
    def timestamp1(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select * from values('2020-05-01 13:11:20.000' :: timestamp),"
            "('2020-08-21 01:30:05.000' :: timestamp) as T(a)"
        )

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
    def long1(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select * from values(1561479557),(1565479557),(1161479557) as T(a)"
        )

    @classmethod
    def monthly_sales(cls, session: "Session") -> DataFrame:
        return session.createDataFrame(
            [
                cls.MonthlySales(1, 10000, "JAN"),
                cls.MonthlySales(1, 400, "JAN"),
                cls.MonthlySales(2, 4500, "JAN"),
                cls.MonthlySales(2, 35000, "JAN"),
                cls.MonthlySales(1, 5000, "FEB"),
                cls.MonthlySales(1, 3000, "FEB"),
                cls.MonthlySales(2, 200, "FEB"),
                cls.MonthlySales(2, 90500, "FEB"),
                cls.MonthlySales(1, 6000, "MAR"),
                cls.MonthlySales(1, 5000, "MAR"),
                cls.MonthlySales(2, 2500, "MAR"),
                cls.MonthlySales(2, 9500, "MAR"),
                cls.MonthlySales(1, 8000, "APR"),
                cls.MonthlySales(1, 10000, "APR"),
                cls.MonthlySales(2, 800, "APR"),
                cls.MonthlySales(2, 4500, "APR"),
            ]
        )

    @classmethod
    def column_has_special_char(cls, session: "Session") -> DataFrame:
        return session.createDataFrame([[1, 2], [3, 4]]).toDF(['"col %"', '"col *"'])

    @classmethod
    def nurse(cls, session: "Session") -> DataFrame:
        return session.createDataFrame(
            [
                [201, "Thomas Leonard Vicente", "LVN", "Technician"],
                [202, "Tamara Lolita VanZant", "LVN", "Technician"],
                [341, "Georgeann Linda Vente", "LVN", "General"],
                [471, "Andrea Renee Nouveau", "RN", "Amateur Extra"],
                [101, "Lily Vine", "LVN", None],
                [102, "Larry Vancouver", "LVN", None],
                [172, "Rhonda Nova", "RN", None],
            ]
        ).toDF(["id", "full_name", "medical_license", "radio_license"])


class TestFiles:
    def __init__(self, resources_path):
        self.resources_path = resources_path

    @property
    def test_file_csv(self):
        return os.path.join(self.resources_path, "testCSV.csv")

    @property
    def test_file2_csv(self):
        return os.path.join(self.resources_path, "test2CSV.csv")

    @property
    def test_file_csv_colon(self):
        return os.path.join(self.resources_path, "testCSVcolon.csv")

    @property
    def test_file_csv_quotes(self):
        return os.path.join(self.resources_path, "testCSVquotes.csv")

    @property
    def test_file_json(self):
        return os.path.join(self.resources_path, "testJson.json")

    @property
    def test_file_avro(self):
        return os.path.join(self.resources_path, "test.avro")

    @property
    def test_file_parquet(self):
        return os.path.join(self.resources_path, "test.parquet")

    @property
    def test_file_orc(self):
        return os.path.join(self.resources_path, "test.orc")

    @property
    def test_file_xml(self):
        return os.path.join(self.resources_path, "test.xml")

    @property
    def test_broken_csv(self):
        return os.path.join(self.resources_path, "broken.csv")

    @property
    def test_udf_directory(self):
        return os.path.join(self.resources_path, "test_udf_dir")

    @property
    def test_udf_py_file(self):
        return os.path.join(self.test_udf_directory, "test_udf_file.py")
