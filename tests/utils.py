#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import functools
import math
import os
import platform
import random
import string
from decimal import Decimal
from typing import List, NamedTuple, Optional, Union

from snowflake.connector.constants import FIELD_ID_TO_NAME
from snowflake.snowpark import DataFrame, Row, Session
from snowflake.snowpark._internal import utils
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    quote_name,
    quote_name_without_upper_casing,
)
from snowflake.snowpark._internal.type_utils import convert_sf_to_sp_type
from snowflake.snowpark._internal.utils import TempObjectType, is_in_stored_procedure
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    GeographyType,
    LongType,
    MapType,
    StringType,
    StructType,
    TimestampType,
    TimeType,
    VariantType,
)

IS_WINDOWS = platform.system() == "Windows"
IS_MACOS = platform.system() == "Darwin"
IS_LINUX = platform.system() == "Linux"
IS_UNIX = IS_LINUX or IS_MACOS
IS_IN_STORED_PROC = is_in_stored_procedure()
# this env variable is set in regression test
IS_IN_STORED_PROC_LOCALFS = IS_IN_STORED_PROC and os.getenv("IS_LOCAL_FS")


class Utils:
    @staticmethod
    def escape_path(path):
        if IS_WINDOWS:
            return path.replace("\\", "\\\\")
        else:
            return path

    @staticmethod
    def random_name_for_temp_object(object_type: TempObjectType) -> str:
        return utils.random_name_for_temp_object(object_type)

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
        return Utils.random_name_for_temp_object(TempObjectType.STAGE)

    @staticmethod
    def random_function_name():
        return Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

    @staticmethod
    def random_view_name():
        return Utils.random_name_for_temp_object(TempObjectType.VIEW)

    @staticmethod
    def random_table_name() -> str:
        return Utils.random_name_for_temp_object(TempObjectType.TABLE)

    @staticmethod
    def create_table(
        session: "Session", name: str, schema: str, is_temporary: bool = False
    ):
        session._run_query(
            f"create or replace {'temporary' if is_temporary else ''} table {name} ({schema})"
        )

    @staticmethod
    def create_stage(session: "Session", name: str, is_temporary: bool = True):
        session._run_query(
            f"create or replace {'temporary' if is_temporary else ''} stage {quote_name(name)}"
        )

    @staticmethod
    def drop_stage(session: "Session", name: str):
        session._run_query(f"drop stage if exists {quote_name(name)}")

    @staticmethod
    def drop_table(session: "Session", name: str):
        session._run_query(f"drop table if exists {quote_name(name)}")

    @staticmethod
    def drop_view(session: "Session", name: str):
        session._run_query(f"drop view if exists {quote_name(name)}")

    @staticmethod
    def drop_function(session: "Session", name: str):
        session._run_query(f"drop function if exists {name}")

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
        return f"SCHEMA_{cls.random_alphanumeric_str(10)}"

    @classmethod
    def random_temp_database(cls):
        return f"DATABASE_{cls.random_alphanumeric_str(10)}"

    @classmethod
    def get_fully_qualified_temp_schema(cls, session: Session):
        return f"{session.get_current_database()}.{cls.random_temp_schema()}"

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
    def get_sorted_rows(rows: List[Row]) -> List[Row]:
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
        return sorted(rows, key=sort_key)

    @staticmethod
    def check_answer(
        actual: Union[Row, List[Row], DataFrame],
        expected: Union[Row, List[Row], DataFrame],
        sort=True,
    ) -> None:
        def get_rows(input_data: Union[Row, List[Row], DataFrame]):
            if isinstance(input_data, list):
                rows = input_data
            elif isinstance(input_data, DataFrame):
                rows = input_data.collect()
            elif isinstance(input_data, Row):
                rows = [input_data]
            else:
                raise TypeError(
                    "input_data must be a DataFrame, a list of Row objects or a Row object"
                )
            return rows

        actual_rows = get_rows(actual)
        expected_rows = get_rows(expected)
        if sort:
            sorted_expected_rows = Utils.get_sorted_rows(expected_rows)
            sorted_actual_rows = Utils.get_sorted_rows(actual_rows)
            Utils.assert_rows(sorted_actual_rows, sorted_expected_rows)
        else:
            Utils.assert_rows(actual_rows, expected_rows)

    @staticmethod
    def verify_schema(sql: str, expected_schema: StructType, session: Session) -> None:
        session._run_query(sql)
        result_meta = session._conn._cursor.description

        assert len(result_meta) == len(expected_schema.fields)
        for meta, field in zip(result_meta, expected_schema.fields):
            assert (
                quote_name_without_upper_casing(meta.name)
                == field.column_identifier.quoted_name
            )
            assert meta.is_nullable == field.nullable
            assert (
                convert_sf_to_sp_type(
                    FIELD_ID_TO_NAME[meta.type_code], meta.precision, meta.scale
                )
                == field.datatype
            )

    @staticmethod
    def is_active_transaction(session: Session) -> bool:
        # `SELECT CURRENT_TRANSACTION()` returns a valid txn ID if there is active txn or NULL otherwise
        return session.sql("SELECT CURRENT_TRANSACTION()").collect()[0][0] is not None

    @staticmethod
    def assert_table_type(session: Session, table_name: str, table_type: str) -> None:
        table_info = session.sql(f"show tables like '{table_name}'").collect()
        if not table_type:
            expected_table_kind = "TABLE"
        elif table_type == "temp":
            expected_table_kind = "TEMPORARY"
        else:
            expected_table_kind = table_type.upper()
        assert table_info[0]["kind"] == expected_table_kind


class TestData:
    class Data(NamedTuple):
        num: int
        bool: bool
        str: str

    class Data2(NamedTuple):
        a: int
        b: int

    class Data3(NamedTuple):
        a: int
        b: Optional[int]

    class Data4(NamedTuple):
        key: int
        value: str

    class LowerCaseData(NamedTuple):
        n: int
        l: str

    class UpperCaseData(NamedTuple):
        N: int
        L: str

    NullInt = NamedTuple("NullInts", [("a", Optional[int])])

    class Number1(NamedTuple):
        K: int
        v1: float
        v2: float

    class Number2(NamedTuple):
        x: int
        y: int
        z: int

    class MonthlySales(NamedTuple):
        empid: int
        amount: int
        month: str

    @classmethod
    def test_data1(cls, session: "Session") -> DataFrame:
        return session.create_dataframe(
            [cls.Data(1, True, "a"), cls.Data(2, False, "b")]
        )

    @classmethod
    def test_data2(cls, session: "Session") -> DataFrame:
        return session.create_dataframe(
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
        return session.create_dataframe([cls.Data3(1, None), cls.Data3(2, 2)])

    @classmethod
    def test_data4(cls, session: "Session") -> DataFrame:
        return session.create_dataframe([cls.Data4(i, str(i)) for i in range(1, 101)])

    @classmethod
    def lower_case_data(cls, session: "Session") -> DataFrame:
        return session.create_dataframe(
            [
                cls.LowerCaseData(1, "a"),
                cls.LowerCaseData(2, "b"),
                cls.LowerCaseData(3, "c"),
                cls.LowerCaseData(4, "d"),
            ]
        )

    @classmethod
    def upper_case_data(cls, session: "Session") -> DataFrame:
        return session.create_dataframe(
            [
                cls.UpperCaseData(1, "A"),
                cls.UpperCaseData(2, "B"),
                cls.UpperCaseData(3, "C"),
                cls.UpperCaseData(4, "D"),
                cls.UpperCaseData(5, "E"),
                cls.UpperCaseData(6, "F"),
            ]
        )

    @classmethod
    def null_ints(cls, session: "Session") -> DataFrame:
        return session.create_dataframe(
            [cls.NullInt(1), cls.NullInt(2), cls.NullInt(3), cls.NullInt(None)]
        )

    @classmethod
    def all_nulls(cls, session: "Session") -> DataFrame:
        return session.create_dataframe(
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
    def geography(cls, session: "Session") -> DataFrame:
        return session.sql(
            """
            select *
            from values
            ('{
                "coordinates": [
                  30,
                  10
                ],
                "type": "Point"
            }') as T(a)
            """
        )

    @classmethod
    def geography_type(cls, session: "Session") -> DataFrame:
        return session.sql(
            """
            select to_geography(a) as geo
            from values
            ('{
                "coordinates": [
                  30,
                  10
                ],
                "type": "Point"
            }') as T(a)
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
        return session.create_dataframe(
            [
                [Decimal(1), Decimal(1)],
                [Decimal(1), Decimal(2)],
                [Decimal(2), Decimal(1)],
                [Decimal(2), Decimal(2)],
                [Decimal(3), Decimal(1)],
                [Decimal(3), Decimal(2)],
            ]
        ).to_df(["a", "b"])

    @classmethod
    def number1(cls, session) -> DataFrame:
        return session.create_dataframe(
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
        return session.create_dataframe(
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
        return session.create_dataframe(
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
        return session.create_dataframe(
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
    def monthly_sales_flat(cls, session: "Session"):
        return session.create_dataframe(
            [
                (1, "electronics", 100, 200, 300, 100),
                (2, "clothes", 100, 300, 150, 200),
                (3, "cars", 200, 400, 100, 50),
            ],
            schema=["empid", "dept", "jan", "feb", "mar", "apr"],
        )

    @classmethod
    def column_has_special_char(cls, session: "Session") -> DataFrame:
        return session.create_dataframe([[1, 2], [3, 4]]).to_df(['"col %"', '"col *"'])

    @classmethod
    def sql_using_with_select_statement(cls, session: "Session") -> DataFrame:
        return session.sql(
            "with t1 as (select 1 as a), t2 as (select 2 as b) select a, b from t1, t2"
        )

    @classmethod
    def nurse(cls, session: "Session") -> DataFrame:
        return session.create_dataframe(
            [
                [201, "Thomas Leonard Vicente", "LVN", "Technician"],
                [202, "Tamara Lolita VanZant", "LVN", "Technician"],
                [341, "Georgeann Linda Vente", "LVN", "General"],
                [471, "Andrea Renee Nouveau", "RN", "Amateur Extra"],
                [101, "Lily Vine", "LVN", None],
                [102, "Larry Vancouver", "LVN", None],
                [172, "Rhonda Nova", "RN", None],
            ]
        ).to_df(["id", "full_name", "medical_license", "radio_license"])


class TestFiles:
    def __init__(self, resources_path) -> None:
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

    @functools.cached_property
    def test_file_csv_special_format(self):
        return os.path.join(self.resources_path, "testCSVspecialFormat.csv")

    @functools.cached_property
    def test_file_json_special_format(self):
        return os.path.join(self.resources_path, "testJSONspecialFormat.json.gz")

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
    def test_file_all_data_types_parquet(self):
        return os.path.join(self.resources_path, "test_all_data_types.parquet")

    @property
    def test_file_with_special_characters_parquet(self):
        return os.path.join(
            self.resources_path, "test_file_with_special_characters.parquet"
        )

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

    @property
    def test_udtf_directory(self):
        return os.path.join(self.resources_path, "test_udtf_dir")

    @property
    def test_udtf_py_file(self):
        return os.path.join(self.test_udtf_directory, "test_udtf_file.py")

    @property
    def test_sp_directory(self):
        return os.path.join(self.resources_path, "test_sp_dir")

    @property
    def test_sp_py_file(self):
        return os.path.join(self.test_sp_directory, "test_sp_file.py")

    @property
    def test_pandas_udf_py_file(self):
        return os.path.join(self.test_udf_directory, "test_pandas_udf_file.py")

    @property
    def test_requirements_file(self):
        return os.path.join(self.resources_path, "test_requirements.txt")


class TypeMap(NamedTuple):
    col_name: str
    sf_type: str
    snowpark_type: DataType


TYPE_MAP = [
    TypeMap("number", "number(10,2)", DecimalType(10, 2)),
    TypeMap("decimal", "decimal(38,0)", LongType()),
    TypeMap("numeric", "numeric(0,0)", LongType()),
    TypeMap("int", "int", LongType()),
    TypeMap("integer", "integer", LongType()),
    TypeMap("bigint", "bigint", LongType()),
    TypeMap("smallint", "smallint", LongType()),
    TypeMap("tinyint", "tinyint", LongType()),
    TypeMap("byteint", "byteint", LongType()),
    TypeMap("float", "float", DoubleType()),
    TypeMap("float4", "float4", DoubleType()),
    TypeMap("float8", "float8", DoubleType()),
    TypeMap("double", "double", DoubleType()),
    TypeMap("doubleprecision", "double precision", DoubleType()),
    TypeMap("real", "real", DoubleType()),
    TypeMap("varchar", "varchar", StringType()),
    TypeMap("char", "char", StringType()),
    TypeMap("character", "character", StringType()),
    TypeMap("string", "string", StringType()),
    TypeMap("text", "text", StringType()),
    TypeMap("binary", "binary", BinaryType()),
    TypeMap("varbinary", "varbinary", BinaryType()),
    TypeMap("boolean", "boolean", BooleanType()),
    TypeMap("date", "date", DateType()),
    TypeMap("datetime", "datetime", TimestampType()),
    TypeMap("time", "time", TimeType()),
    TypeMap("timestamp", "timestamp", TimestampType()),
    TypeMap("timestamp_ltz", "timestamp_ltz", TimestampType()),
    TypeMap("timestamp_ntz", "timestamp_ntz", TimestampType()),
    TypeMap("timestamp_tz", "timestamp_tz", TimestampType()),
    TypeMap("variant", "variant", VariantType()),
    TypeMap("object", "object", MapType(StringType(), StringType())),
    TypeMap("array", "array", ArrayType(StringType())),
    TypeMap("geography", "geography", GeographyType()),
]
