#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import functools
import math
import os
import platform
import random
import string
import uuid
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, NamedTuple, Optional, Union
from threading import Thread
from unittest import mock

import pytest
import pytz

from snowflake.connector.constants import FIELD_ID_TO_NAME
from snowflake.snowpark import DataFrame, Row, Session
from snowflake.snowpark._internal import utils
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    quote_name_without_upper_casing,
)
from snowflake.snowpark._internal.type_utils import convert_sf_to_sp_type
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    is_in_stored_procedure,
    quote_name,
)
from snowflake.snowpark.functions import (
    array_construct,
    col,
    lit,
    object_construct,
    parse_json,
    parse_xml,
    to_array,
    to_binary,
    to_date,
    to_decimal,
    to_double,
    to_object,
    to_time,
    to_timestamp,
    to_timestamp_ltz,
    to_timestamp_ntz,
    to_timestamp_tz,
    to_variant,
)
from snowflake.snowpark.mock._connection import MockServerConnection
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    GeographyType,
    GeometryType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampTimeZone,
    TimestampType,
    TimeType,
    VariantType,
)

IS_WINDOWS = platform.system() == "Windows"
IS_MACOS = platform.system() == "Darwin"
IS_LINUX = platform.system() == "Linux"
IS_UNIX = IS_LINUX or IS_MACOS
IS_IN_STORED_PROC = is_in_stored_procedure()
IS_NOT_ON_GITHUB = os.getenv("GITHUB_ACTIONS") != "true"
# this env variable is set in regression test
IS_IN_STORED_PROC_LOCALFS = IS_IN_STORED_PROC and os.getenv("IS_LOCAL_FS")

RUNNING_ON_GH = os.getenv("GITHUB_ACTIONS") == "true"
RUNNING_ON_JENKINS = "JENKINS_HOME" in os.environ
TEST_SCHEMA = f"GH_JOB_{(str(uuid.uuid4()).replace('-', '_'))}"
if RUNNING_ON_JENKINS:
    TEST_SCHEMA = f"JENKINS_JOB_{(str(uuid.uuid4()).replace('-', '_'))}"

# SNOW-1348805: Structured types have not been rolled out to all accounts yet.
# Once rolled out this should be updated to include all accounts.
STRUCTURED_TYPE_ENVIRONMENTS = {"SFCTEST0_AWS_US_WEST_2", "SNOWPARK_PYTHON_TEST"}
ICEBERG_ENVIRONMENTS = {"SFCTEST0_AWS_US_WEST_2"}
STRUCTURED_TYPE_PARAMETERS = {
    "ENABLE_STRUCTURED_TYPES_IN_FDN_TABLES",
    "ENABLE_STRUCTURED_TYPES_IN_CLIENT_RESPONSE",
    "ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT",
    "FORCE_ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT",
    "IGNORE_CLIENT_VESRION_IN_STRUCTURED_TYPES_RESPONSE",
}


def current_account(session):
    return session.sql("select CURRENT_ACCOUNT_NAME()").collect()[0][0].upper()


def structured_types_supported(session, local_testing_mode):
    if local_testing_mode:
        return True
    return current_account(session) in STRUCTURED_TYPE_ENVIRONMENTS


def iceberg_supported(session, local_testing_mode):
    if local_testing_mode:
        return False
    return current_account(session) in ICEBERG_ENVIRONMENTS


@contextmanager
def structured_types_enabled_session(session):
    for param in STRUCTURED_TYPE_PARAMETERS:
        session.sql(f"alter session set {param}=true").collect()
    with mock.patch("snowflake.snowpark.context._use_structured_type_semantics", True):
        yield session
    for param in STRUCTURED_TYPE_PARAMETERS:
        session.sql(f"alter session unset {param}").collect()


def running_on_public_ci() -> bool:
    """Whether tests are currently running on one of our public CIs."""
    return RUNNING_ON_GH


def running_on_jenkins() -> bool:
    """Whether tests are currently running on a Jenkins node."""
    return RUNNING_ON_JENKINS


def multithreaded_run(num_threads: int = 5) -> None:
    """When multithreading_mode is enabled, run the decorated test function in multiple threads."""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            all_threads = []
            for _ in range(num_threads):
                job = Thread(target=func, args=args, kwargs=kwargs)
                all_threads.append(job)
                job.start()
            for thread in all_threads:
                thread.join()

        return wrapper

    return decorator


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
    def create_schema(session: "Session", name: str, is_temporary: bool = False):
        session._run_query(
            f"create or replace {'temporary' if is_temporary else ''} schema {name}"
        )

    @staticmethod
    def create_stage(session: "Session", name: str, is_temporary: bool = True):
        if isinstance(session._conn, MockServerConnection):
            # no-op in local testing
            return
        session._run_query(
            f"create or replace {'temporary' if is_temporary else ''} stage {quote_name(name)}"
        )

    @staticmethod
    def drop_stage(session: "Session", name: str):
        if isinstance(session._conn, MockServerConnection):
            # no-op in local testing
            return
        session._run_query(f"drop stage if exists {quote_name(name)}")

    @staticmethod
    def drop_table(session: "Session", name: str):
        if isinstance(session._conn, MockServerConnection):
            session.table(name).drop_table()
        else:
            session._run_query(f"drop table if exists {quote_name(name)}")

    @staticmethod
    def drop_dynamic_table(session: "Session", name: str):
        session._run_query(f"drop dynamic table if exists {quote_name(name)}")

    @staticmethod
    def drop_view(session: "Session", name: str):
        session._run_query(f"drop view if exists {quote_name(name)}")

    @staticmethod
    def drop_function(session: "Session", name: str):
        session._run_query(f"drop function if exists {name}")

    @staticmethod
    def drop_procedure(session: "Session", name: str):
        session._run_query(f"drop procedure if exists {name}")

    @staticmethod
    def drop_schema(session: "Session", name: str):
        session._run_query(f"drop schema if exists {name}")

    @staticmethod
    def drop_database(session: "Session", name: str):
        session._run_query(f"drop database if exists {name}")

    @staticmethod
    def unset_query_tag(session: "Session"):
        session.query_tag = None

    @staticmethod
    def upload_to_stage(
        session: "Session", stage_name: str, filename: str, compress: bool
    ):
        session.file.put(
            local_file_name=filename, stage_location=stage_name, auto_compress=compress
        )

    @staticmethod
    def is_schema_same(
        schema_a: StructType, schema_b: StructType, case_sensitive=True
    ) -> None:
        if case_sensitive:
            assert str(schema_a) == str(schema_b), "str(schema) mismatch"

        if len(schema_a.fields) != len(schema_b.fields):
            raise AssertionError("field length mismatch")

        for field_a, field_b in zip(schema_a, schema_b):
            if field_a.name.lower() != field_b.name.lower():
                raise AssertionError(f"name mismatch {field_a.name} != {field_b.name}")
            if repr(field_a.datatype) != repr(field_b.datatype):
                raise AssertionError(
                    f"datatype mismatch {field_a.datatype} != {field_b.datatype} for {field_a.name}"
                )
            if field_a.nullable != field_b.nullable:
                raise AssertionError(
                    f"nullable mismatch {field_a.nullable} != {field_b.nullable} for {field_a.name}"
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
    def assert_rows(actual_rows, expected_rows, float_equality_threshold=0.0):
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
                    elif float_equality_threshold > 0:
                        assert actual_value == pytest.approx(
                            expected_value, abs=float_equality_threshold
                        )
                    else:
                        assert math.isclose(
                            actual_value, expected_value
                        ), f"Expected {expected_value}. Actual {actual_value}"
                elif isinstance(expected_value, list):
                    if len(expected_value) > 0 and any(
                        [isinstance(v, float) for v in expected_value]
                    ):
                        assert actual_value == pytest.approx(
                            expected_value
                        ), f"Mismatch on row {row_index} at column {column_index}. Expected {expected_value}. Actual {actual_value}"
                    else:
                        assert (
                            actual_value == expected_value
                        ), f"Mismatch on row {row_index} at column {column_index}. Expected {expected_value}. Actual {actual_value}"
                else:
                    assert (
                        actual_value == expected_value
                    ), f"Mismatch on row {row_index} at column {column_index}. Expected {expected_value}. Actual {actual_value}"

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
        statement_params: Optional[Dict[str, str]] = None,
        float_equality_threshold=0.0,
    ) -> None:

        # Check that statement_params are passed as Dict[str, str].
        assert statement_params is None or (
            isinstance(statement_params, dict)
            and all(
                isinstance(k, str) and isinstance(v, str)
                for k, v in statement_params.items()
            )
        )

        def get_rows(input_data: Union[Row, List[Row], DataFrame]):
            if isinstance(input_data, list):
                rows = input_data
            elif isinstance(input_data, DataFrame):
                rows = input_data.collect(statement_params=statement_params)
            elif isinstance(input_data, Row):
                rows = [input_data]
            else:
                raise TypeError(
                    "input_data must be a DataFrame, a list of Row objects or a Row object"
                )

            # Strip column names to make errors more concise
            rows = [Row(*list(x)) for x in rows]
            return rows

        actual_rows = get_rows(actual)
        expected_rows = get_rows(expected)
        if sort:
            sorted_expected_rows = Utils.get_sorted_rows(expected_rows)
            sorted_actual_rows = Utils.get_sorted_rows(actual_rows)
            Utils.assert_rows(
                sorted_actual_rows, sorted_expected_rows, float_equality_threshold
            )
        else:
            Utils.assert_rows(actual_rows, expected_rows, float_equality_threshold)

    @staticmethod
    def verify_schema(
        sql: str,
        expected_schema: StructType,
        session: Session,
        max_string_size: int = None,
    ) -> None:
        session._run_query(sql)
        result_meta = session._conn._cursor.description

        assert len(result_meta) == len(expected_schema.fields)
        for meta, field in zip(result_meta, expected_schema.fields):
            assert (
                quote_name_without_upper_casing(meta.name)
                == field.column_identifier.quoted_name
            )
            assert meta.is_nullable == field.nullable

            sp_type = convert_sf_to_sp_type(
                FIELD_ID_TO_NAME[meta.type_code],
                meta.precision,
                meta.scale,
                meta.internal_size,
                max_string_size or session._conn.max_string_size,
            )
            assert (
                sp_type == field.datatype
            ), f"{sp_type=} is not equal to {field.datatype=}"

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

    @staticmethod
    def assert_rows_count(data: DataFrame, row_number: int):
        row_counter = len(data.collect())

        assert (
            row_counter == row_number
        ), f"Expect {row_number} rows, Got {row_counter} instead"

    @staticmethod
    def assert_executed_with_query_tag(
        session: Session, query_tag: str, local_testing_mode: bool = False
    ) -> None:
        # inside the stored proc information_schema.query_history_by_session() is not accessible
        # which leads to "Requested information on the current user is not accessible in stored procedure."
        if local_testing_mode or IS_IN_STORED_PROC:
            return
        query_details = session.sql(
            f"select * from table(information_schema.query_history_by_session()) where QUERY_TAG='{query_tag}'"
        )
        assert (
            len(query_details.collect()) > 0
        ), f"query tag '{query_tag}' not present in query history for given session"


class TestData:
    __test__ = (
        False  # silence pytest warnings for trying to collect this class as a test
    )

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
        return session.create_dataframe([[None], [2], [1], [3], [None]], schema=["a"])

    @classmethod
    def null_data2(cls, session: "Session") -> DataFrame:
        return session.create_dataframe(
            [
                [1, 2, 3],
                [None, 2, 3],
                [None, None, 3],
                [None, None, None],
                [1, None, 3],
                [1, None, None],
                [1, 2, None],
            ],
            schema=["a", "b", "c"],
        )

    @classmethod
    def null_data3(cls, session: "Session", local_testing_mode=False) -> DataFrame:
        return (
            session.sql(
                "select * from values(1.0, 1, true, 'a'),('NaN'::Double, 2, null, 'b'),"
                "(null, 3, false, null), (4.0, null, null, 'd'), (null, null, null, null),"
                "('NaN'::Double, null, null, null) as T(flo, int, boo, str)"
            )
            if not local_testing_mode
            else session.create_dataframe(
                [
                    [1.0, 1, True, "a"],
                    [math.nan, 2, None, "b"],
                    [None, 3, False, None],
                    [4.0, None, None, "d"],
                    [None, None, None, None],
                    [math.nan, None, None, None],
                ],
                schema=["flo", "int", "boo", "str"],
            )
        )

    @classmethod
    def null_data4(cls, session: "Session") -> DataFrame:
        return session.create_dataframe(
            [
                [Decimal(1), None],
                [None, Decimal(2)],
            ]
        ).to_df(["a", "b"])

    @classmethod
    def integer1(cls, session: "Session") -> DataFrame:
        return session.create_dataframe([[1], [2], [3]]).to_df(["a"])

    @classmethod
    def double1(cls, session: "Session") -> DataFrame:
        return session.create_dataframe(
            [[1.111], [2.222], [3.333]],
            schema=StructType([StructField("a", DecimalType(scale=3))]),
        )

    @classmethod
    def double2(cls, session: "Session") -> DataFrame:
        return session.create_dataframe(
            [[0.1, 0.5], [0.2, 0.6], [0.3, 0.7]], schema=["a", "b"]
        )

    @classmethod
    def double3(cls, session: "Session", local_testing_mode=False) -> DataFrame:
        return (
            session.sql(
                "select * from values(1.0, 1),('NaN'::Double, 2),(null, 3),"
                "(4.0, null), (null, null), ('NaN'::Double, null) as T(a, b)"
            )
            if not local_testing_mode
            else session.create_dataframe(
                [
                    [1.0, 1],
                    [math.nan, 2],
                    [None, 3],
                    [4.0, None],
                    [None, None],
                    [math.nan, None],
                ],
                schema=["a", "b"],
            )
        )

    @classmethod
    def nan_data1(cls, session: "Session") -> DataFrame:
        return session.create_dataframe(
            [(1.2,), (math.nan,), (None,), (2.3,)], schema=["a"]
        )

    @classmethod
    def double4(cls, session: "Session") -> DataFrame:
        return session.sql("select * from values(1.0, 1) as T(a, b)")

    @classmethod
    def duplicated_numbers(cls, session: "Session") -> DataFrame:
        return session.create_dataframe(
            [
                (3,),
                (2,),
                (1,),
                (3,),
                (2,),
            ],
            schema=["a"],
        )

    @classmethod
    def approx_numbers(cls, session: "Session") -> DataFrame:
        return session.create_dataframe(
            [[1], [2], [3], [4], [5], [6], [7], [8], [9], [0]], schema=["a"]
        )

    @classmethod
    def approx_numbers2(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select * from values(1, 1),(2, 1),(3, 3),(4, 3),(5, 3),(6, 3),(7, 3),"
            + "(8, 5),(9, 5),(0, 5) as T(a, T)"
        )

    @classmethod
    def string1(cls, session: "Session") -> DataFrame:
        return session.create_dataframe(
            [["test1", "a"], ["test2", "b"], ["test3", "c"]],
            schema=StructType(
                [StructField("a", StringType(5)), StructField("b", StringType(1))]
            ),
        )

    @classmethod
    def string2(cls, session: "Session") -> DataFrame:
        return session.create_dataframe([["asdFg"], ["qqq"], ["Qw"]], schema=["a"])

    @classmethod
    def string3(cls, session: "Session") -> DataFrame:
        return session.create_dataframe([["  abcba  "], [" a12321a   "]], schema=["a"])

    @classmethod
    def string4(cls, session: "Session") -> DataFrame:
        return session.create_dataframe(
            [["apple"], ["banana"], ["peach"]], schema=["a"]
        )

    @classmethod
    def string5(cls, session: "Session") -> DataFrame:
        return session.create_dataframe([["1,2,3,4,5"]], schema=["a"])

    @classmethod
    def string6(cls, session: "Session") -> DataFrame:
        return session.create_dataframe(
            [["1,2,3,4,5", ","], ["1 2 3 4 5", " "]], schema=["a", "b"]
        )

    @classmethod
    def string7(cls, session: "Session") -> DataFrame:
        return session.create_dataframe([["str", 1], [None, 2]], schema=["a", "b"])

    @classmethod
    def string8(cls, session: "Session") -> DataFrame:
        return session.create_dataframe(
            [
                (
                    "foo-bar;baz",
                    "qwer,dvor>azer",
                    "lower",
                    "UPPER",
                    "Chief Variable Officer",
                    "Lorem ipsum dolor sit amet",
                )
            ],
            schema=["delim1", "delim2", "lower", "upper", "title", "sentence"],
        )

    @classmethod
    def string9(cls, session: "Session") -> DataFrame:
        return session.create_dataframe(
            [
                ("foo\nbar1"),
                ("foo\tbar2"),
                ("foo\rbar3"),
                ("foo\r\nbar4"),
            ],
            schema=["a"],
        )

    @classmethod
    def array1(cls, session: "Session") -> DataFrame:
        df = session.create_dataframe(
            [
                (1, 2, 3, 3, 4, 5),
                (6, 7, 8, 9, 0, 1),
            ],
            schema=["a", "b", "c", "d", "e", "f"],
        )
        return df.select(
            array_construct("a", "b", "c").alias("arr1"),
            array_construct("d", "e", "f").alias("arr2"),
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
        return (
            session.create_dataframe(
                [
                    ("age", 21),
                    ("zip", 94401),
                ]
            )
            .to_df(["key", "value"])
            .select("key", to_variant("value").alias("value"))
        )

    @classmethod
    def object2(cls, session: "Session") -> DataFrame:
        return (
            session.create_dataframe(
                [
                    ("age", 21, "zip", 21021, "name", "Joe", "age", 0, True),
                    ("age", 26, "zip", 94021, "name", "Jay", "key", 0, False),
                ]
            )
            .to_df(["a", "b", "c", "d", "e", "f", "k", "v", "flag"])
            .select(
                object_construct("a", "b", "c", "d", "e", "f").alias("obj"),
                "k",
                "v",
                "flag",
            )
        )

    @classmethod
    def object3(cls, session: "Session") -> DataFrame:
        return (
            session.create_dataframe(
                [
                    (None, 21),
                    ("zip", None),
                ]
            )
            .to_df(["key", "value"])
            .select("key", to_variant("value").alias("value"))
        )

    @classmethod
    def null_array1(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select array_construct(a,b,c) as arr1, array_construct(d,e,f) as arr2 "
            "from values(1,null,3,3,null,5),(6,null,8,9,null,1) as T(a,b,c,d,e,f)"
        )

    @classmethod
    def zero1(cls, session: "Session") -> DataFrame:
        return session.create_dataframe([(0,)], schema=["a"])

    @classmethod
    def variant1(cls, session: "Session") -> DataFrame:
        df = session.create_dataframe([1]).select(
            to_variant(to_array(lit("Example"))).alias("arr1"),
            to_variant(to_object(parse_json(lit('{"Tree": "Pine"}')))).alias("obj1"),
            to_variant(to_binary(lit("snow"), "utf-8")).alias("bin1"),
            to_variant(lit(True)).alias("bool1"),
            to_variant(lit("X")).alias("str1"),
            to_variant(to_date(lit("2017-02-24"))).alias("date1"),
            to_variant(
                to_time(lit("20:57:01.123456+0700"), "HH24:MI:SS.FFTZHTZM")
            ).alias("time1"),
            to_variant(to_timestamp_ntz(lit("2017-02-24 12:00:00.456"))).alias(
                "timestamp_ntz1"
            ),
            to_variant(to_timestamp_ltz(lit("2017-02-24 13:00:00.123 +01:00"))).alias(
                "timestamp_ltz1"
            ),
            to_variant(to_timestamp_tz(lit("2017-02-24 13:00:00.123 +01:00"))).alias(
                "timestamp_tz1"
            ),
            to_variant(to_decimal(lit(1.23), 6, 3)).alias("decimal1"),
            to_variant(to_double(lit(3.21))).alias("double1"),
            to_variant(lit(15)).alias("num1"),
        )
        return df

    @classmethod
    def variant2(cls, session: "Session") -> DataFrame:
        df = session.create_dataframe(
            data=[
                """\
{
    "date with ' and .": "2017-04-28",
    "salesperson": {
        "id": "55",
        "name": "Frank Beasley"
    },
    "customer": [
        {"name": "Joyce Ridgely", "phone": "16504378889", "address": "San Francisco, CA"}
    ],
    "vehicle": [
        {"make": "Honda", "extras": ["ext warranty", "paint protection"]}
    ]
}\
"""
            ],
            schema=["values"],
        )
        return df.select(parse_json("values").as_("src"))

    @classmethod
    def datetime_primitives1(cls, session: "Session") -> DataFrame:
        data = [
            (
                1706774400.987654321,
                1706774400,
                "2024-02-01 00:00:00.000000",
                "Thu, 01 Feb 2024 00:00:00 -0600",
                "1706774400",
                date(2024, 2, 1),
                datetime(2024, 2, 1, 12, 0, 0),
                datetime(2017, 2, 24, 12, 0, 0, 456000),
                datetime(
                    2017, 2, 24, 13, 0, 0, 123000, tzinfo=pytz.timezone("Etc/GMT-1")
                ),
                datetime(
                    2017, 2, 24, 14, 0, 0, 789000, tzinfo=pytz.timezone("Etc/GMT-1")
                ),
            )
        ]
        schema = StructType(
            [
                StructField("dec", DecimalType()),
                StructField("int", IntegerType()),
                StructField("str", StringType()),
                StructField("str_w_tz", StringType()),
                StructField("str_ts", StringType()),
                StructField("date", DateType()),
                StructField("timestamp", TimestampType(TimestampTimeZone.DEFAULT)),
                StructField("timestamp_ntz", TimestampType(TimestampTimeZone.NTZ)),
                StructField("timestamp_ltz", TimestampType(TimestampTimeZone.LTZ)),
                StructField("timestamp_tz", TimestampType(TimestampTimeZone.TZ)),
            ]
        )
        return session.create_dataframe(data, schema)

    @classmethod
    def datetime_primitives2(cls, session: "Session") -> DataFrame:
        data = [
            "9999-12-31 00:00:00.123456",
            "1583-01-01 23:59:59.56789",
        ]
        schema = StructType(
            [
                StructField("timestamp", TimestampType(TimestampTimeZone.NTZ)),
            ]
        )
        return session.create_dataframe(data, schema)

    @classmethod
    def time_primitives1(cls, session: "Session") -> DataFrame:
        # simple string data
        data = [("01:02:03",), ("22:33:44",), ("22:33:44.123",), ("22:33:44.56789",)]
        schema = StructType([StructField("a", StringType())])
        return session.create_dataframe(data, schema)

    @classmethod
    def time_primitives2(cls, session: "Session") -> DataFrame:
        # string data needs format
        data = [
            ("01.02-03 PM",),
            ("10.33-44 PM",),
            ("12.55-19 PM",),
        ]
        schema = StructType([StructField("a", StringType())])
        return session.create_dataframe(data, schema)

    @classmethod
    def time_primitives3(cls, session: "Session") -> DataFrame:
        # timestamp data
        data = [
            (datetime(2024, 2, 1, 12, 13, 14),),
            (datetime(2017, 2, 24, 20, 21, 22),),
            ("1712265619",),
            ("1712265619000",),
            ("1712265619000000",),
            ("1712265619000000000",),
        ]
        schema = StructType(
            [
                StructField("a", TimestampType(TimestampTimeZone.NTZ)),
            ]
        )
        return session.create_dataframe(data, schema)

    @classmethod
    def time_primitives4(cls, session: "Session") -> DataFrame:
        # variant data
        data = [
            ("01:02:03",),
            ("1712265619",),
            (None,),
            (time(1, 2, 3),),
        ]
        schema = StructType(
            [
                StructField("a", VariantType()),
            ]
        )
        return session.create_dataframe(data, schema)

    @classmethod
    def variant_datetimes1(cls, session: "Session") -> DataFrame:
        primitives_df = cls.datetime_primitives1(session)
        variant_cols = [
            to_variant(col).alias(f"var_{col}") for col in primitives_df.columns
        ]
        return primitives_df.select(variant_cols)

    @classmethod
    def date_primitives1(cls, session: "Session") -> DataFrame:
        # simple string data + auto detection
        data = ["2023-03-16", "30-JUL-2010", "1713414143"]
        schema = StructType([StructField("a", StringType())])
        return session.create_dataframe(data, schema)

    @classmethod
    def date_primitives2(cls, session: "Session") -> DataFrame:
        # datetime type
        data = [
            datetime(2023, 3, 16),
            datetime(2010, 7, 30, 1, 2, 3),
            datetime(2024, 4, 18),
        ]
        schema = StructType([StructField("a", TimestampType())])
        return session.create_dataframe(data, schema)

    @classmethod
    def date_primitives3(cls, session: "Session") -> DataFrame:
        # variant type
        data = ["1713414143", None, "2024-06-03", "03/21/2000", datetime(2025, 12, 31)]
        schema = StructType([StructField("a", VariantType())])
        return session.create_dataframe(data, schema)

    @classmethod
    def date_primitives4(cls, session: "Session") -> DataFrame:
        # string + format
        data = [
            ("2024-04-18", "AUTO"),
            ("01-09-1999", "DD-MM-YYYY"),
            ("10.2024.29", "MM.YYYY.DD"),
            ("05/15/2015", "MM/DD/YYYY"),
        ]
        schema = StructType(
            [StructField("a", StringType()), StructField("b", StringType())]
        )
        return session.create_dataframe(data, schema)

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
    def geometry(cls, session: "Session") -> DataFrame:
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
    def geometry_type(cls, session: "Session") -> DataFrame:
        return session.sql(
            """
            select to_geometry(a) as geo
            from values
            ('{
                "coordinates": [
                  20,
                  81
                ],
                "type": "Point"
            }') as T(a)
            """
        )

    @classmethod
    def null_json1(cls, session: "Session") -> DataFrame:
        res = session.create_dataframe([['{"a": null}'], ['{"a": "foo"}'], [None]])
        return res.select(parse_json(col("_1")).as_("v"))

    @classmethod
    def valid_json1(cls, session: "Session") -> DataFrame:
        return session.create_dataframe(
            [
                ('{"a": null}', "a"),
                ('{"a": "foo"}', "a"),
                ('{"a": "foo"}', "b"),
                (None, "a"),
            ],
            schema=["v", "k"],
        ).select(parse_json("v").alias("v"), "k")

    @classmethod
    def invalid_json1(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select (column1) as v from values ('{\"a\": null'), ('{\"a: \"foo\"}'), ('{\"a:')"
        )

    @classmethod
    def null_xml1(cls, session: "Session") -> DataFrame:
        return session.create_dataframe(
            [
                ("<t1>foo<t2>bar</t2><t3></t3></t1>",),
                ("<t1></t1>",),
                (None,),
                ("",),
            ],
            schema=["v"],
        )

    @classmethod
    def valid_xml1(cls, session: "Session") -> DataFrame:
        return session.create_dataframe(
            [
                ("<t1>foo<t2>bar</t2><t3></t3></t1>", "t2", "t3", 0),
                ("<t1></t1>", "t2", "t3", 0),
                ("<t1><t2>foo</t2><t2>bar</t2></t1>", "t2", "t3", 1),
            ],
            schema=["a", "b", "c", "d"],
        ).select(
            parse_xml("a").alias("v"),
            col("b").alias("t2"),
            col("c").alias("t3"),
            col("d").alias("instance"),
        )

    @classmethod
    def invalid_xml1(cls, session: "Session") -> DataFrame:
        return session.sql(
            "select (column1) as v from values ('<t1></t>'), ('<t1><t1>'), ('<t1</t1>')"
        )

    @classmethod
    def date1(cls, session: "Session") -> DataFrame:
        return session.create_dataframe(
            [(date(2020, 8, 1), 1), (date(2010, 12, 1), 2)]
        ).to_df(["a", "b"])

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
        return session.create_dataframe(
            [("2020-05-01 13:11:20.000",), ("2020-08-21 01:30:05.000",)], schema=["a"]
        ).select(to_timestamp("a").alias("a"))

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
    def xyz2(cls, session: "Session") -> DataFrame:
        return session.create_dataframe(
            [
                cls.Number2(1, 2, 1),
                cls.Number2(1, 2, 3),
                cls.Number2(2, 1, 10),
                cls.Number2(2, 2, 1),
                cls.Number2(2, 2, 3),
                cls.Number2(2, 3, 5),
                cls.Number2(2, 3, 8),
                cls.Number2(2, 4, 7),
            ]
        )

    @classmethod
    def long1(cls, session: "Session") -> DataFrame:
        data = [
            (1561479557),
            (1565479557),
            (1161479557),
        ]
        schema = StructType(
            [
                StructField("a", LongType()),
            ]
        )
        return session.create_dataframe(data, schema)

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
    def monthly_sales_with_team(cls, session: "Session") -> DataFrame:
        return session.create_dataframe(
            [
                (1, "A", 10000, "JAN"),
                (1, "A", 400, "JAN"),
                (2, "A", 4500, "JAN"),
                (2, "B", 35000, "JAN"),
                (1, "B", 5000, "FEB"),
                (1, "B", 3000, "FEB"),
                (2, "A", 200, "FEB"),
                (2, "A", 90500, "FEB"),
                (1, "B", 6000, "MAR"),
                (1, "A", 5000, "MAR"),
                (2, "B", 2500, "MAR"),
                (2, "B", 9500, "MAR"),
                (1, "B", 8000, "APR"),
                (1, "A", 10000, "APR"),
                (2, "A", 800, "APR"),
                (2, "A", 4500, "APR"),
            ],
            schema=("empid", "team", "amount", "month"),
        )

    @classmethod
    def monthly_sales_flat(cls, session: "Session"):
        return session.create_dataframe(
            [
                (1, "electronics", 100, 200, 300, 100),
                (2, "clothes", 100, 300, 150, 200),
                (3, "cars", 200, 400, 100, 50),
                (4, "appliances", 100, None, 100, 50),
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
    __test__ = (
        False  # silence pytest warnings for trying to collect this class as a test
    )

    def __init__(self, resources_path) -> None:
        self.resources_path = resources_path

    @property
    def test_file_csv(self):
        return os.path.join(self.resources_path, "testCSV.csv")

    @property
    def test_file_csv_various_data(self):
        return os.path.join(self.resources_path, "testCSVvariousData.csv")

    @property
    def test_file2_csv(self):
        return os.path.join(self.resources_path, "test2CSV.csv")

    @property
    def test_file_csv_colon(self):
        return os.path.join(self.resources_path, "testCSVcolon.csv")

    @property
    def test_file_csv_header(self):
        return os.path.join(self.resources_path, "testCSVheader.csv")

    @property
    def test_file_csv_quotes(self):
        return os.path.join(self.resources_path, "testCSVquotes.csv")

    @property
    def test_file_csv_quotes_special(self):
        return os.path.join(self.resources_path, "testCSVquotesSpecial.csv")

    @functools.cached_property
    def test_file_csv_special_format(self):
        return os.path.join(self.resources_path, "testCSVspecialFormat.csv")

    @property
    def test_file_csv_timestamps(self):
        return os.path.join(self.resources_path, "testCSVformattedTime.csv")

    @property
    def test_file_excel(self):
        return os.path.join(self.resources_path, "test_excel.xlsx")

    @functools.cached_property
    def test_file_json_special_format(self):
        return os.path.join(self.resources_path, "testJSONspecialFormat.json.gz")

    @property
    def test_file_json(self):
        return os.path.join(self.resources_path, "testJson.json")

    @property
    def test_file_json_same_schema(self):
        return os.path.join(self.resources_path, "testJsonSameSchema.json")

    @property
    def test_file_json_new_schema(self):
        return os.path.join(self.resources_path, "testJsonNewSchema.json")

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
    def test_file_sas_sas7bdat(self):
        return os.path.join(self.resources_path, "test_sas.sas7bdat")

    @property
    def test_file_sas_xpt(self):
        return os.path.join(self.resources_path, "test_sas.xpt")

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
    def test_another_udf_py_file(self):
        return os.path.join(self.test_udf_directory, "test_another_udf_file.py")

    @property
    def test_udtf_directory(self):
        return os.path.join(self.resources_path, "test_udtf_dir")

    @property
    def test_udtf_py_file(self):
        return os.path.join(self.test_udtf_directory, "test_udtf_file.py")

    @property
    def test_udaf_directory(self):
        return os.path.join(self.resources_path, "test_udaf_dir")

    @property
    def test_udaf_py_file(self):
        return os.path.join(self.test_udaf_directory, "test_udaf_file.py")

    @property
    def test_vectorized_udtf_py_file(self):
        return os.path.join(self.test_udtf_directory, "test_vectorized_udtf.py")

    @property
    def test_sp_directory(self):
        return os.path.join(self.resources_path, "test_sp_dir")

    @property
    def test_sp_py_file(self):
        return os.path.join(self.test_sp_directory, "test_sp_file.py")

    @property
    def test_sp_mod3_py_file(self):
        return os.path.join(self.test_sp_directory, "test_sp_mod3_file.py")

    @property
    def test_table_sp_py_file(self):
        return os.path.join(self.test_sp_directory, "test_table_sp_file.py")

    @property
    def test_pandas_udf_py_file(self):
        return os.path.join(self.test_udf_directory, "test_pandas_udf_file.py")

    @property
    def test_requirements_file(self):
        return os.path.join(self.resources_path, "test_requirements.txt")

    @property
    def test_unsupported_requirements_file(self):
        return os.path.join(self.resources_path, "test_requirements_unsupported.txt")

    @property
    def test_conda_environment_file(self):
        return os.path.join(self.resources_path, "test_environment.yml")

    @property
    def test_concat_file1_csv(self):
        return os.path.join(self.resources_path, "test_concat_file1.csv")

    @property
    def test_concat_file2_csv(self):
        return os.path.join(self.resources_path, "test_concat_file2.csv")

    @property
    def test_books_xml(self):
        return os.path.join(self.resources_path, "books.xml")

    @property
    def test_books2_xml(self):
        return os.path.join(self.resources_path, "books2.xml")

    @property
    def test_house_xml(self):
        return os.path.join(self.resources_path, "fias_house.xml")

    @property
    def test_house_large_xml(self):
        return os.path.join(self.resources_path, "fias_house.large.xml")

    @property
    def test_xxe_xml(self):
        return os.path.join(self.resources_path, "xxe.xml")

    @property
    def test_nested_xml(self):
        return os.path.join(self.resources_path, "nested.xml")

    @property
    def test_malformed_no_closing_tag_xml(self):
        return os.path.join(self.resources_path, "malformed_no_closing_tag.xml")

    @property
    def test_malformed_not_self_closing_xml(self):
        return os.path.join(self.resources_path, "malformed_not_self_closing.xml")

    @property
    def test_malformed_record_xml(self):
        return os.path.join(self.resources_path, "malformed_record.xml")

    @property
    def test_xml_declared_namespace(self):
        return os.path.join(self.resources_path, "declared_namespace.xml")

    @property
    def test_xml_undeclared_namespace(self):
        return os.path.join(self.resources_path, "undeclared_namespace.xml")

    @property
    def test_dog_image(self):
        return os.path.join(self.resources_path, "dog.jpg")


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
    TypeMap("geometry", "geometry", GeometryType()),
]


def check_tracing_span_single_answer(result: dict, expected_answer: dict):
    # this is a helper function to check one result from all results stored in exporter
    for answer_name in expected_answer:
        if answer_name == "status_description":
            if expected_answer[answer_name] not in result[answer_name]:
                return False
            else:
                continue
        if expected_answer[answer_name] != result[answer_name]:
            return False
    return True


def check_tracing_span_answers(results: list, expected_answer: tuple):
    # this function meant to check if there is one match among all the results stored in exporter
    # The answers are checked in this way because exporter is a public resource that only one exporter can
    # exist globally, which could lead to race condition if cleaning exporter after every test
    for result in results:
        if expected_answer[0] == result[0]:
            if check_tracing_span_single_answer(result[1], expected_answer[1]):
                return True
    return False


def local_to_utc_offset_in_hours():
    """In tests on CI UTC is assumed. However, when comparing dates these are returned in local timezone format.
    Adjust expectations with this localization function.
    Returns difference between UTC and current, local timezone."""
    offset = datetime.now(timezone.utc).astimezone().tzinfo.utcoffset(datetime.now())
    if offset.days < 0:
        return -(24 - offset.seconds / 3600)
    else:
        return offset.seconds / 3600


def add_to_time(t: datetime.time, delta: timedelta) -> time:
    """datetime.time does not support +, implement this here."""
    now = datetime.now()
    dt = datetime(
        now.year,
        now.month,
        now.day,
        hour=t.hour,
        minute=t.minute,
        second=t.second,
        microsecond=t.microsecond,
    )
    ans = dt + delta
    return time(ans.hour, ans.minute, ans.second, ans.microsecond)
