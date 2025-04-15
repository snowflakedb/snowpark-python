#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import json

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.exceptions import SnowparkColumnException, SnowparkSQLException
from snowflake.snowpark.functions import (
    col,
    lit,
    parse_json,
    second,
    to_timestamp,
    when,
    hour,
    minute,
    window,
)
from snowflake.snowpark.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampTimeZone,
    TimestampType,
)
from tests.utils import TestData, Utils


def test_column_constructors_subscriptable(session):
    df = session.create_dataframe([[1, 2, 3]]).to_df("col", '"col"', "col .")
    assert df.select(df["col"]).collect() == [Row(1)]
    assert df.select(df['"col"']).collect() == [Row(2)]
    assert df.select(df["col ."]).collect() == [Row(3)]
    assert df.select(df["COL"]).collect() == [Row(1)]
    assert df.select(df["CoL"]).collect() == [Row(1)]
    assert df.select(df['"COL"']).collect() == [Row(1)]

    with pytest.raises(SnowparkColumnException) as ex_info:
        df.select(df['"Col"']).collect()
    assert "The DataFrame does not contain the column" in str(ex_info)
    with pytest.raises(SnowparkColumnException) as ex_info:
        df.select(df["COL ."]).collect()
    assert "The DataFrame does not contain the column" in str(ex_info)


def test_between(session):
    df = session.create_dataframe([[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]]).to_df(
        ["a", "b"]
    )

    # between in where
    Utils.check_answer(df.where(col("a").between(lit(2), 6)), [Row(3, 4), Row(5, 6)])

    # between in select
    Utils.check_answer(
        df.select(col("a").between(lit(2), 6)),
        [Row(False), Row(True), Row(True), Row(False), Row(False)],
    )


def test_try_cast(session):
    df = session.create_dataframe([["2018-01-01"]], schema=["a"])
    cast_res = df.select(df["a"].cast("date")).collect()
    try_cast_res = df.select(df["a"].try_cast("date")).collect()
    assert cast_res[0][0] == try_cast_res[0][0] == datetime.date(2018, 1, 1)


def test_try_cast_work_cast_not_work(session, local_testing_mode):
    df = session.create_dataframe([["aaa"]], schema=["a"])
    with pytest.raises(SnowparkSQLException) as execinfo:
        df.select(df["a"].cast("date")).collect()
    if not local_testing_mode:
        assert "Date 'aaa' is not recognized" in str(execinfo)

    Utils.check_answer(
        df.select(df["a"].try_cast("date")), [Row(None)]
    )  # try_cast doesn't throw exception


def test_cast_try_cast_negative(session):
    df = session.create_dataframe([["aaa"]], schema=["a"])
    with pytest.raises(ValueError) as execinfo:
        df.select(df["a"].cast("wrong_type"))
    assert "'wrong_type' is not a supported type" in str(execinfo)
    with pytest.raises(ValueError) as execinfo:
        df.select(df["a"].try_cast("wrong_type"))
    assert "'wrong_type' is not a supported type" in str(execinfo)


@pytest.mark.parametrize("number_word", ["decimal", "number", "numeric"])
def test_cast_decimal(session, number_word):
    df = session.create_dataframe([[5.2354]], schema=["a"])
    Utils.check_answer(
        df.select(df["a"].cast(f" {number_word} ( 3, 2 ) ")), [Row(5.24)]
    )


def test_cast_map_type(session):
    df = session.create_dataframe([['{"key": "1"}']], schema=["a"])
    result = df.select(parse_json(df["a"]).cast("object")).collect()
    assert json.loads(result[0][0]) == {"key": "1"}


def test_cast_array_type(session):
    df = session.create_dataframe([["[1,2,3]"]], schema=["a"])
    result = df.select(parse_json(df["a"]).cast("array")).collect()
    assert json.loads(result[0][0]) == [1, 2, 3]


def test_startswith(session):
    Utils.check_answer(
        TestData.string4(session).select(col("a").startswith(lit("a"))),
        [Row(True), Row(False), Row(False)],
        sort=False,
    )


def test_endswith(session):
    Utils.check_answer(
        TestData.string4(session).select(col("a").endswith(lit("ana"))),
        [Row(False), Row(True), Row(False)],
        sort=False,
    )


def test_substring(session):
    Utils.check_answer(
        TestData.string4(session).select(
            col("a").substring(1, 3), col("a").substring(2, 100)
        ),
        [Row("app", "pple"), Row("ban", "anana"), Row("pea", "each")],
        sort=False,
    )

    Utils.check_answer(
        TestData.string4(session).select(
            col("a").substring(0, 3), col("a").substr(1, 3)
        ),
        [Row("app", "app"), Row("ban", "ban"), Row("pea", "pea")],
        sort=False,
    )


def test_contains(session):
    Utils.check_answer(
        TestData.string4(session).filter(col("a").contains(lit("e"))),
        [Row("apple"), Row("peach")],
        sort=False,
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="window function is not supported in Local Testing",
)
def test_internal_alias(session):
    df = session.create_dataframe(
        [[datetime.datetime(1970, 1, 1, 0, 0, 0)]], schema=["ts"]
    )
    Utils.check_answer(
        df.select(window(df.ts, "10 seconds").alias("my_alias")),
        [
            Row(
                MY_ALIAS='{\n  "end": "1970-01-01 00:00:10.000",\n  "start": "1970-01-01 00:00:00.000"\n}'
            )
        ],
    )

    Utils.check_answer(
        df.select(window(df.ts, "10 seconds").cast(StringType())),
        [Row('{"end":"1970-01-01 00:00:10.000","start":"1970-01-01 00:00:00.000"}')],
    )
    Utils.check_answer(
        df.select(second(to_timestamp(window(df.ts, "10 seconds")["end"]))),
        [Row(10)],
    )


def test_when_accept_literal_value(session):
    assert TestData.null_data1(session).select(
        when(col("a").is_null(), 5).when(col("a") == 1, 6).otherwise(7).as_("a")
    ).collect() == [Row(5), Row(7), Row(6), Row(7), Row(5)]
    assert TestData.null_data1(session).select(
        when(col("a").is_null(), 5).when(col("a") == 1, 6).else_(7).as_("a")
    ).collect() == [Row(5), Row(7), Row(6), Row(7), Row(5)]

    # empty otherwise
    assert TestData.null_data1(session).select(
        when(col("a").is_null(), 5).when(col("a") == 1, 6).as_("a")
    ).collect() == [Row(5), Row(None), Row(6), Row(None), Row(5)]


def test_logical_operator_raise_error(session):
    df = session.create_dataframe([[1, 2]], schema=["a", "b"])
    with pytest.raises(TypeError) as execinfo:
        df.filter(df.a > 1 and df.b > 1)
    assert "Cannot convert a Column object into bool" in str(execinfo)


def test_function_calls_inside_when(session):
    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("timestamp", TimestampType(TimestampTimeZone.NTZ)),
        ]
    )
    df = session.create_dataframe(
        [
            (1, datetime.datetime(2020, 1, 1, 1, 1, 1)),
            (1, datetime.datetime(2020, 1, 1, 23, 46, 1)),
            (1, datetime.datetime(2020, 1, 1, 1, 46, 1)),
        ],
        schema=schema,
    )

    df2 = df.withColumn(
        "hour_rounded",
        when(
            minute("timestamp") > 45,
            when(hour("timestamp") == 23, lit(0)).otherwise(hour("timestamp") + 1),
        ).otherwise(hour("timestamp")),
    )

    Utils.check_answer(
        df2,
        [
            Row(1, datetime.datetime(2020, 1, 1, 1, 1, 1), 1),
            Row(1, datetime.datetime(2020, 1, 1, 23, 46, 1), 0),
            Row(1, datetime.datetime(2020, 1, 1, 1, 46, 1), 2),
        ],
    )


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SQL expr is not supported in Local Testing",
    run=False,
)
def test_when_accept_sql_expr(session):
    assert TestData.null_data1(session).select(
        when("a is NULL", 5).when("a = 1", 6).otherwise(7).as_("a")
    ).collect() == [Row(5), Row(7), Row(6), Row(7), Row(5)]
    assert TestData.null_data1(session).select(
        when("a is NULL", 5).when("a = 1", 6).else_(7).as_("a")
    ).collect() == [Row(5), Row(7), Row(6), Row(7), Row(5)]

    # empty otherwise
    assert TestData.null_data1(session).select(
        when("a is NULL", 5).when("a = 1", 6).as_("a")
    ).collect() == [Row(5), Row(None), Row(6), Row(None), Row(5)]


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1358930 TODO: Decimal should not be casted to int64",
)
def test_column_with_builtins_that_shadow_functions(session):
    conversion_error_msg_text = "Cannot convert a Column object into bool"
    iter_error_msg_text = "Column is not iterable. This error can occur when you use the Python built-ins for sum, min and max. Please make sure you use the corresponding function from snowflake.snowpark.functions."
    round_error_msg_text = "Column cannot be rounded. This error can occur when you use the Python built-in round. Please make sure you use the snowflake.snowpark.functions.round function instead."

    with pytest.raises(TypeError) as ex_info:
        TestData.double1(session).select(max(col("a"), 25)).collect()
    assert conversion_error_msg_text in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        TestData.double1(session).select(max(col("a"))).collect()
    assert iter_error_msg_text in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        TestData.double1(session).select(min(col("a"), 25)).collect()
    assert conversion_error_msg_text in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        TestData.double1(session).select(min(col("a"))).collect()
    assert iter_error_msg_text in str(ex_info)

    # This works because we explicitly implement the __pow__ and __rpow__ methods
    assert TestData.double1(session).select(pow(col("a"), 2)).collect() == [
        Row(1.234321),
        Row(4.937284),
        Row(11.108889000000001),
    ]

    with pytest.raises(TypeError) as ex_info:
        TestData.double1(session).select(round(col("a"))).collect()
    assert round_error_msg_text in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        TestData.double1(session).select(sum(col("a"))).collect()
    assert iter_error_msg_text in str(ex_info)


def test_column_regex(session, local_testing_mode):
    if not session.sql_simplifier_enabled and local_testing_mode:
        pytest.skip("disable sql simplifier is not supported in local testing mode")
    df = session.create_dataframe([[1, 2, 3, 4]]).to_df(
        ["COL1", "col2_a", "col2_b", "Col3"]
    )
    Utils.check_answer(df.select(df.col_regex("col2_.*")), [Row(COL2_A=2, COL2_B=3)])
    Utils.check_answer(df.select(df.col_regex("COL2_.*")), [Row(COL2_A=2, COL2_B=3)])
    Utils.check_answer(df.select(df.col_regex("Col2_.*")), [Row(COL2_A=2, COL2_B=3)])

    Utils.check_answer(df.select(df.col_regex("COL1")), [Row(COL1=1)])
    Utils.check_answer(df.select(df.col_regex("col1")), [Row(COL1=1)])
    Utils.check_answer(df.select(df.col_regex("Col1")), [Row(COL1=1)])

    Utils.check_answer(df.select(df.col_regex("COL3")), [Row(COL3=4)])
    Utils.check_answer(df.select(df.col_regex("col3")), [Row(COL3=4)])
    Utils.check_answer(df.select(df.col_regex("Col3")), [Row(COL3=4)])

    with pytest.raises(
        ValueError, match="No columns matched for the provided regex:no_match"
    ):
        df.select(df.col_regex("no_match"))

    with pytest.raises(
        ValueError, match='No columns matched for the provided regex:Col2_.*"'
    ):
        df.select(df.col_regex('Col2_.*"'))

    case_sensitive_df = session.create_dataframe([[1, 2, 3, 4]]).to_df(
        ['"COL1"', '"Col2_a"', '"Col2_b"', '"col3"']
    )
    with pytest.raises(
        ValueError, match="No columns matched for the provided regex:Col2_.*"
    ):
        case_sensitive_df.select(
            case_sensitive_df.col_regex("Col2_.*", case_sensitive=True)
        )

    Utils.check_answer(
        case_sensitive_df.select(
            case_sensitive_df.col_regex('"Col2_.*"', case_sensitive=True)
        ).collect(),
        [Row(Col2_a=2, Col2_b=3)],
    )
    with pytest.raises(
        ValueError, match="No columns matched for the provided regex:col2_.*"
    ):
        case_sensitive_df.select(
            case_sensitive_df.col_regex("col2_.*", case_sensitive=True)
        )

    with pytest.raises(
        ValueError, match="No columns matched for the provided regex:COL2.*"
    ):
        case_sensitive_df.select(
            case_sensitive_df.col_regex("COL2_.*", case_sensitive=True)
        )

    # fail because it requires to start without double quote and end with double quote, which is an illegal regex
    with pytest.raises(
        ValueError, match='No columns matched for the provided regex:Col2_.*"'
    ):
        case_sensitive_df.select(
            case_sensitive_df.col_regex('Col2_.*"', case_sensitive=True)
        )

    Utils.check_answer(
        case_sensitive_df.select(
            case_sensitive_df.col_regex('^"Col2_.*', case_sensitive=True)
        ).collect(),
        [Row(Col2_a=2, Col2_b=3)],
    )

    # succeed because it only requires matching last double quote
    Utils.check_answer(
        case_sensitive_df.select(
            case_sensitive_df.col_regex('.*Col2_.*"', case_sensitive=True)
        ).collect(),
        [Row(Col2_a=2, Col2_b=3)],
    )
