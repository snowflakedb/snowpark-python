#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import decimal
import json
import math
import re
from itertools import chain

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import (
    _concat_ws_ignore_nulls,
    abs,
    array_agg,
    array_append,
    array_cat,
    array_compact,
    array_construct,
    array_construct_compact,
    array_contains,
    array_distinct,
    array_flatten,
    array_generate_range,
    array_insert,
    array_intersection,
    array_max,
    array_min,
    array_position,
    array_prepend,
    array_size,
    array_slice,
    array_sort,
    array_to_string,
    array_unique_agg,
    arrays_overlap,
    arrays_zip,
    as_array,
    as_binary,
    as_char,
    as_date,
    as_decimal,
    as_double,
    as_integer,
    as_number,
    as_object,
    as_real,
    as_time,
    as_timestamp_ltz,
    as_timestamp_ntz,
    as_timestamp_tz,
    as_varchar,
    asc,
    asc_nulls_first,
    asc_nulls_last,
    bitshiftright,
    bround,
    builtin,
    call_builtin,
    cast,
    ceil,
    char,
    check_json,
    check_xml,
    coalesce,
    col,
    concat,
    concat_ws,
    contains,
    count_distinct,
    create_map,
    current_date,
    current_time,
    current_timestamp,
    date_add,
    date_sub,
    dateadd,
    datediff,
    daydiff,
    desc,
    desc_nulls_first,
    desc_nulls_last,
    exp,
    floor,
    format_number,
    get,
    greatest,
    is_array,
    is_binary,
    is_char,
    is_date,
    is_decimal,
    is_double,
    is_integer,
    is_null_value,
    is_object,
    is_real,
    is_time,
    is_timestamp_ltz,
    is_timestamp_ntz,
    is_timestamp_tz,
    is_varchar,
    json_extract_path_text,
    least,
    lit,
    ln,
    log,
    month,
    months_between,
    negate,
    not_,
    object_agg,
    object_construct,
    object_construct_keep_null,
    object_delete,
    object_insert,
    object_pick,
    parse_json,
    parse_xml,
    pow,
    random,
    regexp_extract,
    regexp_replace,
    reverse,
    sequence,
    size,
    snowflake_cortex_sentiment,
    snowflake_cortex_summarize,
    split,
    sqrt,
    startswith,
    strip_null_value,
    strtok_to_array,
    struct,
    substring,
    substring_index,
    sum as sum_,
    to_array,
    to_binary,
    to_boolean,
    to_char,
    to_date,
    to_decimal,
    to_double,
    to_json,
    to_object,
    to_varchar,
    to_variant,
    to_xml,
    translate,
    trim,
    try_cast,
    uniform,
    upper,
    vector_cosine_distance,
    vector_inner_product,
    vector_l2_distance,
    year,
)
from snowflake.snowpark.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampTimeZone,
    TimestampType,
    VariantType,
)
from tests.utils import (
    IS_IN_STORED_PROC,
    TestData,
    Utils,
    running_on_jenkins,
    structured_types_enabled_session,
    structured_types_supported,
)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="querying qualified name is not supported in local testing",
)
def test_col_is_qualified_name(session):
    # 2-level deep
    df = session.sql(
        'select parse_json(\'{"firstname": "John", "lastname": "Doe"}\') as name'
    )
    Utils.check_answer(
        df.select(
            col("name.firstname", _is_qualified_name=True),
            col("name.lastname", _is_qualified_name=True),
        ),
        [Row('"John"', '"Doe"')],
    )
    Utils.check_answer(
        df.select(
            col('name."firstname"', _is_qualified_name=True),
            col('NAME."lastname"', _is_qualified_name=True),
        ),
        [Row('"John"', '"Doe"')],
    )
    Utils.check_answer(
        df.select(col("name.FIRSTNAME", _is_qualified_name=True)), [Row(None)]
    )

    # 3-level deep
    with pytest.raises(SnowparkSQLException, match="invalid identifier"):
        df.select(col("name:firstname", _is_qualified_name=True)).collect()

    with pytest.raises(SnowparkSQLException, match="invalid identifier"):
        df.select(col("name.firstname")).collect()

    df = session.sql('select parse_json(\'{"l1": {"l2": "xyz"}}\') as value')
    Utils.check_answer(
        df.select(col("value.l1.l2", _is_qualified_name=True)), Row('"xyz"')
    )
    Utils.check_answer(
        df.select(col('value."l1"."l2"', _is_qualified_name=True)), Row('"xyz"')
    )
    Utils.check_answer(
        df.select(col("value.L1.l2", _is_qualified_name=True)), Row(None)
    )
    Utils.check_answer(
        df.select(col("value.l1.L2", _is_qualified_name=True)), Row(None)
    )

    with pytest.raises(SnowparkSQLException, match="invalid identifier"):
        df.select(col("value:l1.l2", _is_qualified_name=True)).collect()

    with pytest.raises(SnowparkSQLException, match="invalid identifier"):
        df.select(col("value.l1.l2")).collect()

    # dots inside quotes should not be treated as separators
    df = session.sql('select parse_json(\'{"a.b": {"b.c": "xyz"}}\') as value')
    Utils.check_answer(
        df.select(col('value."a.b"."b.c"', _is_qualified_name=True)), Row('"xyz"')
    )
    Utils.check_answer(
        df.select(col("value.a.b.b.c", _is_qualified_name=True)).collect(), Row(None)
    )


def test_order(session):
    null_data1 = TestData.null_data1(session)
    assert null_data1.sort(asc(null_data1["A"])).collect() == [
        Row(None),
        Row(None),
        Row(1),
        Row(2),
        Row(3),
    ]
    assert null_data1.sort(asc_nulls_first(null_data1["A"])).collect() == [
        Row(None),
        Row(None),
        Row(1),
        Row(2),
        Row(3),
    ]
    assert null_data1.sort(asc_nulls_last(null_data1["A"])).collect() == [
        Row(1),
        Row(2),
        Row(3),
        Row(None),
        Row(None),
    ]
    assert null_data1.sort(desc(null_data1["A"])).collect() == [
        Row(3),
        Row(2),
        Row(1),
        Row(None),
        Row(None),
    ]
    assert null_data1.sort(desc_nulls_last(null_data1["A"])).collect() == [
        Row(3),
        Row(2),
        Row(1),
        Row(None),
        Row(None),
    ]
    assert null_data1.sort(desc_nulls_first(null_data1["A"])).collect() == [
        Row(None),
        Row(None),
        Row(3),
        Row(2),
        Row(1),
    ]


def test_current_date_and_time(session):
    max_delta = 1
    df = (
        session.create_dataframe([1, 2])
        .to_df(["a"])
        .select("a", current_date(), current_time(), current_timestamp())
    )
    rows = df.collect()

    assert len(rows) == 2, "df should contain 2 rows"
    for row in rows:
        assert isinstance(
            row[1], datetime.date
        ), f"current_date ({row[1]}) should be datetime.date type"
        assert isinstance(
            row[2], datetime.time
        ), f"current_time ({row[2]}) should be datetime.time type"
        assert isinstance(
            row[3], datetime.datetime
        ), f"current_timestamp ({row[3]}) should be datetime.datetime type"
        _, date, time, timestamp = row
        time1 = datetime.datetime.combine(date, time).timestamp()
        time2 = timestamp.timestamp()

        assert time1 == pytest.approx(
            time2, max_delta
        ), f"Times should be within {max_delta} seconds of each other."


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: regexp_replace function not supported",
)
@pytest.mark.parametrize("col_a", ["a", col("a")])
def test_regexp_replace(session, col_a):
    df = session.create_dataframe(
        [["It was the best of times, it was the worst of times"]], schema=["a"]
    )
    res = df.select(regexp_replace(col_a, lit("( ){1,}"), lit(""))).collect()
    assert res[0][0] == "Itwasthebestoftimes,itwastheworstoftimes"

    df2 = session.create_dataframe(
        [["It was the best of times, it was the worst of times"]], schema=["a"]
    )
    res = df2.select(regexp_replace(col_a, "times", "days", 1, 2, "i")).collect()
    assert res[0][0] == "It was the best of times, it was the worst of days"

    df3 = session.create_dataframe([["firstname middlename lastname"]], schema=["a"])
    res = df3.select(
        regexp_replace(col_a, lit("(.*) (.*) (.*)"), lit("\\3, \\1 \\2"))
    ).collect()
    assert res[0][0] == "lastname, firstname middlename"


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: regexp_extract function not supported",
)
def test_regexp_extract(session):
    df = session.createDataFrame([["id_20_30", 10], ["id_40_50", 30]], ["id", "age"])
    res = df.select(regexp_extract("id", r"(\d+)", 1).alias("RES")).collect()
    assert res[0]["RES"] == "20" and res[1]["RES"] == "40"
    res = df.select(regexp_extract("id", r"(\d+)_(\d+)", 2).alias("RES")).collect()
    assert res[0]["RES"] == "30" and res[1]["RES"] == "50"


@pytest.mark.parametrize(
    "col_a, col_b, col_c", [("a", "b", "c"), (col("a"), col("b"), col("c"))]
)
def test_concat(session, col_a, col_b, col_c):
    df = session.create_dataframe([["1", "2", "3"]], schema=["a", "b", "c"])
    res = df.select(concat(col_a, col_b, col_c)).collect()
    assert res[0][0] == "123"


@pytest.mark.parametrize(
    "col_a, col_b, col_c", [("a", "b", "c"), (col("a"), col("b"), col("c"))]
)
def test_concat_ws(session, col_a, col_b, col_c):
    df = session.create_dataframe([["1", "2", "3"]], schema=["a", "b", "c"])
    res = df.select(concat_ws(lit(","), col_a, col_b, col_c)).collect()
    assert res[0][0] == "1,2,3"


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="lambda function not supported",
)
@pytest.mark.parametrize("structured_type_semantics", [True, False])
def test__concat_ws_ignore_nulls(session, structured_type_semantics):
    data = [
        (["a", "b"], ["c"], "d", "e", 1, 2),  # no nulls column
        (
            ["Hello", None, "world"],
            [None, "!", None],
            "bye",
            "world",
            3,
            None,
        ),  # some nulls column
        ([None, None], ["R", "H"], None, "TD", 4, 5),  # some nulls column
        (None, [None], None, None, None, None),  # all nulls column
        (None, None, None, None, None, None),  # all nulls column
    ]
    cols = ["arr1", "arr2", "str1", "str2", "int1", "int2"]

    def check_concat_ws_ignore_nulls_output(session):
        df = session.create_dataframe(data, schema=cols)

        # single character delimiter
        Utils.check_answer(
            df.select(_concat_ws_ignore_nulls(",", *cols)),
            [
                Row("a,b,c,d,e,1,2"),
                Row("Hello,world,!,bye,world,3"),
                Row("R,H,TD,4,5"),
                Row(""),
                Row(""),
            ],
        )

        # multi-character delimiter
        Utils.check_answer(
            df.select(_concat_ws_ignore_nulls(" : ", *cols)),
            [
                Row("a : b : c : d : e : 1 : 2"),
                Row("Hello : world : ! : bye : world : 3"),
                Row("R : H : TD : 4 : 5"),
                Row(""),
                Row(""),
            ],
        )

        df = session.create_dataframe(
            [(datetime.date(2021, 12, 21),), (datetime.date(1969, 12, 31),)],
            schema=["year_month"],
        )

        Utils.check_answer(
            df.select(
                _concat_ws_ignore_nulls("-", year("year_month"), month("year_month"))
            ),
            [
                Row("2021-12"),
                Row("1969-12"),
            ],
        )

        Utils.check_answer(
            df.select(
                _concat_ws_ignore_nulls(
                    "-", year("year_month"), month("year_month")
                ).alias("year_month")
            ),
            [
                Row(YEAR_MONTH="2021-12"),
                Row(YEAR_MONTH="1969-12"),
            ],
        )

    if structured_type_semantics:
        if not structured_types_supported(session, False):
            pytest.skip("Structured type support required.")
        with structured_types_enabled_session(session) as session:
            check_concat_ws_ignore_nulls_output(session)
    else:
        check_concat_ws_ignore_nulls_output(session)


def test_concat_edge_cases(session):
    df = session.create_dataframe(
        [[None, 1, 2, 3], [4, None, 6, 7], [8, 9, None, 11], [12, 13, 14, None]]
    ).to_df(["a", "b", "c", "d"])

    single = df.select(concat("a")).collect()
    single_ws = df.select(concat_ws(lit(","), "a")).collect()
    assert single == single_ws == [Row(None), Row("4"), Row("8"), Row("12")]

    nulls = df.select(concat("a", "b", "c")).collect()
    nulls_ws = df.select(concat_ws(lit(","), "a", "b", "c")).collect()
    assert nulls == [Row(None), Row(None), Row(None), Row("121314")]
    assert nulls_ws == [Row(None), Row(None), Row(None), Row("12,13,14")]


@pytest.mark.parametrize(
    "col_a",
    ["a", col("a")],
)
@pytest.mark.parametrize(
    "data, fmt, expected",
    [
        (1, None, "1"),  # IntegerType
        (12345.6789, None, "12345.6789"),  # FloatType
        (
            decimal.Decimal("12345.6789"),
            None,
            "12345.678900000000000000",
        ),  # DecimalType
        ("abc", None, "abc"),  # StringType
        (True, None, "true"),  # BooleanType
        (None, None, None),  # NullType
        (b"123", None, "313233"),  # BinaryType + default hex encoding
        (b"123", "hex", "313233"),  # BinaryType + hex encoding
        (b"123", "base64", "MTIz"),  # BinaryType + base64 encoding
        (b"123", "utf-8", "123"),  # BinaryType + utf-8 encoding
    ],
)
@pytest.mark.parametrize("convert_func", [to_char, to_varchar])
def test_primitive_to_char(session, col_a, data, fmt, expected, convert_func):
    df = session.create_dataframe([[data]], schema=["a"])
    res = df.select(convert_func(col_a, fmt)).collect()
    assert res[0][0] == expected


@pytest.mark.parametrize("convert_func", [to_char, to_varchar])
def test_date_or_time_to_char(session, convert_func):
    # DateType
    df = session.create_dataframe(
        [datetime.date(2021, 12, 21), datetime.date(1969, 12, 31)], schema=["a"]
    )
    assert df.select(convert_func(col("a"), "mm-dd-yyyy")).collect() == [
        Row("12-21-2021"),
        Row("12-31-1969"),
    ]
    assert df.select(convert_func(col("a"))).collect() == [
        Row("2021-12-21"),
        Row("1969-12-31"),
    ]

    # TimeType
    df = session.create_dataframe(
        [datetime.time(9, 12, 56), datetime.time(22, 33, 44)], schema=["a"]
    )
    assert df.select(convert_func(col("a"), "mi:hh24:ss")).collect() == [
        Row("12:09:56"),
        Row("33:22:44"),
    ]
    assert df.select(convert_func(col("a"), "mi:hh12:ss AM")).collect() == [
        Row("12:09:56 AM"),
        Row("33:10:44 PM"),
    ]
    assert df.select(convert_func(col("a"))).collect() == [
        Row("09:12:56"),
        Row("22:33:44"),
    ]

    # TimestampType
    df = session.create_dataframe(
        [
            datetime.datetime(2021, 12, 21, 9, 12, 56),
            datetime.datetime(1969, 1, 1, 1, 1, 1),
        ],
        schema=StructType([StructField("a", TimestampType(TimestampTimeZone.NTZ))]),
    )
    assert df.select(convert_func(col("a"), "mm-dd-yyyy mi:ss:hh24")).collect() == [
        Row("12-21-2021 12:56:09"),
        Row("01-01-1969 01:01:01"),
    ]
    # we explicitly set format here because in some test env the default output format is changed
    # leading to different result
    assert df.select(convert_func(col("a"), "yyyy-mm-dd hh24:mi:ss.FF3")).collect() == [
        Row("2021-12-21 09:12:56.000"),
        Row("1969-01-01 01:01:01.000"),
    ]


@pytest.mark.parametrize("convert_func", [to_char, to_varchar])
def test_semi_structure_to_char(session, convert_func):
    assert session.create_dataframe([1]).select(
        convert_func(to_array(lit("Example"))),  # ArrayType
        convert_func(to_variant(lit("Example"))),  # VariantType
        convert_func(to_variant(lit(123))),  # VariantType
        convert_func(to_variant(lit("123"))),  # VariantType
        convert_func(to_object(parse_json(lit('{"Tree": "Pine"}')))),  # MapType
    ).collect() == [Row('["Example"]', "Example", "123", "123", '{"Tree":"Pine"}')]


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: round function not supported",
)
def test_format_number(session):
    # Create a dataframe with a column of numbers
    data = [
        (1, decimal.Decimal(3.14159)),
        (2, decimal.Decimal(2.71828)),
        (3, decimal.Decimal(1.41421)),
    ]
    df = session.createDataFrame(data, ["id", "value"])
    # Use the format_number function to format the numbers to two decimal places
    df = df.select("id", format_number("value", 2).alias("value_formatted"))
    res = df.collect()
    assert res[0].VALUE_FORMATTED == "3.14"
    assert res[1].VALUE_FORMATTED == "2.72"
    assert res[2].VALUE_FORMATTED == "1.41"


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: months_between function not supported",
)
@pytest.mark.parametrize("col_a, col_b", [("a", "b"), (col("a"), col("b"))])
def test_months_between(session, col_a, col_b):
    df = session.create_dataframe(
        [[datetime.date(2021, 12, 20), datetime.date(2021, 11, 20)]], schema=["a", "b"]
    )
    res = df.select(months_between(col_a, col_b)).collect()
    assert res[0][0] == 1.0


@pytest.mark.parametrize("col_a", ["a", col("a")])
def test_cast(session, col_a):
    df = session.create_dataframe([["2018-01-01"]], schema=["a"])
    cast_res = df.select(cast(col_a, "date")).collect()
    try_cast_res = df.select(try_cast(col_a, "date")).collect()
    assert cast_res[0][0] == try_cast_res[0][0] == datetime.date(2018, 1, 1)


@pytest.mark.parametrize("number_word", ["decimal", "number", "numeric"])
def test_cast_decimal(session, number_word):
    df = session.create_dataframe([[5.2354]], schema=["a"])
    Utils.check_answer(
        df.select(cast(df["a"], f" {number_word} ( 3, 2 ) ")), [Row(5.24)]
    )


def test_cast_map_type(session):
    df = session.create_dataframe([['{"key": "1"}']], schema=["a"])
    result = df.select(cast(parse_json(df["a"]), "object")).collect()
    assert json.loads(result[0][0]) == {"key": "1"}


def test_cast_array_type(session):
    df = session.create_dataframe([["[1,2,3]"]], schema=["a"])
    result = df.select(cast(parse_json(df["a"]), "array")).collect()
    assert json.loads(result[0][0]) == [1, 2, 3]


def test_cast_variant_type(session):
    df = session.create_dataframe([[True, 1]], schema=["a", "b"])
    Utils.check_answer(
        df.select(cast(df["a"], "variant"), cast(df["b"], "variant")),
        [Row("true", "1")],
    )


def test_to_boolean(session):
    df = session.create_dataframe(
        [[True, 1, "yes", True], [False, 0, "no", False]],
        schema=StructType(
            [
                StructField("b", BooleanType()),
                StructField("n", IntegerType()),
                StructField("s", StringType()),
                StructField("v", VariantType()),
            ]
        ),
    )
    Utils.check_answer(
        df.select(*[to_boolean(col) for col in df.columns]),
        [Row(True, True, True, True), Row(False, False, False, False)],
        sort=False,
    )

    # Invalid coercion type
    with pytest.raises(SnowparkSQLException):
        df = session.create_dataframe(
            [
                [datetime.datetime.now()],
            ],
            schema=StructType(
                [
                    StructField("t", TimestampType()),
                ]
            ),
        )
        df.select(to_boolean("t")).collect()


def test_startswith(session):
    Utils.check_answer(
        TestData.string4(session).select(col("a").startswith(lit("a"))),
        [Row(True), Row(False), Row(False)],
        sort=False,
    )


def test_struct(session):
    df = session.createDataFrame([("Bob", 80), ("Alice", None)], ["name", "age"])
    # case sensitive
    res = df.select(struct("age", "name").alias("struct")).collect(case_sensitive=True)
    #     [Row(STRUCT='{\n  "age": 80,\n  "name": "Bob"\n}'), Row(STRUCT='{\n  "age": null,\n  "name": "Alice"\n}')]
    assert len(res) == 2
    assert re.sub(r"\s", "", res[0].STRUCT) == '{"age":80,"name":"Bob"}'
    assert re.sub(r"\s", "", res[1].STRUCT) == '{"age":null,"name":"Alice"}'
    with pytest.raises(AttributeError) as field_error:
        # when case sensitive attribute will be .NAME
        print(res[0].sTruct)
    assert "Row object has no attribute sTruct" in str(field_error)
    # case insensitive
    res = df.select(struct("age", "name").alias("struct")).collect(case_sensitive=False)
    res = df.select(struct([df.AGE, df.nAme]).alias("struct")).collect(
        case_sensitive=False
    )
    print(res[0].sTruct)
    #    [Row(STRUCT='{\n  "AGE": 80,\n  "NAME": "Bob"\n}'), Row(STRUCT='{\n  "AGE": null,\n  "NAME": "Alice"\n}')]
    assert len(res) == 2
    assert re.sub(r"\s", "", res[0].STRUCT) == '{"AGE":80,"NAME":"Bob"}'
    assert re.sub(r"\s", "", res[1].STRUCT) == '{"AGE":null,"NAME":"Alice"}'
    #   [Row(STRUCT='{\n  "A": 80,\n  "B": "Bob"\n}'), Row(STRUCT='{\n  "A": null,\n  "B": "Alice"\n}')]
    res = df.select(
        struct(df.age.alias("A"), df.name.alias("B")).alias("struct")
    ).collect()
    assert len(res) == 2
    assert re.sub(r"\s", "", res[0].STRUCT) == '{"A":80,"B":"Bob"}'
    assert re.sub(r"\s", "", res[1].STRUCT) == '{"A":null,"B":"Alice"}'


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: strtok_to_array function not supported",
)
def test_strtok_to_array(session):
    # Create a dataframe
    data = [("a.b.c")]
    df = session.createDataFrame(data, ["value"])
    res = json.loads(df.select(strtok_to_array("VALUE", lit("."))).collect()[0][0])
    assert res[0] == "a" and res[1] == "b" and res[2] == "c"


@pytest.mark.parametrize("use_col", [True, False])
@pytest.mark.parametrize(
    "values,expected",
    [
        ([1, 2, 3], 3),
        ([1, None, 3], None),
        ([None, 2.0, 3], None),
        (["1.0", 2, 3], 3.0),
        ([3.1, 2, 1], 3.1),
        ([None, None, None], None),
        (["abc", "cde", "bcd"], "cde"),
    ],
)
def test_greatest(session, use_col, values, expected):
    df = session.create_dataframe([values], schema=["a", "b", "c"])
    cols = [col(c) if use_col else c for c in df.columns]
    res = df.select(greatest(*cols)).collect()
    assert res[0][0] == expected


@pytest.mark.parametrize("use_col", [True, False])
@pytest.mark.parametrize(
    "values,expected",
    [
        ([1, 2, 3], 1),
        ([1, None, 3], None),
        ([None, 2.0, 3], None),
        (["1.0", 2, 3], 1.0),
        ([3.1, 2, 1], 1.0),
        ([None, None, None], None),
        (["abc", "cde", "bcd"], "abc"),
    ],
)
def test_least(session, use_col, values, expected):
    df = session.create_dataframe([values], schema=["a", "b", "c"])
    cols = [col(c) if use_col else c for c in df.columns]
    res = df.select(least(*cols)).collect()
    assert res[0][0] == expected


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: hash function not supported",
)
@pytest.mark.parametrize("col_a, col_b", [("a", "b"), (col("a"), col("b"))])
def test_hash(session, col_a, col_b):
    df = session.create_dataframe([[10, "10"]], schema=["a", "b"])
    from snowflake.snowpark.functions import hash as snow_hash

    res = df.select(snow_hash(col_a), snow_hash(col_b)).collect()
    assert res[0][0] == 1599627706822963068
    assert res[0][1] == 3622494980440108984


def test_basic_numerical_operations_negative(session, local_testing_mode):
    # sqrt
    df = session.create_dataframe([4], schema=["a"])
    with pytest.raises(TypeError) as ex_info:
        df.select(sqrt([1])).collect()
    assert "'SQRT' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(SnowparkSQLException) as ex_info:
        df.select(sqrt(lit(-1))).collect()
    if not local_testing_mode:
        assert "Invalid floating point operation: sqrt(-1)" in str(ex_info.value)

    # abs
    with pytest.raises(TypeError) as ex_info:
        df.select(abs([None])).collect()
    assert "'ABS' expected Column or str, got: <class 'list'>" in str(ex_info)

    # exp
    with pytest.raises(TypeError) as ex_info:
        df.select(exp([None])).collect()
    assert "'EXP' expected Column or str, got: <class 'list'>" in str(ex_info)

    # log
    with pytest.raises(TypeError) as ex_info:
        df.select(log([None], "a")).collect()
    assert "'LOG' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(log("a", [123])).collect()
    assert "'LOG' expected Column or str, got: <class 'list'>" in str(ex_info)

    # pow
    with pytest.raises(TypeError) as ex_info:
        df.select(pow([None], "a")).collect()
    assert "'POW' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(pow("a", [123])).collect()
    assert "'POW' expected Column or str, got: <class 'list'>" in str(ex_info)

    # floor
    with pytest.raises(TypeError) as ex_info:
        df.select(floor([None])).collect()
    assert "'FLOOR' expected Column or str, got: <class 'list'>" in str(ex_info)

    # ceil
    with pytest.raises(TypeError) as ex_info:
        df.select(ceil([None])).collect()
    assert "'CEIL' expected Column or str, got: <class 'list'>" in str(ex_info)


def test_basic_string_operations(session, local_testing_mode):
    # Substring
    df = session.create_dataframe(["a not that long string"], schema=["a"])
    with pytest.raises(SnowparkSQLException) as ex_info:
        df.select(substring("a", "b", 1)).collect()
    assert local_testing_mode or "Numeric value 'b' is not recognized" in str(
        ex_info.value
    )

    # substring - negative length yields empty string
    res = df.select(substring("a", 6, -1)).collect()
    assert len(res) == 1
    assert len(res[0]) == 1
    assert res[0][0] == ""

    with pytest.raises(SnowparkSQLException) as ex_info:
        df.select(substring("a", 1, "c")).collect()
    assert local_testing_mode or "Numeric value 'c' is not recognized" in str(
        ex_info.value
    )

    # Split is not yet supported by local testing mode
    if not local_testing_mode:
        # split
        res = df.select(split("a", lit("not"))).collect()
        assert res == [Row("""[\n  "a ",\n  " that long string"\n]""")]

        with pytest.raises(TypeError) as ex_info:
            df.select(split([1, 2, 3], "b")).collect()
        assert "'SPLIT' expected Column or str, got: <class 'list'>" in str(
            ex_info.value
        )

        with pytest.raises(TypeError) as ex_info:
            df.select(split("a", [1, 2, 3])).collect()
        assert "'SPLIT' expected Column or str, got: <class 'list'>" in str(
            ex_info.value
        )

    # upper
    with pytest.raises(TypeError) as ex_info:
        df.select(upper([1])).collect()
    assert "'UPPER' expected Column or str, got: <class 'list'>" in str(ex_info.value)

    # contains
    with pytest.raises(TypeError) as ex_info:
        df.select(contains("a", [1])).collect()
    assert "'CONTAINS' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(contains([1], "b")).collect()
    assert "'CONTAINS' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    # startswith
    with pytest.raises(TypeError) as ex_info:
        df.select(startswith("a", [1])).collect()
    assert "'STARTSWITH' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(startswith([1], "b")).collect()
    assert "'STARTSWITH' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    # char
    with pytest.raises(TypeError) as ex_info:
        df.select(char([1])).collect()
    assert "'CHAR' expected Column or str, got: <class 'list'>" in str(ex_info.value)

    # translate
    with pytest.raises(TypeError) as ex_info:
        df.select(translate("a", "b", [1])).collect()
    assert "'TRANSLATE' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(translate("a", [1], "c")).collect()
    assert "'TRANSLATE' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(translate([1], "a", "c")).collect()
    assert "'TRANSLATE' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    # trim
    with pytest.raises(TypeError) as ex_info:
        df.select(trim("a", [1])).collect()
    assert "'TRIM' expected Column or str, got: <class 'list'>" in str(ex_info.value)

    with pytest.raises(TypeError) as ex_info:
        df.select(trim([1], "b")).collect()
    assert "'TRIM' expected Column or str, got: <class 'list'>" in str(ex_info.value)

    # reverse
    with pytest.raises(TypeError) as ex_info:
        df.select(reverse([1])).collect()
    assert "'REVERSE' expected Column or str, got: <class 'list'>" in str(ex_info.value)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: array_to_string function not supported",
)
def test_substring_index(session):
    """test calling substring_index with delimiter as string"""
    df = session.create_dataframe([[0, "a.b.c.d"], [1, ""], [2, None]], ["id", "s"])
    # substring_index when count is positive
    respos = df.select(substring_index("s", ".", 2), "id").order_by("id").collect()
    assert respos[0][0] == "a.b"
    assert respos[1][0] == ""
    assert respos[2][0] is None
    # substring_index when count is negative
    resneg = df.select(substring_index("s", ".", -3), "id").order_by("id").collect()
    assert resneg[0][0] == "b.c.d"
    assert respos[1][0] == ""
    assert respos[2][0] is None
    # substring_index when count is 0, result should be empty string
    reszero = df.select(substring_index("s", ".", 0), "id").order_by("id").collect()
    assert reszero[0][0] == ""
    assert respos[1][0] == ""
    assert respos[2][0] is None


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: array_to_string function not supported",
)
def test_substring_index_col(session):
    """test calling substring_index with delimiter as column"""
    df = session.create_dataframe([["a,b,c,d", ","]], ["s", "delimiter"])
    res = df.select(substring_index(col("s"), df["delimiter"], 2)).collect()
    assert res[0][0] == "a,b"
    res = df.select(substring_index(col("s"), col("delimiter"), 3)).collect()
    assert res[0][0] == "a,b,c"
    reslit = df.select(substring_index("s", lit(","), -3)).collect()
    assert reslit[0][0] == "b,c,d"


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: bitshitright function not supported",
)
def test_bitshiftright(session):
    # Create a dataframe
    data = [(65504), (1), (4)]
    df = session.createDataFrame(data, ["value"])
    res = df.select(bitshiftright("VALUE", 1)).collect()
    assert res[0][0] == 32752 and res[1][0] == 0 and res[2][0] == 2


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: round function not supported",
)
def test_bround(session):
    # Create a dataframe
    data = [(decimal.Decimal(1.235)), decimal.Decimal(3.5)]
    df = session.createDataFrame(data, ["VALUE"])
    res = df.select(bround("VALUE", 1)).collect()
    assert str(res[0][0]) == "1.2" and str(res[1][0]) == "3.5"
    res = df.select(bround("VALUE", 0)).collect()
    assert str(res[0][0]) == "1" and str(res[1][0]) == "4"


def test_count_distinct(session):
    df = session.create_dataframe(
        [["a", 1, 1], ["b", 2, 2], ["c", 1, None], ["d", 5, None]]
    ).to_df(["id", "value", "other"])

    res = df.select(
        count_distinct(df["id"]),
        count_distinct(df["value"]),
        count_distinct(df["other"]),
    ).collect()
    assert res == [Row(4, 3, 2)]

    res = df.select(count_distinct(df["id"], df["value"])).collect()
    assert res == [Row(4)]

    # Pass invalid type - list of numbers
    with pytest.raises(TypeError) as ex_info:
        df.select(count_distinct(123, 456))
    assert "'COUNT_DISTINCT' expected Column or str, got: <class 'int'>" in str(ex_info)

    assert df.select(count_distinct(df["*"])).collect() == [Row(2)]


def test_builtin_avg_from_range(session):
    """Tests the builtin functionality, using avg()."""
    avg = builtin("avg")

    df = session.range(1, 10, 2).select(avg(col("id")))
    res = df.collect()
    expected = [Row(5.000)]
    assert res == expected

    df = session.range(1, 10, 2).filter(col("id") > 2).select(avg(col("id")))
    res = df.collect()
    expected = [Row(6.000)]
    assert res == expected

    # Add extra select on existing column
    df = (
        session.range(1, 10, 2)
        .select("id")
        .filter(col("id") > 2)
        .select(avg(col("id")))
    )
    res = df.collect()
    expected = [Row(6.000)]
    assert res == expected

    # Add extra selects on existing column
    df = (
        session.range(1, 10, 2)
        .select("id")
        .select("id")
        .select("id")
        .select("id")
        .filter(col("id") > 2)
        .select(avg(col("id")))
    )
    res = df.collect()
    expected = [Row(6.000)]
    assert res == expected


def test_call_builtin_avg_from_range(session):
    """Tests the builtin functionality, using avg()."""
    df = session.range(1, 10, 2).select(call_builtin("avg", col("id")))
    res = df.collect()
    expected = [Row(5.000)]
    assert res == expected

    df = (
        session.range(1, 10, 2)
        .filter(col("id") > 2)
        .select(call_builtin("avg", col("id")))
    )
    res = df.collect()
    expected = [Row(6.000)]
    assert res == expected

    # Add extra select on existing column
    df = (
        session.range(1, 10, 2)
        .select("id")
        .filter(col("id") > 2)
        .select(call_builtin("avg", col("id")))
    )
    res = df.collect()
    expected = [Row(6.000)]
    assert res == expected

    # Add extra selects on existing column
    df = (
        session.range(1, 10, 2)
        .select("id")
        .select("id")
        .select("id")
        .select("id")
        .filter(col("id") > 2)
        .select(call_builtin("avg", col("id")))
    )
    res = df.collect()
    expected = [Row(6.000)]
    assert res == expected


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: is_array function not supported",
)
def test_is_negative(session):
    td = TestData.string1(session)

    # Test negative input types for __to_col_if_str
    with pytest.raises(TypeError) as ex_info:
        td.select(is_array(["a"])).collect()
    assert "'IS_ARRAY' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    with pytest.raises(TypeError) as ex_info:
        td.select(is_binary(["a"])).collect()
    assert "'IS_BINARY' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    with pytest.raises(TypeError) as ex_info:
        td.select(is_char(["a"])).collect()
    assert "'IS_CHAR' expected Column or str, got: <class 'list'>" in str(ex_info.value)

    with pytest.raises(TypeError) as ex_info:
        td.select(is_varchar(["a"])).collect()
    assert "'IS_CHAR' expected Column or str, got: <class 'list'>" in str(ex_info.value)

    with pytest.raises(TypeError) as ex_info:
        td.select(is_date(["a"])).collect()
    assert "'IS_DATE' expected Column or str, got: <class 'list'>" in str(ex_info.value)

    with pytest.raises(TypeError) as ex_info:
        td.select(is_decimal(["a"])).collect()
    assert "'IS_DECIMAL' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    with pytest.raises(TypeError) as ex_info:
        td.select(is_double(["a"])).collect()
    assert "'IS_DOUBLE' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    with pytest.raises(TypeError) as ex_info:
        td.select(is_real(["a"])).collect()
    assert "'IS_REAL' expected Column or str, got: <class 'list'>" in str(ex_info.value)

    with pytest.raises(TypeError) as ex_info:
        td.select(is_integer(["a"])).collect()
    assert "'IS_INTEGER' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    with pytest.raises(TypeError) as ex_info:
        td.select(is_null_value(["a"])).collect()
    assert "'IS_NULL_VALUE' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    with pytest.raises(TypeError) as ex_info:
        td.select(is_object(["a"])).collect()
    assert "'IS_OBJECT' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    with pytest.raises(TypeError) as ex_info:
        td.select(is_time(["a"])).collect()
    assert "'IS_TIME' expected Column or str, got: <class 'list'>" in str(ex_info.value)

    with pytest.raises(TypeError) as ex_info:
        td.select(is_timestamp_ltz(["a"])).collect()
    assert "'IS_TIMESTAMP_LTZ' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    with pytest.raises(TypeError) as ex_info:
        td.select(is_timestamp_ntz(["a"])).collect()
    assert "'IS_TIMESTAMP_NTZ' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    with pytest.raises(TypeError) as ex_info:
        td.select(is_timestamp_tz(["a"])).collect()
    assert "'IS_TIMESTAMP_TZ' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    # Test that we can only use these with variants
    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(is_array("a")).collect()
    assert "Invalid argument types for function 'IS_ARRAY'" in str(ex_info.value)

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(is_binary("a")).collect()
    assert "Invalid argument types for function 'IS_BINARY'" in str(ex_info.value)

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(is_char("a")).collect()
    assert "Invalid argument types for function 'IS_CHAR'" in str(ex_info.value)

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(is_varchar("a")).collect()
    assert "Invalid argument types for function 'IS_CHAR'" in str(ex_info.value)

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(is_date("a")).collect()
    assert "Invalid argument types for function 'IS_DATE'" in str(ex_info.value)

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(is_decimal("a")).collect()
    assert "Invalid argument types for function 'IS_DECIMAL'" in str(ex_info.value)

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(is_double("a")).collect()
    assert "Invalid argument types for function 'IS_DOUBLE'" in str(ex_info.value)

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(is_real("a")).collect()
    assert "Invalid argument types for function 'IS_REAL'" in str(ex_info.value)

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(is_integer("a")).collect()
    assert "Invalid argument types for function 'IS_INTEGER'" in str(ex_info.value)

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(is_null_value("a")).collect()
    assert "Invalid argument types for function 'IS_NULL_VALUE'" in str(ex_info.value)

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(is_object("a")).collect()
    assert "Invalid argument types for function 'IS_OBJECT'" in str(ex_info.value)

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(is_time("a")).collect()
    assert "Invalid argument types for function 'IS_TIME'" in str(ex_info.value)

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(is_timestamp_ltz("a")).collect()
    assert "Invalid argument types for function 'IS_TIMESTAMP_LTZ'" in str(
        ex_info.value
    )

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(is_timestamp_ntz("a")).collect()
    assert "Invalid argument types for function 'IS_TIMESTAMP_NTZ'" in str(
        ex_info.value
    )

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(is_timestamp_tz("a")).collect()
    assert "Invalid argument types for function 'IS_TIMESTAMP_TZ'" in str(ex_info.value)


def test_parse_json(session):
    assert TestData.null_json1(session).select(parse_json(col("v"))).collect() == [
        Row('{\n  "a": null\n}'),
        Row('{\n  "a": "foo"\n}'),
        Row(None),
    ]

    # same as above, but pass str instead of Column
    assert TestData.null_json1(session).select(parse_json("v")).collect() == [
        Row('{\n  "a": null\n}'),
        Row('{\n  "a": "foo"\n}'),
        Row(None),
    ]


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: as_array function not supported",
)
def test_as_negative(session):
    td = TestData.string1(session)

    # Test negative input types for __to_col_if_str
    with pytest.raises(TypeError) as ex_info:
        td.select(as_array(["a"])).collect()
    assert "'AS_ARRAY' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    with pytest.raises(TypeError) as ex_info:
        td.select(as_binary(["a"])).collect()
    assert "'AS_BINARY' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    with pytest.raises(TypeError) as ex_info:
        td.select(as_char(["a"])).collect()
    assert "'AS_CHAR' expected Column or str, got: <class 'list'>" in str(ex_info.value)

    with pytest.raises(TypeError) as ex_info:
        td.select(as_varchar(["a"])).collect()
    assert "'AS_VARCHAR' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    with pytest.raises(TypeError) as ex_info:
        td.select(as_date(["a"])).collect()
    assert "'AS_DATE' expected Column or str, got: <class 'list'>" in str(ex_info.value)

    with pytest.raises(TypeError) as ex_info:
        td.select(as_decimal(["a"])).collect()
    assert "'AS_DECIMAL' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    with pytest.raises(TypeError) as ex_info:
        # as_number is an alias to as_decimal.
        td.select(as_number(["a"])).collect()
    assert "'AS_DECIMAL' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    with pytest.raises(TypeError) as ex_info:
        td.select(as_double(["a"])).collect()
    assert "'AS_DOUBLE' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    with pytest.raises(TypeError) as ex_info:
        td.select(as_real(["a"])).collect()
    assert "'AS_REAL' expected Column or str, got: <class 'list'>" in str(ex_info.value)

    with pytest.raises(TypeError) as ex_info:
        td.select(as_integer(["a"])).collect()
    assert "'AS_INTEGER' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    with pytest.raises(TypeError) as ex_info:
        td.select(as_object(["a"])).collect()
    assert "'AS_OBJECT' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    with pytest.raises(TypeError) as ex_info:
        td.select(as_time(["a"])).collect()
    assert "'AS_TIME' expected Column or str, got: <class 'list'>" in str(ex_info.value)

    with pytest.raises(TypeError) as ex_info:
        td.select(as_timestamp_ltz(["a"])).collect()
    assert "'AS_TIMESTAMP_LTZ' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    with pytest.raises(TypeError) as ex_info:
        td.select(as_timestamp_ntz(["a"])).collect()
    assert "'AS_TIMESTAMP_NTZ' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    with pytest.raises(TypeError) as ex_info:
        td.select(as_timestamp_tz(["a"])).collect()
    assert "'AS_TIMESTAMP_TZ' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )

    # Test that we can only use these with variants
    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(as_array("a")).collect()
    assert "Invalid argument types for function 'AS_ARRAY'" in str(ex_info.value)

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(as_binary("a")).collect()
    assert "Invalid argument types for function 'AS_BINARY'" in str(ex_info.value)

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(as_char("a")).collect()
    assert "Invalid argument types for function 'AS_CHAR'" in str(ex_info.value)

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(as_varchar("a")).collect()
    assert "Invalid argument types for function 'AS_VARCHAR'" in str(ex_info.value)

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(as_date("a")).collect()
    assert "Invalid argument types for function 'AS_DATE'" in str(ex_info.value)

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(as_decimal("a")).collect()
    assert (
        "invalid type [VARCHAR(5)] for parameter 'AS_DECIMAL(variantValue...)'"
        in str(ex_info.value)
    )

    with pytest.raises(ValueError) as ex_info:
        td.select(as_decimal("a", None, 3)).collect()
    assert "Cannot define scale without precision" in str(ex_info.value)

    with pytest.raises(SnowparkSQLException) as ex_info:
        TestData.variant1(session).select(as_decimal(col("decimal1"), -1)).collect()
    assert "invalid value [-1] for parameter 'AS_DECIMAL(?, precision...)'" in str(
        ex_info.value
    )

    with pytest.raises(SnowparkSQLException) as ex_info:
        TestData.variant1(session).select(as_decimal(col("decimal1"), 6, -1)).collect()
    assert "invalid value [-1] for parameter 'AS_DECIMAL(?, ?, scale)'" in str(
        ex_info.value
    )

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(as_number("a")).collect()
    assert (
        "invalid type [VARCHAR(5)] for parameter 'AS_DECIMAL(variantValue...)'"
        in str(ex_info.value)
    )

    with pytest.raises(ValueError) as ex_info:
        td.select(as_number("a", None, 3)).collect()
    assert "Cannot define scale without precision" in str(ex_info.value)

    with pytest.raises(SnowparkSQLException) as ex_info:
        TestData.variant1(session).select(as_number(col("decimal1"), -1)).collect()
    assert "invalid value [-1] for parameter 'AS_DECIMAL(?, precision...)'" in str(
        ex_info.value
    )

    with pytest.raises(SnowparkSQLException) as ex_info:
        TestData.variant1(session).select(as_number(col("decimal1"), 6, -1)).collect()
    assert "invalid value [-1] for parameter 'AS_DECIMAL(?, ?, scale)'" in str(
        ex_info.value
    )

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(as_double("a")).collect()
    assert "Invalid argument types for function 'AS_DOUBLE'" in str(ex_info.value)

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(as_real("a")).collect()
    assert "Invalid argument types for function 'AS_REAL'" in str(ex_info.value)

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(as_integer("a")).collect()
    assert (
        "invalid type [VARCHAR(5)] for parameter 'AS_INTEGER(variantValue...)'"
        in str(ex_info.value)
    )

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(as_object("a")).collect()
    assert "Invalid argument types for function 'AS_OBJECT'" in str(ex_info.value)

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(as_time("a")).collect()
    assert "Invalid argument types for function 'AS_TIME'" in str(ex_info.value)

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(as_timestamp_ltz("a")).collect()
    assert (
        "invalid type [VARCHAR(5)] for parameter 'AS_TIMESTAMP_LTZ(variantValue...)'"
        in str(ex_info.value)
    )

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(as_timestamp_ntz("a")).collect()
    assert (
        "invalid type [VARCHAR(5)] for parameter 'AS_TIMESTAMP_NTZ(variantValue...)'"
        in str(ex_info.value)
    )

    with pytest.raises(SnowparkSQLException) as ex_info:
        td.select(as_timestamp_tz("a")).collect()
    assert (
        "invalid type [VARCHAR(5)] for parameter 'AS_TIMESTAMP_TZ(variantValue...)'"
        in str(ex_info.value)
    )


def test_to_date_to_array_to_variant_to_object(session, local_testing_mode):
    df = (
        session.create_dataframe(
            [["2013-05-17", 1, 3.14, '{"a":1}'], [None, None, None, None]]
        )
        .to_df("date", "array", "var", "obj")
        .with_column("json", parse_json("obj"))
    )

    df1 = df.select(
        to_date("date"), to_array("array"), to_variant("var"), to_object("json")
    )

    expected_pi_repr = "3.14" if local_testing_mode else "3.140000000000000e+00"

    expected = [
        Row(
            datetime.date(2013, 5, 17), "[\n  1\n]", expected_pi_repr, '{\n  "a": 1\n}'
        ),
        Row(None, None, None, None),
    ]

    res1 = df1.collect()
    Utils.assert_rows(res1, expected)

    assert df1.schema.fields[0].datatype == DateType()
    assert df1.schema.fields[1].datatype == ArrayType()
    assert df1.schema.fields[2].datatype == VariantType()
    assert df1.schema.fields[3].datatype == MapType()


def test_to_binary(session):
    res = (
        TestData.test_data1(session)
        .to_df("a", "b", "c")
        .select(to_binary(col("c"), "utf-8"))
        .collect()
    )
    assert res == [Row(bytearray(b"a")), Row(bytearray(b"b"))]

    res = (
        TestData.test_data1(session)
        .to_df("a", "b", "c")
        .select(to_binary("c", "utf-8"))
        .collect()
    )
    assert res == [Row(bytearray(b"a")), Row(bytearray(b"b"))]

    # For NULL input, the output is NULL
    res = TestData.all_nulls(session).to_df("a").select(to_binary(col("a"))).collect()
    assert res == [Row(None), Row(None), Row(None), Row(None)]


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: array_construct function not supported",
)
def test_array_min_max_functions(session):
    # array_min
    df = session.create_dataframe([1]).select(
        array_construct(lit(20), lit(0), lit(None), lit(10)).alias("a")
    )
    res = df.select(array_min(df["a"]).as_("min_a")).collect()
    assert res == [Row(MIN_A="0")]

    df = session.create_dataframe([1]).select(array_construct().alias("A"))
    res = df.select(array_min(df.a).as_("min_a")).collect()
    assert res == [Row(MIN_A=None)]

    df = session.create_dataframe([1]).select(
        array_construct(lit(None), lit(None), lit(None)).alias("a")
    )
    res = df.select(array_min(df.a).as_("min_a")).collect()
    assert res == [Row(MIN_A=None)]

    df = session.create_dataframe([[[None, None, None]]], schema=["a"])
    res = df.select(array_min(df.a).as_("min_a")).collect()
    assert res == [Row(MIN_A="null")]

    # array_max
    df = session.create_dataframe([1]).select(
        array_construct(lit(20), lit(0), lit(None), lit(10)).alias("a")
    )
    res = df.select(array_max(df.a).as_("max_a")).collect()
    assert res == [Row(MAX_A="20")]

    df = session.create_dataframe([1]).select(array_construct().alias("A"))
    res = df.select(array_max(df.a).as_("max_a")).collect()
    assert res == [Row(MAX_A=None)]

    df = session.create_dataframe([1]).select(
        array_construct(lit(None), lit(None), lit(None)).alias("A")
    )
    res = df.select(array_max(df.a).as_("max_a")).collect()
    assert res == [Row(MAX_A=None)]

    df = session.create_dataframe([[[None, None, None]]], schema=["A"])
    res = df.select(array_max(df.a).as_("max_a")).collect()
    assert res == [Row(MAX_A="null")]


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: array_flatten function not supported",
)
def test_array_flatten(session):
    df = session.create_dataframe(
        [
            [[[1, 2, 3], [None], [4, 5]]],
        ],
        schema=["a"],
    )
    df = df.select(array_flatten(df.a).as_("flatten_a"))
    Utils.check_answer(
        df,
        [Row(FLATTEN_A="[\n  1,\n  2,\n  3,\n  null,\n  4,\n  5\n]")],
    )

    df = session.create_dataframe(
        [
            [[[[1, 2], [3]]]],
        ],
        schema=["a"],
    )
    df = df.select(array_flatten(df.a).as_("flatten_a"))
    Utils.check_answer(
        df,
        [Row(FLATTEN_A="[\n  [\n    1,\n    2\n  ],\n  [\n    3\n  ]\n]")],
    )

    df = session.sql("select [[1, 2], null, [3]] as A")
    df = df.select(array_flatten(df.a).as_("flatten_a"))
    Utils.check_answer(
        df,
        [Row(FLATTEN_A=None)],
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: arrays_zip function not supported",
)
@pytest.mark.parametrize(
    "data, expected",
    [
        (
            [([1, 2], ["a", "b"])],
            [
                Row(
                    ZIPPED='[\n  {\n    "$1": 1,\n    "$2": "a"\n  },\n  {\n    "$1": 2,\n    "$2": "b"\n  }\n]'
                )
            ],
        ),
        (
            [([1, 2], ["a", "b", "c"])],
            [
                Row(
                    ZIPPED='[\n  {\n    "$1": 1,\n    "$2": "a"\n  },\n  {\n    "$1": 2,\n    "$2": "b"\n  },\n  {\n    "$1": null,\n    "$2": "c"\n  }\n]'
                )
            ],
        ),
        (
            [([1, 2], ["a", "b"], [10.1, 10.2])],
            [
                Row(
                    ZIPPED='[\n  {\n    "$1": 1,\n    "$2": "a",\n    "$3": 10.1\n  },\n  {\n    "$1": 2,\n    "$2": "b",\n    "$3": 10.2\n  }\n]'
                )
            ],
        ),
    ],
)
def test_arrays_zip(session, data, expected):
    df = session.create_dataframe(data)
    df = df.select(arrays_zip(*df.columns).as_("zipped"))

    Utils.check_answer(
        df, expected, statement_params={"enable_arrays_zip_function": "TRUE"}
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: array_construct function not supported",
)
def test_array_sort(session):
    # Behavior with SQL nulls:
    df = session.create_dataframe([1]).select(
        array_construct(lit(20), lit(0), lit(None), lit(10)).alias("a")
    )

    res = df.select(array_sort(df.a).as_("sorted_a")).collect()
    Utils.check_answer(res, [Row(SORTED_A="[\n  0,\n  10,\n  20,\n  undefined\n]")])

    res = df.select(array_sort(df.a, False).as_("sorted_a")).collect()
    Utils.check_answer(res, [Row(SORTED_A="[\n  20,\n  10,\n  0,\n  undefined\n]")])

    res = df.select(array_sort(df.a, False, True).as_("sorted_a")).collect()
    Utils.check_answer(res, [Row(SORTED_A="[\n  undefined,\n  20,\n  10,\n  0\n]")])

    # Behavior with JSON nulls:
    df = session.create_dataframe([[[20, 0, None, 10]]], schema=["a"])
    res = df.select(array_sort(df.a, False, False).as_("sorted_a")).collect()
    Utils.check_answer(res, [Row(SORTED_A="[\n  null,\n  20,\n  10,\n  0\n]")])
    res = df.select(array_sort(df.a, False, True).as_("sorted_a")).collect()
    Utils.check_answer(res, [Row(SORTED_A="[\n  null,\n  20,\n  10,\n  0\n]")])


@pytest.mark.xfail(reason="SNOW-974852 vectors are not yet rolled out", strict=False)
def test_vector_distances(session):
    df = session.sql("select [1,2,3]::vector(int,3) as a, [2,3,4]::vector(int,3) as b")

    res = df.select(vector_cosine_distance(df.a, df.b).as_("distance")).collect()
    Utils.check_answer(
        res, [Row(DISTANCE=20 / ((1 + 4 + 9) ** 0.5 * (4 + 9 + 16) ** 0.5))]
    )

    res = df.select(vector_l2_distance(df.a, df.b).as_("distance")).collect()
    Utils.check_answer(res, [Row(DISTANCE=(1 + 1 + 1) ** 0.5)])

    res = df.select(vector_inner_product(df.a, df.b).as_("distance")).collect()
    Utils.check_answer(res, [Row(DISTANCE=20)])

    df = session.sql(
        "select [1.1,2.2]::vector(float,2) as a, [2.2,3.3]::vector(float,2) as b"
    )
    res = df.select(vector_cosine_distance(df.a, df.b).as_("distance")).collect()
    inner_product = 1.1 * 2.2 + 2.2 * 3.3
    Utils.check_answer(
        res,
        [
            Row(
                DISTANCE=inner_product
                / ((1.1**2 + 2.2**2) ** 0.5 * (2.2**2 + 3.3**2) ** 0.5)
            )
        ],
        float_equality_threshold=0.0005,
    )

    res = df.select(vector_l2_distance(df.a, df.b).as_("distance")).collect()
    Utils.check_answer(
        res,
        [Row(DISTANCE=(1.1**2 + 1.1**2) ** 0.5)],
        float_equality_threshold=0.0005,
    )

    res = df.select(vector_inner_product(df.a, df.b).as_("distance")).collect()
    Utils.check_answer(
        res, [Row(DISTANCE=inner_product)], float_equality_threshold=0.0005
    )


def test_coalesce(session):
    # Taken from FunctionSuite.scala
    Utils.check_answer(
        TestData.null_data2(session).select(coalesce("A", "B", "C")),
        [Row(1), Row(2), Row(3), Row(None), Row(1), Row(1), Row(1)],
        sort=False,
    )

    # single input column
    with pytest.raises(SnowparkSQLException) as ex_info:
        TestData.null_data2(session).select(coalesce(col("A"))).collect()
    assert "not enough arguments for function [COALESCE" in str(ex_info.value)

    with pytest.raises(TypeError) as ex_info:
        TestData.null_data2(session).select(coalesce(["A", "B", "C"]))
    assert "'COALESCE' expected Column or str, got: <class 'list'>" in str(
        ex_info.value
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: uniform function not supported",
)
def test_uniform(session):
    df = session.create_dataframe([1], schema=["a"])

    # both intervals are ints
    int_uniform = df.select(uniform(1, 100, random()).alias("X")).collect()[0]
    assert isinstance(int_uniform.as_dict()["X"], int)

    # both intervals are decimals
    float_uniform0 = df.select(uniform(1.0, 100.0, random()).alias("X")).collect()[0]
    assert isinstance(float_uniform0.as_dict()["X"], float)

    # min interval is decimal
    float_uniform1 = df.select(uniform(1.0, 100, random()).alias("X")).collect()[0]
    assert isinstance(float_uniform1.as_dict()["X"], float)

    # max interval is decimal
    float_uniform2 = df.select(uniform(1, 100.0, random()).alias("X")).collect()[0]
    assert isinstance(float_uniform2.as_dict()["X"], float)

    # float intervals are same explicit float interval queries
    explicit = df.select(
        uniform(
            lit(-1.0).cast(FloatType()), lit(1.0).cast(FloatType()), col("a")
        ).alias("X")
    ).collect()[0]
    non_explicit = df.select(uniform(-1.0, 1.0, col("a")).alias("X")).collect()[0]
    assert explicit == non_explicit

    # mix of decimal and int give same result as decimal and decimal
    decimal_int = df.select(uniform(-10.0, 10, col("a")).alias("X")).collect()[0]
    decimal_decimal = df.select(uniform(-10.0, 10.0, col("a")).alias("X")).collect()[0]
    assert decimal_int == decimal_decimal


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: uniform function not supported",
)
def test_uniform_negative(session):
    df = session.create_dataframe([1], schema=["a"])
    with pytest.raises(SnowparkSQLException) as ex_info:
        df.select(uniform(lit("z"), 11, random())).collect()
    assert "Numeric value 'z' is not recognized" in str(ex_info.value)


def test_negate_and_not_negative(session):
    with pytest.raises(TypeError) as ex_info:
        TestData.null_data2(session).select(negate(["A", "B", "C"]))
    assert "'NEGATE' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        TestData.null_data2(session).select(not_(["A", "B", "C"]))
    assert "'NOT_' expected Column or str, got: <class 'list'>" in str(ex_info)


def test_random_negative(session, local_testing_mode):
    df = session.create_dataframe([1], schema=["a"])

    with pytest.raises(SnowparkSQLException) as ex_info:
        df.select(random("abc")).collect()
    err_str = (
        "Error executing mocked function 'random'"
        if local_testing_mode
        else "Numeric value 'abc' is not recognized"
    )
    assert err_str in str(ex_info.value)


def test_check_functions_negative(session):
    df = session.create_dataframe([1], schema=["a"])

    # check_json
    with pytest.raises(TypeError) as ex_info:
        df.select(check_json([1])).collect()
    assert "'CHECK_JSON' expected Column or str, got: <class 'list'>" in str(ex_info)

    # check_xml
    with pytest.raises(TypeError) as ex_info:
        df.select(check_xml([1])).collect()
    assert "'CHECK_XML' expected Column or str, got: <class 'list'>" in str(ex_info)


def test_parse_functions_negative(session):
    df = session.create_dataframe([1], schema=["a"])

    # parse_json
    with pytest.raises(TypeError) as ex_info:
        df.select(parse_json([1])).collect()
    assert "'PARSE_JSON' expected Column or str, got: <class 'list'>" in str(ex_info)

    # parse_xml
    with pytest.raises(TypeError) as ex_info:
        df.select(parse_xml([1])).collect()
    assert "'PARSE_XML' expected Column or str, got: <class 'list'>" in str(ex_info)


def test_json_functions_negative(session):
    df = session.create_dataframe([1], schema=["a"])

    # json_extract_path_text
    with pytest.raises(TypeError) as ex_info:
        df.select(json_extract_path_text([1], "a")).collect()
    assert (
        "'JSON_EXTRACT_PATH_TEXT' expected Column or str, got: <class 'list'>"
        in str(ex_info)
    )

    # strip_null_value
    with pytest.raises(TypeError) as ex_info:
        df.select(strip_null_value([1])).collect()
    assert "'STRIP_NULL_VALUE' expected Column or str, got: <class 'list'>" in str(
        ex_info
    )


def test_to_filetype_negative(session):
    df = session.create_dataframe([1], schema=["a"])
    # to_json
    with pytest.raises(TypeError) as ex_info:
        df.select(to_json([1])).collect()
    assert "'TO_JSON' expected Column or str, got: <class 'list'>" in str(ex_info)

    # to_xml
    with pytest.raises(TypeError) as ex_info:
        df.select(to_xml([1])).collect()
    assert "'TO_XML' expected Column or str, got: <class 'list'>" in str(ex_info)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: array_construct function not supported",
)
def test_array_distinct(session):
    df = session.create_dataframe([1], schema=["a"])
    df = df.withColumn(
        "array", array_construct(lit(1), lit(1), lit(1), lit(2), lit(3), lit(2), lit(2))
    )
    res = df.withColumn("array_d", array_distinct("ARRAY")).collect()
    assert len(res) == 1
    array = eval(res[0][2])
    assert len(array) == 3
    assert array[0] == 1 and array[1] == 2 and array[2] == 3


def test_array_negative(session):
    df = session.create_dataframe([1], schema=["a"])

    with pytest.raises(TypeError) as ex_info:
        df.select(array_agg([1])).collect()
    assert "'ARRAY_AGG' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(array_append([1], "column")).collect()
    assert "'ARRAY_APPEND' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(array_cat([1], "column")).collect()
    assert "'ARRAY_CAT' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(array_compact([1])).collect()
    assert "'ARRAY_COMPACT' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(array_construct([1])).collect()
    assert "'ARRAY_CONSTRUCT' expected Column or str, got: <class 'list'>" in str(
        ex_info
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(array_construct_compact([1])).collect()
    assert (
        "'ARRAY_CONSTRUCT_COMPACT' expected Column or str, got: <class 'list'>"
        in str(ex_info)
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(array_contains([1], "column")).collect()
    assert "'ARRAY_CONTAINS' expected Column or str, got: <class 'list'>" in str(
        ex_info
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(array_insert([1], lit(3), "column")).collect()
    assert "'ARRAY_INSERT' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(array_generate_range([1], "column")).collect()
    assert "'ARRAY_GENERATE_RANGE' expected Column or str, got: <class 'list'>" in str(
        ex_info
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(array_position([1], "column")).collect()
    assert "'ARRAY_POSITION' expected Column or str, got: <class 'list'>" in str(
        ex_info
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(array_prepend([1], "column")).collect()
    assert "'ARRAY_PREPEND' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(array_size([1])).collect()
    assert "'ARRAY_SIZE' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(
        TypeError, match="'SIZE' expected Column or str, got: <class 'list'>"
    ):
        df.select(size([1])).collect()

    with pytest.raises(TypeError) as ex_info:
        df.select(array_slice([1], "col1", "col2")).collect()
    assert "'ARRAY_SLICE' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(array_to_string([1], "column")).collect()
    assert "'ARRAY_TO_STRING' expected Column or str, got: <class 'list'>" in str(
        ex_info
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(arrays_overlap([1], "column")).collect()
    assert "'ARRAYS_OVERLAP' expected Column or str, got: <class 'list'>" in str(
        ex_info
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(array_intersection([1], "column")).collect()
    assert "'ARRAY_INTERSECTION' expected Column or str, got: <class 'list'>" in str(
        ex_info
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(array_unique_agg([1])).collect()
    assert "'ARRAY_UNIQUE_AGG' expected Column or str, got: <class 'list'>" in str(
        ex_info
    )


def test_object_negative(session):
    df = session.create_dataframe([1], schema=["a"])

    with pytest.raises(TypeError) as ex_info:
        df.select(object_agg([1], "column")).collect()
    assert "'OBJECT_AGG' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(object_construct([1])).collect()
    assert "'OBJECT_CONSTRUCT' expected Column or str, got: <class 'list'>" in str(
        ex_info
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(object_construct_keep_null([1], "column")).collect()
    assert (
        "'OBJECT_CONSTRUCT_KEEP_NULL' expected Column or str, got: <class 'list'>"
        in str(ex_info)
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(object_delete([1], "column", "col1", "col2")).collect()
    assert "'OBJECT_DELETE' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(object_insert([1], "key", "key1")).collect()
    assert "'OBJECT_INSERT' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(object_pick([1], "key", "key1")).collect()
    assert "'OBJECT_PICK' expected Column or str, got: <class 'list'>" in str(ex_info)


def test_date_operations_negative(session):
    df = session.create_dataframe([1], schema=["a"])

    with pytest.raises(TypeError) as ex_info:
        df.select(datediff("year", [1], "col")).collect()
    assert "'DATEDIFF' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(dateadd("year", [1], "col")).collect()
    assert "'DATEADD' expected Column or str, got: <class 'list'>" in str(ex_info)


def test_date_add_date_sub(session):
    df = session.createDataFrame(
        [
            ("2019-01-23"),
            ("2019-06-24"),
            ("2019-09-20"),
            (None),
        ],
        ["date"],
    )
    df = df.withColumn("date", to_date("date"))
    assert df.withColumn("date", date_add("date", 4)).collect() == [
        Row(datetime.date(2019, 1, 27)),
        Row(datetime.date(2019, 6, 28)),
        Row(datetime.date(2019, 9, 24)),
        Row(None),
    ]
    assert df.withColumn("date", date_sub("date", 4)).collect() == [
        Row(datetime.date(2019, 1, 19)),
        Row(datetime.date(2019, 6, 20)),
        Row(datetime.date(2019, 9, 16)),
        Row(None),
    ]


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: datediff function not supported",
)
def test_daydiff(session):
    df = session.createDataFrame([("2015-04-08", "2015-05-10")], ["d1", "d2"])
    res = df.select(daydiff(to_date(df.d2), to_date(df.d1)).alias("diff")).collect()
    assert res[0].DIFF == 32


def test_get_negative(session):
    df = session.create_dataframe([1], schema=["a"])

    with pytest.raises(TypeError) as ex_info:
        df.select(get([1], 1)).collect()
    assert "'GET' expected Column, int or str, got: <class 'list'>" in str(ex_info)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: array_generate_range function not supported",
)
def test_array_generate_range(session):
    df = session.createDataFrame([(-2, 2)], ["C1", "C2"])
    Utils.check_answer(
        df.select(array_generate_range("C1", "C2").alias("r")),
        [Row(R="[\n  -2,\n  -1,\n  0,\n  1\n]")],
        sort=False,
    )

    df = session.createDataFrame([(4, -4, -2)], ["C1", "C2", "C3"])
    Utils.check_answer(
        df.select(array_generate_range("C1", "C2", "C3").alias("r")),
        [Row(R="[\n  4,\n  2,\n  0,\n  -2\n]")],
        sort=False,
    )

    df = session.createDataFrame([(2, -2)], ["C1", "C2"])
    Utils.check_answer(
        df.select(array_generate_range("C1", "C2").alias("r")),
        [Row(R="[]")],
        sort=False,
    )

    df = session.createDataFrame([(-2.0, 3.3)], ["C1", "C2"])
    Utils.check_answer(
        df.select(array_generate_range("C1", "C2").alias("r")),
        [Row(R="[\n  -2,\n  -1,\n  0,\n  1,\n  2\n]")],
        sort=False,
    )


def test_sequence_negative(session):
    df = session.create_dataframe([1], schema=["a"])

    with pytest.raises(TypeError) as ex_info:
        df.select(sequence([1], 1)).collect()
    assert "'SEQUENCE' expected Column or str, got: <class 'list'>" in str(ex_info)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: array_generate_range function not supported",
)
def test_sequence(session):
    df = session.createDataFrame([(-2, 2)], ["C1", "C2"])
    Utils.check_answer(
        df.select(sequence("C1", "C2").alias("r")),
        [Row(R="[\n  -2,\n  -1,\n  0,\n  1,\n  2\n]")],
        sort=False,
    )

    df = session.createDataFrame([(4, -4, -2)], ["C1", "C2", "C3"])
    Utils.check_answer(
        df.select(sequence("C1", "C2", "C3").alias("r")),
        [Row(R="[\n  4,\n  2,\n  0,\n  -2,\n  -4\n]")],
        sort=False,
    )

    df = session.createDataFrame([(0, 5, 4)], ["C1", "C2", "C3"])
    Utils.check_answer(
        df.select(sequence("C1", "C2", "C3").alias("r")),
        [Row(R="[\n  0,\n  4\n]")],
        sort=False,
    )

    df = session.createDataFrame([(-5, 0, 4)], ["C1", "C2", "C3"])
    Utils.check_answer(
        df.select(sequence("C1", "C2", "C3").alias("r")),
        [Row(R="[\n  -5,\n  -1\n]")],
        sort=False,
    )

    df = session.createDataFrame([(2, -2)], ["C1", "C2"])
    Utils.check_answer(
        df.select(sequence("C1", "C2").alias("r")),
        [Row(R="[\n  2,\n  1,\n  0,\n  -1,\n  -2\n]")],
        sort=False,
    )

    df = session.createDataFrame([(-2.0, 3.3)], ["C1", "C2"])
    Utils.check_answer(
        df.select(sequence("C1", "C2").alias("r")),
        [Row(R="[\n  -2,\n  -1,\n  0,\n  1,\n  2,\n  3\n]")],
        sort=False,
    )

    df = session.createDataFrame([(-2, -2)], ["C1", "C2"])
    Utils.check_answer(
        df.select(sequence("C1", "C2").alias("r")),
        [Row(R="[\n  -2\n]")],
        sort=False,
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: array_unqite_agg function not supported",
)
def test_array_unique_agg(session):
    def _result_str2lst(result):
        col_str = result[0][0]
        col_lst = [int(i) for i in re.sub(r"[\[|\]|,]", " ", col_str).strip().split()]
        col_lst.sort()
        return col_lst

    df1 = session.create_dataframe([[1], [2], [5], [2], [1]], schema=["a"])
    result_str = df1.select(array_unique_agg("a").alias("result")).collect()
    result_list = _result_str2lst(result_str)
    expected_result = [1, 2, 5]
    assert (
        result_list == expected_result
    ), f"Unexpected result: {result_list}, expected: {expected_result}"

    result_col = df1.select(array_unique_agg(col("a")).alias("result")).collect()
    result_list = _result_str2lst(result_col)
    assert (
        result_list == expected_result
    ), f"Unexpected result: {result_list}, expected: {expected_result}"

    df2 = session.create_dataframe([[1], [2], [None], [2], [None]], schema=["a"])
    result_str = df2.select(array_unique_agg("a").alias("result")).collect()
    result_list = _result_str2lst(result_str)
    expected_result = [1, 2]
    assert (
        result_list == expected_result
    ), f"Unexpected result: {result_list}, expected: {expected_result}"


def test_create_map(session):
    df = session.create_dataframe(
        [("Sales", 6500, "USA"), ("Legal", 3000, None)],
        ("department", "salary", "location"),
    )

    # Case 1: create_map with column names
    Utils.check_answer(
        df.select(create_map("department", "salary").alias("map")),
        [Row(MAP='{\n  "Sales": 6500\n}'), Row(MAP='{\n  "Legal": 3000\n}')],
        sort=False,
    )

    # Case 2: create_map with column objects
    Utils.check_answer(
        df.select(create_map(df.department, df.salary).alias("map")),
        [Row(MAP='{\n  "Sales": 6500\n}'), Row(MAP='{\n  "Legal": 3000\n}')],
        sort=False,
    )

    # Case 3: create_map with a list of column names
    Utils.check_answer(
        df.select(create_map(["department", "salary"]).alias("map")),
        [Row(MAP='{\n  "Sales": 6500\n}'), Row(MAP='{\n  "Legal": 3000\n}')],
        sort=False,
    )

    # Case 4: create_map with a list of column objects
    Utils.check_answer(
        df.select(create_map([df.department, df.salary]).alias("map")),
        [Row(MAP='{\n  "Sales": 6500\n}'), Row(MAP='{\n  "Legal": 3000\n}')],
        sort=False,
    )

    # Case 5: create_map with constant values
    Utils.check_answer(
        df.select(
            create_map(
                lit("department"), col("department"), lit("salary"), col("salary")
            ).alias("map")
        ),
        [
            Row(MAP='{\n  "department": "Sales",\n  "salary": 6500\n}'),
            Row(MAP='{\n  "department": "Legal",\n  "salary": 3000\n}'),
        ],
        sort=False,
    )

    # Case 6: create_map with a nested map
    Utils.check_answer(
        df.select(
            create_map(
                col("department"), create_map(lit("salary"), col("salary"))
            ).alias("map")
        ),
        [
            Row(MAP='{\n  "Sales": {\n    "salary": 6500\n  }\n}'),
            Row(MAP='{\n  "Legal": {\n    "salary": 3000\n  }\n}'),
        ],
        sort=False,
    )

    # Case 7: create_map with None values
    Utils.check_answer(
        df.select(create_map("department", "location").alias("map")),
        [Row(MAP='{\n  "Sales": "USA"\n}'), Row(MAP='{\n  "Legal": null\n}')],
        sort=False,
    )

    # Case 8: create_map dynamic creation
    Utils.check_answer(
        df.select(
            create_map(
                list(chain(*((lit(name), col(name)) for name in df.columns)))
            ).alias("map")
        ),
        [
            Row(
                MAP='{\n  "DEPARTMENT": "Sales",\n  "LOCATION": "USA",\n  "SALARY": 6500\n}'
            ),
            Row(
                MAP='{\n  "DEPARTMENT": "Legal",\n  "LOCATION": null,\n  "SALARY": 3000\n}'
            ),
        ],
        sort=False,
    )

    # Case 9: create_map without columns
    Utils.check_answer(
        df.select(create_map().alias("map")),
        [Row(MAP="{}"), Row(MAP="{}")],
        sort=False,
    )


def test_create_map_negative(session):
    df = session.create_dataframe(
        [("Sales", 6500, "USA"), ("Legal", 3000, None)],
        ("department", "salary", "location"),
    )

    # Case 1: create_map with odd number of columns
    with pytest.raises(ValueError) as ex_info:
        df.select(create_map("department").alias("map"))
    assert (
        "The 'create_map' function requires an even number of parameters but the actual number is 1"
        in str(ex_info)
    )

    # Case 2: create_map with odd number of columns (list)
    with pytest.raises(ValueError) as ex_info:
        df.select(create_map([df.department, df.salary, df.location]).alias("map"))
    assert (
        "The 'create_map' function requires an even number of parameters but the actual number is 3"
        in str(ex_info)
    )


def test_to_double(session, local_testing_mode):

    # Test supported input type
    df = session.create_dataframe(
        [[decimal.Decimal("12.34"), "12", "12.34", "-inf", "3.45e-4", None]],
        schema=StructType(
            [
                StructField("decimal_col", DecimalType(26, 12)),
                StructField("str_col1", StringType()),
                StructField("str_col2", StringType()),
                StructField("str_col3", StringType()),
                StructField("str_col4", StringType()),
                StructField("str_col5", StringType()),
            ]
        ),
    )

    Utils.check_answer(
        df.select([to_double(c) for c in df.columns]),
        [Row(12.34, 12.0, 12.34, -float("inf"), 3.45e-4, None)],
    )

    # Test unsupported input type
    df = session.create_dataframe(
        [[False], [True]], schema=StructType([StructField("bool_col", BooleanType())])
    )

    with pytest.raises(SnowparkSQLException):
        df.select([to_double(c) for c in df.columns]).collect()

    # Test variant conversion
    df = session.create_dataframe(
        [[decimal.Decimal("56.78"), 90.12, "6.78e-10", True, False, None]],
        StructType(
            [
                StructField("variant_col1", VariantType()),
                StructField("variant_col2", VariantType()),
                StructField("variant_col3", VariantType()),
                StructField("variant_col4", VariantType()),
                StructField("variant_col5", VariantType()),
                StructField("variant_col6", VariantType()),
            ]
        ),
    )
    Utils.check_answer(
        df.select([to_double(c) for c in df.columns]).collect(),
        [Row(56.78, 90.12, 6.78e-10, 1.0, 0.0, None)],
    )

    # Test specifying fmt, TODO: not supported in Local Testing
    if not local_testing_mode:
        # Local testing only covers partial implementation of to_double
        df = session.create_dataframe([["1.2", "2.34-", "9.99MI"]]).to_df(
            ["a", "b", "fmt"]
        )

        Utils.check_answer(
            df.select(
                to_double("a"), to_double("b", "9.99MI"), to_double("b", col("fmt"))
            ),
            [Row(1.2, -2.34, -2.34)],
            sort=False,
        )


def test_to_decimal(session, local_testing_mode):
    # Supported input type
    df = session.create_dataframe(
        [[decimal.Decimal("12.34"), 12.345678, "3.14E-6", True, None]],
        schema=StructType(
            [
                StructField("decimal_col", DecimalType(26, 12)),
                StructField("float_col", DoubleType()),
                StructField("str_col", StringType()),
                StructField("bool_col1", BooleanType()),
                StructField("bool_col2", BooleanType()),
            ]
        ),
    )
    # Test when scale is 0
    Utils.check_answer(
        df.select([to_decimal(c, 38, 0) for c in df.columns]),
        [
            Row(
                decimal.Decimal("12"),
                decimal.Decimal("12"),
                decimal.Decimal("0"),
                decimal.Decimal("1"),
                None,
            )
        ],
    )

    # Test when scale is 2
    Utils.check_answer(
        df.select([to_decimal(c, 38, 2) for c in df.columns]),
        [
            Row(
                decimal.Decimal("12.34"),
                decimal.Decimal("12.35"),
                decimal.Decimal("0"),
                decimal.Decimal("1"),
                None,
            )
        ],
    )

    # Test when scale is 6
    Utils.check_answer(
        df.select([to_decimal(c, 38, 6) for c in df.columns]),
        [
            Row(
                decimal.Decimal("12.34"),
                decimal.Decimal("12.345678"),
                decimal.Decimal("0.000003"),
                decimal.Decimal("1"),
                None,
            )
        ],
    )

    # Unsupported input
    df = session.create_dataframe(
        [[-math.inf, datetime.date.today()]],
        schema=StructType(
            [StructField("float_col", FloatType()), StructField("date_col", DateType())]
        ),
    )

    # Test when input type is not supported
    with pytest.raises(SnowparkSQLException):
        df.select([to_decimal(df.date_col, 38, 0)]).collect()

    # Test when input value is not supported
    with pytest.raises(SnowparkSQLException):
        df.select([to_decimal(df.float_col, 38, 0)]).collect()


def test_negative_function_call(session):
    df = session.create_dataframe(["a", "b"], schema=["a"])

    with pytest.raises(SnowparkSQLException) as ex_info:
        df.select(sum_(col("a"))).collect()
        assert "is not recognized" in str(ex_info)


def test_ln(session):
    from math import e

    df = session.create_dataframe([[e]], schema=["ln_value"])
    res = df.select(ln(col("ln_value")).alias("result")).collect()
    assert res[0][0] == 1.0


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="Snowflake Cortex functions not supported in SP"
)
@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: snowflake_cortex functions not supported",
)
@pytest.mark.skipif(
    running_on_jenkins(),
    reason="TODO: SNOW-1859087 snowflake.cortex.summarize SSL error",
)
def test_snowflake_cortex_summarize(session):
    # TODO: SNOW-1758914 snowflake.cortex.summarize error on GCP
    if session.connection.host == "sfctest0.us-central1.gcp.snowflakecomputing.com":
        return

    content = """In Snowpark, the main way in which you query and process data is through a DataFrame. This topic explains how to work with DataFrames.

To retrieve and manipulate data, you use the DataFrame class. A DataFrame represents a relational dataset that is evaluated lazily: it only executes when a specific action is triggered. In a sense, a DataFrame is like a query that needs to be evaluated in order to retrieve data.

To retrieve data into a DataFrame:

Construct a DataFrame, specifying the source of the data for the dataset.

For example, you can create a DataFrame to hold data from a table, an external CSV file, from local data, or the execution of a SQL statement.

Specify how the dataset in the DataFrame should be transformed.

For example, you can specify which columns should be selected, how the rows should be filtered, how the results should be sorted and grouped, etc.

Execute the statement to retrieve the data into the DataFrame.

In order to retrieve the data into the DataFrame, you must invoke a method that performs an action (for example, the collect() method).

The next sections explain these steps in more detail.
"""
    df = session.create_dataframe([[content]], schema=["content"])
    summary_from_col = df.select(snowflake_cortex_summarize(col("content"))).collect()[
        0
    ][0]
    summary_from_str = df.select(snowflake_cortex_summarize(content)).collect()[0][0]
    # this length check is to get around the fact that this function may not be deterministic
    assert 0 < len(summary_from_col) < len(content)
    assert 0 < len(summary_from_str) < len(content)


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="Snowflake Cortex functions not supported in SP"
)
@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: snowflake_cortex functions not supported",
)
@pytest.mark.skipif(
    running_on_jenkins(),
    reason="TODO: SNOW-1859087 snowflake.cortex.sentiment SSL error",
)
def test_snowflake_cortex_sentiment(session):
    # TODO: SNOW-1758914 snowflake.cortex.sentiment error on GCP
    if session.connection.host == "sfctest0.us-central1.gcp.snowflakecomputing.com":
        return
    content = "A very very bad review!"
    df = session.create_dataframe([[content]], schema=["content"])

    sentiment_from_col = df.select(
        snowflake_cortex_sentiment(col("content"))
    ).collect()[0][0]
    sentiment_from_str = df.select(snowflake_cortex_sentiment(content)).collect()[0][0]

    assert -1 <= sentiment_from_col <= 0
    assert -1 <= sentiment_from_str <= 0
