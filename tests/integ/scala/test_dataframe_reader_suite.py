#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import csv
import datetime
import json
import logging
import os
import random
import tempfile
from decimal import Decimal
from unittest import mock

import pytest

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark import Row
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.column import (
    METADATA_FILE_CONTENT_KEY,
    METADATA_FILE_LAST_MODIFIED,
    METADATA_FILE_ROW_NUMBER,
    METADATA_FILENAME,
    METADATA_START_SCAN_TIME,
)
from snowflake.snowpark.dataframe_reader import READER_OPTIONS_ALIAS_MAP
from snowflake.snowpark.exceptions import (
    SnowparkDataframeReaderException,
    SnowparkPlanException,
    SnowparkSQLException,
)
from snowflake.snowpark.functions import col, lit, sql_expr
from snowflake.snowpark.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampTimeZone,
    TimestampType,
    TimeType,
    VariantType,
)
from tests.utils import (
    IS_IN_STORED_PROC,
    IS_IN_STORED_PROC_LOCALFS,
    TestFiles,
    Utils,
    multithreaded_run,
)

test_file_csv = "testCSV.csv"
test_file_cvs_various_data = "testCSVvariousData.csv"
test_file2_csv = "test2CSV.csv"
test_file_csv_colon = "testCSVcolon.csv"
test_file_csv_header = "testCSVheader.csv"
test_file_csv_quotes = "testCSVquotes.csv"
test_file_csv_quotes_special = "testCSVquotesSpecial.csv"
test_file_csv_timestamps = "testCSVformattedTime.csv"
test_file_json = "testJson.json"
test_file_json_same_schema = "testJsonSameSchema.json"
test_file_json_new_schema = "testJsonNewSchema.json"
test_file_avro = "test.avro"
test_file_parquet = "test.parquet"
test_file_all_data_types_parquet = "test_all_data_types.parquet"
test_file_with_special_characters_parquet = "test_file_with_special_characters.parquet"
test_file_orc = "test.orc"
test_file_xml = "test.xml"
test_broken_csv = "broken.csv"


# In the tests below, we test both scenarios: SELECT & COPY
def get_reader(session, mode):
    if mode == "select":
        reader = session.read
    elif mode == "copy":
        reader = session.read.option("PURGE", False)
    else:
        raise Exception("incorrect input for mode")
    return reader


user_schema = StructType(
    [
        StructField("a", IntegerType()),
        StructField("b", StringType()),
        StructField("c", DoubleType()),
    ]
)


def get_file_path_for_format(file_format):
    if file_format == "csv":
        return test_file_csv
    if file_format == "json":
        return test_file_json
    if file_format == "avro":
        return test_file_avro
    if file_format == "parquet":
        return test_file_parquet
    if file_format == "orc":
        return test_file_orc
    if file_format == "xml":
        return test_file_xml

    raise ValueError(f"Do not have test file for format : '{file_format}'")


def get_df_from_reader_and_file_format(reader, file_format):
    test_file = get_file_path_for_format(file_format)
    file_path = f"@{tmp_stage_name1}/{test_file}"

    if file_format == "csv":
        return reader.schema(user_schema).csv(file_path)
    if file_format == "json":
        return reader.json(file_path)
    if file_format == "avro":
        return reader.avro(file_path)
    if file_format == "parquet":
        return reader.parquet(file_path)
    if file_format == "orc":
        return reader.orc(file_path)
    if file_format == "xml":
        return reader.xml(file_path)


tmp_stage_name1 = Utils.random_stage_name()
tmp_stage_name2 = Utils.random_stage_name()
tmp_stage_only_json_file = Utils.random_stage_name()


@pytest.fixture(scope="module", autouse=True)
def setup(session, resources_path, local_testing_mode):
    test_files = TestFiles(resources_path)
    if not local_testing_mode:
        Utils.create_stage(session, tmp_stage_name1, is_temporary=True)
        Utils.create_stage(session, tmp_stage_name2, is_temporary=True)
        Utils.create_stage(session, tmp_stage_only_json_file, is_temporary=True)
    Utils.upload_to_stage(
        session, "@" + tmp_stage_name1, test_files.test_file_csv, compress=False
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name1,
        test_files.test_file_csv_various_data,
        compress=False,
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name1,
        test_files.test_file2_csv,
        compress=False,
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name1,
        test_files.test_file_csv_colon,
        compress=False,
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name1,
        test_files.test_file_csv_quotes,
        compress=False,
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name1,
        test_files.test_file_csv_quotes_special,
        compress=False,
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name1,
        test_files.test_file_csv_header,
        compress=False,
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name1,
        test_files.test_file_csv_timestamps,
        compress=False,
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name1,
        test_files.test_file_json,
        compress=False,
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_only_json_file,
        test_files.test_file_json,
        compress=False,
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_only_json_file,
        test_files.test_file_json_same_schema,
        compress=False,
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_only_json_file,
        test_files.test_file_json_new_schema,
        compress=False,
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name1,
        test_files.test_file_avro,
        compress=False,
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name1,
        test_files.test_file_parquet,
        compress=False,
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name1,
        test_files.test_file_all_data_types_parquet,
        compress=False,
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name1,
        test_files.test_file_with_special_characters_parquet,
        compress=False,
    )
    Utils.upload_to_stage(
        session, "@" + tmp_stage_name1, test_files.test_file_orc, compress=False
    )
    Utils.upload_to_stage(
        session, "@" + tmp_stage_name1, test_files.test_file_xml, compress=False
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name1,
        test_files.test_broken_csv,
        compress=False,
    )
    Utils.upload_to_stage(
        session, "@" + tmp_stage_name2, test_files.test_file_csv, compress=False
    )
    yield
    # tear down the resources after yield (pytest fixture feature)
    # https://docs.pytest.org/en/6.2.x/fixture.html#yield-fixtures-recommended
    if not local_testing_mode:
        session.sql(f"DROP STAGE IF EXISTS {tmp_stage_name1}").collect()
        session.sql(f"DROP STAGE IF EXISTS {tmp_stage_name2}").collect()
        session.sql(f"DROP STAGE IF EXISTS {tmp_stage_only_json_file}").collect()


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_csv(session, mode):
    reader = get_reader(session, mode)
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"
    df1 = reader.schema(user_schema).csv(test_file_on_stage)
    res = df1.collect()
    res.sort(key=lambda x: x[0])

    assert len(res) == 2
    assert len(res[0]) == 3
    assert res == [Row(1, "one", 1.2), Row(2, "two", 2.2)]

    # if users give an incorrect schema with type error
    # the system will throw SnowflakeSQLException during execution
    incorrect_schema = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", IntegerType()),
            StructField("c", IntegerType()),
        ]
    )
    df2 = reader.schema(incorrect_schema).csv(test_file_on_stage)
    with pytest.raises(SnowparkSQLException) as ex_info:
        df2.collect()
    assert "Numeric value 'one' is not recognized" in ex_info.value.message

    cvs_schema = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", LongType()),
            StructField("c", StringType()),
            StructField("d", DoubleType()),
            StructField("e", DecimalType(scale=0)),
            StructField("f", DecimalType(scale=2)),
            StructField("g", DecimalType(precision=2)),
            StructField("h", DecimalType(precision=10, scale=3)),
            StructField("i", FloatType()),
            StructField("j", BooleanType()),
            StructField("k", DateType()),
            # default timestamp type: https://docs.snowflake.com/en/sql-reference/parameters#timestamp-type-mapping
            StructField("l", TimestampType(TimestampTimeZone.NTZ)),
            StructField("m", TimeType()),
        ]
    )
    df3 = reader.schema(cvs_schema).csv(
        f"@{tmp_stage_name1}/{test_file_cvs_various_data}"
    )
    res = df3.collect()
    res.sort(key=lambda x: x[0])
    assert res == [
        Row(
            1,
            234,
            "one",
            1.2,
            12,
            Decimal("12.35"),
            -12,
            Decimal("12.346"),
            56.78,
            True,
            datetime.date(2023, 6, 6),
            datetime.datetime(2023, 6, 6, 12, 34, 56),
            datetime.time(12, 34, 56),
        ),
        Row(
            2,
            567,
            "two",
            2.2,
            57,
            Decimal("56.79"),
            -57,
            Decimal("56.787"),
            89.01,
            False,
            datetime.date(2023, 6, 6),
            datetime.datetime(2023, 6, 6, 12, 34, 56),
            datetime.time(12, 34, 56),
        ),
    ]

    cvs_schema = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", LongType()),
            StructField("c", StringType()),
            StructField("d", DoubleType()),
            StructField("e", DecimalType(scale=0)),
            StructField("f", DecimalType(scale=2)),
            StructField("g", DecimalType(precision=1)),
            StructField("h", DecimalType(precision=10, scale=3)),
            StructField("i", FloatType()),
            StructField("j", BooleanType()),
            StructField("k", DateType()),
            # default timestamp type: https://docs.snowflake.com/en/sql-reference/parameters#timestamp-type-mapping
            StructField("l", TimestampType(TimestampTimeZone.NTZ)),
            StructField("m", TimeType()),
        ]
    )
    df3 = reader.schema(cvs_schema).csv(
        f"@{tmp_stage_name1}/{test_file_cvs_various_data}"
    )
    with pytest.raises(SnowparkSQLException) as ex_info:
        df3.collect()
    assert "is out of range" in str(ex_info.value)


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1435112: csv infer schema option is not supported",
    run=False,
)
@pytest.mark.parametrize(
    "format,file,expected",
    [
        ("csv", test_file_csv, [Row(1, "one", 1.2), Row(2, "two", 2.2)]),
        (
            "json",
            test_file_json,
            [
                Row(
                    COL1='{\n  "color": "Red",\n  "fruit": "Apple",\n  "size": "Large"\n}'
                )
            ],
        ),
        (
            "avro",
            test_file_avro,
            [
                Row(str="str1", num=1),
                Row(str="str2", num=2),
            ],
        ),
        (
            "parquet",
            test_file_parquet,
            [
                Row(str="str1", num=1),
                Row(str="str2", num=2),
            ],
        ),
        (
            "orc",
            test_file_orc,
            [
                Row(str="str1", num=1),
                Row(str="str2", num=2),
            ],
        ),
        (
            "xml",
            test_file_xml,
            [
                Row("<test>\n  <num>1</num>\n  <str>str1</str>\n</test>"),
                Row("<test>\n  <num>2</num>\n  <str>str2</str>\n</test>"),
            ],
        ),
    ],
)
def test_format_load(session, format, file, expected):
    test_file_on_stage = f"@{tmp_stage_name1}/{file}"
    df = session.read.format(format).load(test_file_on_stage)
    Utils.check_answer(df, expected)


def test_format_load_negative(session):
    with pytest.raises(
        ValueError, match="Invalid format 'unsupported_format'. Supported formats are"
    ):
        session.read.format("unsupported_format")

    with pytest.raises(ValueError, match="Please specify the format of the file"):
        session.read.load("path")


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1435112: csv infer schema option is not supported",
    run=False,
)
def test_read_csv_with_default_infer_schema(session):
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"

    with pytest.raises(SnowparkDataframeReaderException) as exec_info:
        session.read.options({"infer_schema": False}).csv(test_file_on_stage)
    assert (
        'No schema specified in DataFrameReader.schema(). Please specify the schema or set session.read.options({"infer_schema":True})'
        in str(exec_info)
    )

    # check infer_schema default as true
    Utils.check_answer(
        session.read.csv(test_file_on_stage),
        [
            Row(c1=1, c2="one", c3=Decimal("1.2")),
            Row(c1=2, c2="two", c3=Decimal("2.2")),
        ],
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1435112: csv infer schema option is not supported",
)
@pytest.mark.parametrize("mode", ["select", "copy"])
@pytest.mark.parametrize("parse_header", [True, False])
def test_read_csv_with_infer_schema(session, mode, parse_header):
    reader = get_reader(session, mode)
    if parse_header:
        test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv_header}"
    else:
        test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"
    df = (
        reader.option("INFER_SCHEMA", True)
        .option("PARSE_HEADER", parse_header)
        .csv(test_file_on_stage)
    )
    Utils.check_answer(df, [Row(1, "one", 1.2), Row(2, "two", 2.2)])


@multithreaded_run()
@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1435112: csv infer schema option is not supported",
)
@pytest.mark.parametrize("mode", ["select", "copy"])
@pytest.mark.parametrize("ignore_case", [True, False])
def test_read_csv_with_infer_schema_options(session, mode, ignore_case):
    reader = get_reader(session, mode)
    df = (
        reader.option("INFER_SCHEMA", True)
        .option("PARSE_HEADER", True)
        .option("INFER_SCHEMA_OPTIONS", {"IGNORE_CASE": ignore_case})
        .csv(f"@{tmp_stage_name1}/{test_file_csv_header}")
    )
    Utils.check_answer(df, [Row(1, "one", 1.2), Row(2, "two", 2.2)])
    headers = ["id", "name", "rating"]
    if ignore_case:
        expected_cols = [c.upper() for c in headers]
    else:
        expected_cols = [f'"{c}"' for c in headers]
    assert df.columns == expected_cols


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1435112: csv infer schema option is not supported",
)
@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_csv_with_infer_schema_negative(session, mode, caplog):
    reader = get_reader(session, mode)
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_parquet}"

    def mock_run_query(*args, **kwargs):
        if "INFER_SCHEMA ( LOCATION  =>" in args[0]:
            raise ProgrammingError("Cannot infer schema")
        return session._conn.run_query(args, kwargs)

    with mock.patch(
        "snowflake.snowpark._internal.server_connection.ServerConnection.run_query",
        side_effect=mock_run_query,
    ):
        with caplog.at_level(logging.WARN):
            reader.option("INFER_SCHEMA", True).csv(test_file_on_stage)
            assert "Could not infer csv schema due to exception:" in caplog.text


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_reader_option_aliases(session, mode, caplog):
    reader = get_reader(session, mode)
    with caplog.at_level(logging.WARN):
        for key, _aliased_key in READER_OPTIONS_ALIAS_MAP.items():
            reader.option(key, "test")
        assert (
            f"Option '{key}' is aliased to '{_aliased_key}'. You may see unexpected behavior"
            in caplog.text
        )
        caplog.clear()


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_csv_incorrect_schema(session, mode):
    reader = get_reader(session, mode)
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"
    incorrect_schema = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", StringType()),
            StructField("c", IntegerType()),
            StructField("d", IntegerType()),
        ]
    )
    df = reader.option("purge", False).schema(incorrect_schema).csv(test_file_on_stage)
    with pytest.raises(SnowparkSQLException) as ex_info:
        df.collect()
    assert "Number of columns in file (3) does not match" in str(ex_info.value)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: xml not supported",
)
def test_save_as_table_work_with_df_created_from_read(session):
    reader = get_reader(session, "select")
    test_json_on_stage = f"@{tmp_stage_name1}/{test_file_json}"
    test_xml_on_stage = f"@{tmp_stage_name1}/{test_file_xml}"
    json_table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    xml_table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)

    df_json = reader.json(test_json_on_stage)
    df_xml = reader.xml(test_xml_on_stage)

    try:
        df_json.write.save_as_table(json_table_name)
        df_xml.write.save_as_table(xml_table_name)

        Utils.check_answer(
            session.table(json_table_name),
            [
                Row(
                    COL1='{\n  "color": "Red",\n  "fruit": "Apple",\n  "size": "Large"\n}'
                )
            ],
        )
        Utils.check_answer(
            session.table(xml_table_name),
            [
                Row(COL1="<test>\n  <num>1</num>\n  <str>str1</str>\n</test>"),
                Row(COL1="<test>\n  <num>2</num>\n  <str>str2</str>\n</test>"),
            ],
        )
    finally:
        Utils.drop_table(session, json_table_name)
        Utils.drop_table(session, xml_table_name)


def test_save_as_table_do_not_change_col_name(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    schema = StructType(
        [
            StructField("$# $1 $y", LongType(), True),
        ]
    )
    try:
        df = session.create_dataframe([1], schema=schema)
        df.write.saveAsTable(table_name=table_name)
        # Util.check_answer() cannot be used here because column name contain space and will lead to error
        assert df.collect() == session.table(table_name).collect()
        assert ['"$# $1 $y"'] == session.table(table_name).columns
    finally:
        Utils.drop_table(session, table_name)


def test_read_csv_with_more_operations(session):
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"
    df1 = session.read.schema(user_schema).csv(test_file_on_stage).filter(col("a") < 2)
    res = df1.collect()
    res.sort(key=lambda x: x[0])
    assert res == [Row(1, "one", 1.2)]

    # test for self union
    df = session.read.schema(user_schema).csv(test_file_on_stage)
    df2 = df.union(df)
    res = df2.collect()
    res.sort(key=lambda x: x[0])
    assert res == [
        Row(1, "one", 1.2),
        Row(2, "two", 2.2),
    ]
    df22 = df.union_all(df)
    res = df22.collect()
    res.sort(key=lambda x: x[0])
    assert res == [
        Row(1, "one", 1.2),
        Row(1, "one", 1.2),
        Row(2, "two", 2.2),
        Row(2, "two", 2.2),
    ]

    # test for union between two stages
    test_file_on_stage2 = f"@{tmp_stage_name2}/{test_file_csv}"
    df3 = session.read.schema(user_schema).csv(test_file_on_stage2)
    df4 = df.union(df3)
    res = df4.collect()
    res.sort(key=lambda x: x[0])
    assert res == [
        Row(1, "one", 1.2),
        Row(2, "two", 2.2),
    ]
    df44 = df.union_all(df3)
    res = df44.collect()
    res.sort(key=lambda x: x[0])
    assert res == [
        Row(1, "one", 1.2),
        Row(1, "one", 1.2),
        Row(2, "two", 2.2),
        Row(2, "two", 2.2),
    ]


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_csv_with_format_type_options(session, mode):
    test_file_colon = f"@{tmp_stage_name1}/{test_file_csv_colon}"
    options = {
        "field_delimiter": "';'",
        "skip_blank_lines": True,
        "skip_header": 1,
    }
    df1 = (
        get_reader(session, mode)
        .schema(user_schema)
        .options(options)
        .csv(test_file_colon)
    )
    res = df1.collect()
    res.sort(key=lambda x: x[0])
    assert res == [Row(1, "one", 1.2), Row(2, "two", 2.2)]

    # test when user does not input a right option:
    with pytest.raises(
        ValueError, match="DataFrameReader can only read files from stage locations."
    ):
        get_reader(session, mode).schema(user_schema).csv(test_file_csv_colon)

    # test for multiple formatTypeOptions
    df3 = (
        get_reader(session, mode)
        .schema(user_schema)
        .option("field_delimiter", ";")
        .option("ENCODING", "wrongEncoding")
        .option("ENCODING", "UTF8")
        .options(compression="NONE", skip_header=1)
        .csv(test_file_colon)
    )
    res = df3.collect()
    res.sort(key=lambda x: x[0])
    assert res == [Row(1, "one", 1.2), Row(2, "two", 2.2)]

    # test for union between files with different schema and different stage
    test_file_on_stage2 = f"@{tmp_stage_name2}/{test_file_csv}"
    df4 = get_reader(session, mode).schema(user_schema).csv(test_file_on_stage2)
    df5 = df1.union_all(df4)
    res = df5.collect()
    res.sort(key=lambda x: x[0])
    assert res == [
        Row(1, "one", 1.2),
        Row(1, "one", 1.2),
        Row(2, "two", 2.2),
        Row(2, "two", 2.2),
    ]
    df6 = df1.union(df4)
    res = df6.collect()
    res.sort(key=lambda x: x[0])
    assert res == [
        Row(1, "one", 1.2),
        Row(2, "two", 2.2),
    ]


def test_reader_options_negative(session):
    with pytest.raises(
        ValueError,
        match="Cannot set options with both a dictionary and keyword arguments",
    ):
        session.read.options({"field_delimiter": ";"}, compression="NONE").csv(
            test_file_csv_colon
        )

    with pytest.raises(ValueError, match="No options were provided"):
        session.read.options().csv(test_file_csv_colon)


@multithreaded_run()
@pytest.mark.parametrize("mode", ["select", "copy"])
def test_to_read_files_from_stage(session, resources_path, mode, local_testing_mode):
    data_files_stage = Utils.random_stage_name()
    test_files = TestFiles(resources_path)
    if not local_testing_mode:
        Utils.create_stage(session, data_files_stage, is_temporary=True)
    Utils.upload_to_stage(
        session, "@" + data_files_stage, test_files.test_file_csv, False
    )
    Utils.upload_to_stage(
        session, "@" + data_files_stage, test_files.test_file2_csv, False
    )

    reader = get_reader(session, mode)

    try:
        df = (
            reader.schema(user_schema)
            .option("compression", "auto")
            .csv(f"@{data_files_stage}/")
        )
        res = df.collect()
        res.sort(key=lambda x: x[0])
        assert res == [
            Row(1, "one", 1.2),
            Row(2, "two", 2.2),
            Row(3, "three", 3.3),
            Row(4, "four", 4.4),
        ]
    finally:
        if not local_testing_mode:
            session.sql(f"DROP STAGE IF EXISTS {data_files_stage}")


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="Local testing does not support external file formats",
    run=False,
)
def test_csv_external_file_format(session, resources_path, temp_schema):
    data_files_stage = Utils.random_stage_name()
    file_format = Utils.random_stage_name()
    test_files = TestFiles(resources_path)

    try:
        # Upload test file
        Utils.create_stage(session, data_files_stage, is_temporary=True)
        Utils.upload_to_stage(
            session, "@" + data_files_stage, test_files.test_file_csv_header, False
        )

        # Create external file format
        session.sql(
            f"""
        CREATE OR REPLACE FILE FORMAT {file_format}
        """
            + r"""
        TYPE = 'CSV'
        ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
        FIELD_DELIMITER = ','
        REPLACE_INVALID_CHARACTERS = TRUE
        ESCAPE = '\\'
        FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
        ESCAPE_UNENCLOSED_FIELD = none
        SKIP_BLANK_LINES = TRUE
        EMPTY_FIELD_AS_NULL = FALSE
        PARSE_HEADER = True
        ENCODING = 'UTF8';
        """
        ).collect()

        # Try loading with external format
        df = session.read.option("format_name", file_format).csv(
            f"@{data_files_stage}/"
        )

        # Validate Merge
        assert df._session._plan_builder._merge_file_format_options(
            {}, {"FORMAT_NAME": file_format}
        ) == {
            "EMPTY_FIELD_AS_NULL": False,
            "PARSE_HEADER": True,
            "ESCAPE": r"\\",
            "ESCAPE_UNENCLOSED_FIELD": "NONE",
            "FIELD_OPTIONALLY_ENCLOSED_BY": r"\"",
            "SKIP_BLANK_LINES": True,
            "REPLACE_INVALID_CHARACTERS": True,
        }

        res = df.collect()
        res.sort(key=lambda x: x[0])
        assert res == [
            Row(id=1, name="one", rating=Decimal("1.2")),
            Row(id=2, name="two", rating=Decimal("2.2")),
        ]
    finally:
        session.sql(f"DROP STAGE IF EXISTS {data_files_stage}").collect()
        session.sql(f"drop file format if exists {file_format}").collect()


@pytest.mark.xfail(reason="SNOW-575700 flaky test", strict=False)
@pytest.mark.parametrize("mode", ["select", "copy"])
def test_for_all_csv_compression_keywords(session, temp_schema, mode):
    tmp_table = (
        temp_schema + "." + Utils.random_name_for_temp_object(TempObjectType.TABLE)
    )
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"
    format_name = Utils.random_name_for_temp_object(TempObjectType.FILE_FORMAT)
    try:
        get_reader(session, mode).schema(user_schema).option("compression", "auto").csv(
            test_file_on_stage
        ).write.save_as_table(tmp_table)

        session.sql(f"create file format {format_name} type = 'csv'").collect()

        for ctype in ["gzip", "bz2", "brotli", "zstd", "deflate", "raw_deflate"]:
            path = f"@{tmp_stage_name1}/{ctype}/{abs(random.randint(0, 2 ** 31))}/"
            # upload data
            session.sql(
                f"copy into {path} from ( select * from {tmp_table}) file_format=(format_name='{format_name}' compression='{ctype}')"
            ).collect()

            # read the data
            df = (
                get_reader(session, mode)
                .option("COMPRESSION", ctype)
                .schema(user_schema)
                .csv(path)
            )
            res = df.collect()
            res.sort(key=lambda x: x[0])
            Utils.check_answer(res, [Row(1, "one", 1.2), Row(2, "two", 2.2)])
    finally:
        session.sql(f"drop file format {format_name}")


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_csv_with_special_chars_in_format_type_options(session, mode):
    schema1 = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", StringType()),
            StructField("c", DoubleType()),
            StructField("d", DoubleType()),
            StructField("e", StringType()),
            StructField("f", BooleanType()),
            StructField("g", TimestampType(TimestampTimeZone.NTZ)),
            StructField("h", TimeType()),
        ]
    )
    test_file = f"@{tmp_stage_name1}/{test_file_csv_quotes}"

    reader = get_reader(session, mode)

    bad_option_df = (
        reader.schema(schema1)
        .option("field_optionally_enclosed_by", '""')  # only single char is allowed
        .csv(test_file)
    )
    with pytest.raises(SnowparkSQLException):
        bad_option_df.collect()

    df1 = (
        reader.schema(schema1)
        .option("field_optionally_enclosed_by", '"')
        .csv(test_file)
    )
    res = df1.collect()
    res.sort(key=lambda x: x[0])
    assert res == [
        Row(
            1,
            "one",
            1.2,
            1.234,
            "quoted",
            True,
            datetime.datetime(2024, 3, 1, 9, 10, 11),
            datetime.time(9, 10, 11),
        ),
        Row(
            2,
            "two",
            2.2,
            2.5,
            "unquoted",
            False,
            datetime.datetime(2024, 2, 29, 12, 34, 56),
            datetime.time(12, 34, 56),
        ),
    ]

    # without the setting it should fail schema validation
    df2 = get_reader(session, mode).schema(schema1).csv(test_file)
    with pytest.raises(SnowparkSQLException) as ex_info:
        df2.collect()
    assert "Numeric value '\"1.234\"' is not recognized" in ex_info.value.message

    schema2 = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", StringType()),
            StructField("c", DoubleType()),
            StructField("d", StringType()),
            StructField("e", StringType()),
            StructField("f", StringType()),
            StructField("g", StringType()),
            StructField("h", StringType()),
        ]
    )
    df3 = get_reader(session, mode).schema(schema2).csv(test_file)
    df3.collect()
    res = df3.select("d", "h").collect()
    res.sort(key=lambda x: x[0])
    assert res == [Row('"1.234"', '"09:10:11"'), Row('"2.5"', "12:34:56")]


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_csv_with_quotes_containing_delimiter(session, mode):
    schema1 = StructType(
        [
            StructField("col1", StringType()),
            StructField("col2", StringType()),
            StructField("col3", StringType()),
        ]
    )
    test_file = f"@{tmp_stage_name1}/{test_file_csv_quotes_special}"

    reader = get_reader(session, mode)

    df1 = (
        reader.schema(schema1)
        .option("field_optionally_enclosed_by", '"')
        .option("skip_header", 1)
        .csv(test_file)
    )
    res = df1.collect()
    res.sort(key=lambda x: x[0])
    assert res == [
        Row(
            "value 1",
            "value 2 with no comma",
            "value3",
        ),
        Row("value 4", "value 5, but with a comma", "     value6"),
    ]


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1435385 DataFrameReader.with_metadata is not supported",
)
@pytest.mark.skipif(
    IS_IN_STORED_PROC_LOCALFS,
    reason="metadata info is not available in localfs",
)
@pytest.mark.parametrize(
    "file_format", ["csv", "json", "avro", "parquet", "xml", "orc"]
)
def test_read_metadata_column_from_stage(session, file_format):
    if file_format == "json":
        filename = "testJson.json"
    elif file_format == "csv":
        filename = "testCSV.csv"
    else:
        filename = f"test.{file_format}"

    # test that all metadata columns are supported
    reader = session.read.with_metadata(
        METADATA_FILENAME,
        METADATA_FILE_ROW_NUMBER,
        METADATA_FILE_CONTENT_KEY,
        METADATA_FILE_LAST_MODIFIED,
        METADATA_START_SCAN_TIME,
    )

    df = get_df_from_reader_and_file_format(reader, file_format)
    res = df.collect()
    assert res[0]["METADATA$FILENAME"] == filename
    assert res[0]["METADATA$FILE_ROW_NUMBER"] >= 0
    assert (
        len(res[0]["METADATA$FILE_CONTENT_KEY"]) > 0
    )  # ensure content key is non-null
    assert isinstance(res[0]["METADATA$FILE_LAST_MODIFIED"], datetime.datetime)
    assert isinstance(res[0]["METADATA$START_SCAN_TIME"], datetime.datetime)

    table_name = Utils.random_table_name()
    df.write.save_as_table(table_name, mode="append")
    with session.table(table_name) as table_df:
        table_res = table_df.collect()
        assert table_res[0]["METADATA$FILENAME"] == res[0]["METADATA$FILENAME"]
        assert (
            table_res[0]["METADATA$FILE_ROW_NUMBER"]
            == res[0]["METADATA$FILE_ROW_NUMBER"]
        )
        assert (
            table_res[0]["METADATA$FILE_CONTENT_KEY"]
            == res[0]["METADATA$FILE_CONTENT_KEY"]
        )
        assert (
            table_res[0]["METADATA$FILE_LAST_MODIFIED"]
            == res[0]["METADATA$FILE_LAST_MODIFIED"]
        )
        assert isinstance(res[0]["METADATA$START_SCAN_TIME"], datetime.datetime)

    # test single column works
    reader = session.read.with_metadata(METADATA_FILENAME)
    df = get_df_from_reader_and_file_format(reader, file_format)
    res = df.collect()
    assert res[0]["METADATA$FILENAME"] == filename

    table_name = Utils.random_table_name()
    df.write.save_as_table(table_name, mode="append")
    with session.table(table_name) as table_df:
        table_res = table_df.collect()
        assert table_res[0]["METADATA$FILENAME"] == res[0]["METADATA$FILENAME"]

    # test that alias works
    reader = session.read.with_metadata(METADATA_FILENAME.alias("filename"))
    df = get_df_from_reader_and_file_format(reader, file_format)
    res = df.collect()
    assert res[0]["FILENAME"] == filename

    table_name = Utils.random_table_name()
    df.write.save_as_table(table_name, mode="append")
    with session.table(table_name) as table_df:
        table_res = table_df.collect()
        assert table_res[0]["FILENAME"] == res[0]["FILENAME"]

    # test that column name with str works
    reader = session.read.with_metadata("metadata$filename", "metadata$file_row_number")
    df = get_df_from_reader_and_file_format(reader, file_format)
    res = df.collect()
    assert res[0]["METADATA$FILENAME"] == filename
    assert res[0]["METADATA$FILE_ROW_NUMBER"] >= 0

    table_name = Utils.random_table_name()
    df.write.save_as_table(table_name, mode="append")
    with session.table(table_name) as table_df:
        table_res = table_df.collect()
        assert table_res[0]["METADATA$FILENAME"] == res[0]["METADATA$FILENAME"]
        assert (
            table_res[0]["METADATA$FILE_ROW_NUMBER"]
            == res[0]["METADATA$FILE_ROW_NUMBER"]
        )

    # test non-existing metadata column
    with pytest.raises(ValueError, match="Metadata column name is not supported"):
        get_df_from_reader_and_file_format(
            session.read.with_metadata("metadata$non-existing"), file_format
        )


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_json_with_no_schema(session, mode, resources_path):
    json_path = f"@{tmp_stage_name1}/{test_file_json}"

    df1 = get_reader(session, mode).json(json_path)
    assert (
        len(df1.schema.fields) == 1
        and isinstance(df1.schema.fields[0].datatype, VariantType)
        and df1.schema.fields[0].nullable
    )
    res = df1.collect()
    assert res == [
        Row('{\n  "color": "Red",\n  "fruit": "Apple",\n  "size": "Large"\n}')
    ]

    # query_test
    res = df1.where(df1["$1"]["color"] == "Red").collect()
    assert res == [
        Row('{\n  "color": "Red",\n  "fruit": "Apple",\n  "size": "Large"\n}')
    ]

    # user can input customized formatTypeOptions
    df2 = get_reader(session, mode).option("FILE_EXTENSION", "json").json(json_path)
    assert df2.collect() == [
        Row('{\n  "color": "Red",\n  "fruit": "Apple",\n  "size": "Large"\n}')
    ]

    # test read multiple files in a directory
    json_path = f"@{tmp_stage_only_json_file}"

    df1 = get_reader(session, mode).option("PATTERN", ".*json.*").json(json_path)
    res = df1.collect()
    res.sort(key=lambda x: x[0])
    assert res == [
        Row(
            '{\n  "CoLor": 10,\n  "new_field": true,\n  "new_fruit": "NewApple",\n  "new_size": 10,\n  "z\\"\'na\\"me": 0\n}'
        ),
        Row('{\n  "color": "Red",\n  "fruit": "Apple",\n  "size": "Large"\n}'),
        Row('{\n  "color": true,\n  "fruit": "Banana",\n  "size": "Small"\n}'),
    ]

    # query_test
    res = df1.where(df1["$1"]["color"] == "Red").collect()
    assert res == [
        Row('{\n  "color": "Red",\n  "fruit": "Apple",\n  "size": "Large"\n}')
    ]

    # assert local directory is invalid
    with pytest.raises(
        ValueError, match="DataFrameReader can only read files from stage locations."
    ):
        get_reader(session, mode).json(resources_path)


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_json_with_infer_schema(session, mode):
    json_path = f"@{tmp_stage_name1}/{test_file_json}"

    df1 = get_reader(session, mode).option("INFER_SCHEMA", True).json(json_path)
    res = df1.collect()
    assert res == [Row(color="Red", fruit="Apple", size="Large")]

    # query_test
    res = df1.where(col('"color"') == lit("Red")).collect()
    assert res == [Row(color="Red", fruit="Apple", size="Large")]

    # user can input customized formatTypeOptions
    df2 = (
        get_reader(session, mode)
        .option("INFER_SCHEMA", True)
        .option("FILE_EXTENSION", "json")
        .json(json_path)
    )
    assert df2.collect() == [Row(color="Red", fruit="Apple", size="Large")]

    # test multiples files of different schema
    # the first two json files (testJson.json, testJsonSameSchema.json) contains the same schema [color, fruit, size],
    # the third json file (testJsonNewSchema.json) contains a different schema [new_color, new_fruit, new_size]
    # snowflake will merge the schema

    json_path = f"@{tmp_stage_only_json_file}"
    df3 = get_reader(session, mode).option("INFER_SCHEMA", True).json(json_path)
    res = df3.collect()

    # the order of the merged columns is un-deterministic in snowflake, we sort the columns first
    new_rows = [column for column in sorted(res[0]._fields)]
    expected_sorted_ans = Utils.get_sorted_rows(
        [Row(*[res[i][column] for column in new_rows]) for i in range(len(res))]
    )
    assert len(res) == 3
    assert expected_sorted_ans == [
        Row(None, "Red", "Apple", None, None, None, "Large", None),
        Row(None, "true", "Banana", None, None, None, "Small", None),
        Row(10, None, None, True, "NewApple", 10, None, 0),
    ]
    # note here through testing that the ordering of returning columns is non-deterministic in snowflake
    # thus here we sort it
    assert sorted(df3.columns) == [
        '"CoLor"',
        '"color"',
        '"fruit"',
        '"new_field"',
        '"new_fruit"',
        '"new_size"',
        '"size"',
        '"z""\'na""me"',
    ]


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Local Testing does not support loading json with user specified schema.",
)
def test_read_json_quoted_names(session):
    stage_name = Utils.random_name_for_temp_object(TempObjectType.STAGE)
    quoted_column_data = {'"A"': 1, '"B"': 2.2}
    schema = StructType(
        [
            StructField('"A"', LongType(), True),
            StructField('"B"', DoubleType(), False),
        ]
    )
    parsed_schema = StructType(
        [
            StructField('"""A"""', LongType(), True),
            StructField('"""B"""', DoubleType(), False),
        ]
    )

    with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".json") as file:
        file.write(json.dumps(quoted_column_data))
        file_path = file.name
        file.flush()

    try:
        Utils.create_stage(session, stage_name, is_temporary=True)
        put_result = session.file.put(
            file_path, f"@{stage_name}", auto_compress=False, overwrite=True
        )
        reader = session.read.schema(schema)
        df_1 = reader.json(f"@{stage_name}/{put_result[0].target}")
        assert df_1.schema == parsed_schema
        df_2 = reader.json(f"@{stage_name}/{put_result[0].target}")
        assert df_2.schema == parsed_schema
        result = df_1.union_all(df_2).collect()
        Utils.check_answer(result, [Row(1, 2.2), Row(1, 2.2)])
    finally:
        Utils.drop_stage(session, stage_name)
        if os.path.exists(file_path):
            os.remove(file_path)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: parquet not supported",
)
@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: avro not supported",
)
@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_avro_with_no_schema(session, mode):
    avro_path = f"@{tmp_stage_name1}/{test_file_avro}"

    df1 = get_reader(session, mode).avro(avro_path)
    res = df1.collect()
    assert res == [
        Row(str="str1", num=1),
        Row(str="str2", num=2),
    ]

    # query_test
    res = df1.where(sql_expr('"num"') > 1).collect()
    assert res == [Row(str="str2", num=2)]

    # assert user cannot input a schema to read avro
    with pytest.raises(ValueError):
        get_reader(session, mode).schema(user_schema).avro(avro_path)

    # user can input customized formatTypeOptions
    df2 = get_reader(session, mode).option("COMPRESSION", "NONE").avro(avro_path)
    res = df2.collect()
    assert res == [
        Row(str="str1", num=1),
        Row(str="str2", num=2),
    ]


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: parquet not supported",
)
@pytest.mark.parametrize("mode", ["select", "copy"])
def test_for_all_parquet_compression_keywords(session, temp_schema, mode):
    tmp_table = (
        temp_schema + "." + Utils.random_name_for_temp_object(TempObjectType.TABLE)
    )
    reader = get_reader(session, mode)
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_parquet}"

    # Schema inference has to be False since we can only write 1 column as parquet
    reader.option("INFER_SCHEMA", False).parquet(test_file_on_stage).to_df(
        "a"
    ).write.save_as_table(tmp_table)

    format_name = Utils.random_name_for_temp_object(TempObjectType.FILE_FORMAT)
    session.sql(f"create file format {format_name} type = 'parquet'").collect()
    for ctype in ["snappy", "lzo"]:
        # upload data
        session.sql(
            f"copy into @{tmp_stage_name1}/{ctype}/ from ( select * from {tmp_table}) file_format=(format_name='{format_name}' compression='{ctype}') overwrite = true"
        ).collect()

        # read the data
        get_reader(session, mode).option("COMPRESSION", ctype).parquet(
            f"@{tmp_stage_name1}/{ctype}/"
        ).collect()


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: parquet not supported",
)
@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_parquet_with_no_schema(session, mode):
    path = f"@{tmp_stage_name1}/{test_file_parquet}"

    df1 = get_reader(session, mode).parquet(path)
    res = df1.collect()
    assert res == [
        Row(str="str1", num=1),
        Row(str="str2", num=2),
    ]

    # query_test
    res = df1.where(col('"num"') > 1).collect()
    assert res == [Row(str="str2", num=2)]

    # assert user cannot input a schema to read json
    with pytest.raises(ValueError):
        get_reader(session, mode).schema(user_schema).parquet(path)

    # user can input customized formatTypeOptions
    df2 = get_reader(session, mode).option("COMPRESSION", "NONE").parquet(path)
    res = df2.collect()
    assert res == [
        Row(str="str1", num=1),
        Row(str="str2", num=2),
    ]


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: parquet not supported",
)
@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_parquet_with_join_table_function(session, mode):
    path = f"@{tmp_stage_name1}/{test_file_parquet}"

    df = get_reader(session, mode).parquet(path)
    df1 = df.join_table_function("split_to_table", col('"str"'), lit(""))
    res = df1.collect()
    assert res == [
        Row(str="str1", num=1, SEQ=1, INDEX=1, VALUE="str1"),
        Row(str="str2", num=2, SEQ=2, INDEX=1, VALUE="str2"),
    ]


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: parquet not supported",
)
@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="SNOW-645154 Need to enable ENABLE_SCHEMA_DETECTION_COLUMN_ORDER",
)
@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_parquet_all_data_types_with_no_schema(session, mode):
    path = f"@{tmp_stage_name1}/{test_file_all_data_types_parquet}"

    df1 = get_reader(session, mode).parquet(path)
    res = df1.collect()
    assert res == [
        Row(
            N=Decimal("10.123456"),
            F=1.2,
            I=1,
            S="string",
            C="a",
            D=datetime.date(2022, 4, 1),
            T=datetime.time(11, 11, 11),
            TS_NTZ=datetime.datetime(2022, 4, 1, 11, 11, 11),
            TS=datetime.datetime(2022, 4, 1, 11, 11, 11),
            V='{"key":"value"}',
        ),
    ]

    # type and column test
    schema = df1.schema
    assert schema.names == ["N", "F", "I", "S", "C", "D", "T", "TS_NTZ", "TS", "V"]
    assert schema.fields == [
        StructField('"N"', DecimalType(38, 6), nullable=True),
        StructField('"F"', DoubleType(), nullable=True),
        StructField('"I"', LongType(), nullable=True),
        StructField('"S"', StringType(), nullable=True),
        StructField('"C"', StringType(), nullable=True),
        StructField('"D"', DateType(), nullable=True),
        StructField('"T"', TimeType(), nullable=True),
        StructField('"TS_NTZ"', TimestampType(TimestampTimeZone.NTZ), nullable=True),
        StructField('"TS"', TimestampType(TimestampTimeZone.NTZ), nullable=True),
        StructField('"V"', StringType(), nullable=True),
    ]

    # user can input customized formatTypeOptions
    df2 = get_reader(session, mode).option("COMPRESSION", "NONE").parquet(path)
    res = df2.collect()
    assert res == [
        Row(
            N=Decimal("10.123456"),
            F=1.2,
            I=1,
            S="string",
            C="a",
            D=datetime.date(2022, 4, 1),
            T=datetime.time(11, 11, 11),
            TS_NTZ=datetime.datetime(2022, 4, 1, 11, 11, 11),
            TS=datetime.datetime(2022, 4, 1, 11, 11, 11),
            V='{"key":"value"}',
        ),
    ]

    # type and column test
    schema = df2.schema
    assert schema.names == ["N", "F", "I", "S", "C", "D", "T", "TS_NTZ", "TS", "V"]
    assert schema.fields == [
        StructField('"N"', DecimalType(38, 6), nullable=True),
        StructField('"F"', DoubleType(), nullable=True),
        StructField('"I"', LongType(), nullable=True),
        StructField('"S"', StringType(), nullable=True),
        StructField('"C"', StringType(), nullable=True),
        StructField('"D"', DateType(), nullable=True),
        StructField('"T"', TimeType(), nullable=True),
        StructField('"TS_NTZ"', TimestampType(TimestampTimeZone.NTZ), nullable=True),
        StructField('"TS"', TimestampType(TimestampTimeZone.NTZ), nullable=True),
        StructField('"V"', StringType(), nullable=True),
    ]


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: parquet not supported",
)
@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="SNOW-645154 Need to enable ENABLE_SCHEMA_DETECTION_COLUMN_ORDER",
)
@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_parquet_with_special_characters_in_column_names(session, mode):
    path = f"@{tmp_stage_name1}/{test_file_with_special_characters_parquet}"
    df1 = get_reader(session, mode).parquet(path)
    res = df1.collect()
    assert len(res) == 500

    schema = df1.schema
    assert schema.names == [
        '"Ema!l"',
        '"Address"',
        '"Av@t@r"',
        '"Avg. $ession Length"',
        '"T!me on App"',
        '"T!me on Website"',
        '"Length of Membership"',
        '"Ye@rly Amount $pent"',
    ]
    assert schema.fields == [
        StructField('"Ema!l"', StringType(), nullable=True),
        StructField('"Address"', StringType(), nullable=True),
        StructField('"Av@t@r"', StringType(), nullable=True),
        StructField('"Avg. $ession Length"', DoubleType(), nullable=True),
        StructField('"T!me on App"', DoubleType(), nullable=True),
        StructField('"T!me on Website"', DoubleType(), nullable=True),
        StructField('"Length of Membership"', DoubleType(), nullable=True),
        StructField('"Ye@rly Amount $pent"', DoubleType(), nullable=True),
    ]


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: orc not supported",
)
@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_orc_with_no_schema(session, mode, resources_path):
    path = f"@{tmp_stage_name1}/{test_file_orc}"

    df1 = get_reader(session, mode).orc(path)
    res = df1.collect()
    assert res == [
        Row(str="str1", num=1),
        Row(str="str2", num=2),
    ]

    # query_test
    res = df1.where(sql_expr('"num"') > 1).collect()
    assert res == [Row(str="str2", num=2)]

    # assert user cannot input a schema to read json
    with pytest.raises(ValueError):
        get_reader(session, mode).schema(user_schema).orc(path)

    # assert local directory is invalid
    with pytest.raises(
        ValueError, match="DataFrameReader can only read files from stage locations."
    ):
        get_reader(session, mode).orc(resources_path)

    # user can input customized formatTypeOptions
    df2 = get_reader(session, mode).option("TRIM_SPACE", False).orc(path)
    res = df2.collect()
    assert res == [
        Row(str="str1", num=1),
        Row(str="str2", num=2),
    ]


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: xml not supported",
)
@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_xml_with_no_schema(session, mode, resources_path):
    path = f"@{tmp_stage_name1}/{test_file_xml}"

    df1 = get_reader(session, mode).xml(path)
    res = df1.collect()
    assert res == [
        Row("<test>\n  <num>1</num>\n  <str>str1</str>\n</test>"),
        Row("<test>\n  <num>2</num>\n  <str>str2</str>\n</test>"),
    ]

    # query_test
    res = df1.where(sql_expr("xmlget($1, 'num', 0):\"$\"") > 1).collect()
    assert res == [Row("<test>\n  <num>2</num>\n  <str>str2</str>\n</test>")]

    # assert user cannot input a schema to read json
    with pytest.raises(ValueError):
        get_reader(session, mode).schema(user_schema).xml(path)

    # assert local directory is invalid
    with pytest.raises(
        ValueError, match="DataFrameReader can only read files from stage locations."
    ):
        get_reader(session, mode).xml(resources_path)

    # user can input customized formatTypeOptions
    df2 = get_reader(session, mode).option("COMPRESSION", "NONE").xml(path)
    res = df2.collect()
    assert res == [
        Row("<test>\n  <num>1</num>\n  <str>str1</str>\n</test>"),
        Row("<test>\n  <num>2</num>\n  <str>str2</str>\n</test>"),
    ]


def test_copy(session, local_testing_mode):
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"

    # SNOW-1432856 on_error support
    df = (
        session.read.schema(user_schema)
        .option("on_error", "continue" if not local_testing_mode else None)
        .option("COMPRESSION", "none")
        .csv(test_file_on_stage)
    )

    if not local_testing_mode:
        # use copy
        assert len(df._plan.queries) == 3
        assert df._plan.queries[1].sql.find("COPY") >= 0
        assert len(df._plan.post_actions) == 1

    df1 = (
        session.read.schema(user_schema)
        .option("COMPRESSION", "none")
        .csv(test_file_on_stage)
    )

    if not local_testing_mode:
        # no copy
        assert len(df1._plan.queries) == 2

    res = df.collect()
    res.sort(key=lambda x: x[0])
    assert res == [Row(1, "one", 1.2), Row(2, "two", 2.2)]

    # fail to read since the file is not compressed.
    # return empty result since enable on_error = continue

    if not local_testing_mode:
        # SNOW-1432856 on_error support
        # SNOW-1432857 compression support
        df2 = (
            session.read.schema(user_schema)
            .option("on_error", "continue")
            .option("COMPRESSION", "gzip")
            .csv(test_file_on_stage)
        )
        assert df2.collect() == []


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1433016 force option is not supported",
)
def test_copy_option_force(session):
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"

    with pytest.raises(SnowparkPlanException) as ex_info:
        session.read.schema(user_schema).option("force", "false").csv(
            test_file_on_stage
        ).collect()

    assert (
        "The COPY option 'FORCE = false' is not supported by the Snowpark library"
        in ex_info.value.message
    )

    with pytest.raises(SnowparkPlanException) as ex_info:
        session.read.schema(user_schema).option("FORCE", "FALSE").csv(
            test_file_on_stage
        ).collect()

    assert (
        "The COPY option 'FORCE = FALSE' is not supported by the Snowpark library"
        in ex_info.value.message
    )

    with pytest.raises(SnowparkPlanException) as ex_info:
        session.read.schema(user_schema).option("fORce", "faLsE").csv(
            test_file_on_stage
        ).collect()

    assert (
        "The COPY option 'FORCE = faLsE' is not supported by the Snowpark library"
        in ex_info.value.message
    )

    # no error
    session.read.schema(user_schema).option("fORce", "true").csv(
        test_file_on_stage
    ).collect()

    session.read.schema(user_schema).option("fORce", "trUe").csv(
        test_file_on_stage
    ).collect()


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1432856 on_error option is not supported",
)
def test_read_file_on_error_continue_on_csv(session, db_parameters, resources_path):
    broken_file = f"@{tmp_stage_name1}/{test_broken_csv}"

    # skip (2, two, wrong)
    df = (
        session.read.schema(user_schema)
        .option("on_error", "continue")
        .option("COMPRESSION", "none")
        .csv(broken_file)
    )
    res = df.collect()
    res.sort(key=lambda x: x[0])
    assert res == [Row(1, "one", 1.1), Row(3, "three", 3.3)]


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1432856 option on_error is not supported",
)
def test_read_file_on_error_continue_on_avro(session):
    broken_file = f"@{tmp_stage_name1}/{test_file_avro}"

    # skip all
    df = (
        session.read.schema(user_schema)
        .option("on_error", "continue")
        .option("COMPRESSION", "none")
        .csv(broken_file)
    )
    res = df.collect()
    assert res == []


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: parquet not supported",
)
def test_select_and_copy_on_non_csv_format_have_same_result_schema(session):
    path = f"@{tmp_stage_name1}/{test_file_parquet}"

    copy = (
        session.read.option("purge", False).option("COMPRESSION", "none").parquet(path)
    )
    select = session.read.option("COMPRESSION", "none").parquet(path)

    copy_fields = copy.schema.fields
    select_fields = select.schema.fields
    assert len(copy_fields) > 0
    assert len(copy_fields) == len(select_fields)
    for c, f in zip(copy_fields, select_fields):
        assert c.datatype == f.datatype
        assert c.name == f.name
        assert c.nullable == f.nullable
        assert (
            c.column_identifier.normalized_name == f.column_identifier.normalized_name
        )
        assert c.column_identifier.quoted_name == f.column_identifier.quoted_name


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_pattern(session, mode):
    assert (
        get_reader(session, mode)
        .schema(user_schema)
        .option("COMPRESSION", "none")
        .option("pattern", ".*CSV[.]csv")
        .csv(f"@{tmp_stage_name1}")
        .count()
        == 4
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Local testing does not support file.put",
)
@pytest.mark.xfail(
    reason="SNOW-2138003: Param difference between environments",
    strict=False,
)
@pytest.mark.parametrize("mode", ["select", "copy"])
def test_pattern_with_infer(session, mode):
    stage_name = Utils.random_name_for_temp_object(TempObjectType.STAGE)
    expected_schema = StructType(
        [
            StructField("col1", LongType(), True),
            StructField("col2", StringType(), True),
            StructField("col3", DecimalType(2, 1), True),
        ]
    )
    example_data = [expected_schema.names] + [(1, "A", 2.3), (2, "B", 3.4)]
    incompatible_data = ["A", "B", "C", "D"]
    expected_rows = [
        Row(1, "A", 2.3),
        Row(2, "B", 3.4),
    ]

    try:
        Utils.create_stage(session, stage_name, is_temporary=True)
        with tempfile.TemporaryDirectory() as temp_dir:
            for i in range(3):
                good_file_path = os.path.join(temp_dir, f"good{i}.csv")
                bad_file_path = os.path.join(temp_dir, f"bad{i}.csv")

                with open(good_file_path, "w+", newline="") as ofile:
                    csv_writer = csv.writer(ofile)
                    csv_writer.writerows(example_data)

                with open(bad_file_path, "w+", newline="") as ofile:
                    csv_writer = csv.writer(ofile)
                    csv_writer.writerows(incompatible_data)

                session.file.put(good_file_path, f"@{stage_name}/path")
                session.file.put(bad_file_path, f"@{stage_name}/path")

        def base_reader(include_pattern=True):
            reader = (
                get_reader(session, mode)
                .option("INFER_SCHEMA", True)
                .option("INFER_SCHEMA_OPTIONS", {"MAX_RECORDS_PER_FILE": 10000})
                .option("PARSE_HEADER", True)
            )
            if include_pattern:
                reader = reader.option("PATTERN", ".*good.*")
            return reader

        # Test loading a single file
        single_file_df = base_reader(False).csv(f"@{stage_name}/path/good1.csv")
        Utils.check_answer(single_file_df, expected_rows)

        # Test loading a single file while pattern is defined
        reader = base_reader()
        single_file_with_pattern_df = reader.csv(f"@{stage_name}/path/good1.csv")
        Utils.check_answer(single_file_with_pattern_df, expected_rows)

        # Test loading a directory while including patter and FILES
        reader = base_reader().option(
            "INFER_SCHEMA_OPTIONS", {"FILES": ["good1.csv.gz"]}
        )
        file_override_df = reader.csv(f"@{stage_name}/path")
        Utils.check_answer(file_override_df, expected_rows * 3)

        # Test just pattern
        reader = base_reader()
        df = reader.csv(f"@{stage_name}/path")
        assert df.schema == expected_schema
        Utils.check_answer(df, expected_rows * 3)

        # Test using fully qualified stage name
        reader = base_reader()
        df = reader.csv(
            f"@{session.get_current_database()}.{session.get_current_schema()}.{stage_name}"
        )
        assert df.schema == expected_schema
        Utils.check_answer(df, expected_rows * 3)

    finally:
        Utils.drop_stage(session, stage_name)


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SQL query not supported",
    run=False,
)
def test_read_staged_file_no_commit(session):
    path = f"@{tmp_stage_name1}/{test_file_csv}"

    # Test reading from staged file with TEMP FILE FORMAT
    session.sql("begin").collect()
    session.read.schema(user_schema).csv(path).collect()
    assert Utils.is_active_transaction(session)
    session.sql("commit").collect()
    assert not Utils.is_active_transaction(session)

    # Test reading from staged file with TEMP TABLE
    session.sql("begin").collect()
    session.read.option("purge", False).schema(user_schema).csv(path).collect()
    assert Utils.is_active_transaction(session)
    session.sql("commit").collect()
    assert not Utils.is_active_transaction(session)


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="This is testing SQL generation",
    run=False,
)
def test_read_csv_with_sql_simplifier(session):
    if session.sql_simplifier_enabled is False:
        pytest.skip("Applicable only when sql simplifier is enabled")
    reader = get_reader(session, "select")
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"
    df = reader.schema(user_schema).csv(test_file_on_stage)
    df1 = df.select("a").select("a").select("a")
    assert df1.queries["queries"][-1].count("SELECT") == 2

    df2 = (
        df.select((col("a") + 1).as_("a"))
        .select((col("a") + 1).as_("a"))
        .select((col("a") + 1).as_("a"))
    )
    assert df2.queries["queries"][-1].count("SELECT") == 4


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="This is testing SQL generation",
    run=False,
)
def test_read_parquet_with_sql_simplifier(session):
    if session.sql_simplifier_enabled is False:
        pytest.skip("Applicable only when sql simplifier is enabled")
    path = f"@{tmp_stage_name1}/{test_file_parquet}"
    df = get_reader(session, "select").parquet(path)
    df1 = df.select("str").select("str").select("str")
    assert df1.queries["queries"][-1].count("SELECT") == 3

    df2 = (
        df.select((col("num") + 1).as_("num"))
        .select((col("num") + 1).as_("num"))
        .select((col("num") + 1).as_("num"))
    )
    assert df2.queries["queries"][-1].count("SELECT") == 4


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1435112: csv infer schema option is not supported",
)
def test_filepath_not_exist_or_empty(session):
    empty_stage = Utils.random_stage_name()
    not_exist_file = f"not_exist_file_{Utils.random_alphanumeric_str(5)}"
    Utils.create_stage(session, empty_stage, is_temporary=True)
    empty_file_path = f"@{empty_stage}/"
    not_exist_file_path = f"@{tmp_stage_name1}/{not_exist_file}"

    with pytest.raises(FileNotFoundError) as ex_info:
        session.read.option("PARSE_HEADER", True).option("INFER_SCHEMA", True).csv(
            empty_file_path
        )
    assert f"Given path: '{empty_file_path}' could not be found or is empty." in str(
        ex_info
    )

    with pytest.raises(FileNotFoundError) as ex_info:
        session.read.option("PARSE_HEADER", True).option("INFER_SCHEMA", True).csv(
            not_exist_file_path
        )
    assert (
        f"Given path: '{not_exist_file_path}' could not be found or is empty."
        in str(ex_info)
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1435112: csv infer schema option is not supported",
)
def test_filepath_with_single_quote(session):
    test_file_on_stage_with_quote = f"'@{tmp_stage_name1}/{test_file_csv}'"
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"
    result1 = (
        session.read.option("INFER_SCHEMA", True).csv(test_file_on_stage).collect()
    )
    result2 = (
        session.read.option("INFER_SCHEMA", True)
        .csv(test_file_on_stage_with_quote)
        .collect()
    )

    assert result1 == result2


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="read json not supported in localtesting",
)
def test_read_json_user_input_schema(session):
    test_file = f"@{tmp_stage_name1}/{test_file_json}"

    schema = StructType(
        [
            StructField("fruit", StringType(), True),
            StructField("size", StringType(), True),
            StructField("color", StringType(), True),
        ]
    )

    df = session.read.schema(schema).json(test_file)
    Utils.check_answer(df, [Row(fruit="Apple", size="Large", color="Red")])

    # schema that have part of column in file and column not in the file
    schema = StructType(
        [
            StructField("fruit", StringType(), True),
            StructField("size", StringType(), True),
            StructField("not_included_column", StringType(), True),
        ]
    )

    df = session.read.schema(schema).json(test_file)
    Utils.check_answer(df, [Row(fruit="Apple", size="Large", not_included_column=None)])

    # schema that have extra column
    schema = StructType(
        [
            StructField("fruit", StringType(), True),
            StructField("size", StringType(), True),
            StructField("color", StringType(), True),
            StructField("extra_column", StringType(), True),
        ]
    )

    df = session.read.schema(schema).json(test_file)
    Utils.check_answer(
        df, [Row(fruit="Apple", size="Large", color="Red", extra_column=None)]
    )

    # schema that have false datatype
    schema = StructType(
        [
            StructField("fruit", StringType(), True),
            StructField("size", StringType(), True),
            StructField("color", IntegerType(), True),
        ]
    )
    with pytest.raises(SnowparkSQLException, match="Failed to cast variant value"):
        session.read.schema(schema).json(test_file).collect()


def test_read_csv_nulls(session):
    # Test that a csv read with NULLVALUE set loads the configured representation as None
    reader = get_reader(session, "select")
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"
    df = (
        reader.option("NULLVALUE", ["one", "two"])
        .schema(user_schema)
        .csv(test_file_on_stage)
    )
    Utils.check_answer(df, [Row(A=1, B=None, C=1.2), Row(A=2, B=None, C=2.2)])


def test_read_csv_alternate_time_formats(session):
    # Test that a csv read with NULLVALUE set loads the configured representation as None
    reader = get_reader(session, "copy")
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv_timestamps}"

    time_format = "HH12.MI.SS.FF3"
    date_format = "YYYYMONDD"
    timestamp_format = f"{date_format}-{time_format}"

    schema = StructType(
        [
            StructField("date", DateType()),
            StructField("timestamp", TimestampType(TimestampTimeZone.NTZ)),
            StructField("time", TimeType()),
        ]
    )

    df = (
        reader.option("DATE_FORMAT", date_format)
        .option("TIME_FORMAT", time_format)
        .option("TIMESTAMP_FORMAT", timestamp_format)
        .option("PARSE_HEADER", False)
        .schema(schema)
        .csv(test_file_on_stage)
    )
    Utils.check_answer(
        df,
        [
            Row(
                datetime.date(2024, 1, 1),
                datetime.datetime(2025, 1, 1, 0, 0, 1),
                datetime.time(0, 0, 1),
            ),
            Row(
                datetime.date(2022, 2, 3),
                datetime.datetime(2022, 2, 3, 1, 2, 3, 456),
                datetime.time(1, 2, 3, 456),
            ),
            Row(
                datetime.date(2025, 2, 13),
                datetime.datetime(2025, 2, 13, 6, 33, 36, 348925),
                datetime.time(6, 33, 36, 348925),
            ),
        ],
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="sql not supported in local testing mode",
)
def test_read_multiple_csvs(session):
    reader = get_reader(session, "copy")
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    user_schema = StructType(
        [
            StructField("A", LongType()),
            StructField("B", StringType()),
            StructField("C", DoubleType()),
        ]
    )
    test_file_on_stage = f"@{tmp_stage_name1}/"
    try:
        Utils.create_table(session, table_name, "A float, B string, C double")
        df = reader.schema(user_schema).csv(test_file_on_stage)
        df.copy_into_table(table_name, files=[test_file_csv, test_file2_csv])
        Utils.check_answer(
            session.table(table_name),
            [
                Row(3.0, "three", 3.3),
                Row(4.0, "four", 4.4),
                Row(1.0, "one", 1.2),
                Row(2.0, "two", 2.2),
            ],
        )
    finally:
        Utils.drop_table(session, table_name)


@pytest.mark.parametrize(
    "file_format_name,infer_schema",
    [
        ("ab_c1", True),
        ("ABC", True),
        ('"a$B12""cD"', True),
        ('"a$B12""cD"', False),
    ],
)
@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="sql not supported in local testing mode",
)
@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="Temp file format with custom name is not supported in stored proc",
)
def test_read_file_with_enforced_existing_file_format(
    session, file_format_name, infer_schema
):
    try:
        session.sql(
            f"CREATE OR REPLACE TEMP FILE FORMAT {file_format_name} TYPE = 'CSV' FIELD_DELIMITER = ','"
        ).collect()
        test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"

        df_reader = session.read.option("FORMAT_NAME", file_format_name).option(
            "ENFORCE_EXISTING_FILE_FORMAT", True
        )

        if not infer_schema:
            df_reader = df_reader.schema(user_schema)

        df = df_reader.csv(test_file_on_stage)

        # Verify there are no queries related to temp file format creation or removal
        assert len(df.queries["queries"]) == 1
        assert len(df.queries["post_actions"]) == 0

        result = df.collect()

        assert len(result) == 2
        assert len(result[0]) == 3
    finally:
        session.sql(f"DROP FILE FORMAT IF EXISTS {file_format_name}").collect()


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="sql not supported in local testing mode",
)
@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="Temp file format with custom name is not supported in stored proc",
)
def test_enforce_existing_file_format_with_additional_format_type_options(session):
    file_format_name = "ABC"

    try:
        session.sql(
            f"CREATE OR REPLACE TEMP FILE FORMAT {file_format_name} TYPE = 'CSV' FIELD_DELIMITER = ','"
        ).collect()
        test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"

        with pytest.raises(
            ValueError,
            match="Option 'ENFORCE_EXISTING_FILE_FORMAT' can not be used with any format type options.",
        ):
            (
                session.read.option("FORMAT_NAME", file_format_name)
                .option("ENFORCE_EXISTING_FILE_FORMAT", True)
                .option(
                    "DELIMITER", "!"
                )  # Forbidden option if ENFORCE_EXISTING_FILE_FORMAT is set
                .csv(test_file_on_stage)
            )
    finally:
        session.sql(f"DROP FILE FORMAT IF EXISTS {file_format_name}").collect()


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="sql not supported in local testing mode",
)
@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="Temp file format with custom name is not supported in stored proc",
)
def test_enforce_existing_file_format_without_providing_format_name(session):
    file_format_name = "ABC"

    try:
        session.sql(
            f"CREATE OR REPLACE TEMP FILE FORMAT {file_format_name} TYPE = 'CSV' FIELD_DELIMITER = ','"
        ).collect()
        test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"

        with pytest.raises(
            ValueError,
            match="Setting the ENFORCE_EXISTING_FILE_FORMAT option requires providing FORMAT_NAME.",
        ):
            (
                session.read.option("ENFORCE_EXISTING_FILE_FORMAT", True).csv(
                    test_file_on_stage
                )
            )
    finally:
        session.sql(f"DROP FILE FORMAT IF EXISTS {file_format_name}").collect()


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Read option 'FORMAT_NAME' not supported in local testing mode",
)
def test_enforce_existing_file_format_object_doesnt_exist(session):
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"

    df = (
        session.read.schema(StructType([StructField("xyz", StringType())]))
        .option("FORMAT_NAME", "NON_EXISTENT_FILE_FORMAT")
        .option("ENFORCE_EXISTING_FILE_FORMAT", True)
        .csv(test_file_on_stage)
    )

    with pytest.raises(
        SnowparkSQLException,
        match="File format 'NON_EXISTENT_FILE_FORMAT' does not exist or not authorized.",
    ):
        df.collect()


@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="TODO: SNOW-2201113, STRING types have different precision in stored procs due to LOB",
)
@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="relaxed_types not supported by local testing mode",
)
def test_use_relaxed_types(session):
    stage_name = Utils.random_name_for_temp_object(TempObjectType.STAGE)
    header = ("A", "B", "C")
    short_data = [
        ("A", 1.1, True),
        ("B", 1.2, True),
    ]
    long_data = [
        ("CCCCCCCCCC", 3.9999999993, False),
        ("DDDDDDDDDD", 4.9999999994, False),
    ]

    schema = StructType(
        [
            StructField("A", StringType(), True),
            StructField("B", DecimalType(2, 1), True),
            StructField("C", BooleanType(), True),
        ]
    )

    relaxed_schema = StructType(
        [
            StructField("A", StringType(), True),
            StructField("B", DoubleType(), True),
            StructField("C", BooleanType(), True),
        ]
    )

    def write_csv(data):
        with tempfile.NamedTemporaryFile(
            mode="w+",
            delete=False,
            suffix=".csv",
            newline="",
        ) as file:
            writer = csv.writer(file)
            writer.writerow(header)
            for row in data:
                writer.writerow(row)
            return file.name

    short_path = write_csv(short_data)
    long_path = write_csv(long_data)

    try:
        Utils.create_stage(session, stage_name, is_temporary=True)
        short_result = session.file.put(
            short_path, f"@{stage_name}", auto_compress=False, overwrite=True
        )

        session.file.put(
            long_path, f"@{stage_name}", auto_compress=False, overwrite=True
        )

        # Infer schema from only the short file
        constrained_reader = session.read.options(
            {
                "INFER_SCHEMA": True,
                "INFER_SCHEMA_OPTIONS": {"FILES": [short_result[0].target]},
                "PARSE_HEADER": True,
                # Only load the short file
                "PATTERN": f".*{short_result[0].target}",
            }
        )

        # df1 uses constrained types
        df1 = constrained_reader.csv(f"@{stage_name}/")
        assert df1.schema == schema
        # Load both files
        constrained_reader.option("PATTERN", None)

        # Data is truncated if loading the higher prescion file
        df1_both = constrained_reader.csv(f"@{stage_name}/")
        Utils.check_answer(
            df1_both,
            [
                Row("CCCCCCCCCC", Decimal("4.0"), False),
                Row("DDDDDDDDDD", Decimal("5.0"), False),
                Row("A", Decimal("1.1"), True),
                Row("B", Decimal("1.2"), True),
            ],
        )

        # Relaxed reader uses more permissive types
        relaxed_reader = session.read.options(
            {
                "INFER_SCHEMA": True,
                "INFER_SCHEMA_OPTIONS": {
                    "FILES": [short_result[0].target],
                    "USE_RELAXED_TYPES": True,
                },
                "PARSE_HEADER": True,
                "PATTERN": f".*{short_result[0].target}",
            }
        )

        # df2 users relaxed types
        df2 = relaxed_reader.csv(f"@{stage_name}/")
        assert df2.schema == relaxed_schema
        relaxed_reader.option("PATTERN", None)

        # Data is no longer truncated
        df2_both = relaxed_reader.csv(f"@{stage_name}/")
        Utils.check_answer(
            df2_both,
            [
                Row("CCCCCCCCCC", 3.9999999993, False),
                Row("DDDDDDDDDD", 4.9999999994, False),
                Row("A", 1.1, True),
                Row("B", 1.2, True),
            ],
        )
    finally:
        Utils.drop_stage(session, stage_name)
        if os.path.exists(short_path):
            os.remove(short_path)
        if os.path.exists(long_path):
            os.remove(long_path)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="relaxed_types not supported by local testing mode",
)
def test_try_cast(session):
    stage_name = Utils.random_name_for_temp_object(TempObjectType.STAGE)
    header = ("A", "B", "C")
    valid_data = [
        ("A", 1.1, True),
        ("B", 1.2, True),
    ]
    invalid_data = [
        ("C", "invalid", False),
        ("D", 4.9999999994, "invalid"),
        (0, 5, "invalid"),
        (1, "invalid", "invalid"),
    ]

    schema = StructType(
        [
            StructField("A", StringType(), True),
            StructField("B", DecimalType(2, 1), True),
            StructField("C", BooleanType(), True),
        ]
    )

    lob_schema = StructType(
        [
            StructField("A", StringType(134217728), True),
            StructField("B", DecimalType(2, 1), True),
            StructField("C", BooleanType(), True),
        ]
    )

    def write_csv(data):
        with tempfile.NamedTemporaryFile(
            mode="w+",
            delete=False,
            suffix=".csv",
            newline="",
        ) as file:
            writer = csv.writer(file)
            writer.writerow(header)
            for row in data:
                writer.writerow(row)
            return file.name

    valid_path = write_csv(valid_data)
    invalid_data_path = write_csv(invalid_data)

    try:
        Utils.create_stage(session, stage_name, is_temporary=True)
        valid_result = session.file.put(
            valid_path, f"@{stage_name}", auto_compress=False, overwrite=True
        )

        session.file.put(
            invalid_data_path, f"@{stage_name}", auto_compress=False, overwrite=True
        )

        # Infer schema from only the valid file
        default_reader = session.read.options(
            {
                "INFER_SCHEMA": True,
                "INFER_SCHEMA_OPTIONS": {"FILES": [valid_result[0].target]},
                "PARSE_HEADER": True,
                # Only load the valid file
                "PATTERN": f".*{valid_result[0].target}",
                # Files that fail to parse are skipped
                "COPY_OPTIONS": {"ON_ERROR": "SKIP_FILE"},
            }
        )

        # df1 uses constrained types
        df1 = default_reader.csv(f"@{stage_name}/")
        # TODO: SNOW-2201113, revert the condition once the LOB issue is fixed
        assert df1.schema == schema or df1.schema == lob_schema
        # Try to load both files
        default_reader.option("PATTERN", None)

        # Only the data for the valid file is available
        df1_both = default_reader.csv(f"@{stage_name}/")
        Utils.check_answer(
            df1_both, [Row("A", Decimal("1.1"), True), Row("B", Decimal("1.2"), True)]
        )

        # try_cast reader attempts to recover as much data as possible
        try_cast_reader = session.read.options(
            {
                "INFER_SCHEMA": True,
                "INFER_SCHEMA_OPTIONS": {
                    "FILES": [valid_result[0].target],
                },
                "TRY_CAST": True,
                "PARSE_HEADER": True,
                # Files that fail to parse are skipped
                "COPY_OPTIONS": {"ON_ERROR": "SKIP_FILE"},
            }
        )

        # Valid data is available, invalid data is null
        df2 = try_cast_reader.csv(f"@{stage_name}/")
        Utils.check_answer(
            df2,
            [
                Row("A", Decimal("1.1"), True),
                Row("B", Decimal("1.2"), True),
                Row("C", None, False),
                Row("D", Decimal("5.0"), None),
                Row("0", Decimal("5.0"), None),
                Row("1", None, None),
            ],
        )
    finally:
        Utils.drop_stage(session, stage_name)
        if os.path.exists(valid_path):
            os.remove(valid_path)
        if os.path.exists(invalid_data_path):
            os.remove(invalid_data_path)
