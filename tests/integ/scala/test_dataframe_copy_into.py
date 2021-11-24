#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from tests.utils import TestFiles, Utils

test_file_csv = "testCSV.csv"
test_file2_csv = "test2CSV.csv"
test_file_csv_colon = "testCSVcolon.csv"
test_file_csv_quotes = "testCSVquotes.csv"
test_file_json = "testJson.json"
test_file_avro = "test.avro"
test_file_parquet = "test.parquet"
test_file_orc = "test.orc"
test_file_xml = "test.xml"
test_broken_csv = "broken.csv"

user_fields = [
    StructField("A", LongType()),
    StructField("B", StringType()),
    StructField("C", DoubleType()),
]
user_schema = StructType(user_fields)


@pytest.fixture(scope="module")
def tmp_stage_name1(session):
    stage_name = Utils.random_stage_name()
    Utils.create_stage(session, stage_name)
    try:
        yield stage_name
    finally:
        Utils.drop_stage(session, stage_name)


@pytest.fixture(scope="module")
def tmp_stage_name2(session):
    stage_name = Utils.random_stage_name()
    Utils.create_stage(session, stage_name)
    try:
        yield stage_name
    finally:
        Utils.drop_stage(session, stage_name)


@pytest.fixture(scope="module")
def tmp_table_name(session):
    table_name = Utils.random_name()
    Utils.create_table(session, table_name, "a Int, b String, c Double")
    try:
        yield table_name
    finally:
        Utils.drop_table(session, table_name)


@pytest.fixture(scope="module", autouse=True)
def upload_files(session, tmp_stage_name1, tmp_stage_name2, resources_path):
    test_files = TestFiles(resources_path)
    Utils.upload_to_stage(
        session, "@" + tmp_stage_name1, test_files.test_file_csv, compress=False
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
        test_files.test_file_json,
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


def test_copy_csv_basic(session, tmp_stage_name1, tmp_table_name):
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"
    assert session.table(tmp_table_name).count() == 0
    df = session.read.schema(user_schema).csv(test_file_on_stage)
    df.copy_into_table(tmp_table_name)
    Utils.check_answer(
        session.table(tmp_table_name),
        [Row(1, "one", 1.2), Row(2, "two", 2.2)],
        sort=False,
    )

    # run COPY again, the loaded files will be skipped by default
    df.copy_into_table(tmp_table_name)
    Utils.check_answer(
        session.table(tmp_table_name),
        [Row(1, "one", 1.2), Row(2, "two", 2.2)],
        sort=False,
    )

    # Copy again with FORCE = TRUE, loaded file are NOT skipped.
    df.copy_into_table(tmp_table_name, copy_options={"FORCE": True})
    Utils.check_answer(
        session.table(tmp_table_name),
        [
            Row(1, "one", 1.2),
            Row(2, "two", 2.2),
            Row(1, "one", 1.2),
            Row(2, "two", 2.2),
        ],
        sort=False,
    )


def test_copy_csv_create_table_if_not_exists(session, tmp_stage_name1):
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"
    df = session.read.schema(user_schema).csv(test_file_on_stage)
    test_table_name = Utils.random_name()
    df.copy_into_table(test_table_name)
    try:
        df2 = session.table(test_table_name)
        Utils.check_answer(df2, [Row(1, "one", 1.2), Row(2, "two", 2.2)])
        assert df2.schema.names == [f.name for f in user_fields]
    finally:
        Utils.drop_table(session, test_table_name)


def test_saveAsTable_not_affect_copy_into(session, tmp_stage_name1):
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"
    table_name = Utils.random_name()
    Utils.create_table(session, table_name, "c1 Int, c2 String, c3 Double")
    try:
        assert session.table(table_name).count() == 0
        df = session.read.schema(user_schema).csv(test_file_on_stage)
        df.copy_into_table(table_name)
        Utils.check_answer(
            session.table(table_name), [Row(1, "one", 1.2), Row(2, "two", 2.2)]
        )

        # Write data with saveAsTable(), loaded file are NOT skipped.
        df.write.saveAsTable(table_name)
        Utils.check_answer(
            session.table(table_name).collect(),
            [
                Row(1, "one", 1.2),
                Row(2, "two", 2.2),
                Row(1, "one", 1.2),
                Row(2, "two", 2.2),
            ],
            sort=False,
        )

        # Write data with saveAsTable() again, loaded file are NOT skipped.
        df.write.saveAsTable(table_name)
        Utils.check_answer(
            session.table(table_name),
            [
                Row(1, "one", 1.2),
                Row(2, "two", 2.2),
                Row(1, "one", 1.2),
                Row(2, "two", 2.2),
                Row(1, "one", 1.2),
                Row(2, "two", 2.2),
            ],
            sort=False,
        )
    finally:
        Utils.drop_table(session, table_name)


def test_copy_csv_transformation(session, tmp_stage_name1):
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"
    table_name = Utils.random_name()
    Utils.create_table(session, table_name, "c1 String, c2 String, c3 String")
    try:
        assert session.table(table_name).count() == 0
        df = session.read.schema(user_schema).csv(test_file_on_stage)
        # copy data with $1, $2, $3
        df.copy_into_table(
            table_name, transformations=[col("$1"), col("$2"), col("$3")]
        )
        Utils.check_answer(
            session.table(table_name), [Row("1", "one", "1.2"), Row("2", "two", "2.2")]
        )
        # Copy data in order of $3, $2, $1 with FORCE = TRUE
        df.copy_into_table(
            table_name,
            transformations=[col("$3"), col("$2"), col("$1")],
            copy_options={"FORCE": True},
        )
        Utils.check_answer(
            session.table(table_name),
            [
                Row("1", "one", "1.2"),
                Row("2", "two", "2.2"),
                Row("1.2", "one", "1"),
                Row("2.2", "two", "2"),
            ],
            sort=False,
        )
        # Copy data in order of $2, $3, $1 with FORCE = TRUE and skip_header = 1
        df.copy_into_table(
            table_name,
            transformations=[col("$2"), col("$3"), col("$1")],
            format_type_options={"SKIP_HEADER": 1},
            copy_options={"FORCE": True},
        )
        Utils.check_answer(
            session.table(table_name),
            [
                Row("1", "one", "1.2"),
                Row("2", "two", "2.2"),
                Row("1.2", "one", "1"),
                Row("2.2", "two", "2"),
                Row("two", "2.2", "2"),
            ],
            sort=False,
        )
    finally:
        Utils.drop_table(session, table_name)


def test_copy_csv_negative(session, tmp_stage_name1):
    ...


"""
  test("copy csv test: negative test") {
    val testFileOnStage = s"@$tmpStageName/$testFileCsv"
    val df = session.read.schema(userSchema).csv(testFileOnStage)

    // case 1: copyInto with transformation, but table doesn't exist
    dropTable(testTableName)(session)
    val ex1 = intercept[SnowparkClientException] {
      df.copyInto(testTableName, Seq(col("$1").as("c1_alias")))
    }
    assert(
      ex1.errorCode.equals("0122") && ex1.message.contains(
        s"Cannot create the target table $testTableName because Snowpark cannot determine" +
          " the column names to use. You should create the table before calling copyInto()."))

    // case 2: copyInto transformation doesn't match table schema.
    createTable(testTableName, "c1 String")
    // table has one column, transformation has 2 columns.
    val ex2 = intercept[SnowflakeSQLException] {
      df.copyInto(testTableName, Seq(col("$1").as("c1"), col("$2").as("c2")))
    }
    assert(ex2.getMessage.contains("Insert value list does not match column list"))

    // case 3: copyInto transformation doesn't match table schema.
    createTable(testTableName, "c1 String, c2 String")
    // table has two column, transformation has one columns.
    val ex3 = intercept[SnowflakeSQLException] {
      df.copyInto(testTableName, Seq(col("$1").as("c1")))
    }
    assert(ex3.getMessage.contains("Insert value list does not match column list"))
  }
"""
