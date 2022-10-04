#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import datetime
import json
from decimal import Decimal
from textwrap import dedent

import pytest

from snowflake.snowpark import Row, Session
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import (
    SnowparkDataframeException,
    SnowparkDataframeReaderException,
    SnowparkSQLException,
)
from snowflake.snowpark.functions import builtin, col, get, lit, sql_expr, xmlget
from snowflake.snowpark.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from tests.utils import IS_IN_STORED_PROC, TestFiles, Utils

test_file_csv = "testCSV.csv"
test_file2_csv = "test2CSV.csv"
test_file_csv_colon = "testCSVcolon.csv"
test_file_csv_quotes = "testCSVquotes.csv"
test_file_json = "testJson.json"
test_file_csv_special_format = "testCSVspecialFormat.csv"
test_file_json_special_format = "testJSONspecialFormat.json.gz"
test_file_avro = "test.avro"
test_file_parquet = "test.parquet"
test_file_all_data_types_parquet = "test_all_data_types.parquet"
test_file_with_special_characters_parquet = "test_file_with_special_characters.parquet"
test_file_orc = "test.orc"
test_file_xml = "test.xml"
test_broken_csv = "broken.csv"

user_fields = [
    StructField("A", LongType()),
    StructField("B", StringType()),
    StructField("C", DoubleType()),
]
user_schema = StructType(user_fields)


def create_df_for_file_format(
    session: Session, file_format: str, file_location: str, infer_schema: bool = False
):
    df_reader = session.read
    if not infer_schema and file_format not in ("json", "xml"):
        df_reader.option("INFER_SCHEMA", False)
    if "json" == file_format:
        df = df_reader.json(file_location)
    elif "parquet" == file_format:
        df = df_reader.parquet(file_location)
    elif "avro" == file_format:
        df = df_reader.avro(file_location)
    elif "orc" == file_format:
        df = df_reader.orc(file_location)
    else:  # "xml" == file_format:
        df = df_reader.xml(file_location)
    return df


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
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
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
        test_files.test_file_csv_special_format,
        compress=False,
    )
    Utils.upload_to_stage(
        session,
        "@" + tmp_stage_name1,
        test_files.test_file_json_special_format,
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
    df.copy_into_table(tmp_table_name, force=True)
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

    # test statement params
    df.copy_into_table(tmp_table_name, statement_params={"SF_PARTNER": "FAKE_PARTNER"})
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
    test_table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    df.copy_into_table(test_table_name)
    try:
        df2 = session.table(test_table_name)
        Utils.check_answer(df2, [Row(1, "one", 1.2), Row(2, "two", 2.2)])
        assert df2.schema.names == [f.name for f in user_fields]
    finally:
        Utils.drop_table(session, test_table_name)


def test_save_as_table_not_affect_copy_into(session, tmp_stage_name1):
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, "c1 Int, c2 String, c3 Double")
    try:
        assert session.table(table_name).count() == 0
        df = session.read.schema(user_schema).csv(test_file_on_stage)
        df.copy_into_table(table_name)
        Utils.check_answer(
            session.table(table_name), [Row(1, "one", 1.2), Row(2, "two", 2.2)]
        )

        # Write data with save_as_table(), loaded file are NOT skipped.
        df.write.save_as_table(table_name, mode="append")
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

        # Write data with save_as_table() again, loaded file are NOT skipped.
        df.write.save_as_table(table_name, mode="append")
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


@pytest.mark.xfail(reason="SNOW-632268 flaky test", strict=False)
@pytest.mark.parametrize(
    "trans_columns", [([col("$1"), col("$2"), col("$3")]), (["$1", "$2", "$3"])]
)
def test_copy_csv_transformation(session, tmp_stage_name1, trans_columns):
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, "c1 String, c2 String, c3 String")
    try:
        assert session.table(table_name).count() == 0
        df = session.read.schema(user_schema).csv(test_file_on_stage)
        # copy data with $1, $2, $3
        df.copy_into_table(table_name, transformations=trans_columns)
        Utils.check_answer(
            session.table(table_name), [Row("1", "one", "1.2"), Row("2", "two", "2.2")]
        )
        # Copy data in order of $3, $2, $1 with FORCE = TRUE
        df.copy_into_table(
            table_name,
            transformations=trans_columns[::-1],
            force=True,
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
            transformations=[trans_columns[1], trans_columns[2], trans_columns[0]],
            format_type_options={"SKIP_HEADER": 1},
            force=True,
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


def test_copy_csv_negative(session, tmp_stage_name1, tmp_table_name):
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"
    df = session.read.schema(user_schema).csv(test_file_on_stage)
    # case 1: copy into a non-existing table with transformation but doesn't match the provided schema
    with pytest.raises(SnowparkDataframeReaderException) as exec_info:
        table_name_not_exist = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        df.copy_into_table(
            table_name_not_exist, transformations=[col("$1").as_("c1_alias")]
        )
    assert (
        f"Cannot create the target table {table_name_not_exist} because Snowpark cannot determine the column names to use. You should create the table before calling copy_into_table()"
        in str(exec_info)
    )

    # case 2: copy into an existing table table with unmatched transformations
    with pytest.raises(SnowparkSQLException) as exec_info:
        df.copy_into_table(tmp_table_name, transformations=[col("$s1").as_("c1_alias")])
    assert "Insert value list does not match column list expecting 3 but got 1" in str(
        exec_info
    )


def test_copy_csv_copy_transformation_with_column_names(session, tmp_stage_name1):
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"

    # create target table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, "c1 String, c2 String, c3 String")
    try:
        assert session.table(table_name).count() == 0
        df = session.read.schema(user_schema).csv(test_file_on_stage)
        df.copy_into_table(
            table_name,
            target_columns=["c1", "c2"],
            transformations=[col("$1"), col("$2")],
        )
        Utils.check_answer(
            session.table(table_name), [Row("1", "one", None), Row("2", "two", None)]
        )

        # Copy data in order of $3, $2 to column c3 and c2 with FORCE = TRUE
        df.copy_into_table(
            table_name,
            target_columns=["c3", "c2"],
            transformations=[col("$3"), col("$2")],
            force=True,
        )
        Utils.check_answer(
            session.table(table_name),
            [
                Row("1", "one", None),
                Row("2", "two", None),
                Row(None, "one", "1.2"),
                Row(None, "two", "2.2"),
            ],
            sort=False,
        )

        #     // Copy data $1 to column c3 with FORCE = TRUE and skip_header = 1
        df.copy_into_table(
            table_name,
            target_columns=["c3"],
            transformations=[col("$1")],
            force=True,
            format_type_options={"skip_header": 1},
        )
        Utils.check_answer(
            session.table(table_name),
            [
                Row("1", "one", None),
                Row("2", "two", None),
                Row(None, "one", "1.2"),
                Row(None, "two", "2.2"),
                Row(None, None, "2"),
            ],
            sort=False,
        )
    finally:
        Utils.drop_table(session, table_name)


def test_copy_csv_copy_into_columns_without_transformation(session, tmp_stage_name1):
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"

    # create target table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(
        session, table_name, "c1 String, c2 String, c3 String, c4 String"
    )
    try:
        assert session.table(table_name).count() == 0
        df = session.read.schema(user_schema).csv(test_file_on_stage)
        # copy data to column c1, c2, c3
        df.copy_into_table(table_name, target_columns=["c1", "c2", "c3"])
        Utils.check_answer(
            session.table(table_name),
            [Row("1", "one", "1.2", None), Row("2", "two", "2.2", None)],
        )

        # more target table columns than CSV data columns
        df.copy_into_table(
            table_name, target_columns=["c1", "c2", "c3", "c4"], force=True
        )
        Utils.check_answer(
            session.table(table_name),
            [
                Row("1", "one", "1.2", None),
                Row("2", "two", "2.2", None),
                Row("1", "one", "1.2", None),
                Row("2", "two", "2.2", None),
            ],
            sort=False,
        )
    finally:
        Utils.drop_table(session, table_name)


def test_copy_json_write_with_column_names(session, tmp_stage_name1):
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_json}"
    df = session.read.json(test_file_on_stage)

    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, "c1 String, c2 Variant, c3 String")
    try:
        df.copy_into_table(
            table_name,
            target_columns=["c1", "c2"],
            transformations=[sql_expr("$1:color"), sql_expr("$1:fruit")],
        )
        Utils.check_answer(session.table(table_name), [Row("Red", '"Apple"', None)])
    finally:
        Utils.drop_table(session, table_name)


def test_csv_read_format_name(session, tmp_stage_name1):
    temp_file_fmt_name = Utils.random_name_for_temp_object(TempObjectType.FILE_FORMAT)
    session.sql(
        f"create temporary file format {temp_file_fmt_name} type = csv skip_header=1 "
        "null_if = 'none';"
    ).collect()
    df = (
        session.read.schema(
            StructType(
                [
                    StructField("ID", IntegerType()),
                    StructField("USERNAME", StringType()),
                    StructField("FIRSTNAME", StringType()),
                    StructField("LASTNAME", StringType()),
                ]
            )
        )
        .option("format_name", temp_file_fmt_name)
        .csv(
            f"@{tmp_stage_name1}/{test_file_csv_special_format}",
        )
        .sort("ID")
        .collect()
    )

    Utils.check_answer(
        df,
        [
            Row(0, "admin", None, None),
            Row(1, "test_user", "test", "user"),
        ],
    )


def test_copy_into_csv_format_name(session, tmp_stage_name1):
    temp_table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    file_fmt_name = Utils.random_name_for_temp_object(TempObjectType.FILE_FORMAT)
    session.sql(
        dedent(
            f"""create temporary table {temp_table_name}
                (ID number not null, username varchar, firstname varchar, lastname varchar);
            """
        )
    ).collect()
    session.sql(
        f"create temporary file format {file_fmt_name} type = csv skip_header=1 "
        "null_if = 'none';"
    ).collect()
    session.read.option("format_name", file_fmt_name).schema(
        StructType(
            [
                StructField("ID", IntegerType()),
                StructField("USERNAME", StringType()),
                StructField("FIRSTNAME", StringType()),
                StructField("LASTNAME", StringType()),
            ]
        )
    ).csv(f"@{tmp_stage_name1}/{test_file_csv_special_format}",).copy_into_table(
        temp_table_name
    )

    Utils.check_answer(
        session.table(temp_table_name).sort("ID"),
        [
            Row(0, "admin", None, None),
            Row(1, "test_user", "test", "user"),
        ],
    )


def test_json_read_format_name(session, tmp_stage_name1):
    file_fmt_name = Utils.random_name_for_temp_object(TempObjectType.FILE_FORMAT)
    session.sql(
        f"create temporary file format {file_fmt_name} type = json compression=gzip;"
    ).collect()
    sf_df = session.read.option("format_name", file_fmt_name).json(
        f"@{tmp_stage_name1}/{test_file_json_special_format}",
    )

    assert any(
        f"FILE_FORMAT  => '{file_fmt_name}'" in q for q in sf_df.queries["queries"]
    )

    df = sf_df.collect()

    assert json.loads(df[0][0]) == [
        {
            "id": 10,
            "log": "user xyz logged in",
            "ts": "20:57:01.123456789",
        },
    ]


def test_copy_into_json_format_name(session, tmp_stage_name1):
    file_fmt_name = Utils.random_name_for_temp_object(TempObjectType.FILE_FORMAT)
    temp_table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.sql(f"create temporary table {temp_table_name} (src variant);").collect()
    session.sql(
        f"create temporary file format {file_fmt_name} type = json compression=gzip;"
    ).collect()
    session.read.option("format_name", file_fmt_name).json(
        f"@{tmp_stage_name1}/{test_file_json_special_format}",
    ).copy_into_table(temp_table_name)

    assert json.loads(session.table(temp_table_name).collect()[0][0]) == [
        {
            "id": 10,
            "log": "user xyz logged in",
            "ts": "20:57:01.123456789",
        },
    ]


def test_copy_json_negative_test_with_column_names(session, tmp_stage_name1):
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_json}"
    df = session.read.json(test_file_on_stage)

    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, "c1 String, c2 Variant, c3 String")
    try:
        with pytest.raises(SnowparkSQLException) as exec_info:
            df.copy_into_table(table_name, target_columns=["c1", "c2"])
        assert (
            "JSON file format can produce one and only one column of type"
            in exec_info.value.message
        )
    finally:
        Utils.drop_table(session, table_name)


def test_copy_csv_negative_test_with_column_names(session, tmp_stage_name1):
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, "c1 String, c2 Variant, c3 String")
    try:
        df = session.read.schema(user_schema).csv(test_file_on_stage)
        # case 1: the number of column names does not match the number of transformations
        # table has 3 column, transformation has 2 columns, column name has 1
        with pytest.raises(ValueError) as exec_info:
            df.copy_into_table(
                table_name,
                target_columns=["c1"],
                transformations=[col("$1"), col("$2")],
            )
        assert (
            "Number of column names provided to copy into does not match the number of transformations provided. Number of column names: 1, number of transformations: 2"
            in str(exec_info)
        )

        # case 2: column names contains unknown columns
        with pytest.raises(SnowparkSQLException) as exec_info:
            df.copy_into_table(
                table_name,
                target_columns=["c1", "c2", "c3", "c4"],
                transformations=[col("$1"), col("$2"), col("$3"), col("$4")],
            )
        assert "invalid identifier 'C4'" in str(exec_info)
    finally:
        Utils.drop_table(session, table_name)


def test_transormation_as_clause_no_effect(session, tmp_stage_name1):
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, "c1 String, c2 String")
    try:
        df = session.read.schema(user_schema).csv(test_file_on_stage)
        df.copy_into_table(
            table_name, transformations=[col("$1").as_("aaa"), col("$2").as_("bbb")]
        )
        Utils.check_answer(
            session.table(table_name), [Row("1", "one"), Row("2", "two")]
        )
    finally:
        Utils.drop_table(session, table_name)


def test_copy_with_wrong_dataframe(session):
    with pytest.raises(SnowparkDataframeException) as exec_info:
        session.table("a_table_name").copy_into_table("a_table_name")
    assert (
        "To copy into a table, the DataFrame must be created from a DataFrameReader and specify a file path."
        in str(exec_info)
    )


@pytest.mark.parametrize(
    "file_format, file_name, assert_data, infer_schema",
    [
        (
            "json",
            test_file_json,
            [Row('{\n  "color": "Red",\n  "fruit": "Apple",\n  "size": "Large"\n}')],
            False,
        ),
        (
            "parquet",
            test_file_parquet,
            [
                Row('{\n  "num": 1,\n  "str": "str1"\n}'),
                Row('{\n  "num": 2,\n  "str": "str2"\n}'),
            ],
            True,
        ),
        (
            "parquet",
            test_file_parquet,
            [
                Row('{\n  "num": 1,\n  "str": "str1"\n}'),
                Row('{\n  "num": 2,\n  "str": "str2"\n}'),
            ],
            False,
        ),
        (
            "avro",
            test_file_avro,
            [
                Row('{\n  "num": 1,\n  "str": "str1"\n}'),
                Row('{\n  "num": 2,\n  "str": "str2"\n}'),
            ],
            True,
        ),
        (
            "avro",
            test_file_avro,
            [
                Row('{\n  "num": 1,\n  "str": "str1"\n}'),
                Row('{\n  "num": 2,\n  "str": "str2"\n}'),
            ],
            False,
        ),
        (
            "orc",
            test_file_orc,
            [
                Row('{\n  "num": 1,\n  "str": "str1"\n}'),
                Row('{\n  "num": 2,\n  "str": "str2"\n}'),
            ],
            True,
        ),
        (
            "orc",
            test_file_orc,
            [
                Row('{\n  "num": 1,\n  "str": "str1"\n}'),
                Row('{\n  "num": 2,\n  "str": "str2"\n}'),
            ],
            False,
        ),
        (
            "xml",
            test_file_xml,
            [
                Row("<test>\n  <num>1</num>\n  <str>str1</str>\n</test>"),
                Row("<test>\n  <num>2</num>\n  <str>str2</str>\n</test>"),
            ],
            False,
        ),
    ],
)
def test_copy_non_csv_basic(
    session, tmp_stage_name1, file_format, file_name, assert_data, infer_schema
):
    test_file_on_stage = f"@{tmp_stage_name1}/{file_name}"
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, "c1 Variant")
    try:
        df = create_df_for_file_format(
            session, file_format, test_file_on_stage, infer_schema
        )
        #  copy file in table
        df.copy_into_table(table_name, transformations=[col("$1").as_("A")])
        Utils.check_answer(session.table(table_name), assert_data, sort=False)
        # Copy again. Loaded file is skipped.
        df.copy_into_table(table_name, transformations=[col("$1").as_("A")])
        Utils.check_answer(session.table(table_name), assert_data, sort=False)

        # Copy again with force
        df.copy_into_table(table_name, transformations=[col("$1").as_("A")], force=True)
        Utils.check_answer(session.table(table_name), assert_data * 2, sort=False)
    finally:
        Utils.drop_table(session, table_name)


@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="SNOW-645154 Need to enable ENABLE_SCHEMA_DETECTION_COLUMN_ORDER",
)
@pytest.mark.parametrize(
    "file_format, file_name, schema, transformations, assert_data, infer_schema",
    [
        (
            "json",
            test_file_json,
            "c1 String, c2 Variant, c3 String",
            [
                sql_expr("$1:color").as_("color"),
                sql_expr("$1:fruit").as_("fruit"),
                sql_expr("$1:size").as_("size"),
            ],
            [Row("Red", '"Apple"', "Large")],
            False,
        ),
        (
            "parquet",
            test_file_parquet,
            "NUM Bigint, STR variant, str_length bigint",
            [
                sql_expr("$1:num").cast(IntegerType()).as_("num"),
                sql_expr("$1:str").as_("str"),
                builtin("length")(sql_expr("$1:str")).as_("str_length"),
            ],
            [Row(1, '"str1"', 4), Row(2, '"str2"', 4)],
            True,
        ),
        (
            "parquet",
            test_file_parquet,
            "NUM Bigint, STR variant, str_length bigint",
            [
                sql_expr("$1:num").cast(IntegerType()).as_("num"),
                sql_expr("$1:str").as_("str"),
                builtin("length")(sql_expr("$1:str")).as_("str_length"),
            ],
            [Row(1, '"str1"', 4), Row(2, '"str2"', 4)],
            False,
        ),
        (
            "avro",
            test_file_avro,
            "NUM Bigint, STR variant, str_length bigint",
            [
                sql_expr("$1:num").cast(IntegerType()).as_("num"),
                sql_expr("$1:str").as_("str"),
                builtin("length")(sql_expr("$1:str")).as_("str_length"),
            ],
            [Row(1, '"str1"', 4), Row(2, '"str2"', 4)],
            True,
        ),
        (
            "avro",
            test_file_avro,
            "NUM Bigint, STR variant, str_length bigint",
            [
                sql_expr("$1:num").cast(IntegerType()).as_("num"),
                sql_expr("$1:str").as_("str"),
                builtin("length")(sql_expr("$1:str")).as_("str_length"),
            ],
            [Row(1, '"str1"', 4), Row(2, '"str2"', 4)],
            False,
        ),
        (
            "orc",
            test_file_orc,
            "NUM Bigint, STR variant, str_length bigint",
            [
                sql_expr("$1:num").cast(IntegerType()).as_("num"),
                sql_expr("$1:str").as_("str"),
                builtin("length")(sql_expr("$1:str")).as_("str_length"),
            ],
            [Row(1, '"str1"', 4), Row(2, '"str2"', 4)],
            True,
        ),
        (
            "orc",
            test_file_orc,
            "NUM Bigint, STR variant, str_length bigint",
            [
                sql_expr("$1:num").cast(IntegerType()).as_("num"),
                sql_expr("$1:str").as_("str"),
                builtin("length")(sql_expr("$1:str")).as_("str_length"),
            ],
            [Row(1, '"str1"', 4), Row(2, '"str2"', 4)],
            False,
        ),
        (
            "xml",
            test_file_xml,
            "NUM Bigint, STR variant, str_length bigint",
            [
                get(xmlget(col("$1"), lit("num"), lit(0)), lit("$"))
                .cast(IntegerType())
                .as_("num"),
                get(xmlget(col("$1"), lit("str"), lit(0)), lit("$")).as_("str"),
                builtin("length")(
                    get(xmlget(col("$1"), lit("str"), lit(0)), lit("$"))
                ).as_("str_length"),
            ],
            [Row(1, '"str1"', 4), Row(2, '"str2"', 4)],
            False,
        ),
    ],
)
def test_copy_non_csv_transformation(
    session,
    tmp_stage_name1,
    file_format,
    file_name,
    schema,
    transformations,
    assert_data,
    infer_schema,
):
    test_file_on_stage = f"@{tmp_stage_name1}/{file_name}"
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, schema)
    try:
        df = create_df_for_file_format(
            session, file_format, test_file_on_stage, infer_schema
        )
        df.copy_into_table(table_name, transformations=transformations)
        Utils.check_answer(session.table(table_name), assert_data, sort=False)

        df.copy_into_table(
            table_name,
            transformations=[
                transformations[2],
                transformations[1],
                transformations[0],
            ],
            force=True,
        )

        Utils.check_answer(
            session.table(table_name),
            [*assert_data, *[Row(data[2], data[1], data[0]) for data in assert_data]],
            sort=False,
        )
    finally:
        Utils.drop_table(session, table_name)


@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="SNOW-645154 Need to enable ENABLE_SCHEMA_DETECTION_COLUMN_ORDER",
)
@pytest.mark.parametrize(
    "file_format, file_name, assert_data",
    [
        (
            "parquet",
            test_file_parquet,
            [Row("str1", 1), Row("str2", 2)],
        ),
        (
            "parquet",
            test_file_all_data_types_parquet,
            [
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
                )
            ],
        ),
        (
            "parquet",
            test_file_with_special_characters_parquet,
            [
                Row(
                    "mstephenson@fernandez.com",
                    "835 Frank Tunnel\nWrightmouth, MI 82180-9605",
                    "Violet",
                    34.49726772511229,
                    12.655651149166752,
                    39.57766801952616,
                    4.082620632952961,
                    587.9510539684005,
                ),
                Row(
                    "hduke@hotmail.com",
                    "4547 Archer Common\nDiazchester, CA 06566-8576",
                    "DarkGreen",
                    31.92627202636016,
                    11.109460728682564,
                    37.268958868297744,
                    2.66403418213262,
                    392.2049334443264,
                ),
                Row(
                    "pallen@yahoo.com",
                    "24645 Valerie Unions Suite 582\nCobbborough, DC 99414-7564",
                    "Bisque",
                    33.000914755642675,
                    11.330278057777512,
                    37.11059744212085,
                    4.104543202376424,
                    487.54750486747207,
                ),
                Row(
                    "riverarebecca@gmail.com",
                    "1414 David Throughway\nPort Jason, OH 22070-1220",
                    "SaddleBrown",
                    34.30555662975554,
                    13.717513665142508,
                    36.72128267790313,
                    3.120178782748092,
                    581.8523440352178,
                ),
                Row(
                    "mstephens@davidson-herman.com",
                    "14023 Rodriguez Passage\nPort Jacobville, PR 37242-1057",
                    "MediumAquaMarine",
                    33.33067252364639,
                    12.795188551078114,
                    37.53665330059473,
                    4.446308318351435,
                    599.4060920457634,
                ),
            ],
        ),
        (
            "avro",
            test_file_avro,
            [Row("str1", 1), Row("str2", 2)],
        ),
        (
            "orc",
            test_file_orc,
            [Row("str1", 1), Row("str2", 2)],
        ),
    ],
)
def test_copy_non_csv_auto_transformation(
    session,
    tmp_stage_name1,
    file_format,
    file_name,
    assert_data,
):
    test_file_on_stage = f"@{tmp_stage_name1}/{file_name}"
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    try:
        df = create_df_for_file_format(session, file_format, test_file_on_stage, True)
        df.copy_into_table(table_name)
        Utils.check_answer(session.table(table_name).limit(5), assert_data, sort=False)
    finally:
        Utils.drop_table(session, table_name)


@pytest.mark.parametrize(
    "file_format, file_name, infer_schema",
    [
        ("json", test_file_json, False),
        ("parquet", test_file_parquet, True),
        ("parquet", test_file_parquet, False),
        ("avro", test_file_avro, True),
        ("avro", test_file_avro, False),
        ("orc", test_file_orc, True),
        ("orc", test_file_orc, False),
        ("xml", test_file_xml, False),
    ],
)
def test_copy_non_csv_negative_test(
    session, tmp_stage_name1, file_format, file_name, infer_schema
):
    test_file_on_stage = f"@{tmp_stage_name1}/{file_name}"
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    df = create_df_for_file_format(
        session, file_format, test_file_on_stage, infer_schema
    )
    # For parquet, avro, and orc, this now works
    if file_format in ("json", "xml") or not infer_schema:
        with pytest.raises(SnowparkDataframeReaderException) as exec_info:
            df.copy_into_table(table_name)
        assert (
            f"Cannot create the target table {table_name} because Snowpark cannot determine the column names to use. You should create the table before calling copy_into_table()"
            in str(exec_info)
        )

    with pytest.raises(SnowparkDataframeReaderException) as exec_info:
        df.copy_into_table(table_name, transformations=[col("$1").as_("A")])
    assert (
        f"Cannot create the target table {table_name} because Snowpark cannot determine the column names to use. You should create the table before calling copy_into_table()"
        in str(exec_info)
    )

    Utils.create_table(session, table_name, "c1 String")
    try:
        with pytest.raises(SnowparkSQLException) as exec_info:
            df.copy_into_table(
                table_name, transformations=[col("$1").as_("c1"), col("$2").as_("c2")]
            )
        assert (
            "Insert value list does not match column list expecting 1 but got 2"
            in str(exec_info)
        )
    finally:
        Utils.drop_table(session, table_name)


def test_copy_into_with_validation_mode(session, tmp_stage_name1, tmp_table_name):
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"
    df = session.read.schema(user_schema).csv(test_file_on_stage)
    rows = df.copy_into_table(tmp_table_name, validation_mode="RETURN_2_ROWS")
    assert rows == [Row(1, "one", 1.2), Row(2, "two", 2.2)]


@pytest.mark.parametrize(
    "pattern, result",
    [
        (r".*estCSV\.csv", [Row(1, "one", 1.2), Row(2, "two", 2.2)]),
        (r".*asdf\.csv", []),  # no files match
    ],
)
def test_copy_into_with_pattern(
    session, tmp_stage_name1, tmp_table_name, pattern, result
):
    test_file_on_stage = f"@{tmp_stage_name1}/"
    df = session.read.schema(user_schema).csv(test_file_on_stage)
    rows = df.copy_into_table(
        tmp_table_name, pattern=pattern, validation_mode="RETURN_2_ROWS"
    )
    assert rows == result


def test_copy_into_with_files(session, tmp_stage_name1, tmp_table_name):
    test_file_on_stage = f"@{tmp_stage_name1}/"
    df = session.read.schema(user_schema).csv(test_file_on_stage)
    rows = df.copy_into_table(
        tmp_table_name, files=["testCSV.csv"], validation_mode="RETURN_2_ROWS"
    )
    assert rows == [Row(1, "one", 1.2), Row(2, "two", 2.2)]


def test_copy_into_with_files_no_match(session, tmp_stage_name1, tmp_table_name):
    test_file_on_stage = f"@{tmp_stage_name1}/"
    df = session.read.schema(user_schema).csv(test_file_on_stage)
    rows = df.copy_into_table(
        tmp_table_name, files=["asdf.csv"], validation_mode="RETURN_ERRORS"
    )
    assert (
        "The file might not exist." in rows[0]["ERROR"]
        and rows[0]["FILE"] == "asdf.csv"
    )


# This is not in scala test, but we should cover it
def test_copy_into_new_table_no_commit(session, tmp_stage_name1):
    test_file_on_stage = f"@{tmp_stage_name1}/"
    new_table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)

    try:
        session.sql("begin").collect()
        df = session.read.schema(user_schema).csv(test_file_on_stage)
        assert Utils.is_active_transaction(session)
        df.copy_into_table(
            new_table_name, files=["testCSV.csv"], validation_mode="RETURN_2_ROWS"
        )
        assert Utils.is_active_transaction(session)

        session.sql("commit").collect()
        assert not Utils.is_active_transaction(session)
    finally:
        Utils.drop_table(session, new_table_name)
