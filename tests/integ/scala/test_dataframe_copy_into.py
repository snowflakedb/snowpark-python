#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.connector import ProgrammingError
from snowflake.snowpark import Row
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import (
    SnowparkDataframeException,
    SnowparkDataframeReaderException,
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


def create_df_for_file_format(session, file_format, file_location):
    if "json" == file_format:
        df = session.read.json(file_location)
    elif "parquet" == file_format:
        df = session.read.parquet(file_location)
    elif "avro" == file_format:
        df = session.read.avro(file_location)
    elif "orc" == file_format:
        df = session.read.orc(file_location)
    else:  # "xml" == file_format:
        df = session.read.xml(file_location)
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


def test_saveAsTable_not_affect_copy_into(session, tmp_stage_name1):
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
        df.write.save_as_table(table_name)
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
        df.write.save_as_table(table_name)
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
    with pytest.raises(ProgrammingError) as exec_info:
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


def test_copy_json_negative_test_with_column_names(session, tmp_stage_name1):
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_json}"
    df = session.read.json(test_file_on_stage)

    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, "c1 String, c2 Variant, c3 String")
    try:
        with pytest.raises(ProgrammingError) as exec_info:
            df.copy_into_table(table_name, target_columns=["c1", "c2"])
        assert (
            "JSON file format can produce one and only one column of type variant or object or array. Use CSV file format if you want to load more than one column."
            in str(exec_info)
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
        with pytest.raises(ProgrammingError) as exec_info:
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
    "file_format, file_name, assert_data",
    [
        (
            "json",
            test_file_json,
            [Row('{\n  "color": "Red",\n  "fruit": "Apple",\n  "size": "Large"\n}')],
        ),
        (
            "parquet",
            test_file_parquet,
            [
                Row('{\n  "num": 1,\n  "str": "str1"\n}'),
                Row('{\n  "num": 2,\n  "str": "str2"\n}'),
            ],
        ),
        (
            "avro",
            test_file_avro,
            [
                Row('{\n  "num": 1,\n  "str": "str1"\n}'),
                Row('{\n  "num": 2,\n  "str": "str2"\n}'),
            ],
        ),
        (
            "orc",
            test_file_orc,
            [
                Row('{\n  "num": 1,\n  "str": "str1"\n}'),
                Row('{\n  "num": 2,\n  "str": "str2"\n}'),
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
def test_copy_non_csv_basic(
    session, tmp_stage_name1, file_format, file_name, assert_data
):
    test_file_on_stage = f"@{tmp_stage_name1}/{file_name}"
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, "c1 Variant")
    try:
        df = create_df_for_file_format(session, file_format, test_file_on_stage)
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


@pytest.mark.parametrize(
    "file_format, file_name, schema, transformations, assert_data",
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
):
    test_file_on_stage = f"@{tmp_stage_name1}/{file_name}"
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(session, table_name, schema)
    try:
        df = create_df_for_file_format(session, file_format, test_file_on_stage)
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


@pytest.mark.parametrize(
    "file_format, file_name",
    [
        ("json", test_file_json),
        ("parquet", test_file_parquet),
        ("avro", test_file_avro),
        ("orc", test_file_orc),
        ("xml", test_file_xml),
    ],
)
def test_copy_non_csv_negative_test(session, tmp_stage_name1, file_format, file_name):
    test_file_on_stage = f"@{tmp_stage_name1}/{file_name}"
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    df = create_df_for_file_format(session, file_format, test_file_on_stage)
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
        with pytest.raises(ProgrammingError) as exec_info:
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
