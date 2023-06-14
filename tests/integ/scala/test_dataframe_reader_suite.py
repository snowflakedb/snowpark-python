#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import datetime
import random
from decimal import Decimal

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.column import (
    METADATA_FILE_CONTENT_KEY,
    METADATA_FILE_LAST_MODIFIED,
    METADATA_FILE_ROW_NUMBER,
    METADATA_FILENAME,
    METADATA_START_SCAN_TIME,
)
from snowflake.snowpark.exceptions import (
    SnowparkDataframeReaderException,
    SnowparkPlanException,
    SnowparkSQLException,
)
from snowflake.snowpark.functions import col, lit, sql_expr
from snowflake.snowpark.types import (
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    TimeType,
)
from tests.utils import IS_IN_STORED_PROC, TestFiles, Utils

test_file_csv = "testCSV.csv"
test_file2_csv = "test2CSV.csv"
test_file_csv_colon = "testCSVcolon.csv"
test_file_csv_quotes = "testCSVquotes.csv"
test_file_json = "testJson.json"
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

    print(f"file format is {file_format} and returning reader with .format")
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


@pytest.fixture(scope="module", autouse=True)
def setup(session, resources_path):
    test_files = TestFiles(resources_path)
    Utils.create_stage(session, tmp_stage_name1, is_temporary=True)
    Utils.create_stage(session, tmp_stage_name2, is_temporary=True)
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
    session.sql(f"DROP STAGE IF EXISTS {tmp_stage_name1}").collect()
    session.sql(f"DROP STAGE IF EXISTS {tmp_stage_name2}").collect()


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

    with pytest.raises(SnowparkDataframeReaderException):
        session.read.csv(test_file_on_stage)

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
    assert "Number of columns in file (3) does not match" in str(ex_info)


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
    df2 = get_reader(session, mode).schema(user_schema).csv(test_file_csv_colon)
    with pytest.raises(SnowparkSQLException) as ex_info:
        df2.collect()
    assert "SQL compilation error" in str(ex_info)

    # test for multiple formatTypeOptions
    df3 = (
        get_reader(session, mode)
        .schema(user_schema)
        .option("field_delimiter", ";")
        .option("ENCODING", "wrongEncoding")
        .option("ENCODING", "UTF8")
        .option("COMPRESSION", "NONE")
        .option("skip_header", 1)
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


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_to_read_files_from_stage(session, resources_path, mode):
    data_files_stage = Utils.random_stage_name()
    Utils.create_stage(session, data_files_stage, is_temporary=True)
    test_files = TestFiles(resources_path)
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
        session.sql(f"DROP STAGE IF EXISTS {data_files_stage}")


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
            StructField("d", IntegerType()),
        ]
    )
    test_file = f"@{tmp_stage_name1}/{test_file_csv_quotes}"

    reader = get_reader(session, mode)
    df1 = (
        reader.schema(schema1)
        .option("field_optionally_enclosed_by", '"')
        .csv(test_file)
    )
    res = df1.collect()
    res.sort(key=lambda x: x[0])
    assert res == [Row(1, "one", 1.2, 1), Row(2, "two", 2.2, 2)]

    # without the setting it should fail schema validation
    df2 = get_reader(session, mode).schema(schema1).csv(test_file)
    with pytest.raises(SnowparkSQLException) as ex_info:
        df2.collect()
    assert "Numeric value '\"1\"' is not recognized" in ex_info.value.message

    schema2 = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", StringType()),
            StructField("c", DoubleType()),
            StructField("d", StringType()),
        ]
    )
    df3 = get_reader(session, mode).schema(schema2).csv(test_file)
    df3.collect()
    res = df3.select("d").collect()
    res.sort(key=lambda x: x[0])
    assert res == [Row('"1"'), Row('"2"')]


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

    # test single column works
    reader = session.read.with_metadata(METADATA_FILENAME)
    df = get_df_from_reader_and_file_format(reader, file_format)
    res = df.collect()
    assert res[0]["METADATA$FILENAME"] == filename

    # test that alias works
    reader = session.read.with_metadata(METADATA_FILENAME.alias("filename"))
    df = get_df_from_reader_and_file_format(reader, file_format)
    res = df.collect()
    assert res[0]["FILENAME"] == filename

    # test that column name with str works
    reader = session.read.with_metadata("metadata$filename", "metadata$file_row_number")
    df = get_df_from_reader_and_file_format(reader, file_format)
    res = df.collect()
    assert res[0]["METADATA$FILENAME"] == filename
    assert res[0]["METADATA$FILE_ROW_NUMBER"] >= 0


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_json_with_no_schema(session, mode):
    json_path = f"@{tmp_stage_name1}/{test_file_json}"

    df1 = get_reader(session, mode).json(json_path)
    res = df1.collect()
    assert res == [
        Row('{\n  "color": "Red",\n  "fruit": "Apple",\n  "size": "Large"\n}')
    ]

    # query_test
    res = df1.where(sql_expr("$1:color") == "Red").collect()
    assert res == [
        Row('{\n  "color": "Red",\n  "fruit": "Apple",\n  "size": "Large"\n}')
    ]

    # assert user cannot input a schema to read json
    with pytest.raises(ValueError):
        get_reader(session, mode).schema(user_schema).json(json_path)

    # user can input customized formatTypeOptions
    df2 = get_reader(session, mode).option("FILE_EXTENSION", "json").json(json_path)
    assert df2.collect() == [
        Row('{\n  "color": "Red",\n  "fruit": "Apple",\n  "size": "Large"\n}')
    ]


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_json_with_infer_schema(session, mode):
    json_path = f"@{tmp_stage_name1}/{test_file_json}"

    df1 = get_reader(session, mode).option("INFER_SCHEMA", True).json(json_path)
    res = df1.collect()
    assert res == [
        Row(color="Red", fruit="Apple", size="Large")
    ]

    # query_test
    res = df1.where(col('"color"') == lit("Red")).collect()
    assert res == [
        Row(color="Red", fruit="Apple", size="Large")
    ]

    # assert user cannot input a schema to read json
    with pytest.raises(ValueError):
        get_reader(session, mode).schema(user_schema).json(json_path)

    # user can input customized formatTypeOptions
    df2 = get_reader(session, mode).option("INFER_SCHEMA", True).option("FILE_EXTENSION", "json").json(json_path)
    assert df2.collect() == [
        Row(color="Red", fruit="Apple", size="Large")
    ]


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
        StructField('"TS_NTZ"', TimestampType(), nullable=True),
        StructField('"TS"', TimestampType(), nullable=True),
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
        StructField('"TS_NTZ"', TimestampType(), nullable=True),
        StructField('"TS"', TimestampType(), nullable=True),
        StructField('"V"', StringType(), nullable=True),
    ]


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


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_orc_with_no_schema(session, mode):
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

    # user can input customized formatTypeOptions
    df2 = get_reader(session, mode).option("TRIM_SPACE", False).orc(path)
    res = df2.collect()
    assert res == [
        Row(str="str1", num=1),
        Row(str="str2", num=2),
    ]


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_xml_with_no_schema(session, mode):
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

    # user can input customized formatTypeOptions
    df2 = get_reader(session, mode).option("COMPRESSION", "NONE").xml(path)
    res = df2.collect()
    assert res == [
        Row("<test>\n  <num>1</num>\n  <str>str1</str>\n</test>"),
        Row("<test>\n  <num>2</num>\n  <str>str2</str>\n</test>"),
    ]


def test_copy(session):
    test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"

    df = (
        session.read.schema(user_schema)
        .option("on_error", "continue")
        .option("COMPRESSION", "none")
        .csv(test_file_on_stage)
    )

    # use copy
    assert len(df._plan.queries) == 3
    assert df._plan.queries[1].sql.find("COPY") >= 0
    assert len(df._plan.post_actions) == 1

    df1 = (
        session.read.schema(user_schema)
        .option("COMPRESSION", "none")
        .csv(test_file_on_stage)
    )

    # no copy
    assert len(df1._plan.queries) == 2

    res = df.collect()
    res.sort(key=lambda x: x[0])
    assert res == [Row(1, "one", 1.2), Row(2, "two", 2.2)]

    # fail to read since the file is not compressed.
    # return empty result since enable on_error = continue

    df2 = (
        session.read.schema(user_schema)
        .option("on_error", "continue")
        .option("COMPRESSION", "gzip")
        .csv(test_file_on_stage)
    )
    assert df2.collect() == []


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
