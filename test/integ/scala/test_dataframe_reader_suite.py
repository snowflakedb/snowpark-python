#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

import random
from test.utils import TestFiles, Utils

import pytest

from snowflake.connector import ProgrammingError
from snowflake.snowpark.functions import col, sql_expr
from snowflake.snowpark.row import Row
from snowflake.snowpark.session import Session
from snowflake.snowpark.snowpark_client_exception import SnowparkClientException
from snowflake.snowpark.types.sf_types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

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


# In the tests below, we test both scenarios: SELECT & COPY
def get_reader(session, mode):
    if mode == "select":
        reader = session.read()
    elif mode == "copy":
        reader = session.read().option("PURGE", False)
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


def before_all(
    session: Session, resources_path: str, tmp_stage_name1=None, tmp_stage_name2=None
):
    test_files = TestFiles(resources_path)

    # create temporary stage to store the file
    if tmp_stage_name1:
        # TODO edit: CREATE TEMPORARY STAGE
        session.sql(f"CREATE STAGE {tmp_stage_name1}").collect()
        Utils.upload_to_stage(
            session, "@" + tmp_stage_name1, test_files.test_file_csv, compress=False
        )
        Utils.upload_to_stage(
            session, "@" + tmp_stage_name1, test_files.test_file2_csv, compress=False
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
            session, "@" + tmp_stage_name1, test_files.test_file_json, compress=False
        )
        Utils.upload_to_stage(
            session, "@" + tmp_stage_name1, test_files.test_file_avro, compress=False
        )
        Utils.upload_to_stage(
            session, "@" + tmp_stage_name1, test_files.test_file_parquet, compress=False
        )
        Utils.upload_to_stage(
            session, "@" + tmp_stage_name1, test_files.test_file_orc, compress=False
        )
        Utils.upload_to_stage(
            session, "@" + tmp_stage_name1, test_files.test_file_xml, compress=False
        )
        Utils.upload_to_stage(
            session, "@" + tmp_stage_name1, test_files.test_broken_csv, compress=False
        )

    if tmp_stage_name2:
        session.sql(f"CREATE TEMPORARY STAGE {tmp_stage_name2}").collect()
        Utils.upload_to_stage(
            session, "@" + tmp_stage_name2, test_files.test_file_csv, compress=False
        )


def after_all(session: Session, tmp_stage_name1=None, tmp_stage_name2=None):
    if tmp_stage_name1:
        session.sql(f"DROP STAGE IF EXISTS {tmp_stage_name1}").collect()

    if tmp_stage_name2:
        session.sql(f"DROP STAGE IF EXISTS {tmp_stage_name2}").collect()


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_csv(session_cnx, db_parameters, resources_path, mode):
    with session_cnx(db_parameters) as session:
        reader = get_reader(session, mode)
        tmp_stage_name = Utils.random_stage_name()

        try:
            before_all(session, resources_path, tmp_stage_name)

            test_file_on_stage = f"@{tmp_stage_name}/{test_file_csv}"
            df1 = reader.schema(user_schema).csv(test_file_on_stage)
            res = df1.collect()
            res.sort(key=lambda x: x[0])

            assert len(res) == 2
            assert len(res[0]) == 3
            assert res == [Row([1, "one", 1.2]), Row([2, "two", 2.2])]

            with pytest.raises(SnowparkClientException):
                session.read().csv(test_file_on_stage)

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
            with pytest.raises(ProgrammingError) as ex_info:
                df2.collect()
            assert "Numeric value 'one' is not recognized" in str(ex_info)

        finally:
            after_all(session, tmp_stage_name)


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_csv_incorrect_schema(session_cnx, db_parameters, resources_path, mode):
    with session_cnx(db_parameters) as session:
        reader = get_reader(session, mode)
        tmp_stage_name = Utils.random_stage_name()
        try:
            before_all(session, resources_path, tmp_stage_name)

            test_file_on_stage = f"@{tmp_stage_name}/{test_file_csv}"
            incorrect_schema = StructType(
                [
                    StructField("a", IntegerType()),
                    StructField("b", StringType()),
                    StructField("c", IntegerType()),
                    StructField("d", IntegerType()),
                ]
            )
            df = (
                reader.option("purge", False)
                .schema(incorrect_schema)
                .csv(test_file_on_stage)
            )
            with pytest.raises(ProgrammingError) as ex_info:
                df.collect()
            assert "Number of columns in file (3) does not match" in str(ex_info)

        finally:
            after_all(session, tmp_stage_name)


def test_read_csv_with_more_operations(session_cnx, db_parameters, resources_path):
    with session_cnx(db_parameters) as session:
        tmp_stage_name = Utils.random_stage_name()
        tmp_stage_name2 = Utils.random_stage_name()

        try:
            before_all(session, resources_path, tmp_stage_name, tmp_stage_name2)

            test_file_on_stage = f"@{tmp_stage_name}/{test_file_csv}"
            df1 = (
                session.read()
                .schema(user_schema)
                .csv(test_file_on_stage)
                .filter(col("a") < 2)
            )
            res = df1.collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1, "one", 1.2])]

            # test for self union
            df = session.read().schema(user_schema).csv(test_file_on_stage)
            df2 = df.union(df)
            res = df2.collect()
            res.sort(key=lambda x: x[0])
            assert res == [
                Row([1, "one", 1.2]),
                Row([1, "one", 1.2]),
                Row([2, "two", 2.2]),
                Row([2, "two", 2.2]),
            ]

            # test for union between two stages
            test_file_on_stage2 = f"@{tmp_stage_name2}/{test_file_csv}"
            df3 = session.read().schema(user_schema).csv(test_file_on_stage2)
            df4 = df.union(df3)
            res = df4.collect()
            res.sort(key=lambda x: x[0])
            assert res == [
                Row([1, "one", 1.2]),
                Row([1, "one", 1.2]),
                Row([2, "two", 2.2]),
                Row([2, "two", 2.2]),
            ]

        finally:
            after_all(session, tmp_stage_name)


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_csv_with_format_type_options(
    session_cnx, db_parameters, resources_path, mode
):
    with session_cnx(db_parameters) as session:
        tmp_stage_name = Utils.random_stage_name()
        tmp_stage_name2 = Utils.random_stage_name()

        try:
            before_all(session, resources_path, tmp_stage_name, tmp_stage_name2)

            test_file_colon = f"@{tmp_stage_name}/{test_file_csv_colon}"
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
            assert res == [Row([1, "one", 1.2]), Row([2, "two", 2.2])]

            # test when user does not input a right option:
            df2 = get_reader(session, mode).schema(user_schema).csv(test_file_csv_colon)
            with pytest.raises(ProgrammingError) as ex_info:
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
            assert res == [Row([1, "one", 1.2]), Row([2, "two", 2.2])]

            # test for union between files with different schema and different stage
            test_file_on_stage2 = f"@{tmp_stage_name2}/{test_file_csv}"
            df4 = get_reader(session, mode).schema(user_schema).csv(test_file_on_stage2)
            df5 = df1.union(df4)
            res = df5.collect()
            res.sort(key=lambda x: x[0])
            assert res == [
                Row([1, "one", 1.2]),
                Row([1, "one", 1.2]),
                Row([2, "two", 2.2]),
                Row([2, "two", 2.2]),
            ]

        finally:
            after_all(session, tmp_stage_name, tmp_stage_name2)


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_to_read_files_from_stage(session_cnx, db_parameters, resources_path, mode):
    with session_cnx(db_parameters) as session:
        data_files_stage = Utils.random_stage_name()
        session.sql(f"CREATE TEMPORARY STAGE {data_files_stage}").collect()
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
                Row([1, "one", 1.2]),
                Row([2, "two", 2.2]),
                Row([3, "three", 3.3]),
                Row([4, "four", 4.4]),
            ]
        finally:
            session.sql(f"DROP STAGE IF EXISTS {data_files_stage}")


@pytest.mark.skip("Needs DataFrameWriter.")
@pytest.mark.parametrize("mode", ["select", "copy"])
def test_for_all_csv_compression_keywords(
    session_cnx, db_parameters, resources_path, mode
):
    with session_cnx(db_parameters) as session:

        tmp_table = (
            Utils.get_fully_qualified_temp_schema(session) + "." + Utils.random_name()
        )
        tmp_stage_name = Utils.random_stage_name()
        test_file_on_stage = f"@{tmp_stage_name}/{test_file_csv}"
        format_name = Utils.random_name()
        try:
            get_reader(session, mode).schema(user_schema).option(
                "compression", "auto"
            ).csv(test_file_on_stage).write.saveAsTable(tmp_table)

            session.sql(f"create file format {format_name} type = 'csv'").collect()

            for ctype in ["gzip", "bz2", "brotli", "zstd", "deflate", "raw_deflate"]:
                path = f"@{tmp_stage_name}/{ctype}/{abs(random.randint(0, 2 ** 31))}/"
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
                assert res == [Row([1, "one", 1.2]), Row([2, "two", 2.2])]
        finally:
            session.sql(f"drop file format {format_name}")
            after_all(session, tmp_stage_name)


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_csv_with_special_chars_in_formatTypeOptions(
    session_cnx, db_parameters, resources_path, mode
):

    schema1 = StructType(
        [
            StructField("a", IntegerType()),
            StructField("b", StringType()),
            StructField("c", DoubleType()),
            StructField("d", IntegerType()),
        ]
    )
    tmp_stage_name = Utils.random_stage_name()
    test_file = f"@{tmp_stage_name}/{test_file_csv_quotes}"

    with session_cnx(db_parameters) as session:
        try:
            before_all(session, resources_path, tmp_stage_name)
            reader = get_reader(session, mode)
            df1 = (
                reader.schema(schema1)
                .option("field_optionally_enclosed_by", '"')
                .csv(test_file)
            )
            res = df1.collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1, "one", 1.2, 1]), Row([2, "two", 2.2, 2])]

            # without the setting it should fail schema validation
            df2 = get_reader(session, mode).schema(schema1).csv(test_file)
            with pytest.raises(ProgrammingError) as ex_info:
                df2.collect()
            assert f"Numeric value '\"1\"' is not recognized" in str(ex_info)

            schema2 = StructType(
                [
                    StructField("a", IntegerType()),
                    StructField("b", StringType()),
                    StructField("c", DoubleType()),
                    StructField("d", StringType()),
                ]
            )
            df3 = get_reader(session, mode).schema(schema2).csv(test_file)
            res = df3.select("d").collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row('"1"'), Row('"2"')]

        finally:
            after_all(session, tmp_stage_name)


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_json_with_no_schema(session_cnx, db_parameters, resources_path, mode):
    tmp_stage_name = Utils.random_stage_name()
    json_path = f"@{tmp_stage_name}/{test_file_json}"

    with session_cnx(db_parameters) as session:
        try:
            before_all(session, resources_path, tmp_stage_name)
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
            df2 = (
                get_reader(session, mode)
                .option("FILE_EXTENSION", "json")
                .json(json_path)
            )
            assert df2.collect() == [
                Row('{\n  "color": "Red",\n  "fruit": "Apple",\n  "size": "Large"\n}')
            ]

        finally:
            after_all(session, tmp_stage_name)


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_avro_with_no_schema(session_cnx, db_parameters, resources_path, mode):
    tmp_stage_name = Utils.random_stage_name()
    avro_path = f"@{tmp_stage_name}/{test_file_avro}"

    with session_cnx(db_parameters) as session:
        try:
            before_all(session, resources_path, tmp_stage_name)
            df1 = get_reader(session, mode).avro(avro_path)
            res = df1.collect()
            assert res == [
                Row('{\n  "num": 1,\n  "str": "str1"\n}'),
                Row('{\n  "num": 2,\n  "str": "str2"\n}'),
            ]

            # query_test
            res = df1.where(sql_expr("$1:num") > 1).collect()
            assert res == [Row('{\n  "num": 2,\n  "str": "str2"\n}')]

            # assert user cannot input a schema to read json
            with pytest.raises(ValueError):
                get_reader(session, mode).schema(user_schema).avro(avro_path)

            # user can input customized formatTypeOptions
            df2 = (
                get_reader(session, mode).option("COMPRESSION", "NONE").avro(avro_path)
            )
            res = df2.collect()
            assert res == [
                Row('{\n  "num": 1,\n  "str": "str1"\n}'),
                Row('{\n  "num": 2,\n  "str": "str2"\n}'),
            ]
        finally:
            after_all(session, tmp_stage_name)


@pytest.mark.skip("Needs DataFrameWriter.")
@pytest.mark.parametrize("mode", ["select", "copy"])
def test_for_all_csv_compression_keywords(
    session_cnx, db_parameters, resources_path, mode
):
    # TODO
    pass


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_parquet_with_no_schema(session_cnx, db_parameters, resources_path, mode):
    tmp_stage_name = Utils.random_stage_name()
    path = f"@{tmp_stage_name}/{test_file_parquet}"

    with session_cnx(db_parameters) as session:
        try:
            before_all(session, resources_path, tmp_stage_name)
            df1 = get_reader(session, mode).parquet(path)
            res = df1.collect()
            assert res == [
                Row('{\n  "num": 1,\n  "str": "str1"\n}'),
                Row('{\n  "num": 2,\n  "str": "str2"\n}'),
            ]

            # query_test
            res = df1.where(sql_expr("$1:num") > 1).collect()
            assert res == [Row('{\n  "num": 2,\n  "str": "str2"\n}')]

            # assert user cannot input a schema to read json
            with pytest.raises(ValueError):
                get_reader(session, mode).schema(user_schema).parquet(path)

            # user can input customized formatTypeOptions
            df2 = get_reader(session, mode).option("COMPRESSION", "NONE").parquet(path)
            res = df2.collect()
            assert res == [
                Row('{\n  "num": 1,\n  "str": "str1"\n}'),
                Row('{\n  "num": 2,\n  "str": "str2"\n}'),
            ]
        finally:
            after_all(session, tmp_stage_name)


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_orc_with_no_schema(session_cnx, db_parameters, resources_path, mode):
    tmp_stage_name = Utils.random_stage_name()
    path = f"@{tmp_stage_name}/{test_file_orc}"

    with session_cnx(db_parameters) as session:
        try:
            before_all(session, resources_path, tmp_stage_name)
            df1 = get_reader(session, mode).orc(path)
            res = df1.collect()
            assert res == [
                Row('{\n  "num": 1,\n  "str": "str1"\n}'),
                Row('{\n  "num": 2,\n  "str": "str2"\n}'),
            ]

            # query_test
            res = df1.where(sql_expr("$1:num") > 1).collect()
            assert res == [Row('{\n  "num": 2,\n  "str": "str2"\n}')]

            # assert user cannot input a schema to read json
            with pytest.raises(ValueError):
                get_reader(session, mode).schema(user_schema).orc(path)

            # user can input customized formatTypeOptions
            df2 = get_reader(session, mode).option("TRIM_SPACE", False).orc(path)
            res = df2.collect()
            assert res == [
                Row('{\n  "num": 1,\n  "str": "str1"\n}'),
                Row('{\n  "num": 2,\n  "str": "str2"\n}'),
            ]
        finally:
            after_all(session, tmp_stage_name)


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_xml_with_no_schema(session_cnx, db_parameters, resources_path, mode):
    tmp_stage_name = Utils.random_stage_name()
    path = f"@{tmp_stage_name}/{test_file_xml}"

    with session_cnx(db_parameters) as session:
        try:
            before_all(session, resources_path, tmp_stage_name)
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
        finally:
            after_all(session, tmp_stage_name)


def test_copy(session_cnx, db_parameters, resources_path):
    tmp_stage_name = Utils.random_stage_name()
    test_file_on_stage = f"@{tmp_stage_name}/{test_file_csv}"

    with session_cnx(db_parameters) as session:
        try:
            before_all(session, resources_path, tmp_stage_name)
            df = (
                session.read()
                .schema(user_schema)
                .option("on_error", "continue")
                .option("COMPRESSION", "none")
                .csv(test_file_on_stage)
            )

            # use copy
            assert len(df._DataFrame__plan.queries) == 3
            assert df._DataFrame__plan.queries[1].sql.find("COPY") >= 0
            assert len(df._DataFrame__plan.post_actions) == 1

            df1 = (
                session.read()
                .schema(user_schema)
                .option("COMPRESSION", "none")
                .csv(test_file_on_stage)
            )

            # no copy
            assert len(df1._DataFrame__plan.queries) == 2

            res = df.collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1, "one", 1.2]), Row([2, "two", 2.2])]

            # fail to read since the file is not compressed.
            # return empty result since enable on_error = continue

            df2 = (
                session.read()
                .schema(user_schema)
                .option("on_error", "continue")
                .option("COMPRESSION", "gzip")
                .csv(test_file_on_stage)
            )
            assert df2.collect() == []

        finally:
            after_all(session, tmp_stage_name)


def test_copy_option_force(session_cnx, db_parameters, resources_path):
    tmp_stage_name = Utils.random_stage_name()
    test_file_on_stage = f"@{tmp_stage_name}/{test_file_csv}"

    with session_cnx(db_parameters) as session:
        try:
            before_all(session, resources_path, tmp_stage_name)

            with pytest.raises(SnowparkClientException) as ex_info:
                session.read().schema(user_schema).option("force", "false").csv(
                    test_file_on_stage
                ).collect()

            assert "Copy option 'FORCE = false' is not supported." in str(ex_info)

            with pytest.raises(SnowparkClientException) as ex_info:
                session.read().schema(user_schema).option("FORCE", "FALSE").csv(
                    test_file_on_stage
                ).collect()

            assert "Copy option 'FORCE = FALSE' is not supported." in str(ex_info)

            with pytest.raises(SnowparkClientException) as ex_info:
                session.read().schema(user_schema).option("fORce", "faLsE").csv(
                    test_file_on_stage
                ).collect()

                assert "Copy option 'FORCE = faLsE' is not supported." in str(ex_info)

            # no error
            session.read().schema(user_schema).option("fORce", "true").csv(
                test_file_on_stage
            ).collect()

            session.read().schema(user_schema).option("fORce", "trUe").csv(
                test_file_on_stage
            ).collect()

        finally:
            after_all(session, tmp_stage_name)


def test_read_file_on_error_continue_on_csv(session_cnx, db_parameters, resources_path):
    tmp_stage_name = Utils.random_stage_name()
    broken_file = f"@{tmp_stage_name}/{test_broken_csv}"

    with session_cnx(db_parameters) as session:
        try:
            before_all(session, resources_path, tmp_stage_name)

            # skip (2, two, wrong)
            df = (
                session.read()
                .schema(user_schema)
                .option("on_error", "continue")
                .option("COMPRESSION", "none")
                .csv(broken_file)
            )
            res = df.collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1, "one", 1.1]), Row([3, "three", 3.3])]

        finally:
            after_all(session, tmp_stage_name)


def test_read_file_on_error_continue_on_avro(
    session_cnx, db_parameters, resources_path
):
    tmp_stage_name = Utils.random_stage_name()
    broken_file = f"@{tmp_stage_name}/{test_file_avro}"

    with session_cnx(db_parameters) as session:
        try:
            before_all(session, resources_path, tmp_stage_name)

            # skip all
            df = (
                session.read()
                .schema(user_schema)
                .option("on_error", "continue")
                .option("COMPRESSION", "none")
                .csv(broken_file)
            )
            res = df.collect()
            assert res == []

        finally:
            after_all(session, tmp_stage_name)


def test_select_and_copy_on_non_csv_format_have_same_result_schema(
    session_cnx, db_parameters, resources_path
):
    tmp_stage_name = Utils.random_stage_name()
    path = f"@{tmp_stage_name}/{test_file_parquet}"

    with session_cnx(db_parameters) as session:
        try:
            before_all(session, resources_path, tmp_stage_name)

            copy = (
                session.read()
                .option("purge", False)
                .option("COMPRESSION", "none")
                .parquet(path)
            )
            select = session.read().option("COMPRESSION", "none").parquet(path)

            copy_fields = copy.schema.fields
            select_fields = select.schema.fields
            assert len(copy_fields) > 0
            assert len(copy_fields) == len(select_fields)
            for c, f in zip(copy_fields, select_fields):
                assert c.datatype.type_name == f.datatype.type_name
                assert c.name == f.name
                assert c.nullable == f.nullable
                assert (
                    c.column_identifier.normalized_name
                    == f.column_identifier.normalized_name
                )
                assert (
                    c.column_identifier.quoted_name == f.column_identifier.quoted_name
                )

        finally:
            after_all(session, tmp_stage_name)


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_pattern(session_cnx, db_parameters, resources_path, mode):
    tmp_stage_name = Utils.random_stage_name()

    with session_cnx(db_parameters) as session:
        try:
            before_all(session, resources_path, tmp_stage_name)
            assert (
                get_reader(session, mode)
                .schema(user_schema)
                .option("COMPRESSION", "none")
                .option("pattern", ".*CSV[.]csv")
                .csv(f"@{tmp_stage_name}")
                .count()
                == 4
            )
        finally:
            after_all(session, tmp_stage_name)
