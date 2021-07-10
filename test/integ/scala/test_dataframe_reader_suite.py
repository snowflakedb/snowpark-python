import pytest
import random
from snowflake.connector import ProgrammingError

from snowflake.snowpark.functions import col, sql_expr
from snowflake.snowpark.row import Row
from snowflake.snowpark.session import Session
from snowflake.snowpark.snowpark_client_exception import SnowparkClientException
from snowflake.snowpark.types.sf_types import StructType, StructField, IntegerType, \
    StringType, DoubleType
from test.utils import Utils, SnowTestFiles

testFileCsv = "testCSV.csv"
testFile2Csv = "test2CSV.csv"
testFileCsvColon = "testCSVcolon.csv"
testFileCsvQuotes = "testCSVquotes.csv"
testFileJson = "testJson.json"
testFileAvro = "test.avro"
testFileParquet = "test.parquet"
testFileOrc = "test.orc"
testFileXml = "test.xml"
testBrokenCsv = "broken.csv"


# In the tests below, we test both scenarios: SELECT & COPY
def get_reader(session, mode):
    if mode == "select":
        reader = session.read()
    elif mode == "copy":
        reader = session.read().option("PURGE", False)
    else:
        raise Exception("incorrect input for mode")
    return reader


user_schema = StructType([
    StructField("a", IntegerType()),
    StructField("b", StringType()),
    StructField("c", DoubleType())])


def before_all(session: Session, resources_path: str, tmp_stage_name1= None, tmp_stage_name2 = None):
    test_files = SnowTestFiles(resources_path)

    # create temporary stage to store the file
    if tmp_stage_name1:
        # TODO edit: CREATE TEMPORARY STAGE
        session.sql(f"CREATE STAGE {tmp_stage_name1}").collect()
        Utils.upload_to_stage(session, "@"+tmp_stage_name1, test_files.testFileCsv, compress=False)
        Utils.upload_to_stage(session, "@"+tmp_stage_name1, test_files.testFile2Csv, compress=False)
        Utils.upload_to_stage(session, "@"+tmp_stage_name1, test_files.testFileCsvColon, compress=False)
        Utils.upload_to_stage(session, "@"+tmp_stage_name1, test_files.testFileCsvQuotes, compress=False)
        Utils.upload_to_stage(session, "@"+tmp_stage_name1, test_files.testFileJson, compress=False)

    if tmp_stage_name2:
        session.sql(f"CREATE TEMPORARY STAGE {tmp_stage_name2}").collect()
        Utils.upload_to_stage(session, "@"+tmp_stage_name2, test_files.testFileCsv, compress=False)


def after_all(session: Session, tmp_stage_name1 = None, tmp_stage_name2 = None):
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

            test_file_on_stage = f"@{tmp_stage_name}/{testFileCsv}"
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
            incorrect_schema = StructType([
                StructField("a", IntegerType()),
                StructField("b", IntegerType()),
                StructField("c", IntegerType())])
            df2 = reader.schema(incorrect_schema).csv(test_file_on_stage)
            with pytest.raises(ProgrammingError):
                df2.collect()

        finally:
            after_all(session, tmp_stage_name)


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_csv_incorrect_schema(session_cnx, db_parameters, resources_path, mode):
    with session_cnx(db_parameters) as session:
        reader = get_reader(session, mode)
        tmp_stage_name = Utils.random_stage_name()
        try:
            before_all(session, resources_path, tmp_stage_name)

            test_file_on_stage = f"@{tmp_stage_name}/{testFileCsv}"
            incorrect_schema = StructType([
                StructField("a", IntegerType()),
                StructField("b", StringType()),
                StructField("c", IntegerType()),
                StructField("d", IntegerType())])
            df = reader.option("purge", False).schema(incorrect_schema).csv(test_file_on_stage)
            with pytest.raises(ProgrammingError):
                df.collect()

        finally:
            after_all(session, tmp_stage_name)


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_csv_with_more_operations(session_cnx, db_parameters, resources_path, mode):
    with session_cnx(db_parameters) as session:
        reader = get_reader(session, mode)
        tmp_stage_name = Utils.random_stage_name()
        tmp_stage_name2 = Utils.random_stage_name()

        try:
            before_all(session, resources_path, tmp_stage_name, tmp_stage_name2)

            test_file_on_stage = f"@{tmp_stage_name}/{testFileCsv}"
            df1 = reader.schema(user_schema).csv(test_file_on_stage).filter(col("a")<2)
            res = df1.collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1, "one", 1.2])]

            # test for self union
            df = reader.schema(user_schema).csv(test_file_on_stage)
            df2 = df.union(df)
            res = df2.collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1, "one", 1.2]), Row([1, "one", 1.2]), Row([2, "two", 2.2]), Row([2, "two", 2.2])]

            # test for union between two stages
            test_file_on_stage2 = f"@{tmp_stage_name2}/{testFileCsv}"
            df3 = reader.schema(user_schema).csv(test_file_on_stage2)
            df4 = df.union(df3)
            res = df4.collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1, "one", 1.2]), Row([1, "one", 1.2]), Row([2, "two", 2.2]), Row([2, "two", 2.2])]

        finally:
            after_all(session, tmp_stage_name)


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_csv_with_format_type_options(session_cnx, db_parameters, resources_path, mode):
    with session_cnx(db_parameters) as session:
        tmp_stage_name = Utils.random_stage_name()
        tmp_stage_name2 = Utils.random_stage_name()

        try:
            before_all(session, resources_path, tmp_stage_name, tmp_stage_name2)

            test_file_colon = f"@{tmp_stage_name}/{testFileCsvColon}"
            options = {"field_delimiter": "';'", "skip_blank_lines":True, "skip_header": 1}
            df1 = get_reader(session, mode).schema(user_schema).options(options).csv(test_file_colon)
            res = df1.collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1, "one", 1.2]), Row([2, "two", 2.2])]

            # test when user does not input a right option:
            df2 = get_reader(session, mode).schema(user_schema).csv(testFileCsvColon)
            with pytest.raises(ProgrammingError):
                df2.collect()

            # test for multiple formatTypeOptions
            df3 = get_reader(session, mode).schema(user_schema).option("field_delimiter", ";")\
                            .option("ENCODING", "wrongEncoding")\
                            .option("ENCODING", "UTF8")\
                            .option("COMPRESSION", "NONE")\
                            .option("skip_header", 1)\
                            .csv(test_file_colon)
            res = df3.collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1, "one", 1.2]), Row([2, "two", 2.2])]

            # test for union between files with different schema and different stage
            test_file_on_stage2 = f"@{tmp_stage_name2}/{testFileCsv}"
            df4 = get_reader(session, mode).schema(user_schema).csv(test_file_on_stage2)
            df5 = df1.union(df4)
            res = df5.collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1, "one", 1.2]), Row([1, "one", 1.2]), Row([2, "two", 2.2]), Row([2, "two", 2.2])]

        finally:
            after_all(session, tmp_stage_name, tmp_stage_name2)


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_to_read_files_from_stage(session_cnx, db_parameters, resources_path, mode):
    with session_cnx(db_parameters) as session:
        data_files_stage = Utils.random_stage_name()
        session.sql(f"CREATE TEMPORARY STAGE {data_files_stage}").collect()
        test_files = SnowTestFiles(resources_path)
        Utils.upload_to_stage(session, "@"+data_files_stage, test_files.testFileCsv, False)
        Utils.upload_to_stage(session, "@"+data_files_stage, test_files.testFile2Csv, False)

        reader = get_reader(session, mode)

        try:
            df = reader.schema(user_schema).option("compression", "auto").csv(f"@{data_files_stage}/")
            res = df.collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1, "one", 1.2]), Row([2, "two", 2.2]), Row([3, "three", 3.3]), Row([4, "four", 4.4])]
        finally:
            session.sql(f"DROP STAGE IF EXISTS {data_files_stage}")


@pytest.mark.skip("Needs DataFrameWriter.")
@pytest.mark.parametrize("mode", ["select", "copy"])
def test_for_all_csv_compression_keywords(session_cnx, db_parameters, resources_path, mode):
    with session_cnx(db_parameters) as session:

        tmp_table = Utils.get_fully_qualified_temp_schema(session) + "." + Utils.random_name()
        tmp_stage_name = Utils.random_stage_name()
        test_file_on_stage = f"@{tmp_stage_name}/{testFileCsv}"
        format_name = Utils.random_name()
        try:
            get_reader(session, mode).schema(user_schema).option("compression", "auto")\
                .csv(test_file_on_stage).write.saveAsTable(tmp_table)

            session.sql(f"create file format {format_name} type = 'csv'").collect()

            for ctype in ["gzip", "bz2", "brotli", "zstd", "deflate", "raw_deflate"]:
                path = f"@{tmp_stage_name}/{ctype}/{abs(random.randint(0, 2 ** 31))}/"
                # upload data
                session.sql(f"copy into {path} from ( select * from {tmp_table}) file_format=(format_name='{format_name}' compression='{ctype}')").collect()

                # read the data
                df = get_reader(session, mode).option("COMPRESSION", ctype).schema(user_schema).csv(path)
                res = df.collect()
                res.sort(key=lambda x: x[0])
                assert res == [Row([1, "one", 1.2]), Row([2, "two", 2.2])]
        finally:
            session.sql(f"drop file format {format_name}")
            after_all(session, tmp_stage_name)


@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_csv_with_special_chars_in_formatTypeOptions(session_cnx, db_parameters, resources_path, mode):

    schema1 = StructType([
        StructField("a", IntegerType()),
        StructField("b", StringType()),
        StructField("c", DoubleType()),
        StructField("d", IntegerType())])
    tmp_stage_name = Utils.random_stage_name()
    test_file = f"@{tmp_stage_name}/{testFileCsvQuotes}"

    with session_cnx(db_parameters) as session:
        try:
            before_all(session, resources_path, tmp_stage_name)
            reader = get_reader(session, mode)
            df1 = reader.schema(schema1).option("field_optionally_enclosed_by", "\"").csv(test_file)
            res = df1.collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row([1, "one", 1.2, 1]), Row([2, "two", 2.2, 2])]

            # without the setting it should fail schema validation
            df2 = get_reader(session, mode).schema(schema1).csv(test_file)
            with pytest.raises(ProgrammingError) as ex_info:
                df2.collect()
            assert f"Numeric value '\"1\"' is not recognized" in str(ex_info)

            schema2 = StructType([
                StructField("a", IntegerType()),
                StructField("b", StringType()),
                StructField("c", DoubleType()),
                StructField("d", StringType())])
            df3 = get_reader(session, mode).schema(schema2).csv(test_file)
            res = df3.select("d").collect()
            res.sort(key=lambda x: x[0])
            assert res == [Row("\"1\""), Row("\"2\"")]

        finally:
            after_all(session, tmp_stage_name)

@pytest.mark.parametrize("mode", ["select", "copy"])
def test_read_json_with_no_schema(session_cnx, db_parameters, resources_path, mode):
    tmp_stage_name = Utils.random_stage_name()
    json_path = f"@{tmp_stage_name}/{testFileJson}"

    with session_cnx(db_parameters) as session:
        try:
            before_all(session, resources_path, tmp_stage_name)
            df1 = get_reader(session, mode).json(json_path)

            res = df1.collect()
            assert res == [Row("{\n  \"color\": \"Red\",\n  \"fruit\": \"Apple\",\n  \"size\": \"Large\"\n}")]

            # query_test
            res = df1.where(sql_expr("$1:color") == "Red").collect()
            assert res == [Row("{\n  \"color\": \"Red\",\n  \"fruit\": \"Apple\",\n  \"size\": \"Large\"\n}")]

            # assert user cannot input a schema to read json
            with pytest.raises(ValueError):
                get_reader(session, mode).schema(user_schema).json(json_path)

            # user can input customized formatTypeOptions
            df2 = get_reader(session, mode).option("FILE_EXTENSION", "json").json(json_path)
            assert df2.collect() == [Row("{\n  \"color\": \"Red\",\n  \"fruit\": \"Apple\",\n  \"size\": \"Large\"\n}")]

        finally:
            after_all(session, tmp_stage_name)