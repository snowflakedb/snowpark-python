import pytest

from snowflake.snowpark.row import Row
from snowflake.snowpark.types.sf_types import StructType, StructField, IntegerType, \
    StringType, DoubleType
from utils import Utils, TestFiles

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

tmp_stage_name1 = Utils.random_stage_name()
tmp_stage_name2 = Utils.random_stage_name()

user_schema = StructType(
    [
        StructField("a", IntegerType()),
        StructField("b", StringType()),
        StructField("c", DoubleType()),
    ]
)


@pytest.fixture(scope="module")
def before_all(session_cnx, resources_path):
    def do():
        test_files = TestFiles(resources_path)
        with session_cnx() as session:
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

    return do


@pytest.fixture(scope="module")
def after_all(session_cnx):
    def do():
        with session_cnx() as session:
            session.sql(f"DROP STAGE IF EXISTS {tmp_stage_name1}").collect()
            session.sql(f"DROP STAGE IF EXISTS {tmp_stage_name2}").collect()

    return do


def test_copy_csv_test_basic(session_cnx):
    with session_cnx() as session:
        test_file_on_stage = f"@{tmp_stage_name1}/{test_file_csv}"

        # create target table
        test_table_name = Utils.random_name()
        Utils.create_table(session, test_table_name, "a Int, b String, c Double")

        assert session.table(test_table_name).count() == 0

        # When the feature is ready, csv() will return CopyableDataFrame.
        df = session.read.schema(user_schema).csv(test_file_on_stage)

        df.copyInto(test_table_name)
        Utils.check_answer(df.session.table(test_table_name), [Row([1, "one", 1.2]), Row([2, "two", 2.2])])

        # run COPY again, the loaded files will be skipped by default
        df.copyInto(test_table_name)
        Utils.check_answer(df.session.table(test_table_name), [Row([1, "one", 1.2]), Row([2, "two", 2.2])])

        # Copy again with FORCE = TRUE, loaded file are NOT skipped.
        session.read.schema(user_schema).option("FORCE", True).csv(test_file_on_stage).copyInto(test_table_name)
        Utils.check_answer(df.session.table(test_table_name), [Row([1, "one", 1.2]), Row([2, "two", 2.2]), Row([1, "one", 1.2]), Row([2, "two", 2.2])])
