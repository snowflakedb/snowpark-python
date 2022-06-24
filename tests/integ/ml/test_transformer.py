#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#


import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.ml import (
    MinMaxScaler,
    OneHotEncoder,
    OrdinalEncoder,
    StandardScaler,
)
from snowflake.snowpark.types import IntegerType, StructField, StructType
from tests.utils import TestFiles, Utils

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
        StructField("b", IntegerType()),
        StructField("c", IntegerType()),
    ]
)


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


def test_standardscaler(session):
    df = session.createDataFrame(
        [[1, 2, 3], [2, 3, 4], [4, 5, 6]], schema=["a", "b", "c"]
    )
    standarscaler = StandardScaler()
    standarscaler.input_cols = ["A"]
    standarscaler.output_cols = ["res"]
    model = standarscaler.fit(df)
    res = model.transform(df)
    Utils.check_answer(
        res,
        [Row(-1.069044547667817), Row(-0.26726093647105154), Row(1.3363062859224795)],
    )


def test_minmaxscaler(session):
    df = session.createDataFrame(
        [[1, 2, 3], [2, 3, 4], [4, 5, 6]], schema=["a", "b", "c"]
    )
    minmaxscaler = MinMaxScaler()
    minmaxscaler.input_cols = ["A"]
    minmaxscaler.output_cols = ["res"]
    model = minmaxscaler.fit(df)
    res = model.transform(df)
    Utils.check_answer(
        res,
        [Row(0.0), Row(0.3333333333333333), Row(1.0)],
    )


def test_merge_onehotencoder(session):
    df = session.createDataFrame(
        [[1, 4, 5], [2, 4, 5], [2, 3, 6]], schema=["a", "b", "c"]
    )
    onehotencoder = OneHotEncoder()
    onehotencoder.input_cols = ["A", "B", "C"]
    model = onehotencoder.fit(df)
    res = model.transform(df)
    Utils.check_answer(
        res,
        [
            Row(
                A=1,
                B=4,
                C=5,
                ONEHOTARRAY="[\n  6,\n  [\n    0,\n    2,\n    4\n  ],\n  1\n]",
            ),
            Row(
                A=2,
                B=4,
                C=5,
                ONEHOTARRAY="[\n  6,\n  [\n    1,\n    2,\n    4\n  ],\n  1\n]",
            ),
            Row(
                A=2,
                B=3,
                C=6,
                ONEHOTARRAY="[\n  6,\n  [\n    1,\n    3,\n    5\n  ],\n  1\n]",
            ),
        ],
    )


def test_no_merge_onehotencoder(session):
    df = session.createDataFrame(
        [[1, 4, 5], [2, 4, 5], [2, 3, 6]], schema=["a", "b", "c"]
    )
    onehotencoder = OneHotEncoder()
    onehotencoder.input_cols = ["A", "B", "C"]
    onehotencoder.output_cols = ["A", "B", "C"]
    model = onehotencoder.fit(df)
    res = model.transform(df)
    Utils.check_answer(
        res,
        [
            Row(
                A=1,
                B=4,
                C=5,
                A_ONEHOT="[\n  2,\n  0,\n  1\n]",
                B_ONEHOT="[\n  2,\n  0,\n  1\n]",
                C_ONEHOT="[\n  2,\n  0,\n  1\n]",
            ),
            Row(
                A=2,
                B=4,
                C=5,
                A_ONEHOT="[\n  2,\n  1,\n  1\n]",
                B_ONEHOT="[\n  2,\n  0,\n  1\n]",
                C_ONEHOT="[\n  2,\n  0,\n  1\n]",
            ),
            Row(
                A=2,
                B=3,
                C=6,
                A_ONEHOT="[\n  2,\n  1,\n  1\n]",
                B_ONEHOT="[\n  2,\n  1,\n  1\n]",
                C_ONEHOT="[\n  2,\n  1,\n  1\n]",
            ),
        ],
    )


def test_preset_category_onehotencoder(session):
    df = session.createDataFrame(
        [[1, 4, 5], [2, 4, 5], [2, 3, 6]], schema=["a", "b", "c"]
    )
    onehotencoder = OneHotEncoder(category=[["1", "2"], ["4", "3"], ["5", "6"]])
    onehotencoder.input_cols = ["A", "B", "C"]
    model = onehotencoder.fit(df)
    res = model.transform(df)
    res.show()
    Utils.check_answer(
        res,
        [
            Row(
                A=1,
                B=4,
                C=5,
                ONEHOTARRAY="[\n  6,\n  [\n    0,\n    2,\n    4\n  ],\n  1\n]",
            ),
            Row(
                A=2,
                B=4,
                C=5,
                ONEHOTARRAY="[\n  6,\n  [\n    1,\n    2,\n    4\n  ],\n  1\n]",
            ),
            Row(
                A=2,
                B=3,
                C=6,
                ONEHOTARRAY="[\n  6,\n  [\n    1,\n    3,\n    5\n  ],\n  1\n]",
            ),
        ],
    )


def test_merge_ordinalcoder(session):
    df = session.createDataFrame(
        [[1, 4, 5], [2, 4, 5], [2, 3, 6]], schema=["a", "b", "c"]
    )
    ordinalcoder = OrdinalEncoder()
    ordinalcoder.input_cols = ["A", "B", "C"]
    model = ordinalcoder.fit(df)
    res = model.transform(df)
    Utils.check_answer(
        res,
        [
            Row(A=1, B=4, C=5, ORDINALARRAY="[\n  0,\n  0,\n  0\n]"),
            Row(A=2, B=3, C=6, ORDINALARRAY="[\n  1,\n  1,\n  1\n]"),
            Row(A=2, B=4, C=5, ORDINALARRAY="[\n  1,\n  0,\n  0\n]"),
        ],
    )
