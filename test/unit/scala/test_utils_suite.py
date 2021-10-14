#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
import os
import zipfile
from test.utils import IS_WINDOWS, TestFiles

import pytest

from snowflake.snowpark._internal.utils import Utils

resources_path = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../../resources")
)
test_files = TestFiles(resources_path)


def test_utils_validate_object_name():
    valid_identifiers = [
        "a",
        "b.a",
        "c.b.a",
        "c..a",
        "table_name",
        "_azAz09$",
        "schema.table_name",
        "db.schema.table_name",
        "db..table_name",
        '"table name"',
        '"SN_TEST_OBJECT_1364386155.!| @,#$"',
        '"schema "." table name"',
        '"db"."schema"."table_name"',
        '"db".."table_name"',
        '"""db""".."""table_name"""',
        '"""name"""',
        '"n""am""e"',
        '"""na.me"""',
        '"n""a..m""e"',
        '"schema"""."n""a..m""e"',
        '"""db"."schema"""."n""a..m""e"',
    ]

    for identifier in valid_identifiers:
        Utils.validate_object_name(identifier)


def test_md5():
    assert (
        Utils.calculate_md5(test_files.test_file_avro)
        == "85bd7b9363853f1815254b1cbc608c22"
    )
    if IS_WINDOWS:
        assert (
            Utils.calculate_md5(test_files.test_udf_directory)
            == "051593215a8fa0445d81154e5fdfd89e"
        )
        assert Utils.calculate_md5(resources_path) == "bdb9d0f05314e528a2267393ce4c46fb"
    else:
        assert (
            Utils.calculate_md5(test_files.test_udf_directory)
            == "956d97863a5e5f11840339bd208549ef"
        )
        assert Utils.calculate_md5(resources_path) == "4a7e0e3ffda738600b344ac12c167aa4"


def test_normalize_stage_location():
    name1 = "stage"
    assert Utils.normalize_stage_location(name1 + "  ") == f"@{name1}"
    assert Utils.normalize_stage_location("@" + name1 + "  ") == f"@{name1}"
    name1 = '"DATABASE"."SCHEMA"."STAGE"'
    assert Utils.normalize_stage_location(name1 + "  ") == f"@{name1}"
    assert Utils.normalize_stage_location("@" + name1 + "  ") == f"@{name1}"


def test_get_udf_upload_prefix():
    assert Utils.get_udf_upload_prefix("name") == "name"
    assert Utils.get_udf_upload_prefix("abcABC_0123456789") == "abcABC_0123456789"
    assert "name_" in Utils.get_udf_upload_prefix('"name"')
    assert "table_" in Utils.get_udf_upload_prefix(" table")
    assert "table_" in Utils.get_udf_upload_prefix("table ")
    assert "schemaview_" in Utils.get_udf_upload_prefix("schema.view")
    assert "SCHEMAVIEW_" in Utils.get_udf_upload_prefix('"SCHEMA"."VIEW"')
    assert "dbschematable" in Utils.get_udf_upload_prefix("db.schema.table")
    assert "dbschematable" in Utils.get_udf_upload_prefix('"db"."schema"."table"')


def test_zip_file_or_directory_to_stream():
    def check_zip_files_and_close_stream(input_stream, expected_files):
        with zipfile.ZipFile(input_stream) as zf:
            assert zf.testzip() is None
            assert sorted(zf.namelist()) == sorted(expected_files)
        input_stream.close()

    stream = Utils.zip_file_or_directory_to_stream(test_files.test_udf_py_file)
    check_zip_files_and_close_stream(stream, ["test_udf_file.py"])

    stream = Utils.zip_file_or_directory_to_stream(
        test_files.test_udf_py_file,
        leading_path=test_files.test_udf_directory,
        add_init_py=True,
    )
    check_zip_files_and_close_stream(stream, ["test_udf_file.py"])

    stream = Utils.zip_file_or_directory_to_stream(
        test_files.test_udf_py_file,
        leading_path=os.path.dirname(test_files.test_udf_directory),
    )
    check_zip_files_and_close_stream(stream, ["test_udf_dir/test_udf_file.py"])

    stream = Utils.zip_file_or_directory_to_stream(
        test_files.test_udf_py_file,
        leading_path=os.path.dirname(test_files.test_udf_directory),
        add_init_py=True,
    )
    check_zip_files_and_close_stream(
        stream, ["test_udf_dir/test_udf_file.py", "test_udf_dir/__init__.py"]
    )

    stream = Utils.zip_file_or_directory_to_stream(
        test_files.test_udf_py_file,
        leading_path=os.path.dirname(os.path.dirname(test_files.test_udf_directory)),
        add_init_py=True,
    )
    check_zip_files_and_close_stream(
        stream,
        [
            "resources/test_udf_dir/test_udf_file.py",
            "resources/test_udf_dir/__init__.py",
            "resources/__init__.py",
        ],
    )

    stream = Utils.zip_file_or_directory_to_stream(test_files.test_udf_directory)
    check_zip_files_and_close_stream(
        stream, ["test_udf_dir/", "test_udf_dir/test_udf_file.py"]
    )

    stream = Utils.zip_file_or_directory_to_stream(
        test_files.test_udf_directory,
        leading_path=os.path.dirname(test_files.test_udf_directory),
        add_init_py=True,
    )
    check_zip_files_and_close_stream(
        stream, ["test_udf_dir/", "test_udf_dir/test_udf_file.py"]
    )

    stream = Utils.zip_file_or_directory_to_stream(
        test_files.test_udf_directory,
        leading_path=os.path.dirname(os.path.dirname(test_files.test_udf_directory)),
        add_init_py=True,
    )
    check_zip_files_and_close_stream(
        stream,
        [
            "resources/test_udf_dir/",
            "resources/test_udf_dir/test_udf_file.py",
            "resources/__init__.py",
        ],
    )

    stream = Utils.zip_file_or_directory_to_stream(resources_path)
    check_zip_files_and_close_stream(
        stream,
        [
            "resources/",
            "resources/broken.csv",
            "resources/test.avro",
            "resources/test.orc",
            "resources/test.parquet",
            "resources/test.xml",
            "resources/test2CSV.csv",
            "resources/testCSV.csv",
            "resources/testCSVcolon.csv",
            "resources/testCSVquotes.csv",
            "resources/testJson.json",
            "resources/test_udf_dir/",
            "resources/test_udf_dir/test_udf_file.py",
        ],
    )

    with pytest.raises(FileNotFoundError):
        Utils.zip_file_or_directory_to_stream("file_not_found.txt")

    with pytest.raises(ValueError) as ex_info:
        Utils.zip_file_or_directory_to_stream(
            test_files.test_udf_directory, "test_udf_dir"
        )
    assert "doesn't lead to" in str(ex_info)


def test_get_stage_file_prefix_length():
    stageName = "@stage"  # stage/
    assert Utils.get_stage_file_prefix_length(stageName) == 6

    stageName2 = "@stage/"  # stage/
    assert Utils.get_stage_file_prefix_length(stageName2) == 6

    stageName3 = '@"sta/ge"/'  # sta/ge/
    assert Utils.get_stage_file_prefix_length(stageName3) == 7

    stageName4 = '@"stage.1"/dir'  # stage.1/dir/
    assert Utils.get_stage_file_prefix_length(stageName4) == 12

    quotedStageName = '@"stage"'  # stage/
    assert Utils.get_stage_file_prefix_length(quotedStageName) == 6

    quotedStageName2 = '@"stage"/'  # stage/
    assert Utils.get_stage_file_prefix_length(quotedStageName2) == 6

    stagePrefix = "@stage/dir"  # stage/dir/
    assert Utils.get_stage_file_prefix_length(stagePrefix) == 10

    stagePrefix2 = '@"stage"/dir'  # stage/dir/
    assert Utils.get_stage_file_prefix_length(stagePrefix2) == 10

    schemaStage = "@schema.stage"  # stage/
    assert Utils.get_stage_file_prefix_length(schemaStage) == 6

    schemaStage2 = "@schema.stage/"  # stage/
    assert Utils.get_stage_file_prefix_length(schemaStage2) == 6

    schemaStage3 = '@"schema".stage'  # stage/
    assert Utils.get_stage_file_prefix_length(schemaStage3) == 6

    schemaStage4 = '@"schema".stage/'  # stage/
    assert Utils.get_stage_file_prefix_length(schemaStage4) == 6

    schemaStage5 = '@"schema"."stage"'  # stage/
    assert Utils.get_stage_file_prefix_length(schemaStage5) == 6

    schemaStage6 = '@"schema"."sta/ge"/'  # sta/ge/
    assert Utils.get_stage_file_prefix_length(schemaStage6) == 7

    schemaStage7 = '@"schema.1".stage/dir'  # stage/dir/
    assert Utils.get_stage_file_prefix_length(schemaStage7) == 10

    dbStage = "@db.schema.stage"  # stage/
    assert Utils.get_stage_file_prefix_length(dbStage) == 6

    dbStage1 = "@db..stage"  # stage/
    assert Utils.get_stage_file_prefix_length(dbStage1) == 6

    dbStage2 = "@db.schema.stage/"  # stage/
    assert Utils.get_stage_file_prefix_length(dbStage2) == 6

    dbStage3 = "@db..stage/"  # stage/
    assert Utils.get_stage_file_prefix_length(dbStage3) == 6

    dbStage4 = '@"db"."schema"."stage"'  # stage/
    assert Utils.get_stage_file_prefix_length(dbStage4) == 6

    dbStage5 = '@"db".."stage"/'  # stage/
    assert Utils.get_stage_file_prefix_length(dbStage5) == 6

    dbStage6 = '@"db.1"."schema.1"."stage.1"/dir'  # stage.1/dir/
    assert Utils.get_stage_file_prefix_length(dbStage6) == 12

    tempStage = '@"TESTDB_SNOWPARK"."SN_TEST_OBJECT_1509309849".SNOWPARK_TEMP_STAGE_AS0HRUKQIZH0JOL'
    assert Utils.get_stage_file_prefix_length(tempStage) == 36

    tempStage2 = '@"TESTDB_SNOWPARK"."SN_TEST_OBJECT_1509309849".SNOWPARK_TEMP_STAGE_AS0HRUKQIZH0JOL/'
    assert Utils.get_stage_file_prefix_length(tempStage2) == 36

    tempStage3 = '@"TESTDB_SNOWPARK"."SN_TEST_OBJECT_1509309849"."SNOWPARK_TEMP_STAGE_AS0HRUKQIZH0JOL"/'
    assert Utils.get_stage_file_prefix_length(tempStage3) == 36

    userStage = "@~/dir"  # dir/
    assert Utils.get_stage_file_prefix_length(userStage) == 4

    tableStage = "db.schema.%table/dir"  # dir/
    assert Utils.get_stage_file_prefix_length(tableStage) == 4
