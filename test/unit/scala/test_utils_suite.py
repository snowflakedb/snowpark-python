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
