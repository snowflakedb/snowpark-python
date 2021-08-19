#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import os
from test.utils import TestFiles

from snowflake.snowpark.internal.utils import Utils


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
    resources_path = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "../../resources")
    )
    test_files = TestFiles(resources_path)
    assert (
        Utils.calculate_md5(test_files.test_file_avro)
        == "85bd7b9363853f1815254b1cbc608c22"
    )
    assert (
        Utils.calculate_md5(test_files.test_udf_directory)
        == "baacee1f13346c5515cab6be2612231d"
    )


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
