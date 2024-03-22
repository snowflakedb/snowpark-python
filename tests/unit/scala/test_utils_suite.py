#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import logging
import os
import zipfile

import pytest

from snowflake.snowpark._internal.utils import (
    SCOPED_TEMPORARY_STRING,
    TEMPORARY_STRING,
    calculate_checksum,
    deprecated,
    experimental,
    get_stage_file_prefix_length,
    get_temp_type_for_object,
    get_udf_upload_prefix,
    is_snowflake_quoted_id_case_insensitive,
    is_snowflake_unquoted_suffix_case_insensitive,
    is_sql_select_statement,
    normalize_path,
    private_preview,
    result_set_to_iter,
    result_set_to_rows,
    unwrap_stage_location_single_quote,
    validate_object_name,
    warning,
    warning_dict,
    zip_file_or_directory_to_stream,
)
from tests.utils import IS_WINDOWS, TestFiles

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
        validate_object_name(identifier)


def test_calculate_checksum():
    assert (
        calculate_checksum(test_files.test_file_avro)
        == "3317ed5a935104274f4c7ae12b81fac063ed252570876e3f535cd8e13d8cbbb8"
    )

    # Check that a smaller chunk size returns a different size on a sufficiently larger file
    # Default Chunk Size
    assert (
        calculate_checksum(test_files.test_file_with_special_characters_parquet)
        == "45128082344751b0e35f3c4d07108a42064d2326029e7fca08a7ceb0053ead9f"
    )
    # Smaller Chunk Size
    assert (
        calculate_checksum(
            test_files.test_file_with_special_characters_parquet, chunk_size=1024
        )
        == "83c9e09fcadca8637e5c29870d996dc3ef5acfc9f838ae8ae1cd1fdbae86e2dc"
    )
    # Read whole file
    assert (
        calculate_checksum(
            test_files.test_file_with_special_characters_parquet,
            chunk_size=1024,
            whole_file_hash=True,
        )
        == "f7bb6ba7de6d458945831882d34937d9f157ccd3186423a4e70b608292ae1cef"
    )

    assert (
        calculate_checksum(test_files.test_file_avro, algorithm="md5")
        == "85bd7b9363853f1815254b1cbc608c22"
    )
    if IS_WINDOWS:
        assert (
            calculate_checksum(test_files.test_udf_directory)
            == "dacc9a957e526063c57a9d5c03644b24dfbe0f789efa224ff1b4d88138a2a6d8"
        )
        assert (
            calculate_checksum(test_files.test_udf_directory, algorithm="md5")
            == "c3988b8dcab346a2e8152e06276b4033"
        )
    else:
        assert (
            calculate_checksum(test_files.test_udf_directory)
            == "3a2607ef293801f59e7840f5be423d4a55edfe2ac732775dcfda01205df377f0"
        )
        assert (
            calculate_checksum(test_files.test_udf_directory, algorithm="md5")
            == "b72b61c8d5639fff8aa9a80278dba60f"
        )
        # Validate that hashes are different when reading whole dir.
        # Using a sufficiently small chunk size so that the hashes differ.
        assert (
            calculate_checksum(test_files.test_udf_directory, chunk_size=128)
            == "c071de824a67c083edad45c2b18729e17c50f1b13be980140437063842ea2469"
        )
        assert (
            calculate_checksum(
                test_files.test_udf_directory, chunk_size=128, whole_file_hash=True
            )
            == "3a2607ef293801f59e7840f5be423d4a55edfe2ac732775dcfda01205df377f0"
        )


def test_normalize_stage_location():
    name1 = "stage"
    unwrap_name1 = unwrap_stage_location_single_quote(name1 + "  ")
    assert unwrap_name1 == f"@{name1}"
    assert unwrap_stage_location_single_quote("@" + name1 + "  ") == f"@{name1}"
    assert unwrap_stage_location_single_quote(unwrap_name1) == unwrap_name1

    name2 = '"DATABASE"."SCHEMA"."STAGE"'
    unwrap_name2 = unwrap_stage_location_single_quote(name2 + "  ")
    assert unwrap_name2 == f"@{name2}"
    assert unwrap_stage_location_single_quote("@" + name2 + "  ") == f"@{name2}"
    assert unwrap_stage_location_single_quote(unwrap_name2) == unwrap_name2

    name3 = "s t a g 'e"
    unwrap_name3 = unwrap_stage_location_single_quote(name3)
    assert unwrap_name3 == "@s t a g 'e"
    assert unwrap_stage_location_single_quote(unwrap_name3) == unwrap_name3

    name4 = "' s t a g 'e'"
    unwrap_name4 = unwrap_stage_location_single_quote(name4)
    assert unwrap_name4 == "@ s t a g 'e"
    assert unwrap_stage_location_single_quote(unwrap_name4) == unwrap_name4


@pytest.mark.parametrize("is_local", [True, False])
def test_normalize_file(is_local):
    symbol = "file://" if is_local else "@"
    name1 = "stage"
    assert normalize_path(name1, is_local) == f"'{symbol}stage'"
    name2 = "sta'ge"
    assert normalize_path(name2, is_local) == f"'{symbol}sta\\'ge'"
    name3 = "s ta\\'ge "
    assert normalize_path(name3, is_local) == (
        f"'{symbol}s ta/\\'ge'" if is_local and IS_WINDOWS else f"'{symbol}s ta\\\\'ge'"
    )


def test_get_udf_upload_prefix():
    assert get_udf_upload_prefix("name") == "name"
    assert get_udf_upload_prefix("abcABC_0123456789") == "abcABC_0123456789"
    assert "name_" in get_udf_upload_prefix('"name"')
    assert "table_" in get_udf_upload_prefix(" table")
    assert "table_" in get_udf_upload_prefix("table ")
    assert "schemaview_" in get_udf_upload_prefix("schema.view")
    assert "SCHEMAVIEW_" in get_udf_upload_prefix('"SCHEMA"."VIEW"')
    assert "dbschematable" in get_udf_upload_prefix("db.schema.table")
    assert "dbschematable" in get_udf_upload_prefix('"db"."schema"."table"')


def test_zip_file_or_directory_to_stream():
    def check_zip_files_and_close_stream(input_stream, expected_files):
        with zipfile.ZipFile(input_stream) as zf:
            assert zf.testzip() is None
            assert sorted(zf.namelist()) == sorted(expected_files)

    with zip_file_or_directory_to_stream(test_files.test_udf_py_file) as stream:
        check_zip_files_and_close_stream(stream, ["test_udf_file.py"])

    with zip_file_or_directory_to_stream(
        test_files.test_udf_py_file,
        leading_path=test_files.test_udf_directory,
    ) as stream:
        check_zip_files_and_close_stream(stream, ["test_udf_file.py"])

    with zip_file_or_directory_to_stream(
        test_files.test_udf_py_file,
        leading_path=os.path.dirname(test_files.test_udf_directory),
    ) as stream:
        check_zip_files_and_close_stream(
            stream, ["test_udf_dir/", "test_udf_dir/test_udf_file.py"]
        )

    with zip_file_or_directory_to_stream(
        test_files.test_udf_py_file,
        leading_path=os.path.dirname(os.path.dirname(test_files.test_udf_directory)),
    ) as stream:
        check_zip_files_and_close_stream(
            stream,
            [
                "resources/",
                "resources/test_udf_dir/",
                "resources/test_udf_dir/test_udf_file.py",
            ],
        )

    with zip_file_or_directory_to_stream(test_files.test_udf_directory) as stream:
        check_zip_files_and_close_stream(
            stream,
            [
                "test_udf_dir/",
                "test_udf_dir/test_another_udf_file.py",
                "test_udf_dir/test_pandas_udf_file.py",
                "test_udf_dir/test_udf_file.py",
            ],
        )

    with zip_file_or_directory_to_stream(
        test_files.test_udf_directory,
        leading_path=os.path.dirname(test_files.test_udf_directory),
    ) as stream:
        check_zip_files_and_close_stream(
            stream,
            [
                "test_udf_dir/",
                "test_udf_dir/test_another_udf_file.py",
                "test_udf_dir/test_pandas_udf_file.py",
                "test_udf_dir/test_udf_file.py",
            ],
        )

    with zip_file_or_directory_to_stream(
        test_files.test_udf_directory,
        leading_path=os.path.dirname(os.path.dirname(test_files.test_udf_directory)),
    ) as stream:
        check_zip_files_and_close_stream(
            stream,
            [
                "resources/",
                "resources/test_udf_dir/",
                "resources/test_udf_dir/test_another_udf_file.py",
                "resources/test_udf_dir/test_pandas_udf_file.py",
                "resources/test_udf_dir/test_udf_file.py",
            ],
        )

    with zip_file_or_directory_to_stream(
        test_files.test_udtf_directory,
        leading_path=os.path.dirname(os.path.dirname(test_files.test_udtf_directory)),
    ) as stream:
        check_zip_files_and_close_stream(
            stream,
            [
                "resources/",
                "resources/test_udtf_dir/",
                "resources/test_udtf_dir/test_udtf_file.py",
                "resources/test_udtf_dir/test_vectorized_udtf.py",
            ],
        )

    with zip_file_or_directory_to_stream(resources_path) as stream:
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
                "resources/testCSVvariousData.csv",
                "resources/testCSVcolon.csv",
                "resources/testCSVheader.csv",
                "resources/testCSVquotes.csv",
                "resources/testCSVspecialFormat.csv",
                "resources/testJSONspecialFormat.json.gz",
                "resources/testJson.json",
                "resources/test_all_data_types.parquet",
                "resources/test_file_with_special_characters.parquet",
                "resources/test_requirements.txt",
                "resources/test_requirements_unsupported.txt",
                "resources/test_environment.yml",
                "resources/test_sp_dir/",
                "resources/test_sp_dir/test_sp_file.py",
                "resources/test_sp_dir/test_sp_mod3_file.py",
                "resources/test_sp_dir/test_table_sp_file.py",
                "resources/test_udf_dir/",
                "resources/test_udf_dir/test_another_udf_file.py",
                "resources/test_udf_dir/test_pandas_udf_file.py",
                "resources/test_udf_dir/test_udf_file.py",
                "resources/test_udtf_dir/",
                "resources/test_udtf_dir/test_udtf_file.py",
                "resources/test_udtf_dir/test_vectorized_udtf.py",
                "resources/test_udaf_dir/",
                "resources/test_udaf_dir/test_udaf_file.py",
            ],
        )

    with pytest.raises(FileNotFoundError):
        with zip_file_or_directory_to_stream("file_not_found.txt"):
            pass

    with pytest.raises(ValueError) as ex_info:
        with zip_file_or_directory_to_stream(
            test_files.test_udf_directory, "test_udf_dir"
        ):
            pass
    assert "doesn't lead to" in str(ex_info)


def test_get_stage_file_prefix_length():
    stageName = "@stage"  # stage/
    assert get_stage_file_prefix_length(stageName) == 6

    stageName2 = "@stage/"  # stage/
    assert get_stage_file_prefix_length(stageName2) == 6

    stageName3 = '@"sta/ge"/'  # sta/ge/
    assert get_stage_file_prefix_length(stageName3) == 7

    stageName4 = '@"stage.1"/dir'  # stage.1/dir/
    assert get_stage_file_prefix_length(stageName4) == 12

    stageName5 = '@" stage.\'1"/dir'  # [whitespace]stage.'1/dir/
    assert get_stage_file_prefix_length(stageName5) == 14

    stageName6 = "'@\" stage.'1\"/dir'"  # [whitespace]stage.'1/dir/
    assert get_stage_file_prefix_length(stageName6) == 14

    stageName7 = "'@\" stage.\\'1\"/dir'"  # [whitespace]stage.'1/dir/
    assert get_stage_file_prefix_length(stageName7) == 14

    quotedStageName = '@"stage"'  # stage/
    assert get_stage_file_prefix_length(quotedStageName) == 6

    quotedStageName2 = '@"stage"/'  # stage/
    assert get_stage_file_prefix_length(quotedStageName2) == 6

    stagePrefix = "@stage/dir"  # stage/dir/
    assert get_stage_file_prefix_length(stagePrefix) == 10

    stagePrefix2 = '@"stage"/dir'  # stage/dir/
    assert get_stage_file_prefix_length(stagePrefix2) == 10

    schemaStage = "@schema.stage"  # stage/
    assert get_stage_file_prefix_length(schemaStage) == 6

    schemaStage2 = "@schema.stage/"  # stage/
    assert get_stage_file_prefix_length(schemaStage2) == 6

    schemaStage3 = '@"schema".stage'  # stage/
    assert get_stage_file_prefix_length(schemaStage3) == 6

    schemaStage4 = '@"schema".stage/'  # stage/
    assert get_stage_file_prefix_length(schemaStage4) == 6

    schemaStage5 = '@"schema"."stage"'  # stage/
    assert get_stage_file_prefix_length(schemaStage5) == 6

    schemaStage6 = '@"schema"."sta/ge"/'  # sta/ge/
    assert get_stage_file_prefix_length(schemaStage6) == 7

    schemaStage7 = '@"schema.1".stage/dir'  # stage/dir/
    assert get_stage_file_prefix_length(schemaStage7) == 10

    dbStage = "@db.schema.stage"  # stage/
    assert get_stage_file_prefix_length(dbStage) == 6

    dbStage1 = "@db..stage"  # stage/
    assert get_stage_file_prefix_length(dbStage1) == 6

    dbStage2 = "@db.schema.stage/"  # stage/
    assert get_stage_file_prefix_length(dbStage2) == 6

    dbStage3 = "@db..stage/"  # stage/
    assert get_stage_file_prefix_length(dbStage3) == 6

    dbStage4 = '@"db"."schema"."stage"'  # stage/
    assert get_stage_file_prefix_length(dbStage4) == 6

    dbStage5 = '@"db".."stage"/'  # stage/
    assert get_stage_file_prefix_length(dbStage5) == 6

    dbStage6 = '@"db.1"."schema.1"."stage.1"/dir'  # stage.1/dir/
    assert get_stage_file_prefix_length(dbStage6) == 12

    dbStage7 = '\'@"db.1"."schema.1"."\'stage.1"/dir\''  # 'stage.1/dir/
    assert get_stage_file_prefix_length(dbStage7) == 13

    tempStage = '@"TESTDB_SNOWPARK"."SN_TEST_OBJECT_1509309849".SNOWPARK_TEMP_STAGE_AS0HRUKQIZH0JOL'
    assert get_stage_file_prefix_length(tempStage) == 36

    tempStage2 = '@"TESTDB_SNOWPARK"."SN_TEST_OBJECT_1509309849".SNOWPARK_TEMP_STAGE_AS0HRUKQIZH0JOL/'
    assert get_stage_file_prefix_length(tempStage2) == 36

    tempStage3 = '@"TESTDB_SNOWPARK"."SN_TEST_OBJECT_1509309849"."SNOWPARK_TEMP_STAGE_AS0HRUKQIZH0JOL"/'
    assert get_stage_file_prefix_length(tempStage3) == 36

    userStage = "@~/dir"  # dir/
    assert get_stage_file_prefix_length(userStage) == 4

    tableStage = "db.schema.%table/dir"  # dir/
    assert get_stage_file_prefix_length(tableStage) == 4


def test_use_scoped_temporary():
    assert (
        get_temp_type_for_object(use_scoped_temp_objects=True, is_generated=False)
        == TEMPORARY_STRING
    )
    assert (
        get_temp_type_for_object(use_scoped_temp_objects=True, is_generated=True)
        == SCOPED_TEMPORARY_STRING
    )
    assert (
        get_temp_type_for_object(use_scoped_temp_objects=False, is_generated=True)
        == TEMPORARY_STRING
    )
    assert (
        get_temp_type_for_object(use_scoped_temp_objects=False, is_generated=False)
        == TEMPORARY_STRING
    )


def test_warning(caplog):
    def f():
        return 1

    try:
        with caplog.at_level(logging.WARNING):
            warning("aaa", "bbb", 2)
            warning("aaa", "bbb", 2)
            warning("aaa", "bbb", 2)
        assert caplog.text.count("bbb") == 2
        with caplog.at_level(logging.WARNING):
            warning(f.__qualname__, "ccc", 2)
            warning(f.__qualname__, "ccc", 2)
            warning(f.__qualname__, "ccc", 2)
        assert caplog.text.count("ccc") == 2
    finally:
        warning_dict.clear()


@pytest.mark.parametrize("decorator", [deprecated, experimental])
def test_func_decorator(caplog, decorator):
    try:

        @decorator(
            version="1.0.0",
            extra_warning_text="extra_warning_text",
            extra_doc_string="extra_doc_string",
        )
        def f():
            return 1

        assert "extra_doc_string" in f.__doc__
        with caplog.at_level(logging.WARNING):
            f()
            f()

        assert decorator.__name__ in caplog.text
        assert caplog.text.count("1.0.0") == 1
        assert caplog.text.count("extra_warning_text") == 1
    finally:
        warning_dict.clear()


def test_is_sql_select_statement():
    select_sqls = [
        "select * from dual",
        "(select * from dual)",
        "(((select * from dual)))",
        " select * from dual",
        "   select * from dual",
        "( ( ( select * from dual",
        "with t as (select 1) select * from t",
        "(with t as (select 1) select * from t",
        "(((with t as (select 1) select * from t",
        " with t as (select 1) select * from t",
        "   with t as (select 1) select * from t",
        "( ( ( with t as (select 1) select * from t",
        "select*fromdual",  # don't care if the sql is valid.
        "SELECT 1",
        "SeLeCt 1",
        "WITH t as (select 1) select * from t",
        "WiTh t as (select 1) select * from t",
        """WITH t AS (
            SELECT '
            with anon_sproc as procedure
            ' as col1
           ) select col1 from t
        """,
    ]
    for s in select_sqls:
        assert is_sql_select_statement(s)

    non_select_sqls = [
        "selec * from tables",
        "wit t as (select 1) select * from t",
        "()select * from tables",
        "()with t as (select 1) select * from t",
        "show tables",
        "lkdfadsk select",
        "ljkfdshdf with",
        "with anon_sproc0 as procedure ",
        "  with anon_sproc1 AS  PROCEDURE ",
    ]
    for ns in non_select_sqls:
        assert is_sql_select_statement(ns) is False


def test_is_snowflake_quoted_id_case_insensitive():
    assert is_snowflake_quoted_id_case_insensitive('"ABC"')
    assert is_snowflake_quoted_id_case_insensitive('"AB_C"')
    assert is_snowflake_quoted_id_case_insensitive('"_AB0C_9"')
    # negative
    assert is_snowflake_quoted_id_case_insensitive('"aBC"') is False
    assert is_snowflake_quoted_id_case_insensitive('"AbC"') is False
    assert is_snowflake_quoted_id_case_insensitive('"ab c"') is False
    assert is_snowflake_quoted_id_case_insensitive('"A BC"') is False


def test_is_snowflake_unquoted_suffix_case_insensitive():
    assert is_snowflake_unquoted_suffix_case_insensitive("ABC")
    assert is_snowflake_unquoted_suffix_case_insensitive("AbC")
    assert is_snowflake_unquoted_suffix_case_insensitive("abc")
    assert is_snowflake_unquoted_suffix_case_insensitive("ab_c")
    assert is_snowflake_unquoted_suffix_case_insensitive("_ab0c_9")
    assert is_snowflake_unquoted_suffix_case_insensitive("0ABC")
    # negative
    assert is_snowflake_unquoted_suffix_case_insensitive('"ABC"') is False
    assert is_snowflake_unquoted_suffix_case_insensitive("ab c") is False
    assert is_snowflake_unquoted_suffix_case_insensitive("A BC") is False


def test_private_preview_decorator(caplog):
    extra_warning = "Extra warning."
    extra_doc = "Extra doc."
    expected_warning_text = f"test_private_preview_decorator.<locals>.foo() is in private preview since 0.1. Do not use it in production. {extra_warning}"

    @private_preview(
        version="0.1", extra_warning_text=extra_warning, extra_doc_string=extra_doc
    )
    def foo():
        pass

    caplog.clear()
    warning_dict.clear()
    try:
        with caplog.at_level(logging.WARNING):
            foo()
        assert extra_doc in foo.__doc__
        assert expected_warning_text in caplog.messages
        caplog.clear()
        with caplog.at_level(logging.WARNING):
            foo()
        assert expected_warning_text not in caplog.text
    finally:
        warning_dict.clear()


@pytest.mark.parametrize("function", [result_set_to_iter, result_set_to_rows])
def test_result_set_none(function):
    data = [[1], None, []]
    with pytest.raises(
        ValueError, match="Result returned from Python connector is None"
    ):
        list(function(data))
