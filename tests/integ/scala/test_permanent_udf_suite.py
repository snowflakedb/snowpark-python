#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import os
import random
import string

import pytest

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark import Row, Session
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import SnowparkInvalidObjectNameException
from snowflake.snowpark.functions import call_udf
from tests.utils import TestFiles, Utils

pytestmark = pytest.mark.udf


@pytest.fixture(scope="module")
def new_session(session, db_parameters) -> Session:
    new_session = Session.builder.configs(db_parameters).create()
    yield new_session
    new_session.close()


@pytest.mark.skip(
    "Skip the test before SNOW-541414 is fixed and temp functions are not leaked"
)
def test_mix_temporary_and_permanent_udf(session, new_session):
    def add_one(x: int) -> int:
        return x + 1

    temp_func_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)
    perm_func_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)
    stage_name = Utils.random_stage_name()
    try:
        Utils.create_stage(session, stage_name, is_temporary=False)
        session.udf.register(add_one, name=temp_func_name, is_permanent=False)
        session.udf.register(
            add_one, name=perm_func_name, is_permanent=True, stage_location=stage_name
        )
        df = session.create_dataframe([1, 2], schema=["a"])
        Utils.check_answer(df.select(call_udf(temp_func_name, "a")), [Row(2), Row(3)])
        Utils.check_answer(df.select(call_udf(perm_func_name, "a")), [Row(2), Row(3)])

        # another session
        df2 = new_session.create_dataframe([1, 2], schema=["a"])
        Utils.check_answer(df2.select(call_udf(perm_func_name, "a")), [Row(2), Row(3)])
        with pytest.raises(ProgrammingError) as ex_info:
            Utils.check_answer(
                df2.select(call_udf(temp_func_name, "a")), [Row(2), Row(3)]
            )
        assert "SQL compilation error" in str(ex_info)
    finally:
        session._run_query(f"drop function if exists {temp_func_name}(int)")
        session._run_query(f"drop function if exists {perm_func_name}(int)")
        Utils.drop_stage(session, stage_name)


def test_valid_quoted_function_name(session):
    def add_one(x: int) -> int:
        return x + 1

    special_chars = "quoted_name"
    temp_func_name = (
        f'"{Utils.random_name_for_temp_object(TempObjectType.FUNCTION)}{special_chars}"'
    )
    perm_func_name = (
        f'"{special_chars}{Utils.random_name_for_temp_object(TempObjectType.FUNCTION)}"'
    )
    stage_name = Utils.random_stage_name()
    try:
        Utils.create_stage(session, stage_name, is_temporary=False)
        df = session.create_dataframe([1, 2], schema=["a"])
        session.udf.register(add_one, name=temp_func_name, is_permanent=False)
        session.udf.register(
            add_one, name=perm_func_name, is_permanent=True, stage_location=stage_name
        )
        Utils.check_answer(df.select(call_udf(temp_func_name, "a")), [Row(2), Row(3)])
        Utils.check_answer(df.select(call_udf(perm_func_name, "a")), [Row(2), Row(3)])
    finally:
        session._run_query(f"drop function if exists {temp_func_name}(int)")
        session._run_query(f"drop function if exists {perm_func_name}(int)")
        Utils.drop_stage(session, stage_name)


@pytest.mark.skip(
    "Skip the test before SNOW-541414 is fixed and temp functions are not leaked"
)
def test_support_fully_qualified_udf_name(session, new_session):
    def add_one(x: int) -> int:
        return x + 1

    temp_func_name = f"{session.get_fully_qualified_current_schema()}.{Utils.random_name_for_temp_object(TempObjectType.FUNCTION)}"
    perm_func_name = f"{session.get_fully_qualified_current_schema()}.{Utils.random_name_for_temp_object(TempObjectType.FUNCTION)}"
    stage_name = Utils.random_stage_name()
    try:
        Utils.create_stage(session, stage_name, is_temporary=False)
        session.udf.register(add_one, name=temp_func_name, is_permanent=False)
        session.udf.register(
            add_one, name=perm_func_name, is_permanent=True, stage_location=stage_name
        )
        df = session.create_dataframe([1, 2], schema=["a"])
        Utils.check_answer(df.select(call_udf(temp_func_name, "a")), [Row(2), Row(3)])
        Utils.check_answer(df.select(call_udf(perm_func_name, "a")), [Row(2), Row(3)])
        Utils.check_answer(
            df.select(call_udf(temp_func_name.split(".")[-1], "a")), [Row(2), Row(3)]
        )
        Utils.check_answer(
            df.select(call_udf(perm_func_name.split(".")[-1], "a")), [Row(2), Row(3)]
        )

        # another session
        df2 = new_session.create_dataframe([1, 2], schema=["a"])
        Utils.check_answer(df2.select(call_udf(perm_func_name, "a")), [Row(2), Row(3)])
        with pytest.raises(ProgrammingError) as ex_info:
            Utils.check_answer(
                df2.select(call_udf(temp_func_name, "a")), [Row(2), Row(3)]
            )
        assert "SQL compilation error" in str(ex_info)
    finally:
        session._run_query(f"drop function if exists {temp_func_name}(int)")
        session._run_query(f"drop function if exists {perm_func_name}(int)")
        Utils.drop_stage(session, stage_name)


def test_negative_invalid_permanent_function_name(session):
    def add_one(x: int) -> int:
        return x + 1

    stage_name = Utils.random_stage_name()
    invalid_func_names = ["testFunction ", " testFunction", "test Function"]
    for func_name in invalid_func_names:
        with pytest.raises(SnowparkInvalidObjectNameException) as ex_info:
            session.udf.register(
                add_one, name=func_name, is_permanent=True, stage_location=stage_name
            )
        assert f"The object name '{func_name}' is invalid." in str(ex_info)


def test_clean_up_files_if_udf_registration_fails(session):
    long_string = "".join(random.choices(string.ascii_letters, k=64 * 1024))

    def large_udf() -> str:
        return long_string

    perm_func_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)
    stage_name = Utils.random_stage_name()
    try:
        Utils.create_stage(session, stage_name, is_temporary=False)
        session.udf.register(
            large_udf, name=perm_func_name, is_permanent=True, stage_location=stage_name
        )
        # only udf_py_xxxx.py will exsit under stage_name/func_name
        assert len(session.sql(f"ls @{stage_name}/{perm_func_name}").collect()) == 1

        # register the same name UDF, CREATE UDF will fail
        with pytest.raises(ProgrammingError) as ex_info:
            session.udf.register(
                large_udf,
                name=perm_func_name,
                is_permanent=True,
                stage_location=stage_name,
            )
        assert "SQL compilation error" in str(ex_info)
        # without clean up, below LIST gets 2 files
        assert len(session.sql(f"ls @{stage_name}/{perm_func_name}").collect()) == 1
    finally:
        session._run_query(f"drop function if exists {perm_func_name}(int)")
        Utils.drop_stage(session, stage_name)


def test_udf_read_file_with_snowflake_import_directory_basic(session, resources_path):
    def read_file(name: str) -> str:
        import sys

        IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
        import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]
        file_path = import_dir + name
        file = open(file_path, "r")
        return file.read()

    test_csv_file = TestFiles(resources_path).test_file_csv
    filename = os.path.basename(test_csv_file)
    with open(test_csv_file, "r") as f:
        file_content = f.read()
    func_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)
    stage_name = Utils.random_stage_name()
    try:
        Utils.create_stage(session, stage_name, is_temporary=False)
        session.add_import(test_csv_file)
        df = session.create_dataframe([filename]).to_df("a")
        session.udf.register(
            read_file, name=func_name, is_permanent=True, stage_location=stage_name
        )
        assert df.select(call_udf(func_name, df.a)).collect() == [Row(file_content)]
    finally:
        session._run_query(f"drop function if exists {func_name}(string)")
        Utils.drop_stage(session, stage_name)
        session.clear_imports()


def test_udf_read_file_with_snowflake_import_directory_complex(
    session, tmpdir_factory, new_session
):
    def read_file(name: str) -> str:
        import sys

        IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
        import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]
        file_path = import_dir + name
        file = open(file_path, "r")
        return file.read()

    # Two session to read two files (same file name, but different content) in UDF
    filename = f"file_{Utils.random_alphanumeric_str(10)}"
    temp_dir1 = tmpdir_factory.mktemp("data")
    temp_dir2 = tmpdir_factory.mktemp("data")
    temp_file_path1 = temp_dir1.join(filename)
    temp_file_path2 = temp_dir2.join(filename)
    file_content1 = "abc,123"
    file_content2 = "abcd,1234"
    with open(temp_file_path1, "w") as f1:
        f1.write(file_content1)
    with open(temp_file_path2, "w") as f2:
        f2.write(file_content2)

    func_name1 = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)
    func_name2 = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)
    stage_name = Utils.random_stage_name()
    try:
        Utils.create_stage(session, stage_name, is_temporary=False)
        session.add_import(temp_file_path1.strpath)
        new_session.add_import(temp_file_path2.strpath)
        df1 = session.create_dataframe([filename]).to_df("a")
        df2 = new_session.create_dataframe([filename]).to_df("a")
        session.udf.register(
            read_file, name=func_name1, is_permanent=True, stage_location=stage_name
        )
        new_session.udf.register(
            read_file, name=func_name2, is_permanent=True, stage_location=stage_name
        )
        assert df1.select(call_udf(func_name1, df1.a)).collect() == [Row(file_content1)]
        assert df2.select(call_udf(func_name2, df2.a)).collect() == [Row(file_content2)]
    finally:
        session._run_query(f"drop function if exists {func_name1}(string)")
        Utils.drop_stage(session, stage_name)
        session.clear_imports()
        new_session.clear_imports()


def test_udf_read_file_with_staged_file(session, resources_path):
    def read_file(name: str) -> str:
        import sys

        IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
        import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]
        file_path = import_dir + name
        file = open(file_path, "r")
        return file.read()

    test_csv_file = TestFiles(resources_path).test_file_csv
    filename = os.path.basename(test_csv_file)
    with open(test_csv_file, "r") as f:
        file_content = f.read()
    func_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)
    stage_name = Utils.random_stage_name()
    try:
        Utils.create_stage(session, stage_name, is_temporary=False)
        session._conn.upload_file(
            test_csv_file, stage_location=stage_name, compress_data=False
        )
        session.add_import(f"@{stage_name}/{filename}")
        df = session.create_dataframe([filename]).to_df("a")
        session.udf.register(
            read_file, name=func_name, is_permanent=True, stage_location=stage_name
        )
        assert df.select(call_udf(func_name, df.a)).collect() == [Row(file_content)]
    finally:
        session._run_query(f"drop function if exists {func_name}(string)")
        Utils.drop_stage(session, stage_name)
        session.clear_imports()
