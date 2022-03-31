#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import datetime
import os
import sys
from typing import Dict, List, Optional, Union
from unittest.mock import patch

import pytest

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark import Session
from snowflake.snowpark._internal.utils import Utils as InternalUtils
from snowflake.snowpark.exceptions import SnowparkInvalidObjectNameException
from snowflake.snowpark.functions import sproc
from snowflake.snowpark.types import DoubleType, IntegerType, PandasSeries, StringType
from tests.utils import TempObjectType, TestFiles, Utils

try:
    import numpy
    import pandas

    is_pandas_and_numpy_available = True
except ImportError:
    is_pandas_and_numpy_available = False

tmp_stage_name = Utils.random_stage_name()


@pytest.fixture(scope="module", autouse=True)
def setup(session, resources_path):
    test_files = TestFiles(resources_path)
    Utils.create_stage(session, tmp_stage_name, is_temporary=True)
    session.add_packages("snowflake-snowpark-python")
    Utils.upload_to_stage(
        session, tmp_stage_name, test_files.test_sp_py_file, compress=False
    )


def test_basic_stored_procedure(session):
    def return1(session_):
        return session_.sql("select '1'").collect()[0][0]

    def plus1(session_, x):
        return session_.sql(f"select {x} + 1").collect()[0][0]

    def add(session_, x, y):
        return session_.sql(f"select {x} + {y}").collect()[0][0]

    def int2str(session_, x):
        return session_.sql(f"select cast({x} as string)").collect()[0][0]

    return1_sp = sproc(return1, return_type=StringType())
    plus1_sp = sproc(plus1, return_type=IntegerType(), input_types=[IntegerType()])
    add_sp = sproc(
        add, return_type=IntegerType(), input_types=[IntegerType(), IntegerType()]
    )
    int2str_sp = sproc(int2str, return_type=StringType(), input_types=[IntegerType()])
    pow_sp = sproc(
        lambda session_, x, y: session_.sql(f"select pow({x}, {y})").collect()[0][0],
        return_type=DoubleType(),
        input_types=[IntegerType(), IntegerType()],
    )

    assert return1_sp() == "1"
    assert plus1_sp(1) == 2
    assert add_sp(4, 6) == 10
    assert int2str_sp(123) == "123"
    assert pow_sp(2, 10) == 1024
    assert return1_sp(session=session) == "1"
    assert plus1_sp(1, session=session) == 2
    assert add_sp(4, 6, session=session) == 10
    assert int2str_sp(123, session=session) == "123"
    assert pow_sp(2, 10, session=session) == 1024


def test_call_named_stored_procedure(session, temp_schema, db_parameters):
    session._run_query("drop function if exists test_mul(int, int)")
    sproc(
        lambda session_, x, y: session_.sql(f"select {x} * {y}").collect()[0][0],
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
        name="test_mul",
    )
    assert session.call("test_mul", 13, 19) == 13 * 19
    assert (
        session.call(f"{session.get_fully_qualified_current_schema()}.test_mul", 13, 19)
        == 13 * 19
    )

    # create a stored procedure when the session doesn't have a schema
    new_session = (
        Session.builder.configs(db_parameters)._remove_config("schema").create()
    )
    new_session.add_packages("snowflake-snowpark-python")
    try:
        assert not new_session.getDefaultSchema()
        tmp_stage_name_in_temp_schema = (
            f"{temp_schema}.{Utils.random_name_for_temp_object(TempObjectType.STAGE)}"
        )
        new_session._run_query(f"create temp stage {tmp_stage_name_in_temp_schema}")
        full_sp_name = f"{temp_schema}.test_add"
        new_session._run_query(f"drop function if exists {full_sp_name}(int, int)")
        new_session.sproc.register(
            lambda session_, x, y: session_.sql(f"select {x} + {y}").collect()[0][0],
            return_type=IntegerType(),
            input_types=[IntegerType(), IntegerType()],
            name=[*temp_schema.split("."), "test_add"],
            stage_location=InternalUtils.unwrap_stage_location_single_quote(
                tmp_stage_name_in_temp_schema
            ),
        )
        assert new_session.call(full_sp_name, 13, 19) == 13 + 19
        # oen result in the temp schema
        assert (
            len(
                new_session.sql(
                    f"show procedures like '%test_add%' in schema {temp_schema}"
                ).collect()
            )
            == 1
        )
    finally:
        new_session.close()
        # restore active session


def test_recursive_function(session):
    # Test recursive function
    def factorial(session_, n):
        return 1 if n == 1 or n == 0 else n * factorial(session_, n - 1)

    factorial_sp = sproc(
        factorial, return_type=IntegerType(), input_types=[IntegerType()]
    )
    assert factorial_sp(3) == factorial(session, 3)


def test_nested_function(session):
    def outer_func(session_):
        def inner_func():
            return "snow"

        return session_.sql(f"select '{inner_func()}-{inner_func()}'").collect()[0][0]

    def square(session_, x):
        return session_.sql(f"select square({x})").collect()[0][0]

    def cube(session_, x):
        return square(session_, x) * x

    outer_func_sp = sproc(outer_func, return_type=StringType())
    assert outer_func_sp() == "snow-snow"

    # we don't need to register function square()
    cube_sp = sproc(cube, return_type=IntegerType(), input_types=[IntegerType()])
    assert cube_sp(2) == 8

    # but we can still register function square()
    square_sp = sproc(square, return_type=IntegerType(), input_types=[IntegerType()])
    assert cube_sp(2) == 8
    assert square_sp(2) == 4


def test_decorator_function(session):
    def decorator_do_twice(func):
        def wrapper(*args, **kwargs):
            l1 = func(*args, **kwargs)
            l2 = func(*args, **kwargs)
            return l1 * l2

        return wrapper

    @decorator_do_twice
    def square(session_, x):
        return session_.sql(f"select square({x})").collect()[0][0]

    square_twice_sp = sproc(
        square,
        return_type=IntegerType(),
        input_types=[IntegerType()],
    )
    assert square_twice_sp(2) == 16


def test_annotation_syntax(session):
    @sproc(return_type=IntegerType(), input_types=[IntegerType(), IntegerType()])
    def add_sp(session_, x, y):
        return session_.sql(f"SELECT {x} + {y}").collect()[0][0]

    @sproc(return_type=StringType())
    def snow(session_):
        return session_.sql("SELECT 'snow'").collect()[0][0]

    assert add_sp(1, 2) == 3
    assert snow() == "snow"


def test_register_sp_from_file(session, resources_path, tmpdir):
    test_files = TestFiles(resources_path)

    mod5_sp = session.sproc.register_from_file(
        test_files.test_sp_py_file,
        "mod5",
        return_type=IntegerType(),
        input_types=[IntegerType()],
    )
    assert isinstance(mod5_sp.func, tuple)
    assert mod5_sp(3) == 3

    # test zip file
    from zipfile import ZipFile

    zip_path = f"{tmpdir.join(os.path.basename(test_files.test_sp_py_file))}.zip"
    with ZipFile(zip_path, "w") as zf:
        zf.write(
            test_files.test_sp_py_file, os.path.basename(test_files.test_sp_py_file)
        )

    mod5_sp_zip = session.sproc.register_from_file(
        zip_path, "mod5", return_type=IntegerType(), input_types=[IntegerType()]
    )
    assert mod5_sp_zip(3) == 3

    # test a remote python file
    stage_file = f"@{tmp_stage_name}/{os.path.basename(test_files.test_sp_py_file)}"
    mod5_sp_stage = session.sproc.register_from_file(
        stage_file, "mod5", return_type=IntegerType(), input_types=[IntegerType()]
    )
    assert mod5_sp_stage(3) == 3


def test_session_register_sp(session):
    add_sp = session.sproc.register(
        lambda session_, x, y: session_.sql(f"SELECT {x} + {y}").collect()[0][0],
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
    )
    assert add_sp(1, 2) == 3


def test_add_import_local_file(session, resources_path):
    test_files = TestFiles(resources_path)
    # This is a hack in the test such that we can just use `from test_sp import mod5`,
    # instead of `from test.resources.test_sp.test_sp import mod5`. Then we can test
    # `import_as` argument.
    with patch.object(
        sys, "path", [*sys.path, resources_path, test_files.test_sp_directory]
    ):

        def plus4_then_mod5(session_, x):
            from test_sp_dir.test_sp_file import mod5

            return mod5(session_, session_.sql(f"SELECT {x} + 4").collect()[0][0])

        def plus4_then_mod5_direct_import(session_, x):
            from test_sp_file import mod5

            return mod5(session_, session_.sql(f"SELECT {x} + 4").collect()[0][0])

        session.add_import(
            test_files.test_sp_py_file, import_path="test_sp_dir.test_sp_file"
        )
        plus4_then_mod5_sp = sproc(
            plus4_then_mod5, return_type=IntegerType(), input_types=[IntegerType()]
        )
        assert plus4_then_mod5_sp(3) == 2

        # if import_as argument changes, the checksum of the file will also change
        # and we will overwrite the file in the stage
        session.add_import(test_files.test_sp_py_file)
        plus4_then_mod5_direct_import_sp = sproc(
            plus4_then_mod5_direct_import,
            return_type=IntegerType(),
            input_types=[IntegerType()],
        )
        assert plus4_then_mod5_direct_import_sp(3) == 2

        # clean
        session.clear_imports()


def test_add_import_local_directory(session, resources_path):
    test_files = TestFiles(resources_path)
    with patch.object(
        sys, "path", [*sys.path, resources_path, os.path.dirname(resources_path)]
    ):

        def plus4_then_mod5(session_, x):
            from resources.test_sp_dir.test_sp_file import mod5

            return mod5(session_, session_.sql(f"SELECT {x} + 4").collect()[0][0])

        def plus4_then_mod5_direct_import(session_, x):
            from test_sp_dir.test_sp_file import mod5

            return mod5(session_, session_.sql(f"SELECT {x} + 4").collect()[0][0])

        session.add_import(
            test_files.test_sp_directory, import_path="resources.test_sp_dir"
        )
        plus4_then_mod5_sp = sproc(
            plus4_then_mod5, return_type=IntegerType(), input_types=[IntegerType()]
        )
        assert plus4_then_mod5_sp(3) == 2

        session.add_import(test_files.test_sp_directory)
        plus4_then_mod5_direct_import_sp = sproc(
            plus4_then_mod5_direct_import,
            return_type=IntegerType(),
            input_types=[IntegerType()],
        )
        assert plus4_then_mod5_direct_import_sp(3) == 2

        # clean
        session.clear_imports()


def test_add_import_stage_file(session, resources_path):
    test_files = TestFiles(resources_path)
    with patch.object(sys, "path", [*sys.path, test_files.test_sp_directory]):

        def plus4_then_mod5(session_, x):
            from test_sp_file import mod5

            return mod5(session_, session_.sql(f"SELECT {x} + 4").collect()[0][0])

        stage_file = f"@{tmp_stage_name}/{os.path.basename(test_files.test_sp_py_file)}"
        Utils.upload_to_stage(
            session, tmp_stage_name, test_files.test_sp_py_file, compress=False
        )
        session.add_import(stage_file)
        plus4_then_mod5_sp = sproc(
            plus4_then_mod5, return_type=IntegerType(), input_types=[IntegerType()]
        )

        assert plus4_then_mod5_sp(3) == 2

        # clean
        session.clear_imports()


def test_sp_level_import(session, resources_path):
    test_files = TestFiles(resources_path)
    with patch.object(sys, "path", [*sys.path, resources_path]):

        def plus4_then_mod5(session_, x):
            from test_sp_dir.test_sp_file import mod5

            return mod5(session_, session_.sql(f"SELECT {x} + 4").collect()[0][0])

        # with sp-level imports
        plus4_then_mod5_sp = sproc(
            plus4_then_mod5,
            return_type=IntegerType(),
            input_types=[IntegerType()],
            imports=[(test_files.test_sp_py_file, "test_sp_dir.test_sp_file")],
        )
        assert plus4_then_mod5_sp(3) == 2

        # without sp-level imports
        plus4_then_mod5_sp = sproc(
            plus4_then_mod5,
            return_type=IntegerType(),
            input_types=[IntegerType()],
        )
        with pytest.raises(ProgrammingError) as ex_info:
            plus4_then_mod5_sp(3)
        assert "No module named" in str(ex_info)


def test_type_hints(session):
    @sproc()
    def add_sp(session_: Session, x: int, y: int) -> int:
        return session_.sql(f"SELECT {x} + {y}").collect()[0][0]

    @sproc
    def snow_sp(session_: Session, x: int) -> Optional[str]:
        return session_.sql(f"SELECT IFF({x} % 2 = 0, 'snow', NULL)").collect()[0][0]

    @sproc
    def double_str_list_sp(session_: Session, x: str) -> List[str]:
        val = session_.sql(f"SELECT '{x}'").collect()[0][0]
        return [val, val]

    dt = datetime.datetime.strptime("2017-02-24 12:00:05.456", "%Y-%m-%d %H:%M:%S.%f")

    @sproc
    def return_datetime_sp(_: Session) -> datetime.datetime:
        return dt

    @sproc
    def first_element_sp(_: Session, x: List[str]) -> str:
        return x[0]

    @sproc
    def get_sp(_: Session, d: Dict[str, str], i: str) -> str:
        return d[i]

    assert add_sp(1, 2) == 3
    assert snow_sp(1) is None
    assert snow_sp(2) == "snow"
    assert double_str_list_sp("abc") == '[\n  "abc",\n  "abc"\n]'
    assert return_datetime_sp() == dt
    assert first_element_sp(["0", "'"]) == "0"
    assert get_sp({"0": "snow", "1": "flake"}, "0") == "snow"


def test_type_hint_no_change_after_registration(session):
    def add(session_: Session, x: int, y: int) -> int:
        return session_.sql(f"SELECT {x} + {y}").collect()[0][0]

    annotations = add.__annotations__
    session.sproc.register(add)
    assert annotations == add.__annotations__


def test_register_sp_from_file_type_hints(session, tmpdir):
    source = """
import datetime
import snowflake
from snowflake.snowpark import Session
from typing import Dict, List, Optional

def add(session: snowflake.snowpark.Session, x: int, y: int) -> int:
    return session.sql(f"select {x} + {y}").collect()[0][0]

def snow(session_: Session, x: int) -> Optional[str]:
    return session_.sql(f"SELECT IFF({x} % 2 = 0, 'snow', NULL)").collect()[0][0]

def double_str_list(session_: snowflake.snowpark.Session, x: str) -> List[str]:
    val = session_.sql(f"SELECT '{x}'").collect()[0][0]
    return [val, val]

dt = datetime.datetime.strptime("2017-02-24 12:00:05.456", "%Y-%m-%d %H:%M:%S.%f")

def return_datetime(_: Session) -> datetime.datetime:
    return dt
"""
    file_path = os.path.join(tmpdir, "register_from_file_type_hints.py")
    with open(file_path, "w") as f:
        f.write(source)

    add_sp = session.sproc.register_from_file(file_path, "add")
    snow_sp = session.sproc.register_from_file(file_path, "snow")
    double_str_list_sp = session.sproc.register_from_file(file_path, "double_str_list")
    return_datetime_sp = session.sproc.register_from_file(file_path, "return_datetime")

    assert add_sp(1, 2) == 3
    assert snow_sp(0) == "snow"
    assert snow_sp(1) is None
    assert double_str_list_sp("abc") == '[\n  "abc",\n  "abc"\n]'

    dt = datetime.datetime.strptime("2017-02-24 12:00:05.456", "%Y-%m-%d %H:%M:%S.%f")
    assert return_datetime_sp() == dt


def test_permanent_sp(session, db_parameters):
    stage_name = Utils.random_stage_name()
    sp_name = Utils.random_name_for_temp_object(TempObjectType.PROCEDURE)
    with Session.builder.configs(db_parameters).create() as new_session:
        new_session.add_packages("snowflake-snowpark-python")
        try:
            Utils.create_stage(session, stage_name, is_temporary=False)
            sproc(
                lambda session_, x, y: session_.sql(f"SELECT {x} + {y}").collect()[0][
                    0
                ],
                return_type=IntegerType(),
                input_types=[IntegerType(), IntegerType()],
                name=sp_name,
                is_permanent=True,
                stage_location=stage_name,
                session=new_session,
            )

            assert session.call(sp_name, 1, 2) == 3
            assert new_session.call(sp_name, 8, 9) == 17
        finally:
            session._run_query(f"drop function if exists {sp_name}(int, int)")
            Utils.drop_stage(session, stage_name)


def test_sp_negative(session):
    def f(_, x):
        return x

    empty_sp = sproc()
    with pytest.raises(TypeError) as ex_info:
        empty_sp(session)
    assert "Invalid function: not a function or callable" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        sproc(1, return_type=IntegerType())
    assert "Invalid function: not a function or callable" in str(ex_info)

    # if return_type is specified, it must be passed passed with keyword argument
    with pytest.raises(TypeError) as ex_info:
        sproc(f, IntegerType())
    assert "sproc() takes from 0 to 1 positional arguments but 2 were given" in str(
        ex_info
    )

    f_sp = sproc(f, return_type=IntegerType(), input_types=[IntegerType()])
    with pytest.raises(ValueError) as ex_info:
        f_sp("a", "")
    assert "Incorrect number of arguments passed to the stored procedure" in str(
        ex_info
    )

    with pytest.raises(ProgrammingError) as ex_info:
        session.sql("call f(1)").collect()
    assert "Unknown function" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        session.call("f", 1)
    assert "Unknown function" in str(ex_info)

    with pytest.raises(SnowparkInvalidObjectNameException) as ex_info:
        sproc(
            f,
            return_type=IntegerType(),
            input_types=[IntegerType()],
            name="invalid name",
        )
    assert "The object name 'invalid name' is invalid" in str(ex_info)

    # incorrect data type
    int_sp = sproc(
        lambda x: int(x), return_type=IntegerType(), input_types=[IntegerType()]
    )
    with pytest.raises(ProgrammingError) as ex_info:
        int_sp("x")
    assert "Numeric value" in str(ex_info) and "is not recognized" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        int_sp(None)
    assert "Python Interpreter Error" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:

        @sproc(IntegerType())
        def g(_, x):
            return x

    assert "Invalid function: not a function or callable" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:

        @sproc
        def add(_: Session, x: int, y: int):
            return x + y

    assert "The return type must be specified" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:

        @sproc
        def add(_: Session, x, y: int) -> int:
            return x + y

    assert (
        "Excluding session argument in stored procedure, "
        "the number of arguments (2) is different from "
        "the number of argument type hints (1)" in str(ex_info)
    )

    with pytest.raises(TypeError) as ex_info:

        @sproc
        def add(_: Session, x: int, y: Union[int, float]) -> Union[int, float]:
            return x + y

    assert "invalid type typing.Union[int, float]" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:

        @sproc
        def add(_: int, x: int, y: int) -> int:
            return x + y

    assert "The first argument of stored proc function should be Session" in str(
        ex_info
    )

    with pytest.raises(ValueError) as ex_info:

        @sproc(is_permanent=True)
        def add(_: Session, x: int, y: int) -> int:
            return x + y

    assert "name must be specified for permanent stored proc" in str(ex_info)

    with pytest.raises(ValueError) as ex_info:

        @sproc(is_permanent=True, name="sp")
        def add(_: Session, x: int, y: int) -> int:
            return x + y

    assert "stage_location must be specified for permanent stored proc" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:

        @sproc
        def add(
            _: Session, x: PandasSeries[int], y: PandasSeries[int]
        ) -> PandasSeries[int]:
            return x + y

    assert "Pandas stored procedure is not supported" in str(ex_info)


def test_add_import_negative(session, resources_path):
    test_files = TestFiles(resources_path)

    def plus4_then_mod5(_, x):
        from test.resources.test_sp_dir.test_sp_file import mod5

        return mod5(x + 4)

    for import_path in [
        None,
        "resources.test_sp_dir.test_sp_file",
        "test_sp_dir.test_sp_file",
        "test_sp_file",
    ]:
        session.add_import(test_files.test_sp_py_file, import_path)
        plus4_then_mod5_sp = sproc(
            plus4_then_mod5, return_type=IntegerType(), input_types=[IntegerType()]
        )
        with pytest.raises(ProgrammingError) as ex_info:
            plus4_then_mod5_sp(1)
        assert "No module named 'test.resources'" in str(ex_info)
    session.clear_imports()

    with pytest.raises(TypeError) as ex_info:
        sproc(
            plus4_then_mod5,
            return_type=IntegerType(),
            input_types=[IntegerType()],
            imports=[1],
        )
    assert (
        "stored-proc-level import can only be a file path (str) "
        "or a tuple of the file path (str) and the import path (str)" in str(ex_info)
    )


def test_sp_replace(session):
    # Register named sp and expect that it works.
    add_sp = session.sproc.register(
        lambda session_, x, y: session_.sql(f"SELECT {x} + {y}").collect()[0][0],
        name="test_sp_replace_add",
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
        replace=True,
    )
    assert add_sp(1, 2) == 3

    # Replace named sp with different one and expect that data is changed.
    add_sp = session.sproc.register(
        lambda session_, x, y: session_.sql(f"SELECT {x} + {y} + 1").collect()[0][0],
        name="test_sp_replace_add",
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
        replace=True,
    )
    assert add_sp(1, 2) == 4

    # Try to register sp without replacing and expect failure.
    with pytest.raises(ProgrammingError) as ex_info:
        add_sp = session.sproc.register(
            lambda session_, x, y: session_.sql(f"SELECT {x} + {y}").collect()[0][0],
            name="test_sp_replace_add",
            return_type=IntegerType(),
            input_types=[IntegerType(), IntegerType()],
        )
    assert "SQL compilation error" in str(ex_info)

    # Expect second sp version to still be there.
    assert add_sp(1, 2) == 4

    # Register via sproc() in functions.py and expect that it works.
    add_sp = sproc(
        lambda session_, x, y: session_.sql(f"SELECT {x} + {y}").collect()[0][0],
        name="test_sp_replace_add",
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
        replace=True,
    )
    assert add_sp(1, 2) == 3


def test_sp_parallel(session):
    for i in [1, 50, 99]:
        sproc(
            lambda session_, x, y: session_.sql(f"SELECT {x} + {y}").collect()[0][0],
            return_type=IntegerType(),
            input_types=[IntegerType(), IntegerType()],
            parallel=i,
        )

    with pytest.raises(ValueError) as ex_info:
        sproc(
            lambda session_, x, y: session_.sql(f"SELECT {x} + {y}").collect()[0][0],
            return_type=IntegerType(),
            input_types=[IntegerType(), IntegerType()],
            parallel=0,
        )
    assert "Supported values of parallel are from 1 to 99" in str(ex_info)

    with pytest.raises(ValueError) as ex_info:
        sproc(
            lambda session_, x, y: session_.sql(f"SELECT {x} + {y}").collect()[0][0],
            return_type=IntegerType(),
            input_types=[IntegerType(), IntegerType()],
            parallel=100,
        )
    assert "Supported values of parallel are from 1 to 99" in str(ex_info)


def test_describe_sp(session):
    def return1(session_: Session) -> str:
        return session_.sql("select '1'").collect()[0][0]

    return1_sp = session.sproc.register(return1)
    describe_res = session.sproc.describe(return1_sp).collect()
    assert [row[0] for row in describe_res] == [
        "signature",
        "returns",
        "language",
        "null handling",
        "volatility",
        "execute as",
        "body",
        "imports",
        "handler",
        "runtime_version",
        "packages",
        "installed_packages",
    ]
    for row in describe_res:
        if row[0] == "packages":
            assert "snowflake-snowpark-python" in row[1]
            break


def test_register_sp_no_commit(session):
    def plus1(_: Session, x: int) -> int:
        return x + 1

    temp_sp_name = Utils.random_name_for_temp_object(TempObjectType.PROCEDURE)
    perm_sp_name = Utils.random_name_for_temp_object(TempObjectType.PROCEDURE)

    try:
        # Test stored proc registration
        session.sql("begin").collect()
        session.sproc.register(func=plus1, name=temp_sp_name)
        assert Utils.is_active_transaction(session)
        session.sproc.register(
            func=plus1, name=perm_sp_name, stage_location=tmp_stage_name
        )
        assert Utils.is_active_transaction(session)

        # Test stored proc call
        assert session.call(temp_sp_name, 1) == 2
        assert session.call(perm_sp_name, 1) == 2
        assert Utils.is_active_transaction(session)

        session.sql("commit").collect()
        assert not Utils.is_active_transaction(session)
    finally:
        session._run_query(f"drop procedure if exists {temp_sp_name}(int)")
        session._run_query(f"drop procedure if exists {perm_sp_name}(int)")
