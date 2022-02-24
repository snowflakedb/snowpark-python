#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import datetime
import logging
import math
import os
import sys
from unittest.mock import patch

import pytest

try:
    import dateutil

    # six is the dependency of dateutil
    import six
    from dateutil.relativedelta import relativedelta

    is_dateutil_available = True
except ImportError:
    is_dateutil_available = False


try:
    import numpy
    import pandas

    is_pandas_and_numpy_available = True
except ImportError:
    is_pandas_and_numpy_available = False

from typing import Dict, List, Optional, Union

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark import Row, Session
from snowflake.snowpark._internal.utils import Utils as InternalUtils
from snowflake.snowpark.exceptions import SnowparkInvalidObjectNameException
from snowflake.snowpark.functions import call_udf, col, udf
from snowflake.snowpark.types import (
    ArrayType,
    DateType,
    DoubleType,
    Geography,
    GeographyType,
    IntegerType,
    StringType,
    Variant,
    VariantType,
)
from tests.utils import TempObjectType, TestData, TestFiles, Utils

pytestmark = pytest.mark.udf

tmp_stage_name = Utils.random_stage_name()


@pytest.fixture(scope="module", autouse=True)
def setup(session):
    Utils.create_stage(session, tmp_stage_name, is_temporary=True)


def test_basic_udf(session):
    def return1():
        return "1"

    def plus1(x):
        return x + 1

    def add(x, y):
        return x + y

    def int2str(x):
        return str(x)

    return1_udf = udf(return1, return_type=StringType())
    plus1_udf = udf(plus1, return_type=IntegerType(), input_types=[IntegerType()])
    add_udf = udf(
        add, return_type=IntegerType(), input_types=[IntegerType(), IntegerType()]
    )
    int2str_udf = udf(int2str, return_type=StringType(), input_types=[IntegerType()])
    pow_udf = udf(
        lambda x, y: x ** y,
        return_type=DoubleType(),
        input_types=[IntegerType(), IntegerType()],
    )

    df = session.create_dataframe([[1, 2], [3, 4]]).to_df("a", "b")
    Utils.check_answer(df.select(return1_udf()).collect(), [Row("1"), Row("1")])
    Utils.check_answer(
        df.select(plus1_udf(col("a")), "a").collect(),
        [
            Row(2, 1),
            Row(4, 3),
        ],
    )
    Utils.check_answer(df.select(add_udf("a", "b")).collect(), [Row(3), Row(7)])
    Utils.check_answer(df.select(int2str_udf("a")).collect(), [Row("1"), Row("3")])
    Utils.check_answer(
        df.select(pow_udf(col("a"), "b"), "b"),
        [
            Row(1.0, 2),
            Row(81.0, 4),
        ],
    )


def test_call_named_udf(session, temp_schema, db_parameters):
    session._run_query("drop function if exists test_mul(int, int)")
    udf(
        lambda x, y: x * y,
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
        name="test_mul",
    )
    Utils.check_answer(session.sql("select test_mul(13, 19)").collect(), [Row(13 * 19)])

    df = session.create_dataframe([[1, 2], [3, 4]]).to_df("a", "b")
    Utils.check_answer(
        df.select(call_udf("test_mul", col("a"), col("b"))).collect(),
        [
            Row(2),
            Row(12),
        ],
    )
    Utils.check_answer(
        df.select(
            call_udf(
                f"{session.get_fully_qualified_current_schema()}.test_mul", "a", "b"
            )
        ).collect(),
        [Row(2), Row(12)],
    )

    # create a UDF when the session doesn't have a schema
    new_session = (
        Session.builder.configs(db_parameters)._remove_config("schema").create()
    )
    try:
        assert not new_session.getDefaultSchema()
        tmp_stage_name_in_temp_schema = (
            f"{temp_schema}.{Utils.random_name_for_temp_object(TempObjectType.STAGE)}"
        )
        new_session._run_query(f"create temp stage {tmp_stage_name_in_temp_schema}")
        full_udf_name = f"{temp_schema}.test_add"
        new_session._run_query(f"drop function if exists {full_udf_name}(int, int)")
        new_session.udf.register(
            lambda x, y: x + y,
            return_type=IntegerType(),
            input_types=[IntegerType(), IntegerType()],
            name=[*temp_schema.split("."), "test_add"],
            stage_location=InternalUtils.unwrap_stage_location_single_quote(
                tmp_stage_name_in_temp_schema
            ),
        )
        Utils.check_answer(
            new_session.sql(f"select {full_udf_name}(13, 19)").collect(), [Row(13 + 19)]
        )
        # oen result in the temp schema
        assert (
            len(
                new_session.sql(
                    f"show functions like '%test_add%' in schema {temp_schema}"
                ).collect()
            )
            == 1
        )
    finally:
        new_session.close()
        # restore active session


def test_recursive_udf(session):
    def factorial(n):
        return 1 if n == 1 or n == 0 else n * factorial(n - 1)

    factorial_udf = udf(
        factorial, return_type=IntegerType(), input_types=[IntegerType()]
    )
    df = session.range(10).to_df("a")
    Utils.check_answer(
        df.select(factorial_udf("a")).collect(), [Row(factorial(i)) for i in range(10)]
    )


def test_nested_udf(session):
    def outer_func():
        def inner_func():
            return "snow"

        return "{}-{}".format(inner_func(), inner_func())

    def square(x):
        return x ** 2

    def cube(x):
        return square(x) * x

    df = session.create_dataframe([1, 2]).to_df("a")
    outer_func_udf = udf(outer_func, return_type=StringType())
    Utils.check_answer(
        df.select(outer_func_udf()).collect(),
        [
            Row("snow-snow"),
            Row("snow-snow"),
        ],
    )

    # we don't need to register function square()
    cube_udf = udf(cube, return_type=IntegerType(), input_types=[IntegerType()])
    Utils.check_answer(df.select(cube_udf("a")).collect(), [Row(1), Row(8)])

    # but we can still register function square()
    square_udf = udf(square, return_type=IntegerType(), input_types=[IntegerType()])
    Utils.check_answer(
        df.select(cube_udf("a"), square_udf("a")).collect(),
        [
            Row(1, 1),
            Row(8, 4),
        ],
    )


def test_python_builtin_udf(session):
    def my_sqrt(x):
        return math.sqrt(x)

    abs_udf = udf(abs, return_type=IntegerType(), input_types=[IntegerType()])
    sqrt_udf = udf(math.sqrt, return_type=DoubleType(), input_types=[DoubleType()])
    my_sqrt_udf = udf(my_sqrt, return_type=DoubleType(), input_types=[DoubleType()])

    df = session.range(-5, 5).to_df("a")
    Utils.check_answer(
        df.select(abs_udf("a")).collect(), [Row(abs(i)) for i in range(-5, 5)]
    )
    Utils.check_answer(
        df.select(sqrt_udf(abs_udf("a"))).collect(),
        [Row(math.sqrt(abs(i))) for i in range(-5, 5)],
    )
    Utils.check_answer(
        df.select(my_sqrt_udf(abs_udf("a"))).collect(),
        [Row(my_sqrt(abs(i))) for i in range(-5, 5)],
    )


def test_decorator_udf(session):
    def decorator_do_twice(func):
        def wrapper(*args, **kwargs):
            l1 = func(*args, **kwargs)
            l2 = func(*args, **kwargs)
            return l1 + l2

        return wrapper

    @decorator_do_twice
    def duplicate_list_elements(x):
        y = x.copy()
        return x + y

    duplicate_list_elements_udf = udf(
        duplicate_list_elements,
        return_type=ArrayType(IntegerType()),
        input_types=[ArrayType(IntegerType())],
    )
    df = session.create_dataframe([[[1, 2], [2, 3]], [[3, 4], [4, 5]]]).to_df("a", "b")
    res = df.select(
        duplicate_list_elements_udf("a"), duplicate_list_elements_udf("b")
    ).collect()
    Utils.check_answer(
        res,
        [
            Row(
                "[\n  1,\n  2,\n  1,\n  2,\n  1,\n  2,\n  1,\n  2\n]",
                "[\n  2,\n  3,\n  2,\n  3,\n  2,\n  3,\n  2,\n  3\n]",
            ),
            Row(
                "[\n  3,\n  4,\n  3,\n  4,\n  3,\n  4,\n  3,\n  4\n]",
                "[\n  4,\n  5,\n  4,\n  5,\n  4,\n  5,\n  4,\n  5\n]",
            ),
        ],
    )


def test_annotation_syntax_udf(session):
    @udf(return_type=IntegerType(), input_types=[IntegerType(), IntegerType()])
    def add_udf(x, y):
        return x + y

    @udf(return_type=StringType())
    def snow():
        return "snow"

    df = session.create_dataframe([[1, 2], [3, 4]]).to_df("a", "b")
    Utils.check_answer(
        df.select(add_udf("a", "b"), snow()).collect(),
        [
            Row(3, "snow"),
            Row(7, "snow"),
        ],
    )

    # add_udf is a UDF instead of a normal python function,
    # so it can't be simply called
    with pytest.raises(TypeError) as ex_info:
        add_udf(1, 2)
    assert "must be Column, column name, or a list of them" in str(ex_info)


def test_session_register_udf(session):
    df = session.create_dataframe([[1, 2], [3, 4]]).to_df("a", "b")
    add_udf = session.udf.register(
        lambda x, y: x + y,
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
    )
    Utils.check_answer(
        df.select(add_udf("a", "b")).collect(),
        [
            Row(3),
            Row(7),
        ],
    )


def test_add_import_local_file(session, resources_path):
    test_files = TestFiles(resources_path)
    # This is a hack in the test such that we can just use `from test_udf import mod5`,
    # instead of `from test.resources.test_udf.test_udf import mod5`. Then we can test
    # `import_as` argument.
    with patch.object(
        sys, "path", [*sys.path, resources_path, test_files.test_udf_directory]
    ):

        def plus4_then_mod5(x):
            from test_udf_dir.test_udf_file import mod5

            return mod5(x + 4)

        def plus4_then_mod5_direct_import(x):
            from test_udf_file import mod5

            return mod5(x + 4)

        df = session.range(-5, 5).to_df("a")

        session.add_import(
            test_files.test_udf_py_file, import_path="test_udf_dir.test_udf_file"
        )
        plus4_then_mod5_udf = udf(
            plus4_then_mod5, return_type=IntegerType(), input_types=[IntegerType()]
        )
        Utils.check_answer(
            df.select(plus4_then_mod5_udf("a")).collect(),
            [Row(plus4_then_mod5(i)) for i in range(-5, 5)],
        )

        # if import_as argument changes, the checksum of the file will also change
        # and we will overwrite the file in the stage
        session.add_import(test_files.test_udf_py_file)
        plus4_then_mod5_direct_import_udf = udf(
            plus4_then_mod5_direct_import,
            return_type=IntegerType(),
            input_types=[IntegerType()],
        )
        Utils.check_answer(
            df.select(plus4_then_mod5_direct_import_udf("a")).collect(),
            [Row(plus4_then_mod5_direct_import(i)) for i in range(-5, 5)],
        )

        # clean
        session.clear_imports()


def test_add_import_local_directory(session, resources_path):
    test_files = TestFiles(resources_path)
    with patch.object(
        sys, "path", [*sys.path, resources_path, os.path.dirname(resources_path)]
    ):

        def plus4_then_mod5(x):
            from resources.test_udf_dir.test_udf_file import mod5

            return mod5(x + 4)

        def plus4_then_mod5_direct_import(x):
            from test_udf_dir.test_udf_file import mod5

            return mod5(x + 4)

        df = session.range(-5, 5).to_df("a")

        session.add_import(
            test_files.test_udf_directory, import_path="resources.test_udf_dir"
        )
        plus4_then_mod5_udf = udf(
            plus4_then_mod5, return_type=IntegerType(), input_types=[IntegerType()]
        )
        Utils.check_answer(
            df.select(plus4_then_mod5_udf("a")).collect(),
            [Row(plus4_then_mod5(i)) for i in range(-5, 5)],
        )

        session.add_import(test_files.test_udf_directory)
        plus4_then_mod5_direct_import_udf = udf(
            plus4_then_mod5_direct_import,
            return_type=IntegerType(),
            input_types=[IntegerType()],
        )
        Utils.check_answer(
            df.select(plus4_then_mod5_direct_import_udf("a")).collect(),
            [Row(plus4_then_mod5_direct_import(i)) for i in range(-5, 5)],
        )

        # clean
        session.clear_imports()


def test_add_import_stage_file(session, resources_path):
    test_files = TestFiles(resources_path)
    with patch.object(sys, "path", [*sys.path, test_files.test_udf_directory]):

        def plus4_then_mod5(x):
            from test_udf_file import mod5

            return mod5(x + 4)

        stage_file = "@{}.{}/test_udf_file.py".format(
            session.get_fully_qualified_current_schema(), tmp_stage_name
        )
        Utils.upload_to_stage(
            session, tmp_stage_name, test_files.test_udf_py_file, compress=False
        )
        session.add_import(stage_file)
        plus4_then_mod5_udf = udf(
            plus4_then_mod5, return_type=IntegerType(), input_types=[IntegerType()]
        )

        df = session.range(-5, 5).to_df("a")
        Utils.check_answer(
            df.select(plus4_then_mod5_udf("a")).collect(),
            [Row(plus4_then_mod5(i)) for i in range(-5, 5)],
        )

        # clean
        session.clear_imports()


@pytest.mark.skipif(not is_dateutil_available, reason="dateutil is required")
def test_add_import_package(session):
    def plus_one_month(x):
        return x + relativedelta(month=1)

    d = datetime.date.today()
    session.add_import(os.path.dirname(dateutil.__file__))
    session.add_import(six.__file__)
    df = session.create_dataframe([d]).to_df("a")
    plus_one_month_udf = udf(
        plus_one_month, return_type=DateType(), input_types=[DateType()]
    )
    Utils.check_answer(
        df.select(plus_one_month_udf("a")).collect(), [Row(plus_one_month(d))]
    )

    # clean
    session.clear_imports()


def test_add_import_duplicate(session, resources_path, caplog):
    test_files = TestFiles(resources_path)
    abs_path = test_files.test_udf_directory
    rel_path = os.path.relpath(abs_path)

    session.add_import(abs_path)
    session.add_import(f"{abs_path}/")
    session.add_import(rel_path)
    assert session.get_imports() == [test_files.test_udf_directory]

    # skip upload the file because the calculated checksum is same
    session_stage = session.get_session_stage()
    session._resolve_imports(session_stage)
    session.add_import(abs_path)
    session._resolve_imports(session_stage)
    assert (
        f"{os.path.basename(abs_path)}.zip exists on {session_stage}, skipped"
        in caplog.text
    )

    session.remove_import(rel_path)
    assert len(session.get_imports()) == 0


def test_udf_level_import(session, resources_path):
    test_files = TestFiles(resources_path)
    with patch.object(sys, "path", [*sys.path, resources_path]):

        def plus4_then_mod5(x):
            from test_udf_dir.test_udf_file import mod5

            return mod5(x + 4)

        df = session.range(-5, 5).to_df("a")

        # with udf-level imports
        plus4_then_mod5_udf = udf(
            plus4_then_mod5,
            return_type=IntegerType(),
            input_types=[IntegerType()],
            imports=[(test_files.test_udf_py_file, "test_udf_dir.test_udf_file")],
        )
        Utils.check_answer(
            df.select(plus4_then_mod5_udf("a")).collect(),
            [Row(plus4_then_mod5(i)) for i in range(-5, 5)],
        )

        # without udf-level imports
        plus4_then_mod5_udf = udf(
            plus4_then_mod5,
            return_type=IntegerType(),
            input_types=[IntegerType()],
        )
        with pytest.raises(ProgrammingError) as ex_info:
            df.select(plus4_then_mod5_udf("a")).collect(),
        assert "No module named" in str(ex_info)


def test_type_hints(session):
    @udf
    def add_udf(x: int, y: int) -> int:
        return x + y

    @udf
    def snow_udf(x: int) -> Optional[str]:
        return "snow" if x % 2 else None

    @udf
    def double_str_list_udf(x: str) -> List[str]:
        return [x, x]

    dt = datetime.datetime.strptime("2017-02-24 12:00:05.456", "%Y-%m-%d %H:%M:%S.%f")

    @udf
    def return_datetime_udf() -> datetime.datetime:
        return dt

    @udf
    def return_variant_dict_udf(v: Variant) -> Dict[str, str]:
        return {str(k): f"{str(k)} {str(v)}" for k, v in v.items()}

    @udf
    def return_geography_dict_udf(g: Geography) -> Dict[str, str]:
        return g

    df = session.create_dataframe([[1, 4], [2, 3]]).to_df("a", "b")
    Utils.check_answer(
        df.select(
            add_udf("a", "b"),
            snow_udf("a"),
            double_str_list_udf(snow_udf("b")),
            return_datetime_udf(),
        ).collect(),
        [
            Row(5, "snow", "[\n  null,\n  null\n]", dt),
            Row(5, None, '[\n  "snow",\n  "snow"\n]', dt),
        ],
    )

    Utils.check_answer(
        TestData.variant1(session).select(return_variant_dict_udf("obj1")).collect(),
        [Row('{\n  "Tree": "Tree Pine"\n}')],
    )

    Utils.check_answer(
        TestData.geography_type(session).select(return_geography_dict_udf("geo")),
        [Row('{\n  "coordinates": [\n    30,\n    10\n  ],\n  "type": "Point"\n}')],
    )


def test_type_hint_no_change_after_registration(session):
    def add(x: int, y: int) -> int:
        return x + y

    annotations = add.__annotations__
    session.udf.register(add)
    assert annotations == add.__annotations__


def test_permanent_udf(session, db_parameters):
    stage_name = Utils.random_stage_name()
    udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)
    with Session.builder.configs(db_parameters).create() as new_session:
        try:
            Utils.create_stage(session, stage_name, is_temporary=False)
            udf(
                lambda x, y: x + y,
                return_type=IntegerType(),
                input_types=[IntegerType(), IntegerType()],
                name=udf_name,
                is_permanent=True,
                stage_location=stage_name,
                session=new_session,
            )
            Utils.check_answer(
                session.sql(f"select {udf_name}(8, 9)").collect(), [Row(17)]
            )
            Utils.check_answer(
                new_session.sql(f"select {udf_name}(8, 9)").collect(), [Row(17)]
            )
        finally:
            session._run_query(f"drop function if exists {udf_name}(int, int)")
            Utils.drop_stage(session, stage_name)


def test_udf_negative(session):
    def f(x):
        return x

    df1 = session.create_dataframe(["a", "b"]).to_df("x")

    udf0 = udf()
    with pytest.raises(TypeError) as ex_info:
        df1.select(udf0("x")).collect()
    assert "Invalid function: not a function or callable" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        udf(1, return_type=IntegerType())
    assert "Invalid function: not a function or callable" in str(ex_info)

    # if return_type is specified, it must be passed passed with keyword argument
    with pytest.raises(TypeError) as ex_info:
        udf(f, IntegerType())
    assert "udf() takes from 0 to 1 positional arguments but 2 were given" in str(
        ex_info
    )

    udf1 = udf(f, return_type=IntegerType(), input_types=[IntegerType()])
    with pytest.raises(ValueError) as ex_info:
        udf1("a", "")
    assert "Incorrect number of arguments passed to the UDF" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        session.sql("select f(1)").collect()
    assert "Unknown function" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        df1.select(call_udf("f", "x")).collect()
    assert "Unknown function" in str(ex_info)

    with pytest.raises(SnowparkInvalidObjectNameException) as ex_info:
        udf(
            f,
            return_type=IntegerType(),
            input_types=[IntegerType()],
            name="invalid name",
        )
    assert "The object name 'invalid name' is invalid" in str(ex_info)

    # incorrect data type
    udf2 = udf(lambda x: int(x), return_type=IntegerType(), input_types=[IntegerType()])
    with pytest.raises(ProgrammingError) as ex_info:
        df1.select(udf2("x")).collect()
    assert "Numeric value" in str(ex_info) and "is not recognized" in str(ex_info)
    df2 = session.create_dataframe([1, None]).to_df("x")
    with pytest.raises(ProgrammingError) as ex_info:
        df2.select(udf2("x")).collect()
    assert "Python Interpreter Error" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:

        @udf(IntegerType())
        def g(x):
            return x

    assert "Invalid function: not a function or callable" in str(ex_info)

    with pytest.raises(AssertionError) as ex_info:

        @udf
        def add_udf(x: int, y: int):
            return x + y

    assert "The return type must be specified" in str(ex_info)

    with pytest.raises(AssertionError) as ex_info:

        @udf
        def add_udf(x, y: int) -> int:
            return x + y

    assert (
        "The number of arguments (2) is different from"
        " the number of argument type hints (1)" in str(ex_info)
    )

    with pytest.raises(TypeError) as ex_info:

        @udf
        def add_udf(x: int, y: Union[int, float]) -> Union[int, float]:
            return x + y

    assert "invalid type typing.Union[int, float]" in str(ex_info)

    with pytest.raises(ValueError) as ex_info:

        @udf(is_permanent=True)
        def add_udf(x: int, y: int) -> int:
            return x + y

    assert "name must be specified for permanent udf" in str(ex_info)

    with pytest.raises(ValueError) as ex_info:

        @udf(is_permanent=True, name="udf")
        def add_udf(x: int, y: int) -> int:
            return x + y

    assert "stage_location must be specified for permanent udf" in str(ex_info)


def test_add_import_negative(session, resources_path):
    test_files = TestFiles(resources_path)

    with pytest.raises(FileNotFoundError) as ex_info:
        session.add_import("file_not_found.py")
    assert "is not found" in str(ex_info)

    with pytest.raises(KeyError) as ex_info:
        session.remove_import("file_not_found.py")
    assert "is not found in the existing imports" in str(ex_info)

    with pytest.raises(ValueError) as ex_info:
        session.add_import(
            test_files.test_udf_py_file, import_path="test_udf_dir.test_udf_file.py"
        )
    assert "import_path test_udf_dir.test_udf_file.py is invalid" in str(ex_info)

    def plus4_then_mod5(x):
        from test.resources.test_udf_dir.test_udf_file import mod5

        return mod5(x + 4)

    df = session.range(-5, 5).to_df("a")
    for import_path in [
        None,
        "resources.test_udf_dir.test_udf_file",
        "test_udf_dir.test_udf_file",
        "test_udf_file",
    ]:
        session.add_import(test_files.test_udf_py_file, import_path)
        plus4_then_mod5_udf = udf(
            plus4_then_mod5, return_type=IntegerType(), input_types=[IntegerType()]
        )
        with pytest.raises(ProgrammingError) as ex_info:
            df.select(plus4_then_mod5_udf("a")).collect()
        assert "No module named 'test.resources'" in str(ex_info)
    session.clear_imports()

    with pytest.raises(TypeError) as ex_info:
        udf(
            plus4_then_mod5,
            return_type=IntegerType(),
            input_types=[IntegerType()],
            imports=[1],
        )
    assert (
        "UDF-level import can only be a file path (str) "
        "or a tuple of the file path (str) and the import path (str)" in str(ex_info)
    )


def test_udf_variant_type(session):
    def variant_get_data_type(v):
        return str(type(v))

    variant_udf = udf(
        variant_get_data_type, return_type=StringType(), input_types=[VariantType()]
    )

    # TODO: SNOW-447601 change to the correct types after the server side has
    #  the complete mapping, for binary and time-related data
    Utils.check_answer(
        TestData.variant1(session).select(variant_udf("bin1")).collect(),
        [Row("<class 'str'>")],
    )

    Utils.check_answer(
        TestData.variant1(session).select(variant_udf("bool1")).collect(),
        [Row("<class 'bool'>")],
    )

    Utils.check_answer(
        TestData.variant1(session).select(variant_udf("str1")).collect(),
        [Row("<class 'str'>")],
    )

    Utils.check_answer(
        TestData.variant1(session).select(variant_udf("num1")).collect(),
        [Row("<class 'int'>")],
    )

    Utils.check_answer(
        TestData.variant1(session).select(variant_udf("double1")).collect(),
        [Row("<class 'float'>")],
    )

    Utils.check_answer(
        TestData.variant1(session).select(variant_udf("decimal1")).collect(),
        [Row("<class 'float'>")],
    )

    Utils.check_answer(
        TestData.variant1(session).select(variant_udf("date1")).collect(),
        [Row("<class 'str'>")],
    )

    Utils.check_answer(
        TestData.variant1(session).select(variant_udf("time1")).collect(),
        [Row("<class 'str'>")],
    )

    Utils.check_answer(
        TestData.variant1(session).select(variant_udf("timestamp_ntz1")).collect(),
        [Row("<class 'str'>")],
    )

    Utils.check_answer(
        TestData.variant1(session).select(variant_udf("timestamp_ltz1")).collect(),
        [Row("<class 'str'>")],
    )

    Utils.check_answer(
        TestData.variant1(session).select(variant_udf("timestamp_tz1")).collect(),
        [Row("<class 'str'>")],
    )

    Utils.check_answer(
        TestData.variant1(session).select(variant_udf("arr1")).collect(),
        [Row("<class 'list'>")],
    )

    Utils.check_answer(
        TestData.variant1(session).select(variant_udf("obj1")).collect(),
        [Row("<class 'dict'>")],
    )

    # dynamic typing on one single column
    df = session.sql(
        "select parse_json(column1) as a from values"
        "('1'), ('1.1'), ('\"2\"'), ('true'), ('[1, 2, 3]'),"
        ' (\'{"a": "foo"}\')'
    )
    Utils.check_answer(
        df.select(variant_udf("a")).collect(),
        [
            Row("<class 'int'>"),
            Row("<class 'float'>"),
            Row("<class 'str'>"),
            Row("<class 'bool'>"),
            Row("<class 'list'>"),
            Row("<class 'dict'>"),
        ],
    )


def test_udf_geography_type(session):
    def get_type(g):
        return str(type(g))

    geography_udf = udf(
        get_type, return_type=StringType(), input_types=[GeographyType()]
    )

    Utils.check_answer(
        TestData.geography_type(session).select(geography_udf(col("geo"))).collect(),
        [Row("<class 'dict'>")],
    )


def test_udf_replace(session):
    df = session.create_dataframe([[1, 2], [3, 4]]).to_df("a", "b")

    # Register named UDF and expect that it works.
    add_udf = session.udf.register(
        lambda x, y: x + y,
        name="test_udf_replace_add",
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
        replace=True,
    )
    Utils.check_answer(
        df.select(add_udf("a", "b")).collect(),
        [
            Row(3),
            Row(7),
        ],
    )

    # Replace named UDF with different one and expect that data is changed.
    add_udf = session.udf.register(
        lambda x, y: x + y + 1,
        name="test_udf_replace_add",
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
        replace=True,
    )
    Utils.check_answer(
        df.select(add_udf("a", "b")).collect(),
        [
            Row(4),
            Row(8),
        ],
    )

    # Try to register UDF without replacing and expect failure.
    with pytest.raises(ProgrammingError) as ex_info:
        add_udf = session.udf.register(
            lambda x, y: x + y,
            name="test_udf_replace_add",
            return_type=IntegerType(),
            input_types=[IntegerType(), IntegerType()],
            replace=False,
        )
    assert "SQL compilation error" in str(ex_info)

    # Expect second UDF version to still be there.
    Utils.check_answer(
        df.select(add_udf("a", "b")).collect(),
        [
            Row(4),
            Row(8),
        ],
    )

    # Register via udf() in functions.py and expect that it works.
    add_udf = udf(
        lambda x, y: x + y,
        name="test_udf_replace_add",
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
        replace=True,
    )
    Utils.check_answer(
        df.select(add_udf("a", "b")).collect(),
        [
            Row(3),
            Row(7),
        ],
    )


def test_udf_parallel(session):
    for i in [1, 50, 99]:
        udf(
            lambda x, y: x + y,
            return_type=IntegerType(),
            input_types=[IntegerType(), IntegerType()],
            parallel=i,
        )

    with pytest.raises(ValueError) as ex_info:
        udf(
            lambda x, y: x + y,
            return_type=IntegerType(),
            input_types=[IntegerType(), IntegerType()],
            parallel=0,
        )
    assert "Supported values of parallel are from 1 to 99" in str(ex_info)

    with pytest.raises(ValueError) as ex_info:
        udf(
            lambda x, y: x + y,
            return_type=IntegerType(),
            input_types=[IntegerType(), IntegerType()],
            parallel=100,
        )
    assert "Supported values of parallel are from 1 to 99" in str(ex_info)


@pytest.mark.skipif(
    not is_pandas_and_numpy_available, reason="numpy and pandas are required"
)
def test_add_packages(session):
    session.add_packages(["numpy==1.20.1", "pandas==1.3.5", "pandas==1.3.5"])
    assert session.get_packages() == {
        "numpy": "numpy==1.20.1",
        "pandas": "pandas==1.3.5",
    }

    # dateutil is a dependency of pandas
    def get_numpy_pandas_dateutil_version() -> str:
        return f"{numpy.__version__}/{pandas.__version__}/{dateutil.__version__}"

    udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)
    session.udf.register(get_numpy_pandas_dateutil_version, name=udf_name)
    # don't need to check the version of dateutil, as it can be changed on the server side
    assert (
        session.sql(f"select {udf_name}()").collect()[0][0].startswith("1.20.1/1.3.5")
    )

    # only add numpy, which will overwrite the previously added packages
    # so pandas will not be available on the server side
    def is_pandas_available() -> bool:
        try:
            import pandas
        except ModuleNotFoundError:
            return False
        return True

    session.udf.register(
        is_pandas_available, name=udf_name, replace=True, packages=["numpy"]
    )
    Utils.check_answer(session.sql(f"select {udf_name}()"), [Row(False)])
    session.clear_packages()

    # add module objects
    # but we can't register a udf with these versions
    # because the server might not have them
    session.add_packages(numpy, pandas, dateutil)
    assert session.get_packages() == {
        "numpy": f"numpy=={numpy.__version__}",
        "pandas": f"pandas=={pandas.__version__}",
        "python-dateutil": f"python-dateutil=={dateutil.__version__}",
    }

    session.clear_packages()


def test_add_packages_negative(session, caplog):
    with pytest.raises(ValueError) as ex_info:
        session.add_packages("python-dateutil****")
    assert "InvalidRequirement" in str(ex_info)

    with pytest.raises(ValueError) as ex_info:
        session.add_packages("dateutil")
    assert "it is not available in Snowflake. Check information_schema.packages" in str(
        ex_info
    )

    with pytest.raises(ValueError) as ex_info:
        with caplog.at_level(logging.WARNING):
            session.add_packages("numpy", "numpy==0.1.0")
    assert "is already added" in str(ex_info)
    assert "which does not fit the criteria for the requirement" in caplog.text

    with pytest.raises(ValueError) as ex_info:
        session.remove_package("python-dateutil")
    assert "is not in the package list" in str(ex_info)

    session.clear_packages()


@pytest.mark.skipif(
    not is_pandas_and_numpy_available, reason="numpy and pandas are required"
)
def test_add_requirements(session, resources_path):
    test_files = TestFiles(resources_path)
    session.clear_packages()

    session.add_requirements(test_files.test_requirements_file)
    assert session.get_packages() == {
        "numpy": "numpy==1.21.2",
        "pandas": "pandas==1.3.5",
    }

    udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

    @udf(name=udf_name)
    def get_numpy_pandas_version() -> str:
        return f"{numpy.__version__}/{pandas.__version__}"

    Utils.check_answer(session.sql(f"select {udf_name}()"), [Row("1.21.2/1.3.5")])

    session.clear_packages()
