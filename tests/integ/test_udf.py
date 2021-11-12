#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
import datetime
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

from typing import Any, Dict, List, Optional, Union

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark import Row, Session
from snowflake.snowpark.exceptions import SnowparkInvalidObjectNameException
from snowflake.snowpark.functions import call_udf, col, udf
from snowflake.snowpark.types import (
    ArrayType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    VariantType,
)
from tests.utils import TestData, TestFiles, Utils

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

    df = session.createDataFrame([[1, 2], [3, 4]]).toDF("a", "b")
    assert df.select(return1_udf()).collect() == [Row("1"), Row("1")]
    assert df.select(plus1_udf(col("a")), "a").collect() == [
        Row(2, 1),
        Row(4, 3),
    ]
    assert df.select(add_udf("a", "b")).collect() == [Row(3), Row(7)]
    assert df.select(int2str_udf("a")).collect() == [Row("1"), Row("3")]
    assert df.select(pow_udf(col("a"), "b"), "b").collect() == [
        Row(1.0, 2),
        Row(81.0, 4),
    ]


def test_call_named_udf(session):
    session._run_query("drop function if exists mul(int, int)")
    udf(
        lambda x, y: x * y,
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
        name="mul",
    )
    assert session.sql("select mul(13, 19)").collect() == [Row(13 * 19)]

    df = session.createDataFrame([[1, 2], [3, 4]]).toDF("a", "b")
    assert df.select(call_udf("mul", col("a"), col("b"))).collect() == [
        Row(2),
        Row(12),
    ]
    assert df.select(
        call_udf(f"{session.getFullyQualifiedCurrentSchema()}.mul", "a", "b")
    ).collect() == [Row(2), Row(12)]


def test_recursive_udf(session):
    def factorial(n):
        return 1 if n == 1 or n == 0 else n * factorial(n - 1)

    factorial_udf = udf(
        factorial, return_type=IntegerType(), input_types=[IntegerType()]
    )
    df = session.range(10).toDF("a")
    assert df.select(factorial_udf("a")).collect() == [
        Row(factorial(i)) for i in range(10)
    ]


def test_nested_udf(session):
    def outer_func():
        def inner_func():
            return "snow"

        return "{}-{}".format(inner_func(), inner_func())

    def square(x):
        return x ** 2

    def cube(x):
        return square(x) * x

    df = session.createDataFrame([1, 2]).toDF("a")
    outer_func_udf = udf(outer_func, return_type=StringType())
    assert df.select(outer_func_udf()).collect() == [
        Row("snow-snow"),
        Row("snow-snow"),
    ]

    # we don't need to register function square()
    cube_udf = udf(cube, return_type=IntegerType(), input_types=[IntegerType()])
    assert df.select(cube_udf("a")).collect() == [Row(1), Row(8)]

    # but we can still register function square()
    square_udf = udf(square, return_type=IntegerType(), input_types=[IntegerType()])
    assert df.select(cube_udf("a"), square_udf("a")).collect() == [
        Row(1, 1),
        Row(8, 4),
    ]


def test_python_builtin_udf(session):
    def my_sqrt(x):
        return math.sqrt(x)

    abs_udf = udf(abs, return_type=IntegerType(), input_types=[IntegerType()])
    sqrt_udf = udf(math.sqrt, return_type=DoubleType(), input_types=[DoubleType()])
    my_sqrt_udf = udf(my_sqrt, return_type=DoubleType(), input_types=[DoubleType()])

    df = session.range(-5, 5).toDF("a")
    assert df.select(abs_udf("a")).collect() == [Row(abs(i)) for i in range(-5, 5)]
    assert df.select(sqrt_udf(abs_udf("a"))).collect() == [
        Row(math.sqrt(abs(i))) for i in range(-5, 5)
    ]
    assert df.select(my_sqrt_udf(abs_udf("a"))).collect() == [
        Row(my_sqrt(abs(i))) for i in range(-5, 5)
    ]


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
    df = session.createDataFrame([[[1, 2], [2, 3]], [[3, 4], [4, 5]]]).toDF("a", "b")
    res = df.select(
        duplicate_list_elements_udf("a"), duplicate_list_elements_udf("b")
    ).collect()
    assert res == [
        Row(
            "[\n  1,\n  2,\n  1,\n  2,\n  1,\n  2,\n  1,\n  2\n]",
            "[\n  2,\n  3,\n  2,\n  3,\n  2,\n  3,\n  2,\n  3\n]",
        ),
        Row(
            "[\n  3,\n  4,\n  3,\n  4,\n  3,\n  4,\n  3,\n  4\n]",
            "[\n  4,\n  5,\n  4,\n  5,\n  4,\n  5,\n  4,\n  5\n]",
        ),
    ]


def test_annotation_syntax_udf(session):
    @udf(return_type=IntegerType(), input_types=[IntegerType(), IntegerType()])
    def add_udf(x, y):
        return x + y

    @udf(return_type=StringType())
    def snow():
        return "snow"

    df = session.createDataFrame([[1, 2], [3, 4]]).toDF("a", "b")
    assert df.select(add_udf("a", "b"), snow()).collect() == [
        Row(3, "snow"),
        Row(7, "snow"),
    ]

    # add_udf is a UDF instead of a normal python function,
    # so it can't be simply called
    with pytest.raises(TypeError) as ex_info:
        add_udf(1, 2)
    assert "input must be Column, str, or list" in str(ex_info)


def test_session_register_udf(session):
    df = session.createDataFrame([[1, 2], [3, 4]]).toDF("a", "b")
    add_udf = session.udf.register(
        lambda x, y: x + y,
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
    )
    assert df.select(add_udf("a", "b")).collect() == [
        Row(3),
        Row(7),
    ]


def test_add_imports_local_file(session, resources_path):
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

        df = session.range(-5, 5).toDF("a")

        session.addImport(
            test_files.test_udf_py_file, import_path="test_udf_dir.test_udf_file"
        )
        plus4_then_mod5_udf = udf(
            plus4_then_mod5, return_type=IntegerType(), input_types=[IntegerType()]
        )
        assert df.select(plus4_then_mod5_udf("a")).collect() == [
            Row(plus4_then_mod5(i)) for i in range(-5, 5)
        ]

        # if import_as argument changes, the checksum of the file will also change
        # and we will overwrite the file in the stage
        session.addImport(test_files.test_udf_py_file)
        plus4_then_mod5_direct_import_udf = udf(
            plus4_then_mod5_direct_import,
            return_type=IntegerType(),
            input_types=[IntegerType()],
        )
        assert df.select(plus4_then_mod5_direct_import_udf("a")).collect() == [
            Row(plus4_then_mod5_direct_import(i)) for i in range(-5, 5)
        ]

        # clean
        session.clearImports()


def test_add_imports_local_directory(session, resources_path):
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

        df = session.range(-5, 5).toDF("a")

        session.addImport(
            test_files.test_udf_directory, import_path="resources.test_udf_dir"
        )
        plus4_then_mod5_udf = udf(
            plus4_then_mod5, return_type=IntegerType(), input_types=[IntegerType()]
        )
        assert df.select(plus4_then_mod5_udf("a")).collect() == [
            Row(plus4_then_mod5(i)) for i in range(-5, 5)
        ]

        session.addImport(test_files.test_udf_directory)
        plus4_then_mod5_direct_import_udf = udf(
            plus4_then_mod5_direct_import,
            return_type=IntegerType(),
            input_types=[IntegerType()],
        )
        assert df.select(plus4_then_mod5_direct_import_udf("a")).collect() == [
            Row(plus4_then_mod5_direct_import(i)) for i in range(-5, 5)
        ]

        # clean
        session.clearImports()


def test_add_imports_stage_file(session, resources_path):
    test_files = TestFiles(resources_path)
    with patch.object(sys, "path", [*sys.path, test_files.test_udf_directory]):

        def plus4_then_mod5(x):
            from test_udf_file import mod5

            return mod5(x + 4)

        stage_file = "@{}.{}/test_udf_file.py".format(
            session.getFullyQualifiedCurrentSchema(), tmp_stage_name
        )
        Utils.upload_to_stage(
            session, tmp_stage_name, test_files.test_udf_py_file, compress=False
        )
        session.addImport(stage_file)
        plus4_then_mod5_udf = udf(
            plus4_then_mod5, return_type=IntegerType(), input_types=[IntegerType()]
        )

        df = session.range(-5, 5).toDF("a")
        assert df.select(plus4_then_mod5_udf("a")).collect() == [
            Row(plus4_then_mod5(i)) for i in range(-5, 5)
        ]

        # clean
        session.clearImports()


@pytest.mark.skipif(not is_dateutil_available, reason="dateutil is required")
def test_add_imports_package(session):
    def plus_one_month(x):
        return x + relativedelta(month=1)

    d = datetime.date.today()
    session.addImport(os.path.dirname(dateutil.__file__))
    session.addImport(six.__file__)
    df = session.createDataFrame([d]).toDF("a")
    plus_one_month_udf = udf(
        plus_one_month, return_type=DateType(), input_types=[DateType()]
    )
    assert df.select(plus_one_month_udf("a")).collect() == [Row(plus_one_month(d))]

    # clean
    session.clearImports()


def test_add_imports_duplicate(session, resources_path, caplog):
    test_files = TestFiles(resources_path)
    abs_path = test_files.test_udf_directory
    rel_path = os.path.relpath(abs_path)

    session.addImport(abs_path)
    session.addImport(f"{abs_path}/")
    session.addImport(rel_path)
    assert session.getImports() == [test_files.test_udf_directory]

    # skip upload the file because the calculated checksum is same
    session_stage = session.getSessionStage()
    session._resolve_imports(session_stage)
    session.addImport(abs_path)
    session._resolve_imports(session_stage)
    assert (
        f"{os.path.basename(abs_path)}.zip exists on {session_stage}, skipped"
        in caplog.text
    )

    session.removeImport(rel_path)
    assert len(session.getImports()) == 0


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
    def return_variant_dict_udf(v: Any) -> Dict[str, str]:
        return {str(k): f"{str(k)} {str(v)}" for k, v in v.items()}

    df = session.createDataFrame([[1, 4], [2, 3]]).toDF("a", "b")
    assert df.select(
        add_udf("a", "b"),
        snow_udf("a"),
        double_str_list_udf(snow_udf("b")),
        return_datetime_udf(),
    ).collect() == [
        Row(5, "snow", "[\n  null,\n  null\n]", dt),
        Row(5, None, '[\n  "snow",\n  "snow"\n]', dt),
    ]

    assert TestData.variant1(session).select(
        return_variant_dict_udf("obj1")
    ).collect() == [Row('{\n  "Tree": "Tree Pine"\n}')]


def test_permanent_udf(session, db_parameters):
    stage_name = Utils.random_stage_name()
    udf_name = Utils.random_name()
    new_session = Session.builder.configs(db_parameters).create()
    try:
        Utils.create_stage(session, stage_name, is_temporary=False)
        udf(
            lambda x, y: x + y,
            return_type=IntegerType(),
            input_types=[IntegerType(), IntegerType()],
            name=udf_name,
            is_permanent=True,
            stage_location=stage_name,
        )
        assert session.sql(f"select {udf_name}(8, 9)").collect() == [Row(17)]
        assert new_session.sql(f"select {udf_name}(8, 9)").collect() == [Row(17)]
    finally:
        session._run_query(f"drop function if exists {udf_name}(int, int)")
        Utils.drop_stage(session, stage_name)
        new_session.close()
        Session._set_active_session(session)


def test_udf_negative(session):
    def f(x):
        return x

    df1 = session.createDataFrame(["a", "b"]).toDF("x")

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
    assert "Numeric value 'a' is not recognized" in str(ex_info)
    df2 = session.createDataFrame([1, None]).toDF("x")
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


def test_add_imports_negative(session, resources_path):
    test_files = TestFiles(resources_path)

    with pytest.raises(FileNotFoundError) as ex_info:
        session.addImport("file_not_found.py")
    assert "is not found" in str(ex_info)

    with pytest.raises(KeyError) as ex_info:
        session.removeImport("file_not_found.py")
    assert "is not found in the existing imports" in str(ex_info)

    with pytest.raises(ValueError) as ex_info:
        session.addImport(
            test_files.test_udf_py_file, import_path="test_udf_dir.test_udf_file.py"
        )
    assert "import_path test_udf_dir.test_udf_file.py is invalid" in str(ex_info)

    def plus4_then_mod5(x):
        from test.resources.test_udf_dir.test_udf_file import mod5

        return mod5(x + 4)

    df = session.range(-5, 5).toDF("a")
    for import_path in [
        None,
        "resources.test_udf_dir.test_udf_file",
        "test_udf_dir.test_udf_file",
        "test_udf_file",
    ]:
        session.addImport(test_files.test_udf_py_file, import_path)
        plus4_then_mod5_udf = udf(
            plus4_then_mod5, return_type=IntegerType(), input_types=[IntegerType()]
        )
        with pytest.raises(ProgrammingError) as ex_info:
            df.select(plus4_then_mod5_udf("a")).collect()
        assert "No module named 'test.resources'" in str(ex_info)


def test_udf_variant_type(session):
    def variant_get_data_type(v):
        return str(type(v))

    variant_udf = udf(
        variant_get_data_type, return_type=StringType(), input_types=[VariantType()]
    )

    # TODO: SNOW-447601 change to the correct types after the server side has
    #  the complete mapping, for binary and time-related data
    assert TestData.variant1(session).select(variant_udf("bin1")).collect() == [
        Row("<class 'str'>")
    ]

    assert TestData.variant1(session).select(variant_udf("bool1")).collect() == [
        Row("<class 'bool'>")
    ]

    assert TestData.variant1(session).select(variant_udf("str1")).collect() == [
        Row("<class 'str'>")
    ]

    assert TestData.variant1(session).select(variant_udf("num1")).collect() == [
        Row("<class 'int'>")
    ]

    assert TestData.variant1(session).select(variant_udf("double1")).collect() == [
        Row("<class 'float'>")
    ]

    assert TestData.variant1(session).select(variant_udf("decimal1")).collect() == [
        Row("<class 'float'>")
    ]

    assert TestData.variant1(session).select(variant_udf("date1")).collect() == [
        Row("<class 'str'>")
    ]

    assert TestData.variant1(session).select(variant_udf("time1")).collect() == [
        Row("<class 'str'>")
    ]

    assert TestData.variant1(session).select(
        variant_udf("timestamp_ntz1")
    ).collect() == [Row("<class 'str'>")]

    assert TestData.variant1(session).select(
        variant_udf("timestamp_ltz1")
    ).collect() == [Row("<class 'str'>")]

    assert TestData.variant1(session).select(
        variant_udf("timestamp_tz1")
    ).collect() == [Row("<class 'str'>")]

    assert TestData.variant1(session).select(variant_udf("arr1")).collect() == [
        Row("<class 'list'>")
    ]

    assert TestData.variant1(session).select(variant_udf("obj1")).collect() == [
        Row("<class 'dict'>")
    ]

    # dynamic typing on one single column
    df = session.sql(
        "select parse_json(column1) as a from values"
        "('1'), ('1.1'), ('\"2\"'), ('true'), ('[1, 2, 3]'),"
        ' (\'{"a": "foo"}\')'
    )
    assert df.select(variant_udf("a")).collect() == [
        Row("<class 'int'>"),
        Row("<class 'float'>"),
        Row("<class 'str'>"),
        Row("<class 'bool'>"),
        Row("<class 'list'>"),
        Row("<class 'dict'>"),
    ]


def test_udf_replace(session):
    df = session.createDataFrame([[1, 2], [3, 4]]).toDF("a", "b")

    # Register named UDF and expect that it works.
    add_udf = session.udf.register(
        lambda x, y: x + y,
        name="test_udf_replace_add",
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
        replace=True,
    )
    assert df.select(add_udf("a", "b")).collect() == [
        Row(3),
        Row(7),
    ]

    # Replace named UDF with different one and expect that data is changed.
    add_udf = session.udf.register(
        lambda x, y: x + y + 1,
        name="test_udf_replace_add",
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
        replace=True,
    )
    assert df.select(add_udf("a", "b")).collect() == [
        Row(4),
        Row(8),
    ]

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
    assert df.select(add_udf("a", "b")).collect() == [
        Row(4),
        Row(8),
    ]

    # Register via udf() in functions.py and expect that it works.
    add_udf = udf(
        lambda x, y: x + y,
        name="test_udf_replace_add",
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
        replace=True,
    )
    assert df.select(add_udf("a", "b")).collect() == [
        Row(3),
        Row(7),
    ]
