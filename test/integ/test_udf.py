#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import datetime
import math
import os
import sys

import pytest

try:
    import dateutil

    # six is the dependency of dateutil
    import six
    from dateutil.relativedelta import relativedelta

    is_dateutil_available = True
except ImportError:
    is_dateutil_available = False

from test.utils import TestFiles, Utils

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark.functions import call_udf, col, udf
from snowflake.snowpark.row import Row
from snowflake.snowpark.snowpark_client_exception import SnowparkClientException
from snowflake.snowpark.types.sf_types import (
    ArrayType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
)

tmp_stage_name = Utils.random_stage_name()


@pytest.fixture(scope="module")
def before_all(session_cnx):
    def do():
        with session_cnx() as session:
            Utils.create_stage(session, tmp_stage_name, is_temporary=True)

    return do


def test_basic_udf(session_cnx):
    def return1():
        return "1"

    def plus1(x):
        return x + 1

    def add(x, y):
        return x + y

    def int2str(x):
        return str(x)

    with session_cnx() as session:
        return1_udf = udf(return1)
        plus1_udf = udf(plus1, return_type=IntegerType(), input_types=[IntegerType()])
        add_udf = udf(
            add, return_type=IntegerType(), input_types=[IntegerType(), IntegerType()]
        )
        int2str_udf = udf(
            int2str, return_type=StringType(), input_types=[IntegerType()]
        )
        pow_udf = udf(
            lambda x, y: x ** y,
            return_type=DoubleType(),
            input_types=[IntegerType(), IntegerType()],
        )

        df = session.createDataFrame([[1, 2], [3, 4]]).toDF("a", "b")
        assert df.select(return1_udf()).collect() == [Row("1"), Row("1")]
        assert df.select(plus1_udf(col("a")), "a").collect() == [
            Row([2, 1]),
            Row([4, 3]),
        ]
        assert df.select(add_udf("a", "b")).collect() == [Row(3), Row(7)]
        assert df.select(int2str_udf("a")).collect() == [Row("1"), Row("3")]
        assert df.select(pow_udf(col("a"), "b"), "b").collect() == [
            Row([1.0, 2]),
            Row([81.0, 4]),
        ]


def test_call_named_udf(session_cnx):
    with session_cnx() as session:
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


def test_recursive_udf(session_cnx):
    def factorial(n):
        return 1 if n == 1 or n == 0 else n * factorial(n - 1)

    with session_cnx() as session:
        factorial_udf = udf(
            factorial, return_type=IntegerType(), input_types=[IntegerType()]
        )
        df = session.range(10).toDF("a")
        assert df.select(factorial_udf("a")).collect() == [
            Row(factorial(i)) for i in range(10)
        ]


def test_nested_udf(session_cnx):
    def outer_func():
        def inner_func():
            return "snow"

        return "{}-{}".format(inner_func(), inner_func())

    def square(x):
        return x ** 2

    def cube(x):
        return square(x) * x

    with session_cnx() as session:
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
            Row([1, 1]),
            Row([8, 4]),
        ]


def test_python_builtin_udf(session_cnx):
    def my_sqrt(x):
        return math.sqrt(x)

    with session_cnx() as session:
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


def test_decorator_udf(session_cnx):
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

    with session_cnx() as session:
        duplicate_list_elements_udf = udf(
            duplicate_list_elements,
            return_type=ArrayType(IntegerType()),
            input_types=[ArrayType(IntegerType())],
        )
        df = session.createDataFrame([[[1, 2], [2, 3]], [[3, 4], [4, 5]]]).toDF(
            "a", "b"
        )
        res = df.select(
            duplicate_list_elements_udf("a"), duplicate_list_elements_udf("b")
        ).collect()
        assert res == [
            Row(
                [
                    "[\n  1,\n  2,\n  1,\n  2,\n  1,\n  2,\n  1,\n  2\n]",
                    "[\n  2,\n  3,\n  2,\n  3,\n  2,\n  3,\n  2,\n  3\n]",
                ]
            ),
            Row(
                [
                    "[\n  3,\n  4,\n  3,\n  4,\n  3,\n  4,\n  3,\n  4\n]",
                    "[\n  4,\n  5,\n  4,\n  5,\n  4,\n  5,\n  4,\n  5\n]",
                ]
            ),
        ]


def test_annotation_syntax_udf(session_cnx):
    with session_cnx() as session:

        @udf(return_type=IntegerType(), input_types=[IntegerType(), IntegerType()])
        def add_udf(x, y):
            return x + y

        @udf
        def snow():
            return "snow"

        df = session.createDataFrame([[1, 2], [3, 4]]).toDF("a", "b")
        assert df.select(add_udf("a", "b"), snow()).collect() == [
            Row([3, "snow"]),
            Row([7, "snow"]),
        ]

        # add_udf is a UDF instead of a normal python function,
        # so it can't be simply called
        with pytest.raises(TypeError) as ex_info:
            add_udf(1, 2)
        assert "input must be Column, str, or list" in str(ex_info)


def test_add_imports_local_file(session_cnx, resources_path):
    test_files = TestFiles(resources_path)
    # This is a hack in the test such that we can just use `from test_udf import mod5`,
    # instead of `from test.resources.test_udf.test_udf import mod5`.
    # When create a Python UDF with imports `test_udf.py`, only `from test_udf import mod5`
    # is allowed to execute that code at the depickling time.
    sys.path.append(test_files.test_udf_directory)

    def plus4_then_mod5(x):
        from test_udf_file import mod5

        return mod5(x + 4)

    with session_cnx() as session:
        session.addImports(test_files.test_udf_py_file)
        plus4_then_mod5_udf = udf(
            plus4_then_mod5, return_type=IntegerType(), input_types=[IntegerType()]
        )

        df = session.range(-5, 5).toDF("a")
        assert df.select(plus4_then_mod5_udf("a")).collect() == [
            Row(plus4_then_mod5(i)) for i in range(-5, 5)
        ]

        # clean
        session.clearImports()


def test_add_imports_local_directory(session_cnx, resources_path):
    test_files = TestFiles(resources_path)
    sys.path.append(resources_path)

    def plus4_then_mod5(x):
        from test_udf_dir.test_udf_file import mod5

        return mod5(x + 4)

    with session_cnx() as session:
        session.addImports(test_files.test_udf_directory)
        plus4_then_mod5_udf = udf(
            plus4_then_mod5, return_type=IntegerType(), input_types=[IntegerType()]
        )

        df = session.range(-5, 5).toDF("a")
        assert df.select(plus4_then_mod5_udf("a")).collect() == [
            Row(plus4_then_mod5(i)) for i in range(-5, 5)
        ]

        # clean
        session.removeImports(test_files.test_udf_directory)


# TODO: SNOW-406036 unblock this test after the server side issue is fixed
@pytest.mark.skip(
    "skip the test due to the issue on the server side aboit finding .py file"
)
def test_add_imports_stage_file(session_cnx, resources_path):
    test_files = TestFiles(resources_path)
    sys.path.append(test_files.test_udf_directory)

    def plus4_then_mod5(x):
        from test_udf_file import mod5

        return mod5(x + 4)

    with session_cnx() as session:
        stage_file = "@{}.{}/test_udf_file.py".format(
            session.getFullyQualifiedCurrentSchema(), tmp_stage_name
        )
        Utils.upload_to_stage(
            session, tmp_stage_name, test_files.test_udf_py_file, compress=False
        )
        session.addImports(stage_file)
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
def test_add_imports_package(session_cnx):
    def plus_one_month(x):
        return x + relativedelta(month=1)

    d = datetime.date.today()
    libs = [os.path.dirname(dateutil.__file__), six.__file__]
    with session_cnx() as session:
        session.addImports(libs)
        df = session.createDataFrame([d]).toDF("a")
        plus_one_month_udf = udf(
            plus_one_month, return_type=DateType(), input_types=[DateType()]
        )
        assert df.select(plus_one_month_udf("a")).collect() == [Row(plus_one_month(d))]

        # clean
        session.clearImports()


def test_add_imports_duplicate(session_cnx, resources_path, caplog):
    test_files = TestFiles(resources_path)
    abs_path = test_files.test_udf_directory
    rel_path = os.path.relpath(abs_path)
    with session_cnx() as session:
        session.addImports(abs_path)
        session.addImports(f"{abs_path}/")
        session.addImports(rel_path)
        assert session.getImports() == [test_files.test_udf_directory]

        # skip upload the file because the calculated checksum is same
        session_stage = session.getSessionStage()
        session._resolve_imports(session_stage)
        session.addImports(abs_path)
        session._resolve_imports(session_stage)
        assert (
            f"{os.path.basename(abs_path)}.zip exists on {session_stage}, skipped"
            in caplog.text
        )

        session.removeImports(rel_path)
        assert len(session.getImports()) == 0


def test_udf_negative(session_cnx):
    def f(x):
        return x

    with session_cnx() as session:
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

        with pytest.raises(SnowparkClientException) as ex_info:
            udf(
                f,
                return_type=IntegerType(),
                input_types=[IntegerType()],
                name="invalid name",
            )
        assert "The object name 'invalid name' is invalid" in str(ex_info)

        # incorrect data type
        udf2 = udf(
            lambda x: int(x), return_type=IntegerType(), input_types=[IntegerType()]
        )
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


# TODO: add more after solving relative imports
def test_add_imports_negative(session_cnx):
    with session_cnx() as session:
        with pytest.raises(FileNotFoundError) as ex_info:
            session.addImports("file_not_found.py")
        assert "is not found" in str(ex_info)

        with pytest.raises(KeyError) as ex_info:
            session.removeImports("file_not_found.py")
        assert "is not found in the existing imports" in str(ex_info)
