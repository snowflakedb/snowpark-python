#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import math

import pytest

from snowflake.snowpark._internal.code_generation import generate_source_code

try:
    from dateutil.relativedelta import relativedelta

    is_dateutil_available = True
except ImportError:
    is_dateutil_available = False


def test_basic_udf():
    def add(x, y):
        return x + y

    assert (
        generate_source_code(add)
        == """\
# The following comment contains the UDF source code generated by snowpark-python for explanatory purposes.
# def add(x, y):
#     return x + y
#
# func = add\
"""
    )

    assert (
        generate_source_code(lambda x, y: x * y)
        == """\
# The following comment contains the UDF source code generated by snowpark-python for explanatory purposes.
# func = lambda x, y: x * y\
"""
    )


def test_non_local_or_global_vars():
    var = 123

    def udf():
        return var + 1

    assert (
        generate_source_code(udf)
        == """\
# The following comment contains the UDF source code generated by snowpark-python for explanatory purposes.
# var  # variable of type <class 'int'>
# def udf():
#     return var + 1
#
# func = udf\
"""
    )


def test_recursive_udf():
    def factorial(n):
        return 1 if n == 1 or n == 0 else n * factorial(n - 1)

    assert (
        generate_source_code(factorial)
        == """\
# The following comment contains the UDF source code generated by snowpark-python for explanatory purposes.
# def factorial(n):
#     return 1 if n == 1 or n == 0 else n * factorial(n - 1)
#
# func = factorial\
"""
    )


def test_nested_udf():
    def outer_func():
        def inner_func():
            return "snow"

        return f"{inner_func()}-{inner_func()}"

    def square(x):
        return x**2

    def cube(x):
        return square(x) * x

    assert (
        generate_source_code(outer_func)
        == """\
# The following comment contains the UDF source code generated by snowpark-python for explanatory purposes.
# def outer_func():
#     def inner_func():
#         return "snow"
#
#     return f"{inner_func()}-{inner_func()}"
#
# func = outer_func\
"""
    )

    assert (
        generate_source_code(cube)
        == """\
# The following comment contains the UDF source code generated by snowpark-python for explanatory purposes.
# def square(x):
#     return x ** 2
# def cube(x):
#     return square(x) * x
#
# func = cube\
"""
    )


def test_python_built_in():
    def my_sqrt(x):
        return math.sqrt(x)

    assert (
        generate_source_code(my_sqrt)
        == """\
# The following comment contains the UDF source code generated by snowpark-python for explanatory purposes.
# import math
# def my_sqrt(x):
#     return math.sqrt(x)
#
# func = my_sqrt\
"""
    )

    assert (
        generate_source_code(abs)
        == """\
# The following comment contains the UDF source code generated by snowpark-python for explanatory purposes.
# func = abs\
"""
    )

    assert (
        generate_source_code(math.sqrt)
        == """\
# The following comment contains the UDF source code generated by snowpark-python for explanatory purposes.
# import math
# func = math.sqrt\
"""
    )


def test_class_used_in_udf():
    import datetime

    class Foo:
        def bar(self):
            return 1

    def udf(a, b):
        return Foo().bar() + datetime.datetime.now().year

    assert (
        generate_source_code(udf)
        == """\
# The following comment contains the UDF source code generated by snowpark-python for explanatory purposes.
# import datetime
# class Foo:
#     def __init__(self):
#         pass
#
#     def bar(self):
#         return 1
# def udf(a, b):
#     return Foo().bar() + datetime.datetime.now().year
#
# func = udf\
"""
    )


@pytest.mark.skipif(not is_dateutil_available, reason="dateutil is required")
def test_add_import_package():
    def plus_one_month(x):
        return x + relativedelta(month=1)

    assert (
        generate_source_code(plus_one_month)
        == """\
# The following comment contains the UDF source code generated by snowpark-python for explanatory purposes.
# from dateutil.relativedelta import relativedelta
# def plus_one_month(x):
#     return x + relativedelta(month=1)
#
# func = plus_one_month\
"""
    )


def test_negative_udfs():
    # classmethod is not supported yet
    class Foo:
        def bar1(self):
            pass

        @classmethod
        def bar2(cls):
            pass

    # The source code for whole class should be used
    # Check  https://snowflakecomputing.atlassian.net/browse/SNOW-644984
    assert (
        generate_source_code(Foo.bar1)
        == """\
# The following comment contains the UDF source code generated by snowpark-python for explanatory purposes.
# def bar1(self):
#     pass
#
# func = bar1\
"""
    )
    assert generate_source_code(Foo.bar2) == ""
    assert generate_source_code(Foo().bar1) == ""
    assert generate_source_code(Foo().bar2) == ""
