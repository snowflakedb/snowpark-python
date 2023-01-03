#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import datetime
import json
import logging
import math
import os
import sys
from typing import Callable
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

from snowflake.snowpark import Row, Session
from snowflake.snowpark._internal.utils import (
    unwrap_stage_location_single_quote,
    warning_dict,
)
from snowflake.snowpark.exceptions import (
    SnowparkInvalidObjectNameException,
    SnowparkSQLException,
)
from snowflake.snowpark.functions import call_udf, col, count_distinct, pandas_udf, udf
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    Geography,
    GeographyType,
    IntegerType,
    MapType,
    PandasDataFrame,
    PandasDataFrameType,
    PandasSeries,
    PandasSeriesType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    TimeType,
    Variant,
    VariantType,
)
from tests.utils import IS_IN_STORED_PROC, TempObjectType, TestData, TestFiles, Utils

pytestmark = pytest.mark.udf

tmp_stage_name = Utils.random_stage_name()


@pytest.fixture(scope="module", autouse=True)
def setup(session, resources_path):
    test_files = TestFiles(resources_path)
    Utils.create_stage(session, tmp_stage_name, is_temporary=True)
    Utils.upload_to_stage(
        session, tmp_stage_name, test_files.test_udf_py_file, compress=False
    )


def return1():
    return "1"


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="Source code extraction fails in stored proc"
)
def test_basic_non_local_udf(session):
    return1_udf = udf(return1, return_type=StringType())

    df = session.create_dataframe([[1, 2], [3, 4]]).to_df("a", "b")
    Utils.check_answer(df.select(return1_udf()).collect(), [Row("1"), Row("1")])


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
        lambda x, y: x**y,
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


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="Named temporary udf is not supported in stored proc"
)
def test_call_named_udf(session, temp_schema, db_parameters):
    mult_udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)
    session._run_query("drop function if exists test_mul(int, int)")
    udf(
        lambda x, y: x * y,
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
        name=mult_udf_name,
    )
    Utils.check_answer(
        session.sql(f"select {mult_udf_name}(13, 19)").collect(), [Row(13 * 19)]
    )

    df = session.create_dataframe([[1, 2], [3, 4]]).to_df("a", "b")
    Utils.check_answer(
        df.select(call_udf(mult_udf_name, col("a"), col("b"))).collect(),
        [
            Row(2),
            Row(12),
        ],
    )
    Utils.check_answer(
        df.select(
            call_udf(
                f"{session.get_fully_qualified_current_schema()}.{mult_udf_name}",
                6,
                7,
            )
        ).collect(),
        [Row(42), Row(42)],
    )

    # create a UDF when the session doesn't have a schema
    new_session = (
        Session.builder.configs(db_parameters)._remove_config("schema").create()
    )
    new_session.sql_simplifier_enabled = session.sql_simplifier_enabled
    try:
        assert not new_session.get_current_schema()
        add_udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)
        tmp_stage_name_in_temp_schema = (
            f"{temp_schema}.{Utils.random_name_for_temp_object(TempObjectType.STAGE)}"
        )
        new_session._run_query(f"create temp stage {tmp_stage_name_in_temp_schema}")
        full_udf_name = f"{temp_schema}.{add_udf_name}"
        new_session._run_query(f"drop function if exists {full_udf_name}(int, int)")
        new_session.udf.register(
            lambda x, y: x + y,
            return_type=IntegerType(),
            input_types=[IntegerType(), IntegerType()],
            name=[*temp_schema.split("."), add_udf_name],
            stage_location=unwrap_stage_location_single_quote(
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
                    f"show functions like '%{add_udf_name}%' in schema {temp_schema}"
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

        return f"{inner_func()}-{inner_func()}"

    def square(x):
        return x**2

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
    assert isinstance(add_udf.func, Callable)
    Utils.check_answer(
        df.select(add_udf("a", "b")).collect(),
        [
            Row(3),
            Row(7),
        ],
    )

    add_udf_with_statement_params = session.udf.register(
        lambda x, y: x + y,
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
        statement_params={"SF_PARTNER": "FAKE_PARTNER"},
    )
    assert isinstance(add_udf_with_statement_params.func, Callable)
    Utils.check_answer(
        df.select(add_udf_with_statement_params("a", "b")).collect(),
        [
            Row(3),
            Row(7),
        ],
    )


def test_register_udf_from_file(session, resources_path, tmpdir):
    test_files = TestFiles(resources_path)
    df = session.create_dataframe([[3, 4], [5, 6]]).to_df("a", "b")

    mod5_udf = session.udf.register_from_file(
        test_files.test_udf_py_file,
        "mod5",
        return_type=IntegerType(),
        input_types=[IntegerType()],
    )
    assert isinstance(mod5_udf.func, tuple)
    Utils.check_answer(
        df.select(mod5_udf("a"), mod5_udf("b")).collect(),
        [
            Row(3, 4),
            Row(0, 1),
        ],
    )

    mod5_pandas_udf = session.udf.register_from_file(
        test_files.test_pandas_udf_py_file,
        "pandas_apply_mod5",
        return_type=IntegerType(),
        input_types=[IntegerType()],
    )
    Utils.check_answer(
        df.select(mod5_pandas_udf("a"), mod5_pandas_udf("b")).collect(),
        [
            Row(3, 4),
            Row(0, 1),
        ],
    )

    # test zip file
    from zipfile import ZipFile

    zip_path = f"{tmpdir.join(os.path.basename(test_files.test_udf_py_file))}.zip"
    with ZipFile(zip_path, "w") as zf:
        zf.write(
            test_files.test_udf_py_file, os.path.basename(test_files.test_udf_py_file)
        )

    mod5_udf2 = session.udf.register_from_file(
        zip_path, "mod5", return_type=IntegerType(), input_types=[IntegerType()]
    )

    Utils.check_answer(
        df.select(mod5_udf2("a"), mod5_udf2("b")).collect(),
        [
            Row(3, 4),
            Row(0, 1),
        ],
    )

    # test a remote python file
    stage_file = f"@{tmp_stage_name}/{os.path.basename(test_files.test_udf_py_file)}"
    mod5_udf3 = session.udf.register_from_file(
        stage_file, "mod5", return_type=IntegerType(), input_types=[IntegerType()]
    )
    Utils.check_answer(
        df.select(mod5_udf3("a"), mod5_udf3("b")).collect(),
        [
            Row(3, 4),
            Row(0, 1),
        ],
    )

    mod5_udf3_with_statement_params = session.udf.register_from_file(
        stage_file,
        "mod5",
        return_type=IntegerType(),
        input_types=[IntegerType()],
        statement_params={"SF_PARTNER": "FAKE_PARTNER"},
    )
    Utils.check_answer(
        df.select(
            mod5_udf3_with_statement_params("a"), mod5_udf3_with_statement_params("b")
        ).collect(),
        [
            Row(3, 4),
            Row(0, 1),
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

        stage_file = (
            f"@{tmp_stage_name}/{os.path.basename(test_files.test_udf_py_file)}"
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


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="SNOW-609328: support caplog in SP regression test"
)
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
        with pytest.raises(SnowparkSQLException) as ex_info:
            df.select(plus4_then_mod5_udf("a")).collect(),
        assert "No module named" in ex_info.value.message

        session.add_import(test_files.test_udf_py_file, "test_udf_dir.test_udf_file")
        # with an empty list of udf-level imports
        # it will still fail even if we have session-level imports
        plus4_then_mod5_udf = udf(
            plus4_then_mod5,
            return_type=IntegerType(),
            input_types=[IntegerType()],
            imports=[],
        )
        with pytest.raises(SnowparkSQLException) as ex_info:
            df.select(plus4_then_mod5_udf("a")).collect(),
        assert "No module named" in ex_info.value.message

        # clean
        session.clear_imports()


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


def test_register_udf_from_file_type_hints(session, tmpdir):
    source = """
import datetime
from typing import Dict, List, Optional

def add(x: int, y: int) -> int:
        return x + y

def snow(x: int) -> Optional[str]:
    return "snow" if x % 2 else None

def double_str_list(x: str) -> List[str]:
    return [x, x]

dt = datetime.datetime.strptime("2017-02-24 12:00:05.456", "%Y-%m-%d %H:%M:%S.%f")

def return_datetime() -> datetime.datetime:
    return dt

def return_dict(v: dict) -> Dict[str, str]:
    return {str(k): f"{str(k)} {str(v)}" for k, v in v.items()}
"""
    file_path = os.path.join(tmpdir, "register_from_file_type_hints.py")
    with open(file_path, "w") as f:
        f.write(source)

    add_udf = session.udf.register_from_file(file_path, "add")
    snow_udf = session.udf.register_from_file(file_path, "snow")
    double_str_list_udf = session.udf.register_from_file(file_path, "double_str_list")
    return_datetime_udf = session.udf.register_from_file(file_path, "return_datetime")
    return_variant_dict_udf = session.udf.register_from_file(file_path, "return_dict")

    df = session.create_dataframe([[1, 4], [2, 3]]).to_df("a", "b")
    dt = datetime.datetime.strptime("2017-02-24 12:00:05.456", "%Y-%m-%d %H:%M:%S.%f")
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


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
def test_permanent_udf(session, db_parameters):
    stage_name = Utils.random_stage_name()
    udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)
    with Session.builder.configs(db_parameters).create() as new_session:
        new_session.sql_simplifier_enabled = session.sql_simplifier_enabled
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

    with pytest.raises(SnowparkSQLException) as ex_info:
        session.sql("select f(1)").collect()
    assert "Unknown function" in str(ex_info)

    with pytest.raises(SnowparkSQLException) as ex_info:
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
    with pytest.raises(SnowparkSQLException) as ex_info:
        df1.select(udf2("x")).collect()
    assert "Numeric value" in str(ex_info) and "is not recognized" in str(ex_info)
    df2 = session.create_dataframe([1, None]).to_df("x")
    with pytest.raises(SnowparkSQLException) as ex_info:
        df2.select(udf2("x")).collect()
    assert "Python Interpreter Error" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:

        @udf(IntegerType())
        def g(x):
            return x

    assert "Invalid function: not a function or callable" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:

        @udf
        def _(x: int, y: int):
            return x + y

    assert "The return type must be specified" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:

        @udf
        def _(x, y: int) -> int:
            return x + y

    assert (
        "the number of arguments (2) is different from "
        "the number of argument type hints (1)" in str(ex_info)
    )

    with pytest.raises(TypeError) as ex_info:

        @udf
        def _(x: int, y: Union[int, float]) -> Union[int, float]:
            return x + y

    assert "invalid type typing.Union[int, float]" in str(ex_info)

    with pytest.raises(ValueError) as ex_info:

        @udf(is_permanent=True)
        def _(x: int, y: int) -> int:
            return x + y

    assert "name must be specified for permanent udf" in str(ex_info)

    with pytest.raises(ValueError) as ex_info:

        @udf(is_permanent=True, name="udf")
        def _(x: int, y: int) -> int:
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
        with pytest.raises(SnowparkSQLException) as ex_info:
            df.select(plus4_then_mod5_udf("a")).collect()
        assert "No module named 'test.resources'" in ex_info.value.message
    session.clear_imports()

    with pytest.raises(TypeError) as ex_info:
        udf(
            plus4_then_mod5,
            return_type=IntegerType(),
            input_types=[IntegerType()],
            imports=[1],
        )
    assert (
        "udf-level import can only be a file path (str) "
        "or a tuple of the file path (str) and the import path (str)" in str(ex_info)
    )


def test_udf_variant_type(session):
    def variant_get_data_type(v):
        return str(type(v))

    variant_udf = udf(
        variant_get_data_type, return_type=StringType(), input_types=[VariantType()]
    )

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


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="Named temporary udf is not supported in stored proc"
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
    with pytest.raises(SnowparkSQLException) as ex_info:
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
    (not is_pandas_and_numpy_available) or IS_IN_STORED_PROC,
    reason="numpy and pandas are required",
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
            import pandas  # noqa: F401
        except ModuleNotFoundError:
            return False
        return True

    session.udf.register(
        is_pandas_available, name=udf_name, replace=True, packages=["numpy"]
    )
    Utils.check_answer(session.sql(f"select {udf_name}()"), [Row(False)])

    # with an empty list of udf-level packages
    # it will still fail even if we have session-level packages
    def is_numpy_available() -> bool:
        try:
            import numpy  # noqa: F401
        except ModuleNotFoundError:
            return False
        return True

    session.udf.register(is_numpy_available, name=udf_name, replace=True, packages=[])
    Utils.check_answer(session.sql(f"select {udf_name}()"), [Row(False)])

    session.clear_packages()

    # add module objects
    # but we can't register a udf with these versions
    # because the server might not have them
    resolved_packages = session._resolve_packages(
        [numpy, pandas, dateutil], validate_package=False
    )
    assert f"numpy=={numpy.__version__}" in resolved_packages
    assert f"pandas=={pandas.__version__}" in resolved_packages
    assert f"python-dateutil=={dateutil.__version__}" in resolved_packages

    session.clear_packages()


def test_add_packages_with_underscore(session):
    packages = ["spacy-model-en_core_web_sm", "typing_extensions"]
    count = (
        session.table("information_schema.packages")
        .where(col("package_name").in_(packages))
        .select(count_distinct("package_name"))
        .collect()[0][0]
    )
    if count != len(packages):
        pytest.skip("These packages with underscores are not available")

    udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

    @udf(name=udf_name, packages=packages)
    def check_if_package_installed() -> bool:
        try:
            import spacy
            import typing_extensions  # noqa: F401

            spacy.load("en_core_web_sm")
            return True
        except Exception:
            return False

    Utils.check_answer(session.sql(f"select {udf_name}()").collect(), [Row(True)])


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="Need certain version of datautil/pandas/numpy"
)
def test_add_packages_negative(session, caplog):
    with pytest.raises(ValueError) as ex_info:
        session.add_packages("python-dateutil****")
    assert "InvalidRequirement" in str(ex_info)

    with pytest.raises(ValueError) as ex_info:
        session.add_packages("dateutil")
    assert "Cannot add package dateutil" in str(ex_info)

    with pytest.raises(ValueError) as ex_info:
        with caplog.at_level(logging.WARNING):
            # using numpy version 1.16.6 here because using any other version raises a
            # ValueError for "non-existent python version in Snowflake" instead of
            # "package is already added".
            # In case this test fails in the future, choose a version of numpy which
            # is supportezd by Snowflake using query:
            #     select package_name, array_agg(version)
            #     from information_schema.packages
            #     where language='python' and package_name like 'numpy'
            #     group by package_name;
            session.add_packages("numpy", "numpy==1.16.6")
    assert "is already added" in str(ex_info)
    assert "which does not fit the criteria for the requirement" in caplog.text

    with pytest.raises(ValueError) as ex_info:
        session.remove_package("python-dateutil")
    assert "is not in the package list" in str(ex_info)

    with pytest.raises(ValueError) as ex_info:
        session.add_packages("xgboost==0.1.0")
    assert "xgboost==0.1.0 because it is not available in Snowflake." in str(ex_info)

    session.clear_packages()


@pytest.mark.skipif(
    (not is_pandas_and_numpy_available) or IS_IN_STORED_PROC,
    reason="numpy and pandas are required",
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


@pytest.mark.skipif(
    (not is_pandas_and_numpy_available) or IS_IN_STORED_PROC,
    reason="numpy and pandas are required",
)
def test_udf_describe(session):
    def get_numpy_pandas_version(s: str) -> str:
        return f"{s}{numpy.__version__}/{pandas.__version__}"

    get_numpy_pandas_version_udf = session.udf.register(
        get_numpy_pandas_version, packages=["numpy", "pandas"]
    )
    describe_res = session.udf.describe(get_numpy_pandas_version_udf).collect()
    assert [row[0] for row in describe_res] == [
        "signature",
        "returns",
        "language",
        "null handling",
        "volatility",
        "body",
        "imports",
        "handler",
        "runtime_version",
        "packages",
        "installed_packages",
    ]
    for row in describe_res:
        if row[0] == "packages":
            assert "numpy" in row[1] and "pandas" in row[1]
            break


@pytest.mark.skipif(not is_pandas_and_numpy_available, reason="pandas is required")
def test_basic_pandas_udf(session):
    def return1():
        return pandas.Series(["1"])

    @pandas_udf(
        return_type=PandasSeriesType(IntegerType()),
        input_types=[PandasDataFrameType([IntegerType(), IntegerType()])],
    )
    def add_one_df_pandas_udf(df):
        return df[0] + df[1] + 1

    def get_type_str(x):
        return pandas.Series([str(type(x))])

    return1_pandas_udf = udf(return1, return_type=PandasSeriesType(StringType()))
    add_series_pandas_udf = udf(
        lambda x, y: x + y,
        return_type=PandasSeriesType(IntegerType()),
        input_types=[PandasSeriesType(IntegerType()), PandasSeriesType(IntegerType())],
    )
    get_type_str_series_udf = udf(
        get_type_str,
        return_type=PandasSeriesType(StringType()),
        input_types=[PandasSeriesType(IntegerType())],
    )
    get_type_str_df_udf = pandas_udf(
        get_type_str,
        return_type=PandasSeriesType(StringType()),
        input_types=[PandasDataFrameType([IntegerType()])],
    )

    df = session.create_dataframe([[1, 2], [3, 4]]).to_df("a", "b")
    Utils.check_answer(df.select(return1_pandas_udf()), [Row("1"), Row("1")])
    Utils.check_answer(df.select(add_series_pandas_udf("a", "b")), [Row(3), Row(7)])
    Utils.check_answer(df.select(add_one_df_pandas_udf("a", "b")), [Row(4), Row(8)])
    Utils.check_answer(
        df.select(get_type_str_series_udf("a")),
        [
            Row("<class 'pandas.core.series.Series'>"),
            Row("<class 'pandas.core.series.Series'>"),
        ],
    )
    Utils.check_answer(
        df.select(get_type_str_df_udf("a")),
        [
            Row("<class 'pandas.core.frame.DataFrame'>"),
            Row("<class 'pandas.core.frame.DataFrame'>"),
        ],
    )


@pytest.mark.skipif(not is_pandas_and_numpy_available, reason="pandas is required")
def test_pandas_udf_type_hints(session):
    def return1() -> PandasSeries[str]:
        return pandas.Series(["1"])

    def return1_pandas_annotation() -> pandas.Series:
        return pandas.Series(["1"])

    def add_series(x: PandasSeries[int], y: PandasSeries[int]) -> PandasSeries[int]:
        return x + y

    def add_series_pandas_annotation(
        x: pandas.Series, y: pandas.Series
    ) -> pandas.Series:
        return x + y

    def add_one_df(df: PandasDataFrame[int, int]) -> PandasSeries[int]:
        return df[0] + df[1] + 1

    def add_one_df_pandas_annotation(df: pandas.DataFrame) -> pandas.Series:
        return df[0] + df[1] + 1

    return1_pandas_udf = udf(return1)
    return1_pandas_annotation_pandas_udf = udf(
        return1_pandas_annotation, return_type=StringType()
    )
    add_series_pandas_udf = udf(add_series)
    add_series_pandas_annotation_pandas_udf = udf(
        add_series_pandas_annotation,
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
    )
    add_one_df_pandas_udf = udf(add_one_df)
    add_one_df_pandas_annotation_pandas_udf = udf(
        add_one_df_pandas_annotation,
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
    )

    df = session.create_dataframe([[1, 2], [3, 4]]).to_df("a", "b")
    Utils.check_answer(df.select(return1_pandas_udf()), [Row("1"), Row("1")])
    Utils.check_answer(
        df.select(return1_pandas_annotation_pandas_udf()), [Row("1"), Row("1")]
    )
    Utils.check_answer(df.select(add_series_pandas_udf("a", "b")), [Row(3), Row(7)])
    Utils.check_answer(
        df.select(add_series_pandas_annotation_pandas_udf("a", "b")), [Row(3), Row(7)]
    )
    Utils.check_answer(df.select(add_one_df_pandas_udf("a", "b")), [Row(4), Row(8)])
    Utils.check_answer(
        df.select(add_one_df_pandas_annotation_pandas_udf("a", "b")), [Row(4), Row(8)]
    )


@pytest.mark.skipif(not is_pandas_and_numpy_available, reason="pandas is required")
@pytest.mark.parametrize(
    "_type, data, expected_types, expected_dtypes",
    [
        (
            IntegerType,
            [[4096]],
            (numpy.int16, int, numpy.short),
            ("int16", "object"),
        ),
        (IntegerType, [[1048576]], (numpy.int32, int, numpy.intc), ("int32", "object")),
        (
            IntegerType,
            [[8589934592]],
            (numpy.int64, int, numpy.int_, numpy.intp),
            ("int64", "object"),
        ),
        (FloatType, [[1.0]], (numpy.float64, float), ("float64",)),
        (StringType, [["1"]], (str,), ("string", "object")),
        (BooleanType, [[True]], (numpy.bool_, bool), ("boolean",)),
        (BinaryType, [[(1).to_bytes(1, byteorder="big")]], (bytes,), ("object",)),
        (
            DateType,
            [[datetime.date(2021, 12, 20)]],
            (pandas._libs.tslibs.timestamps.Timestamp,),
            ("datetime64[ns]",),
        ),
        (ArrayType, [[[1]]], (list,), ("object",)),
        (
            TimeType,
            [[datetime.time(1, 1, 1)]],
            (pandas._libs.tslibs.timedeltas.Timedelta,),
            ("timedelta64[ns]",),
        ),
        (
            TimestampType,
            [[datetime.datetime(2016, 3, 13, 5, tzinfo=datetime.timezone.utc)]],
            (pandas._libs.tslibs.timestamps.Timestamp,),
            ("datetime64[ns]",),
        ),
        (GeographyType, [["POINT(30 10)"]], (dict,), ("object",)),
        (MapType, [[{1: 2}]], (dict,), ("object",)),
    ],
)
def test_pandas_udf_input_types(session, _type, data, expected_types, expected_dtypes):
    expected_types = [str(x) for x in expected_types]
    expected_dtypes = [str(x) for x in expected_dtypes]
    schema = StructType([StructField("a", _type())])
    df = session.create_dataframe(data, schema=schema)

    def return_type_in_series(x):
        return x.apply(lambda val: f"{type(val)}/{x.dtype}")

    series_udf = udf(
        return_type_in_series,
        return_type=PandasSeriesType(StringType()),
        input_types=[PandasSeriesType(_type())],
    )
    returned_type, returned_dtype = (
        df.select(series_udf("a")).to_df("col1").collect()[0][0].split("/")
    )
    assert (
        returned_type in expected_types
    ), f"returned type is {returned_type} instead of {expected_types}"
    assert (
        returned_dtype in expected_dtypes
    ), f"returned dtype is {returned_dtype} instead of {expected_dtypes}"

    def return_type_in_dataframe(x):
        return x[0].apply(lambda val: f"{type(val)}/{x.dtypes[0]}")

    dataframe_udf = udf(
        return_type_in_dataframe,
        return_type=PandasSeriesType(StringType()),
        input_types=[PandasDataFrameType([_type()])],
    )
    returned_type, returned_dtype = (
        df.select(dataframe_udf("a")).to_df("col2").collect()[0][0].split("/")
    )
    assert (
        returned_type in expected_types
    ), f"returned type is {returned_type} instead of {expected_types}"
    assert (
        returned_dtype in expected_dtypes
    ), f"returned dtype is {returned_dtype} instead of {expected_dtypes}"


@pytest.mark.skipif(not is_pandas_and_numpy_available, reason="pandas is required")
def test_pandas_udf_input_variant(session):
    data = [
        [4096],
        [1.234],
        ["abc"],
        [True],
        [(1).to_bytes(1, byteorder="big")],
        [datetime.date(2021, 12, 20)],
        [[1]],
        [datetime.time(1, 1, 1)],
        [datetime.datetime(2016, 3, 13, 5, tzinfo=datetime.timezone.utc)],
        ["POINT(30 10)"],
        [{1: 2}],
    ]
    expected_types = [int, float, str, bool, str, str, list, str, str, str, dict]
    expected_results = [Row(f"{_type}/object") for _type in expected_types]
    schema = StructType([StructField("a", VariantType())])
    df = session.create_dataframe(data, schema=schema)

    def return_type_in_series(x):
        return x.apply(lambda val: f"{type(val)}/{x.dtype}")

    series_udf = udf(
        return_type_in_series,
        return_type=PandasSeriesType(StringType()),
        input_types=[PandasSeriesType(VariantType())],
    )
    rows = df.select(series_udf("a")).to_df("a").collect()
    Utils.check_answer(rows, expected_results)

    def return_type_in_dataframe(x):
        return x[0].apply(lambda val: f"{type(val)}/{x.dtypes[0]}")

    dataframe_udf = udf(
        return_type_in_dataframe,
        return_type=PandasSeriesType(StringType()),
        input_types=[PandasDataFrameType([VariantType()])],
    )

    rows = df.select(dataframe_udf("a")).to_df("a").collect()
    Utils.check_answer(rows, expected_results)


@pytest.mark.skipif(not is_pandas_and_numpy_available, reason="pandas is required")
@pytest.mark.parametrize(
    "_type, data, expected_types, expected_dtypes",
    [
        (IntegerType, [[4096]], (numpy.int16, int, numpy.short), ("int16", "object")),
        (
            IntegerType,
            [[1048576]],
            (numpy.int32, int, numpy.intc),
            ("int32", "object"),
        ),
        (
            IntegerType,
            [[8589934592]],
            (numpy.int64, int, numpy.int_, numpy.intp),
            ("int64", "object"),
        ),
        (FloatType, [[1.0]], (numpy.float64, float), ("float64",)),
        (StringType, [["1"]], (str,), ("object",)),
        (BooleanType, [[True]], (numpy.bool_, bool), ("bool",)),
        (BinaryType, [[(1).to_bytes(1, byteorder="big")]], (bytes,), ("object",)),
        (
            DateType,
            [[datetime.date(2021, 12, 20)]],
            (datetime.date,),
            ("object",),
        ),
        (ArrayType, [[[1]]], (list,), ("object",)),
        (
            TimeType,
            [[datetime.time(1, 1, 1)]],
            (datetime.time,),
            ("object",),  # TODO: should be timedelta64[ns]
        ),
        (
            TimestampType,
            [[datetime.datetime(2016, 3, 13, 5, tzinfo=datetime.timezone.utc)]],
            (pandas._libs.tslibs.timestamps.Timestamp,),
            ("datetime64[ns]",),
        ),
        (GeographyType, [["POINT(30 10)"]], (dict,), ("object",)),
        (MapType, [[{1: 2}]], (dict,), ("object",)),
    ],
)
def test_pandas_udf_return_types(session, _type, data, expected_types, expected_dtypes):
    """
    Note: See https://docs.snowflake.com/en/user-guide/python-connector-pandas.html#snowflake-to-pandas-data-mapping for
    some special cases, e.g. `Date` is mapped to `object`, `Variant` is mapped to `str`.
    """
    schema = StructType([StructField("a", _type())])
    df = session.create_dataframe(data, schema=schema)
    series_udf = udf(
        lambda x: x,
        return_type=PandasSeriesType(_type()),
        input_types=[PandasSeriesType(_type())],
    )
    result_df = df.select(series_udf("a")).to_pandas()
    result_val = result_df.iloc[0][0]
    if _type in (ArrayType, MapType, GeographyType):  # TODO: SNOW-573478
        result_val = json.loads(result_val)
    assert isinstance(
        result_val, expected_types
    ), f"returned type is {type(result_val)} instead of {expected_types}"
    assert (
        result_df.dtypes[0] in expected_dtypes
    ), f"returned dtype is {result_df.dtypes[0]} instead of {expected_dtypes}"


def test_pandas_udf_return_variant(session):
    schema = StructType([StructField("a", VariantType())])
    data = [
        [4096],
        [1.234],
        ["abc"],
        [True],
        [(1).to_bytes(1, byteorder="big")],
        [datetime.date(2021, 12, 20)],
        [[1]],
        [datetime.time(1, 1, 1)],
        [datetime.datetime(2016, 3, 13, 5, tzinfo=datetime.timezone.utc)],
        ["POINT(30 10)"],
        [{1: 2}],
    ]
    expected_types = [
        str,
        str,
        str,
        str,
        str,
        str,
        str,
        str,
        str,
        str,
        str,
    ]  # TODO: SNOW-573478
    df = session.create_dataframe(data, schema=schema)
    series_udf = udf(
        lambda x: x,
        return_type=PandasSeriesType(VariantType()),
        input_types=[PandasSeriesType(VariantType())],
    )
    temp = df.select(series_udf("a")).to_pandas()
    assert (
        temp.dtypes[0] == object
    ), f"returned dtype is {temp.dtypes[0]} instead of object"
    for i, row in temp.iterrows():
        assert isinstance(
            row[0], expected_types[i]
        ), f"returned type is {type(row[0])} instead of {expected_types[i]}"


@pytest.mark.skipif(not is_pandas_and_numpy_available, reason="pandas is required")
def test_pandas_udf_max_batch_size(session):
    def check_len(s):
        length = s[0].size if isinstance(s, pandas.DataFrame) else s.size
        return pandas.Series([1 if 0 < length <= 100 else -1] * length)

    max_batch_size = 100
    add_len_series_pandas_udf = udf(
        check_len,
        return_type=PandasSeriesType(IntegerType()),
        input_types=[PandasSeriesType(IntegerType())],
        max_batch_size=max_batch_size,
    )
    add_len_df_pandas_udf = udf(
        check_len,
        return_type=PandasSeriesType(IntegerType()),
        input_types=[PandasDataFrameType([IntegerType()])],
        max_batch_size=max_batch_size,
    )

    df = session.range(1000).to_df("a")
    Utils.check_answer(
        df.select(add_len_series_pandas_udf("a")), [Row(1) for _ in range(1000)]
    )
    Utils.check_answer(
        df.select(add_len_df_pandas_udf("a")), [Row(1) for _ in range(1000)]
    )


@pytest.mark.skipif(not is_pandas_and_numpy_available, reason="pandas is required")
def test_pandas_udf_negative(session):
    with pytest.raises(ValueError) as ex_info:
        pandas_udf(
            lambda x: x + 1, return_type=IntegerType(), input_types=[IntegerType()]
        )
    assert "You cannot create a non-vectorized UDF using pandas_udf()" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        pandas_udf(
            lambda df: df,
            return_type=PandasDataFrameType([IntegerType()]),
            input_types=[PandasDataFrameType([IntegerType()])],
        )
    assert "Invalid return type or input types for UDF" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        pandas_udf(
            lambda df: df,
            return_type=IntegerType(),
            input_types=[PandasDataFrameType([IntegerType()])],
        )
    assert "Invalid return type or input types for UDF" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        pandas_udf(
            lambda x, y: x + y,
            return_type=PandasSeriesType(IntegerType()),
            input_types=[IntegerType(), PandasSeriesType(IntegerType())],
        )
    assert "Invalid return type or input types for UDF" in str(ex_info)

    def add(x: pandas.Series, y: pandas.Series) -> pandas.Series:
        return x + y

    with pytest.raises(TypeError) as ex_info:
        pandas_udf(add)
    assert "The return type must be specified" in str(ex_info)


def test_register_udf_no_commit(session):
    def plus1(x: int) -> int:
        return x + 1

    temp_func_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)
    perm_func_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

    try:
        # Test function registration
        session.sql("begin").collect()
        session.udf.register(func=plus1, name=temp_func_name)
        assert Utils.is_active_transaction(session)
        session.udf.register(
            func=plus1, name=perm_func_name, stage_location=tmp_stage_name
        )
        assert Utils.is_active_transaction(session)

        # Test UDF call
        df = session.create_dataframe([1]).to_df(["a"])
        Utils.check_answer(df.select(call_udf(temp_func_name, col("a"))), [Row(2)])
        Utils.check_answer(df.select(call_udf(perm_func_name, col("a"))), [Row(2)])
        assert Utils.is_active_transaction(session)

        session.sql("commit").collect()
        assert not Utils.is_active_transaction(session)
    finally:
        session._run_query(f"drop function if exists {temp_func_name}(int)")
        session._run_query(f"drop function if exists {perm_func_name}(int)")


def test_udf_class_method(session):
    # Note that we never mention in the doc that we support registering UDF from a class method.
    # However, some users might still be interested in doing that.
    class UDFTest:
        a = 1

        def __init__(self, b) -> None:
            self.b = b

        @staticmethod
        def plus1(x: int) -> int:
            return x + 1

        @classmethod
        def plus_a(cls, x):
            return x + cls.a

        def plus_b(self, x):
            return x + self.b

        @property
        def double_b(self):
            return self.b * 2

    # test staticmethod
    plus1_udf = session.udf.register(UDFTest.plus1)
    Utils.check_answer(
        session.range(5).select(plus1_udf("id")),
        [Row(1), Row(2), Row(3), Row(4), Row(5)],
    )

    # test classmethod (type hint does not work here because it has an extra argument cls)
    plus_a_udf = session.udf.register(
        UDFTest.plus_a, return_type=IntegerType(), input_types=[IntegerType()]
    )
    Utils.check_answer(
        session.range(5).select(plus_a_udf("id")),
        [Row(1), Row(2), Row(3), Row(4), Row(5)],
    )

    # test the general method
    udf_test = UDFTest(b=-1)
    plus_b_udf = session.udf.register(
        udf_test.plus_b, return_type=IntegerType(), input_types=[IntegerType()]
    )
    Utils.check_answer(
        session.range(5).select(plus_b_udf("id")),
        [Row(-1), Row(0), Row(1), Row(2), Row(3)],
    )

    # test property
    with pytest.raises(TypeError) as ex_info:
        session.udf.register(udf_test.double_b, return_type=IntegerType())
    assert (
        "Invalid function: not a function or callable (__call__ is not defined)"
        in str(ex_info)
    )


def test_udf_pickle_failure(session):
    from weakref import WeakValueDictionary

    d = WeakValueDictionary()

    with pytest.raises(TypeError) as ex_info:
        session.udf.register(lambda: len(d), return_type=IntegerType())
    assert (
        "cannot pickle 'weakref' object: you might have to save the unpicklable object in the "
        "local environment first, add it to the UDF with session.add_import(), and read it from "
        "the UDF." in str(ex_info)
    )


def test_comment_in_udf_description(session):
    def return1():
        return "1"

    return1_udf = udf(return1, return_type=StringType())

    for row in session.udf.describe(return1_udf).collect():
        if row[0] == "body":
            assert (
                """\
# The following comment contains the UDF source code generated by snowpark-python for explanatory purposes.
# def return1():
#     return "1"
#
# func = return1\
"""
                in row[1]
            )
            break

    return1_udf = udf(return1, return_type=StringType(), source_code_display=False)

    for row in session.udf.describe(return1_udf).collect():
        if row[0] == "body":
            assert (
                """\
# The following comment contains the UDF source code generated by snowpark-python for explanatory purposes.
# def return1():
#     return "1"
#
# func = return1\
"""
                not in row[1]
            )
            break


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="SNOW-609328: support caplog in SP regression test"
)
def test_deprecate_call_udf_with_list(session, caplog):
    add_udf = session.udf.register(
        lambda x, y: x + y,
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
    )
    try:
        with caplog.at_level(logging.WARNING):
            add_udf(["a", "b"])
        assert (
            "Passing arguments to a UDF with a list or tuple is deprecated"
            in caplog.text
        )
    finally:
        warning_dict.clear()


def test_strict_udf(session):
    @udf(strict=True)
    def echo(num: int) -> int:
        if num is None:
            raise ValueError("num should not be None")
        return num

    Utils.check_answer(
        TestData.all_nulls(session).to_df(["a"]).select(echo("a")),
        [Row(None), Row(None), Row(None), Row(None)],
    )


def test_secure_udf(session):
    @udf(secure=True)
    def echo(num: int) -> int:
        return num

    Utils.check_answer(
        session.create_dataframe([[1, 2], [3, 4]]).to_df("a", "b").select(echo("a")),
        [Row(1), Row(3)],
    )
    ddl_sql = f"select get_ddl('function', '{echo.name}(int)')"
    assert "SECURE" in session.sql(ddl_sql).collect()[0][0]
