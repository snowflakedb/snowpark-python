#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import decimal  # noqa: F401
import json
import logging
import math
import os
import re
import sys
from textwrap import dedent
from typing import Callable

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

    from snowflake.snowpark.types import (
        PandasDataFrame,
        PandasDataFrameType,
        PandasSeries,
        PandasSeriesType,
    )

    is_pandas_available = True
    numpy_funcs = [numpy.min, numpy.sqrt, numpy.tan, numpy.sum, numpy.median]
except ImportError:
    is_pandas_available = False
    numpy_funcs = []

from typing import Dict, List, Optional, Union

from snowflake.connector.version import VERSION as SNOWFLAKE_CONNECTOR_VERSION
from snowflake.snowpark import Row, Session
from snowflake.snowpark._internal.utils import unwrap_stage_location_single_quote
from snowflake.snowpark.exceptions import (
    SnowparkInvalidObjectNameException,
    SnowparkSQLException,
)
from snowflake.snowpark.functions import call_udf, col, lit, pandas_udf, parse_json, udf
from snowflake.snowpark.types import (
    LTZ,
    NTZ,
    TZ,
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    Geography,
    GeographyType,
    Geometry,
    GeometryType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    Timestamp,
    TimestampTimeZone,
    TimestampType,
    TimeType,
    Variant,
    VariantType,
)
from tests.utils import (
    IS_IN_STORED_PROC,
    IS_NOT_ON_GITHUB,
    TempObjectType,
    TestData,
    TestFiles,
    Utils,
)

pytestmark = [
    pytest.mark.udf,
]

tmp_stage_name = Utils.random_stage_name()


@pytest.fixture(scope="module", autouse=True)
def setup(session, resources_path, local_testing_mode):
    test_files = TestFiles(resources_path)
    if not local_testing_mode:
        Utils.create_stage(session, tmp_stage_name, is_temporary=True)
    Utils.upload_to_stage(
        session, tmp_stage_name, test_files.test_udf_py_file, compress=False
    )


@pytest.fixture(autouse=True)
def clean_up(session):
    session.clear_packages()
    session.clear_imports()
    session._runtime_version_from_requirement = None
    yield


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
    plus1_udf = udf(
        plus1, return_type=IntegerType(), input_types=[IntegerType()], immutable=True
    )
    add_udf = udf(
        add,
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
        immutable=True,
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
def test_call_named_udf(session, temp_schema, db_parameters, local_testing_mode):
    mult_udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)
    if not local_testing_mode:
        session._run_query("drop function if exists test_mul(int, int)")
    udf(
        lambda x, y: x * y,
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
        name=mult_udf_name,
    )
    if not local_testing_mode:
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
                session.get_fully_qualified_name_if_possible(mult_udf_name),
                6,
                7,
            )
        ).collect(),
        [Row(42), Row(42)],
    )

    if not local_testing_mode:  # this test case does not apply to Local Testing
        # create a UDF when the session doesn't have a schema
        new_session = (
            Session.builder.configs(db_parameters)._remove_config("schema").create()
        )
        new_session.sql_simplifier_enabled = session.sql_simplifier_enabled
        try:
            assert not new_session.get_current_schema()
            add_udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)
            tmp_stage_name_in_temp_schema = f"{temp_schema}.{Utils.random_name_for_temp_object(TempObjectType.STAGE)}"
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
                is_permanent=True,
            )
            Utils.check_answer(
                new_session.sql(f"select {full_udf_name}(13, 19)").collect(),
                [Row(13 + 19)],
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


def test_session_register_udf(session, local_testing_mode):
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
    # Query tags not supported in local testing.
    if not local_testing_mode:
        query_tag = f"QUERY_TAG_{Utils.random_alphanumeric_str(10)}"
        add_udf_with_statement_params = session.udf.register(
            lambda x, y: x + y,
            return_type=IntegerType(),
            input_types=[IntegerType(), IntegerType()],
            statement_params={"QUERY_TAG": query_tag},
        )
        assert isinstance(add_udf_with_statement_params.func, Callable)
        Utils.check_answer(
            df.select(add_udf_with_statement_params("a", "b")).collect(),
            [
                Row(3),
                Row(7),
            ],
        )
        Utils.assert_executed_with_query_tag(session, query_tag)


def test_register_udf_from_file(session, resources_path):
    test_files = TestFiles(resources_path)
    df = session.create_dataframe([[3, 4], [5, 6]]).to_df("a", "b")

    mod5_udf = session.udf.register_from_file(
        test_files.test_udf_py_file,
        "mod5",
        return_type=IntegerType(),
        input_types=[IntegerType()],
        immutable=True,
    )
    assert isinstance(mod5_udf.func, tuple)
    Utils.check_answer(
        df.select(mod5_udf("a"), mod5_udf("b")).collect(),
        [
            Row(3, 4),
            Row(0, 1),
        ],
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Vectorized UDF is not supported in Local Testing",
)
@pytest.mark.skipif(
    not is_pandas_available, reason="pandas is required to register vectorized UDFs"
)
def test_register_vectorized_udf_from_file(session, resources_path):
    test_files = TestFiles(resources_path)
    df = session.create_dataframe([[3, 4], [5, 6]]).to_df("a", "b")

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


def test_register_udf_from_zip_file(session, resources_path, tmpdir):
    test_files = TestFiles(resources_path)
    df = session.create_dataframe([[3, 4], [5, 6]]).to_df("a", "b")

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


def test_register_udf_from_remote_file(session, resources_path):
    test_files = TestFiles(resources_path)
    df = session.create_dataframe([[3, 4], [5, 6]]).to_df("a", "b")

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


def test_register_udf_from_remote_file_with_statement_params(
    session, resources_path, local_testing_mode
):
    test_files = TestFiles(resources_path)
    query_tag = f"QUERY_TAG_{Utils.random_alphanumeric_str(10)}"
    df = session.create_dataframe([[3, 4], [5, 6]]).to_df("a", "b")

    stage_file = f"@{tmp_stage_name}/{os.path.basename(test_files.test_udf_py_file)}"
    mod5_udf3_with_statement_params = session.udf.register_from_file(
        stage_file,
        "mod5",
        return_type=IntegerType(),
        input_types=[IntegerType()],
        statement_params={"QUERY_TAG": query_tag},
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
    Utils.assert_executed_with_query_tag(session, query_tag, local_testing_mode)


@pytest.mark.xfail(reason="SNOW-799761 flaky test", strict=False)
@pytest.mark.skipif(
    SNOWFLAKE_CONNECTOR_VERSION < (3, 0, 3, None),
    reason="skip_upload_on_content_match is ignored by connector if connector version is older than 3.0.3",
)
def test_register_from_file_with_skip_upload(session, resources_path, caplog):
    test_files = TestFiles(resources_path)
    stage_name = Utils.random_stage_name()
    udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)
    try:
        Utils.create_stage(session, stage_name, is_temporary=False)
        # register first time
        session.udf.register_from_file(
            test_files.test_udf_py_file,
            "mod5",
            name=udf_name,
            return_type=IntegerType(),
            input_types=[IntegerType()],
            stage_location=stage_name,
            is_permanent=True,
            replace=True,
            skip_upload_on_content_match=False,
        )

        # test skip_upload_on_content_match
        with caplog.at_level(
            logging.DEBUG, logger="snowflake.connector.storage_client"
        ):
            session.udf.register_from_file(
                test_files.test_udf_py_file,
                "mod5",
                name=udf_name,
                return_type=IntegerType(),
                input_types=[IntegerType()],
                stage_location=stage_name,
                is_permanent=True,
                replace=True,
                skip_upload_on_content_match=True,
            )
        assert "skipping upload" in caplog.text
    finally:
        session._run_query(f"drop function if exists {udf_name}(int)")
        Utils.drop_stage(session, stage_name)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1625599: Modulo on coerced integers does not return correct result.",
)
def test_add_import_local_file(session, resources_path):
    test_files = TestFiles(resources_path)

    def plus4_then_mod5_with_1_level_import(x):
        from test_udf_dir.test_udf_file import mod5

        return mod5(x + 4)

    def plus4_then_mod5_with_2_level_import(x):
        from test_udf_file import mod5

        return mod5(x + 4)

    df = session.range(-5, 5).to_df("a")

    session.add_import(
        test_files.test_udf_py_file, import_path="test_udf_dir.test_udf_file"
    )
    plus4_then_mod5_udf = udf(
        plus4_then_mod5_with_1_level_import,
        return_type=IntegerType(),
        input_types=[IntegerType()],
    )
    Utils.check_answer(
        df.select(plus4_then_mod5_udf("a")).collect(),
        [Row((i + 4) % 5) for i in range(-5, 5)],
    )

    # if import_as argument changes, the checksum of the file will also change
    # and we will overwrite the file in the stage
    session.add_import(test_files.test_udf_py_file)

    plus4_then_mod5_direct_import_udf = udf(
        plus4_then_mod5_with_2_level_import,
        return_type=IntegerType(),
        input_types=[IntegerType()],
    )
    Utils.check_answer(
        df.select(plus4_then_mod5_direct_import_udf("a")).collect(),
        [Row((i + 4) % 5) for i in range(-5, 5)],
    )

    # clean
    session.clear_imports()


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1625599: Modulo on coerced integers does not return correct result.",
)
def test_add_import_local_directory(session, resources_path):
    test_files = TestFiles(resources_path)

    def plus4_then_mod5_with_3_level_import(x):
        from resources.test_udf_dir.test_udf_file import mod5

        return mod5(x + 4)

    def plus4_then_mod5_with_2_level_import(x):
        from test_udf_dir.test_udf_file import mod5

        return mod5(x + 4)

    df = session.range(-5, 5).to_df("a")

    session.add_import(
        test_files.test_udf_directory, import_path="resources.test_udf_dir"
    )
    plus4_then_mod5_udf = udf(
        plus4_then_mod5_with_3_level_import,
        return_type=IntegerType(),
        input_types=[IntegerType()],
    )
    Utils.check_answer(
        df.select(plus4_then_mod5_udf("a")).collect(),
        [Row((i + 4) % 5) for i in range(-5, 5)],
    )

    session.add_import(test_files.test_udf_directory)
    plus4_then_mod5_udf = udf(
        plus4_then_mod5_with_2_level_import,
        return_type=IntegerType(),
        input_types=[IntegerType()],
    )
    Utils.check_answer(
        df.select(plus4_then_mod5_udf("a")).collect(),
        [Row((i + 4) % 5) for i in range(-5, 5)],
    )

    # clean
    session.clear_imports()


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1625599: Modulo on coerced integers does not return correct result.",
)
def test_add_import_stage_file(session, resources_path):
    test_files = TestFiles(resources_path)

    def plus4_then_mod5_with_import(x):
        from test_udf_file import mod5

        return mod5(x + 4)

    stage_file = f"@{tmp_stage_name}/{os.path.basename(test_files.test_udf_py_file)}"
    session.add_import(stage_file)
    plus4_then_mod5_udf = udf(
        plus4_then_mod5_with_import,
        return_type=IntegerType(),
        input_types=[IntegerType()],
    )

    df = session.range(-5, 5).to_df("a")
    Utils.check_answer(
        df.select(plus4_then_mod5_udf("a")).collect(),
        [Row((i + 4) % 5) for i in range(-5, 5)],
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
def test_add_import_duplicate(session, resources_path, caplog, local_testing_mode):
    test_files = TestFiles(resources_path)
    abs_path = test_files.test_udf_directory
    rel_path = os.path.relpath(abs_path)

    session.add_import(abs_path)
    session.add_import(f"{abs_path}/")
    session.add_import(rel_path)
    assert session.get_imports() == [test_files.test_udf_directory]

    if not local_testing_mode:
        # skip upload the file because the calculated checksum is same
        session_stage = session.get_session_stage()
        session._resolve_imports(session_stage, session_stage)
        session.add_import(abs_path)
        session._resolve_imports(session_stage, session_stage)
        assert (
            f"{os.path.basename(abs_path)}.zip exists on {session_stage}, skipped"
            in caplog.text
        )

    session.remove_import(rel_path)
    assert len(session.get_imports()) == 0


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1625599: Modulo on coerced integers does not return correct result.",
)
def test_udf_level_import(session, resources_path, local_testing_mode):
    test_files = TestFiles(resources_path)

    def plus4_then_mod5_with_import(x):
        from test_udf_dir.test_udf_file import mod5

        return mod5(x + 4)

    df = session.range(-5, 5).to_df("a")

    # with udf-level imports
    plus4_then_mod5_udf = udf(
        plus4_then_mod5_with_import,
        return_type=IntegerType(),
        input_types=[IntegerType()],
        imports=[(test_files.test_udf_py_file, "test_udf_dir.test_udf_file")],
    )
    Utils.check_answer(
        df.select(plus4_then_mod5_udf("a")).collect(),
        [Row((i + 4) % 5) for i in range(-5, 5)],
    )

    # without udf-level imports
    plus4_then_mod5_udf = udf(
        plus4_then_mod5_with_import,
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
        plus4_then_mod5_with_import,
        return_type=IntegerType(),
        input_types=[IntegerType()],
        imports=[],
    )
    with pytest.raises(SnowparkSQLException) as ex_info:
        df.select(plus4_then_mod5_udf("a")).collect(),

    assert "No module named" in ex_info.value.message

    # clean
    session.clear_imports()


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1625599: Modulo on coerced integers does not return correct result.",
)
def test_add_import_namespace_collision(session, resources_path):
    test_files = TestFiles(resources_path)

    def plus4_then_mod5(x):
        from test_udf_dir.test_another_udf_file import mod17  # noqa: F401
        from test_udf_dir.test_udf_file import mod5

        return mod5(x + 4)

    session.add_import(
        test_files.test_udf_py_file, import_path="test_udf_dir.test_udf_file"
    )
    session.add_import(
        test_files.test_another_udf_py_file,
        import_path="test_udf_dir.test_another_udf_file",
    )

    df = session.range(-5, 5).to_df("a")

    plus4_then_mod5_udf = udf(
        plus4_then_mod5,
        return_type=IntegerType(),
        input_types=[IntegerType()],
        packages=["pandas"],
    )

    Utils.check_answer(
        df.select(plus4_then_mod5_udf("a")).collect(),
        [Row((i + 4) % 5) for i in range(-5, 5)],
    )

    # clean
    session.clear_imports()


def test_add_import_namespace_collision_snowflake_package(session, tmp_path):
    fake_snowflake_dir = tmp_path / "snowflake" / "task"
    fake_snowflake_dir.mkdir(parents=True)
    py_file = fake_snowflake_dir / "test_udf_file.py"
    py_file.write_text("def f(x): return x+1")

    def test(x):
        from snowflake.task.test_udf_file import f  # noqa: F401

        from snowflake.snowpark.functions import udf  # noqa: F401

        return f(x)

    session.add_import(str(py_file), import_path="snowflake.task.test_udf_file")

    df = session.range(-5, 5).to_df("a")

    test_udf = udf(
        test,
        return_type=IntegerType(),
        input_types=[IntegerType()],
        packages=["snowflake-snowpark-python"],
    )
    Utils.check_answer(
        df.select(test_udf("a")).collect(),
        [Row(i + 1) for i in range(-5, 5)],
    )

    # clean
    session.clear_imports()


def test_type_hints(session, local_testing_mode):
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

    @udf
    def return_geometry_dict_udf(g: Geometry) -> Dict[str, str]:
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

    if not local_testing_mode:
        # TODO: SNOW-946829 support Geometry and Geography datatypes
        Utils.check_answer(
            TestData.geography_type(session).select(return_geography_dict_udf("geo")),
            [Row('{\n  "coordinates": [\n    30,\n    10\n  ],\n  "type": "Point"\n}')],
        )

        Utils.check_answer(
            TestData.geometry_type(session).select(return_geometry_dict_udf("geo")),
            [Row('{\n  "coordinates": [\n    20,\n    81\n  ],\n  "type": "Point"\n}')],
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


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1412530 to fix bug",
    run=False,
)
@pytest.mark.parametrize("register_from_file", [True, False])
def test_register_udf_with_empty_optional_args(
    session: Session, tmpdir, register_from_file, caplog
):
    func_body = """
def empty_args() -> str:
    return "success"

def only_type_hint(s: str) -> str:
    return s
"""
    session.add_packages("snowflake-snowpark-python")
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        if register_from_file:
            file_path = os.path.join(tmpdir, "register_from_file_optional_args.py")
            with open(file_path, "w") as f:
                source = f"{func_body}"
                f.write(source)

            empty_args_udf = session.udf.register_from_file(file_path, "empty_args")
            only_type_hint_udf = session.udf.register_from_file(
                file_path, "only_type_hint"
            )
        else:
            d = {}
            exec(func_body, {**globals(), **locals()}, d)

            empty_args_udf = session.udf.register(d["empty_args"])
            only_type_hint_udf = session.udf.register(d["only_type_hint"])

    # assert that no warnings are raised here
    # SNOW-1734254 for suppressing opentelemetry warning log
    assert len(caplog.records) == 0 or [
        all("opentelemetry" in str(record) for record in caplog.records)
    ]


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1412530 to fix bug",
    run=False,
)
@pytest.mark.parametrize("register_from_file", [True, False])
def test_register_udf_with_optional_args(session: Session, tmpdir, register_from_file):
    import_body = """
import datetime
import decimal
from snowflake.snowpark.types import Variant, Geometry, Geography
from typing import Dict, List, Optional
"""
    func_body = """
def add(x: int = 0, y: int = 0) -> int:
    return x + y

def snow(x: int = 1) -> Optional[str]:
    return "snow" if x % 2 == 0 else None

def double_str_list(x: str = "a") -> List[str]:
    return [x, x]

def return_date(
    dt: datetime.date = datetime.date(2017, 1, 1)
) -> datetime.date:
    return dt

def return_arr(
    base_arr: List[int], extra_arr: List[int] = [4]
) -> List[int]:
    base_arr.extend(extra_arr)
    return base_arr

def return_all_datatypes(
    a: int = 1,
    b: float = 1.0,
    c: str = "one",
    d: List[int] = [],
    e: Dict[str, int] = {"s": 1},
    f: Variant = {"key": "val"},
    g: Geometry = "POINT(-122.35 37.55)",
    h: Geography = "POINT(-122.35 37.55)",
    i: datetime.datetime = datetime.datetime(2021, 1, 1, 0, 0, 0),
    j: datetime.date = datetime.date(2021, 1, 1),
    k: datetime.time = datetime.time(0, 0, 0),
    l: bytes = b"123",
    m: bool = True,
    n: decimal.Decimal = decimal.Decimal(1.0),
) -> str:
    final_str = f"{a}, {b}, {c}, {d}, {e}, {f}, {g}, {h}, {i}, {j}, {k}, {l}, {m}, {n}"
    return final_str
"""
    session.add_packages("snowflake-snowpark-python")
    if register_from_file:
        file_path = os.path.join(tmpdir, "register_from_file_optional_args.py")
        with open(file_path, "w") as f:
            source = f"{import_body}\n{func_body}"
            f.write(source)

        add_udf = session.udf.register_from_file(file_path, "add")
        snow_udf = session.udf.register_from_file(file_path, "snow")
        double_str_list_udf = session.udf.register_from_file(
            file_path, "double_str_list"
        )
        return_date_udf = session.udf.register_from_file(file_path, "return_date")
        return_arr_udf = session.udf.register_from_file(file_path, "return_arr")
        return_all_datatypes_udf = session.udf.register_from_file(
            file_path, "return_all_datatypes"
        )
    else:
        d = {}
        exec(func_body, {**globals(), **locals()}, d)

        add_udf = session.udf.register(d["add"])
        snow_udf = session.udf.register(d["snow"])
        double_str_list_udf = session.udf.register(d["double_str_list"])
        return_date_udf = session.udf.register(d["return_date"])
        return_arr_udf = session.udf.register(d["return_arr"])
        return_all_datatypes_udf = session.udf.register(d["return_all_datatypes"])

    df = session.create_dataframe([[1, 4], [2, 3]]).to_df("a", "b")
    Utils.check_answer(
        df.select(
            add_udf("a", "b"),
            add_udf("a"),
            add_udf(),
        ),
        [
            Row(5, 1, 0),
            Row(5, 2, 0),
        ],
    )
    Utils.check_answer(
        df.select(
            snow_udf("a"),
            snow_udf(),
        ),
        [
            Row(None, None),
            Row("snow", None),
        ],
    )
    Utils.check_answer(
        df.select(
            double_str_list_udf("a"),
            double_str_list_udf(),
        ),
        [
            Row('[\n  "1",\n  "1"\n]', '[\n  "a",\n  "a"\n]'),
            Row('[\n  "2",\n  "2"\n]', '[\n  "a",\n  "a"\n]'),
        ],
    )
    Utils.check_answer(
        df.select(
            return_date_udf(lit(datetime.date(2024, 4, 4))),
            return_date_udf(),
        ),
        [
            Row(datetime.date(2024, 4, 4), datetime.date(2017, 1, 1)),
            Row(datetime.date(2024, 4, 4), datetime.date(2017, 1, 1)),
        ],
    )
    Utils.check_answer(
        df.select(
            return_arr_udf(lit([1, 2, 3]), lit(list([4, 5]))),
            return_arr_udf(lit([1, 2, 3])),
        ),
        [
            Row("[\n  1,\n  2,\n  3,\n  4,\n  5\n]", "[\n  1,\n  2,\n  3,\n  4\n]"),
            Row("[\n  1,\n  2,\n  3,\n  4,\n  5\n]", "[\n  1,\n  2,\n  3,\n  4\n]"),
        ],
    )

    default_row = "1, 1.0, one, [], {'s': 1}, {'key': 'val'}, {'coordinates': [-122.35, 37.55], 'type': 'Point'}, {'coordinates': [-122.35, 37.55], 'type': 'Point'}, 2021-01-01 00:00:00, 2021-01-01, 00:00:00, b'123', True, 1.000000000000000000"
    custom_row = "2, 2.0, two, [], {'s': 1}, {'key': 'val'}, {'coordinates': [-122.35, 37.55], 'type': 'Point'}, {'coordinates': [-122.35, 37.55], 'type': 'Point'}, 2021-01-01 00:00:00, 2021-01-01, 00:00:00, b'123', True, 1.000000000000000000"
    Utils.check_answer(
        df.select(
            return_all_datatypes_udf(),
            return_all_datatypes_udf(lit(2), lit(2.0), lit("two")),
        ),
        [
            Row(default_row, custom_row),
            Row(default_row, custom_row),
        ],
    )


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="Database objects are session scoped in Local Testing",
    run=False,
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


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="Database objects are session scoped in Local Testing",
    run=False,
)
@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
def test_permanent_udf_negative(session, db_parameters):
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
                is_permanent=False,
                stage_location=stage_name,
                session=new_session,
            )

            with pytest.raises(
                SnowparkSQLException, match=f"Unknown function {udf_name}"
            ):
                session.sql(f"select {udf_name}(8, 9)").collect()

            Utils.check_answer(
                new_session.sql(f"select {udf_name}(8, 9)").collect(), [Row(17)]
            )
        finally:
            new_session._run_query(f"drop function if exists {udf_name}(int, int)")
            Utils.drop_stage(session, stage_name)


def test_udf_negative(session, local_testing_mode):
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
    with pytest.raises(
        TypeError,
        match=re.escape("udf() takes from 0 to 1 positional arguments but 2") + ".*",
    ):
        udf(f, IntegerType())

    udf1 = udf(f, return_type=IntegerType(), input_types=[IntegerType()])
    with pytest.raises(ValueError) as ex_info:
        udf1("a", "")
    assert (
        "Incorrect number of arguments passed to the UDF: Expected: <=1, Found: 2"
        in str(ex_info)
    )

    if not local_testing_mode:
        with pytest.raises(SnowparkSQLException) as ex_info:
            session.sql("select f(1)").collect()
        assert "Unknown function" in str(ex_info.value)

    with pytest.raises(SnowparkSQLException) as ex_info:
        df1.select(call_udf("f", "x")).collect()
    assert "Unknown function" in str(ex_info.value)

    with pytest.raises(SnowparkInvalidObjectNameException) as ex_info:
        udf(
            f,
            return_type=IntegerType(),
            input_types=[IntegerType()],
            name="invalid name",
        )
    assert "The object name 'invalid name' is invalid" in str(ex_info.value)

    # incorrect data type
    udf2 = udf(lambda x: int(x), return_type=IntegerType(), input_types=[IntegerType()])
    with pytest.raises(SnowparkSQLException) as ex_info:
        df1.select(udf2("x")).collect()
    assert (
        local_testing_mode
        or "Numeric value" in str(ex_info.value)
        and "is not recognized" in str(ex_info.value)
    )
    df2 = session.create_dataframe([1, None]).to_df("x")
    with pytest.raises(SnowparkSQLException) as ex_info:
        df2.select(udf2("x")).collect()
    assert "Python Interpreter Error" in str(ex_info.value)

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


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-1374204: Align error behavior when UDF/Sproc registration receives bad import",
)
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


def test_udf_coercion(session):
    def get_data_type(value):
        return str(type(value))

    udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

    str_udf = udf(
        get_data_type,
        return_type=StringType(),
        input_types=[StringType()],
        name=f"{udf_name}str",
        session=session,
    )
    float_udf = udf(
        get_data_type,
        return_type=StringType(),
        input_types=[FloatType()],
        name=f"{udf_name}float",
        session=session,
    )
    int_udf = udf(
        get_data_type,
        return_type=StringType(),
        input_types=[LongType()],
        name=f"{udf_name}int",
        session=session,
    )

    df = session.create_dataframe([[1]], schema=["a"])

    # Check that int input type can be coerced to expected inputs
    Utils.check_answer(df.select(str_udf("a")), [Row("<class 'str'>")])
    Utils.check_answer(df.select(float_udf("a")), [Row("<class 'float'>")])
    Utils.check_answer(df.select(int_udf("a")), [Row("<class 'int'>")])


def test_udf_variant_type(session, local_testing_mode):
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
    df = (
        session.create_dataframe(
            [("1"), ("1.1"), ('"2"'), ("true"), ("[1, 2, 3]"), ('{"a": "foo"}')]
        )
        .to_df(["a"])
        .select(parse_json("a").alias("a"))
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


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-946829 Support Geography datatype in Local Testing",
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
    "config.getoption('local_testing_mode', default=False)",
    reason="SNOW-946829 Support Geometry datatype in Local Testing",
)
def test_udf_geometry_type(session):
    def get_type(g):
        return str(type(g))

    geometry_udf = udf(get_type, return_type=StringType(), input_types=[GeometryType()])

    Utils.check_answer(
        TestData.geometry_type(session).select(geometry_udf(col("geo"))).collect(),
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
    assert "SQL compilation error" in str(ex_info.value)

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


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="Named temporary udf is not supported in stored proc"
)
def test_udf_if_not_exists(session):
    df = session.create_dataframe([[1, 2], [3, 4]]).to_df("a", "b")

    # Register named UDF and expect that it works.
    add_udf = session.udf.register(
        lambda x, y: x + y,
        name="test_udf_if_not_exist_add",
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
        if_not_exists=True,
    )
    Utils.check_answer(
        df.select(add_udf("a", "b")).collect(),
        [
            Row(3),
            Row(7),
        ],
    )

    # Replace named UDF with different one and expect that data is not changed.
    add_udf = session.udf.register(
        lambda x, y: x + y + 1,
        name="test_udf_if_not_exist_add",
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
        if_not_exists=True,
    )
    Utils.check_answer(
        df.select(add_udf("a", "b")).collect(),
        [
            Row(3),
            Row(7),
        ],
    )

    # Try to register UDF without if-exists check and expect failure.
    with pytest.raises(SnowparkSQLException, match="already exists"):
        add_udf = session.udf.register(
            lambda x, y: x + y,
            name="test_udf_if_not_exist_add",
            return_type=IntegerType(),
            input_types=[IntegerType(), IntegerType()],
            if_not_exists=False,
        )

    # Try to register UDF with replace and if-exists check and expect failure.
    with pytest.raises(
        ValueError,
        match="options replace and if_not_exists are incompatible",
    ):
        add_udf = session.udf.register(
            lambda x, y: x + y,
            name="test_udf_if_not_exist_add",
            return_type=IntegerType(),
            input_types=[IntegerType(), IntegerType()],
            replace=True,
            if_not_exists=True,
        )

    # Expect first UDF version to still be there.
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


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="Comment is a SQL feature",
    run=False,
)
def test_udf_comment(session):
    comment = f"COMMENT_{Utils.random_alphanumeric_str(6)}"

    def plus1(x: int) -> int:
        return x + 1

    plus1_udf = session.udf.register(plus1, comment=comment)

    ddl_sql = f"select get_ddl('FUNCTION', '{plus1_udf.name}(number)')"
    assert comment in session.sql(ddl_sql).collect()[0][0]


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Describe UDF is not supported in Local Testing",
)
@pytest.mark.skipif(
    (not is_pandas_available) or IS_IN_STORED_PROC,
    reason="numpy and pandas are required",
)
def test_udf_describe(session):
    def get_numpy_pandas_version(s: str) -> str:
        return f"{s}{numpy.__version__}/{pandas.__version__}"

    get_numpy_pandas_version_udf = session.udf.register(
        get_numpy_pandas_version, packages=["numpy", "pandas"]
    )
    describe_res = session.udf.describe(get_numpy_pandas_version_udf).collect()
    actual_fields = [row[0] for row in describe_res]
    expected_fields = [
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
        "is_aggregate",
    ]
    # We use zip such that it is compatible regardless of UDAF is enabled or not on the merge gate accounts
    for actual_field, expected_field in zip(actual_fields, expected_fields):
        assert (
            actual_field == expected_field
        ), f"Actual: {actual_fields}, Expected: {expected_fields}"

    for row in describe_res:
        if row[0] == "packages":
            assert "numpy" in row[1] and "pandas" in row[1]
            break


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Vectorized UDF is not supported in Local Testing",
)
@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
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


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Vectorized UDF is not supported in Local Testing",
)
@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
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


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Vectorized UDF is not supported in Local Testing",
)
@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
@pytest.mark.parametrize(
    "_type, data, expected_types, expected_dtypes",
    [
        (
            IntegerType,
            [[4096]],
            ("<class 'numpy.int16'>", "<class 'int'>"),
            ("int16", "object"),
        ),
        (
            IntegerType,
            [[1048576]],
            ("<class 'numpy.int32'>", "<class 'int'>"),
            ("int32", "object"),
        ),
        (
            IntegerType,
            [[8589934592]],
            ("<class 'numpy.int64'>", "<class 'int'>"),
            ("int64", "object"),
        ),
        (
            FloatType,
            [[1.0]],
            ("<class 'numpy.float64'>", "<class 'float'>"),
            ("float64",),
        ),
        (StringType, [["1"]], ("<class 'str'>",), ("string", "object")),
        (
            BooleanType,
            [[True]],
            (
                "<class 'bool'>",
                "<class 'numpy.bool_'>",
            ),
            ("boolean",),
        ),
        (
            BinaryType,
            [[(1).to_bytes(1, byteorder="big")]],
            ("<class 'bytes'>",),
            ("object",),
        ),
        (GeographyType, [["POINT(30 10)"]], ("<class 'dict'>",), ("object",)),
        (GeometryType, [["POINT(30 10)"]], ("<class 'dict'>",), ("object",)),
        (MapType, [[{1: 2}]], ("<class 'dict'>",), ("object",)),
        (ArrayType, [[[1]]], ("<class 'list'>",), ("object",)),
        (
            DateType,
            [[datetime.date(2021, 12, 20)]],
            ("<class 'pandas._libs.tslibs.timestamps.Timestamp'>",),
            ("datetime64[s]", "datetime64[ns]", "datetime64[us]"),
        ),
        (ArrayType, [[[1]]], ("<class 'list'>",), ("object",)),
        (
            TimeType,
            [[datetime.time(1, 1, 1)]],
            ("<class 'pandas._libs.tslibs.timedeltas.Timedelta'>",),
            ("timedelta64[s]", "timedelta64[ns]", "timedelta64[us]"),
        ),
        (
            TimestampType,
            [[datetime.datetime(2016, 3, 13, 5, tzinfo=datetime.timezone.utc)]],
            ("<class 'pandas._libs.tslibs.timestamps.Timestamp'>",),
            ("datetime64[s]", "datetime64[ns]", "datetime64[us]"),
        ),
    ],
)
def test_pandas_udf_input_types(session, _type, data, expected_types, expected_dtypes):
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


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Vectorized UDF is not supported in Local Testing",
)
@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
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


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Vectorized UDF is not supported in Local Testing",
)
@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
@pytest.mark.parametrize(
    "_type, data, expected_types, expected_dtypes",
    [
        (
            IntegerType,
            [[4096]],
            ("<class 'numpy.int16'>", "<class 'numpy.int64'>", "<class 'int'>"),
            ("int16", "int64", "object"),
        ),
        (
            IntegerType,
            [[1048576]],
            (
                "<class 'numpy.int32'>",
                "<class 'numpy.int64'>",
                "<class 'int'>",
                "<class 'numpy.intc'>",
            ),
            ("int32", "int64", "object"),
        ),
        (
            IntegerType,
            [[8589934592]],
            ("<class 'numpy.int64'>", "<class 'int'>"),
            ("int64", "object"),
        ),
        (
            FloatType,
            [[1.0]],
            ("<class 'numpy.float64'>", "<class 'float'>"),
            ("float64",),
        ),
        (StringType, [["1"]], ("<class 'str'>",), ("object",)),
        (
            BooleanType,
            [[True]],
            (
                "<class 'bool'>",
                "<class 'numpy.bool'>",
                "<class 'numpy.bool_'>",
            ),
            ("bool",),
        ),
        (
            BinaryType,
            [[(1).to_bytes(1, byteorder="big")]],
            ("<class 'bytes'>",),
            ("object",),
        ),
        (
            DateType,
            [[datetime.date(2021, 12, 20)]],
            ("<class 'datetime.date'>",),
            ("object",),
        ),
        (ArrayType, [[[1]]], ("<class 'list'>",), ("object",)),
        (
            TimeType,
            [[datetime.time(1, 1, 1)]],
            ("<class 'datetime.time'>",),
            ("object",),  # TODO: should be timedelta64[ns]
        ),
        (
            TimestampType,
            [[datetime.datetime(2016, 3, 13, 5, tzinfo=datetime.timezone.utc)]],
            ("<class 'pandas._libs.tslibs.timestamps.Timestamp'>",),
            ("datetime64[ns]",),
        ),
        (GeographyType, [["POINT(30 10)"]], ("<class 'dict'>",), ("object",)),
        (GeometryType, [["POINT(30 10)"]], ("<class 'dict'>",), ("object",)),
        (MapType, [[{1: 2}]], ("<class 'dict'>",), ("object",)),
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
        immutable=True,
    )
    result_df = df.select(series_udf("a")).to_pandas()
    result_val = result_df.iloc[0][0]
    if _type in (ArrayType, MapType, GeographyType, GeometryType):  # TODO: SNOW-573478
        result_val = json.loads(result_val)

    assert (
        str(type(result_val)) in expected_types
    ), f"returned type is {type(result_val)} instead of {expected_types}"
    assert (
        result_df.dtypes[0] in expected_dtypes
    ), f"returned dtype is {result_df.dtypes[0]} instead of {expected_dtypes}"


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Vectorized UDF is not supported in Local Testing",
)
@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
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


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Vectorized UDF is not supported in Local Testing",
)
@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
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


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Vectorized UDF is not supported in Local Testing",
)
@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
def test_pandas_udf_negative(session):

    with pytest.raises(ValueError) as ex_info:
        pandas_udf(
            lambda x: x + 1, return_type=IntegerType(), input_types=[IntegerType()]
        )
    assert "You cannot create a non-vectorized UDF using pandas_udf()" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        pandas_udf(
            lambda df: df,
            return_type=IntegerType(),
            input_types=[PandasDataFrameType([IntegerType()])],
        )
    assert "Invalid return type or input types" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        pandas_udf(
            lambda x, y: x + y,
            return_type=PandasSeriesType(IntegerType()),
            input_types=[IntegerType(), PandasSeriesType(IntegerType())],
        )
    assert "Invalid return type or input types" in str(ex_info)

    def add(x: pandas.Series, y: pandas.Series) -> pandas.Series:
        return x + y

    with pytest.raises(TypeError) as ex_info:
        pandas_udf(add)
    assert "The return type must be specified" in str(ex_info)


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="This is testing SQL feature",
    run=False,
)
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


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="Local Testing doesn't pickle",
    run=False,
)
def test_udf_pickle_failure(session):
    from weakref import WeakValueDictionary

    d = WeakValueDictionary()

    with pytest.raises(TypeError) as ex_info:
        session.udf.register(lambda: len(d), return_type=IntegerType())
    assert (
        "you might have to save the unpicklable object in the "
        "local environment first, add it to the UDF with session.add_import(), and read it from "
        "the UDF." in str(ex_info)
    )


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="Comment is a SQL feature",
    run=False,
)
def test_comment_in_udf_description(session):
    def return1():
        return "1"

    return1_udf = udf(return1, return_type=StringType())

    for row in session.udf.describe(return1_udf).collect():
        if row[0] == "body":
            assert (
                """\
# The following comment contains the source code generated by snowpark-python for explanatory purposes.
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
# The following comment contains the source code generated by snowpark-python for explanatory purposes.
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
    with caplog.at_level(logging.WARNING):
        add_udf(["a", "b"])
    assert (
        "Passing arguments to a UDF with a list or tuple is deprecated" in caplog.text
    )


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


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="Secure UDF is a SQL feature",
    run=False,
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


@pytest.mark.skipif(
    (not is_pandas_available) or IS_IN_STORED_PROC,
    reason="numpy and pandas are required",
)
@pytest.mark.parametrize("func", numpy_funcs)
def test_numpy_udf(session, func):
    numpy_udf = udf(
        func, return_type=DoubleType(), input_types=[DoubleType()], packages=["numpy"]
    )
    df = session.range(-5, 5).to_df("a")
    Utils.check_answer(
        df.select(numpy_udf("a")).collect(), [Row(func(i)) for i in range(-5, 5)]
    )


@pytest.mark.skipif(
    not is_pandas_available, reason="pandas required for vectorized UDF"
)
def test_udf_timestamp_type_hint(session):
    data = [
        [
            datetime.datetime(2023, 1, 1, 1, 1, 1),
            datetime.datetime(2023, 1, 1),
            datetime.datetime.now(datetime.timezone.utc),
            datetime.datetime.now().astimezone(),
        ],
        [
            datetime.datetime(2022, 12, 30, 12, 12, 12),
            datetime.datetime(2022, 12, 30),
            datetime.datetime(2023, 1, 1, 1, 1, 1).astimezone(datetime.timezone.utc),
            datetime.datetime(2023, 1, 1, 1, 1, 1, tzinfo=datetime.timezone.utc),
        ],
        [None, None, None, None],
    ]
    schema = StructType(
        [
            StructField('"tz_default"', TimestampType()),
            StructField('"ntz"', TimestampType(TimestampTimeZone.NTZ)),
            StructField('"ltz"', TimestampType(TimestampTimeZone.LTZ)),
            StructField('"tz"', TimestampType(TimestampTimeZone.TZ)),
        ]
    )
    df = session.create_dataframe(data, schema=schema)

    def f(x):
        return x + datetime.timedelta(days=1, hours=2) if x is not None else None

    @udf
    def func_tz_default_udf(x: Timestamp) -> Timestamp:
        return f(x)

    @udf
    def func_ntz_udf(x: Timestamp[NTZ]) -> Timestamp[NTZ]:
        return f(x)

    @udf
    def func_ltz_udf(x: Timestamp[LTZ]) -> Timestamp[LTZ]:
        return f(x)

    @udf
    def func_tz_udf(x: Timestamp[TZ]) -> Timestamp[TZ]:
        return f(x)

    expected_res = [Row(*[f(e) for e in row]) for row in data]
    Utils.check_answer(
        df.select(
            func_tz_default_udf('"tz_default"'),
            func_ntz_udf('"ntz"'),
            func_ltz_udf('"ltz"'),
            func_tz_udf('"tz"'),
        ),
        expected_res,
    )


@pytest.mark.skipif(
    not is_pandas_available, reason="pandas required for vectorized UDF"
)
def test_udf_return_none(session):
    data = [
        [
            1,
            "a",
            "a",
        ],
        [
            2,
            "b",
            "b",
        ],
        [None, None, None],
    ]
    schema = StructType(
        [
            StructField('"int"', IntegerType()),
            StructField('"str"', StringType()),
            StructField('"var"', VariantType()),
        ]
    )
    df = session.create_dataframe(data, schema=schema)

    def f(x):
        return x if x is not None else None

    @udf
    def func_int_udf(x: int) -> int:
        return f(x)

    @udf
    def func_str_udf(x: str) -> str:
        return f(x)

    @udf
    def func_var_udf(x: Variant) -> Variant:
        return f(x)

    Utils.check_answer(
        df.select(
            func_int_udf('"int"'),
            func_str_udf('"str"'),
            func_var_udf('"var"'),
        ),
        [Row(1, "a", '"a"'), Row(2, "b", '"b"'), Row(None, None, None)],
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="Vectorized UDTF is not supported in Local Testing",
)
@pytest.mark.skipif(
    not is_pandas_available, reason="pandas is required to register vectorized UDFs"
)
def test_vectorized_udf_timestamp_type_hint(session):
    data = [
        [
            datetime.datetime(2023, 1, 1, 1, 1, 1),
            datetime.datetime(2023, 1, 1),
            datetime.datetime.now(datetime.timezone.utc),
            datetime.datetime.now().astimezone(),
        ],
        [
            datetime.datetime(2022, 12, 30, 12, 12, 12),
            datetime.datetime(2022, 12, 30),
            datetime.datetime(2023, 1, 1, 1, 1, 1).astimezone(datetime.timezone.utc),
            datetime.datetime(2023, 1, 1, 1, 1, 1, tzinfo=datetime.timezone.utc),
        ],
        [None, None, None, None],
    ]
    schema = StructType(
        [
            StructField('"tz_default"', TimestampType()),
            StructField('"ntz"', TimestampType(TimestampTimeZone.NTZ)),
            StructField('"ltz"', TimestampType(TimestampTimeZone.LTZ)),
            StructField('"tz"', TimestampType(TimestampTimeZone.TZ)),
        ]
    )
    df = session.create_dataframe(data, schema=schema)

    def f(x):
        return x + datetime.timedelta(days=1, hours=2) if x is not None else None

    @udf
    def func_tz_default_vectorized_udf(
        x: PandasSeries[Timestamp],
    ) -> PandasSeries[Timestamp]:
        return f(x)

    @udf
    def func_ntz_vectorized_udf(
        x: PandasSeries[Timestamp[NTZ]],
    ) -> PandasSeries[Timestamp[NTZ]]:
        return f(x)

    @udf
    def func_ltz_vectorized_udf(
        x: PandasSeries[Timestamp[LTZ]],
    ) -> PandasSeries[Timestamp[LTZ]]:
        return f(x)

    @udf
    def func_tz_vectorized_udf(
        x: PandasSeries[Timestamp[TZ]],
    ) -> PandasSeries[Timestamp[TZ]]:
        return f(x)

    expected_res = [Row(*[f(e) for e in row]) for row in data]

    Utils.check_answer(
        df.select(
            func_tz_default_vectorized_udf('"tz_default"'),
            func_ntz_vectorized_udf('"ntz"'),
            func_ltz_vectorized_udf('"ltz"'),
            func_tz_vectorized_udf('"tz"'),
        ),
        expected_res,
    )


def test_udf_timestamp_type_hint_negative(session):

    with pytest.raises(
        TypeError,
        match=r"Only Timestamp, Timestamp\[NTZ\], Timestamp\[LTZ\] and Timestamp\[TZ\] are allowed",
    ):

        @udf
        def f(x: Timestamp) -> Timestamp[int]:
            return x


@pytest.mark.skipif(IS_NOT_ON_GITHUB, reason="need resources")
def test_udf_external_access_integration(session, db_parameters):
    def return_success():
        import _snowflake
        import requests

        if (
            _snowflake.get_generic_secret_string("cred") == "replace-with-your-api-key"
            and requests.get("https://www.google.com").status_code == 200
        ):
            return "success"
        return "failure"

    try:
        return_success_udf = session.udf.register(
            return_success,
            return_type=StringType(),
            packages=["requests", "snowflake-snowpark-python"],
            external_access_integrations=[
                db_parameters["external_access_integration1"]
            ],
            secrets={
                "cred": f"{db_parameters['external_access_key1']}",
            },
        )
        df = session.create_dataframe([[1, 2], [3, 4]]).to_df("a", "b")
        Utils.check_answer(
            df.select(return_success_udf()).collect(), [Row("success"), Row("success")]
        )
    except KeyError:
        pytest.skip("External Access Integration is not supported on the deployment.")


def test_access_snowflake_import_directory(session, resources_path):
    test_files = TestFiles(resources_path)

    def handler():
        import json
        import sys

        with open(
            os.path.join(sys._xoptions["snowflake_import_directory"], "testJson.json")
        ) as f:
            res = json.load(f)
        return res["fruit"]

    df = session.create_dataframe([[1]])

    session.add_import(test_files.test_file_json)

    import_udf = udf(
        handler,
        return_type=StringType(),
        input_types=[],
    )

    Utils.check_answer(
        df.select(import_udf()).collect(),
        [Row("Apple")],
    )

    # clean
    session.clear_imports()


@pytest.mark.xfail(reason="SNOW-2041110: flaky test", strict=False)
@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="artifact repository not supported in local testing",
)
@pytest.mark.skipif(IS_NOT_ON_GITHUB, reason="need resources")
@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="artifact repository requires Python 3.9+"
)
def test_register_artifact_repository(session):
    def test_urllib() -> str:
        import urllib3

        return str(urllib3.exceptions.HTTPError("test"))

    temp_func_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

    try:
        # Test function registration
        udf(
            func=test_urllib,
            name=temp_func_name,
            artifact_repository="SNOWPARK_PYTHON_TEST_REPOSITORY",
            artifact_repository_packages=["urllib3", "requests"],
        )

        # Test UDF call
        df = session.create_dataframe([1]).to_df(["a"])
        Utils.check_answer(df.select(call_udf(temp_func_name)), [Row("test")])
    finally:
        session._run_query(f"drop function if exists {temp_func_name}(int)")


@pytest.mark.xfail(reason="SNOW-2041110: flaky test", strict=False)
@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="artifact repository not supported in local testing",
)
@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="artifact repository requires Python 3.9+"
)
def test_register_artifact_repository_negative(session):
    def test_nop() -> str:
        pass

    temp_func_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)
    with pytest.raises(
        ValueError,
        match="artifact_repository must be specified when artifact_repository_packages has been specified",
    ):
        udf(
            func=test_nop,
            name=temp_func_name,
            artifact_repository_packages=["urllib3", "requests"],
        )

    with pytest.raises(
        ValueError,
        match="Cannot create a function with duplicates between packages and artifact repository packages.",
    ):
        udf(
            func=test_nop,
            name=temp_func_name,
            packages=["urllib3==2.3.0"],
            artifact_repository="SNOWPARK_PYTHON_TEST_REPOSITORY",
            artifact_repository_packages=["urllib3==2.1.0", "requests"],
        )

    with pytest.raises(Exception, match="Unknown resource constraint key"):
        udf(
            func=test_nop,
            name=temp_func_name,
            artifact_repository="SNOWPARK_PYTHON_TEST_REPOSITORY",
            artifact_repository_packages=["urllib3", "requests"],
            resource_constraint={"cpu": "x86"},
        )

    with pytest.raises(
        Exception, match="Unknown value 'risc-v' for key 'architecture'"
    ):
        udf(
            func=test_nop,
            name=temp_func_name,
            artifact_repository="SNOWPARK_PYTHON_TEST_REPOSITORY",
            artifact_repository_packages=["urllib3", "requests"],
            resource_constraint={"architecture": "risc-v"},
        )

    try:
        udf(
            func=test_nop,
            name=temp_func_name,
            artifact_repository="SNOWPARK_PYTHON_TEST_REPOSITORY",
            artifact_repository_packages=["urllib3", "requests"],
            resource_constraint={"architecture": "x86"},
        )
    except SnowparkSQLException as ex:
        assert (
            "Cannot create on a Python function with 'X86' architecture annotation using an 'ARM' warehouse."
            in str(ex.value)
        )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="artifact repository not supported in local testing",
)
@pytest.mark.skipif(IS_NOT_ON_GITHUB, reason="need resources")
@pytest.mark.skipif(
    sys.version_info < (3, 9), reason="artifact repository requires Python 3.9+"
)
def test_udf_artifact_repository_from_file(session, tmpdir):
    source = dedent(
        """
    import urllib3
    def test_urllib() -> str:
        import urllib3

        return str(urllib3.exceptions.HTTPError("test"))
    """
    )
    file_path = os.path.join(tmpdir, "artifact_repository_udf.py")
    with open(file_path, "w") as f:
        f.write(source)

    ar_udf = session.udf.register_from_file(
        file_path,
        "test_urllib",
        artifact_repository="SNOWPARK_PYTHON_TEST_REPOSITORY",
        artifact_repository_packages=["urllib3", "requests"],
    )
    df = session.create_dataframe([1]).to_df(["a"])
    Utils.check_answer(df.select(ar_udf()), [Row("test")])
