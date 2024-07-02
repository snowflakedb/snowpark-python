#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import inspect
import os

import pytest
from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import snowflake
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import sproc, udaf, udf, udtf
from snowflake.snowpark.types import (
    BinaryType,
    BooleanType,
    DecimalType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from tests.utils import IS_IN_STORED_PROC, check_answers

pytestmark = [
    pytest.mark.udf,
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="UDTF not supported in Local Testing",
    ),
]


def spans_to_dict(spans):
    res = {}
    for span in spans:
        res[span.name] = span
    return res


@pytest.fixture(scope="module")
def dict_exporter():
    resource = Resource(attributes={SERVICE_NAME: "snowpark-python-open-telemetry"})
    trace_provider = TracerProvider(resource=resource)
    dict_exporter = InMemorySpanExporter()
    processor = SimpleSpanProcessor(dict_exporter)
    trace_provider.add_span_processor(processor)
    trace.set_tracer_provider(trace_provider)
    yield dict_exporter
    dict_exporter.shutdown()


def test_open_telemetry_in_table_stored_proc(session, dict_exporter):
    df = session.create_dataframe([1, 2, 3, 4]).to_df("a")
    df._execute_and_get_query_id()
    lineno = inspect.currentframe().f_lineno - 1

    answer = (
        "_execute_and_get_query_id",
        {
            "code.filepath": "test_open_telemetry.py",
            "code.lineno": lineno,
            "method.chain": "DataFrame.to_df()._execute_and_get_query_id()",
        },
    )
    assert check_answers(dict_exporter, answer)


def test_catch_error_during_action_function(session, dict_exporter):
    df = session.sql("select 1/0")
    with pytest.raises(SnowparkSQLException, match="Division by zero"):
        df.collect()

    answer = (
        "collect",
        {
            "status_code": trace.status.StatusCode.ERROR,
            "status_description": "Division by zero",
        },
    )
    assert check_answers(dict_exporter, answer)


def test_catch_error_during_registration_function(session, dict_exporter):
    with pytest.raises(ValueError, match="does not exist"):
        session.udf.register_from_file(
            "empty file",
            "mod5",
            name="mod5_function",
            return_type=IntegerType(),
            input_types=[IntegerType()],
            replace=True,
            immutable=True,
        )
    answer = (
        "register_from_file",
        {
            "status_code": trace.status.StatusCode.ERROR,
            "status_description": "does not exist",
        },
    )
    assert check_answers(dict_exporter, answer)


@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="Cannot create session in SP",
)
def test_register_stored_procedure_from_file(session, dict_exporter):
    session.add_packages("snowflake-snowpark-python")
    test_file = os.path.normpath(
        os.path.join(
            os.path.dirname(__file__),
            "../resources",
            "test_sp_dir",
            "test_sp_file.py",
        )
    )

    lineno = inspect.currentframe().f_lineno + 1
    session.sproc.register_from_file(
        test_file,
        "mod5",
        name="register_stored_proc_from_file",
        return_type=IntegerType(),
        input_types=[IntegerType()],
        replace=True,
    )
    answer = (
        "register_from_file",
        {
            "code.filepath": "test_open_telemetry.py",
            "code.lineno": lineno,
            "snow.executable.name": "register_stored_proc_from_file",
            "snow.executable.handler": "mod5",
        },
    )
    assert check_answers(dict_exporter, answer)


@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="Cannot create session in SP",
)
def test_inline_register_stored_procedure(session, dict_exporter):
    session.add_packages("snowflake-snowpark-python")
    # test register with sproc.register

    def add_sp(session_: snowflake.snowpark.Session, x: int, y: int) -> int:
        return session_.sql(f"select {x} + {y}").collect()[0][0]

    lineno = inspect.currentframe().f_lineno + 1
    session.sproc.register(
        add_sp,
        return_type=IntegerType(),
        input_types=[IntegerType()],
        name="add_stored_proc",
    )
    answer = (
        "register",
        {
            "code.filepath": "test_open_telemetry.py",
            "code.lineno": lineno,
            "snow.executable.name": "add_stored_proc",
            "snow.executable.handler": "add_sp",
        },
    )
    assert check_answers(dict_exporter, answer)

    # test register with @sproc
    @sproc(name="minus_stored_proc")
    def minus_sp(session_: snowflake.snowpark.Session, x: int, y: int) -> int:
        return session_.sql(f"select {x} - {y}").collect()[0][0]

    answer = (
        "register",
        {
            "code.filepath": "test_open_telemetry.py",
            "snow.executable.name": "minus_stored_proc",
            "snow.executable.handler": "minus_sp",
        },
    )
    assert check_answers(dict_exporter, answer)


def test_register_udaf_from_file(session, dict_exporter):
    test_file = os.path.normpath(
        os.path.join(
            os.path.dirname(__file__),
            "../resources",
            "test_udaf_dir",
            "test_udaf_file.py",
        )
    )

    lineno = inspect.currentframe().f_lineno + 1
    session.udaf.register_from_file(
        test_file,
        "MyUDAFWithoutTypeHints",
        name="udaf_register_from_file",
        return_type=IntegerType(),
        input_types=[IntegerType()],
        replace=True,
        immutable=True,
    )
    answer = (
        "register_from_file",
        {
            "code.filepath": "test_open_telemetry.py",
            "code.lineno": lineno,
            "snow.executable.name": "udaf_register_from_file",
            "snow.executable.handler": "MyUDAFWithoutTypeHints",
        },
    )
    assert check_answers(dict_exporter, answer)


def test_inline_register_udaf(session, dict_exporter):
    # test register with udaf.register

    class PythonSumUDAF:
        def __init__(self) -> None:
            self._sum = 0

        @property
        def aggregate_state(self):
            return self._sum

        def accumulate(self, input_value):
            self._sum += input_value

        def merge(self, other_sum):
            self._sum += other_sum

        def finish(self):
            return self._sum

    lineno = inspect.currentframe().f_lineno + 1
    session.udaf.register(
        PythonSumUDAF,
        output_schema=StructType([StructField("number", IntegerType())]),
        input_types=[IntegerType()],
        return_type=IntegerType(),
        name="sum_udaf_integ",
        replace=True,
    )
    answer = (
        "register",
        {
            "code.filepath": "test_open_telemetry.py",
            "code.lineno": lineno,
            "snow.executable.name": "sum_udaf_integ",
            "snow.executable.handler": "PythonSumUDAF",
        },
    )
    assert check_answers(dict_exporter, answer)

    # test register with @udaf
    @udaf(
        name="sum_udaf_decorator_integ",
        session=session,
        input_types=[IntegerType()],
        return_type=IntegerType(),
        replace=True,
    )
    class PythonSumUDAF:
        def __init__(self) -> None:
            self._sum = 0

        @property
        def aggregate_state(self):
            return self._sum

        def accumulate(self, input_value):
            self._sum += input_value

        def merge(self, other_sum):
            self._sum += other_sum

        def finish(self):
            return self._sum

    answer = (
        "register",
        {
            "code.filepath": "test_open_telemetry.py",
            "snow.executable.name": "sum_udaf_decorator_integ",
            "snow.executable.handler": "PythonSumUDAF",
        },
    )
    assert check_answers(dict_exporter, answer)


def test_register_udtf_from_file(session, dict_exporter):
    test_file = os.path.normpath(
        os.path.join(
            os.path.dirname(__file__),
            "../resources",
            "test_udtf_dir",
            "test_udtf_file.py",
        )
    )

    schema = StructType(
        [
            StructField("int_", IntegerType()),
            StructField("float_", FloatType()),
            StructField("bool_", BooleanType()),
            StructField("decimal_", DecimalType(10, 2)),
            StructField("str_", StringType()),
            StructField("bytes_", BinaryType()),
            StructField("bytearray_", BinaryType()),
        ]
    )

    lineno = inspect.currentframe().f_lineno + 1
    session.udtf.register_from_file(
        test_file,
        "MyUDTFWithTypeHints",
        name="MyUDTFWithTypeHints_from_file",
        output_schema=schema,
        input_types=[
            IntegerType(),
            FloatType(),
            BooleanType(),
            DecimalType(10, 2),
            StringType(),
            BinaryType(),
            BinaryType(),
        ],
        immutable=True,
        replace=True,
    )
    answer = (
        "register_from_file",
        {
            "code.filepath": "test_open_telemetry.py",
            "code.lineno": lineno,
            "snow.executable.name": "MyUDTFWithTypeHints_from_file",
            "snow.executable.handler": "MyUDTFWithTypeHints",
        },
    )
    assert check_answers(dict_exporter, answer)


def test_inline_register_udtf(session, dict_exporter):
    # test register with udtf.register

    class GeneratorUDTF:
        def process(self, n):
            for i in range(n):
                yield (i,)

    lineno = inspect.currentframe().f_lineno + 1
    session.udtf.register(
        GeneratorUDTF,
        output_schema=StructType([StructField("number", IntegerType())]),
        input_types=[IntegerType()],
        replace=True,
        name="generate_udtf_integ",
    )
    answer = (
        "register",
        {
            "code.filepath": "test_open_telemetry.py",
            "code.lineno": lineno,
            "snow.executable.name": "generate_udtf_integ",
            "snow.executable.handler": "GeneratorUDTF",
        },
    )
    assert check_answers(dict_exporter, answer)

    # test register with @udtf
    @udtf(
        output_schema=StructType([StructField("number", IntegerType())]),
        name="generate_udtf_with_decorator_integ",
        session=session,
        replace=True,
    )
    class GeneratorUDTFwithDecorator:
        def process(self, n):
            for i in range(n):
                yield (i,)

    answer = (
        "register",
        {
            "code.filepath": "test_open_telemetry.py",
            "snow.executable.name": "generate_udtf_with_decorator_integ",
            "snow.executable.handler": "GeneratorUDTFwithDecorator",
        },
    )
    assert check_answers(dict_exporter, answer)


def test_register_udf_from_file(session, dict_exporter):
    test_file = os.path.normpath(
        os.path.join(
            os.path.dirname(__file__),
            "../resources",
            "test_udf_dir",
            "test_udf_file.py",
        )
    )

    lineno = inspect.currentframe().f_lineno + 1
    session.udf.register_from_file(
        test_file,
        "mod5",
        name="mod5_function",
        return_type=IntegerType(),
        input_types=[IntegerType()],
        replace=True,
        immutable=True,
    )
    answer = (
        "register_from_file",
        {
            "code.filepath": "test_open_telemetry.py",
            "code.lineno": lineno,
            "snow.executable.name": "mod5_function",
            "snow.executable.handler": "mod5",
        },
    )
    assert check_answers(dict_exporter, answer)


def test_inline_register_udf(session, dict_exporter):
    # test register with udf.register

    def add_udf(x: int, y: int) -> int:
        return x + y

    lineno = inspect.currentframe().f_lineno + 1
    session.udf.register(
        add_udf,
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
        replace=True,
        name="add",
    )
    answer = (
        "register",
        {
            "code.filepath": "test_open_telemetry.py",
            "code.lineno": lineno,
            "snow.executable.name": "add",
            "snow.executable.handler": "add_udf",
        },
    )
    assert check_answers(dict_exporter, answer)

    # test register with decorator @udf
    @udf(name="minus_function", session=session, replace=True)
    def minus_udf(x: int, y: int) -> int:
        return x - y

    answer = (
        "register",
        {
            "code.filepath": "test_open_telemetry.py",
            "snow.executable.name": "minus_function",
            "snow.executable.handler": "minus_udf",
        },
    )
    assert check_answers(dict_exporter, answer)


def test_open_telemetry_span_from_dataframe_writer_and_dataframe(
    session, dict_exporter
):
    df = session.create_dataframe([1, 2, 3, 4]).to_df("a")
    df.write.mode("overwrite").save_as_table("saved_table", table_type="temporary")
    lineno = inspect.currentframe().f_lineno - 1

    answer = (
        "save_as_table",
        {
            "code.filepath": "test_open_telemetry.py",
            "code.lineno": lineno,
            "method.chain": "DataFrame.to_df().save_as_table()",
        },
    )
    assert check_answers(dict_exporter, answer)


def test_open_telemetry_span_from_dataframe_writer(session, dict_exporter):
    df = session.create_dataframe([1, 2, 3, 4]).to_df("a")
    df.collect()
    lineno = inspect.currentframe().f_lineno - 1

    answer = (
        "collect",
        {
            "code.filepath": "test_open_telemetry.py",
            "code.lineno": lineno,
            "method.chain": "DataFrame.to_df().collect()",
        },
    )
    assert check_answers(dict_exporter, answer)
