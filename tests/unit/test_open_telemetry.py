#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import functools
import inspect
import os
from unittest import mock

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import span

import snowflake.snowpark.session
from snowflake.snowpark._internal.open_telemetry import (
    build_method_chain,
    decorator_count,
)
from snowflake.snowpark._internal.server_connection import ServerConnection
from snowflake.snowpark.functions import udaf, udf, udtf
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
from tests.utils import check_tracing_span_answers

from ..conftest import opentelemetry_installed

pytestmark = [
    pytest.mark.udf,
    pytest.mark.skipif(
        "config.getoption('enable_cte_optimization', default=False)",
        reason="Flaky in CTE mode",
    ),
    pytest.mark.skipif(
        not opentelemetry_installed,
        reason="opentelemetry is not installed",
    ),
]


def attr_to_dict(tracing: span):
    dict_attr = {}
    for name in tracing.attributes:
        dict_attr[name] = tracing.attributes[name]
    dict_attr["code.filepath"] = os.path.basename(dict_attr["code.filepath"])
    dict_attr["status_code"] = tracing.status.status_code
    dict_attr["status_description"] = tracing.status.description
    return dict_attr


def span_extractor(dict_exporter: InMemorySpanExporter):
    spans = []
    raw_spans = dict_exporter.get_finished_spans()
    for raw_span in raw_spans:
        spans.append((raw_span.name, attr_to_dict(raw_span), raw_span))
    return spans


def dummy_decorator(func):
    @functools.wraps(func)
    def wrapper(*arg, **kwarg):
        return func(*arg, **kwarg)

    return wrapper


@dummy_decorator
def dummy_function1():
    return


def dummy_function2():
    return


api_calls = [
    {"name": "Session.create_dataframe[values]"},
    {"name": "DataFrame.to_df", "subcalls": [{"name": "DataFrame.select"}]},
]


def test_disable_open_telemetry(monkeypatch, dict_exporter):
    from snowflake.snowpark._internal import open_telemetry

    monkeypatch.setattr(open_telemetry, "open_telemetry_enabled", True)
    open_telemetry.disable_open_telemetry()
    mock_connection = mock.create_autospec(ServerConnection)
    mock_connection._conn = mock.MagicMock()
    session = snowflake.snowpark.session.Session(mock_connection)
    session._conn._telemetry_client = mock.MagicMock()
    session.create_dataframe([1, 2, 3, 4]).to_df("a").collect()

    lineno = inspect.currentframe().f_lineno - 1
    answer = (
        "collect",
        {"code.lineno": lineno, "code.filepath": "test_open_telemetry.py"},
    )
    assert check_tracing_span_answers(span_extractor(dict_exporter), answer) is False

    def minus_udf(x: int, y: int) -> int:
        return x - y

    session.udf.register(minus_udf, name="test_minus_unit_disable_telemetry")
    lineno = inspect.currentframe().f_lineno - 1
    answer = (
        "register",
        {"code.lineno": lineno, "snow.executable.name": "test_minus_unit_no_telemetry"},
    )
    assert check_tracing_span_answers(span_extractor(dict_exporter), answer) is False


def test_without_open_telemetry(monkeypatch, dict_exporter):
    from snowflake.snowpark._internal import open_telemetry

    monkeypatch.setattr(open_telemetry, "open_telemetry_found", False)
    mock_connection = mock.create_autospec(ServerConnection)
    mock_connection._conn = mock.MagicMock()
    session = snowflake.snowpark.session.Session(mock_connection)
    session._conn._telemetry_client = mock.MagicMock()
    session.create_dataframe([1, 2, 3, 4]).to_df("a").collect()

    lineno = inspect.currentframe().f_lineno - 1
    answer = (
        "collect",
        {"code.lineno": lineno, "code.filepath": "test_open_telemetry.py"},
    )
    assert check_tracing_span_answers(span_extractor(dict_exporter), answer) is False

    def minus_udf(x: int, y: int) -> int:
        return x - y

    session.udf.register(minus_udf, name="test_minus_unit_no_telemetry")
    lineno = inspect.currentframe().f_lineno - 1
    answer = (
        "register",
        {"code.lineno": lineno, "snow.executable.name": "test_minus_unit_no_telemetry"},
    )
    assert check_tracing_span_answers(span_extractor(dict_exporter), answer) is False


def test_register_udaf_from_file(dict_exporter):
    test_file = os.path.normpath(
        os.path.join(
            os.path.dirname(__file__),
            "../resources",
            "test_udaf_dir",
            "test_udaf_file.py",
        )
    )
    mock_connection = mock.create_autospec(ServerConnection)
    mock_connection._conn = mock.MagicMock()
    session = snowflake.snowpark.session.Session(mock_connection)
    session._conn._telemetry_client = mock.MagicMock()

    lineno = inspect.currentframe().f_lineno + 1
    session.udaf.register_from_file(
        test_file,
        "MyUDAFWithoutTypeHints",
        name="udaf_register_from_file_unit",
        return_type=IntegerType(),
        input_types=[IntegerType()],
        immutable=True,
    )
    answer = (
        "register_from_file",
        {
            "code.filepath": "test_open_telemetry.py",
            "code.lineno": lineno,
            "snow.executable.name": "udaf_register_from_file_unit",
            "snow.executable.handler": "MyUDAFWithoutTypeHints",
        },
    )
    assert check_tracing_span_answers(span_extractor(dict_exporter), answer)


def test_inline_register_udaf(dict_exporter):
    mock_connection = mock.create_autospec(ServerConnection)
    mock_connection._conn = mock.MagicMock()
    session = snowflake.snowpark.session.Session(mock_connection)
    session._conn._telemetry_client = mock.MagicMock()
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
        name="sum_udaf_unit",
    )
    answer = (
        "register",
        {
            "code.filepath": "test_open_telemetry.py",
            "code.lineno": lineno,
            "snow.executable.name": "sum_udaf_unit",
            "snow.executable.handler": "PythonSumUDAF",
        },
    )
    assert check_tracing_span_answers(span_extractor(dict_exporter), answer)

    # test register with @udaf
    @udaf(
        name="sum_udaf_decorator_unit",
        session=session,
        input_types=[IntegerType()],
        return_type=IntegerType(),
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
            "snow.executable.name": "sum_udaf_decorator_unit",
            "snow.executable.handler": "PythonSumUDAF",
        },
    )
    assert check_tracing_span_answers(span_extractor(dict_exporter), answer)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="UDTF not supported in Local Testing",
)
def test_register_udtf_from_file(dict_exporter):
    test_file = os.path.normpath(
        os.path.join(
            os.path.dirname(__file__),
            "../resources",
            "test_udtf_dir",
            "test_udtf_file.py",
        )
    )
    mock_connection = mock.create_autospec(ServerConnection)
    mock_connection._conn = mock.MagicMock()
    session = snowflake.snowpark.session.Session(mock_connection)
    session._conn._telemetry_client = mock.MagicMock()
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
        name="MyUDTFWithTypeHints_from_file_unit",
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
    )
    answer = (
        "register_from_file",
        {
            "code.filepath": "test_open_telemetry.py",
            "code.lineno": lineno,
            "snow.executable.name": "MyUDTFWithTypeHints_from_file_unit",
            "snow.executable.handler": "MyUDTFWithTypeHints",
        },
    )
    assert check_tracing_span_answers(span_extractor(dict_exporter), answer)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="UDTF not supported in Local Testing",
)
def test_inline_register_udtf(dict_exporter):
    mock_connection = mock.create_autospec(ServerConnection)
    mock_connection._conn = mock.MagicMock()
    session = snowflake.snowpark.session.Session(mock_connection)
    session._conn._telemetry_client = mock.MagicMock()
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
        name="generate_udtf_unit",
    )
    answer = (
        "register",
        {
            "code.filepath": "test_open_telemetry.py",
            "code.lineno": lineno,
            "snow.executable.name": "generate_udtf_unit",
            "snow.executable.handler": "GeneratorUDTF",
        },
    )
    assert check_tracing_span_answers(span_extractor(dict_exporter), answer)

    # test register with @udtf
    @udtf(
        output_schema=StructType([StructField("number", IntegerType())]),
        name="generate_udtf_with_decorator_unit",
        session=session,
    )
    class GeneratorUDTFwithDecorator:
        def process(self, n):
            for i in range(n):
                yield (i,)

    answer = (
        "register",
        {
            "code.filepath": "test_open_telemetry.py",
            "snow.executable.name": "generate_udtf_with_decorator_unit",
            "snow.executable.handler": "GeneratorUDTFwithDecorator",
        },
    )
    assert check_tracing_span_answers(span_extractor(dict_exporter), answer)


def test_register_udf_from_file(dict_exporter):
    test_file = os.path.normpath(
        os.path.join(
            os.path.dirname(__file__),
            "../resources",
            "test_udf_dir",
            "test_udf_file.py",
        )
    )
    mock_connection = mock.create_autospec(ServerConnection)
    mock_connection._conn = mock.MagicMock()
    session = snowflake.snowpark.session.Session(mock_connection)
    session._conn._telemetry_client = mock.MagicMock()

    lineno = inspect.currentframe().f_lineno + 1
    session.udf.register_from_file(
        test_file,
        "mod5",
        name="mod5_function_unit",
        return_type=IntegerType(),
        input_types=[IntegerType()],
        immutable=True,
    )
    answer = (
        "register_from_file",
        {
            "code.filepath": "test_open_telemetry.py",
            "code.lineno": lineno,
            "snow.executable.name": "mod5_function_unit",
            "snow.executable.handler": "mod5",
        },
    )
    assert check_tracing_span_answers(span_extractor(dict_exporter), answer)


def test_inline_register_udf(dict_exporter):
    mock_connection = mock.create_autospec(ServerConnection)
    mock_connection._conn = mock.MagicMock()
    session = snowflake.snowpark.session.Session(mock_connection)
    session._conn._telemetry_client = mock.MagicMock()
    # test register with udf.register

    def add_udf(x: int, y: int) -> int:
        return x + y

    lineno = inspect.currentframe().f_lineno + 1
    session.udf.register(
        add_udf,
        return_type=IntegerType(),
        input_types=[IntegerType(), IntegerType()],
        replace=True,
        stage_location="@test_stage",
        name="add_unit",
    )
    answer = (
        "register",
        {
            "code.filepath": "test_open_telemetry.py",
            "code.lineno": lineno,
            "snow.executable.name": "add_unit",
            "snow.executable.handler": "add_udf",
        },
    )
    assert check_tracing_span_answers(span_extractor(dict_exporter), answer)

    # test register with decorator @udf
    @udf(name="minus_decorator_unit", session=session)
    def minus_udf(x: int, y: int) -> int:
        return x - y

    answer = (
        "register",
        {
            "code.filepath": "test_open_telemetry.py",
            "snow.executable.name": "minus_decorator_unit",
            "snow.executable.handler": "minus_udf",
        },
    )
    assert check_tracing_span_answers(span_extractor(dict_exporter), answer)


def test_open_telemetry_span_from_dataframe_writer_and_dataframe(dict_exporter):
    # set up exporter
    mock_connection = mock.create_autospec(ServerConnection)
    mock_connection._conn = mock.MagicMock()
    session = snowflake.snowpark.session.Session(mock_connection)
    session._conn._telemetry_client = mock.MagicMock()
    df = session.create_dataframe([1, 2, 3, 4]).to_df("a")
    df.write.mode("overwrite").save_as_table("saved_table", table_type="temporary")

    answer = (
        "save_as_table",
        {
            "code.filepath": "test_open_telemetry.py",
            "method.chain": "DataFrame.to_df().save_as_table()",
        },
    )
    assert check_tracing_span_answers(span_extractor(dict_exporter), answer)


def test_open_telemetry_span_from_dataframe_writer(dict_exporter):
    mock_connection = mock.create_autospec(ServerConnection)
    mock_connection._conn = mock.MagicMock()
    session = snowflake.snowpark.session.Session(mock_connection)
    session._conn._telemetry_client = mock.MagicMock()
    df = session.create_dataframe([1, 2, 3, 4]).to_df("a")
    df.collect()

    answer = (
        "collect",
        {
            "code.filepath": "test_open_telemetry.py",
            "method.chain": "DataFrame.to_df().collect()",
        },
    )
    assert check_tracing_span_answers(span_extractor(dict_exporter), answer)


def test_decorator_count():
    decorator_number1 = decorator_count(dummy_function1)
    decorator_number2 = decorator_count(dummy_function2)
    assert decorator_number1 == 1
    assert decorator_number2 == 0


def test_build_method_chain():
    method_chain = build_method_chain(api_calls, "DataFrame.collect")
    assert method_chain == "DataFrame.to_df().collect()"
