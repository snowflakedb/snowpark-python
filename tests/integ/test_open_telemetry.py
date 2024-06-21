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
    spans = spans_to_dict(dict_exporter.get_finished_spans())
    assert "register_from_file" in spans
    span = spans["register_from_file"]
    assert (
        os.path.basename(span.attributes["code.filepath"]) == "test_open_telemetry.py"
    )
    assert span.attributes["code.lineno"] == lineno
    assert span.attributes["snow.executable.name"] == "udaf_register_from_file"
    assert span.attributes["snow.executable.handler"] == "MyUDAFWithoutTypeHints"
    dict_exporter.clear()


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
        name="sum_udaf",
        replace=True,
    )
    spans = spans_to_dict(dict_exporter.get_finished_spans())
    assert "register" in spans
    span = spans["register"]
    assert (
        os.path.basename(span.attributes["code.filepath"]) == "test_open_telemetry.py"
    )
    assert span.attributes["code.lineno"] == lineno
    assert span.attributes["snow.executable.name"] == "sum_udaf"
    assert span.attributes["snow.executable.handler"] == "PythonSumUDAF"

    # test register with @udaf
    @udaf(
        name="sum_udaf",
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

    spans = spans_to_dict(dict_exporter.get_finished_spans())
    assert "register" in spans
    span = spans["register"]
    assert (
        os.path.basename(span.attributes["code.filepath"]) == "test_open_telemetry.py"
    )
    assert span.attributes["snow.executable.name"] == "sum_udaf"
    assert span.attributes["snow.executable.handler"] == "PythonSumUDAF"
    dict_exporter.clear()


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
    spans = spans_to_dict(dict_exporter.get_finished_spans())
    assert "register_from_file" in spans
    span = spans["register_from_file"]
    assert (
        os.path.basename(span.attributes["code.filepath"]) == "test_open_telemetry.py"
    )
    assert span.attributes["code.lineno"] == lineno
    assert span.attributes["snow.executable.name"] == "MyUDTFWithTypeHints_from_file"
    assert span.attributes["snow.executable.handler"] == "MyUDTFWithTypeHints"
    dict_exporter.clear()


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
        name="generate_udtf",
    )
    spans = spans_to_dict(dict_exporter.get_finished_spans())
    assert "register" in spans
    span = spans["register"]
    assert (
        os.path.basename(span.attributes["code.filepath"]) == "test_open_telemetry.py"
    )
    assert span.attributes["code.lineno"] == lineno
    assert span.attributes["snow.executable.name"] == "generate_udtf"
    assert span.attributes["snow.executable.handler"] == "GeneratorUDTF"

    # test register with @udtf
    @udtf(
        output_schema=StructType([StructField("number", IntegerType())]),
        name="generate_udtf_with_decorator",
        session=session,
        replace=True,
    )
    class GeneratorUDTFwithDecorator:
        def process(self, n):
            for i in range(n):
                yield (i,)

    spans = spans_to_dict(dict_exporter.get_finished_spans())
    assert "register" in spans
    span = spans["register"]
    assert (
        os.path.basename(span.attributes["code.filepath"]) == "test_open_telemetry.py"
    )
    assert span.attributes["snow.executable.name"] == "generate_udtf_with_decorator"
    assert span.attributes["snow.executable.handler"] == "GeneratorUDTFwithDecorator"
    dict_exporter.clear()


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
    spans = spans_to_dict(dict_exporter.get_finished_spans())
    assert "register_from_file" in spans
    span = spans["register_from_file"]
    assert (
        os.path.basename(span.attributes["code.filepath"]) == "test_open_telemetry.py"
    )
    assert span.attributes["code.lineno"] == lineno
    assert span.attributes["snow.executable.name"] == "mod5_function"
    assert span.attributes["snow.executable.handler"] == "mod5"
    dict_exporter.clear()


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
    spans = spans_to_dict(dict_exporter.get_finished_spans())
    assert "register" in spans
    span = spans["register"]
    assert (
        os.path.basename(span.attributes["code.filepath"]) == "test_open_telemetry.py"
    )
    assert span.attributes["code.lineno"] == lineno
    assert span.attributes["snow.executable.name"] == "add"
    assert span.attributes["snow.executable.handler"] == "add_udf"

    # test register with decorator @udf
    @udf(name="minus_function", session=session, replace=True)
    def minus_udf(x: int, y: int) -> int:
        return x - y

    spans = spans_to_dict(dict_exporter.get_finished_spans())
    assert "register" in spans
    span = spans["register"]
    print(span.attributes)
    assert (
        os.path.basename(span.attributes["code.filepath"]) == "test_open_telemetry.py"
    )
    assert span.attributes["snow.executable.name"] == "minus_function"
    assert span.attributes["snow.executable.handler"] == "minus_udf"
    dict_exporter.clear()


def test_open_telemetry_span_from_dataframe_writer_and_dataframe(
    session, dict_exporter
):
    df = session.create_dataframe([1, 2, 3, 4]).to_df("a")
    df.write.mode("overwrite").save_as_table("saved_table", table_type="temporary")
    lineno = inspect.currentframe().f_lineno - 1

    spans = spans_to_dict(dict_exporter.get_finished_spans())
    assert "save_as_table" in spans
    span = spans["save_as_table"]
    assert span.attributes["method.chain"] == "DataFrame.to_df().save_as_table()"
    assert (
        os.path.basename(span.attributes["code.filepath"]) == "test_open_telemetry.py"
    )
    assert span.attributes["code.lineno"] == lineno
    dict_exporter.clear()


def test_open_telemetry_span_from_dataframe_writer(session, dict_exporter):
    df = session.create_dataframe([1, 2, 3, 4]).to_df("a")
    df.collect()
    lineno = inspect.currentframe().f_lineno - 1

    spans = spans_to_dict(dict_exporter.get_finished_spans())
    assert "collect" in spans
    span = spans["collect"]
    assert span.attributes["method.chain"] == "DataFrame.to_df().collect()"
    assert (
        os.path.basename(span.attributes["code.filepath"]) == "test_open_telemetry.py"
    )
    assert span.attributes["code.lineno"] == lineno
    dict_exporter.clear()
