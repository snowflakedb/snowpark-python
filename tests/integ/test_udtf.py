#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import decimal
from typing import Iterable, Tuple

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.functions import lit, udtf
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
from tests.utils import TestFiles, Utils


def test_register_udtf_from_file_no_type_hints(session, resources_path):
    test_files = TestFiles(resources_path)
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
    my_udtf = session.udtf.register_from_file(
        test_files.test_udtf_py_file,
        "MyUDTFWithoutTypeHints",
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
    )
    assert isinstance(my_udtf.handler, tuple)
    df = session.table_function(
        my_udtf(
            lit(1),
            lit(2.2),
            lit(True),
            lit(decimal.Decimal("3.33")).cast("number(10, 2)"),
            lit("python"),
            lit(b"bytes"),
            lit(bytearray("bytearray", "utf-8")),
        )
    )
    Utils.check_answer(
        df,
        [
            Row(
                1,
                2.2,
                True,
                decimal.Decimal("3.33"),
                "python",
                b"bytes",
                bytearray("bytearray", "utf-8"),
            )
        ],
    )


def test_register_udtf_from_file_with_typehints(session, resources_path):
    test_files = TestFiles(resources_path)
    schema = ["int_", "float_", "bool_", "decimal_", "str_", "bytes_", "bytearray_"]
    my_udtf = session.udtf.register_from_file(
        test_files.test_udtf_py_file,
        "MyUDTFWithTypeHints",
        output_schema=schema,
    )
    assert isinstance(my_udtf.handler, tuple)
    df = session.table_function(
        my_udtf(
            lit(1),
            lit(2.2),
            lit(True),
            lit(decimal.Decimal("3.33")),
            lit("python"),
            lit(b"bytes"),
            lit(bytearray("bytearray", "utf-8")),
        )
    )
    Utils.check_answer(
        df,
        [
            Row(
                1,
                2.2,
                True,
                decimal.Decimal("3.33"),
                "python",
                b"bytes",
                bytearray("bytearray", "utf-8"),
            )
        ],
    )

    my_udtf_with_statement_params = session.udtf.register_from_file(
        test_files.test_udtf_py_file,
        "MyUDTFWithTypeHints",
        output_schema=schema,
        statement_params={"SF_PARTNER": "FAKE_PARTNER"},
    )
    assert isinstance(my_udtf_with_statement_params.handler, tuple)
    df = session.table_function(
        my_udtf_with_statement_params(
            lit(1),
            lit(2.2),
            lit(True),
            lit(decimal.Decimal("3.33")),
            lit("python"),
            lit(b"bytes"),
            lit(bytearray("bytearray", "utf-8")),
        )
    )
    Utils.check_answer(
        df,
        [
            Row(
                1,
                2.2,
                True,
                decimal.Decimal("3.33"),
                "python",
                b"bytes",
                bytearray("bytearray", "utf-8"),
            )
        ],
    )


def test_strict_utdf(session):
    @udtf(output_schema=["num"], strict=True)
    class UDTFEcho:
        def process(
            self,
            num: int,
        ) -> Iterable[Tuple[int]]:
            if num is None:
                raise ValueError("num should not be None")
            return [(num,)]

    df = session.table_function(UDTFEcho(lit(None).cast("int")))
    Utils.check_answer(
        df,
        [Row(None)],
    )


def test_udtf_negative(session):
    with pytest.raises(
        ValueError, match="name must be specified for permanent table function"
    ):

        @udtf(output_schema=["num"], is_permanent=True)
        class UDTFEcho:
            def process(
                self,
                num: int,
            ) -> Iterable[Tuple[int]]:
                return [(num,)]

    with pytest.raises(ValueError, match="file_path.*does not exist"):
        session.udtf.register_from_file(
            "fake_path",
            "MyUDTFWithTypeHints",
            output_schema=[
                "int_",
                "float_",
                "bool_",
                "decimal_",
                "str_",
                "bytes_",
                "bytearray_",
            ],
        )
