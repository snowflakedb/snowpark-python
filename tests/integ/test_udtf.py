#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import decimal
from collections import Counter
from typing import Iterable, Tuple

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.functions import lit
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


def test_create_secure_udtf(session):
    class MyWordCount:
        def process(self, s1: str) -> Iterable[Tuple[str, int]]:
            counter = Counter(s1.split())
            return counter.items()

    secure_udtf = session.udtf.register(
        MyWordCount,
        name="secure_udtf",
        output_schema=["word", "count"],
        secure=True,
        is_permanent=True,
        stage_location="@mystage/test",
    )
    desc_rows = session.sql(
        f"select is_secure from information_schema.functions where function_name = 'secure_udtf'"
    ).collect()
    ...
    # TODO: Python UDF server doesn't support Secure UDF/UDTF yet.
