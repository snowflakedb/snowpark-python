#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import numpy
import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.mock.exceptions import SnowparkLocalTestingException
from snowflake.snowpark.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    VariantType,
)

SCHEMA = StructType([StructField("S", StringType()), StructField("N", LongType())])


def _put(session, tmp_path, name, content):
    path = tmp_path / name
    path.write_text(content)
    session.file.put(str(path), "@test_read_csv_stage", auto_compress=False)
    return f"@test_read_csv_stage/{name}"


def test_read_csv_keeps_na_like_strings(session, tmp_path):
    location = _put(session, tmp_path, "na.csv", "NA,1\nnull,2\nx,3\n")
    assert session.read.schema(SCHEMA).csv(location).collect() == [
        Row("NA", 1),
        Row("null", 2),
        Row("x", 3),
    ]


def test_read_csv_keeps_na_like_strings_in_variant_column(session, tmp_path):
    location = _put(session, tmp_path, "na_variant.csv", "NA,1\nnull,2\n")
    schema = StructType([StructField("X", VariantType()), StructField("N", LongType())])
    assert session.read.schema(schema).csv(location).collect() == [
        Row('"NA"', 1),
        Row('"null"', 2),
    ]


def test_read_csv_skip_blank_lines_requires_bool(session, tmp_path):
    location = _put(session, tmp_path, "blank.csv", "a,1\n\nb,2\n")

    def read(value):
        return (
            session.read.schema(SCHEMA)
            .option("SKIP_BLANK_LINES", value)
            .csv(location)
            .collect()
        )

    assert read(True) == read(numpy.bool_(True)) == [Row("a", 1), Row("b", 2)]
    assert len(read(False)) == len(read(None)) == 3
    for value in ("TRUE", "FALSE", "abc", 1):
        with pytest.raises(SnowparkLocalTestingException, match="SKIP_BLANK_LINES"):
            read(value)
