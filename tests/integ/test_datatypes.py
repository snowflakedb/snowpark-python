#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import csv
import os
import tempfile
from decimal import Decimal
from unittest.mock import patch

import pytest

from snowflake.snowpark import DataFrame, Row, context
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import (
    BooleanType,
    DecimalType,
    DoubleType,
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from tests.utils import Utils


def test_basic_filter(session):
    df: DataFrame = session.create_dataframe(
        [
            [1, 2, "abc"],
            [3, 4, "def"],
            [6, 5, "ghi"],
            [8, 7, "jkl"],
            [100, 200, "mno"],
            [400, 300, "pqr"],
        ],
        schema=["a", "b", "c"],
    ).select("a", "b", "c")
    assert repr(df.schema) == repr(
        StructType(
            [
                StructField("A", LongType(), nullable=False),
                StructField("B", LongType(), nullable=False),
                StructField("C", StringType(), nullable=False),
            ]
        )
    )


def test_plus_basic(session):
    df = session.create_dataframe(
        [[1, 1.1, 2.2, 3.3]],
        schema=StructType(
            [
                StructField("a", LongType(), nullable=False),
                StructField("b", DecimalType(3, 1), nullable=False),
                StructField("c", DoubleType(), nullable=False),
                StructField("d", DecimalType(4, 2), nullable=False),
            ]
        ),
    )

    df = df.select(
        (df["a"] + 1).as_("new_a"),
        (df["b"] + df["d"]).as_("new_b"),
        (df["c"] + 3).as_("new_c"),
    )
    assert repr(df.schema) == repr(
        StructType(
            [
                StructField("NEW_A", LongType(), nullable=False),
                StructField("NEW_B", DecimalType(5, 2), nullable=False),
                StructField("NEW_C", DoubleType(), nullable=False),
            ]
        )
    )


def test_minus_basic(session):
    df = session.create_dataframe(
        [[1, 1.1, 2.2, 3.3]],
        schema=StructType(
            [
                StructField("a", LongType(), nullable=False),
                StructField("b", DecimalType(3, 1), nullable=False),
                StructField("c", DoubleType(), nullable=False),
                StructField("d", DecimalType(4, 2), nullable=False),
            ]
        ),
    )

    df = df.select(
        (df["a"] - 1).as_("new_a"),
        (df["b"] - df["d"]).as_("new_b"),
        (df["c"] - 3).as_("new_c"),
    )
    assert repr(df.schema) == repr(
        StructType(
            [
                StructField("NEW_A", LongType(), nullable=False),
                StructField("NEW_B", DecimalType(5, 2), nullable=False),
                StructField("NEW_C", DoubleType(), nullable=False),
            ]
        )
    )


def test_multiple_basic(session):
    df = session.create_dataframe(
        [[1, 1.1, 2.2, 3.3]],
        schema=StructType(
            [
                StructField("a", LongType(), nullable=False),
                StructField("b", DecimalType(3, 1), nullable=False),
                StructField("c", FloatType(), nullable=False),
                StructField("d", DecimalType(4, 2), nullable=False),
            ]
        ),
    )

    df = df.select(
        (df["a"] * 1).as_("new_a"),
        (df["b"] * df["d"]).as_("new_b"),
        (df["c"] * 3).as_("new_c"),
    )
    assert repr(df.schema) == repr(
        StructType(
            [
                StructField("NEW_A", LongType(), nullable=False),
                StructField("NEW_B", DecimalType(7, 3), nullable=False),
                StructField("NEW_C", DoubleType(), nullable=False),
            ]
        )
    )


def test_divide_basic(session):
    df = session.create_dataframe(
        [[1, 1.1, 2.2, 3.3]],
        schema=StructType(
            [
                StructField("a", LongType(), nullable=False),
                StructField("b", DecimalType(3, 1), nullable=False),
                StructField("c", DoubleType(), nullable=False),
                StructField("d", DecimalType(4, 2), nullable=False),
            ]
        ),
    )

    df = df.select(
        (df["a"] / 1).as_("new_a"),
        (df["b"] / df["d"]).as_("new_b"),
        (df["c"] / 3).as_("new_c"),
    )
    assert repr(df.schema) == repr(
        StructType(
            [
                StructField("NEW_A", DecimalType(38, 6), nullable=False),
                StructField("NEW_B", DecimalType(11, 7), nullable=False),
                StructField("NEW_C", DoubleType(), nullable=False),
            ]
        )
    )
    Utils.check_answer(
        df, [Row(Decimal("1.0"), Decimal("0.3333333"), 0.7333333333333334)]
    )


def test_div_decimal_double(session):
    df = session.create_dataframe(
        [[11.0, 13.0]],
        schema=StructType(
            [StructField("a", DoubleType()), StructField("b", DoubleType())]
        ),
    )
    df = df.select([df["a"] / df["b"]])
    Utils.check_answer(df, [Row(0.8461538461538461)])

    df2 = session.create_dataframe([[11, 13]], schema=["a", "b"])
    df2 = df2.select([df2["a"] / df2["b"]])
    Utils.check_answer(df2, [Row(Decimal("0.846154"))])


def test_modulo_basic(session):
    df = session.create_dataframe(
        [[1, 1.1, 2.2, 3.3]],
        schema=StructType(
            [
                StructField("a", LongType(), nullable=False),
                StructField("b", DecimalType(3, 1), nullable=False),
                StructField("c", DoubleType(), nullable=False),
                StructField("d", DecimalType(4, 2), nullable=False),
            ]
        ),
    )

    df = df.select(
        (df["a"] % 1).as_("new_a"),
        (df["b"] % df["d"]).as_("new_b"),
        (df["c"] % 3).as_("new_c"),
    )
    assert repr(df.schema) == repr(
        StructType(
            [
                StructField("NEW_A", LongType(), nullable=False),
                StructField("NEW_B", DecimalType(4, 2), nullable=False),
                StructField("NEW_C", DoubleType(), nullable=False),
            ]
        )
    )


def test_binary_ops_bool(session):
    df = session.create_dataframe(
        [[1, 1.1]],
        schema=StructType(
            [
                StructField("a", LongType(), nullable=False),
                StructField("b", DecimalType(3, 1), nullable=False),
            ]
        ),
    )
    df1 = df.select(
        df["a"] > df["b"],
        df["a"] >= df["b"],
        df["a"] == df["b"],
        df["a"] != df["b"],
        df["a"] < df["b"],
        df["a"] <= df["b"],
    )
    assert repr(df1.schema) == repr(
        StructType(
            [
                StructField('"(""A"" > ""B"")"', BooleanType(), nullable=True),
                StructField('"(""A"" >= ""B"")"', BooleanType(), nullable=True),
                StructField('"(""A"" = ""B"")"', BooleanType(), nullable=True),
                StructField('"(""A"" != ""B"")"', BooleanType(), nullable=True),
                StructField('"(""A"" < ""B"")"', BooleanType(), nullable=True),
                StructField('"(""A"" <= ""B"")"', BooleanType(), nullable=True),
            ]
        )
    )

    df2 = df.select(
        (df["a"] > df["b"]) & (df["a"] >= df["b"]),
        (df["a"] > df["b"]) | (df["a"] >= df["b"]),
    )
    assert repr(df2.schema) == repr(
        StructType(
            [
                StructField(
                    '"((""A"" > ""B"") AND (""A"" >= ""B""))"',
                    BooleanType(),
                    nullable=True,
                ),
                StructField(
                    '"((""A"" > ""B"") OR (""A"" >= ""B""))"',
                    BooleanType(),
                    nullable=True,
                ),
            ]
        )
    )


def test_unary_ops_bool(session):
    df = session.create_dataframe(
        [[1, 1.1]],
        schema=StructType(
            [
                StructField("a", LongType(), nullable=False),
                StructField("b", DecimalType(3, 1), nullable=False),
            ]
        ),
    )

    df = df.select(
        df["a"].is_null(),
        df["a"].is_not_null(),
        df["a"].equal_nan(),
        ~df["a"].is_null(),
    )
    assert repr(df.schema) == repr(
        StructType(
            [
                StructField('"""A"" IS NULL"', BooleanType(), nullable=True),
                StructField('"""A"" IS NOT NULL"', BooleanType(), nullable=True),
                StructField('"""A"" = \'NAN\'"', BooleanType(), nullable=True),
                StructField('"NOT ""A"" IS NULL"', BooleanType(), nullable=True),
            ]
        )
    )


def test_literal(session):
    df = session.create_dataframe(
        [[1]], schema=StructType([StructField("a", LongType(), nullable=False)])
    )
    df = df.select(lit("lit_value"))
    assert repr(df.schema) == repr(
        StructType([StructField("\"'LIT_VALUE'\"", StringType(9), nullable=False)])
    )


def test_string_op_bool(session):
    df = session.create_dataframe([["value"]], schema=["a"])
    df = df.select(df["a"].like("v%"), df["a"].regexp("v"), df["a"].regexp("v", "c"))
    assert repr(df.schema) == repr(
        StructType(
            [
                StructField('"""A"" LIKE \'V%\'"', BooleanType(), nullable=True),
                StructField('"""A"" REGEXP \'V\'"', BooleanType(), nullable=True),
                StructField(
                    '"RLIKE(""A"", \'V\', \'C\')"', BooleanType(), nullable=True
                ),
            ]
        )
    )


def test_filter(session):
    df = session.create_dataframe(
        [[1, 1.1, 2.2, 3.3]],
        schema=StructType(
            [
                StructField("a", LongType(), nullable=False),
                StructField("b", DecimalType(3, 1), nullable=False),
                StructField("c", DoubleType(), nullable=False),
                StructField("d", DecimalType(4, 2), nullable=False),
            ]
        ),
    )

    df1 = df.filter(df["a"] > 1).filter(df["b"] > 1)
    assert repr(df1.schema) == repr(df.schema)


def test_sort(session):
    df = session.create_dataframe(
        [[1, 1.1, 2.2, 3.3]],
        schema=StructType(
            [
                StructField("a", LongType(), nullable=False),
                StructField("b", DecimalType(3, 1), nullable=False),
                StructField("c", DoubleType(), nullable=False),
                StructField("d", DecimalType(4, 2), nullable=False),
            ]
        ),
    )

    df1 = df.sort(df["a"].asc_nulls_last())
    assert repr(df1.schema) == repr(df.schema)


def test_limit(session):
    df = session.create_dataframe(
        [[1, 1.1, 2.2, 3.3]],
        schema=StructType(
            [
                StructField("a", LongType(), nullable=False),
                StructField("b", DecimalType(3, 1), nullable=False),
                StructField("c", DoubleType(), nullable=False),
                StructField("d", DecimalType(4, 2), nullable=False),
            ]
        ),
    )

    df1 = df.limit(5)
    assert repr(df1.schema) == repr(df.schema)


def test_chain_filter_sort_limit(session):
    df = session.create_dataframe(
        [[1, 1.1, 2.2, 3.3]],
        schema=StructType(
            [
                StructField("a", LongType(), nullable=False),
                StructField("b", DecimalType(3, 1), nullable=False),
                StructField("c", DoubleType(), nullable=False),
                StructField("d", DecimalType(4, 2), nullable=False),
            ]
        ),
    )
    df1 = (
        df.filter(df["a"] > 1)
        .filter(df["b"] > 1)
        .sort(df["a"].asc_nulls_last())
        .limit(5)
    )
    assert repr(df1.schema) == repr(df.schema)


def test_join_basic(session):
    df = session.create_dataframe(
        [[1, 1.1, 2.2, 3.3]],
        schema=["a", "b", "c"],
    )
    df2 = session.create_dataframe(
        [[1, 1.1, 2.2, 3.3]],
        schema=["a", "b", "c"],
    )
    df3 = df.join(df2, lsuffix="_l", rsuffix="_r")
    assert repr(df3.schema) == repr(
        StructType(
            [
                StructField("A_L", LongType(), nullable=False),
                StructField("B_L", DoubleType(), nullable=False),
                StructField("C_L", DoubleType(), nullable=False),
                StructField("_4_L", DoubleType(), nullable=False),
                StructField("A_R", LongType(), nullable=False),
                StructField("B_R", DoubleType(), nullable=False),
                StructField("C_R", DoubleType(), nullable=False),
                StructField("_4_R", DoubleType(), nullable=False),
            ]
        )
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql not supported by local testing mode",
)
@pytest.mark.parametrize(
    "massive_number, precision", [("9" * 38, 38), ("5" * 20, 20), ("7" * 10, 10)]
)
def test_numeric_type_store_precision_and_scale(session, massive_number, precision):
    table_name = Utils.random_table_name()
    try:
        with patch.object(context, "_store_precision_and_scale_in_numeric_type", True):
            df = session.create_dataframe(
                [Decimal(massive_number)],
                StructType(
                    [StructField("large_value", DecimalType(precision, 0), True)]
                ),
            )
            df.write.save_as_table(table_name, mode="overwrite", table_type="temp")
            result = session.sql(f"select * from {table_name}")
            datatype = result.schema.fields[0].datatype
            assert isinstance(datatype, LongType)
            assert datatype._precision == 38 and datatype._scale == 0
    finally:
        session.sql(f"drop table {table_name}").collect()


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="relaxed_types not supported by local testing mode",
)
@pytest.mark.parametrize("massive_number", ["9" * 38, "5" * 20, "7" * 10])
def test_numeric_type_store_precision_and_scale_read_file(session, massive_number):
    stage_name = Utils.random_stage_name()
    header = ("BIG_NUM",)
    test_data = [(massive_number,)]

    def write_csv(data):
        with tempfile.NamedTemporaryFile(
            mode="w+",
            delete=False,
            suffix=".csv",
            newline="",
        ) as file:
            writer = csv.writer(file)
            writer.writerow(header)
            for row in data:
                writer.writerow(row)
            return file.name

    file_path = write_csv(test_data)

    try:
        with patch.object(context, "_store_precision_and_scale_in_numeric_type", True):
            Utils.create_stage(session, stage_name, is_temporary=True)
            result = session.file.put(
                file_path, f"@{stage_name}", auto_compress=False, overwrite=True
            )

            # Infer schema from only the short file
            constrained_reader = session.read.options(
                {
                    "INFER_SCHEMA": True,
                    "INFER_SCHEMA_OPTIONS": {"FILES": [result[0].target]},
                    "PARSE_HEADER": True,
                    # Only load the short file
                    "PATTERN": f".*{result[0].target}",
                }
            )

            # df1 uses constrained types
            df1 = constrained_reader.csv(f"@{stage_name}/")
            datatype = df1.schema.fields[0].datatype
            assert isinstance(datatype, LongType)
            assert datatype._precision == 38 and datatype._scale == 0

    finally:
        Utils.drop_stage(session, stage_name)
        if os.path.exists(file_path):
            os.remove(file_path)
