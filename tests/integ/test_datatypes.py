#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import pytest

from snowflake.snowpark import DataFrame, Session
from snowflake.snowpark.functions import lit
from snowflake.snowpark.mock.connection import MockServerConnection
from snowflake.snowpark.types import (
    BooleanType,
    DecimalType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


@pytest.fixture(scope="module")
def session1():
    return Session(MockServerConnection())


def test_basic_filter(session1):
    df: DataFrame = session1.create_dataframe(
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
                StructField("C", StringType(16777216), nullable=False),
            ]
        )
    )


def test_plus_basic(session1):
    df = session1.create_dataframe(
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


def test_minus_basic(session1):
    df = session1.create_dataframe(
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


def test_multiple_basic(session1):
    df = session1.create_dataframe(
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


def test_divide_basic(session1):
    df = session1.create_dataframe(
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


def test_modulo_basic(session1):
    df = session1.create_dataframe(
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


def test_binary_ops_bool(session1):
    df = session1.create_dataframe(
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


def test_unary_ops_bool(session1):
    df = session1.create_dataframe(
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


def test_literal(session1):
    df = session1.create_dataframe(
        [[1]], schema=StructType([StructField("a", LongType(), nullable=False)])
    )
    df = df.select(lit("lit_value"))
    assert repr(df.schema) == repr(
        StructType([StructField("\"'LIT_VALUE'\"", StringType(9), nullable=False)])
    )


def test_string_op_bool(session1):
    df = session1.create_dataframe([["value"]], schema=["a"])
    df = df.select(df["a"].like("v%"), df["a"].regexp("v"))
    assert repr(df.schema) == repr(
        StructType(
            [
                StructField('"""A"" LIKE \'V%\'"', BooleanType(), nullable=True),
                StructField('"""A"" REGEXP \'V\'"', BooleanType(), nullable=True),
            ]
        )
    )


@pytest.mark.skip("Cast is not implemented yet.")
def test_cast(session1):
    ...


@pytest.mark.skip("In expression is not implemented yet.")
def test_in_expression(session1):
    ...


def test_filter(session1):
    df = session1.create_dataframe(
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


def test_sort(session1):
    df = session1.create_dataframe(
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


def test_limit(session1):
    df = session1.create_dataframe(
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


def test_chain_filter_sort_limit(session1):
    df = session1.create_dataframe(
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


def test_join_basic(session1):
    df = session1.create_dataframe(
        [[1, 1.1, 2.2, 3.3]],
        schema=["a", "b", "c"],
    )
    df2 = session1.create_dataframe(
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
