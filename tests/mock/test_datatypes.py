#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import pytest

from snowflake.snowpark import DataFrame, Session
from snowflake.snowpark.functions import lit
from snowflake.snowpark.mock.mock_connection import MockServerConnection
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

session = Session(MockServerConnection())


def test_basic_filter():
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
                StructField("A", LongType(), nullable=True),
                StructField("B", LongType(), nullable=True),
                StructField("C", StringType(), nullable=True),
            ]
        )
    )


def test_plus_basic():
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
                StructField("NEW_A", DecimalType(38, 0), nullable=False),
                StructField("NEW_B", DecimalType(5, 2), nullable=False),
                StructField("NEW_C", DoubleType(), nullable=False),
            ]
        )
    )


def test_minus_basic():
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
                StructField("NEW_A", DecimalType(38, 0), nullable=False),
                StructField("NEW_B", DecimalType(5, 2), nullable=False),
                StructField("NEW_C", DoubleType(), nullable=False),
            ]
        )
    )


def test_multiple_basic():
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
        (df["a"] * 1).as_("new_a"),
        (df["b"] * df["d"]).as_("new_b"),
        (df["c"] * 3).as_("new_c"),
    )
    assert repr(df.schema) == repr(
        StructType(
            [
                StructField("NEW_A", DecimalType(38, 0), nullable=False),
                StructField("NEW_B", DecimalType(7, 3), nullable=False),
                StructField("NEW_C", DoubleType(), nullable=False),
            ]
        )
    )


def test_divide_basic():
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


def test_modulo_basic():
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
                StructField("NEW_A", DecimalType(38, 0), nullable=False),
                StructField("NEW_B", DecimalType(4, 2), nullable=False),
                StructField("NEW_C", DoubleType(), nullable=False),
            ]
        )
    )


def test_binary_ops_bool():
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
                StructField('GREATERTHAN("A", "B")', BooleanType(), nullable=False),
                StructField(
                    'GREATERTHANOREQUAL("A", "B")', BooleanType(), nullable=False
                ),
                StructField('EQUALTO("A", "B")', BooleanType(), nullable=False),
                StructField('NOTEQUALTO("A", "B")', BooleanType(), nullable=False),
                StructField('LESSTHAN("A", "B")', BooleanType(), nullable=False),
                StructField('LESSTHANOREQUAL("A", "B")', BooleanType(), nullable=False),
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
                    'AND(GREATERTHAN("A", "B"), GREATERTHANOREQUAL("A", "B"))',
                    BooleanType(),
                    nullable=False,
                ),
                StructField(
                    'OR(GREATERTHAN("A", "B"), GREATERTHANOREQUAL("A", "B"))',
                    BooleanType(),
                    nullable=False,
                ),
            ]
        )
    )


def test_unary_ops_bool():
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
                StructField('ISNULL("A")', BooleanType(), nullable=False),
                StructField('ISNOTNULL("A")', BooleanType(), nullable=False),
                StructField('ISNAN("A")', BooleanType(), nullable=False),
                StructField('NOT(ISNULL("A"))', BooleanType(), nullable=False),
            ]
        )
    )


def test_literal():
    df = session.create_dataframe(
        [[1]], schema=StructType([StructField("a", LongType(), nullable=False)])
    )
    df = df.select(lit("lit_value"))
    assert repr(df.schema) == repr(
        StructType([StructField("LITERAL()", StringType(), nullable=False)])
    )


def test_string_op_bool():
    df = session.create_dataframe([["value"]], schema=["a"])
    df = df.select(df["a"].like("v%"), df["a"].regexp("v"))
    assert repr(df.schema) == repr(
        StructType(
            [
                StructField('LIKE("A")', BooleanType(), nullable=True),
                StructField('REGEXP("A")', BooleanType(), nullable=True),
            ]
        )
    )


@pytest.mark.skip("Cast is not implemented yet.")
def test_cast():
    ...


@pytest.mark.skip("In expression is not implemented yet.")
def test_in_expression():
    ...


def test_filter():
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


def test_sort():
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


def test_limit():
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


def test_chain_filter_sort_limit():
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


def test_join_basic():
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
                StructField("A_L", LongType(), nullable=True),
                StructField("B_L", FloatType(), nullable=True),
                StructField("C_L", FloatType(), nullable=True),
                StructField("_4_L", FloatType(), nullable=True),
                StructField("A_R", LongType(), nullable=True),
                StructField("B_R", FloatType(), nullable=True),
                StructField("C_R", FloatType(), nullable=True),
                StructField("_4_R", FloatType(), nullable=True),
            ]
        )
    )
