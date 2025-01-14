#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import difflib
import functools
import math
from typing import List

from snowflake.snowpark._internal.utils import experimental
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.row import Row
from snowflake.snowpark.types import StructType, _FractionalType, _IntegralType

ACTUAL_EXPECTED_STRING = "--- actual ---\n+++ expected +++"


def _get_sorted_rows(rows: List[Row]) -> List[Row]:
    def compare_rows(row1, row2):
        for value1, value2 in zip(row1, row2):
            if value1 == value2:
                continue
            if value1 is None:
                return -1
            elif value2 is None:
                return 1
            elif value1 > value2:
                return 1
            elif value1 < value2:
                return -1
        return 0

    sort_key = functools.cmp_to_key(compare_rows)
    return sorted(rows, key=sort_key)


def _assert_schema_equal(
    actual: StructType,
    expected: StructType,
):
    """
    Asserts whether two :class:`types.StructType` objects are the same.
    """
    assert len(actual.fields) == len(
        expected.fields
    ), f"Different number of columns: actual has {len(actual.fields)} columns, expected has {len(expected.fields)} columns"

    for column_index, (actual_field, expected_field) in enumerate(
        zip(actual.fields, expected.fields)
    ):
        error_message = None
        if actual_field.name != expected_field.name:
            error_message = f"Column name mismatch at column {column_index}: actual {actual_field.name}, expected {expected_field.name}"
        if actual_field.datatype != expected_field.datatype:
            if not (
                (
                    isinstance(actual_field.datatype, _IntegralType)
                    and isinstance(expected_field, _IntegralType)
                )
                or (
                    isinstance(actual_field.datatype, _FractionalType)
                    and isinstance(expected_field, _FractionalType)
                )
            ):
                error_message = f"Column data type mismatch at column {column_index}: actual {actual_field.datatype}, expected {expected_field.datatype}"
        if actual_field.nullable != expected_field.nullable:
            error_message = f"Column nullable mismatch at column {column_index}: actual {actual_field.nullable}, expected {expected_field.nullable}"
        if error_message:
            actual_str = str(actual)
            expected_str = str(expected)
            if actual_str != expected_str:
                diff = difflib.ndiff(actual_str.splitlines(), expected_str.splitlines())
                diff_str = "\n".join(diff)
                raise AssertionError(
                    f"{error_message}\nDifferent schema:\n{ACTUAL_EXPECTED_STRING}\n{diff_str}"
                )


@experimental(version="1.21.0")
def assert_dataframe_equal(
    actual: DataFrame,
    expected: DataFrame,
    rtol: float = 1e-5,
    atol: float = 1e-8,
) -> None:
    """
    Asserts that two Snowpark :class:`DataFrame` objects are equal. This function compares both the schema and the data
    of the DataFrames. If there are differences, an ``AssertionError`` is raised with a detailed message including differences.
    This function is useful for unit testing and validating data transformations and processing in Snowpark.

    Args:
        actual: The actual DataFrame to be compared.
        expected: The expected DataFrame to compare against.
        rtol: The relative tolerance for comparing float values. Default is 1e-5.
        atol: The absolute tolerance for comparing float values. Default is 1e-8.

    Examples::

        >>> from snowflake.snowpark.testing import assert_dataframe_equal
        >>> from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType, DoubleType
        >>> schema1 = StructType([
        ...     StructField("id", IntegerType()),
        ...     StructField("name", StringType()),
        ...     StructField("value", DoubleType())
        ... ])
        >>> data1 = [[1, "Rice", 1.0], [2, "Saka", 2.0], [3, "White", 3.0]]
        >>> df1 = session.create_dataframe(data1, schema1)
        >>> df2 = session.create_dataframe(data1, schema1)
        >>> assert_dataframe_equal(df2, df1)  # pass, DataFrames are identical

        >>> data2 = [[2, "Saka", 2.0], [1, "Rice", 1.0], [3, "White", 3.0]]  # change the order
        >>> df3 = session.create_dataframe(data2, schema1)
        >>> assert_dataframe_equal(df3, df1)  # pass, DataFrames are identical

        >>> data3 = [[1, "Rice", 1.0], [2, "Saka", 2.0], [4, "Rowe", 4.0]]
        >>> df4 = session.create_dataframe(data3, schema1)
        >>> assert_dataframe_equal(df4, df1)  # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        AssertionError: Value mismatch on row 2 at column 0: actual 4, expected 3
        Different row:
        --- actual ---
        +++ expected +++
        - Row(ID=4, NAME='Rowe', VALUE=4.0)
        ?        ^        ^^^          ^

        + Row(ID=3, NAME='White', VALUE=3.0)
        ?        ^        ^^^^          ^

        >>> data4 = [[1, "Rice", 1.0], [2, "Saka", 2.0], [3, "White", 3.0001]]
        >>> df5 = session.create_dataframe(data4, schema1)
        >>> assert_dataframe_equal(df5, df1, atol=1e-3)  # pass, DataFrames are identical due to higher error tolerance
        >>> assert_dataframe_equal(df5, df1, atol=1e-5)  # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        AssertionError: Value mismatch on row 2 at column 2: actual 3.0001, expected 3.0
        Different row:
        --- actual ---
        +++ expected +++
        - Row(ID=3, NAME='White', VALUE=3.0001)
        ?                                  ---

        + Row(ID=3, NAME='White', VALUE=3.0)

        >>> schema2 = StructType([
        ...     StructField("id", IntegerType()),
        ...     StructField("key", StringType()),
        ...     StructField("value", DoubleType())
        ... ])
        >>> df6 = session.create_dataframe(data1, schema2)
        >>> assert_dataframe_equal(df6, df1)  # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        AssertionError: Column name mismatch at column 1: actual KEY, expected NAME
        Different schema:
        --- actual ---
        +++ expected +++
        - StructType([StructField('ID', LongType(), nullable=True), StructField('KEY', StringType(), nullable=True), StructField('VALUE', DoubleType(), nullable=True)])
        ?                                                                        ^ -

        + StructType([StructField('ID', LongType(), nullable=True), StructField('NAME', StringType(), nullable=True), StructField('VALUE', DoubleType(), nullable=True)])
        ?

        >>> schema3 = StructType([
        ...     StructField("id", IntegerType()),
        ...     StructField("name", StringType()),
        ...     StructField("value", IntegerType())
        ... ])
        >>> df7 = session.create_dataframe(data1, schema3)
        >>> assert_dataframe_equal(df7, df1)  # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        AssertionError: Column data type mismatch at column 2: actual LongType(), expected DoubleType()
        Different schema:
        --- actual ---
        +++ expected +++
        - StructType([StructField('ID', LongType(), nullable=True), StructField('NAME', StringType(), nullable=True), StructField('VALUE', LongType(), nullable=True)])
        ?                                                                                                                                  ^ ^^

        + StructType([StructField('ID', LongType(), nullable=True), StructField('NAME', StringType(), nullable=True), StructField('VALUE', DoubleType(), nullable=True)])
        ?

    Note:
        1. Data in a Snowpark DataFrame is unordered, so when comparing two DataFrames, this function
        sorts rows based on their values first.

        2. When comparing schemas, :class:`types.IntegerType` and :class:`types.DoubleType` are considered different,
        even if the underlying values are equal (e.g., 2 vs 2.0).

    """
    if not isinstance(actual, DataFrame):
        raise TypeError("actual must be a Snowpark DataFrame")
    if not isinstance(expected, DataFrame):
        raise TypeError("expected must be a Snowpark DataFrame")

    actual_schema = actual.schema
    expected_schema = expected.schema
    _assert_schema_equal(actual_schema, expected_schema)

    actual_rows = _get_sorted_rows(actual.collect())
    expected_rows = _get_sorted_rows(expected.collect())
    assert len(actual_rows) == len(
        expected_rows
    ), f"Different number of rows: actual has {len(actual_rows)} rows, expected has {len(expected_rows)} rows"

    for row_index, (actual_row, expected_row) in enumerate(
        zip(actual_rows, expected_rows)
    ):
        for column_index, (actual_value, expected_value) in enumerate(
            zip(actual_row, expected_row)
        ):
            error_message = f"Value mismatch on row {row_index} at column {column_index}: actual {actual_value}, expected {expected_value}"
            failed = False
            if isinstance(expected_value, float):
                if math.isnan(actual_value) != math.isnan(expected_value):
                    failed = True
                if not math.isclose(
                    actual_value, expected_value, rel_tol=rtol, abs_tol=atol
                ):
                    failed = True
            else:
                failed = bool(actual_value != expected_value)
            if failed:
                actual_row_str = str(actual_row)
                expected_row_str = str(expected_row)
                if actual_row_str != expected_row_str:
                    diff = difflib.ndiff(
                        actual_row_str.splitlines(), expected_row_str.splitlines()
                    )
                    diff_str = "\n".join(diff)
                    raise AssertionError(
                        f"{error_message}\nDifferent row:\n{ACTUAL_EXPECTED_STRING}\n{diff_str}"
                    )


assertDataFrameEqual = assert_dataframe_equal
