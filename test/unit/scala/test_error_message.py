#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

import pytest

from snowflake.snowpark.internal.error_message import SnowparkClientExceptionMessages


def test_df_join_invalid_join_type():
    type1 = "inner"
    types = "outer, left, right"
    ex = SnowparkClientExceptionMessages.DF_JOIN_INVALID_JOIN_TYPE(type1, types)
    assert ex.error_code == "0116"
    assert (
        ex.message
        == f"Unsupported join type '{type1}'. Supported join types include: {types}."
    )


def test_df_join_invalid_natural_join_type():
    tpe = "inner"
    ex = SnowparkClientExceptionMessages.DF_JOIN_INVALID_NATURAL_JOIN_TYPE(tpe)
    assert ex.error_code == "0117"
    assert ex.message == f"Unsupported natural join type '{tpe}'."


def test_df_join_invalid_using_join_type():
    tpe = "inner"
    ex = SnowparkClientExceptionMessages.DF_JOIN_INVALID_USING_JOIN_TYPE(tpe)
    assert ex.error_code == "0118"
    assert ex.message == f"Unsupported using join type '{tpe}'."


def test_plan_sampling_need_one_parameter():
    ex = SnowparkClientExceptionMessages.PLAN_SAMPLING_NEED_ONE_PARAMETER()
    assert ex.error_code == "0303"
    assert (
        ex.message
        == "You must specify either the fraction of rows or the number of rows to sample."
    )


def test_plan_python_report_unexpected_alias():
    ex = SnowparkClientExceptionMessages.PLAN_PYTHON_REPORT_UNEXPECTED_ALIAS()
    assert ex.error_code == "0308"
    assert (
        ex.message
        == "You can only define aliases for the root Columns in a DataFrame returned by "
        "select() and agg(). You cannot use aliases for Columns in expressions."
    )


def test_plan_python_report_invalid_id():
    name = "C1"
    ex = SnowparkClientExceptionMessages.PLAN_PYTHON_REPORT_INVALID_ID(name)
    assert ex.error_code == "0309"
    assert (
        ex.message
        == f'The column specified in df("{name}") is not present in the output of the DataFrame.'
    )


def test_plan_report_join_ambiguous():
    column = "A"
    c1 = column
    c2 = column
    ex = SnowparkClientExceptionMessages.PLAN_PYTHON_REPORT_JOIN_AMBIGUOUS(c1, c2)
    assert ex.error_code == "0310"
    assert (
        ex.message == f"The reference to the column '{c1}' is ambiguous. The column is "
        f"present in both DataFrames used in the join. To identify the "
        f"DataFrame that you want to use in the reference, use the syntax "
        f'<df>("{c2}") in join conditions and in select() calls on the '
        f"result of the join."
    )
