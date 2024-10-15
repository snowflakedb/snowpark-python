#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import inspect
import threading
from typing import Union
from unittest import mock

import pytest

import snowflake.snowpark
from snowflake.snowpark import Column
from snowflake.snowpark._internal.analyzer.table_function import (
    NamedArgumentsTableFunction,
    PosArgumentsTableFunction,
)
from snowflake.snowpark._internal.server_connection import ServerConnection
from snowflake.snowpark.functions import (
    approx_percentile,
    approx_percentile_accumulate,
    approx_percentile_combine,
    approx_percentile_estimate,
    col,
    corr,
    covar_pop,
    covar_samp,
    get_ignore_case,
    get_path,
    lit,
    map,
    object_keys,
    sha2,
    typeof,
    xmlget,
)
from snowflake.snowpark.table_function import _create_table_function_expression
from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType


@pytest.mark.parametrize(
    "func",
    [
        xmlget,
        typeof,
        get_ignore_case,
        object_keys,
        get_path,
        approx_percentile,
        approx_percentile_accumulate,
        approx_percentile_estimate,
        approx_percentile_combine,
        corr,
        covar_pop,
        covar_samp,
    ],
)
def test_funcs_negative(func):
    signature = inspect.signature(func)
    params = list(signature.parameters.values())
    for i in range(
        len(params)
    ):  # Pass an integer (invalid parameter type) as the i-th parameter for every iteration.
        param_values = [1] * len(params)
        for j in range(len(params)):
            if i != j and params[j].annotation == Union[Column, str]:
                param_values[j] = lit(1)  # pass a value of type Column
        if (
            params[j].annotation == Union[Column, str]
        ):  # it should be Column or str, but given 1
            with pytest.raises(TypeError) as ex_info:
                func(*param_values)
            assert (
                f"'{func.__name__.upper()}' expected Column or str, got: {int}"
                in str(ex_info)
            )


def test_create_table_function_expression_args():
    function_expression = _create_table_function_expression(
        "func_name", lit("v1"), lit("v2")
    )
    assert isinstance(function_expression, PosArgumentsTableFunction)
    assert function_expression.func_name == "func_name"
    assert [arg.value for arg in function_expression.args] == [
        "v1",
        "v2",
    ]  # arg is Literal


def test_create_table_function_expression_named_args():
    function_expression = _create_table_function_expression(
        "func_name", arg_a=lit("v1"), arg_b=lit("v2")
    )
    assert isinstance(function_expression, NamedArgumentsTableFunction)
    assert function_expression.func_name == "func_name"
    assert [(key, arg.value) for key, arg in function_expression.args.items()] == [
        ("arg_a", "v1"),
        ("arg_b", "v2"),
    ]


def test_create_table_function_expression_named_wrong_params():
    with pytest.raises(ValueError) as ve:
        _create_table_function_expression("func_name", lit("v1"), argb=lit("v2"))
    assert (
        "A table function shouldn't have both args and named args." == ve.value.args[0]
    )


def test_create_table_function_expression_named_wrong_table_name():
    with pytest.raises(TypeError) as ve:
        _create_table_function_expression(1)
    assert (
        "'func' should be a function name in str, a list of strs that have all or a part of the fully qualified name, or a TableFunctionCall instance."
        == ve.value.args[0]
    )


def test_sha2_negative():
    num_bits = 2
    with pytest.raises(
        ValueError, match=f"num_bits {num_bits} is not in the permitted values"
    ):
        sha2(col("a"), num_bits)


def test_functions_alias():
    import snowflake.snowpark.functions as functions

    assert functions.substr == functions.substring
    assert functions.count_distinct == functions.countDistinct
    assert functions.collect_set == functions.array_unique_agg
    assert functions.to_char == functions.to_varchar
    assert functions.function == functions.builtin
    assert functions.call_function == functions.call_builtin
    assert functions.expr == functions.sql_expr
    assert functions.monotonically_increasing_id == functions.seq8
    assert functions.from_unixtime == functions.to_timestamp


def test_map():
    mock_connection = mock.create_autospec(ServerConnection)
    mock_connection._conn = mock.MagicMock()
    session = snowflake.snowpark.session.Session(mock_connection)
    session._packages = {
        "snowflake-snowpark-python": "snowflake-snowpark-python",
    }
    session._package_lock = threading.RLock()

    def generated_udtf(*args, **kwargs):
        return lambda *arguments: args[0]()

    with mock.patch(
        "snowflake.snowpark.DataFrame.join_table_function",
        return_value=mock.MagicMock(),
    ) as join_table_func_mock, mock.patch(
        "snowflake.snowpark.DataFrame.columns", new_callable=mock.PropertyMock
    ) as columns, mock.patch(
        "snowflake.snowpark.udtf.UDTFRegistration.register", side_effect=generated_udtf
    ) as register_mock:
        columns.return_value = ["i", "n"]
        df1 = session.create_dataframe(
            [[1, "x"], [3, None]],
            schema=StructType(
                [StructField("i", IntegerType()), StructField("n", StringType())]
            ),
        )
        columns.return_value = ["i", "n"]
        map(df1, lambda row: row[1], output_types=[StringType()])

        assert register_mock.call_count == 1
        handler = join_table_func_mock.mock_calls[0].args[0]
        assert list(handler.process(1, "x")) == [("x",)]

        df2 = session.create_dataframe(
            [["x"], ["y"]], schema=StructType([StructField("C_1", StringType())])
        )
        columns.return_value = ["C_1"]
        map(df2, lambda r: (r, r), output_types=[StringType()], wrap_row=False)

        assert register_mock.call_count == 2
        handler = join_table_func_mock.mock_calls[1].args[0]
        assert list(handler.process("x")) == [("x", "x")]

        columns.return_value = ["i", "n"]
        map(df1, lambda r: snowflake.snowpark.Row(r[1], 1), output_types=[StringType()])

        assert register_mock.call_count == 3
        handler = join_table_func_mock.mock_calls[2].args[0]
        assert list(handler.process(1, "x")) == [("x", 1)]
