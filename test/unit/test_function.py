#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

import inspect
from typing import Union

import pytest

from snowflake.snowpark import Column
from snowflake.snowpark._internal.sp_expressions import (
    NamedArgumentsTableFunction as SPNamedArgumentsTableFunction,
    TableFunction as SPTableFunction,
)
from snowflake.snowpark.functions import (
    _create_table_function_expression,
    get_ignore_case,
    get_path,
    lit,
    object_keys,
    typeof,
    xmlget,
)


@pytest.mark.parametrize(
    "func", [xmlget, typeof, get_ignore_case, object_keys, get_path]
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
        with pytest.raises(TypeError) as ex_info:
            func(*param_values)
        assert f"'{func.__name__.upper()}' expected Column or str, got: {int}" in str(
            ex_info
        )


def test_create_table_function_expression_args():
    function_expression = _create_table_function_expression(
        "func_name", lit("v1"), lit("v2")
    )
    assert isinstance(function_expression, SPTableFunction)
    assert function_expression.func_name == "func_name"
    assert [arg.value for arg in function_expression.args] == [
        "v1",
        "v2",
    ]  # arg is Literal


def test_create_table_function_expression_named_args():
    function_expression = _create_table_function_expression(
        "func_name", arg_a=lit("v1"), arg_b=lit("v2")
    )
    assert isinstance(function_expression, SPNamedArgumentsTableFunction)
    assert function_expression.func_name == "func_name"
    assert [(key, arg.value) for key, arg in function_expression.args.items()] == [
        ("arg_a", "v1"),
        ("arg_b", "v2"),
    ]


def test_create_table_function_expression_named_wrong_params():
    with pytest.raises(ValueError) as ve:
        _create_table_function_expression("func_name", lit("v1"), argb=lit("v2"))
    assert (
        "A table function shouldn't have both args and named args" == ve.value.args[0]
    )


def test_create_table_function_expression_named_wrong_table_name():
    with pytest.raises(TypeError) as ve:
        _create_table_function_expression(1)
    assert (
        "The table function name should be a str or a list of strs." == ve.value.args[0]
    )
