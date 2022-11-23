#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import inspect
from typing import Union

import pytest

from snowflake.snowpark import Column
from snowflake.snowpark._internal.analyzer.table_function import (
    NamedArgumentsTableFunction,
    PosArgumentsTableFunction,
)
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
    object_keys,
    sha2,
    typeof,
    xmlget,
)
from snowflake.snowpark.table_function import _create_table_function_expression


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
    assert functions.to_char == functions.to_varchar
    assert functions.function == functions.builtin
    assert functions.call_function == functions.call_builtin
    assert functions.expr == functions.sql_expr
    assert functions.date_format == functions.to_date
    assert functions.first == functions.any_value
    assert functions.create_map == functions.object_construct
    assert functions.map_keys == functions.object_keys
    assert functions.monotonically_increasing_id == functions.seq8
    assert functions.from_unixtime == functions.to_timestamp
