#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

import inspect
from typing import Union

import pytest

from snowflake.snowpark import Column
from snowflake.snowpark.functions import (
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
def test_typeof_negative(func):
    signature = inspect.signature(func)
    params = list(signature.parameters.values())
    for i in range(len(params)):  # Test the i-th parameter for every iteration
        param_values = [1] * len(params)
        for j in range(len(params)):
            if i != j and params[j].annotation == Union[Column, str]:
                param_values[j] = lit(1)  # pass a value of type Column
        with pytest.raises(TypeError) as ex_info:
            func(*param_values)
        assert f"'{func.__name__.upper()}' expected Column or str, got: {int}" in str(
            ex_info
        )
