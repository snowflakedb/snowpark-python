#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""
File containing utilities for the extensions API.
"""
from snowflake.snowpark.modin.utils import Fn

cache_result_docstring = """
Persists the current Snowpark pandas {object_name} to a temporary table to improve the latency of subsequent operations.

Args:
    inplace: bool, default False
        Whether to perform the materialization inplace.

Returns:
    Snowpark pandas {object_name} or None
        Cached Snowpark pandas {object_name} or None if ``inplace=True``.

Note:
    - The temporary table produced by this method lasts for the duration of the session.

Examples:
{examples}
"""

cache_result_examples = """
Let's make a {object_name} using a computationally expensive operation, e.g.:
>>> {object_var_name} = {object_creation_call}

Due to Snowpark pandas lazy evaluation paradigm, every time this {object_name} is used, it will be recomputed -
causing every subsequent operation on this {object_name} to re-perform the 30 unions required to produce it.
This makes subsequent operations more expensive. The `cache_result` API can be used to persist the
{object_name} to a temporary table for the duration of the session - replacing the nested 30 unions with a single
read from a table.

>>> new_{object_var_name} = {object_var_name}.cache_result()

>>> import numpy as np

>>> np.all((new_{object_var_name} == {object_var_name}).values)
True

>>> {object_var_name}.reset_index(drop=True, inplace=True) # Slower

>>> new_{object_var_name}.reset_index(drop=True, inplace=True) # Faster

"""


def add_cache_result_docstring(func: Fn) -> Fn:
    """
    Decorator to add docstring to cache_result method.
    """
    # In this case, we are adding the docstring to Series.cache_result.
    if "series" in func.__module__:
        object_name = "Series"
        examples_portion = cache_result_examples.format(
            object_name=object_name,
            object_var_name="series",
            object_creation_call="pd.concat([pd.Series([i]) for i in range(30)])",
        )
    else:
        object_name = "DataFrame"
        examples_portion = cache_result_examples.format(
            object_name=object_name,
            object_var_name="df",
            object_creation_call="pd.concat([pd.DataFrame([range(i, i+5)]) for i in range(0, 150, 5)])",
        )
    func.__doc__ = cache_result_docstring.format(
        object_name=object_name, examples=examples_portion
    )
    return func
