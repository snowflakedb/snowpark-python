#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
File containing BasePandasDataset APIs defined in Snowpark pandas but not the Modin API layer.
"""

from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)
from .base_overrides import register_base_override_with_telemetry
from snowflake.snowpark.modin.plugin.extensions.utils import is_autoswitch_enabled


@register_base_override_with_telemetry(name="__array_function__")
def __array_function__(self, func: callable, types: tuple, args: tuple, kwargs: dict):
    """
    Apply the `func` to the `BasePandasDataset`.

    Parameters
    ----------
    func : np.func
        The NumPy func to apply.
    types : tuple
        The types of the args.
    args : tuple
        The args to the func.
    kwargs : dict
        Additional keyword arguments.

    Returns
    -------
    BasePandasDataset
        The result of the ufunc applied to the `BasePandasDataset`.
    """
    from snowflake.snowpark.modin.plugin.utils.numpy_to_pandas import (
        numpy_to_pandas_func_map,
    )

    if func.__name__ in numpy_to_pandas_func_map:
        return numpy_to_pandas_func_map[func.__name__](*args, **kwargs)
    else:
        # per NEP18 we raise NotImplementedError so that numpy can intercept
        return NotImplemented  # pragma: no cover


@register_base_override_with_telemetry(name="__switcheroo__")
def __switcheroo__(self, inplace=False, operation=""):
    if not is_autoswitch_enabled():
        return self
    from modin.core.storage_formats.pandas.native_query_compiler import (
        NativeQueryCompiler,
    )

    cost_to_move = self._get_query_compiler().move_to_cost(
        NativeQueryCompiler, "", operation
    )

    # figure out if this needs to be a standard API
    cost_to_stay = self._get_query_compiler().stay_cost(
        NativeQueryCompiler, "", operation
    )

    # prototype explain
    import modin.pandas as pd

    row_estimate = SnowflakeQueryCompiler._get_rows(self._get_query_compiler())
    import inspect

    stack = inspect.stack()
    frame_before_snowpandas = None
    location = "<unknown>"
    for i, f in enumerate(reversed(stack)):
        if f.filename is None:
            continue
        if "snowpark" in f.filename or "modin" in f.filename:
            break
        else:
            frame_before_snowpandas = f
    if (
        frame_before_snowpandas is not None
        and frame_before_snowpandas.code_context is not None
    ):
        location = frame_before_snowpandas.code_context[0].replace("\n", "")
    pd.add_switcheroo_log(
        location,
        operation,
        "Snowflake",
        row_estimate,
        cost_to_stay,
        cost_to_move,
        "Pandas" if cost_to_move < cost_to_stay else "Snowflake",
    )

    if cost_to_move < cost_to_stay:
        the_new_me_maybe = self.move_to("Pandas", inplace=inplace)
        if inplace:
            return self
        else:
            return the_new_me_maybe
    return self
