#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
File containing BasePandasDataset APIs defined in Snowpark pandas but not the Modin API layer.
"""

from modin.pandas.api.extensions import register_base_accessor


@register_base_accessor(name="__array_function__", backend="Snowflake")
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

@register_base_accessor(name="__switcheroo__", backend="Snowflake")
def __switcheroo__(self, inplace=False, operation=""):
    # look up available;
    # lookup stay cost
    # for each backend we look up the cost_to; and compare
    me = self
    from modin.core.storage_formats.pandas.native_query_compiler import NativeQueryCompiler
    cost_to = self._get_query_compiler().move_to_cost(NativeQueryCompiler, "", operation)
    # figure out if this needs to be a standard API
    cost_from = self._get_query_compiler().stay_cost(NativeQueryCompiler, "", operation) 
    if cost_to < cost_from:
        the_new_me_maybe = self.move_to("Pandas", inplace=inplace)
        if inplace:
            return self
        else:
            return the_new_me_maybe
    return self