#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
File containing BasePandasDataset APIs defined in Snowpark pandas but not the Modin API layer.
"""

from .base_overrides import register_base_override
from snowflake.snowpark.modin.plugin._internal.utils import (
    MODIN_IS_AT_LEAST_0_34_0,
)

# TODO delete this file once modin 0.33.x is no longer supported
if not MODIN_IS_AT_LEAST_0_34_0:

    @register_base_override("__array_function__")
    def __array_function__(
        self, func: callable, types: tuple, args: tuple, kwargs: dict
    ):
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
