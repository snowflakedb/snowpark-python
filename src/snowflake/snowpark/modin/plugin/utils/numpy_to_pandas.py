#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from typing import Any, Optional, Union

import snowflake.snowpark.modin.pandas as pd
from snowflake.snowpark.modin.pandas.base import BasePandasDataset
from snowflake.snowpark.modin.pandas.utils import is_scalar
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage


def where_mapper(
    cond: Union[bool, pd.DataFrame, pd.Series],
    x: Optional[Union[pd.DataFrame, pd.Series]] = None,
    y: Optional[Union[pd.DataFrame, pd.Series]] = None,
) -> Union[pd.DataFrame, pd.Series]:
    """
    Maps and executes the numpy where signature to the pandas where signature
    if it can be handled, otherwise returns NotImplemented. This implementation
    has several implementation differences:
    * Column

    Numpy Signature:
    Return elements chosen from x or y depending on condition.

    Parameters
    ----------
    cond : condition, array_like, bool, or Modin Query Compiler
        Where True, yield x, otherwise yield y.
    x,y : array_like, or Modin Query Compiler

    Returns
    -------
    Returns a DataFrame, Series or NotImplemented if can't support the
    operation and we want numpy to continue looking for implementations

    """

    # We check the shape of the input objects first, even before checking
    # to see if we have a boolean cond because this addresses broadcasting
    # differences with numpy and vanilla pandas. Unfortunately we cannot
    # check the index length efficiently.
    def is_same_shape(
        cond: Union[bool, pd.DataFrame, pd.Series],
        x: Optional[Union[pd.DataFrame, pd.Series]],
        y: Optional[Union[pd.DataFrame, pd.Series]],
    ) -> bool:
        inputs = [cond, x, y]
        shape_data = []
        for obj in inputs:
            if hasattr(obj, "_query_compiler"):
                curr_df = hasattr(obj, "columns")
                curr_num_cols = len(obj.columns) if curr_df else None  # type: ignore
                cols = tuple(obj.columns) if curr_df else None  # type: ignore
                shape_data.append((curr_df, curr_num_cols, cols))
        return len(set(shape_data)) == 1

    if not is_same_shape(cond, x, y):
        WarningMessage.mismatch_with_pandas(
            "np.where",
            "Using np.where with Snowpark pandas requires objects of the same shape and columns",
        )
        # Return sentinel value to tell np.where to keep looking for an
        # implementation
        return NotImplemented

    # conditional boolean cases
    if cond is True:
        return x
    if cond is False:
        return y

    # Detect whether the input condition
    if hasattr(cond, "_query_compiler"):
        WarningMessage.mismatch_with_pandas(
            "np.where",
            "Returns a Snowpark pandas object instead of a np array",
        )
        # handles np.where(df1, df2, df3)
        if hasattr(x, "_query_compiler") and hasattr(y, "_query_compiler"):
            return x.where(cond, y)  # type: ignore

        # handles np.where(df1, df2, scalar)
        if hasattr(x, "_query_compiler") and is_scalar(y):
            # no need to broadcast y
            return x.where(cond, y)  # type: ignore

        if is_scalar(x):
            # broadcast scalar x to size of cond
            object_shape = cond.shape
            if len(object_shape) == 1:
                df_scalar = pd.Series(x, index=range(object_shape[0]))
            elif len(object_shape) == 2:
                df_scalar = pd.DataFrame(
                    x, index=range(object_shape[0]), columns=range(object_shape[1])
                )

            # handles np.where(df, scalar1, scalar2)
            # handles np.where(df1, scalar, df2)
            return df_scalar.where(cond, y)
    # return the sentinel NotImplemented if we do not support this function
    return NotImplemented


# We also need to convert everything to booleans, since numpy will
# do this implicitly on logical operators and pandas does not.
def map_to_bools(inputs: Any) -> Any:
    return (v.astype("bool") if isinstance(v, BasePandasDataset) else v for v in inputs)


# Map that associates a numpy general function (np.where) with
# an associated pandas function (pd.where) using a mapping function
# (where_mapper) which can adapt differing function signatures. These
# functions are called by numpy
numpy_to_pandas_func_map = {"where": where_mapper}

# Map that associates a numpy universal function name that operates on
# ndarrays in an element by element fashion with a lambda which performs
# the same function on the input type. These functions are called from
# the __array_ufunc__ function
# https://numpy.org/doc/stable/reference/ufuncs.html
numpy_to_pandas_universal_func_map = {
    "add": lambda obj, inputs, kwargs: obj.__add__(*inputs, **kwargs),
    "logical_and": lambda obj, inputs, kwargs: obj.astype("bool").__and__(
        *map_to_bools(inputs), **kwargs
    ),
    "logical_or": lambda obj, inputs, kwargs: obj.astype("bool").__or__(
        *map_to_bools(inputs), **kwargs
    ),
    "logical_not": lambda obj, inputs, kwargs: ~obj.astype("bool"),
    "logical_xor": lambda obj, inputs, kwargs: obj.astype("bool").__xor__(
        *map_to_bools(inputs), **kwargs
    ),
}
