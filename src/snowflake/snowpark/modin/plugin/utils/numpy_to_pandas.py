#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from typing import Any, Hashable, Optional, Union

import modin.pandas as pd
from modin.pandas.base import BasePandasDataset
from modin.pandas.utils import is_scalar

from snowflake.snowpark import functions as sp_func
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
            if cond.ndim == 1:
                df_cond = cond.to_frame()
            else:
                df_cond = cond.copy()

            origin_columns = df_cond.columns
            # rename the columns of df_cond for ensure no conflict happens when
            # appending new columns
            renamed_columns = [f"col_{i}" for i in range(len(origin_columns))]
            df_cond.columns = renamed_columns
            # broadcast scalar x to size of cond through indexing
            new_columns = [f"new_col_{i}" for i in range(len(origin_columns))]
            df_cond[new_columns] = x

            if cond.ndim == 1:
                df_scalar = df_cond[new_columns[0]]
                df_scalar.name = cond.name
            else:
                df_scalar = df_cond[new_columns]
                # use the same name as the cond dataframe to make sure
                # pandas where happens correctly
                df_scalar.columns = origin_columns

            # handles np.where(df, scalar1, scalar2)
            # handles np.where(df1, scalar, df2)
            return df_scalar.where(cond, y)

    # return the sentinel NotImplemented if we do not support this function
    return NotImplemented


def may_share_memory_mapper(a: Any, b: Any, max_work: Optional[int] = None) -> bool:
    """
    Maps and executes the numpy may_share_memory signature and always
    returns False
    """
    return False


def full_like_mapper(
    a: Union[pd.DataFrame, pd.Series],
    fill_value: Hashable,
    dtype: Optional[Any] = None,
    order: Optional[str] = "K",
    subok: Optional[bool] = True,
    shape: Optional[tuple[Any]] = None,
) -> Union[pd.DataFrame, pd.Series]:
    if not subok:
        return NotImplemented
    if not order == "K":
        return NotImplemented
    if dtype is not None:
        return NotImplemented

    result_shape = shape
    if isinstance(result_shape, tuple) and len(result_shape) == 0:
        result_shape = (1,)
    if isinstance(result_shape, int):
        result_shape = (result_shape,)
    if result_shape is None:
        result_shape = a.shape
    if len(result_shape) == 2:
        height, width = result_shape  # type: ignore
        return pd.DataFrame(fill_value, index=range(height), columns=range(width))
    if len(result_shape) == 1:
        return pd.Series(fill_value, index=range(result_shape[0]))
    return NotImplemented


# We also need to convert everything to booleans, since numpy will
# do this implicitly on logical operators and pandas does not.
def map_to_bools(inputs: Any) -> Any:
    return (v.astype("bool") if isinstance(v, BasePandasDataset) else v for v in inputs)


# Map that associates a numpy general function (np.where) with
# an associated pandas function (pd.where) using a mapping function
# (where_mapper) which can adapt differing function signatures. These
# functions are called by numpy
numpy_to_pandas_func_map = {
    "where": where_mapper,
    "may_share_memory": may_share_memory_mapper,
    "full_like": full_like_mapper,
}

# Map that associates a numpy universal function name that operates on
# ndarrays in an element by element fashion with a lambda which performs
# the same function on the input type. These functions are called from
# the __array_ufunc__ function
#
# Functions which are not implemented are explicitly listed here as
# well to provide the reader with an understanding of the current
# breadth. If a new ufunc is defined by numpy the absence of that
# function from this list also implies that is it NotImplemented
#
# Numpy ufunc Reference
# https://numpy.org/doc/stable/reference/ufuncs.html
numpy_to_pandas_universal_func_map = {
    # Math functions
    "add": lambda obj, inputs: obj.__add__(*inputs),
    "subtract": lambda obj, inputs: obj.__sub__(*inputs),
    "multiply": lambda obj, inputs: obj.__mul__(*inputs),
    "divide": lambda obj, inputs: obj.__truediv__(*inputs),  # same as true_divide
    "logaddexp": NotImplemented,
    "logaddexp2": NotImplemented,
    "true_divide": lambda obj, inputs: obj.__truediv__(*inputs),
    "floor_divide": lambda obj, inputs: obj.__floordiv__(*inputs),
    "negative": NotImplemented,
    "positive": NotImplemented,
    "power": NotImplemented,  # cannot use obj.__pow__
    "float_power": lambda obj, inputs: obj.__pow__(*inputs),
    "remainder": lambda obj, inputs: obj.__mod__(*inputs),
    "mod": lambda obj, inputs: obj.__mod__(*inputs),
    "fmod": NotImplemented,
    "divmod": NotImplemented,
    "absolute": NotImplemented,
    "fabs": NotImplemented,
    "rint": NotImplemented,
    "sign": NotImplemented,
    "heaviside": NotImplemented,  # heaviside step function
    "conj": NotImplemented,  # same as conjugate
    "conjugate": NotImplemented,
    "exp": NotImplemented,
    "exp2": NotImplemented,
    "log": lambda obj, inputs: obj.apply(sp_func.ln),  # use built-in function
    "log2": lambda obj, inputs: obj.apply(sp_func.log, base=2),
    "log10": lambda obj, inputs: obj.apply(sp_func.log, base=10),
    "expm1": NotImplemented,
    "log1p": NotImplemented,
    "sqrt": NotImplemented,
    "square": NotImplemented,
    "cbrt": NotImplemented,  # Cube root
    "reciprocal": NotImplemented,
    "gcd": NotImplemented,
    "lcm": NotImplemented,
    # trigonometric functions
    "sin": NotImplemented,
    "cos": NotImplemented,
    "tan": NotImplemented,
    "arcsin": NotImplemented,
    "arccos": NotImplemented,
    "arctan": NotImplemented,
    "arctan2": NotImplemented,
    "hypot": NotImplemented,
    "sinh": NotImplemented,
    "cosh": NotImplemented,
    "tanh": NotImplemented,
    "arcsinh": NotImplemented,
    "arccosh": NotImplemented,
    "arctanh": NotImplemented,
    "degrees": NotImplemented,
    "radians": NotImplemented,
    "deg2rad": NotImplemented,
    "rad2deg": NotImplemented,
    # bitwise operations
    "bitwise_and": NotImplemented,
    "bitwise_or": NotImplemented,
    "bitwise_xor": NotImplemented,
    "invert": NotImplemented,
    "left_shift": NotImplemented,
    "right_shift": NotImplemented,
    # comparison functions
    "greater": lambda obj, inputs: obj > inputs[0],
    "greater_equal": lambda obj, inputs: obj >= inputs[0],
    "less": lambda obj, inputs: obj < inputs[0],
    "less_equal": lambda obj, inputs: obj <= inputs[0],
    "not_equal": lambda obj, inputs: obj != inputs[0],
    "equal": lambda obj, inputs: obj == inputs[0],
    "logical_and": lambda obj, inputs: obj.astype("bool").__and__(
        *map_to_bools(inputs)
    ),
    "logical_or": lambda obj, inputs: obj.astype("bool").__or__(*map_to_bools(inputs)),
    "logical_not": lambda obj, inputs: ~obj.astype("bool"),
    "logical_xor": lambda obj, inputs: obj.astype("bool").__xor__(
        *map_to_bools(inputs)
    ),
    "maximum": NotImplemented,
    "minimum": NotImplemented,
    "fmax": NotImplemented,
    "fmin": NotImplemented,
    # floating functions
    "isfinite": NotImplemented,
    "isinf": NotImplemented,
    "isnan": NotImplemented,
    "isnat": NotImplemented,
    "fabs": NotImplemented,
    "signbit": NotImplemented,
    "copysign": NotImplemented,
    "nextafter": NotImplemented,
    "spacing": NotImplemented,
    "modf": NotImplemented,
    "ldexp": NotImplemented,
    "frexp": NotImplemented,
    "fmod": NotImplemented,
    "floor": NotImplemented,
    "ceil": NotImplemented,
    "trunc": NotImplemented,
}
