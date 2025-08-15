#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import Any, Hashable, Optional, Union

import modin.pandas as pd
from modin.pandas.base import BasePandasDataset
from modin.pandas.utils import is_scalar
import numpy as np

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


def unique_mapper(
    ar: Union[pd.DataFrame, pd.Series],
    return_index: bool = False,
    return_inverse: bool = False,
    return_counts: bool = False,
    axis: Optional[int] = None,
    equal_nan: bool = True,
    sorted: bool = True,
) -> np.ndarray:
    """
    Maps and executes the numpy unique signature to the pandas where signature
    if it can be handled, otherwise returns NotImplemented. No parameters
    are supported beyond the input ar array. A DataFrame will first be stacked
    so that the pd.unique function can be used.

    Numpy np.unique signature:
    Return unique elements in an ndarray, Series, or DataFrame (ar) as a
    sorted ndarray

    Pandas pd.unique signature:
    Return unique elements of a Series as an unsorted ndarray

    Parameters
    ----------
    ar : A modin pandas DataFrame or Series
    return_index : NotImplemented ( returns an ndarray )
    return_inverse : NotImplemented
    return_counts : NotImplemented
    axis : NotImplemented
    equal_nan : NotImplemented
    sorted : NotImplemented

    Returns
    -------
    Returns an ndarray, sorted

    """
    if return_index:
        return NotImplemented
    if return_inverse:
        return NotImplemented
    if return_counts:
        return NotImplemented
    if axis is not None:
        return NotImplemented
    if not equal_nan:
        return NotImplemented
    if not sorted:
        return NotImplemented
    input_values = ar

    # pd.unique does not take DataFrames, so we stack it into a Series.
    if isinstance(input_values, pd.DataFrame):
        input_values = input_values.stack().reset_index(drop=True)

    # pandas.unique and modin.pandas.unique differ from np.unique in
    # the sense that by default, numpy sorts the result. Unlike the
    # where mapper we always return a numpy array the same as we do
    # in pd.unique.
    result_unsorted = pd.unique(input_values)
    result_unsorted.sort()
    return result_unsorted


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
    "unique": unique_mapper,
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
    "negative": lambda obj, inputs: -obj,
    "positive": lambda obj, inputs: obj,
    "power": NotImplemented,  # cannot use obj.__pow__
    "float_power": lambda obj, inputs: obj.__pow__(*inputs),
    "remainder": lambda obj, inputs: obj.__mod__(*inputs),
    "mod": lambda obj, inputs: obj.__mod__(*inputs),
    "fmod": NotImplemented,
    "divmod": NotImplemented,
    "absolute": lambda obj, inputs: obj.abs(),
    "abs": lambda obj, inputs: obj.abs(),
    "rint": NotImplemented,
    "sign": NotImplemented,
    "heaviside": NotImplemented,  # heaviside step function
    "conj": NotImplemented,  # same as conjugate
    "conjugate": NotImplemented,
    "exp": lambda obj, inputs: obj.apply(sp_func.exp),
    "exp2": NotImplemented,
    "log": lambda obj, inputs: obj.apply(sp_func.ln),  # use built-in function
    "log2": lambda obj, inputs: obj.apply(sp_func._log2),
    "log10": lambda obj, inputs: obj.apply(sp_func._log10),
    "expm1": NotImplemented,
    "log1p": NotImplemented,
    "sqrt": lambda obj, inputs: obj.apply(sp_func.sqrt),
    "square": NotImplemented,
    "cbrt": NotImplemented,  # Cube root
    "reciprocal": NotImplemented,
    "gcd": NotImplemented,
    "lcm": NotImplemented,
    # trigonometric functions
    "sin": lambda obj, inputs: obj.apply(sp_func.sin),
    "cos": lambda obj, inputs: obj.apply(sp_func.cos),
    "tan": lambda obj, inputs: obj.apply(sp_func.tan),
    "arcsin": NotImplemented,
    "arccos": NotImplemented,
    "arctan": NotImplemented,
    "arctan2": NotImplemented,
    "hypot": NotImplemented,
    "sinh": lambda obj, inputs: obj.apply(sp_func.sinh),
    "cosh": lambda obj, inputs: obj.apply(sp_func.cosh),
    "tanh": lambda obj, inputs: obj.apply(sp_func.tanh),
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
    "floor": lambda obj, inputs: obj.apply(sp_func.floor),
    "ceil": lambda obj, inputs: obj.apply(sp_func.ceil),
    "trunc": lambda obj, inputs: obj.apply(
        sp_func.trunc
    ),  # df.truncate not supported in snowpandas yet
}


# Map from numpy universal (element-wise) function to Snowflake function.
# This is used to map numpy functions to builtin sql functions when numpy function is
# passed in apply and map methods. For example: df.apply(np.<func>)
# Using native SQL functions instead of creating UDF/UDTF provides significant
# better performance.
NUMPY_UNIVERSAL_FUNCTION_TO_SNOWFLAKE_FUNCTION = {
    # Math operations
    np.absolute: sp_func.abs,
    np.sign: sp_func.sign,
    np.negative: sp_func.negate,
    np.positive: lambda col: col,
    np.sqrt: sp_func.sqrt,
    np.square: lambda col: sp_func.builtin("square")(col),
    np.cbrt: lambda col: sp_func.builtin("cbrt")(col),
    np.reciprocal: lambda col: 1 / col,
    np.exp: sp_func.exp,
    np.exp2: lambda col: sp_func.pow(2, col),
    np.expm1: lambda col: sp_func.exp(col) - 1,
    np.log: sp_func.ln,
    np.log2: sp_func._log2,
    np.log10: sp_func._log10,
    np.log1p: lambda col: sp_func.ln(col + 1),
    # Trigonometric functions
    np.sin: sp_func.sin,
    np.cos: sp_func.cos,
    np.tan: sp_func.tan,
    np.sinh: sp_func.sinh,
    np.cosh: sp_func.cosh,
    np.tanh: sp_func.tanh,
    np.arcsin: lambda col: sp_func.builtin("asin")(col),
    np.arccos: lambda col: sp_func.builtin("acos")(col),
    np.arctan: lambda col: sp_func.builtin("atan")(col),
    np.arctan2: lambda col: sp_func.builtin("atan2")(col),
    np.arcsinh: lambda col: sp_func.builtin("asinh")(col),
    np.arccosh: lambda col: sp_func.builtin("acosh")(col),
    np.arctanh: lambda col: sp_func.builtin("atanh")(col),
    np.degrees: lambda col: sp_func.builtin("degrees")(col),
    np.radians: lambda col: sp_func.builtin("radians")(col),
    # Floating functions
    np.ceil: sp_func.ceil,
    np.floor: sp_func.floor,
    np.trunc: sp_func.trunc,
    np.isnan: sp_func.is_null,
}
