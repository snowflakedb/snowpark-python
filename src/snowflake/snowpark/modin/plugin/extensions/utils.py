#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
File containing utilities for the extensions API.
"""
from __future__ import annotations

from collections.abc import Hashable, Sequence
from types import BuiltinFunctionType
from typing import Any, Callable

import modin.pandas as pd
import numpy as np
import pandas
from modin.core.storage_formats import BaseQueryCompiler  # pragma: no cover
from pandas._libs import lib
from pandas._typing import AggFuncType, AggFuncTypeBase, AnyArrayLike, Axes, Scalar
from pandas.core.dtypes.common import is_array_like, is_dict_like, is_list_like
from pandas.errors import SpecificationError

from snowflake.snowpark.modin.plugin._internal.aggregation_utils import (
    AggFuncWithLabel,
    get_pandas_aggr_func_name,
)
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

>>> np.all((new_{object_var_name} == {object_var_name}).values)  # doctest: +SKIP
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


def raise_if_native_pandas_objects(obj: Any) -> None:
    """
    Raise TypeError if provided object is of type pandas.Series or pandas.DataFrame

    Args:
        obj: object to check

    Raises:
        TypeError if provided ``obj`` is either native pandas DataFrame or Series

    """
    if isinstance(obj, (pandas.DataFrame, pandas.Series)):
        raise TypeError(
            f"{type(obj)} is not supported as 'value' argument. Please convert this to "
            "Snowpark pandas objects by calling modin.pandas.Series()/DataFrame()"
        )


def replace_external_data_keys_with_empty_pandas_series(
    keys: None | (Hashable | AnyArrayLike | Sequence[Hashable | AnyArrayLike]) = None,
) -> Hashable | pandas.Series | list[Hashable | pandas.Series] | None:
    """
    Replace any array-like key with empty series.
    Args:
        keys: join key or sequence of join keys.

    Returns:
        Join key(s) by replacing array-like join key with empty series.
    """
    if keys is None:
        return None
    if is_array_like(keys):
        return create_empty_pandas_series_from_array_like(keys)
    if isinstance(keys, (list, tuple)):
        return [
            create_empty_pandas_series_from_array_like(key)
            if is_array_like(key)
            else key
            for key in keys
        ]
    return keys


def create_empty_pandas_series_from_array_like(obj: AnyArrayLike) -> pandas.Series:
    """
    Create empty (zero rows) native pandas series from given array-like object.
    Args:
        obj:  array-like object

    Returns:
        Native pandas series with zero rows.

    """
    assert is_array_like(obj)
    # Snowpark pandas series.
    if isinstance(obj, pd.Series):
        return create_empty_native_pandas_frame(obj).squeeze()
    # Everything else first gets converted to pandas.Series
    if not isinstance(obj, pandas.Series):
        obj = pandas.Series(obj)
    # Create empty series by calling head with zero rows.
    return obj.head(0)


def create_empty_native_pandas_frame(obj: pd.Series | pd.DataFrame) -> pandas.DataFrame:
    """
    Create an empty native pandas DataFrame using the columns and index labels info from
    the given object. Empty here implies zero rows.

    Args:
        obj: Snowflake Series or DataFrame.

    Returns:
        A native pandas DataFrame with 0 rows in it.
    """
    qc = obj._query_compiler
    index_names = qc.get_index_names()
    index = (
        pandas.MultiIndex.from_tuples(tuples=[], names=index_names)
        if len(index_names) > 1
        else pandas.Index(data=[], name=index_names[0])
    )
    return pandas.DataFrame(columns=qc.columns, index=index)


def replace_external_data_keys_with_query_compiler(
    frame: pd.DataFrame,
    keys: None | (Hashable | AnyArrayLike | Sequence[Hashable | AnyArrayLike]) = None,
) -> None | (Hashable | BaseQueryCompiler | list[Hashable | BaseQueryCompiler]):
    """
    Replace any array-like join key(s) with query compiler.

    Args:
        frame: dataframe, join keys belong to.
        keys: join key or sequence of join keys.

    Returns:
        List of join keys by replacing array-like join keys with query compiler.

    """
    if keys is None:
        return None
    if not isinstance(keys, (list, tuple)):
        keys = [keys]
    replaced_keys = []
    for key in keys:
        if is_array_like(key):
            raise_if_native_pandas_objects(key)
            if not isinstance(key, pd.Series):
                key = pd.Series(key)
            # Native pandas raises
            # ValueError: The truth value of an array with more than one element is ambiguous
            # Error message is not very helpful. We instead raise error with
            # more helpful message.
            if frame.shape[0] != key.shape[0]:
                raise ValueError(
                    "array-like join key must be of same length as dataframe"
                )
            replaced_keys.append(key._query_compiler)
        else:
            replaced_keys.append(key)
    return replaced_keys


def try_convert_builtin_func_to_str(
    fn: AggFuncTypeBase | list[AggFuncTypeBase], obj: object
) -> AggFuncTypeBase | list[AggFuncTypeBase]:
    """
    Try to convert an aggregation function to a string or list of such if the function is a
    builtin function and supported in the current object dir.

    This is mainly required by our server backend aggregation function mapping, which requires the
    function to be in string format or numpy function (numpy function is handled differently because
    it can potentially have different behavior as builtin function, For example: np.percentile and
    percentile have different behavior). For any function that can not find a map in snowflake, it will
    go through fallback, includes function that is not a numpy function and can not be converted to
    string format.

    Args:
        fn : callable, str, or list of above
        obj : the object to search for function dir

    Returns:
        str, callable or list of above
            If `fn` is a callable, return its name if it's a builtin function (i.e. min, max)
            and it is a method of the current object, otherwise return `fn` itself.
            If `fn` is a string, return it.
            If `fn` is an Iterable, return a list of try_convert_func_to_str applied to
            each element of `fn`.
    """

    def _try_convert_single_builtin_func_to_str(f):
        return (
            f.__name__
            if (
                callable(f)
                and isinstance(f, BuiltinFunctionType)
                and f.__name__ in dir(obj)
            )
            else f
        )

    if is_list_like(fn):
        return [_try_convert_single_builtin_func_to_str(f) for f in fn]
    else:
        return _try_convert_single_builtin_func_to_str(fn)


def extract_validate_and_try_convert_named_aggs_from_kwargs(
    obj: object, allow_duplication: bool, axis: int, **kwargs
) -> AggFuncType:
    """
    Attempt to extract pd.NamedAgg (or tuples of the same format) from the kwargs.

    kwargs: dict
        The kwargs to extract from.

    Returns:
        A dictionary mapping columns to a tuple containing the aggregation to perform, as well
        as the pandas label to give the aggregated column.
    """
    from modin.pandas import Series
    from modin.pandas.groupby import SeriesGroupBy

    is_series_like = isinstance(obj, (Series, SeriesGroupBy))
    named_aggs = {}
    accepted_keys = []
    columns = obj._query_compiler.columns
    for key, value in kwargs.items():
        if isinstance(value, pd.NamedAgg) or (
            isinstance(value, tuple) and len(value) == 2
        ):
            if is_series_like:
                # pandas does not allow pd.NamedAgg or 2-tuples for named aggregations
                # when the base object is a Series, but has different errors depending
                # on whether we are doing a Series.agg or Series.groupby.agg.
                if isinstance(obj, Series):
                    raise SpecificationError("nested renamer is not supported")
                else:
                    value_type_str = (
                        "NamedAgg" if isinstance(value, pd.NamedAgg) else "tuple"
                    )
                    raise TypeError(
                        f"func is expected but received {value_type_str} in **kwargs."
                    )
            if axis == 0:
                # If axis == 1, we would need a query to materialize the index to check its existence
                # so we defer the error checking to later.
                if value[0] not in columns:
                    raise KeyError(f"Column(s) ['{value[0]}'] do not exist")

            # This function converts our named aggregations dictionary from a mapping of
            # new_label -> tuple[column_name, agg_func] to a mapping of
            # column_name -> tuple[agg_func, new_label] in order to process
            # the aggregation functions internally. One issue with this is that the order
            # of the named aggregations can change - say we have the following aggregations:
            # {new_col: ('A', min), new_col1: ('B', max), new_col2: ('A', max)}
            # The output of this function will look like this:
            # {A: [AggFuncWithLabel(func=min, label=new_col), AggFuncWithLabel(func=max, label=new_col2)]
            # B: AggFuncWithLabel(func=max, label=new_col1)}
            # And so our final dataframe will have the wrong order. We handle the reordering of the generated
            # labels at the QC layer.
            if value[0] in named_aggs:
                if not isinstance(named_aggs[value[0]], list):
                    named_aggs[value[0]] = [named_aggs[value[0]]]
                named_aggs[value[0]] += [
                    AggFuncWithLabel(func=value[1], pandas_label=key)
                ]
            else:
                named_aggs[value[0]] = AggFuncWithLabel(func=value[1], pandas_label=key)
            accepted_keys += [key]
        elif is_series_like:
            if isinstance(obj, SeriesGroupBy):
                col_name = obj._df._query_compiler.columns[0]
            else:
                col_name = obj._query_compiler.columns[0]
            if col_name not in named_aggs:
                named_aggs[col_name] = AggFuncWithLabel(func=value, pandas_label=key)
            else:
                if not isinstance(named_aggs[col_name], list):
                    named_aggs[col_name] = [named_aggs[col_name]]
                named_aggs[col_name] += [AggFuncWithLabel(func=value, pandas_label=key)]
            accepted_keys += [key]

    if len(named_aggs.keys()) == 0 or any(
        key not in accepted_keys for key in kwargs.keys()
    ):
        # First check makes sure that some functions have been passed. If nothing has been passed,
        # we raise the TypeError.
        # The second check is for compatibility with pandas errors. Say the user does something like this:
        # df.agg(x=pd.NamedAgg('A', 'min'), random_extra_kwarg=14). pandas errors out, since func is None
        # and not every kwarg is a named aggregation. Without this check explicitly, we would just ignore
        # the extraneous kwargs, so we include this check for parity with pandas.
        raise TypeError("Must provide 'func' or tuples of '(column, aggfunc).")

    validated_named_aggs = {}
    for key, value in named_aggs.items():
        if isinstance(value, list):
            validated_named_aggs[key] = [
                AggFuncWithLabel(
                    func=validate_and_try_convert_agg_func_arg_func_to_str(
                        v.func, obj, allow_duplication, axis
                    ),
                    pandas_label=v.pandas_label,
                )
                for v in value
            ]
        else:
            validated_named_aggs[key] = AggFuncWithLabel(
                func=validate_and_try_convert_agg_func_arg_func_to_str(
                    value.func, obj, allow_duplication, axis
                ),
                pandas_label=value.pandas_label,
            )
    return validated_named_aggs


def validate_and_try_convert_agg_func_arg_func_to_str(
    agg_func: AggFuncType, obj: object, allow_duplication: bool, axis: int
) -> AggFuncType:
    """
    Perform validation on the func argument for aggregation, and try to convert builtin function in agg_func to str.
    Following validation is performed:
    1) Argument agg_func can not be None.
    2) If agg_func is dict like, the values of the dict can not be dict like, and if the aggregation is across axis=0,
       all keys must be a valid column of the object. When axis=1, we do not check if the labels are present in the index
       to avoid the extra query needed to materialize it.
    3) If allow_duplication is False, more than one aggregation function with the same name can not be applied on the
        same column. For example: [min, max, min] is not valid. This is mainly used by general aggregation.

    This function also calls try_convert_func_to_str on agg_func to convert the builtin functions used in agg_func to
    str but keep the original dict like or list like format. This is mainly required by our server backend aggregation
    function mapping, which requires the function to be in string format or numpy function (numpy function is handled
    differently because it can potentially have different behavior as builtin function, For example: np.percentile and
    percentile have different behavior). For any function that can not find a map in snowflake, it will
    go through fallback, includes function that is not a numpy function and can not be converted to string format.

    Args:
        agg_func: AggFuncType
            The func arg passed for the aggregation
        obj: object
            The object to search for attributes
        allow_duplication: bool
            Whether allow duplicated function with the same name. Note that numpy functions has different function
            name compare with the equivalent builtin function, for example, np.min and min have different
            names ('amin' and 'min'). However, this behavior is changing with python 3.9,
            where np.min will have the same name 'min'.
        axis: int
            The axis across which the aggregation is applied.

    Returns:
        Processed aggregation function arg with builtin function converted to name
    Raises:
        SpecificationError
            If nested dict configuration is used when agg_func is dict like or functions with duplicated names.

    """
    if callable(agg_func):
        result_agg_func = try_convert_builtin_func_to_str(agg_func, obj)
    elif is_dict_like(agg_func):
        # A dict like func input should in format like {'col1': max, 'col2': [min, np.max]}, where each
        # entry have key as the data column label, and value as the aggregation functions to apply on
        # the column. Following checks and process will be performed if the input is dict like:
        # 1) Perform check for the dict entries to make sure all columns belongs to the data columns, and
        #    no nested dictionary is used in the configuration.
        # 2) Perform a processing to the values (aggregation function) to convert the function to string
        #    format if possible. For example, {'col1': max, 'col2': [min, np.max]} will be processed to
        #    {'col1': 'max', 'col2': ['min', np.max]}

        # check if there is any value also in dictionary format, which is not allowed in pandas
        if any(is_dict_like(fn) for fn in agg_func.values()):
            raise SpecificationError(
                "Value for func argument with nested dict format is not allowed."
            )
        if any(is_list_like(fn) and len(fn) == 0 for fn in agg_func.values()):
            # A label must have aggregations provided, e.g. df.agg({0: []}) is illegal
            raise ValueError("No objects to concatenate")
        # check that all columns in the dictionary exists in the data columns of the current dataframe
        columns = obj._query_compiler.columns
        if axis == 0:
            # If axis == 1, we would need a query to materialize the index to check its existence
            # so we defer the error checking to later.
            for i in agg_func.keys():
                if i not in columns:
                    raise KeyError(f"Column(s) ['{i}'] do not exist")

        func_dict = {
            label: try_convert_builtin_func_to_str(fn, obj)
            for label, fn in agg_func.items()
        }

        result_agg_func = func_dict
    elif is_list_like(agg_func):
        # When the input func is in list like format like [min, max, np.sum], perform a processing to the
        # aggregation function to convert it to string representation if possible.
        result_agg_func = try_convert_builtin_func_to_str(agg_func, obj)
    else:
        result_agg_func = agg_func

    if not allow_duplication:
        # if allow_duplication is False, check is there duplication in the function names, which
        # are used as the row label for the aggregation result in dataframe/series aggregation, and
        # not allowed in pandas.
        found_duplication = False
        if is_dict_like(result_agg_func):
            for agg_func in result_agg_func.values():
                if is_list_like(agg_func):
                    agg_func_names = [get_pandas_aggr_func_name(fn) for fn in agg_func]
                    found_duplication = len(agg_func_names) > len(set(agg_func_names))
                    break
        elif is_list_like(result_agg_func):
            agg_func_names = [get_pandas_aggr_func_name(fn) for fn in result_agg_func]
            found_duplication = len(agg_func_names) > len(set(agg_func_names))

        if found_duplication:
            raise SpecificationError("Function names must be unique!")

    return result_agg_func


def get_as_shape_compatible_dataframe_or_series(
    other: pd.DataFrame | pd.Series | Callable | AnyArrayLike | Scalar,
    reference_df: pd.DataFrame,
    shape_mismatch_message: None
    | (str) = "Array conditional must be same shape as self",
) -> pd.DataFrame | pd.Series:
    """
    Get the "other" type as a shape compatible dataframe or series using the reference_df as a reference for
    compatible shape and construction.  If there is no shape on the other type then wrap as a numpy array.

    Parameters
    ----------
        other : Other type which could be array like
        reference_df : Reference dataframe or series

    Returns
    -------
        Dataframe or series that contains same values as other
    """
    if not hasattr(other, "shape"):
        # If an array type is provided that doesn't have a shape, then wrap it so it has a shape.
        # For example, if other=[1,2,3] then np.asanyarray will wrap as a numpy array with correct shape,
        # ie, np.anyarray(other).shape=(3,) in this case.
        other = np.asanyarray(other)

    if len(other.shape) == 0 or other.shape != reference_df.shape:
        raise ValueError(shape_mismatch_message)

    if "columns" in reference_df:
        other = reference_df.__constructor__(
            other, index=reference_df.index, columns=reference_df.columns
        )
    else:
        other = reference_df.__constructor__(other, index=reference_df.index)

    return other


def ensure_index(
    index_like: Axes | pd.Index | pd.Series, copy: bool = False
) -> pd.Index | pandas.MultiIndex:
    """
    Ensure that we have an index from some index-like object.

    Parameters
    ----------
    index_like : sequence
        An Index or other sequence
    copy : bool, default False

    Returns
    -------
    Index

    Examples
    --------
    >>> ensure_index(['a', 'b'])
    Index(['a', 'b'], dtype='object')

    # Snowpark pandas converts these tuples to lists
    >>> ensure_index([('a', 'a'),  ('b', 'c')])
    Index([['a', 'a'], ['b', 'c']], dtype='object')

    >>> ensure_index([['a', 'a'], ['b', 'c']])
    MultiIndex([('a', 'b'),
                ('a', 'c')],
               )
    """
    # if we have an index object already, simply copy it if required and return
    if isinstance(index_like, (pandas.MultiIndex, pd.Index)):
        if copy:
            index_like = index_like.copy()
        return index_like

    if isinstance(index_like, pd.Series):
        return pd.Index(index_like.values)

    if isinstance(index_like, list):
        # if we have a non-empty list that is multi dimensional, convert this to a multi-index and return
        if len(index_like) and lib.is_all_arraylike(index_like):
            return pandas.MultiIndex.from_arrays(index_like)
        else:
            # otherwise, we have a one dimensional index, so set tupleize_cols=False and return a pd.Index
            return pd.Index(index_like, copy=copy, tupleize_cols=False)
    else:
        return pd.Index(index_like, copy=copy)


def try_convert_index_to_native(index_like: Any) -> Any:
    """
    Try to convert the given item to a native pandas Index.
    This conversion is only performed if `index_like` is a Snowpark pandas Index. Otherwise, the original input will be returned.

    Parameters
    ----------
    index_like : Any
        An index-like object, such as a list, ndarray or Index object that we would like to try to convert to pandas Index

    Return
    ----------
        A pandas Index if index_like is a Snowpark pandas Index, otherwise return index_like
    """
    from snowflake.snowpark.modin.plugin.extensions.index import Index

    if isinstance(index_like, Index):
        index_like = index_like.to_pandas()
    return index_like
