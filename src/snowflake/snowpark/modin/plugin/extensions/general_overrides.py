#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

# Licensed to Modin Development Team under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional information regarding
# copyright ownership.  The Modin Development Team licenses this file to you under the
# Apache License, Version 2.0 (the "License"); you may not use this file except in
# compliance with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific language
# governing permissions and limitations under the License.

# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the Modin project, under the Apache License,
# Version 2.0.

"""Implement pandas general API."""
from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Hashable, Iterable, Mapping, Sequence
from datetime import date, datetime, timedelta, tzinfo
import functools
from logging import getLogger
from typing import Any, Literal, Union

import modin.pandas as pd
import numpy as np
import pandas
import pandas.core.common as common
from modin.pandas import DataFrame, Series
from modin.pandas.base import BasePandasDataset
from modin.pandas.utils import is_scalar
from pandas import IntervalIndex, NaT, Timedelta, Timestamp
from pandas._libs import NaTType, lib
from pandas._libs.tslibs import to_offset
from pandas._typing import (
    AnyArrayLike,
    ArrayLike,
    Axis,
    DateTimeErrorChoices,
    Frequency,
    IndexLabel,
    IntervalClosedType,
    Scalar,
    Suffixes,
)
from pandas.core.arrays import datetimelike
from pandas.core.arrays.datetimes import (
    _infer_tz_from_endpoints,
    _maybe_localize_point,
    _maybe_normalize_endpoints,
)
from pandas.core.dtypes.common import is_list_like, is_nested_list_like
from pandas.core.dtypes.inference import is_array_like
from pandas.core.tools.datetimes import (
    ArrayConvertible,
    DatetimeScalar,
    DatetimeScalarOrArrayConvertible,
    DictConvertible,
)
from pandas.errors import MergeError
from pandas.util._validators import validate_inclusive

from snowflake.snowpark.modin.plugin._internal.timestamp_utils import (
    VALID_TO_DATETIME_UNIT,
)
from snowflake.snowpark.modin.plugin._typing import ListLike, ListLikeOfFloats
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)
from snowflake.snowpark.modin.plugin.extensions.utils import (
    raise_if_native_pandas_objects,
)
from snowflake.snowpark.modin.plugin.utils.error_message import (
    ErrorMessage,
    pandas_module_level_function_not_implemented,
)
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from snowflake.snowpark.modin.utils import _inherit_docstrings, to_pandas

from modin.pandas.api.extensions import (
    register_pd_accessor as _register_pd_accessor,
)

register_pd_accessor = functools.partial(_register_pd_accessor, backend="Snowflake")

# To prevent cross-reference warnings when building documentation and prevent erroneously
# linking to `snowflake.snowpark.DataFrame`, we need to explicitly
# qualify return types in this file with `modin.pandas.DataFrame`.
# SNOW-1233342: investigate how to fix these links without using absolute paths

_logger = getLogger(__name__)

VALID_DATE_TYPE = Union[
    np.integer, float, str, date, datetime, np.datetime64, pd.Timestamp
]


###########################################################################
# Data manipulations
###########################################################################


@register_pd_accessor("melt")
@_inherit_docstrings(pandas.melt)
def melt(
    frame,
    id_vars=None,
    value_vars=None,
    var_name=None,
    value_name="value",
    col_level=None,
    ignore_index: bool = True,
):  # noqa: PR01, RT01, D200
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    return frame.melt(
        id_vars=id_vars,
        value_vars=value_vars,
        var_name=var_name,
        value_name=value_name,
        col_level=col_level,
        ignore_index=ignore_index,
    )


@register_pd_accessor("pivot")
@_inherit_docstrings(pandas.pivot)
def pivot(data, index=None, columns=None, values=None):  # noqa: PR01, RT01, D200
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    if not isinstance(data, DataFrame):
        raise ValueError(
            f"can not pivot with instance of type {type(data)}"
        )  # pragma: no cover
    return data.pivot(index=index, columns=columns, values=values)


@register_pd_accessor("pivot_table")
@_inherit_docstrings(pandas.pivot_table)
def pivot_table(
    data,
    values=None,
    index=None,
    columns=None,
    aggfunc="mean",
    fill_value=None,
    margins=False,
    dropna=True,
    margins_name="All",
    observed=False,
    sort=True,
):
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    if not isinstance(data, DataFrame):
        raise ValueError(
            f"can not create pivot table with instance of type {type(data)}"
        )

    return data.pivot_table(
        values=values,
        index=index,
        columns=columns,
        aggfunc=aggfunc,
        fill_value=fill_value,
        margins=margins,
        dropna=dropna,
        margins_name=margins_name,
        sort=sort,
    )


@register_pd_accessor("crosstab")
@_inherit_docstrings(pandas.crosstab)
def crosstab(
    index,
    columns,
    values=None,
    rownames=None,
    colnames=None,
    aggfunc=None,
    margins=False,
    margins_name: str = "All",
    dropna: bool = True,
    normalize=False,
) -> DataFrame:  # noqa: PR01, RT01, D200
    if values is None and aggfunc is not None:
        raise ValueError("aggfunc cannot be used without values.")

    if values is not None and aggfunc is None:
        raise ValueError("values cannot be used without an aggfunc.")

    if not is_nested_list_like(index):
        index = [index]
    if not is_nested_list_like(columns):
        columns = [columns]

    if (
        values is not None
        and margins is True
        and (normalize is True or normalize == "all")
    ):
        raise NotImplementedError(
            'Snowpark pandas does not yet support passing in margins=True, normalize="all", and values.'
        )

    user_passed_rownames = rownames is not None
    user_passed_colnames = colnames is not None

    from pandas.core.reshape.pivot import _build_names_mapper, _get_names

    def _get_names_wrapper(list_of_objs, names, prefix):
        """
        Helper method to expand DataFrame objects containing
        multiple columns into Series, since `_get_names` expects
        one column per entry.
        """
        expanded_list_of_objs = []
        for obj in list_of_objs:
            if isinstance(obj, DataFrame):
                for col in obj.columns:
                    expanded_list_of_objs.append(obj[col])
            else:
                expanded_list_of_objs.append(obj)
        return _get_names(expanded_list_of_objs, names, prefix)

    rownames = _get_names_wrapper(index, rownames, prefix="row")
    colnames = _get_names_wrapper(columns, colnames, prefix="col")

    (
        rownames_mapper,
        unique_rownames,
        colnames_mapper,
        unique_colnames,
    ) = _build_names_mapper(rownames, colnames)

    pass_objs = [x for x in index + columns if isinstance(x, (Series, DataFrame))]
    row_idx_names = None
    col_idx_names = None
    if pass_objs:
        # If we have any Snowpark pandas objects in the index or columns, then we
        # need to find the intersection of their indices, and only pick rows from
        # the objects that have indices in the intersection of their indices.
        # After we do that, we then need to append the non Snowpark pandas objects
        # using the intersection of indices as the final index for the DataFrame object.
        # First, we separate the objects into Snowpark pandas objects, and non-Snowpark
        # pandas objects (while renaming them so that they have unique names).
        rownames_idx = 0
        row_idx_names = []
        dfs = []
        arrays = []
        array_lengths = []
        for obj in index:
            if isinstance(obj, Series):
                row_idx_names.append(obj.name)
                df = pd.DataFrame(obj)
                df.columns = [unique_rownames[rownames_idx]]
                rownames_idx += 1
                dfs.append(df)
            elif isinstance(obj, DataFrame):
                row_idx_names.extend(obj.columns)
                obj.columns = unique_rownames[
                    rownames_idx : rownames_idx + len(obj.columns)
                ]
                rownames_idx += len(obj.columns)
                dfs.append(obj)
            else:
                row_idx_names.append(None)
                array_lengths.append(len(obj))
                df = pd.DataFrame(obj)
                df.columns = unique_rownames[
                    rownames_idx : rownames_idx + len(df.columns)
                ]
                rownames_idx += len(df.columns)
                arrays.append(df)

        colnames_idx = 0
        col_idx_names = []
        for obj in columns:
            if isinstance(obj, Series):
                col_idx_names.append(obj.name)
                df = pd.DataFrame(obj)
                df.columns = [unique_colnames[colnames_idx]]
                colnames_idx += 1
                dfs.append(df)
            elif isinstance(obj, DataFrame):
                col_idx_names.extend(obj.columns)
                obj.columns = unique_colnames[
                    colnames_idx : colnames_idx + len(obj.columns)
                ]
                colnames_idx += len(obj.columns)
                dfs.append(obj)
            else:
                col_idx_names.append(None)  # pragma: no cover
                array_lengths.append(len(obj))  # pragma: no cover
                df = pd.DataFrame(obj)  # pragma: no cover
                df.columns = unique_colnames[  # pragma: no cover
                    colnames_idx : colnames_idx + len(df.columns)
                ]
                colnames_idx += len(df.columns)  # pragma: no cover
                arrays.append(df)  # pragma: no cover

        if len(set(array_lengths)) > 1:
            raise ValueError(
                "All arrays must be of the same length"
            )  # pragma: no cover

        # Now, we have two lists - a list of Snowpark pandas objects, and a list of objects
        # that were not passed in as Snowpark pandas objects, but that we have converted
        # to Snowpark pandas objects to give them column names. We can perform inner joins
        # on the dfs list to get a DataFrame with the final index (that is only an intersection
        # of indices.)
        df = dfs[0]
        for right in dfs[1:]:
            df = df.merge(right, left_index=True, right_index=True)
        if len(arrays) > 0:
            index = df.index
            right_df = pd.concat(arrays, axis=1)
            # Increases query count by 1, but necessary for error checking.
            index_length = len(df)
            if index_length != array_lengths[0]:
                raise ValueError(
                    f"Length mismatch: Expected {array_lengths[0]} rows, received array of length {index_length}"
                )
            right_df.index = index
            df = df.merge(right_df, left_index=True, right_index=True)
    else:
        data = {
            **dict(zip(unique_rownames, index)),
            **dict(zip(unique_colnames, columns)),
        }
        df = DataFrame(data)

    if values is None:
        df["__dummy__"] = 0
        kwargs = {"aggfunc": "count"}
    else:
        df["__dummy__"] = values
        kwargs = {"aggfunc": aggfunc}

    table = df.pivot_table(
        "__dummy__",
        index=unique_rownames,
        columns=unique_colnames,
        margins=margins,
        margins_name=margins_name,
        dropna=dropna,
        **kwargs,  # type: ignore[arg-type]
    )

    if row_idx_names is not None and not user_passed_rownames:
        table.index = table.index.set_names(row_idx_names)

    if col_idx_names is not None and not user_passed_colnames:
        table.columns = table.columns.set_names(col_idx_names)

    if aggfunc is None:
        # If no aggfunc is provided, we are computing frequencies. Since we use
        # pivot_table above, pairs that are not observed will get a NaN value,
        # so we need to fill all NaN values with 0.
        table = table.fillna(0)

    # We must explicitly check that the value of normalize is not False here,
    # as a valid value of normalize is `0` (for normalizing index).
    if normalize is not False:
        if normalize not in [0, 1, "index", "columns", "all", True]:
            raise ValueError("Not a valid normalize argument")
        if normalize is True:
            normalize = "all"
        normalize = {0: "index", 1: "columns"}.get(normalize, normalize)

        # Actual Normalizations
        normalizers: dict[bool | str, Callable] = {
            "all": lambda x: x / x.sum(axis=0).sum(),
            "columns": lambda x: x / x.sum(),
            "index": lambda x: x.div(x.sum(axis=1), axis="index"),
        }

        if margins is False:

            f = normalizers[normalize]
            names = table.columns.names
            table = f(table)
            table.columns.names = names
            table = table.fillna(0)
        else:
            # keep index and column of pivoted table
            table_index = table.index
            table_columns = table.columns

            column_margin = table.iloc[:-1, -1]

            if normalize == "columns":
                # keep the core table
                table = table.iloc[:-1, :-1]

                # Normalize core
                f = normalizers[normalize]
                table = f(table)
                table = table.fillna(0)
                # Fix Margins
                column_margin = column_margin / column_margin.sum()
                table = pd.concat([table, column_margin], axis=1)
                table = table.fillna(0)
                table.columns = table_columns

            elif normalize == "index":
                table = table.iloc[:, :-1]

                # Normalize core
                f = normalizers[normalize]
                table = f(table)
                table = table.fillna(0).reindex(index=table_index)

            elif normalize == "all":
                # Normalize core
                f = normalizers[normalize]

                # When we perform the normalization function, we take the sum over
                # the rows, and divide every value by the sum. Since margins is included
                # though, the result of the sum is actually 2 * the sum of the original
                # values (since the margin itself is the sum of the original values),
                # so we need to multiply by 2 here to account for that.
                # The alternative would be to apply normalization to the main table
                # and the index margins separately, but that would require additional joins
                # to get the final table, which we want to avoid.
                table = f(table.iloc[:, :-1]) * 2.0

                column_margin = column_margin / column_margin.sum()
                table = pd.concat([table, column_margin], axis=1)
                table.iloc[-1, -1] = 1

                table = table.fillna(0)
                table.index = table_index
                table.columns = table_columns

    table = table.rename_axis(index=rownames_mapper, axis=0)
    table = table.rename_axis(columns=colnames_mapper, axis=1)
    table.attrs = {}  # native pandas crosstab does not propagate attrs form the input

    return table


@register_pd_accessor("cut")
@_inherit_docstrings(pandas.cut)
def cut(
    x: AnyArrayLike,
    bins: int | Sequence[Scalar] | IntervalIndex,
    right: bool = True,
    labels=None,
    retbins: bool = False,
    precision: int = 3,
    include_lowest: bool = False,
    duplicates: str = "raise",
    ordered: bool = True,
):
    if retbins is True:
        ErrorMessage.not_implemented("retbins not supported.")

    # Execute other supported objects via native pandas.
    if not isinstance(x, Series):
        return pandas.cut(
            x,
            bins,
            right=right,
            labels=labels,
            retbins=retbins,
            precision=precision,
            include_lowest=include_lowest,
            duplicates=duplicates,
            ordered=ordered,
        )

    # Produce pandas-compatible error if ordered=False and labels are not specified.
    # No error is raised when labels are not desired (labels=False).
    if ordered is False and labels is None:
        raise ValueError("'labels' must be provided if 'ordered = False'")

    bins, qc = x._query_compiler.cut(
        bins,
        right=right,
        labels=labels,
        precision=precision,
        include_lowest=include_lowest,
        duplicates=duplicates,
    )

    # Depending on setting, reconstruct bins and convert qc to the correct result.
    if labels is False:
        return pd.Series(query_compiler=qc)
    else:
        # Raise NotImplemented Error as categorical is not supported.
        ErrorMessage.not_implemented("categorical not supported in Snowpark pandas API")

        # Following code would produce correct result, uncomment once categorical is supported.
        # Convert to pandas categorical and return as Series.
        # Note: In the future, once we support CategoricalType we could keep this lazily around. For now,
        # match what pandas does here. In the future, change pandas -> pd and everything should work out-of-the box.
        # arr = qc.to_numpy().ravel()
        # return pandas.Series(
        #    pandas.Categorical(values=arr, categories=labels, ordered=ordered)
        # )


@register_pd_accessor("qcut")
@_inherit_docstrings(pandas.qcut)
def qcut(
    x: np.ndarray | Series,
    q: int | ListLikeOfFloats,
    labels: ListLike | bool | None = None,
    retbins: bool = False,
    precision: int = 3,
    duplicates: Literal["raise"] | Literal["drop"] = "raise",
) -> Series:
    kwargs = {
        "labels": labels,
        "retbins": retbins,
        "precision": precision,
        "duplicates": duplicates,
    }

    # For numpy or list, call to native pandas.
    if not isinstance(x, Series):
        return pandas.qcut(x, q, **kwargs)

    # Check that labels is supported as in pandas.
    if not (labels is None or labels is False or is_list_like(labels)):
        raise ValueError(
            "Bin labels must either be False, None or passed in as a list-like argument"
        )

    # Carry out check that for the list-like case quantiles are (monotonically) increasing,
    # if not the case throw pandas compatible error.
    if not isinstance(q, int) and np.all(np.diff(q) < 0):
        # Note: Pandas 2.x changed the error message here, using Pandas 2.x behavior here.
        raise ValueError("left side of interval must be <= right side")

        # remove duplicates (input like [0.5, 0.5] is ok)
        q = sorted(list(set(q)))  # pragma: no cover

    if labels is not False:
        # Labels require categorical, not yet supported. Use native pandas conversion here to compute result.
        ErrorMessage.not_implemented(
            "Snowpark pandas API qcut method supports only labels=False, if you need support"
            " for labels consider calling pandas.qcut(x.to_pandas(), q, ...)"
        )

    ans = x._qcut(q, retbins, duplicates)

    if isinstance(q, int) and q != 1 and len(ans) == 1:
        if duplicates == "raise":
            # We issue a count query since if q !=1 and x is a Series/list-like containing
            # a single element, an error will be produced  ValueError: Bin edges must be unique: array([0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.]).
            #                You can drop duplicate edges by setting the 'duplicates' kwarg.
            # With qcut being an API that requires conversion, we can mimick this behavior here.

            # Produce raising error.
            raise ValueError(
                f"Bin edges must be unique: {repr(np.array([0.] * q))}.\nYou can drop duplicate edges by setting the 'duplicates' kwarg."
            )
        else:
            # The result will always be NaN because no unique bin could be found.
            return pd.Series([np.nan])

    return ans


@register_pd_accessor("merge")
@_inherit_docstrings(pandas.merge)
def merge(
    left: pd.DataFrame | Series,
    right: pd.DataFrame | Series,
    how: str | None = "inner",
    on: IndexLabel | None = None,
    left_on: None
    | (Hashable | AnyArrayLike | Sequence[Hashable | AnyArrayLike]) = None,
    right_on: None
    | (Hashable | AnyArrayLike | Sequence[Hashable | AnyArrayLike]) = None,
    left_index: bool | None = False,
    right_index: bool | None = False,
    sort: bool | None = False,
    suffixes: Suffixes | None = ("_x", "_y"),
    copy: bool | None = True,
    indicator: bool | str | None = False,
    validate: str | None = None,
):
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    # Raise error if 'left' or 'right' is native pandas object.
    raise_if_native_pandas_objects(left)
    raise_if_native_pandas_objects(right)

    if isinstance(left, Series):
        if left.name is None:
            raise ValueError("Cannot merge a Series without a name")
        else:
            left = left.to_frame()

    if not isinstance(left, DataFrame):
        raise TypeError(  # pragma: no cover
            f"Can only merge Series or DataFrame objects, a {type(left)} was passed"
        )

    return left.merge(
        right,
        how=how,
        on=on,
        left_on=left_on,
        right_on=right_on,
        left_index=left_index,
        right_index=right_index,
        sort=sort,
        suffixes=suffixes,
        copy=copy,
        indicator=indicator,
        validate=validate,
    )


@register_pd_accessor("merge_ordered")
@pandas_module_level_function_not_implemented()
@_inherit_docstrings(pandas.merge_ordered, apilink="pandas.merge_ordered")
def merge_ordered(
    left,
    right,
    on=None,
    left_on=None,
    right_on=None,
    left_by=None,
    right_by=None,
    fill_method=None,
    suffixes=("_x", "_y"),
    how: str = "outer",
) -> DataFrame:  # noqa: PR01, RT01, D200
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    if not isinstance(left, DataFrame):  # pragma: no cover
        raise ValueError(
            f"can not merge DataFrame with instance of type {type(right)}"
        )  # pragma: no cover
    if isinstance(right, DataFrame):  # pragma: no cover
        right = to_pandas(right)  # pragma: no cover
    return DataFrame(  # pragma: no cover
        pandas.merge_ordered(
            to_pandas(left),
            right,
            on=on,
            left_on=left_on,
            right_on=right_on,
            left_by=left_by,
            right_by=right_by,
            fill_method=fill_method,
            suffixes=suffixes,
            how=how,
        )
    )


@register_pd_accessor("merge_asof")
@_inherit_docstrings(pandas.merge_asof, apilink="pandas.merge_asof")
def merge_asof(
    left,
    right,
    on: str | None = None,
    left_on: str | None = None,
    right_on: str | None = None,
    left_index: bool = False,
    right_index: bool = False,
    by: str | list[str] | None = None,
    left_by: str | None = None,
    right_by: str | None = None,
    suffixes: Suffixes = ("_x", "_y"),
    tolerance: int | Timedelta | None = None,
    allow_exact_matches: bool = True,
    direction: str = "backward",
) -> pd.DataFrame:
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    if not isinstance(left, DataFrame):
        raise ValueError(
            f"can not merge DataFrame with instance of type {type(left)}"
        )  # pragma: no cover
    if not isinstance(right, DataFrame):
        raise ValueError(
            f"can not merge DataFrame with instance of type {type(right)}"
        )  # pragma: no cover

    # As of pandas 1.2 these should raise an error; before that it did
    # something likely random:
    if (
        (on and (left_index or right_index))
        or (left_on and left_index)
        or (right_on and right_index)
    ):
        raise ValueError(
            "Can't combine left/right_index with left/right_on or on."
        )  # pragma: no cover

    if on is not None:
        if left_on is not None or right_on is not None:
            raise ValueError(
                "If 'on' is set, 'left_on' and 'right_on' can't be set."
            )  # pragma: no cover
        if is_list_like(on) and len(on) > 1:
            raise MergeError("can only asof on a key for left")
        left_on = on
        right_on = on

    if by is not None:
        if left_by is not None or right_by is not None:
            raise ValueError(
                "Can't have both 'by' and 'left_by' or 'right_by'"
            )  # pragma: no cover
        left_by = right_by = by

    if left_on is None and not left_index:
        raise ValueError(
            "Must pass on, left_on, or left_index=True"
        )  # pragma: no cover

    if right_on is None and not right_index:
        raise ValueError(
            "Must pass on, right_on, or right_index=True"
        )  # pragma: no cover

    if not left_index and not right_index:
        left_on_length = len(left_on) if is_list_like(left_on) else 1
        right_on_length = len(right_on) if is_list_like(right_on) else 1
        if left_on_length != right_on_length:
            raise ValueError("len(right_on) must equal len(left_on)")
        if left_on_length > 1:
            raise MergeError("can only asof on a key for left")

    return DataFrame(
        query_compiler=left._query_compiler.merge_asof(
            right._query_compiler,
            on,
            left_on,
            right_on,
            left_index,
            right_index,
            by,
            left_by,
            right_by,
            suffixes,
            tolerance,
            allow_exact_matches,
            direction,
        )
    )


@register_pd_accessor("concat")
@_inherit_docstrings(pandas.concat)
def concat(
    objs: (Iterable[pd.DataFrame | Series] | Mapping[Hashable, pd.DataFrame | Series]),
    axis: Axis = 0,
    join: str = "outer",
    ignore_index: bool = False,
    keys: Sequence[Hashable] = None,
    levels: list[Sequence[Hashable]] = None,
    names: list[Hashable] = None,
    verify_integrity: bool = False,
    sort: bool = False,
    copy: bool = True,
) -> pd.DataFrame | Series:
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    # Raise error if native pandas objects are passed.
    raise_if_native_pandas_objects(objs)

    # In native pandas 'concat' API is expected to work with all types of iterables like
    # tuples, list, generators, custom iterators, deque etc.
    # Few exceptions are 'DataFrame', 'Series', 'str', these are also technically
    # iterables, but they are not iterables of pandas objects.
    # Note other iterables can also have non pandas objects as element in them, but it's
    # not possible to know that in advance without iterating over all objects, so we
    # also individual element later.

    # Raise error if 'objs' is not an iterable or an iterable of non-pandas objects.
    if not isinstance(objs, Iterable) or isinstance(
        objs, (pd.DataFrame, pd.Series, str)
    ):
        # Same error as native pandas.
        raise TypeError(
            "first argument must be an iterable of pandas "
            f'objects, you passed an object of type "{type(objs).__name__}"'
        )

    if isinstance(objs, dict):
        if keys is None:
            keys = list(objs.keys())
        # if 'keys' is not none, filter out additional objects from mapping.
        objs = [objs[k] for k in keys]
    else:
        # Native pandas also supports generators as input, that can only be iterated
        # only once so first create a list from 'objs'.
        objs = list(objs)

    for obj in objs:
        # Raise error if native pandas objects are passed.
        raise_if_native_pandas_objects(obj)

    if join not in ("inner", "outer"):
        # Same error as native pandas.
        raise ValueError(
            "Only can inner (intersect) or outer (union) join the other axis"
        )

    axis = pandas.DataFrame()._get_axis_number(axis)

    if len(objs) == 0:
        # Same error as native pandas.
        raise ValueError("No objects to concatenate")

    # Filter out None objects
    if keys is None:
        objs = [o for o in objs if o is not None]
    else:
        tuples = [(k, v) for k, v in zip(keys, objs) if v is not None]
        # convert list of tuples to tuples of list.
        keys, objs = list(map(list, zip(*tuples))) if tuples else ([], [])

    if len(objs) == 0:
        # Same error as native pandas.
        raise ValueError("All objects passed were None")

    for obj in objs:
        # Same error as native pandas.
        if not isinstance(obj, (Series, DataFrame)):
            raise TypeError(
                f"cannot concatenate object of type '{type(obj)}'; "
                "only Series and DataFrame objs are valid"
            )

    # Assign names to unnamed series - the names function as column labels for Series.
    # If all Series have no name, use the keys as names.
    if (
        axis == 1
        and keys is not None
        and all(isinstance(obj, Series) and obj.name is None for obj in objs)
    ):
        for i, obj in enumerate(objs):
            objs[i] = obj.rename(keys[i])

    # If only some Series have names, give them temporary names.
    series_name = 0
    for i, obj in enumerate(objs):
        if isinstance(obj, pd.Series) and obj.name is None:
            objs[i] = obj.rename(series_name)
            series_name = series_name + 1

    # Check if all objects are of Series types.
    all_series = all([isinstance(obj, pd.Series) for obj in objs])
    # When concatenating Series objects on axis 0, pandas tries to preserve name from
    # input if all have same name otherwise set it to None.
    if all_series and axis == 0:
        unique_names = {obj.name for obj in objs}
        name = objs[0].name if len(unique_names) == 1 else None
        objs = [obj.rename(name) for obj in objs]

    if not copy:
        WarningMessage.ignored_argument(
            operation="concat",
            argument="copy",
            message="copy parameter has been ignored with Snowflake execution engine",
        )

    # For the edge case where concatenation is done on the columns where all the objects are series,
    # need to prevent a second column level from being created - therefore, keys is None.
    keys = None if axis == 1 and all(isinstance(obj, Series) for obj in objs) else keys

    result = objs[0]._query_compiler.concat(
        axis,
        [o._query_compiler for o in objs[1:]],
        join=join,
        ignore_index=ignore_index,
        keys=keys,
        levels=levels,
        names=names,
        verify_integrity=verify_integrity,
        sort=sort,
    )
    # If all objects are series and concat axis=0, return Series else return DataFrame.
    if all_series and axis == 0:
        return Series(query_compiler=result)
    return DataFrame(query_compiler=result)


@register_pd_accessor("get_dummies")
@_inherit_docstrings(pandas.get_dummies)
def get_dummies(
    data,
    prefix=None,
    prefix_sep="_",
    dummy_na=False,
    columns=None,
    sparse=False,
    drop_first=False,
    dtype=None,
):  # noqa: PR01, RT01, D200
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    new_qc = data._query_compiler.get_dummies(
        columns=columns,
        prefix=prefix,
        prefix_sep=prefix_sep,
        dummy_na=dummy_na,
        drop_first=drop_first,
        dtype=dtype,
        is_series=not data._is_dataframe,
    )
    return DataFrame(query_compiler=new_qc)


@register_pd_accessor("unique")
@_inherit_docstrings(pandas.unique)
def unique(values) -> np.ndarray:
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    if is_list_like(values) and not isinstance(values, dict):
        return Series(values).unique()
    else:
        raise TypeError("Only list-like objects can be used with unique()")


# Adding docstring since pandas docs don't have web section for this function.


@register_pd_accessor("lreshape")
@pandas_module_level_function_not_implemented()
@_inherit_docstrings(pandas.lreshape)
def lreshape(data: DataFrame, groups, dropna=True, label=None):
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    if not isinstance(data, DataFrame):  # pragma: no cover
        raise ValueError(
            f"can not lreshape with instance of type {type(data)}"
        )  # pragma: no cover
    return DataFrame(  # pragma: no cover
        pandas.lreshape(to_pandas(data), groups, dropna=dropna, label=label)
    )


@register_pd_accessor("wide_to_long")
@_inherit_docstrings(pandas.wide_to_long, apilink="pandas.wide_to_long")
@pandas_module_level_function_not_implemented()
def wide_to_long(
    df: DataFrame, stubnames, i, j, sep: str = "", suffix: str = r"\d+"
) -> DataFrame:  # noqa: PR01, RT01, D200
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    if not isinstance(df, DataFrame):  # pragma: no cover
        raise ValueError(
            f"can not wide_to_long with instance of type {type(df)}"
        )  # pragma: no cover
    # ErrorMessage.default_to_pandas("`wide_to_long`")
    return DataFrame(  # pragma: no cover
        pandas.wide_to_long(to_pandas(df), stubnames, i, j, sep=sep, suffix=suffix)
    )


###########################################################################
# Top-level missing data
###########################################################################


@register_pd_accessor("isna")
@_inherit_docstrings(pandas.isna, apilink="pandas.isna")
def isna(obj):  # noqa: PR01, RT01, D200
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    if isinstance(obj, BasePandasDataset):
        return obj.isna()  # pragma: no cover
    else:
        return pandas.isna(obj)


register_pd_accessor("isnull")(isna)


@register_pd_accessor("notna")
@_inherit_docstrings(pandas.notna, apilink="pandas.notna")
def notna(obj):  # noqa: PR01, RT01, D200
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    if isinstance(obj, BasePandasDataset):  # pragma: no cover
        return obj.notna()  # pragma: no cover
    else:
        return pandas.notna(obj)  # pragma: no cover


notnull = notna


###########################################################################
# Top-level dealing with numeric data
###########################################################################


@register_pd_accessor("to_numeric")
@_inherit_docstrings(pandas.to_numeric)
def to_numeric(
    arg: Scalar | Series | ArrayConvertible,
    errors: Literal["ignore", "raise", "coerce"] = "raise",
    downcast: Literal["integer", "signed", "unsigned", "float"] | None = None,
) -> Series | Scalar | None:
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    raise_if_native_pandas_objects(arg)
    if errors not in ("ignore", "raise", "coerce"):
        raise ValueError("invalid error value specified")
    if downcast is not None:
        WarningMessage.ignored_argument(
            operation="to_numeric",
            argument="downcast",
            message="downcast is ignored in Snowflake backend",
        )
    # convert arg to series
    arg_is_scalar = is_scalar(arg)

    if (
        not arg_is_scalar
        and not isinstance(arg, (list, tuple, Series))
        and not (is_array_like(arg) and arg.ndim == 1)
    ):
        raise TypeError("arg must be a list, tuple, 1-d array, or Series")

    if arg_is_scalar:
        arg = Series([arg])

    if not isinstance(arg, Series):
        name = None
        # keep index name
        if isinstance(arg, pandas.Index):
            name = arg.name  # pragma: no cover
        arg = Series(arg, name=name)

    ret = arg._to_numeric(errors=errors)
    if arg_is_scalar:
        # use squeeze to turn the series result into a scalar
        ret = ret.squeeze()
    return ret


###########################################################################
# Top-level dealing with datetimelike data
###########################################################################


@register_pd_accessor("to_datetime")
@_inherit_docstrings(pandas.to_datetime)
def to_datetime(
    arg: DatetimeScalarOrArrayConvertible | DictConvertible | pd.DataFrame | Series,
    errors: DateTimeErrorChoices = "raise",
    dayfirst: bool = False,
    yearfirst: bool = False,
    utc: bool = False,
    format: str | None = None,
    exact: bool | lib.NoDefault = lib.no_default,
    unit: str | None = None,
    infer_datetime_format: lib.NoDefault | bool = lib.no_default,
    origin: Any = "unix",
    cache: bool = True,
) -> pd.DatetimeIndex | Series | DatetimeScalar | NaTType | None:
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    raise_if_native_pandas_objects(arg)

    if not isinstance(arg, (DataFrame, Series, pd.Index)):
        # use pandas.to_datetime to convert local data to datetime
        res = pandas.to_datetime(
            arg,
            errors,
            dayfirst,
            yearfirst,
            utc,
            format,
            exact,
            unit,
            infer_datetime_format,
            origin,
            cache,
        )
        if isinstance(res, pandas.Series):
            res = pd.Series(res)
        elif not is_scalar(res):
            res = pd.Index(res)
        return res

    # handle modin objs
    if unit and unit not in VALID_TO_DATETIME_UNIT:
        raise ValueError(f"Unrecognized unit {unit}")

    if not cache:
        WarningMessage.ignored_argument(
            operation="to_datetime",
            argument="cache",
            message="cache parameter is ignored with Snowflake backend, i.e., no caching will be applied",
        )

    return arg._to_datetime(
        errors=errors,
        dayfirst=dayfirst,
        yearfirst=yearfirst,
        utc=utc,
        format=format,
        exact=exact,
        unit=unit,
        infer_datetime_format=infer_datetime_format,
        origin=origin,
    )


@register_pd_accessor("to_timedelta")
@_inherit_docstrings(pandas.to_timedelta)
def to_timedelta(
    arg: str
    | int
    | float
    | timedelta
    | list
    | tuple
    | range
    | ArrayLike
    | pd.Index
    | pd.Series
    | pandas.Index
    | pandas.Series,
    unit: str = None,
    errors: DateTimeErrorChoices = "raise",
):
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    # If arg is snowpark pandas lazy object call to_timedelta on the query compiler.
    if isinstance(arg, (Series, pd.Index)):
        query_compiler = arg._query_compiler.to_timedelta(
            unit=unit if unit else "ns",
            errors=errors,
            include_index=isinstance(arg, pd.Index),
        )
        return arg.__constructor__(query_compiler=query_compiler)

    # Use native pandas to_timedelta for scalar values and list-like objects.
    result = pandas.to_timedelta(arg, unit=unit, errors=errors)
    # Convert to lazy if result is a native pandas Series or Index.
    if isinstance(result, pandas.Index):
        return pd.Index(result)
    if isinstance(result, pandas.Series):
        return pd.Series(result)
    # Return the result as is for scaler.
    return result


@register_pd_accessor("date_range")
@_inherit_docstrings(pandas.date_range)
def date_range(
    start: VALID_DATE_TYPE | None = None,
    end: VALID_DATE_TYPE | None = None,
    periods: int | None = None,
    freq: str | pd.DateOffset | None = None,
    tz: str | tzinfo | None = None,
    normalize: bool = False,
    name: Hashable | None = None,
    inclusive: IntervalClosedType = "both",
    **kwargs,
) -> pd.DatetimeIndex:
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py

    if freq is None and common.any_none(periods, start, end):
        freq = "D"

    if common.count_not_none(start, end, periods, freq) != 3:
        raise ValueError(
            "Of the four parameters: start, end, periods, and freq, exactly three must be specified"
        )

    # Validation code is mostly copied from pandas code DatetimeArray._generate_range and it will cast it to an integer
    periods = datetimelike.validate_periods(periods)

    # Return DateOffset object from string or datetime.timedelta object
    freq = to_offset(freq)

    if freq is None and periods < 0:
        raise ValueError("Number of samples, %s, must be non-negative." % periods)

    if start is not None:
        start = Timestamp(start)

    if end is not None:
        end = Timestamp(end)

    if start is NaT or end is NaT:
        raise ValueError("Neither `start` nor `end` can be NaT")

    # Check that the `inclusive` argument is among {"both", "neither", "left", "right"}
    left_inclusive, right_inclusive = validate_inclusive(inclusive)

    # If normalize is needed, set start and end time to midnight
    start, end = _maybe_normalize_endpoints(start, end, normalize)

    # If a timezone is not explicitly given via `tz`, see if one can be inferred from the `start` and `end` endpoints.
    # If more than one of these inputs provides a timezone, require that they all agree.
    tz = _infer_tz_from_endpoints(start, end, tz)

    if tz is not None:
        # Localize the start and end arguments (reuse native pandas code)
        start = _maybe_localize_point(
            start, freq, tz, ambiguous="raise", nonexistent="raise"
        )
        end = _maybe_localize_point(
            end, freq, tz, ambiguous="raise", nonexistent="raise"
        )

    qc = SnowflakeQueryCompiler.from_date_range(
        start=start,
        end=end,
        periods=periods,
        freq=freq,
        tz=tz,
        left_inclusive=left_inclusive,
        right_inclusive=right_inclusive,
    )
    # Set date range as index column.
    qc = qc.set_index_from_columns(qc.columns.tolist(), include_index=False)
    # Set index column name.
    qc = qc.set_index_names([name])
    idx = pd.DatetimeIndex(query_compiler=qc)
    if tz is not None:
        idx = idx.tz_localize(tz)
    return idx


@register_pd_accessor("bdate_range")
@_inherit_docstrings(pandas.bdate_range)
def bdate_range(
    start: VALID_DATE_TYPE | None = None,
    end: VALID_DATE_TYPE | None = None,
    periods: int | None = None,
    freq: Frequency | str | pd.DateOffset | dt.timedelta | None = "B",
    tz: str | tzinfo | None = None,
    normalize: bool = True,
    name: Hashable | None = None,
    weekmask: str | None = None,
    holidays: ListLike | None = None,
    inclusive: IntervalClosedType = "both",
    **kwargs,
) -> pd.DatetimeIndex:
    if freq is None:
        msg = "freq must be specified for bdate_range; use date_range instead"
        raise TypeError(msg)

    if isinstance(freq, str) and freq.startswith("C"):
        ErrorMessage.not_implemented(
            "custom frequency is not supported in Snowpark pandas API"
        )
    elif holidays or weekmask:
        ErrorMessage.not_implemented(
            "custom holidays or weekmask are not supported in Snowpark pandas API"
        )

    return date_range(
        start=start,
        end=end,
        periods=periods,
        freq=freq,
        tz=tz,
        normalize=normalize,
        name=name,
        inclusive=inclusive,
        **kwargs,
    )


# Adding docstring since pandas docs don't have web section for this function.


@register_pd_accessor("value_counts")
@pandas_module_level_function_not_implemented()
@_inherit_docstrings(pandas.value_counts)
def value_counts(
    values, sort=True, ascending=False, normalize=False, bins=None, dropna=True
):
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    return Series(values).value_counts(  # pragma: no cover
        sort=sort,
        ascending=ascending,
        normalize=normalize,
        bins=bins,
        dropna=dropna,
    )
