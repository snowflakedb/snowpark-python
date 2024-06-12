#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
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

"""Module houses ``DataFrame`` class, that is distributed version of ``pandas.DataFrame``."""

from __future__ import annotations

import collections
import datetime
import functools
import itertools
import re
import sys
import warnings
from collections.abc import Hashable, Iterable, Iterator, Mapping, Sequence
from logging import getLogger
from typing import IO, Any, Callable, Literal

import numpy as np
import pandas
from modin.pandas.accessor import CachedAccessor, SparseFrameAccessor

# from . import _update_engine
from modin.pandas.iterator import PartitionIterator
from pandas._libs.lib import NoDefault, no_default
from pandas._typing import (
    AggFuncType,
    AnyArrayLike,
    Axes,
    Axis,
    CompressionOptions,
    FilePath,
    FillnaOptions,
    IgnoreRaise,
    IndexLabel,
    Level,
    PythonFuncType,
    Renamer,
    Scalar,
    StorageOptions,
    Suffixes,
    WriteBuffer,
)
from pandas.core.common import apply_if_callable, is_bool_indexer
from pandas.core.dtypes.common import (
    infer_dtype_from_object,
    is_bool_dtype,
    is_dict_like,
    is_list_like,
    is_numeric_dtype,
)
from pandas.core.dtypes.inference import is_hashable, is_integer
from pandas.core.indexes.frozen import FrozenList
from pandas.io.formats.printing import pprint_thing
from pandas.util._validators import validate_bool_kwarg

from snowflake.snowpark.modin import pandas as pd
from snowflake.snowpark.modin.pandas.base import _ATTRS_NO_LOOKUP, BasePandasDataset
from snowflake.snowpark.modin.pandas.groupby import (
    DataFrameGroupBy,
    validate_groupby_args,
)
from snowflake.snowpark.modin.pandas.series import Series
from snowflake.snowpark.modin.pandas.snow_partition_iterator import (
    SnowparkPandasRowPartitionIterator,
)
from snowflake.snowpark.modin.pandas.utils import (
    create_empty_native_pandas_frame,
    from_non_pandas,
    from_pandas,
    is_scalar,
    raise_if_native_pandas_objects,
    replace_external_data_keys_with_empty_pandas_series,
    replace_external_data_keys_with_query_compiler,
)
from snowflake.snowpark.modin.plugin._internal.aggregation_utils import (
    is_snowflake_agg_func,
)
from snowflake.snowpark.modin.plugin._internal.utils import is_repr_truncated
from snowflake.snowpark.modin.plugin._typing import DropKeep, ListLike
from snowflake.snowpark.modin.plugin.utils.error_message import (
    ErrorMessage,
    dataframe_not_implemented,
)
from snowflake.snowpark.modin.plugin.utils.warning_message import (
    SET_DATAFRAME_ATTRIBUTE_WARNING,
    WarningMessage,
)
from snowflake.snowpark.modin.utils import _inherit_docstrings, hashable, to_pandas
from snowflake.snowpark.udf import UserDefinedFunction

logger = getLogger(__name__)

DF_SETITEM_LIST_LIKE_KEY_AND_RANGE_LIKE_VALUE = (
    "Currently do not support Series or list-like keys with range-like values"
)

DF_SETITEM_SLICE_AS_SCALAR_VALUE = (
    "Currently do not support assigning a slice value as if it's a scalar value"
)

DF_ITERROWS_ITERTUPLES_WARNING_MESSAGE = (
    "{} will result eager evaluation and potential data pulling, which is inefficient. For efficient Snowpark "
    "pandas usage, consider rewriting the code with an operator (such as DataFrame.apply or DataFrame.applymap) which "
    "can work on the entire DataFrame in one shot."
)

# Dictionary of extensions assigned to this class
_DATAFRAME_EXTENSIONS_ = {}


@_inherit_docstrings(
    pandas.DataFrame,
    excluded=[
        pandas.DataFrame.flags,
        pandas.DataFrame.cov,
        pandas.DataFrame.merge,
        pandas.DataFrame.reindex,
        pandas.DataFrame.to_parquet,
        pandas.DataFrame.fillna,
    ],
    apilink="pandas.DataFrame",
)
class DataFrame(BasePandasDataset):
    _pandas_class = pandas.DataFrame

    def __init__(
        self,
        data=None,
        index=None,
        columns=None,
        dtype=None,
        copy=None,
        query_compiler=None,
    ) -> None:
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        # Siblings are other dataframes that share the same query compiler. We
        # use this list to update inplace when there is a shallow copy.
        from snowflake.snowpark.modin.pandas.utils import try_convert_index_to_native

        self._siblings = []

        # Engine.subscribe(_update_engine)
        if isinstance(data, (DataFrame, Series)):
            self._query_compiler = data._query_compiler.copy()
            if index is not None and any(i not in data.index for i in index):
                ErrorMessage.not_implemented(
                    "Passing non-existant columns or index values to constructor not"
                    + " yet implemented."
                )  # pragma: no cover
            if isinstance(data, Series):
                # We set the column name if it is not in the provided Series
                if data.name is None:
                    self.columns = [0] if columns is None else columns
                # If the columns provided are not in the named Series, pandas clears
                # the DataFrame and sets columns to the columns provided.
                elif columns is not None and data.name not in columns:
                    self._query_compiler = from_pandas(
                        self.__constructor__(columns=columns)
                    )._query_compiler
                if index is not None:
                    self._query_compiler = data.loc[index]._query_compiler
            elif columns is None and index is None:
                data._add_sibling(self)
            else:
                if columns is not None and any(i not in data.columns for i in columns):
                    ErrorMessage.not_implemented(
                        "Passing non-existant columns or index values to constructor not"
                        + " yet implemented."
                    )  # pragma: no cover
                if index is None:
                    index = slice(None)
                if columns is None:
                    columns = slice(None)
                self._query_compiler = data.loc[index, columns]._query_compiler

        # Check type of data and use appropriate constructor
        elif query_compiler is None:
            distributed_frame = from_non_pandas(data, index, columns, dtype)
            if distributed_frame is not None:
                self._query_compiler = distributed_frame._query_compiler
                return

            if isinstance(data, pandas.Index):
                pass
            elif is_list_like(data) and not is_dict_like(data):
                old_dtype = getattr(data, "dtype", None)
                values = [
                    obj._to_pandas() if isinstance(obj, Series) else obj for obj in data
                ]
                if isinstance(data, np.ndarray):
                    data = np.array(values, dtype=old_dtype)
                else:
                    try:
                        data = type(data)(values, dtype=old_dtype)
                    except TypeError:
                        data = values
            elif is_dict_like(data) and not isinstance(
                data, (pandas.Series, Series, pandas.DataFrame, DataFrame)
            ):
                if columns is not None:
                    data = {key: value for key, value in data.items() if key in columns}

                if len(data) and all(isinstance(v, Series) for v in data.values()):
                    from .general import concat

                    new_qc = concat(
                        data.values(), axis=1, keys=data.keys()
                    )._query_compiler

                    if dtype is not None:
                        new_qc = new_qc.astype({col: dtype for col in new_qc.columns})
                    if index is not None:
                        new_qc = new_qc.reindex(
                            axis=0, labels=try_convert_index_to_native(index)
                        )
                    if columns is not None:
                        new_qc = new_qc.reindex(
                            axis=1, labels=try_convert_index_to_native(columns)
                        )

                    self._query_compiler = new_qc
                    return

                data = {
                    k: v._to_pandas() if isinstance(v, Series) else v
                    for k, v in data.items()
                }
            pandas_df = pandas.DataFrame(
                data=try_convert_index_to_native(data),
                index=try_convert_index_to_native(index),
                columns=try_convert_index_to_native(columns),
                dtype=dtype,
                copy=copy,
            )
            self._query_compiler = from_pandas(pandas_df)._query_compiler
        else:
            self._query_compiler = query_compiler

    def __repr__(self):
        """
        Return a string representation for a particular ``DataFrame``.

        Returns
        -------
        str
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        num_rows = pandas.get_option("display.max_rows") or len(self)
        # see _repr_html_ for comment, allow here also all column behavior
        num_cols = pandas.get_option("display.max_columns") or len(self.columns)

        (
            row_count,
            col_count,
            repr_df,
        ) = self._query_compiler.build_repr_df(num_rows, num_cols, "x")
        result = repr(repr_df)

        # if truncated, add shape information
        if is_repr_truncated(row_count, col_count, num_rows, num_cols):
            # The split here is so that we don't repr pandas row lengths.
            return result.rsplit("\n\n", 1)[0] + "\n\n[{} rows x {} columns]".format(
                row_count, col_count
            )
        else:
            return result

    def _repr_html_(self):  # pragma: no cover
        """
        Return a html representation for a particular ``DataFrame``.

        Returns
        -------
        str

        Notes
        -----
        Supports pandas `display.max_rows` and `display.max_columns` options.
        """
        num_rows = pandas.get_option("display.max_rows") or 60
        # Modin uses here 20 as default, but this does not coincide well with pandas option. Therefore allow
        # here value=0 which means display all columns.
        num_cols = pandas.get_option("display.max_columns")

        (
            row_count,
            col_count,
            repr_df,
        ) = self._query_compiler.build_repr_df(num_rows, num_cols)
        result = repr_df._repr_html_()

        if is_repr_truncated(row_count, col_count, num_rows, num_cols):
            # We split so that we insert our correct dataframe dimensions.
            return (
                result.split("<p>")[0]
                + f"<p>{row_count} rows × {col_count} columns</p>\n</div>"
            )
        else:
            return result

    def _get_columns(self) -> pd.Index:
        """
        Get the columns for this Snowpark pandas ``DataFrame``.

        Returns
        -------
        Index
            The all columns.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._query_compiler.columns

    def _set_columns(self, new_columns: Axes) -> None:
        """
        Set the columns for this Snowpark pandas  ``DataFrame``.

        Parameters
        ----------
        new_columns :
            The new columns to set.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        self._update_inplace(
            new_query_compiler=self._query_compiler.set_columns(new_columns)
        )

    columns = property(_get_columns, _set_columns)

    @property
    def ndim(self) -> int:
        return 2

    def drop_duplicates(
        self, subset=None, keep="first", inplace=False, ignore_index=False
    ):  # noqa: PR01, RT01, D200
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        """
        Return ``DataFrame`` with duplicate rows removed.
        """
        return super().drop_duplicates(
            subset=subset, keep=keep, inplace=inplace, ignore_index=ignore_index
        )

    def dropna(
        self,
        *,
        axis: Axis = 0,
        how: str | NoDefault = no_default,
        thresh: int | NoDefault = no_default,
        subset: IndexLabel = None,
        inplace: bool = False,
    ):  # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return super()._dropna(
            axis=axis, how=how, thresh=thresh, subset=subset, inplace=inplace
        )

    @property
    def dtypes(self):  # noqa: RT01, D200
        """
        Return the dtypes in the ``DataFrame``.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._query_compiler.dtypes

    def duplicated(
        self, subset: Hashable | Sequence[Hashable] = None, keep: DropKeep = "first"
    ):
        """
        Return boolean ``Series`` denoting duplicate rows.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        df = self[subset] if subset is not None else self
        new_qc = df._query_compiler.duplicated(keep=keep)
        duplicates = self._reduce_dimension(new_qc)
        # remove Series name which was assigned automatically by .apply in QC
        # this is pandas behavior, i.e., if duplicated result is a series, no name is returned
        duplicates.name = None
        return duplicates

    @property
    def empty(self) -> bool:
        """
        Indicate whether ``DataFrame`` is empty.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return len(self.columns) == 0 or len(self) == 0

    @property
    def axes(self):
        """
        Return a list representing the axes of the ``DataFrame``.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return [self.index, self.columns]

    @property
    def shape(self) -> tuple[int, int]:
        """
        Return a tuple representing the dimensionality of the ``DataFrame``.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return len(self), len(self.columns)

    def add_prefix(self, prefix):
        """
        Prefix labels with string `prefix`.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        # pandas converts non-string prefix values into str and adds it to the column labels.
        return self.__constructor__(
            query_compiler=self._query_compiler.add_substring(
                str(prefix), substring_type="prefix", axis=1
            )
        )

    def add_suffix(self, suffix):
        """
        Suffix labels with string `suffix`.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        # pandas converts non-string suffix values into str and appends it to the column labels.
        return self.__constructor__(
            query_compiler=self._query_compiler.add_substring(
                str(suffix), substring_type="suffix", axis=1
            )
        )

    def applymap(self, func: PythonFuncType, na_action: str | None = None, **kwargs):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        if not callable(func):
            raise TypeError(f"{func} is not callable")
        return self.__constructor__(
            query_compiler=self._query_compiler.applymap(
                func, na_action=na_action, **kwargs
            )
        )

    def aggregate(
        self, func: AggFuncType = None, axis: Axis = 0, *args: Any, **kwargs: Any
    ):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return super().aggregate(func, axis, *args, **kwargs)

    agg = aggregate

    def apply(
        self,
        func: AggFuncType | UserDefinedFunction,
        axis: Axis = 0,
        raw: bool = False,
        result_type: Literal["expand", "reduce", "broadcast"] | None = None,
        args=(),
        **kwargs,
    ):
        """
        Apply a function along an axis of the ``DataFrame``.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        axis = self._get_axis_number(axis)
        query_compiler = self._query_compiler.apply(
            func,
            axis,
            raw=raw,
            result_type=result_type,
            args=args,
            **kwargs,
        )
        if not isinstance(query_compiler, type(self._query_compiler)):
            # A scalar was returned
            return query_compiler

        # If True, it is an unamed series.
        # Theoretically, if df.apply returns a Series, it will only be an unnamed series
        # because the function is supposed to be series -> scalar.
        if query_compiler._modin_frame.is_unnamed_series():
            return Series(query_compiler=query_compiler)
        else:
            return self.__constructor__(query_compiler=query_compiler)

    def groupby(
        self,
        by=None,
        axis: Axis | NoDefault = no_default,
        level: IndexLabel | None = None,
        as_index: bool = True,
        sort: bool = True,
        group_keys: bool = True,
        observed: bool | NoDefault = no_default,
        dropna: bool = True,
    ):
        """
        Group ``DataFrame`` using a mapper or by a ``Series`` of columns.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        if axis is not no_default:
            axis = self._get_axis_number(axis)
            if axis == 1:
                warnings.warn(
                    "DataFrame.groupby with axis=1 is deprecated. Do "
                    + "`frame.T.groupby(...)` without axis instead.",
                    FutureWarning,
                    stacklevel=1,
                )
            else:
                warnings.warn(
                    "The 'axis' keyword in DataFrame.groupby is deprecated and "
                    + "will be removed in a future version.",
                    FutureWarning,
                    stacklevel=1,
                )
        else:
            axis = 0

        validate_groupby_args(by, level, observed)

        axis = self._get_axis_number(axis)

        if axis != 0 and as_index is False:
            raise ValueError("as_index=False only valid for axis=0")

        idx_name = None

        if (
            not isinstance(by, Series)
            and is_list_like(by)
            and len(by) == 1
            # if by is a list-like of (None,), we have to keep it as a list because
            # None may be referencing a column or index level whose label is
            # `None`, and by=None wold mean that there is no `by` param.
            and by[0] is not None
        ):
            by = by[0]

        if hashable(by) and (
            not callable(by) and not isinstance(by, (pandas.Grouper, FrozenList))
        ):
            idx_name = by
        elif isinstance(by, Series):
            idx_name = by.name
            if by._parent is self:
                # if the SnowSeries comes from the current dataframe,
                # convert it to labels directly for easy processing
                by = by.name
        elif is_list_like(by):
            if axis == 0 and all(
                (
                    (hashable(o) and (o in self))
                    or isinstance(o, Series)
                    or (is_list_like(o) and len(o) == len(self.shape[axis]))
                )
                for o in by
            ):
                # plit 'by's into those that belongs to the self (internal_by)
                # and those that doesn't (external_by). For SnowSeries that belongs
                # to current DataFrame, we convert it to labels for easy process.
                internal_by, external_by = [], []

                for current_by in by:
                    if hashable(current_by):
                        internal_by.append(current_by)
                    elif isinstance(current_by, Series):
                        if current_by._parent is self:
                            internal_by.append(current_by.name)
                        else:
                            external_by.append(current_by)  # pragma: no cover
                    else:
                        external_by.append(current_by)

                by = internal_by + external_by

        return DataFrameGroupBy(
            self,
            by,
            axis,
            level,
            as_index,
            sort,
            group_keys,
            idx_name,
            observed=observed,
            dropna=dropna,
        )

    def keys(self):  # noqa: RT01, D200
        """
        Get columns of the ``DataFrame``.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self.columns

    def transform(
        self, func: PythonFuncType, axis: Axis = 0, *args: Any, **kwargs: Any
    ) -> DataFrame:  # noqa: PR01, RT01, D200
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        if is_list_like(func) or is_dict_like(func):
            ErrorMessage.not_implemented(
                "dict and list parameters are not supported for transform"
            )
        # throw the same error as pandas for cases where the function type is
        # invalid.
        if not isinstance(func, str) and not callable(func):
            raise TypeError(f"{type(func)} object is not callable")

        # if the function is an aggregation function, we'll produce
        # some bogus results while pandas will throw the error the
        # code below is throwing. So we do the same.
        if is_snowflake_agg_func(func):
            raise ValueError("Function did not transform")

        return self.apply(func, axis, False, args=args, **kwargs)

    def transpose(self, copy=False, *args):  # noqa: PR01, RT01, D200
        """
        Transpose index and columns.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        if copy:
            WarningMessage.ignored_argument(
                operation="transpose",
                argument="copy",
                message="Transpose ignore copy argument in Snowpark pandas API",
            )

        if args:
            WarningMessage.ignored_argument(
                operation="transpose",
                argument="args",
                message="Transpose ignores args in Snowpark pandas API",
            )

        return self.__constructor__(query_compiler=self._query_compiler.transpose())

    T = property(transpose)

    def add(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get addition of ``DataFrame`` and `other`, element-wise (binary operator `add`).
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op(
            "add",
            other,
            axis=axis,
            level=level,
            fill_value=fill_value,
        )

    @dataframe_not_implemented()
    def assign(self, **kwargs):  # noqa: PR01, RT01, D200
        """
        Assign new columns to a ``DataFrame``.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions

        df = self.copy()
        for k, v in kwargs.items():
            if callable(v):
                df[k] = v(df)
            else:
                df[k] = v
        return df

    @dataframe_not_implemented()
    def boxplot(
        self,
        column=None,
        by=None,
        ax=None,
        fontsize=None,
        rot=0,
        grid=True,
        figsize=None,
        layout=None,
        return_type=None,
        backend=None,
        **kwargs,
    ):  # noqa: PR01, RT01, D200
        """
        Make a box plot from ``DataFrame`` columns.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return to_pandas(self).boxplot(
            column=column,
            by=by,
            ax=ax,
            fontsize=fontsize,
            rot=rot,
            grid=grid,
            figsize=figsize,
            layout=layout,
            return_type=return_type,
            backend=backend,
            **kwargs,
        )

    @dataframe_not_implemented()
    def combine(
        self, other, func, fill_value=None, overwrite=True
    ):  # noqa: PR01, RT01, D200
        """
        Perform column-wise combine with another ``DataFrame``.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return super().combine(other, func, fill_value=fill_value, overwrite=overwrite)

    @dataframe_not_implemented()
    def compare(
        self,
        other,
        align_axis=1,
        keep_shape: bool = False,
        keep_equal: bool = False,
        result_names=("self", "other"),
    ) -> DataFrame:  # noqa: PR01, RT01, D200
        """
        Compare to another ``DataFrame`` and show the differences.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        if not isinstance(other, DataFrame):
            raise TypeError(f"Cannot compare DataFrame to {type(other)}")
        other = self._validate_other(other, 0, compare_index=True)
        return self.__constructor__(
            query_compiler=self._query_compiler.compare(
                other,
                align_axis=align_axis,
                keep_shape=keep_shape,
                keep_equal=keep_equal,
                result_names=result_names,
            )
        )

    @dataframe_not_implemented()
    def corr(
        self, method="pearson", min_periods=1, numeric_only=False
    ):  # noqa: PR01, RT01, D200
        """
        Compute pairwise correlation of columns, excluding NA/null values.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        if not numeric_only:
            return self._default_to_pandas(
                pandas.DataFrame.corr,
                method=method,
                min_periods=min_periods,
                numeric_only=numeric_only,
            )
        return self.__constructor__(
            query_compiler=self._query_compiler.corr(
                method=method,
                min_periods=min_periods,
            )
        )

    @dataframe_not_implemented()
    def corrwith(
        self, other, axis=0, drop=False, method="pearson", numeric_only=False
    ):  # noqa: PR01, RT01, D200
        """
        Compute pairwise correlation.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        if isinstance(other, DataFrame):
            other = other._query_compiler.to_pandas()
        return self._default_to_pandas(
            pandas.DataFrame.corrwith,
            other,
            axis=axis,
            drop=drop,
            method=method,
            numeric_only=numeric_only,
        )

    @dataframe_not_implemented()
    def cov(
        self,
        min_periods: int | None = None,
        ddof: int | None = 1,
        numeric_only: bool = False,
    ):
        """
        Compute pairwise covariance of columns, excluding NA/null values.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self.__constructor__(
            query_compiler=self._query_compiler.cov(
                min_periods=min_periods,
                ddof=ddof,
                numeric_only=numeric_only,
            )
        )

    @dataframe_not_implemented()
    def dot(self, other):  # noqa: PR01, RT01, D200
        """
        Compute the matrix multiplication between the ``DataFrame`` and `other`.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions

        if isinstance(other, BasePandasDataset):
            common = self.columns.union(other.index)
            if len(common) > len(self.columns) or len(common) > len(
                other
            ):  # pragma: no cover
                raise ValueError("Matrices are not aligned")

            if isinstance(other, DataFrame):
                return self.__constructor__(
                    query_compiler=self._query_compiler.dot(
                        other.reindex(index=common), squeeze_self=False
                    )
                )
            else:
                return self._reduce_dimension(
                    query_compiler=self._query_compiler.dot(
                        other.reindex(index=common), squeeze_self=False
                    )
                )

        other = np.asarray(other)
        if self.shape[1] != other.shape[0]:
            raise ValueError(
                f"Dot product shape mismatch, {self.shape} vs {other.shape}"
            )

        if len(other.shape) > 1:
            return self.__constructor__(
                query_compiler=self._query_compiler.dot(other, squeeze_self=False)
            )

        return self._reduce_dimension(
            query_compiler=self._query_compiler.dot(other, squeeze_self=False)
        )

    def eq(self, other, axis="columns", level=None):  # noqa: PR01, RT01, D200
        """
        Perform equality comparison of ``DataFrame`` and `other` (binary operator `eq`).
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op("eq", other, axis=axis, level=level)

    @dataframe_not_implemented()
    def equals(self, other):  # noqa: PR01, RT01, D200
        """
        Test whether two objects contain the same elements.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions

        if isinstance(other, pandas.DataFrame):
            # Copy into a Modin DataFrame to simplify logic below
            other = self.__constructor__(other)
        return (
            self.index.equals(other.index)
            and self.columns.equals(other.columns)
            and self.eq(other).all().all()
        )

    def _update_var_dicts_in_kwargs(self, expr, kwargs):
        """
        Copy variables with "@" prefix in `local_dict` and `global_dict` keys of kwargs.

        Parameters
        ----------
        expr : str
            The expression string to search variables with "@" prefix.
        kwargs : dict
            See the documentation for eval() for complete details on the keyword arguments accepted by query().
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        if "@" not in expr:
            return
        frame = sys._getframe()
        try:
            f_locals = frame.f_back.f_back.f_back.f_back.f_locals
            f_globals = frame.f_back.f_back.f_back.f_back.f_globals
        finally:
            del frame
        local_names = set(re.findall(r"@([\w]+)", expr))
        local_dict = {}
        global_dict = {}

        for name in local_names:
            for dct_out, dct_in in ((local_dict, f_locals), (global_dict, f_globals)):
                try:
                    dct_out[name] = dct_in[name]
                except KeyError:
                    pass

        if local_dict:
            local_dict.update(kwargs.get("local_dict") or {})
            kwargs["local_dict"] = local_dict
        if global_dict:
            global_dict.update(kwargs.get("global_dict") or {})
            kwargs["global_dict"] = global_dict

    @dataframe_not_implemented()
    def eval(self, expr, inplace=False, **kwargs):  # noqa: PR01, RT01, D200
        """
        Evaluate a string describing operations on ``DataFrame`` columns.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        self._validate_eval_query(expr, **kwargs)
        inplace = validate_bool_kwarg(inplace, "inplace")
        self._update_var_dicts_in_kwargs(expr, kwargs)
        new_query_compiler = self._query_compiler.eval(expr, **kwargs)
        return_type = type(
            pandas.DataFrame(columns=self.columns)
            .astype(self.dtypes)
            .eval(expr, **kwargs)
        ).__name__
        if return_type == type(self).__name__:
            return self._create_or_update_from_compiler(new_query_compiler, inplace)
        else:
            if inplace:
                raise ValueError("Cannot operate inplace if there is no assignment")
            return getattr(sys.modules[self.__module__], return_type)(
                query_compiler=new_query_compiler
            )

    def fillna(
        self,
        value: Hashable | Mapping | Series | DataFrame = None,
        *,
        method: FillnaOptions | None = None,
        axis: Axis | None = None,
        inplace: bool = False,
        limit: int | None = None,
        downcast: dict | None = None,
    ) -> DataFrame | None:
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return super().fillna(
            self_is_series=False,
            value=value,
            method=method,
            axis=axis,
            inplace=inplace,
            limit=limit,
            downcast=downcast,
        )

    def floordiv(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get integer division of ``DataFrame`` and `other`, element-wise (binary operator `floordiv`).
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op(
            "floordiv",
            other,
            axis=axis,
            level=level,
            fill_value=fill_value,
        )

    @classmethod
    @dataframe_not_implemented()
    def from_dict(
        cls, data, orient="columns", dtype=None, columns=None
    ):  # pragma: no cover # noqa: PR01, RT01, D200
        """
        Construct ``DataFrame`` from dict of array-like or dicts.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return from_pandas(
            pandas.DataFrame.from_dict(
                data, orient=orient, dtype=dtype, columns=columns
            )
        )

    @classmethod
    @dataframe_not_implemented()
    def from_records(
        cls,
        data,
        index=None,
        exclude=None,
        columns=None,
        coerce_float=False,
        nrows=None,
    ):  # pragma: no cover # noqa: PR01, RT01, D200
        """
        Convert structured or record ndarray to ``DataFrame``.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return from_pandas(
            pandas.DataFrame.from_records(
                data,
                index=index,
                exclude=exclude,
                columns=columns,
                coerce_float=coerce_float,
                nrows=nrows,
            )
        )

    def ge(self, other, axis="columns", level=None):  # noqa: PR01, RT01, D200
        """
        Get greater than or equal comparison of ``DataFrame`` and `other`, element-wise (binary operator `ge`).
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op("ge", other, axis=axis, level=level)

    def gt(self, other, axis="columns", level=None):  # noqa: PR01, RT01, D200
        """
        Get greater than comparison of ``DataFrame`` and `other`, element-wise (binary operator `ge`).
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op("gt", other, axis=axis, level=level)

    @dataframe_not_implemented()
    def hist(
        self,
        column=None,
        by=None,
        grid=True,
        xlabelsize=None,
        xrot=None,
        ylabelsize=None,
        yrot=None,
        ax=None,
        sharex=False,
        sharey=False,
        figsize=None,
        layout=None,
        bins=10,
        **kwds,
    ):  # pragma: no cover # noqa: PR01, RT01, D200
        """
        Make a histogram of the ``DataFrame``.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._default_to_pandas(
            pandas.DataFrame.hist,
            column=column,
            by=by,
            grid=grid,
            xlabelsize=xlabelsize,
            xrot=xrot,
            ylabelsize=ylabelsize,
            yrot=yrot,
            ax=ax,
            sharex=sharex,
            sharey=sharey,
            figsize=figsize,
            layout=layout,
            bins=bins,
            **kwds,
        )

    def info(
        self,
        verbose: bool | None = None,
        buf: IO[str] | None = None,
        max_cols: int | None = None,
        memory_usage: bool | str | None = None,
        show_counts: bool | None = None,
        null_counts: bool | None = None,
    ):  # noqa: PR01, D200
        """
        Print a concise summary of the ``DataFrame``.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        def put_str(src, output_len=None, spaces=2):
            src = str(src)
            return src.ljust(output_len if output_len else len(src)) + " " * spaces

        def format_size(num):
            for x in ["bytes", "KB", "MB", "GB", "TB"]:
                if num < 1024.0:
                    return f"{num:3.1f} {x}"
                num /= 1024.0
            return f"{num:3.1f} PB"

        output = []

        type_line = str(type(self))
        index_line = "SnowflakeIndex"
        columns = self.columns
        columns_len = len(columns)
        dtypes = self.dtypes
        dtypes_line = f"dtypes: {', '.join(['{}({})'.format(dtype, count) for dtype, count in dtypes.value_counts().items()])}"

        if max_cols is None:
            max_cols = 100

        exceeds_info_cols = columns_len > max_cols

        if buf is None:
            buf = sys.stdout

        if null_counts is None:
            null_counts = not exceeds_info_cols

        if verbose is None:
            verbose = not exceeds_info_cols

        if null_counts and verbose:
            # We're gonna take items from `non_null_count` in a loop, which
            # works kinda slow with `Modin.Series`, that's why we call `_to_pandas()` here
            # that will be faster.
            non_null_count = self.count()._to_pandas()

        if memory_usage is None:
            memory_usage = True

        def get_header(spaces=2):
            output = []
            head_label = " # "
            column_label = "Column"
            null_label = "Non-Null Count"
            dtype_label = "Dtype"
            non_null_label = " non-null"
            delimiter = "-"

            lengths = {}
            lengths["head"] = max(len(head_label), len(pprint_thing(len(columns))))
            lengths["column"] = max(
                len(column_label), max(len(pprint_thing(col)) for col in columns)
            )
            lengths["dtype"] = len(dtype_label)
            dtype_spaces = (
                max(lengths["dtype"], max(len(pprint_thing(dtype)) for dtype in dtypes))
                - lengths["dtype"]
            )

            header = put_str(head_label, lengths["head"]) + put_str(
                column_label, lengths["column"]
            )
            if null_counts:
                lengths["null"] = max(
                    len(null_label),
                    max(len(pprint_thing(x)) for x in non_null_count)
                    + len(non_null_label),
                )
                header += put_str(null_label, lengths["null"])
            header += put_str(dtype_label, lengths["dtype"], spaces=dtype_spaces)

            output.append(header)

            delimiters = put_str(delimiter * lengths["head"]) + put_str(
                delimiter * lengths["column"]
            )
            if null_counts:
                delimiters += put_str(delimiter * lengths["null"])
            delimiters += put_str(delimiter * lengths["dtype"], spaces=dtype_spaces)
            output.append(delimiters)

            return output, lengths

        output.extend([type_line, index_line])

        def verbose_repr(output):
            columns_line = f"Data columns (total {len(columns)} columns):"
            header, lengths = get_header()
            output.extend([columns_line, *header])
            for i, col in enumerate(columns):
                i, col_s, dtype = map(pprint_thing, [i, col, dtypes[col]])

                to_append = put_str(f" {i}", lengths["head"]) + put_str(
                    col_s, lengths["column"]
                )
                if null_counts:
                    non_null = pprint_thing(non_null_count[col])
                    to_append += put_str(f"{non_null} non-null", lengths["null"])
                to_append += put_str(dtype, lengths["dtype"], spaces=0)
                output.append(to_append)

        def non_verbose_repr(output):
            output.append(columns._summary(name="Columns"))

        if verbose:
            verbose_repr(output)
        else:
            non_verbose_repr(output)

        output.append(dtypes_line)

        if memory_usage:
            deep = memory_usage == "deep"
            mem_usage_bytes = self.memory_usage(index=True, deep=deep).sum()
            mem_line = f"memory usage: {format_size(mem_usage_bytes)}"

            output.append(mem_line)

        output.append("")
        buf.write("\n".join(output))

    def insert(
        self,
        loc: int,
        column: Hashable,
        value: Scalar | AnyArrayLike,
        allow_duplicates: bool | NoDefault = no_default,
    ) -> None:
        """
        Insert column into ``DataFrame`` at specified location.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        raise_if_native_pandas_objects(value)
        if allow_duplicates is no_default:
            allow_duplicates = False
        if not allow_duplicates and column in self.columns:
            raise ValueError(f"cannot insert {column}, already exists")

        if not isinstance(loc, int):
            raise TypeError("loc must be int")

        # If columns labels are multilevel, we implement following behavior (this is
        # name native pandas):
        # Case 1: if 'column' is tuple it's length must be same as number of levels
        #    otherwise raise error.
        # Case 2: if 'column' is not a tuple, create a tuple out of it by filling in
        #     empty strings to match the length of column levels in self frame.
        if self.columns.nlevels > 1:
            if isinstance(column, tuple) and len(column) != self.columns.nlevels:
                # same error as native pandas.
                raise ValueError("Item must have length equal to number of levels.")
            if not isinstance(column, tuple):
                # Fill empty strings to match length of levels
                suffix = [""] * (self.columns.nlevels - 1)
                column = tuple([column] + suffix)

        # Dictionary keys are treated as index column and this should be joined with
        # index of target dataframe. This behavior is similar to 'value' being DataFrame
        # or Series, so we simply create Series from dict data here.
        if isinstance(value, dict):
            value = Series(value, name=column)

        if isinstance(value, DataFrame) or (
            isinstance(value, np.ndarray) and len(value.shape) > 1
        ):
            # Supported numpy array shapes are
            # 1. (N, )  -> Ex. [1, 2, 3]
            # 2. (N, 1) -> Ex> [[1], [2], [3]]
            if value.shape[1] != 1:
                if isinstance(value, DataFrame):
                    # Error message updated in pandas 2.1, needs to be upstreamed to OSS modin
                    raise ValueError(
                        f"Expected a one-dimensional object, got a {type(value).__name__} with {value.shape[1]} columns instead."
                    )
                else:
                    raise ValueError(
                        f"Expected a 1D array, got an array with shape {value.shape}"
                    )
            # Change numpy array shape from (N, 1) to (N, )
            if isinstance(value, np.ndarray):
                value = value.squeeze(axis=1)

        if (
            is_list_like(value)
            and not isinstance(value, (Series, DataFrame))
            and len(value) != self.shape[0]
            and not 0 == self.shape[0]  # dataframe holds no rows
        ):
            raise ValueError(
                "Length of values ({}) does not match length of index ({})".format(
                    len(value), len(self)
                )
            )
        if not -len(self.columns) <= loc <= len(self.columns):
            raise IndexError(
                f"index {loc} is out of bounds for axis 0 with size {len(self.columns)}"
            )
        elif loc < 0:
            raise ValueError("unbounded slice")

        join_on_index = False
        if isinstance(value, (Series, DataFrame)):
            value = value._query_compiler
            join_on_index = True
        elif is_list_like(value):
            value = Series(value, name=column)._query_compiler

        new_query_compiler = self._query_compiler.insert(
            loc, column, value, join_on_index
        )
        # In pandas, 'insert' operation is always inplace.
        self._update_inplace(new_query_compiler=new_query_compiler)

    @dataframe_not_implemented()
    def interpolate(
        self,
        method="linear",
        axis=0,
        limit=None,
        inplace=False,
        limit_direction: str | None = None,
        limit_area=None,
        downcast=None,
        **kwargs,
    ):  # noqa: PR01, RT01, D200
        """
        Fill NaN values using an interpolation method.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._default_to_pandas(
            pandas.DataFrame.interpolate,
            method=method,
            axis=axis,
            limit=limit,
            inplace=inplace,
            limit_direction=limit_direction,
            limit_area=limit_area,
            downcast=downcast,
            **kwargs,
        )

    def iterrows(self) -> Iterator[tuple[Hashable, Series]]:
        """
        Iterate over ``DataFrame`` rows as (index, ``Series``) pairs.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        def iterrow_builder(s):
            """Return tuple of the given `s` parameter name and the parameter themselves."""
            return s.name, s

        # Raise warning message since iterrows is very inefficient.
        WarningMessage.single_warning(
            DF_ITERROWS_ITERTUPLES_WARNING_MESSAGE.format("DataFrame.iterrows")
        )

        partition_iterator = SnowparkPandasRowPartitionIterator(self, iterrow_builder)
        yield from partition_iterator

    @dataframe_not_implemented()
    def items(self):  # noqa: D200
        """
        Iterate over (column name, ``Series``) pairs.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        def items_builder(s):
            """Return tuple of the given `s` parameter name and the parameter themselves."""
            return s.name, s

        partition_iterator = PartitionIterator(self, 1, items_builder)
        yield from partition_iterator

    @dataframe_not_implemented()
    def iteritems(self):  # noqa: RT01, D200
        """
        Iterate over (column name, ``Series``) pairs.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self.items()

    def itertuples(
        self, index: bool = True, name: str | None = "Pandas"
    ) -> Iterable[tuple[Any, ...]]:
        """
        Iterate over ``DataFrame`` rows as ``namedtuple``-s.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions

        def itertuples_builder(s):
            """Return the next namedtuple."""
            # s is the Series of values in the current row.
            fields = []  # column names
            data = []  # values under each column

            if index:
                data.append(s.name)
                fields.append("Index")

            # Fill column names and values.
            fields.extend(list(self.columns))
            data.extend(s)

            if name is not None:
                # Creating the namedtuple.
                itertuple = collections.namedtuple(name, fields, rename=True)
                return itertuple._make(data)

            # When the name is None, return a regular tuple.
            return tuple(data)

        # Raise warning message since itertuples is very inefficient.
        WarningMessage.single_warning(
            DF_ITERROWS_ITERTUPLES_WARNING_MESSAGE.format("DataFrame.itertuples")
        )
        return SnowparkPandasRowPartitionIterator(self, itertuples_builder, True)

    def join(
        self,
        other: DataFrame | Series | Iterable[DataFrame | Series],
        on: IndexLabel | None = None,
        how: str = "left",
        lsuffix: str = "",
        rsuffix: str = "",
        sort: bool = False,
        validate: str | None = None,
    ) -> DataFrame:
        """
        Join columns of another ``DataFrame``.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        for o in other if isinstance(other, list) else [other]:
            raise_if_native_pandas_objects(o)

        # Similar to native pandas we implement 'join' using 'pd.merge' method.
        # Following code is copied from native pandas (with few changes explained below)
        # https://github.com/pandas-dev/pandas/blob/v1.5.3/pandas/core/frame.py#L10002
        if isinstance(other, Series):
            # Same error as native pandas.
            if other.name is None:
                raise ValueError("Other Series must have a name")
            other = DataFrame(other)
        elif is_list_like(other):
            if any([isinstance(o, Series) and o.name is None for o in other]):
                raise ValueError("Other Series must have a name")

        if isinstance(other, DataFrame):
            if how == "cross":
                return pd.merge(
                    self,
                    other,
                    how=how,
                    on=on,
                    suffixes=(lsuffix, rsuffix),
                    sort=sort,
                    validate=validate,
                )
            return pd.merge(
                self,
                other,
                left_on=on,
                how=how,
                left_index=on is None,
                right_index=True,
                suffixes=(lsuffix, rsuffix),
                sort=sort,
                validate=validate,
            )
        else:  # List of DataFrame/Series
            # Same error as native pandas.
            if on is not None:
                raise ValueError(
                    "Joining multiple DataFrames only supported for joining on index"
                )

            # Same error as native pandas.
            if rsuffix or lsuffix:
                raise ValueError(
                    "Suffixes not supported when joining multiple DataFrames"
                )

            # NOTE: These are not the differences between Snowpark pandas API and pandas behavior
            # these are differences between native pandas join behavior when join
            # frames have unique index or not.

            # In native pandas logic to join multiple DataFrames/Series is data
            # dependent. Under the hood it will either use 'concat' or 'merge' API
            # Case 1. If all objects being joined have unique index use 'concat' (axis=1)
            # Case 2. Otherwise use 'merge' API by looping through objects left to right.
            # https://github.com/pandas-dev/pandas/blob/v1.5.3/pandas/core/frame.py#L10046

            # Even though concat (axis=1) and merge are very similar APIs they have
            # some differences which leads to inconsistent behavior in native pandas.
            # 1. Treatment of un-named Series
            # Case #1: Un-named series is allowed in concat API. Objects are joined
            #   successfully by assigning a number as columns name (see 'concat' API
            #   documentation for details on treatment of un-named series).
            # Case #2: It raises 'ValueError: Other Series must have a name'

            # 2. how='right'
            # Case #1: 'concat' API doesn't support right join. It raises
            #   'ValueError: Only can inner (intersect) or outer (union) join the other axis'
            # Case #2: Merges successfully.

            # 3. Joining frames with duplicate labels but no conflict with other frames
            # Example:  self = DataFrame(... columns=["A", "B"])
            #           other = [DataFrame(... columns=["C", "C"])]
            # Case #1: 'ValueError: Indexes have overlapping values'
            # Case #2: Merged successfully.

            # In addition to this, native pandas implementation also leads to another
            # type of inconsistency where left.join(other, ...) and
            # left.join([other], ...) might behave differently for cases mentioned
            # above.
            # Example:
            # import pandas as pd
            # df = pd.DataFrame({"a": [4, 5]})
            # other = pd.Series([1, 2])
            # df.join([other])  # this is successful
            # df.join(other)  # this raises 'ValueError: Other Series must have a name'

            # In Snowpark pandas API, we provide consistent behavior by always using 'merge' API
            # to join multiple DataFrame/Series. So always follow the behavior
            # documented as Case #2 above.

            joined = self
            for frame in other:
                if isinstance(frame, DataFrame):
                    overlapping_cols = set(joined.columns).intersection(
                        set(frame.columns)
                    )
                    if len(overlapping_cols) > 0:
                        # Native pandas raises: 'Indexes have overlapping values'
                        # We differ slightly from native pandas message to make it more
                        # useful to users.
                        raise ValueError(
                            f"Join dataframes have overlapping column labels: {overlapping_cols}"
                        )
                joined = pd.merge(
                    joined,
                    frame,
                    how=how,
                    left_index=True,
                    right_index=True,
                    validate=validate,
                    sort=sort,
                    suffixes=(None, None),
                )
            return joined

    def isna(self):
        return super().isna()

    def isnull(self):
        return super().isnull()

    @dataframe_not_implemented()
    def isetitem(self, loc, value):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._default_to_pandas(
            pandas.DataFrame.isetitem,
            loc=loc,
            value=value,
        )

    def le(self, other, axis="columns", level=None):  # noqa: PR01, RT01, D200
        """
        Get less than or equal comparison of ``DataFrame`` and `other`, element-wise (binary operator `le`).
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op("le", other, axis=axis, level=level)

    def lt(self, other, axis="columns", level=None):  # noqa: PR01, RT01, D200
        """
        Get less than comparison of ``DataFrame`` and `other`, element-wise (binary operator `le`).
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op("lt", other, axis=axis, level=level)

    def melt(
        self,
        id_vars=None,
        value_vars=None,
        var_name=None,
        value_name="value",
        col_level=None,
        ignore_index=True,
    ):  # noqa: PR01, RT01, D200
        """
        Unpivot a ``DataFrame`` from wide to long format, optionally leaving identifiers set.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        if id_vars is None:
            id_vars = []
        if not is_list_like(id_vars):
            id_vars = [id_vars]
        if value_vars is None:
            # Behavior of Index.difference changed in 2.2.x
            # https://github.com/pandas-dev/pandas/pull/55113
            # This change needs upstream to Modin:
            # https://github.com/modin-project/modin/issues/7206
            value_vars = self.columns.drop(id_vars)
        if var_name is None:
            columns_name = self._query_compiler.get_index_name(axis=1)
            var_name = columns_name if columns_name is not None else "variable"
        return self.__constructor__(
            query_compiler=self._query_compiler.melt(
                id_vars=id_vars,
                value_vars=value_vars,
                var_name=var_name,
                value_name=value_name,
                col_level=col_level,
                ignore_index=ignore_index,
            )
        )

    @dataframe_not_implemented()
    def memory_usage(self, index=True, deep=False):  # noqa: PR01, RT01, D200
        """
        Return the memory usage of each column in bytes.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions

        if index:
            result = self._reduce_dimension(
                self._query_compiler.memory_usage(index=False, deep=deep)
            )
            index_value = self.index.memory_usage(deep=deep)
            return pd.concat(
                [Series(index_value, index=["Index"]), result]
            )  # pragma: no cover
        return super().memory_usage(index=index, deep=deep)

    def merge(
        self,
        right: DataFrame | Series,
        how: str = "inner",
        on: IndexLabel | None = None,
        left_on: Hashable
        | AnyArrayLike
        | Sequence[Hashable | AnyArrayLike]
        | None = None,
        right_on: Hashable
        | AnyArrayLike
        | Sequence[Hashable | AnyArrayLike]
        | None = None,
        left_index: bool = False,
        right_index: bool = False,
        sort: bool = False,
        suffixes: Suffixes = ("_x", "_y"),
        copy: bool = True,
        indicator: bool = False,
        validate: str | None = None,
    ) -> DataFrame:
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        # Raise error if native pandas objects are passed.
        raise_if_native_pandas_objects(right)

        if isinstance(right, Series) and right.name is None:
            raise ValueError("Cannot merge a Series without a name")
        if not isinstance(right, (Series, DataFrame)):
            raise TypeError(
                f"Can only merge Series or DataFrame objects, a {type(right)} was passed"
            )

        if isinstance(right, Series):
            right_column_nlevels = (
                len(right.name) if isinstance(right.name, tuple) else 1
            )
        else:
            right_column_nlevels = right.columns.nlevels
        if self.columns.nlevels != right_column_nlevels:
            # This is deprecated in native pandas. We raise explicit error for this.
            raise ValueError(
                "Can not merge objects with different column levels."
                + f" ({self.columns.nlevels} levels on the left,"
                + f" {right_column_nlevels} on the right)"
            )

        # Merge empty native pandas dataframes for error checking. Otherwise, it will
        # require a lot of logic to be written. This takes care of raising errors for
        # following scenarios:
        # 1. Only 'left_index' is set to True.
        # 2. Only 'right_index is set to True.
        # 3. Only 'left_on' is provided.
        # 4. Only 'right_on' is provided.
        # 5. 'on' and 'left_on' both are provided
        # 6. 'on' and 'right_on' both are provided
        # 7. 'on' and 'left_index' both are provided
        # 8. 'on' and 'right_index' both are provided
        # 9. 'left_on' and 'left_index' both are provided
        # 10. 'right_on' and 'right_index' both are provided
        # 11. Length mismatch between 'left_on' and 'right_on'
        # 12. 'left_index' is not a bool
        # 13. 'right_index' is not a bool
        # 14. 'on' is not None and how='cross'
        # 15. 'left_on' is not None and how='cross'
        # 16. 'right_on' is not None and how='cross'
        # 17. 'left_index' is True and how='cross'
        # 18. 'right_index' is True and how='cross'
        # 19. Unknown label in 'on', 'left_on' or 'right_on'
        # 20. Provided 'suffixes' is not sufficient to resolve conflicts.
        # 21. Merging on column with duplicate labels.
        # 22. 'how' not in {'left', 'right', 'inner', 'outer', 'cross'}
        # 23. conflict with existing labels for array-like join key
        # 24. 'indicator' argument is not bool or str
        # 25. indicator column label conflicts with existing data labels
        create_empty_native_pandas_frame(self).merge(
            create_empty_native_pandas_frame(right),
            on=on,
            how=how,
            left_on=replace_external_data_keys_with_empty_pandas_series(left_on),
            right_on=replace_external_data_keys_with_empty_pandas_series(right_on),
            left_index=left_index,
            right_index=right_index,
            suffixes=suffixes,
            indicator=indicator,
        )

        return self.__constructor__(
            query_compiler=self._query_compiler.merge(
                right._query_compiler,
                how=how,
                on=on,
                left_on=replace_external_data_keys_with_query_compiler(self, left_on),
                right_on=replace_external_data_keys_with_query_compiler(
                    right, right_on
                ),
                left_index=left_index,
                right_index=right_index,
                sort=sort,
                suffixes=suffixes,
                copy=copy,
                indicator=indicator,
                validate=validate,
            )
        )

    def mod(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get modulo of ``DataFrame`` and `other`, element-wise (binary operator `mod`).
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op(
            "mod",
            other,
            axis=axis,
            level=level,
            fill_value=fill_value,
        )

    def mul(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get multiplication of ``DataFrame`` and `other`, element-wise (binary operator `mul`).
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op(
            "mul",
            other,
            axis=axis,
            level=level,
            fill_value=fill_value,
        )

    multiply = mul

    def rmul(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get multiplication of ``DataFrame`` and `other`, element-wise (binary operator `mul`).
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op(
            "rmul",
            other,
            axis=axis,
            level=level,
            fill_value=fill_value,
        )

    def ne(self, other, axis="columns", level=None):  # noqa: PR01, RT01, D200
        """
        Get not equal comparison of ``DataFrame`` and `other`, element-wise (binary operator `ne`).
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op("ne", other, axis=axis, level=level)

    @dataframe_not_implemented()
    def nlargest(self, n, columns, keep="first"):  # noqa: PR01, RT01, D200
        """
        Return the first `n` rows ordered by `columns` in descending order.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self.__constructor__(
            query_compiler=self._query_compiler.nlargest(n, columns, keep)
        )

    @dataframe_not_implemented()
    def nsmallest(self, n, columns, keep="first"):  # noqa: PR01, RT01, D200
        """
        Return the first `n` rows ordered by `columns` in ascending order.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self.__constructor__(
            query_compiler=self._query_compiler.nsmallest(
                n=n, columns=columns, keep=keep
            )
        )

    @dataframe_not_implemented()
    def unstack(self, level=-1, fill_value=None):  # noqa: PR01, RT01, D200
        """
        Pivot a level of the (necessarily hierarchical) index labels.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        if not isinstance(self.index, pandas.MultiIndex) or (
            isinstance(self.index, pandas.MultiIndex)
            and is_list_like(level)
            and len(level) == self.index.nlevels
        ):
            return self._reduce_dimension(
                query_compiler=self._query_compiler.unstack(level, fill_value)
            )
        else:
            return self.__constructor__(
                query_compiler=self._query_compiler.unstack(level, fill_value)
            )

    @dataframe_not_implemented()
    def pivot(self, index=None, columns=None, values=None):  # noqa: PR01, RT01, D200
        """
        Return reshaped ``DataFrame`` organized by given index / column values.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self.__constructor__(
            query_compiler=self._query_compiler.pivot(
                index=index, columns=columns, values=values
            )
        )

    def pivot_table(
        self,
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
        """
        Create a spreadsheet-style pivot table as a ``DataFrame``.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        result = self.__constructor__(
            query_compiler=self._query_compiler.pivot_table(
                index=index,
                values=values,
                columns=columns,
                aggfunc=aggfunc,
                fill_value=fill_value,
                margins=margins,
                dropna=dropna,
                margins_name=margins_name,
                observed=observed,
                sort=sort,
            )
        )
        return result

    @dataframe_not_implemented()
    @property
    def plot(
        self,
        x=None,
        y=None,
        kind="line",
        ax=None,
        subplots=False,
        sharex=None,
        sharey=False,
        layout=None,
        figsize=None,
        use_index=True,
        title=None,
        grid=None,
        legend=True,
        style=None,
        logx=False,
        logy=False,
        loglog=False,
        xticks=None,
        yticks=None,
        xlim=None,
        ylim=None,
        rot=None,
        fontsize=None,
        colormap=None,
        table=False,
        yerr=None,
        xerr=None,
        secondary_y=False,
        sort_columns=False,
        **kwargs,
    ):  # noqa: PR01, RT01, D200
        """
        Make plots of ``DataFrame``.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._to_pandas().plot

    def pow(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get exponential power of ``DataFrame`` and `other`, element-wise (binary operator `pow`).
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op(
            "pow",
            other,
            axis=axis,
            level=level,
            fill_value=fill_value,
        )

    @dataframe_not_implemented()
    def prod(
        self,
        axis=None,
        skipna=True,
        numeric_only=False,
        min_count=0,
        **kwargs,
    ):  # noqa: PR01, RT01, D200
        """
        Return the product of the values over the requested axis.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        validate_bool_kwarg(skipna, "skipna", none_allowed=False)
        axis = self._get_axis_number(axis)
        axis_to_apply = self.columns if axis else self.index
        if (
            skipna is not False
            and numeric_only is None
            and min_count > len(axis_to_apply)
        ):
            new_index = self.columns if not axis else self.index
            return Series(
                [np.nan] * len(new_index), index=new_index, dtype=np.dtype("object")
            )

        data = self._validate_dtypes_sum_prod_mean(axis, numeric_only, ignore_axis=True)
        if min_count > 1:
            return data._reduce_dimension(
                data._query_compiler.prod_min_count(
                    axis=axis,
                    skipna=skipna,
                    numeric_only=numeric_only,
                    min_count=min_count,
                    **kwargs,
                )
            )
        return data._reduce_dimension(
            data._query_compiler.prod(
                axis=axis,
                skipna=skipna,
                numeric_only=numeric_only,
                min_count=min_count,
                **kwargs,
            )
        )

    product = prod

    def quantile(
        self,
        q: Scalar | ListLike = 0.5,
        axis: Axis = 0,
        numeric_only: bool = False,
        interpolation: Literal[
            "linear", "lower", "higher", "midpoint", "nearest"
        ] = "linear",
        method: Literal["single", "table"] = "single",
    ):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return super().quantile(
            q=q,
            axis=axis,
            numeric_only=numeric_only,
            interpolation=interpolation,
            method=method,
        )

    @dataframe_not_implemented()
    def query(self, expr, inplace=False, **kwargs):  # noqa: PR01, RT01, D200
        """
        Query the columns of a ``DataFrame`` with a boolean expression.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        self._update_var_dicts_in_kwargs(expr, kwargs)
        self._validate_eval_query(expr, **kwargs)
        inplace = validate_bool_kwarg(inplace, "inplace")
        new_query_compiler = self._query_compiler.query(expr, **kwargs)
        return self._create_or_update_from_compiler(new_query_compiler, inplace)

    def rename(
        self,
        mapper: Renamer | None = None,
        *,
        index: Renamer | None = None,
        columns: Renamer | None = None,
        axis: Axis | None = None,
        copy: bool | None = None,
        inplace: bool = False,
        level: Level | None = None,
        errors: IgnoreRaise = "ignore",
    ) -> DataFrame | None:
        """
        Alter axes labels.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        inplace = validate_bool_kwarg(inplace, "inplace")
        if mapper is None and index is None and columns is None:
            raise TypeError("must pass an index to rename")

        if index is not None or columns is not None:
            if axis is not None:
                raise TypeError(
                    "Cannot specify both 'axis' and any of 'index' or 'columns'"
                )
            elif mapper is not None:
                raise TypeError(
                    "Cannot specify both 'mapper' and any of 'index' or 'columns'"
                )
        else:
            # use the mapper argument
            if axis and self._get_axis_number(axis) == 1:
                columns = mapper
            else:
                index = mapper

        if copy is not None:
            WarningMessage.ignored_argument(
                operation="dataframe.rename",
                argument="copy",
                message="copy parameter has been ignored with Snowflake execution engine",
            )

        if isinstance(index, dict):
            index = Series(index)

        new_qc = self._query_compiler.rename(
            index_renamer=index, columns_renamer=columns, level=level, errors=errors
        )
        return self._create_or_update_from_compiler(
            new_query_compiler=new_qc, inplace=inplace
        )

    @dataframe_not_implemented()
    def reindex(
        self,
        labels=None,
        index=None,
        columns=None,
        axis=None,
        method=None,
        copy=None,
        level=None,
        fill_value=np.nan,
        limit=None,
        tolerance=None,
    ):  # noqa: PR01, RT01, D200
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions

        axis = self._get_axis_number(axis)
        if axis == 0 and labels is not None:
            index = labels
        elif labels is not None:
            columns = labels
        return super().reindex(
            index=index,
            columns=columns,
            method=method,
            copy=copy,
            level=level,
            fill_value=fill_value,
            limit=limit,
            tolerance=tolerance,
        )

    def replace(
        self,
        to_replace=None,
        value=no_default,
        inplace: bool = False,
        limit=None,
        regex: bool = False,
        method: str | NoDefault = no_default,
    ):
        """
        Replace values given in `to_replace` with `value`.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        inplace = validate_bool_kwarg(inplace, "inplace")
        new_query_compiler = self._query_compiler.replace(
            to_replace=to_replace,
            value=value,
            limit=limit,
            regex=regex,
            method=method,
        )
        return self._create_or_update_from_compiler(new_query_compiler, inplace)

    def rfloordiv(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get integer division of ``DataFrame`` and `other`, element-wise (binary operator `rfloordiv`).
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op(
            "rfloordiv",
            other,
            axis=axis,
            level=level,
            fill_value=fill_value,
        )

    def radd(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get addition of ``DataFrame`` and `other`, element-wise (binary operator `radd`).
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op(
            "radd",
            other,
            axis=axis,
            level=level,
            fill_value=fill_value,
        )

    def rmod(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get modulo of ``DataFrame`` and `other`, element-wise (binary operator `rmod`).
        """
        return self._binary_op(
            "rmod",
            other,
            axis=axis,
            level=level,
            fill_value=fill_value,
        )

    def round(self, decimals=0, *args, **kwargs):  # noqa: PR01, RT01, D200
        return super().round(decimals, args=args, **kwargs)

    def rpow(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get exponential power of ``DataFrame`` and `other`, element-wise (binary operator `rpow`).
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op(
            "rpow",
            other,
            axis=axis,
            level=level,
            fill_value=fill_value,
        )

    def rsub(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get subtraction of ``DataFrame`` and `other`, element-wise (binary operator `rsub`).
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op(
            "rsub",
            other,
            axis=axis,
            level=level,
            fill_value=fill_value,
        )

    def rtruediv(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get floating division of ``DataFrame`` and `other`, element-wise (binary operator `rtruediv`).
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op(
            "rtruediv",
            other,
            axis=axis,
            level=level,
            fill_value=fill_value,
        )

    rdiv = rtruediv

    def select_dtypes(
        self,
        include: ListLike | str | type | None = None,
        exclude: ListLike | str | type | None = None,
    ) -> DataFrame:
        """
        Return a subset of the ``DataFrame``'s columns based on the column dtypes.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        # This line defers argument validation to pandas, which will raise errors on our behalf in cases
        # like if `include` and `exclude` are None, the same type is specified in both lists, or a string
        # dtype (as opposed to object) is specified.
        pandas.DataFrame().select_dtypes(include, exclude)

        if include and not is_list_like(include):
            include = [include]
        elif include is None:
            include = []
        if exclude and not is_list_like(exclude):
            exclude = [exclude]
        elif exclude is None:
            exclude = []

        sel = tuple(map(set, (include, exclude)))

        # The width of the np.int_/float_ alias differs between Windows and other platforms, so
        # we need to include a workaround.
        # https://github.com/numpy/numpy/issues/9464
        # https://github.com/pandas-dev/pandas/blob/f538741432edf55c6b9fb5d0d496d2dd1d7c2457/pandas/core/frame.py#L5036
        def check_sized_number_infer_dtypes(dtype):
            if (isinstance(dtype, str) and dtype == "int") or (dtype is int):
                return [np.int32, np.int64]
            elif dtype == "float" or dtype is float:
                return [np.float64, np.float32]
            else:
                return [infer_dtype_from_object(dtype)]

        include, exclude = map(
            lambda x: set(
                itertools.chain.from_iterable(map(check_sized_number_infer_dtypes, x))
            ),
            sel,
        )
        # We need to index on column position rather than label in case of duplicates
        include_these = pandas.Series(not bool(include), index=range(len(self.columns)))
        exclude_these = pandas.Series(not bool(exclude), index=range(len(self.columns)))

        def is_dtype_instance_mapper(dtype):
            return functools.partial(issubclass, dtype.type)

        for i, dtype in enumerate(self.dtypes):
            if include:
                include_these[i] = any(map(is_dtype_instance_mapper(dtype), include))
            if exclude:
                exclude_these[i] = not any(
                    map(is_dtype_instance_mapper(dtype), exclude)
                )

        dtype_indexer = include_these & exclude_these
        indicate = [i for i, should_keep in dtype_indexer.items() if should_keep]
        # We need to use iloc instead of drop in case of duplicate column names
        return self.iloc[:, indicate]

    def shift(
        self,
        periods: int | Sequence[int] = 1,
        freq=None,
        axis: Axis = 0,
        fill_value: Hashable = no_default,
        suffix: str | None = None,
    ) -> DataFrame:
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return super().shift(periods, freq, axis, fill_value, suffix)

    def set_index(
        self,
        keys: IndexLabel
        | list[IndexLabel | pd.Index | pd.Series | list | np.ndarray | Iterable],
        drop: bool = True,
        append: bool = False,
        inplace: bool = False,
        verify_integrity: bool = False,
    ) -> None | DataFrame:
        """
        Set the ``DataFrame`` index using existing columns.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        inplace = validate_bool_kwarg(inplace, "inplace")
        if not isinstance(keys, list):
            keys = [keys]

        # make sure key is either hashable, index, or series
        label_or_series = []

        missing = []
        columns = self.columns.tolist()
        for key in keys:
            raise_if_native_pandas_objects(key)
            if isinstance(key, pd.Series):
                label_or_series.append(key._query_compiler)
            elif isinstance(key, (np.ndarray, list, Iterator)):
                label_or_series.append(pd.Series(key)._query_compiler)
            elif isinstance(key, (pd.Index, pandas.MultiIndex)):
                label_or_series += [
                    s._query_compiler for s in self._to_series_list(key)
                ]
            else:
                if not is_hashable(key):
                    raise TypeError(
                        f'The parameter "keys" may be a column key, one-dimensional array, or a list '
                        f"containing only valid column keys and one-dimensional arrays. Received column "
                        f"of type {type(key)}"
                    )
                label_or_series.append(key)
                found = key in columns
                if columns.count(key) > 1:
                    raise ValueError(f"The column label '{key}' is not unique")
                elif not found:
                    missing.append(key)

        if missing:
            raise KeyError(f"None of {missing} are in the columns")

        new_query_compiler = self._query_compiler.set_index(
            label_or_series, drop=drop, append=append
        )

        # TODO: SNOW-782633 improve this code once duplicate is supported
        # this needs to pull all index which is inefficient
        if verify_integrity and not new_query_compiler.index.is_unique:
            duplicates = new_query_compiler.index[
                new_query_compiler.index.duplicated()
            ].unique()
            raise ValueError(f"Index has duplicate keys: {duplicates}")

        return self._create_or_update_from_compiler(new_query_compiler, inplace=inplace)

    sparse = CachedAccessor("sparse", SparseFrameAccessor)

    def squeeze(self, axis: Axis | None = None):
        """
        Squeeze 1 dimensional axis objects into scalars.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        axis = self._get_axis_number(axis) if axis is not None else None
        len_columns = self._query_compiler.get_axis_len(1)
        if axis == 1 and len_columns == 1:
            return Series(query_compiler=self._query_compiler)
        if axis in [0, None]:
            # get_axis_len(0) results in a sql query to count number of rows in current
            # dataframe. We should only compute len_index if axis is 0 or None.
            len_index = len(self)
            if axis is None and (len_columns == 1 or len_index == 1):
                return Series(query_compiler=self._query_compiler).squeeze()
            if axis == 0 and len_index == 1:
                return Series(query_compiler=self.T._query_compiler)
        return self.copy()

    @dataframe_not_implemented()
    def stack(self, level=-1, dropna=True):  # noqa: PR01, RT01, D200
        """
        Stack the prescribed level(s) from columns to index.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        if not isinstance(self.columns, pandas.MultiIndex) or (
            isinstance(self.columns, pandas.MultiIndex)
            and is_list_like(level)
            and len(level) == self.columns.nlevels
        ):
            return self._reduce_dimension(
                query_compiler=self._query_compiler.stack(level, dropna)
            )
        else:
            return self.__constructor__(
                query_compiler=self._query_compiler.stack(level, dropna)
            )

    def sub(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get subtraction of ``DataFrame`` and `other`, element-wise (binary operator `sub`).
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op(
            "sub",
            other,
            axis=axis,
            level=level,
            fill_value=fill_value,
        )

    subtract = sub

    @dataframe_not_implemented()
    def to_feather(self, path, **kwargs):  # pragma: no cover # noqa: PR01, RT01, D200
        """
        Write a ``DataFrame`` to the binary Feather format.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._default_to_pandas(pandas.DataFrame.to_feather, path, **kwargs)

    @dataframe_not_implemented()
    def to_gbq(
        self,
        destination_table,
        project_id=None,
        chunksize=None,
        reauth=False,
        if_exists="fail",
        auth_local_webserver=True,
        table_schema=None,
        location=None,
        progress_bar=True,
        credentials=None,
    ):  # pragma: no cover # noqa: PR01, RT01, D200
        """
        Write a ``DataFrame`` to a Google BigQuery table.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functionsf
        return self._default_to_pandas(
            pandas.DataFrame.to_gbq,
            destination_table,
            project_id=project_id,
            chunksize=chunksize,
            reauth=reauth,
            if_exists=if_exists,
            auth_local_webserver=auth_local_webserver,
            table_schema=table_schema,
            location=location,
            progress_bar=progress_bar,
            credentials=credentials,
        )

    @dataframe_not_implemented()
    def to_orc(self, path=None, *, engine="pyarrow", index=None, engine_kwargs=None):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._default_to_pandas(
            pandas.DataFrame.to_orc,
            path=path,
            engine=engine,
            index=index,
            engine_kwargs=engine_kwargs,
        )

    @dataframe_not_implemented()
    def to_html(
        self,
        buf=None,
        columns=None,
        col_space=None,
        header=True,
        index=True,
        na_rep="NaN",
        formatters=None,
        float_format=None,
        sparsify=None,
        index_names=True,
        justify=None,
        max_rows=None,
        max_cols=None,
        show_dimensions=False,
        decimal=".",
        bold_rows=True,
        classes=None,
        escape=True,
        notebook=False,
        border=None,
        table_id=None,
        render_links=False,
        encoding=None,
    ):  # noqa: PR01, RT01, D200
        """
        Render a ``DataFrame`` as an HTML table.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._default_to_pandas(
            pandas.DataFrame.to_html,
            buf=buf,
            columns=columns,
            col_space=col_space,
            header=header,
            index=index,
            na_rep=na_rep,
            formatters=formatters,
            float_format=float_format,
            sparsify=sparsify,
            index_names=index_names,
            justify=justify,
            max_rows=max_rows,
            max_cols=max_cols,
            show_dimensions=show_dimensions,
            decimal=decimal,
            bold_rows=bold_rows,
            classes=classes,
            escape=escape,
            notebook=notebook,
            border=border,
            table_id=table_id,
            render_links=render_links,
            encoding=None,
        )

    @dataframe_not_implemented()
    def to_parquet(
        self,
        path=None,
        engine="auto",
        compression="snappy",
        index=None,
        partition_cols=None,
        storage_options: StorageOptions = None,
        **kwargs,
    ):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        from snowflake.snowpark.modin.pandas.dispatching.factories.dispatcher import (
            FactoryDispatcher,
        )

        return FactoryDispatcher.to_parquet(
            self._query_compiler,
            path=path,
            engine=engine,
            compression=compression,
            index=index,
            partition_cols=partition_cols,
            storage_options=storage_options,
            **kwargs,
        )

    @dataframe_not_implemented()
    def to_period(
        self, freq=None, axis=0, copy=True
    ):  # pragma: no cover # noqa: PR01, RT01, D200
        """
        Convert ``DataFrame`` from ``DatetimeIndex`` to ``PeriodIndex``.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return super().to_period(freq=freq, axis=axis, copy=copy)

    @dataframe_not_implemented()
    def to_records(
        self, index=True, column_dtypes=None, index_dtypes=None
    ):  # noqa: PR01, RT01, D200
        """
        Convert ``DataFrame`` to a NumPy record array.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._default_to_pandas(
            pandas.DataFrame.to_records,
            index=index,
            column_dtypes=column_dtypes,
            index_dtypes=index_dtypes,
        )

    @dataframe_not_implemented()
    def to_stata(
        self,
        path: FilePath | WriteBuffer[bytes],
        convert_dates: dict[Hashable, str] | None = None,
        write_index: bool = True,
        byteorder: str | None = None,
        time_stamp: datetime.datetime | None = None,
        data_label: str | None = None,
        variable_labels: dict[Hashable, str] | None = None,
        version: int | None = 114,
        convert_strl: Sequence[Hashable] | None = None,
        compression: CompressionOptions = "infer",
        storage_options: StorageOptions = None,
        *,
        value_labels: dict[Hashable, dict[float | int, str]] | None = None,
    ):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._default_to_pandas(
            pandas.DataFrame.to_stata,
            path,
            convert_dates=convert_dates,
            write_index=write_index,
            byteorder=byteorder,
            time_stamp=time_stamp,
            data_label=data_label,
            variable_labels=variable_labels,
            version=version,
            convert_strl=convert_strl,
            compression=compression,
            storage_options=storage_options,
            value_labels=value_labels,
        )

    @dataframe_not_implemented()
    def to_xml(
        self,
        path_or_buffer=None,
        index=True,
        root_name="data",
        row_name="row",
        na_rep=None,
        attr_cols=None,
        elem_cols=None,
        namespaces=None,
        prefix=None,
        encoding="utf-8",
        xml_declaration=True,
        pretty_print=True,
        parser="lxml",
        stylesheet=None,
        compression="infer",
        storage_options=None,
    ):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self.__constructor__(
            query_compiler=self._query_compiler.default_to_pandas(
                pandas.DataFrame.to_xml,
                path_or_buffer=path_or_buffer,
                index=index,
                root_name=root_name,
                row_name=row_name,
                na_rep=na_rep,
                attr_cols=attr_cols,
                elem_cols=elem_cols,
                namespaces=namespaces,
                prefix=prefix,
                encoding=encoding,
                xml_declaration=xml_declaration,
                pretty_print=pretty_print,
                parser=parser,
                stylesheet=stylesheet,
                compression=compression,
                storage_options=storage_options,
            )
        )

    def to_dict(
        self,
        orient: Literal[
            "dict", "list", "series", "split", "tight", "records", "index"
        ] = "dict",
        into: type[dict] = dict,
    ) -> dict | list[dict]:
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._to_pandas().to_dict(orient=orient, into=into)

    def to_timestamp(
        self, freq=None, how="start", axis=0, copy=True
    ):  # noqa: PR01, RT01, D200
        """
        Cast to DatetimeIndex of timestamps, at *beginning* of period.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return super().to_timestamp(freq=freq, how=how, axis=axis, copy=copy)

    def truediv(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get floating division of ``DataFrame`` and `other`, element-wise (binary operator `truediv`).
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op(
            "truediv",
            other,
            axis=axis,
            level=level,
            fill_value=fill_value,
        )

    div = divide = truediv

    def update(
        self, other, join="left", overwrite=True, filter_func=None, errors="ignore"
    ):  # noqa: PR01, RT01, D200
        """
        Modify in place using non-NA values from another ``DataFrame``.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        if not isinstance(other, DataFrame):
            other = self.__constructor__(other)
        query_compiler = self._query_compiler.df_update(
            other._query_compiler,
            join=join,
            overwrite=overwrite,
            filter_func=filter_func,
            errors=errors,
        )
        self._update_inplace(new_query_compiler=query_compiler)

    def diff(
        self,
        periods: int = 1,
        axis: Axis = 0,
    ):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return super().diff(
            periods=periods,
            axis=axis,
        )

    def drop(
        self,
        labels: IndexLabel = None,
        axis: Axis = 0,
        index: IndexLabel = None,
        columns: IndexLabel = None,
        level: Level = None,
        inplace: bool = False,
        errors: IgnoreRaise = "raise",
    ):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return super().drop(
            labels=labels,
            axis=axis,
            index=index,
            columns=columns,
            level=level,
            inplace=inplace,
            errors=errors,
        )

    def value_counts(
        self,
        subset: Sequence[Hashable] | None = None,
        normalize: bool = False,
        sort: bool = True,
        ascending: bool = False,
        dropna: bool = True,
    ):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return Series(
            query_compiler=self._query_compiler.value_counts(
                subset=subset,
                normalize=normalize,
                sort=sort,
                ascending=ascending,
                dropna=dropna,
            ),
            name="proportion" if normalize else "count",
        )

    def mask(
        self,
        cond: DataFrame | Series | Callable | AnyArrayLike,
        other: DataFrame | Series | Callable | Scalar | None = np.nan,
        *,
        inplace: bool = False,
        axis: Axis | None = None,
        level: Level | None = None,
    ):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        if isinstance(other, Series) and axis is None:
            raise ValueError(
                "df.mask requires an axis parameter (0 or 1) when given a Series"
            )

        return super().mask(
            cond,
            other=other,
            inplace=inplace,
            axis=axis,
            level=level,
        )

    def where(
        self,
        cond: DataFrame | Series | Callable | AnyArrayLike,
        other: DataFrame | Series | Callable | Scalar | None = np.nan,
        *,
        inplace: bool = False,
        axis: Axis | None = None,
        level: Level | None = None,
    ):
        """
        Replace values where the condition is False.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        if isinstance(other, Series) and axis is None:
            raise ValueError(
                "df.where requires an axis parameter (0 or 1) when given a Series"
            )

        return super().where(
            cond,
            other=other,
            inplace=inplace,
            axis=axis,
            level=level,
        )

    @dataframe_not_implemented()
    def xs(self, key, axis=0, level=None, drop_level=True):  # noqa: PR01, RT01, D200
        """
        Return cross-section from the ``DataFrame``.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._default_to_pandas(
            pandas.DataFrame.xs, key, axis=axis, level=level, drop_level=drop_level
        )

    def set_axis(
        self,
        labels: IndexLabel,
        *,
        axis: Axis = 0,
        copy: bool | NoDefault = no_default,  # ignored
    ):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        if not is_scalar(axis):
            raise TypeError(f"{type(axis).__name__} is not a valid type for axis.")
        return super().set_axis(
            labels=labels,
            # 'columns', 'rows, 'index, 0, and 1 are the only valid axis values for df.
            axis=pandas.DataFrame._get_axis_name(axis),
            copy=copy,
        )

    def __getattr__(self, key):
        """
        Return item identified by `key`.

        Parameters
        ----------
        key : hashable
            Key to get.

        Returns
        -------
        Any

        Notes
        -----
        First try to use `__getattribute__` method. If it fails
        try to get `key` from ``DataFrame`` fields.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        try:
            return object.__getattribute__(self, key)
        except AttributeError as err:
            if key not in _ATTRS_NO_LOOKUP and key in self.columns:
                return self[key]
            raise err

    def __setattr__(self, key, value):
        """
        Set attribute `value` identified by `key`.

        Parameters
        ----------
        key : hashable
            Key to set.
        value : Any
            Value to set.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        # While we let users assign to a column labeled "x" with "df.x" , there
        # are some attributes that we should assume are NOT column names and
        # therefore should follow the default Python object assignment
        # behavior. These are:
        # - anything in self.__dict__. This includes any attributes that the
        #   user has added to the dataframe with,  e.g., `df.c = 3`, and
        #   any attribute that Modin has added to the frame, e.g.
        #   `_query_compiler` and `_siblings`
        # - `_query_compiler`, which Modin initializes before it appears in
        #   __dict__
        # - `_siblings`, which Modin initializes before it appears in __dict__
        # - `_cache`, which pandas.cache_readonly uses to cache properties
        #   before it appears in __dict__.
        if key in ("_query_compiler", "_siblings", "_cache") or key in self.__dict__:
            pass
        elif key in self and key not in dir(self):
            self.__setitem__(key, value)
            # Note: return immediately so we don't keep this `key` as dataframe state.
            # `__getattr__` will return the columns not present in `dir(self)`, so we do not need
            # to manually track this state in the `dir`.
            return
        elif is_list_like(value) and key not in ["index", "columns"]:
            WarningMessage.single_warning(
                SET_DATAFRAME_ATTRIBUTE_WARNING
            )  # pragma: no cover
        object.__setattr__(self, key, value)

    def __setitem__(self, key: Any, value: Any):
        """
        Set attribute `value` identified by `key`.

        Args:
            key: Key to set
            value:  Value to set

        Note:
            In the case where value is any list like or array, pandas checks the array length against the number of rows
            of the input dataframe. If there is a mismatch, a ValueError is raised. Snowpark pandas indexing won't throw
            a ValueError because knowing the length of the current dataframe can trigger eager evaluations; instead if
            the array is longer than the number of rows we ignore the additional values. If the array is shorter, we use
            enlargement filling with the last value in the array.

        Returns:
            None
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        key = apply_if_callable(key, self)
        if isinstance(key, DataFrame) or (
            isinstance(key, np.ndarray) and len(key.shape) == 2
        ):
            # This case uses mask's codepath to perform the set, but
            # we need to duplicate the code here since we are passing
            # an additional kwarg `cond_fillna_with_true` to the QC here.
            # We need this additional kwarg, since if df.shape
            # and key.shape do not align (i.e. df has more rows),
            # mask's codepath would mask the additional rows in df
            # while for setitem, we need to keep the original values.
            if not isinstance(key, DataFrame):
                if key.dtype != bool:
                    raise TypeError(
                        "Must pass DataFrame or 2-d ndarray with boolean values only"
                    )
                key = DataFrame(key)
                key._query_compiler._shape_hint = "array"

            if value is not None:
                value = apply_if_callable(value, self)

                if isinstance(value, np.ndarray):
                    value = DataFrame(value)
                    value._query_compiler._shape_hint = "array"
                elif isinstance(value, pd.Series):
                    # pandas raises the `mask` ValueError here: Must specify axis = 0 or 1. We raise this
                    # error instead, since it is more descriptive.
                    raise ValueError(
                        "setitem with a 2D key does not support Series values."
                    )

                if isinstance(value, BasePandasDataset):
                    value = value._query_compiler

            query_compiler = self._query_compiler.mask(
                cond=key._query_compiler,
                other=value,
                axis=None,
                level=None,
                cond_fillna_with_true=True,
            )

            return self._create_or_update_from_compiler(query_compiler, inplace=True)

        # Error Checking:
        if (isinstance(key, pd.Series) or is_list_like(key)) and (
            isinstance(value, range)
        ):
            raise NotImplementedError(DF_SETITEM_LIST_LIKE_KEY_AND_RANGE_LIKE_VALUE)
        elif isinstance(value, slice):
            # Here, the whole slice is assigned as a scalar variable, i.e., a spot at an index gets a slice value.
            raise NotImplementedError(DF_SETITEM_SLICE_AS_SCALAR_VALUE)

        # Note: when key is a boolean indexer or slice the key is a row key; otherwise, the key is always a column
        # key.
        index, columns = slice(None), key
        index_is_bool_indexer = False
        if isinstance(key, slice):
            if is_integer(key.start) and is_integer(key.stop):
                # when slice are integer slice, e.g., df[1:2] = val, the behavior is the same as
                # df.iloc[1:2, :] = val
                self.iloc[key] = value
                return
            index, columns = key, slice(None)
        elif isinstance(key, pd.Series):
            if is_bool_dtype(key.dtype):
                index, columns = key, slice(None)
                index_is_bool_indexer = True
        elif is_bool_indexer(key):
            index, columns = pd.Series(key), slice(None)
            index_is_bool_indexer = True

        # The reason we do not call loc directly is that setitem has different behavior compared to loc in this case
        # we have to explicitly set matching_item_columns_by_label to False for setitem.
        index = index._query_compiler if isinstance(index, BasePandasDataset) else index
        columns = (
            columns._query_compiler
            if isinstance(columns, BasePandasDataset)
            else columns
        )
        from .indexing import is_2d_array

        matching_item_rows_by_label = not is_2d_array(value)
        if is_2d_array(value):
            value = DataFrame(value)
        item = value._query_compiler if isinstance(value, BasePandasDataset) else value
        new_qc = self._query_compiler.set_2d_labels(
            index,
            columns,
            item,
            # setitem always matches item by position
            matching_item_columns_by_label=False,
            matching_item_rows_by_label=matching_item_rows_by_label,
            index_is_bool_indexer=index_is_bool_indexer,
            # setitem always deduplicates columns. E.g., if df has two columns "A" and "B", after calling
            # df[["A","A"]] = item, df still only has two columns "A" and "B", and "A"'s values are set by the
            # second "A" column from value; instead, if we call df.loc[:, ["A", "A"]] = item, then df will have
            # three columns "A", "A", "B". Similarly, if we call df[["X","X"]] = item, df will have three columns
            # "A", "B", "X", while if we call df.loc[:, ["X", "X"]] = item, then df will have four columns "A", "B",
            # "X", "X".
            deduplicate_columns=True,
        )
        return self._update_inplace(new_query_compiler=new_qc)

    def abs(self):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return super().abs()

    def __and__(self, other):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op("__and__", other, axis=1)

    def __rand__(self, other):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op("__rand__", other, axis=1)

    def __or__(self, other):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op("__or__", other, axis=1)

    def __ror__(self, other):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op("__ror__", other, axis=1)

    def __neg__(self):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return super().__neg__()

    def __iter__(self):
        """
        Iterate over info axis.

        Returns
        -------
        iterable
            Iterator of the columns names.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return iter(self.columns)

    def __contains__(self, key):
        """
        Check if `key` in the ``DataFrame.columns``.

        Parameters
        ----------
        key : hashable
            Key to check the presence in the columns.

        Returns
        -------
        bool
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self.columns.__contains__(key)

    def __round__(self, decimals=0):
        """
        Round each value in a ``DataFrame`` to the given number of decimals.

        Parameters
        ----------
        decimals : int, default: 0
            Number of decimal places to round to.

        Returns
        -------
        DataFrame
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return super().round(decimals)

    @dataframe_not_implemented()
    def __delitem__(self, key):
        """
        Delete item identified by `key` label.

        Parameters
        ----------
        key : hashable
            Key to delete.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        if key not in self:
            raise KeyError(key)
        self._update_inplace(new_query_compiler=self._query_compiler.delitem(key))

    __add__ = add
    __iadd__ = add  # pragma: no cover
    __radd__ = radd
    __mul__ = mul
    __imul__ = mul  # pragma: no cover
    __rmul__ = rmul
    __pow__ = pow
    __ipow__ = pow  # pragma: no cover
    __rpow__ = rpow
    __sub__ = sub
    __isub__ = sub  # pragma: no cover
    __rsub__ = rsub
    __floordiv__ = floordiv
    __ifloordiv__ = floordiv  # pragma: no cover
    __rfloordiv__ = rfloordiv
    __truediv__ = truediv
    __itruediv__ = truediv  # pragma: no cover
    __rtruediv__ = rtruediv
    __mod__ = mod
    __imod__ = mod  # pragma: no cover
    __rmod__ = rmod
    __rdiv__ = rdiv

    def __dataframe__(self, nan_as_null: bool = False, allow_copy: bool = True):
        """
        Get a Modin DataFrame that implements the dataframe exchange protocol.

        See more about the protocol in https://data-apis.org/dataframe-protocol/latest/index.html.

        Parameters
        ----------
        nan_as_null : bool, default: False
            A keyword intended for the consumer to tell the producer
            to overwrite null values in the data with ``NaN`` (or ``NaT``).
            This currently has no effect; once support for nullable extension
            dtypes is added, this value should be propagated to columns.
        allow_copy : bool, default: True
            A keyword that defines whether or not the library is allowed
            to make a copy of the data. For example, copying data would be necessary
            if a library supports strided buffers, given that this protocol
            specifies contiguous buffers. Currently, if the flag is set to ``False``
            and a copy is needed, a ``RuntimeError`` will be raised.

        Returns
        -------
        ProtocolDataframe
            A dataframe object following the dataframe protocol specification.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented(
            "Snowpark pandas does not support the DataFrame interchange "
            + "protocol method `__dataframe__`. To use Snowpark pandas "
            + "DataFrames with third-party libraries that try to call the "
            + "`__dataframe__` method, please convert this Snowpark pandas "
            + "DataFrame to pandas with `to_pandas()`."
        )

        return self._query_compiler.to_dataframe(
            nan_as_null=nan_as_null, allow_copy=allow_copy
        )

    @dataframe_not_implemented()
    @property
    def attrs(self):  # noqa: RT01, D200
        """
        Return dictionary of global attributes of this dataset.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        def attrs(df):
            return df.attrs

        return self._default_to_pandas(attrs)

    @dataframe_not_implemented()
    @property
    def style(self):  # noqa: RT01, D200
        """
        Return a Styler object.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        def style(df):
            """Define __name__ attr because properties do not have it."""
            return df.style

        return self._default_to_pandas(style)

    def isin(
        self, values: ListLike | Series | DataFrame | dict[Hashable, ListLike]
    ) -> DataFrame:
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        if isinstance(values, dict):
            return super().isin(values)
        elif isinstance(values, Series):
            # Note: pandas performs explicit is_unique check here, deactivated for performance reasons.
            # if not values.index.is_unique:
            #   raise ValueError("cannot compute isin with a duplicate axis.")
            return self.__constructor__(
                query_compiler=self._query_compiler.isin(values._query_compiler)
            )
        elif isinstance(values, DataFrame):
            # Note: pandas performs explicit is_unique check here, deactivated for performance reasons.
            # if not (values.columns.is_unique and values.index.is_unique):
            #    raise ValueError("cannot compute isin with a duplicate axis.")
            return self.__constructor__(
                query_compiler=self._query_compiler.isin(values._query_compiler)
            )
        else:
            if not is_list_like(values):
                # throw pandas compatible error
                raise TypeError(
                    "only list-like or dict-like objects are allowed "
                    f"to be passed to {self.__class__.__name__}.isin(), "
                    f"you passed a '{type(values).__name__}'"
                )
            return super().isin(values)

    def _create_or_update_from_compiler(self, new_query_compiler, inplace=False):
        """
        Return or update a ``DataFrame`` with given `new_query_compiler`.

        Parameters
        ----------
        new_query_compiler : PandasQueryCompiler
            QueryCompiler to use to manage the data.
        inplace : bool, default: False
            Whether or not to perform update or creation inplace.

        Returns
        -------
        DataFrame or None
            None if update was done, ``DataFrame`` otherwise.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        assert (
            isinstance(new_query_compiler, type(self._query_compiler))
            or type(new_query_compiler) in self._query_compiler.__class__.__bases__
        ), f"Invalid Query Compiler object: {type(new_query_compiler)}"
        if not inplace:
            return self.__constructor__(query_compiler=new_query_compiler)
        else:
            self._update_inplace(new_query_compiler=new_query_compiler)

    def _get_numeric_data(self, axis: int):
        """
        Grab only numeric data from ``DataFrame``.

        Parameters
        ----------
        axis : {0, 1}
            Axis to inspect on having numeric types only.

        Returns
        -------
        DataFrame
            ``DataFrame`` with numeric data.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        # pandas ignores `numeric_only` if `axis` is 1, but we do have to drop
        # non-numeric columns if `axis` is 0.
        if axis != 0:
            return self
        return self.drop(
            columns=[
                i for i in self.dtypes.index if not is_numeric_dtype(self.dtypes[i])
            ]
        )

    def _validate_dtypes(self, numeric_only=False):
        """
        Check that all the dtypes are the same.

        Parameters
        ----------
        numeric_only : bool, default: False
            Whether or not to allow only numeric data.
            If True and non-numeric data is found, exception
            will be raised.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        dtype = self.dtypes[0]
        for t in self.dtypes:
            if numeric_only and not is_numeric_dtype(t):
                raise TypeError(f"{t} is not a numeric data type")
            elif not numeric_only and t != dtype:
                raise TypeError(f"Cannot compare type '{t}' with type '{dtype}'")

    def _validate_dtypes_sum_prod_mean(self, axis, numeric_only, ignore_axis=False):
        """
        Validate data dtype for `sum`, `prod` and `mean` methods.

        Parameters
        ----------
        axis : {0, 1}
            Axis to validate over.
        numeric_only : bool
            Whether or not to allow only numeric data.
            If True and non-numeric data is found, exception
            will be raised.
        ignore_axis : bool, default: False
            Whether or not to ignore `axis` parameter.

        Returns
        -------
        DataFrame
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        # We cannot add datetime types, so if we are summing a column with
        # dtype datetime64 and cannot ignore non-numeric types, we must throw a
        # TypeError.
        if (
            not axis
            and numeric_only is False
            and any(dtype == np.dtype("datetime64[ns]") for dtype in self.dtypes)
        ):
            raise TypeError("Cannot add Timestamp Types")

        # If our DataFrame has both numeric and non-numeric dtypes then
        # operations between these types do not make sense and we must raise a
        # TypeError. The exception to this rule is when there are datetime and
        # timedelta objects, in which case we proceed with the comparison
        # without ignoring any non-numeric types. We must check explicitly if
        # numeric_only is False because if it is None, it will default to True
        # if the operation fails with mixed dtypes.
        if (
            (axis or ignore_axis)
            and numeric_only is False
            and np.unique([is_numeric_dtype(dtype) for dtype in self.dtypes]).size == 2
        ):
            # check if there are columns with dtypes datetime or timedelta
            if all(
                dtype != np.dtype("datetime64[ns]")
                and dtype != np.dtype("timedelta64[ns]")
                for dtype in self.dtypes
            ):
                raise TypeError("Cannot operate on Numeric and Non-Numeric Types")

        return self._get_numeric_data(axis) if numeric_only else self

    def _to_pandas(
        self,
        *,
        statement_params: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> pandas.DataFrame:
        """
        Convert Snowpark pandas DataFrame to pandas DataFrame

        Args:
            statement_params: Dictionary of statement level parameters to be set while executing this action.

        Returns:
            pandas DataFrame
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._query_compiler.to_pandas(
            statement_params=statement_params, **kwargs
        )

    def _validate_eval_query(self, expr, **kwargs):
        """
        Validate the arguments of ``eval`` and ``query`` functions.

        Parameters
        ----------
        expr : str
            The expression to evaluate. This string cannot contain any
            Python statements, only Python expressions.
        **kwargs : dict
            Optional arguments of ``eval`` and ``query`` functions.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        if isinstance(expr, str) and expr == "":
            raise ValueError("expr cannot be an empty string")

        if isinstance(expr, str) and "not" in expr:
            if "parser" in kwargs and kwargs["parser"] == "python":
                ErrorMessage.not_implemented(  # pragma: no cover
                    "Snowpark pandas does not yet support 'not' in the "
                    + "expression for the methods `DataFrame.eval` or "
                    + "`DataFrame.query`"
                )

    def _reduce_dimension(self, query_compiler):
        """
        Reduce the dimension of data from the `query_compiler`.

        Parameters
        ----------
        query_compiler : BaseQueryCompiler
            Query compiler to retrieve the data.

        Returns
        -------
        Series
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return Series(query_compiler=query_compiler)

    def _set_axis_name(self, name, axis=0, inplace=False):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        axis = self._get_axis_number(axis)
        renamed = self if inplace else self.copy()
        if axis == 0:
            renamed.index = renamed.index.set_names(name)
        else:
            renamed.columns = renamed.columns.set_names(name)
        if not inplace:
            return renamed

    def _to_datetime(self, **kwargs):
        """
        Convert `self` to datetime.

        Parameters
        ----------
        **kwargs : dict
            Optional arguments to use during query compiler's
            `to_datetime` invocation.

        Returns
        -------
        Series of datetime64 dtype
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._reduce_dimension(
            query_compiler=self._query_compiler.dataframe_to_datetime(**kwargs)
        )

    # Persistance support methods - BEGIN
    @classmethod
    def _inflate_light(cls, query_compiler):
        """
        Re-creates the object from previously-serialized lightweight representation.

        The method is used for faster but not disk-storable persistence.

        Parameters
        ----------
        query_compiler : BaseQueryCompiler
            Query compiler to use for object re-creation.

        Returns
        -------
        DataFrame
            New ``DataFrame`` based on the `query_compiler`.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return cls(query_compiler=query_compiler)

    @classmethod
    def _inflate_full(cls, pandas_df):
        """
        Re-creates the object from previously-serialized disk-storable representation.

        Parameters
        ----------
        pandas_df : pandas.DataFrame
            Data to use for object re-creation.

        Returns
        -------
        DataFrame
            New ``DataFrame`` based on the `pandas_df`.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return cls(data=from_pandas(pandas_df))

    @dataframe_not_implemented()
    def __reduce__(self):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        self._query_compiler.finalize()
        # if PersistentPickle.get():
        #    return self._inflate_full, (self._to_pandas(),)
        return self._inflate_light, (self._query_compiler,)

    # Persistance support methods - END
