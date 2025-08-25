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

"""Module houses ``Index`` class, that is distributed version of ``pandas.Index``."""

from __future__ import annotations

import inspect
from functools import cached_property
from typing import Any, Callable, Hashable, Iterable, Iterator, Literal

import modin
from modin.config import context as config_context
import numpy as np
import numpy.typing as npt
import pandas as native_pd
from modin.pandas import DataFrame, Series
from modin.pandas.base import BasePandasDataset
from pandas import get_option
from pandas._libs import lib
from pandas._libs.lib import is_list_like, is_scalar, no_default
from pandas._typing import ArrayLike, DateTimeErrorChoices, DtypeObj, NaPosition, Scalar
from pandas.core.arrays import ExtensionArray
from pandas.core.dtypes.base import ExtensionDtype
from pandas.core.dtypes.common import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_float_dtype,
    is_integer_dtype,
    is_numeric_dtype,
    is_object_dtype,
    pandas_dtype,
)
from pandas.core.dtypes.inference import is_hashable

from snowflake.snowpark.modin.plugin._internal.telemetry import TelemetryMeta
from snowflake.snowpark.modin.plugin._internal.timestamp_utils import DateTimeOrigin
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)
from snowflake.snowpark.modin.plugin.extensions.utils import try_convert_index_to_native
from snowflake.snowpark.modin.plugin.utils.error_message import (
    ErrorMessage,
    index_not_implemented,
)
from snowflake.snowpark.modin.plugin.utils.warning_message import (
    WarningMessage,
    materialization_warning,
)
from snowflake.snowpark.modin.utils import (
    _inherit_docstrings,
    doc_replace_dataframe_with_link,
)
from snowflake.snowpark.types import ArrayType

_CONSTRUCTOR_DEFAULTS = {
    "dtype": None,
    "copy": False,
    "name": None,
    "tupleize_cols": True,
}


class IndexParent:
    def __init__(self, parent: DataFrame | Series) -> None:
        """
        Initialize the IndexParent object.

        IndexParent is used to keep track of the parent object that the Index is a part of.
        It tracks the parent object and the parent object's query compiler at the time of creation.

        Parameters
        ----------
        parent : DataFrame or Series
            The parent object that the Index is a part of.
        """
        assert isinstance(parent, (DataFrame, Series))
        self._parent = parent
        self._parent_qc = parent._query_compiler

    def check_and_update_parent_qc_index_names(self, names: list) -> None:
        """
        Update the Index and its parent's index names if the query compiler associated with the parent is
        different from the original query compiler recorded, i.e., an inplace update has been applied to the parent.
        """
        if self._parent._query_compiler is self._parent_qc:
            new_query_compiler = self._parent_qc.set_index_names(names)
            self._parent._update_inplace(new_query_compiler=new_query_compiler)
            # Update the query compiler after naming operation.
            self._parent_qc = new_query_compiler


@_inherit_docstrings(native_pd.Index, modify_doc=doc_replace_dataframe_with_link)
class Index(metaclass=TelemetryMeta):

    # Equivalent index type in native pandas
    _NATIVE_INDEX_TYPE = native_pd.Index

    _comparables: list[str] = ["name"]

    def __new__(
        cls,
        data: ArrayLike | native_pd.Index | Series | None = None,
        dtype: str | np.dtype | ExtensionDtype | None = _CONSTRUCTOR_DEFAULTS["dtype"],
        copy: bool = _CONSTRUCTOR_DEFAULTS["copy"],
        name: object = _CONSTRUCTOR_DEFAULTS["name"],
        tupleize_cols: bool = _CONSTRUCTOR_DEFAULTS["tupleize_cols"],
        query_compiler: SnowflakeQueryCompiler = None,
    ) -> Index:
        from snowflake.snowpark.modin.plugin.extensions.datetime_index import (
            DatetimeIndex,
        )
        from snowflake.snowpark.modin.plugin.extensions.timedelta_index import (
            TimedeltaIndex,
        )

        kwargs = {
            "dtype": dtype,
            "copy": copy,
            "name": name,
            "tupleize_cols": tupleize_cols,
        }
        query_compiler = cls._init_query_compiler(
            data, _CONSTRUCTOR_DEFAULTS, query_compiler, **kwargs
        )
        if query_compiler.is_datetime64_any_dtype(idx=0, is_index=True):
            return DatetimeIndex(query_compiler=query_compiler)
        if query_compiler.is_timedelta64_dtype(idx=0, is_index=True):
            return TimedeltaIndex(query_compiler=query_compiler)
        index = object.__new__(cls)
        # Initialize the Index
        index._query_compiler = query_compiler
        # `_parent` keeps track of the parent object that this Index is a part of.
        index._parent = None
        return index

    def __init__(
        self,
        data: ArrayLike | native_pd.Index | Series | None = None,
        dtype: str | np.dtype | ExtensionDtype | None = _CONSTRUCTOR_DEFAULTS["dtype"],
        copy: bool = _CONSTRUCTOR_DEFAULTS["copy"],
        name: object = _CONSTRUCTOR_DEFAULTS["name"],
        tupleize_cols: bool = _CONSTRUCTOR_DEFAULTS["tupleize_cols"],
        query_compiler: SnowflakeQueryCompiler = None,
    ) -> None:
        # Index is already initialized in __new__ method. We keep this method only for
        # docstring generation.
        pass  # pragma: no cover

    @classmethod
    def _init_query_compiler(
        cls,
        data: ArrayLike | native_pd.Index | Series | None,
        ctor_defaults: dict,
        query_compiler: SnowflakeQueryCompiler = None,
        **kwargs: Any,
    ) -> SnowflakeQueryCompiler:
        # Keep the backend as Snowflake within this method to ensure we always return an SFQC object.
        # In hybrid mode, the DataFrame constructor may inappropriately create a native QC object
        # that causes difficult-to-find errors further down the line.
        with config_context(Backend="Snowflake", AutoSwitchBackend=False):
            if query_compiler:
                # Raise warning if `data` is query compiler with non-default arguments.
                for arg_name, arg_value in kwargs.items():
                    assert (
                        arg_value == ctor_defaults[arg_name]
                    ), f"Non-default argument '{arg_name}={arg_value}' when constructing Index with query compiler"
            elif isinstance(data, BasePandasDataset):
                if data.ndim != 1:
                    raise ValueError("Index data must be 1 - dimensional")
                series_has_no_name = data.name is None
                idx = (
                    data.to_frame()
                    .set_index(0 if series_has_no_name else data.name)
                    .index
                )
                if series_has_no_name:
                    idx.name = None
                query_compiler = idx._query_compiler
            elif isinstance(data, Index):
                query_compiler = data._query_compiler
            else:
                query_compiler = DataFrame(
                    index=cls._NATIVE_INDEX_TYPE(data=data, **kwargs)
                )._query_compiler

            if len(query_compiler.columns):
                query_compiler = query_compiler.drop(columns=query_compiler.columns)
            return query_compiler

    def __getattr__(self, key: str) -> Any:
        try:
            return object.__getattribute__(self, key)
        except AttributeError as err:
            if not key.startswith("_"):
                native_index = self._NATIVE_INDEX_TYPE([])
                if hasattr(native_index, key):
                    # Any methods that not supported by the current Index.py but exist in a
                    # native pandas index object should raise a not implemented error for now.
                    ErrorMessage.not_implemented(f"Index.{key} is not yet implemented")
            raise err

    def _set_parent(self, parent: Series | DataFrame) -> None:
        self._parent = IndexParent(parent)

    def _binary_ops(self, method: str, other: Any) -> Index:
        if isinstance(other, Index):
            other = other.to_series().reset_index(drop=True)
        series = getattr(self.to_series().reset_index(drop=True), method)(other)
        qc = series._query_compiler
        qc = qc.set_index_from_columns(qc.columns, include_index=False)
        # Use base constructor to ensure that the correct type is returned.
        idx = Index(query_compiler=qc)
        idx.name = series.name
        return idx

    def _unary_ops(self, method: str) -> Index:
        return self.__constructor__(
            getattr(self.to_series().reset_index(drop=True), method)()
        )

    def __add__(self, other: Any) -> Index:
        return self._binary_ops("__add__", other)

    def __radd__(self, other: Any) -> Index:
        return self._binary_ops("__radd__", other)

    def __mul__(self, other: Any) -> Index:
        return self._binary_ops("__mul__", other)

    def __rmul__(self, other: Any) -> Index:
        return self._binary_ops("__rmul__", other)

    def __neg__(self) -> Index:
        return self._unary_ops("__neg__")

    def __sub__(self, other: Any) -> Index:
        return self._binary_ops("__sub__", other)

    def __rsub__(self, other: Any) -> Index:
        return self._binary_ops("__rsub__", other)

    def __truediv__(self, other: Any) -> Index:
        return self._binary_ops("__truediv__", other)

    def __rtruediv__(self, other: Any) -> Index:
        return self._binary_ops("__rtruediv__", other)

    def __floordiv__(self, other: Any) -> Index:
        return self._binary_ops("__floordiv__", other)

    def __rfloordiv__(self, other: Any) -> Index:
        return self._binary_ops("__rfloordiv__", other)

    def __pow__(self, other: Any) -> Index:
        return self._binary_ops("__pow__", other)

    def __rpow__(self, other: Any):
        return self._binary_ops("__rpow__", other)

    def __mod__(self, other: Any) -> Index:
        return self._binary_ops("__mod__", other)

    def __rmod__(self, other: Any):
        return self._binary_ops("__rmod__", other)

    def __eq__(self, other: Any) -> Index:
        return self._binary_ops("eq", other)

    def __ne__(self, other: Any) -> Index:
        return self._binary_ops("ne", other)

    def __ge__(self, other: Any) -> Index:
        return self._binary_ops("ge", other)

    def __gt__(self, other: Any) -> Index:
        return self._binary_ops("gt", other)

    def __le__(self, other: Any) -> Index:
        return self._binary_ops("le", other)

    def __lt__(self, other: Any) -> Index:
        return self._binary_ops("lt", other)

    def __or__(self, other: Any) -> Index:
        ErrorMessage.not_implemented(
            f"Index.{inspect.currentframe().f_code.co_name} is not yet implemented"
        )

    def __and__(self, other: Any) -> Index:
        ErrorMessage.not_implemented(
            f"Index.{inspect.currentframe().f_code.co_name} is not yet implemented"
        )

    def __xor__(self, other: Any) -> Index:
        ErrorMessage.not_implemented(
            f"Index.{inspect.currentframe().f_code.co_name} is not yet implemented"
        )

    def __lshift__(self, n: int) -> int:
        ErrorMessage.not_implemented(
            f"Index.{inspect.currentframe().f_code.co_name} is not yet implemented"
        )

    def __rshift__(self, n: int) -> int:
        ErrorMessage.not_implemented(
            f"Index.{inspect.currentframe().f_code.co_name} is not yet implemented"
        )

    def __rand__(self, n: int) -> int:
        ErrorMessage.not_implemented(
            f"Index.{inspect.currentframe().f_code.co_name} is not yet implemented"
        )

    def __ror__(self, n: int) -> int:
        ErrorMessage.not_implemented(
            f"Index.{inspect.currentframe().f_code.co_name} is not yet implemented"
        )

    def __rxor__(self, n: int) -> int:
        ErrorMessage.not_implemented(
            f"Index.{inspect.currentframe().f_code.co_name} is not yet implemented"
        )

    def __rlshift__(self, n: int) -> int:
        ErrorMessage.not_implemented(
            f"Index.{inspect.currentframe().f_code.co_name} is not yet implemented"
        )

    def __rrshift__(self, n: int) -> int:
        ErrorMessage.not_implemented(
            f"Index.{inspect.currentframe().f_code.co_name} is not yet implemented"
        )

    @materialization_warning
    def to_pandas(
        self,
        *,
        statement_params: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> native_pd.Index:
        return self._query_compiler._modin_frame.index_columns_pandas_index(
            statement_params=statement_params, **kwargs
        )

    @cached_property
    def __constructor__(self):
        return type(self)

    @property
    def values(self) -> ArrayLike:
        return self.to_pandas().values

    @property
    def is_monotonic_increasing(self) -> bool:
        return self.to_series().is_monotonic_increasing

    @property
    def is_monotonic_decreasing(self) -> bool:
        return self.to_series().is_monotonic_decreasing

    @property
    def is_unique(self) -> bool:
        return self._query_compiler._modin_frame.has_unique_index()

    @property
    def has_duplicates(self) -> bool:
        return not self.is_unique

    def unique(self, level: Hashable | None = None) -> Index:
        if level not in [None, 0, -1]:
            raise IndexError(
                f"Too many levels: Index has only 1 level, {level} is not a valid level number."
            )
        return self.__constructor__(
            query_compiler=self._query_compiler.groupby_agg(
                by=self._query_compiler.get_index_names(axis=0),
                agg_func={},
                axis=0,
                groupby_kwargs={"sort": False, "as_index": True, "dropna": False},
                agg_args=[],
                agg_kwargs={},
            )
        )

    @property
    def dtype(self) -> DtypeObj:
        return self._query_compiler.index_dtypes[0]

    @property
    def shape(self) -> tuple:
        return (len(self),)

    def astype(self, dtype: str | type | ExtensionDtype, copy: bool = True) -> Index:
        if dtype is not None:
            dtype = pandas_dtype(dtype)

        if self.dtype == dtype:
            # Ensure that self.astype(self.dtype) is self
            return self.copy() if copy else self

        col_dtypes = {
            column: dtype for column in self._query_compiler.get_index_names()
        }
        new_query_compiler = self._query_compiler.astype_index(col_dtypes)

        if is_datetime64_any_dtype(dtype):
            # local import to avoid circular dependency.
            from snowflake.snowpark.modin.plugin.extensions.datetime_index import (
                DatetimeIndex,
            )

            return DatetimeIndex(query_compiler=new_query_compiler)

        return Index(query_compiler=new_query_compiler)

    @property
    def name(self) -> Hashable:
        return self.names[0] if self.names else None

    @name.setter
    def name(self, value: Hashable) -> None:
        if not is_hashable(value):
            raise TypeError(f"{type(self).__name__}.name must be a hashable type")
        self._query_compiler = self._query_compiler.set_index_names([value])
        # Update the name of the parent's index only if an inplace update is performed on
        # the parent object, i.e., the parent's current query compiler matches the originally
        # recorded query compiler.
        if self._parent is not None:
            self._parent.check_and_update_parent_qc_index_names([value])

    def _get_names(self) -> list[Hashable]:
        return self._query_compiler.get_index_names()

    def _set_names(self, values: list) -> None:
        if not is_list_like(values):
            raise ValueError("Names must be a list-like")
        if isinstance(values, Index):
            values = values.to_list()
        self._query_compiler = self._query_compiler.set_index_names(values)
        # Update the name of the parent's index only if the parent's current query compiler
        # matches the recorded query compiler.
        if self._parent is not None:
            self._parent.check_and_update_parent_qc_index_names(values)

    names = property(fset=_set_names, fget=_get_names)

    def set_names(
        self, names: Any, level: Any = None, inplace: bool = False
    ) -> Index | None:
        if is_list_like(names) and len(names) > 1:
            raise ValueError(
                f"Since Index is a single index object in Snowpark pandas, "
                f"the length of new names must be 1, got {len(names)}."
            )
        if level is not None and level not in [0, -1]:
            raise IndexError(
                f"Level does not exist: Index has only 1 level, {level} is not a valid level number."
            )
        if inplace:
            name = names[0] if is_list_like(names) else names
            self.name = name
            return None
        else:
            res = self.__constructor__(query_compiler=self._query_compiler)
            res.name = names if is_scalar(names) else names[0]
            return res

    @property
    def ndim(self) -> int:
        return 1

    @property
    def size(self) -> int:
        return len(self)

    @property
    def nlevels(self) -> int:
        return 1

    @property
    def empty(self) -> bool:
        return self.size == 0

    @property
    def T(self, *args: Any, **kwargs: Any) -> Index:
        return self

    def all(self, *args, **kwargs) -> bool | ExtensionArray:
        return self.to_series().all(**kwargs)

    def any(self, *args, **kwargs) -> bool | ExtensionArray:
        return self.to_series().any(**kwargs)

    def argmin(self, axis=None, skipna: bool = True, *args, **kwargs) -> int:
        return self.to_series().argmin(skipna=skipna, *args, **kwargs)

    def argmax(self, axis=None, skipna: bool = True, *args, **kwargs) -> int:
        return self.to_series().argmax(skipna=skipna, *args, **kwargs)

    def copy(
        self,
        name: Hashable | None = None,
        deep: bool = False,
    ) -> Index:
        WarningMessage.ignored_argument(operation="copy", argument="deep", message="")
        return self.__constructor__(
            query_compiler=self._query_compiler.copy(), name=name
        )

    @index_not_implemented()
    def delete(self) -> None:
        # TODO: SNOW-1458146 implement delete
        pass  # pragma: no cover

    @index_not_implemented()
    def drop(
        self,
        labels: Any,
        errors: Literal["ignore", "raise"] = "raise",
    ) -> Index:
        # TODO: SNOW-1458146 implement drop
        pass  # pragma: no cover

    def drop_duplicates(self, keep="first") -> None:
        if keep not in ("first", "last", False):
            raise ValueError('keep must be either "first", "last" or False')
        return self.__constructor__(self.to_series().drop_duplicates(keep=keep))

    @index_not_implemented()
    def duplicated(self, keep: Literal["first", "last", False] = "first") -> np.ndarray:
        # TODO: SNOW-1458147 implement duplicated
        pass  # pragma: no cover

    def equals(self, other: Any) -> bool:
        if self is other:
            return True

        if not isinstance(other, (type(self), self._NATIVE_INDEX_TYPE)):
            return False

        if isinstance(other, self._NATIVE_INDEX_TYPE):
            # Same as DataFrame/Series equals. Convert native Index to Snowpark pandas
            # Index for comparison.
            other = self.__constructor__(other)

        return self._query_compiler.index_equals(other._query_compiler)

    def identical(self, other: Any) -> bool:
        return (
            all(
                getattr(self, c, None) == getattr(other, c, None)
                for c in self._comparables
            )
            and type(self) == type(other)
            and self.dtype == other.dtype
            and self.equals(other)
        )

    @index_not_implemented()
    def insert(self) -> None:
        # TODO: SNOW-1458138 implement insert
        pass  # pragma: no cover

    def is_boolean(self) -> bool:
        return is_bool_dtype(self.dtype)

    def is_floating(self) -> bool:
        return is_float_dtype(self.dtype)

    def is_integer(self) -> bool:
        return is_integer_dtype(self.dtype)

    @index_not_implemented()
    def is_interval(self) -> None:
        pass  # pragma: no cover

    def is_numeric(self) -> bool:
        return is_numeric_dtype(self.dtype) and not is_bool_dtype(self.dtype)

    def is_object(self) -> bool:
        return is_object_dtype(self.dtype)

    def min(
        self, axis: int | None = None, skipna: bool = True, *args: Any, **kwargs: Any
    ) -> Scalar:
        if axis:
            raise ValueError("Axis must be None or 0 for Index objects")
        return self.to_series().min(skipna=skipna, **kwargs)

    def max(
        self, axis: int | None = None, skipna: bool = True, *args: Any, **kwargs: Any
    ) -> Scalar:
        if axis:
            raise ValueError("Axis must be None or 0 for Index objects")
        return self.to_series().max(skipna=skipna, **kwargs)

    def reindex(
        self,
        target: Iterable,
        method: str | None = None,
        level: int | None = None,
        limit: int | None = None,
        tolerance: int | float | None = None,
    ) -> tuple[Index, np.ndarray]:

        # This code path is only hit if our index is lazy (as an eager index would simply call
        # the method on its underlying pandas Index object and return the result of that wrapped
        # appropriately.) Therefore, we specify axis=0, since the QueryCompiler expects lazy indices
        # on axis=0, but eager indices on axis=1 (used for error checking).
        if limit is not None and method is None:
            raise ValueError(
                "limit argument only valid if doing pad, backfill or nearest reindexing"
            )
        kwargs = {
            "method": method,
            "level": level,
            "limit": limit,
            "tolerance": tolerance,
            "_is_index": True,
        }

        internal_index_column = (
            self._query_compiler._modin_frame.index_column_snowflake_quoted_identifiers[
                0
            ]
        )
        internal_index_type = self._query_compiler._modin_frame.get_snowflake_type(
            internal_index_column
        )
        if isinstance(internal_index_type, ArrayType):
            raise NotImplementedError(
                "Snowpark pandas does not support `reindex` with tuple-like Index values."
            )
        else:
            query_compiler, indices = self._query_compiler.reindex(
                axis=0, labels=target, **kwargs
            )
            return Index(query_compiler=query_compiler), indices

    def rename(self, name: Any, inplace: bool = False) -> None:
        if isinstance(name, tuple):
            name = [name]  # The entire tuple is the name
        return self.set_names(names=name, inplace=inplace)

    def nunique(self, dropna: bool = True) -> int:
        return self._query_compiler.nunique_index(dropna=dropna)

    def value_counts(
        self,
        normalize: bool = False,
        sort: bool = True,
        ascending: bool = False,
        bins: int | None = None,
        dropna: bool = True,
    ) -> Series:
        return Series(
            query_compiler=self._query_compiler.value_counts_index(
                normalize=normalize,
                sort=sort,
                ascending=ascending,
                bins=bins,
                dropna=dropna,
            ).set_index_names([self.name]),
            name="proportion" if normalize else "count",
        )

    def item(self) -> Hashable:
        # slice the first two elements of the index and materialize them
        item = self._query_compiler.take_2d_positional(
            index=slice(2), columns=[]
        ).index.to_pandas()

        # return the element as a scalar if the index is exacly one element large
        if len(item) == 1:
            return item[0]

        # otherwise raise the same value error as pandas
        raise ValueError("can only convert an array of size 1 to a Python scalar")

    def to_series(
        self, index: Index | None = None, name: Hashable | None = None
    ) -> Series:
        # get the index name if the name is not given
        if name is None:
            name = self.name

        # convert self to a dataframe and get qc
        # this will give us a df where the index and data columns both have self
        new_qc = self.to_frame(name=name)._query_compiler

        # if we are given an index, join this index column into qc
        if index is not None:
            new_qc = new_qc.set_index_from_series(Series(index)._query_compiler)

        # create series and set the name
        ser = Series(query_compiler=new_qc)
        ser.name = name
        return ser

    def to_frame(
        self, index: bool = True, name: Hashable | None = lib.no_default
    ) -> modin.pandas.DataFrame:
        # Do a reset index to convert the index column to a data column,
        # the index column becomes the pandas default index of row position
        # Example:
        # before
        # index columns:    data columns (empty):
        #      100
        #      200
        #      300
        # after
        # index columns:    data columns (name=column_name):
        #       0               100
        #       1               200
        #       2               300
        new_qc = self._query_compiler.reset_index()
        # if index is true, we want self to be in the index and data columns of the df,
        # so set the index as the data column and set the name of the index
        if index:
            new_qc = new_qc.set_index([new_qc.columns[0]], drop=False).set_index_names(
                [self.name]
            )
        # If `name` is specified, use it as new column name; otherwise, set new column name to the original index name.
        # Note there is one exception case: when the original index name is None, the new column name should be 0.
        if name != lib.no_default:
            new_col_name = name
        else:
            new_col_name = self.name
            if new_col_name is None:
                new_col_name = 0
        new_qc = new_qc.set_columns([new_col_name])

        return DataFrame(query_compiler=new_qc)

    def to_numpy(
        self,
        dtype: npt.DTypeLike | None = None,
        copy: bool = False,
        na_value: object = no_default,
        **kwargs: Any,
    ) -> np.ndarray:
        if copy:
            WarningMessage.ignored_argument(
                operation="to_numpy",
                argument="copy",
                message="copy is ignored in Snowflake backend",
            )
        return (
            self.to_pandas()
            .to_numpy(
                dtype=dtype,
                na_value=na_value,
                **kwargs,
            )
            .flatten()
        )

    @index_not_implemented()
    def fillna(self) -> None:
        # TODO: SNOW-1458139 implement fillna
        pass  # pragma: no cover

    @index_not_implemented()
    def dropna(self) -> None:
        # TODO: SNOW-1458139 implement dropna
        pass  # pragma: no cover

    @index_not_implemented()
    def isna(self) -> None:
        # TODO: SNOW-1458139 implement isna
        pass  # pragma: no cover

    @index_not_implemented()
    def notna(self) -> None:
        # TODO: SNOW-1458139 implement notna
        pass  # pragma: no cover

    @index_not_implemented()
    def hasnans(self) -> None:
        # TODO: SNOW-1458139 implement hasnans
        pass  # pragma: no cover

    @materialization_warning
    def tolist(self) -> list:
        return self.to_pandas().tolist()

    to_list = tolist

    def sort_values(
        self,
        return_indexer: bool = False,
        ascending: bool = True,
        na_position: NaPosition = "last",
        key: Callable | None = None,
    ) -> Index | tuple[Index, np.ndarray]:
        res = self._query_compiler.sort_index(
            axis=0,
            level=None,
            ascending=ascending,
            kind="quicksort",
            na_position=na_position,
            sort_remaining=True,
            ignore_index=False,
            key=key,
            include_indexer=return_indexer,
        )
        index = self.__constructor__(query_compiler=res)
        if return_indexer:
            # When `return_indexer` is True, `res` is a query compiler with one index column
            # and one data column.
            # The resultant sorted Index is the index column and the indexer is the data column.
            # Therefore, performing Index(qc) and Series(qc).to_numpy() yields the required
            # objects to return.
            return index, Series(query_compiler=res).to_numpy()
        else:
            # When `return_indexer` is False, a query compiler with only one index column
            # is returned.
            return index

    @index_not_implemented()
    def append(self) -> None:
        # TODO: SNOW-1458149 implement append
        pass  # pragma: no cover

    @index_not_implemented()
    def join(self) -> None:
        # TODO: SNOW-1458150 implement join
        pass  # pragma: no cover

    def intersection(self, other: Any, sort: bool = False) -> Index:
        # TODO: SNOW-1458151 implement intersection
        WarningMessage.index_to_pandas_warning("intersection")
        return self.__constructor__(
            self.to_pandas().intersection(
                other=try_convert_index_to_native(other), sort=sort
            )
        )

    @index_not_implemented()
    def union(self, other: Any, sort: bool = False) -> Index:
        # TODO: SNOW-1458149 implement union w/o sort
        # TODO: SNOW-1468240 implement union w/ sort
        pass  # pragma: no cover

    @index_not_implemented()
    def difference(self, other: Any, sort: Any = None) -> Index:
        # TODO: SNOW-1458152 implement difference
        pass  # pragma: no cover

    @index_not_implemented()
    def get_indexer_for(self, target: Any) -> Any:
        WarningMessage.index_to_pandas_warning("get_indexer_for")
        return self.to_pandas().get_indexer_for(target=target)

    def _get_indexer_strict(self, key: Any, axis_name: str) -> tuple[Index, np.ndarray]:
        WarningMessage.index_to_pandas_warning("_get_indexer_strict")
        tup = self.to_pandas()._get_indexer_strict(key=key, axis_name=axis_name)
        return self.__constructor__(tup[0]), tup[1]

    def get_level_values(self, level: Any) -> Index:
        if self.nlevels > 1:
            ErrorMessage.not_implemented_error(
                "get_level_values() is not supported for MultiIndex"
            )  # pragma: no cover
        if isinstance(level, int):
            if level not in (0, -1):
                raise IndexError(
                    f"Too many levels: Index has only 1 level, not {level + 1}"
                )
        elif not (level is self.name or level == self.name):
            raise KeyError(
                f"Requested level ({level}) does not match index name ({self.name})"
            )
        return self

    @index_not_implemented()
    def isin(self) -> None:
        # TODO: SNOW-1458153 implement isin
        pass  # pragma: no cover

    @index_not_implemented()
    def slice_indexer(
        self,
        start: Hashable | None = None,
        end: Hashable | None = None,
        step: int | None = None,
    ) -> slice:
        WarningMessage.index_to_pandas_warning("slice_indexer")
        return self.to_pandas().slice_indexer(start=start, end=end, step=step)

    @property
    def array(self) -> ExtensionArray:
        return self.to_pandas().array

    def _summary(self, name: Any = None) -> str:
        WarningMessage.index_to_pandas_warning("_summary")
        return self.to_pandas()._summary(name=name)

    @materialization_warning
    def __array__(self, dtype: Any = None) -> np.ndarray:
        # Ensure that the existing index dtype is preserved in the returned array
        # if no other dtype is given.
        if dtype is None:
            dtype = self.dtype
        return self.to_pandas().__array__(dtype=dtype)

    def __repr__(self) -> str:
        # Create the representation for each field in the index and then join them.
        # First, create the data representation.
        # When the number of elements in the Index is greater than the number of
        # elements to display, display only the first and last 10 elements.
        max_seq_items = get_option("display.max_seq_items") or 100
        length_of_index, _, temp_df = self.to_series()._query_compiler.build_repr_df(
            max_seq_items, 1
        )
        if isinstance(temp_df, native_pd.DataFrame) and not temp_df.empty:
            local_index = temp_df.iloc[:, 0].to_list()
        else:
            local_index = []
        too_many_elem = max_seq_items < length_of_index

        # The representation begins with class name followed by parentheses; the data representation is enclosed in
        # square brackets. For example, "DatetimeIndex([" or "Index([".
        class_name = self.__class__.__name__

        # In the case of DatetimeIndex, if the data is timezone-aware, the timezone is displayed
        # within the dtype field. This is not directly supported in Snowpark pandas.
        native_pd_idx = native_pd.Index(local_index)
        dtype = native_pd_idx.dtype if "DatetimeIndex" in class_name else self.dtype

        # _format_data() correctly indents the data and places newlines where necessary.
        # It also accounts for the comma, newline, and indentation for the next field (dtype).
        data_repr = native_pd_idx._format_data()

        # Next, creating the representation for each field with their respective labels.
        # The index always displays the data and datatype, and optionally the name, length, and freq.
        dtype_repr = f"dtype='{dtype}'"
        name_repr = f", name='{self.name}'" if self.name else ""
        # Length is displayed only when the number of elements is greater than the number of elements to display.
        length_repr = f", length={length_of_index}" if too_many_elem else ""
        # The frequency is displayed for DatetimeIndex and TimedeltaIndex
        # TODO: SNOW-1625233 update freq_repr; replace None with the correct value.
        freq_repr = (
            ", freq=None" if class_name in ("DatetimeIndex", "TimedeltaIndex") else ""
        )

        repr = (
            class_name
            + "("
            + data_repr
            + dtype_repr
            + name_repr
            + length_repr
            + freq_repr
            + ")"
        )
        return repr

    def __iter__(self) -> Iterator:
        WarningMessage.index_to_pandas_warning("__iter__")
        return self.to_pandas().__iter__()

    def __contains__(self, key: Any) -> bool:
        WarningMessage.index_to_pandas_warning("__contains__")
        return self.to_pandas().__contains__(key=key)

    def __len__(self) -> int:
        return self._query_compiler.get_axis_len(0)

    def __getitem__(self, key: Any) -> np.ndarray | None | Index:
        try:
            res = self.to_series().iloc[key]
            if isinstance(res, Series):
                res = res.index
            return res
        except IndexError as ie:
            raise IndexError(
                "only integers, slices (`:`), ellipsis (`...`), numpy.newaxis (`None`) and integer or "
                "boolean arrays are valid indices"
            ) from ie

    def __setitem__(self, key: Any, value: Any) -> None:
        raise TypeError("Index does not support mutable operations")

    @property
    def str(self) -> native_pd.core.strings.accessor.StringMethods:
        return self.to_pandas().str

    def _to_datetime(
        self,
        errors: DateTimeErrorChoices = "raise",
        dayfirst: bool = False,
        yearfirst: bool = False,
        utc: bool = False,
        format: str = None,
        exact: bool | lib.NoDefault = lib.no_default,
        unit: str = None,
        infer_datetime_format: bool | lib.NoDefault = lib.no_default,
        origin: DateTimeOrigin = "unix",
    ) -> Index:
        from snowflake.snowpark.modin.plugin.extensions.datetime_index import (
            DatetimeIndex,
        )

        new_qc = self._query_compiler.series_to_datetime(
            errors,
            dayfirst,
            yearfirst,
            utc,
            format,
            exact,
            unit,
            infer_datetime_format,
            origin,
            include_index=True,
        )
        return DatetimeIndex(query_compiler=new_qc)
