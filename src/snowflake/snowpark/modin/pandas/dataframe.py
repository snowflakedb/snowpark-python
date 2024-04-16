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
from textwrap import dedent
from typing import IO, Any, Callable, Literal

import numpy as np
import pandas
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
from pandas.util._decorators import doc
from pandas.util._validators import validate_bool_kwarg

from snowflake.snowpark.modin import pandas as pd
from snowflake.snowpark.modin.pandas.accessor import CachedAccessor, SparseFrameAccessor
from snowflake.snowpark.modin.pandas.base import (
    _ATTRS_NO_LOOKUP,
    BasePandasDataset,
    _doc_binary_op_kwargs,
)
from snowflake.snowpark.modin.pandas.groupby import (
    DataFrameGroupBy,
    validate_groupby_args,
)

# from . import _update_engine
from snowflake.snowpark.modin.pandas.iterator import PartitionIterator
from snowflake.snowpark.modin.pandas.series import Series
from snowflake.snowpark.modin.pandas.shared_docs import _shared_docs
from snowflake.snowpark.modin.pandas.snow_partition_iterator import (
    SnowparkPandasRowPartitionIterator,
)
from snowflake.snowpark.modin.pandas.utils import (
    _doc_binary_op,
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
from snowflake.snowpark.modin.plugin.utils.error_message import ErrorMessage
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

_shared_doc_kwargs = {
    "axes": "index, columns",
    "klass": "DataFrame",
    "axes_single_arg": "{0 or 'index', 1 or 'columns'}",
    "axis": """axis : {0 or 'index', 1 or 'columns'}, default 0
        If 0 or 'index': apply function to each column.
        If 1 or 'columns': apply function to each row.""",
    "inplace": """
    inplace : bool, default False
        Whether to modify the DataFrame rather than creating a new one.""",
    "optional_by": """
by : str or list of str
    Name or list of names to sort by.

    - if `axis` is 0 or `'index'` then `by` may contain index
      levels and/or column labels.
    - if `axis` is 1 or `'columns'` then `by` may contain column
      levels and/or index labels.""",
    "optional_reindex": """
labels : array-like, optional
    New labels / index to conform the axis specified by 'axis' to.
index : array-like, optional
    New labels for the index. Preferably an Index object to avoid
    duplicating data.
columns : array-like, optional
    New labels for the columns. Preferably an Index object to avoid
    duplicating data.
axis : int or str, optional
    Axis to target. Can be either the axis name ('index', 'columns')
    or number (0, 1).""",
}
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
    ],
    apilink="pandas.DataFrame",
)
class DataFrame(BasePandasDataset):
    """
    Snowpark pandas representation of ``pandas.DataFrame`` with a lazily-evaluated relational dataset.

    A DataFrame is considered lazy because it encapsulates the computation or query required to produce
    the final dataset. The computation is not performed until the datasets need to be displayed, or I/O
    methods like to_pandas, to_snowflake are called.

    Internally, the underlying data are stored as Snowflake table with rows and columns.

    Parameters
    ----------
    data : DataFrame, Series, pandas.DataFrame, ndarray, Iterable or dict, optional
        Dict can contain ``Series``, arrays, constants, dataclass or list-like objects.
        If data is a dict, column order follows insertion-order.
    index : Index or array-like, optional
        Index to use for resulting frame. Will default to ``RangeIndex`` if no
        indexing information part of input data and no index provided.
    columns : Index or array-like, optional
        Column labels to use for resulting frame. Will default to
        ``RangeIndex`` if no column labels are provided.
    dtype : str, np.dtype, or pandas.ExtensionDtype, optional
        Data type to force. Only a single dtype is allowed. If None, infer.
    copy : bool, default: False
        Copy data from inputs. Only affects ``pandas.DataFrame`` / 2d ndarray input.
    query_compiler : BaseQueryCompiler, optional
        A query compiler object to create the ``DataFrame`` from.

    Notes
    -----
    ``DataFrame`` can be created either from passed `data` or `query_compiler`. If both
    parameters are provided, data source will be prioritized in the next order:

    1) Modin ``DataFrame`` or ``Series`` passed with `data` parameter.
    2) Query compiler from the `query_compiler` parameter.
    3) Various pandas/NumPy/Python data structures passed with `data` parameter.

    The last option is less desirable since import of such data structures is very
    inefficient, please use previously created Modin structures from the fist two
    options or import data using highly efficient Modin IO tools (for example
    ``pd.read_csv``).

    Examples
    --------
    Creating a Snowpark pandas DataFrame from a dictionary:

    >>> d = {'col1': [1, 2], 'col2': [3, 4]}
    >>> df = pd.DataFrame(data=d)
    >>> df
       col1  col2
    0     1     3
    1     2     4

    Constructing DataFrame from numpy ndarray:

    >>> df2 = pd.DataFrame(np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),
    ...                    columns=['a', 'b', 'c'])
    >>> df2
       a  b  c
    0  1  2  3
    1  4  5  6
    2  7  8  9

    Constructing DataFrame from a numpy ndarray that has labeled columns:

    >>> data = np.array([(1, 2, 3), (4, 5, 6), (7, 8, 9)],
    ...                 dtype=[("a", "i4"), ("b", "i4"), ("c", "i4")])
    >>> df3 = pd.DataFrame(data, columns=['c', 'a'])
    ...
    >>> df3
       c  a
    0  3  1
    1  6  4
    2  9  7

    Constructing DataFrame from Series/DataFrame:

    >>> ser = pd.Series([1, 2, 3], index=["a", "b", "c"], name = "s")
    >>> df = pd.DataFrame(data=ser, index=["a", "c"])
    >>> df
       s
    a  1
    c  3
    >>> df1 = pd.DataFrame([1, 2, 3], index=["a", "b", "c"], columns=["x"])
    >>> df2 = pd.DataFrame(data=df1, index=["a", "c"])
    >>> df2
       x
    a  1
    c  3
    """

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
                        new_qc = new_qc.reindex(axis=0, labels=index)
                    if columns is not None:
                        new_qc = new_qc.reindex(axis=1, labels=columns)

                    self._query_compiler = new_qc
                    return

                data = {
                    k: v._to_pandas() if isinstance(v, Series) else v
                    for k, v in data.items()
                }
            pandas_df = pandas.DataFrame(
                data=data, index=index, columns=columns, dtype=dtype, copy=copy
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
        num_rows = pandas.get_option("display.max_rows") or 10
        # see _repr_html_ for comment, allow here also all column behavior
        num_cols = pandas.get_option("display.max_columns")

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

    def _get_columns(self) -> pandas.Index:
        """
        Get the columns for this Snowpark pandas ``DataFrame``.

        Returns
        -------
        pandas.Index
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
        """
        Return ``DataFrame`` with duplicate rows removed.

        Considering certain columns is optional. Indexes, including time indexes are ignored.

        Parameters
        ----------
        subset : column label or sequence of labels, optional
            Only consider certain columns for identifying duplicates, by default use all columns.
        keep : {'first', 'last', False}, default 'first'
            Determines which duplicates (if any) to keep.
            'first' : Drop duplicates except for the first occurrence.
            'last' : Drop duplicates except for the last occurrence.
            False : Drop all duplicates.
        inplace : bool, default False
            Whether to modify the DataFrame rather than creating a new one.
        ignore_index : bool, default False
            If True, the resulting axis will be labeled 0, 1, …, n - 1.

        Returns
        -------
        DataFrame or None
            DataFrame with duplicates removed or None if inplace=True.

        Examples
        --------
        Consider dataset containing ramen rating.

        >>> df = pd.DataFrame({
        ...     'brand': ['Yum Yum', 'Yum Yum', 'Indomie', 'Indomie', 'Indomie'],
        ...     'style': ['cup', 'cup', 'cup', 'pack', 'pack'],
        ...     'rating': [4, 4, 3.5, 15, 5]
        ... })
        >>> df
             brand style  rating
        0  Yum Yum   cup     4.0
        1  Yum Yum   cup     4.0
        2  Indomie   cup     3.5
        3  Indomie  pack    15.0
        4  Indomie  pack     5.0

        By default, it removes duplicate rows based on all columns.

        >>> df.drop_duplicates()
             brand style  rating
        0  Yum Yum   cup     4.0
        2  Indomie   cup     3.5
        3  Indomie  pack    15.0
        4  Indomie  pack     5.0

        To remove duplicates on specific column(s), use subset.

        >>> df.drop_duplicates(subset=['brand'])
             brand style  rating
        0  Yum Yum   cup     4.0
        2  Indomie   cup     3.5

        To remove duplicates and keep last occurrences, use keep.

        >>> df.drop_duplicates(subset=['brand', 'style'], keep='last')
             brand style  rating
        1  Yum Yum   cup     4.0
        2  Indomie   cup     3.5
        4  Indomie  pack     5.0
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
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
    ):
        """
        Remove missing values.

        Parameters
        ----------
        axis : {0 or 'index', 1 or 'columns'}, default 0
            Determine if rows or columns which contain missing values are
            removed.

            * 0, or 'index' : Drop rows which contain missing values.
            * 1, or 'columns' : Drop columns which contain missing value.

            .. versionchanged:: 1.0.0

               Pass tuple or list to drop on multiple axes.
               Only a single axis is allowed.

        how : {'any', 'all'}, default 'any'
            Determine if row or column is removed from DataFrame, when we have
            at least one NA or all NA.

            * 'any' : If any NA values are present, drop that row or column.
            * 'all' : If all values are NA, drop that row or column.

        thresh : int, optional
            Require that many non-NA values. Cannot be combined with how.
        subset : column label or sequence of labels, optional
            Labels along other axis to consider, e.g. if you are dropping rows
            these would be a list of columns to include.
        inplace : bool, default False
            Whether to modify the DataFrame rather than creating a new one.

        Returns
        -------
        Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.DataFrame` or None
            Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.DataFrame` with NA entries dropped from it or None if ``inplace=True``.

        See Also
        --------
        DataFrame.isna : Indicate missing values.
        DataFrame.notna : Indicate existing (non-missing) values.
        DataFrame.fillna : Replace missing values.
        Series.dropna : Drop missing values.
        Index.dropna : Drop missing indices.

        Examples
        --------
        >>> df = pd.DataFrame({"name": ['Alfred', 'Batman', 'Catwoman'],
        ...                    "toy": [None, 'Batmobile', 'Bullwhip'],
        ...                    "born": [pd.NaT, pd.Timestamp("1940-04-25"),
        ...                             pd.NaT]})
        >>> df
               name        toy       born
        0    Alfred       None        NaT
        1    Batman  Batmobile 1940-04-25
        2  Catwoman   Bullwhip        NaT

        Drop the rows where at least one element is missing.

        >>> df.dropna()
             name        toy       born
        1  Batman  Batmobile 1940-04-25

        Drop the columns where at least one element is missing.

        >>> df.dropna(axis='columns')
               name
        0    Alfred
        1    Batman
        2  Catwoman

        Drop the rows where all elements are missing.

        >>> df.dropna(how='all')
               name        toy       born
        0    Alfred       None        NaT
        1    Batman  Batmobile 1940-04-25
        2  Catwoman   Bullwhip        NaT

        Keep only the rows with at least 2 non-NA values.

        >>> df.dropna(thresh=2)
               name        toy       born
        1    Batman  Batmobile 1940-04-25
        2  Catwoman   Bullwhip        NaT

        Define in which columns to look for missing values.

        >>> df.dropna(subset=['name', 'toy'])
               name        toy       born
        1    Batman  Batmobile 1940-04-25
        2  Catwoman   Bullwhip        NaT

        Keep the DataFrame with valid entries in the same variable.

        >>> df.dropna(inplace=True)
        >>> df
             name        toy       born
        1  Batman  Batmobile 1940-04-25
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return super()._dropna(
            axis=axis, how=how, thresh=thresh, subset=subset, inplace=inplace
        )

    @property
    def dtypes(self):  # noqa: RT01, D200
        """
        Return the dtypes in the ``DataFrame``.
        This returns a Series with the data type of each column.
        The result's index is the original DataFrame's columns. Columns
        with mixed types are stored with the  ``object`` dtype.

        The returned dtype for each label is the 'largest' numpy type for the
        underlying data.

        For labels with integer-type data, int64 is returned.

        For floating point and decimal data, float64 is returned.

        For boolean data, numpy.bool is returned.

        For datetime or timestamp data, datetime64[ns] is returned.

        For all other data types, including string, date, binary or snowflake variants,
        the dtype object is returned.

        This function is lazy and does NOT trigger evaluation of the underlying
        ``DataFrame``.

        Note that because the returned dtype(s) may be of a larger type than the underlying
        data, the result of this function may differ from the dtypes of the output of the
        :func:`to_pandas()` function.
        Calling :func:`to_pandas()` triggers materialization into a native
        pandas DataFrame. The dtypes of this materialized result are the narrowest
        type(s) that can represent the underlying data (like int16, or int32).

        Returns
        -------
        pandas.Series
            Native pandas (not Snowpark pandas) Series with the dtype for each label.

        Examples
        --------
        >>> df = pd.DataFrame({'float': [1.0],
        ...                    'int': [1],
        ...                    'datetime': [pd.Timestamp('20180310')],
        ...                    'string': ['foo']})
        >>> df.dtypes
        float              float64
        int                  int64
        datetime    datetime64[ns]
        string              object
        dtype: object
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._query_compiler.dtypes

    def duplicated(
        self, subset: Hashable | Sequence[Hashable] = None, keep: DropKeep = "first"
    ):
        """
        Return boolean Series denoting duplicate rows.

        Considering certain columns is optional.

        Parameters
        ----------
        subset : column label or sequence of labels, optional
            Only consider certain columns for identifying duplicates, by
            default use all the columns.
        keep : {'first', 'last', False}, default 'first'
            Determines which duplicates (if any) to mark.

            - ``first`` : Mark duplicates as ``True`` except for the first occurrence.
            - ``last`` : Mark duplicates as ``True`` except for the last occurrence.
            - False : Mark all duplicates as ``True``.

        Returns
        -------
        Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.Series`
            Boolean series for each duplicated rows.

        See Also
        --------
        Index.duplicated : Equivalent method on index.
        Series.duplicated : Equivalent method on Series.
        Series.drop_duplicates : Remove duplicate values from Series.
        DataFrame.drop_duplicates : Remove duplicate values from DataFrame.

        Examples
        --------
        Consider dataset containing ramen rating.

        >>> df = pd.DataFrame({
        ...     'brand': ['Yum Yum', 'Yum Yum', 'Indomie', 'Indomie', 'Indomie'],
        ...     'style': ['cup', 'cup', 'cup', 'pack', 'pack'],
        ...     'rating': [4, 4, 3.5, 15, 5]
        ... })
        >>> df
             brand style  rating
        0  Yum Yum   cup     4.0
        1  Yum Yum   cup     4.0
        2  Indomie   cup     3.5
        3  Indomie  pack    15.0
        4  Indomie  pack     5.0

        By default, for each set of duplicated values, the first occurrence
        is set on False and all others on True.

        >>> df.duplicated()
        0    False
        1     True
        2    False
        3    False
        4    False
        dtype: bool

        By using 'last', the last occurrence of each set of duplicated values
        is set on False and all others on True.

        >>> df.duplicated(keep='last')
        0     True
        1    False
        2    False
        3    False
        4    False
        dtype: bool

        By setting ``keep`` on False, all duplicates are True.

        >>> df.duplicated(keep=False)
        0     True
        1     True
        2    False
        3    False
        4    False
        dtype: bool

        To find duplicates on specific column(s), use ``subset``.

        >>> df.duplicated(subset=['brand'])
        0    False
        1     True
        2    False
        3     True
        4     True
        dtype: bool
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
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return len(self.columns) == 0 or len(self) == 0

    @property
    def axes(self):
        """
        Return a list representing the axes of the DataFrame.

        It has the row axis labels and column axis labels as the only members.
        They are returned in that order.

        Examples
        --------
        >>> df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        >>> df.axes
        [Index([0, 1], dtype='int64'), Index(['col1', 'col2'], dtype='object')]
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return [self.index, self.columns]

    @property
    def shape(self) -> tuple[int, int]:
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return len(self), len(self.columns)

    def add_prefix(self, prefix):
        """
        Prefix labels with string `prefix`.

        For Series, the row labels are prefixed.
        For DataFrame, the column labels are prefixed.

        Parameters
        ----------
        prefix : str
            The string to add before each label.

        Returns
        -------
        Series or DataFrame
            New Series or DataFrame with updated labels.

        See Also
        --------
        Series.add_suffix: Suffix row labels with string `suffix`.
        DataFrame.add_suffix: Suffix column labels with string `suffix`.

        Examples
        --------
        >>> s = pd.Series([1, 2, 3, 4])
        >>> s
        0    1
        1    2
        2    3
        3    4
        dtype: int64

        >>> s.add_prefix('item_')
        item_0    1
        item_1    2
        item_2    3
        item_3    4
        dtype: int64

        >>> df = pd.DataFrame({'A': [1, 2, 3, 4], 'B': [3, 4, 5, 6]})
        >>> df
           A  B
        0  1  3
        1  2  4
        2  3  5
        3  4  6

        >>> df.add_prefix('col_')
           col_A  col_B
        0      1      3
        1      2      4
        2      3      5
        3      4      6
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

        For Series, the row labels are suffixed.
        For DataFrame, the column labels are suffixed.

        Parameters
        ----------
        suffix : str
            The string to add after each label.

        Returns
        -------
        Series or DataFrame
            New Series or DataFrame with updated labels.

        See Also
        --------
        Series.add_prefix: Prefix row labels with string `prefix`.
        DataFrame.add_prefix: Prefix column labels with string `prefix`.

        Examples
        --------
        >>> s = pd.Series([1, 2, 3, 4])
        >>> s
        0    1
        1    2
        2    3
        3    4
        dtype: int64

        >>> s.add_suffix('_item')
        0_item    1
        1_item    2
        2_item    3
        3_item    4
        dtype: int64

        >>> df = pd.DataFrame({'A': [1, 2, 3, 4], 'B': [3, 4, 5, 6]})
        >>> df
           A  B
        0  1  3
        1  2  4
        2  3  5
        3  4  6

        >>> df.add_suffix('_col')
           A_col  B_col
        0      1      3
        1      2      4
        2      3      5
        3      4      6
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        # pandas converts non-string suffix values into str and appends it to the column labels.
        return self.__constructor__(
            query_compiler=self._query_compiler.add_substring(
                str(suffix), substring_type="suffix", axis=1
            )
        )

    def applymap(self, func: PythonFuncType, na_action: str | None = None, **kwargs):
        """
        Apply a function to a Dataframe elementwise.

        This method applies a function that accepts and returns a scalar
        to every element of a DataFrame.

        Parameters
        ----------
        func : callable
            Python function, returns a single value from a single value.
        na_action : {None, 'ignore'}, default None
            If ‘ignore’, propagate NaN values, without passing them to func.
        **kwargs
            Additional keyword arguments to pass as keywords arguments to
            `func`.

        Returns
        -------
        Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Transformed Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.DataFrame`.

        See Also
        --------
        :func:`Series.apply <snowflake.snowpark.modin.pandas.Series.apply>` : For applying more complex functions on a Series.

        :func:`DataFrame.apply <snowflake.snowpark.modin.pandas.DataFrame.apply>` : Apply a function row-/column-wise.

        Examples
        --------
        >>> df = pd.DataFrame([[1, 2.12], [3.356, 4.567]])
        >>> df
               0      1
        0  1.000  2.120
        1  3.356  4.567

        >>> df.applymap(lambda x: len(str(x)))
           0  1
        0  3  4
        1  5  5

        Like Series.map, NA values can be ignored: (TODO SNOW-888095: re-enable the test once fallback solution is used)

        >>> df_copy = df.copy()
        >>> df_copy.iloc[0, 0] = pd.NA
        >>> df_copy.applymap(lambda x: len(str(x)), na_action='ignore')  # doctest: +SKIP
             0  1
        0  NaN  4
        1  5.0  5

        When you use the applymap function, a user-defined function (UDF) is generated and
        applied to each column. However, in many cases, you can achieve the same results
        more efficiently by utilizing alternative dataframe operations instead of applymap.
        For example, You could square each number elementwise.

        >>> df.applymap(lambda x: x**2)
                   0          1
        0   1.000000   4.494400
        1  11.262736  20.857489

        But it's better to avoid applymap in that case.

        >>> df ** 2
                   0          1
        0   1.000000   4.494400
        1  11.262736  20.857489
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        if not callable(func):
            raise TypeError(f"{func} is not callable")
        return self.__constructor__(
            query_compiler=self._query_compiler.applymap(
                func, na_action=na_action, **kwargs
            )
        )

    _agg_examples_doc = dedent(
        """
    Examples
    --------
    >>> df = pd.DataFrame([[1, 2, 3],
    ...                    [4, 5, 6],
    ...                    [7, 8, 9],
    ...                    [np.nan, np.nan, np.nan]],
    ...                   columns=['A', 'B', 'C'])

    Aggregate these functions over the rows.

    >>> df.agg(['sum', 'min'])
            A     B     C
    sum  12.0  15.0  18.0
    min   1.0   2.0   3.0

    Different aggregations per column.

    >>> df.agg({'A' : ['sum', 'min'], 'B' : ['min', 'max']})
            A    B
    sum  12.0  NaN
    min   1.0  2.0
    max   NaN  8.0

    Aggregate over the columns.

    >>> df.agg("max", axis="columns")
    0    3.0
    1    6.0
    2    9.0
    3    NaN
    dtype: float64

    Different aggregations per row.

    >>> df.agg({ 0: ["sum"], 1: ["min"] }, axis=1)
       sum  min
    0  6.0  NaN
    1  NaN  4.0
    """
    )

    @doc(
        _shared_docs["aggregate"],
        klass=_shared_doc_kwargs["klass"],
        axis=_shared_doc_kwargs["axis"],
        examples=_agg_examples_doc,
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
        Apply a function along an axis of the DataFrame.

        Objects passed to the function are Series objects whose index is
        either the DataFrame's index (``axis=0``) or the DataFrame's columns
        (``axis=1``). By default (``result_type=None``), the final return type
        is inferred from the return type of the applied function. Otherwise,
        it depends on the `result_type` argument.

        Parameters
        ----------
        func : function
            A Python function object to apply to each column or row, or a Python function decorated with @udf.
        axis : {0 or 'index', 1 or 'columns'}, default 0
            Axis along which the function is applied:

            * 0 or 'index': apply function to each column.
            * 1 or 'columns': apply function to each row.

        raw : bool, default False
            Determines if row or column is passed as a Series or ndarray object:

            * ``False`` : passes each row or column as a Series to the
              function.
            * ``True`` : the passed function will receive ndarray objects
              instead.

        result_type : {'expand', 'reduce', 'broadcast', None}, default None
            These only act when ``axis=1`` (columns):

            * 'expand' : list-like results will be turned into columns.
            * 'reduce' : returns a Series if possible rather than expanding
              list-like results. This is the opposite of 'expand'.
            * 'broadcast' : results will be broadcast to the original shape
              of the DataFrame, the original index and columns will be
              retained.
        args : tuple
            Positional arguments to pass to `func` in addition to the
            array/series.
        **kwargs
            Additional keyword arguments to pass as keywords arguments to
            `func`.

        Returns
        -------
        Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.Series` or Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Result of applying ``func`` along the given axis of the DataFrame.

        See Also
        --------
        :func:`Series.apply <snowflake.snowpark.modin.pandas.Series.apply>` : For applying more complex functions on a Series.

        :func:`DataFrame.applymap <snowflake.snowpark.modin.pandas.DataFrame.applymap>` : Apply a function elementwise on a whole DataFrame.

        Notes
        -----
        1. When the type annotation of return value is provided on ``func``, the result will be cast
        to the corresponding dtype. When no type annotation is provided, data will be converted
        to Variant type in Snowflake and leave as dtype=object. In this case, the return value must
        be JSON-serializable, which can be a valid input to ``json.dumps`` (e.g., ``dict`` and
        ``list`` objects are json-serializable, but ``bytes`` and ``datetime.datetime`` objects
        are not). The return type hint takes effect solely when ``func`` is a series-to-scalar function.

        2. Under the hood, we use Snowflake Vectorized Python UDFs to implement apply() method with
        `axis=1`. You can find type mappings from Snowflake SQL types to pandas dtypes
        `here <https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-batch#type-support>`_.

        3. Snowflake supports two types of NULL values in variant data: `JSON NULL and SQL NULL <https://docs.snowflake.com/en/user-guide/semistructured-considerations#null-values>`_.
        When no type annotation is provided and Variant data is returned, Python ``None`` is translated to
        JSON NULL, and all other pandas missing values (np.nan, pd.NA, pd.NaT) are translated to SQL NULL.

        4. If ``func`` is a series-to-series function and can also be a scalar-to-scalar function
        (e.g., ``np.sqrt``, ``lambad x: x+1``), it is equivalent to use `df.applymap()`,
        which will give better performance.

        5. When ``func`` can return a series with different indices, e.g.,
        ``lambda x: pd.Series([1, 2], index=["a", "b"] if x.sum() > 2 else ["b", "c"]).``,
        the values with the same label will be merged together.

        6. The index values of returned series from ``func`` must be json-serializable. For example,
        ``lambda x: pd.Series([1], index=[bytes(1)])`` will raise a SQL execption.

        7. When ``func`` uses any first-party modules or third-party packages inside the function,
        you need to add these dependencies via ``session.add_import()`` and ``session.add_packages()``.
        Alternatively. specify third-party packages with the @udf decorator. When using the @udf decorator,
        annotations using PandasSeriesType or PandasDataFrameType are not supported.

        8. Snowpark pandas module is currently not supported inside ``func``. If you need to call
        general pandas API (e.g., ``pd.Timestamp``) inside ``func``, the workaround is to use
        the actual pandas module.

        Examples
        --------
        >>> import snowflake.snowpark.modin.pandas as pd
        >>> df = pd.DataFrame([[2, 0], [3, 7], [4, 9]], columns=['A', 'B'])
        >>> df
           A  B
        0  2  0
        1  3  7
        2  4  9

        Using a numpy universal function (in this case the same as
        ``np.sqrt(df)``):

        >>> df.apply(np.sqrt)
                  A         B
        0  1.414062  0.000000
        1  1.732422  2.646484
        2  2.000000  3.000000


        Using a reducing function on either axis

        >>> df.apply(np.sum, axis=0)
        A     9
        B    16
        dtype: int8

        >>> df.apply(np.sum, axis=1)
        0     2
        1    10
        2    13
        dtype: int64

        Returning a list-like will result in a Series

        >>> df.apply(lambda x: [1, 2], axis=1)
        0    [1, 2]
        1    [1, 2]
        2    [1, 2]
        dtype: object

        Passing ``result_type='broadcast'`` will ensure the same shape
        result, whether list-like or scalar is returned by the function,
        and broadcast it along the axis. The resulting column names will
        be the originals.

        >>> df.apply(lambda x: [1, 2], axis=1, result_type='broadcast')
           A  B
        0  1  2
        1  1  2
        2  1  2

        To work with 3rd party packages add them to the current session

        >>> import scipy.stats
        >>> pd.session.custom_package_usage_config['enabled'] = True
        >>> pd.session.add_packages(['numpy', scipy])
        >>> df.apply(lambda x: np.dot(x * scipy.stats.norm.cdf(0), x * scipy.stats.norm.cdf(0)), axis=1)
        0     1.00
        1    14.50
        2    24.25
        dtype: float64

        or annotate the function
        to pass to apply with the @udf decorator from Snowpark https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/latest/api/snowflake.snowpark.functions.udf.

        >>> from snowflake.snowpark.functions import udf
        >>> from snowflake.snowpark.types import DoubleType
        >>> @udf(packages=['statsmodels>0.12'], return_type=DoubleType())
        ... def autocorr(column):
        ...    import pandas as pd
        ...    import statsmodels.tsa.stattools
        ...    return pd.Series(statsmodels.tsa.stattools.pacf_ols(column.values)).mean()
        ...
        >>> df.apply(autocorr, axis=0)
        A    0.857143
        B    0.428571
        dtype: float64
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
        Group DataFrame using a mapper or by a Series of columns.

        Args:
            by: mapping, function, label, Snowpark pandas Series or a list of such. Used to determine the groups for the groupby.
                If by is a function, it’s called on each value of the object’s index. If a dict or Snowpark pandas Series is
                passed, the Series or dict VALUES will be used to determine the groups (the Series’ values are first aligned;
                see .align() method). If a list or ndarray of length equal to the selected axis is passed (see the groupby
                user guide), the values are used as-is to determine the groups. A label or list of labels may be passed
                to group by the columns in self. Notice that a tuple is interpreted as a (single) key.

            axis: {0 or ‘index’, 1 or ‘columns’}, default 0
                Split along rows (0) or columns (1). For Series this parameter is unused and defaults to 0.

            level: int, level name, or sequence of such, default None
                If the axis is a MultiIndex (hierarchical), group by a particular level or levels. Do not specify both by and level.

            as_index: bool, default True
                    For aggregated output, return object with group labels as the index. Only relevant for DataFrame input.
                    as_index=False is effectively “SQL-style” grouped output.

            sort: bool, default True
                Sort group keys. Groupby preserves the order of rows within each group. Note that in pandas,
                better performance can be achieved by turning sort off, this is not going to be true with
                SnowparkPandas. When sort=False, the performance will be no better than sort=True.

            group_keys: bool, default True
                    When calling apply and the by argument produces a like-indexed (i.e. a transform) result, add group
                    keys to index to identify pieces. By default group keys are not included when the result’s index
                    (and column) labels match the inputs, and are included otherwise.

            observed: bool, default False
                    This only applies if any of the groupers are Categoricals. If True: only show observed values for
                    categorical groupers. If False: show all values for categorical groupers. This parameter is
                    currently ignored with Snowpark pandas API, since Category type is currently not supported with
                    Snowpark pandas API.

            dropna: bool, default True
                    If True, and if group keys contain NA values, NA values together with row/column will be dropped.
                    If False, NA values will also be treated as the key in groups.

        Returns:
            Snowpark pandas DataFrameGroupBy: Returns a groupby object that contains information about the groups.

        Examples::

            >>> df = pd.DataFrame({'Animal': ['Falcon', 'Falcon',
            ...                               'Parrot', 'Parrot'],
            ...                    'Max Speed': [380., 370., 24., 26.]})
            >>> df
               Animal  Max Speed
            0  Falcon      380.0
            1  Falcon      370.0
            2  Parrot       24.0
            3  Parrot       26.0

            >>> df.groupby(['Animal']).mean()   # doctest: +NORMALIZE_WHITESPACE
                    Max Speed
            Animal
            Falcon      375.0
            Parrot       25.0

            **Hierarchical Indexes**

            We can groupby different levels of a hierarchical index
            using the `level` parameter:

            >>> arrays = [['Falcon', 'Falcon', 'Parrot', 'Parrot'],
            ...           ['Captive', 'Wild', 'Captive', 'Wild']]
            >>> index = pd.MultiIndex.from_arrays(arrays, names=('Animal', 'Type'))
            >>> df = pd.DataFrame({'Max Speed': [390., 350., 30., 20.]},
            ...                   index=index)
            >>> df      # doctest: +NORMALIZE_WHITESPACE
                            Max Speed
            Animal Type
            Falcon Captive      390.0
                   Wild         350.0
            Parrot Captive       30.0
                   Wild          20.0

            >>> df.groupby(level=0).mean()      # doctest: +NORMALIZE_WHITESPACE
                    Max Speed
            Animal
            Falcon      370.0
            Parrot       25.0

            >>> df.groupby(level="Type").mean()     # doctest: +NORMALIZE_WHITESPACE
                     Max Speed
            Type
            Captive      210.0
            Wild         185.0
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
        """
        Call ``func`` on self producing a Snowpark pandas DataFrame with the same axis shape as self.
        Currently only callable and string functions are supported since those can be directly mapped to
        the apply function.

        Examples::
            Increment every value in dataframe by 1.

            >>> d1 = {'col1': [1, 2, 3], 'col2': [3, 4, 5]}
            >>> df = pd.DataFrame(data=d1)
            >>> df
               col1  col2
            0     1     3
            1     2     4
            2     3     5
            >>> df.transform(lambda x: x + 1)
               col1  col2
            0     2     4
            1     3     5
            2     4     6
            >>> df.transform(np.square)
               col1  col2
            0     1     9
            1     4    16
            2     9    25
            >>> df.transform("square")
               col1  col2
            0     1     9
            1     4    16
            2     9    25

        """
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

        Reflect the DataFrame over its main diagonal by writing rows as columns and vice-versa. The property T is an accessor to the method transpose().

        Args:
            *args tuple, optional
                Accepted for compatibility with NumPy.  Note these arguments are ignored in the snowpark pandas
                implementation unless go through a fallback path, in which case they may be used by the native
                pandas implementation.

            copy bool, default False
                Whether to copy the data after transposing, even for DataFrames with a single dtype.  The snowpark
                pandas implementation ignores this parameter.

            Note that a copy is always required for mixed dtype DataFrames, or for DataFrames with any extension types.

        Returns:
            Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.DataFrame`
                The transposed DataFrame.

        Examples::
            Square DataFrame with homogeneous dtype

            >>> d1 = {'col1': [1, 2], 'col2': [3, 4]}
            >>> df1 = pd.DataFrame(data=d1)
            >>> df1
               col1  col2
            0     1     3
            1     2     4

            >>> df1_transposed = df1.T  # or df1.transpose()
            >>> df1_transposed
                  0  1
            col1  1  2
            col2  3  4

            When the dtype is homogeneous in the original DataFrame, we get a transposed DataFrame with the same dtype:

            >>> df1.dtypes
            col1    int64
            col2    int64
            dtype: object

            >>> df1_transposed.dtypes
            0    int64
            1    int64
            dtype: object

            Non-square DataFrame with mixed dtypes

            >>> d2 = {'name': ['Alice', 'Bob'],
            ...      'score': [9.5, 8],
            ...      'employed': [False, True],
            ...       'kids': [0, 0]}
            >>> df2 = pd.DataFrame(data=d2)
            >>> df2
                name  score  employed  kids
            0  Alice    9.5     False     0
            1    Bob    8.0      True     0

            >>> df2_transposed = df2.T  # or df2.transpose()
            >>> df2_transposed
                          0     1
            name      Alice   Bob
            score       9.5   8.0
            employed  False  True
            kids          0     0

            When the DataFrame has mixed dtypes, we get a transposed DataFrame with the object dtype:

            >>> df2.dtypes
            name         object
            score       float64
            employed       bool
            kids          int64
            dtype: object

            >>> df2_transposed.dtypes
            0    object
            1    object
            dtype: object
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

    def assign(self, **kwargs):  # noqa: PR01, RT01, D200
        """
        Assign new columns to a ``DataFrame``.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()  # pragma: no cover

        df = self.copy()
        for k, v in kwargs.items():
            if callable(v):
                df[k] = v(df)
            else:
                df[k] = v
        return df

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
        ErrorMessage.not_implemented()
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

    def combine(
        self, other, func, fill_value=None, overwrite=True
    ):  # noqa: PR01, RT01, D200
        """
        Perform column-wise combine with another ``DataFrame``.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()
        return super().combine(other, func, fill_value=fill_value, overwrite=overwrite)

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
        ErrorMessage.not_implemented()
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

    def corr(
        self, method="pearson", min_periods=1, numeric_only=False
    ):  # noqa: PR01, RT01, D200
        """
        Compute pairwise correlation of columns, excluding NA/null values.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()
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

    def corrwith(
        self, other, axis=0, drop=False, method="pearson", numeric_only=False
    ):  # noqa: PR01, RT01, D200
        """
        Compute pairwise correlation.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()
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

    def cov(
        self,
        min_periods: int | None = None,
        ddof: int | None = 1,
        numeric_only: bool = False,
    ):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()
        return self.__constructor__(
            query_compiler=self._query_compiler.cov(
                min_periods=min_periods,
                ddof=ddof,
                numeric_only=numeric_only,
            )
        )

    def dot(self, other):  # noqa: PR01, RT01, D200
        """
        Compute the matrix multiplication between the ``DataFrame`` and `other`.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()  # pragma: no cover

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

    def equals(self, other):  # noqa: PR01, RT01, D200
        """
        Test whether two objects contain the same elements.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()  # pragma: no cover

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

    def eval(self, expr, inplace=False, **kwargs):  # noqa: PR01, RT01, D200
        """
        Evaluate a string describing operations on ``DataFrame`` columns.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()
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

    @_inherit_docstrings(pandas.DataFrame.fillna, apilink="pandas.DataFrame.fillna")
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
    def from_dict(
        cls, data, orient="columns", dtype=None, columns=None
    ):  # pragma: no cover # noqa: PR01, RT01, D200
        """
        Construct ``DataFrame`` from dict of array-like or dicts.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()
        return from_pandas(
            pandas.DataFrame.from_dict(
                data, orient=orient, dtype=dtype, columns=columns
            )
        )

    @classmethod
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
        ErrorMessage.not_implemented()
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
        ErrorMessage.not_implemented()
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
        Print a concise summary of the ``DataFrame``. Snowflake
        DataFrames mirror the output of pandas df.info but with some
        specific limitations ( zeroed memory usage, no index information ).

        Parameters
        ----------
        verbose : bool, optional
            Whether to print the full summary. By default, the setting in
            ``pandas.options.display.max_info_columns`` is followed.
        buf : writable buffer, defaults to sys.stdout
            Where to send the output. By default, the output is printed to
            sys.stdout. Pass a writable buffer if you need to further process
            the output.
        max_cols : int, optional
            When to switch from the verbose to the truncated output. If the
            DataFrame has more than `max_cols` columns, the truncated output
            is used. By default, the setting in
            ``pandas.options.display.max_info_columns`` is used.
        memory_usage : bool, str, optional
            Displays 0 for memory usage, since the memory usage of a snowflake
            dataframe is remote and partially indeterminant.
        show_counts : bool, optional
            Whether to show the non-null counts. By default, this is shown
            only if the DataFrame is smaller than
            ``pandas.options.display.max_info_rows`` and
            ``pandas.options.display.max_info_columns``. A value of True always
            shows the counts, and False never shows the counts.

        Returns
        -------
        None
            This method prints a summary of a DataFrame and returns None.

        Examples
        --------
            >>> df = pd.DataFrame({'COL1': [1, 2, 3],
            ...                    'COL2': ['A', 'B', 'C']})

            >>> df.info() # doctest: +NORMALIZE_WHITESPACE
            <class 'snowflake.snowpark.modin.pandas.dataframe.DataFrame'>
            SnowflakeIndex
            Data columns (total 2 columns):
             #   Column  Non-Null Count  Dtype
            ---  ------  --------------  -----
             0   COL1    3 non-null      int64
             1   COL2    3 non-null      object
            dtypes: int64(1), object(1)
            memory usage: 0.0 bytes
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
        Insert column into DataFrame at specified location.

        Raises a ValueError if `column` is already contained in the DataFrame,
        unless `allow_duplicates` is set to True.

        Parameters
        ----------
        loc : int
            Insertion index. Must verify 0 <= loc <= len(columns).
        column : str, number, or hashable object
            Label of the inserted column.
        value : Scalar, Series, or array-like
        allow_duplicates : bool, optional, default lib.no_default

        See Also
        --------
        Index.insert : Insert new item by index.

        Examples
        --------
        >>> df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        >>> df
           col1  col2
        0     1     3
        1     2     4
        >>> df.insert(1, "newcol", [99, 99])
        >>> df
           col1  newcol  col2
        0     1      99     3
        1     2      99     4
        >>> df.insert(0, "col1", [100, 100], allow_duplicates=True)
        >>> df
           col1  col1  newcol  col2
        0   100     1      99     3
        1   100     2      99     4

        Notice that pandas uses index alignment in case of `value` from type `Series`:

        >>> df.insert(0, "col0", pd.Series([5, 6], index=[1, 2]))
        >>> df
           col0  col1  col1  newcol  col2
        0   NaN   100     1      99     3
        1   5.0   100     2      99     4
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
        ErrorMessage.not_implemented()
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

        Yields
        ------
        index : label or tuple of label
            The index of the row. A tuple for a `MultiIndex`.
        data : Series
            The data of the row as a Series.

        See Also
        --------
        DataFrame.itertuples : Iterate over DataFrame rows as namedtuples of the values.
        DataFrame.items : Iterate over (column name, Series) pairs.

        Notes
        -----
        1. Iterating over rows is an antipattern in Snowpark pandas and pandas. Use df.apply() or other aggregation
           methods when possible instead of iterating over a DataFrame. Iterators and for loops do not scale well.
        2. Because ``iterrows`` returns a Series for each row, it does **not** preserve dtypes across the rows (dtypes
           are preserved across columns for DataFrames).
        3. You should **never modify** something you are iterating over. This will not work. The iterator returns a copy
           of the data and writing to it will have no effect.

        Examples
        --------
        >>> df = pd.DataFrame([[1, 1.5], [2, 2.5], [3, 7.8]], columns=['int', 'float'])
        >>> df
           int  float
        0    1    1.5
        1    2    2.5
        2    3    7.8

        Print the first row's index and the row as a Series.
        >>> index_and_row = next(df.iterrows())
        >>> index_and_row
        (0, int      1.0
        float    1.5
        Name: 0, dtype: float64)

        Print the first row as a Series.
        >>> row = next(df.iterrows())[1]
        >>> row
        int      1.0
        float    1.5
        Name: 0, dtype: float64

        Pretty printing every row.
        >>> for row in df.iterrows():
        ...     print(row[1])
        ...
        int      1.0
        float    1.5
        Name: 0, dtype: float64
        int      2.0
        float    2.5
        Name: 1, dtype: float64
        int      3.0
        float    7.8
        Name: 2, dtype: float64

        >>> df = pd.DataFrame([[0, 2, 3], [0, 4, 1]], columns=['A', 'B', 'C'])
        >>> df
           A  B  C
        0  0  2  3
        1  0  4  1

        Pretty printing the results to distinguish index and Series.
        >>> for row in df.iterrows():
        ...     print(f"Index: {row[0]}")
        ...     print("Series:")
        ...     print(row[1])
        ...
        Index: 0
        Series:
        A    0
        B    2
        C    3
        Name: 0, dtype: int64
        Index: 1
        Series:
        A    0
        B    4
        C    1
        Name: 1, dtype: int64
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

    def items(self):  # noqa: D200
        """
        Iterate over (column name, ``Series``) pairs.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()

        def items_builder(s):
            """Return tuple of the given `s` parameter name and the parameter themselves."""
            return s.name, s

        partition_iterator = PartitionIterator(self, 1, items_builder)
        yield from partition_iterator

    def iteritems(self):  # noqa: RT01, D200
        """
        Iterate over (column name, ``Series``) pairs.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()
        return self.items()

    def itertuples(
        self, index: bool = True, name: str | None = "Pandas"
    ) -> Iterable[tuple[Any, ...]]:
        """
        Iterate over DataFrame rows as namedtuples.

        Parameters
        ----------
        index : bool, default True
            If True, return the index as the first element of the tuple.
        name : str or None, default "Pandas"
            The name of the returned namedtuples or None to return regular tuples.

        Returns
        -------
        iterator
            An object to iterate over namedtuples for each row in the DataFrame with the first field possibly being the
            index and following fields being the column values.

        See Also
        --------
        DataFrame.iterrows : Iterate over DataFrame rows as (index, Series) pairs.
        DataFrame.items : Iterate over (column name, Series) pairs.

        Notes
        -----
        1. Iterating over rows is an antipattern in Snowpark pandas and pandas. Use df.apply() or other aggregation
           methods when possible instead of iterating over a DataFrame. Iterators and for loops do not scale well.
        2. The column names will be renamed to positional names if they are invalid Python identifiers, repeated, or
           start with an underscore (follows namedtuple rules).

        Examples
        --------
        >>> df = pd.DataFrame({'num_legs': [4, 2], 'num_wings': [0, 2]}, index=['dog', 'hawk'])
        >>> df
              num_legs  num_wings
        dog          4          0
        hawk         2          2
        >>> for row in df.itertuples():
        ...     print(row)
        ...
        Pandas(Index='dog', num_legs=4, num_wings=0)
        Pandas(Index='hawk', num_legs=2, num_wings=2)

        By setting the `index` parameter to False we can remove the index as the first element of the tuple:
        >>> for row in df.itertuples(index=False):
        ...     print(row)
        ...
        Pandas(num_legs=4, num_wings=0)
        Pandas(num_legs=2, num_wings=2)

        >>> df = pd.DataFrame([[1, 2], [4, 5], [7, 8]],
        ...      index=['cobra', 'viper', 'sidewinder'],
        ...      columns=['max_speed', 'shield'])
        >>> df
                    max_speed  shield
        cobra               1       2
        viper               4       5
        sidewinder          7       8

        >>> for row in df.itertuples():
        ...     print(row)
        ...
        Pandas(Index='cobra', max_speed=1, shield=2)
        Pandas(Index='viper', max_speed=4, shield=5)
        Pandas(Index='sidewinder', max_speed=7, shield=8)

        Rename the namedtuple and create it without the index values.
        >>> for row in df.itertuples(name="NewName", index=False):
        ...     print(row)
        ...
        NewName(max_speed=1, shield=2)
        NewName(max_speed=4, shield=5)
        NewName(max_speed=7, shield=8)

        When name is None, return a regular tuple.
        >>> for row in df.itertuples(name=None):
        ...     print(row)
        ...
        ('cobra', 1, 2)
        ('viper', 4, 5)
        ('sidewinder', 7, 8)
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
        Join columns of another DataFrame.

        Join columns with `other` DataFrame either on index or on a key
        column. Efficiently join multiple DataFrame objects by index at once by
        passing a list.

        Parameters
        ----------
        other : :class:`~snowflake.snowpark.modin.pandas.DataFrame`, :class:`~snowflake.snowpark.modin.pandas.Series`, or a list containing any combination of them
            Index should be similar to one of the columns in this one. If a
            Series is passed, its name attribute must be set, and that will be
            used as the column name in the resulting joined DataFrame.
        on : str, list of str, or array-like, optional
            Column or index level name(s) in the caller to join on the index
            in `other`, otherwise joins index-on-index. If multiple
            values given, the `other` DataFrame must have a MultiIndex. Can
            pass an array as the join key if it is not already contained in
            the calling DataFrame. Like an Excel VLOOKUP operation.
        how : {'left', 'right', 'outer', 'inner'}, default 'left'
            How to handle the operation of the two objects.

            * left: use calling frame's index (or column if on is specified)
            * right: use `other`'s index.
            * outer: form union of calling frame's index (or column if on is
              specified) with `other`'s index, and sort it.
              lexicographically.
            * inner: form intersection of calling frame's index (or column if
              on is specified) with `other`'s index, preserving the order
              of the calling's one.
            * cross: creates the cartesian product from both frames, preserves the order
              of the left keys.
        lsuffix : str, default ''
            Suffix to use from left frame's overlapping columns.
        rsuffix : str, default ''
            Suffix to use from right frame's overlapping columns.
        sort : bool, default False
            Order result DataFrame lexicographically by the join key. If False,
            the order of the join key depends on the join type (how keyword).
        validate : str, optional
            If specified, checks if join is of specified type.
            * "one_to_one" or "1:1": check if join keys are unique in both left
            and right datasets.
            * "one_to_many" or "1:m": check if join keys are unique in left dataset.
            * "many_to_one" or "m:1": check if join keys are unique in right dataset.
            * "many_to_many" or "m:m": allowed, but does not result in checks.

        Returns
        -------
        Snowapark pandas :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            A dataframe containing columns from both the caller and `other`.

        Notes
        -----
        Parameters `on`, `lsuffix`, and `rsuffix` are not supported when
        passing a list of `DataFrame` objects.

        Snowpark pandas API doesn't currently support distributed computation of join with
        'validate' argument.

        Examples
        --------
            >>> df = pd.DataFrame({'key': ['K0', 'K1', 'K2', 'K3', 'K4', 'K5'],
            ...                    'A': ['A0', 'A1', 'A2', 'A3', 'A4', 'A5']})

            >>> df
              key   A
            0  K0  A0
            1  K1  A1
            2  K2  A2
            3  K3  A3
            4  K4  A4
            5  K5  A5

            >>> other = pd.DataFrame({'key': ['K0', 'K1', 'K2'],
            ...                       'B': ['B0', 'B1', 'B2']})

            >>> other
              key   B
            0  K0  B0
            1  K1  B1
            2  K2  B2

            Join DataFrames using their indexes.

            >>> df.join(other, lsuffix='_caller', rsuffix='_other')
              key_caller   A key_other     B
            0         K0  A0        K0    B0
            1         K1  A1        K1    B1
            2         K2  A2        K2    B2
            3         K3  A3      None  None
            4         K4  A4      None  None
            5         K5  A5      None  None

            If we want to join using the key columns, we need to set key to be
            the index in both `df` and `other`. The joined DataFrame will have
            key as its index.

            >>> df.set_index('key').join(other.set_index('key'))  # doctest: +NORMALIZE_WHITESPACE
                  A     B
            key
            K0   A0    B0
            K1   A1    B1
            K2   A2    B2
            K3   A3  None
            K4   A4  None
            K5   A5  None

            Another option to join using the key columns is to use the `on`
            parameter. DataFrame.join always uses `other`'s index but we can use
            any column in `df`. This method preserves the original DataFrame's
            index in the result.

            >>> df.join(other.set_index('key'), on='key')
              key   A     B
            0  K0  A0    B0
            1  K1  A1    B1
            2  K2  A2    B2
            3  K3  A3  None
            4  K4  A4  None
            5  K5  A5  None

            Using non-unique key values shows how they are matched.

            >>> df = pd.DataFrame({'key': ['K0', 'K1', 'K1', 'K3', 'K0', 'K1'],
            ...                    'A': ['A0', 'A1', 'A2', 'A3', 'A4', 'A5']})

            >>> df
              key   A
            0  K0  A0
            1  K1  A1
            2  K1  A2
            3  K3  A3
            4  K0  A4
            5  K1  A5

            TODO: SNOW-890653 Enable this test

            >>> df.join(other.set_index('key'), on='key', validate='m:1')  # doctest: +SKIP
              key   A    B
            0  K0  A0   B0
            1  K1  A1   B1
            2  K1  A2   B1
            3  K3  A3  NaN
            4  K0  A4   B0
            5  K1  A5   B1
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
        """
        Detect missing values for an array-like object.

        This function takes a scalar or array-like object and indicates whether values are missing (NaN in numeric
        arrays, None or NaN in object arrays, NaT in datetimelike).

        Parameters
        ----------
        obj : scalar or array-like
                Object to check for null or missing values.

        Returns
        -------
        bool or array-like of bool
            For scalar input, returns a scalar boolean. For array input, returns an array of boolean indicating whether
            each corresponding element is missing.

        Examples
        --------
        >>> df = pd.DataFrame([['ant', 'bee', 'cat'], ['dog', None, 'fly']])
        >>> df
             0     1    2
        0  ant   bee  cat
        1  dog  None  fly
        >>> df.isna()
               0      1      2
        0  False  False  False
        1  False   True  False
        >>> df.isnull()
               0      1      2
        0  False  False  False
        1  False   True  False
        """
        return super().isna()

    def isnull(self):
        """
        Detect missing values for an array-like object.

        This function takes a scalar or array-like object and indicates whether values are missing (NaN in numeric
        arrays, None or NaN in object arrays, NaT in datetimelike).

        Parameters
        ----------
        obj : scalar or array-like
                Object to check for null or missing values.

        Returns
        -------
        bool or array-like of bool
            For scalar input, returns a scalar boolean. For array input, returns an array of boolean indicating whether
            each corresponding element is missing.

        Examples
        --------
        >>> df = pd.DataFrame([['ant', 'bee', 'cat'], ['dog', None, 'fly']])
        >>> df
             0     1    2
        0  ant   bee  cat
        1  dog  None  fly
        >>> df.isna()
               0      1      2
        0  False  False  False
        1  False   True  False
        >>> df.isnull()
               0      1      2
        0  False  False  False
        1  False   True  False
        """
        return super().isnull()

    def isetitem(self, loc, value):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()
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

        Parameters
        ----------
        id_vars : list of identifiers to retain in the result
        value_vars : list of columns to unpivot on
               defaults to all columns, excluding the id_vars columns
        var_name : variable name, defaults to "variable"
        value_name : value name, defaults to "value"
        col_level : int, not implemented
        ignore_index : bool

        Returns
        -------
            DataFrame
                unpivoted on the value columns

        Examples
        --------
        >>> df = pd.DataFrame({'A': {0: 'a', 1: 'b', 2: 'c'},
        ...           'B': {0: 1, 1: 3, 2: 5},
        ...           'C': {0: 2, 1: 4, 2: 6}})
        >>> df
           A  B  C
        0  a  1  2
        1  b  3  4
        2  c  5  6

        >>> df.melt()
          variable value
        0        A     a
        1        A     b
        2        A     c
        3        B     1
        4        B     3
        5        B     5
        6        C     2
        7        C     4
        8        C     6

        >>> df = pd.DataFrame({'A': {0: 'a', 1: 'b', 2: 'c'},
        ...           'B': {0: 1, 1: 3, 2: 5},
        ...           'C': {0: 2, 1: 4, 2: 6}})
        >>> df.melt(id_vars=['A'], value_vars=['B'], var_name='myVarname', value_name='myValname')
           A myVarname  myValname
        0  a         B          1
        1  b         B          3
        2  c         B          5

        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        if id_vars is None:
            id_vars = []
        if not is_list_like(id_vars):
            id_vars = [id_vars]
        if value_vars is None:
            value_vars = self.columns.difference(id_vars)
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

    def memory_usage(self, index=True, deep=False):  # noqa: PR01, RT01, D200
        """
        Return the memory usage of each column in bytes.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()  # pragma: no cover

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

    def nlargest(self, n, columns, keep="first"):  # noqa: PR01, RT01, D200
        """
        Return the first `n` rows ordered by `columns` in descending order.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()
        return self.__constructor__(
            query_compiler=self._query_compiler.nlargest(n, columns, keep)
        )

    def nsmallest(self, n, columns, keep="first"):  # noqa: PR01, RT01, D200
        """
        Return the first `n` rows ordered by `columns` in ascending order.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()
        return self.__constructor__(
            query_compiler=self._query_compiler.nsmallest(
                n=n, columns=columns, keep=keep
            )
        )

    def unstack(self, level=-1, fill_value=None):  # noqa: PR01, RT01, D200
        """
        Pivot a level of the (necessarily hierarchical) index labels.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()
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

    def pivot(self, index=None, columns=None, values=None):  # noqa: PR01, RT01, D200
        """
        Return reshaped ``DataFrame`` organized by given index / column values.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()
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

        The levels in the pivot table will be stored in MultiIndex objects
        (hierarchical indexes) on the index and columns of the result DataFrame.

        Parameters
        ----------
        values : list-like or scalar, optional
            Column or columns to aggregate.
        index : column, Grouper, array, or list of the previous
            Keys to group by on the pivot table index. If a list is passed,
            it can contain any of the other types (except list). If an array is
            passed, it must be the same length as the data and will be used in
            the same manner as column values.
        columns : column, Grouper, array, or list of the previous
            Keys to group by on the pivot table column. If a list is passed,
            it can contain any of the other types (except list). If an array is
            passed, it must be the same length as the data and will be used in
            the same manner as column values.
        aggfunc : function, list of functions, dict in string, default "mean".
            If a list of functions is passed, the resulting pivot table will have
            hierarchical columns whose top level are the function names
            (inferred from the function objects themselves).
            If a dict is passed, the key is column to aggregate and the value is
            function or list of functions. If ``margin=True``, aggfunc will be
            used to calculate the partial aggregates.
        fill_value : scalar, default None
            Value to replace missing values with (in the resulting pivot table,
            after aggregation).
        margins : bool, default False
            If ``margins=True``, special ``All`` columns and rows
            will be added with partial group aggregates across the categories
            on the rows and columns.
        dropna : bool, default True
            Do not include columns whose entries are all NaN. If True,
            rows with a NaN value in any column will be omitted before
            computing margins.
        margins_name : str, default 'All'
            Name of the row / column that will contain the totals
            when margins is True.
        observed : bool, default False
            This only applies if any of the groupers are Categoricals.
            If True: only show observed values for categorical groupers.
            If False: show all values for categorical groupers.

        sort : bool, default True
            Specifies if the result should be sorted.

        Returns
        -------
        Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            An Excel style pivot table.

        Notes
        -----
        Raise NotImplementedError if

            * margins, observed, or sort is given;
            * or index, columns, or values is not str;
            * or DataFrame contains MultiIndex;
            * or any argfunc is not "count", "mean", "min", "max", or "sum"

        See Also
        --------
        DataFrame.pivot : Pivot without aggregation that can handle
            non-numeric data.
        DataFrame.melt: Unpivot a DataFrame from wide to long format,
            optionally leaving identifiers set.
        wide_to_long : Wide panel to long format. Less flexible but more
            user-friendly than melt.

        Examples
        --------
        >>> df = pd.DataFrame({"A": ["foo", "foo", "foo", "foo", "foo",
        ...                          "bar", "bar", "bar", "bar"],
        ...                    "B": ["one", "one", "one", "two", "two",
        ...                          "one", "one", "two", "two"],
        ...                    "C": ["small", "large", "large", "small",
        ...                          "small", "large", "small", "small",
        ...                          "large"],
        ...                    "D": [1, 2, 2, 3, 3, 4, 5, 6, 7],
        ...                    "E": [2, 4, 5, 5, 6, 6, 8, 9, 9]})
        >>> df
             A    B      C  D  E
        0  foo  one  small  1  2
        1  foo  one  large  2  4
        2  foo  one  large  2  5
        3  foo  two  small  3  5
        4  foo  two  small  3  6
        5  bar  one  large  4  6
        6  bar  one  small  5  8
        7  bar  two  small  6  9
        8  bar  two  large  7  9

        This first example aggregates values by taking the sum.

        >>> table = df.pivot_table(values='D', index=['A', 'B'],
        ...                        columns=['C'], aggfunc="sum")
        >>> table  # doctest: +NORMALIZE_WHITESPACE
        C        large  small
        A   B
        bar one    4.0      5
            two    7.0      6
        foo one    4.0      1
            two    NaN      6

        We can also fill missing values using the `fill_value` parameter.

        >>> table = df.pivot_table(values='D', index=['A', 'B'],
        ...                        columns=['C'], aggfunc="sum", fill_value=0)
        >>> table  # doctest: +NORMALIZE_WHITESPACE
        C        large  small
        A   B
        bar one    4.0      5
            two    7.0      6
        foo one    4.0      1
            two    NaN      6

        >>> table = df.pivot_table(values=['D', 'E'], index=['A', 'C'],
        ...                        aggfunc={'D': "mean", 'E': "mean"})
        >>> table  # doctest: +NORMALIZE_WHITESPACE
                          D         E
                          D         E
        A   C
        bar large  5.500000  7.500000
            small  5.500000  8.500000
        foo large  2.000000  4.500000
            small  2.333333  4.333333

        >>> table = df.pivot_table(values=['D', 'E'], index=['A', 'C'],
        ...                        aggfunc={'D': "mean",
        ...                                 'E': ["min", "max", "mean"]})
        >>> table  # doctest: +NORMALIZE_WHITESPACE
                          D   E
                       mean max      mean min
                          D   E         E   E
        A   C
        bar large  5.500000   9  7.500000   6
            small  5.500000   9  8.500000   8
        foo large  2.000000   5  4.500000   4
            small  2.333333   6  4.333333   2
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
        ErrorMessage.not_implemented()
        return self._to_pandas().plot

    def pow(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get exponential power of ``DataFrame`` and `other`, element-wise (binary operator `pow`).

        Note:
            Native pandas doesn't allow ``a ** b``, where ``a`` is an integer and ``b``
            is a negative integer. However, Snowpark pandas API allows it and return the correct result.
            For example, ``pd.DataFrame([5]).pow(-7)`` is allowed, whereas it will raise an
            exception in native pandas.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op(
            "pow",
            other,
            axis=axis,
            level=level,
            fill_value=fill_value,
        )

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
        ErrorMessage.not_implemented()
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
        """
        Return values at the given quantile over requested axis.

        Parameters
        ----------
        q: float or array-like of float, default 0.5
            Value between 0 <= q <= 1, the quantile(s) to compute.
        axis: {0 or 'index', 1 or 'columns'}, default 0
            Axis across which to compute quantiles.
        numeric_only: bool, default False
            Include only data where `is_numeric_dtype` is true.
            When True, bool columns are included, but attempting to compute quantiles across
            bool values is an ill-defined error in both pandas and Snowpark pandas.
        interpolation: {"linear", "lower", "higher", "midpoint", "nearest"}, default "linear"
            Specifies the interpolation method to use if a quantile lies between two data points
            *i* and *j*:

            * linear: *i* + (*j* - *i*) * *fraction*, where *fraction* is the fractional part of the
              index surrounded by *i* and *j*.
            * lower: *i*.
            * higher: *j*.
            * nearest: *i* or *j*, whichever is nearest.
            * midpoint: (*i* + *j*) / 2.

        method: {"single", "table"}, default "single"
            Whether to compute quantiles per-column ("single") or over all columns ("table").
            When "table", the only allowed interpolation methods are "nearest", "lower", and "higher".

        Returns
        -------
        Series or DataFrame
            If ``q`` is an array, a DataFrame will be returned where the index is ``q``, the columns
            are the columns of ``self``, and the values are the quantiles.
            If ``q`` is a float, a Series will be returned where the index is the columns of
            ``self`` and the values are the quantiles.

        Examples
        --------
        >>> df = pd.DataFrame(np.array([[1, 1], [2, 10], [3, 100], [4, 100]]), columns=['a', 'b'])

        With a scalar q:

        >>> df.quantile(.1) # doctest: +NORMALIZE_WHITESPACE
        a    1.3
        b    3.7
        Name: 0.1, dtype: float64

        With a list q:

        >>> df.quantile([.1, .5]) # doctest: +NORMALIZE_WHITESPACE
               a     b
        0.1  1.3   3.7
        0.5  2.5  55.0

        Values considered NaN do not affect the result:

        >>> df = pd.DataFrame({"a": [None, 0, 25, 50, 75, 100, np.nan]})
        >>> df.quantile([0, 0.25, 0.5, 0.75, 1]) # doctest: +NORMALIZE_WHITESPACE
                  a
        0.00    0.0
        0.25   25.0
        0.50   50.0
        0.75   75.0
        1.00  100.0

        Notes
        -----
        Currently only supports calls with axis=0.

        Also, unsupported if q is a Snowpandas DataFrame or Series.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return super().quantile(
            q=q,
            axis=axis,
            numeric_only=numeric_only,
            interpolation=interpolation,
            method=method,
        )

    def query(self, expr, inplace=False, **kwargs):  # noqa: PR01, RT01, D200
        """
        Query the columns of a ``DataFrame`` with a boolean expression.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()
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
        Rename columns or index labels.

        Function / dict values must be unique (1-to-1). Labels not contained in
        a dict / Series will be left as-is. Extra labels listed don't throw an
        error.

        Parameters
        ----------
        mapper : dict-like or function
            Dict-like or function transformations to apply to
            that axis' values. Use either ``mapper`` and ``axis`` to
            specify the axis to target with ``mapper``, or ``index`` and
            ``columns``.
        index : dict-like or function
            Alternative to specifying axis (``mapper, axis=0``
            is equivalent to ``index=mapper``).
        columns : dict-like or function
            Alternative to specifying axis (``mapper, axis=1``
            is equivalent to ``columns=mapper``).
        axis : {0 or 'index', 1 or 'columns'}, default 0
            Axis to target with ``mapper``. Can be either the axis name
            ('index', 'columns') or number (0, 1). The default is 'index'.
        copy : bool, default True
            Also copy underlying data. copy has been ignored with Snowflake execution engine.
        inplace : bool, default False
            Whether to modify the DataFrame rather than creating a new one.
            If True then value of copy is ignored.
        level : int or level name, default None
            In case of a MultiIndex, only rename labels in the specified
            level.
        errors : {'ignore', 'raise'}, default 'ignore'
            If 'raise', raise a `KeyError` when a dict-like `mapper`, `index`,
            or `columns` contains labels that are not present in the Index
            being transformed.
            If 'ignore', existing keys will be renamed and extra keys will be
            ignored.

        Returns
        -------
        Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.DataFrame` or None
            DataFrame with the renamed axis labels or None if ``inplace=True``.

        Raises
        ------
        KeyError
            If any of the labels is not found in the selected axis and
            "errors='raise'".

        See Also
        --------
        DataFrame.rename_axis : Set the name of the axis.

        Examples
        --------
        ``DataFrame.rename`` supports two calling conventions

        * ``(index=index_mapper, columns=columns_mapper, ...)``
        * ``(mapper, axis={'index', 'columns'}, ...)``

        We *highly* recommend using keyword arguments to clarify your
        intent.

        Rename columns using a mapping:

        >>> df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
        >>> df.rename(columns={"A": "a", "B": "c"})
           a  c
        0  1  4
        1  2  5
        2  3  6

        Rename index using a mapping:

        >>> df.rename(index={0: "x", 1: "y", 2: "z"})
           A  B
        x  1  4
        y  2  5
        z  3  6

        Cast index labels to a different type:

        >>> df.index
        Index([0, 1, 2], dtype='int64')
        >>> df.rename(index=str).index
        Index(['0', '1', '2'], dtype='object')

        >>> df.rename(columns={"A": "a", "B": "b", "C": "c"}, errors="raise")
        Traceback (most recent call last):
          ...
        KeyError: "['C'] not found in axis"

        Using axis-style parameters:

        >>> df.rename(str.lower, axis='columns')
           a  b
        0  1  4
        1  2  5
        2  3  6

        >>> df.rename({1: 2, 2: 4}, axis='index')
           A  B
        0  1  4
        2  2  5
        4  3  6
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
        ErrorMessage.not_implemented()  # pragma: no cover

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

        Values of the DataFrame are replaced with other values dynamically.
        This differs from updating with ``.loc`` or ``.iloc``, which require
        you to specify a location to update with some value.

        Parameters
        ----------
        to_replace : str, regex, list, dict, Series, int, float, or None
            How to find the values that will be replaced.

            * numeric, str or regex:

                - numeric: numeric values equal to `to_replace` will be
                  replaced with `value`
                - str: string exactly matching `to_replace` will be replaced
                  with `value`
                - regex: regexs matching `to_replace` will be replaced with
                  `value`

            * list of str, regex, or numeric:

                - First, if `to_replace` and `value` are both lists, they
                  **must** be the same length.
                - Second, if ``regex=True`` then all the strings in **both**
                  lists will be interpreted as regexs otherwise they will match
                  directly. This doesn't matter much for `value` since there
                  are only a few possible substitution regexes you can use.
                - str, regex and numeric rules apply as above.

            * dict:

                - Dicts can be used to specify different replacement values
                  for different existing values. For example,
                  ``{{'a': 'b', 'y': 'z'}}`` replaces the value 'a' with 'b' and
                  'y' with 'z'. To use a dict in this way, the optional `value`
                  parameter should not be given.
                - For a DataFrame a dict can specify that different values
                  should be replaced in different columns. For example,
                  ``{{'a': 1, 'b': 'z'}}`` looks for the value 1 in column 'a'
                  and the value 'z' in column 'b' and replaces these values
                  with whatever is specified in `value`. The `value` parameter
                  should not be ``None`` in this case. You can treat this as a
                  special case of passing two lists except that you are
                  specifying the column to search in.
                - For a DataFrame nested dictionaries, e.g.,
                  ``{{'a': {{'b': np.nan}}}}``, are read as follows: look in column
                  'a' for the value 'b' and replace it with NaN. The optional `value`
                  parameter should not be specified to use a nested dict in this
                  way. You can nest regular expressions as well. Note that
                  column names (the top-level dictionary keys in a nested
                  dictionary) **cannot** be regular expressions.

            * None:

                - This means that the `regex` argument must be a string,
                  compiled regular expression, or list, dict, ndarray or
                  Series of such elements. If `value` is also ``None`` then
                  this **must** be a nested dictionary or Series.

            See the examples section for examples of each of these.
        value : scalar, dict, list, str, regex, default None
            Value to replace any values matching `to_replace` with.
            For a DataFrame a dict of values can be used to specify which
            value to use for each column (columns not in the dict will not be
            filled). Regular expressions, strings and lists or dicts of such
            objects are also allowed.
        inplace : bool, default False
            Whether to modify the DataFrame rather than creating a new one.
        limit : int, default None
            Maximum size gap to forward or backward fill.
            This parameter is not supported.
        regex : bool or same types as `to_replace`, default False
            Whether to interpret `to_replace` and/or `value` as regular
            expressions. Alternatively, this could be a regular expression or a
            list, dict, or array of regular expressions in which case
            `to_replace` must be ``None``.
        method : {{'pad', 'ffill', 'bfill'}}
            The method to use when for replacement, when `to_replace` is a
            scalar, list or tuple and `value` is ``None``.
            This parameter is not supported.

        Returns
        -------
        DataFrame
            DataFrame Object after replacement if inplace=False, None otherwise.

        Raises
        ------
        AssertionError
            * If `regex` is not a ``bool`` and `to_replace` is not ``None``.

        TypeError
            * If `to_replace` is not a scalar, array-like, ``dict``, or ``None``
            * If `to_replace` is a ``dict`` and `value` is not a ``list``,
              ``dict``, ``ndarray``, or ``Series``
            * If `to_replace` is ``None`` and `regex` is not compilable
              into a regular expression or is a list, dict, ndarray, or
              Series.
            * When replacing multiple ``bool`` or ``datetime64`` objects and
              the arguments to `to_replace` does not match the type of the
              value being replaced

        ValueError
            * If a ``list`` or an ``ndarray`` is passed to `to_replace` and
              `value` but they are not the same length.

        NotImplementedError
            * If ``method`` or ``limit`` is provided.

        Notes
        -----
        * Regex substitution is performed under the hood using snowflake backend.
          which supports POSIX ERE syntax for regular expressions. Please check usage
          notes for details.
          https://docs.snowflake.com/en/sql-reference/functions-regexp#general-usage-notes
        * Regular expressions only replace string values. If a regular expression is
          created to match floating point numbers, it will only match string data not
          numeric data.
        * This method has *a lot* of options. You are encouraged to experiment
          and play with this method to gain intuition about how it works.

        Examples
        --------

        **Scalar `to_replace` and `value`**

        >>> df = pd.DataFrame({'A': [0, 1, 2, 3, 4], 'B': [5, 6, 7, 8, 9]})
        >>> df.replace(0, 5)
           A  B
        0  5  5
        1  1  6
        2  2  7
        3  3  8
        4  4  9

        **List-like `to_replace`**

        >>> df.replace([0, 1, 2, 3], 4)
           A  B
        0  4  5
        1  4  6
        2  4  7
        3  4  8
        4  4  9

        >>> df.replace([0, 1, 2, 3], [4, 3, 2, 1])
           A  B
        0  4  5
        1  3  6
        2  2  7
        3  1  8
        4  4  9

        **dict-like `to_replace`**

        >>> df.replace({0: 10, 1: 100})
             A  B
        0   10  5
        1  100  6
        2    2  7
        3    3  8
        4    4  9

        >>> df = pd.DataFrame({'A': [0, 1, 2, 3, 4], 'B': [5, 6, 7, 8, 9], 'C': ['a', 'b', 'c', 'd', 'e']})
        >>> df.replace({'A': 0, 'B': 5}, 100)
             A    B  C
        0  100  100  a
        1    1    6  b
        2    2    7  c
        3    3    8  d
        4    4    9  e

        >>> df.replace({'A': {0: 100, 4: 400}})
             A  B  C
        0  100  5  a
        1    1  6  b
        2    2  7  c
        3    3  8  d
        4  400  9  e

        **Regular expression `to_replace`**

        >>> df = pd.DataFrame({'A': ['bat', 'foo', 'bait'],
        ...                    'B': ['abc', 'bar', 'xyz']})
        >>> df.replace(to_replace=r'^ba.$', value='new', regex=True)
              A    B
        0   new  abc
        1   foo  new
        2  bait  xyz

        >>> df.replace({'A': r'^ba.$'}, {'A': 'new'}, regex=True)
              A    B
        0   new  abc
        1   foo  bar
        2  bait  xyz

        >>> df.replace(regex=r'^ba.$', value='new')
              A    B
        0   new  abc
        1   foo  new
        2  bait  xyz

        >>> df.replace(regex={r'^ba.$': 'new', 'foo': 'xyz'})
              A    B
        0   new  abc
        1   xyz  new
        2  bait  xyz

        >>> df.replace(regex=[r'^ba.$', 'foo'], value='new')
              A    B
        0   new  abc
        1   new  new
        2  bait  xyz

        When ``regex=True``, ``value`` is not ``None`` and `to_replace` is a string,
        the replacement will be applied in all columns of the DataFrame.

        >>> df = pd.DataFrame({'A': [0, 1, 2, 3, 4],
        ...                    'B': ['a', 'b', 'c', 'd', 'e'],
        ...                    'C': ['f', 'g', 'h', 'i', 'j']})

        >>> df.replace(to_replace='^[a-g]', value='e', regex=True)
             A  B  C
        0  0.0  e  e
        1  1.0  e  e
        2  2.0  e  h
        3  3.0  e  i
        4  4.0  e  j

        If ``value`` is not ``None`` and `to_replace` is a dictionary, the dictionary
        keys will be the DataFrame columns that the replacement will be applied.

        >>> df.replace(to_replace={'B': '^[a-c]', 'C': '^[h-j]'}, value='e', regex=True)
           A  B  C
        0  0  e  f
        1  1  e  g
        2  2  e  e
        3  3  d  e
        4  4  e  e
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
        """
        Round a DataFrame to a variable number of decimal places.

        Parameters
        ----------
        decimals : int, dict, Series
            Number of decimal places to round each column to. If an int is given, round each column to the same number of places. Otherwise dict and Series round to variable numbers of places. Column names should be in the keys if decimals is a dict-like, or in the index if decimals is a Series. Any columns not included in decimals will be left as is. Elements of decimals which are not columns of the input will be ignored.
        *args
            Additional keywords have no effect but might be accepted for compatibility with numpy.
        **kwargs
            Additional keywords have no effect but might be accepted for compatibility with numpy.

        Returns
        -------
        DataFrame
            A DataFrame with the affected columns rounded to the specified number of decimal places.

        See Also
        --------
        numpy.around : Round a numpy array to the given number of decimals.
        Series.round : Round a Series to the given number of decimals.

        Examples
        --------
        >>> df = pd.DataFrame([(.21, .32), (.01, .67), (.66, .03), (.21, .18)], columns=['dogs', 'cats'])
        >>> df
           dogs  cats
        0  0.21  0.32
        1  0.01  0.67
        2  0.66  0.03
        3  0.21  0.18

        By providing an integer each column is rounded to the same number of decimal places

        >>> df.round(1)
           dogs  cats
        0   0.2   0.3
        1   0.0   0.7
        2   0.7   0.0
        3   0.2   0.2

        With a dict, the number of places for specific columns can be specified with the column names as key and the number of decimal places as value

        >>> df.round({'dogs': 1, 'cats': 0})
           dogs  cats
        0   0.2   0.0
        1   0.0   1.0
        2   0.7   0.0
        3   0.2   0.0
        """
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
        At least one of the parameters must be specified, and the include/exclude lists must not overlap.

        Parameters
        ----------
        include : Optional[ListLike | type], default None
            A list of dtypes to include in the result.
        exclude : Optional[ListLike | type], default None
            A list of dtypes to exclude from the result.

        Result
        ------
        DataFrame
            The subset of the frame including the dtypes in `include` and excluding the dtypes in
            `exclude`. If a column's dtype is a subtype of a type in both `include` and `exclude` (such
            as if `include=[np.number]` and `exclude=[int]`), then the column will be excluded.

        Examples
        --------
        >>> df = pd.DataFrame({'a': [1, 2] * 3,
        ...                    'b': [True, False] * 3,
        ...                    'c': [1.0, 2.0] * 3})

        >>> df.dtypes  # doctest: +NORMALIZE_WHITESPACE
        a      int64
        b       bool
        c    float64
        dtype: object

        Including all number columns:

        >>> df.select_dtypes("number")  # doctest: +NORMALIZE_WHITESPACE
           a    c
        0  1  1.0
        1  2  2.0
        2  1  1.0
        3  2  2.0
        4  1  1.0
        5  2  2.0

        Including only bool columns:

        >>> df.select_dtypes(include=[bool])  # doctest: +NORMALIZE_WHITESPACE
               b
        0   True
        1  False
        2   True
        3  False
        4   True
        5  False

        Excluding int columns:

        >>> df.select_dtypes(exclude=[int])  # doctest: +NORMALIZE_WHITESPACE
               b    c
        0   True  1.0
        1  False  2.0
        2   True  1.0
        3  False  2.0
        4   True  1.0
        5  False  2.0
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
        periods: int = 1,
        freq=None,
        axis: Axis = 0,
        fill_value: Hashable = no_default,
    ) -> DataFrame:
        """
        Shift data by desired number of periods along axis and replace columns with fill_value (default: None).

        Snowpark pandas does not support `freq` currently.

        Parameters
        ----------
        periods : int
            Number of periods to shift. Can be positive or negative.
        freq : not supported, default None
        axis : {0 or 'index', 1 or 'columns', None}, default None
            Shift direction.
        fill_value : object, optional
            The scalar value to use for newly introduced missing values.
            the default depends on the dtype of `self`.
            For numeric data, ``np.nan`` is used.
            For datetime, timedelta, or period data, etc. :attr:`NaT` is used.
            For extension dtypes, ``self.dtype.na_value`` is used.

        Returns
        -------
        DataFrame
            Copy of input object, shifted.

        Examples
        --------
        >>> df = pd.DataFrame({"Col1": [10, 20, 15, 30, 45],
        ...                    "Col2": [13, 23, 18, 33, 48],
        ...                    "Col3": [17, 27, 22, 37, 52]},
        ...                   index=pd.date_range("2020-01-01", "2020-01-05"))
        >>> df
                    Col1  Col2  Col3
        2020-01-01    10    13    17
        2020-01-02    20    23    27
        2020-01-03    15    18    22
        2020-01-04    30    33    37
        2020-01-05    45    48    52

        >>> df.shift(periods=3)
                    Col1  Col2  Col3
        2020-01-01   NaN   NaN   NaN
        2020-01-02   NaN   NaN   NaN
        2020-01-03   NaN   NaN   NaN
        2020-01-04  10.0  13.0  17.0
        2020-01-05  20.0  23.0  27.0

        >>> df.shift(periods=1, axis="columns")
                    Col1  Col2  Col3
        2020-01-01  None    10    13
        2020-01-02  None    20    23
        2020-01-03  None    15    18
        2020-01-04  None    30    33
        2020-01-05  None    45    48

        >>> df.shift(periods=3, fill_value=0)
                    Col1  Col2  Col3
        2020-01-01     0     0     0
        2020-01-02     0     0     0
        2020-01-03     0     0     0
        2020-01-04    10    13    17
        2020-01-05    20    23    27

        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return super().shift(periods, freq, axis, fill_value)

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
        Set the DataFrame index using existing columns.

        Set the DataFrame index (row labels) using one or more existing
        columns or arrays (of the correct length). The index can replace the
        existing index or expand on it.

        Parameters
        ----------
        keys : label or array-like or list of labels/arrays
            This parameter can be either a single column key, a single array of
            the same length as the calling DataFrame, or a list containing an
            arbitrary combination of column keys and arrays. Here, "array"
            encompasses :class:`Series`, :class:`Index`, ``np.ndarray``, and
            instances of :class:`~collections.abc.Iterator`.
        drop : bool, default True
            Delete columns to be used as the new index.
        append : bool, default False
            Whether to append columns to existing index.
        inplace : bool, default False
            Whether to modify the DataFrame rather than creating a new one.
        verify_integrity : bool, default False
            Check the new index for duplicates. Otherwise, defer the check until
            necessary. Setting to False will improve the performance of this
            method.

        Returns
        -------
        DataFrame or None
            Changed row labels or None if ``inplace=True``.

        See Also
        --------
        DataFrame.reset_index : Opposite of set_index.
        DataFrame.reindex : Change to new indices or expand indices.
        DataFrame.reindex_like : Change to same indices as other DataFrame.

        Examples
        --------
        >>> df = pd.DataFrame({'month': [1, 4, 7, 10],
        ...                    'year': [2012, 2014, 2013, 2014],
        ...                    'sale': [55, 40, 84, 31]})
        >>> df
           month  year  sale
        0      1  2012    55
        1      4  2014    40
        2      7  2013    84
        3     10  2014    31

        Set the index to become the 'month' column:

        >>> df.set_index('month')  # doctest: +NORMALIZE_WHITESPACE
               year  sale
        month
        1      2012    55
        4      2014    40
        7      2013    84
        10     2014    31

        Create a MultiIndex using columns 'year' and 'month':

        >>> df.set_index(['year', 'month'])  # doctest: +NORMALIZE_WHITESPACE
                    sale
        year month
        2012 1        55
        2014 4        40
        2013 7        84
        2014 10       31

        Create a MultiIndex using an Index and a column:

        >>> df.set_index([pd.Index([1, 2, 3, 4]), 'year']) # doctest: +NORMALIZE_WHITESPACE
                 month  sale
           year
        1  2012  1      55
        2  2014  4      40
        3  2013  7      84
        4  2014  10     31

        Create a MultiIndex using two Series:

        >>> s = pd.Series([1, 2, 3, 4])
        >>> df.set_index([s, s**2]) # doctest: +NORMALIZE_WHITESPACE
                month  year  sale
        1 1.0       1  2012    55
        2 4.0       4  2014    40
        3 9.0       7  2013    84
        4 16.0     10  2014    31
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
            elif isinstance(key, pd.Index):
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

        Series or DataFrames with a single element are squeezed to a scalar.
        DataFrames with a single column or a single row are squeezed to a
        Series. Otherwise, the object is unchanged.

        This method is most useful when you don't know if your
        object is a Series or DataFrame, but you do know it has just a single
        column. In that case you can safely call `squeeze` to ensure you have a
        Series.

        Parameters
        ----------
        axis : {0 or 'index', 1 or 'columns', None}, default None
            A specific axis to squeeze. By default, all length-1 axes are
            squeezed. For `Series` this parameter is unused and defaults to `None`.

        Returns
        -------
        Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.DataFrame`, Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.Series`, or scalar
            The projection after squeezing `axis` or all the axes.

        See Also
        --------
        Series.iloc : Integer-location based indexing for selecting scalars.
        DataFrame.iloc : Integer-location based indexing for selecting Series.
        Series.to_frame : Inverse of DataFrame.squeeze for a
            single-column DataFrame.

        Examples
        --------
        >>> primes = pd.Series([2, 3, 5, 7])

        Slicing might produce a Series with a single value:

        >>> even_primes = primes[primes % 2 == 0]   # doctest: +SKIP
        >>> even_primes  # doctest: +SKIP
        0    2
        dtype: int64

        >>> even_primes.squeeze()  # doctest: +SKIP
        2

        Squeezing objects with more than one value in every axis does nothing:

        >>> odd_primes = primes[primes % 2 == 1]  # doctest: +SKIP
        >>> odd_primes  # doctest: +SKIP
        1    3
        2    5
        3    7
        dtype: int64

        >>> odd_primes.squeeze()  # doctest: +SKIP
        1    3
        2    5
        3    7
        dtype: int64

        Squeezing is even more effective when used with DataFrames.

        >>> df = pd.DataFrame([[1, 2], [3, 4]], columns=['a', 'b'])
        >>> df
           a  b
        0  1  2
        1  3  4

        Slicing a single column will produce a DataFrame with the columns
        having only one value:

        >>> df_a = df[['a']]
        >>> df_a
           a
        0  1
        1  3

        So the columns can be squeezed down, resulting in a Series:

        >>> df_a.squeeze('columns')
        0    1
        1    3
        Name: a, dtype: int64

        Slicing a single row from a single column will produce a single
        scalar DataFrame:

        >>> df_0a = df.loc[df.index < 1, ['a']]
        >>> df_0a
           a
        0  1

        Squeezing the rows produces a single scalar Series:

        >>> df_0a.squeeze('rows')
        a    1
        Name: 0, dtype: int64

        Squeezing all axes will project directly into a scalar:

        >>> df_0a.squeeze()
        1
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        axis = self._get_axis_number(axis) if axis is not None else None
        len_columns = self._query_compiler.get_axis_len(1)
        if axis == 1 and len_columns == 1:
            return Series(query_compiler=self._query_compiler)
        # get_axis_len(0) results in a sql query to count number of rows in current
        # dataframe. We should only compute len_index if axis is 0 or None.
        len_index = len(self)
        if axis is None and (len_columns == 1 or len_index == 1):
            return Series(query_compiler=self._query_compiler).squeeze()
        if axis == 0 and len_index == 1:
            return Series(query_compiler=self.T._query_compiler)
        else:
            return self.copy()

    def stack(self, level=-1, dropna=True):  # noqa: PR01, RT01, D200
        """
        Stack the prescribed level(s) from columns to index.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()
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

    def to_feather(self, path, **kwargs):  # pragma: no cover # noqa: PR01, RT01, D200
        """
        Write a ``DataFrame`` to the binary Feather format.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()
        return self._default_to_pandas(pandas.DataFrame.to_feather, path, **kwargs)

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
        ErrorMessage.not_implemented()
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

    def to_orc(self, path=None, *, engine="pyarrow", index=None, engine_kwargs=None):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()
        return self._default_to_pandas(
            pandas.DataFrame.to_orc,
            path=path,
            engine=engine,
            index=index,
            engine_kwargs=engine_kwargs,
        )

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
        ErrorMessage.not_implemented()
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
        ErrorMessage.not_implemented()
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

    def to_period(
        self, freq=None, axis=0, copy=True
    ):  # pragma: no cover # noqa: PR01, RT01, D200
        """
        Convert ``DataFrame`` from ``DatetimeIndex`` to ``PeriodIndex``.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()
        return super().to_period(freq=freq, axis=axis, copy=copy)

    def to_records(
        self, index=True, column_dtypes=None, index_dtypes=None
    ):  # noqa: PR01, RT01, D200
        """
        Convert ``DataFrame`` to a NumPy record array.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()
        return self._default_to_pandas(
            pandas.DataFrame.to_records,
            index=index,
            column_dtypes=column_dtypes,
            index_dtypes=index_dtypes,
        )

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
        ErrorMessage.not_implemented()
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
        ErrorMessage.not_implemented()
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
        """
        Convert the DataFrame to a dictionary.
        Note that this method will pull the data to the client side.

        The type of the key-value pairs can be customized with the parameters
        (see below).

        Parameters
        ----------
        orient : str {'dict', 'list', 'series', 'split', 'tight', 'records', 'index'}
            Determines the type of the values of the dictionary.

            - 'dict' (default) : dict like {column -> {index -> value}}
            - 'list' : dict like {column -> [values]}
            - 'series' : dict like {column -> Series(values)}. Note that the result will be native pandas Series
            - 'split' : dict like
              {'index' -> [index], 'columns' -> [columns], 'data' -> [values]}
            - 'tight' : dict like
              {'index' -> [index], 'columns' -> [columns], 'data' -> [values],
              'index_names' -> [index.names], 'column_names' -> [column.names]}
            - 'records' : list like
              [{column -> value}, ... , {column -> value}]
            - 'index' : dict like {index -> {column -> value}}

        into : class, default dict
            The collections.abc.Mapping subclass used for all Mappings
            in the return value.  Can be the actual class or an empty
            instance of the mapping type you want.  If you want a
            collections.defaultdict, you must pass it initialized.

        Returns
        -------
        dict, list or collections.abc.Mapping
            Return a collections.abc.Mapping object representing the DataFrame.
            The resulting transformation depends on the `orient` parameter.

        Examples
        --------
        >>> df = pd.DataFrame({'col1': [1, 2],
        ...                    'col2': [0.5, 0.75]},
        ...                   index=['row1', 'row2'])
        >>> df
              col1  col2
        row1     1  0.50
        row2     2  0.75
        >>> df.to_dict()  # doctest: +NORMALIZE_WHITESPACE
        {'col1': {'row1': 1, 'row2': 2}, 'col2': {'row1': 0.5, 'row2': 0.75}}

        You can specify the return orientation.

        >>> df.to_dict('series')  # doctest: +NORMALIZE_WHITESPACE
        {'col1': row1    1
                 row2    2
        Name: col1, dtype: int64,
        'col2': row1    0.50
                row2    0.75
        Name: col2, dtype: float64}

        >>> df.to_dict('split')  # doctest: +NORMALIZE_WHITESPACE
        {'index': ['row1', 'row2'], 'columns': ['col1', 'col2'],
         'data': [[1, 0.5], [2, 0.75]]}

        >>> df.to_dict('records')  # doctest: +NORMALIZE_WHITESPACE
        [{'col1': 1, 'col2': 0.5}, {'col1': 2, 'col2': 0.75}]

        >>> df.to_dict('index')  # doctest: +NORMALIZE_WHITESPACE
        {'row1': {'col1': 1, 'col2': 0.5}, 'row2': {'col1': 2, 'col2': 0.75}}

        >>> df.to_dict('tight')  # doctest: +NORMALIZE_WHITESPACE
        {'index': ['row1', 'row2'], 'columns': ['col1', 'col2'],
         'data': [[1, 0.5], [2, 0.75]], 'index_names': [None], 'column_names': [None]}

        You can also specify the mapping type.

        >>> from collections import OrderedDict, defaultdict
        >>> df.to_dict(into=OrderedDict)  # doctest: +NORMALIZE_WHITESPACE
        OrderedDict([('col1', OrderedDict([('row1', 1), ('row2', 2)])),
                     ('col2', OrderedDict([('row1', 0.5), ('row2', 0.75)]))])

        If you want a `defaultdict`, you need to initialize it:

        >>> dd = defaultdict(list)
        >>> df.to_dict('records', into=dd)  # doctest: +NORMALIZE_WHITESPACE
        [defaultdict(<class 'list'>, {'col1': 1, 'col2': 0.5}),
         defaultdict(<class 'list'>, {'col1': 2, 'col2': 0.75})]
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._to_pandas().to_dict(orient=orient, into=into)

    def to_timestamp(
        self, freq=None, how="start", axis=0, copy=True
    ):  # noqa: PR01, RT01, D200
        """
        Cast to DatetimeIndex of timestamps, at *beginning* of period.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()
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
        """
        First discrete difference of element.

        Calculates the difference of a DataFrame element compared with another element
        in the DataFrame (default is element in previous row).

        Parameters
        ----------
        periods : int, default 1
            Periods to shift for calculating difference, accepts negative values.

        axis : {0 or 'index', 1 or 'columns'}, default 0
            Take difference over rows (0) or columns (1).

        Returns
        -------
        Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.DataFrame` with the
            first differences of the Series.

        Notes
        -----
        For boolean dtypes, this uses operator.xor() rather than operator.sub(). The result
        is calculated according to current dtype in DataFrame, however dtype of the result
        is always float64.

        Examples
        --------
        >>> df = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6],
        ...                    'b': [1, 1, 2, 3, 5, 8],
        ...                    'c': [1, 4, 9, 16, 25, 36]})
        >>> df # doctest: +NORMALIZE_WHITESPACE
           a  b   c
        0  1  1   1
        1  2  1   4
        2  3  2   9
        3  4  3  16
        4  5  5  25
        5  6  8  36

        Difference with previous row

        >>> df.diff() # doctest: +NORMALIZE_WHITESPACE
            a    b     c
        0  NaN  NaN   NaN
        1  1.0  0.0   3.0
        2  1.0  1.0   5.0
        3  1.0  1.0   7.0
        4  1.0  2.0   9.0
        5  1.0  3.0  11.0

        Difference with previous column

        >>> df.diff(axis=1) # doctest: +NORMALIZE_WHITESPACE
            a   b   c
        0 None  0   0
        1 None -1   3
        2 None -1   7
        3 None -1  13
        4 None  0  20
        5 None  2  28

        Difference with 3rd previous row

        >>> df.diff(periods=3) # doctest: +NORMALIZE_WHITESPACE
            a    b     c
        0  NaN  NaN   NaN
        1  NaN  NaN   NaN
        2  NaN  NaN   NaN
        3  3.0  2.0  15.0
        4  3.0  4.0  21.0
        5  3.0  6.0  27.0

        Difference with following row

        >>> df.diff(periods=-1) # doctest: +NORMALIZE_WHITESPACE
            a    b     c
        0 -1.0  0.0  -3.0
        1 -1.0 -1.0  -5.0
        2 -1.0 -1.0  -7.0
        3 -1.0 -2.0  -9.0
        4 -1.0 -3.0 -11.0
        5  NaN  NaN   NaN
        """
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
        """
        Drop specified labels from rows or columns.

        Remove rows or columns by specifying label names and corresponding
        axis, or by specifying directly index or column names. When using a
        multi-index, labels on different levels can be removed by specifying
        the level. See the `user guide <advanced.shown_levels>`
        for more information about the now unused levels.

        Parameters
        ----------
        labels : single label or list-like
            Index or column labels to drop. A tuple will be used as a single
            label and not treated as a list-like.
        axis : {0 or 'index', 1 or 'columns'}, default 0
            Whether to drop labels from the index (0 or 'index') or
            columns (1 or 'columns').
        index : single label or list-like
            Alternative to specifying axis (``labels, axis=0``
            is equivalent to ``index=labels``).
        columns : single label or list-like
            Alternative to specifying axis (``labels, axis=1``
            is equivalent to ``columns=labels``).
        level : int or level name, optional
            For MultiIndex, level from which the labels will be removed.
        inplace : bool, default False
            If False, return a copy. Otherwise, do operation
            inplace and return None.
        errors : {'ignore', 'raise'}, default 'raise'
            If 'ignore', suppress error and only existing labels are
            dropped.

        Returns
        -------
        Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.DataFrame` or None
            Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.DataFrame` without the removed index or column labels or
            None if ``inplace=True``.

        Raises
        ------
        KeyError
            If any of the labels is not found in the selected axis.


        Examples
        --------
        >>> df = pd.DataFrame(np.arange(12).reshape(3, 4),
        ...                   columns=['A', 'B', 'C', 'D'])
        >>> df
           A  B   C   D
        0  0  1   2   3
        1  4  5   6   7
        2  8  9  10  11

        Drop columns

        >>> df.drop(['B', 'C'], axis=1)
           A   D
        0  0   3
        1  4   7
        2  8  11

        >>> df.drop(columns=['B', 'C'])
           A   D
        0  0   3
        1  4   7
        2  8  11

        Drop a row by index

        >>> df.drop([0, 1])
           A  B   C   D
        2  8  9  10  11

        Drop columns and/or rows of MultiIndex DataFrame

        >>> midx = pd.MultiIndex(levels=[['lama', 'cow', 'falcon'],
        ...                              ['speed', 'weight', 'length']],
        ...                      codes=[[0, 0, 0, 1, 1, 1, 2, 2, 2],
        ...                             [0, 1, 2, 0, 1, 2, 0, 1, 2]])
        >>> df = pd.DataFrame(index=midx, columns=['big', 'small'],
        ...                   data=[[45, 30], [200, 100], [1.5, 1], [30, 20],
        ...                         [250, 150], [1.5, 0.8], [320, 250],
        ...                         [1, 0.8], [0.3, 0.2]])
        >>> df
                         big  small
        lama   speed    45.0   30.0
               weight  200.0  100.0
               length    1.5    1.0
        cow    speed    30.0   20.0
               weight  250.0  150.0
               length    1.5    0.8
        falcon speed   320.0  250.0
               weight    1.0    0.8
               length    0.3    0.2

        Drop a specific index combination from the MultiIndex
        DataFrame, i.e., drop the combination ``'falcon'`` and
        ``'weight'``, which deletes only the corresponding row

        >>> df.drop(index=('falcon', 'weight'))
                         big  small
        lama   speed    45.0   30.0
               weight  200.0  100.0
               length    1.5    1.0
        cow    speed    30.0   20.0
               weight  250.0  150.0
               length    1.5    0.8
        falcon speed   320.0  250.0
               length    0.3    0.2

        >>> df.drop(index='cow', columns='small')
                         big
        lama   speed    45.0
               weight  200.0
               length    1.5
        falcon speed   320.0
               weight    1.0
               length    0.3

        >>> df.drop(index='length', level=1)
                         big  small
        lama   speed    45.0   30.0
               weight  200.0  100.0
        cow    speed    30.0   20.0
               weight  250.0  150.0
        falcon speed   320.0  250.0
               weight    1.0    0.8
        """
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
        """
        Return a Series containing the frequency of each distinct row in the Dataframe.

        Parameters
        ----------
        subset : label or list of labels, optional
            Columns to use when counting unique combinations.
        normalize : bool, default False
            Return proportions rather than frequencies. Being different from native pandas,
            Snowpark pandas will return a Series with `decimal.Decimal` values.
        sort : bool, default True
            Sort by frequencies when True. Sort by DataFrame column values when False.
            When there is a tie between counts, the order is still deterministic, but
            may be different from the result from native pandas.
        ascending : bool, default False
            Sort in ascending order.
        dropna : bool, default True
            Don't include counts of rows that contain NA values.

        Returns
        -------
        Series

        See Also
        --------
        :func:`Series.map <snowflake.snowpark.modin.pandas.Series.>` : Equivalent method on Series.

        Notes
        -----
        The returned Series will have a MultiIndex with one level per input
        column but an Index (non-multi) for a single label. By default, rows
        that contain any NA values are omitted from the result. By default,
        the resulting Series will be in descending order so that the first
        element is the most frequently-occurring row.

        Examples
        --------
        >>> df = pd.DataFrame({'num_legs': [2, 4, 4, 6],
        ...                    'num_wings': [2, 0, 0, 0]},
        ...                   index=['falcon', 'dog', 'cat', 'ant'])
        >>> df
                num_legs  num_wings
        falcon         2          2
        dog            4          0
        cat            4          0
        ant            6          0

        >>> df.value_counts()
        num_legs  num_wings
        4         0            2
        2         2            1
        6         0            1
        Name: count, dtype: int64

        >>> df.value_counts(sort=False)
        num_legs  num_wings
        2         2            1
        4         0            2
        6         0            1
        Name: count, dtype: int64

        >>> df.value_counts(ascending=True)
        num_legs  num_wings
        2         2            1
        6         0            1
        4         0            2
        Name: count, dtype: int64

        >>> df.value_counts(normalize=True)
        num_legs  num_wings
        4         0            0.50
        2         2            0.25
        6         0            0.25
        Name: proportion, dtype: float64

        With `dropna` set to `False` we can also count rows with NA values.

        >>> df = pd.DataFrame({'first_name': ['John', 'Anne', 'John', 'Beth'],
        ...                    'middle_name': ['Smith', None, None, 'Louise']})
        >>> df
          first_name middle_name
        0       John       Smith
        1       Anne        None
        2       John        None
        3       Beth      Louise

        >>> df.value_counts()
        first_name  middle_name
        John        Smith          1
        Beth        Louise         1
        Name: count, dtype: int64

        >>> df.value_counts(dropna=False)
        first_name  middle_name
        John        Smith          1
        Anne        NaN            1
        John        NaN            1
        Beth        Louise         1
        Name: count, dtype: int64

        >>> df.value_counts("first_name")
        first_name
        John    2
        Anne    1
        Beth    1
        Name: count, dtype: int64
        """
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
        """
        Replace values where the condition is True.

        Args:
            cond: bool Series/DataFrame, array-like, or callable
                Where cond is False, keep the original value. Where True, replace with corresponding value from other.
                If cond is callable, it is computed on the Series/DataFrame and should return boolean Series/DataFrame
                or array. The callable must not change input Series/DataFrame (though pandas doesn’t check it).

            other: scalar, Series/DataFrame, or callable
                Entries where cond is True are replaced with corresponding value from other. If other is callable,
                it is computed on the Series/DataFrame and should return scalar or Series/DataFrame. The callable must
                not change input Series/DataFrame (though pandas doesn’t check it).

            inplace: bool, default False
                Whether to perform the operation in place on the data.

            axis: int, default None
                Alignment axis if needed. For Series this parameter is unused and defaults to 0.

            level: int, default None
                Alignment level if needed.

        Returns:
            Same type as caller or None if inplace=True.

        See Also:
            DataFrame.where : Replace values where the condition is False.

        Notes:
            The mask method is an application of the if-then idiom. For each element in the calling DataFrame, if cond
            is False the element is used; otherwise the corresponding element from the DataFrame other is used. If the
            axis of other does not align with axis of cond Series/DataFrame, the misaligned index positions will be
            filled with True.

            The signature for DataFrame.where() differs from numpy.where(). Roughly df1.where(m, df2) is equivalent to
            np.where(m, df1, df2).

            For further details and examples see the mask documentation in indexing.

            The dtype of the object takes precedence. The fill value is cast to the object’s dtype, if this can be
            done losslessly.

        Examples::
        >>> df = pd.DataFrame(np.arange(10).reshape(-1, 2), columns=['A', 'B'])
        >>> df   # doctest: +NORMALIZE_WHITESPACE
        A  B
        0  0  1
        1  2  3
        2  4  5
        3  6  7
        4  8  9

        >>> m = df % 3 == 0
        >>> df.mask(m, -df)   # doctest: +NORMALIZE_WHITESPACE
           A  B
        0  0  1
        1  2 -3
        2  4  5
        3 -6  7
        4  8 -9

        Snowpark pandas `DataFrame.mask` behaves the same as `numpy.where`.

        >>> data = np.where(~m, df, -df)
        >>> df.mask(m, -df) == pd.DataFrame(data, columns=['A', 'B'])  # doctest: +NORMALIZE_WHITESPACE
            A     B
        0  True  True
        1  True  True
        2  True  True
        3  True  True
        4  True  True

        >>> df.mask(m, -df) == df.where(~m, -df)  # doctest: +NORMALIZE_WHITESPACE
            A     B
        0  True  True
        1  True  True
        2  True  True
        3  True  True
        4  True  True
        """
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

        Args:
            cond: bool Series/DataFrame, array-like, or callable
                Where cond is True, keep the original value. Where False, replace with corresponding value from other.
                If cond is callable, it is computed on the Series/DataFrame and should return boolean Series/DataFrame
                or array. The callable must not change input Series/DataFrame (though pandas doesn’t check it).

            other: scalar, Series/DataFrame, or callable
                Entries where cond is False are replaced with corresponding value from other. If other is callable,
                it is computed on the Series/DataFrame and should return scalar or Series/DataFrame. The callable must
                not change input Series/DataFrame (though pandas doesn’t check it). If not specified, entries will be
                filled with the corresponding NULL value (np.nan for numpy dtypes, pd.NA for extension dtypes).

            inplace: bool, default False
                Whether to perform the operation in place on the data.

            axis: int, default None
                Alignment axis if needed. For Series this parameter is unused and defaults to 0.

            level: int, default None
                Alignment level if needed.

        Returns:
            Same type as caller or None if inplace=True.

        Notes:
            The where method is an application of the if-then idiom. For each element in the calling DataFrame, if cond
            is True the element is used; otherwise the corresponding element from the DataFrame other is used. If the
            axis of other does not align with axis of cond Series/DataFrame, the misaligned index positions will be
            filled with False.

            The signature for DataFrame.where() differs from numpy.where(). Roughly df1.where(m, df2) is equivalent to
            np.where(m, df1, df2).

            For further details and examples see the where documentation in indexing.

            The dtype of the object takes precedence. The fill value is cast to the object’s dtype, if this can be
            done losslessly.

        Examples::

        >>> df = pd.DataFrame(np.arange(10).reshape(-1, 2), columns=['A', 'B'])

        >>> df  # doctest: +NORMALIZE_WHITESPACE
           A  B
        0  0  1
        1  2  3
        2  4  5
        3  6  7
        4  8  9

        >>> m = df % 3 == 0
        >>> df.where(m, -df)  # doctest: +NORMALIZE_WHITESPACE
           A  B
        0  0 -1
        1 -2  3
        2 -4 -5
        3  6 -7
        4 -8  9

        Snowpark pandas `DataFrame.where` behaves the same as `numpy.where`.

        >>> data = np.where(m, df, -df)
        >>> df.where(m, -df) == pd.DataFrame(data, columns=['A', 'B'])  # doctest: +NORMALIZE_WHITESPACE
              A     B
        0  True  True
        1  True  True
        2  True  True
        3  True  True
        4  True  True

        >>> df.where(m, -df) == df.mask(~m, -df)  # doctest: +SKIP
              A     B
        0  True  True
        1  True  True
        2  True  True
        3  True  True
        4  True  True
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

    def xs(self, key, axis=0, level=None, drop_level=True):  # noqa: PR01, RT01, D200
        """
        Return cross-section from the ``DataFrame``.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()
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
        """
        Assign desired index to given axis.

        Parameters
        ----------
        labels : list-like, Index, MultiIndex
            The values for the new index.
        axis : {index (0), rows(0), columns (1)}
            Axis for the function to be applied on.
        copy : bool, default True
            To maintain compatibility with pandas, does nothing.

        Returns
        -------
        DataFrame

        Examples
        --------
        >>> df = pd.DataFrame({
        ... "Videogame": ["Dark Souls", "Cruelty Squad", "Stardew Valley"],
        ... "Genre": ["Souls-like", "Immersive-sim", "Farming-sim"],
        ... "Rating": [9.5, 9.0, 8.7]})
        >>> df.set_axis(['a', 'b', 'c'], axis="index") # doctest: +NORMALIZE_WHITESPACE
                Videogame          Genre  Rating
        a      Dark Souls     Souls-like     9.5
        b   Cruelty Squad  Immersive-sim     9.0
        c  Stardew Valley    Farming-sim     8.7

        >>> df.set_axis(["Name", "Sub-genre", "Rating out of 10"], axis=1) # doctest: +NORMALIZE_WHITESPACE
                     Name      Sub-genre  Rating out of 10
        0      Dark Souls     Souls-like               9.5
        1   Cruelty Squad  Immersive-sim               9.0
        2  Stardew Valley    Farming-sim               8.7

        >>> columns = pd.MultiIndex.from_tuples([("Gas", "Toyota"), ("Gas", "Ford"), ("Electric", "Tesla"), ("Electric", "Nio"),])
        >>> data = [[100, 300, 900, 400], [200, 500, 300, 600]]
        >>> df = pd.DataFrame(columns=columns, data=data)
        >>> df.set_axis([2010, 2015], axis="rows") # doctest: +NORMALIZE_WHITESPACE
                Gas      Electric
             Toyota Ford    Tesla  Nio
        2010    100  300      900  400
        2015    200  500      300  600
        """
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
        """
        Return a DataFrame with absolute numeric value of each element.

        Returns
        -------
        DataFrame

        Examples
        --------
        >>> df = pd.DataFrame({'a': [1,-2,3], 'b': [-4.33, 5, 6]})
        >>> df
           a     b
        0  1 -4.33
        1 -2  5.00
        2  3  6.00

        >>> abs(df)
           a     b
        0  1  4.33
        1  2  5.00
        2  3  6.00
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return super().abs()

    @_doc_binary_op(
        operation="union", bin_op="and", right="other", **_doc_binary_op_kwargs
    )
    def __and__(self, other):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op("__and__", other, axis=1)

    @_doc_binary_op(
        operation="union", bin_op="rand", right="other", **_doc_binary_op_kwargs
    )
    def __rand__(self, other):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op("__rand__", other, axis=1)

    @_doc_binary_op(
        operation="disjunction",
        bin_op="or",
        right="other",
        **_doc_binary_op_kwargs,
    )
    def __or__(self, other):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op("__or__", other, axis=1)

    @_doc_binary_op(
        operation="disjunction",
        bin_op="ror",
        right="other",
        **_doc_binary_op_kwargs,
    )
    def __ror__(self, other):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        return self._binary_op("__ror__", other, axis=1)

    def __neg__(self):
        """
        Returns a DataFrame with the sign changed for each element.

        Returns
        -------
        DataFrame

        Examples
        --------
        >>> df = pd.DataFrame({'a': [1,-2,3], 'b': [-4.33, 5, 6]})
        >>> df
           a     b
        0  1 -4.33
        1 -2  5.00
        2  3  6.00

        >>> - df
           a     b
        0 -1  4.33
        1  2 -5.00
        2 -3 -6.00
        """
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

    def __delitem__(self, key):
        """
        Delete item identified by `key` label.

        Parameters
        ----------
        key : hashable
            Key to delete.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()  # pragma: no cover

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
        ErrorMessage.not_implemented()  # pragma: no cover

        return self._query_compiler.to_dataframe(
            nan_as_null=nan_as_null, allow_copy=allow_copy
        )

    @property
    def attrs(self):  # noqa: RT01, D200
        """
        Return dictionary of global attributes of this dataset.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()  # pragma: no cover

        def attrs(df):
            return df.attrs

        return self._default_to_pandas(attrs)

    @property
    def style(self):  # noqa: RT01, D200
        """
        Return a Styler object.
        """
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()

        def style(df):
            """Define __name__ attr because properties do not have it."""
            return df.style

        return self._default_to_pandas(style)

    def isin(
        self, values: ListLike | Series | DataFrame | dict[Hashable, ListLike]
    ) -> DataFrame:
        """
        Whether each element in the DataFrame is contained in values.

        Parameters
        ----------
        values : list-like, Series, DataFrame or dict
            The result will only be true at a location if all the
            labels match. If `values` is a Series, that's the index. If
            `values` is a dict, the keys must be the column names,
            which must match. If `values` is a DataFrame,
            then both the index and column labels must match.

            Snowpark pandas assumes that in the case of values being Series or DataFrame that the index of
            values is unique (i.e., values.index.is_unique() = True)

        Returns
        -------
        DataFrame
            DataFrame of booleans showing whether each element in the DataFrame
            is contained in values.

        Examples
        --------
        >>> df = pd.DataFrame({'num_legs': [2, 4], 'num_wings': [2, 0]},
        ...                   index=['falcon', 'dog'])
        >>> df
                num_legs  num_wings
        falcon         2          2
        dog            4          0

        When ``values`` is a list check whether every value in the DataFrame
        is present in the list (which animals have 0 or 2 legs or wings)

        >>> df.isin([0, 2])
                num_legs  num_wings
        falcon      True       True
        dog        False       True

        To check if ``values`` is *not* in the DataFrame, use the ``~`` operator:

        >>> ~df.isin([0, 2])
                num_legs  num_wings
        falcon     False      False
        dog         True      False

        When ``values`` is a dict, we can pass values to check for each
        column separately:

        >>> df.isin({'num_wings': [0, 3]})
                num_legs  num_wings
        falcon     False      False
        dog        False       True

        When ``values`` is a Series or DataFrame the index and column must
        match. Note that 'falcon' does not match based on the number of legs
        in other.

        >>> other = pd.DataFrame({'num_legs': [8, 3], 'num_wings': [0, 2]},
        ...                      index=['spider', 'falcon'])
        >>> df.isin(other)
                num_legs  num_wings
        falcon     False       True
        dog        False      False

        Caution
        -------
        Snowpark pandas does not perform a check for the case that values is a DataFrame or Series nor does it check
        whether the index is unique. Snowpark pandas preserves NULL values; if the DataFrame contains NULL in
        a cell the output cell will be NULL.
        """
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
                ErrorMessage.not_implemented()  # pragma: no cover

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
        """
        Alter the name or names of the axis.

        Parameters
        ----------
        name : str or list of str
            Name for the Index, or list of names for the MultiIndex.
        axis : str or int, default: 0
            The axis to set the label.
            0 or 'index' for the index, 1 or 'columns' for the columns.
        inplace : bool, default: False
            Whether to modify `self` directly or return a copy.

        Returns
        -------
        DataFrame or None
        """
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

    def __reduce__(self):
        # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
        ErrorMessage.not_implemented()  # pragma: no cover

        self._query_compiler.finalize()
        # if PersistentPickle.get():
        #    return self._inflate_full, (self._to_pandas(),)
        return self._inflate_light, (self._query_compiler,)

    # Persistance support methods - END
