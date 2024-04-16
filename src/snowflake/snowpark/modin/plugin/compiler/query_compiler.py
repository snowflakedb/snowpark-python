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

"""
Module contains class ``BaseQueryCompiler``.

``BaseQueryCompiler`` is a parent abstract class for any other query compiler class.
"""

import abc
from collections.abc import Hashable
from typing import Any, Optional

import numpy as np
import pandas
import pandas.core.resample
from pandas._libs.lib import no_default
from pandas._typing import Axis, IndexLabel, Suffixes
from pandas.core.dtypes.common import is_scalar

from snowflake.snowpark.modin.core.dataframe.algebra.default2pandas import (
    BinaryDefault,
    CatDefault,
    DataFrameDefault,
    DateTimeDefault,
    GroupByDefault,
    ResampleDefault,
    RollingDefault,
    SeriesDefault,
    StrDefault,
)
from snowflake.snowpark.modin.plugin.compiler import doc_utils
from snowflake.snowpark.modin.plugin.utils.error_message import ErrorMessage
from snowflake.snowpark.modin.utils import (
    MODIN_UNNAMED_SERIES_LABEL,
    try_cast_to_pandas,
)


# FIXME: many of the BaseQueryCompiler methods are hiding actual arguments
# by using *args and **kwargs. They should be spread into actual parameters.
# Currently actual arguments are placed in the methods docstrings, but since they're
# not presented in the function's signature it makes linter to raise `PR02: unknown parameters`
# warning. For now, they're silenced by using `noqa` (Modin issue #3108).
class BaseQueryCompiler(abc.ABC):
    """
    Abstract class that handles the queries to Modin dataframes.

    This class defines common query compilers API, most of the methods
    are already implemented and defaulting to pandas.

    Attributes
    ----------
    _shape_hint : {"row", "column", None}, default: None
        Shape hint for frames known to be a column or a row, otherwise None.

    Notes
    -----
    See the Abstract Methods and Fields section immediately below this
    for a list of requirements for subclassing this object.
    """

    def default_to_pandas(self, pandas_op, *args, **kwargs):
        """
        Do fallback to pandas for the passed function.

        Parameters
        ----------
        pandas_op : callable(pandas.DataFrame) -> object
            Function to apply to the casted to pandas frame.
        *args : iterable
            Positional arguments to pass to `pandas_op`.
        **kwargs : dict
            Key-value arguments to pass to `pandas_op`.

        Returns
        -------
        BaseQueryCompiler
            The result of the `pandas_op`, converted back to ``BaseQueryCompiler``.
        """
        args = try_cast_to_pandas(args)
        kwargs = try_cast_to_pandas(kwargs)

        result = pandas_op(try_cast_to_pandas(self), *args, **kwargs)
        if isinstance(result, pandas.Series):
            if result.name is None:
                result.name = MODIN_UNNAMED_SERIES_LABEL
            result = result.to_frame()
        if isinstance(result, pandas.DataFrame):
            return self.from_pandas(result, type(self._modin_frame))
        else:
            return result

    # Abstract Methods and Fields: Must implement in children classes
    # In some cases, there you may be able to use the same implementation for
    # some of these abstract methods, but for the sake of generality they are
    # treated differently.

    _shape_hint = None

    # Metadata modification abstract methods
    def add_prefix(self, prefix, axis=1):
        """
        Add string prefix to the index labels along specified axis.

        Parameters
        ----------
        prefix : str
            The string to add before each label.
        axis : {0, 1}, default: 1
            Axis to add prefix along. 0 is for index and 1 is for columns.

        Returns
        -------
        BaseQueryCompiler
            New query compiler with updated labels.
        """
        if axis:
            return DataFrameDefault.register(pandas.DataFrame.add_prefix)(
                self, prefix=prefix
            )
        else:
            return SeriesDefault.register(pandas.Series.add_prefix)(self, prefix=prefix)

    def add_suffix(self, suffix, axis=1):
        """
        Add string suffix to the index labels along specified axis.

        Parameters
        ----------
        suffix : str
            The string to add after each label.
        axis : {0, 1}, default: 1
            Axis to add suffix along. 0 is for index and 1 is for columns.

        Returns
        -------
        BaseQueryCompiler
            New query compiler with updated labels.
        """
        if axis:
            return DataFrameDefault.register(pandas.DataFrame.add_suffix)(
                self, suffix=suffix
            )
        else:
            return SeriesDefault.register(pandas.Series.add_suffix)(self, suffix=suffix)

    # END Metadata modification abstract methods

    # Abstract copy

    def copy(self):
        """
        Make a copy of this object.

        Returns
        -------
        BaseQueryCompiler
            Copy of self.

        Notes
        -----
        For copy, we don't want a situation where we modify the metadata of the
        copies if we end up modifying something here. We copy all of the metadata
        to prevent that.
        """
        raise NotImplementedError  # pragma: no cover

    # END Abstract copy

    # Data Management Methods
    @abc.abstractmethod
    def free(self):
        """Trigger a cleanup of this object."""
        pass

    @abc.abstractmethod
    def finalize(self):
        """Finalize constructing the dataframe calling all deferred functions which were used to build it."""
        pass

    # END Data Management Methods

    # To/From pandas
    @abc.abstractmethod
    def to_pandas(
        self,
        *,
        statement_params: Optional[dict[str, str]] = None,
        **kwargs: Any,
    ) -> pandas.DataFrame:
        """
        Convert underlying query compilers data to ``pandas.DataFrame``.

        Args:
            statement_params: Dictionary of statement level parameters to be set while executing this action.

        Returns:
        pandas.DataFrame
            The QueryCompiler converted to pandas."""
        pass

    @classmethod
    @abc.abstractmethod
    def from_pandas(cls, df, data_cls):
        """
        Build QueryCompiler from pandas DataFrame.

        Parameters
        ----------
        df : pandas.DataFrame
            The pandas DataFrame to convert from.
        data_cls : type
            :py:class:`~modin.core.dataframe.pandas.dataframe.dataframe.PandasDataframe` class
            (or its descendant) to convert to.

        Returns
        -------
        BaseQueryCompiler
            QueryCompiler containing data from the pandas DataFrame.
        """
        pass

    # END To/From pandas

    # From Arrow
    @classmethod
    @abc.abstractmethod
    def from_arrow(cls, at, data_cls):
        """
        Build QueryCompiler from Arrow Table.

        Parameters
        ----------
        at : Arrow Table
            The Arrow Table to convert from.
        data_cls : type
            :py:class:`~modin.core.dataframe.pandas.dataframe.dataframe.PandasDataframe` class
            (or its descendant) to convert to.

        Returns
        -------
        BaseQueryCompiler
            QueryCompiler containing data from the pandas DataFrame.
        """
        pass

    # END From Arrow

    # Dataframe exchange protocol

    @abc.abstractmethod
    def to_dataframe(self, nan_as_null: bool = False, allow_copy: bool = True):
        """
        Get a DataFrame exchange protocol object representing data of the Modin DataFrame.

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
            A dataframe object following the DataFrame protocol specification.
        """
        pass

    @classmethod
    @abc.abstractmethod
    def from_dataframe(cls, df, data_cls):
        """
        Build QueryCompiler from a DataFrame object supporting the dataframe exchange protocol `__dataframe__()`.

        Parameters
        ----------
        df : DataFrame
            The DataFrame object supporting the dataframe exchange protocol.
        data_cls : type
            :py:class:`~modin.core.dataframe.pandas.dataframe.dataframe.PandasDataframe` class
            (or its descendant) to convert to.

        Returns
        -------
        BaseQueryCompiler
            QueryCompiler containing data from the DataFrame.
        """
        pass

    # END Dataframe exchange protocol

    # Abstract inter-data operations (e.g. add, sub)
    # These operations require two DataFrames and will change the shape of the
    # data if the index objects don't match. An outer join + op is performed,
    # such that columns/rows that don't have an index on the other DataFrame
    # result in NaN values.

    @doc_utils.doc_binary_method(operation="addition", sign="+")
    def add(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.add)(self, other=other, **kwargs)

    @doc_utils.add_refer_to("DataFrame.combine")
    def combine(self, other, **kwargs):  # noqa: PR02
        """
        Perform column-wise combine with another QueryCompiler with passed `func`.

        If axes are not equal, perform frames alignment first.

        Parameters
        ----------
        other : BaseQueryCompiler
            Left operand of the binary operation.
        func : callable(pandas.Series, pandas.Series) -> pandas.Series
            Function that takes two ``pandas.Series`` with aligned axes
            and returns one ``pandas.Series`` as resulting combination.
        fill_value : float or None
            Value to fill missing values with after frame alignment occurred.
        overwrite : bool
            If True, columns in `self` that do not exist in `other`
            will be overwritten with NaNs.
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        BaseQueryCompiler
            Result of combine.
        """
        return BinaryDefault.register(pandas.DataFrame.combine)(
            self, other=other, **kwargs
        )

    @doc_utils.add_refer_to("DataFrame.combine_first")
    def combine_first(self, other, **kwargs):  # noqa: PR02
        """
        Fill null elements of `self` with value in the same location in `other`.

        If axes are not equal, perform frames alignment first.

        Parameters
        ----------
        other : BaseQueryCompiler
            Provided frame to use to fill null values from.
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        BaseQueryCompiler
        """
        return BinaryDefault.register(pandas.DataFrame.combine_first)(
            self, other=other, **kwargs
        )

    @doc_utils.doc_binary_method(operation="equality comparison", sign="==")
    def eq(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.eq)(self, other=other, **kwargs)

    @doc_utils.doc_binary_method(operation="integer division", sign="//")
    def floordiv(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.floordiv)(
            self, other=other, **kwargs
        )

    @doc_utils.doc_binary_method(
        operation="greater than or equal comparison", sign=">=", op_type="comparison"
    )
    def ge(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.ge)(self, other=other, **kwargs)

    @doc_utils.doc_binary_method(
        operation="greater than comparison", sign=">", op_type="comparison"
    )
    def gt(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.gt)(self, other=other, **kwargs)

    @doc_utils.doc_binary_method(
        operation="less than or equal comparison", sign="<=", op_type="comparison"
    )
    def le(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.le)(self, other=other, **kwargs)

    @doc_utils.doc_binary_method(
        operation="less than comparison", sign="<", op_type="comparison"
    )
    def lt(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.lt)(self, other=other, **kwargs)

    @doc_utils.doc_binary_method(operation="modulo", sign="%")
    def mod(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.mod)(self, other=other, **kwargs)

    @doc_utils.doc_binary_method(operation="multiplication", sign="*")
    def mul(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.mul)(self, other=other, **kwargs)

    @doc_utils.doc_binary_method(
        operation="multiplication", sign="*", self_on_right=True
    )
    def rmul(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.rmul)(
            self, other=other, **kwargs
        )

    @doc_utils.add_refer_to("DataFrame.corr")
    def corr(self, **kwargs):  # noqa: PR02
        """
        Compute pairwise correlation of columns, excluding NA/null values.

        Parameters
        ----------
        method : {'pearson', 'kendall', 'spearman'} or callable(pandas.Series, pandas.Series) -> pandas.Series
            Correlation method.
        min_periods : int
            Minimum number of observations required per pair of columns
            to have a valid result. If fewer than `min_periods` non-NA values
            are present the result will be NA.
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        BaseQueryCompiler
            Correlation matrix.
        """
        return DataFrameDefault.register(pandas.DataFrame.corr)(self, **kwargs)

    @doc_utils.add_refer_to("DataFrame.cov")
    def cov(self, **kwargs):  # noqa: PR02
        """
        Compute pairwise covariance of columns, excluding NA/null values.

        Parameters
        ----------
        min_periods : int
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        BaseQueryCompiler
            Covariance matrix.
        """
        return DataFrameDefault.register(pandas.DataFrame.cov)(self, **kwargs)

    def dot(self, other, **kwargs):  # noqa: PR02
        """
        Compute the matrix multiplication of `self` and `other`.

        Parameters
        ----------
        other : BaseQueryCompiler or NumPy array
            The other query compiler or NumPy array to matrix multiply with `self`.
        squeeze_self : boolean
            If `self` is a one-column query compiler, indicates whether it represents Series object.
        squeeze_other : boolean
            If `other` is a one-column query compiler, indicates whether it represents Series object.
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        BaseQueryCompiler
            A new query compiler that contains result of the matrix multiply.
        """
        if kwargs.get("squeeze_self", False):
            applyier = pandas.Series.dot
        else:
            applyier = pandas.DataFrame.dot
        return BinaryDefault.register(applyier)(self, other=other, **kwargs)

    @doc_utils.doc_binary_method(
        operation="not equal comparison", sign="!=", op_type="comparison"
    )
    def ne(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.ne)(self, other=other, **kwargs)

    @doc_utils.doc_binary_method(operation="exponential power", sign="**")
    def pow(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.pow)(self, other=other, **kwargs)

    @doc_utils.doc_binary_method(operation="addition", sign="+", self_on_right=True)
    def radd(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.radd)(
            self, other=other, **kwargs
        )

    @doc_utils.doc_binary_method(
        operation="integer division", sign="//", self_on_right=True
    )
    def rfloordiv(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.rfloordiv)(
            self, other=other, **kwargs
        )

    @doc_utils.doc_binary_method(operation="modulo", sign="%", self_on_right=True)
    def rmod(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.rmod)(
            self, other=other, **kwargs
        )

    @doc_utils.doc_binary_method(
        operation="exponential power", sign="**", self_on_right=True
    )
    def rpow(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.rpow)(
            self, other=other, **kwargs
        )

    @doc_utils.doc_binary_method(operation="subtraction", sign="-", self_on_right=True)
    def rsub(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.rsub)(
            self, other=other, **kwargs
        )

    @doc_utils.doc_binary_method(operation="division", sign="/", self_on_right=True)
    def rtruediv(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.rtruediv)(
            self, other=other, **kwargs
        )

    @doc_utils.doc_binary_method(operation="subtraction", sign="-")
    def sub(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.sub)(self, other=other, **kwargs)

    @doc_utils.doc_binary_method(operation="division", sign="/")
    def truediv(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.truediv)(
            self, other=other, **kwargs
        )

    @doc_utils.doc_binary_method(operation="conjunction", sign="&", op_type="logical")
    def __and__(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.__and__)(
            self, other=other, **kwargs
        )

    @doc_utils.doc_binary_method(operation="disjunction", sign="|", op_type="logical")
    def __or__(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.__or__)(
            self, other=other, **kwargs
        )

    @doc_utils.doc_binary_method(
        operation="conjunction", sign="&", op_type="logical", self_on_right=True
    )
    def __rand__(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.__rand__)(
            self, other=other, **kwargs
        )

    @doc_utils.doc_binary_method(
        operation="disjunction", sign="|", op_type="logical", self_on_right=True
    )
    def __ror__(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.__ror__)(
            self, other=other, **kwargs
        )

    @doc_utils.doc_binary_method(
        operation="exclusive or", sign="^", op_type="logical", self_on_right=True
    )
    def __rxor__(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.__rxor__)(
            self, other=other, **kwargs
        )

    @doc_utils.doc_binary_method(operation="exclusive or", sign="^", op_type="logical")
    def __xor__(self, other, **kwargs):  # noqa: PR02
        return BinaryDefault.register(pandas.DataFrame.__xor__)(
            self, other=other, **kwargs
        )

    # FIXME: query compiler shoudln't care about differences between Frame and Series.
    # We should combine `df_update` and `series_update` into one method (Modin issue #3101).
    @doc_utils.add_refer_to("DataFrame.update")
    def df_update(self, other, **kwargs):  # noqa: PR02
        """
        Update values of `self` using non-NA values of `other` at the corresponding positions.

        If axes are not equal, perform frames alignment first.

        Parameters
        ----------
        other : BaseQueryCompiler
            Frame to grab replacement values from.
        join : {"left"}
            Specify type of join to align frames if axes are not equal
            (note: currently only one type of join is implemented).
        overwrite : bool
            Whether to overwrite every corresponding value of self, or only if it's NAN.
        filter_func : callable(pandas.Series, pandas.Series) -> numpy.ndarray<bool>
            Function that takes column of the self and return bool mask for values, that
            should be overwritten in the self frame.
        errors : {"raise", "ignore"}
            If "raise", will raise a ``ValueError`` if `self` and `other` both contain
            non-NA data in the same place.
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler with updated values.
        """
        return BinaryDefault.register(pandas.DataFrame.update, inplace=True)(
            self, other=other, **kwargs
        )

    @doc_utils.add_refer_to("Series.update")
    def series_update(self, other, **kwargs):  # noqa: PR02
        """
        Update values of `self` using values of `other` at the corresponding indices.

        Parameters
        ----------
        other : BaseQueryCompiler
            One-column query compiler with updated values.
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler with updated values.
        """
        return BinaryDefault.register(pandas.Series.update, inplace=True)(
            self,
            other=other,
            squeeze_self=True,
            **kwargs,
        )

    @doc_utils.add_refer_to("DataFrame.clip")
    def clip(self, lower, upper, **kwargs):  # noqa: PR02
        """
        Trim values at input threshold.

        Parameters
        ----------
        lower : float or list-like
        upper : float or list-like
        axis : {0, 1}
        inplace : {False}
            This parameter serves the compatibility purpose. Always has to be False.
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        BaseQueryCompiler
            QueryCompiler with values limited by the specified thresholds.
        """
        if isinstance(lower, BaseQueryCompiler):
            lower = lower.to_pandas().squeeze(1)
        if isinstance(upper, BaseQueryCompiler):
            upper = upper.to_pandas().squeeze(1)
        return DataFrameDefault.register(pandas.DataFrame.clip)(
            self, lower=lower, upper=upper, **kwargs
        )

    @doc_utils.add_refer_to("DataFrame.merge")
    def merge(self, right, **kwargs):  # noqa: PR02
        """
        Merge QueryCompiler objects using a database-style join.

        Parameters
        ----------
        right : BaseQueryCompiler
            QueryCompiler of the right frame to merge with.
        how : {"left", "right", "outer", "inner", "cross"}
        on : label or list of such
        left_on : label or list of such
        right_on : label or list of such
        left_index : bool
        right_index : bool
        sort : bool
        suffixes : list-like
        copy : bool
        indicator : bool or str
        validate : str
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        BaseQueryCompiler
            QueryCompiler that contains result of the merge.
        """
        raise NotImplementedError

    def _get_column_as_pandas_series(self, key):
        """
        Get column data by label as pandas.Series.

        Parameters
        ----------
        key : Any
            Column label.

        Returns
        -------
        pandas.Series
        """
        result = self.getitem_array([key]).to_pandas().squeeze(axis=1)
        if not isinstance(result, pandas.Series):
            raise RuntimeError(
                f"Expected getting column {key} to give "
                + f"pandas.Series, but instead got {type(result)}"
            )
        return result

    def merge_asof(
        self,
        right: "BaseQueryCompiler",
        left_on: Optional[IndexLabel] = None,
        right_on: Optional[IndexLabel] = None,
        left_index: bool = False,
        right_index: bool = False,
        left_by=None,
        right_by=None,
        suffixes: Suffixes = ("_x", "_y"),
        tolerance=None,
        allow_exact_matches: bool = True,
        direction: str = "backward",
    ):
        # pandas fallbacks for tricky cases:
        if (
            # No idea how this works or why it does what it does; and in fact
            # there's a pandas bug suggesting it's wrong:
            # https://github.com/pandas-dev/pandas/issues/33463
            (left_index and right_on is not None)
            # This is the case where by is a list of columns. If we're copying lots
            # of columns out of pandas, maybe not worth trying our path, it's not
            # clear it's any better:
            or not (left_by is None or is_scalar(left_by))
            or not (right_by is None or is_scalar(right_by))
            # The implementation below assumes that the right index is unique
            # because it uses merge_asof to map each position in the merged
            # index to the label of the one right row that should be merged
            # at that row position.
            or not right.index.is_unique
        ):
            return self.default_to_pandas(
                pandas.merge_asof,
                right,
                left_on=left_on,
                right_on=right_on,
                left_index=left_index,
                right_index=right_index,
                left_by=left_by,
                right_by=right_by,
                suffixes=suffixes,
                tolerance=tolerance,
                allow_exact_matches=allow_exact_matches,
                direction=direction,
            )

        if left_on is None:
            left_column = self.index
        else:
            left_column = self._get_column_as_pandas_series(left_on)

        if right_on is None:
            right_column = right.index
        else:
            right_column = right._get_column_as_pandas_series(right_on)

        left_pandas_limited = {"on": left_column}
        right_pandas_limited = {"on": right_column, "right_labels": right.index}
        extra_kwargs = {}  # extra arguments to pandas merge_asof # pragma: no cover

        if left_by is not None or right_by is not None:
            extra_kwargs["by"] = "by"
            left_pandas_limited["by"] = self._get_column_as_pandas_series(left_by)
            right_pandas_limited["by"] = right._get_column_as_pandas_series(right_by)

        # 1. Construct pandas DataFrames with just the 'on' and optional 'by'
        # columns, and the index as another column.
        left_pandas_limited = pandas.DataFrame(left_pandas_limited, index=self.index)
        right_pandas_limited = pandas.DataFrame(right_pandas_limited)

        # 2. Use pandas' merge_asof to figure out how to map labels on left to
        # labels on the right.
        merged = pandas.merge_asof(
            left_pandas_limited,
            right_pandas_limited,
            on="on",
            direction=direction,
            allow_exact_matches=allow_exact_matches,
            tolerance=tolerance,
            **extra_kwargs,
        )
        # Now merged["right_labels"] shows which labels from right map to left's index.

        # 3. Re-index right using the merged["right_labels"]; at this point right
        # should be same length and (semantically) same order as left:
        right_subset = right.reindex(
            axis=0, labels=pandas.Index(merged["right_labels"])
        )
        if not right_index:
            right_subset = right_subset.drop(columns=[right_on])
        if right_by is not None and left_by == right_by:
            right_subset = right_subset.drop(columns=[right_by])
        right_subset.index = self.index

        # 4. Merge left and the new shrunken right:
        result = self.merge(
            right_subset,
            left_index=True,
            right_index=True,
            suffixes=suffixes,
            how="left",
        )

        # 5. Clean up to match pandas output:
        if left_on is not None and right_index:
            result = result.insert(
                # In theory this could use get_indexer_for(), but that causes an error:
                list(result.columns).index(left_on + suffixes[0]),
                left_on,
                result.getitem_array([left_on + suffixes[0]]),
            )
        if not left_index and not right_index:
            result = result.reset_index(drop=True)

        return result

    # END Abstract inter-data operations

    def is_series_like(self):
        raise NotImplementedError  # pragma: no cover

    # END Abstract Transpose

    # Abstract reindex/reset_index (may shuffle data)
    @doc_utils.add_refer_to("DataFrame.reindex")
    def reindex(self, axis, labels, **kwargs):  # noqa: PR02
        """
        Align QueryCompiler data with a new index along specified axis.

        Parameters
        ----------
        axis : {0, 1}
            Axis to align labels along. 0 is for index, 1 is for columns.
        labels : list-like
            Index-labels to align with.
        method : {None, "backfill"/"bfill", "pad"/"ffill", "nearest"}
            Method to use for filling holes in reindexed frame.
        fill_value : scalar
            Value to use for missing values in the resulted frame.
        limit : int
        tolerance : int
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        BaseQueryCompiler
            QueryCompiler with aligned axis.
        """
        return DataFrameDefault.register(pandas.DataFrame.reindex)(
            self, axis=axis, labels=labels, **kwargs
        )

    @doc_utils.add_refer_to("DataFrame.reset_index")
    def reset_index(self, **kwargs):  # noqa: PR02
        """
        Reset the index, or a level of it.

        Parameters
        ----------
        drop : bool
            Whether to drop the reset index or insert it at the beginning of the frame.
        level : int or label, optional
            Level to remove from index. Removes all levels by default.
        col_level : int or label
            If the columns have multiple levels, determines which level the labels
            are inserted into.
        col_fill : label
            If the columns have multiple levels, determines how the other levels
            are named.
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        BaseQueryCompiler
            QueryCompiler with reset index.
        """
        return DataFrameDefault.register(pandas.DataFrame.reset_index)(self, **kwargs)

    # END Abstract reindex/reset_index

    # Full Reduce operations
    #
    # These operations result in a reduced dimensionality of data.
    # Currently, this means a pandas Series will be returned, but in the future
    # we will implement a Distributed Series, and this will be returned
    # instead.

    def is_monotonic_increasing(self):
        """
        Return boolean if values in the object are monotonically increasing.

        Returns
        -------
        bool
        """
        return SeriesDefault.register(pandas.Series.is_monotonic_increasing)(self)

    def is_monotonic_decreasing(self):
        """
        Return boolean if values in the object are monotonically decreasing.

        Returns
        -------
        bool
        """
        return SeriesDefault.register(pandas.Series.is_monotonic_decreasing)(self)

    @doc_utils.doc_reduce_agg(
        method="production",
        refer_to="prod",
        extra_params=["**kwargs"],
        params="axis : {0, 1}",
    )
    def prod(self, **kwargs):  # noqa: PR02
        return DataFrameDefault.register(pandas.DataFrame.prod)(self, **kwargs)

    # END Abstract full Reduce operations

    # Abstract map partitions operations
    # These operations are operations that apply a function to every partition.
    def abs(self):
        """
        Get absolute numeric value of each element.

        Returns
        -------
        BaseQueryCompiler
            QueryCompiler with absolute numeric value of each element.
        """
        return DataFrameDefault.register(pandas.DataFrame.abs)(self)

    # FIXME: `**kwargs` which follows `numpy.conj` signature was inherited
    # from ``PandasQueryCompiler``, we should get rid of this dependency.
    # (Modin issue #3108)
    def conj(self, **kwargs):
        """
        Get the complex conjugate for every element of self.

        Parameters
        ----------
        **kwargs : dict

        Returns
        -------
        BaseQueryCompiler
            QueryCompiler with conjugate applied element-wise.

        Notes
        -----
        Please refer to ``numpy.conj`` for parameters description.
        """

        def conj(df, *args, **kwargs):
            return pandas.DataFrame(np.conj(df))

        return DataFrameDefault.register(conj)(self, **kwargs)

    # FIXME: this method is not supposed to take any parameters (Modin issue #3108).
    def negative(self, **kwargs):
        """
        Change the sign for every value of self.

        Parameters
        ----------
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        BaseQueryCompiler

        Notes
        -----
        Be aware, that all QueryCompiler values have to be numeric.
        """
        return DataFrameDefault.register(pandas.DataFrame.__neg__)(self, **kwargs)

    @doc_utils.add_one_column_warning
    # FIXME: adding refer-to note will create two instances of the "Notes" section,
    # this breaks numpydoc style rules and also crashes the doc-style checker script.
    # For now manually added the refer-to message.
    # @doc_utils.add_refer_to("Series.view")
    def series_view(self, **kwargs):  # noqa: PR02
        """
        Reinterpret underlying data with new dtype.

        Parameters
        ----------
        dtype : dtype
            Data type to reinterpret underlying data with.
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler of the same data in memory, with reinterpreted values.

        Notes
        -----
            - Be aware, that if this method do fallback to pandas, then newly created
              QueryCompiler will be the copy of the original data.
            - Please refer to ``modin.pandas.Series.view`` for more information
              about parameters and output format.
        """
        return SeriesDefault.register(pandas.Series.view)(self, **kwargs)

    @doc_utils.add_one_column_warning
    @doc_utils.add_refer_to("to_timedelta")
    def to_timedelta(self, unit="ns", errors="raise"):  # noqa: PR02
        """
        Convert argument to timedelta.

        Parameters
        ----------
        unit : str, default: "ns"
            Denotes the unit of the arg for numeric arg. Defaults to "ns".
        errors : {"ignore", "raise", "coerce"}, default: "raise"

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler with converted to timedelta values.
        """
        return SeriesDefault.register(pandas.to_timedelta)(
            self, unit=unit, errors=errors
        )

    @doc_utils.add_one_column_warning
    @doc_utils.add_refer_to("Series.searchsorted")
    def searchsorted(self, **kwargs):  # noqa: PR02
        """
        Find positions in a sorted `self` where `value` should be inserted to maintain order.

        Parameters
        ----------
        value : list-like
        side : {"left", "right"}
        sorter : list-like, optional
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        BaseQueryCompiler
            One-column QueryCompiler which contains indices to insert.
        """
        return SeriesDefault.register(pandas.Series.searchsorted)(self, **kwargs)

    # END Abstract map partitions operations

    @doc_utils.add_refer_to("DataFrame.stack")
    def stack(self, level, dropna):
        """
        Stack the prescribed level(s) from columns to index.

        Parameters
        ----------
        level : int or label
        dropna : bool

        Returns
        -------
        BaseQueryCompiler
        """
        return DataFrameDefault.register(pandas.DataFrame.stack)(
            self, level=level, dropna=dropna
        )

    def infer_objects(self):
        """
        Attempt to infer better dtypes for object columns.

        Attempts soft conversion of object-dtyped columns, leaving non-object
        and unconvertible columns unchanged. The inference rules are the same
        as during normal Series/DataFrame construction.

        Returns
        -------
        BaseQueryCompiler
            New query compiler with udpated dtypes.
        """
        return DataFrameDefault.register(pandas.DataFrame.infer_objects)(self)

    @property
    def dtypes(self):
        """
        Get columns dtypes.

        Returns
        -------
        pandas.Series
            Series with dtypes of each column.
        """
        return self.to_pandas().dtypes

    # END Abstract map partitions across select indices

    # Abstract column/row partitions reduce operations
    #
    # These operations result in a reduced dimensionality of data.
    # Currently, this means a pandas Series will be returned, but in the future
    # we will implement a Distributed Series, and this will be returned
    # instead.

    # FIXME: we're handling level parameter at front-end, it shouldn't
    # propagate to the query compiler (Modin issue #3102)
    @doc_utils.add_refer_to("DataFrame.all")
    def all(self, **kwargs):  # noqa: PR02
        """
        Return whether all the elements are true, potentially over an axis.

        Parameters
        ----------
        axis : {0, 1}, optional
        bool_only : bool, optional
        skipna : bool
        level : int or label
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        BaseQueryCompiler
            If axis was specified return one-column QueryCompiler with index labels
            of the specified axis, where each row contains boolean of whether all elements
            at the corresponding row or column are True. Otherwise return QueryCompiler
            with a single bool of whether all elements are True.
        """
        return DataFrameDefault.register(pandas.DataFrame.all)(self, **kwargs)

    @doc_utils.add_refer_to("DataFrame.any")
    def any(self, **kwargs):  # noqa: PR02
        """
        Return whether any element is true, potentially over an axis.

        Parameters
        ----------
        axis : {0, 1}, optional
        bool_only : bool, optional
        skipna : bool
        level : int or label
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        BaseQueryCompiler
            If axis was specified return one-column QueryCompiler with index labels
            of the specified axis, where each row contains boolean of whether any element
            at the corresponding row or column is True. Otherwise return QueryCompiler
            with a single bool of whether any element is True.
        """
        return DataFrameDefault.register(pandas.DataFrame.any)(self, **kwargs)

    @doc_utils.add_refer_to("DataFrame.memory_usage")
    def memory_usage(self, **kwargs):  # noqa: PR02
        """
        Return the memory usage of each column in bytes.

        Parameters
        ----------
        index : bool
        deep : bool
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        BaseQueryCompiler
            One-column QueryCompiler with index labels of `self`, where each row
            contains the memory usage for the corresponding column.
        """
        return DataFrameDefault.register(pandas.DataFrame.memory_usage)(self, **kwargs)

    @doc_utils.doc_reduce_agg(
        method="value at the given quantile",
        refer_to="quantile",
        params="""
        q : float
        axis : {0, 1}
        numeric_only : bool
        interpolation : {"linear", "lower", "higher", "midpoint", "nearest"}""",
        extra_params=["**kwargs"],
    )
    def quantile_for_single_value(self, **kwargs):  # noqa: PR02
        return DataFrameDefault.register(pandas.DataFrame.quantile)(self, **kwargs)

    @doc_utils.doc_reduce_agg(
        method="unbiased skew", refer_to="skew", extra_params=["skipna", "**kwargs"]
    )
    def skew(self, **kwargs):  # noqa: PR02
        return DataFrameDefault.register(pandas.DataFrame.skew)(self, **kwargs)

    @doc_utils.doc_reduce_agg(
        method="standard deviation of the mean",
        refer_to="sem",
        extra_params=["skipna", "ddof", "**kwargs"],
    )
    def sem(self, **kwargs):  # noqa: PR02
        return DataFrameDefault.register(pandas.DataFrame.sem)(self, **kwargs)

    # END Abstract column/row partitions reduce operations

    # Abstract column/row partitions reduce operations over select indices
    #
    # These operations result in a reduced dimensionality of data.
    # Currently, this means a pandas Series will be returned, but in the future
    # we will implement a Distributed Series, and this will be returned
    # instead.
    @doc_utils.add_refer_to("DataFrame.describe")
    def describe(self, **kwargs):  # noqa: PR02
        """
        Generate descriptive statistics.

        Parameters
        ----------
        percentiles : list-like
        include : "all" or list of dtypes, optional
        exclude : list of dtypes, optional
        datetime_is_numeric : bool
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        BaseQueryCompiler
            QueryCompiler object containing the descriptive statistics
            of the underlying data.
        """
        return DataFrameDefault.register(pandas.DataFrame.describe)(self, **kwargs)

    # END Abstract column/row partitions reduce operations over select indices

    # Map across rows/columns
    # These operations require some global knowledge of the full column/row
    # that is being operated on. This means that we have to put all of that
    # data in the same place.

    @doc_utils.doc_cum_agg(method="sum", refer_to="cumsum")
    def cumsum(self, fold_axis, **kwargs):  # noqa: PR02
        return DataFrameDefault.register(pandas.DataFrame.cumsum)(self, **kwargs)

    @doc_utils.doc_cum_agg(method="maximum", refer_to="cummax")
    def cummax(self, fold_axis, **kwargs):  # noqa: PR02
        return DataFrameDefault.register(pandas.DataFrame.cummax)(self, **kwargs)

    @doc_utils.doc_cum_agg(method="minimum", refer_to="cummin")
    def cummin(self, fold_axis, **kwargs):  # noqa: PR02
        return DataFrameDefault.register(pandas.DataFrame.cummin)(self, **kwargs)

    @doc_utils.doc_cum_agg(method="product", refer_to="cumprod")
    def cumprod(self, fold_axis, **kwargs):  # noqa: PR02
        return DataFrameDefault.register(pandas.DataFrame.cumprod)(self, **kwargs)

    @doc_utils.add_refer_to("DataFrame.diff")
    def diff(self, fold_axis, **kwargs):  # noqa: PR02
        """
        First discrete difference of element.

        Parameters
        ----------
        periods : int
        fold_axis : {0, 1}
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        BaseQueryCompiler
            QueryCompiler of the same shape as `self`, where each element is the difference
            between the corresponding value and the previous value in this row or column.
        """
        return DataFrameDefault.register(pandas.DataFrame.diff)(self, **kwargs)

    @doc_utils.add_refer_to("DataFrame.nlargest")
    def nlargest(self, n=5, columns=None, keep="first"):
        """
        Return the first `n` rows ordered by `columns` in descending order.

        Parameters
        ----------
        n : int, default: 5
        columns : list of labels, optional
            Column labels to order by.
            (note: this parameter can be omitted only for a single-column query compilers
            representing Series object, otherwise `columns` has to be specified).
        keep : {"first", "last", "all"}, default: "first"

        Returns
        -------
        BaseQueryCompiler
        """
        if columns is None:
            return SeriesDefault.register(pandas.Series.nlargest)(self, n=n, keep=keep)
        else:
            return DataFrameDefault.register(pandas.DataFrame.nlargest)(
                self, n=n, columns=columns, keep=keep
            )

    @doc_utils.add_refer_to("DataFrame.nsmallest")
    def nsmallest(self, n=5, columns=None, keep="first"):
        """
        Return the first `n` rows ordered by `columns` in ascending order.

        Parameters
        ----------
        n : int, default: 5
        columns : list of labels, optional
            Column labels to order by.
            (note: this parameter can be omitted only for a single-column query compilers
            representing Series object, otherwise `columns` has to be specified).
        keep : {"first", "last", "all"}, default: "first"

        Returns
        -------
        BaseQueryCompiler
        """
        if columns is None:
            return SeriesDefault.register(pandas.Series.nsmallest)(self, n=n, keep=keep)
        else:
            return DataFrameDefault.register(pandas.DataFrame.nsmallest)(
                self, n=n, columns=columns, keep=keep
            )

    @doc_utils.add_refer_to("DataFrame.eval")
    def eval(self, expr, **kwargs):
        """
        Evaluate string expression on QueryCompiler columns.

        Parameters
        ----------
        expr : str
        **kwargs : dict

        Returns
        -------
        BaseQueryCompiler
            QueryCompiler containing the result of evaluation.
        """
        return DataFrameDefault.register(pandas.DataFrame.eval)(
            self, expr=expr, **kwargs
        )

    @doc_utils.add_refer_to("DataFrame.mode")
    def mode(self, **kwargs):  # noqa: PR02
        """
        Get the modes for every column or row.

        Parameters
        ----------
        axis : {0, 1}
        numeric_only : bool
        dropna : bool
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler with modes calculated along given axis.
        """
        return DataFrameDefault.register(pandas.DataFrame.mode)(self, **kwargs)

    @doc_utils.add_refer_to("DataFrame.query")
    def query(self, expr, **kwargs):
        """
        Query columns of the QueryCompiler with a boolean expression.

        Parameters
        ----------
        expr : str
        **kwargs : dict

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler containing the rows where the boolean expression is satisfied.
        """
        return DataFrameDefault.register(pandas.DataFrame.query)(
            self, expr=expr, **kwargs
        )

    @doc_utils.add_refer_to("DataFrame.rank")
    def rank(self, **kwargs):  # noqa: PR02
        """
        Compute numerical rank along the specified axis.

        By default, equal values are assigned a rank that is the average of the ranks
        of those values, this behavior can be changed via `method` parameter.

        Parameters
        ----------
        axis : {0, 1}
        method : {"average", "min", "max", "first", "dense"}
        numeric_only : bool
        na_option : {"keep", "top", "bottom"}
        ascending : bool
        pct : bool
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        BaseQueryCompiler
            QueryCompiler of the same shape as `self`, where each element is the
            numerical rank of the corresponding value along row or column.
        """
        return DataFrameDefault.register(pandas.DataFrame.rank)(self, **kwargs)

    @doc_utils.add_refer_to("DataFrame.melt")
    def melt(self, *args, **kwargs):  # noqa: PR02
        """
        Unpivot QueryCompiler data from wide to long format.

        Parameters
        ----------
        id_vars : list of labels, optional
        value_vars : list of labels, optional
        var_name : label
        value_name : label
        col_level : int or label
        ignore_index : bool
        *args : iterable
            Serves the compatibility purpose. Does not affect the result.
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler with unpivoted data.
        """
        return DataFrameDefault.register(pandas.DataFrame.melt)(self, *args, **kwargs)

    @doc_utils.add_refer_to("DataFrame.sort_values")
    def sort_columns_by_row_values(self, rows, ascending=True, **kwargs):  # noqa: PR02
        """
        Reorder the columns based on the lexicographic order of the given rows.

        Parameters
        ----------
        rows : label or list of labels
            The row or rows to sort by.
        ascending : bool, default: True
            Sort in ascending order (True) or descending order (False).
        kind : {"quicksort", "mergesort", "heapsort"}
        na_position : {"first", "last"}
        ignore_index : bool
        key : callable(pandas.Index) -> pandas.Index, optional
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler that contains result of the sort.
        """
        return DataFrameDefault.register(pandas.DataFrame.sort_values)(
            self, by=rows, axis=1, ascending=ascending, **kwargs
        )

    # END Abstract map across rows/columns

    # Map across rows/columns
    # These operations require some global knowledge of the full column/row
    # that is being operated on. This means that we have to put all of that
    # data in the same place.
    @doc_utils.doc_reduce_agg(
        method="value at the given quantile",
        refer_to="quantile",
        params="""
        q : list-like
        axis : {0, 1}
        numeric_only : bool
        interpolation : {"linear", "lower", "higher", "midpoint", "nearest"}""",
        extra_params=["**kwargs"],
    )
    def quantile_for_list_of_values(self, **kwargs):  # noqa: PR02
        return DataFrameDefault.register(pandas.DataFrame.quantile)(self, **kwargs)

    # END Abstract map across rows/columns

    # Abstract __getitem__ methods
    def getitem_array(self, key):
        """
        Mask QueryCompiler with `key`.

        Parameters
        ----------
        key : BaseQueryCompiler, np.ndarray or list of column labels
            Boolean mask represented by QueryCompiler or ``np.ndarray`` of the same
            shape as `self`, or enumerable of columns to pick.

        Returns
        -------
        BaseQueryCompiler
            New masked QueryCompiler.
        """
        if isinstance(key, type(self)):
            key = key.to_pandas().squeeze(axis=1)

        def getitem_array(df, key):
            return df[key]

        return DataFrameDefault.register(getitem_array)(self, key)

    # END Abstract __getitem__ methods

    # Abstract insert
    # This method changes the shape of the resulting data. In pandas, this
    # operation is always inplace, but this object is immutable, so we just
    # return a new one from here and let the front end handle the inplace
    # update.
    def insert(self, loc, column, value):
        """
        Insert new column.

        Parameters
        ----------
        loc : int
            Insertion position.
        column : label
            Label of the new column.
        value : One-column BaseQueryCompiler, 1D array or scalar
            Data to fill new column with.

        Returns
        -------
        BaseQueryCompiler
            QueryCompiler with new column inserted.
        """
        raise NotImplementedError

    # END Abstract insert

    def explode(self, column):
        """
        Explode the given columns.

        Parameters
        ----------
        column : Union[Hashable, Sequence[Hashable]]
            The columns to explode.

        Returns
        -------
        BaseQueryCompiler
            QueryCompiler that contains the results of execution. For each row
            in the input QueryCompiler, if the selected columns each contain M
            items, there will be M rows created by exploding the columns.
        """
        return DataFrameDefault.register(pandas.DataFrame.explode)(self, column)

    # END UDF

    # Manual Partitioning methods (e.g. merge, groupby)
    # These methods require some sort of manual partitioning due to their
    # nature. They require certain data to exist on the same partition, and
    # after the shuffle, there should be only a local map required.

    # FIXME: `map_args` and `reduce_args` leaked there from `PandasQueryCompiler.groupby_*`,
    # pandas storage format implements groupby via TreeReduce approach, but for other storage formats these
    # parameters make no sense, they shouldn't be present in a base class.

    @doc_utils.doc_groupby_method(
        action="count non-null values",
        result="number of non-null values",
        refer_to="count",
    )
    def groupby_count(
        self,
        by,
        axis,
        groupby_kwargs,
        agg_args,
        agg_kwargs,
        drop=False,
    ):
        return GroupByDefault.register(pandas.core.groupby.DataFrameGroupBy.count)(
            self,
            by=by,
            axis=axis,
            groupby_kwargs=groupby_kwargs,
            agg_args=agg_args,
            agg_kwargs=agg_kwargs,
            drop=drop,
        )

    @doc_utils.doc_groupby_method(
        action="check whether any element is True",
        result="boolean of whether there is any element which is True",
        refer_to="any",
    )
    def groupby_any(
        self,
        by,
        axis,
        groupby_kwargs,
        agg_args,
        agg_kwargs,
        drop=False,
    ):
        return GroupByDefault.register(pandas.core.groupby.DataFrameGroupBy.any)(
            self,
            by=by,
            axis=axis,
            groupby_kwargs=groupby_kwargs,
            agg_args=agg_args,
            agg_kwargs=agg_kwargs,
            drop=drop,
        )

    @doc_utils.doc_groupby_method(
        action="get the minimum value", result="minimum value", refer_to="min"
    )
    def groupby_min(
        self,
        by,
        axis,
        groupby_kwargs,
        agg_args,
        agg_kwargs,
        drop=False,
    ):
        return GroupByDefault.register(pandas.core.groupby.DataFrameGroupBy.min)(
            self,
            by=by,
            axis=axis,
            groupby_kwargs=groupby_kwargs,
            agg_args=agg_args,
            agg_kwargs=agg_kwargs,
            drop=drop,
        )

    @doc_utils.doc_groupby_method(result="product", refer_to="prod")
    def groupby_prod(
        self,
        by,
        axis,
        groupby_kwargs,
        agg_args,
        agg_kwargs,
        drop=False,
    ):
        return GroupByDefault.register(pandas.core.groupby.DataFrameGroupBy.prod)(
            self,
            by=by,
            axis=axis,
            groupby_kwargs=groupby_kwargs,
            agg_args=agg_args,
            agg_kwargs=agg_kwargs,
            drop=drop,
        )

    @doc_utils.doc_groupby_method(
        action="get the maximum value", result="maximum value", refer_to="max"
    )
    def groupby_max(
        self,
        by,
        axis,
        groupby_kwargs,
        agg_args,
        agg_kwargs,
        drop=False,
    ):
        return GroupByDefault.register(pandas.core.groupby.DataFrameGroupBy.max)(
            self,
            by=by,
            axis=axis,
            groupby_kwargs=groupby_kwargs,
            agg_args=agg_args,
            agg_kwargs=agg_kwargs,
            drop=drop,
        )

    @doc_utils.doc_groupby_method(
        action="check whether all elements are True",
        result="boolean of whether all elements are True",
        refer_to="all",
    )
    def groupby_all(
        self,
        by,
        axis,
        groupby_kwargs,
        agg_args,
        agg_kwargs,
        drop=False,
    ):
        return GroupByDefault.register(pandas.core.groupby.DataFrameGroupBy.all)(
            self,
            by=by,
            axis=axis,
            groupby_kwargs=groupby_kwargs,
            agg_args=agg_args,
            agg_kwargs=agg_kwargs,
            drop=drop,
        )

    @doc_utils.doc_groupby_method(result="sum", refer_to="sum")
    def groupby_sum(
        self,
        by,
        axis,
        groupby_kwargs,
        agg_args,
        agg_kwargs,
        drop=False,
    ):
        return GroupByDefault.register(pandas.core.groupby.DataFrameGroupBy.sum)(
            self,
            by=by,
            axis=axis,
            groupby_kwargs=groupby_kwargs,
            agg_args=agg_args,
            agg_kwargs=agg_kwargs,
            drop=drop,
        )

    @doc_utils.doc_groupby_method(
        action="get the number of elements",
        result="number of elements",
        refer_to="size",
    )
    def groupby_size(
        self,
        by,
        axis,
        groupby_kwargs,
        agg_args,
        agg_kwargs,
        drop=False,
    ):
        result = GroupByDefault.register(pandas.core.groupby.DataFrameGroupBy.size)(
            self,
            by=by,
            axis=axis,
            groupby_kwargs=groupby_kwargs,
            agg_args=agg_args,
            agg_kwargs=agg_kwargs,
            drop=drop,
            method="size",
        )
        if not groupby_kwargs.get("as_index", False):
            # Renaming 'MODIN_UNNAMED_SERIES_LABEL' to a proper name
            result.columns = result.columns[:-1].append(pandas.Index(["size"]))
        return result

    @doc_utils.add_refer_to("GroupBy.aggregate")
    def groupby_agg(
        self,
        by,
        agg_func,
        axis,
        groupby_kwargs,
        agg_args,
        agg_kwargs,
        how="axis_wise",
        drop=False,
    ):
        raise NotImplementedError  # pragma: no cover

    @doc_utils.doc_groupby_method(
        action="compute the mean value", result="mean value", refer_to="mean"
    )
    def groupby_mean(
        self,
        by,
        axis,
        groupby_kwargs,
        agg_args,
        agg_kwargs,
        drop=False,
    ):
        return self.groupby_agg(
            by=by,
            agg_func="mean",
            axis=axis,
            groupby_kwargs=groupby_kwargs,
            agg_args=agg_args,
            agg_kwargs=agg_kwargs,
            drop=drop,
        )

    @doc_utils.doc_groupby_method(
        action="compute unbiased skew", result="unbiased skew", refer_to="skew"
    )
    def groupby_skew(
        self,
        by,
        axis,
        groupby_kwargs,
        agg_args,
        agg_kwargs,
        drop=False,
    ):
        return self.groupby_agg(
            by=by,
            agg_func="skew",
            axis=axis,
            groupby_kwargs=groupby_kwargs,
            agg_args=agg_args,
            agg_kwargs=agg_kwargs,
            drop=drop,
        )

    @doc_utils.doc_groupby_method(
        action="get cumulative production",
        result="production of all the previous values",
        refer_to="cumprod",
    )
    def groupby_cumprod(
        self,
        by,
        axis,
        groupby_kwargs,
        agg_args,
        agg_kwargs,
        drop=False,
    ):
        return self.groupby_agg(
            by=by,
            agg_func="cumprod",
            axis=axis,
            groupby_kwargs=groupby_kwargs,
            agg_args=agg_args,
            agg_kwargs=agg_kwargs,
            drop=drop,
        )

    @doc_utils.doc_groupby_method(
        action="compute standard deviation", result="standard deviation", refer_to="std"
    )
    def groupby_std(
        self,
        by,
        axis,
        groupby_kwargs,
        agg_args,
        agg_kwargs,
        drop=False,
    ):
        return self.groupby_agg(
            by=by,
            agg_func="std",
            axis=axis,
            groupby_kwargs=groupby_kwargs,
            agg_args=agg_args,
            agg_kwargs=agg_kwargs,
            drop=drop,
        )

    @doc_utils.doc_groupby_method(
        action="compute numerical rank", result="numerical rank", refer_to="rank"
    )
    def groupby_rank(
        self,
        by,
        axis,
        groupby_kwargs,
        agg_args,
        agg_kwargs,
        drop=False,
    ):
        return self.groupby_agg(
            by=by,
            agg_func="rank",
            axis=axis,
            groupby_kwargs=groupby_kwargs,
            agg_args=agg_args,
            agg_kwargs=agg_kwargs,
            drop=drop,
        )

    @doc_utils.doc_groupby_method(
        action="compute variance", result="variance", refer_to="var"
    )
    def groupby_var(
        self,
        by,
        axis,
        groupby_kwargs,
        agg_args,
        agg_kwargs,
        drop=False,
    ):
        return self.groupby_agg(
            by=by,
            agg_func="var",
            axis=axis,
            groupby_kwargs=groupby_kwargs,
            agg_args=agg_args,
            agg_kwargs=agg_kwargs,
            drop=drop,
        )

    @doc_utils.doc_groupby_method(
        action="get the number of unique values",
        result="number of unique values",
        refer_to="nunique",
    )
    def groupby_nunique(
        self,
        by,
        axis,
        groupby_kwargs,
        agg_args,
        agg_kwargs,
        drop=False,
    ):
        return self.groupby_agg(
            by=by,
            agg_func="nunique",
            axis=axis,
            groupby_kwargs=groupby_kwargs,
            agg_args=agg_args,
            agg_kwargs=agg_kwargs,
            drop=drop,
        )

    @doc_utils.doc_groupby_method(
        action="get the median value", result="median value", refer_to="median"
    )
    def groupby_median(
        self,
        by,
        axis,
        groupby_kwargs,
        agg_args,
        agg_kwargs,
        drop=False,
    ):
        return self.groupby_agg(
            by=by,
            agg_func="median",
            axis=axis,
            groupby_kwargs=groupby_kwargs,
            agg_args=agg_args,
            agg_kwargs=agg_kwargs,
            drop=drop,
        )

    @doc_utils.doc_groupby_method(
        action="compute specified quantile",
        result="quantile value",
        refer_to="quantile",
    )
    def groupby_quantile(
        self,
        by,
        axis,
        groupby_kwargs,
        agg_args,
        agg_kwargs,
        drop=False,
    ):
        raise NotImplementedError  # pragma: no cover

    @doc_utils.doc_groupby_method(
        action="fill NaN values",
        result="`fill_value` if it was NaN, original value otherwise",
        refer_to="fillna",
    )
    def groupby_fillna(
        self,
        by,
        axis,
        groupby_kwargs,
        agg_args,
        agg_kwargs,
        drop=False,
    ):
        return self.groupby_agg(
            by=by,
            agg_func="fillna",
            axis=axis,
            groupby_kwargs=groupby_kwargs,
            agg_args=agg_args,
            agg_kwargs=agg_kwargs,
            drop=drop,
        )

    @doc_utils.doc_groupby_method(
        action="get data types", result="data type", refer_to="dtypes"
    )
    def groupby_dtypes(
        self,
        by,
        axis,
        groupby_kwargs,
        agg_args,
        agg_kwargs,
        drop=False,
    ):
        return self.groupby_agg(
            by=by,
            agg_func="dtypes",
            axis=axis,
            groupby_kwargs=groupby_kwargs,
            agg_args=agg_args,
            agg_kwargs=agg_kwargs,
            drop=drop,
        )

    @doc_utils.doc_groupby_method(
        action="shift data with the specified settings",
        result="shifted value",
        refer_to="shift",
    )
    def groupby_shift(
        self,
        by,
        axis,
        groupby_kwargs,
        agg_args,
        agg_kwargs,
        drop=False,
    ):
        return self.groupby_agg(
            by=by,
            agg_func="shift",
            axis=axis,
            groupby_kwargs=groupby_kwargs,
            agg_args=agg_args,
            agg_kwargs=agg_kwargs,
            drop=drop,
        )

    # END Manual Partitioning methods

    @doc_utils.add_refer_to("DataFrame.unstack")
    def unstack(self, level, fill_value):
        """
        Pivot a level of the (necessarily hierarchical) index labels.

        Parameters
        ----------
        level : int or label
        fill_value : scalar or dict

        Returns
        -------
        BaseQueryCompiler
        """
        return DataFrameDefault.register(pandas.DataFrame.unstack)(
            self, level=level, fill_value=fill_value
        )

    @doc_utils.add_refer_to("DataFrame.pivot")
    def pivot(self, index, columns, values):
        """
        Produce pivot table based on column values.

        Parameters
        ----------
        index : label or list of such, pandas.Index, optional
        columns : label or list of such
        values : label or list of such, optional

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler containing pivot table.
        """
        return DataFrameDefault.register(pandas.DataFrame.pivot)(
            self, index=index, columns=columns, values=values
        )

    @doc_utils.add_refer_to("DataFrame.pivot_table")
    def pivot_table(
        self,
        index,
        values,
        columns,
        aggfunc,
        fill_value,
        margins,
        dropna,
        margins_name,
        observed,
        sort,
    ):
        """
        Create a spreadsheet-style pivot table from underlying data.

        Parameters
        ----------
        index : label, pandas.Grouper, array or list of such
        values : label, optional
        columns : column, pandas.Grouper, array or list of such
        aggfunc : callable(pandas.Series) -> scalar, dict of list of such
        fill_value : scalar, optional
        margins : bool
        dropna : bool
        margins_name : str
        observed : bool
        sort : bool

        Returns
        -------
        BaseQueryCompiler
        """
        return DataFrameDefault.register(pandas.DataFrame.pivot_table)(
            self,
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

    @doc_utils.add_refer_to("get_dummies")
    def get_dummies(self, columns, **kwargs):  # noqa: PR02
        """
        Convert categorical variables to dummy variables for certain columns.

        Parameters
        ----------
        columns : label or list of such
            Columns to convert.
        prefix : str or list of such
        prefix_sep : str
        dummy_na : bool
        drop_first : bool
        dtype : dtype
        **kwargs : dict
            Serves the compatibility purpose. Does not affect the result.

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler with categorical variables converted to dummy.
        """

        def get_dummies(df, columns, **kwargs):
            return pandas.get_dummies(df, columns=columns, **kwargs)

        return DataFrameDefault.register(get_dummies)(self, columns=columns, **kwargs)

    @doc_utils.add_one_column_warning
    @doc_utils.add_refer_to("Series.repeat")
    def repeat(self, repeats):
        """
        Repeat each element of one-column QueryCompiler given number of times.

        Parameters
        ----------
        repeats : int or array of ints
            The number of repetitions for each element. This should be a
            non-negative integer. Repeating 0 times will return an empty
            QueryCompiler.

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler with repeated elements.
        """
        return SeriesDefault.register(pandas.Series.repeat)(self, repeats=repeats)

    def get_axis(self, axis):
        """
        Return index labels of the specified axis.

        Parameters
        ----------
        axis : {0, 1}
            Axis to return labels on.
            0 is for index, when 1 is for columns.

        Returns
        -------
        pandas.Index
        """
        return self.index if axis == 0 else self.columns

    # TODO SNOW-884220: deprecate this function when loc getitem is supported.
    # Note: reference the latest modin when deprecating this function.
    def get_positions_from_labels(self, row_loc, col_loc):
        """
        Compute index and column positions from their respective locators.

        Inputs to this method are arguments the the pandas user could pass to loc.
        This function will compute the corresponding index and column positions
        that the user could equivalently pass to iloc.

        Parameters
        ----------
        row_loc : scalar, slice, list, array or tuple
            Row locator.
        col_loc : scalar, slice, list, array or tuple
            Columns locator.

        Returns
        -------
        row_lookup : slice(None) if full axis grab, pandas.RangeIndex if repetition is detected, numpy.ndarray otherwise
            List of index labels.
        col_lookup : slice(None) if full axis grab, pandas.RangeIndex if repetition is detected, numpy.ndarray otherwise
            List of columns labels.

        Notes
        -----
        Usage of `slice(None)` as a resulting lookup is a hack to pass information about
        full-axis grab without computing actual indices that triggers lazy computations.
        Ideally, this API should get rid of using slices as indexers and either use a
        common ``Indexer`` object or range and ``np.ndarray`` only.
        """
        from snowflake.snowpark.modin.pandas.indexing import (
            boolean_mask_to_numeric,
            is_boolean_array,
            is_list_like,
            is_range_like,
        )

        lookups = []
        for axis, axis_loc in enumerate((row_loc, col_loc)):
            if is_scalar(axis_loc):
                axis_loc = np.array([axis_loc])
            if isinstance(axis_loc, slice) or is_range_like(axis_loc):
                if isinstance(axis_loc, slice) and axis_loc == slice(None):
                    axis_lookup = axis_loc
                else:
                    axis_labels = self.get_axis(axis)
                    # `slice_indexer` returns a fully-defined numeric slice for a non-fully-defined labels-based slice
                    axis_lookup = axis_labels.slice_indexer(
                        axis_loc.start, axis_loc.stop, axis_loc.step
                    )
                    # Converting negative indices to their actual positions:
                    axis_lookup = pandas.RangeIndex(
                        start=(
                            axis_lookup.start
                            if axis_lookup.start >= 0
                            else axis_lookup.start + len(axis_labels)
                        ),
                        stop=(
                            axis_lookup.stop
                            if axis_lookup.stop >= 0
                            else axis_lookup.stop + len(axis_labels)
                        ),
                        step=axis_lookup.step,
                    )
            elif self.has_multiindex(axis):
                # `Index.get_locs` raises an IndexError by itself if missing labels were provided,
                # we don't have to do missing-check for the received `axis_lookup`.
                if isinstance(axis_loc, pandas.MultiIndex):
                    axis_lookup = self.get_axis(axis).get_indexer_for(axis_loc)
                else:
                    axis_lookup = self.get_axis(axis).get_locs(axis_loc)
            elif is_boolean_array(axis_loc):
                axis_lookup = boolean_mask_to_numeric(axis_loc)
            else:
                axis_labels = self.get_axis(axis)
                if is_list_like(axis_loc) and not isinstance(
                    axis_loc, (np.ndarray, pandas.Index)
                ):
                    # `Index.get_indexer_for` works much faster with numpy arrays than with python lists,
                    # so although we lose some time here on converting to numpy, `Index.get_indexer_for`
                    # speedup covers the loss that we gain here.
                    axis_loc = np.array(axis_loc, dtype=axis_labels.dtype)
                axis_lookup = axis_labels.get_indexer_for(axis_loc)
                # `Index.get_indexer_for` sets -1 value for missing labels, we have to verify whether
                # there are any -1 in the received indexer to raise a KeyError here.
                missing_mask = axis_lookup == -1
                if missing_mask.any():
                    missing_labels = (
                        axis_loc[missing_mask]
                        if is_list_like(axis_loc)
                        # If `axis_loc` is not a list-like then we can't select certain
                        # labels that are missing and so printing the whole indexer
                        else axis_loc
                    )
                    raise KeyError(missing_labels)

            if isinstance(axis_lookup, pandas.Index) and not is_range_like(axis_lookup):
                axis_lookup = axis_lookup.values

            lookups.append(axis_lookup)
        return lookups

    @abc.abstractmethod
    def take_2d_positional(self, index, columns):  # pragma: no cover
        """
        Index QueryCompiler with passed keys.

        Parameters
        ----------
        index : list-like of ints,
            Positional indices of rows to grab.
        columns : list-like of ints,
            Positional indices of columns to grab.

        Returns
        -------
        BaseQueryCompiler
            New masked QueryCompiler.
        """
        pass

    # END Abstract methods for QueryCompiler

    @pandas.util.cache_readonly
    def __constructor__(self):
        """
        Get query compiler constructor.

        By default, constructor method will invoke an init.

        Returns
        -------
        callable
        """
        return type(self)

    # __delitem__
    # This will change the shape of the resulting data.
    def delitem(self, key):
        """
        Drop `key` column.

        Parameters
        ----------
        key : label
            Column name to drop.

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler without `key` column.
        """
        return self.drop(columns=[key])

    # END __delitem__

    @abc.abstractmethod
    def has_multiindex(self, axis=0):  # pragma: no cover
        pass

    def get_index_name(self, axis=0):
        # TODO (SNOW-850751): clean this up and add implementation in snowflake query compiler
        """
        Get index name of specified axis.

        Parameters
        ----------
        axis : {0, 1}, default: 0
            Axis to get index name on.

        Returns
        -------
        hashable
            Index name, None for MultiIndex.
        """
        return self.get_axis(axis).name

    def set_index_name(self, name, axis=0):
        # TODO (SNOW-850751): clean this up and add implementation in snowflake query compiler
        """
        Set index name for the specified axis.

        Parameters
        ----------
        name : hashable
            New index name.
        axis : {0, 1}, default: 0
            Axis to set name along.
        """
        self.get_axis(axis).name = name

    def get_index_names(self, axis=0):
        """
        Get index names of specified axis.

        Parameters
        ----------
        axis : {0, 1}, default: 0
            Axis to get index names on.

        Returns
        -------
        list
            Index names.
        """
        raise NotImplementedError

    # DateTime methods

    @doc_utils.doc_dt_round(refer_to="ceil")
    def dt_ceil(self, freq, ambiguous="raise", nonexistent="raise"):
        return DateTimeDefault.register(pandas.Series.dt.ceil)(
            self, freq, ambiguous, nonexistent
        )

    @doc_utils.add_one_column_warning
    @doc_utils.add_refer_to("Series.dt.components")
    def dt_components(self):
        """
        Spread each date-time value into its components (days, hours, minutes...).

        Returns
        -------
        BaseQueryCompiler
        """
        return DateTimeDefault.register(pandas.Series.dt.components)(self)

    @doc_utils.doc_dt_timestamp(
        prop="the date without timezone information", refer_to="date"
    )
    def dt_date(self):
        return DateTimeDefault.register(pandas.Series.dt.date)(self)

    @doc_utils.doc_dt_timestamp(prop="day component", refer_to="day")
    def dt_day(self):
        return DateTimeDefault.register(pandas.Series.dt.day)(self)

    @doc_utils.doc_dt_timestamp(
        prop="day name", refer_to="day_name", params="locale : str, optional"
    )
    def dt_day_name(self, locale=None):
        return DateTimeDefault.register(pandas.Series.dt.day_name)(self, locale)

    @doc_utils.doc_dt_timestamp(prop="integer day of week", refer_to="dayofweek")
    # FIXME: `dt_dayofweek` is an alias for `dt_weekday`, one of them should
    # be removed (Modin issue #3107).
    def dt_dayofweek(self):
        return DateTimeDefault.register(pandas.Series.dt.dayofweek)(self)

    @doc_utils.doc_dt_timestamp(prop="day of year", refer_to="dayofyear")
    def dt_dayofyear(self):
        return DateTimeDefault.register(pandas.Series.dt.dayofyear)(self)

    @doc_utils.doc_dt_interval(prop="days", refer_to="days")
    def dt_days(self):
        return DateTimeDefault.register(pandas.Series.dt.days)(self)

    @doc_utils.doc_dt_timestamp(
        prop="number of days in month", refer_to="days_in_month"
    )
    # FIXME: `dt_days_in_month` is an alias for `dt_daysinmonth`, one of them should
    # be removed (Modin issue #3107).
    def dt_days_in_month(self):
        return DateTimeDefault.register(pandas.Series.dt.days_in_month)(self)

    @doc_utils.doc_dt_timestamp(prop="number of days in month", refer_to="daysinmonth")
    def dt_daysinmonth(self):
        return DateTimeDefault.register(pandas.Series.dt.daysinmonth)(self)

    @doc_utils.doc_dt_period(prop="the timestamp of end time", refer_to="end_time")
    def dt_end_time(self):
        return DateTimeDefault.register(pandas.Series.dt.end_time)(self)

    @doc_utils.doc_dt_round(refer_to="floor")
    def dt_floor(self, freq, ambiguous="raise", nonexistent="raise"):
        return DateTimeDefault.register(pandas.Series.dt.floor)(
            self, freq, ambiguous, nonexistent
        )

    @doc_utils.add_one_column_warning
    @doc_utils.add_refer_to("Series.dt.freq")
    def dt_freq(self):
        """
        Get the time frequency of the underlying time-series data.

        Returns
        -------
        BaseQueryCompiler
            QueryCompiler containing a single value, the frequency of the data.
        """
        return DateTimeDefault.register(pandas.Series.dt.freq)(self)

    @doc_utils.doc_dt_timestamp(prop="hour", refer_to="hour")
    def dt_hour(self):
        return DateTimeDefault.register(pandas.Series.dt.hour)(self)

    @doc_utils.doc_dt_timestamp(
        prop="the boolean of whether corresponding year is leap",
        refer_to="is_leap_year",
    )
    def dt_is_leap_year(self):
        return DateTimeDefault.register(pandas.Series.dt.is_leap_year)(self)

    @doc_utils.doc_dt_timestamp(
        prop="the boolean of whether the date is the last day of the month",
        refer_to="is_month_end",
    )
    def dt_is_month_end(self):
        return DateTimeDefault.register(pandas.Series.dt.is_month_end)(self)

    @doc_utils.doc_dt_timestamp(
        prop="the boolean of whether the date is the first day of the month",
        refer_to="is_month_start",
    )
    def dt_is_month_start(self):
        return DateTimeDefault.register(pandas.Series.dt.is_month_start)(self)

    @doc_utils.doc_dt_timestamp(
        prop="the boolean of whether the date is the last day of the quarter",
        refer_to="is_quarter_end",
    )
    def dt_is_quarter_end(self):
        return DateTimeDefault.register(pandas.Series.dt.is_quarter_end)(self)

    @doc_utils.doc_dt_timestamp(
        prop="the boolean of whether the date is the first day of the quarter",
        refer_to="is_quarter_start",
    )
    def dt_is_quarter_start(self):
        return DateTimeDefault.register(pandas.Series.dt.is_quarter_start)(self)

    @doc_utils.doc_dt_timestamp(
        prop="the boolean of whether the date is the last day of the year",
        refer_to="is_year_end",
    )
    def dt_is_year_end(self):
        return DateTimeDefault.register(pandas.Series.dt.is_year_end)(self)

    @doc_utils.doc_dt_timestamp(
        prop="the boolean of whether the date is the first day of the year",
        refer_to="is_year_start",
    )
    def dt_is_year_start(self):
        return DateTimeDefault.register(pandas.Series.dt.is_year_start)(self)

    @doc_utils.doc_dt_timestamp(prop="microseconds component", refer_to="microsecond")
    def dt_microsecond(self):
        return DateTimeDefault.register(pandas.Series.dt.microsecond)(self)

    @doc_utils.doc_dt_interval(prop="microseconds component", refer_to="microseconds")
    def dt_microseconds(self):
        return DateTimeDefault.register(pandas.Series.dt.microseconds)(self)

    @doc_utils.doc_dt_timestamp(prop="minute component", refer_to="minute")
    def dt_minute(self):
        return DateTimeDefault.register(pandas.Series.dt.minute)(self)

    @doc_utils.doc_dt_timestamp(prop="month component", refer_to="month")
    def dt_month(self):
        return DateTimeDefault.register(pandas.Series.dt.month)(self)

    @doc_utils.doc_dt_timestamp(
        prop="the month name", refer_to="month name", params="locale : str, optional"
    )
    def dt_month_name(self, locale=None):
        return DateTimeDefault.register(pandas.Series.dt.month_name)(self, locale)

    @doc_utils.doc_dt_timestamp(prop="nanoseconds component", refer_to="nanosecond")
    def dt_nanosecond(self):
        return DateTimeDefault.register(pandas.Series.dt.nanosecond)(self)

    @doc_utils.doc_dt_interval(prop="nanoseconds component", refer_to="nanoseconds")
    def dt_nanoseconds(self):
        return DateTimeDefault.register(pandas.Series.dt.nanoseconds)(self)

    @doc_utils.add_one_column_warning
    @doc_utils.add_refer_to("Series.dt.normalize")
    def dt_normalize(self):
        """
        Set the time component of each date-time value to midnight.

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler containing date-time values with midnight time.
        """
        return DateTimeDefault.register(pandas.Series.dt.normalize)(self)

    @doc_utils.doc_dt_timestamp(prop="quarter component", refer_to="quarter")
    def dt_quarter(self):
        return DateTimeDefault.register(pandas.Series.dt.quarter)(self)

    @doc_utils.doc_dt_period(prop="the fiscal year", refer_to="qyear")
    def dt_qyear(self):
        return DateTimeDefault.register(pandas.Series.dt.qyear)(self)

    @doc_utils.doc_dt_round(refer_to="round")
    def dt_round(self, freq, ambiguous="raise", nonexistent="raise"):
        return DateTimeDefault.register(pandas.Series.dt.round)(
            self, freq, ambiguous, nonexistent
        )

    @doc_utils.doc_dt_timestamp(prop="seconds component", refer_to="second")
    def dt_second(self):
        return DateTimeDefault.register(pandas.Series.dt.second)(self)

    @doc_utils.doc_dt_interval(prop="seconds component", refer_to="seconds")
    def dt_seconds(self):
        return DateTimeDefault.register(pandas.Series.dt.seconds)(self)

    @doc_utils.doc_dt_period(prop="the timestamp of start time", refer_to="start_time")
    def dt_start_time(self):
        return DateTimeDefault.register(pandas.Series.dt.start_time)(self)

    @doc_utils.add_refer_to("Series.dt.strftime")
    def dt_strftime(self, date_format):
        """
        Format underlying date-time data using specified format.

        Parameters
        ----------
        date_format : str

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler containing formatted date-time values.
        """
        return DateTimeDefault.register(pandas.Series.dt.strftime)(self, date_format)

    @doc_utils.doc_dt_timestamp(prop="time component", refer_to="time")
    def dt_time(self):
        return DateTimeDefault.register(pandas.Series.dt.time)(self)

    @doc_utils.doc_dt_timestamp(
        prop="time component with timezone information", refer_to="timetz"
    )
    def dt_timetz(self):
        return DateTimeDefault.register(pandas.Series.dt.timetz)(self)

    @doc_utils.add_one_column_warning
    @doc_utils.add_refer_to("Series.dt.to_period")
    def dt_to_period(self, freq=None):
        """
        Convert underlying data to the period at a particular frequency.

        Parameters
        ----------
        freq : str, optional

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler containing period data.
        """
        return DateTimeDefault.register(pandas.Series.dt.to_period)(self, freq)

    @doc_utils.add_one_column_warning
    @doc_utils.add_refer_to("Series.dt.to_pydatetime")
    def dt_to_pydatetime(self):
        """
        Convert underlying data to array of python native ``datetime``.

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler containing 1D array of ``datetime`` objects.
        """
        return DateTimeDefault.register(pandas.Series.dt.to_pydatetime)(self)

    # FIXME: there are no references to this method, we should either remove it
    # or add a call reference at the DataFrame level (Modin issue #3103).
    @doc_utils.add_one_column_warning
    @doc_utils.add_refer_to("Series.dt.to_pytimedelta")
    def dt_to_pytimedelta(self):
        """
        Convert underlying data to array of python native ``datetime.timedelta``.

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler containing 1D array of ``datetime.timedelta``.
        """
        return DateTimeDefault.register(pandas.Series.dt.to_pytimedelta)(self)

    @doc_utils.doc_dt_period(
        prop="the timestamp representation", refer_to="to_timestamp"
    )
    def dt_to_timestamp(self):
        return DateTimeDefault.register(pandas.Series.dt.to_timestamp)(self)

    @doc_utils.doc_dt_interval(prop="duration in seconds", refer_to="total_seconds")
    def dt_total_seconds(self):
        return DateTimeDefault.register(pandas.Series.dt.total_seconds)(self)

    @doc_utils.add_one_column_warning
    @doc_utils.add_refer_to("Series.dt.tz")
    def dt_tz(self):
        """
        Get the time-zone of the underlying time-series data.

        Returns
        -------
        BaseQueryCompiler
            QueryCompiler containing a single value, time-zone of the data.
        """
        return DateTimeDefault.register(pandas.Series.dt.tz)(self)

    @doc_utils.add_one_column_warning
    @doc_utils.add_refer_to("Series.dt.tz_convert")
    def dt_tz_convert(self, tz):
        """
        Convert time-series data to the specified time zone.

        Parameters
        ----------
        tz : str, pytz.timezone

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler containing values with converted time zone.
        """
        return DateTimeDefault.register(pandas.Series.dt.tz_convert)(self, tz)

    @doc_utils.add_one_column_warning
    @doc_utils.add_refer_to("Series.dt.tz_localize")
    def dt_tz_localize(self, tz, ambiguous="raise", nonexistent="raise"):
        """
        Localize tz-naive to tz-aware.

        Parameters
        ----------
        tz : str, pytz.timezone, optional
        ambiguous : {"raise", "inner", "NaT"} or bool mask, default: "raise"
        nonexistent : {"raise", "shift_forward", "shift_backward, "NaT"} or pandas.timedelta, default: "raise"

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler containing values with localized time zone.
        """
        return DateTimeDefault.register(pandas.Series.dt.tz_localize)(
            self, tz, ambiguous, nonexistent
        )

    @doc_utils.doc_dt_timestamp(prop="week component", refer_to="week")
    def dt_week(self):
        return DateTimeDefault.register(pandas.Series.dt.week)(self)

    @doc_utils.doc_dt_timestamp(prop="integer day of week", refer_to="weekday")
    def dt_weekday(self):
        return DateTimeDefault.register(pandas.Series.dt.weekday)(self)

    @doc_utils.doc_dt_timestamp(prop="week of year", refer_to="weekofyear")
    def dt_weekofyear(self):
        return DateTimeDefault.register(pandas.Series.dt.weekofyear)(self)

    @doc_utils.doc_dt_timestamp(prop="year component", refer_to="year")
    def dt_year(self):
        return DateTimeDefault.register(pandas.Series.dt.year)(self)

    # End of DateTime methods

    # Resample methods

    # FIXME:
    #   1. Query Compiler shouldn't care about differences between Series and DataFrame
    #      so `resample_agg_df` and `resample_agg_ser` should be combined (Modin issue #3104).
    #   2. In DataFrame API `Resampler.aggregate` is an alias for `Resampler.apply`
    #      we should remove one of these methods: `resample_agg_*` or `resample_app_*` (Modin issue #3107).
    @doc_utils.doc_resample_agg(
        action="apply passed aggregation function",
        params="func : str, dict, callable(pandas.Series) -> scalar, or list of such",
        output="function names",
        refer_to="agg",
    )
    def resample_agg_df(self, resample_kwargs, func, *args, **kwargs):
        return ResampleDefault.register(pandas.core.resample.Resampler.aggregate)(
            self, resample_kwargs, func, *args, **kwargs
        )

    @doc_utils.add_deprecation_warning(replacement_method="resample_agg_df")
    @doc_utils.doc_resample_agg(
        action="apply passed aggregation function in a one-column query compiler",
        params="func : str, dict, callable(pandas.Series) -> scalar, or list of such",
        output="function names",
        refer_to="agg",
    )
    def resample_agg_ser(self, resample_kwargs, func, *args, **kwargs):
        return ResampleDefault.register(
            pandas.core.resample.Resampler.aggregate, squeeze_self=True
        )(self, resample_kwargs, func, *args, **kwargs)

    @doc_utils.add_deprecation_warning(replacement_method="resample_agg_df")
    @doc_utils.doc_resample_agg(
        action="apply passed aggregation function",
        params="func : str, dict, callable(pandas.Series) -> scalar, or list of such",
        output="function names",
        refer_to="apply",
    )
    def resample_app_df(self, resample_kwargs, func, *args, **kwargs):
        return ResampleDefault.register(pandas.core.resample.Resampler.apply)(
            self, resample_kwargs, func, *args, **kwargs
        )

    @doc_utils.add_deprecation_warning(replacement_method="resample_agg_df")
    @doc_utils.doc_resample_agg(
        action="apply passed aggregation function in a one-column query compiler",
        params="func : str, dict, callable(pandas.Series) -> scalar, or list of such",
        output="function names",
        refer_to="apply",
    )
    def resample_app_ser(self, resample_kwargs, func, *args, **kwargs):
        return ResampleDefault.register(
            pandas.core.resample.Resampler.apply, squeeze_self=True
        )(self, resample_kwargs, func, *args, **kwargs)

    def resample_asfreq(self, resample_kwargs, fill_value):
        """
        Resample time-series data and get the values at the new frequency.

        Group data into intervals by time-series row/column with
        a specified frequency and get values at the new frequency.

        Parameters
        ----------
        resample_kwargs : dict
            Resample parameters as expected by ``modin.pandas.DataFrame.resample`` signature.
        fill_value : scalar

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler containing values at the specified frequency.
        """
        return ResampleDefault.register(pandas.core.resample.Resampler.asfreq)(
            self, resample_kwargs, fill_value
        )

    # FIXME: `resample_backfill` is an alias for `resample_bfill`, one of these method
    # should be removed (Modin issue #3107).
    @doc_utils.doc_resample_fillna(method="back-fill", refer_to="backfill")
    def resample_backfill(self, resample_kwargs, limit):
        return ResampleDefault.register(pandas.core.resample.Resampler.backfill)(
            self, resample_kwargs, limit
        )

    @doc_utils.doc_resample_fillna(method="back-fill", refer_to="bfill")
    def resample_bfill(self, resample_kwargs, limit):
        return ResampleDefault.register(pandas.core.resample.Resampler.bfill)(
            self, resample_kwargs, limit
        )

    @doc_utils.doc_resample_reduce(
        result="number of non-NA values", refer_to="count", compatibility_params=False
    )
    def resample_count(self, resample_kwargs):
        return ResampleDefault.register(pandas.core.resample.Resampler.count)(
            self, resample_kwargs
        )

    # FIXME: `resample_ffill` is an alias for `resample_pad`, one of these method
    # should be removed (Modin issue #3107).
    @doc_utils.doc_resample_fillna(method="forward-fill", refer_to="ffill")
    def resample_ffill(self, resample_kwargs, limit):
        return ResampleDefault.register(pandas.core.resample.Resampler.ffill)(
            self, resample_kwargs, limit
        )

    # FIXME: we should combine all resample fillna methods into `resample_fillna`
    # (Modin issue #3107)
    @doc_utils.doc_resample_fillna(
        method="specified", refer_to="fillna", params="method : str"
    )
    def resample_fillna(self, resample_kwargs, method, limit):
        return ResampleDefault.register(pandas.core.resample.Resampler.fillna)(
            self, resample_kwargs, method, limit
        )

    @doc_utils.doc_resample_reduce(result="first element", refer_to="first")
    def resample_first(self, resample_kwargs, numeric_only, min_count, *args, **kwargs):
        return ResampleDefault.register(pandas.core.resample.Resampler.first)(
            self,
            resample_kwargs,
            numeric_only=numeric_only,
            min_count=min_count,
            *args,  # noqa: B026
            **kwargs,
        )

    # FIXME: This function takes Modin DataFrame via `obj` parameter,
    # we should avoid leaking of the high-level objects to the query compiler level.
    # (Modin issue #3106)
    def resample_get_group(self, resample_kwargs, name, obj):
        """
        Resample time-series data and get the specified group.

        Group data into intervals by time-series row/column with
        a specified frequency and get the values of the specified group.

        Parameters
        ----------
        resample_kwargs : dict
            Resample parameters as expected by ``modin.pandas.DataFrame.resample`` signature.
        name : object
        obj : modin.pandas.DataFrame, optional

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler containing the values from the specified group.
        """
        return ResampleDefault.register(pandas.core.resample.Resampler.get_group)(
            self, resample_kwargs, name, obj
        )

    @doc_utils.doc_resample_fillna(
        method="specified interpolation",
        refer_to="interpolate",
        params="""
        method : str
        axis : {0, 1}
        limit : int
        inplace : {False}
            This parameter serves the compatibility purpose. Always has to be False.
        limit_direction : {"forward", "backward", "both"}
        limit_area : {None, "inside", "outside"}
        downcast : str, optional
        **kwargs : dict
        """,
        overwrite_template_params=True,
    )
    def resample_interpolate(
        self,
        resample_kwargs,
        method,
        axis,
        limit,
        inplace,
        limit_direction,
        limit_area,
        downcast,
        **kwargs,
    ):
        return ResampleDefault.register(pandas.core.resample.Resampler.interpolate)(
            self,
            resample_kwargs,
            method,
            axis,
            limit,
            inplace,
            limit_direction,
            limit_area,
            downcast,
            **kwargs,
        )

    @doc_utils.doc_resample_reduce(result="last element", refer_to="last")
    def resample_last(self, resample_kwargs, numeric_only, min_count, *args, **kwargs):
        return ResampleDefault.register(pandas.core.resample.Resampler.last)(
            self,
            resample_kwargs,
            numeric_only=numeric_only,
            min_count=min_count,
            *args,  # noqa: B026
            **kwargs,
        )

    @doc_utils.doc_resample_reduce(result="maximum value", refer_to="max")
    def resample_max(self, resample_kwargs, numeric_only, min_count, *args, **kwargs):
        return ResampleDefault.register(pandas.core.resample.Resampler.max)(
            self,
            resample_kwargs,
            numeric_only=numeric_only,
            min_count=min_count,
            *args,  # noqa: B026
            **kwargs,
        )

    @doc_utils.doc_resample_reduce(result="mean value", refer_to="mean")
    def resample_mean(self, resample_kwargs, numeric_only, *args, **kwargs):
        return ResampleDefault.register(pandas.core.resample.Resampler.mean)(
            self,
            resample_kwargs,
            numeric_only=numeric_only,
            *args,  # noqa: B026
            **kwargs,
        )

    @doc_utils.doc_resample_reduce(result="median value", refer_to="median")
    def resample_median(self, resample_kwargs, numeric_only, *args, **kwargs):
        return ResampleDefault.register(pandas.core.resample.Resampler.median)(
            self,
            resample_kwargs,
            numeric_only=numeric_only,
            *args,  # noqa: B026
            **kwargs,
        )

    @doc_utils.doc_resample_reduce(result="minimum value", refer_to="min")
    def resample_min(self, resample_kwargs, numeric_only, min_count, *args, **kwargs):
        return ResampleDefault.register(pandas.core.resample.Resampler.min)(
            self,
            resample_kwargs,
            numeric_only=numeric_only,
            min_count=min_count,
            *args,  # noqa: B026
            **kwargs,
        )

    @doc_utils.doc_resample_fillna(method="'nearest'", refer_to="nearest")
    def resample_nearest(self, resample_kwargs, limit):
        return ResampleDefault.register(pandas.core.resample.Resampler.nearest)(
            self, resample_kwargs, limit
        )

    @doc_utils.doc_resample_reduce(result="number of unique values", refer_to="nunique")
    def resample_nunique(self, resample_kwargs, *args, **kwargs):
        return ResampleDefault.register(pandas.core.resample.Resampler.nunique)(
            self, resample_kwargs, *args, **kwargs
        )

    # FIXME: Query Compiler shouldn't care about differences between Series and DataFrame
    # so `resample_ohlc_df` and `resample_ohlc_ser` should be combined (Modin issue #3104).
    @doc_utils.doc_resample_agg(
        action="compute open, high, low and close values",
        output="labels of columns containing computed values",
        refer_to="ohlc",
    )
    def resample_ohlc_df(self, resample_kwargs, *args, **kwargs):
        return ResampleDefault.register(pandas.core.resample.Resampler.ohlc)(
            self, resample_kwargs, *args, **kwargs
        )

    @doc_utils.doc_resample_agg(
        action="compute open, high, low and close values",
        output="labels of columns containing computed values",
        refer_to="ohlc",
    )
    def resample_ohlc_ser(self, resample_kwargs, *args, **kwargs):
        return ResampleDefault.register(
            pandas.core.resample.Resampler.ohlc, squeeze_self=True
        )(self, resample_kwargs, *args, **kwargs)

    @doc_utils.doc_resample_fillna(method="'pad'", refer_to="pad")
    def resample_pad(self, resample_kwargs, limit):
        return ResampleDefault.register(pandas.core.resample.Resampler.pad)(
            self, resample_kwargs, limit
        )

    # FIXME: This method require us to build high-level resampler object
    # which we shouldn't do at the query compiler. We need to move this at the front.
    # (Modin issue #3105)
    @doc_utils.add_refer_to("Resampler.pipe")
    def resample_pipe(self, resample_kwargs, func, *args, **kwargs):
        """
        Resample time-series data and apply aggregation on it.

        Group data into intervals by time-series row/column with
        a specified frequency, build equivalent ``pandas.Resampler`` object
        and apply passed function to it.

        Parameters
        ----------
        resample_kwargs : dict
            Resample parameters as expected by ``modin.pandas.DataFrame.resample`` signature.
        func : callable(pandas.Resampler) -> object or tuple(callable, str)
        *args : iterable
            Positional arguments to pass to function.
        **kwargs : dict
            Keyword arguments to pass to function.

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler containing the result of passed function.
        """
        return ResampleDefault.register(pandas.core.resample.Resampler.pipe)(
            self, resample_kwargs, func, *args, **kwargs
        )

    @doc_utils.doc_resample_reduce(
        result="product",
        params="""
        numeric_only: bool
        min_count : int
        """,
        refer_to="prod",
    )
    def resample_prod(self, resample_kwargs, numeric_only, min_count, *args, **kwargs):
        return ResampleDefault.register(pandas.core.resample.Resampler.prod)(
            self,
            resample_kwargs,
            numeric_only=numeric_only,
            min_count=min_count,
            *args,  # noqa: B026
            **kwargs,
        )

    @doc_utils.doc_resample_reduce(
        result="quantile", params="q : float", refer_to="quantile"
    )
    def resample_quantile(self, resample_kwargs, q, *args, **kwargs):
        return ResampleDefault.register(pandas.core.resample.Resampler.quantile)(
            self, resample_kwargs, q, *args, **kwargs
        )

    @doc_utils.doc_resample_reduce(
        result="standard error of the mean",
        params="""
        ddof : int
        numeric_only: bool
        """,
        refer_to="sem",
    )
    def resample_sem(self, resample_kwargs, ddof, numeric_only, *args, **kwargs):
        return ResampleDefault.register(pandas.core.resample.Resampler.sem)(
            self,
            resample_kwargs,
            ddof=ddof,
            numeric_only=numeric_only,
            *args,  # noqa: B026
            **kwargs,
        )

    @doc_utils.doc_resample_reduce(
        result="number of elements in a group", refer_to="size"
    )
    def resample_size(self, resample_kwargs, *args, **kwargs):
        return ResampleDefault.register(pandas.core.resample.Resampler.size)(
            self, resample_kwargs, *args, **kwargs
        )

    @doc_utils.doc_resample_reduce(
        result="standard deviation",
        params="""
        ddof : int
        numeric_only: bool
        """,
        refer_to="std",
    )
    def resample_std(self, resample_kwargs, ddof, numeric_only, *args, **kwargs):
        return ResampleDefault.register(pandas.core.resample.Resampler.std)(
            self, resample_kwargs, ddof, numeric_only, *args, **kwargs
        )

    @doc_utils.doc_resample_reduce(
        result="sum",
        params="""
        numeric_only: bool
        min_count : int
        """,
        refer_to="sum",
    )
    def resample_sum(self, resample_kwargs, numeric_only, min_count, *args, **kwargs):
        return ResampleDefault.register(pandas.core.resample.Resampler.sum)(
            self,
            resample_kwargs,
            numeric_only=numeric_only,
            min_count=min_count,
            *args,  # noqa: B026
            **kwargs,
        )

    def resample_transform(self, resample_kwargs, arg, *args, **kwargs):
        """
        Resample time-series data and apply aggregation on it.

        Group data into intervals by time-series row/column with
        a specified frequency and call passed function on each group.
        In contrast to ``resample_app_df`` apply function to the whole group,
        instead of a single axis.

        Parameters
        ----------
        resample_kwargs : dict
            Resample parameters as expected by ``modin.pandas.DataFrame.resample`` signature.
        arg : callable(pandas.DataFrame) -> pandas.Series
        *args : iterable
            Positional arguments to pass to function.
        **kwargs : dict
            Keyword arguments to pass to function.

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler containing the result of passed function.
        """
        return ResampleDefault.register(pandas.core.resample.Resampler.transform)(
            self, resample_kwargs, arg, *args, **kwargs
        )

    @doc_utils.doc_resample_reduce(
        result="variance",
        params="""
        ddof : int
        numeric_only: bool
        """,
        refer_to="var",
    )
    def resample_var(self, resample_kwargs, ddof, numeric_only, *args, **kwargs):
        return ResampleDefault.register(pandas.core.resample.Resampler.var)(
            self, resample_kwargs, ddof, numeric_only, *args, **kwargs
        )

    # End of Resample methods

    # Str methods

    @doc_utils.doc_str_method(refer_to="capitalize", params="")
    def str_capitalize(self):
        return StrDefault.register(pandas.Series.str.capitalize)(self)

    @doc_utils.doc_str_method(
        refer_to="center",
        params="""
        width : int
        fillchar : str, default: ' '""",
    )
    def str_center(self, width, fillchar=" "):
        return StrDefault.register(pandas.Series.str.center)(self, width, fillchar)

    @doc_utils.doc_str_method(
        refer_to="contains",
        params="""
        pat : str
        case : bool, default: True
        flags : int, default: 0
        na : object, default: np.NaN
        regex : bool, default: True""",
    )
    def str_contains(self, pat, case=True, flags=0, na=np.NaN, regex=True):
        return StrDefault.register(pandas.Series.str.contains)(
            self, pat, case, flags, na, regex
        )

    @doc_utils.doc_str_method(
        refer_to="count",
        params="""
        pat : str
        flags : int, default: 0
        **kwargs : dict""",
    )
    def str_count(self, pat, flags=0, **kwargs):
        return StrDefault.register(pandas.Series.str.count)(self, pat, flags, **kwargs)

    @doc_utils.doc_str_method(
        refer_to="endswith",
        params="""
        pat : str
        na : object, default: np.NaN""",
    )
    def str_endswith(self, pat, na=np.NaN):
        return StrDefault.register(pandas.Series.str.endswith)(self, pat, na)

    @doc_utils.doc_str_method(
        refer_to="find",
        params="""
        sub : str
        start : int, default: 0
        end : int, optional""",
    )
    def str_find(self, sub, start=0, end=None):
        return StrDefault.register(pandas.Series.str.find)(self, sub, start, end)

    @doc_utils.doc_str_method(
        refer_to="findall",
        params="""
        pat : str
        flags : int, default: 0
        **kwargs : dict""",
    )
    def str_findall(self, pat, flags=0, **kwargs):
        return StrDefault.register(pandas.Series.str.findall)(
            self, pat, flags, **kwargs
        )

    @doc_utils.doc_str_method(refer_to="get", params="i : int")
    def str_get(self, i):
        return StrDefault.register(pandas.Series.str.get)(self, i)

    @doc_utils.doc_str_method(refer_to="get_dummies", params="sep : str")
    def str_get_dummies(self, sep):
        return StrDefault.register(pandas.Series.str.get_dummies)(self, sep)

    @doc_utils.doc_str_method(
        refer_to="index",
        params="""
        sub : str
        start : int, default: 0
        end : int, optional""",
    )
    def str_index(self, sub, start=0, end=None):
        return StrDefault.register(pandas.Series.str.index)(self, sub, start, end)

    @doc_utils.doc_str_method(refer_to="isalnum", params="")
    def str_isalnum(self):
        return StrDefault.register(pandas.Series.str.isalnum)(self)

    @doc_utils.doc_str_method(refer_to="isalpha", params="")
    def str_isalpha(self):
        return StrDefault.register(pandas.Series.str.isalpha)(self)

    @doc_utils.doc_str_method(refer_to="isdecimal", params="")
    def str_isdecimal(self):
        return StrDefault.register(pandas.Series.str.isdecimal)(self)

    @doc_utils.doc_str_method(refer_to="isdigit", params="")
    def str_isdigit(self):
        return StrDefault.register(pandas.Series.str.isdigit)(self)

    @doc_utils.doc_str_method(refer_to="islower", params="")
    def str_islower(self):
        return StrDefault.register(pandas.Series.str.islower)(self)

    @doc_utils.doc_str_method(refer_to="isnumeric", params="")
    def str_isnumeric(self):
        return StrDefault.register(pandas.Series.str.isnumeric)(self)

    @doc_utils.doc_str_method(refer_to="isspace", params="")
    def str_isspace(self):
        return StrDefault.register(pandas.Series.str.isspace)(self)

    @doc_utils.doc_str_method(refer_to="istitle", params="")
    def str_istitle(self):
        return StrDefault.register(pandas.Series.str.istitle)(self)

    @doc_utils.doc_str_method(refer_to="isupper", params="")
    def str_isupper(self):
        return StrDefault.register(pandas.Series.str.isupper)(self)

    @doc_utils.doc_str_method(refer_to="join", params="sep : str")
    def str_join(self, sep):
        return StrDefault.register(pandas.Series.str.join)(self, sep)

    @doc_utils.doc_str_method(refer_to="len", params="")
    def str_len(self):
        return StrDefault.register(pandas.Series.str.len)(self)

    @doc_utils.doc_str_method(
        refer_to="ljust",
        params="""
        width : int
        fillchar : str, default: ' '""",
    )
    def str_ljust(self, width, fillchar=" "):
        return StrDefault.register(pandas.Series.str.ljust)(self, width, fillchar)

    @doc_utils.doc_str_method(refer_to="lower", params="")
    def str_lower(self):
        return StrDefault.register(pandas.Series.str.lower)(self)

    @doc_utils.doc_str_method(refer_to="lstrip", params="to_strip : str, optional")
    def str_lstrip(self, to_strip=None):
        return StrDefault.register(pandas.Series.str.lstrip)(self, to_strip)

    @doc_utils.doc_str_method(
        refer_to="match",
        params="""
        pat : str
        case : bool, default: True
        flags : int, default: 0
        na : object, default: np.NaN""",
    )
    def str_match(self, pat, case=True, flags=0, na=np.NaN):
        return StrDefault.register(pandas.Series.str.match)(self, pat, case, flags, na)

    @doc_utils.doc_str_method(
        refer_to="extract",
        params="""
        pat : str
        flags : int, default: 0
        expand : bool, default: True""",
    )
    def str_extract(self, pat, flags=0, expand=True):
        return StrDefault.register(pandas.Series.str.extract)(self, pat, flags, expand)

    @doc_utils.doc_str_method(
        refer_to="extractall",
        params="""
           pat : str
           flags : int, default: 0""",
    )
    def str_extractall(self, pat, flags=0):
        return StrDefault.register(pandas.Series.str.extractall)(self, pat, flags)

    @doc_utils.doc_str_method(
        refer_to="normalize", params="form : {'NFC', 'NFKC', 'NFD', 'NFKD'}"
    )
    def str_normalize(self, form):
        return StrDefault.register(pandas.Series.str.normalize)(self, form)

    @doc_utils.doc_str_method(
        refer_to="pad",
        params="""
        width : int
        side : {'left', 'right', 'both'}, default: 'left'
        fillchar : str, default: ' '""",
    )
    def str_pad(self, width, side="left", fillchar=" "):
        return StrDefault.register(pandas.Series.str.pad)(self, width, side, fillchar)

    @doc_utils.doc_str_method(
        refer_to="partition",
        params="""
        sep : str, default: ' '
        expand : bool, default: True""",
    )
    def str_partition(self, sep=" ", expand=True):
        return StrDefault.register(pandas.Series.str.partition)(self, sep, expand)

    @doc_utils.doc_str_method(refer_to="removeprefix", params="prefix : str")
    def str_removeprefix(self, prefix):
        return StrDefault.register(pandas.Series.str.removeprefix)(self, prefix)

    @doc_utils.doc_str_method(refer_to="removesuffix", params="suffix : str")
    def str_removesuffix(self, suffix):
        return StrDefault.register(pandas.Series.str.removesuffix)(self, suffix)

    @doc_utils.doc_str_method(refer_to="repeat", params="repeats : int")
    def str_repeat(self, repeats):
        return StrDefault.register(pandas.Series.str.repeat)(self, repeats)

    @doc_utils.doc_str_method(
        refer_to="replace",
        params="""
        pat : str
        repl : str or callable
        n : int, default: -1
        case : bool, optional
        flags : int, default: 0
        regex : bool, default: True""",
    )
    def str_replace(self, pat, repl, n=-1, case=None, flags=0, regex=True):
        return StrDefault.register(pandas.Series.str.replace)(
            self, pat, repl, n, case, flags, regex
        )

    @doc_utils.doc_str_method(
        refer_to="rfind",
        params="""
        sub : str
        start : int, default: 0
        end : int, optional""",
    )
    def str_rfind(self, sub, start=0, end=None):
        return StrDefault.register(pandas.Series.str.rfind)(self, sub, start, end)

    @doc_utils.doc_str_method(
        refer_to="rindex",
        params="""
        sub : str
        start : int, default: 0
        end : int, optional""",
    )
    def str_rindex(self, sub, start=0, end=None):
        return StrDefault.register(pandas.Series.str.rindex)(self, sub, start, end)

    @doc_utils.doc_str_method(
        refer_to="rjust",
        params="""
        width : int
        fillchar : str, default: ' '""",
    )
    def str_rjust(self, width, fillchar=" "):
        return StrDefault.register(pandas.Series.str.rjust)(self, width, fillchar)

    @doc_utils.doc_str_method(
        refer_to="rpartition",
        params="""
        sep : str, default: ' '
        expand : bool, default: True""",
    )
    def str_rpartition(self, sep=" ", expand=True):
        return StrDefault.register(pandas.Series.str.rpartition)(self, sep, expand)

    @doc_utils.doc_str_method(
        refer_to="rsplit",
        params="""
        pat : str, optional
        n : int, default: -1
        expand : bool, default: False""",
    )
    def str_rsplit(self, pat=None, n=-1, expand=False):
        return StrDefault.register(pandas.Series.str.rsplit)(self, pat, n, expand)

    @doc_utils.doc_str_method(refer_to="rstrip", params="to_strip : str, optional")
    def str_rstrip(self, to_strip=None):
        return StrDefault.register(pandas.Series.str.rstrip)(self, to_strip)

    @doc_utils.doc_str_method(
        refer_to="slice",
        params="""
        start : int, optional
        stop : int, optional
        step : int, optional""",
    )
    def str_slice(self, start=None, stop=None, step=None):
        return StrDefault.register(pandas.Series.str.slice)(self, start, stop, step)

    @doc_utils.doc_str_method(
        refer_to="slice_replace",
        params="""
        start : int, optional
        stop : int, optional
        repl : str or callable, optional""",
    )
    def str_slice_replace(self, start=None, stop=None, repl=None):
        return StrDefault.register(pandas.Series.str.slice_replace)(
            self, start, stop, repl
        )

    @doc_utils.doc_str_method(
        refer_to="split",
        params="""
        pat : str, optional
        n : int, default: -1
        expand : bool, default: False""",
    )
    def str_split(self, pat=None, n=-1, expand=False, regex=None):
        return StrDefault.register(pandas.Series.str.split)(
            self, pat, n=n, expand=expand, regex=regex
        )

    @doc_utils.doc_str_method(
        refer_to="startswith",
        params="""
        pat : str
        na : object, default: np.NaN""",
    )
    def str_startswith(self, pat, na=np.NaN):
        return StrDefault.register(pandas.Series.str.startswith)(self, pat, na)

    @doc_utils.doc_str_method(refer_to="strip", params="to_strip : str, optional")
    def str_strip(self, to_strip=None):
        return StrDefault.register(pandas.Series.str.strip)(self, to_strip)

    @doc_utils.doc_str_method(refer_to="swapcase", params="")
    def str_swapcase(self):
        return StrDefault.register(pandas.Series.str.swapcase)(self)

    @doc_utils.doc_str_method(refer_to="title", params="")
    def str_title(self):
        return StrDefault.register(pandas.Series.str.title)(self)

    @doc_utils.doc_str_method(refer_to="translate", params="table : dict")
    def str_translate(self, table):
        return StrDefault.register(pandas.Series.str.translate)(self, table)

    @doc_utils.doc_str_method(refer_to="upper", params="")
    def str_upper(self):
        return StrDefault.register(pandas.Series.str.upper)(self)

    @doc_utils.doc_str_method(
        refer_to="wrap",
        params="""
        width : int
        **kwargs : dict""",
    )
    def str_wrap(self, width, **kwargs):
        return StrDefault.register(pandas.Series.str.wrap)(self, width, **kwargs)

    @doc_utils.doc_str_method(refer_to="zfill", params="width : int")
    def str_zfill(self, width):
        return StrDefault.register(pandas.Series.str.zfill)(self, width)

    @doc_utils.doc_str_method(
        refer_to="encode", params="encoding: str, errors: str, optional"
    )
    def str_encode(self, encoding, errors="strict"):
        return StrDefault.register(pandas.Series.str.encode)(self, encoding, errors)

    @doc_utils.doc_str_method(
        refer_to="decode", params="encoding: str, errors: str, optional"
    )
    def str_decode(self, encoding, errors="strict"):
        return StrDefault.register(pandas.Series.str.decode)(self, encoding, errors)

    @doc_utils.doc_str_method(
        refer_to="cat",
        params="""
               others : Series, Index, DataFrame, np.ndarray or list-like,
               sep : str, default: '',
               na_rep : str or None, default: None,
               join : {'left', 'right', 'outer', 'inner'}, default: 'left'""",
    )
    def str_cat(self, others, sep=None, na_rep=None, join="left"):
        return StrDefault.register(pandas.Series.str.cat)(
            self, others, sep, na_rep, join
        )

    @doc_utils.doc_str_method(
        refer_to="casefold",
        params="",
    )
    def str_casefold(self):
        return StrDefault.register(pandas.Series.str.casefold)(self)

    @doc_utils.doc_str_method(refer_to="__getitem__", params="key : object")
    def str___getitem__(self, key):
        return StrDefault.register(pandas.Series.str.__getitem__)(self, key)

    # End of Str methods

    # Rolling methods

    def shift(
        self,
        periods: int = 1,
        freq=None,
        axis: Axis = 0,
        fill_value: Hashable = no_default,
    ) -> "BaseQueryCompiler":

        # TODO: implement generic Modin version
        ErrorMessage.not_implemented(  # pragma: no cover
            "base method shift not implemented"  # pragma: no cover
        )  # pragma: no cover

    # FIXME: most of the rolling/window methods take *args and **kwargs parameters
    # which are only needed for the compatibility with numpy, this behavior is inherited
    # from the API level, we should get rid of it (Modin issue #3108).

    @doc_utils.doc_window_method(
        result="the result of passed functions",
        action="apply specified functions",
        refer_to="aggregate",
        params="""
        func : str, dict, callable(pandas.Series) -> scalar, or list of such
        *args : iterable
        **kwargs : dict""",
        build_rules="udf_aggregation",
    )
    def rolling_aggregate(self, fold_axis, rolling_args, func, *args, **kwargs):
        return RollingDefault.register(pandas.core.window.rolling.Rolling.aggregate)(
            self, rolling_args, func, *args, **kwargs
        )

    # FIXME: at the query compiler method `rolling_apply` is an alias for `rolling_aggregate`,
    # one of these should be removed (Modin issue #3107).
    @doc_utils.add_deprecation_warning(replacement_method="rolling_aggregate")
    @doc_utils.doc_window_method(
        result="the result of passed function",
        action="apply specified function",
        refer_to="apply",
        params="""
        func : callable(pandas.Series) -> scalar
        raw : bool, default: False
        engine : None, default: None
            This parameters serves the compatibility purpose. Always has to be None.
        engine_kwargs : None, default: None
            This parameters serves the compatibility purpose. Always has to be None.
        args : tuple, optional
        kwargs : dict, optional""",
        build_rules="udf_aggregation",
    )
    def rolling_apply(
        self,
        fold_axis,
        rolling_args,
        func,
        raw=False,
        engine=None,
        engine_kwargs=None,
        args=None,
        kwargs=None,
    ):
        return RollingDefault.register(pandas.core.window.rolling.Rolling.apply)(
            self, rolling_args, func, raw, engine, engine_kwargs, args, kwargs
        )

    @doc_utils.doc_window_method(
        result="correlation",
        refer_to="corr",
        params="""
        other : modin.pandas.Series, modin.pandas.DataFrame, list-like, optional
        pairwise : bool, optional
        *args : iterable
        **kwargs : dict""",
    )
    def rolling_corr(
        self, fold_axis, rolling_args, other=None, pairwise=None, *args, **kwargs
    ):
        return RollingDefault.register(pandas.core.window.rolling.Rolling.corr)(
            self, rolling_args, other, pairwise, *args, **kwargs
        )

    @doc_utils.doc_window_method(result="number of non-NA values", refer_to="count")
    def rolling_count(self, fold_axis, rolling_args):
        return RollingDefault.register(pandas.core.window.rolling.Rolling.count)(
            self, rolling_args
        )

    @doc_utils.doc_window_method(
        result="covariance",
        refer_to="cov",
        params="""
        other : modin.pandas.Series, modin.pandas.DataFrame, list-like, optional
        pairwise : bool, optional
        ddof : int, default:  1
        **kwargs : dict""",
    )
    def rolling_cov(
        self, fold_axis, rolling_args, other=None, pairwise=None, ddof=1, **kwargs
    ):
        return RollingDefault.register(pandas.core.window.rolling.Rolling.cov)(
            self, rolling_args, other, pairwise, ddof, **kwargs
        )

    @doc_utils.doc_window_method(
        result="unbiased kurtosis", refer_to="kurt", params="**kwargs : dict"
    )
    def rolling_kurt(self, fold_axis, rolling_args, **kwargs):
        return RollingDefault.register(pandas.core.window.rolling.Rolling.kurt)(
            self, rolling_args, **kwargs
        )

    @doc_utils.doc_window_method(
        result="maximum value",
        refer_to="max",
        params="""
        *args : iterable
        **kwargs : dict""",
    )
    def rolling_max(self, fold_axis, rolling_args, *args, **kwargs):
        return RollingDefault.register(pandas.core.window.rolling.Rolling.max)(
            self, rolling_args, *args, **kwargs
        )

    @doc_utils.doc_window_method(
        result="mean value",
        refer_to="mean",
        params="""
        *args : iterable
        **kwargs : dict""",
    )
    def rolling_mean(self, fold_axis, rolling_args, *args, **kwargs):
        return RollingDefault.register(pandas.core.window.rolling.Rolling.mean)(
            self, rolling_args, *args, **kwargs
        )

    @doc_utils.doc_window_method(
        result="median value", refer_to="median", params="**kwargs : dict"
    )
    def rolling_median(self, fold_axis, rolling_args, **kwargs):
        return RollingDefault.register(pandas.core.window.rolling.Rolling.median)(
            self, rolling_args, **kwargs
        )

    @doc_utils.doc_window_method(
        result="minimum value",
        refer_to="min",
        params="""
        *args : iterable
        **kwargs : dict""",
    )
    def rolling_min(self, fold_axis, rolling_args, *args, **kwargs):
        return RollingDefault.register(pandas.core.window.rolling.Rolling.min)(
            self, rolling_args, *args, **kwargs
        )

    @doc_utils.doc_window_method(
        result="quantile",
        refer_to="quantile",
        params="""
        quantile : float
        interpolation : {'linear', 'lower', 'higher', 'midpoint', 'nearest'}, default: 'linear'
        **kwargs : dict""",
    )
    def rolling_quantile(
        self, fold_axis, rolling_args, quantile, interpolation="linear", **kwargs
    ):
        return RollingDefault.register(pandas.core.window.rolling.Rolling.quantile)(
            self, rolling_args, quantile, interpolation, **kwargs
        )

    @doc_utils.doc_window_method(
        result="unbiased skewness", refer_to="skew", params="**kwargs : dict"
    )
    def rolling_skew(self, fold_axis, rolling_args, **kwargs):
        return RollingDefault.register(pandas.core.window.rolling.Rolling.skew)(
            self, rolling_args, **kwargs
        )

    @doc_utils.doc_window_method(
        result="standard deviation",
        refer_to="std",
        params="""
        ddof : int, default: 1
        *args : iterable
        **kwargs : dict""",
    )
    def rolling_std(self, fold_axis, rolling_args, ddof=1, *args, **kwargs):
        return RollingDefault.register(pandas.core.window.rolling.Rolling.std)(
            self, rolling_args, ddof, *args, **kwargs
        )

    @doc_utils.doc_window_method(
        result="sum",
        refer_to="sum",
        params="""
        *args : iterable
        **kwargs : dict""",
    )
    def rolling_sum(self, fold_axis, rolling_args, *args, **kwargs):
        return RollingDefault.register(pandas.core.window.rolling.Rolling.sum)(
            self, rolling_args, *args, **kwargs
        )

    @doc_utils.doc_window_method(
        result="variance",
        refer_to="var",
        params="""
        ddof : int, default: 1
        *args : iterable
        **kwargs : dict""",
    )
    def rolling_var(self, fold_axis, rolling_args, ddof=1, *args, **kwargs):
        return RollingDefault.register(pandas.core.window.rolling.Rolling.var)(
            self, rolling_args, ddof, *args, **kwargs
        )

    # End of Rolling methods

    # Window methods

    @doc_utils.doc_window_method(
        win_type="window of the specified type",
        result="mean",
        refer_to="mean",
        params="""
        *args : iterable
        **kwargs : dict""",
    )
    def window_mean(self, fold_axis, window_args, *args, **kwargs):
        return RollingDefault.register(pandas.core.window.Window.mean)(
            self, window_args, *args, **kwargs
        )

    @doc_utils.doc_window_method(
        win_type="window of the specified type",
        result="standard deviation",
        refer_to="std",
        params="""
        ddof : int, default: 1
        *args : iterable
        **kwargs : dict""",
    )
    def window_std(self, fold_axis, window_args, ddof=1, *args, **kwargs):
        return RollingDefault.register(pandas.core.window.Window.std)(
            self, window_args, ddof, *args, **kwargs
        )

    @doc_utils.doc_window_method(
        win_type="window of the specified type",
        result="sum",
        refer_to="sum",
        params="""
        *args : iterable
        **kwargs : dict""",
    )
    def window_sum(self, fold_axis, window_args, *args, **kwargs):
        return RollingDefault.register(pandas.core.window.Window.sum)(
            self, window_args, *args, **kwargs
        )

    @doc_utils.doc_window_method(
        win_type="window of the specified type",
        result="variance",
        refer_to="var",
        params="""
        ddof : int, default: 1
        *args : iterable
        **kwargs : dict""",
    )
    def window_var(self, fold_axis, window_args, ddof=1, *args, **kwargs):
        return RollingDefault.register(pandas.core.window.Window.var)(
            self, window_args, ddof, *args, **kwargs
        )

    # End of Window methods

    # Categories methods

    @doc_utils.add_one_column_warning
    @doc_utils.add_refer_to("Series.cat.codes")
    def cat_codes(self):
        """
        Convert underlying categories data into its codes.

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler containing the integer codes of the underlying
            categories.
        """
        return CatDefault.register(pandas.Series.cat.codes)(self)

    # End of Categories methods

    # DataFrame methods

    @doc_utils.doc_reduce_agg(
        method="mean absolute deviation",
        params="""
        axis : {0, 1}
        skipna : bool
        level : None, default: None
            Serves the compatibility purpose. Always has to be None.""",
        refer_to="mad",
    )
    def mad(self, axis, skipna, level=None):
        return DataFrameDefault.register(pandas.DataFrame.mad)(
            self, axis=axis, skipna=skipna, level=level
        )

    @doc_utils.doc_reduce_agg(
        method="unbiased kurtosis", refer_to="kurt", extra_params=["skipna", "**kwargs"]
    )
    def kurt(self, axis, level=None, numeric_only=None, skipna=True, **kwargs):
        return DataFrameDefault.register(pandas.DataFrame.kurt)(
            self, axis=axis, skipna=skipna, numeric_only=numeric_only, **kwargs
        )

    sum_min_count = sum
    prod_min_count = prod

    @doc_utils.add_refer_to("DataFrame.compare")
    def compare(self, other, align_axis, keep_shape, keep_equal, result_names):
        """
        Compare data of two QueryCompilers and highlight the difference.

        Parameters
        ----------
        other : BaseQueryCompiler
            Query compiler to compare with. Have to be the same shape and the same
            labeling as `self`.
        align_axis : {0, 1}
        keep_shape : bool
        keep_equal : bool
        result_names : tuple

        Returns
        -------
        BaseQueryCompiler
            New QueryCompiler containing the differences between `self` and passed
            query compiler.
        """
        return DataFrameDefault.register(pandas.DataFrame.compare)(
            self,
            other=other,
            align_axis=align_axis,
            keep_shape=keep_shape,
            keep_equal=keep_equal,
            result_names=result_names,
        )

    def repartition(self, axis=None):
        """
        Repartitioning QueryCompiler objects to get ideal partitions inside.

        Allows to improve performance where the query compiler can't improve
        yet by doing implicit repartitioning.

        Parameters
        ----------
        axis : {0, 1, None}, optional
            The axis along which the repartitioning occurs.
            `None` is used for repartitioning along both axes.

        Returns
        -------
        BaseQueryCompiler
            The repartitioned BaseQueryCompiler.
        """

        axes = [0, 1] if axis is None else [axis]

        new_query_compiler = self
        for _ax in axes:
            new_query_compiler = new_query_compiler.__constructor__(
                new_query_compiler._modin_frame.apply_full_axis(
                    _ax,
                    lambda df: df,
                    new_index=self._modin_frame._index_cache,
                    new_columns=self._modin_frame._columns_cache,
                    keep_partitioning=False,
                    sync_labels=False,
                )
            )
        return new_query_compiler

    # End of DataFrame methods
