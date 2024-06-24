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

# noqa: MD02
"""
Details about how Indexing Helper Class works.

_LocationIndexerBase provide methods framework for __getitem__
  and __setitem__ that work with Modin DataFrame's internal index. Base
  class's __{get,set}item__ takes in partitions & idx_in_partition data
  and perform lookup/item write.

_LocIndexer and _iLocIndexer is responsible for indexer specific logic and
  lookup computation. Loc will take care of enlarge DataFrame. Both indexer
  will take care of translating pandas' lookup to Modin DataFrame's internal
  lookup.

An illustration is available at
https://github.com/ray-project/ray/pull/1955#issuecomment-386781826
"""

import itertools
import numbers
from typing import Any, Callable, Optional, Union

import numpy as np
import pandas
from pandas._libs.tslibs import Resolution, parsing
from pandas._typing import AnyArrayLike, Scalar
from pandas.api.types import is_bool, is_list_like
from pandas.core.dtypes.common import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_integer,
    is_integer_dtype,
    is_numeric_dtype,
    pandas_dtype,
)
from pandas.core.indexing import IndexingError

import snowflake.snowpark.modin.pandas as pd
import snowflake.snowpark.modin.pandas.utils as frontend_utils
from snowflake.snowpark.modin.pandas.base import BasePandasDataset
from snowflake.snowpark.modin.pandas.dataframe import DataFrame
from snowflake.snowpark.modin.pandas.series import (
    SERIES_SETITEM_LIST_LIKE_KEY_AND_RANGE_LIKE_VALUE_ERROR_MESSAGE,
    SERIES_SETITEM_SLICE_AS_SCALAR_VALUE_ERROR_MESSAGE,
    Series,
)
from snowflake.snowpark.modin.pandas.utils import is_scalar
from snowflake.snowpark.modin.plugin._internal.indexing_utils import (
    MULTIPLE_ELLIPSIS_INDEXING_ERROR_MESSAGE,
    TOO_FEW_INDEXERS_INDEXING_ERROR_MESSAGE,
    TOO_MANY_INDEXERS_INDEXING_ERROR_MESSAGE,
)
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)
from snowflake.snowpark.modin.plugin.utils.error_message import ErrorMessage

INDEXING_KEY_TYPE = Union[Scalar, list, slice, Callable, tuple, AnyArrayLike]
INDEXING_ITEM_TYPE = Union[Scalar, AnyArrayLike, pd.Series, pd.DataFrame]
INDEXING_LOCATOR_TYPE = Union[Scalar, list, slice, tuple, pd.Series]

ILOC_SET_INDICES_MUST_BE_INTEGER_OR_BOOL_ERROR_MESSAGE = (
    "arrays used as indices must be of integer (or boolean) type"
)
ILOC_GET_REQUIRES_NUMERIC_INDEXERS_ERROR_MESSAGE = (
    ".{} requires numeric indexers, got {}"
)
LOC_SET_INCOMPATIBLE_INDEXER_WITH_DF_ERROR_MESSAGE = (
    "Incompatible indexer with DataFrame"
)
LOC_SET_INCOMPATIBLE_INDEXER_WITH_SERIES_ERROR_MESSAGE = (
    "Incompatible indexer with Series"
)
LOC_SET_INCOMPATIBLE_INDEXER_WITH_SCALAR_ERROR_MESSAGE = (
    "Scalar indexer incompatible with {} item"
)
SET_CELL_WITH_LIST_LIKE_VALUE_ERROR_MESSAGE = (
    "Currently do not support setting cell with list-like values"
)


ILOC_GET_DATAFRAME_INDEXER_NOT_ALLOWED_ERROR_MESSAGE = (
    "DataFrame indexer is not allowed for .iloc\nConsider using"
    " .loc for automatic alignment."
)


def is_boolean_array(x: Any) -> bool:
    """
    Check that argument is an array of bool.

    Parameters
    ----------
    x : object
        Object to check.

    Returns
    -------
    bool
        True if argument is an array of bool, False otherwise.
    """

    # special case empty list is not regarded as boolean array;
    # because of later Numpy versions (for Python 3.9+), can't
    # compare directly to [], but need workaround to detect list properly
    if isinstance(x, list) and 0 == len(x):
        return False

    if isinstance(x, (np.ndarray, Series, pandas.Series, pandas.Index)):
        # check dtype, if != object, no need to perform element-wise check
        if pandas_dtype(x.dtype) != pandas_dtype("object"):
            return is_bool_dtype(x.dtype)
    elif isinstance(x, (DataFrame, pandas.DataFrame)):
        return all(map(is_bool_dtype, x.dtypes))
    return is_list_like(x) and all(map(is_bool, x))


def is_2d_array(x: Any) -> bool:
    """
    Check that argument is a 2D array.

    Parameters
    ----------
    x : object
        Object to check.

    Returns
    -------
    bool
        True if argument is a 2D array, False otherwise.
    """
    return isinstance(x, (list, np.ndarray)) and len(x) > 0 and is_list_like(x[0])


def is_range_like(obj: Any) -> bool:
    """
    Check if the object is range-like.

    Objects that are considered range-like have information about the range (start and
    stop positions, and step) and also have to be iterable. Examples of range-like
    objects are: Python range, pandas.RangeIndex.

    Parameters
    ----------
    obj : object

    Returns
    -------
    bool
    """
    if not isinstance(obj, (DataFrame, Series)):
        return (
            hasattr(obj, "__iter__")
            and hasattr(obj, "start")
            and hasattr(obj, "stop")
            and hasattr(obj, "step")
        )
    else:
        # This would potentially have to change once RangeIndex is supported
        return False


def boolean_mask_to_numeric(indexer: Any) -> np.ndarray:
    """
    Convert boolean mask to numeric indices.

    Parameters
    ----------
    indexer : list-like of booleans

    Returns
    -------
    np.ndarray of ints
        Numerical positions of ``True`` elements in the passed `indexer`.
    """
    if isinstance(indexer, (np.ndarray, Series, pandas.Series)):
        return np.where(indexer)[0]
    else:
        # It's faster to build the resulting numpy array from the reduced amount of data via
        # `compress` iterator than convert non-numpy-like `indexer` to numpy and apply `np.where`.
        return np.fromiter(
            # `itertools.compress` masks `data` with the `selectors` mask,
            # works about ~10% faster than a pure list comprehension
            itertools.compress(data=range(len(indexer)), selectors=indexer),
            dtype=np.int64,
        )


def check_dict_or_set_indexers(key: Any) -> None:
    """
    Check if the indexer is or contains a dict or set, which is no longer allowed since pandas 2.0.
    Our error messages and types are the same as pandas 2.0.

    Raises
    ----------
    TypeError:
        If key is set or dict type or a tuple with any set or dict type item.
    """
    if (
        isinstance(key, set)
        or isinstance(key, tuple)
        and any(isinstance(x, set) for x in key)
    ):
        raise TypeError(
            "Passing a set as an indexer is not supported. Use a list instead."
        )

    if (
        isinstance(key, dict)
        or isinstance(key, tuple)
        and any(isinstance(x, dict) for x in key)
    ):
        raise TypeError(
            "Passing a dict as an indexer is not supported. Use a list instead."
        )


def validate_positional_slice(slice_key: Any) -> None:
    """
    Validate slice start, stop, and step are int typed.

    Parameters
    ----------
    slice_key : slice or is_range_like

    Raises
    ----------
    TypeError:
        If the start, stop, or step of slice_key is not None and is not integer.
    """
    for key in [slice_key.start, slice_key.stop, slice_key.step]:
        if key is not None and not is_integer(key):
            raise TypeError(
                f"cannot do positional indexing with these indexers [{key}] of type {type(key).__name__}"
            )


def validate_key_for_single_dim_for_at_iat(
    modin_df: BasePandasDataset, key: INDEXING_KEY_TYPE, for_at: bool, axis: int
) -> None:
    """
    Validate key is suitable for a single dimension when calling modin_df.at or modin_df.iat.

    Parameters
    ----------
    modin_df : BasePandasDataset
        DataFrame to operate on.
    key: INDEXING_KEY_TYPE
        indexing key.
    for_at: bool
        True when 'key' is to be passed to the 'at' method.
        Otherwise, 'key' is to be passed to the 'iat' method.
    axis: int
        Specifies the dimension to validate 'key' for.
    """
    if for_at and modin_df._query_compiler.has_multiindex(axis=axis):
        if not isinstance(key, tuple):
            raise IndexingError(TOO_FEW_INDEXERS_INDEXING_ERROR_MESSAGE)
        else:
            if len(key) < modin_df._query_compiler.nlevels(axis=axis):
                raise IndexingError(TOO_FEW_INDEXERS_INDEXING_ERROR_MESSAGE)
            elif len(key) > modin_df._query_compiler.nlevels(axis=axis):
                raise IndexingError(TOO_MANY_INDEXERS_INDEXING_ERROR_MESSAGE)
    else:
        if isinstance(key, tuple) and len(key) == 1:
            key = key[0]
        if not is_scalar(key):
            raise KeyError(key)


def validate_key_for_at_iat(
    modin_df: BasePandasDataset, key: INDEXING_KEY_TYPE, for_at: bool
) -> None:
    """
    Validate key is suitable for modin_df.at or modin_df.iat.

    Parameters
    ----------
    modin_df : BasePandasDataset
        DataFrame to operate on.
    key: INDEXING_KEY_TYPE
        indexing key.
    for_at: bool
        True when 'key' is to be passed to the 'at' method.
        Otherwise, 'key' is to be passed to the 'iat' method.
    """
    if modin_df.ndim == 1:
        validate_key_for_single_dim_for_at_iat(
            modin_df=modin_df, key=key, for_at=for_at, axis=0
        )
    else:
        assert modin_df.ndim == 2
        if not isinstance(key, tuple):
            raise IndexingError(TOO_FEW_INDEXERS_INDEXING_ERROR_MESSAGE)
        else:
            if len(key) < 2:
                raise IndexingError(TOO_FEW_INDEXERS_INDEXING_ERROR_MESSAGE)
            elif len(key) > 2:
                raise IndexingError(TOO_MANY_INDEXERS_INDEXING_ERROR_MESSAGE)
            else:
                row_loc = key[0]
                col_loc = key[1]
                validate_key_for_single_dim_for_at_iat(
                    modin_df=modin_df, key=row_loc, for_at=for_at, axis=0
                )
                validate_key_for_single_dim_for_at_iat(
                    modin_df=modin_df, key=col_loc, for_at=for_at, axis=1
                )


class _LocationIndexerBase:
    """
    Base class for location indexer like loc and iloc.

    Parameters
    ----------
    modin_df : modin.pandas.DataFrame
        DataFrame to operate on.
    """

    api_name = "undefined"

    def __init__(self, modin_df: BasePandasDataset) -> None:
        # TODO: SNOW-1063351: Modin upgrade - modin.pandas.indexing._LocationIndexerBase
        self.df = modin_df
        self.qc = modin_df._query_compiler

    def _validate_key_length_with_ellipsis_stripping(self, key: tuple) -> tuple:
        """
        Validate tuple type key's length and strip leading ellipsis.

        If tuple length is no greater than ndim of DataFrame df: return key
        Else:
            If the first entry is ellipsis, strip leading ellipsis and call this function
        on the remaining tuple again.
            Else raise IndexingError.

        e.g. (..., 2 , 3) is reduced to (2 , 3); (..., 3) is reduced to (3,)
        """
        if len(key) > self.df.ndim:
            if key[0] is Ellipsis:
                # e.g. Series.iloc[..., 3] reduces to just Series.iloc[3]
                key = key[1:]
                if Ellipsis in key:
                    raise IndexingError(MULTIPLE_ELLIPSIS_INDEXING_ERROR_MESSAGE)
                return self._validate_key_length_with_ellipsis_stripping(key)
            raise IndexingError(TOO_MANY_INDEXERS_INDEXING_ERROR_MESSAGE)
        return key

    def __getitem__(self, key: INDEXING_KEY_TYPE) -> None:  # pragma: no cover
        """
        Retrieve dataset according to `key`.

        Parameters
        ----------
        key : callable, scalar, or tuple
            The global row index to retrieve data from.

        Returns
        -------
        modin.pandas.DataFrame or modin.pandas.Series
            Located dataset.

        See Also
        --------
        pandas.DataFrame.loc
        """
        # TODO: SNOW-1063351: Modin upgrade - modin.pandas.indexing._LocationIndexerBase
        ErrorMessage.not_implemented("Implemented by subclasses")

    def __setitem__(
        self, key: INDEXING_KEY_TYPE, item: INDEXING_ITEM_TYPE
    ) -> None:  # pragma: no cover
        """
        Assign `item` value to dataset located by `key`.

        Parameters
        ----------
        key : callable or tuple
            The global row numbers to assign data to.
        item : modin.pandas.DataFrame, modin.pandas.Series or scalar
            Value that should be assigned to located dataset.

        See Also
        --------
        pandas.DataFrame.iloc
        """
        # TODO: SNOW-1063351: Modin upgrade - modin.pandas.indexing._LocationIndexerBase
        ErrorMessage.not_implemented("Implemented by subclasses")

    def _should_squeeze(
        self,
        locator: Union[Scalar, list, slice, tuple, pd.Series],
        axis: int,
    ) -> Optional[bool]:
        """
        The method helps to make the decision whether squeeze is needed to get the final pandas object. Specifically,
        squeeze is needed:
        - if self is series and axis = 1
        - if the locator are not scalar and tuple
        Otherwise, the decision is not sure (return None)

        Args:
            locator: locator on the axis
            axis: the axis to check

        Returns:
            A tuple of boolean values to indicate whether to squeeze on the two axis.
        """
        # TODO: SNOW-1063351: Modin upgrade - modin.pandas.indexing._LocationIndexerBase
        if axis == 1 and isinstance(self.df, Series):
            # squeeze col is always False for Series
            return False

        not_dataset = not isinstance(locator, BasePandasDataset)
        is_scalar_loc = not_dataset and is_scalar(locator)
        is_tuple_loc = not_dataset and isinstance(locator, tuple)

        if not is_scalar_loc and not is_tuple_loc:
            # no need to squeeze if any axis key are not scalar or tuple
            return False

        # otherwise, not sure
        return None

    def _get_pandas_object_from_qc_view(
        self,
        qc_view: SnowflakeQueryCompiler,
        *,
        squeeze_row: bool,
        squeeze_col: bool,
    ) -> Union[Scalar, pd.Series, pd.DataFrame]:
        """
        Convert the query compiler view to the appropriate pandas object. The method helps to call squeeze to get the
        final pandas object.
        Args:
            qc_view: SnowflakeQueryCompiler
                Query compiler to convert.
            squeeze_row: bool
                Whether to squeeze row
            squeeze_col: bool
                Whether to squeeze column

        Returns: DataFrame, Series or Scalar
            The pandas object with the data from the query compiler view.
        """
        # TODO: SNOW-1063351: Modin upgrade - modin.pandas.indexing._LocationIndexerBase
        res_df = self.df.__constructor__(query_compiler=qc_view)

        if not squeeze_row and not squeeze_col:
            return res_df

        if squeeze_row and squeeze_col:
            axis = None
        elif squeeze_row:
            axis = 0
        else:
            axis = 1
        return res_df.squeeze(axis=axis)

    def _parse_row_and_column_locators(
        self, key: INDEXING_KEY_TYPE
    ) -> tuple[INDEXING_LOCATOR_TYPE, INDEXING_LOCATOR_TYPE]:
        """
        Unpack the user input. This shared parsing helper method is used by both iloc and loc's getitem and setitem.

        Examples:
            loc[:] -> (slice(None), slice(None))
            loc[a] -> (a, slice(None))
            loc[,b] -> (slice(None), b)
            loc[a,:] -> (a, slice(None))
            loc[:,b] -> (slice(None), b)
            loc[a,...] -> (a, slice(None))
            loc[...,b] -> (slice(None), b)
            loc[[a,b]] -> ([a,b], slice(None)),
            loc[a,b] -> ([a], [b])
            loc[...,a,b] -> ([a], [b])
            loc[lambda df: df.col > 0,b] -> (df.col > 0, [b])
            (same for iloc too)

        Args:
            key: User input to unpack.

        Returns:
            row_loc : scalar or list
                Row locator(s) as a scalar or list.
            col_loc : scalar or list
                Column locator(s) as a scalar or list.

        Raises:
            index error if key is tuple(...,...)
        """
        # TODO: SNOW-1063351: Modin upgrade - modin.pandas.indexing._LocationIndexerBase
        row_loc: INDEXING_LOCATOR_TYPE = slice(None)
        col_loc: INDEXING_LOCATOR_TYPE = slice(None)
        if isinstance(key, tuple):
            key = self._validate_key_length_with_ellipsis_stripping(key)
            if len(key) > 2:
                raise IndexingError(TOO_MANY_INDEXERS_INDEXING_ERROR_MESSAGE)
            if len(key) > 0:
                row_loc = key[0]
            if len(key) == 2:
                if key[0] is Ellipsis and key[1] is Ellipsis:
                    raise IndexingError(MULTIPLE_ELLIPSIS_INDEXING_ERROR_MESSAGE)
                col_loc = key[1]
        else:
            row_loc = key

        def _parse_locator(_key: INDEXING_LOCATOR_TYPE) -> INDEXING_LOCATOR_TYPE:
            # Ellipsis to slice(None)
            if _key is Ellipsis:
                return slice(None)
            # callable will be evaluated to use the result as locator
            if callable(_key):
                _key = _key(self.df)
            return _key

        return _parse_locator(row_loc), _parse_locator(col_loc)

    def _parse_get_row_and_column_locators(
        self, key: INDEXING_KEY_TYPE
    ) -> tuple[
        Union[Scalar, list, slice, tuple, pd.Series],
        Union[Scalar, list, slice, tuple, pd.Series],
    ]:
        """Used by loc and iloc.  See _LocationIndexerBase._parse_row_and_column_locators"""
        # TODO: SNOW-1063351: Modin upgrade - modin.pandas.indexing._LocationIndexerBase
        row_key, col_key = self._parse_row_and_column_locators(key)
        self._validate_get_locator_key(row_key)
        self._validate_get_locator_key(col_key)

        return row_key, col_key

    def _parse_set_row_and_column_locators(
        self, key: INDEXING_KEY_TYPE
    ) -> tuple[
        Union[Scalar, list, slice, tuple, pd.Series],
        Union[Scalar, list, slice, tuple, pd.Series],
    ]:
        """Used by loc and iloc.  See _LocationIndexerBase._parse_row_and_column_locators"""
        # TODO: SNOW-1063351: Modin upgrade - modin.pandas.indexing._LocationIndexerBase
        row_key, col_key = self._parse_row_and_column_locators(key)
        self._validate_set_locator_key(row_key)
        self._validate_set_locator_key(col_key)

        return row_key, col_key

    def _is_multiindex_full_lookup(
        self, axis: int, key: Union[Scalar, list, slice, tuple, pd.Series]
    ) -> bool:
        """
        Determine if the key will perform a full lookup for MultiIndex. "Multiindex full lookup" is True only when the
        axis is MultiIndex and the key is a tuple and the number of levels matches up with the length of the tuple key.
        When it is True, pandas will drop all levels from the multiindex axis and call squeeze on the axis.

        Examples:
            if self has a three level multiindex ["l0","l1","l2], then key has to be a tuple with length equals to 3 to
            perform a multiindex full lookup.

        Args:
            axis: {0, 1}
                0 for row, 1 for column.
            key: Scalar, tuple, or other list like
                Lookup key for MultiIndex row/column.

        Returns: bool
            True if the key will perform a full lookup for the MultiIndex.

        """
        # TODO: SNOW-1063351: Modin upgrade - modin.pandas.indexing._LocationIndexerBase
        if not self.qc.has_multiindex(axis=axis):
            return False

        if not isinstance(key, tuple):
            return False

        if any(isinstance(key_level, slice) for key_level in key):
            # do not squeeze if any level of the key is a slice
            return False

        return len(key) == self.qc.nlevels(axis)

    def _validate_locator_key(self, key: INDEXING_KEY_TYPE) -> None:
        """Validate indexing key type.

        Parameters
        ----------
        key: indexing key

        Raises
        ------
        TypeError:
            native pandas object.
            set or dict.
            all other types out of scalar, list like, slice, series, or, index.
            For iloc, raise if scalar is not integer
        IndexingError:
            tuple.
        ValueError:
            SnowDataFrame.
        """
        # TODO: SNOW-1063351: Modin upgrade - modin.pandas.indexing._LocationIndexerBase
        frontend_utils.raise_if_native_pandas_objects(key)
        check_dict_or_set_indexers(key)

        if not (
            is_scalar(key)
            or isinstance(key, (pd.Series, slice))
            or is_list_like(key)
            or is_range_like(key)
        ):
            raise TypeError(
                f".{self.api_name} requires scalars, list-like indexers, slices, or ranges. Got {key}"
            )

    def _validate_get_locator_key(self, key: INDEXING_KEY_TYPE) -> None:
        """
        Helper function to validate the locator key for get is valid.

        Parameter:
        ----------
        key: get locator key

        """
        # TODO: SNOW-1063351: Modin upgrade - modin.pandas.indexing._LocationIndexerBase
        self._validate_locator_key(key)

    def _validate_set_locator_key(self, key: INDEXING_KEY_TYPE) -> None:
        """
        Helper function to validate the locator key for set is valid.

        Parameter:
        ----------
        key: set locator key

        """
        # TODO: SNOW-1063351: Modin upgrade - modin.pandas.indexing._LocationIndexerBase
        self._validate_locator_key(key)


class _LocIndexer(_LocationIndexerBase):
    """
    An indexer for modin_df.loc[] functionality.

    Parameters
    ----------
    modin_df : modin.pandas.DataFrame
        DataFrame to operate on.
    """

    api_name = "loc"

    def _should_squeeze(
        self,
        locator: Union[Scalar, list, slice, tuple, pd.Series],
        axis: int,
    ) -> bool:
        """
        The method helps to make the decision whether squeeze is needed to get the final pandas object. Specifically,
        squeeze is needed:
        - if self is series and axis = 1
        - if the locator are not scalar and tuple
        - if the locator is scalar but on a multiindex
        - if it is a multiindex full lookup, i.e., an exact match on the multiindex

        Args:
            locator: locator on the axis
            axis: the axis to check

        Returns:
            A tuple of boolean values to indicate whether to squeeze on the two axis.
        """
        # TODO: SNOW-1063352: Modin upgrade - modin.pandas.indexing._LocIndexer
        do_squeeze = super()._should_squeeze(locator, axis)
        if do_squeeze is not None:
            return do_squeeze

        not_dataset = not isinstance(locator, BasePandasDataset)
        is_scalar_loc = not_dataset and is_scalar(locator)
        is_tuple_loc = not_dataset and isinstance(locator, tuple)

        if (is_scalar_loc or is_tuple_loc) and not self.qc.is_multiindex(axis=axis):
            # for single index, if the locator is scalar or tuple, then squeeze is needed
            return True

        if self._is_multiindex_full_lookup(axis=axis, key=locator):
            # for multiindex, squeeze is needed only when full lookup happens, i.e., exact match on all levels.
            return True

        # otherwise, no squeeze is needed
        return False

    def _parse_row_and_column_locators(
        self, key: INDEXING_KEY_TYPE
    ) -> tuple[
        Union[Scalar, list, slice, tuple, pd.Series],
        Union[Scalar, list, slice, tuple, pd.Series],
    ]:
        """
        Unpack the user input. This shared parsing helper method is used by both iloc and loc's getitem and setitem.

        Examples:
            loc[:] -> (slice(None), slice(None))
            loc[a] -> (a, slice(None))
            loc[,b] -> (slice(None), b)
            loc[a,:] -> (a, slice(None))
            loc[:,b] -> (slice(None), b)
            loc[a,...] -> (a, slice(None))
            loc[...,b] -> (slice(None), b)
            loc[[a,b]] -> ([a,b], slice(None)),
            loc[a,b] -> ([a], [b])
            loc[...,a,b] -> ([a], [b])
            loc[lambda df: df.col > 0,b] -> (df.col > 0, [b])
            Also, for multiindex cases used by loc:
            loc[("level0", "level1")] -> (("level0", "level1"), slice(None))

        Args:
            key: User input to unpack.

        Returns:
            row_loc : scalar or list
                Row locator(s) as a scalar or list.
            col_loc : scalar or list
                Column locator(s) as a scalar or list.

        Raises:
            index error if key is tuple(...,...)
        """
        # TODO: SNOW-1063352: Modin upgrade - modin.pandas.indexing._LocIndexer
        if isinstance(key, tuple):
            is_nested_tuple = any([not is_scalar(k) for k in key])
            if (
                self.qc.is_multiindex(axis=0)
                and not is_nested_tuple
                and not (self.df.ndim == 2 and self.qc.is_multiindex(axis=1))
            ):
                # always treat tuple loc key as row_loc when the key is not nested tuple and the frame is a Series or
                # the frame's column is not multiindex
                # e.g., df.loc['cobra', 'mark i'], key = ('cobra', 'mark i') should be treated as row_loc if the row is
                # multiindex or the frame is a Series
                row_loc = key
                if len(row_loc) > self.qc.nlevels(axis=0):
                    raise IndexingError(TOO_MANY_INDEXERS_INDEXING_ERROR_MESSAGE)
                return row_loc, slice(None)

        return super()._parse_row_and_column_locators(key)

    def _locator_type_convert(
        self, locator: INDEXING_LOCATOR_TYPE
    ) -> Union[INDEXING_LOCATOR_TYPE, "SnowflakeQueryCompiler"]:
        """
        A helper function to convert locator type before passing to the backend
        Args:
            locator: row or column locator

        Returns:
            Processed locator
        """
        # TODO: SNOW-1063352: Modin upgrade - modin.pandas.indexing._LocIndexer
        if isinstance(locator, pd.Series):
            locator = locator._query_compiler
        elif not isinstance(locator, slice) and is_range_like(locator):
            locator = slice(locator.start, locator.stop, locator.step)  # type: ignore[union-attr]
        return locator

    def _try_partial_string_indexing(
        self, row_loc: Union[Scalar, list, slice, tuple, pd.Series]
    ) -> Union[Scalar, list, slice, tuple, pd.Series]:
        """
        Try to convert row locator to slice if it matches partial string indexing criteria:
            1. `row_loc` needs to be a valid datetime string
            2. the index is datetime type

        Args:
            row_loc: the original row locator

        Returns:
            the new row locator for partial string indexing; otherwise, the original row locator
        """
        # TODO: SNOW-1063352: Modin upgrade - modin.pandas.indexing._LocIndexer

        def _try_partial_string_indexing_for_string(
            row_loc: str,
        ) -> Union[Scalar, list, slice, tuple, pd.Series]:
            """
            Convert string `row_loc` into slice if it matches the partial string indexing criteria. Otherwise, return
            the original `row_loc`.

            Args:
                row_loc: input

            Returns:
                slice or the original `row_loc`
            """
            # TODO: SNOW-1063352: Modin upgrade - modin.pandas.indexing._LocIndexer
            try:
                parsed, reso_str = parsing.parse_datetime_string_with_reso(row_loc)
            except ValueError:
                return row_loc

            # extract tzinfo first since Period will drop tzinfo later; then the tzinfo will be added back when
            # assembling the final slice
            tzinfo = parsed.tzinfo
            reso = Resolution.from_attrname(reso_str)
            period = pd.Period(parsed, freq=reso.attr_abbrev)

            # partial string indexing only works for DatetimeIndex
            if is_datetime64_any_dtype(self.df._query_compiler.index_dtypes[0]):
                return slice(
                    pd.Timestamp(period.start_time, tzinfo=tzinfo),
                    pd.Timestamp(period.end_time, tzinfo=tzinfo),
                )

            return row_loc

        if isinstance(row_loc, str):
            return _try_partial_string_indexing_for_string(row_loc)

        if isinstance(row_loc, slice):
            start, stop = row_loc.start, row_loc.stop
            if isinstance(row_loc.start, str):
                start = _try_partial_string_indexing_for_string(row_loc.start)
                if isinstance(start, slice):
                    start = start.start
            if isinstance(row_loc.stop, str):
                stop = _try_partial_string_indexing_for_string(row_loc.stop)
                if isinstance(stop, slice):
                    stop = stop.stop
            # partial string indexing only updates start and stop, and should keep using the original step.
            row_loc = slice(start, stop, row_loc.step)

        return row_loc

    def __getitem__(
        self, key: INDEXING_KEY_TYPE
    ) -> Union[Scalar, pd.Series, pd.DataFrame]:
        """
        Retrieve dataset according to `key`.

        Parameters
        ----------
        key : callable, scalar, list-like, boolean mask, Snowpark pandas Series, slice, or size-two tuple of these
            The 2D locator.

        Returns
        -------
        modin.pandas.DataFrame or modin.pandas.Series
            Located dataset.

        See Also
        --------
        pandas.DataFrame.loc
        """
        # TODO: SNOW-1063352: Modin upgrade - modin.pandas.indexing._LocIndexer
        row_loc, col_loc = self._parse_get_row_and_column_locators(key)
        row_loc = self._try_partial_string_indexing(row_loc)
        squeeze_row, squeeze_col = self._should_squeeze(
            locator=row_loc, axis=0
        ), self._should_squeeze(locator=col_loc, axis=1)

        qc_view = self.qc.take_2d_labels(
            self._locator_type_convert(row_loc), self._locator_type_convert(col_loc)
        )

        result = self._get_pandas_object_from_qc_view(
            qc_view, squeeze_row=squeeze_row, squeeze_col=squeeze_col
        )
        if isinstance(result, Series):
            result._parent = self.df
            result._parent_axis = 0

        return result

    def _loc_set_matching_item_columns_by_label(
        self, key: INDEXING_KEY_TYPE, item: INDEXING_ITEM_TYPE
    ) -> bool:
        """
        Decide whether loc set behavior is to match item columns by label or by position.
        Note: loc set's behavior is different when key is a tuple of row and col keys vs. key is a row key only. When
        key is tuple (e.g., df.loc[row_key, col_key] = item), only ``item``'s column labels that match with col_key are
        used to set df values; otherwise, (e.g., df.loc[row_key_only] = item), loc set columns based on ``item``'s
        column positions not labels. E.g., df has columns ["A", "B", "C"] and item has columns ["C", "B", "A"],
        df.loc[:] = item will update df's columns "A", "B", "C" using item column "C", "B", "A" respectively.
        TODO: SNOW-972417 pandas has some complicated logic to use dtypes from both self df and item to decide whether
        the loc set behavior for df.loc[row_key, col_key] = item) is matching by label or not. Further effort is needed
        to decide what the right behavior for Snowpark pandas.

        Args:
            key: loc key
            item: the RHS in loc set

        Returns:
            True if matching item by label
        """
        # TODO: SNOW-1063352: Modin upgrade - modin.pandas.indexing._LocIndexer
        if is_2d_array(item):
            return False
        return (
            isinstance(self.df, pd.DataFrame)
            and isinstance(key, tuple)
            and not is_scalar(
                key[1]
            )  # e.g., df.loc[:, 'A'] = item is matching item by position
            and isinstance(item, pd.DataFrame)
        )

    def __setitem__(
        self,
        key: INDEXING_KEY_TYPE,
        item: INDEXING_ITEM_TYPE,
    ) -> None:
        """
        Assign `item` value to dataset located by label `key`.

        Args:
            key: indexing key type
            item: indexing item type

        See Also:
        DataFrame.loc
        """
        # TODO: SNOW-1063352: Modin upgrade - modin.pandas.indexing._LocIndexer
        row_loc, col_loc = self._parse_row_and_column_locators(key)

        # TODO SNOW-962260 support multiindex
        if self.qc.is_multiindex(axis=0) or self.qc.is_multiindex(axis=1):
            ErrorMessage.not_implemented(
                f".{self.api_name} set for multiindex is not yet implemented"
            )

        self._validate_item_type(item, row_loc)

        # If the row key is list-like (Index, list, np.ndarray, etc.), convert it to Series.
        if not isinstance(row_loc, pd.Series) and is_list_like(row_loc):
            row_loc = pd.Series(row_loc)

        matching_item_columns_by_label = self._loc_set_matching_item_columns_by_label(
            key, item
        )
        item_is_2d_array = is_2d_array(item)
        matching_item_rows_by_label = not item_is_2d_array

        index_is_bool_indexer = isinstance(
            row_loc, BasePandasDataset
        ) and is_bool_dtype(row_loc.dtypes)

        index = (
            row_loc._query_compiler
            if isinstance(row_loc, BasePandasDataset)
            else row_loc
        )
        columns = (
            col_loc._query_compiler
            if isinstance(col_loc, BasePandasDataset)
            else col_loc
        )
        if item_is_2d_array:
            item = pd.DataFrame(item)
        item = item._query_compiler if isinstance(item, BasePandasDataset) else item
        new_qc = self.qc.set_2d_labels(
            index,
            columns,
            item,
            matching_item_columns_by_label=matching_item_columns_by_label,
            matching_item_rows_by_label=matching_item_rows_by_label,
            index_is_bool_indexer=index_is_bool_indexer,
        )

        self.df._update_inplace(new_query_compiler=new_qc)

    def _validate_locator_key(self, key: INDEXING_KEY_TYPE) -> None:
        """Used by loc.  See LocationIndexerBase._validate_locator_key"""
        # TODO: SNOW-1063352: Modin upgrade - modin.pandas.indexing._LocIndexer
        super()._validate_locator_key(key)
        if isinstance(key, pd.DataFrame):
            raise ValueError("Cannot index with multidimensional key")

    def _validate_item_type(
        self,
        item: INDEXING_ITEM_TYPE,
        row_loc: Union[Scalar, list, slice, tuple, AnyArrayLike],
    ) -> None:
        """
        Validate item data type for loc set. Raise error if the type is invalid.
        Args:
            item: the item to set
            row_loc: row locator

        Returns:
            None
        """
        # TODO: SNOW-1063352: Modin upgrade - modin.pandas.indexing._LocIndexer
        frontend_utils.raise_if_native_pandas_objects(item)

        if isinstance(self.df, pd.Series):
            if isinstance(item, pd.DataFrame):
                raise ValueError(LOC_SET_INCOMPATIBLE_INDEXER_WITH_DF_ERROR_MESSAGE)
            elif is_scalar(row_loc) and (
                isinstance(item, pd.Series) or is_list_like(item)
            ):
                ErrorMessage.not_implemented(
                    SET_CELL_WITH_LIST_LIKE_VALUE_ERROR_MESSAGE
                )
        else:
            if is_scalar(row_loc) and (
                isinstance(item, pd.DataFrame) or is_2d_array(item)
            ):
                raise ValueError(
                    LOC_SET_INCOMPATIBLE_INDEXER_WITH_SCALAR_ERROR_MESSAGE.format(
                        item.__class__.__name__
                    )
                )

        if (isinstance(row_loc, pd.Series) or is_list_like(row_loc)) and (
            isinstance(item, range)
        ):
            ErrorMessage.not_implemented(
                SERIES_SETITEM_LIST_LIKE_KEY_AND_RANGE_LIKE_VALUE_ERROR_MESSAGE
            )

        if isinstance(item, slice):
            # Here, the whole slice is assigned as a scalar variable, i.e., a spot at an index gets a slice value.
            ErrorMessage.not_implemented(
                SERIES_SETITEM_SLICE_AS_SCALAR_VALUE_ERROR_MESSAGE
            )


class _iLocIndexer(_LocationIndexerBase):
    """
    An indexer for modin_df.iloc[] functionality.

    Parameters
    ----------
    modin_df : modin.pandas.DataFrame
        DataFrame to operate on.
    """

    api_name = "iloc"

    def _should_squeeze(
        self,
        locator: Union[Scalar, list, slice, tuple, pd.Series],
        axis: int,
    ) -> bool:
        """
        The method helps to make the decision whether squeeze is needed to get the final pandas object. Specifically,
        squeeze is needed:
        - if self is series and axis = 1
        - if the locator are not scalar and tuple
        - if the locator is scalar

        Args:
            locator: locator on the axis
            axis: the axis to check

        Returns:
            A tuple of boolean values to indicate whether to squeeze on the two axis.
        """
        # TODO: SNOW-1063355: Modin upgrade - modin.pandas.indexing._iLocIndexer
        do_squeeze = super()._should_squeeze(locator, axis)
        if do_squeeze is not None:
            return do_squeeze

        not_dataset = not isinstance(locator, BasePandasDataset)
        is_scalar_loc = not_dataset and is_scalar(locator)
        if is_scalar_loc:
            return True

        # otherwise, no squeeze is needed
        return False

    @staticmethod
    def _convert_range_to_valid_slice(range_key: Any) -> slice:
        # TODO: SNOW-1063355: Modin upgrade - modin.pandas.indexing._iLocIndexer
        start, stop, step = range_key.start, range_key.stop, range_key.step
        # range has different logic from slice: slice can handle cases where (start > stop and step > 0)
        # and (start < stop and step < 0) but range has an empty result for this. For example, slice(3, -1, 1)
        # and slice(-1, 0, -1) are not empty results but range is.
        if (start > stop and step > 0) or (start < stop and step < 0):
            return slice(0, 0, 1)
        else:
            return slice(start, stop, step)

    def __getitem__(
        self,
        key: INDEXING_KEY_TYPE,
    ) -> Union[Scalar, pd.DataFrame, pd.Series]:
        """
        Retrieve dataset according to positional `key`.

        Args:
            key: int, bool, list like of int or bool, slice of int, series, callable or tuple
                The global row numbers to retrieve data from.

        Returns:
            DataFrame, Series, or scalar.
        """
        # TODO: SNOW-1063355: Modin upgrade - modin.pandas.indexing._iLocIndexer
        row_loc, col_loc = self._parse_get_row_and_column_locators(key)
        squeeze_row = self._should_squeeze(locator=row_loc, axis=0)
        squeeze_col = self._should_squeeze(locator=col_loc, axis=1)

        original_row_loc = row_loc  # keep a copy for error message

        # Convert range to slice objects.
        if not isinstance(row_loc, pd.Series) and is_range_like(row_loc):
            row_loc = self._convert_range_to_valid_slice(row_loc)
        if not isinstance(col_loc, pd.Series) and is_range_like(col_loc):
            col_loc = self._convert_range_to_valid_slice(col_loc)

        # Convert all scalar, list-like, and indexer row_loc to a Series object to get a query compiler object.
        if is_scalar(row_loc):
            row_loc = pd.Series([row_loc])
        elif is_list_like(row_loc):
            if hasattr(row_loc, "dtype"):
                dtype = row_loc.dtype
            elif not row_loc:
                # If the list-like object is empty, we need to explicitly specify a dtype
                dtype = float
            else:
                dtype = None
            row_loc = pd.Series(row_loc, dtype=dtype)

        # Check whether the row and column input is of numeric dtype.
        self._validate_numeric_get_key_values(row_loc, original_row_loc)
        self._validate_numeric_get_key_values(col_loc)

        if isinstance(row_loc, pd.Series):
            # Get the corresponding query compiler object.
            row_loc = row_loc._query_compiler

        if isinstance(col_loc, pd.Series):
            col_loc = col_loc.to_list()

        qc_view = self.qc.take_2d_positional(row_loc, col_loc)
        result = self._get_pandas_object_from_qc_view(
            qc_view,
            squeeze_row=squeeze_row,
            squeeze_col=squeeze_col,
        )

        if isinstance(result, Series):
            result._parent = self.df
            result._parent_axis = 0
        return result

    def _get_pandas_object_from_qc_view(
        self,
        qc_view: SnowflakeQueryCompiler,
        *,
        squeeze_row: bool,
        squeeze_col: bool,
    ) -> Union[Scalar, list, pd.Series, pd.DataFrame]:
        """
        Convert the query compiler view to the appropriate pandas object.

        Args:
            qc_view: SnowflakeQueryCompiler
                Query compiler to convert.
            squeeze_row: bool
                Whether to squeeze row
            squeeze_col: bool
                Whether to squeeze column

        Returns: DataFrame, Series or Scalar
            The pandas object with the data from the query compiler view.
        """
        # TODO: SNOW-1063355: Modin upgrade - modin.pandas.indexing._iLocIndexer
        res_df = self.df.__constructor__(query_compiler=qc_view)

        if not squeeze_row and not squeeze_col:
            return res_df

        if squeeze_row and squeeze_col:
            res = res_df.to_pandas().squeeze()
            # res can be an empty pandas series where the key is out of bounds, here we convert to an empty list to
            # avoid return a native pandas object
            if isinstance(res, pandas.Series):
                res = []
            return res

        if squeeze_row:
            if isinstance(res_df, pd.Series):
                # call _reduce_dimension directly instead of calling series.squeeze() to avoid to call len(res_df)
                res = res_df._reduce_dimension(res_df._query_compiler)
                # res can be an empty pandas series where the key is out of bounds, here we convert to an empty list to
                # avoid return a native pandas object
                if isinstance(res, pandas.Series):
                    res = []
                return res
            return Series(query_compiler=res_df.T._query_compiler)

        # where only squeeze_col is True
        len_columns = len(res_df.columns)
        if len_columns == 1:
            return Series(query_compiler=res_df._query_compiler)
        else:
            return res_df.copy()

    def __setitem__(
        self,
        key: INDEXING_KEY_TYPE,
        item: INDEXING_ITEM_TYPE,
    ) -> None:
        """
        Assign `item` value to dataset located by `key`.

        Parameters
        ----------
        key : callable or tuple
            The global row numbers to assign data to.
        item : modin.pandas.DataFrame, modin.pandas.Series, scalar or list like of similar
            Value that should be assigned to located dataset.
        """
        # TODO: SNOW-1063355: Modin upgrade - modin.pandas.indexing._iLocIndexer
        row_loc, col_loc = self._parse_set_row_and_column_locators(key)

        self._validate_numeric_set_key_values(row_loc)
        self._validate_numeric_set_key_values(col_loc)

        is_item_series = isinstance(item, pd.Series)

        if not isinstance(item, BasePandasDataset) and is_list_like(item):
            if isinstance(self.df, pd.Series) and is_scalar(row_loc):
                ErrorMessage.not_implemented(
                    SET_CELL_WITH_LIST_LIKE_VALUE_ERROR_MESSAGE
                )

            if isinstance(item, pd.Index):
                item = np.array(item.tolist()).transpose()
            else:
                item = np.array(item)

            if all(sz == 1 for sz in item.shape):
                # Treat as a scalar if a single value regardless of dimensions
                item = item.flatten()[0]
            else:
                if item.ndim == 1:
                    item = pd.Series(item)
                    is_item_series = True
                else:
                    item = pd.DataFrame(item)

        is_row_key_df = isinstance(row_loc, pd.DataFrame)
        is_col_key_df = isinstance(col_loc, pd.DataFrame)

        # The semantics of iloc setitem differ if the row and col key are both
        # tuples or dataframes, in particular they set as row, key location coordinates
        # rather than entire rows or columns.  So for example
        #
        # row_key=[1,2] and col_key=[3,4] would be locations (1,3), (1,4), (2,3), (2,4)
        # but
        # row_key=(1,2) and col_key=(3,4) would only set locations (1,3), (2, 4).

        if not is_row_key_df and not is_col_key_df:
            set_as_coords = isinstance(row_loc, tuple) or isinstance(col_loc, tuple)
        else:
            set_as_coords = is_row_key_df and is_col_key_df

        new_qc = self.qc.set_2d_positional(
            row_loc._query_compiler
            if isinstance(row_loc, BasePandasDataset)
            else row_loc,
            col_loc._query_compiler
            if isinstance(col_loc, BasePandasDataset)
            else col_loc,
            item._query_compiler if isinstance(item, BasePandasDataset) else item,
            set_as_coords,
            is_item_series,
        )

        self.df._create_or_update_from_compiler(new_qc, inplace=True)

    def _validate_locator_key(self, key: INDEXING_KEY_TYPE) -> None:
        """Used by iloc.  See _LocationIndexerBase._validate_locator_key"""
        # TODO: SNOW-1063355: Modin upgrade - modin.pandas.indexing._iLocIndexer
        super()._validate_locator_key(key)

        if isinstance(key, pd.MultiIndex):
            raise TypeError("key of type MultiIndex cannot be used with iloc")

    def _validate_get_locator_key(self, key: INDEXING_KEY_TYPE) -> None:
        """Used by iloc.  See _LocationIndexerBase._validate_get_locator_key"""
        # TODO: SNOW-1063355: Modin upgrade - modin.pandas.indexing._iLocIndexer
        super()._validate_get_locator_key(key)

        if is_scalar(key) and not is_integer(key):
            raise IndexError(
                ILOC_GET_REQUIRES_NUMERIC_INDEXERS_ERROR_MESSAGE.format(
                    self.api_name, key
                )
            )

        # Tuple e.g. (1, 2)
        if isinstance(key, tuple):
            # `key` is not allowed to be tuple since nested tuple is not allowed.
            # `key` here, which is a 1d indexing key, is generated from 2d indexing key which split into two 1d indexing
            # keys if is tuple type. e,g. 2d_key = ((1,2),0), then 1d key for row key=(1,2). This is not allowed.
            raise IndexingError(TOO_MANY_INDEXERS_INDEXING_ERROR_MESSAGE)

        if isinstance(key, pd.DataFrame):
            raise IndexError(ILOC_GET_DATAFRAME_INDEXER_NOT_ALLOWED_ERROR_MESSAGE)

    def _validate_set_locator_key(self, key: INDEXING_KEY_TYPE) -> None:
        """Used by iloc.  See _LocationIndexerBase._validate_set_locator_key"""
        # TODO: SNOW-1063355: Modin upgrade - modin.pandas.indexing._iLocIndexer
        super()._validate_set_locator_key(key)

        if is_scalar(key) and not is_integer(key):
            raise IndexError(
                ILOC_SET_INDICES_MUST_BE_INTEGER_OR_BOOL_ERROR_MESSAGE.format(key)
            )

    def _are_valid_numeric_key_values(
        self,
        key: Union[slice, int, list[int], list[bool], AnyArrayLike],
        is_valid_numeric_dtype: Callable = is_numeric_dtype,
        is_valid_numeric_type: Callable = lambda v: isinstance(v, numbers.Number),
    ) -> bool:
        """
        Validate iloc input key type after relevant type conversion.

        Args:
            key: positional key or pd.Series version of positional key
            is_valid_numeric_dtype: callable that checks numeric dtype
            is_valid_numeric_type: callable that checks numeric type

        Returns:
            bool: True if the key is valid else False for invalid key

        Notes:
            Snowpark pandas implicitly allows float list like or series values to be compatible with pandas.
            For row values, array-like objects, Index objects, and scalars must be converted to a Series object
            before calling this method. The original key should be passed in along with the Series version for
            printing the error message.

        Raises:
            Series:
                validate numeric type;
            Scalar:
                validate numeric type;
            slice or range like:
                validate start, stop, and step are int type.
            list_like:
                validate numeric type;
            Other invalid types:
                raise IndexingError.
        """
        # TODO: SNOW-1063355: Modin upgrade - modin.pandas.indexing._iLocIndexer
        if isinstance(key, pd.Series):
            return is_valid_numeric_dtype(key.dtype)
        elif isinstance(key, slice) or is_range_like(key):
            validate_positional_slice(key)
        elif hasattr(key, "dtype"):
            return is_valid_numeric_dtype(key.dtype)
        elif is_list_like(key):
            return all(
                is_scalar(x) and (is_bool(x) or is_valid_numeric_type(x)) for x in key  # type: ignore[union-attr]
            )
        elif is_scalar(key):
            return is_valid_numeric_type(key)

        return True

    def _validate_numeric_get_key_values(
        self,
        key: Union[slice, int, list[int], list[bool], AnyArrayLike],
        original_key: Union[slice, int, list[int], list[bool], AnyArrayLike] = None,
    ) -> None:
        """See _iLocIndexer._validate_numeric_key_values"""
        # TODO: SNOW-1063355: Modin upgrade - modin.pandas.indexing._iLocIndexer
        are_valid = self._are_valid_numeric_key_values(key)
        if not are_valid:
            raise IndexError(
                ILOC_GET_REQUIRES_NUMERIC_INDEXERS_ERROR_MESSAGE.format(
                    self.api_name, key if original_key is None else original_key
                )
            )

    def _validate_numeric_set_key_values(
        self,
        key: Union[slice, int, list[int], list[bool], AnyArrayLike],
    ) -> None:
        """See _iLocIndexer._validate_numeric_key_values"""
        # TODO: SNOW-1063355: Modin upgrade - modin.pandas.indexing._iLocIndexer
        are_valid = self._are_valid_numeric_key_values(
            key,
            lambda k: is_integer_dtype(k) or is_bool_dtype(k),
            lambda k: isinstance(k, numbers.Integral) or is_bool(k),
        )
        if not are_valid:
            raise IndexError(ILOC_SET_INDICES_MUST_BE_INTEGER_OR_BOOL_ERROR_MESSAGE)


class _AtIndexer(_LocIndexer):
    """
    An indexer for modin_df.at[] functionality.

    Parameters
    ----------
    modin_df : modin.pandas.DataFrame
        DataFrame to operate on.
    """

    api_name = "at"

    def __getitem__(
        self, key: INDEXING_KEY_TYPE
    ) -> Union[Scalar, pd.Series, pd.DataFrame]:
        """
        Retrieve dataset according to `key`.

        Parameters:
        -----------
        key : indexing key type

        Returns:
        --------
        DataFrame, Series, or scalar
            Located dataset.

        See Also:
        ---------
        pandas.Series.loc
        pandas.DataFrame.loc
        """
        validate_key_for_at_iat(modin_df=self.df, key=key, for_at=True)
        res = super().__getitem__(key)
        if isinstance(res, (DataFrame, Series)):
            res = res.squeeze()
        return res

    def __setitem__(
        self,
        key: INDEXING_KEY_TYPE,
        item: INDEXING_ITEM_TYPE,
    ) -> None:
        """
        Assign `item` value to dataset located by label `key`.

        Parameters:
        -----------
        key : indexing key type
        item: indexing item type

        See Also:
        ---------
        pandas.Series.loc
        pandas.DataFrame.loc
        """
        validate_key_for_at_iat(modin_df=self.df, key=key, for_at=True)
        return super().__setitem__(key, item)


class _iAtIndexer(_iLocIndexer):
    """
    An indexer for modin_df.iat[] functionality.

    Parameters
    ----------
    modin_df : modin.pandas.Series or modin.pandas.DataFrame
        Serires or DataFrame to operate on.
    """

    api_name = "iat"

    def __getitem__(
        self,
        key: INDEXING_KEY_TYPE,
    ) -> Union[Scalar, pd.DataFrame, pd.Series]:
        """
        Retrieve dataset according to positional `key`.

        Parameters:
        -----------
        key : indexing key type

        Returns:
        --------
        DataFrame, Series, or scalar.
            Located dataset.

        See Also:
        ---------
        pandas.Series.iloc
        pandas.DataFrame.iloc
        """
        validate_key_for_at_iat(modin_df=self.df, key=key, for_at=False)
        res = super().__getitem__(key)
        if isinstance(res, (DataFrame, Series)):
            res = res.squeeze()
        return res

    def __setitem__(
        self,
        key: INDEXING_KEY_TYPE,
        item: INDEXING_ITEM_TYPE,
    ) -> None:
        """
        Assign `item` value to dataset located by `key`.

        Parameters:
        -----------
        key : indexing key type
        item: indexing item type

        See Also:
        ---------
        pandas.Series.iloc
        pandas.DataFrame.iloc
        """
        validate_key_for_at_iat(modin_df=self.df, key=key, for_at=False)
        return super().__setitem__(key, item)
