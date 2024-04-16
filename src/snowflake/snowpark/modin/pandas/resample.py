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

# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the pandas project, under the BSD 3-Clause License

"""Implement Resampler public API."""
from textwrap import dedent
from typing import Any, Callable, Literal, Optional, Union

import numpy as np
import pandas
import pandas.core.resample
from pandas._libs import lib
from pandas._libs.lib import no_default
from pandas._typing import AggFuncType, AnyArrayLike, Axis, T
from pandas.util._decorators import doc

from snowflake.snowpark.modin import (  # noqa: F401  # add this line to enable doc tests to run
    pandas as pd,
)
from snowflake.snowpark.modin.plugin._internal.telemetry import TelemetryMeta
from snowflake.snowpark.modin.plugin._typing import InterpolateOptions
from snowflake.snowpark.modin.plugin.utils.error_message import ErrorMessage
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from snowflake.snowpark.modin.utils import (
    _inherit_docstrings,
    doc_replace_dataframe_with_link,
)


@_inherit_docstrings(
    pandas.core.resample.Resampler, modify_doc=doc_replace_dataframe_with_link
)
class Resampler(metaclass=TelemetryMeta):
    def __init__(
        self,
        dataframe,
        rule,
        axis=0,
        closed=None,
        label=None,
        convention="start",
        kind=None,
        on=None,
        level=None,
        origin="start_day",
        offset=None,
        group_keys=no_default,
    ) -> None:
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._dataframe = dataframe
        self._query_compiler = dataframe._query_compiler
        self.axis = self._dataframe._get_axis_number(axis)
        self.resample_kwargs = {
            "rule": rule,
            "axis": axis,
            "closed": closed,
            "label": label,
            "convention": convention,
            "kind": kind,
            "on": on,
            "level": level,
            "origin": origin,
            "offset": offset,
            "group_keys": group_keys,
        }
        self.__groups = self._get_groups()

    def _method_not_implemented(self, method: str):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        raise ErrorMessage.not_implemented(
            f"Method {method} is not implemented for Resampler!"
        )

    def _validate_numeric_only_for_aggregate_methods(self, numeric_only):
        """
        When the caller object is Series (ndim == 1), it is not valid to call aggregation
        method with numeric_only = True.

        Raises:
            NotImplementedError if the above condition is encountered.
        """
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        if self._dataframe.ndim == 1:
            if numeric_only and numeric_only is not lib.no_default:
                raise ErrorMessage.not_implemented(
                    "Series Resampler does not implement numeric_only."
                )

    def _get_groups(self):
        """
        Compute the resampled groups.

        Returns
        -------
        PandasGroupby
            Groups as specified by resampling arguments.
        """
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        df = self._dataframe if self.axis == 0 else self._dataframe.T
        groups = df.groupby(
            pandas.Grouper(
                key=self.resample_kwargs["on"],
                freq=self.resample_kwargs["rule"],
                closed=self.resample_kwargs["closed"],
                label=self.resample_kwargs["label"],
                convention=self.resample_kwargs["convention"],
                level=self.resample_kwargs["level"],
                origin=self.resample_kwargs["origin"],
                offset=self.resample_kwargs["offset"],
            ),
            group_keys=self.resample_kwargs["group_keys"],
        )
        return groups

    def __getitem__(self, key):  # pragma: no cover
        """
        Get ``Resampler`` based on `key` columns of original dataframe.

        Parameters
        ----------
        key : str or list
            String or list of selections.

        Returns
        -------
        modin.pandas.BasePandasDataset
            New ``Resampler`` based on `key` columns subset
            of the original dataframe.
        """

        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample

        def _get_new_resampler(key):
            subset = self._dataframe[key]
            resampler = type(self)(subset, **self.resample_kwargs)
            return resampler

        from snowflake.snowpark.modin.pandas.series import Series

        if isinstance(key, (list, tuple, Series, pandas.Index, np.ndarray)):
            if len(self._dataframe.columns.intersection(key)) != len(set(key)):
                missed_keys = list(set(key).difference(self._dataframe.columns))
                raise KeyError(f"Columns not found: {str(sorted(missed_keys))[1:-1]}")
            return _get_new_resampler(list(key))

        if key not in self._dataframe:
            raise KeyError(f"Column not found: {key}")

        return _get_new_resampler(key)

    @property
    def groups(self):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("groups")
        # This property is currently not supported, and NotImplementedError will be
        # thrown before reach here. This is kept here because property function requires
        # a return value.
        return self._query_compiler.default_to_pandas(
            lambda df: pandas.DataFrame.resample(df, **self.resample_kwargs).groups
        )

    @property
    def indices(self):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("indices")
        # Same as groups, keeps the return because indices requires return value
        return self._query_compiler.default_to_pandas(
            lambda df: pandas.DataFrame.resample(df, **self.resample_kwargs).indices
        )

    def get_group(self, name, obj=None):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("get_group")

    _shared_docs = dedent(
        """
    Aggregate using one or more operations over the specified axis.

    Parameters
    ----------
    func : function, str, list or dict
        Function to use for aggregating the data. If a function, must either
        work when passed a {klass} or when passed to {klass}.apply.

        Accepted combinations are:

        - function
        - string function name
        - list of functions and/or function names, e.g. ``[np.sum, 'mean']``
        - dict of axis labels -> functions, function names or list of such.

    *args
        Positional arguments to pass to `func`.
    **kwargs
        Keyword arguments to pass to `func`.
    {axis}
    Returns
    -------
    scalar, Series or DataFrame

        The return can be:

        * scalar : when Series.agg is called with single function
        * Series : when DataFrame.agg is called with a single function
        * DataFrame : when DataFrame.agg is called with several functions

        Return scalar, Series or DataFrame.
    {see_also}
    Notes
    -----
    `agg` is an alias for `aggregate`. Use the alias.

    A passed user-defined-function will be passed a Series for evaluation.
    {examples}"""
    )

    _agg_see_also_doc = dedent(
        """
    See Also
    --------
    DataFrame.groupby.aggregate : Aggregate using callable, string, dict,
        or list of string/callables.
    DataFrame.resample.transform : Transforms the Series on each group
        based on the given function.
    DataFrame.aggregate: Aggregate using one or more
        operations over the specified axis.
    """
    )

    _agg_examples_doc = dedent(
        """
    Examples
    --------

    >>> s = pd.Series([1, 2, 3, 4, 5],
    ...               index=pd.date_range('20130101', periods=5, freq='s'))
    >>> s
    2013-01-01 00:00:00    1
    2013-01-01 00:00:01    2
    2013-01-01 00:00:02    3
    2013-01-01 00:00:03    4
    2013-01-01 00:00:04    5
    Freq: None, dtype: int8

    >>> r = s.resample('2s')

    >>> r.agg(np.sum)
    2013-01-01 00:00:00    3
    2013-01-01 00:00:02    7
    2013-01-01 00:00:04    5
    Freq: None, dtype: int8

    >>> r.agg(['sum', 'mean', 'max'])
                         sum  mean  max
    2013-01-01 00:00:00    3   1.5    2
    2013-01-01 00:00:02    7   3.5    4
    2013-01-01 00:00:04    5   5.0    5

    >>> r.agg({'result': lambda x: x.mean() / x.std(),
    ...        'total': np.sum})
                           result  total
    2013-01-01 00:00:00  2.121320      3
    2013-01-01 00:00:02  4.949747      7
    2013-01-01 00:00:04       NaN      5

    """
    )

    @doc(
        _shared_docs,
        see_also=_agg_see_also_doc,
        examples=_agg_examples_doc,
        klass="DataFrame",
        axis="",
    )
    def apply(
        self, func: Optional[AggFuncType] = None, *args: Any, **kwargs: Any
    ):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("aggregate")

    @doc(
        _shared_docs,
        see_also=_agg_see_also_doc,
        examples=_agg_examples_doc,
        klass="DataFrame",
        axis="",
    )
    def aggregate(
        self, func: Optional[AggFuncType] = None, *args: Any, **kwargs: Any
    ):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("aggregate")

    agg = aggregate

    def transform(
        self,
        arg: Union[Callable[..., T], tuple[Callable[..., T], str]],
        *args: Any,
        **kwargs: Any,
    ):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("transform")

    def pipe(
        self,
        func: Union[Callable[..., T], tuple[Callable[..., T], str]],
        *args: Any,
        **kwargs: Any,
    ):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("pipe")

    def ffill(self, limit: Optional[int] = None) -> Union[pd.DataFrame, pd.Series]:
        """
        Forward fill values for missing resample bins.

        Parameters
        ----------
        limit : int, optional
            This parameter is not supported and will raise a NotImplementedError.

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            A DataFrame with values forward filled for missing resample bins.

        Examples
        --------
        For Series:
        >>> lst1 = pd.to_datetime(['2020-01-03', '2020-01-04', '2020-01-05', '2020-01-07', '2020-01-08'])
        >>> ser1 = pd.Series([1, 2, 3, 4, 5], index=lst1)
        >>> ser1
        2020-01-03    1
        2020-01-04    2
        2020-01-05    3
        2020-01-07    4
        2020-01-08    5
        Freq: None, dtype: int8

        >>> ser1.resample('1D').ffill()
        2020-01-03    1
        2020-01-04    2
        2020-01-05    3
        2020-01-06    3
        2020-01-07    4
        2020-01-08    5
        Freq: None, dtype: int8

        >>> ser1.resample('3D').ffill()
        2020-01-03    1
        2020-01-06    3
        Freq: None, dtype: int8

        >>> lst2 = pd.to_datetime(['2023-01-03 1:00:00', '2023-01-04', '2023-01-05 23:00:00', '2023-01-06', '2023-01-07 2:00:00', '2023-01-10'])
        >>> ser2 = pd.Series([1, 2, 3, 4, None, 6], index=lst2)
        >>> ser2
        2023-01-03 01:00:00    1.0
        2023-01-04 00:00:00    2.0
        2023-01-05 23:00:00    3.0
        2023-01-06 00:00:00    4.0
        2023-01-07 02:00:00    NaN
        2023-01-10 00:00:00    6.0
        Freq: None, dtype: float64

        >>> ser2.resample('1D').ffill()
        2023-01-03    NaN
        2023-01-04    2.0
        2023-01-05    2.0
        2023-01-06    4.0
        2023-01-07    4.0
        2023-01-08    NaN
        2023-01-09    NaN
        2023-01-10    6.0
        Freq: None, dtype: float64

        >>> ser2.resample('2D').ffill()
        2023-01-03    NaN
        2023-01-05    2.0
        2023-01-07    4.0
        2023-01-09    NaN
        Freq: None, dtype: float64

        For DataFrame:

        >>> index1 = pd.to_datetime(['2020-01-03', '2020-01-04', '2020-01-05', '2020-01-07', '2020-01-08'])
        >>> df1 = pd.DataFrame({'a': range(len(index1)),
        ... 'b': range(len(index1) + 10, len(index1) * 2 + 10)},
        ...  index=index1)
        >>> df1
                    a   b
        2020-01-03  0  15
        2020-01-04  1  16
        2020-01-05  2  17
        2020-01-07  3  18
        2020-01-08  4  19

        >>> df1.resample('1D').ffill()
                    a   b
        2020-01-03  0  15
        2020-01-04  1  16
        2020-01-05  2  17
        2020-01-06  2  17
        2020-01-07  3  18
        2020-01-08  4  19

        >>> df1.resample('3D').ffill()
                    a   b
        2020-01-03  0  15
        2020-01-06  2  17

        >>> index2 = pd.to_datetime(['2023-01-03 1:00:00', '2023-01-04', '2023-01-05 23:00:00', '2023-01-06', '2023-01-07 2:00:00', '2023-01-10'])
        >>> df2 = pd.DataFrame({'a': range(len(index2)),
        ... 'b': range(len(index2) + 10, len(index2) * 2 + 10)},
        ...  index=index2)
        >>> df2
                             a   b
        2023-01-03 01:00:00  0  16
        2023-01-04 00:00:00  1  17
        2023-01-05 23:00:00  2  18
        2023-01-06 00:00:00  3  19
        2023-01-07 02:00:00  4  20
        2023-01-10 00:00:00  5  21

        >>> df2.resample('1D').ffill()
                      a     b
        2023-01-03  NaN   NaN
        2023-01-04  1.0  17.0
        2023-01-05  1.0  17.0
        2023-01-06  3.0  19.0
        2023-01-07  3.0  19.0
        2023-01-08  4.0  20.0
        2023-01-09  4.0  20.0
        2023-01-10  5.0  21.0

        >>> df2.resample('2D').ffill()
                      a     b
        2023-01-03  NaN   NaN
        2023-01-05  1.0  17.0
        2023-01-07  3.0  19.0
        2023-01-09  4.0  20.0
        """

        is_series = not self._dataframe._is_dataframe

        if limit is not None:
            ErrorMessage.not_implemented(
                "Parameter limit of resample.ffill has not been implemented."
            )

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.resample(
                self.resample_kwargs,
                "ffill",
                (),
                {},
                is_series,
            )
        )

    def backfill(self, limit: Optional[int] = None):
        self._method_not_implemented("backfill")  # pragma: no cover

    def bfill(self, limit: Optional[int] = None):  # pragma: no cover
        self._method_not_implemented("bfill")

    def pad(self, limit: Optional[int] = None):  # pragma: no cover
        self._method_not_implemented("pad")

    def nearest(self, limit: Optional[int] = None):  # pragma: no cover
        self._method_not_implemented("nearest")

    def fillna(self, method, limit: Optional[int] = None):  # pragma: no cover
        self._method_not_implemented("fillna")

    def asfreq(self, fill_value: Optional[Any] = None):  # pragma: no cover
        self._method_not_implemented("asfreq")

    def interpolate(
        self,
        method: InterpolateOptions = "linear",
        *,
        axis: Axis = 0,
        limit: Optional[int] = None,
        inplace: bool = False,
        limit_direction: Literal["forward", "backward", "both"] = "forward",
        limit_area: Optional[Literal["inside", "outside"]] = None,
        downcast: Optional[Literal["infer"]] = None,
        **kwargs,
    ):  # pragma: no cover
        self._method_not_implemented("interpolate")

    def count(self) -> Union[pd.DataFrame, pd.Series]:
        """
        Compute count of resample bins.

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Computed count of values within each resample bin.

        Examples
        --------
        For Series:

        >>> lst1 = pd.date_range('2020-01-01', periods=4, freq='1D')
        >>> ser1 = pd.Series([1, 2, 3, 4], index=lst1)
        >>> ser1
        2020-01-01    1
        2020-01-02    2
        2020-01-03    3
        2020-01-04    4
        Freq: None, dtype: int8

        >>> ser1.resample('2D').count()
        2020-01-01    2
        2020-01-03    2
        Freq: None, dtype: int8

        >>> lst2 = pd.date_range('2020-01-01', periods=4, freq='S')
        >>> ser2 = pd.Series([1, 2, np.nan, 4], index=lst2)
        >>> ser2
        2020-01-01 00:00:00    1.0
        2020-01-01 00:00:01    2.0
        2020-01-01 00:00:02    NaN
        2020-01-01 00:00:03    4.0
        Freq: None, dtype: float64

        >>> ser2.resample('2S').count()
        2020-01-01 00:00:00    2
        2020-01-01 00:00:02    1
        Freq: None, dtype: int64

        For DataFrame:

        >>> data = [[1, 8, 2], [1, 2, 5], [2, 5, 8], [2, 6, 9]]
        >>> df = pd.DataFrame(data,
        ...      columns=["a", "b", "c"],
        ...      index=pd.date_range('2020-01-01', periods=4, freq='1D'))
        >>> df
                    a  b  c
        2020-01-01  1  8  2
        2020-01-02  1  2  5
        2020-01-03  2  5  8
        2020-01-04  2  6  9

        >>> df.resample('2D').count()
                    a  b  c
        2020-01-01  2  2  2
        2020-01-03  2  2  2
        """
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.resample(
                self.resample_kwargs,
                "count",
                tuple(),
                dict(),
                is_series,
            )
        )

    def nunique(self, *args: Any, **kwargs: Any):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("nunique")

    def first(
        self,
        numeric_only: Union[bool, lib.NoDefault] = lib.no_default,
        min_count: int = 0,
        *args: Any,
        **kwargs: Any,
    ):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("first")

    def last(
        self,
        numeric_only: Union[bool, lib.NoDefault] = lib.no_default,
        min_count: int = 0,
        *args: Any,
        **kwargs: Any,
    ):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("last")

    def max(
        self,
        numeric_only: bool = False,
        min_count: int = 0,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        """
        Compute maximum of resample bins.

        Parameters
        ----------
        numeric_only : bool, default False
            Include only float, int, boolean columns.

        min_count : int, default 0
            The required number of valid values to perform the operation. If fewer
            than ``min_count`` non-NA values are present the result will be NA.

        engine : str, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        engine_kwargs : dict, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Computed maximum of values within each resample bin.

        Examples
        --------
        For Series:

        >>> lst1 = pd.date_range('2020-01-01', periods=4, freq='1D')
        >>> ser1 = pd.Series([1, 2, 3, 4], index=lst1)
        >>> ser1
        2020-01-01    1
        2020-01-02    2
        2020-01-03    3
        2020-01-04    4
        Freq: None, dtype: int8

        >>> ser1.resample('2D').max()
        2020-01-01    2
        2020-01-03    4
        Freq: None, dtype: int8

        >>> lst2 = pd.date_range('2020-01-01', periods=4, freq='S')
        >>> ser2 = pd.Series([1, 2, np.nan, 4], index=lst2)
        >>> ser2
        2020-01-01 00:00:00    1.0
        2020-01-01 00:00:01    2.0
        2020-01-01 00:00:02    NaN
        2020-01-01 00:00:03    4.0
        Freq: None, dtype: float64

        >>> ser2.resample('2S').max()
        2020-01-01 00:00:00    2.0
        2020-01-01 00:00:02    4.0
        Freq: None, dtype: float64

        For DataFrame:

        >>> data = [[1, 8], [1, 2], [2, 5], [2, 6]]
        >>> df1 = pd.DataFrame(data,
        ... columns=["a", "b"],
        ... index=pd.date_range('2020-01-01', periods=4, freq='1D'))
        >>> df1
                    a  b
        2020-01-01  1  8
        2020-01-02  1  2
        2020-01-03  2  5
        2020-01-04  2  6

        >>> df1.resample('2D').max()
                    a  b
        2020-01-01  1  8
        2020-01-03  2  6

        >>> df2 = pd.DataFrame(
        ... {'A': [1, 2, 3, np.nan], 'B': [np.nan, np.nan, 3, 4]},
        ... index=pd.date_range('2020-01-01', periods=4, freq='1S'))
        >>> df2
                               A    B
        2020-01-01 00:00:00  1.0  NaN
        2020-01-01 00:00:01  2.0  NaN
        2020-01-01 00:00:02  3.0  3.0
        2020-01-01 00:00:03  NaN  4.0

        >>> df2.resample('2S').max()
                               A    B
        2020-01-01 00:00:00  2.0  NaN
        2020-01-01 00:00:02  3.0  4.0
        """
        self._validate_numeric_only_for_aggregate_methods(numeric_only)
        WarningMessage.warning_if_engine_args_is_set("resample_max", args, kwargs)

        agg_kwargs = dict(numeric_only=numeric_only, min_count=min_count)
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.resample(
                self.resample_kwargs,
                "max",
                tuple(),
                agg_kwargs,
                is_series,
            )
        )

    def mean(
        self,
        numeric_only: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        """
        Compute mean of resample bins.

        Parameters
        ----------
        numeric_only : bool, default False
            Include only float, int, boolean columns.

        engine : str, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        engine_kwargs : dict, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Computed mean of values within each resample bin.

        Examples
        --------
        For Series:

        >>> lst1 = pd.date_range('2020-01-01', periods=4, freq='1D')
        >>> ser1 = pd.Series([1, 2, 3, 4], index=lst1)
        >>> ser1
        2020-01-01    1
        2020-01-02    2
        2020-01-03    3
        2020-01-04    4
        Freq: None, dtype: int8

        >>> ser1.resample('2D').mean()
        2020-01-01    1.500000
        2020-01-03    3.500000
        Freq: None, dtype: object

        >>> lst2 = pd.date_range('2020-01-01', periods=4, freq='S')
        >>> ser2 = pd.Series([1, 2, np.nan, 4], index=lst2)
        >>> ser2
        2020-01-01 00:00:00    1.0
        2020-01-01 00:00:01    2.0
        2020-01-01 00:00:02    NaN
        2020-01-01 00:00:03    4.0
        Freq: None, dtype: float64

        >>> ser2.resample('2S').mean()
        2020-01-01 00:00:00    1.5
        2020-01-01 00:00:02    4.0
        Freq: None, dtype: float64

        For DataFrame:

        >>> df1 = pd.DataFrame(
        ... {'A': [1, 1, 2, 1, 2], 'B': [np.nan, 2, 3, 4, 5]},
        ... index=pd.date_range('2020-01-01', periods=5, freq='1D'))
        >>> df1
                    A    B
        2020-01-01  1  NaN
        2020-01-02  1  2.0
        2020-01-03  2  3.0
        2020-01-04  1  4.0
        2020-01-05  2  5.0

        >>> df1.resample('2D').mean()
                           A    B
        2020-01-01  1.000000  2.0
        2020-01-03  1.500000  3.5
        2020-01-05  2.000000  5.0

        >>> df1.resample('2D')['B'].mean()
        2020-01-01    2.0
        2020-01-03    3.5
        2020-01-05    5.0
        Freq: None, Name: B, dtype: float64

        >>> df2 = pd.DataFrame(
        ... {'A': [1, 2, 3, np.nan], 'B': [np.nan, np.nan, 3, 4]},
        ... index=pd.date_range('2020-01-01', periods=4, freq='1S'))
        >>> df2
                               A    B
        2020-01-01 00:00:00  1.0  NaN
        2020-01-01 00:00:01  2.0  NaN
        2020-01-01 00:00:02  3.0  3.0
        2020-01-01 00:00:03  NaN  4.0

        >>> df2.resample('2S').mean()
                               A    B
        2020-01-01 00:00:00  1.5  NaN
        2020-01-01 00:00:02  3.0  3.5
        """
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._validate_numeric_only_for_aggregate_methods(numeric_only)
        WarningMessage.warning_if_engine_args_is_set("resample_mean", args, kwargs)

        agg_kwargs = dict(numeric_only=numeric_only)
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.resample(
                self.resample_kwargs,
                "mean",
                tuple(),
                agg_kwargs,
                is_series,
            )
        )

    def median(
        self,
        numeric_only: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        """
        Compute median of resample bins.

        Parameters
        ----------
        numeric_only : bool, default False
            Include only float, int, boolean columns.

        engine : str, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        engine_kwargs : dict, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Computed median of values within each resample bin.

        Examples
        --------
        For Series:

        >>> lst1 = pd.date_range('2020-01-01', periods=4, freq='1D')
        >>> ser1 = pd.Series([1, 2, 3, 4], index=lst1)
        >>> ser1
        2020-01-01    1
        2020-01-02    2
        2020-01-03    3
        2020-01-04    4
        Freq: None, dtype: int8

        >>> ser1.resample('2D').median()
        2020-01-01    1.5
        2020-01-03    3.5
        Freq: None, dtype: float64

        >>> lst2 = pd.date_range('2020-01-01', periods=4, freq='S')
        >>> ser2 = pd.Series([1, 2, np.nan, 4], index=lst2)
        >>> ser2
        2020-01-01 00:00:00    1.0
        2020-01-01 00:00:01    2.0
        2020-01-01 00:00:02    NaN
        2020-01-01 00:00:03    4.0
        Freq: None, dtype: float64

        >>> ser2.resample('2S').median()
        2020-01-01 00:00:00    1.5
        2020-01-01 00:00:02    4.0
        Freq: None, dtype: float64

        For DataFrame:

        >>> data = [[1, 8, 2], [1, 2, 5], [2, 5, 8], [2, 6, 9]]
        >>> df = pd.DataFrame(data,
        ...      columns=["a", "b", "c"],
        ...      index=pd.date_range('2020-01-01', periods=4, freq='1D'))
        >>> df
                    a  b  c
        2020-01-01  1  8  2
        2020-01-02  1  2  5
        2020-01-03  2  5  8
        2020-01-04  2  6  9

        >>> df.resample('2D').median()
                    a    b    c
        2020-01-01  1.0  5.0  3.5
        2020-01-03  2.0  5.5  8.5
        """
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._validate_numeric_only_for_aggregate_methods(numeric_only)
        WarningMessage.warning_if_engine_args_is_set("resample_median", args, kwargs)

        agg_kwargs = dict(numeric_only=numeric_only)
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.resample(
                self.resample_kwargs,
                "median",
                tuple(),
                agg_kwargs,
                is_series,
            )
        )

    def min(
        self,
        numeric_only: bool = False,
        min_count: int = 0,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        """
        Compute minimum of resample bins.

        Parameters
        ----------
        numeric_only : bool, default False
            Include only float, int, boolean columns.

        min_count : int, default 0
            The required number of valid values to perform the operation. If fewer
            than ``min_count`` non-NA values are present the result will be NA.

        engine : str, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        engine_kwargs : dict, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Computed minimum of values within each resample bin.

        Examples
        --------
        For Series:

        >>> lst1 = pd.date_range('2020-01-01', periods=4, freq='1D')
        >>> ser1 = pd.Series([1, 2, 3, 4], index=lst1)
        >>> ser1
        2020-01-01    1
        2020-01-02    2
        2020-01-03    3
        2020-01-04    4
        Freq: None, dtype: int8

        >>> ser1.resample('2D').min()
        2020-01-01    1
        2020-01-03    3
        Freq: None, dtype: int8

        >>> lst2 = pd.date_range('2020-01-01', periods=4, freq='S')
        >>> ser2 = pd.Series([1, 2, np.nan, 4], index=lst2)
        >>> ser2
        2020-01-01 00:00:00    1.0
        2020-01-01 00:00:01    2.0
        2020-01-01 00:00:02    NaN
        2020-01-01 00:00:03    4.0
        Freq: None, dtype: float64

        >>> ser2.resample('2S').min()
        2020-01-01 00:00:00    1.0
        2020-01-01 00:00:02    4.0
        Freq: None, dtype: float64

        For DataFrame:

        >>> data = [[1, 8, 2], [1, 2, 5], [2, 5, 8], [2, 6, 9]]
        >>> df = pd.DataFrame(data,
        ...      columns=["a", "b", "c"],
        ...      index=pd.date_range('2020-01-01', periods=4, freq='1D'))
        >>> df
                    a  b  c
        2020-01-01  1  8  2
        2020-01-02  1  2  5
        2020-01-03  2  5  8
        2020-01-04  2  6  9

        >>> df.resample('2D').min()
                    a  b  c
        2020-01-01  1  2  2
        2020-01-03  2  5  8
        """
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._validate_numeric_only_for_aggregate_methods(numeric_only)
        WarningMessage.warning_if_engine_args_is_set("resample_min", args, kwargs)

        agg_kwargs = dict(numeric_only=numeric_only, min_count=min_count)
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.resample(
                self.resample_kwargs,
                "min",
                tuple(),
                agg_kwargs,
                is_series,
            )
        )

    def ohlc(self, *args: Any, **kwargs: Any):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("ohlc")

    def prod(
        self,
        numeric_only: Union[bool, lib.NoDefault] = lib.no_default,
        min_count: int = 0,
        *args: Any,
        **kwargs: Any,
    ):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("prod")

    def size(self):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("size")

    def sem(
        self,
        ddof: int = 1,
        numeric_only: Union[bool, lib.NoDefault] = lib.no_default,
        *args: Any,
        **kwargs: Any,
    ):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("sem")

    def std(
        self,
        ddof: int = 1,
        numeric_only: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        """
        Compute standard deviation of resample bins.

        Parameters
        ----------
        ddof : int, default 1
            Delta Degrees of Freedom. The divisor used in calculations is N - ddof,
            where N represents the number of elements.

        numeric_only : bool, default False
            Include only float, int, boolean columns.

        min_count : int, default 0
            The required number of valid values to perform the operation. If fewer
            than ``min_count`` non-NA values are present the result will be NA.

        engine : str, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        engine_kwargs : dict, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Computed standard deviation of values within each resample bin.

        Examples
        --------
        For Series:

        >>> lst1 = pd.date_range('2020-01-01', periods=4, freq='1D')
        >>> ser1 = pd.Series([1, 2, 3, 4], index=lst1)
        >>> ser1
        2020-01-01    1
        2020-01-02    2
        2020-01-03    3
        2020-01-04    4
        Freq: None, dtype: int8

        >>> ser1.resample('2D').std()
        2020-01-01    0.707107
        2020-01-03    0.707107
        Freq: None, dtype: float64

        >>> lst2 = pd.date_range('2020-01-01', periods=4, freq='S')
        >>> ser2 = pd.Series([1, 2, np.nan, 4], index=lst2)
        >>> ser2
        2020-01-01 00:00:00    1.0
        2020-01-01 00:00:01    2.0
        2020-01-01 00:00:02    NaN
        2020-01-01 00:00:03    4.0
        Freq: None, dtype: float64

        >>> ser2.resample('2S').std()
        2020-01-01 00:00:00    0.707107
        2020-01-01 00:00:02    NaN
        Freq: None, dtype: float64

        For DataFrame:

        >>> data = [[1, 8, 2], [1, 2, 5], [2, 5, 8], [2, 6, 9]]
        >>> df = pd.DataFrame(data,
        ...      columns=["a", "b", "c"],
        ...      index=pd.date_range('2020-01-01', periods=4, freq='1D'))
        >>> df
                    a  b  c
        2020-01-01  1  8  2
        2020-01-02  1  2  5
        2020-01-03  2  5  8
        2020-01-04  2  6  9

        >>> df.resample('2D').std()
                    a   b        c
        2020-01-01  0.0 4.242641 2.121320
        2020-01-03  0.0 0.707107 0.707107
        """
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._validate_numeric_only_for_aggregate_methods(numeric_only)
        WarningMessage.warning_if_engine_args_is_set("resample_std", args, kwargs)

        agg_kwargs = dict(numeric_only=numeric_only, ddof=ddof)
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.resample(
                self.resample_kwargs,
                "std",
                tuple(),
                agg_kwargs,
                is_series,
            )
        )

    def sum(
        self,
        numeric_only: bool = False,
        min_count: int = 0,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        """
        Compute sum of resample bins.

        Parameters
        ----------
        numeric_only : bool, default False
            Include only float, int, boolean columns.

        min_count : int, default 0
            The required number of valid values to perform the operation. If fewer
            than ``min_count`` non-NA values are present the result will be NA.

        engine : str, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        engine_kwargs : dict, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Computed sum of values within each resample bin.

        Examples
        --------
        For Series:

        >>> lst1 = pd.date_range('2020-01-01', periods=4, freq='1D')
        >>> ser1 = pd.Series([1, 2, 3, 4], index=lst1)
        >>> ser1
        2020-01-01    1
        2020-01-02    2
        2020-01-03    3
        2020-01-04    4
        Freq: None, dtype: int8

        >>> ser1.resample('2D').sum()
        2020-01-01    3
        2020-01-03    7
        Freq: None, dtype: int8

        >>> lst2 = pd.date_range('2020-01-01', periods=4, freq='S')
        >>> ser2 = pd.Series([1, 2, np.nan, 4], index=lst2)
        >>> ser2
        2020-01-01 00:00:00    1.0
        2020-01-01 00:00:01    2.0
        2020-01-01 00:00:02    NaN
        2020-01-01 00:00:03    4.0
        Freq: None, dtype: float64

        >>> ser2.resample('2S').sum()
        2020-01-01 00:00:00    3.0
        2020-01-01 00:00:02    4.0
        Freq: None, dtype: float64

        For DataFrame:

        >>> data = [[1, 8, 2], [1, 2, 5], [2, 5, 8], [2, 6, 9]]
        >>> df = pd.DataFrame(data,
        ...      columns=["a", "b", "c"],
        ...      index=pd.date_range('2020-01-01', periods=4, freq='1D'))
        >>> df
                    a  b  c
        2020-01-01  1  8  2
        2020-01-02  1  2  5
        2020-01-03  2  5  8
        2020-01-04  2  6  9

        >>> df.resample('2D').sum()
                    a  b  c
        2020-01-01  2  10 7
        2020-01-03  4  11 17
        """
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._validate_numeric_only_for_aggregate_methods(numeric_only)
        WarningMessage.warning_if_engine_args_is_set("resample_sum", args, kwargs)

        agg_kwargs = dict(numeric_only=numeric_only, min_count=min_count)
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.resample(
                self.resample_kwargs,
                "sum",
                tuple(),
                agg_kwargs,
                is_series,
            )
        )

    def var(
        self,
        ddof: int = 1,
        numeric_only: Union[bool, lib.NoDefault] = lib.no_default,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        """
        Compute variance of resample bins.

        Parameters
        ----------
        ddof : int, default 1
            Delta Degrees of Freedom. The divisor used in calculations is N - ddof,
            where N represents the number of elements.

        numeric_only : bool, default False
            Include only float, int, boolean columns.

        min_count : int, default 0
            The required number of valid values to perform the operation. If fewer
            than ``min_count`` non-NA values are present the result will be NA.

        engine : str, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        engine_kwargs : dict, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Computed variance of values within each resample bin.

        Examples
        --------
        For Series:

        >>> lst1 = pd.date_range('2020-01-01', periods=4, freq='1D')
        >>> ser1 = pd.Series([1, 2, 3, 4], index=lst1)
        >>> ser1
        2020-01-01    1
        2020-01-02    2
        2020-01-03    3
        2020-01-04    4
        Freq: None, dtype: int8

        >>> ser1.resample('2D').var()
        2020-01-01    0.5
        2020-01-03    0.5
        Freq: None, dtype: float64

        >>> lst2 = pd.date_range('2020-01-01', periods=4, freq='S')
        >>> ser2 = pd.Series([1, 2, np.nan, 4], index=lst2)
        >>> ser2
        2020-01-01 00:00:00    1.0
        2020-01-01 00:00:01    2.0
        2020-01-01 00:00:02    NaN
        2020-01-01 00:00:03    4.0
        Freq: None, dtype: float64

        >>> ser2.resample('2S').var()
        2020-01-01 00:00:00    0.5
        2020-01-01 00:00:02    NaN
        Freq: None, dtype: float64

        For DataFrame:

        >>> data = [[1, 8, 2], [1, 2, 5], [2, 5, 8], [2, 6, 9]]
        >>> df = pd.DataFrame(data,
        ...      columns=["a", "b", "c"],
        ...      index=pd.date_range('2020-01-01', periods=4, freq='1D'))
        >>> df
                    a  b  c
        2020-01-01  1  8  2
        2020-01-02  1  2  5
        2020-01-03  2  5  8
        2020-01-04  2  6  9

        >>> df.resample('2D').var()
                    a   b    c
        2020-01-01  0.0 18.0 4.5
        2020-01-03  0.0 0.5  0.5
        """
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._validate_numeric_only_for_aggregate_methods(numeric_only)
        WarningMessage.warning_if_engine_args_is_set("resample_var", args, kwargs)

        agg_kwargs = dict(numeric_only=numeric_only, ddof=ddof)
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.resample(
                self.resample_kwargs,
                "var",
                tuple(),
                agg_kwargs,
                is_series,
            )
        )

    def quantile(
        self, q: Union[float, AnyArrayLike] = 0.5, **kwargs: Any
    ):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("quantile")
