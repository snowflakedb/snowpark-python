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

"""Implement Window and Rolling public API."""
from textwrap import dedent
from typing import Any, Literal, Optional, Union

import numpy as np  # noqa: F401
import pandas.core.window.rolling
from pandas.util._decorators import doc

from snowflake.snowpark.dataframe import DataFrame as SnowparkDataFrame

# add these two lines to enable doc tests to run
from snowflake.snowpark.modin import pandas as pd  # noqa: F401
from snowflake.snowpark.modin.plugin._internal.telemetry import TelemetryMeta
from snowflake.snowpark.modin.utils import (
    _inherit_docstrings,
    doc_replace_dataframe_with_link,
)

_rolling_agg_method_engine_template = """
Compute the rolling {fname}.

Parameters
----------
numeric_only : bool, default {no}
    Include only float, int, boolean columns.

{args}

engine : str, default None {e}
    * ``'cython'`` : Runs the operation through C-extensions from cython.
    * ``'numba'`` : Runs the operation through JIT compiled code from numba.
    * ``None`` : Defaults to ``'cython'`` or globally setting ``compute.use_numba``

    **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

engine_kwargs : dict, default None {ek}
    * For ``'cython'`` engine, there are no accepted ``engine_kwargs``
    * For ``'numba'`` engine, the engine can accept ``nopython``, ``nogil``
        and ``parallel`` dictionary keys. The values must either be ``True`` or
        ``False``. The default ``engine_kwargs`` for the ``'numba'`` engine is
        ``{{'nopython': True, 'nogil': False, 'parallel': False}}``.

    **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

**kwargs
    Keyword arguments to be passed into func.

Returns
-------
:class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
    Computed rolling {fname} of values.

Examples
--------
{example}
"""

_rolling_aggregate_method_doc_template = """
Rolling aggregate using one or more operations.

Parameters
----------
func : function, str, list, or dict
    Function to use for aggregating the data.
    Accepted combinations are:
    - function
    - string function name
    - list of functions and/or function names, e.g. ``[np.sum, 'mean']``
    - dict of axis labels -> functions, function names or list of such.

*args
    Positional arguments to pass to func.

**kwargs
    Keyword arguments to be passed into func.

Returns
-------
Scalar
    Case when `Series.agg` is called with a single function.
:class:`~snowflake.snowpark.modin.pandas.Series`
    Case when `DataFrame.agg` is called with a single function.
:class:`~snowflake.snowpark.modin.pandas.DataFrame`
    Case when `DataFrame.agg` is called with several functions.

{examples}
"""


@_inherit_docstrings(
    pandas.core.window.rolling.Window, modify_doc=doc_replace_dataframe_with_link
)
# TODO SNOW-1041934: Add support for more window aggregations
class Window(metaclass=TelemetryMeta):
    def __init__(
        self,
        dataframe,
        window: Any = None,
        min_periods: int = None,
        center: bool = False,
        win_type: str = None,
        on: str = None,
        axis: Union[int, str] = 0,
        closed: str = None,
        step: int = None,
        method: str = "single",
    ) -> None:
        # TODO: SNOW-1063357: Modin upgrade - modin.pandas.window.Window
        self._dataframe = dataframe
        self._query_compiler = dataframe._query_compiler
        self.window_kwargs = {  # pragma: no cover
            "window": window,
            "min_periods": min_periods,
            "center": center,
            "win_type": win_type,
            "on": on,
            "axis": axis,
            "closed": closed,
            "step": step,
            "method": method,
        }
        self.axis = axis

    def mean(self, *args, **kwargs):
        # TODO: SNOW-1063357: Modin upgrade - modin.pandas.window.Window
        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.window_mean(
                self.axis, self.window_kwargs, *args, **kwargs
            )
        )

    def sum(self, *args, **kwargs):
        # TODO: SNOW-1063357: Modin upgrade - modin.pandas.window.Window
        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.window_sum(
                self.axis, self.window_kwargs, *args, **kwargs
            )
        )

    def var(self, ddof=1, *args, **kwargs):
        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.window_var(
                self.axis, self.window_kwargs, ddof, *args, **kwargs
            )
        )

    def std(self, ddof=1, *args, **kwargs):
        # TODO: SNOW-1063357: Modin upgrade - modin.pandas.window.Window
        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.window_std(
                self.axis, self.window_kwargs, ddof, *args, **kwargs
            )
        )


@_inherit_docstrings(
    pandas.core.window.rolling.Rolling,
    excluded=[pandas.core.window.rolling.Rolling.__init__],
    modify_doc=doc_replace_dataframe_with_link,
)
# TODO SNOW-1041934: Add support for more window aggregations
class Rolling(metaclass=TelemetryMeta):
    def __init__(
        self,
        dataframe,
        window: Any,
        min_periods: Optional[int] = None,
        center: bool = False,
        win_type: Optional[str] = None,
        on: Optional[str] = None,
        axis: Union[int, str] = 0,
        closed: Optional[str] = None,
        step: Optional[int] = None,
        method: str = "single",
    ) -> None:
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        # Raise ValueError when invalid parameter values/combinations
        if (isinstance(window, int) and window <= 0) or window is None:
            raise ValueError("window must be an integer 0 or greater")
        if not isinstance(center, bool):
            raise ValueError("center must be a boolean")
        if min_periods is not None and not isinstance(min_periods, int):
            raise ValueError("min_periods must be an integer")
        if isinstance(min_periods, int) and min_periods < 0:
            raise ValueError("min_periods must be >= 0")
        if (
            isinstance(min_periods, int)
            and isinstance(window, int)
            and min_periods > window
        ):
            raise ValueError(f"min_periods {min_periods} must be <= window {window}")

        self._dataframe = dataframe
        self._query_compiler = dataframe._query_compiler
        self.rolling_kwargs = {
            "window": window,
            "min_periods": min_periods,
            "center": center,
            "win_type": win_type,
            "on": on,
            "axis": axis,
            "closed": closed,
            "step": step,
            "method": method,
        }
        self.axis = axis

    def _call_qc_method(self, method_name, *args, **kwargs):
        """
        Call a query compiler method for the specified rolling aggregation.

        Parameters
        ----------
        method_name : str
            Name of the aggregation.
        *args : tuple
            Positional arguments to pass to the query compiler method.
        **kwargs : dict
            Keyword arguments to pass to the query compiler method.

        Returns
        -------
        BaseQueryCompiler
            QueryCompiler holding the result of the aggregation.
        """
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        qc_method = getattr(self._query_compiler, f"rolling_{method_name}")
        return qc_method(self.axis, self.rolling_kwargs, *args, **kwargs)

    def _aggregate(self, method_name, *args, **kwargs):
        """
        Run the specified rolling aggregation.

        Parameters
        ----------
        method_name : str
            Name of the aggregation.
        *args : tuple
            Positional arguments to pass to the aggregation.
        **kwargs : dict
            Keyword arguments to pass to the aggregation.

        Returns
        -------
        DataFrame or Series
            Result of the aggregation.
        """
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        qc_result = self._call_qc_method(method_name, *args, **kwargs)
        return self._dataframe.__constructor__(query_compiler=qc_result)

    def count(self, numeric_only: bool = False):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(method_name="count", numeric_only=numeric_only)

    def sem(
        self,
        ddof: int = 1,
        numeric_only: bool = False,
        *args: Any,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="sem", ddof=ddof, numeric_only=numeric_only, *args, **kwargs
        )

    @doc(
        _rolling_agg_method_engine_template,
        fname="sum",
        no=False,
        args=dedent(
            """\
        *args
            Positional arguments to pass to func."""
        ),
        e=None,
        ek=None,
        example=dedent(
            """\
        >>> df = pd.DataFrame({'B': [0, 1, 2, np.nan, 4]})
        >>> df
             B
        0  0.0
        1  1.0
        2  2.0
        3  NaN
        4  4.0
        dtype: float64
        >>> df.rolling(2, min_periods=1).sum()
             B
        0  0.0
        1  1.0
        2  3.0
        3  2.0
        4  4.0
        dtype: float64
        >>> df.rolling(2, min_periods=2).sum()
             B
        0  NaN
        1  1.0
        2  3.0
        3  NaN
        4  NaN
        dtype: float64
        >>> df.rolling(3, min_periods=1, center=True).sum()
             B
        0  1.0
        1  3.0
        2  3.0
        3  6.0
        4  4.0
        dtype: float64"""
        ),
    )
    def sum(
        self,
        numeric_only: bool = False,
        *args: Any,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="sum",
            numeric_only=numeric_only,
            engine=engine,
            engine_kwargs=engine_kwargs,
            *args,
            **kwargs,
        )

    @doc(
        _rolling_agg_method_engine_template,
        fname="mean",
        args=dedent(
            """\
        *args
            Positional arguments to pass to func."""
        ),
        no=False,
        e=None,
        ek=None,
        example=dedent(
            """\
        >>> df = pd.DataFrame({'B': [0, 1, 2, np.nan, 4]})
        >>> df
             B
        0  0.0
        1  1.0
        2  2.0
        3  NaN
        4  4.0
        dtype: float64
        >>> df.rolling(2, min_periods=1).mean()
             B
        0  0.0
        1  0.5
        2  1.5
        3  2.0
        4  4.0
        dtype: float64
        >>> df.rolling(2, min_periods=2).mean()
             B
        0  NaN
        1  0.5
        2  1.5
        3  NaN
        4  NaN
        dtype: float64
        >>> df.rolling(3, min_periods=1, center=True).mean()
             B
        0  0.5
        1  1.0
        2  1.5
        3  3.0
        4  4.0
        dtype: float64"""
        ),
    )
    def mean(
        self,
        numeric_only: bool = False,
        *args: Any,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="mean",
            numeric_only=numeric_only,
            engine=engine,
            engine_kwargs=engine_kwargs,
            *args,
            **kwargs,
        )

    @doc(
        _rolling_agg_method_engine_template,
        fname="median",
        args=None,
        no=False,
        e=None,
        ek=None,
        example=dedent(
            """\
        >>> df = pd.DataFrame({'B': [0, 1, 2, np.nan, 4]})
        >>> df
             B
        0  0.0
        1  1.0
        2  2.0
        3  NaN
        4  4.0
        dtype: float64
        >>> df.rolling(2, min_periods=1).median()
             B
        0  0.0
        1  0.5
        2  1.5
        3  2.0
        4  4.0
        dtype: float64"""
        ),
    )
    def median(
        self,
        numeric_only: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="median",
            numeric_only=numeric_only,
            engine=engine,
            engine_kwargs=engine_kwargs,
            **kwargs,
        )

    @doc(
        _rolling_agg_method_engine_template,
        fname="var",
        args=dedent(
            """\
        *args
            Positional arguments to pass to func."""
        ),
        no=False,
        e=None,
        ek=None,
        example=dedent(
            """\
        >>> df = pd.DataFrame({'B': [0, 1, 2, np.nan, 4]})
        >>> df
             B
        0  0.0
        1  1.0
        2  2.0
        3  NaN
        4  4.0
        dtype: float64
        >>> df.rolling(2, min_periods=1).var()
             B
        0  NaN
        1  0.5
        2  0.5
        3  NaN
        4  NaN
        dtype: float64
        >>> df.rolling(2, min_periods=1).var(ddof=0)
              B
        0  0.00
        1  0.25
        2  0.25
        3  0.00
        4  0.00
        >>> df.rolling(3, min_periods=1, center=True).var()
             B
        0  0.5
        1  1.0
        2  0.5
        3  2.0
        4  NaN
        dtype: float64"""
        ),
    )
    def var(
        self,
        ddof: int = 1,
        numeric_only: bool = False,
        *args: Any,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="var",
            ddof=ddof,
            numeric_only=numeric_only,
            engine=engine,
            engine_kwargs=engine_kwargs,
            *args,
            **kwargs,
        )

    @doc(
        _rolling_agg_method_engine_template,
        fname="std",
        args=dedent(
            """\
        *args
            Positional arguments to pass to func."""
        ),
        no=False,
        e=None,
        ek=None,
        example=dedent(
            """\
        >>> df = pd.DataFrame({'B': [0, 1, 2, np.nan, 4]})
        >>> df
             B
        0  0.0
        1  1.0
        2  2.0
        3  NaN
        4  4.0
        dtype: float64
        >>> df.rolling(2, min_periods=1).std()
                  B
        0       NaN
        1  0.707107
        2  0.707107
        3       NaN
        4       NaN
        dtype: float64
        >>> df.rolling(2, min_periods=1).std(ddof=0)
             B
        0  0.0
        1  0.5
        2  0.5
        3  0.0
        4  0.0
        >>> df.rolling(3, min_periods=1, center=True).std()
                  B
        0  0.707107
        1  1.000000
        2  0.707107
        3  1.414214
        4       NaN
        dtype: float64"""
        ),
    )
    def std(
        self,
        ddof: int = 1,
        numeric_only: bool = False,
        *args: Any,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="std",
            ddof=ddof,
            numeric_only=numeric_only,
            engine=engine,
            engine_kwargs=engine_kwargs,
            *args,
            **kwargs,
        )

    @doc(
        _rolling_agg_method_engine_template,
        fname="min",
        args=dedent(
            """\
        *args
            Positional arguments to pass to func."""
        ),
        no=False,
        e=None,
        ek=None,
        example=dedent(
            """\
        >>> df = pd.DataFrame({'B': [0, 1, 2, np.nan, 4]})
        >>> df
             B
        0  0.0
        1  1.0
        2  2.0
        3  NaN
        4  4.0
        dtype: float64
        >>> df.rolling(2, min_periods=1).min()
             B
        0  0.0
        1  0.0
        2  1.0
        3  2.0
        4  4.0
        dtype: float64"""
        ),
    )
    def min(
        self,
        numeric_only: bool = False,
        *args: Any,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="min",
            numeric_only=numeric_only,
            engine=engine,
            engine_kwargs=engine_kwargs,
            *args,
            **kwargs,
        )

    @doc(
        _rolling_agg_method_engine_template,
        fname="max",
        args=dedent(
            """\
        *args
            Positional arguments to pass to func."""
        ),
        no=False,
        e=None,
        ek=None,
        example=dedent(
            """\
        >>> df = pd.DataFrame({'B': [0, 1, 2, np.nan, 4]})
        >>> df
             B
        0  0.0
        1  1.0
        2  2.0
        3  NaN
        4  4.0
        dtype: float64
        >>> df.rolling(2, min_periods=1).max()
             B
        0  0.0
        1  1.0
        2  2.0
        3  2.0
        4  4.0
        dtype: float64"""
        ),
    )
    def max(
        self,
        numeric_only: bool = False,
        *args: Any,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="max",
            numeric_only=numeric_only,
            engine=engine,
            engine_kwargs=engine_kwargs,
            *args,
            **kwargs,
        )

    def corr(
        self,
        other: Optional[SnowparkDataFrame] = None,
        pairwise: Optional[bool] = None,
        ddof: int = 1,
        numeric_only: bool = False,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="corr",
            other=other,
            pairwise=pairwise,
            ddof=ddof,
            numeric_only=numeric_only,
            **kwargs,
        )

    def cov(
        self,
        other: Optional[SnowparkDataFrame] = None,
        pairwise: Optional[bool] = None,
        ddof: int = 1,
        numeric_only: bool = False,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="cov",
            other=other,
            pairwise=pairwise,
            ddof=ddof,
            numeric_only=numeric_only,
            **kwargs,
        )

    def skew(
        self,
        numeric_only: bool = False,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(method_name="skew", numeric_only=numeric_only, **kwargs)

    def kurt(
        self,
        numeric_only: bool = False,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(method_name="kurt", numeric_only=numeric_only, **kwargs)

    def apply(
        self,
        func: Any,
        raw: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        args: Optional[tuple] = None,
        kwargs: Optional[dict] = None,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="apply",
            func=func,
            raw=raw,
            engine=engine,
            engine_kwargs=engine_kwargs,
            args=args,
            kwargs=kwargs,
        )

    _aggregate_examples_rolling_doc = dedent(
        """
    Examples
    --------
    >>> df = pd.DataFrame({'B': [0, 1, 2, np.nan, 4]})
    >>> df
         B
    0  0.0
    1  1.0
    2  2.0
    3  NaN
    4  4.0
    dtype: float64
    >>> df.rolling(2, min_periods=1).aggregate("mean")
         B
    0  0.0
    1  0.5
    2  1.5
    3  2.0
    4  4.0
    dtype: float64
    >>> df.rolling(2, min_periods=1).aggregate(["min", "max"])
         B
       min  max
    0  0.0  0.0
    1  0.0  1.0
    2  1.0  2.0
    3  2.0  2.0
    4  4.0  4.0
    dtype: float64
    """
    )

    @doc(
        _rolling_aggregate_method_doc_template, examples=_aggregate_examples_rolling_doc
    )
    def aggregate(
        self,
        func: Union[str, list, dict],
        *args: Any,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(method_name="aggregate", func=func, *args, **kwargs)

    agg = aggregate

    def quantile(
        self,
        quantile: float,
        interpolation: str = "linear",
        numeric_only: bool = False,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="quantile",
            quantile=quantile,
            interpolation=interpolation,
            numeric_only=numeric_only,
            **kwargs,
        )

    def rank(
        self,
        method: str = "average",
        ascending: bool = True,
        pct: bool = False,
        numeric_only: bool = False,
        **kwargs,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="rank",
            method=method,
            ascending=ascending,
            pct=pct,
            numeric_only=numeric_only,
            **kwargs,
        )


# TODO: SNOW-1063366: Modin upgrade - modin.pandas.window.Expanding
