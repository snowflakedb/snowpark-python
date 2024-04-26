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

"""Module houses default Rolling functions builder class."""

from typing import Any, Callable, Union

import pandas

from snowflake.snowpark.modin.core.dataframe.algebra.default2pandas.default import (
    DefaultMethod,
)


class RollingDefault(DefaultMethod):
    """Builder for default-to-pandas aggregation on a rolling window functions."""

    OBJECT_TYPE = "Rolling"

    @classmethod
    def _build_rolling(cls, func: Union[Callable, property]) -> Callable:
        """
        Build function that creates a rolling window and executes `func` on it.

        Parameters
        ----------
        func : callable
            Function to execute on a rolling window.

        Returns
        -------
        callable
            Function that takes pandas DataFrame and applies `func` on a rolling window.
        """

        def fn(  # pragma: no cover
            df: pandas.DataFrame,
            rolling_kwargs: dict[str, Any],
            *args: Any,
            **kwargs: Any
        ) -> Any:
            """Create rolling window for the passed frame and execute specified `func` on it."""
            roller = df.rolling(**rolling_kwargs)  # pragma: no cover

            if type(func) == property:  # pragma: no cover
                return func.fget(roller)  # type: ignore[misc]      # pragma: no cover

            return func(roller, *args, **kwargs)  # type: ignore[operator]      # pragma: no cover

        return fn  # pragma: no cover

    @classmethod
    def register(cls, func: Callable, **kwargs: Any) -> Callable:
        """
        Build function that do fallback to pandas to apply `func` on a rolling window.

        Parameters
        ----------
        func : callable
            Function to execute on a rolling window.
        **kwargs : kwargs
            Additional arguments that will be passed to function builder.

        Returns
        -------
        callable
            Function that takes query compiler and defaults to pandas to apply aggregation
            `func` on a rolling window.
        """
        return super().register(  # pragma: no cover
            cls._build_rolling(func), fn_name=func.__name__, **kwargs
        )


class ExpandingDefault(DefaultMethod):
    """Builder for default-to-pandas aggregation on an expanding window functions."""

    OBJECT_TYPE = "Expanding"

    @classmethod
    def _build_expanding(
        cls, func: Union[Callable, property], squeeze_self: bool
    ) -> Callable:
        """
        Build function that creates an expanding window and executes `func` on it.

        Parameters
        ----------
        func : callable
            Function to execute on a expanding window.
        squeeze_self : bool
            Whether or not to squeeze frame before executing the window function.

        Returns
        -------
        callable
            Function that takes pandas DataFrame and applies `func` on a expanding window.
        """

        def fn(  # pragma: no cover
            df: pandas.DataFrame, rolling_args: Any, *args: Any, **kwargs: Any
        ) -> Any:
            """Create rolling window for the passed frame and execute specified `func` on it."""
            if squeeze_self:  # pragma: no cover
                df = df.squeeze(axis=1)  # pragma: no cover
            roller = df.expanding(*rolling_args)  # pragma: no cover

            if type(func) == property:  # pragma: no cover
                return func.fget(roller)  # type: ignore[misc]      # pragma: no cover

            return func(roller, *args, **kwargs)  # type: ignore[operator]      # pragma: no cover

        return fn  # pragma: no cover

    @classmethod
    def register(
        cls, func: Callable, squeeze_self: bool = False, **kwargs: Any
    ) -> Callable:
        """
        Build function that do fallback to pandas to apply `func` on a expanding window.

        Parameters
        ----------
        func : callable
            Function to execute on an expanding window.
        squeeze_self : bool, default: False
            Whether or not to squeeze frame before executing the window function.
        **kwargs : kwargs
            Additional arguments that will be passed to function builder.

        Returns
        -------
        callable
            Function that takes query compiler and defaults to pandas to apply aggregation
            `func` on an expanding window.
        """
        return super().register(  # pragma: no cover
            cls._build_expanding(func, squeeze_self=squeeze_self),
            fn_name=func.__name__,
            **kwargs
        )
