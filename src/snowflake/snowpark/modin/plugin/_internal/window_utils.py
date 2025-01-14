#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

#
# This file contains utils functions used by the groupby functionalities.
#
#
from enum import Enum
from typing import Any

from snowflake.snowpark.column import Column as SnowparkColumn
from snowflake.snowpark.functions import (
    builtin,
    col,
    iff,
    make_interval,
    stddev_pop,
    sum as sum_,
)
from snowflake.snowpark.modin.plugin._internal.resample_utils import (
    rule_to_snowflake_width_and_slice_unit,
)
from snowflake.snowpark.modin.plugin._internal.utils import pandas_lit
from snowflake.snowpark.modin.plugin.utils.error_message import ErrorMessage


class WindowFunction(Enum):
    """
    Type of window function.

    Attributes:
        EXPANDING (str): Represents the expanding window.
        ROLLING (str): Represents the rolling window.
    """

    EXPANDING = "expanding"
    ROLLING = "rolling"


def check_and_raise_error_rolling_window_supported_by_snowflake(
    rolling_kwargs: dict[str, Any]
) -> None:
    """
    Check if execution with snowflake engine is available for the rolling window operation.
    If not, raise NotImplementedError.

    Parameters
    ----------
    rolling_kwargs: keyword arguments passed to rolling. The rolling keywords handled in the
        function contains:
        window: int, timedelta, str, offset, or BaseIndexer subclass. Size of the moving window.
            If an integer, the fixed number of observations used for each window.
            If a timedelta, str, or offset, the time period of each window. Each window will be a variable sized based on the observations included in the time-period. This is only valid for datetimelike indexes.
            If a BaseIndexer subclass, the window boundaries based on the defined get_window_bounds method. Additional rolling keyword arguments, namely min_periods, center, closed and step will be passed to get_window_bounds.
        min_periods: int, default None.
            Minimum number of observations in window required to have a value; otherwise, result is np.nan.
            For a window that is specified by an offset, min_periods will default to 1.
            For a window that is specified by an integer, min_periods will default to the size of the window.
        center: bool, default False.
            If False, set the window labels as the right edge of the window index.
            If True, set the window labels as the center of the window index.
        win_type: str, default None
            If None, all points are evenly weighted.
            If a string, it must be a valid scipy.signal window function.
            Certain Scipy window types require additional parameters to be passed in the aggregation function. The additional parameters must match the keywords specified in the Scipy window type method signature.
        on: str, optional
            For a DataFrame, a column label or Index level on which to calculate the rolling window, rather than the DataFrame’s index.
            Provided integer column is ignored and excluded from result since an integer index is not used to calculate the rolling window.
        axis: int or str, default 0
            If 0 or 'index', roll across the rows.
            If 1 or 'columns', roll across the columns.
            For Series this parameter is unused and defaults to 0.
        closed: str, default None
            If 'right', the first point in the window is excluded from calculations.
            If 'left', the last point in the window is excluded from calculations.
            If 'both', the no points in the window are excluded from calculations.
            If 'neither', the first and last points in the window are excluded from calculations.
            Default None ('right').
        step: int, default None
            Evaluate the window at every step result, equivalent to slicing as [::step]. window must be an integer. Using a step argument other than None or 1 will produce a result with a different shape than the input.
        method: str {‘single’, ‘table’}, default ‘single’
            **This parameter is ignored in Snowpark pandas since the execution engine will always be Snowflake.**
    """
    # Snowflake pandas implementation only supports integer window_size, min_periods >= 1, and center on axis = 0
    window = rolling_kwargs.get("window")
    min_periods = rolling_kwargs.get("min_periods")
    win_type = rolling_kwargs.get("win_type")
    on = rolling_kwargs.get("on")
    axis = rolling_kwargs.get("axis", 0)
    closed = rolling_kwargs.get("closed")
    step = rolling_kwargs.get("step")

    # Raise not implemented error for unsupported params
    if not isinstance(window, (int, str)):
        ErrorMessage.not_implemented(
            "Snowpark pandas does not yet support Rolling with windows that are not strings or integers"
        )
    if min_periods == 0:
        ErrorMessage.parameter_not_implemented_error(
            parameter_name="min_periods = 0", method_name="Rolling"
        )
    if win_type:
        ErrorMessage.parameter_not_implemented_error(
            parameter_name="win_type", method_name="Rolling"
        )  # pragma: no cover
    if on:
        ErrorMessage.parameter_not_implemented_error(
            parameter_name="on", method_name="Rolling"
        )  # pragma: no cover
    if axis not in (0, "index"):
        # Note that this is deprecated since pandas 2.1.0
        ErrorMessage.parameter_not_implemented_error(
            parameter_name="axis = 1", method_name="Rolling"
        )  # pragma: no cover
    if closed:
        ErrorMessage.parameter_not_implemented_error(
            parameter_name="closed", method_name="Rolling"
        )  # pragma: no cover
    if step:
        ErrorMessage.parameter_not_implemented_error(
            parameter_name="step", method_name="Rolling"
        )  # pragma: no cover


def check_and_raise_error_expanding_window_supported_by_snowflake(
    expanding_kwargs: dict[str, Any]
) -> None:
    """
    Check if execution with snowflake engine is available for the expanding window operation.
    If not, raise NotImplementedError.

    Parameters
    ----------
    expanding_kwargs: keyword arguments passed to expanding. The expanding keywords handled in the
        function contains:
        min_periods: int, default 1.
            Minimum number of observations in window required to have a value; otherwise, result is np.nan.
        axis: int or str, default 0
            If 0 or 'index', roll across the rows.
            If 1 or 'columns', roll across the columns.
            For Series this parameter is unused and defaults to 0.
        method: str {‘single’, ‘table’}, default ‘single’
            **This parameter is ignored in Snowpark pandas since the execution engine will always be Snowflake.**
    """

    axis = expanding_kwargs.get("axis", 0)

    if axis not in (0, "index"):
        # Note that this is deprecated since pandas 2.1.0
        ErrorMessage.parameter_not_implemented_error(
            parameter_name="axis = 1", method_name="Expanding"
        )  # pragma: no cover


def create_snowpark_interval_from_window(window: str) -> SnowparkColumn:
    """
    This function creates a Snowpark column consisting of an Interval Expression from a given
    window string.

    Parameters
    ----------
    window: str
        The given window (e.g. '2s') that we want to use to create a Snowpark column Interval
        Expression to pass to Window.range_between.

    Returns
    -------
    Snowpark Column
    """
    slice_width, slice_unit = rule_to_snowflake_width_and_slice_unit(window)
    if slice_width < 0:
        ErrorMessage.not_implemented(
            "Snowpark pandas 'Rolling' does not yet support negative time 'window' offset"
        )
    # Ensure all possible frequencies 'rule_to_snowflake_width_and_slice_unit' can output are
    # accounted for before creating the Interval column
    if slice_unit not in (
        "second",
        "minute",
        "hour",
        "day",
        "week",
        "month",
        "quarter",
        "year",
    ):
        raise AssertionError(
            f"Snowpark pandas cannot map 'window' {window} to an offset"
        )
    seconds = slice_width - 1 if slice_unit == "second" else 0
    minutes = slice_width - 1 if slice_unit == "minute" else 0
    hours = slice_width - 1 if slice_unit == "hour" else 0
    days = slice_width - 1 if slice_unit == "day" else 0
    weeks = slice_width - 1 if slice_unit == "week" else 0
    months = slice_width - 1 if slice_unit == "month" else 0
    quarters = slice_width - 1 if slice_unit == "quarter" else 0
    years = slice_width - 1 if slice_unit == "year" else 0
    return make_interval(
        seconds=seconds,
        minutes=minutes,
        hours=hours,
        days=days,
        weeks=weeks,
        months=months,
        quarters=quarters,
        years=years,
    )


def get_rolling_corr_column(
    quoted_identifier: str,
    other_quoted_identifier: str,
    window_expr: Any,
    window: Any,
) -> SnowparkColumn:
    """
    Get the correlation column for rolling corr calculations based on two input columns and given window.

    Parameters
    ----------
    quoted_identifier: left column quoted identifier.
    other_quoted_identifier: right column quoted identifier.
    window_expr: WindowSpec object for rolling calculations.
    window: size of the moving window.
    """
    # pearson correlation calculated using formula here: https://byjus.com/jee/correlation-coefficient/
    # corr = top_exp / (count_exp * sig_exp)

    # count of non-null values in the window
    count_exp = builtin("count_if")(
        col(quoted_identifier).is_not_null()
        & col(other_quoted_identifier).is_not_null()
    ).over(window_expr)

    # std_prod_exp = std_pop(x)*std_pop(y)
    std_prod_exp = stddev_pop(
        iff(
            col(quoted_identifier).is_null(),
            pandas_lit(None),
            col(other_quoted_identifier),
        )
    ).over(window_expr) * stddev_pop(
        iff(
            col(other_quoted_identifier).is_null(),
            pandas_lit(None),
            col(quoted_identifier),
        )
    ).over(
        window_expr
    )

    # top expr = sum(x,y) - (sum(x)*sum(y) / n)
    top_exp = (
        sum_(col(quoted_identifier) * col(other_quoted_identifier)).over(window_expr)
    ) - (
        sum_(
            iff(
                col(quoted_identifier).is_null(),
                pandas_lit(None),
                col(other_quoted_identifier),
            )
        ).over(window_expr)
        * (
            sum_(
                iff(
                    col(other_quoted_identifier).is_null(),
                    pandas_lit(None),
                    col(quoted_identifier),
                )
            ).over(window_expr)
        )
    ) / count_exp
    new_col = iff(
        count_exp.__eq__(window) & (count_exp * std_prod_exp).__gt__(0),
        top_exp / (count_exp * std_prod_exp),
        pandas_lit(None),
    )
    return new_col
