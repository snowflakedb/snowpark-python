#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
#
# This file contains utils functions used by the groupby functionalities.
#
#
from typing import Any

from snowflake.snowpark.modin.plugin.utils.error_message import ErrorMessage

IMPLEMENTED_AGG_FUNCS = ["sum", "mean", "var", "std", "min", "max"]


def check_is_window_supported_by_snowflake(rolling_kwargs: dict[str, Any]) -> None:
    """
    Check if execution with snowflake engine is available for the window operation.

    Parameters
    ----------
    rolling_kwargs: keyword arguments passed for to rolling. The rolling keywords handled in the
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
            Execute the rolling operation per single column or row ('single') or over the entire object ('table').
            This argument is only implemented when specifying engine='numba' in the method call.

    Returns
    -------
    bool
        Whether operations can be executed with snowflake sql engine.
    """
    # Snowflake pandas implementation only supports integer window_size, min_periods >= 1, and center on axis = 0
    window = rolling_kwargs.get("window")
    min_periods = rolling_kwargs.get("min_periods")
    win_type = rolling_kwargs.get("win_type")
    on = rolling_kwargs.get("on")
    axis = rolling_kwargs.get("axis", 0)
    closed = rolling_kwargs.get("closed")
    step = rolling_kwargs.get("step")
    # Method is only used for the numba engine, so no need to check the param/raise a warning to the user.

    # Raise not implemented error for unsupported params
    if not isinstance(window, int):
        ErrorMessage.method_not_implemented_error(
            name="Non-integer window", class_="Rolling"
        )
    if min_periods is None or min_periods == 0:
        ErrorMessage.method_not_implemented_error(
            name=f"min_periods {min_periods}", class_="Rolling"
        )
    if win_type:
        ErrorMessage.method_not_implemented_error(
            name="win_type", class_="Rolling"
        )  # pragma: no cover
    if on:
        ErrorMessage.method_not_implemented_error(
            name="on", class_="Rolling"
        )  # pragma: no cover
    if axis not in (0, "index"):
        # Note that this is deprecated since pandas 2.1.0
        ErrorMessage.method_not_implemented_error(
            name="axis = 1", class_="Rolling"
        )  # pragma: no cover
    if closed:
        ErrorMessage.method_not_implemented_error(
            name="closed", class_="Rolling"
        )  # pragma: no cover
    if step:
        ErrorMessage.method_not_implemented_error(
            name="step", class_="Rolling"
        )  # pragma: no cover
