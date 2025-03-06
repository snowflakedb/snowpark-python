#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime as dt
from typing import Any, Literal, NoReturn, Optional, Union

import modin.pandas as pd
from pandas._libs.lib import no_default, NoDefault
from pandas._libs.tslibs import to_offset
from pandas._typing import Frequency

from snowflake.snowpark._internal.type_utils import ColumnOrName
from snowflake.snowpark.column import Column
from snowflake.snowpark.functions import (
    builtin,
    dateadd,
    datediff,
    last_day,
    lit,
    to_timestamp_ntz,
    date_trunc,
    max as max_,
    min as min_,
)
from snowflake.snowpark.modin.plugin._internal import join_utils
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin._internal.join_utils import (
    InheritJoinIndex,
    MatchComparator,
    join,
)
from snowflake.snowpark.modin.plugin._internal.utils import pandas_lit
from snowflake.snowpark.modin.plugin.utils.error_message import ErrorMessage
from snowflake.snowpark.types import DateType, TimestampType

RESAMPLE_INDEX_LABEL = "__resample_index__"

SNOWFLAKE_TIMESLICE_ALIGNMENT_DATE = "1970-01-01 00:00:00"

IMPLEMENTED_AGG_METHODS = [
    "max",
    "min",
    "mean",
    "median",
    "sum",
    "std",
    "var",
    "count",
    "size",
    "first",
    "last",
    "quantile",
    "nunique",
    "indices",
]
SUPPORTED_RESAMPLE_RULES = ("second", "minute", "hour", "day", "week", "month", "year")
RULE_SECOND_TO_DAY = ("second", "minute", "hour", "day")
RULE_WEEK_TO_YEAR = ("week", "quarter", "month", "year")


# https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects
ALL_DATEOFFSET_STRINGS = [
    "B",
    "C",
    "W",
    "ME",
    "MS",
    "BME",
    "BMS",
    "CBME",
    "CBMS",
    "SME",
    "SMS",
    "QE",
    "QS",
    "BQE",
    "BQS",
    "YE",
    "YS",
    "BYS",
    "BYE",
    "bh",
    "cbh",
    "D",
    "h",
    "min",
    "s",
    "ms",
    "us",
    "ns",
]

SNOWFLAKE_SUPPORTED_DATEOFFSETS = [
    "s",
    "min",
    "h",
    "D",
    "W",
    "MS",
    "ME",
    "QS",
    "QE",
    "YS",
    "YE",
]

IMPLEMENTED_DATEOFFSET_STRINGS = ["s", "min", "h", "D", "W", "ME", "YE"]

UNSUPPORTED_DATEOFFSET_STRINGS = list(
    # sort so that tests that generate test cases from this last always use the
    # list in the same order (see SNOW-1000116).
    sorted(set(ALL_DATEOFFSET_STRINGS) - set(SNOWFLAKE_SUPPORTED_DATEOFFSETS))
)

NOT_IMPLEMENTED_DATEOFFSET_STRINGS = list(
    # sort so that tests that generate test cases from this last always use the
    # list in the same order (see SNOW-1000116).
    sorted(set(SNOWFLAKE_SUPPORTED_DATEOFFSETS) - set(IMPLEMENTED_DATEOFFSET_STRINGS))
)


def rule_to_snowflake_width_and_slice_unit(rule: Frequency) -> tuple[int, str]:
    """
    Converts pandas resample bin rule to Snowflake's slice_width and slice_unit
    format.

    Parameters
    ----------
    rule : Frequency
        The offset or string representing resample bin size. For example: '1D', '2T', etc.

    Returns
    -------
    slice_width : int
        Width of the slice (i.e. how many units of time are contained in the slice).

    slice_unit : str
        Time unit for the slice length.

    Raises
    ------
    ValueError
        A ValueError is raised if an invalid rule is passed in.

    NotImplementedError
        A NotImplementedError is raised if we cannot map the pandas rule to
        a Snowflake date or time unit.
    """

    try:
        offset = to_offset(rule)
    except ValueError:
        raise ValueError(f"Invalid frequency: {rule}.")

    rule_code = offset.rule_code
    slice_width = offset.n
    if rule_code == "s":
        slice_unit = "second"
    elif rule_code == "min":
        slice_unit = "minute"
    elif rule_code == "h":
        slice_unit = "hour"
    elif rule_code == "D":
        slice_unit = "day"
    elif rule_code[0] == "W":
        # treat codes like W-MON and W-SUN as "week":
        slice_unit = "week"
    elif rule_code == "ME":
        slice_unit = "month"
    elif rule_code[0:2] == "QE":  # pragma: no cover
        # treat codes like QE-DEC and QE-JAN as "quarter":
        slice_unit = "quarter"
    elif rule_code[0:2] == "YE":
        # treat codes like YE-DEC and YE-JAN as "year":
        slice_unit = "year"
    else:
        raise NotImplementedError(
            f"Unsupported frequency: {rule}. Snowpark pandas cannot map {rule} "
            f"to a Snowflake date or time unit."
        )

    return slice_width, slice_unit


def _argument_not_implemented(param: str, arg: Any) -> Optional[NoReturn]:
    """
    Raises a NotImplementedError for an argument `arg`
    that is unsupported by parameter `param`.

    Parameters
    ----------
    param : str
        Name of the parameter.

    arg : Any
        Unsupported argument of parameter `param`.

    Raises
    ------
    NotImplementedError
    """
    return ErrorMessage.not_implemented(
        f"Resample argument {arg} for parameter {param} is not implemented for Resampler!"
    )


def validate_resample_supported_by_snowflake(
    resample_kwargs: dict[str, Any]
) -> Optional[NoReturn]:
    """
    Checks whether execution with Snowflake engine is available for resample operation.

    Parameters:
    ----------
    resample_kwargs : Dict[str, Any]
        keyword arguments of Resample operation. rule, axis, axis, etc.

    Raises
    ------
    NotImplementedError
        Raises a NotImplementedError if a keyword argument of resample has an
        unsupported parameter-argument combination.
    """
    rule = resample_kwargs.get("rule")

    _, slice_unit = rule_to_snowflake_width_and_slice_unit(rule)

    if slice_unit not in SUPPORTED_RESAMPLE_RULES:
        _argument_not_implemented("rule", rule)

    axis = resample_kwargs.get("axis")
    if axis != 0:  # pragma: no cover
        _argument_not_implemented("axis", axis)

    closed = resample_kwargs.get("closed")
    if closed not in ("left", None) and slice_unit in RULE_SECOND_TO_DAY:
        _argument_not_implemented("closed", closed)
    if slice_unit in RULE_WEEK_TO_YEAR:
        if closed != "left":
            ErrorMessage.not_implemented(
                f"resample with rule offset {rule} is only implemented with closed='left'"
            )

    label = resample_kwargs.get("label")
    if label is not None:  # pragma: no cover
        _argument_not_implemented("label", label)

    convention = resample_kwargs.get("convention")
    if convention != "start":  # pragma: no cover
        _argument_not_implemented("convention", convention)

    kind = resample_kwargs.get("kind")
    if kind is not None:  # pragma: no cover
        _argument_not_implemented("kind", kind)

    level = resample_kwargs.get("level")
    if level is not None:  # pragma: no cover
        _argument_not_implemented("level", level)

    origin = resample_kwargs.get("origin")
    if origin != "start_day":  # pragma: no cover
        _argument_not_implemented("origin", origin)

    offset = resample_kwargs.get("offset")
    if offset is not None:  # pragma: no cover
        _argument_not_implemented("offset", offset)

    group_keys = resample_kwargs.get("group_keys")
    if group_keys is not no_default:  # pragma: no cover
        _argument_not_implemented("group_keys", group_keys)

    return None


def get_snowflake_quoted_identifier_for_resample_index_col(frame: InternalFrame) -> str:
    """
    Returns Snowflake quoted identifier of a column corresponding to a DatetimeIndex in an InternalFrame
    `frame`. Raises TypeError, if more than one index column is present, or index column can not be interpreted as a
    DatetimeIndex column.

    Parameters
    ----------
    frame : InternalFrame
        Internal frame to perform resampling on.

    Returns
    -------
    index_col : str
        Snowflake quoted identifier of a column corresponding to a DatetimeIndex in an InternalFrame

    Raises
    ------
        TypeError if the dataframe's index is not a DatetimeIndex.
    """

    index_cols = frame.index_column_snowflake_quoted_identifiers

    if len(index_cols) > 1:
        raise TypeError(
            "Only valid with DatetimeIndex, but got an instance of 'MultiIndex'"
        )

    index_col = index_cols[0]
    sf_type = frame.get_snowflake_type(index_col)

    if not isinstance(sf_type, (TimestampType, DateType)):
        raise TypeError("Only valid with DatetimeIndex or TimedeltaIndex")

    return index_col


def time_slice(
    column: ColumnOrName,
    slice_length: int,
    date_or_time_part: str,
    start_or_end: Union[str, Literal["start"]] = "start",
) -> Column:
    """
    Calculates the beginning or end of a “slice” of time, where
    the length of the slice is a multiple of a standard unit of
    time (minute, hour, day, etc.).

    `Supported date and time parts <https://docs.snowflake.com/en/sql-reference/functions-date-time.html#label-supported-date-time-parts>`_

    Parameters
    ----------
    column : ColumnOrName
        The timestamp column to calculate the time slice of.

    slice_length : str
        Width of the slice (i.e. how many units of time are contained
        in the slice). For example, if the unit is MONTH and the slice_length is 2, then
        each slice is 2 months wide. The slice_length must be an integer greater than or equal to 1.

    date_or_time_part : str
        Time unit for the slice length.

    start_or_end : str, default 'start'
        Determines whether the start or end of the slice should be returned.

    Returns
    -------
    column : Column
        Beginning or end of a "slice" of time.
    """
    return builtin("TIME_SLICE")(column, slice_length, date_or_time_part, start_or_end)


def compute_resample_start_and_end_date(
    frame: InternalFrame,
    datetime_index_col_identifier: str,
    rule: Frequency,
    *,
    origin_is_start_day: bool = False,
) -> tuple[str, str]:
    """
    Compute the start and end datetimes implied by `rule`, returning start_date and end_date.

    This computation is done eagerly, as start_date and end_date must be known to determine
    resample bins.

    If origin_is_start_day is passed, then the returned start_date will truncate the date of
    the smallest timestamp, then add multiples of the frequency until the smallest timestamp
    is in a bin. That is,

        start_date = date_trunc(DAY, min(datetime_col)) + (k * freq)
        end_date = date_trunc(DAY, min(datetime_col)) + (j * freq)
        start_date <= min(datetime_col)
        end_date <= max(datetime_col)

    for the largest possible integers k and j.
    """
    slice_width, slice_unit = rule_to_snowflake_width_and_slice_unit(rule)

    min_max_index_column_quoted_identifier = (
        frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
            pandas_labels=["min_index", "max_index"]
        )
    )

    # There are two reasons for why we eagerly compute these values:
    # 1. The earliest date, start_date, is needed to perform resampling binning.
    # 2. start_date and end_date are used to fill in any missing resample bins for the frame.

    # date_trunc gives us the correct start date.
    # For instance, if rule='3D' and the earliest date is
    # 2020-03-01 1:00:00, the first date should be 2020-03-01,
    # which is what date_trunc gives us.
    if slice_unit in RULE_SECOND_TO_DAY:
        # `slice_unit` in 'second', 'minute', 'hour', 'day'
        start_date, end_date = frame.ordered_dataframe.agg(
            date_trunc(slice_unit, min_(datetime_index_col_identifier)).as_(
                min_max_index_column_quoted_identifier[0]
            ),
            date_trunc(slice_unit, max_(datetime_index_col_identifier)).as_(
                min_max_index_column_quoted_identifier[1]
            ),
        ).collect()[0]
        if origin_is_start_day:
            # If this resample was called with origin=start_day, then manually compute the correct
            # bins that are an integer multiple of the slice width starting from midnight of the
            # start date. This is easier to express in plain Python than SQL, and we already performed
            # a query anyway.
            start_day_base = dt.datetime(
                start_date.year, start_date.month, start_date.day
            )
            # Now, compute
            #   start_date = start_day_base + (k * freq)
            #   start_date <= min(datetime_col)
            #   end_date = start_day_base + (j * freq)
            #   end_date <= max(datetime_col)
            # for the largest possible integers k and j.
            # The inequalities solve as follows:
            #   k <= (min(datetime_col) - start_day_base) / freq
            #   j <= (max(datetime_col) - start_day_base) / freq
            # and since we're only interested in integer values of k and j, we can just floor the right
            # side of the inequalities to get their values.
            increment = dt.timedelta(**{f"{slice_unit}s": slice_width})
            k = int((start_date - start_day_base) / increment)
            j = int((end_date - start_day_base) / increment)
            start_date = start_day_base + (k * increment)
            end_date = start_day_base + (j * increment)
    else:
        assert slice_unit in RULE_WEEK_TO_YEAR
        # `slice_unit` in 'week', 'month', 'quarter', or 'year'. Set the start and end dates
        # to the last day of the given `slice_unit`. Use the right bin edge by adding a `slice_width`
        # of the given `slice_unit` to the first and last date of the index.
        start_date, end_date = frame.ordered_dataframe.agg(
            last_day(
                date_trunc(
                    slice_unit,
                    dateadd(
                        slice_unit,
                        pandas_lit(slice_width),
                        min_(datetime_index_col_identifier),
                    ),
                ),
                slice_unit,
            ).as_(min_max_index_column_quoted_identifier[0]),
            last_day(
                date_trunc(
                    slice_unit,
                    dateadd(
                        slice_unit,
                        pandas_lit(slice_width),
                        max_(datetime_index_col_identifier),
                    ),
                ),
                slice_unit,
            ).as_(min_max_index_column_quoted_identifier[1]),
        ).collect()[0]
    return start_date, end_date


def perform_resample_binning_on_frame(
    frame: InternalFrame,
    datetime_index_col_identifier: str,
    start_date: str,
    slice_width: int,
    slice_unit: str,
    *,
    resample_output_col_identifier: Optional[str] = None,
) -> InternalFrame:
    """
    Returns a new dataframe where each item of the index column
    is set to its resample bin.

    Parameters
    ----------
    frame : InternalFrame
        The internal frame with a single DatetimeIndex column
        to perform resample binning on.

    datetime_index_col_identifier : str
        The datetime-like column snowflake quoted identifier to use for resampling.

    start_date : str
        The earliest date in the Datetime index column of
        `frame`.

    slice_width : int
        Width of the slice (i.e. how many units of time are contained in the slice).

    slice_unit : str
        Time unit for the slice length.

    resample_output_col_identifier : Optional[str]
        The identifier of the column for the resampled output. If left unspecified, then
        datetime_index_col_identifier is overwritten.

    Returns
    -------
    frame : InternalFrame
        A new internal frame where items in the index column are
        placed in a bin based on `slice_width` and `slice_unit`
    """
    if resample_output_col_identifier is None:
        resample_output_col_identifier = datetime_index_col_identifier
    # Consider the following example:
    # frame:
    #             data_col
    # date
    # 2023-08-07         1
    # 2023-08-08         2
    # 2023-08-09         3
    # 2023-08-10         4
    # 2023-08-11         5
    # 2023-08-14         6
    # 2023-08-15         7
    # 2023-08-16         8
    # 2023-08-17         9
    # start_date = 2023-08-07, rule = 3D (3 days)

    # Time slices in Snowflake are aligned to snowflake_timeslice_alignment_date,
    # so we must normalize input datetimes.
    normalization_amt = (
        pd.to_datetime(start_date) - pd.to_datetime(SNOWFLAKE_TIMESLICE_ALIGNMENT_DATE)
    ).total_seconds()

    # Subtract the normalization amount in seconds from the input datetime.
    normalized_dates = to_timestamp_ntz(
        datediff(
            "second",
            to_timestamp_ntz(lit(normalization_amt)),
            datetime_index_col_identifier,
        )
    )
    # frame:
    #             data_col
    # date
    # 1970-01-01         1
    # 1970-01-02         2
    # 1970-01-03         3
    # 1970-01-04         4
    # 1970-01-05         5
    # 1970-01-08         6
    # 1970-01-09         7
    # 1970-01-10         8
    # 1970-01-11         9

    # Call time_slice on the normalized datetime column with the slice_width and slice_unit.
    # time_slice is not supported for timestamps with timezones, only TIMESTAMP_NTZ
    normalized_dates_set_to_bins = time_slice(
        column=normalized_dates,
        slice_length=slice_width,
        date_or_time_part=slice_unit,
        start_or_end="start" if slice_unit in RULE_SECOND_TO_DAY else "end",
    )
    # frame:
    #             data_col
    # date
    # 1970-01-01         1
    # 1970-01-01         2
    # 1970-01-01         3
    # 1970-01-04         4
    # 1970-01-04         5
    # 1970-01-07         6
    # 1970-01-07         7
    # 1970-01-10         8
    # 1970-01-10         9

    # Add the normalization amount in seconds back to the input datetime for the correct result.
    unnormalized_dates_set_to_bins = (
        dateadd("second", lit(normalization_amt), normalized_dates_set_to_bins)
        if slice_unit in RULE_SECOND_TO_DAY
        else to_timestamp_ntz(
            last_day(
                dateadd("second", lit(normalization_amt), normalized_dates_set_to_bins),
                slice_unit,
            )
        )
    )
    # frame:
    #             data_col
    # date
    # 2023-08-07         1
    # 2023-08-07         2
    # 2023-08-07         3
    # 2023-08-10         4
    # 2023-08-10         5
    # 2023-08-13         6
    # 2023-08-13         7
    # 2023-08-16         8
    # 2023-08-16         9

    return frame.update_snowflake_quoted_identifiers_with_expressions(
        {resample_output_col_identifier: unnormalized_dates_set_to_bins}
    ).frame


def get_expected_resample_bins_frame(
    rule: str,
    start_date: str,
    end_date: str,
    *,
    index_label: Union[str, None, NoDefault] = no_default,
) -> InternalFrame:
    """
    Returns an InternalFrame with a single DatetimeIndex column that holds the
    expected resample bins computed using rule, start_date, and end_date.
    Parameters:
    ----------
    rule : str
        The offset string or object representing target conversion.

    start_date : str
        The earliest date in the timeseries data.

    end_date : str
        The latest date in the timeseries data.

    index_label : Optional[str] | NoDefault, default no_default
        The value to use as the pandas label of the resampled column. Defaults to RESAMPLE_INDEX_LABEL
        if left unspecified.
        Note that a None value is a valid pandas label, which will be used if explicitly specified.

    Returns
    -------
    frame : InternalFrame
        A new internal frame with the expected resample bins.

    Examples
    --------
    frame = get_expected_resample_bins_frame("2D", "2020-01-03", "2020-01-10")

    frame:
    __resample_index__
    2020-01-03
    2020-01-05
    2020-01-07
    2020-01-09
    """
    expected_resample_bins_snowpark_frame = pd.date_range(
        start_date, end_date, freq=rule
    )._query_compiler._modin_frame
    return InternalFrame.create(
        ordered_dataframe=expected_resample_bins_snowpark_frame.ordered_dataframe,
        data_column_pandas_labels=[],
        data_column_snowflake_quoted_identifiers=[],
        index_column_pandas_labels=[
            RESAMPLE_INDEX_LABEL if index_label == no_default else index_label
        ],
        index_column_snowflake_quoted_identifiers=expected_resample_bins_snowpark_frame.index_column_snowflake_quoted_identifiers,
        data_column_pandas_index_names=[None],
        data_column_types=None,
        index_column_types=None,
    )


def fill_missing_resample_bins_for_frame(
    frame: InternalFrame, rule: str, start_date: str, end_date: str
) -> InternalFrame:
    """
    Returns a new InternalFrame created using 2 rules.
    1. Missing resample bins in `frame`'s DatetimeIndex column will be created.
    2. Missing rows in data column will be filled with `None`.

    Parameters:
    ----------
    frame : InternalFrame
        A frame with a single DatetimeIndex column.

    rule : str
        The offset string or object representing target conversion.

    start_date : str
        The earliest date in the DatetimeIndex column of `frame`.

    end_date : str
        The latest date in the DatetimeIndex column of `frame`.

    Returns
    -------
    frame : InternalFrame
        A new internal frame with no missing rows in the resample operation.

    Examples
    --------
    input_frame
                a   b
    __index__
    2020-01-03  1   2
    2020-01-07  3   5
    2020-01-09  4   6

    frame = fill_missing_resample_bins_for_frame(input_frame, '2D', "2020-01-03", "2020-01-12")

    frame:
                  a     b
    __index__
    2020-01-03    1     2
    2020-01-05  NaN   NaN
    2020-01-07    3     5
    2020-01-09    4     6
    2020-01-11  NaN   NaN
    """
    # Compute expected resample bins based on start_date, end_date and rule.
    expected_resample_bins_frame = get_expected_resample_bins_frame(
        rule, start_date, end_date
    )
    # For example, if start_date = '2020-01-01', end_date = '2020-01-05' and rule = '1D'
    #
    # expected_resample_bins_frame:
    # __resample_index__
    # 2020-01-01
    # 2020-01-02
    # 2020-01-03
    # 2020-01-04
    # 2020-01-05

    # Join on expected expected_resample_bins_frame to fill in missing resample bins.
    # Suppose the expected resample bins is as shown above.
    # and `frame` is missing resample bins. (2020-01-03 is missing)
    #
    # frame:
    #             agg_result
    #   date_col
    # 2020-01-01           1
    # 2020-01-02           2
    # 2020-01-04           3
    # 2020-01-05           4
    #
    # After the join, the missing date is populated in `frame`'s
    # DatetimeIndex column and a None is found in the data column.
    #
    # resample_bins_dataframe:
    #             agg_result
    #   date_col
    # 2020-01-01           1
    # 2020-01-02           2
    # 2020-01-03        None
    # 2020-01-04           3
    # 2020-01-05           4
    joined_frame = join(
        left=frame,
        right=expected_resample_bins_frame,
        how="right",
        left_on=frame.index_column_snowflake_quoted_identifiers,
        right_on=expected_resample_bins_frame.index_column_snowflake_quoted_identifiers,
        inherit_join_index=InheritJoinIndex.FROM_RIGHT,
    ).result_frame

    # Ensure data_column_pandas_index_names is correct.
    return InternalFrame.create(
        ordered_dataframe=joined_frame.ordered_dataframe,
        data_column_pandas_labels=frame.data_column_pandas_labels,
        data_column_snowflake_quoted_identifiers=frame.data_column_snowflake_quoted_identifiers,
        index_column_pandas_labels=frame.index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=joined_frame.index_column_snowflake_quoted_identifiers,
        data_column_pandas_index_names=frame.data_column_pandas_index_names,
        data_column_types=frame.cached_data_column_snowpark_pandas_types,
        index_column_types=frame.cached_index_column_snowpark_pandas_types,
    )


def perform_asof_join_on_frame(
    preserving_frame: InternalFrame, referenced_frame: InternalFrame, fill_method: str
) -> InternalFrame:
    """
    Returns a new InternalFrame that performs an ASOF join on the preserving
    frame against the referenced frame. All frame metadata, such as data column
    and index column labels, are inherited from referenced_frame. For each timestamp,
    p, in preserving_frame's DatetimeIndex, the join finds a single row in
    referenced_frame with timestamp, r, such that r <= p. The qualifying row on selected
    from referenced_frame is the closest match, either equal in time or earlier in time.
    If a qualifying row is not found in the referenced_frame, the data columns are padded
    with NULL values.

    Parameters
    ----------
    preserving_frame : InternalFrame
       The frame to select the closest match for using its DatetimeIndex.

    referenced_frame: InternalFrame
        The frame to select the closest match from using its DatetimeIndex.

    fill_method: str
        The method to use for filling values.

    Returns
    -------
    frame : InternalFrame
        A new frame that holds the result of an ASOF join.
    """
    # Consider the following example where we want to perform an ASOF JOIN of preserving_frame
    # and referenced_frame where __resample_index__ >= __index__ if forward fill
    # or __resample_index__ <= __index__ if backward fill:
    #
    # preserved_frame:
    #  __resample_index__
    # 2023-01-03 00:00:00
    # 2023-01-05 00:00:00
    # 2023-01-07 00:00:00
    # 2023-01-09 00:00:00
    #
    # referenced_frame:
    #                         a
    #           __index__
    # 2023-01-03 01:00:00     1
    # 2023-01-04 00:00:00     2
    # 2023-01-05 23:00:00     3
    # 2023-01-06 00:00:00     4
    # 2023-01-07 02:00:00   NaN
    # 2023-01-10 00:00:00     6

    left_timecol_snowflake_quoted_identifier = (
        get_snowflake_quoted_identifier_for_resample_index_col(preserving_frame)
    )
    right_timecol_snowflake_quoted_identifier = (
        get_snowflake_quoted_identifier_for_resample_index_col(referenced_frame)
    )
    output_frame, _ = join_utils.join(
        left=preserving_frame,
        right=referenced_frame,
        how="asof",
        left_match_col=left_timecol_snowflake_quoted_identifier,
        right_match_col=right_timecol_snowflake_quoted_identifier,
        match_comparator=(
            MatchComparator.GREATER_THAN_OR_EQUAL_TO
            if fill_method == "ffill"
            else MatchComparator.LESS_THAN_OR_EQUAL_TO
        ),
        sort=True,
    )
    # output_frame:
    #                            a
    #  __resample_index__
    # 2023-01-03 00:00:00     NULL
    # 2023-01-05 00:00:00        2
    # 2023-01-07 00:00:00        4
    # 2023-01-09 00:00:00     NULL
    return output_frame
