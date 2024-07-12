#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#


from typing import Optional

from pandas import NaT, Timestamp
from pandas._libs.tslibs.offsets import BaseOffset, to_offset
from pandas.core.arrays._ranges import _generate_range_overflow_safe

from snowflake.snowpark import DataFrame
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import builtin, col, to_time
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin._internal.ordered_dataframe import (
    DataFrameReference,
    OrderedDataFrame,
)
from snowflake.snowpark.modin.plugin._internal.utils import pandas_lit
from snowflake.snowpark.modin.plugin.compiler import snowflake_query_compiler
from snowflake.snowpark.modin.plugin.utils.error_message import ErrorMessage


def generate_regular_range(
    start: Optional[Timestamp],
    end: Optional[Timestamp],
    periods: Optional[int],
    freq: BaseOffset,
) -> "snowflake_query_compiler.SnowflakeQueryCompiler":
    """
    Generate a range of timestamps with the spans between dates
    described by the given `freq` DateOffset.

    Parameters
    ----------
    start : Timedelta, Timestamp or None
        First point of produced date range.
    end : Timedelta, Timestamp or None
        Last point of produced date range.
    periods : int or None
        Number of periods in produced date range.
    freq : Tick
        Describes space between dates in produced date range.

    Returns
    -------
    A SnowflakeQueryCompiler with a single int column representing nanoseconds.
    """
    istart = start.value if start is not None else None
    iend = end.value if end is not None else None
    stride = freq.nanos

    # generate start, end, and stride (the logic below is copied from generate_regular_range() method at
    # pandas/core/arrays/_ranges.py#L24
    if periods is None and istart is not None and iend is not None:
        b = istart
        # cannot just use e = Timestamp(end) + 1 because arange breaks when
        # stride is too large, see GH10887
        e = b + (iend - b) // stride * stride + stride // 2 + 1
    elif istart is not None and periods is not None:
        b = istart
        e = _generate_range_overflow_safe(b, periods, stride, side="start")
    elif iend is not None and periods is not None:
        e = iend + stride
        b = _generate_range_overflow_safe(e, periods, stride, side="end")
    else:
        raise ValueError(  # pragma: no cover
            "at least 'start' or 'end' should be specified if a 'period' is given."
        )
    return generate_range(b, e, stride)


def _create_qc_from_snowpark_dataframe(
    sp_df: DataFrame,
) -> "snowflake_query_compiler.SnowflakeQueryCompiler":
    """
    Create a Snowflake query compiler from a Snowpark DataFrame, assuming the DataFrame only contains one column.

    Args:
        sp_df: the Snowpark DataFrame

    Returns:
        A Snowflake query compiler
    """
    odf = OrderedDataFrame(DataFrameReference(sp_df)).ensure_row_position_column()

    from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
        SnowflakeQueryCompiler,
    )

    return SnowflakeQueryCompiler(
        InternalFrame.create(
            ordered_dataframe=odf,
            data_column_pandas_labels=[None],
            data_column_pandas_index_names=[None],
            data_column_snowflake_quoted_identifiers=odf.projected_column_snowflake_quoted_identifiers[
                :-1
            ],
            index_column_pandas_labels=[None],
            index_column_snowflake_quoted_identifiers=[
                odf.row_position_snowflake_quoted_identifier
            ],
        )
    )


def generate_range(
    start: int,
    end: Optional[int],
    step: int,
) -> "snowflake_query_compiler.SnowflakeQueryCompiler":
    """
    Use `session.range` to generate values in range and represent in a query compiler

    Args:
        start: start number
        end: end number
        step: step number

    Returns:
        The query compiler containing int values
    """
    return _create_qc_from_snowpark_dataframe(
        get_active_session().range(start, end, step)
    )


# The mapping from irregular pandas DateOffset to Snowflake date_or_item_part
# See https://pandas.pydata.org/pandas-docs/version/1.5/user_guide/timeseries.html#timeseries-offset-aliases
# See https://docs.snowflake.com/en/sql-reference/functions-date-time#label-supported-date-time-parts
OFFSET_NAME_TO_SF_DATE_OR_TIME_PART_MAP = {
    "ME": "month",
    "MS": "month",
    "W-SUN": "week",
    "QS-JAN": "quarter",
    "QE-DEC": "quarter",
    "YS-JAN": "year",
    "YE-DEC": "year",
}
# The offset names requires last day of a frequency, e.g., "M" means the last day of a month.
LAST_DAY = {"ME", "QE-DEC", "YE-DEC"}


def _offset_name_to_sf_date_or_time_part(name: str) -> Optional[str]:
    """
    Map pandas offset name to Snowflake date_or_time_part.

    Args:
        name: pandas offset name

    Returns:
        Snowflake date_or_time_part

    Raises:
        NotImplementedError if the offset name is not supported.
    """
    if name in OFFSET_NAME_TO_SF_DATE_OR_TIME_PART_MAP:
        return OFFSET_NAME_TO_SF_DATE_OR_TIME_PART_MAP[name]
    ErrorMessage.not_implemented(
        f"offset {name} is not implemented in Snowpark pandas API"
    )
    return None


def generate_irregular_range(
    start: Optional[Timestamp],
    end: Optional[Timestamp],
    periods: Optional[int],
    offset: BaseOffset,
) -> "snowflake_query_compiler.SnowflakeQueryCompiler":
    """
    Generates a sequence of dates corresponding to the specified time
    offset.

    Args:
        start : datetime
        end : datetime
        periods : int
        offset : DateOffset

    Returns:
        The query compiler containing the generated datetime values
    """
    offset = to_offset(offset)

    start = Timestamp(start)
    start = start if start is not NaT else None
    end = Timestamp(end)
    end = end if end is not NaT else None

    if start:
        start = offset.rollforward(start)

    if end:
        end = offset.rollback(end)

    if periods is None and end < start and offset.n >= 0:
        end = None
        periods = 0

    if end is None:
        end = start + (periods - 1) * offset  # type: ignore[operator]

    if start is None:
        start = end - (periods - 1) * offset  # type: ignore[operator]

    if periods is None:
        periods = 0
        while start + periods * offset <= end:
            periods += 1

    num_offsets = get_active_session().range(start=0, end=periods, step=1)
    sf_date_or_time_part = _offset_name_to_sf_date_or_time_part(offset.name)
    dt_col = builtin("DATEADD")(
        sf_date_or_time_part,
        offset.n * col(num_offsets.columns[0]),
        pandas_lit(start),
    )
    if offset.name in LAST_DAY:
        # When last day is required, we need to explicitly call LAST_DAY SQL function to convert DATEADD results to the
        # last day, e.g., adding one month to "2/29/2024" using DATEADD results "3/29/2024", which is not the last day
        # of March. So we need to call LAST_DAY. Also, LAST_DAY only return the date, then we need to reconstruct the
        # timestamp using timestamp_ntz_from_parts
        dt_col = builtin("timestamp_ntz_from_parts")(
            builtin("LAST_DAY")(dt_col, sf_date_or_time_part), to_time(dt_col)
        )
    dt_values = num_offsets.select(dt_col)
    return _create_qc_from_snowpark_dataframe(dt_values)
