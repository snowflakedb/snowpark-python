#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import Sequence, Union

import numpy as np
import pandas
from pandas import Index, IntervalIndex
from pandas._typing import Scalar
from pandas.core.dtypes.common import is_numeric_dtype
from pandas.core.dtypes.inference import is_scalar
from pandas.core.reshape.tile import _is_dt_or_td

from snowflake.snowpark.functions import col, iff
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin._internal.join_utils import MatchComparator, join
from snowflake.snowpark.modin.plugin._internal.utils import pandas_lit
from snowflake.snowpark.modin.plugin.utils.error_message import ErrorMessage
from snowflake.snowpark.types import LongType


# This function stems from pandas 2.2.x and has been minimally modified to not require
# the full data, but instead work with min/max values solely. It replaces
# The pandas 2.1.x function from pandas.core.reshape.tile import _convert_bin_to_numeric_type.
def _nbins_to_bins(x_min: Scalar, x_max: Scalar, nbins: int, right: bool) -> Index:
    """
    If a user passed an integer N for bins, convert this to a sequence of N
    equal(ish)-sized bins.
    """
    if is_scalar(nbins) and nbins < 1:
        raise ValueError("`bins` should be a positive integer.")  # pragma: no cover

    # this snippet of original pandas code is handled outside of this function
    # if x_idx.size == 0:
    #    raise ValueError("Cannot cut empty array")

    # retrieve type of original series used in cut. To speed up processing,
    # infer from aggrgates as the type won't change when computing min/max.
    x_dtype = pandas.Series([x_min, x_max]).dtype
    rng = (x_min, x_max)
    mn, mx = rng

    if is_numeric_dtype(x_dtype) and (np.isinf(mn) or np.isinf(mx)):
        # GH#24314
        raise ValueError(  # pragma: no cover
            "cannot specify integer `bins` when input data contains infinity"  # pragma: no cover
        )  # pragma: no cover

    if mn == mx:  # adjust end points before binning
        if _is_dt_or_td(x_dtype):  # pragma: no cover
            # original pandas code (commented):
            # # using seconds=1 is pretty arbitrary here
            # # error: Argument 1 to "dtype_to_unit" has incompatible type
            # # "dtype[Any] | ExtensionDtype"; expected "DatetimeTZDtype | dtype[Any]"
            # unit = dtype_to_unit(x_dtype)  # type: ignore[arg-type]
            # td = Timedelta(seconds=1).as_unit(unit)
            # # Use DatetimeArray/TimedeltaArray method instead of linspace
            # # error: Item "ExtensionArray" of "ExtensionArray | ndarray[Any, Any]"
            # # has no attribute "_generate_range"
            # bins = x_idx._values._generate_range(  # type: ignore[union-attr]
            #     start=mn - td, end=mx + td, periods=nbins + 1, freq=None, unit=unit
            # )
            ErrorMessage.not_implemented(
                "no support for datetime types yet."
            )  # pragma: no cover
        else:
            mn -= 0.001 * abs(mn) if mn != 0 else 0.001  # pragma: no cover
            mx += 0.001 * abs(mx) if mx != 0 else 0.001  # pragma: no cover

            bins = np.linspace(mn, mx, nbins + 1, endpoint=True)  # pragma: no cover
    else:  # adjust end points after binning
        if _is_dt_or_td(x_dtype):
            # original pandas code (commented):
            # # Use DatetimeArray/TimedeltaArray method instead of linspace
            #
            # # error: Argument 1 to "dtype_to_unit" has incompatible type
            # # "dtype[Any] | ExtensionDtype"; expected "DatetimeTZDtype | dtype[Any]"
            # unit = dtype_to_unit(x_dtype)  # type: ignore[arg-type]
            # # error: Item "ExtensionArray" of "ExtensionArray | ndarray[Any, Any]"
            # # has no attribute "_generate_range"
            # bins = x_idx._values._generate_range(  # type: ignore[union-attr]
            #     start=mn, end=mx, periods=nbins + 1, freq=None, unit=unit
            # )
            ErrorMessage.not_implemented(
                "no support for datetime types yet."
            )  # pragma: no cover
        else:
            bins = np.linspace(mn, mx, nbins + 1, endpoint=True)
        adj = (mx - mn) * 0.001  # 0.1% of the range
        if right:
            bins[0] -= adj
        else:
            bins[-1] += adj

    return Index(bins)


def preprocess_bins_for_cut(
    x_min: Scalar,
    x_max: Scalar,
    bins: Union[int, Sequence[Scalar], pandas.IntervalIndex],
    right: bool,
    include_lowest: bool,
    precision: int,
) -> Union[int, Sequence[Scalar], pandas.IntervalIndex]:
    """
    Adjusts bins to be directly used with compute_bin_indices function below. bins for both qcut and cut are given either as int which will create equidistant bins,
     as list of scalars (typically float), or IntervalIndex (not supported).

    Args:
        x_min: minimum value of the data which will be binned
        x_max: maximum value of the data which will be binned
        bins: the bins according to pandas which will define the buckets
        right: if True use left-open intervals (a, b], if False use right-open intervals [a, b)
        include_lowest: If True and right is True, adjust the first interval by 10 ** (-precision), i.e. the first interval will be (a-10 ** (-precision), b]. This will include the minimum value in the binning process.
        precision: only used together with include_lowest to adjust the first bin (cf. include_lowest)

    Returns:
        adjusted bins
    """
    # Code is mostly from original pandas and adjusted for Snowpark pandas API.

    if not np.iterable(bins):
        # Call adjusted function from pandas 2.2.x branch
        assert type(bins) is int, f"type(bins) is not int but {type(bins)}"
        bins = _nbins_to_bins(x_min, x_max, bins, right)

    elif isinstance(bins, IntervalIndex):
        if bins.is_overlapping:  # pragma: no cover
            raise ValueError(
                "Overlapping IntervalIndex is not accepted."
            )  # pragma: no cover

    else:
        bins = Index(bins)
        if not bins.is_monotonic_increasing:
            raise ValueError("bins must increase monotonically.")

    # if include_lowest is True, then expand first bucket by 10 ** (-precision)
    # I.e., for right=True, intervals will have the form (a, b].
    # If a is now contained in the values, it will fall into (a - 10**(-precision), b].
    # For right=False, this is irrelevant. The expansion only works for right=True.
    if include_lowest and right:
        bins = Index([bins[0] - 10 ** (-precision)] + list(bins[1:].values))

    return bins


def compute_bin_indices(
    values_frame: InternalFrame,
    cuts_frame: InternalFrame,
    n_cuts: int,
    right: bool = True,
) -> InternalFrame:
    """
    Given a frame of cuts, i.e. borders of bins (strictly increasing) compute for the data in values_frame the index of the bin they fall into.
    E.g., cuts_frame may contain the following data
    0.0, 3.0, 7.8, 10.0
    This would form the following bins (0.0, 3.0], (3.0, 7.8], (7.8, 10.0].
    Consequently, this function will return indices in the range 0...2, e.g. for the following data

    -10.0, 0.0, 1.0, 5.6, 9.0, 10.0, 11.0

    the following bin indices

    nan, nan,  0.,  1.,  2.,  2., nan

    Note that NULL (nan) is returned for data which lies outside of the cuts provided.

    Args:
        values_frame: an InternalFrame representing a Series, the data to be binned.
        cuts_frame: an InternalFrame representing a Series with data being a strictly monotonically
         increasing sequence of floating numbers forming the border of bins.
        n_cuts: The length of cuts_frame. Passed in as separate parameter to avoid an additional query.
        right: if True use left-open intervals (a, b], if False use right-open intervals [a, b).
    Returns:
        InternalFrame representing a Series with the bin indices. indices will be in the range [0, n_cuts - 1].
    """

    # There will be 0, ..., len(cuts_frame) - 1 buckets, result will be thus in this range.
    # We can find for values the cut they belong to by performing a left <= join. As this feature is not supported
    # within OrderedDataFrame yet, we use the Snowpark layer directly. This should have no negative
    # consequences when it comes to building lazy graphs, as both cut and qcut are materializing operations.

    cuts_frame = cuts_frame.ensure_row_position_column()
    # perform asof join to find the closet to the cut frame data.
    asof_result = join(
        values_frame,
        cuts_frame,
        how="asof",
        left_match_col=values_frame.data_column_snowflake_quoted_identifiers[0],
        right_match_col=cuts_frame.data_column_snowflake_quoted_identifiers[0],
        match_comparator=MatchComparator.LESS_THAN_OR_EQUAL_TO
        if right
        else MatchComparator.GREATER_THAN_OR_EQUAL_TO,
        sort=False,
    )

    assert cuts_frame.row_position_snowflake_quoted_identifier is not None
    bin_index_col = col(
        asof_result.result_column_mapper.map_right_quoted_identifiers(
            [cuts_frame.row_position_snowflake_quoted_identifier]
        )[0]
    )

    if right:
        # An index value of 0 means the data is outside of the first bucket. Set to NULL. All others, perform -1.
        # For data outside of the last bucket, the left join will automatically fill it with NULL.
        correct_index_expr = iff(
            bin_index_col != pandas_lit(0),
            bin_index_col - pandas_lit(1),
            pandas_lit(None),
        ).astype(LongType())
    else:
        # For right=False, correct for the bin indices exceeding the max value n_cuts - 1. If the index is larger
        # than this number, then set to NULL.
        correct_index_expr = iff(
            bin_index_col >= pandas_lit(n_cuts - 1), pandas_lit(None), bin_index_col
        ).astype(LongType())

    new_frame = asof_result.result_frame.project_columns(
        [values_frame.data_column_pandas_labels[0]], [correct_index_expr]
    )

    return new_frame
