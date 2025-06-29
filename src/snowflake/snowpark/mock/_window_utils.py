#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

# NOTE: This module avoids importing pandas or analyzer modules at module level
# to preserve lazy loading. Window function classes are created dynamically when needed.


def _get_entire_window_indexer():
    """Create EntireWindowIndexer class that properly inherits from pandas.BaseIndexer"""
    from snowflake.snowpark.mock._options import pandas as pd
    import numpy as np

    class EntireWindowIndexer(pd.api.indexers.BaseIndexer):
        def get_window_bounds(self, num_values, min_periods, center, closed, step):
            start = np.zeros(num_values, dtype=np.int64)
            end = np.full(num_values, num_values, dtype=np.int64)
            return start, end

    return EntireWindowIndexer


def _get_row_frame_indexer():
    """Create RowFrameIndexer class that properly inherits from pandas.BaseIndexer"""
    from snowflake.snowpark.mock._options import pandas as pd
    import numpy as np

    # Import analyzer types only when this function is called
    from snowflake.snowpark._internal.analyzer.window_expression import (
        CurrentRow,
        UnboundedPreceding,
        UnboundedFollowing,
    )
    from snowflake.snowpark._internal.analyzer.expression import Literal

    class RowFrameIndexer(pd.api.indexers.BaseIndexer):
        def __init__(self, frame_spec=None, *args, **kwargs):  # noqa: FIR100
            super().__init__(*args, **kwargs)
            self.frame_spec = frame_spec

        def get_window_bounds(self, num_values, min_periods, center, closed, step):
            start = np.empty(num_values, dtype=np.int64)
            end = np.empty(num_values, dtype=np.int64)

            upper = self.frame_spec.upper
            lower = self.frame_spec.lower

            for i in range(num_values):
                if isinstance(lower, CurrentRow):
                    start[i] = i
                elif isinstance(lower, UnboundedPreceding):
                    start[i] = 0
                else:
                    assert isinstance(lower, Literal)
                    start[i] = max(0, min(i + lower.value, num_values))

                if isinstance(upper, CurrentRow):
                    end[i] = i + 1  # + 1 to include the right endpoint
                elif isinstance(upper, UnboundedFollowing):
                    end[i] = num_values
                else:
                    assert isinstance(upper, Literal)
                    end[i] = max(
                        0, min(i + upper.value + 1, num_values)
                    )  # + 1 to include the right endpoint

            return start, end

    return RowFrameIndexer


def is_rank_related_window_function(func):
    """Check if function is rank-related window function (lazy imports)"""
    # Import analyzer types only when this function is called
    from snowflake.snowpark._internal.analyzer.expression import FunctionExpression
    from snowflake.snowpark._internal.analyzer.window_expression import (
        Lead,
        Lag,
        LastValue,
        FirstValue,
    )

    RANK_RELATED_FUNCTIONS = (Lead, Lag, LastValue, FirstValue)
    RANK_RELATED_FUNCTION_NAMES = (
        "row_number",
        "cume_dist",
        "dense_rank",
        "ntile",
        "percent_rank",
        "rank",
    )

    return isinstance(func, RANK_RELATED_FUNCTIONS) or (
        isinstance(func, FunctionExpression)
        and func.name in RANK_RELATED_FUNCTION_NAMES
    )


# Legacy classes for backward compatibility - these are deprecated
# Use _get_entire_window_indexer() and _get_row_frame_indexer() instead
class EntireWindowIndexer:
    """DEPRECATED: Use _get_entire_window_indexer() function instead"""

    def __init__(self, *args, **kwargs) -> None:
        import warnings

        warnings.warn(
            "EntireWindowIndexer class is deprecated. Use _get_entire_window_indexer() function.",
            DeprecationWarning,
            stacklevel=2,
        )

    def get_window_bounds(self, num_values, min_periods, center, closed, step):
        raise NotImplementedError(
            "Legacy EntireWindowIndexer class doesn't work. Use _get_entire_window_indexer() function."
        )


class RowFrameIndexer:
    """DEPRECATED: Use _get_row_frame_indexer() function instead"""

    def __init__(self, frame_spec=None, *args, **kwargs) -> None:
        import warnings

        warnings.warn(
            "RowFrameIndexer class is deprecated. Use _get_row_frame_indexer() function.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.frame_spec = frame_spec

    def get_window_bounds(self, num_values, min_periods, center, closed, step):
        raise NotImplementedError(
            "Legacy RowFrameIndexer class doesn't work. Use _get_row_frame_indexer() function."
        )
