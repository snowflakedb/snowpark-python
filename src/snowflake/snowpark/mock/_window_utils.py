#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#


from snowflake.snowpark._internal.analyzer.expression import FunctionExpression, Literal
from snowflake.snowpark._internal.analyzer.window_expression import (
    CurrentRow,
    FirstValue,
    Lag,
    LastValue,
    Lead,
    UnboundedFollowing,
    UnboundedPreceding,
)


class EntireWindowIndexer:
    def __init__(self, *args, **kwargs) -> None:
        try:
            from pandas.api.indexers import BaseIndexer

            if BaseIndexer not in self.__class__.__bases__:
                self.__class__.__bases__ = (BaseIndexer,)
        except ImportError:
            pass
        super().__init__(*args, **kwargs)

    def get_window_bounds(self, num_values, min_periods, center, closed, step):
        import numpy as np

        start = np.empty(num_values, dtype=np.int64)
        end = np.empty(num_values, dtype=np.int64)
        for i in range(num_values):
            start[i] = 0
            end[i] = num_values

        return start, end


class RowFrameIndexer:
    def __init__(self, *args, **kwargs) -> None:
        try:
            from pandas.api.indexers import BaseIndexer

            if BaseIndexer not in self.__class__.__bases__:
                self.__class__.__bases__ = (BaseIndexer,)
        except ImportError:
            pass
        super().__init__(*args, **kwargs)

    def get_window_bounds(self, num_values, min_periods, center, closed, step):
        import numpy as np

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


RANK_RELATED_FUNCTIONS = (
    Lead,
    Lag,
    LastValue,
    FirstValue,
)

RANK_RELATED_FUNCTION_NAMES = (
    "row_number",
    "cume_dist",
    "dense_rank",
    "ntile",
    "percent_rank",
    "rank",
)


def is_rank_related_window_function(func):
    return isinstance(func, RANK_RELATED_FUNCTIONS) or (
        isinstance(func, FunctionExpression)
        and func.name in RANK_RELATED_FUNCTION_NAMES
    )
