#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import numpy as np
import pandas as native_pd

TEST_DFS = [
    native_pd.DataFrame(),
    native_pd.DataFrame({"col1": [1, 2, 3], "col2": [3, 4, 5], "col3": [5, 6, 7]}),
    native_pd.DataFrame(
        data={"col1": [1, 2, 3], "col2": [3, 4, 5]},
        index=native_pd.Index([[1, 2], [2, 3], [3, 4]]),
    ),
    native_pd.DataFrame([1]),
    native_pd.DataFrame(
        data={"col1": [1, 2, 3], "col2": [3, 4, 5]},
        index=native_pd.DatetimeIndex(["2024-01-01", "2024-02-01", "2024-03-01"]),
    ),
    native_pd.DataFrame(
        data={"col1": [1, 2, 3], "col2": [3, 4, 5]},
        index=native_pd.TimedeltaIndex(["0 days", "1 days", "3 days"]),
    ),
]

NATIVE_INDEX_TEST_DATA = [
    native_pd.Index([], dtype="object"),
    native_pd.Index([[1, 2], [2, 3], [3, 4]]),
    native_pd.Index([1, 2, 3]),
    native_pd.Index([3, np.nan, 5], name="my_index"),
    native_pd.Index([5, None, 7]),
    native_pd.Index([1]),
    native_pd.Index(["a", "b", 1, 2]),
    native_pd.Index(["a", "b", "c", "d"]),
    native_pd.DatetimeIndex(
        ["2020-01-01 10:00:00+00:00", "2020-02-01 11:00:00+00:00"],
        tz="UTC-08:00",
    ),
    native_pd.DatetimeIndex(
        ["2020-01-01 10:00:00+05:00", "2020-02-01 11:00:00+05:00"],
        tz="UTC",
    ),
    native_pd.DatetimeIndex([1262347200000000000, 1262347400000000000]),
    native_pd.TimedeltaIndex(["0 days", "1 days", "3 days"]),
    native_pd.TimedeltaIndex([100, 200, 300]),
]

NATIVE_INDEX_UNIQUE_TEST_DATA = [
    native_pd.Index([], dtype="object", name="empty"),
    native_pd.Index([None], name="single_none"),
    native_pd.Index([None, None], name="all none"),
    native_pd.Index([1, 2, 3.55, -0.992, 1, 3, 2, 1, 2, 3, 4, 2, 1]),
    native_pd.Index([3, np.nan, 5, np.nan, np.nan, 5, 5, 5.0], name="my_index"),
    native_pd.Index([5, None, 7, None]),
    native_pd.Index([1]),
    native_pd.Index(["a", "b", 1, 2, None, "a", 2], name="mixed index"),
    native_pd.DatetimeIndex(
        ["2020-01-01 10:00:00+00:00", "2020-02-01 11:00:00+00:00"],
        tz="UTC",
    ),
    native_pd.DatetimeIndex(
        ["2020-01-01 10:00:00+00:00", "2020-01-01 10:00:00+00:00"],
        tz="UTC-08:00",
    ),
]

NATIVE_INDEX_SCALAR_TEST_DATA = [
    native_pd.Index([], dtype="object"),
    native_pd.Index([1, 2, 3.55, -0.992, 1, 3, 2, 1, 2, 3, 4, 2, 1]),
    native_pd.Index([3, np.nan, 5], name="my_index"),
    native_pd.Index([5, None, 7, None]),
    native_pd.Index([1]),
    native_pd.Index(["a", "b", "c", "d"]),
    native_pd.DatetimeIndex(
        ["2020-01-01 10:00:00+00:00", "2020-02-01 11:00:00+00:00"],
        tz="America/Los_Angeles",
    ),
    native_pd.DatetimeIndex(
        ["2020-01-01 10:00:00+00:00", "2020-01-01 10:00:00+00:00"],
        tz="America/Los_Angeles",
    ),
    native_pd.DatetimeIndex([1262347200000000000, 1262347400000000000]),
    native_pd.TimedeltaIndex(["4 days", None, "-1 days", "5 days"]),
]
