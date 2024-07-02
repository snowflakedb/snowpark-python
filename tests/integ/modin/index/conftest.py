#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
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
]

NATIVE_INDEX_TEST_DATA = [
    native_pd.Index([], dtype="object"),
    native_pd.Index([[1, 2], [2, 3], [3, 4]]),
    native_pd.Index([1, 2, 3]),
    native_pd.Index([3, np.nan, 5], name="my_index"),
    native_pd.Index([5, None, 7]),
    native_pd.Index([1]),
    native_pd.Index(["a", "b", 1, 2]),
]

NATIVE_INDEX_UNIQUE_TEST_DATA = [
    native_pd.Index([], dtype="object"),
    native_pd.Index([1, 2, 3.55, -0.992, 1, 3, 2, 1, 2, 3, 4, 2, 1]),
    native_pd.Index([3, np.nan, 5, np.nan, np.nan, 5, 5, 5.0], name="my_index"),
    native_pd.Index([5, None, 7, None]),
    native_pd.Index([1]),
    native_pd.Index(["a", "b", 1, 2, None, "a", 2]),
]
