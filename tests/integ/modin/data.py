#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pandas as native_pd

RAW_NA_DF_DATA_TEST_CASES = [
    ({"A": [1, 2, 3], "B": [4, 5, 6]}, "numeric-no"),
    ({"A": [None, None, None], "B": [None, None, None]}, "all"),
    ({"A": [1, None, 3], "B": [None, 5, None]}, "numeric-mixed-1"),
    ({"A": [None, 2, None], "B": [4, None, 6]}, "numeric-mixed-2"),
    ({"A": ["a", "b", "c"], "B": ["d", "e", "f"]}, "str-all"),
    ({"A": ["a", None, "c"], "B": [None, "e", None]}, "str-mixed-1"),
    ({"A": [None, "b", None], "B": ["d", None, "f"]}, "str-mixed-2"),
    ({"A": [True, False, True], "B": [True, False, True]}, "bool-all"),
    ({"A": [True, None, True], "B": [None, False, None]}, "bool-mixed-1"),
    ({"A": [None, False, None], "B": [True, None, True]}, "bool-mixed-2"),
    ({"A": [True, 1, "X"], "B": ["Y", 3.14, False]}, "mixed"),
    ({"A": [True, None, "X"], "B": [None, 3.14, None]}, "mixed-mixed-1"),
    ({"A": [None, 1, None], "B": ["Y", None, False]}, "mixed-mixed-2"),
    (
        {
            "A": [None, native_pd.Timedelta(2), None],
            "B": [native_pd.Timedelta(4), None, native_pd.Timedelta(6)],
        },
        "timedelta-mixed-1",
    ),
]

RAW_NA_DF_SERIES_TEST_CASES = [
    (list(df_data.values()), test_case)
    for (df_data, test_case) in RAW_NA_DF_DATA_TEST_CASES[
        :1
    ]  # "timedelta-mixed-1" is not json serializable
]
