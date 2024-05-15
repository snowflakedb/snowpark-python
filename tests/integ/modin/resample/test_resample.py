#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import random
import string

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin._internal.resample_utils import (
    IMPLEMENTED_AGG_METHODS,
    IMPLEMENTED_DATEOFFSET_STRINGS,
)
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import (
    create_test_dfs,
    create_test_series,
    eval_snowpark_pandas_result,
)

agg_func = pytest.mark.parametrize("agg_func", IMPLEMENTED_AGG_METHODS)
freq = pytest.mark.parametrize("freq", IMPLEMENTED_DATEOFFSET_STRINGS)
interval = pytest.mark.parametrize("interval", [1, 2, 3, 5, 15])


def randomword(length):
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(length))


@freq
@interval
@agg_func
@sql_count_checker(query_count=2, join_count=1)
def test_resample_with_varying_freq_and_interval(freq, interval, agg_func):
    rule = f"{interval}{freq}"
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {"A": np.random.randn(15)},
            index=native_pd.date_range("2020-01-01", periods=15, freq=f"1{freq}"),
        ),
        lambda df: getattr(df.resample(rule=rule), agg_func)(),
        check_freq=False,
    )


@sql_count_checker(query_count=2, join_count=1)
def test_resample_date_before_snowflake_alignment_date():
    # Snowflake TIMESLICE alignment date is 1970-01-01 00:00:00
    date_data = native_pd.to_datetime(
        [
            "1960-01-01",
            "1960-01-02",
            "1960-01-03",
            "1960-01-05",
            "1960-01-06",
            "1960-01-10",
        ]
    )
    eval_snowpark_pandas_result(
        *create_test_dfs({"A": np.random.randn(6)}, index=date_data),
        lambda df: df.resample("2D").min(),
        check_freq=False,
    )


@interval
@sql_count_checker(query_count=2, join_count=1)
def test_resample_date_wraparound_snowflake_alignment_date(interval):
    # Snowflake TIMESLICE alignment date is 1970-01-01 00:00:00
    date_data = native_pd.to_datetime(
        [
            "1969-12-01",
            "1969-12-30",
            "1969-12-31",
            "1970-01-02",
            "1970-01-05",
            "1970-01-06",
            "1970-01-10",
        ]
    )
    eval_snowpark_pandas_result(
        *create_test_dfs({"A": np.random.randn(7)}, index=date_data),
        lambda df: df.resample(f"{interval}D").min(),
        check_freq=False,
    )


@agg_func
@freq
@sql_count_checker(query_count=2, join_count=1)
def test_resample_missing_data_upsample(agg_func, freq):
    # this tests to make sure that missing resample bins will be filled in.
    date_data = native_pd.date_range("2020-01-01", periods=13, freq=f"1{freq}").delete(
        [2, 7, 9]
    )
    rule = f"1{freq}"
    eval_snowpark_pandas_result(
        *create_test_dfs({"A": np.random.randn(10)}, index=date_data),
        lambda df: getattr(df.resample(rule=rule), agg_func)(),
        check_freq=False,
    )


@sql_count_checker(query_count=2, join_count=1)
def test_resample_duplicated_timestamps_downsample():
    date_data = native_pd.to_datetime(
        [
            "2020-01-01",
            "2020-01-01",
            "2020-01-01",
            "2020-01-02",
            "2020-01-03",
            "2020-01-07",
        ]
    )
    eval_snowpark_pandas_result(
        *create_test_dfs({"A": np.random.randn(6)}, index=date_data),
        lambda df: df.resample("1D").mean(),
        check_freq=False,
    )


@sql_count_checker(query_count=2, join_count=1)
def test_resample_duplicated_timestamps():
    date_data = native_pd.to_datetime(
        [
            "2020-01-01",
            "2020-01-01",
            "2020-01-01",
            "2020-01-02",
            "2020-01-03",
            "2020-01-04",
            "2020-01-05",
        ]
    )
    eval_snowpark_pandas_result(
        *create_test_dfs({"A": np.random.randn(7)}, index=date_data),
        lambda df: df.resample("1D").min(),
        check_freq=False,
    )


@freq
@interval
@agg_func
@sql_count_checker(query_count=2, join_count=1)
def test_resample_series(freq, interval, agg_func):
    rule = f"{interval}{freq}"
    eval_snowpark_pandas_result(
        *create_test_series(
            range(15),
            index=native_pd.date_range("2020-01-01", periods=15, freq=f"1{freq}"),
        ),
        lambda ser: getattr(ser.resample(rule=rule), agg_func)(),
        check_freq=False,
    )


@pytest.mark.parametrize(
    "agg_func", ["max", "min", "mean", "median", "sum", "std", "var"]
)
@sql_count_checker(query_count=2, join_count=1)
def test_resample_numeric_only(agg_func):
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {"A": np.random.randn(15), "B": [randomword(8) for _ in range(15)]},
            index=native_pd.date_range("2020-01-01", periods=15, freq="1D"),
        ),
        lambda df: getattr(df.resample(rule="4D"), agg_func)(numeric_only=True),
        check_freq=False,
    )


@agg_func
@sql_count_checker(query_count=2, join_count=1)
def test_resample_df_with_nan(agg_func):
    # resample bins of 'A' each have a NaN. 1 resample bin of 'B' is all NaN
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {"A": [np.nan, 3, np.nan, 4, 5.33], "B": [np.nan, np.nan, 6, 7, 9]},
            index=native_pd.date_range("2020-01-01", periods=5, freq="1D"),
        ),
        lambda df: getattr(df.resample("2D"), agg_func)(),
        check_freq=False,
    )


@agg_func
@sql_count_checker(query_count=2, join_count=1)
def test_resample_ser_with_nan(agg_func):
    # 1 resample bin of all NaN, 1 resample bin partially NaN, 1 resample bin no NaNs
    eval_snowpark_pandas_result(
        *create_test_series(
            [np.nan, np.nan, 7.33, np.nan, 9, 10],
            index=native_pd.date_range("2020-01-01", periods=6, freq="1D"),
        ),
        lambda ser: getattr(ser.resample("2D"), agg_func)(),
        check_freq=False,
    )


@agg_func
@sql_count_checker(query_count=2, join_count=1)
def test_resample_single_resample_bin(agg_func):
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {"A": np.random.randn(15)},
            index=native_pd.date_range("2020-01-01", periods=15, freq="1s"),
        ),
        lambda df: getattr(df.resample("1D"), agg_func)(),
        check_freq=False,
    )


@agg_func
@sql_count_checker(query_count=2, join_count=1)
def test_resample_index_with_nan(agg_func):
    datecol = native_pd.to_datetime(
        ["2020-01-01", "2020-01-03", "2020-01-05", np.nan, "2020-01-09", np.nan]
    )
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {"A": np.random.randn(6)},
            index=datecol,
        ),
        lambda df: getattr(df.resample("2D"), agg_func)(),
        check_freq=False,
    )


@sql_count_checker(query_count=2, join_count=1)
def test_resample_df_getitem():
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {"A": np.random.randn(10), "B": np.random.randn(10)},
            index=native_pd.date_range("2020-01-01", periods=10, freq="1D"),
        ),
        lambda df: df.resample("2D").min()["A"],
        check_freq=False,
    )


@sql_count_checker(query_count=2, join_count=1)
def test_resample_ser_getitem():
    eval_snowpark_pandas_result(
        *create_test_series(
            range(15), index=native_pd.date_range("2020-01-01", periods=15, freq="1D")
        ),
        lambda ser: ser.resample(rule="2D").min()[0:2],
        check_freq=False,
    )


@sql_count_checker(query_count=2, join_count=1)
def test_resample_date_trunc_day():
    # resample bins of 'A' each have a NaN. 1 resample bin of 'B' is all NaN
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {"A": [np.nan, 3, np.nan, 4, 5.33], "B": [np.nan, np.nan, 6, 7, 9]},
            index=native_pd.date_range("2020-01-01 2:00:00", periods=5, freq="1D"),
        ),
        lambda df: df.resample("2D").min(),
        check_freq=False,
    )


@sql_count_checker(query_count=2, join_count=1)
def test_resample_date_trunc_hour():
    # resample bins of 'A' each have a NaN. 1 resample bin of 'B' is all NaN
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {"A": [np.nan, 3, np.nan, 4, 5.33], "B": [np.nan, np.nan, 6, 7, 9]},
            index=native_pd.date_range("2020-01-01 2:00:23", periods=5, freq="1h"),
        ),
        lambda df: df.resample("2H").min(),
        check_freq=False,
    )


@interval
@sql_count_checker(query_count=2, join_count=1)
def test_resample_ffill(interval):
    datecol = native_pd.to_datetime(
        [
            "2020-01-01 1:00:00",
            "2020-01-02",
            "2020-01-03",
            "2020-01-06",
            "2020-01-07",
            "2020-01-08",
            "2020-01-09",
            "2020-02-08",
        ],
        format="mixed",
    )
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {"a": range(len(datecol)), "b": range(len(datecol) - 1, -1, -1)},
            index=datecol,
        ),
        lambda df: df.resample(rule=f"{interval}D").ffill(),
        check_freq=False,
    )


@interval
@sql_count_checker(query_count=2, join_count=1)
def test_resample_ffill_ser(interval):
    datecol = native_pd.to_datetime(
        [
            "2020-01-01 1:00:00",
            "2020-01-02",
            "2020-01-03",
            "2020-01-06",
            "2020-01-07",
            "2020-01-08",
            "2020-01-09",
            "2020-02-08",
        ],
        format="mixed",
    )
    eval_snowpark_pandas_result(
        *create_test_series({"a": range(len(datecol))}, index=datecol),
        lambda df: df.resample(rule=f"{interval}D").ffill(),
        check_freq=False,
    )


@interval
@sql_count_checker(query_count=2, join_count=1)
def test_resample_ffill_one_gap(interval):
    datecol = native_pd.to_datetime(
        [
            "2020-01-01",
            "2020-01-02",
            "2020-01-03",
            "2020-01-06",
            "2020-01-07",
            "2020-01-08",
        ]
    )
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {"a": range(len(datecol)), "b": range(len(datecol) - 1, -1, -1)},
            index=datecol,
        ),
        lambda df: df.resample(rule=f"{interval}D").ffill(),
        check_freq=False,
    )


@sql_count_checker(query_count=2, join_count=1)
def resample_ffill_ser_one_gap():
    datecol = native_pd.to_datetime(
        [
            "2020-01-01",
            "2020-01-02",
            "2020-01-03",
            "2020-01-06",
            "2020-01-07",
            "2020-01-08",
        ]
    )
    eval_snowpark_pandas_result(
        *create_test_series({"a": range(len(datecol))}, index=datecol),
        lambda df: df.resample(rule=f"{interval}D").ffill(),
        check_freq=False,
    )


@interval
@sql_count_checker(query_count=2, join_count=1)
def test_resample_ffill_missing_in_middle(interval):
    datecol = native_pd.to_datetime(
        [
            "2020-01-01",
            "2020-01-02",
            "2020-01-03 1:00:00",
            "2020-01-03 2:00:00",
            "2020-01-06",
            "2020-01-07",
            "2020-01-08",
        ],
        format="mixed",
    )
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {"a": range(len(datecol)), "b": range(len(datecol) - 1, -1, -1)},
            index=datecol,
        ),
        lambda df: df.resample(rule=f"{interval}D").ffill(),
        check_freq=False,
    )


@interval
@sql_count_checker(query_count=2, join_count=1)
def test_resample_ffill_ser_missing_in_middle(interval):
    datecol = native_pd.to_datetime(
        [
            "2020-01-01",
            "2020-01-02",
            "2020-01-03 1:00:00",
            "2020-01-03 2:00:00",
            "2020-01-06",
            "2020-01-07",
            "2020-01-08",
        ],
        format="mixed",
    )
    eval_snowpark_pandas_result(
        *create_test_series({"a": range(len(datecol))}, index=datecol),
        lambda df: df.resample(rule=f"{interval}D").ffill(),
        check_freq=False,
    )


@interval
@sql_count_checker(query_count=2, join_count=1)
def test_resample_ffill_ffilled_with_none(interval):
    datecol = native_pd.to_datetime(
        [
            "2020-01-01",
            "2020-01-02",
            "2020-01-03 2:00:00",
            "2020-01-03 1:00:00",
            "2020-01-06",
            "2020-01-07",
            "2020-01-08",
            "2020-01-10",
        ],
        format="mixed",
    )
    eval_snowpark_pandas_result(
        *create_test_dfs({"a": [1, 2, None, 4, 5, 7, None, 8]}, index=datecol),
        lambda df: df.resample(rule=f"{interval}D").ffill(),
        check_freq=False,
    )


@interval
@sql_count_checker(query_count=2, join_count=1)
def test_resample_ffill_large_gaps(interval):
    datecol = native_pd.to_datetime(
        [
            "2020-01-01",
            "2020-01-02",
            "2020-01-03",
            "2020-01-06",
            "2020-07-07",
            "2021-01-08",
            "2021-01-10",
        ]
    )
    eval_snowpark_pandas_result(
        *create_test_dfs({"a": [1, None, 4, 5, 7, None, 8]}, index=datecol),
        lambda df: df.resample(rule=f"{interval}D").ffill(),
        check_freq=False,
    )


def test_resample_docstrings_ffill():
    lst1 = ["2020-01-03", "2020-01-04", "2020-01-05", "2020-01-07", "2020-01-08"]
    # Native pandas needs all string datetime values to be of the same format, Snowpark pandas does not care.
    native_lst2 = [
        "2023-01-03 1:00:00",
        "2023-01-04 00:00:00",
        "2023-01-05 23:00:00",
        "2023-01-06 00:00:00",
        "2023-01-07 2:00:00",
        "2023-01-10 00:00:00",
    ]
    snow_lst2 = [
        "2023-01-03 1:00:00",
        "2023-01-04",
        "2023-01-05 23:00:00",
        "2023-01-06",
        "2023-01-07 2:00:00",
        "2023-01-10",
    ]

    # Series example 1
    native_ser = native_pd.Series([1, 2, 3, 4, 5], index=native_pd.to_datetime(lst1))
    snow_ser = pd.Series(native_ser, index=pd.to_datetime(lst1))
    with SqlCounter(query_count=2, join_count=1):
        eval_snowpark_pandas_result(
            snow_ser, native_ser, lambda s: s.resample("1D").ffill(), check_freq=False
        )
    with SqlCounter(query_count=2, join_count=1):
        eval_snowpark_pandas_result(
            snow_ser, native_ser, lambda s: s.resample("3D").ffill(), check_freq=False
        )

    # Series example 2
    native_ser = native_pd.Series(
        [1, 2, 3, 4, None, 6], index=native_pd.to_datetime(native_lst2)
    )
    snow_ser = pd.Series([1, 2, 3, 4, None, 6], index=pd.to_datetime(snow_lst2))
    with SqlCounter(query_count=2, join_count=1):
        eval_snowpark_pandas_result(
            snow_ser, native_ser, lambda s: s.resample("1D").ffill(), check_freq=False
        )
    with SqlCounter(query_count=2, join_count=1):
        eval_snowpark_pandas_result(
            snow_ser, native_ser, lambda s: s.resample("2D").ffill(), check_freq=False
        )

    # DataFrame example 1
    snow_idx = pd.to_datetime(lst1)
    native_idx = native_pd.to_datetime(lst1)
    snow_df = pd.DataFrame(
        {
            "a": range(len(snow_idx)),
            "b": range(len(snow_idx) + 10, len(snow_idx) * 2 + 10),
        },
        index=snow_idx,
    )
    native_df = native_pd.DataFrame(
        {
            "a": range(len(native_idx)),
            "b": range(len(native_idx) + 10, len(native_idx) * 2 + 10),
        },
        index=native_idx,
    )
    with SqlCounter(query_count=2, join_count=1):
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda df: df.resample("1D").ffill(), check_freq=False
        )
    with SqlCounter(query_count=2, join_count=1):
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda df: df.resample("3D").ffill(), check_freq=False
        )

    # DataFrame example 2
    snow_idx = pd.to_datetime(snow_lst2)
    native_idx = native_pd.to_datetime(native_lst2)
    snow_df = pd.DataFrame(
        {
            "a": range(len(snow_idx)),
            "b": range(len(snow_idx) + 10, len(snow_idx) * 2 + 10),
        },
        index=snow_idx,
    )
    native_df = native_pd.DataFrame(
        {
            "a": range(len(native_idx)),
            "b": range(len(native_idx) + 10, len(native_idx) * 2 + 10),
        },
        index=native_idx,
    )
    with SqlCounter(query_count=2, join_count=1):
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda df: df.resample("1D").ffill(), check_freq=False
        )
    with SqlCounter(query_count=2, join_count=1):
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda df: df.resample("2D").ffill(), check_freq=False
        )


def test_resample_docstrings_df_max():
    native_idx = native_pd.date_range("2020-01-01", periods=4, freq="1D")
    snow_idx = pd.date_range("2020-01-01", periods=4, freq="1D")
    # DataFrame example 1
    data = [[1, 8], [1, 2], [2, 5], [2, 6]]
    native_df = native_pd.DataFrame(data, columns=["a", "b"], index=native_idx)
    snow_df = pd.DataFrame(data, columns=["a", "b"], index=snow_idx)
    with SqlCounter(query_count=2, join_count=1):
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda df: df.resample("2D").max(), check_freq=False
        )

    # DataFrame example 2
    data = {"A": [1, 2, 3, np.nan], "B": [np.nan, np.nan, 3, 4]}
    native_idx = native_pd.date_range("2020-01-01", periods=4, freq="1S")
    snow_idx = pd.date_range("2020-01-01", periods=4, freq="1S")
    native_df = native_pd.DataFrame(data, index=native_idx)
    snow_df = pd.DataFrame(data, index=snow_idx)
    with SqlCounter(query_count=2, join_count=1):
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda df: df.resample("2S").max(), check_freq=False
        )


def test_resample_docstrings_df_mean():
    # DataFrame example 1
    data = {"A": [1, 1, 2, 1, 2], "B": [np.nan, 2, 3, 4, 5]}
    native_idx = native_pd.date_range("2020-01-01", periods=5, freq="1D")
    snow_idx = pd.date_range("2020-01-01", periods=5, freq="1D")
    native_df = native_pd.DataFrame(data, index=native_idx)
    snow_df = pd.DataFrame(data, index=snow_idx)
    with SqlCounter(query_count=2, join_count=1):
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda df: df.resample("2D").mean(), check_freq=False
        )
    with SqlCounter(query_count=2, join_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.resample("2D")["B"].mean(),
            check_freq=False,
        )

    # DataFrame example 2
    data = {"A": [1, 2, 3, np.nan], "B": [np.nan, np.nan, 3, 4]}
    native_idx = native_pd.date_range("2020-01-01", periods=4, freq="1S")
    snow_idx = pd.date_range("2020-01-01", periods=4, freq="1S")
    native_df = native_pd.DataFrame(data, index=native_idx)
    snow_df = pd.DataFrame(data, index=snow_idx)
    with SqlCounter(query_count=2, join_count=1):
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda df: df.resample("2S").mean(), check_freq=False
        )


@pytest.mark.parametrize(
    "func", ["var", "sum", "std", "min", "median", "count", "mean", "max"]
)
def test_resample_docstrings_series(func):
    # Series example 1
    native_lst1 = native_pd.date_range("2020-01-01", periods=4, freq="1D")
    snow_lst1 = pd.date_range("2020-01-01", periods=4, freq="1D")
    native_ser = native_pd.Series([1, 2, 3, 4], index=native_lst1)
    snow_ser = pd.Series([1, 2, 3, 4], index=snow_lst1)
    with SqlCounter(query_count=2, join_count=1):
        eval_snowpark_pandas_result(
            snow_ser,
            native_ser,
            lambda s: getattr(s.resample("2D"), func)(),
            check_freq=False,
        )

    # Series example 2
    native_lst2 = native_pd.date_range("2020-01-01", periods=4, freq="S")
    snow_lst2 = pd.date_range("2020-01-01", periods=4, freq="S")
    native_ser = native_pd.Series([1, 2, np.nan, 4], index=native_lst2)
    snow_ser = pd.Series([1, 2, np.nan, 4], index=snow_lst2)
    with SqlCounter(query_count=2, join_count=1):
        eval_snowpark_pandas_result(
            snow_ser,
            native_ser,
            lambda s: getattr(s.resample("2S"), func)(),
            check_freq=False,
        )


@sql_count_checker(query_count=3, join_count=1)
@pytest.mark.parametrize("func", ["var", "sum", "std", "min", "median", "count"])
def test_resample_docstrings_df(func):
    native_idx = native_pd.date_range("2020-01-01", periods=4, freq="1D")
    snow_idx = pd.date_range("2020-01-01", periods=4, freq="1D")
    data = [[1, 8, 2], [1, 2, 5], [2, 5, 8], [2, 6, 9]]
    native_df = native_pd.DataFrame(data, columns=["a", "b", "c"], index=native_idx)
    snow_df = pd.DataFrame(data, columns=["a", "b", "c"], index=snow_idx)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: getattr(df.resample("2D"), func)(),
        check_freq=False,
    )
