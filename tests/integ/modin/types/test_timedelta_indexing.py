#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import functools
import logging
import re

import modin.pandas as pd
import pandas as native_pd
import pytest
from modin.pandas.utils import is_scalar

from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.modin.plugin._internal.indexing_utils import (
    _LOC_SET_NON_TIMEDELTA_TO_TIMEDELTA_ERROR,
)
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize(
    "key, iloc_join_count, loc_query_count, loc_join_count",
    [
        [2, 0, 2, 2],
        [[2, 1], 2, 1, 1],
        [slice(1, None), 0, 1, 0],
        [[True, False, False, True], 1, 1, 1],
    ],
)
def test_series_indexing_get_timedelta(
    key, iloc_join_count, loc_query_count, loc_join_count
):
    # This test only verify indexing return timedelta results correctly
    td_s = native_pd.Series(
        [
            native_pd.Timedelta("1 days 1 hour"),
            native_pd.Timedelta("2 days 1 minute"),
            native_pd.Timedelta("3 days 1 nanoseconds"),
            native_pd.Timedelta("100 nanoseconds"),
        ]
    )
    snow_td_s = pd.Series(td_s)

    def run_test(api, query_count, join_count):
        with SqlCounter(query_count=query_count, join_count=join_count):
            if is_scalar(key):
                assert api(snow_td_s) == api(td_s)
            else:
                eval_snowpark_pandas_result(snow_td_s, td_s, api)

    run_test(lambda s: s.iloc[key], 1, iloc_join_count)
    run_test(lambda s: s[key], loc_query_count, loc_join_count)
    run_test(lambda s: s.loc[key], loc_query_count, loc_join_count)
    if is_scalar(key):
        run_test(lambda s: s.iat[key], 1, iloc_join_count)
        run_test(lambda s: s.at[key], loc_query_count, loc_join_count)


@pytest.mark.parametrize(
    "key, query_count, join_count, type_preserved",
    [
        [(1, 1), 1, 0, True],
        [(2, 2), 1, 0, True],
        [([2, 1], 1), 1, 2, True],
        [
            (2, [1, 0]),
            1,
            0,
            True,
        ],  # require transpose and keep result column type as timedelta
        [(2, ...), 1, 0, False],  # require transpose but lose the type
        [(slice(1, None), 0), 1, 0, True],
        [([True, False, False, True], 1), 1, 1, True],
        [(1, "a"), 2, 2, True],
        [(2, "b"), 2, 2, True],
        [([2, 1], "a"), 1, 1, True],
        [
            (2, ["b", "a"]),
            2,
            2,
            True,
        ],  # require transpose and keep result column type as timedelta
        [(2, ...), 1, 0, False],  # require transpose but lose the type
        [(slice(1, None), "a"), 1, 0, True],
        [([True, False, False, True], "b"), 1, 1, True],
    ],
)
def test_df_indexing_get_timedelta(
    key, query_count, join_count, type_preserved, caplog
):
    # This test only verify indexing return timedelta results correctly
    td = native_pd.DataFrame(
        {
            "a": [
                native_pd.Timedelta("1 days 1 hour"),
                native_pd.Timedelta("2 days 1 minute"),
                native_pd.Timedelta("3 days 1 nanoseconds"),
                native_pd.Timedelta("100 nanoseconds"),
            ],
            "b": native_pd.timedelta_range("1 hour", "1 day", 4),
            "c": [1, 2, 3, 4],
        }
    )
    snow_td = pd.DataFrame(td)

    expected_warning_msg = (
        "`TimedeltaType` may be lost in `transpose`'s result, please use `astype` to convert the "
        "result type back."
    )

    def run_test(api, query_count, join_count):
        with SqlCounter(query_count=query_count, join_count=join_count):
            caplog.clear()
            if is_scalar(key[0]) and is_scalar(key[1]):
                assert api(snow_td) == api(td)
            else:
                native_td = td if type_preserved else td.astype("int64")
                eval_snowpark_pandas_result(snow_td, native_td, api)
            if type_preserved:
                assert expected_warning_msg not in caplog.text
            else:
                assert expected_warning_msg in caplog.text

    with caplog.at_level(logging.DEBUG):
        if isinstance(key[1], str) or (
            isinstance(key[1], list) and isinstance(key[1][0], str)
        ):
            api = lambda s: s.loc[key]  # noqa: E731
        else:
            api = lambda s: s.iloc[key]  # noqa: E731
        run_test(api, query_count, join_count)
        if is_scalar(key[0]) and is_scalar(key[1]):
            if isinstance(key[1], str):
                api = lambda s: s.at[key]  # noqa: E731
            else:
                api = lambda s: s.iat[key]  # noqa: E731
            run_test(api, query_count, join_count)


def test_df_getitem_timedelta():
    td = native_pd.DataFrame(
        {
            "a": [
                native_pd.Timedelta("1 days 1 hour"),
                native_pd.Timedelta("2 days 1 minute"),
                native_pd.Timedelta("3 days 1 nanoseconds"),
                native_pd.Timedelta("100 nanoseconds"),
            ],
            "b": native_pd.timedelta_range("1 hour", "1 day", 4),
            "c": [1, 2, 3, 4],
        }
    )
    snow_td = pd.DataFrame(td)
    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(
            snow_td.copy(),
            td,
            lambda df: df["b"],
        )

    with SqlCounter(query_count=1, join_count=1):
        eval_snowpark_pandas_result(
            snow_td.copy(),
            td,
            lambda df: df[[True, False, False, True]],
        )


@sql_count_checker(query_count=1, join_count=2)
@pytest.mark.parametrize(
    "key, item",
    [
        [2, pd.Timedelta("2 days 2 hours")],  # single value
        [slice(2, None), pd.Timedelta("2 days 2 hours")],  # multi values
        [slice(2, None), None],  # multi none values
        [slice(2, None), pd.NaT],  # multi none values
        [slice(None, None), pd.NA],  # all none values
    ],
)
def test_series_indexing_set_timedelta(key, item):
    td_s = native_pd.Series(
        [
            native_pd.Timedelta("1 days 1 hour"),
            native_pd.Timedelta("2 days 1 minute"),
            native_pd.Timedelta("3 days 1 nanoseconds"),
            native_pd.Timedelta("100 nanoseconds"),
        ]
    )
    snow_td_s = pd.Series(td_s)

    def iloc_set(key, item, s):
        s.iloc[key] = item
        return s

    eval_snowpark_pandas_result(
        snow_td_s.copy(),
        td_s,
        functools.partial(iloc_set, key, item),
    )


@pytest.mark.parametrize("item", [pd.Timedelta("1 hour"), None])
def test_df_indexing_set_timedelta(item):
    td = native_pd.DataFrame(
        {
            "a": [
                native_pd.Timedelta("1 days 1 hour"),
                native_pd.Timedelta("2 days 1 minute"),
                native_pd.Timedelta("3 days 1 nanoseconds"),
                native_pd.Timedelta("100 nanoseconds"),
            ],
            "b": native_pd.timedelta_range("1 hour", "1 day", 4),
            "c": [1, 2, 3, 4],
        }
    )
    snow_td = pd.DataFrame(td)

    def iloc_set(key, item, df):
        df.iloc[key] = item
        return df

    def run_test(key, item, natvie_df=td, api=iloc_set):
        eval_snowpark_pandas_result(
            snow_td.copy(), natvie_df.copy(), functools.partial(api, key, item)
        )

    with SqlCounter(query_count=1, join_count=2):
        # single value
        key = (1, 1)
        run_test(key, item)

    with SqlCounter(query_count=1, join_count=2):
        # single column
        key = (..., 0)
        run_test(key, item)

    with SqlCounter(query_count=1, join_count=2):
        # multi columns
        key = (..., [0, 1])
        run_test(key, item)

    with SqlCounter(query_count=1, join_count=3):
        # multi columns with array
        key = (..., [0, 1])
        run_test(key, [item] * 2)

    def df_set(key, item, df):
        df[key] = item
        return df

    with SqlCounter(query_count=1, join_count=0):
        # single column
        key = "b"
        run_test(key, item, api=df_set)

    with SqlCounter(query_count=1, join_count=0):
        # multi columns
        key = ["a", "b"]
        run_test(key, item, api=df_set)

    with SqlCounter(query_count=1, join_count=0):
        # multi columns with array
        key = ["a", "b"]
        run_test(key, [item] * 2, api=df_set)

    def loc_set(key, item, df):
        df.loc[key] = item
        return df

    with SqlCounter(query_count=1, join_count=1):
        # single value
        key = (1, "a")
        run_test(key, item, api=loc_set)

    with SqlCounter(query_count=1, join_count=0):
        # single column
        key = (slice(None, None, None), "a")
        run_test(key, item, api=loc_set)

    with SqlCounter(query_count=1, join_count=0):
        # multi columns
        key = (slice(None, None, None), ["a", "b"])
        run_test(key, item, api=loc_set)

    with SqlCounter(query_count=1, join_count=0):
        # multi columns with array
        key = (slice(None, None, None), ["a", "b"])
        run_test(key, [item] * 2, api=loc_set)


def test_df_indexing_set_timedelta_with_other_type():
    td = native_pd.DataFrame(
        {
            "a": [
                native_pd.Timedelta("1 days 1 hour"),
                native_pd.Timedelta("2 days 1 minute"),
                native_pd.Timedelta("3 days 1 nanoseconds"),
                native_pd.Timedelta("100 nanoseconds"),
            ],
            "b": native_pd.timedelta_range("1 hour", "1 day", 4),
            "c": [1, 2, 3, 4],
        }
    )
    snow_td = pd.DataFrame(td)

    def iloc_set(key, item, df):
        df.iloc[key] = item
        return df

    def run_test(key, item, native_df=td, api=iloc_set):
        eval_snowpark_pandas_result(
            snow_td.copy(), native_df.copy(), functools.partial(api, key, item)
        )

    item = "string"
    with SqlCounter(query_count=0):
        # single value
        key = (1, 1)
        with pytest.raises(
            SnowparkSQLException, match="Numeric value 'string' is not recognized"
        ):
            run_test(key, item)

    item = 1000
    with SqlCounter(query_count=1, join_count=2):
        # single value
        key = (1, 1)
        td_int = td.copy()
        td_int["b"] = td_int["b"].astype("int64")
        # timedelta type is not preserved in this case
        run_test(key, item, native_df=td_int)

    def df_set(key, item, df):
        df[key] = item
        return df

    def loc_set(key, item, df):
        df.loc[key] = item
        return df

    # Set other types to timedelta columns
    item = "string"
    with SqlCounter(query_count=0):
        # single value
        key = (1, "a")
        with pytest.raises(
            NotImplementedError,
            match=re.escape(_LOC_SET_NON_TIMEDELTA_TO_TIMEDELTA_ERROR),
        ):
            run_test(key, item, api=loc_set)

    item = 1000
    with SqlCounter(query_count=0):
        # single value
        key = (1, "b")
        td_int = td.copy()
        td_int["b"] = td_int["b"].astype("int64")
        with pytest.raises(
            NotImplementedError,
            match=re.escape(_LOC_SET_NON_TIMEDELTA_TO_TIMEDELTA_ERROR),
        ):
            run_test(key, item, native_df=td_int, api=loc_set)


@pytest.mark.parametrize("item", [None, pd.Timedelta("1 hour")])
def test_df_indexing_enlargement_timedelta(item):
    td = native_pd.DataFrame(
        {
            "a": [
                native_pd.Timedelta("1 days 1 hour"),
                native_pd.Timedelta("2 days 1 minute"),
                native_pd.Timedelta("3 days 1 nanoseconds"),
                native_pd.Timedelta("100 nanoseconds"),
            ],
            "b": native_pd.timedelta_range("1 hour", "1 day", 4),
            "c": [1, 2, 3, 4],
        }
    )
    snow_td = pd.DataFrame(td)

    def setitem_enlargement(key, item, df):
        df[key] = item
        return df

    key = "x"

    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(
            snow_td.copy(), td.copy(), functools.partial(setitem_enlargement, key, item)
        )

    key = 10
    with SqlCounter(query_count=1, join_count=1):
        eval_snowpark_pandas_result(
            snow_td["a"].copy(),
            td["a"].copy(),
            functools.partial(setitem_enlargement, key, item),
        )

    def loc_enlargement(key, item, df):
        df.loc[key] = item
        return df

    key = (slice(None, None, None), "x")

    with SqlCounter(query_count=1, join_count=0):
        eval_snowpark_pandas_result(
            snow_td.copy(), td.copy(), functools.partial(loc_enlargement, key, item)
        )

    key = 10
    with SqlCounter(query_count=1, join_count=1):
        eval_snowpark_pandas_result(
            snow_td["a"].copy(),
            td["a"].copy(),
            functools.partial(loc_enlargement, key, item),
        )

    # single row
    key = (10, slice(None, None, None))

    if pd.isna(item):
        with SqlCounter(query_count=1, join_count=1):
            eval_snowpark_pandas_result(
                snow_td.copy(), td.copy(), functools.partial(loc_enlargement, key, item)
            )
    else:
        with (
            SqlCounter(query_count=0),
            pytest.raises(
                NotImplementedError,
                match=re.escape(_LOC_SET_NON_TIMEDELTA_TO_TIMEDELTA_ERROR),
            ),
        ):
            # Reason for failure is SNOW-1738952
            eval_snowpark_pandas_result(
                snow_td.copy(), td.copy(), functools.partial(loc_enlargement, key, item)
            )


@pytest.mark.parametrize(
    "key, join_count",
    [(2, 0), ([2, 1], 2), (slice(1, None), 0), ([True, False, False, True], 1)],
)
def test_index_get_timedelta(key, join_count):
    td_idx = native_pd.TimedeltaIndex(
        [
            native_pd.Timedelta("1 days 1 hour"),
            native_pd.Timedelta("2 days 1 minute"),
            native_pd.Timedelta("3 days 1 nanoseconds"),
            native_pd.Timedelta("100 nanoseconds"),
        ]
    )
    snow_td_idx = pd.TimedeltaIndex(td_idx)

    with SqlCounter(query_count=1, join_count=join_count):
        if is_scalar(key):
            assert snow_td_idx[key] == td_idx[key]
        else:
            eval_snowpark_pandas_result(snow_td_idx, td_idx, lambda idx: idx[key])


@pytest.mark.parametrize(
    "key, api, query_count, join_count",
    [
        [2, "iat", 1, 1],
        [native_pd.Timedelta("1 days 1 hour"), "at", 2, 4],
        [[2, 1], "iloc", 1, 3],
        [
            [
                native_pd.Timedelta("1 days 1 hour"),
                native_pd.Timedelta("1 days 1 hour"),
            ],
            "loc",
            1,
            2,
        ],
        [slice(1, None), "iloc", 1, 1],
        [[True, False, False, True], "iloc", 1, 2],
        [[True, False, False, True], "loc", 1, 2],
    ],
)
def test_series_with_timedelta_index(key, api, query_count, join_count):
    td_idx = native_pd.TimedeltaIndex(
        [
            native_pd.Timedelta("1 days 1 hour"),
            native_pd.Timedelta("2 days 1 minute"),
            native_pd.Timedelta("3 days 1 nanoseconds"),
            native_pd.Timedelta("100 nanoseconds"),
        ]
    )
    snow_td_idx = pd.TimedeltaIndex(td_idx)

    data = [1, 2, 3, 4]
    native_series = native_pd.Series(data, index=td_idx)
    snow_series = pd.Series(data, index=snow_td_idx)

    with SqlCounter(query_count=query_count, join_count=join_count):
        if is_scalar(key):
            assert getattr(snow_series, api)[key] == getattr(native_series, api)[key]
        else:
            eval_snowpark_pandas_result(
                snow_series, native_series, lambda s: getattr(s, api)[key]
            )


@pytest.mark.parametrize(
    "key, api, query_count, join_count",
    [
        [2, "iat", 1, 1],
        [native_pd.Timedelta("1 days 1 hour"), "at", 2, 4],
        [[2, 1], "iloc", 1, 3],
        [
            [
                native_pd.Timedelta("1 days 1 hour"),
                native_pd.Timedelta("1 days 1 hour"),
            ],
            "loc",
            1,
            2,
        ],
        [slice(1, None), "iloc", 1, 1],
        [[True, False, False, True], "iloc", 1, 2],
        [[True, False, False, True], "loc", 1, 2],
    ],
)
def test_df_with_timedelta_index(key, api, query_count, join_count):
    td_idx = native_pd.TimedeltaIndex(
        [
            native_pd.Timedelta("1 days 1 hour"),
            native_pd.Timedelta("2 days 1 minute"),
            native_pd.Timedelta("3 days 1 nanoseconds"),
            native_pd.Timedelta("100 nanoseconds"),
        ]
    )
    snow_td_idx = pd.TimedeltaIndex(td_idx)

    data = [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12], [13, 14, 15, 16]]
    native_df = native_pd.DataFrame(data, index=td_idx)
    snow_df = pd.DataFrame(data, index=snow_td_idx)

    with SqlCounter(query_count=query_count, join_count=join_count):
        if is_scalar(key):
            assert getattr(snow_df, api)[key, 0] == getattr(native_df, api)[key, 0]
        else:
            eval_snowpark_pandas_result(
                snow_df, native_df, lambda s: getattr(s, api)[key]
            )


def test_df_with_timedelta_index_enlargement_during_indexing():
    td_idx = native_pd.TimedeltaIndex(
        [
            native_pd.Timedelta("1 days 1 hour"),
            native_pd.Timedelta("2 days 1 minute"),
            native_pd.Timedelta("3 days 1 nanoseconds"),
            native_pd.Timedelta("100 nanoseconds"),
        ]
    )
    snow_td_idx = pd.TimedeltaIndex(td_idx)

    data = [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12], [13, 14, 15, 16]]
    cols = ["a", "b", "c", "d"]
    native_df = native_pd.DataFrame(data, index=td_idx, columns=cols)
    snow_df = pd.DataFrame(data, index=snow_td_idx, columns=cols)

    def setitem_enlargement(key, item, df):
        df[key] = item
        return df

    item = 23

    key = native_pd.Timedelta("2 days")
    with SqlCounter(query_count=1, join_count=1):
        eval_snowpark_pandas_result(
            snow_df.copy(),
            native_df.copy(),
            functools.partial(setitem_enlargement, key, item),
        )

    key = native_pd.Timedelta("2 days 45 minutes")
    with SqlCounter(query_count=1, join_count=2):
        eval_snowpark_pandas_result(
            snow_df["a"].copy(),
            native_df["a"].copy(),
            functools.partial(setitem_enlargement, key, item),
        )

    def loc_enlargement(key, item, df):
        df.loc[key] = item
        return df

    key = (slice(None, None, None), "x")

    with SqlCounter(query_count=1, join_count=1):
        eval_snowpark_pandas_result(
            snow_df.copy(),
            native_df.copy(),
            functools.partial(loc_enlargement, key, item),
        )

    key = native_pd.Timedelta("2 days 25 minutes")
    with SqlCounter(query_count=1, join_count=2):
        eval_snowpark_pandas_result(
            snow_df["a"].copy(),
            native_df["a"].copy(),
            functools.partial(loc_enlargement, key, item),
        )

    # single row
    key = (native_pd.Timedelta("2 days 45 minutes"), slice(None, None, None))

    with SqlCounter(query_count=1, join_count=2):
        eval_snowpark_pandas_result(
            snow_df.copy(),
            native_df.copy(),
            functools.partial(loc_enlargement, key, item),
        )
