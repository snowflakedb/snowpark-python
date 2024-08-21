#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import datetime
import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    create_test_dfs,
    create_test_series,
    eval_snowpark_pandas_result,
)

PANDAS_TIMESTAMP_SERIES_WITH_NULLS_NO_TIMEZONE_1 = (
    native_pd.Series(
        [
            None,
            pd.Timestamp(year=1994, month=7, day=29),
            None,
            pd.Timestamp(year=1996, month=1, day=23),
            pd.Timestamp(year=2000, month=1, day=23),
            pd.Timestamp(
                year=1700, month=1, day=1, second=22, microsecond=12345, nanosecond=56
            ),
        ]
    ),
    native_pd.Series(
        [
            None,
            None,
            pd.Timestamp(year=1995, month=7, day=29),
            pd.Timestamp(year=1996, month=1, day=24),
            pd.Timestamp(year=2024, month=7, day=8),
            pd.Timestamp(
                year=1700, month=1, day=2, second=49, microsecond=7, nanosecond=98
            ),
        ]
    ),
)


class TestDataFrameAndScalar:
    @pytest.mark.parametrize(
        "scalar",
        [
            pd.NaT,
            datetime.datetime(year=2024, month=8, day=14, hour=2, minute=32, second=42),
            datetime.datetime(year=2023, month=3, day=14),
            pd.Timestamp(year=2020, month=3, day=25),
        ],
    )
    @pytest.mark.parametrize("operation", ["sub", "rsub"])
    @sql_count_checker(query_count=1)
    def test_timestamp_minus_timestamp(self, scalar, operation):
        eval_snowpark_pandas_result(
            *create_test_dfs(
                [
                    [datetime.datetime(year=2024, month=1, day=1), pd.NaT],
                    [
                        datetime.datetime(year=2023, month=1, day=1),
                        datetime.datetime(year=2030, month=1, day=1),
                    ],
                ]
            ),
            lambda df: getattr(df, operation)(scalar),
        )


class TestSeriesAndScalar:
    @pytest.mark.parametrize(
        "scalar",
        [
            pd.NaT,
            datetime.datetime(year=2024, month=8, day=14, hour=2, minute=32, second=42),
            datetime.datetime(year=2023, month=3, day=14),
            pd.Timestamp(year=2020, month=3, day=25),
        ],
    )
    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize("operation", ["sub", "rsub"])
    def test_timestamp_minus_timestamp(self, operation, scalar):
        eval_snowpark_pandas_result(
            *create_test_series(
                [
                    datetime.datetime(year=2024, month=1, day=1),
                    pd.NaT,
                    datetime.datetime(year=2023, month=1, day=1),
                    datetime.datetime(year=2030, month=1, day=1),
                ]
            ),
            lambda series: getattr(series, operation)(scalar),
        )


class TestDataFrameAndListLikeAxis1:
    @pytest.mark.parametrize("op", ["sub", "rsub"])
    @sql_count_checker(query_count=1)
    def test_timestamp_minus_timestamp(self, op):
        eval_snowpark_pandas_result(
            *create_test_dfs(
                [
                    [
                        pd.Timestamp(5, unit="ns"),
                        pd.Timestamp(700, unit="ns"),
                        pd.Timestamp(1399, unit="ns"),
                    ],
                    [
                        pd.Timestamp(6, unit="ms"),
                        pd.Timestamp(800, unit="ms"),
                        pd.Timestamp(1499, unit="ms"),
                    ],
                ]
            ),
            lambda df: getattr(df, op)(
                [
                    pd.Timestamp(1, unit="ns"),
                    pd.Timestamp(300, unit="ns"),
                    pd.Timestamp(57, unit="ms"),
                ]
            ),
        )


class TestSeriesAndListLike:
    @sql_count_checker(query_count=1, join_count=1)
    @pytest.mark.parametrize("op", ["sub", "rsub"])
    def test_timestamp_minus_timestamp(self, op):
        eval_snowpark_pandas_result(
            *create_test_series(
                [
                    pd.Timestamp(5, unit="ns"),
                    pd.Timestamp(700, unit="ns"),
                    pd.Timestamp(1399, unit="ns"),
                ]
            ),
            lambda series: getattr(series, op)(
                [
                    pd.Timestamp(1, unit="ns"),
                    pd.Timestamp(300, unit="ns"),
                    pd.Timestamp(999, unit="ns"),
                ]
            ),
        )


class TestDataFrameAndListLikeAxis0:
    @sql_count_checker(query_count=1, join_count=1)
    @pytest.mark.parametrize("op", ["sub", "rsub"])
    def test_timestamp_minus_timestamp(self, op):
        eval_snowpark_pandas_result(
            *create_test_dfs(
                [
                    [pd.Timestamp(5, unit="ns"), pd.Timestamp(700, unit="ns")],
                    [pd.Timestamp(6, unit="ns"), pd.Timestamp(800, unit="ns")],
                    [pd.Timestamp(7, unit="ns"), pd.Timestamp(900, unit="ns")],
                ]
            ),
            lambda df: getattr(df, op)(
                [
                    pd.Timestamp(1, unit="ns"),
                    pd.Timestamp(300, unit="ns"),
                    pd.Timestamp(999, unit="ns"),
                ],
                axis=0,
            ),
        )


class TestSeriesAndSeries:
    @pytest.mark.parametrize(
        "pandas_lhs,pandas_rhs",
        [
            PANDAS_TIMESTAMP_SERIES_WITH_NULLS_NO_TIMEZONE_1,
            [
                x.dt.tz_localize("UTC")
                for x in PANDAS_TIMESTAMP_SERIES_WITH_NULLS_NO_TIMEZONE_1
            ],
            (
                PANDAS_TIMESTAMP_SERIES_WITH_NULLS_NO_TIMEZONE_1[0].dt.tz_localize(
                    "UTC"
                ),
                PANDAS_TIMESTAMP_SERIES_WITH_NULLS_NO_TIMEZONE_1[1].dt.tz_localize(
                    "Asia/Kolkata"
                ),
            ),
        ],
    )
    @pytest.mark.parametrize("op", ["sub", "rsub"])
    @sql_count_checker(query_count=1, join_count=1)
    def test_timestamp_minus_timestamp(self, pandas_lhs, pandas_rhs, op):
        """Subtract two series of timestamps to get a timedelta."""
        snow_lhs = pd.Series(pandas_lhs)
        snow_rhs = pd.Series(pandas_rhs)
        eval_snowpark_pandas_result(
            (snow_lhs, snow_rhs),
            (pandas_lhs, pandas_rhs),
            lambda inputs: getattr(inputs[0], op)(inputs[1]),
        )

    @pytest.mark.parametrize(
        "pandas_lhs,pandas_rhs",
        [
            (
                PANDAS_TIMESTAMP_SERIES_WITH_NULLS_NO_TIMEZONE_1[0].dt.tz_localize(
                    "UTC"
                ),
                PANDAS_TIMESTAMP_SERIES_WITH_NULLS_NO_TIMEZONE_1[1],
            ),
            (
                PANDAS_TIMESTAMP_SERIES_WITH_NULLS_NO_TIMEZONE_1[0],
                PANDAS_TIMESTAMP_SERIES_WITH_NULLS_NO_TIMEZONE_1[1].dt.tz_localize(
                    "UTC"
                ),
            ),
        ],
    )
    @pytest.mark.parametrize("op", ["sub", "rsub"])
    @sql_count_checker(query_count=0)
    def test_subtract_two_timestamps_timezones_disallowed(
        self, pandas_lhs, pandas_rhs, op
    ):
        snow_lhs = pd.Series(pandas_lhs)
        snow_rhs = pd.Series(pandas_rhs)
        # pandas is inconsistent about including a period at the end of the end
        # of the error message, but Snowpark pandas is not.
        eval_snowpark_pandas_result(
            (snow_lhs, snow_rhs),
            (pandas_lhs, pandas_rhs),
            lambda inputs: getattr(inputs[0], op)(inputs[1]),
            expect_exception=True,
            expect_exception_match=re.escape(
                "Cannot subtract tz-naive and tz-aware datetime-like objects."
            ),
            assert_exception_equal=False,
            except_exception_type=TypeError,
        )


class TestDataFrameAndSeriesAxis0:
    @pytest.mark.parametrize("op", ["sub", "rsub"])
    @sql_count_checker(query_count=1, join_count=1)
    def test_timestamp_dataframe_minus_timestamp_series(self, op):
        snow_df, pandas_df = create_test_dfs(
            [
                [pd.Timestamp(1, unit="ms"), pd.Timestamp(2, unit="ms")],
                [pd.Timestamp(3, unit="ms"), pd.Timestamp(4, unit="ms")],
            ]
        )
        snow_series, pandas_series = create_test_series(
            [
                pd.Timestamp(5, unit="ms"),
                pd.Timestamp(6, unit="ms"),
                pd.Timestamp(7, unit="ms"),
            ]
        )
        eval_snowpark_pandas_result(
            (snow_df, snow_series),
            (pandas_df, pandas_series),
            lambda t: getattr(t[0], op)(t[1], axis=0),
        )


class TestDataFrameAndSeriesAxis1:
    @sql_count_checker(
        # One query to materialize the series for the subtraction, and another
        # query to materialize the result.
        query_count=2
    )
    def test_timestamp_dataframe_minus_timestamp_series(self):
        """
        Test subtracting a series of timestamps from a dataframe of timestamps on axis 1.
        pandas behavior is incorrect: https://github.com/pandas-dev/pandas/issues/59529
        """
        pandas_df = native_pd.DataFrame(
            [
                [pd.Timestamp(1, unit="ms"), pd.Timestamp(2, unit="ms")],
                [pd.Timestamp(3, unit="ms"), pd.Timestamp(4, unit="ms")],
            ]
        )
        pandas_series = native_pd.Series(
            [
                pd.Timestamp(5, unit="ms"),
                pd.Timestamp(6, unit="ms"),
                pd.Timestamp(7, unit="ms"),
            ]
        )
        with pytest.raises(
            TypeError, match="cannot subtract DatetimeArray from ndarray"
        ):
            pandas_df - pandas_series
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            pd.DataFrame(pandas_df) - pd.Series(pandas_series),
            native_pd.DataFrame(
                [
                    [
                        native_pd.Timedelta(milliseconds=-4),
                        native_pd.Timedelta(milliseconds=-4),
                        pd.NaT,
                    ],
                    [
                        native_pd.Timedelta(milliseconds=-2),
                        native_pd.Timedelta(milliseconds=-2),
                        pd.NaT,
                    ],
                ]
            ),
        )

    @sql_count_checker(
        # One query to materialize the series for the subtraction, and another
        # query to materialize the result.
        query_count=2
    )
    def test_timestamp_series_minus_timestamp_dataframe(self):
        """
        Test subtracting a dataframe of timestamps from a series of timestamps.
        pandas behavior is incorrect: https://github.com/pandas-dev/pandas/issues/59529
        """
        pandas_df = native_pd.DataFrame(
            [
                [pd.Timestamp(1, unit="ms"), pd.Timestamp(2, unit="ms")],
                [pd.Timestamp(3, unit="ms"), pd.Timestamp(4, unit="ms")],
            ]
        )
        pandas_series = native_pd.Series(
            [
                pd.Timestamp(5, unit="ms"),
                pd.Timestamp(6, unit="ms"),
                pd.Timestamp(7, unit="ms"),
            ]
        )
        with pytest.raises(
            np.core._exceptions.UFuncTypeError,
            match=re.escape(
                "ufunc 'subtract' cannot use operands with types dtype('<M8[ns]') and dtype('float64')"
            ),
        ):
            pandas_series - pandas_df
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            pd.Series(pandas_series) - pd.DataFrame(pandas_df),
            native_pd.DataFrame(
                [
                    [
                        native_pd.Timedelta(milliseconds=4),
                        native_pd.Timedelta(milliseconds=4),
                        pd.NaT,
                    ],
                    [
                        native_pd.Timedelta(milliseconds=2),
                        native_pd.Timedelta(milliseconds=2),
                        pd.NaT,
                    ],
                ]
            ),
        )


class TestDataFrameAndDataFrameAxis1:
    @pytest.mark.parametrize(
        "fill_value", [None, pd.NaT, pd.Timestamp(year=1999, month=12, day=31)]
    )
    @pytest.mark.parametrize("op", ["sub", "rsub"])
    @sql_count_checker(query_count=1, join_count=1)
    def test_timestamp_sub_dataframe_and_dataframe(self, fill_value, op):
        # at position [0, 0], both values are non-null.
        # at position [0, 1], both values are null.
        # at position [1, 0], lhs is null and rhs is not null.
        # at position [1, 1], lhs is non-null and rhs is null.
        snow_lhs, pandas_lhs = create_test_dfs(
            [
                [
                    pd.Timestamp(year=2000, month=1, day=1),
                    pd.NaT,
                ],
                [pd.NaT, pd.Timestamp(year=2001, month=2, day=2)],
            ]
        )
        snow_rhs, pandas_rhs = create_test_dfs(
            [
                [pd.Timestamp(year=2002, month=3, day=3), pd.NaT],
                [pd.Timestamp(year=2003, month=4, day=4), pd.NaT],
            ]
        )
        eval_snowpark_pandas_result(
            (snow_lhs, snow_rhs),
            (pandas_lhs, pandas_rhs),
            lambda t: getattr(t[0], op)(t[1], fill_value=fill_value),
        )
