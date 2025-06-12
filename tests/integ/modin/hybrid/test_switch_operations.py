#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

import numpy as np
from numpy.testing import assert_array_equal
import modin.pandas as pd
import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin._internal.utils import MODIN_IS_AT_LEAST_0_33_0

from tests.integ.utils.sql_counter import sql_count_checker


@pytest.fixture(scope="module", autouse=True)
def skip(pytestconfig):
    if not MODIN_IS_AT_LEAST_0_33_0:
        pytest.skip(
            "backend switching tests only work on newer modin versions",
            allow_module_level=True,
        )


@sql_count_checker(query_count=1)
def test_merge(init_transaction_tables, us_holidays_data):
    df_transactions = pd.read_snowflake("REVENUE_TRANSACTIONS")
    df_us_holidays = pd.DataFrame(us_holidays_data, columns=["Holiday", "Date"])
    assert df_transactions.get_backend() == "Snowflake"
    assert df_us_holidays.get_backend() == "Pandas"
    # Since `df_us_holidays` is much smaller than `df_transactions`, we moved `df_us_holidays`
    # to Snowflake where `df_transactions` is, to perform the operation.
    combined = pd.merge(
        df_us_holidays, df_transactions, left_on="Date", right_on="DATE"
    )
    assert combined.get_backend() == "Snowflake"


@sql_count_checker(query_count=6)
def test_filtered_data(init_transaction_tables):
    # When data is filtered, the engine should change when it is sufficiently small.
    df_transactions = pd.read_snowflake("REVENUE_TRANSACTIONS")
    assert df_transactions.get_backend() == "Snowflake"
    # in-place operations that do not change the backend
    df_transactions["DATE"] = pd.to_datetime(df_transactions["DATE"])
    assert df_transactions.get_backend() == "Snowflake"
    base_date = pd.Timestamp("2025-06-09").date()
    df_transactions_filter1 = df_transactions[
        (df_transactions["DATE"] >= base_date - pd.Timedelta("7 days"))
        & (df_transactions["DATE"] < base_date)
    ]
    assert df_transactions_filter1.get_backend() == "Snowflake"
    # The smaller dataframe does operations in pandas
    df_transactions_filter1 = df_transactions_filter1.groupby("DATE").sum()["REVENUE"]
    assert df_transactions_filter1.get_backend() == "Pandas"
    df_transactions_filter2 = pd.read_snowflake(
        "SELECT * FROM revenue_transactions WHERE Date >= DATEADD( 'days', -7, '2025-06-09' ) and Date < '2025-06-09'"
    )
    assert df_transactions_filter2.get_backend() == "Pandas"
    assert_array_equal(
        # Snowpark handles index objects differently from native pandas, so just check values
        df_transactions_filter1.to_pandas().values,
        df_transactions_filter2.groupby("DATE").sum()["REVENUE"].to_pandas().values,
    )


@sql_count_checker(query_count=8)
def test_apply(init_transaction_tables, us_holidays_data):
    df_transactions = pd.read_snowflake("REVENUE_TRANSACTIONS")
    df_us_holidays = pd.DataFrame(us_holidays_data, columns=["Holiday", "Date"])
    df_us_holidays["Date"] = pd.to_datetime(df_us_holidays["Date"])

    def forecast_revenue(df, start_date, end_date):
        # Filter data from last year
        df_filtered = df[
            (df["DATE"] >= start_date - pd.Timedelta(days=365))
            & (df["DATE"] < start_date)
        ]
        # Append future dates to daily_avg for prediction
        future_dates = pd.date_range(start=start_date, end=end_date, freq="D")
        df_future = pd.DataFrame({"DATE": future_dates})

        # Group by DATE and calculate the mean revenue
        daily_avg = df_filtered.groupby("DATE")["REVENUE"].mean().reset_index()
        daily_avg["DATE"] = daily_avg["DATE"].astype("datetime64[ns]")
        # Merge future dates with predicted revenue, filling missing values
        df_forecast = df_future.merge(daily_avg, on="DATE", how="left")
        # Fill missing predicted revenue with overall mean from last year
        df_forecast["PREDICTED_REVENUE"] = np.nan
        df_forecast["PREDICTED_REVENUE"].fillna(
            daily_avg["REVENUE"].mean(), inplace=True
        )
        df_forecast["PREDICTED_REVENUE"] = df_forecast["PREDICTED_REVENUE"].astype(
            "float"
        )
        return df_forecast

    start_date = pd.Timestamp("2025-10-01")
    end_date = pd.Timestamp("2025-10-31")
    df_forecast = forecast_revenue(df_transactions, start_date, end_date)
    assert df_forecast.get_backend() == "Pandas"

    def adjust_for_holiday_weekend(row):
        # For national holidays, revenue down 5% since stores are closed.
        # For weekends, revenue is up 5% due to increased activity.
        if row["DATE"].strftime("%Y-%m-%d") in list(
            df_us_holidays["Date"].dt.strftime("%Y-%m-%d")
        ):
            return row["PREDICTED_REVENUE"] * 0.95
        elif (
            row["DATE"].weekday() == 5 or row["DATE"].weekday() == 6
        ):  # Saturday/Sundays
            return row["PREDICTED_REVENUE"] * 1.05
        return row["PREDICTED_REVENUE"]

    # Adjust for holidays using the apply function
    df_forecast["PREDICTED_REVENUE"] = df_forecast.apply(
        adjust_for_holiday_weekend, axis=1
    )
    assert df_forecast.get_backend() == "Pandas"


@pytest.fixture
def small_snow_df():
    return pd.DataFrame([[0, 1], [2, 3]]).move_to("Snowflake")


@pytest.mark.parametrize(
    "operation",
    [
        pytest.param(
            "tail",
            marks=pytest.mark.xfail(
                reason="pd.DataFrame([[0, 1], [2, 3]]).groupby(0)[1].tail() fails with some indexing error.",
                strict=True,
            ),
        ),
        "var",
        "std",
        "sum",
        # "sem",  # unsupported
        "max",
        "mean",
        "min",
        "count",
        "nunique",
    ],
)
@sql_count_checker(query_count=2)
def test_groupby_agg_post_op_switch(operation, small_snow_df):
    assert small_snow_df.get_backend() == "Snowflake"
    dataframe_groupby_result = getattr(small_snow_df.groupby(0), operation)()
    assert dataframe_groupby_result.get_backend() == "Pandas"
    assert small_snow_df.get_backend() == "Snowflake"
    series_groupby_result = getattr(small_snow_df.groupby(0)[1], operation)()
    assert series_groupby_result.get_backend() == "Pandas"
    assert small_snow_df.get_backend() == "Snowflake"
