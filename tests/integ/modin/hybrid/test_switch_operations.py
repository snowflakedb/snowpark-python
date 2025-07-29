#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
import contextlib
import pytest
from unittest.mock import patch
import tqdm.auto

import pandas as native_pd
import numpy as np
from numpy.testing import assert_array_equal
from modin.config import context as config_context
import modin.pandas as pd
import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin._internal.row_count_estimation import (
    MAX_ROW_COUNT_FOR_ESTIMATION,
)
from snowflake.snowpark.modin.plugin._internal.utils import (
    MODIN_IS_AT_LEAST_0_34_0,
)


from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from tests.integ.utils.sql_counter import sql_count_checker
from modin.core.storage_formats.base.query_compiler import QCCoercionCost


@sql_count_checker(query_count=0)
def test_get_rows_with_large_and_none_upper_bound():
    """
    Tests that _get_rows returns a large default value when row_count_upper_bound
    is None or very large.
    """

    pandas_df = pd.DataFrame({"A": [1, 2, 3, 4]})
    df = pandas_df.move_to("Snowflake")
    df._query_compiler._modin_frame.ordered_dataframe.row_count_upper_bound = None
    assert (
        SnowflakeQueryCompiler._get_rows(df._query_compiler)
        == MAX_ROW_COUNT_FOR_ESTIMATION
    )
    df._query_compiler._modin_frame.ordered_dataframe.row_count_upper_bound = (
        MAX_ROW_COUNT_FOR_ESTIMATION + 1
    )
    assert (
        SnowflakeQueryCompiler._get_rows(df._query_compiler)
        == MAX_ROW_COUNT_FOR_ESTIMATION
    )
    assert (
        df._query_compiler.move_to_cost(
            type(pandas_df._query_compiler), "DataFrame", "apply", {}
        )
        == QCCoercionCost.COST_IMPOSSIBLE
    )


@sql_count_checker(query_count=0)
def test_move_to_me_cost_with_incompatible_dtype(caplog):
    """
    Tests that the move_to_me cost is impossible when the DataFrame has a dtype
    that is incompatible with Snowpark pandas, and that a warning is issued
    when attempting to convert it.
    """

    # DataFrame with a compatible dtype.
    df_compatible = pd.DataFrame({"A": [1, 2, 3]})
    df_compatible.move_to("Pandas")

    cost_compatible = SnowflakeQueryCompiler.move_to_me_cost(
        df_compatible._query_compiler
    )
    assert cost_compatible < QCCoercionCost.COST_IMPOSSIBLE

    # DataFrame with an incompatible dtype.
    df_incompatible = df_compatible.astype("category")

    caplog.clear()
    WarningMessage.printed_warnings.clear()
    with caplog.at_level(logging.WARNING):
        cost_incompatible = SnowflakeQueryCompiler.move_to_me_cost(
            df_incompatible._query_compiler
        )
        assert cost_incompatible == QCCoercionCost.COST_IMPOSSIBLE
        assert "not directly compatible with the Snowflake backend" in caplog.text

    # Verify that attempting to move the incompatible DataFrame to Snowflake
    # issues an exception.
    with pytest.raises(NotImplementedError):
        df_incompatible.move_to("Snowflake")


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


@sql_count_checker(query_count=4)
def test_filtered_data(init_transaction_tables):
    # When data is filtered, the engine should change when it is sufficiently small.
    df_transactions = pd.read_snowflake("REVENUE_TRANSACTIONS")
    assert df_transactions.get_backend() == "Snowflake"
    # in-place operations that do not change the backend
    # TODO: the following will result in an align which will grow the
    # size of the row estimate
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
    # We still operate in Snowflake because we cannot properly estimate the rows
    assert df_transactions_filter1.get_backend() == "Snowflake"
    df_transactions_filter2 = pd.read_snowflake(
        "SELECT * FROM revenue_transactions WHERE Date >= DATEADD( 'days', -7, '2025-06-09' ) and Date < '2025-06-09'"
    )
    assert df_transactions_filter2.get_backend() == "Pandas"
    assert_array_equal(
        # Snowpark handles index objects differently from native pandas, so just check values
        df_transactions_filter1.to_pandas().values,
        df_transactions_filter2.groupby("DATE").sum()["REVENUE"].to_pandas().values,
    )


@sql_count_checker(query_count=4)
def test_apply(init_transaction_tables, us_holidays_data):
    df_transactions = pd.read_snowflake("REVENUE_TRANSACTIONS").head(1000)
    assert df_transactions.get_backend() == "Snowflake"
    df_us_holidays = pd.DataFrame(us_holidays_data, columns=["Holiday", "Date"])
    df_us_holidays["Date"] = pd.to_datetime(df_us_holidays["Date"])
    assert df_us_holidays.get_backend() == "Pandas"

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

    assert (
        df_transactions._query_compiler._modin_frame.ordered_dataframe.row_count_upper_bound
        == 1000
    )
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


@sql_count_checker(query_count=1)
def test_explain_switch(init_transaction_tables, us_holidays_data):
    from snowflake.snowpark.modin.plugin._internal.telemetry import (
        clear_hybrid_switch_log,
    )

    clear_hybrid_switch_log()
    df_transactions = pd.read_snowflake("REVENUE_TRANSACTIONS")
    df_us_holidays = pd.DataFrame(us_holidays_data, columns=["Holiday", "Date"])
    pd.merge(df_us_holidays, df_transactions, left_on="Date", right_on="DATE")
    assert "decision" in str(pd.explain_switch())
    assert "decision" in str(pd.explain_switch(simple=False))
    assert "DataFrame.__init__" in str(pd.explain_switch())
    assert "rows" in str(pd.explain_switch(simple=False))


@sql_count_checker(query_count=1)
def test_np_where_manual_switch():
    df = pd.DataFrame([[True, False]]).set_backend("Snowflake")
    with pytest.raises(TypeError, match=r"no implementation found for 'numpy\.where'"):
        # Snowpark pandas currently does not support np.where with native objects
        np.where(df, [1, 2], [3, 4])
    df.set_backend("Pandas", inplace=True)
    # SNOW-2173644: Prior to modin 0.34, manually switching the backend to pandas would cause lookup
    # of the __array_function__ method to fail.
    with (
        pytest.raises(
            AttributeError,
            match=r"DataFrame object has no attribute __array_function__",
        )
        if not MODIN_IS_AT_LEAST_0_34_0
        else contextlib.nullcontext()
    ):
        result = np.where(df, [1, 2], [3, 4])
        assert_array_equal(result, np.array([[1, 4]]))


@sql_count_checker(query_count=0)
def test_tqdm_usage_during_pandas_to_snowflake_switch():
    progress_iter_count = 2
    df = pd.DataFrame([1, 2, 3]).set_backend("pandas")

    with patch.object(
        tqdm.auto, "trange", return_value=range(progress_iter_count)
    ) as mock_trange:
        df.set_backend("Snowflake")

    mock_trange.assert_called_once()


@sql_count_checker(query_count=1)
def test_tqdm_usage_during_snowflake_to_pandas_switch():
    progress_iter_count = 2
    df = pd.DataFrame([1, 2, 3]).set_backend("Snowflake")

    with patch.object(
        tqdm.auto, "trange", return_value=range(progress_iter_count)
    ) as mock_trange:
        df.set_backend("Pandas")

    mock_trange.assert_called_once()


@pytest.mark.parametrize(
    "class_name, method_name, f_args",
    [
        ("DataFrame", "to_json", ()),  # declared in base_overrides
        ("Series", "to_json", ()),  # declared in base_overrides
        ("DataFrame", "dot", ([6],)),  # declared in dataframe_overrides
        ("Series", "transform", (lambda x: x * 2,)),  # declared in series_overrides
    ],
)
@sql_count_checker(query_count=1)
def test_unimplemented_autoswitches(class_name, method_name, f_args):
    # Unimplemented methods declared via register_*_not_implemented should automatically
    # default to local pandas execution.
    # This test needs to be modified if any of the APIs in question are ever natively implemented
    # for Snowpark pandas.
    data = [1, 2, 3]
    method = getattr(getattr(pd, class_name)(data).move_to("Snowflake"), method_name)
    # Attempting to call the method without switching should raise.
    with config_context(AutoSwitchBackend=False):
        with pytest.raises(
            NotImplementedError, match="Snowpark pandas does not yet support the method"
        ):
            method(*f_args)
    # Attempting to call the method while switching is enabled should work fine.
    snow_result = method(*f_args)
    pandas_result = getattr(getattr(native_pd, class_name)(data), method_name)(*f_args)
    if isinstance(snow_result, (pd.DataFrame, pd.Series)):
        assert snow_result.get_backend() == "Pandas"
        assert_array_equal(snow_result.to_numpy(), pandas_result.to_numpy())
    else:
        # Series.to_json will output an extraneous level for the __reduced__ column, but that's OK
        # since we don't officially support the method.
        # See modin bug: https://github.com/modin-project/modin/issues/7624
        if class_name == "Series" and method_name == "to_json":
            assert snow_result == '{"__reduced__":{"0":1,"1":2,"2":3}}'
        else:
            assert snow_result == pandas_result


@sql_count_checker(
    query_count=12,
    join_count=6,
    udtf_count=2,
    high_count_expected=True,
    high_count_reason="tests queries across different execution modes",
)
def test_query_count_no_switch(init_transaction_tables):
    """
    Tests that when there is no switching behavior the query count is the
    same under hybrid mode and non-hybrid mode.
    """

    def inner_test(df_in):
        df_result = df_in[(df_in["REVENUE"] > 123) & (df_in["REVENUE"] < 200)]
        df_result["REVENUE_DUPE"] = df_result["REVENUE"]
        df_result["COUNT"] = df_result.groupby("DATE")["REVENUE"].transform("count")
        return df_result

    df_transactions = pd.read_snowflake("REVENUE_TRANSACTIONS")
    inner_test(df_transactions)
    orig_len = None
    hybrid_len = None
    with pd.session.query_history() as query_history_orig:
        with config_context(AutoSwitchBackend=False, NativePandasMaxRows=10):
            df_result = inner_test(df_transactions)
            orig_len = len(df_result)

    with pd.session.query_history() as query_history_hybrid:
        with config_context(AutoSwitchBackend=True, NativePandasMaxRows=10):
            df_result = inner_test(df_transactions)
            hybrid_len = len(df_result)

    assert orig_len == hybrid_len
    assert len(query_history_orig.queries) == len(query_history_hybrid.queries)
