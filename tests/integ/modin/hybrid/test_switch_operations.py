#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
from unittest import mock
import pytest
from unittest.mock import patch
import tqdm.auto

from dataclasses import dataclass
from typing import Optional, Any, Dict
import pandas as native_pd
import numpy as np
from numpy.testing import assert_array_equal
from pytest import param
from modin.config import context as config_context, Backend
import modin.pandas as pd
import snowflake.snowpark.functions as snowpark_functions
from tests.utils import running_on_jenkins
from types import MappingProxyType
import re
from snowflake.snowpark.modin.config import SnowflakePandasTransferThreshold
import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin._internal.row_count_estimation import (
    MAX_ROW_COUNT_FOR_ESTIMATION,
)
from snowflake.snowpark.modin.plugin._internal.telemetry import (
    clear_hybrid_switch_log,
)
from modin.core.storage_formats.base.query_compiler import QCCoercionCost
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from snowflake.snowpark.modin.plugin.extensions.datetime_index import DatetimeIndex
from snowflake.snowpark.modin.plugin._internal.utils import (
    MODIN_IS_AT_LEAST_0_37_0,
)
from tests.integ.utils.sql_counter import sql_count_checker, SqlCounter
from tests.integ.modin.utils import assert_snowpark_pandas_equal_to_pandas

# snowflake-ml-python, which provides snowflake.cortex, may not be available in
# the test environment. If it's not available, skip all tests in this module.
cortex = pytest.importorskip("snowflake.cortex")
Sentiment = cortex.Sentiment


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


@sql_count_checker(query_count=9, union_count=1)
def test_snowflake_pandas_transfer_threshold():
    """
    Tests that the SnowflakePandasTransferThreshold configuration variable
    is correctly used in the cost model.
    """
    # Verify the default value of the configuration variable.
    assert SnowflakePandasTransferThreshold.get() == 100_000

    # Create a SnowflakeQueryCompiler and verify that it has the default value.
    compiler = SnowflakeQueryCompiler(mock.create_autospec(InternalFrame))
    assert compiler._transfer_threshold() == 100_000

    df = pd.DataFrame()
    assert df.get_backend() == "Pandas"
    snow_df = pd.DataFrame({"A": [1, 2, 3] * 100})
    snow_df = snow_df.move_to("Snowflake")
    assert snow_df.get_backend() == "Snowflake"
    cost = snow_df._query_compiler.move_to_cost(
        type(df._query_compiler), "DataFrame", "test_op", {}
    )
    assert cost < QCCoercionCost.COST_LOW
    pandas_df = snow_df.transpose()

    assert pandas_df.get_backend() == "Pandas"

    # Set and verify that we can set the transfer cost to
    # something low and it works.
    # TODO: Allow for usage of this variable with the modin
    # config context.
    with config_context(SnowflakePandasTransferThreshold=10):
        compiler = SnowflakeQueryCompiler(mock.create_autospec(InternalFrame))
        assert compiler._transfer_threshold() == 10

        snow_df = pd.DataFrame({"A": [1, 2, 3] * 100})
        snow_df = snow_df.move_to("Snowflake")
        # Verify that the move_to_cost changes when this value is changed.
        cost = snow_df._query_compiler.move_to_cost(
            type(df._query_compiler), "DataFrame", "test_op", {}
        )
        assert cost == QCCoercionCost.COST_IMPOSSIBLE
        assert snow_df.get_backend() == "Snowflake"
        result_df = snow_df.transpose()
        assert result_df.get_backend() == "Snowflake"


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


# Newer version of modin switches before the merge
@sql_count_checker(query_count=2 if MODIN_IS_AT_LEAST_0_37_0 else 0)
def test_merge(revenue_transactions, us_holidays_data):
    df_transactions = pd.read_snowflake(revenue_transactions)
    df_us_holidays = pd.DataFrame(us_holidays_data, columns=["Holiday", "Date"])
    assert df_transactions.get_backend() == "Snowflake"
    assert df_us_holidays.get_backend() == "Pandas"
    combined = pd.merge(
        df_us_holidays, df_transactions, left_on="Date", right_on="DATE"
    )
    if MODIN_IS_AT_LEAST_0_37_0:
        # Because the result of the merge is small enough to be faster to execute in native pandas,
        # we move the Snowflake data to pandas.
        assert combined.get_backend() == "Pandas"
    else:
        # Older version of modin moves to Snowflake because df_us_holidays is small.
        assert combined.get_backend() == "Snowflake"


@sql_count_checker(query_count=2)
def test_filtered_data(revenue_transactions):
    # When data is filtered, the engine should change when it is sufficiently small.
    df_transactions = pd.read_snowflake(revenue_transactions)
    assert df_transactions.get_backend() == "Snowflake"
    # in-place operations that do not change the backend
    # TODO: the following will result in an align which will grow the
    # size of the row estimate
    df_transactions["DATE"] = pd.to_datetime(df_transactions["DATE"])
    assert df_transactions.get_backend() == "Snowflake"
    base_date = pd.Timestamp("2025-06-09").date()

    # Filter 1 will stay in snowflake, because no operations are
    # performed which will trigger a switch
    df_transactions_filter1 = df_transactions[
        (df_transactions["DATE"] >= base_date - pd.Timedelta("7 days"))
        & (df_transactions["DATE"] < base_date)
    ][["DATE", "REVENUE"]]
    assert df_transactions_filter1.get_backend() == "Snowflake"

    # We still do not know the size of the underlying data, so
    # GroupBy.sum will keep the data in Snowflake
    # The smaller dataframe does operations in pandas
    df_transactions_filter1 = df_transactions_filter1.groupby("DATE").sum()
    # We still operate in Snowflake because we cannot properly estimate the rows
    assert df_transactions_filter1.get_backend() == "Snowflake"

    # The SQL here is functionatly the same as above
    # Unlike in previous iterations of hybrid this does *not* move the data immediately
    df_transactions_filter2 = pd.read_snowflake(
        f"SELECT Date, SUM(Revenue) AS REVENUE FROM {revenue_transactions} WHERE Date >= DATEADD( 'days', -7, '2025-06-09' ) and Date < '2025-06-09' GROUP BY DATE"
    )
    # We do not know the size of this data yet, because the query is entirely lazy
    assert df_transactions_filter2.get_backend() == "Snowflake"
    # Move to pandas backend
    df_transactions_filter2.move_to("Pandas", inplace=True)
    assert df_transactions_filter2.get_backend() == "Pandas"

    # Sort and compare the results.
    assert_array_equal(
        # Snowpark handles index objects differently from native pandas, so just check values
        # A .head on filter1 will trigger migration to pandas
        df_transactions_filter1["REVENUE"]
        .to_pandas()
        .sort_values(ascending=True)
        .values,
        df_transactions_filter2["REVENUE"]
        .to_pandas()
        .sort_values(ascending=True)
        .values,
    )


@sql_count_checker(query_count=3)
def test_apply(revenue_transactions, us_holidays_data):
    df_transactions = pd.read_snowflake(revenue_transactions).head(1000)
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
        param(
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


@sql_count_checker(query_count=0)
def test_explain_switch_empty():
    clear_hybrid_switch_log()
    empty_switch = pd.explain_switch()
    assert len(empty_switch) == 0
    empty_switch_cols = empty_switch.columns.tolist()
    empty_switch_index_names = empty_switch.index.names
    pd.DataFrame().move_to("Snowflake")
    new_switch = pd.explain_switch()
    assert len(new_switch) > 0
    new_switch_cols = new_switch.columns.tolist()
    new_switch_index_names = new_switch.index.names
    assert new_switch_cols == empty_switch_cols
    assert new_switch_index_names == empty_switch_index_names


# Newer version of modin switches before the merge
@sql_count_checker(query_count=2 if MODIN_IS_AT_LEAST_0_37_0 else 0)
def test_explain_switch(revenue_transactions, us_holidays_data):
    clear_hybrid_switch_log()
    df_transactions = pd.read_snowflake(revenue_transactions)
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
@pytest.mark.parametrize("use_session_param", [True, False])
@sql_count_checker(query_count=1)
def test_unimplemented_autoswitches(class_name, method_name, f_args, use_session_param):
    # Unimplemented methods declared via register_*_not_implemented should automatically
    # default to local pandas execution.
    # This test needs to be modified if any of the APIs in question are ever natively implemented
    # for Snowpark pandas.
    data = [1, 2, 3]
    method = getattr(getattr(pd, class_name)(data).move_to("Snowflake"), method_name)
    # Attempting to call the method without switching should raise.
    with config_context(AutoSwitchBackend=False):
        if use_session_param:
            from modin.config import AutoSwitchBackend

            AutoSwitchBackend.enable()
            pd.session.pandas_hybrid_execution_enabled = False
            assert pd.session.pandas_hybrid_execution_enabled is False
            assert AutoSwitchBackend.get() is False
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
        if (
            not MODIN_IS_AT_LEAST_0_37_0
            and class_name == "Series"
            and method_name == "to_json"
        ):
            assert snow_result == '{"__reduced__":{"0":1,"1":2,"2":3}}'
        else:
            assert snow_result == pandas_result


@sql_count_checker(query_count=0)
def test_to_datetime():
    assert Backend.get() == "Snowflake"
    # Should return a Snowpark pandas object without error
    result = pd.to_datetime([3, 4, 5], unit="Y")
    assert isinstance(result, DatetimeIndex)


@pytest.mark.parametrize("use_session_param", [True, False])
@sql_count_checker(
    query_count=11,
    join_count=6,
    udtf_count=2,
    high_count_expected=True,
    high_count_reason="tests queries across different execution modes",
)
def test_query_count_no_switch(revenue_transactions, use_session_param):
    """
    Tests that when there is no switching behavior the query count is the
    same under hybrid mode and non-hybrid mode.
    """

    def inner_test(df_in):
        df_result = df_in[(df_in["REVENUE"] > 123) & (df_in["REVENUE"] < 200)]
        df_result["REVENUE_DUPE"] = df_result["REVENUE"]
        df_result["COUNT"] = df_result.groupby("DATE")["REVENUE"].transform("count")
        return df_result

    df_transactions = pd.read_snowflake(revenue_transactions)
    inner_test(df_transactions)
    orig_len = None
    hybrid_len = None
    with pd.session.query_history() as query_history_orig:
        with config_context(AutoSwitchBackend=False, NativePandasMaxRows=10):
            if use_session_param:
                from modin.config import AutoSwitchBackend

                AutoSwitchBackend.enable()
                pd.session.pandas_hybrid_execution_enabled = False
                assert pd.session.pandas_hybrid_execution_enabled is False
                assert AutoSwitchBackend.get() is False
            df_result = inner_test(df_transactions)
            orig_len = len(df_result)

    with pd.session.query_history() as query_history_hybrid:
        with config_context(AutoSwitchBackend=True, NativePandasMaxRows=10):
            if use_session_param:
                from modin.config import AutoSwitchBackend

                AutoSwitchBackend.disable()
                pd.session.pandas_hybrid_execution_enabled = True
                assert pd.session.pandas_hybrid_execution_enabled is True
                assert AutoSwitchBackend.get() is True
            df_result = inner_test(df_transactions)
            hybrid_len = len(df_result)

    assert orig_len == hybrid_len
    assert len(query_history_orig.queries) == len(query_history_hybrid.queries)


non_callable_func_not_implemented = pytest.mark.xfail(
    strict=True,
    raises=NotImplementedError,
    match=re.escape("Snowpark pandas apply API only supports callables func"),
)


class TestApplySnowparkAndCortexFunctions:
    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize(
        "func",
        [
            snowpark_functions.floor,
            param(
                [
                    snowpark_functions.floor,
                    np.floor,
                ],
                marks=non_callable_func_not_implemented,
            ),
            param(
                {
                    "col0": snowpark_functions.floor,
                },
                marks=non_callable_func_not_implemented,
            ),
            param(
                {"col0": [np.floor, snowpark_functions.floor]},
                marks=non_callable_func_not_implemented,
            ),
        ],
    )
    def test_applying_snowpark_function_to_dataframe_causes_backend_switch(self, func):
        """Test that applying Snowpark functions triggers switch from pandas backend to Snowflake."""
        pandas_backend_df = pd.DataFrame({"col0": [-1.7, 2.3, 3.9]}).set_backend(
            "pandas"
        )
        result = pandas_backend_df.apply(func)
        assert result.get_backend() == "Snowflake"
        result.to_pandas()

    @sql_count_checker(query_count=0)
    @pytest.mark.parametrize(
        "func",
        [
            abs,
            [
                abs,
                round,
            ],
            {
                "col0": abs,
            },
            {"col0": [abs, round]},
        ],
    )
    def test_applying_non_snowpark_function_to_dataframe_keeps_pandas_backend(
        self, func
    ):
        """Test that non-snowpark python functions don't trigger backend switch."""

        pandas_backend_df = pd.DataFrame({"col0": [-1.7, 2.3, 3.9]}).set_backend(
            "pandas"
        )
        result = pandas_backend_df.apply(func)
        assert result.get_backend() == "Pandas"
        result.to_pandas()

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize(
        "data_class,method",
        [
            (pd.Series, pd.Series.map),
            (pd.Series, pd.Series.apply),
            (pd.DataFrame, pd.DataFrame.applymap),
            (pd.DataFrame, pd.DataFrame.map),
        ],
    )
    def test_mapping_snowpark_function_causes_backend_switch(self, data_class, method):
        pandas_backend_df = data_class([1.7, 2.3]).set_backend("pandas")
        result = method(pandas_backend_df, snowpark_functions.floor)
        assert result.get_backend() == "Snowflake"
        result.to_pandas()

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize(
        "func",
        [
            snowpark_functions.floor,
            param(
                [snowpark_functions.floor, abs],
                marks=non_callable_func_not_implemented,
            ),
        ],
    )
    def test_applying_snowpark_function_to_series_causes_backend_switch(self, func):
        series = pd.Series([1.7, 2.3, 3.9]).set_backend("pandas")
        result = series.apply(func)
        assert result.get_backend() == "Snowflake"
        result.to_pandas()

    @sql_count_checker(query_count=1)
    @pytest.mark.parametrize(
        "data_class,method",
        [
            (pd.Series, pd.Series.map),
            (pd.Series, pd.Series.apply),
            (pd.DataFrame, pd.DataFrame.apply),
            (pd.DataFrame, pd.DataFrame.applymap),
            (pd.DataFrame, pd.DataFrame.map),
        ],
    )
    @pytest.mark.skipif(
        running_on_jenkins(),
        reason="TODO: SNOW-1859087 applying snowflake.cortex functions causes SSL error",
    )
    def test_applying_cortex_function_causes_backend_switch(self, data_class, method):
        """Test that applying Snowflake Cortex functions triggers switch from pandas backend to Snowflake."""
        pandas_backend_data = data_class(["happy"]).set_backend("pandas")
        sentiment = method(pandas_backend_data, Sentiment)
        assert sentiment.get_backend() == "Snowflake"
        sentiment.to_pandas()


@sql_count_checker(query_count=1, join_count=2)
def test_switch_then_iloc():
    # Switching backends then calling iloc should be valid.
    # Prior to fixing SNOW-2331021, discrepancies with the index class caused an AssertionError.
    df = pd.DataFrame([[0] * 10] * 10)
    assert df.get_backend() == "Pandas"
    # Should not error
    assert_snowpark_pandas_equal_to_pandas(
        df.move_to("Snowflake").iloc[[1, 3, 9], 1],
        df.iloc[[1, 3, 9], 1].to_pandas(),
    )
    # Setting should similarly not error
    df.iloc[1, 1] = 100
    assert df.iloc[1, 1] == 100


@sql_count_checker(query_count=1, join_count=1)
def test_rename():
    # SNOW-2333472: Switching backends then performing a rename should be valid.
    df = pd.DataFrame([[0] * 3] * 3)
    assert df.get_backend() == "Pandas"
    assert_snowpark_pandas_equal_to_pandas(
        df.move_to("Snowflake").rename({0: "a", 1: "b", 2: "c"}),
        # Perform to_pandas first due to modin issue 7667
        df.to_pandas().rename({0: "a", 1: "b", 2: "c"}),
    )


@sql_count_checker(query_count=1, join_count=1)
def test_set_index():
    s = pd.Series([0]).move_to("Snowflake")
    # SNOW-2333472: Switching backends then setting the index should be valid.
    s.index = ["a"]
    assert_snowpark_pandas_equal_to_pandas(s, native_pd.Series([0], index=["a"]))


def _test_stay_cost(data_obj, api_cls_name, method_name, args, expected_cost):
    stay_cost = data_obj._query_compiler.stay_cost(
        api_cls_name, method_name, MappingProxyType(args)
    )
    assert stay_cost == expected_cost


def _test_move_to_me_cost(pandas_qc, api_cls_name, method_name, args, expected_cost):
    move_to_me_cost = SnowflakeQueryCompiler.move_to_me_cost(
        pandas_qc, api_cls_name, method_name, MappingProxyType(args)
    )
    assert move_to_me_cost == expected_cost


def _test_expected_backend(
    data_obj, method_name, args, expected_backend, is_top_level=False
):
    if is_top_level:
        result = getattr(pd, method_name)(data_obj, **args)
    else:
        result = getattr(data_obj, method_name)(**args)

    if hasattr(result, "get_backend"):
        assert result.get_backend() == expected_backend

    return result


@dataclass(frozen=True)
class AutoSwitchCase:
    # Test case for auto-switch on unsupported args functionality
    api_cls_name: Optional[str]
    method: str
    args: Dict[str, Any]
    test_data: Dict[str, Any]
    expected_query_count: int
    is_supported: bool
    skip_backend_check: bool = (
        False  # skip backend behavior checks due to post-operation switching
    )
    test_series: bool = True  # some methods/args are not supported on Series


SUPPORTED_CASES = [
    AutoSwitchCase(
        api_cls_name=None,
        method="get_dummies",
        args={},
        test_data={"A": ["x", "y", "z"], "B": [1, 2, 3]},
        expected_query_count=1,
        is_supported=True,
    ),
    AutoSwitchCase(
        api_cls_name="BasePandasDataset",
        method="skew",
        args={"numeric_only": True},
        test_data={"A": [1, 2, 3], "B": [4, 5, 6]},
        expected_query_count=3,
        is_supported=True,
    ),
    AutoSwitchCase(
        api_cls_name="BasePandasDataset",
        method="round",
        args={"decimals": 2},
        test_data={"A": [1.234, 2.567], "B": [3.891, 4.123]},
        expected_query_count=2,
        is_supported=True,
    ),
    AutoSwitchCase(
        api_cls_name="BasePandasDataset",
        method="cumsum",
        args={"axis": 0},
        test_data={"A": [1, 2, 3], "B": [4, 5, 6]},
        expected_query_count=2,
        skip_backend_check=True,
        is_supported=True,
    ),
    AutoSwitchCase(
        api_cls_name="BasePandasDataset",
        method="cummin",
        args={"axis": 0},
        test_data={"A": [1, 2, 3], "B": [4, 5, 6]},
        expected_query_count=2,
        skip_backend_check=True,
        is_supported=True,
    ),
    AutoSwitchCase(
        api_cls_name="BasePandasDataset",
        method="cummax",
        args={"axis": 0},
        test_data={"A": [1, 2, 3], "B": [4, 5, 6]},
        expected_query_count=2,
        skip_backend_check=True,
        is_supported=True,
    ),
]

UNSUPPORTED_CASES = [
    AutoSwitchCase(
        api_cls_name=None,
        method="get_dummies",
        args={"dummy_na": True},
        test_data={"A": ["x", "y", "z"], "B": [1, 2, 3]},
        expected_query_count=1,
        is_supported=False,
    ),
    AutoSwitchCase(
        api_cls_name=None,
        method="get_dummies",
        args={"drop_first": True},
        test_data={"A": ["x", "y", "z"], "B": [1, 2, 3]},
        expected_query_count=1,
        is_supported=False,
    ),
    AutoSwitchCase(
        api_cls_name=None,
        method="get_dummies",
        args={"dtype": int},
        test_data={"A": ["x", "y", "z"], "B": [1, 2, 3]},
        expected_query_count=1,
        is_supported=False,
    ),
    AutoSwitchCase(
        api_cls_name="BasePandasDataset",
        method="skew",
        args={"axis": 1},
        test_data={"A": [1, 2, 3], "B": [4, 5, 6]},
        expected_query_count=1,
        is_supported=False,
        test_series=False,
    ),
    AutoSwitchCase(
        api_cls_name="BasePandasDataset",
        method="skew",
        args={"numeric_only": False},
        test_data={"A": [1, 2, 3], "B": [4, 5, 6]},
        expected_query_count=2,
        is_supported=False,
    ),
    AutoSwitchCase(
        api_cls_name="BasePandasDataset",
        method="cumsum",
        args={"axis": 1},
        test_data={"A": [1, 2, 3], "B": [4, 5, 6]},
        expected_query_count=1,
        is_supported=False,
        test_series=False,
    ),
    AutoSwitchCase(
        api_cls_name="BasePandasDataset",
        method="cummin",
        args={"axis": 1},
        test_data={"A": [1, 2, 3], "B": [4, 5, 6]},
        expected_query_count=1,
        is_supported=False,
        test_series=False,
    ),
    AutoSwitchCase(
        api_cls_name="BasePandasDataset",
        method="cummax",
        args={"axis": 1},
        test_data={"A": [1, 2, 3], "B": [4, 5, 6]},
        expected_query_count=1,
        is_supported=False,
        test_series=False,
    ),
]


@pytest.mark.parametrize(
    "case", SUPPORTED_CASES + UNSUPPORTED_CASES, ids=lambda c: f"{c.method}({c.args})"
)
def test_auto_switch(case: AutoSwitchCase):
    """Test auto-switch functionality for both supported and unsupported args."""
    with SqlCounter(query_count=case.expected_query_count):
        # Create test objects
        df = series = None
        if case.api_cls_name in [None, "DataFrame", "BasePandasDataset"]:
            df = pd.DataFrame(case.test_data).move_to("Snowflake")
        if case.api_cls_name in ["Series", "BasePandasDataset"]:
            series = pd.Series(list(case.test_data.values())[0]).move_to("Snowflake")

        # Test costs
        expected_cost = (
            QCCoercionCost.COST_ZERO
            if case.is_supported
            else QCCoercionCost.COST_IMPOSSIBLE
        )
        if df is not None:
            _test_stay_cost(
                df, case.api_cls_name, case.method, case.args, expected_cost
            )
        if series is not None:
            _test_stay_cost(
                series, case.api_cls_name, case.method, case.args, expected_cost
            )

        if not case.is_supported:
            pandas_df = pd.DataFrame(case.test_data)
            _test_move_to_me_cost(
                pandas_df._query_compiler,
                case.api_cls_name,
                case.method,
                case.args,
                QCCoercionCost.COST_IMPOSSIBLE,
            )

        # Test expected backend
        if not case.skip_backend_check:
            expected_backend = "Snowflake" if case.is_supported else "Pandas"
            if df is not None:
                _test_expected_backend(
                    df,
                    case.method,
                    case.args,
                    expected_backend,
                    case.api_cls_name is None,
                )
            if series is not None and case.test_series:
                _test_expected_backend(
                    series, case.method, case.args, expected_backend, False
                )

        # Test result equality
        if df is not None:
            if case.api_cls_name is None:
                actual = getattr(pd, case.method)(df, **case.args)
                expected = getattr(native_pd, case.method)(
                    native_pd.DataFrame(case.test_data), **case.args
                )
            else:
                actual = getattr(df, case.method)(**case.args)
                expected = getattr(native_pd.DataFrame(case.test_data), case.method)(
                    **case.args
                )

            if hasattr(actual, "get_backend"):
                assert_snowpark_pandas_equal_to_pandas(actual, expected)
            else:
                assert actual == expected

        if series is not None and case.test_series:
            series_data = list(case.test_data.values())[0]
            actual = getattr(series, case.method)(**case.args)
            expected = getattr(native_pd.Series(series_data), case.method)(**case.args)

            if hasattr(actual, "get_backend"):
                assert_snowpark_pandas_equal_to_pandas(actual, expected)
            else:
                assert actual == expected


@sql_count_checker(query_count=2)
def test_round_with_series_decimals():
    # Test round with Series decimals (unsupported case). round requires a different implementation so we make a separate test.
    round_series_decimals = pd.Series([1, 2], index=["A", "B"])
    test_data = {"A": [1.234, 2.567], "B": [3.891, 4.123]}

    df = pd.DataFrame(test_data).move_to("Snowflake")
    series = pd.Series(list(test_data.values())[0]).move_to("Snowflake")
    args = {"decimals": round_series_decimals}

    _test_stay_cost(
        df, "BasePandasDataset", "round", args, QCCoercionCost.COST_IMPOSSIBLE
    )
    _test_stay_cost(
        series, "BasePandasDataset", "round", args, QCCoercionCost.COST_IMPOSSIBLE
    )

    pandas_df = pd.DataFrame(test_data)
    _test_move_to_me_cost(
        pandas_df._query_compiler,
        "BasePandasDataset",
        "round",
        args,
        QCCoercionCost.COST_IMPOSSIBLE,
    )

    _test_expected_backend(df, "round", args, "Pandas", False)
    _test_expected_backend(series, "round", args, "Pandas", False)


@sql_count_checker(query_count=0)
def test_error_handling_when_auto_switch_disabled():
    # Test that unsupported args raise NotImplementedError when auto-switch is disabled.
    with config_context(AutoSwitchBackend=False):
        for case in UNSUPPORTED_CASES:
            df = series = None

            if case.api_cls_name in [None, "DataFrame", "BasePandasDataset"]:
                df = pd.DataFrame(case.test_data).move_to("Snowflake")
            if case.api_cls_name in ["Series", "BasePandasDataset"]:
                series = pd.Series(list(case.test_data.values())[0]).move_to(
                    "Snowflake"
                )

            if df is not None:
                with pytest.raises(NotImplementedError):
                    if case.api_cls_name is None:
                        getattr(pd, case.method)(df, **case.args)
                    else:
                        getattr(df, case.method)(**case.args)

            if series is not None and case.test_series:
                with pytest.raises(NotImplementedError):
                    getattr(series, case.method)(**case.args)
