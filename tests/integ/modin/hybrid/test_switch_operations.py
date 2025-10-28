#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
from unittest import mock
import pytest
from unittest.mock import patch
import tqdm.auto

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
    UnsupportedArgsRule,
    register_query_compiler_method_not_implemented,
)
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from snowflake.snowpark.modin.plugin.extensions.datetime_index import DatetimeIndex
from snowflake.snowpark.modin.plugin._internal.utils import (
    MODIN_IS_AT_LEAST_0_37_0,
)
from tests.integ.utils.sql_counter import sql_count_checker, SqlCounter
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equal_to_pandas,
    eval_snowpark_pandas_result,
)

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
        df_compatible._query_compiler, None, None, None
    )
    assert cost_compatible < QCCoercionCost.COST_IMPOSSIBLE

    # DataFrame with an incompatible dtype.
    df_incompatible = df_compatible.astype("category")

    caplog.clear()
    WarningMessage.printed_warnings.clear()
    with caplog.at_level(logging.WARNING):
        cost_incompatible = SnowflakeQueryCompiler.move_to_me_cost(
            df_incompatible._query_compiler, None, None, None
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


def _test_stay_cost(*, data_obj, api_cls_name, method_name, args, expected_cost):
    stay_cost = data_obj._query_compiler.stay_cost(
        api_cls_name, method_name, MappingProxyType(args)
    )
    assert stay_cost == expected_cost


def _test_move_to_me_cost(*, pandas_qc, api_cls_name, method_name, args, expected_cost):
    move_to_me_cost = SnowflakeQueryCompiler.move_to_me_cost(
        pandas_qc, api_cls_name, method_name, MappingProxyType(args)
    )
    assert move_to_me_cost == expected_cost


def _test_expected_backend(
    *, data_obj, method_name, args, expected_backend, is_top_level=False
):
    if is_top_level:
        result = getattr(pd, method_name)(data_obj, **args)
    else:
        result = getattr(data_obj, method_name)(**args)

    assert result.get_backend() == expected_backend


@pytest.mark.parametrize(
    "method,kwargs",
    [
        ("get_dummies", {}),
        ("melt", {"id_vars": ["A"], "value_vars": ["B"]}),
        ("pivot_table", {"values": "B", "index": "A"}),
    ],
)
def test_auto_switch_supported_top_level_functions(method, kwargs):
    # Test supported top-level functions that should stay on Snowflake backend.
    test_data = {"A": ["x", "y", "z"], "B": [1, 2, 3]}

    with SqlCounter(query_count=1):
        df = pd.DataFrame(test_data).move_to("Snowflake")
        assert df.get_backend() == "Snowflake"

        _test_stay_cost(
            data_obj=df,
            api_cls_name=None,
            method_name=method,
            args=kwargs,
            expected_cost=QCCoercionCost.COST_ZERO,
        )

        _test_expected_backend(
            data_obj=df,
            method_name=method,
            args=kwargs,
            expected_backend="Snowflake",
            is_top_level=True,
        )

        eval_snowpark_pandas_result(
            df,
            native_pd.DataFrame(test_data),
            lambda df: getattr(native_pd, method)(df, **kwargs)
            if isinstance(df, native_pd.DataFrame)
            else getattr(pd, method)(df, **kwargs),
            test_attrs=False,
        )


@pytest.mark.parametrize(
    "method,kwargs,api_cls_name",
    [
        ("skew", {"numeric_only": True}, "BasePandasDataset"),
        ("round", {"decimals": 1}, "BasePandasDataset"),
        ("shift", {"periods": 1}, "BasePandasDataset"),
        ("sort_index", {"axis": 0}, "BasePandasDataset"),
        ("sort_values", {"by": "A", "axis": 0}, "BasePandasDataset"),
        ("fillna", {"value": 0}, "DataFrame"),
        ("dropna", {"axis": 0}, "DataFrame"),
    ],
)
def test_auto_switch_supported_dataframe(method, kwargs, api_cls_name):
    # Test supported DataFrame operations that should stay on Snowflake backend.
    test_data = {"A": [1.23, None, 3.89], "B": [4.12, 5.26, 6.34]}

    with SqlCounter(
        query_count=1,
    ):
        df = pd.DataFrame(test_data).move_to("Snowflake")
        assert df.get_backend() == "Snowflake"

        _test_stay_cost(
            data_obj=df,
            api_cls_name=api_cls_name,
            method_name=method,
            args=kwargs,
            expected_cost=QCCoercionCost.COST_ZERO,
        )

        _test_expected_backend(
            data_obj=df,
            method_name=method,
            args=kwargs,
            expected_backend="Snowflake",
            is_top_level=False,
        )

        eval_snowpark_pandas_result(
            df, native_pd.DataFrame(test_data), lambda df: getattr(df, method)(**kwargs)
        )


@pytest.mark.parametrize(
    "method,kwargs,is_result_scalar,api_cls_name",
    [
        ("skew", {"numeric_only": True}, True, "BasePandasDataset"),
        ("round", {"decimals": 1}, False, "BasePandasDataset"),
        ("shift", {"periods": 1}, False, "BasePandasDataset"),
        ("sort_index", {"axis": 0}, False, "BasePandasDataset"),
        ("fillna", {"value": 0}, False, "Series"),
    ],
)
def test_auto_switch_supported_series(method, kwargs, is_result_scalar, api_cls_name):
    # Test supported Series operations that should stay on Snowflake backend.
    test_data = [1.89, 2.95, 3.12, None, 5.23, 6.34]

    with SqlCounter(query_count=1):
        series = pd.Series(test_data).move_to("Snowflake")
        assert series.get_backend() == "Snowflake"

        _test_stay_cost(
            data_obj=series,
            api_cls_name=api_cls_name,
            method_name=method,
            args=kwargs,
            expected_cost=QCCoercionCost.COST_ZERO,
        )

        if not is_result_scalar:
            _test_expected_backend(
                data_obj=series,
                method_name=method,
                args=kwargs,
                expected_backend="Snowflake",
                is_top_level=False,
            )

        eval_snowpark_pandas_result(
            series,
            native_pd.Series(test_data),
            lambda series: getattr(series, method)(**kwargs),
            comparator=np.testing.assert_allclose
            if is_result_scalar
            else assert_snowpark_pandas_equal_to_pandas,
            test_attrs=False,
        )


@pytest.mark.parametrize(
    "method,kwargs",
    [
        ("cumsum", {"axis": 0}),
        ("cummin", {"axis": 0}),
        ("cummax", {"axis": 0}),
    ],
)
def test_auto_switch_supported_post_op_switch_point_dataframe(method, kwargs):
    # Test DataFrame operations that execute on Snowflake but switch to Pandas post-operation.
    test_data = {"A": [1, 2, 3], "B": [4, 5, 6]}

    with SqlCounter(query_count=1):
        df = pd.DataFrame(test_data).move_to("Snowflake")
        assert df.get_backend() == "Snowflake"

        _test_stay_cost(
            data_obj=df,
            api_cls_name="BasePandasDataset",
            method_name=method,
            args=kwargs,
            expected_cost=QCCoercionCost.COST_ZERO,
        )

        # Test result equality - don't check backend as it switches post-operation
        eval_snowpark_pandas_result(
            df, native_pd.DataFrame(test_data), lambda df: getattr(df, method)(**kwargs)
        )


@pytest.mark.parametrize(
    "method,kwargs",
    [
        ("cumsum", {"axis": 0}),
        ("cummin", {"axis": 0}),
        ("cummax", {"axis": 0}),
    ],
)
def test_auto_switch_supported_post_op_switch_point_series(method, kwargs):
    # Test Series operations that execute on Snowflake but switch to Pandas post-operation.
    test_data = [1, 2, 3, 4, 5, 6]

    with SqlCounter(query_count=1):
        series = pd.Series(test_data).move_to("Snowflake")
        assert series.get_backend() == "Snowflake"

        _test_stay_cost(
            data_obj=series,
            api_cls_name="BasePandasDataset",
            method_name=method,
            args=kwargs,
            expected_cost=QCCoercionCost.COST_ZERO,
        )

        # Test result equality - don't check backend as it switches post-operation
        eval_snowpark_pandas_result(
            series,
            native_pd.Series(test_data),
            lambda series: getattr(series, method)(**kwargs),
        )


@pytest.mark.parametrize(
    "groupby_kwargs",
    [
        {"by": "A"},
        {"level": 0},
        {"by": pd.Grouper()},
        {"by": ["A", "B", "C"]},
    ],
)
def test_auto_switch_supported_groupby(groupby_kwargs):
    # Test unsupported GroupBy operations that should switch to Pandas backend.
    test_data = {"A": [1, 2, 3], "B": [4, 5, 6]}

    with SqlCounter(query_count=0):
        df = pd.DataFrame(test_data).move_to("Snowflake")
        assert df.get_backend() == "Snowflake"

        _test_stay_cost(
            data_obj=df,
            api_cls_name="DataFrameGroupBy",
            method_name="__init__",
            args=groupby_kwargs,
            expected_cost=QCCoercionCost.COST_ZERO,
        )

        groupby_obj = df.groupby(**groupby_kwargs)
        assert groupby_obj.get_backend() == "Snowflake"


@pytest.mark.parametrize(
    "method,kwargs",
    [
        ("get_dummies", {"dummy_na": True}),
        ("get_dummies", {"drop_first": True}),
        ("melt", {"col_level": 0}),
        ("pivot_table", {"values": "B", "index": "A", "sort": False}),
        ("pivot_table", {"values": "B", "index": "A", "observed": True}),
        ("pivot_table", {"index": ["A", 0], "columns": "B", "values": "B"}),
        ("pivot_table", {"index": "A", "columns": ["B", 0], "values": "B"}),
        ("pivot_table", {"index": "A", "columns": "B", "values": ["B", 0]}),
        (
            "pivot_table",
            {"index": None, "columns": "A", "values": ["B"], "aggfunc": {"B": max}},
        ),
    ],
)
def test_auto_switch_unsupported_top_level_functions(method, kwargs):
    # Test unsupported top-level functions that should switch to Pandas backend.
    test_data = {"A": ["x", "y", "z"], "B": [1, 2, 3], 0: [4, 5, 6], 1: [7, 8, 9]}

    with SqlCounter(query_count=1):
        df = pd.DataFrame(test_data).move_to("Snowflake")
        assert df.get_backend() == "Snowflake"

        _test_stay_cost(
            data_obj=df,
            api_cls_name=None,
            method_name=method,
            args=kwargs,
            expected_cost=QCCoercionCost.COST_IMPOSSIBLE,
        )

        pandas_df = pd.DataFrame(test_data)
        _test_move_to_me_cost(
            pandas_qc=pandas_df._query_compiler,
            api_cls_name=None,
            method_name=method,
            args=kwargs,
            expected_cost=QCCoercionCost.COST_IMPOSSIBLE,
        )

        _test_expected_backend(
            data_obj=df,
            method_name=method,
            args=kwargs,
            expected_backend="Pandas",
            is_top_level=True,
        )

        eval_snowpark_pandas_result(
            df,
            native_pd.DataFrame(test_data),
            lambda df: getattr(
                native_pd if isinstance(df, native_pd.DataFrame) else pd, method
            )(df, **kwargs),
        )


@pytest.mark.parametrize(
    "method,kwargs,api_cls_name",
    [
        ("skew", {"axis": 1}, "BasePandasDataset"),
        ("skew", {"numeric_only": False}, "BasePandasDataset"),
        ("cumsum", {"axis": 1}, "BasePandasDataset"),
        ("cummin", {"axis": 1}, "BasePandasDataset"),
        ("cummax", {"axis": 1}, "BasePandasDataset"),
        ("round", {"decimals": native_pd.Series([0, 1, 1])}, "BasePandasDataset"),
        ("shift", {"periods": [1, 2], "suffix": "suffix"}, "BasePandasDataset"),
        ("shift", {"periods": [1, 2]}, "BasePandasDataset"),
        ("sort_index", {"axis": 1}, "BasePandasDataset"),
        ("sort_index", {"key": lambda x: x}, "BasePandasDataset"),
        ("sort_values", {"by": 0, "axis": 1}, "BasePandasDataset"),
        ("apply", {"func": lambda x: x * 2, "result_type": "expand"}, "DataFrame"),
        ("corr", {"method": "kendall"}, "DataFrame"),
        ("corr", {"method": lambda x, y: np.corrcoef(x, y)[0, 1]}, "DataFrame"),
        ("dropna", {"axis": 1}, "DataFrame"),
        ("fillna", {"value": 0, "limit": 1}, "DataFrame"),
        ("fillna", {"downcast": "infer", "value": 0}, "DataFrame"),
    ],
)
def test_auto_switch_unsupported_dataframe(method, kwargs, api_cls_name):
    # Test unsupported DataFrame operations that should switch to Pandas backend.
    test_data = {"A": [1.234, 2.567, 9.101], "B": [3.891, 4.123, 5.912]}

    with SqlCounter(
        # not sure why query_count=2 in this case, but it doesn't matter much,
        # since the latest version of Modin gives a lower query_count=1.
        query_count=2
        if method == "round" and not MODIN_IS_AT_LEAST_0_37_0
        else 1
    ):
        snowpark_kwargs = {
            k: pd.Series(v) if isinstance(v, native_pd.Series) else v
            for k, v in kwargs.items()
        }
        df = pd.DataFrame(test_data).move_to("Snowflake")

        _test_stay_cost(
            data_obj=df,
            api_cls_name=api_cls_name,
            method_name=method,
            args=snowpark_kwargs,
            expected_cost=QCCoercionCost.COST_IMPOSSIBLE,
        )

        pandas_df = pd.DataFrame(test_data)
        _test_move_to_me_cost(
            pandas_qc=pandas_df._query_compiler,
            api_cls_name=api_cls_name,
            method_name=method,
            args=snowpark_kwargs,
            expected_cost=QCCoercionCost.COST_IMPOSSIBLE,
        )

        _test_expected_backend(
            data_obj=df,
            method_name=method,
            args=snowpark_kwargs,
            expected_backend="Pandas",
            is_top_level=False,
        )

        eval_snowpark_pandas_result(
            df,
            native_pd.DataFrame(test_data),
            lambda df: getattr(df, method)(
                **(kwargs if isinstance(df, native_pd.DataFrame) else snowpark_kwargs)
            ),
        )


@pytest.mark.parametrize(
    "groupby_kwargs",
    [
        {"by": "A", "axis": 1},
        {"by": "A", "level": 0},
        {"by": lambda x: x % 2},
        {"by": np.array([1, 2, 3])},
        {"by": pd.Grouper(axis=1)},
    ],
)
def test_auto_switch_unsupported_dataframe_groupby(groupby_kwargs):
    # Test unsupported GroupBy operations that should switch to Pandas backend.
    test_data = {"A": [1, 2, 3], "B": [4, 5, 6]}

    with SqlCounter(query_count=1):
        df = pd.DataFrame(test_data).move_to("Snowflake")
        assert df.get_backend() == "Snowflake"

        _test_stay_cost(
            data_obj=df,
            api_cls_name="DataFrameGroupBy",
            method_name="__init__",
            args=groupby_kwargs,
            expected_cost=QCCoercionCost.COST_IMPOSSIBLE,
        )

        pandas_df = pd.DataFrame(test_data)
        _test_move_to_me_cost(
            pandas_qc=pandas_df._query_compiler,
            api_cls_name="DataFrameGroupBy",
            method_name="__init__",
            args=groupby_kwargs,
            expected_cost=QCCoercionCost.COST_IMPOSSIBLE,
        )

        groupby_obj = df.groupby(**groupby_kwargs)
        assert groupby_obj.get_backend() == "Pandas"


@pytest.mark.parametrize(
    "method,method_kwargs, groupby_kwargs, query_count",
    [
        ("fillna", {"value": 0}, {"by": "A", "level": 0}, 1),
        ("fillna", {"value": 0}, {"by": lambda x: x % 2}, 1),
        ("cummin", {}, {"by": "A", "axis": 1}, 1),
        ("cummin", {}, {"by": lambda x: x % 2}, 1),
        ("cummin", {}, {"by": "A", "level": 0}, 1),
        ("cumsum", {}, {"by": "A", "axis": 1}, 1),
        ("cumsum", {}, {"by": lambda x: x % 2}, 1),
        ("cumsum", {}, {"by": "A", "level": 0}, 1),
        ("cummax", {}, {"by": "A", "axis": 1}, 1),
        ("cummax", {}, {"by": "A", "level": 0}, 1),
        ("cummax", {}, {"by": lambda x: x % 2}, 1),
        ("cumcount", {}, {"by": "A", "level": 0}, 1),
        ("cumcount", {}, {"by": lambda x: x % 2}, 1),
        ("rank", {}, {"by": "A", "level": 0}, 1),
        ("rank", {}, {"by": lambda x: x % 2}, 1),
        ("shift", {}, {"by": "A", "level": 0}, 1),
        ("shift", {}, {"by": lambda x: x % 2}, 1),
        ("agg", {"func": "sum"}, {"by": "A", "level": 0}, 1),
        ("agg", {"func": "sum"}, {"by": lambda x: x % 2}, 1),
        ("apply", {"func": lambda x: x.sum()}, {"by": "A", "level": 0}, 1),
        ("apply", {"func": lambda x: x.sum()}, {"by": lambda x: x % 2}, 1),
        ("apply", {"func": lambda x: x.sum()}, {"by": "A", "axis": 1}, 1),
        ("first", {}, {"by": "A", "level": 0}, 1),
        ("first", {}, {"by": lambda x: x % 2}, 1),
        ("last", {}, {"by": "A", "level": 0}, 1),
        ("last", {}, {"by": lambda x: x % 2}, 1),
        ("size", {}, {"by": "A", "level": 0}, 1),
        ("size", {}, {"by": lambda x: x % 2}, 1),
        ("size", {}, {"level": 0, "axis": 1}, 1),
        ("get_group", {"name": 1}, {"by": "A", "level": 0}, 1),
        ("get_group", {"name": 0}, {"by": lambda x: x % 2}, 1),
        ("nunique", {}, {"by": "A", "level": 0}, 1),
        ("nunique", {}, {"by": lambda x: x % 2}, 1),
        ("nunique", {}, {"by": "A", "axis": 1}, 1),
        ("any", {}, {"by": "A", "level": 0}, 1),
        ("any", {}, {"by": lambda x: x % 2}, 1),
        ("any", {}, {"by": "A", "axis": 1}, 1),
        ("all", {}, {"by": "A", "level": 0}, 1),
        ("all", {}, {"by": lambda x: x % 2}, 1),
        ("all", {}, {"by": "A", "axis": 1}, 1),
        ("value_counts", {}, {"by": "A", "level": 0}, 1),
    ],
)
def test_auto_switch_unsupported_dataframe_groupby_with_supported_method(
    method, method_kwargs, groupby_kwargs, query_count
):
    # Test unsupported GroupBy operations that should switch to Pandas backend.
    with SqlCounter(query_count=query_count):
        test_data = {"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]}

        df = pd.DataFrame(test_data).move_to("Snowflake")
        assert df.get_backend() == "Snowflake"

        _test_stay_cost(
            data_obj=df,
            api_cls_name="DataFrameGroupBy",
            method_name="__init__",
            args=groupby_kwargs,
            expected_cost=QCCoercionCost.COST_IMPOSSIBLE,
        )

        pandas_df = pd.DataFrame(test_data)
        _test_move_to_me_cost(
            pandas_qc=pandas_df._query_compiler,
            api_cls_name="DataFrameGroupBy",
            method_name="__init__",
            args=groupby_kwargs,
            expected_cost=QCCoercionCost.COST_IMPOSSIBLE,
        )

        groupby_obj = df.groupby(**groupby_kwargs)
        assert groupby_obj.get_backend() == "Pandas"

        _test_expected_backend(
            data_obj=groupby_obj,
            method_name=method,
            args=method_kwargs,
            expected_backend="Pandas",
            is_top_level=False,
        )

        eval_snowpark_pandas_result(
            groupby_obj,
            native_pd.DataFrame(test_data).groupby(**groupby_kwargs),
            lambda df: getattr(df, method)(**method_kwargs),
        )


@pytest.mark.parametrize(
    "method,method_kwargs, groupby_kwargs, query_count, test_index",
    [
        (
            "fillna",
            {"value": 0, "downcast": "infer"},
            {"axis": 0},
            1,
            None,
        ),
        ("first", {"min_count": 2}, {}, 1, None),
        ("last", {"min_count": 2}, {}, 1, None),
        (
            "shift",
            {"freq": "D"},
            {},
            3,
            native_pd.date_range("2023-01-01", periods=3, freq="D"),
        ),
    ],
)
def test_auto_switch_unsupported_dataframe_groupby_method(
    method,
    method_kwargs,
    groupby_kwargs,
    query_count,
    test_index,
):
    # Test unsupported GroupBy operations that should switch to Pandas backend.
    with SqlCounter(query_count=query_count):
        test_data = {"A": [1, 2, 3], "B": [4, 5, 6]}

        # Special handling for shift with freq parameter because it requires DatetimeIndex
        if test_index is not None:
            snowpark_index = pd.DatetimeIndex(test_index)
            df = pd.DataFrame(test_data, index=snowpark_index).move_to("Snowflake")
        else:
            df = pd.DataFrame(test_data).move_to("Snowflake")
        assert df.get_backend() == "Snowflake"

        groupby_obj = df.groupby("A", **groupby_kwargs)
        assert groupby_obj.get_backend() == "Snowflake"

        _test_stay_cost(
            data_obj=groupby_obj,
            api_cls_name="DataFrameGroupBy",
            method_name=method,
            args=method_kwargs,
            expected_cost=QCCoercionCost.COST_IMPOSSIBLE,
        )

        if test_index is not None:
            pandas_df = pd.DataFrame(test_data, index=pd.DatetimeIndex(test_index))
        else:
            pandas_df = pd.DataFrame(test_data)

        pandas_groupby_obj = pandas_df.groupby("A")
        _test_move_to_me_cost(
            pandas_qc=pandas_groupby_obj._query_compiler,
            api_cls_name="DataFrameGroupBy",
            method_name=method,
            args=method_kwargs,
            expected_cost=QCCoercionCost.COST_IMPOSSIBLE,
        )

        _test_expected_backend(
            data_obj=groupby_obj,
            method_name=method,
            args=method_kwargs,
            expected_backend="Pandas",
            is_top_level=False,
        )

        if test_index is not None:
            native_df = native_pd.DataFrame(
                test_data, index=native_pd.DatetimeIndex(test_index, freq=None)
            )
        else:
            native_df = native_pd.DataFrame(test_data)

        eval_snowpark_pandas_result(
            df,
            native_df,
            lambda df: getattr(df.groupby("A"), method)(**method_kwargs),
        )


@pytest.mark.parametrize(
    "method,method_kwargs",
    [
        ("fillna", {"value": 0}),
        ("first", {"min_count": -1}),
        ("last", {"min_count": -1}),
    ],
)
def test_auto_switch_supported_dataframe_groupby(method, method_kwargs):
    # Test supported GroupBy operations that should stay on Snowflake backend.
    test_data = {"A": [1, 2, 3, 1, 2], "B": [4, 5, 6, 7, 8]}

    with SqlCounter(query_count=1):
        df = pd.DataFrame(test_data).move_to("Snowflake")
        groupby_obj = df.groupby("A").move_to("Snowflake")
        assert groupby_obj.get_backend() == "Snowflake"

        _test_stay_cost(
            data_obj=groupby_obj,
            api_cls_name="DataFrameGroupBy",
            method_name=method,
            args=method_kwargs,
            expected_cost=QCCoercionCost.COST_ZERO,
        )

        _test_expected_backend(
            data_obj=groupby_obj,
            method_name=method,
            args=method_kwargs,
            expected_backend="Snowflake",
            is_top_level=False,
        )

        eval_snowpark_pandas_result(
            df,
            native_pd.DataFrame(test_data),
            lambda df: getattr(df.groupby("A"), method)(**method_kwargs),
        )


@pytest.mark.parametrize(
    "method,kwargs,api_cls_name",
    [
        ("skew", {"numeric_only": False}, "BasePandasDataset"),
        ("shift", {"suffix": "_suffix"}, "BasePandasDataset"),
        ("shift", {"periods": [1, 2]}, "BasePandasDataset"),
        ("fillna", {"value": 0, "limit": 1}, "Series"),
        ("fillna", {"downcast": "infer", "value": 0}, "Series"),
        ("sort_index", {"key": lambda x: x}, "BasePandasDataset"),
    ],
)
def test_auto_switch_unsupported_series(method, kwargs, api_cls_name):
    # Test unsupported Series operations that should switch to Pandas backend.
    test_data = [1, 2, 3, 4, 5, 6]

    with SqlCounter(query_count=1):
        series = pd.Series(test_data).move_to("Snowflake")
        assert series.get_backend() == "Snowflake"

        _test_stay_cost(
            data_obj=series,
            api_cls_name=api_cls_name,
            method_name=method,
            args=kwargs,
            expected_cost=QCCoercionCost.COST_IMPOSSIBLE,
        )

        pandas_series = pd.Series(test_data)
        _test_move_to_me_cost(
            pandas_qc=pandas_series._query_compiler,
            api_cls_name=api_cls_name,
            method_name=method,
            args=kwargs,
            expected_cost=QCCoercionCost.COST_IMPOSSIBLE,
        )

        eval_snowpark_pandas_result(
            series,
            native_pd.Series(test_data),
            lambda series: getattr(series, method)(**kwargs),
            comparator=np.testing.assert_allclose,
            test_attrs=False,
        )


@pytest.mark.parametrize(
    "method,kwargs,expected_reason",
    [
        (
            "get_dummies",
            {"dummy_na": True},
            "dummy_na = True is not supported",
        ),
        (
            "get_dummies",
            {"drop_first": True},
            "drop_first = True is not supported",
        ),
        (
            "melt",
            {"col_level": 0},
            "col_level argument is not yet supported",
        ),
        (
            "pivot_table",
            {"sort": False},
            "sort = False is not supported",
        ),
        (
            "pivot_table",
            {"index": ["A", 0], "columns": "B", "values": "B"},
            "index argument should be a string or a list of strings",
        ),
        (
            "pivot_table",
            {"index": "A", "columns": ["B", 0], "values": "B"},
            "columns argument should be a string or a list of strings",
        ),
        (
            "pivot_table",
            {"index": "A", "columns": "B", "values": ["B", 0]},
            "values argument should be a string or a list of strings",
        ),
        (
            "pivot_table",
            {"index": None, "columns": "A", "values": ["B"], "aggfunc": {"B": max}},
            "dictionary aggfunc with non-string aggregation functions is not yet supported for pivot_table when index is None",
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_error_handling_top_level_functions_when_auto_switch_disabled(
    method, kwargs, expected_reason
):
    # Test that unsupported top-level function args raise NotImplementedError when auto-switch is disabled.
    with config_context(AutoSwitchBackend=False):
        df = pd.DataFrame(
            {"A": ["x", "y", "z"], "B": [1, 2, 3], 0: [4, 5, 6], 1: [7, 8, 9]}
        ).move_to("Snowflake")

        with pytest.raises(
            NotImplementedError,
            match=re.escape(
                f"Snowpark pandas {method} does not yet support the parameter combination because {expected_reason}"
            ),
        ):
            getattr(pd, method)(df, **kwargs)


@pytest.mark.parametrize(
    "method,kwargs,expected_reason",
    [
        (
            "skew",
            {"axis": 1},
            "axis = 1 is not supported",
        ),
        (
            "skew",
            {"numeric_only": False},
            "numeric_only = False argument not supported for skew",
        ),
        (
            "cumsum",
            {"axis": 1},
            "axis = 1 is not supported",
        ),
        (
            "cummin",
            {"axis": 1},
            "axis = 1 is not supported",
        ),
        (
            "cummax",
            {"axis": 1},
            "axis = 1 is not supported",
        ),
        (
            "shift",
            {"suffix": "_suffix"},
            "the 'suffix' parameter is not yet supported",
        ),
        (
            "shift",
            {"periods": [1, 2]},
            "only int 'periods' is currently supported",
        ),
        (
            "sort_index",
            {"axis": 1},
            "axis = 1 is not supported",
        ),
        (
            "sort_index",
            {"key": lambda x: x},
            "the 'key' parameter is not yet supported",
        ),
        (
            "sort_values",
            {"by": "A", "axis": 1},
            "axis = 1 is not supported",
        ),
        (
            "apply",
            {"func": lambda x: x * 2, "result_type": "expand"},
            "the 'result_type' parameter is not yet supported",
        ),
        (
            "fillna",
            {"downcast": "infer", "value": 0},
            "the 'downcast' parameter is not yet supported",
        ),
        (
            "fillna",
            {"limit": 1, "value": 0},
            "the 'limit' parameter with 'value' parameter is not yet supported",
        ),
        (
            "dropna",
            {"axis": 1},
            "axis = 1 is not supported",
        ),
        (
            "corr",
            {"method": "kendall"},
            "method = 'kendall' is not supported. Snowpark pandas currently only supports method = 'pearson'.",
        ),
        (
            "corr",
            {"method": 123},
            "method parameter must be a string. Snowpark pandas currently only supports method = 'pearson'.",
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_error_handling_dataframe_when_auto_switch_disabled(
    method, kwargs, expected_reason
):
    # Test that unsupported DataFrame args raise NotImplementedError when auto-switch is disabled.
    with config_context(AutoSwitchBackend=False):
        df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]}).move_to("Snowflake")

        with pytest.raises(
            NotImplementedError,
            match=re.escape(
                f"Snowpark pandas {method} does not yet support the parameter combination because {expected_reason}"
            ),
        ):
            getattr(df, method)(**kwargs)


@pytest.mark.parametrize(
    "method,kwargs,expected_reason",
    [
        (
            "skew",
            {"numeric_only": False},
            "numeric_only = False argument not supported for skew",
        ),
        (
            "shift",
            {"suffix": "_suffix"},
            "the 'suffix' parameter is not yet supported",
        ),
        (
            "shift",
            {"periods": [1, 2]},
            "only int 'periods' is currently supported",
        ),
        (
            "fillna",
            {"downcast": "infer", "value": 0},
            "the 'downcast' parameter is not yet supported",
        ),
        (
            "fillna",
            {"limit": 1, "value": 0},
            "the 'limit' parameter with 'value' parameter is not yet supported",
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_error_handling_series_when_auto_switch_disabled(
    method, kwargs, expected_reason
):
    # Test that unsupported Series args raise NotImplementedError when auto-switch is disabled.
    with config_context(AutoSwitchBackend=False):
        series = pd.Series([1, 2, 3, 4, 5, 6]).move_to("Snowflake")

        with pytest.raises(
            NotImplementedError,
            match=re.escape(
                f"Snowpark pandas {method} does not yet support the parameter combination because {expected_reason}"
            ),
        ):
            getattr(series, method)(**kwargs)


@sql_count_checker(query_count=0)
def test_malformed_decorator_conditions():
    # Test that malformed conditions in decorator are caught during rule creation.

    # Test malformed condition with wrong tuple length
    with pytest.raises(
        ValueError, match="Invalid condition at index 0.*expected tuple of length 2"
    ):

        @register_query_compiler_method_not_implemented(
            api_cls_name="TestClass",
            method_name="test_method_single_item",
            unsupported_args=UnsupportedArgsRule(
                unsupported_conditions=[
                    ("single_item",),
                ]
            ),
        )
        def test_method_single_item(self):
            pass

    # Test malformed condition with non-tuple
    with pytest.raises(
        ValueError, match="Invalid condition at index 1.*expected tuple of length 2"
    ):

        @register_query_compiler_method_not_implemented(
            api_cls_name="TestClass",
            method_name="test_method_not_tuple",
            unsupported_args=UnsupportedArgsRule(
                unsupported_conditions=[
                    ("valid_param", "valid_value"),
                    "not_a_tuple",
                ]
            ),
        )
        def test_method_not_tuple(self):
            pass

    # Test malformed condition with invalid first element
    with pytest.raises(
        ValueError,
        match="Invalid condition at index 0.*first element must be callable or string",
    ):

        @register_query_compiler_method_not_implemented(
            api_cls_name="TestClass",
            method_name="test_method_none_condition",
            unsupported_args=UnsupportedArgsRule(
                unsupported_conditions=[
                    (None, "reason_for_none"),
                ]
            ),
        )
        def test_method_none_condition(self):
            pass

    # Test malformed condition with callable first element but non-string second element
    with pytest.raises(
        ValueError,
        match="Invalid condition at index 0.*when first element is callable.*second element must be a string",
    ):

        @register_query_compiler_method_not_implemented(
            api_cls_name="TestClass",
            method_name="test_method_callable_non_string_reason",
            unsupported_args=UnsupportedArgsRule(
                unsupported_conditions=[
                    (lambda args: True, 123),
                ]
            ),
        )
        def test_method_callable_non_string_reason(self):
            pass


@pytest.mark.parametrize(
    "method,kwargs,expected_reason",
    [
        (
            "fillna",
            {"value": 0, "downcast": "infer"},
            "Snowpark pandas fillna does not yet support the parameter combination because 'downcast' argument is not supported yet in Snowpark pandas",
        ),
        (
            "first",
            {"min_count": 2},
            "does not yet support min_count > 1",
        ),
        (
            "last",
            {"min_count": 2},
            "does not yet support min_count > 1",
        ),
        (
            "shift",
            {"freq": "D"},
            "Snowpark pandas shift does not yet support the parameter combination because 'freq' argument is not supported yet in Snowpark pandas.",
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_error_handling_unsupported_dataframe_groupby_method_when_auto_switch_disabled(
    method, kwargs, expected_reason
):
    # Test that unsupported DataFrame GroupBy args raise NotImplementedError when auto-switch is disabled.
    with config_context(AutoSwitchBackend=False):
        df = pd.DataFrame({"A": [1, 2, 3, 1, 2], "B": [4, 5, 6, 7, 8]}).move_to(
            "Snowflake"
        )

        with pytest.raises(
            NotImplementedError,
            match=re.escape(expected_reason),
        ):
            groupby_obj = df.groupby("A")
            getattr(groupby_obj, method)(**kwargs)


@pytest.mark.parametrize(
    "method, method_kwargs",
    [
        ("fillna", {"value": 0}),
        ("first", {}),
        ("last", {}),
        ("shift", {}),
        ("apply", {"func": lambda x: x.sum()}),
        ("size", {}),
        ("get_group", {"name": 1}),
        ("nunique", {}),
        ("any", {}),
        ("all", {}),
        ("cummin", {}),
        ("cumsum", {}),
        ("cummax", {}),
        ("cumcount", {}),
        ("rank", {}),
        ("value_counts", {}),
        ("pct_change", {}),
    ],
)
@sql_count_checker(query_count=0)
def test_error_handling_unsupported_dataframe_groupby_with_supported_method_when_auto_switch_disabled(
    method, method_kwargs
):
    # Test that unsupported DataFrame GroupBy args raise NotImplementedError when auto-switch is disabled.
    with config_context(AutoSwitchBackend=False):
        df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]}).move_to("Snowflake")

        with pytest.raises(
            NotImplementedError,
            match=re.escape(
                "does not yet support axis == 1, by != None and level != None, or by containing any non-pandas hashable labels."
            ),
        ):
            groupby_obj = df.groupby("A", level=0)
            getattr(groupby_obj, method)(**method_kwargs)
