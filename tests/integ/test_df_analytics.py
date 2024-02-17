#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

try:
    import pandas as pd
    from pandas.testing import assert_frame_equal

    is_pandas_available = True
except ImportError:
    is_pandas_available = False

import pytest

from snowflake.snowpark.dataframe_analytics_functions import DataFrameAnalyticsFunctions
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import col, to_timestamp


def get_sample_dataframe(session):
    data = [
        ["2023-01-01", 101, 200],
        ["2023-01-02", 101, 100],
        ["2023-01-03", 101, 300],
        ["2023-01-04", 102, 250],
    ]
    return session.create_dataframe(data).to_df(
        "ORDERDATE", "PRODUCTKEY", "SALESAMOUNT"
    )


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
def test_moving_agg(session):
    """Tests df.analytics.moving_agg() happy path."""

    df = get_sample_dataframe(session)

    res = df.analytics.moving_agg(
        aggs={"SALESAMOUNT": ["SUM", "AVG"]},
        window_sizes=[2, 3],
        order_by=["ORDERDATE"],
        group_by=["PRODUCTKEY"],
    )

    expected_data = {
        "ORDERDATE": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"],
        "PRODUCTKEY": [101, 101, 101, 102],
        "SALESAMOUNT": [200, 100, 300, 250],
        "SALESAMOUNT_SUM_2": [200, 300, 400, 250],
        "SALESAMOUNT_AVG_2": [200.0, 150.0, 200.0, 250.0],
        "SALESAMOUNT_SUM_3": [200, 300, 600, 250],
        "SALESAMOUNT_AVG_3": [200.0, 150.0, 200.0, 250.0],
    }
    expected_df = pd.DataFrame(expected_data)
    assert_frame_equal(
        res.order_by("ORDERDATE").to_pandas(), expected_df, check_dtype=False, atol=1e-1
    )


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
def test_moving_agg_custom_formatting(session):
    """Tests df.analytics.moving_agg() with custom formatting of output columns."""

    df = get_sample_dataframe(session)

    def custom_formatter(input_col, agg, window):
        return f"{window}_{agg}_{input_col}"

    res = df.analytics.moving_agg(
        aggs={"SALESAMOUNT": ["SUM", "AVG"]},
        window_sizes=[2, 3],
        order_by=["ORDERDATE"],
        group_by=["PRODUCTKEY"],
        col_formatter=custom_formatter,
    )

    expected_data = {
        "ORDERDATE": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"],
        "PRODUCTKEY": [101, 101, 101, 102],
        "SALESAMOUNT": [200, 100, 300, 250],
        "2_SUM_SALESAMOUNT": [200, 300, 400, 250],
        "2_AVG_SALESAMOUNT": [200.0, 150.0, 200.0, 250.0],
        "3_SUM_SALESAMOUNT": [200, 300, 600, 250],
        "3_AVG_SALESAMOUNT": [200.0, 150.0, 200.0, 250.0],
    }
    expected_df = pd.DataFrame(expected_data)
    assert_frame_equal(
        res.order_by("ORDERDATE").to_pandas(), expected_df, check_dtype=False, atol=1e-1
    )

    # With default formatter
    res = df.analytics.moving_agg(
        aggs={"SALESAMOUNT": ["SUM", "AVG"]},
        window_sizes=[2, 3],
        order_by=["ORDERDATE"],
        group_by=["PRODUCTKEY"],
    )

    expected_data = {
        "ORDERDATE": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"],
        "PRODUCTKEY": [101, 101, 101, 102],
        "SALESAMOUNT": [200, 100, 300, 250],
        "SALESAMOUNT_SUM_2": [200, 300, 400, 250],
        "SALESAMOUNT_AVG_2": [200.0, 150.0, 200.0, 250.0],
        "SALESAMOUNT_SUM_3": [200, 300, 600, 250],
        "SALESAMOUNT_AVG_3": [200.0, 150.0, 200.0, 250.0],
    }

    expected_df = pd.DataFrame(expected_data)
    assert_frame_equal(
        res.order_by("ORDERDATE").to_pandas(), expected_df, check_dtype=False, atol=1e-1
    )


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
def test_moving_agg_invalid_inputs(session):
    """Tests df.analytics.moving_agg() with invalid window sizes."""

    df = get_sample_dataframe(session)

    with pytest.raises(TypeError) as exc:
        df.analytics.moving_agg(
            aggs=["AVG"],
            window_sizes=[1, 2, 3],
            order_by=["ORDERDATE"],
            group_by=["PRODUCTKEY"],
        ).collect()
    assert "aggs must be a dictionary" in str(exc)

    with pytest.raises(ValueError) as exc:
        df.analytics.moving_agg(
            aggs={},
            window_sizes=[1, 2, 3],
            order_by=["ORDERDATE"],
            group_by=["PRODUCTKEY"],
        ).collect()
    assert "aggs must not be empty" in str(exc)

    with pytest.raises(ValueError) as exc:
        df.analytics.moving_agg(
            aggs={"SALESAMOUNT": []},
            window_sizes=[1, 2, 3],
            order_by=["ORDERDATE"],
            group_by=["PRODUCTKEY"],
        ).collect()
    assert "non-empty lists of strings as values" in str(exc)

    with pytest.raises(TypeError) as exc:
        df.analytics.moving_agg(
            aggs={"SALESAMOUNT": ["AVG"]},
            window_sizes=[1, 2, 3],
            order_by="ORDERDATE",
            group_by=["PRODUCTKEY"],
        ).collect()
    assert "order_by must be a list" in str(exc)

    with pytest.raises(ValueError) as exc:
        df.analytics.moving_agg(
            aggs={"SALESAMOUNT": ["AVG"]},
            window_sizes=[1, 2, 3],
            order_by=[],
            group_by=["PRODUCTKEY"],
        ).collect()
    assert "order_by must not be empty" in str(exc)

    with pytest.raises(ValueError) as exc:
        df.analytics.moving_agg(
            aggs={"SALESAMOUNT": ["AVG"]},
            window_sizes=[1, 2, 3],
            order_by=[1],
            group_by=["PRODUCTKEY"],
        ).collect()
    assert "order_by must be a list of strings" in str(exc)

    with pytest.raises(ValueError) as exc:
        df.analytics.moving_agg(
            aggs={"SALESAMOUNT": ["AVG"]},
            window_sizes=[-1, 2, 3],
            order_by=["ORDERDATE"],
            group_by=["PRODUCTKEY"],
        ).collect()
    assert "window_sizes must be a list of integers > 0" in str(exc)

    with pytest.raises(ValueError) as exc:
        df.analytics.moving_agg(
            aggs={"SALESAMOUNT": ["AVG"]},
            window_sizes=[0, 2, 3],
            order_by=["ORDERDATE"],
            group_by=["PRODUCTKEY"],
        ).collect()
    assert "window_sizes must be a list of integers > 0" in str(exc)

    with pytest.raises(TypeError) as exc:
        df.analytics.moving_agg(
            aggs={"SALESAMOUNT": ["AVG"]},
            window_sizes=0,
            order_by=["ORDERDATE"],
            group_by=["PRODUCTKEY"],
        ).collect()
    assert "window_sizes must be a list" in str(exc)

    with pytest.raises(ValueError) as exc:
        df.analytics.moving_agg(
            aggs={"SALESAMOUNT": ["AVG"]},
            window_sizes=[],
            order_by=["ORDERDATE"],
            group_by=["PRODUCTKEY"],
        ).collect()
    assert "window_sizes must not be empty" in str(exc)

    with pytest.raises(SnowparkSQLException) as exc:
        df.analytics.moving_agg(
            aggs={"SALESAMOUNT": ["INVALID_FUNC"]},
            window_sizes=[1],
            order_by=["ORDERDATE"],
            group_by=["PRODUCTKEY"],
        ).collect()
    assert "Sliding window frame unsupported for function" in str(exc)

    def bad_formatter(input_col, agg):
        return f"{agg}_{input_col}"

    with pytest.raises(TypeError) as exc:
        df.analytics.moving_agg(
            aggs={"SALESAMOUNT": ["SUM"]},
            window_sizes=[1],
            order_by=["ORDERDATE"],
            group_by=["PRODUCTKEY"],
            col_formatter=bad_formatter,
        ).collect()
    assert "positional arguments but 3 were given" in str(exc)

    with pytest.raises(TypeError) as exc:
        df.analytics.moving_agg(
            aggs={"SALESAMOUNT": ["SUM"]},
            window_sizes=[1],
            order_by=["ORDERDATE"],
            group_by=["PRODUCTKEY"],
            col_formatter="bad_formatter",
        ).collect()
    assert "formatter must be a callable function" in str(exc)


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
def test_cumulative_agg_forward_direction(session):
    """Tests df.analytics.cumulative_agg() with forward direction for cumulative calculations."""

    df = get_sample_dataframe(session)

    def custom_formatter(input_col, agg):
        return f"{agg}_{input_col}"

    res = df.analytics.cumulative_agg(
        aggs={"SALESAMOUNT": ["SUM", "MIN", "MAX"]},
        group_by=["PRODUCTKEY"],
        order_by=["ORDERDATE"],
        is_forward=True,
        col_formatter=custom_formatter,
    )

    # Define expected results
    expected_data = {
        "ORDERDATE": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"],
        "PRODUCTKEY": [101, 101, 101, 102],
        "SALESAMOUNT": [200, 100, 300, 250],
        "SUM_SALESAMOUNT": [600, 400, 300, 250],
        "MIN_SALESAMOUNT": [100, 100, 300, 250],
        "MAX_SALESAMOUNT": [300, 300, 300, 250],
    }
    expected_df = pd.DataFrame(expected_data)

    assert_frame_equal(
        res.order_by("ORDERDATE").to_pandas(),
        expected_df,
        check_dtype=False,
        atol=1e-1,
    )


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
def test_cumulative_agg_backward_direction(session):
    """Tests df.analytics.cumulative_agg() with backward direction for cumulative calculations."""

    df = get_sample_dataframe(session)

    def custom_formatter(input_col, agg):
        return f"{agg}_{input_col}"

    res = df.analytics.cumulative_agg(
        aggs={"SALESAMOUNT": ["SUM", "MIN", "MAX"]},
        group_by=["PRODUCTKEY"],
        order_by=["ORDERDATE"],
        is_forward=False,
        col_formatter=custom_formatter,
    )

    # Define expected results for backward direction
    expected_data = {
        "ORDERDATE": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"],
        "PRODUCTKEY": [101, 101, 101, 102],
        "SALESAMOUNT": [200, 100, 300, 250],
        "SUM_SALESAMOUNT": [200, 300, 600, 250],
        "MIN_SALESAMOUNT": [200, 100, 100, 250],
        "MAX_SALESAMOUNT": [200, 200, 300, 250],
    }
    expected_df = pd.DataFrame(expected_data)

    assert_frame_equal(
        res.order_by("ORDERDATE").to_pandas(),
        expected_df,
        check_dtype=False,
        atol=1e-1,
    )


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
def test_compute_lead(session):
    """Tests df.analytics.compute_lead() happy path."""

    df = get_sample_dataframe(session)

    def custom_col_formatter(input_col, op, lead):
        return f"{op}_{input_col}_{lead}"

    res = df.analytics.compute_lead(
        cols=["SALESAMOUNT"],
        leads=[1, 2],
        order_by=["ORDERDATE"],
        group_by=["PRODUCTKEY"],
        col_formatter=custom_col_formatter,
    )

    res = res.withColumn("LEAD_SALESAMOUNT_1", col("LEAD_SALESAMOUNT_1").cast("float"))
    res = res.withColumn("LEAD_SALESAMOUNT_2", col("LEAD_SALESAMOUNT_2").cast("float"))

    expected_data = {
        "ORDERDATE": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"],
        "PRODUCTKEY": [101, 101, 101, 102],
        "SALESAMOUNT": [200, 100, 300, 250],
        "LEAD_SALESAMOUNT_1": [100, 300, None, None],
        "LEAD_SALESAMOUNT_2": [300, None, None, None],
    }
    expected_df = pd.DataFrame(expected_data)

    assert_frame_equal(
        res.order_by("ORDERDATE").to_pandas(), expected_df, check_dtype=False, atol=1e-1
    )


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
def test_compute_lag(session):
    """Tests df.analytics.compute_lag() happy path."""

    df = get_sample_dataframe(session)

    def custom_col_formatter(input_col, op, lead):
        return f"{op}_{input_col}_{lead}"

    res = df.analytics.compute_lag(
        cols=["SALESAMOUNT"],
        lags=[1, 2],
        order_by=["ORDERDATE"],
        group_by=["PRODUCTKEY"],
        col_formatter=custom_col_formatter,
    )

    res = res.withColumn("LAG_SALESAMOUNT_1", col("LAG_SALESAMOUNT_1").cast("float"))
    res = res.withColumn("LAG_SALESAMOUNT_2", col("LAG_SALESAMOUNT_2").cast("float"))

    expected_data = {
        "ORDERDATE": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"],
        "PRODUCTKEY": [101, 101, 101, 102],
        "SALESAMOUNT": [200, 100, 300, 250],
        "LAG_SALESAMOUNT_1": [None, 200, 100, None],
        "LAG_SALESAMOUNT_2": [None, None, 200, None],
    }
    expected_df = pd.DataFrame(expected_data)

    assert_frame_equal(
        res.order_by("ORDERDATE").to_pandas(), expected_df, check_dtype=False, atol=1e-1
    )


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
def test_lead_lag_invalid_inputs(session):
    """Tests df.analytics.compute_lag() and df.analytics.compute_lead() with invalid_inputs."""

    df = get_sample_dataframe(session)

    with pytest.raises(ValueError) as exc:
        df.analytics.compute_lead(
            cols=["SALESAMOUNT"],
            leads=[-1, -2],
            order_by=["ORDERDATE"],
            group_by=["PRODUCTKEY"],
        ).collect()
    assert "leads must be a list of integers > 0" in str(exc)

    with pytest.raises(ValueError) as exc:
        df.analytics.compute_lead(
            cols=["SALESAMOUNT"],
            leads=[0, 2],
            order_by=["ORDERDATE"],
            group_by=["PRODUCTKEY"],
        ).collect()
    assert "leads must be a list of integers > 0" in str(exc)

    with pytest.raises(ValueError) as exc:
        df.analytics.compute_lag(
            cols=["SALESAMOUNT"],
            lags=[-1, -2],
            order_by=["ORDERDATE"],
            group_by=["PRODUCTKEY"],
        ).collect()
    assert "lags must be a list of integers > 0" in str(exc)


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
def test_time_series_agg(session):
    """Tests time_series_agg_fixed function with various window sizes."""

    df = get_sample_dataframe(session)
    df = df.withColumn("ORDERDATE", to_timestamp(df["ORDERDATE"]))

    def custom_formatter(input_col, agg, window):
        return f"{agg}_{input_col}_{window}"

    res = df.analytics.time_series_agg(
        time_col="ORDERDATE",
        group_by=["PRODUCTKEY"],
        aggs={"SALESAMOUNT": ["SUM", "MAX"]},
        windows=["1D", "-1D", "2D", "-2D"],
        sliding_interval="12H",
        col_formatter=custom_formatter,
    )

    # Define the expected data
    expected_data = {
        "PRODUCTKEY": [101, 101, 101, 102],
        "SLIDING_POINT": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"],
        "SALESAMOUNT": [200, 100, 300, 250],
        "ORDERDATE": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"],
        "SUM_SALESAMOUNT_1D": [300, 400, 300, 250],
        "MAX_SALESAMOUNT_1D": [200, 300, 300, 250],
        "SUM_SALESAMOUNT_-1D": [200, 300, 400, 250],
        "MAX_SALESAMOUNT_-1D": [200, 200, 300, 250],
        "SUM_SALESAMOUNT_2D": [600, 400, 300, 250],
        "MAX_SALESAMOUNT_2D": [300, 300, 300, 250],
        "SUM_SALESAMOUNT_-2D": [200, 300, 600, 250],
        "MAX_SALESAMOUNT_-2D": [200, 200, 300, 250],
    }
    expected_df = pd.DataFrame(expected_data)

    expected_df["ORDERDATE"] = pd.to_datetime(expected_df["ORDERDATE"])
    expected_df["SLIDING_POINT"] = pd.to_datetime(expected_df["SLIDING_POINT"])

    # Compare the result to the expected DataFrame
    assert_frame_equal(
        res.order_by("ORDERDATE").to_pandas(), expected_df, check_dtype=False, atol=1e-1
    )


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
def test_time_series_agg_month_sliding_window(session):
    """Tests time_series_agg_fixed function with month window sizes."""

    data = [
        ["2023-01-15", 101, 100],
        ["2023-02-15", 101, 200],
        ["2023-03-15", 101, 300],
        ["2023-04-15", 101, 400],
        ["2023-01-20", 102, 150],
        ["2023-02-20", 102, 250],
        ["2023-03-20", 102, 350],
        ["2023-04-20", 102, 450],
    ]
    df = session.create_dataframe(data).to_df("ORDERDATE", "PRODUCTKEY", "SALESAMOUNT")

    df = df.withColumn("ORDERDATE", to_timestamp(df["ORDERDATE"]))

    def custom_formatter(input_col, agg, window):
        return f"{agg}_{input_col}_{window}"

    res = df.analytics.time_series_agg(
        time_col="ORDERDATE",
        group_by=["PRODUCTKEY"],
        aggs={"SALESAMOUNT": ["SUM", "MAX"]},
        windows=["-2mm"],
        sliding_interval="1mm",
        col_formatter=custom_formatter,
    )

    expected_data = {
        "PRODUCTKEY": [101, 101, 101, 101, 102, 102, 102, 102],
        "SLIDING_POINT": [
            "2023-01-01",
            "2023-02-01",
            "2023-03-01",
            "2023-04-01",
            "2023-02-01",
            "2023-03-01",
            "2023-04-01",
            "2023-05-01",
        ],
        "SALESAMOUNT": [100, 200, 300, 400, 150, 250, 350, 450],
        "ORDERDATE": [
            "2023-01-15",
            "2023-02-15",
            "2023-03-15",
            "2023-04-15",
            "2023-01-20",
            "2023-02-20",
            "2023-03-20",
            "2023-04-20",
        ],
        "SUM_SALESAMOUNT_-2mm": [100, 300, 600, 900, 150, 400, 750, 1050],
        "MAX_SALESAMOUNT_-2mm": [100, 200, 300, 400, 150, 250, 350, 450],
    }
    expected_df = pd.DataFrame(expected_data)
    expected_df["ORDERDATE"] = pd.to_datetime(expected_df["ORDERDATE"])
    expected_df["SLIDING_POINT"] = pd.to_datetime(expected_df["SLIDING_POINT"])
    expected_df = expected_df.sort_values(by=["PRODUCTKEY", "ORDERDATE"])

    result_df = res.order_by("PRODUCTKEY", "ORDERDATE").to_pandas()
    result_df = result_df.sort_values(by=["PRODUCTKEY", "ORDERDATE"])

    assert_frame_equal(result_df, expected_df, check_dtype=False, atol=1e-1)


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
def test_time_series_agg_year_sliding_window(session):
    """Tests time_series_agg_fixed function with year window sizes."""

    data = [
        ["2021-01-15", 101, 100],
        ["2022-01-15", 101, 200],
        ["2023-01-15", 101, 300],
        ["2024-01-15", 101, 400],
        ["2021-01-20", 102, 150],
        ["2022-01-20", 102, 250],
        ["2023-01-20", 102, 350],
        ["2024-01-20", 102, 450],
    ]
    df = session.create_dataframe(data).to_df("ORDERDATE", "PRODUCTKEY", "SALESAMOUNT")
    df = df.withColumn("ORDERDATE", to_timestamp(df["ORDERDATE"]))

    def custom_formatter(input_col, agg, window):
        return f"{agg}_{input_col}_{window}"

    res = df.analytics.time_series_agg(
        time_col="ORDERDATE",
        group_by=["PRODUCTKEY"],
        aggs={"SALESAMOUNT": ["SUM", "MAX"]},
        windows=["-1Y"],
        sliding_interval="1Y",
        col_formatter=custom_formatter,
    )

    # Calculated expected data for 2Y window with 1Y sliding interval
    expected_data = {
        "PRODUCTKEY": [101, 101, 101, 101, 102, 102, 102, 102],
        "SLIDING_POINT": [
            "2021-01-01",
            "2022-01-01",
            "2023-01-01",
            "2024-01-01",
            "2021-01-01",
            "2022-01-01",
            "2023-01-01",
            "2024-01-01",
        ],
        "SALESAMOUNT": [100, 200, 300, 400, 150, 250, 350, 450],
        "ORDERDATE": [
            "2021-01-15",
            "2022-01-15",
            "2023-01-15",
            "2024-01-15",
            "2021-01-20",
            "2022-01-20",
            "2023-01-20",
            "2024-01-20",
        ],
        "SUM_SALESAMOUNT_-1Y": [100, 300, 500, 700, 150, 400, 600, 800],
        "MAX_SALESAMOUNT_-1Y": [100, 200, 300, 400, 150, 250, 350, 450],
    }
    expected_df = pd.DataFrame(expected_data)
    expected_df["ORDERDATE"] = pd.to_datetime(expected_df["ORDERDATE"])
    expected_df["SLIDING_POINT"] = pd.to_datetime(expected_df["SLIDING_POINT"])
    expected_df = expected_df.sort_values(by=["PRODUCTKEY", "ORDERDATE"])

    result_df = res.order_by("PRODUCTKEY", "ORDERDATE").to_pandas()
    result_df = result_df.sort_values(by=["PRODUCTKEY", "ORDERDATE"])

    assert_frame_equal(result_df, expected_df, check_dtype=False, atol=1e-1)


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
def test_time_series_agg_invalid_inputs(session):
    """Tests time_series_agg function with invalid inputs."""

    df = get_sample_dataframe(session)

    # Test with invalid time_col type
    with pytest.raises(ValueError) as exc:
        df.analytics.time_series_agg(
            time_col=123,  # Invalid type
            group_by=["PRODUCTKEY"],
            aggs={"SALESAMOUNT": ["SUM"]},
            windows=["7D"],
            sliding_interval="1D",
        ).collect()
    assert "time_col must be a string" in str(exc)

    # Test with empty windows list
    with pytest.raises(ValueError) as exc:
        df.analytics.time_series_agg(
            time_col="ORDERDATE",
            group_by=["PRODUCTKEY"],
            aggs={"SALESAMOUNT": ["SUM"]},
            windows=[],  # Empty list
            sliding_interval="1D",
        ).collect()
    assert "windows must not be empty" in str(exc)

    # Test with invalid window format
    with pytest.raises(ValueError) as exc:
        df.analytics.time_series_agg(
            time_col="ORDERDATE",
            group_by=["PRODUCTKEY"],
            aggs={"SALESAMOUNT": ["SUM"]},
            windows=["Invalid"],
            sliding_interval="1D",
        ).collect()
    assert "invalid literal for int() with base 10" in str(exc)

    # Test with invalid window format
    with pytest.raises(ValueError) as exc:
        df.analytics.time_series_agg(
            time_col="ORDERDATE",
            group_by=["PRODUCTKEY"],
            aggs={"SALESAMOUNT": ["SUM"]},
            windows=["2k"],
            sliding_interval="1D",
        ).collect()
    assert "Unsupported unit" in str(exc)

    # Test with invalid sliding_interval format
    with pytest.raises(ValueError) as exc:
        df.analytics.time_series_agg(
            time_col="ORDERDATE",
            group_by=["PRODUCTKEY"],
            aggs={"SALESAMOUNT": ["SUM"]},
            windows=["7D"],
            sliding_interval="invalid",  # Invalid format
        ).collect()
    assert "invalid literal for int() with base 10" in str(exc)


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
def test_parse_time_string(session):
    daf = DataFrameAnalyticsFunctions(pd.DataFrame())
    assert daf._parse_time_string("10d") == (10, "d")
    assert daf._parse_time_string("-5h") == (-5, "h")
    assert daf._parse_time_string("-6mm") == (-6, "mm")
    assert daf._parse_time_string("-6m") == (-6, "m")
