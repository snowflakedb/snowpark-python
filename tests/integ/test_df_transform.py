#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import to_timestamp


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


def test_moving_agg(session):
    """Tests df.transform.moving_agg() happy path."""

    df = get_sample_dataframe(session)

    res = df.transform.moving_agg(
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


def test_moving_agg_custom_formatting(session):
    """Tests df.transform.moving_agg() with custom formatting of output columns."""

    df = get_sample_dataframe(session)

    def custom_formatter(input_col, agg, window):
        return f"{window}_{agg}_{input_col}"

    res = df.transform.moving_agg(
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


def test_moving_agg_invalid_inputs(session):
    """Tests df.transform.moving_agg() with invalid window sizes."""

    df = get_sample_dataframe(session)

    with pytest.raises(ValueError) as exc:
        df.transform.moving_agg(
            aggs={"SALESAMOUNT": ["AVG"]},
            window_sizes=[-1, 2, 3],
            order_by=["ORDERDATE"],
            group_by=["PRODUCTKEY"],
        ).collect()
    assert "window_sizes must be a list of integers > 0" in str(exc)

    with pytest.raises(ValueError) as exc:
        df.transform.moving_agg(
            aggs={"SALESAMOUNT": ["AVG"]},
            window_sizes=[0, 2, 3],
            order_by=["ORDERDATE"],
            group_by=["PRODUCTKEY"],
        ).collect()
    assert "window_sizes must be a list of integers > 0" in str(exc)

    with pytest.raises(ValueError) as exc:
        df.transform.moving_agg(
            aggs={"SALESAMOUNT": []},
            window_sizes=[0, 2, 3],
            order_by=["ORDERDATE"],
            group_by=["PRODUCTKEY"],
        ).collect()
    assert "non-empty lists of strings as values" in str(exc)

    with pytest.raises(SnowparkSQLException) as exc:
        df.transform.moving_agg(
            aggs={"SALESAMOUNT": ["INVALID_FUNC"]},
            window_sizes=[1],
            order_by=["ORDERDATE"],
            group_by=["PRODUCTKEY"],
        ).collect()
    assert "Sliding window frame unsupported for function" in str(exc)

    def bad_formatter(input_col, agg):
        return f"{agg}_{input_col}"

    with pytest.raises(TypeError) as exc:
        df.transform.moving_agg(
            aggs={"SALESAMOUNT": ["SUM"]},
            window_sizes=[1],
            order_by=["ORDERDATE"],
            group_by=["PRODUCTKEY"],
            col_formatter=bad_formatter,
        ).collect()
    assert "positional arguments but 3 were given" in str(exc)


def test_time_series_agg(session):
    """Tests time_series_agg_fixed function with various window sizes."""

    df = get_sample_dataframe(session)
    df = df.withColumn("ORDERDATE", to_timestamp(df["ORDERDATE"]))
    session.sql_simplifier_enabled = False

    def custom_formatter(input_col, agg, window):
        return f"{agg}_{input_col}_{window}"

    res = df.transform.time_series_agg(
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


def test_time_series_agg_month_sliding_window(session):
    """Tests time_series_agg_fixed function with various window sizes."""

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
    session.sql_simplifier_enabled = False

    def custom_formatter(input_col, agg, window):
        return f"{agg}_{input_col}_{window}"

    res = df.transform.time_series_agg(
        time_col="ORDERDATE",
        group_by=["PRODUCTKEY"],
        aggs={"SALESAMOUNT": ["SUM", "MAX"]},
        windows=["-2T"],
        sliding_interval="1T",
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
        "SUM_SALESAMOUNT_-2T": [100, 300, 600, 900, 150, 400, 750, 1050],
        "MAX_SALESAMOUNT_-2T": [100, 200, 300, 400, 150, 250, 350, 450],
    }
    expected_df = pd.DataFrame(expected_data)
    expected_df["ORDERDATE"] = pd.to_datetime(expected_df["ORDERDATE"])
    expected_df["SLIDING_POINT"] = pd.to_datetime(expected_df["SLIDING_POINT"])
    expected_df = expected_df.sort_values(by=["PRODUCTKEY", "ORDERDATE"])

    result_df = res.order_by("PRODUCTKEY", "ORDERDATE").to_pandas()
    result_df = result_df.sort_values(by=["PRODUCTKEY", "ORDERDATE"])

    assert_frame_equal(result_df, expected_df, check_dtype=False, atol=1e-1)
