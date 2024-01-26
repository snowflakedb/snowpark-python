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

from snowflake.snowpark.exceptions import SnowparkSQLException


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
    """Tests df.transform.cumulative_agg() with forward direction for cumulative calculations."""

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
    """Tests df.transform.cumulative_agg() with backward direction for cumulative calculations."""

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
