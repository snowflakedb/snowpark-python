#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.types import IntegerType


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


def get_sample_categorical_dataframe(session):
    data = [
        ["Sunny", "Category1", 100, 10],
        ["Rainy", "Category2", 200, 40],
        ["Cloudy", "Category3", 300, 35],
        ["Sunny", "Category1", 400, 20],
        ["Rainy", "Category2", 500, 60],
        ["Cloudy", "Category1", 600, 30],
    ]
    return session.create_dataframe(data).to_df(
        "WEATHER_DESCRIPTION", "CATEGORY", "VALUE", "TARGET"
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


def test_label_encoding(session):
    """Tests df.transform.encode() for label encoding."""

    df = get_sample_categorical_dataframe(session)

    encoded_df = df.transform.encode(
        cols=["WEATHER_DESCRIPTION"], encoding_types=["label"]
    )

    expected_data = {
        "WEATHER_DESCRIPTION": ["Sunny", "Rainy", "Cloudy", "Sunny", "Rainy", "Cloudy"],
        "WEATHER_DESCRIPTION_LABEL": [
            3,
            2,
            1,
            3,
            2,
            1,
        ],  # Assuming labels are assigned in the order they appear
    }
    expected_df = pd.DataFrame(expected_data)
    result_df = encoded_df.select(
        "WEATHER_DESCRIPTION", "WEATHER_DESCRIPTION_LABEL"
    ).to_pandas()
    assert_frame_equal(result_df, expected_df, check_dtype=True)


def test_one_hot_encoding(session):
    """Tests df.transform.encode() for one-hot encoding."""

    df = get_sample_categorical_dataframe(session)

    def custom_formatter(input_col, encoding):
        return f"{input_col}"

    # Apply one-hot encoding
    encoded_df = df.transform.encode(
        cols=["WEATHER_DESCRIPTION"],
        encoding_types=["one_hot"],
        col_formatter=custom_formatter,
    )

    encoded_df.collect()
    encoded_df.show()
    # Define expected results for one-hot encoding
    expected_data = {
        "WEATHER_DESCRIPTION": ["Sunny", "Rainy", "Cloudy", "Sunny", "Rainy", "Cloudy"],
        "WEATHER_DESCRIPTION_SUNNY": [1, 0, 0, 1, 0, 0],
        "WEATHER_DESCRIPTION_RAINY": [0, 1, 0, 0, 1, 0],
        "WEATHER_DESCRIPTION_CLOUDY": [0, 0, 1, 0, 0, 1],
    }
    expected_df = pd.DataFrame(expected_data)
    result_df = encoded_df.select(
        "WEATHER_DESCRIPTION",
        "WEATHER_DESCRIPTION_SUNNY",
        "WEATHER_DESCRIPTION_RAINY",
        "WEATHER_DESCRIPTION_CLOUDY",
    ).to_pandas()
    assert_frame_equal(result_df, expected_df, check_dtype=False)


def test_target_encoding(session):
    """Tests df.transform.encode() for target encoding."""

    df = get_sample_categorical_dataframe(session)

    # Apply target encoding
    encoded_df = df.transform.encode(
        cols=["CATEGORY"], encoding_types=["target"], targets=["TARGET"]
    )

    encoded_df.collect()
    encoded_df.show()

    expected_data = {
        "CATEGORY": [
            "Category1",
            "Category2",
            "Category3",
            "Category1",
            "Category2",
            "Category1",
        ],
        "TARGET": [10, 40, 35, 20, 60, 30],
        "CATEGORY_TARGET": [20, 50, 35, 20, 50, 20],
    }
    expected_df = pd.DataFrame(expected_data)

    modified_df = encoded_df.withColumn(
        "CATEGORY_TARGET", encoded_df["CATEGORY_TARGET"].cast(IntegerType())
    )
    result_df = modified_df.select("CATEGORY", "TARGET", "CATEGORY_TARGET").to_pandas()

    # Compare the result with the expected dataframe
    assert_frame_equal(
        result_df, expected_df, check_dtype=False, check_exact=False, atol=1
    )


def test_ecoding_invalid_inputs(session):
    """Tests df.transform.encode() for with invalid inputs."""

    df = get_sample_categorical_dataframe(session)

    with pytest.raises(ValueError) as exc:
        df.transform.encode(
            cols=["WEATHER_DESCRIPTION"], encoding_types=["label", "label"]
        )
    assert "Length of cols, encoding_types must be equal" in str(exc)

    with pytest.raises(ValueError) as exc:
        df.transform.encode(cols=[], encoding_types=["label"])
    assert "cols must not be empty" in str(exc)
