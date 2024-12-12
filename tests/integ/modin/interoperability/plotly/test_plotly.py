#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import plotly.express as px
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker, SqlCounter

# Integration tests for plotly.express module (https://plotly.com/python-api-reference/plotly.express.html).
# To add tests for additional APIs,
#     - Call the API with Snowpark pandas and native pandas df input and get the JSON representation with
#     `to_plotly_json()`.
#     - Assert correctness of the plot produced using `assert_plotly_equal` function defined below.


def assert_plotly_equal(expect, got):
    assert type(expect) == type(got)
    if isinstance(expect, dict):
        assert expect.keys() == got.keys()
        for k in expect.keys():
            assert_plotly_equal(expect[k], got[k])
    elif isinstance(got, list):
        assert len(expect) == len(got)
        for i in range(len(expect)):
            assert_plotly_equal(expect[i], got[i])
    elif isinstance(expect, np.ndarray):
        if isinstance(expect[0], float):
            np.testing.assert_allclose(expect, got)
        else:
            assert (expect == got).all()
    else:
        assert expect == got


@pytest.fixture(scope="module")
def df():
    nsamps = 50
    rng = np.random.default_rng(seed=42)
    return pd.DataFrame(
        {
            "x": rng.random(nsamps),
            "y": rng.random(nsamps),
            "category": rng.integers(0, 5, nsamps),
            "category2": rng.integers(0, 5, nsamps),
        }
    )


@sql_count_checker(query_count=2)
def test_scatter(df):
    snow_res = px.scatter(df, x="x", y="y").to_plotly_json()
    native_res = px.scatter(df._to_pandas(), x="x", y="y").to_plotly_json()
    assert_plotly_equal(snow_res, native_res)


@sql_count_checker(query_count=2)
def test_line(df):
    snow_res = px.line(df, x="category", y="y").to_plotly_json()
    native_res = px.line(df._to_pandas(), x="category", y="y").to_plotly_json()
    assert_plotly_equal(snow_res, native_res)


@sql_count_checker(query_count=2)
def test_area(df):
    snow_res = px.area(df, x="category", y="y").to_plotly_json()
    native_res = px.area(df._to_pandas(), x="category", y="y").to_plotly_json()
    assert_plotly_equal(snow_res, native_res)


@sql_count_checker(query_count=2)
def test_timeline():
    df = pd.DataFrame(
        [
            dict(Task="Job A", Start="2009-01-01", Finish="2009-02-28"),
            dict(Task="Job B", Start="2009-03-05", Finish="2009-04-15"),
            dict(Task="Job C", Start="2009-02-20", Finish="2009-05-30"),
        ]
    )
    snow_res = px.timeline(
        df, x_start="Start", x_end="Finish", y="Task"
    ).to_plotly_json()
    native_res = px.timeline(
        df._to_pandas(), x_start="Start", x_end="Finish", y="Task"
    ).to_plotly_json()
    assert_plotly_equal(snow_res, native_res)


@sql_count_checker(query_count=2)
def test_violin(df):
    snow_res = px.violin(df, y="y").to_plotly_json()
    native_res = px.violin(df._to_pandas(), y="y").to_plotly_json()
    assert_plotly_equal(snow_res, native_res)


@sql_count_checker(query_count=2)
def test_bar(df):
    snow_res = px.bar(df, x="category", y="y").to_plotly_json()
    native_res = px.bar(df._to_pandas(), x="category", y="y").to_plotly_json()
    assert_plotly_equal(snow_res, native_res)


@sql_count_checker(query_count=2)
def test_histogram(df):
    snow_res = px.histogram(df, x="category").to_plotly_json()
    native_res = px.histogram(df._to_pandas(), x="category").to_plotly_json()
    assert_plotly_equal(snow_res, native_res)


@sql_count_checker(query_count=2)
def test_pie(df):
    snow_res = px.pie(df, values="category", names="category2").to_plotly_json()
    native_res = px.pie(
        df._to_pandas(), values="category", names="category2"
    ).to_plotly_json()
    assert_plotly_equal(snow_res, native_res)


@sql_count_checker(query_count=2)
def test_treemap(df):
    snow_res = px.treemap(df, names="category", values="y").to_plotly_json()
    native_res = px.treemap(
        df._to_pandas(), names="category", values="y"
    ).to_plotly_json()
    assert_plotly_equal(snow_res, native_res)


@sql_count_checker(query_count=2)
def test_sunburst(df):
    snow_res = px.sunburst(df, names="category", values="y").to_plotly_json()
    native_res = px.sunburst(
        df._to_pandas(), names="category", values="y"
    ).to_plotly_json()
    assert_plotly_equal(snow_res, native_res)


@sql_count_checker(query_count=2)
def test_icicle(df):
    snow_res = px.icicle(df, names="category", values="y").to_plotly_json()
    native_res = px.icicle(
        df._to_pandas(), names="category", values="y"
    ).to_plotly_json()
    assert_plotly_equal(snow_res, native_res)


@sql_count_checker(query_count=2)
def test_scatter_matrix(df):
    snow_res = px.scatter_matrix(df, dimensions=["category"]).to_plotly_json()
    native_res = px.scatter_matrix(
        df._to_pandas(), dimensions=["category"]
    ).to_plotly_json()
    assert_plotly_equal(snow_res, native_res)


@sql_count_checker(query_count=2)
def test_funnel(df):
    snow_res = px.funnel(df, x="x", y="y").to_plotly_json()
    native_res = px.funnel(df._to_pandas(), x="x", y="y").to_plotly_json()
    assert_plotly_equal(snow_res, native_res)


@sql_count_checker(query_count=2)
def test_density_heatmap(df):
    snow_res = px.density_heatmap(df, x="x", y="y").to_plotly_json()
    native_res = px.density_heatmap(df._to_pandas(), x="x", y="y").to_plotly_json()
    assert_plotly_equal(snow_res, native_res)


@sql_count_checker(query_count=2)
def test_box(df):
    snow_res = px.box(df, x="category", y="y").to_plotly_json()
    native_res = px.box(df._to_pandas(), x="category", y="y").to_plotly_json()
    assert_plotly_equal(snow_res, native_res)


def test_imshow(df):
    df = pd.DataFrame([[1, 3], [4, 5], [7, 2]], columns=["a", "b"])
    with SqlCounter(query_count=4):
        snow_res = px.imshow(df, x=df.columns, y=df.index).to_plotly_json()
    native_res = px.imshow(
        df._to_pandas(), x=df._to_pandas().columns, y=df._to_pandas().index
    ).to_plotly_json()
    assert_plotly_equal(snow_res, native_res)
