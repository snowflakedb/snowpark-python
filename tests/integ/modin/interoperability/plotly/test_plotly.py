#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import plotly.express as px
import pytest
import pandas as native_pd

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker
from tests.integ.modin.utils import eval_snowpark_pandas_result

# Integration tests for plotly.express module (https://plotly.com/python-api-reference/plotly.express.html).
# To add tests for additional APIs,
#     - Call the method with Snowpark pandas and native pandas df input and get the JSON representation with
#     `to_plotly_json()`.
#     - Assert correctness of the plot produced using `assert_plotly_equal` function defined below.


def assert_plotly_equal(expect, got):
    # referenced from cudf plotly integration test
    # https://github.com/rapidsai/cudf/blob/main/python/cudf/cudf_pandas_tests/third_party_integration_tests/tests/test_plotly.py#L10

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


@pytest.fixture()
def test_dfs():
    nsamps = 50
    rng = np.random.default_rng(seed=42)
    data = {
        "x": rng.random(nsamps),
        "y": rng.random(nsamps),
        "category": rng.integers(0, 5, nsamps),
        "category2": rng.integers(0, 5, nsamps),
    }
    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)
    return snow_df, native_df


@sql_count_checker(query_count=1)
def test_scatter(test_dfs):
    eval_snowpark_pandas_result(
        *test_dfs,
        lambda df: px.scatter(df, x="x", y="y").to_plotly_json(),
        comparator=assert_plotly_equal
    )


@sql_count_checker(query_count=1)
def test_line(test_dfs):
    eval_snowpark_pandas_result(
        *test_dfs,
        lambda df: px.line(df, x="category", y="y").to_plotly_json(),
        comparator=assert_plotly_equal
    )


@sql_count_checker(query_count=1)
def test_area(test_dfs):
    eval_snowpark_pandas_result(
        *test_dfs,
        lambda df: px.area(df, x="category", y="y").to_plotly_json(),
        comparator=assert_plotly_equal
    )


@sql_count_checker(query_count=1)
def test_timeline():
    native_df = native_pd.DataFrame(
        [
            dict(Task="Job A", Start="2009-01-01", Finish="2009-02-28"),
            dict(Task="Job B", Start="2009-03-05", Finish="2009-04-15"),
            dict(Task="Job C", Start="2009-02-20", Finish="2009-05-30"),
        ]
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: px.timeline(
            df, x_start="Start", x_end="Finish", y="Task"
        ).to_plotly_json(),
        comparator=assert_plotly_equal,
    )


@sql_count_checker(query_count=1)
def test_violin(test_dfs):
    eval_snowpark_pandas_result(
        *test_dfs,
        lambda df: px.violin(df, y="y").to_plotly_json(),
        comparator=assert_plotly_equal
    )


@sql_count_checker(query_count=1)
def test_bar(test_dfs):
    eval_snowpark_pandas_result(
        *test_dfs,
        lambda df: px.bar(df, x="category", y="y").to_plotly_json(),
        comparator=assert_plotly_equal
    )


@sql_count_checker(query_count=1)
def test_histogram(test_dfs):
    eval_snowpark_pandas_result(
        *test_dfs,
        lambda df: px.histogram(df, x="category").to_plotly_json(),
        comparator=assert_plotly_equal
    )


@sql_count_checker(query_count=1)
def test_pie(test_dfs):
    eval_snowpark_pandas_result(
        *test_dfs,
        lambda df: px.pie(df, values="category", names="category2").to_plotly_json(),
        comparator=assert_plotly_equal
    )


@sql_count_checker(query_count=1)
def test_treemap(test_dfs):
    eval_snowpark_pandas_result(
        *test_dfs,
        lambda df: px.treemap(df, names="category", values="y").to_plotly_json(),
        comparator=assert_plotly_equal
    )


@sql_count_checker(query_count=1)
def test_sunburst(test_dfs):
    eval_snowpark_pandas_result(
        *test_dfs,
        lambda df: px.sunburst(df, names="category", values="y").to_plotly_json(),
        comparator=assert_plotly_equal
    )


@sql_count_checker(query_count=1)
def test_icicle(test_dfs):
    eval_snowpark_pandas_result(
        *test_dfs,
        lambda df: px.icicle(df, names="category", values="y").to_plotly_json(),
        comparator=assert_plotly_equal
    )


@sql_count_checker(query_count=1)
def test_scatter_matrix(test_dfs):
    eval_snowpark_pandas_result(
        *test_dfs,
        lambda df: px.scatter_matrix(df, dimensions=["category"]).to_plotly_json(),
        comparator=assert_plotly_equal
    )


@sql_count_checker(query_count=1)
def test_funnel(test_dfs):
    eval_snowpark_pandas_result(
        *test_dfs,
        lambda df: px.funnel(df, x="x", y="y").to_plotly_json(),
        comparator=assert_plotly_equal
    )


@sql_count_checker(query_count=1)
def test_density_heatmap(test_dfs):
    eval_snowpark_pandas_result(
        *test_dfs,
        lambda df: px.density_heatmap(df, x="x", y="y").to_plotly_json(),
        comparator=assert_plotly_equal
    )


@sql_count_checker(query_count=1)
def test_box(test_dfs):
    eval_snowpark_pandas_result(
        *test_dfs,
        lambda df: px.box(df, x="category", y="y").to_plotly_json(),
        comparator=assert_plotly_equal
    )


@sql_count_checker(query_count=3)
def test_imshow(test_dfs):
    eval_snowpark_pandas_result(
        *test_dfs,
        lambda df: px.imshow(df, x=df.columns, y=df.index).to_plotly_json(),
        comparator=assert_plotly_equal
    )
