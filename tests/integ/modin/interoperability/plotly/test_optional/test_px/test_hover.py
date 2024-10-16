#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from collections import OrderedDict  # an OrderedDict is needed for Python 2

import modin.pandas as pd
import numpy as np
import plotly.express as px

# import pandas as pd
import pytest

import snowflake.snowpark.modin.plugin


def test_skip_hover():
    df = px.data.iris()
    df = pd.DataFrame(df)
    fig = px.scatter(
        df,
        x="petal_length",
        y="petal_width",
        size="species_id",
        hover_data={"petal_length": None, "petal_width": None},
    )
    assert fig.data[0].hovertemplate == "species_id=%{marker.size}<extra></extra>"


def test_hover_data_string_column():
    df = px.data.tips()
    df = pd.DataFrame(df)
    fig = px.scatter(
        df,
        x="tip",
        y="total_bill",
        hover_data="sex",
    )
    assert "sex" in fig.data[0].hovertemplate


def test_composite_hover():
    df = px.data.tips()
    df = pd.DataFrame(df)
    hover_dict = OrderedDict(
        {"day": False, "time": False, "sex": True, "total_bill": ":.1f"}
    )
    fig = px.scatter(
        df,
        x="tip",
        y="total_bill",
        color="day",
        facet_row="time",
        hover_data=hover_dict,
    )
    for el in ["tip", "total_bill", "sex"]:
        assert el in fig.data[0].hovertemplate
    for el in ["day", "time"]:
        assert el not in fig.data[0].hovertemplate
    assert ":.1f" in fig.data[0].hovertemplate


def test_newdatain_hover_data():
    hover_dicts = [
        {"comment": ["a", "b", "c"]},
        {"comment": (1.234, 45.3455, 5666.234)},
        {"comment": [1.234, 45.3455, 5666.234]},
        {"comment": np.array([1.234, 45.3455, 5666.234])},
        {"comment": pd.Series([1.234, 45.3455, 5666.234])},
    ]
    for hover_dict in hover_dicts:
        fig = px.scatter(x=[1, 2, 3], y=[3, 4, 5], hover_data=hover_dict)
        assert (
            fig.data[0].hovertemplate
            == "x=%{x}<br>y=%{y}<br>comment=%{customdata[0]}<extra></extra>"
        )
    fig = px.scatter(
        x=[1, 2, 3], y=[3, 4, 5], hover_data={"comment": (True, ["a", "b", "c"])}
    )
    assert (
        fig.data[0].hovertemplate
        == "x=%{x}<br>y=%{y}<br>comment=%{customdata[0]}<extra></extra>"
    )
    hover_dicts = [
        {"comment": (":.1f", (1.234, 45.3455, 5666.234))},
        {"comment": (":.1f", [1.234, 45.3455, 5666.234])},
        {"comment": (":.1f", np.array([1.234, 45.3455, 5666.234]))},
        {"comment": (":.1f", pd.Series([1.234, 45.3455, 5666.234]))},
    ]
    for hover_dict in hover_dicts:
        fig = px.scatter(
            x=[1, 2, 3],
            y=[3, 4, 5],
            hover_data=hover_dict,
        )
        assert (
            fig.data[0].hovertemplate
            == "x=%{x}<br>y=%{y}<br>comment=%{customdata[0]:.1f}<extra></extra>"
        )


def test_formatted_hover_and_labels():
    df = px.data.tips()
    df = pd.DataFrame(df)
    fig = px.scatter(
        df,
        x="tip",
        y="total_bill",
        hover_data={"total_bill": ":.1f"},
        labels={"total_bill": "Total bill"},
    )
    assert ":.1f" in fig.data[0].hovertemplate


def test_fail_wrong_column():
    # Testing for each of bare string, list, and basic dictionary
    for hover_data_value in ["d", ["d"], {"d": True}]:
        with pytest.raises(ValueError) as err_msg:
            px.scatter(
                {"a": [1, 2], "b": [3, 4], "c": [2, 1]},
                x="a",
                y="b",
                hover_data=hover_data_value,
            )
        assert (
            "Value of 'hover_data_0' is not the name of a column in 'data_frame'."
            in str(err_msg.value)
        )
    # Testing other dictionary possibilities below
    with pytest.raises(ValueError) as err_msg:
        px.scatter(
            {"a": [1, 2], "b": [3, 4], "c": [2, 1]},
            x="a",
            y="b",
            hover_data={"d": ":.1f"},
        )
    assert (
        "Value of 'hover_data_0' is not the name of a column in 'data_frame'."
        in str(err_msg.value)
    )
    with pytest.raises(ValueError) as err_msg:
        px.scatter(
            {"a": [1, 2], "b": [3, 4], "c": [2, 1]},
            x="a",
            y="b",
            hover_data={"d": [3, 4, 5]},  # d is too long
        )
    assert (
        "All arguments should have the same length. The length of hover_data key `d` is 3"
        in str(err_msg.value)
    )
    with pytest.raises(ValueError) as err_msg:
        px.scatter(
            {"a": [1, 2], "b": [3, 4], "c": [2, 1]},
            x="a",
            y="b",
            hover_data={"d": (True, [3, 4, 5])},  # d is too long
        )
    assert (
        "All arguments should have the same length. The length of hover_data key `d` is 3"
        in str(err_msg.value)
    )
    with pytest.raises(ValueError) as err_msg:
        px.scatter(
            {"a": [1, 2], "b": [3, 4], "c": [2, 1]},
            x="a",
            y="b",
            hover_data={"c": [3, 4]},
        )
    assert (
        "Ambiguous input: values for 'c' appear both in hover_data and data_frame"
        in str(err_msg.value)
    )
    with pytest.raises(ValueError) as err_msg:
        px.scatter(
            {"a": [1, 2], "b": [3, 4], "c": [2, 1]},
            x="a",
            y="b",
            hover_data={"c": (True, [3, 4])},
        )
    assert (
        "Ambiguous input: values for 'c' appear both in hover_data and data_frame"
        in str(err_msg.value)
    )


def test_sunburst_hoverdict_color():
    df = px.data.gapminder().query("year == 2007")
    df = pd.DataFrame(df)
    fig = px.sunburst(
        df,
        path=["continent", "country"],
        values="pop",
        color="lifeExp",
        hover_data={"pop": ":,"},
    )
    assert "color" in fig.data[0].hovertemplate


def test_date_in_hover():
    df = pd.DataFrame({"date": ["2015-04-04 19:31:30+1:00"], "value": [3]})
    df["date"] = pd.to_datetime(df["date"])
    fig = px.scatter(df, x="value", y="value", hover_data=["date"])
    assert str(fig.data[0].customdata[0][0]) == str(df["date"][0])
