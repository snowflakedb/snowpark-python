#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import operator

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import create_test_dfs, eval_snowpark_pandas_result


def attrs_comparator(snow, native):
    return snow.attrs == native.attrs


@sql_count_checker(query_count=0)
def test_df_attrs_set_deepcopy():
    # When attrs is set to a new value, a deep copy is made:
    # >>> df = native_pd.DataFrame()
    # >>> d = {"a": 1}
    # >>> df.attrs = d
    # >>> df.attrs
    # {'a' : 1}
    # >>> d["a"] = 2
    # >>> df.attrs
    # {'a' : 1}
    def func(df):
        d = {"a": 1}
        df.attrs = d
        d["a"] = 2
        return df

    eval_snowpark_pandas_result(
        *create_test_dfs([]),
        func,
        comparator=attrs_comparator,
    )


@sql_count_checker(query_count=0)
def test_df_attrs_get_no_copy():
    # When df.attrs is read, the value can be modified:
    # >>> df = native_pd.DataFrame()
    # >>> df.attrs
    # {}
    # >>> d = df.attrs
    # >>> d["k"] = 1
    # >>> d
    # {'k': 1}
    # >>> df.attrs
    # {'k': 1}
    def func(df):
        d = df.attrs
        d["k"] = 1
        return df

    eval_snowpark_pandas_result(
        *create_test_dfs([]),
        func,
        comparator=attrs_comparator,
    )


# These lists of operations are taken from pandas's `test_finalize`:
# https://github.com/pandas-dev/pandas/blob/v2.2.3/pandas/tests/generic/test_finalize.py
series_unary_attrs_ops = [
    operator.methodcaller("take", []),
    operator.methodcaller("__getitem__", True),
    operator.methodcaller("repeat", 2),
    operator.methodcaller("reset_index"),
    operator.methodcaller("mode"),
    # (pd.Series, ([1, 2],), operator.methodcaller("squeeze")),
    # (pd.Series, ([1, 2],), operator.methodcaller("rename_axis", index="a")),
    # (pd.Series, [1], operator.neg),
    # (pd.Series, [1], operator.pos),
    # (pd.Series, [1], operator.inv),
    # (pd.Series, [1], abs),
    # (pd.Series, [1], round),
    # (pd.Series, (1, mi), operator.methodcaller("xs", "a")),
    # (
    #     pd.Series,
    #     frame_data,
    #     operator.methodcaller("reindex_like", pd.Series([0, 1, 2])),
    # (pd.Series, (1, ["a", "b"]), operator.methodcaller("add_prefix", "_")),
    # (pd.Series, (1, ["a", "b"]), operator.methodcaller("add_suffix", "_")),
    # (pd.Series, ([3, 2],), operator.methodcaller("sort_values")),
    # (pd.Series, ([1] * 10,), operator.methodcaller("head")),
    # (({"A": [1] * 10},), operator.methodcaller("head")),
    # (pd.Series, ([1] * 10,), operator.methodcaller("tail")),
    # (({"A": [1] * 10},), operator.methodcaller("tail")),
    # (pd.Series, ([1, 2],), operator.methodcaller("sample", n=2, replace=True)),
    # (pd.Series, ([1, 2],), operator.methodcaller("astype", float)),
    # (pd.Series, ([1, 2],), operator.methodcaller("copy")),
    # (pd.Series, ([1, 2], None, object), operator.methodcaller("infer_objects")),   # ),
    # (pd.Series, ([1, 2],), operator.methodcaller("convert_dtypes")),
    # (pd.Series, ([1, None, 3],), operator.methodcaller("interpolate")),
    # (pd.Series, ([1, 2],), operator.methodcaller("clip", lower=1)),
    # (
    #     pd.Series,
    #     (1, pd.date_range("2000", periods=4)),
    #     operator.methodcaller("asfreq", "h"),
    # ),
    # (
    #     pd.Series,
    #     (1, pd.date_range("2000", periods=4)),
    #     operator.methodcaller("at_time", "12:00"),
    # ),
    # (
    #     pd.Series,
    #     (1, pd.date_range("2000", periods=4)),
    #     operator.methodcaller("between_time", "12:00", "13:00"),
    # ),
    # (
    #     pd.Series,
    #     (1, pd.date_range("2000", periods=4)),
    #     operator.methodcaller("last", "3D"),
    # ),
    # (pd.Series, ([1, 2],), operator.methodcaller("rank")),
    # (pd.Series, ([1, 2],), operator.methodcaller("where", np.array([True, False]))),
    # (pd.Series, ([1, 2],), operator.methodcaller("mask", np.array([True, False]))),
    # (pd.Series, ([1, 2],), operator.methodcaller("truncate", before=0)),
    # (
    #     pd.Series,
    #     (1, pd.date_range("2000", periods=4, tz="UTC")),
    #     operator.methodcaller("tz_convert", "CET"),
    # ),
    # (
    #     pd.Series,
    #     (1, pd.date_range("2000", periods=4)),
    #     operator.methodcaller("tz_localize", "CET"),
    # ),
    # (pd.Series, ([1, 2],), operator.methodcaller("describe")),
    (pd.Series, ([1, 2],), operator.methodcaller("pct_change")),
    (pd.Series, ([1],), operator.methodcaller("transform", lambda x: x - x.min())),
    (pd.Series, ([1],), operator.methodcaller("apply", lambda x: x)),
    (pd.Series, ([1],), operator.methodcaller("cumsum")),
    (pd.Series, ([1],), operator.methodcaller("cummin")),
    (pd.Series, ([1],), operator.methodcaller("cummax")),
    (pd.Series, ([1],), operator.methodcaller("cumprod")),
]

frame_data = ({"A": [1]},)
frame_mi_data = (
    {"A": [1, 2, 3, 4]},
    pd.MultiIndex.from_product([["a", "b"], [0, 1]], names=["A", "B"]),
)

dataframe_binary_attrs_ops = [
    "add",
    "__add__",
    "sub",
    "__sub__",
    "mul",
    "__mul__",
    "div",
    "__div__",
    "pow",
    "combine",
    "combine_first",
    "update",
    "merge",
    "corrwith",
]

dataframe_unary_attrs_ops = [
    (frame_data, operator.methodcaller("transpose")),
    (frame_data, operator.methodcaller("__getitem__", "A")),
    # (frame_data, operator.methodcaller("query", "A == 1"),
    # (frame_data, operator.methodcaller("eval", "A + 1", engine="python")),
    (frame_data, operator.methodcaller("select_dtypes", include="int")),
    (frame_data, operator.methodcaller("assign", b=1)),
    (frame_data, operator.methodcaller("set_axis", ["A"])),
    (frame_data, operator.methodcaller("reindex", [0, 1])),
    (frame_data, operator.methodcaller("drop", columns=["A"])),
    (frame_data, operator.methodcaller("rename", columns={"A": "a"})),
    (frame_data, operator.methodcaller("fillna", "A")),
    (frame_data, operator.methodcaller("set_index", "A")),
    (frame_data, operator.methodcaller("reset_index")),
    (frame_data, operator.methodcaller("isna")),
    (frame_data, operator.methodcaller("isnull")),
    (frame_data, operator.methodcaller("notna")),
    (frame_data, operator.methodcaller("notnull")),
    (frame_data, operator.methodcaller("dropna")),
    (frame_data, operator.methodcaller("drop_duplicates")),
    (frame_data, operator.methodcaller("duplicated")),
    (frame_data, operator.methodcaller("sort_values", by="A")),
    (frame_data, operator.methodcaller("sort_index")),
    (frame_data, operator.methodcaller("nlargest", 1, "A")),
    (frame_data, operator.methodcaller("nsmallest", 1, "A")),
    (frame_data, operator.methodcaller("pivot", columns="A")),
    (
        ({"A": [1], "B": [1]},),
        operator.methodcaller("pivot_table", columns="A"),
    ),
    (frame_data, operator.methodcaller("stack")),
    (frame_data, operator.methodcaller("explode", "A")),
    (frame_mi_data, operator.methodcaller("unstack")),
    (
        ({"A": ["a", "b", "c"], "B": [1, 3, 5], "C": [2, 4, 6]},),
        operator.methodcaller("melt", id_vars=["A"], value_vars=["B"]),
    ),
    (frame_data, operator.methodcaller("round", 2)),
    (frame_data, operator.methodcaller("corr")),
    (frame_data, operator.methodcaller("count")),
    (frame_data, operator.methodcaller("nunique")),
    (frame_data, operator.methodcaller("idxmin")),
    (frame_data, operator.methodcaller("idxmax")),
    (frame_data, operator.methodcaller("median")),
    (
        frame_data,
        operator.methodcaller("quantile", numeric_only=True),
    ),
    (frame_mi_data, operator.methodcaller("isin", [1])),
    # Squeeze on columns, otherwise we'll end up with a scalar
    (frame_data, operator.methodcaller("squeeze", axis="columns")),
    (frame_data, operator.methodcaller("rename_axis", columns="a")),
    # Unary ops
    (frame_data, operator.neg),
    (frame_data, operator.pos),
    (frame_data, operator.inv),
    (frame_data, abs),
    (frame_data, round),
    (frame_data, operator.methodcaller("take", [0, 0])),
    (frame_data, operator.methodcaller("get", "A")),
    # (
    #     frame_data,
    #     operator.methodcaller("reindex_like", pd.DataFrame({"A": [1, 2, 3]})),
    # ),
    (frame_data, operator.methodcaller("add_prefix", "_")),
    (frame_data, operator.methodcaller("add_suffix", "_")),
    (frame_data, operator.methodcaller("sample", n=2, replace=True)),
    (frame_data, operator.methodcaller("astype", float)),
    (frame_data, operator.methodcaller("copy")),
    (frame_data, operator.methodcaller("rank")),
    (frame_data, operator.methodcaller("where", np.array([[True]]))),
    (frame_data, operator.methodcaller("mask", np.array([[True]]))),
    (frame_data, operator.methodcaller("truncate", before=0)),
    (frame_data, operator.methodcaller("describe")),
    (frame_data, operator.methodcaller("pct_change")),
    (
        frame_mi_data,
        operator.methodcaller("transform", lambda x: x - x.min()),
    ),
    (frame_mi_data, operator.methodcaller("apply", lambda x: x)),
    # Cumulative reductions
    (frame_data, operator.methodcaller("cumsum")),
    (frame_data, operator.methodcaller("cummin")),
    (frame_data, operator.methodcaller("cummax")),
    (frame_data, operator.methodcaller("cumprod")),
    # Reductions
    (frame_data, operator.methodcaller("any")),
    (frame_data, operator.methodcaller("all")),
    (frame_data, operator.methodcaller("min")),
    (frame_data, operator.methodcaller("max")),
    (frame_data, operator.methodcaller("sum")),
    (frame_data, operator.methodcaller("std")),
    (frame_data, operator.methodcaller("mean")),
    (frame_data, operator.methodcaller("skew")),
]


# Tests that attrs is preserved across a unary operation that returns a Snowpark pandas object.
@pytest.mark.parametrize("frame_data, methodcaller", dataframe_unary_attrs_ops)
def test_df_attrs_unary_methods(frame_data, methodcaller, query_count=0):
    with SqlCounter(query_count=query_count):
        if len(frame_data) == 2:
            native_df = native_pd.DataFrame(frame_data[0], index=frame_data[1])
        else:
            native_df = native_pd.DataFrame(frame_data[0])
        snow_df = pd.DataFrame(native_df)
        native_df.attrs = {"A": [1], "B": "check me"}
        snow_df.attrs = native_df.attrs

        assert methodcaller(native_df).attrs == native_df.attrs
        assert methodcaller(snow_df).attrs == snow_df.attrs


@pytest.mark.parametrize("method_name", dataframe_binary_attrs_ops)
def test_df_attrs_binary_methods(method_name):
    # Binary operators take the attrs field of the left frame
    snow_left, native_left = create_test_dfs(frame_data)
    snow_left.attrs = {"A": "correct"}
    native_left.attrs = {"A": "correct"}
    snow_right, native_right = create_test_dfs(frame_data)
    snow_right.attrs = {"B": "incorrect"}
    native_right.attrs = {"B": "incorrect"}
    with SqlCounter(query_count=0):
        assert (
            getattr(native_left, method_name)(native_right).attrs == native_left.attrs
        )
        assert getattr(snow_left, method_name)(snow_right).attrs == snow_left.attrs
