#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas.errors import SpecificationError

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.mark.parametrize("by", ["a", ["b"], ["a", "b"]])
@pytest.mark.parametrize("sort", [True, False])
@sql_count_checker(query_count=1)
def test_groupby_sort_multiindex_series(
    native_series_multi_numeric, agg_method, by, sort
):
    native_mseries_group = native_series_multi_numeric.groupby(by=by, sort=sort)
    mseries_group = pd.Series(native_series_multi_numeric).groupby(by=by, sort=sort)
    eval_snowpark_pandas_result(mseries_group, native_mseries_group, agg_method)


@sql_count_checker(query_count=2)
def test_groupby_series_count_with_nan():
    index = native_pd.Index(["a", "b", "b", "a", "c"])
    index.names = ["grp_col"]
    series = pd.Series([1.2, np.nan, np.nan, np.nan, np.nan], index=index)
    eval_snowpark_pandas_result(
        series,
        series.to_pandas(),
        lambda se: se.groupby("grp_col").count(),
        # Some calls to the native pandas function propagate attrs while some do not, depending on the values of its arguments.
        test_attrs=False,
    )


@pytest.mark.parametrize(
    "agg_func",
    [
        "max",
        np.min,
        min,
        np.median,
        np.std,
        "var",
        [np.var],
        ["sum", np.std],
        ["sum", np.median, sum],
        {"x": sum},
        {"y": "sum", "x": min},
    ],
)
@pytest.mark.parametrize("sort", [True, False])
@sql_count_checker(query_count=2)
def test_groupby_agg_series(agg_func, sort):
    index = native_pd.Index(["a", "b", "b", "a", "c"])
    index.names = ["grp_col"]
    series = pd.Series([3.5, 1.2, 4.3, 2.0, 1.8], index=index)

    def perform_groupby(se):
        se = se.groupby(by="grp_col", sort=sort)
        if isinstance(agg_func, dict):
            return se.agg(**agg_func)
        else:
            return se.agg(agg_func)

    eval_snowpark_pandas_result(
        series,
        series.to_pandas(),
        perform_groupby,
        # Some calls to the native pandas function propagate attrs while some do not, depending on the values of its arguments.
        test_attrs=False,
    )


@sql_count_checker(query_count=0)
def test_groupby_agg_series_dict_func_negative():
    index = native_pd.Index(["a", "b", "b", "a", "c"])
    index.names = ["grp_col"]
    series = pd.Series([3.5, 1.2, 4.3, 2.0, 1.8], index=index)
    native_series = native_pd.Series([3.5, 1.2, 4.3, 2.0, 1.8], index=index)

    eval_snowpark_pandas_result(
        series,
        native_series,
        lambda se: se.groupby(by="grp_col").agg({"x": "min"}),
        expect_exception=True,
        expect_exception_match="nested renamer is not supported",
        expect_exception_type=SpecificationError,
        assert_exception_equal=True,
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize(
    "agg_func, type_str",
    [({"x": ("y", "sum")}, "tuple"), ({"x": pd.NamedAgg("y", "sum")}, "NamedAgg")],
    ids=["2-tuple", "NamedAgg"],
)
def test_groupby_agg_series_raises_for_2_tuple_agg(agg_func, type_str):
    index = native_pd.Index(["a", "b", "b", "a", "c"])
    index.names = ["grp_col"]
    series = pd.Series([3.5, 1.2, 4.3, 2.0, 1.8], index=index)

    eval_snowpark_pandas_result(
        series,
        series.to_pandas(),
        lambda se: se.groupby(by="grp_col").agg(**agg_func),
        expect_exception=True,
        expect_exception_match=re.escape(
            f"func is expected but received {type_str} in **kwargs."
        ),
        expect_exception_type=TypeError,
        assert_exception_equal=True,
    )


@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize("aggs", [{"minimum": min}, {"minimum": min, "maximum": max}])
@sql_count_checker(query_count=2)
def test_groupby_agg_series_named_agg(aggs, sort):
    index = native_pd.Index(["a", "b", "b", "a", "c"])
    index.names = ["grp_col"]
    series = pd.Series([3.5, 1.2, 4.3, 2.0, 1.8], index=index)

    eval_snowpark_pandas_result(
        series,
        series.to_pandas(),
        lambda se: se.groupby(by="grp_col", sort=sort).agg(**aggs),
        # Some calls to the native pandas function propagate attrs while some do not, depending on the values of its arguments.
        test_attrs=False,
    )


@pytest.mark.parametrize("numeric_only", [False, None])
@sql_count_checker(query_count=2, join_count=2)
def test_groupby_series_numeric_only(series_str, numeric_only):
    native_series = series_str.to_pandas()
    eval_snowpark_pandas_result(
        series_str,
        native_series,
        lambda se: se.groupby(by="grp_col").max(numeric_only=numeric_only),
    )


@pytest.mark.parametrize("level", [0, 1, [1, 0], "b", [1, 1], [0, "b"], [-1]])
@sql_count_checker(query_count=1)
def test_groupby_sort_multiindex_series_level(native_series_multi_numeric, level):
    eval_snowpark_pandas_result(
        pd.Series(native_series_multi_numeric),
        native_series_multi_numeric,
        lambda ser: ser.groupby(level=level).sum(),
    )


@sql_count_checker(query_count=1)
def test_groupby_series_single_index():
    snow_ser = pd.Series([2, 5, 6, 8], index=[2.0, 4.0, 4.0, 5.0])
    native_ser = native_pd.Series([2, 5, 6, 8], index=[2.0, 4.0, 4.0, 5.0])

    eval_snowpark_pandas_result(
        snow_ser, native_ser, lambda ser: ser.groupby(level=0).mean()
    )
