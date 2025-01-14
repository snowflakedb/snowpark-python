#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re

import modin.pandas as pd
import pandas as native_pd
import pytest
from pandas._testing import assert_series_equal

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import multithreaded_run


@sql_count_checker(query_count=1, union_count=2)
def test_cut_empty_series_negative():
    with pytest.raises(ValueError, match="Cannot cut empty array"):
        pd.cut(pd.Series(), 3)


@pytest.mark.parametrize(
    "data,cuts",
    [
        pytest.param(
            [-10.0, 0.0, 1.0, 5.6, 9.0, 10.0, 11.0],
            [0.0, 3.0, 7.8, 10.0],
        ),
        ([-10.0, 0.0, 1.0, 5.6, 9.0, 10.0, 11.0], 4),
    ],
)
@pytest.mark.parametrize("right", [True, False])
@pytest.mark.parametrize("include_lowest", [True, False])
@pytest.mark.parametrize("precision", [3, 8])
@pytest.mark.parametrize("duplicates", ["raise", "drop"])
def test_cut_with_no_labels(data, cuts, right, include_lowest, precision, duplicates):
    snow_series = pd.Series(data)
    native_series = native_pd.Series(data)
    kwargs = {
        "labels": False,
        "right": right,
        "include_lowest": include_lowest,
        "precision": precision,
        "duplicates": duplicates,
    }

    with SqlCounter(query_count=2, join_count=1):
        snow_ans = pd.cut(snow_series, cuts, **kwargs)
        native_ans = native_pd.cut(native_series, cuts, **kwargs)
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_ans, native_ans)


@pytest.mark.xfail(reason="categorical not supported in Snowpark pandas API")
@pytest.mark.parametrize(
    "data,cuts,labels",
    [
        (
            [-10.0, 0.0, 1.0, 5.6, 9.0, 10.0, 11.0],
            [0.0, 3.0, 7.8, 10.0],
            ["A", "B", "C"],
        ),
        ([-10.0, 0.0, 1.0, 5.6, 9.0, 10.0, 11.0], 4, [0, 4, 2, 97]),
    ],
)
@pytest.mark.parametrize("right", [True, False])
@pytest.mark.parametrize("include_lowest", [True, False])
@pytest.mark.parametrize("precision", [3, 8])
@pytest.mark.parametrize("ordered", [True, False])
def test_cut_with_labels(data, cuts, labels, right, include_lowest, precision, ordered):
    snow_series = pd.Series(data)
    native_series = native_pd.Series(data)

    kwargs = {
        "labels": labels,
        "right": right,
        "include_lowest": include_lowest,
        "precision": precision,
        "ordered": ordered,
    }

    with SqlCounter(query_count=2, join_count=2):
        snow_ans = pd.cut(snow_series, cuts, **kwargs)

    native_ans = native_pd.cut(native_series, cuts, **kwargs)

    assert_series_equal(snow_ans, native_ans)


@multithreaded_run()
def test_cut_with_ordered_is_false_negative():
    # ordered=False requires labels to be specified.
    # Test here the scenario where labels=None and ordered=False.

    data, cuts = [1, 2, 3, 2, 5, 6, 7, 3], 3
    snow_series = pd.Series(data)
    native_series = native_pd.Series(data)

    kwargs = {"labels": None, "ordered": False}

    expected_msg = "'labels' must be provided if 'ordered = False'"
    expected_type = ValueError

    with pytest.raises(expected_type, match=re.escape(expected_msg)):
        native_pd.cut(native_series, cuts, **kwargs)

    with SqlCounter(query_count=0, join_count=0):
        with pytest.raises(expected_type, match=re.escape(expected_msg)):
            pd.cut(snow_series, cuts, **kwargs).to_pandas()


@sql_count_checker(query_count=1, union_count=2)
def test_cut_non_increasing_bins_negative():
    with pytest.raises(
        ValueError, match=re.escape("bins must increase monotonically.")
    ):
        pd.cut(pd.Series([0, 7, 8, 90]), [7, 8, 7])


@pytest.mark.parametrize("bins", [[7, 8, 8]])
@sql_count_checker(query_count=1, union_count=2)
def test_cut_duplicate_edges_negative(bins):
    data = [0, 7, 8, 90]
    try:
        native_pd.cut(native_pd.Series(data), bins)
    except ValueError as e:
        expected_msg = str(e)
    with pytest.raises(ValueError, match=re.escape(expected_msg)):
        pd.cut(pd.Series(data), bins)


@sql_count_checker(query_count=0)
def test_cut_retbins_negative():
    with pytest.raises(NotImplementedError, match="retbins"):
        pd.cut(pd.Series([1, 4, 7, 90]), 3, retbins=True)


@sql_count_checker(query_count=1, union_count=2)
def test_cut_labels_none_negative():
    with pytest.raises(
        NotImplementedError, match="pandas type interval.* is not implemented"
    ):
        pd.cut(pd.Series([1, 4, 7, 90]), 3, labels=None)


@sql_count_checker(query_count=1, union_count=2)
@pytest.mark.parametrize(
    "labels",
    [
        [
            "A",
            "B",
            "A",
        ],  # duplicates are not allowed, check same error message is produced.
        [
            "A",
            "B",
        ],  # length of labels given does not match number of bins, check same error message is produced.
    ],
)
def test_cut_labels_negative(labels):
    data, bins = [1, 4, 7, 90], 3
    try:
        native_pd.cut(native_pd.Series(data), bins, labels=labels)
    except Exception as e:
        e_type = type(e)
        e_msg = str(e)
    else:
        pytest.fail(reason="pandas cut did not raise an exception")

    # Check that pandas exception matches snowpark pandas API exception.
    with pytest.raises(e_type, match=re.escape(e_msg)):
        pd.cut(pd.Series(data), bins, labels=labels)
