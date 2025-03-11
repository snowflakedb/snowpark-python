#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re

import modin.pandas as pd
import numpy as np
import numpy.testing as npt
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

bool_arg = {"True": True, "False": False, "None": None}
bool_arg_keys = list(bool_arg.keys())
bool_arg_values = list(bool_arg.values())


@pytest.mark.parametrize(
    "x,q",
    [
        (
            range(5),
            4,
        )
    ],
)
@sql_count_checker(query_count=0)
def test_qcut_non_series(x, q):
    npt.assert_almost_equal(
        pd.qcut(x, q, labels=False), native_pd.qcut(x, q, labels=False)
    )


@pytest.mark.parametrize(
    "n,q,expected_query_count",
    [
        (5, 1, 2),
        (100, 1, 2),
        (1000, 1, 8),
        (5, 10, 3),
        (100, 10, 3),
        (1000, 10, 12),
        (5, 47, 3),
        (100, 47, 3),
        (1000, 47, 12),
        # TODO: SNOW-1229442
        # qcut was significantly optimized with SNOW-1368640 and SNOW-1370365, but still
        # cannot compute 10k q values in a reasonable amount of time.
        # (5, 10000, 1),
        # (100, 10000, 1),
        # (1000, 10000, 1),
    ],
)
def test_qcut_series(n, q, expected_query_count):

    native_ans = native_pd.qcut(
        native_pd.Series(range(n)), q, labels=False, duplicates="drop"
    )

    # Large n can not inline everything into a single query and will instead create a temp table.
    snow_series = pd.Series(list(range(n)))
    with SqlCounter(
        query_count=expected_query_count,
        high_count_expected=True,
        high_count_reason="to_pandas() data transfer issues many CREATE SCOPED TEMPORARY TABLE ... / INSERT INTO ... queries",
    ):
        ans = pd.qcut(snow_series, q, labels=False, duplicates="drop")
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(ans, native_ans)


@pytest.mark.parametrize("data,q", [([0, 100, 200, 400, 600, 700, 2000], 5)])
def test_qcut_series_non_range_data(data, q):
    native_ans = native_pd.qcut(native_pd.Series(data), q, labels=False)

    # Large n can not inline everything into a single query and will instead create a temp table.
    with SqlCounter(query_count=3):
        ans = pd.qcut(pd.Series(data), q, labels=False)

        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(ans, native_ans)


@pytest.mark.parametrize("n,expected_query_count", [(5, 1), (100, 1), (1000, 6)])
@pytest.mark.parametrize("q", [1, 10, 47, 10000])
@sql_count_checker(query_count=0)
def test_qcut_series_with_none_labels_negative(n, q, expected_query_count):

    native_ans = native_pd.qcut(native_pd.Series(range(n)), q, labels=None)

    with pytest.raises(
        NotImplementedError,
        match=re.escape(
            "Snowpark pandas API qcut method supports only labels=False, if you need support"
            " for labels consider calling pandas.qcut(x.to_pandas(), q, ...)"
        ),
    ):
        # Large n can not inline everything into a single query and will instead create a temp table.
        with SqlCounter(query_count=expected_query_count):
            ans = pd.qcut(pd.Series(range(n)), q, labels=None)

        # assign to series to compare
        native_ans = native_pd.Series(native_ans)
        ans = native_pd.Series(ans)

        native_pd.testing.assert_series_equal(
            ans,
            native_ans,
            check_exact=False,
            check_dtype=False,
            check_index_type=False,
        )


@pytest.mark.parametrize(
    "q",
    [
        1,
        10,
        47,
        # TODO: Once SNOW-1229442 is done, uncomment following lin.
        # 10000
    ],
)
@pytest.mark.parametrize("s", [native_pd.Series([0]), native_pd.Series([1])])
def test_qcut_series_single_element_negative(q, s):
    # if q != 1, then an error will be produced for a single-element series.
    if q != 1:
        # Error will be:
        #  ValueError: Bin edges must be unique: array([0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.]).
        #  You can drop duplicate edges by setting the 'duplicates' kwarg
        re_match = "Bin edges must be unique: .*"
        with pytest.raises(ValueError, match=re_match):
            native_pd.qcut(s, q, labels=False)
        with SqlCounter(query_count=2):
            with pytest.raises(ValueError, match=re_match):
                pd.qcut(pd.Series(s), q, labels=False)
    else:
        native_ans = native_pd.qcut(s, q, labels=False)

        with SqlCounter(query_count=1):
            ans = pd.qcut(pd.Series(s), q, labels=False)

        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(ans, native_ans)


@pytest.mark.parametrize(
    "q",
    [
        1,
        10,
        47,
        # uncomment this line once quantile is fixed in TODO SNOW-1229442.
        # 10000
    ],
)
@pytest.mark.parametrize("s", [native_pd.Series([0]), native_pd.Series([1])])
def test_qcut_series_single_element(q, s):
    native_ans = native_pd.qcut(s, q, duplicates="drop", labels=False)

    with SqlCounter(query_count=2 if q == 1 else 3):
        ans = pd.qcut(pd.Series(s), q, duplicates="drop", labels=False)
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(ans, native_ans)


@pytest.mark.xfail(reason="TODO: SNOW-1225562 support retbins")
@sql_count_checker(query_count=1)
def test_qcut_retbins_negative():
    snow_series = pd.Series(range(10))

    pd.qcut(snow_series, 4, retbins=False)


@sql_count_checker(query_count=0)
def test_qcut_labels_negative():

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Bin labels must either be False, None or passed in as a list-like argument"
        ),
    ):
        native_pd.qcut(native_pd.Series(range(5)), 4, labels=True, duplicates="drop")

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Bin labels must either be False, None or passed in as a list-like argument"
        ),
    ):
        pd.qcut(pd.Series(range(5)), 4, labels=True, duplicates="drop")


@pytest.mark.parametrize("q", [[0.5, 0.2]])
@sql_count_checker(query_count=0)
def test_qcut_increasing_quantiles_negative(q):
    native_series = native_pd.Series([1, 2, 4, 6, 9, 2])
    snow_series = pd.Series(native_series)

    def helper(s):
        if isinstance(s, pd.Series):
            return pd.qcut(s, q)
        else:
            return native_pd.qcut(s, q)

    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        helper,
        expect_exception=ValueError,
        expect_exception_match=re.escape("left side of interval must be <= right side"),
    )


@pytest.mark.parametrize("data", [[2014, 2014, 2015, 2016, 2017, 2014]])
@pytest.mark.parametrize(
    "q",
    [[0, 0.15, 0.35, 0.51, 0.78, 1], [0, 0.5, 1], [0.2, 0.8]],
)
def test_qcut_list_of_values(data, q):
    native_s = native_pd.Series(data)
    snow_s = pd.Series(data)

    native_ans = native_pd.qcut(native_s, q, duplicates="drop", labels=False)

    with SqlCounter(query_count=2):
        ans = pd.qcut(snow_s, q, duplicates="drop", labels=False)
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(ans, native_ans)


def test_qcut_list_of_values_raise_negative():
    # There will be duplicate bins for the given quantiles, which will result in an error.
    data = [2014, 2014, 2015, 2016, 2017, 2014]
    q = [0, 0.15, 0.35, 0.51, 0.78, 1]

    native_s = native_pd.Series(data)
    snow_s = pd.Series(data)

    # Error produced here will be:
    # ValueError: Bin edges must be unique: array([2014.  , 2014.  , 2014.  , 2014.55, 2015.9 , 2017.  ]).
    #                You can drop duplicate edges by setting the 'duplicates' kwarg
    expected_msg = "Bin edges must be unique: "
    with pytest.raises(ValueError, match=expected_msg):
        native_pd.qcut(native_s, q, duplicates="raise", labels=False)

    with SqlCounter(query_count=1):
        with pytest.raises(ValueError, match=expected_msg):
            pd.qcut(snow_s, q, duplicates="raise", labels=False)


@sql_count_checker(query_count=0)
def test_qcut_quantile_limit_exhausted():

    snow_s = pd.Series(range(100000))
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas API supports at most .* quantiles\\.",
    ):
        snow_s.quantile(np.linspace(0, 1, 100))


@pytest.mark.parametrize("q", [-2, 4.5, -1.0, [6.8], [0.0, 1.0, -1.0]])
@sql_count_checker(query_count=0)
def test_qcut_invalid_quantiles_negative(q):
    snow_s = pd.Series(range(5))
    native_s = native_pd.Series(range(5))

    # make sure same exception is produced in Snowpark pandas API
    try:
        native_s.quantile(q)
    except Exception as e:
        expected_exception = type(e)
        expected_message = re.escape(str(e))

    with pytest.raises(expected_exception, match=expected_message):
        snow_s.quantile(q)
