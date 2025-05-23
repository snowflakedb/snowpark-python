#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.utils import assert_snowpark_pandas_equal_to_pandas
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize(
    "interpolation",
    [
        "linear",
        pytest.param(
            "lower",
            marks=pytest.mark.xfail(
                reason="Quantile with lower or midpoint interpolation not yet supported",
                strict=True,
                raises=AttributeError,
            ),
        ),
        "nearest",
        pytest.param(
            "midpoint",
            marks=pytest.mark.xfail(
                reason="Quantile with lower or midpoint interpolation not yet supported",
                strict=True,
                raises=AttributeError,
            ),
        ),
    ],
)
@pytest.mark.parametrize(
    "a_vals,b_vals",
    [
        # Ints
        ([1, 2, 3, 4, 5], [5, 4, 3, 2, 1]),
        ([1, 2, 3, 4, 5], [4, 3, 2, 1]),
        # Floats
        ([1.0, 2.0, 3.0, 4.0, 5.0], [5.0, 4.0, 3.0, 2.0, 1.0]),
        # Missing data
        ([1.0, np.nan, 3.0, np.nan, 5.0], [5.0, np.nan, 3.0, np.nan, 1.0]),
        ([np.nan, 4.0, np.nan, 2.0, np.nan], [np.nan, 4.0, np.nan, 2.0, np.nan]),
        # Timestamps
        pytest.param(
            native_pd.date_range("1/1/18", freq="D", periods=5),
            native_pd.date_range("1/1/18", freq="D", periods=5)[::-1],
            # strict=False because it will pass for fallbacks
            marks=pytest.mark.xfail(
                strict=False, reason="SNOW-1003587: timestamp quantile is unsupported"
            ),
        ),
        # as_unit is not supported for DatetimeIndex in pandas 1.5.x
        # (
        #    native_pd.date_range("1/1/18", freq="D", periods=5).as_unit("s"),
        #    native_pd.date_range("1/1/18", freq="D", periods=5)[::-1].as_unit("s"),
        # ),
        # All NA
        ([np.nan] * 5, [np.nan] * 5),
        pytest.param(
            pd.timedelta_range(
                "1 days",
                "5 days",
            ),
            pd.timedelta_range("1 second", "5 second"),
            id="timedelta",
        ),
    ],
)
@pytest.mark.parametrize("q", [0, 0.5, 1])
def test_quantile(interpolation, a_vals, b_vals, q):
    if (
        interpolation == "nearest"
        and q == 0.5
        and isinstance(b_vals, list)
        and b_vals == [4, 3, 2, 1]
    ):
        pytest.xfail(
            reason="Unclear numpy expectation for nearest "
            "result with equidistant data"
        )
    all_vals = native_pd.concat([native_pd.Series(a_vals), native_pd.Series(b_vals)])

    a_expected = native_pd.Series(a_vals).quantile(q, interpolation=interpolation)
    b_expected = native_pd.Series(b_vals).quantile(q, interpolation=interpolation)

    df = pd.DataFrame(
        {"key": ["a"] * len(a_vals) + ["b"] * len(b_vals), "val": all_vals}
    )

    expected = native_pd.DataFrame(
        [a_expected, b_expected],
        columns=["val"],
        index=native_pd.Index(["a", "b"], name="key"),
    )
    with SqlCounter(query_count=1):
        result = df.groupby("key").quantile(q, interpolation=interpolation)
        assert_snowpark_pandas_equal_to_pandas(result, expected, check_dtype=False)


@pytest.mark.xfail(
    reason="SNOW-1062878: Because list-like q would return multiple rows, calling quantile through the aggregate frontend in this manner is unsupported.",
    strict=True,
    raises=AttributeError,
)
@sql_count_checker(query_count=2)
def test_quantile_array():
    # https://github.com/pandas-dev/pandas/issues/27526
    df = pd.DataFrame({"A": [0, 1, 2, 3, 4]})
    key = np.array([0, 0, 1, 1, 1], dtype=np.int64)
    result = df.groupby(key).quantile([0.25])

    index = native_pd.MultiIndex.from_product([[0, 1], [0.25]])
    expected = native_pd.DataFrame({"A": [0.25, 2.50]}, index=index)
    assert_snowpark_pandas_equal_to_pandas(result, expected)

    df = pd.DataFrame({"A": [0, 1, 2, 3], "B": [4, 5, 6, 7]})
    index = native_pd.MultiIndex.from_product([[0, 1], [0.25, 0.75]])

    key = np.array([0, 0, 1, 1], dtype=np.int64)
    result = df.groupby(key).quantile([0.25, 0.75])
    expected = native_pd.DataFrame(
        {"A": [0.25, 0.75, 2.25, 2.75], "B": [4.25, 4.75, 6.25, 6.75]}, index=index
    )
    assert_snowpark_pandas_equal_to_pandas(result, expected)


@pytest.mark.xfail(
    reason="SNOW-1062878: Because list-like q would return multiple rows, calling quantile through the aggregate frontend in this manner is unsupported.",
    strict=True,
    raises=AttributeError,
)
@sql_count_checker(query_count=2)
def test_quantile_array_list_like_q():
    # https://github.com/pandas-dev/pandas/pull/28085#issuecomment-524066959
    arr = np.random.RandomState(0).randint(0, 5, size=(10, 3), dtype=np.int64)
    df = pd.DataFrame(arr, columns=list("ABC"))
    result = df.groupby("A").quantile([0.3, 0.7])
    expected = native_pd.DataFrame(
        {
            "B": [0.9, 2.1, 2.2, 3.4, 1.6, 2.4, 2.3, 2.7, 0.0, 0.0],
            "C": [1.2, 2.8, 1.8, 3.0, 0.0, 0.0, 1.9, 3.1, 3.0, 3.0],
        },
        index=native_pd.MultiIndex.from_product(
            [[0, 1, 2, 3, 4], [0.3, 0.7]], names=["A", None]
        ),
    )
    assert_snowpark_pandas_equal_to_pandas(result, expected)


@pytest.mark.xfail(
    reason="SNOW-1062878: Because list-like q would return multiple rows, calling quantile through the aggregate frontend in this manner is unsupported.",
    strict=True,
    raises=AttributeError,
)
@sql_count_checker(query_count=2)
def test_quantile_array_no_sort():
    df = pd.DataFrame({"A": [0, 1, 2], "B": [3, 4, 5]})
    key = np.array([1, 0, 1], dtype=np.int64)
    result = df.groupby(key, sort=False).quantile([0.25, 0.5, 0.75])
    expected = native_pd.DataFrame(
        {"A": [0.5, 1.0, 1.5, 1.0, 1.0, 1.0], "B": [3.5, 4.0, 4.5, 4.0, 4.0, 4.0]},
        index=pd.MultiIndex.from_product([[1, 0], [0.25, 0.5, 0.75]]),
    )
    assert_snowpark_pandas_equal_to_pandas(result, expected)

    result = df.groupby(key, sort=False).quantile([0.75, 0.25])
    expected = native_pd.DataFrame(
        {"A": [1.5, 0.5, 1.0, 1.0], "B": [4.5, 3.5, 4.0, 4.0]},
        index=pd.MultiIndex.from_product([[1, 0], [0.75, 0.25]]),
    )
    assert_snowpark_pandas_equal_to_pandas(result, expected)


@pytest.mark.xfail(
    reason="SNOW-1062878: Because list-like q would return multiple rows, calling quantile through the aggregate frontend in this manner is unsupported.",
    strict=True,
    raises=AttributeError,
)
@sql_count_checker(query_count=2)
def test_quantile_array_multiple_levels():
    df = pd.DataFrame(
        {"A": [0, 1, 2], "B": [3, 4, 5], "c": ["a", "a", "a"], "d": ["a", "a", "b"]}
    )
    result = df.groupby(["c", "d"]).quantile([0.25, 0.75])
    index = native_pd.MultiIndex.from_tuples(
        [("a", "a", 0.25), ("a", "a", 0.75), ("a", "b", 0.25), ("a", "b", 0.75)],
        names=["c", "d", None],
    )
    expected = native_pd.DataFrame(
        {"A": [0.25, 0.75, 2.0, 2.0], "B": [3.25, 3.75, 5.0, 5.0]}, index=index
    )
    assert_snowpark_pandas_equal_to_pandas(result, expected)


@pytest.mark.xfail(
    reason="SNOW-1062878: Because list-like q would return multiple rows, calling quantile through the aggregate frontend in this manner is unsupported.",
    strict=True,
    raises=AttributeError,
)
@pytest.mark.parametrize("frame_size", [(2, 3), (100, 10)])
@pytest.mark.parametrize("groupby", [[0], [0, 1]])
@pytest.mark.parametrize("q", [[0.5, 0.6]])
def test_groupby_quantile_with_arraylike_q_and_int_columns(frame_size, groupby, q):
    # GH30289
    nrow, ncol = frame_size
    df = pd.DataFrame(
        np.array([ncol * [_ % 4] for _ in range(nrow)]), columns=range(ncol)
    )

    idx_levels = [np.arange(min(nrow, 4))] * len(groupby) + [q]
    idx_codes = [[x for x in range(min(nrow, 4)) for _ in q]] * len(groupby) + [
        list(range(len(q))) * min(nrow, 4)
    ]
    expected_index = pd.MultiIndex(
        levels=idx_levels, codes=idx_codes, names=groupby + [None]
    )
    expected_values = [
        [float(x)] * (ncol - len(groupby)) for x in range(min(nrow, 4)) for _ in q
    ]
    expected_columns = [x for x in range(ncol) if x not in groupby]
    expected = native_pd.DataFrame(
        expected_values, index=expected_index, columns=expected_columns
    )

    if frame_size == (100, 10):
        # The 3 extra queries occur because Snowpark creates a temp table for larger dataframes;
        # 1 query creates the temp table, 1 inserts into it, and 1 drops the temp table.
        expected_query_count = 11
    else:
        expected_query_count = 8

    with SqlCounter(query_count=expected_query_count, sproc_count=1):
        result = df.groupby(groupby).quantile(q)
        assert_snowpark_pandas_equal_to_pandas(result, expected)


@sql_count_checker(query_count=0)
def test_quantile_raises():
    df = pd.DataFrame(
        [["foo", "a"], ["foo", "b"], ["foo", "c"]], columns=["key", "val"]
    )

    with pytest.raises(SnowparkSQLException):
        df.groupby("key").quantile().to_pandas()


@sql_count_checker(query_count=0)
def test_quantile_out_of_bounds_q_raises():
    # https://github.com/pandas-dev/pandas/issues/27470
    df = pd.DataFrame({"a": [0, 0, 0, 1, 1, 1], "b": range(6)})
    g = df.groupby([0, 0, 0, 1, 1, 1])
    with pytest.raises(NotImplementedError):
        g.quantile(50)

    with pytest.raises(NotImplementedError):
        g.quantile(-1)


@pytest.mark.parametrize(
    "key, val, expected_key, expected_val",
    [
        ([1.0, np.nan, 3.0, np.nan], range(4), [1.0, 3.0], [0.0, 2.0]),
        ([1.0, np.nan, 2.0, 2.0], range(4), [1.0, 2.0], [0.0, 2.5]),
        (["a", "b", "b", np.nan], range(4), ["a", "b"], [0, 1.5]),
        ([0], [42], [0], [42.0]),
        # TODO (SNOW-863809): The dtype got changed in to_pandas from float64 to,
        #  object, which causes the test to fail.
        # ([], [], np.array([], dtype="float64"), np.array([], dtype="float64")),
    ],
)
@sql_count_checker(query_count=2)
def test_quantile_missing_group_values_correct_results(
    key, val, expected_key, expected_val
):
    # GH 28662, GH 33200, GH 33569
    df = pd.DataFrame({"key": key, "val": val})

    expected = native_pd.DataFrame(
        expected_val, index=native_pd.Index(expected_key, name="key"), columns=["val"]
    )

    grp = df.groupby("key")

    result = grp.quantile(0.5)
    assert_snowpark_pandas_equal_to_pandas(result, expected)

    result = grp.quantile()
    assert_snowpark_pandas_equal_to_pandas(result, expected)


@pytest.mark.xfail(
    reason="Quantile with these arguments are not yet supported",
    strict=True,
    raises=AttributeError,
)
@pytest.mark.parametrize(
    "interpolation, val1, val2", [("lower", 2, 2), ("higher", 2, 3), ("nearest", 2, 2)]
)
@sql_count_checker(query_count=1)
def test_groupby_quantile_all_na_group_masked(interpolation, val1, val2):
    # GH#37493
    df = pd.DataFrame({"a": [1, 1, 1, 2], "b": [1, 2, 3, pd.NA]})
    result = df.groupby("a").quantile(q=[0.5, 0.7], interpolation=interpolation)
    expected = native_pd.DataFrame(
        {"b": [val1, val2, pd.NA, pd.NA]},
        index=native_pd.MultiIndex.from_arrays(
            [native_pd.Series([1, 1, 2, 2], dtype=np.int64), [0.5, 0.7, 0.5, 0.7]],
            names=["a", None],
        ),
    )
    assert_snowpark_pandas_equal_to_pandas(result, expected, check_dtype=False)


@pytest.mark.xfail(
    reason="SNOW-1062878: Because list-like q would return multiple rows, calling quantile through the aggregate frontend in this manner is unsupported.",
    strict=True,
    raises=AttributeError,
)
@sql_count_checker(query_count=1)
def test_groupby_quantile_nonmulti_levels_order():
    # Non-regression test for GH #53009
    ind = pd.MultiIndex.from_tuples(
        [
            (0, "a", "B"),
            (0, "a", "A"),
            (0, "b", "B"),
            (0, "b", "A"),
            (1, "a", "B"),
            (1, "a", "A"),
            (1, "b", "B"),
            (1, "b", "A"),
        ],
        names=["sample", "cat0", "cat1"],
    )
    ser = pd.Series(range(8), index=ind)
    result = ser.groupby(level="cat1", sort=False).quantile([0.2, 0.8])

    qind = native_pd.MultiIndex.from_tuples(
        [("B", 0.2), ("B", 0.8), ("A", 0.2), ("A", 0.8)], names=["cat1", None]
    )
    expected = native_pd.Series([1.2, 4.8, 2.2, 5.8], index=qind)

    assert_snowpark_pandas_equal_to_pandas(result, expected, check_dtype=False)
