#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import VALID_PANDAS_LABELS, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker

# TODO: SNOW-782594 Add tests for categorical data.


@pytest.fixture(scope="function")
def native_df_simple():
    return native_pd.DataFrame(
        {
            "A": [1, 10, -1, 100, 0, -11],
            "B": [321, 312, 123, 132, 231, 213],
            "a": ["abc", " ", "", "ABC", "_", "XYZ"],
            "b": ["1", "10", "xyz", "0", "2", "abc"],
            "timedelta": [
                pd.Timedelta(10),
                pd.Timedelta(1),
                pd.NaT,
                pd.Timedelta(-1),
                pd.Timedelta(100),
                pd.Timedelta(-11),
            ],
        },
        index=native_pd.Index([1, 2, 3, 4, 5, 6], name="ind"),
    )


@pytest.mark.parametrize("by", ["A", "B", "a", "b", "ind", "timedelta"])
@pytest.mark.parametrize("ascending", [True, False])
@sql_count_checker(query_count=3)
def test_sort_values(native_df_simple, by, ascending):
    snow_df = pd.DataFrame(native_df_simple)
    eval_snowpark_pandas_result(
        snow_df, native_df_simple, lambda df: df.sort_values(by=by, ascending=ascending)
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df_simple,
        lambda df: df.sort_values(by=by, ascending=[ascending]),
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df_simple,
        lambda df: df.sort_values(by=[by], ascending=ascending),
    )


@sql_count_checker(query_count=0)
def test_sort_values_unknown_by_negative(native_df_simple):
    snow_df = pd.DataFrame(native_df_simple)
    eval_snowpark_pandas_result(
        snow_df,
        native_df_simple,
        lambda df: df.sort_values("C"),
        expect_exception=True,
    )


@sql_count_checker(query_count=0)
def test_sort_values_on_duplicate_labels_negative():
    # duplicate data labels.
    native_df = native_pd.DataFrame(np.array([[1, 2], [3, 4]]), columns=["A", "A"])
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.sort_values("A"),
        expect_exception=True,
    )

    # duplicate data and index labels.
    native_df = native_pd.DataFrame(
        {"a": [1, 2]}, index=pd.RangeIndex(start=4, stop=6, step=1, name="a")
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.sort_values("a"),
        expect_exception=True,
    )


@pytest.mark.parametrize("by", [["A", "A"], ["ind", "ind"]])
@sql_count_checker(query_count=1)
def test_sort_values_duplicate_by(native_df_simple, by):
    snow_df = pd.DataFrame(native_df_simple)
    eval_snowpark_pandas_result(
        snow_df, native_df_simple, lambda df: df.sort_values(by)
    )


@pytest.mark.parametrize("by", [["a"], ["A"], ["a", "A"], ["A", "a"]])
@sql_count_checker(query_count=1)
def test_sort_values_case_sensitive(native_df_simple, by):
    snow_df = pd.DataFrame(native_df_simple)
    eval_snowpark_pandas_result(
        snow_df, native_df_simple, lambda df: df.sort_values(by)
    )


@sql_count_checker(query_count=1)
def test_sort_values_empty_by(native_df_simple):
    snow_df = pd.DataFrame(native_df_simple)
    eval_snowpark_pandas_result(
        snow_df, native_df_simple, lambda df: df.sort_values(by=[])
    )


@pytest.mark.parametrize("by", [["B", "a"], ["A", "ind"], ["A", "timedelta"]])
@sql_count_checker(query_count=3)
def test_sort_values_multiple_by(native_df_simple, by):
    snow_df = pd.DataFrame(native_df_simple)
    eval_snowpark_pandas_result(
        snow_df, native_df_simple, lambda df: df.sort_values(by)
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df_simple,
        lambda df: df.sort_values(by, ascending=False),
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df_simple,
        lambda df: df.sort_values(by, ascending=[True, False]),
    )


@sql_count_checker(query_count=0)
def test_sort_values_by_ascending_length_mismatch_negative(native_df_simple):
    snow_df = pd.DataFrame(native_df_simple)
    eval_snowpark_pandas_result(
        snow_df,
        native_df_simple,
        lambda df: df.sort_values(by=["A", "B"], ascending=[True] * 5),
        expect_exception=True,
    )


@pytest.mark.parametrize(
    "sort_op",
    [
        lambda df: df.sort_values(by=3, axis=1),
        lambda df: df.sort_values(by=3, axis=1, ascending=False),
        lambda df: df.sort_values(by=[1, 2], axis="columns"),
        lambda df: df.sort_values(by=[1, 3], axis=1, ascending=[True, False]),
        lambda df: df.sort_values(by=[1, 3], axis=1, ascending=False),
    ],
)
@sql_count_checker(query_count=0)
def test_sort_values_axis_1(sort_op):
    df = pd.DataFrame(
        [[1, 1, 2], [3, 1, 0], [4, 5, 6]], index=[1, 2, 3], columns=list("ABC")
    )
    msg = "Snowpark pandas sort_values API doesn't yet support axis == 1"
    with pytest.raises(NotImplementedError, match=msg):
        sort_op(df)


@pytest.mark.parametrize(
    "sort_op",
    [
        lambda df: df.sort_values(by=3, axis=1, inplace=True),
        lambda df: df.sort_values(by=3, axis=1, ascending=False, inplace=True),
        lambda df: df.sort_values(by=[1, 2], axis="columns", inplace=True),
        lambda df: df.sort_values(
            by=[1, 3], axis=1, ascending=[True, False], inplace=True
        ),
        lambda df: df.sort_values(by=[1, 3], axis=1, ascending=False, inplace=True),
    ],
)
@sql_count_checker(query_count=0)
def test_sort_values_axis_1_inplace(sort_op):
    df = pd.DataFrame(
        [[1, 1, 2], [3, 1, 0], [4, 5, 6]], index=[1, 2, 3], columns=list("ABC")
    )
    msg = "Snowpark pandas sort_values API doesn't yet support axis == 1"
    with pytest.raises(NotImplementedError, match=msg):
        sort_op(df)


@sql_count_checker(query_count=0)
def test_sort_values_invalid_axis_negative(native_df_simple):
    snow_df = pd.DataFrame(native_df_simple)
    eval_snowpark_pandas_result(
        snow_df,
        native_df_simple,
        lambda df: df.sort_values(by=[], axis=2),
        expect_exception=True,
    )


@pytest.mark.parametrize("by", [["A"], ["A", "B"]])
@pytest.mark.parametrize("ascending", [True, False])
@sql_count_checker(query_count=1)
def test_sort_values_inplace(native_df_simple, by, ascending):
    snow_df = pd.DataFrame(native_df_simple)
    eval_snowpark_pandas_result(
        snow_df,
        native_df_simple,
        lambda df: df.sort_values(by=by, ascending=ascending, inplace=True),
        inplace=True,
    )


@pytest.mark.skip(reason="data type mismatch, enable when SNOW-800907 is resolved")
def test_sort_values_uint64():
    native_df = native_pd.DataFrame(
        {
            "a": pd.Series([18446637057563306014, 1162265347240853609]),
            "b": pd.Series([1, 2]),
        }
    )
    native_df["a"] = native_df["a"].astype(np.uint64)
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.sort_values(["a", "b"])
    )


@pytest.mark.parametrize("ascending", [True, False])
@pytest.mark.parametrize("na_position", ["first", "last"])
@sql_count_checker(query_count=2)
def test_sort_values_nan(ascending, na_position):
    native_df = native_pd.DataFrame(
        {"A": [1, 2, np.nan, 1, 6, 8, 4], "B": [9, np.nan, 5, 2, 5, 4, 5]}
    )
    snow_df = pd.DataFrame(native_df)

    # sort on one column only
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.sort_values(
            by=["A"], ascending=ascending, na_position=na_position
        ),
    )

    # sort on multiple columns
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.sort_values(
            by=["A", "B"], ascending=ascending, na_position=na_position
        ),
    )


@sql_count_checker(query_count=0)
def test_sort_values_invalid_na_position_negative(native_df_simple):
    snow_df = pd.DataFrame(native_df_simple)
    eval_snowpark_pandas_result(
        snow_df,
        native_df_simple,
        lambda df: df.sort_values("A", na_position="nulls_first"),
        expect_exception=True,
    )


@pytest.mark.parametrize("ascending", [True, False])
@pytest.mark.parametrize("kind", ["stable", "mergesort"])
@sql_count_checker(query_count=4)
def test_sort_values_stable(ascending, kind):
    cola_data = [3] * 100 + [2] * 100 + [1] * 100
    colb_data = list(np.arange(100)) * 3
    native_df = native_pd.DataFrame({"a": cola_data, "b": colb_data})
    snow_df = pd.DataFrame({"a": cola_data, "b": colb_data})
    # This test should only issue 6 SQL queries given dataframe creation from large
    # local data: 1) Creating temp table, 2) Setting query tag, 3) Inserting into temp table,
    # 4) Unsetting query tag, 5) Sort Operation, 6) Drop temp table.
    # However, an additional 6 queries are issued to eagerly get the index names.
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.sort_values("a", ascending=ascending, kind=kind),
    )


@pytest.mark.parametrize("ascending", [True, False])
@pytest.mark.parametrize("na_position", ["first", "last"])
@pytest.mark.parametrize("kind", ["stable", "mergesort"])
@sql_count_checker(query_count=1)
def test_sort_values_multi_column_stable(ascending, na_position, kind):
    native_df = native_pd.DataFrame(
        {
            "a": [1, 2, 1, 1, 1, 6, 8, 4, 8, 8, 8, 8],
            "b": [9, 5, 2, 2, 2, 5, 4, 5, 3, 4, 4, 4],
        }
    )
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.sort_values(
            ["a", "b"], ascending=ascending, na_position=na_position, kind=kind
        ),
    )


@pytest.mark.parametrize("ascending", [True, False])
@pytest.mark.parametrize("na_position", ["first", "last"])
@pytest.mark.parametrize("kind", ["stable", "mergesort"])
@sql_count_checker(query_count=1)
def test_sort_values_multi_column_stable_nan(ascending, na_position, kind):
    native_df = native_pd.DataFrame(
        {
            "a": [1, 2, np.nan, 1, 1, 1, 6, 8, 4, 8, 8, np.nan, np.nan, 8, 8],
            "b": [9, np.nan, 5, 2, 2, 2, 5, 4, 5, 3, 4, np.nan, np.nan, 4, 4],
        }
    )
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.sort_values(
            ["a", "b"], ascending=ascending, na_position=na_position, kind=kind
        ),
    )


@sql_count_checker(query_count=0)
def test_sort_values_invalid_kind_negative(native_df_simple):
    invalid_kind = "fastsort"
    msg = r"sort kind must be 'stable' or None \(got 'fastsort'\)"
    snow_df = pd.DataFrame(native_df_simple)
    with pytest.raises(AssertionError, match=msg):
        eval_snowpark_pandas_result(
            snow_df,
            native_df_simple,
            lambda df: df.sort_values("A", kind=invalid_kind),
            expect_exception=True,
        )


@sql_count_checker(query_count=2)
def test_sort_values_datetime():
    native_df = native_pd.DataFrame(
        {
            "A": ["a", "a", "a", "b", "c", "d", "e", "f", "g"],
            "B": [
                native_pd.Timestamp(x)
                for x in [
                    "2004-02-11",
                    "2004-01-21",
                    "2004-01-26",
                    "2005-09-20",
                    "2010-10-04",
                    "2009-05-12",
                    "2008-11-12",
                    "2010-09-28",
                    "2010-09-28",
                ]
            ],
        }
    )

    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.sort_values(by="B"))
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.sort_values(by=["B", "A"])
    )


@pytest.mark.skip(reason="SNOW-824304 Add support for DateTime type")
def test_sort_nat():
    col1 = [
        native_pd.Timestamp(x)
        for x in ["2016-01-01", "2015-01-01", np.nan, "2016-01-01"]
    ]
    col2 = [
        native_pd.Timestamp(x)
        for x in ["2017-01-01", "2014-01-01", "2016-01-01", "2015-01-01"]
    ]
    native_df = native_pd.DataFrame({"a": col1, "b": col2}, index=[0, 1, 2, 3])
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.sort_values(by=["a", "b"])
    )


@pytest.mark.parametrize("ascending", [True, False])
@pytest.mark.parametrize("ignore_index", [True, False])
@sql_count_checker(query_count=2)
def test_sort_values_ignore_index(native_df_simple, ascending, ignore_index):
    snow_df = pd.DataFrame(native_df_simple)
    eval_snowpark_pandas_result(
        snow_df,
        native_df_simple,
        lambda df: df.sort_values("A", ascending=ascending, ignore_index=ignore_index),
    )
    eval_snowpark_pandas_result(
        snow_df,
        native_df_simple,
        lambda df: df.sort_values(
            ["A", "B"], ascending=ascending, ignore_index=ignore_index
        ),
    )


@pytest.mark.parametrize(
    "op",
    [
        lambda df: df.sort_values(by="A", key=lambda x: x + 5),
        lambda df: df.sort_values(by="A", key=lambda x: -x),
    ],
)
@sql_count_checker(query_count=0)
def test_sort_values_key(native_df_simple, op):
    snow_df = pd.DataFrame(native_df_simple)
    msg = "Snowpark pandas sort_values API doesn't yet support 'key' parameter"
    with pytest.raises(NotImplementedError, match=msg):
        op(snow_df)


@pytest.mark.parametrize("label", VALID_PANDAS_LABELS)
@sql_count_checker(query_count=1)
def test_sort_values_labels(label):
    native_df = native_pd.DataFrame({label: ["a", "z", "b", "x"], "abc": [1, 2, 3, 4]})
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.sort_values(label))


@sql_count_checker(query_count=1)
def test_sort_values_repeat(native_df_simple):
    snow_df = pd.DataFrame(native_df_simple)
    eval_snowpark_pandas_result(
        snow_df,
        native_df_simple,
        lambda df: df.sort_values(by="A").sort_values(by="a"),
    )


@pytest.mark.parametrize("by", [None, [None], [None, "a"]])
@sql_count_checker(query_count=0)
def test_sort_values_by_is_none_negative(native_df_simple, by):
    snow_df = pd.DataFrame(native_df_simple)
    eval_snowpark_pandas_result(
        snow_df,
        native_df_simple,
        lambda df: df.sort_values(by=by),
        expect_exception=True,
    )
