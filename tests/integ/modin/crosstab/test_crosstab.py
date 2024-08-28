#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import eval_snowpark_pandas_result


@pytest.mark.parametrize("dropna", [True, False])
class TestCrosstab:
    def test_basic_crosstab_with_numpy_arrays(self, dropna):
        query_count = 1
        join_count = 0 if dropna else 1
        a = np.array(
            [
                "foo",
                "foo",
                "foo",
                "foo",
                "bar",
                "bar",
                "bar",
                "bar",
                "foo",
                "foo",
                "foo",
            ],
            dtype=object,
        )
        b = np.array(
            [
                "one",
                "one",
                "one",
                "two",
                "one",
                "one",
                "one",
                "two",
                "two",
                "two",
                "one",
            ],
            dtype=object,
        )
        c = np.array(
            [
                "dull",
                "dull",
                "shiny",
                "dull",
                "dull",
                "shiny",
                "shiny",
                "dull",
                "shiny",
                "shiny",
                "shiny",
            ],
            dtype=object,
        )
        with SqlCounter(query_count=query_count, join_count=join_count):
            eval_snowpark_pandas_result(
                pd,
                native_pd,
                lambda lib: lib.crosstab(
                    a, [b, c], rownames=["a"], colnames=["b", "c"], dropna=dropna
                ),
            )

    def test_basic_crosstab_with_numpy_arrays_different_lengths(self, dropna):
        a = np.array(
            [
                "foo",
                "foo",
                "foo",
                "foo",
                "bar",
                "bar",
                "bar",
                "bar",
                "foo",
                "foo",
            ],
            dtype=object,
        )
        b = np.array(
            [
                "one",
                "one",
                "one",
                "two",
                "one",
                "two",
                "two",
                "two",
                "one",
            ],
            dtype=object,
        )
        c = np.array(
            [
                "dull",
                "dull",
                "shiny",
                "dull",
                "dull",
                "shiny",
                "shiny",
                "shiny",
            ],
            dtype=object,
        )
        with SqlCounter(query_count=0):
            eval_snowpark_pandas_result(
                pd,
                native_pd,
                lambda lib: lib.crosstab(
                    a, [b, c], rownames=["a"], colnames=["b", "c"], dropna=dropna
                ),
                assert_exception_equal=True,
                expect_exception=True,
                expect_exception_match="All arrays must be of the same length",
                expect_exception_type=ValueError,
            )

    # In these tests, `overlap` refers to the intersection of the indices
    # of the Series objects being passed in to crosstab. crosstab takes
    # only the intersection of the index objects of all Series when determining
    # the final DataFrame to pass into pivot_table, so here, we are testing
    # that we follow that behavior.
    def test_basic_crosstab_with_series_objs_full_overlap(self, dropna):
        # In this case, all indexes are identical - hence "full" overlap.
        query_count = 2
        join_count = 5 if dropna else 10
        a = np.array(
            [
                "foo",
                "foo",
                "foo",
                "foo",
                "bar",
                "bar",
                "bar",
                "bar",
                "foo",
                "foo",
                "foo",
            ],
            dtype=object,
        )
        b = native_pd.Series(
            [
                "one",
                "one",
                "one",
                "two",
                "one",
                "one",
                "one",
                "two",
                "two",
                "two",
                "one",
            ],
        )
        c = native_pd.Series(
            [
                "dull",
                "dull",
                "shiny",
                "dull",
                "dull",
                "shiny",
                "shiny",
                "dull",
                "shiny",
                "shiny",
                "shiny",
            ],
        )

        def eval_func(lib):
            if lib is pd:
                return lib.crosstab(
                    a,
                    [lib.Series(b), lib.Series(c)],
                    rownames=["a"],
                    colnames=["b", "c"],
                    dropna=dropna,
                )
            else:
                return lib.crosstab(
                    a, [b, c], rownames=["a"], colnames=["b", "c"], dropna=dropna
                )

        with SqlCounter(query_count=query_count, join_count=join_count):
            eval_snowpark_pandas_result(pd, native_pd, eval_func)

    def test_basic_crosstab_with_series_objs_some_overlap(self, dropna):
        # In this case, some values are shared across indexes (non-zero intersection),
        # hence "some" overlap.
        # When a mix of Series and non-Series objects are passed in, the non-Series
        # objects are expected to have the same length as the intersection of the indexes
        # of the Series objects. This test case passes because we pass in arrays that
        # are the length of the intersection rather than the length of each of the Series.
        query_count = 2
        join_count = 5 if dropna else 10
        a = np.array(
            [
                "foo",
                "foo",
                "foo",
                "foo",
                "bar",
                "bar",
                "bar",
                "bar",
                "foo",
                "foo",
                "foo",
            ],
            dtype=object,
        )
        b = native_pd.Series(
            [
                "one",
                "one",
                "one",
                "two",
                "one",
                "one",
                "one",
                "two",
                "two",
                "two",
                "one",
            ],
            index=list(range(len(a))),
        )
        c = native_pd.Series(
            [
                "dull",
                "dull",
                "shiny",
                "dull",
                "dull",
                "shiny",
                "shiny",
                "dull",
                "shiny",
                "shiny",
                "shiny",
            ],
            index=-1 * np.array(list(range(len(a)))),
        )

        # All columns have to be the same length (if NumPy arrays are present, then
        # pandas errors if they do not match the length of the other Series after
        # they are joined (i.e. filtered so that their indices are the same)). In
        # this test, we truncate the numpy column so that the lengths are correct.
        def eval_func(args_list):
            a, b, c = args_list
            if isinstance(b, native_pd.Series):
                return native_pd.crosstab(
                    a[:1], [b, c], rownames=["a"], colnames=["b", "c"], dropna=dropna
                )
            else:
                return pd.crosstab(
                    a[:1], [b, c], rownames=["a"], colnames=["b", "c"], dropna=dropna
                )

        with SqlCounter(query_count=query_count, join_count=join_count):
            native_args = [a, b, c]
            snow_args = [a, pd.Series(b), pd.Series(c)]
            eval_snowpark_pandas_result(
                snow_args,
                native_args,
                eval_func,
            )

    @sql_count_checker(query_count=1, join_count=1)
    def test_basic_crosstab_with_series_objs_some_overlap_error(self, dropna):
        # Same as above - the intersection of the indexes of the Series objects
        # is non-zero, but the indexes are not identical - hence "some" overlap.
        # When a mix of Series and non-Series objects are passed in, the non-Series
        # objects are expected to have the same length as the intersection of the indexes
        # of the Series objects. This test case errors because we pass in arrays that
        # are the length of the Series, rather than the length of the intersection of
        # the indexes of the Series.
        a = np.array(
            [
                "foo",
                "foo",
                "foo",
                "foo",
                "bar",
                "bar",
                "bar",
                "bar",
                "foo",
                "foo",
                "foo",
            ],
            dtype=object,
        )
        b = native_pd.Series(
            [
                "one",
                "one",
                "one",
                "two",
                "one",
                "one",
                "one",
                "two",
                "two",
                "two",
                "one",
            ],
            index=list(range(len(a))),
        )
        c = native_pd.Series(
            [
                "dull",
                "dull",
                "shiny",
                "dull",
                "dull",
                "shiny",
                "shiny",
                "dull",
                "shiny",
                "shiny",
                "shiny",
            ],
            index=-1 * np.array(list(range(len(a)))),
        )

        # All columns have to be the same length (if NumPy arrays are present, then
        # pandas errors if they do not match the length of the other Series after
        # they are joined (i.e. filtered so that their indices are the same))
        def eval_func(args_list):
            a, b, c = args_list
            if isinstance(b, native_pd.Series):
                return native_pd.crosstab(
                    a, [b, c], rownames=["a"], colnames=["b", "c"], dropna=dropna
                )
            else:
                return pd.crosstab(
                    a, [b, c], rownames=["a"], colnames=["b", "c"], dropna=dropna
                )

        native_args = [a, b, c]
        snow_args = [a, pd.Series(b), pd.Series(c)]
        eval_snowpark_pandas_result(
            snow_args,
            native_args,
            eval_func,
            expect_exception=True,
            expect_exception_match=re.escape(
                "Length mismatch: Expected 11 rows, received array of length 1"
            ),
            expect_exception_type=ValueError,
            assert_exception_equal=False,  # Our error message is a little different.
        )

    @sql_count_checker(query_count=1, join_count=1)
    def test_basic_crosstab_with_series_objs_no_overlap_error(self, dropna):
        # In this case, no values are shared across the indexes - the intersection is an
        # empty set - hence "no" overlap. We error here for the same reason as above - the
        # arrays passed in should also be empty, but are non-empty.
        a = np.array(
            [
                "foo",
                "foo",
                "foo",
                "foo",
                "bar",
                "bar",
                "bar",
                "bar",
                "foo",
                "foo",
                "foo",
            ],
            dtype=object,
        )
        b = native_pd.Series(
            [
                "one",
                "one",
                "one",
                "two",
                "one",
                "one",
                "one",
                "two",
                "two",
                "two",
                "one",
            ],
            index=list(range(len(a))),
        )
        c = native_pd.Series(
            [
                "dull",
                "dull",
                "shiny",
                "dull",
                "dull",
                "shiny",
                "shiny",
                "dull",
                "shiny",
                "shiny",
                "shiny",
            ],
            index=-1 - np.array(list(range(len(a)))),
        )

        # All columns have to be the same length (if NumPy arrays are present, then
        # pandas errors if they do not match the length of the other Series after
        # they are joined (i.e. filtered so that their indices are the same))
        def eval_func(args_list):
            a, b, c = args_list
            if isinstance(b, native_pd.Series):
                return native_pd.crosstab(
                    a, [b, c], rownames=["a"], colnames=["b", "c"], dropna=dropna
                )
            else:
                return pd.crosstab(
                    a, [b, c], rownames=["a"], colnames=["b", "c"], dropna=dropna
                )

        native_args = [a, b, c]
        snow_args = [a, pd.Series(b), pd.Series(c)]
        eval_snowpark_pandas_result(
            snow_args,
            native_args,
            eval_func,
            expect_exception=True,
            expect_exception_match=re.escape(
                "Length mismatch: Expected 11 rows, received array of length 0"
            ),
            expect_exception_type=ValueError,
            assert_exception_equal=False,  # Our error message is a little different.
        )

    def test_basic_crosstab_with_df_and_series_objs_pandas_errors(self, dropna):
        query_count = 4
        join_count = 1 if dropna else 3
        a = native_pd.Series(
            [
                "foo",
                "foo",
                "foo",
                "foo",
                "bar",
                "bar",
                "bar",
                "bar",
                "foo",
                "foo",
                "foo",
            ],
            dtype=object,
        )
        b = native_pd.DataFrame(
            {
                "0": [
                    "one",
                    "one",
                    "one",
                    "two",
                    "one",
                    "one",
                    "one",
                    "two",
                    "two",
                    "two",
                    "one",
                ],
                "1": [
                    "dull",
                    "dull",
                    "shiny",
                    "dull",
                    "dull",
                    "shiny",
                    "shiny",
                    "dull",
                    "shiny",
                    "shiny",
                    "shiny",
                ],
            }
        )
        # pandas expects only Series objects, or DataFrames that have only a single column, while
        # we support accepting DataFrames with multiple columns.
        with pytest.raises(
            AssertionError, match="arrays and names must have the same length"
        ):
            native_pd.crosstab(a, b, rownames=["a"], colnames=["b", "c"], dropna=dropna)

        def eval_func(args_list):
            a, b = args_list
            if isinstance(a, native_pd.Series):
                return native_pd.crosstab(
                    a,
                    [b[c] for c in b.columns],
                    rownames=["a"],
                    colnames=["b", "c"],
                    dropna=dropna,
                )
            else:
                return pd.crosstab(
                    a, b, rownames=["a"], colnames=["b", "c"], dropna=dropna
                )

        with SqlCounter(query_count=query_count, join_count=join_count):
            native_args = [a, b]
            snow_args = [pd.Series(a), pd.DataFrame(b)]
            eval_snowpark_pandas_result(
                snow_args,
                native_args,
                eval_func,
            )

    def test_margins(self, dropna):
        query_count = 1
        join_count = 1 if dropna else 2
        union_count = 1
        a = np.array(
            [
                "foo",
                "foo",
                "foo",
                "foo",
                "bar",
                "bar",
                "bar",
                "bar",
                "foo",
                "foo",
                "foo",
            ],
            dtype=object,
        )
        b = np.array(
            [
                "one",
                "one",
                "one",
                "two",
                "one",
                "one",
                "one",
                "two",
                "two",
                "two",
                "one",
            ],
            dtype=object,
        )
        c = np.array(
            [
                "dull",
                "dull",
                "shiny",
                "dull",
                "dull",
                "shiny",
                "shiny",
                "dull",
                "shiny",
                "shiny",
                "shiny",
            ],
            dtype=object,
        )
        with SqlCounter(
            query_count=query_count, join_count=join_count, union_count=union_count
        ):
            eval_snowpark_pandas_result(
                pd,
                native_pd,
                lambda lib: lib.crosstab(
                    a,
                    [b, c],
                    rownames=["a"],
                    colnames=["b", "c"],
                    margins=True,
                    margins_name="MARGINS_NAME",
                    dropna=dropna,
                ),
            )

    @pytest.mark.parametrize("normalize", [0, 1, True, "all", "index", "columns"])
    def test_normalize(self, dropna, normalize):
        query_count = 1 if normalize in (0, "index") else 2
        join_count = 3 if normalize in (0, "index") else 2
        if dropna:
            join_count -= 2
        a = np.array(
            [
                "foo",
                "foo",
                "foo",
                "foo",
                "bar",
                "bar",
                "bar",
                "bar",
                "foo",
                "foo",
                "foo",
            ],
            dtype=object,
        )
        b = np.array(
            [
                "one",
                "one",
                "one",
                "two",
                "one",
                "one",
                "one",
                "two",
                "two",
                "two",
                "one",
            ],
            dtype=object,
        )
        c = np.array(
            [
                "dull",
                "dull",
                "shiny",
                "dull",
                "dull",
                "shiny",
                "shiny",
                "dull",
                "shiny",
                "shiny",
                "shiny",
            ],
            dtype=object,
        )
        with SqlCounter(query_count=query_count, join_count=join_count):
            eval_snowpark_pandas_result(
                pd,
                native_pd,
                lambda lib: lib.crosstab(
                    a,
                    [b, c],
                    rownames=["a"],
                    colnames=["b", "c"],
                    normalize=normalize,
                    dropna=dropna,
                ),
            )

    @pytest.mark.parametrize("normalize", [0, 1, True, "all", "index", "columns"])
    def test_normalize_and_margins(self, dropna, normalize):
        counts = {
            "columns": [3, 5 if dropna else 9, 4],
            "index": [1, 5 if dropna else 8, 3],
            "all": [3, 12 if dropna else 19, 7],
        }
        counts[0] = counts["index"]
        counts[1] = counts["columns"]
        a = np.array(
            [
                "foo",
                "foo",
                "foo",
                "foo",
                "bar",
                "bar",
                "bar",
                "bar",
                "foo",
                "foo",
                "foo",
            ],
            dtype=object,
        )
        b = np.array(
            [
                "one",
                "one",
                "one",
                "two",
                "one",
                "one",
                "one",
                "two",
                "two",
                "two",
                "one",
            ],
            dtype=object,
        )
        c = np.array(
            [
                "dull",
                "dull",
                "shiny",
                "dull",
                "dull",
                "shiny",
                "shiny",
                "dull",
                "shiny",
                "shiny",
                "shiny",
            ],
            dtype=object,
        )
        if normalize is True:
            sql_counts = counts["all"]
        else:
            sql_counts = counts[normalize]
        with SqlCounter(
            query_count=sql_counts[0],
            join_count=sql_counts[1],
            union_count=sql_counts[2],
        ):
            eval_snowpark_pandas_result(
                pd,
                native_pd,
                lambda lib: lib.crosstab(
                    a,
                    [b, c],
                    rownames=["a"],
                    colnames=["b", "c"],
                    normalize=normalize,
                    margins=True,
                    dropna=dropna,
                ),
            )

    @pytest.mark.parametrize("normalize", [0, 1, "index", "columns"])
    @pytest.mark.parametrize("aggfunc", ["count", "mean", "min", "max", "sum"])
    def test_normalize_margins_and_values(self, dropna, normalize, aggfunc):
        counts = {
            "columns": [3, 29 if dropna else 41, 4],
            "index": [1, 23 if dropna else 32, 3],
            "all": [3, 54 if dropna else 75, 7],
        }
        counts[0] = counts["index"]
        counts[1] = counts["columns"]
        a = np.array(
            [
                "foo",
                "foo",
                "foo",
                "foo",
                "bar",
                "bar",
                "bar",
                "bar",
                "foo",
                "foo",
                "foo",
            ],
            dtype=object,
        )
        b = np.array(
            [
                "one",
                "one",
                "one",
                "two",
                "one",
                "one",
                "one",
                "two",
                "two",
                "two",
                "one",
            ],
            dtype=object,
        )
        c = np.array(
            [
                "dull",
                "dull",
                "shiny",
                "dull",
                "dull",
                "shiny",
                "shiny",
                "dull",
                "shiny",
                "shiny",
                "shiny",
            ],
            dtype=object,
        )
        vals = np.array([12, 10, 9, 4, 3, 49, 19, 20, 21, 34, 0])
        if normalize is True:
            sql_counts = counts["all"]
        else:
            sql_counts = counts[normalize]

        def eval_func(lib):
            df = lib.crosstab(
                a,
                [b, c],
                rownames=["a"],
                colnames=["b", "c"],
                values=vals,
                normalize=normalize,
                margins=True,
                dropna=dropna,
                aggfunc=aggfunc,
            )
            if aggfunc == "sum":
                # Thanks to our hack, the rounding is different for sum.
                df = df.round(decimals=6)
            return df

        with SqlCounter(
            query_count=sql_counts[0],
            join_count=sql_counts[1],
            union_count=sql_counts[2],
        ):
            eval_snowpark_pandas_result(
                pd,
                native_pd,
                eval_func,
            )

    @pytest.mark.parametrize("aggfunc", ["count", "mean", "min", "max", "sum"])
    def test_margins_and_values(self, dropna, aggfunc):
        a = np.array(
            [
                "foo",
                "foo",
                "foo",
                "foo",
                "bar",
                "bar",
                "bar",
                "bar",
                "foo",
                "foo",
                "foo",
            ],
            dtype=object,
        )
        b = np.array(
            [
                "one",
                "one",
                "one",
                "two",
                "one",
                "one",
                "one",
                "two",
                "two",
                "two",
                "one",
            ],
            dtype=object,
        )
        c = np.array(
            [
                "dull",
                "dull",
                "shiny",
                "dull",
                "dull",
                "shiny",
                "shiny",
                "dull",
                "shiny",
                "shiny",
                "shiny",
            ],
            dtype=object,
        )
        vals = np.array([12, 10, 9, 4, 3, 49, 19, 20, 21, 34, 0])

        def eval_func(lib):
            df = lib.crosstab(
                a,
                [b, c],
                rownames=["a"],
                colnames=["b", "c"],
                values=vals,
                margins=True,
                dropna=dropna,
                aggfunc=aggfunc,
            )
            return df

        with SqlCounter(
            query_count=1,
            join_count=7 if dropna else 10,
            union_count=1,
        ):
            eval_snowpark_pandas_result(
                pd,
                native_pd,
                eval_func,
            )

    @pytest.mark.parametrize("normalize", [0, 1, True, "all", "index", "columns"])
    @pytest.mark.parametrize("aggfunc", ["count", "mean", "min", "max", "sum"])
    def test_normalize_and_values(self, dropna, normalize, aggfunc):
        counts = {
            "columns": [2, 4 if dropna else 10],
            "index": [1, 5 if dropna else 11],
            "all": [2, 4 if dropna else 10],
        }
        counts[0] = counts["index"]
        counts[1] = counts["columns"]
        a = np.array(
            [
                "foo",
                "foo",
                "foo",
                "foo",
                "bar",
                "bar",
                "bar",
                "bar",
                "foo",
                "foo",
                "foo",
            ],
            dtype=object,
        )
        b = np.array(
            [
                "one",
                "one",
                "one",
                "two",
                "one",
                "one",
                "one",
                "two",
                "two",
                "two",
                "one",
            ],
            dtype=object,
        )
        c = np.array(
            [
                "dull",
                "dull",
                "shiny",
                "dull",
                "dull",
                "shiny",
                "shiny",
                "dull",
                "shiny",
                "shiny",
                "shiny",
            ],
            dtype=object,
        )
        vals = np.array([12, 10, 9, 4, 3, 49, 19, 20, 21, 34, 0])
        if normalize is True:
            sql_counts = counts["all"]
        else:
            sql_counts = counts[normalize]

        def eval_func(lib):
            df = lib.crosstab(
                a,
                [b, c],
                rownames=["a"],
                colnames=["b", "c"],
                values=vals,
                normalize=normalize,
                dropna=dropna,
                aggfunc=aggfunc,
            )
            if aggfunc in ["sum", "max"]:
                # Thanks to our hack, the rounding is different for sum and max.
                df = df.round(decimals=6)
            return df

        with SqlCounter(
            query_count=sql_counts[0],
            join_count=sql_counts[1],
        ):
            eval_snowpark_pandas_result(
                pd,
                native_pd,
                eval_func,
            )

    @pytest.mark.parametrize("normalize", ["all", True])
    @pytest.mark.parametrize("aggfunc", ["count", "mean", "min", "max", "sum"])
    @sql_count_checker(query_count=0)
    def test_normalize_margins_and_values_not_supported(
        self, dropna, normalize, aggfunc
    ):
        a = np.array(
            [
                "foo",
                "foo",
                "foo",
                "foo",
                "bar",
                "bar",
                "bar",
                "bar",
                "foo",
                "foo",
                "foo",
            ],
            dtype=object,
        )
        b = np.array(
            [
                "one",
                "one",
                "one",
                "two",
                "one",
                "one",
                "one",
                "two",
                "two",
                "two",
                "one",
            ],
            dtype=object,
        )
        c = np.array(
            [
                "dull",
                "dull",
                "shiny",
                "dull",
                "dull",
                "shiny",
                "shiny",
                "dull",
                "shiny",
                "shiny",
                "shiny",
            ],
            dtype=object,
        )
        vals = np.array([12, 10, 9, 4, 3, 49, 19, 20, 21, 34, 0])
        with pytest.raises(
            NotImplementedError,
            match='Snowpark pandas does not yet support passing in margins=True, normalize="all", and values.',
        ):
            pd.crosstab(
                a,
                [b, c],
                rownames=["a"],
                colnames=["b", "c"],
                values=vals,
                normalize=normalize,
                margins=True,
                dropna=dropna,
                aggfunc=aggfunc,
            )

    @pytest.mark.parametrize("aggfunc", ["count", "mean", "min", "max", "sum"])
    def test_values(self, dropna, aggfunc):
        query_count = 1
        join_count = 2 if dropna else 5
        native_df = native_pd.DataFrame(
            {
                "species": ["dog", "cat", "dog", "dog", "cat", "cat", "dog", "cat"],
                "favorite_food": [
                    "chicken",
                    "fish",
                    "fish",
                    "beef",
                    "chicken",
                    "beef",
                    "fish",
                    "beef",
                ],
                "age": [7, 2, 8, 5, 9, 3, 6, 1],
            }
        )

        with SqlCounter(query_count=query_count, join_count=join_count):
            eval_snowpark_pandas_result(
                pd,
                native_pd,
                lambda lib: lib.crosstab(
                    native_df["species"].values,
                    native_df["favorite_food"].values,
                    values=native_df["age"].values,
                    aggfunc=aggfunc,
                    dropna=dropna,
                ),
            )

    @pytest.mark.parametrize("aggfunc", ["count", "mean", "min", "max", "sum"])
    def test_values_series_like(self, dropna, aggfunc):
        query_count = 5
        join_count = 2 if dropna else 5
        native_df = native_pd.DataFrame(
            {
                "species": ["dog", "cat", "dog", "dog", "cat", "cat", "dog", "cat"],
                "favorite_food": [
                    "chicken",
                    "fish",
                    "fish",
                    "beef",
                    "chicken",
                    "beef",
                    "fish",
                    "beef",
                ],
                "age": [7, 2, 8, 5, 9, 3, 6, 1],
            }
        )
        snow_df = pd.DataFrame(native_df)

        def eval_func(df):
            if isinstance(df, pd.DataFrame):
                return pd.crosstab(
                    df["species"],
                    df["favorite_food"],
                    values=df["age"],
                    aggfunc=aggfunc,
                    dropna=dropna,
                )
            else:
                return native_pd.crosstab(
                    df["species"],
                    df["favorite_food"],
                    values=df["age"],
                    aggfunc=aggfunc,
                    dropna=dropna,
                )

        with SqlCounter(query_count=query_count, join_count=join_count):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                eval_func,
            )


@sql_count_checker(query_count=0)
def test_values_unsupported_aggfunc():
    native_df = native_pd.DataFrame(
        {
            "species": ["dog", "cat", "dog", "dog", "cat", "cat", "dog", "cat"],
            "favorite_food": [
                "chicken",
                "fish",
                "fish",
                "beef",
                "chicken",
                "beef",
                "fish",
                "beef",
            ],
            "age": [7, 2, 8, 5, 9, 3, 6, 1],
        }
    )

    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas DataFrame.pivot_table does not yet support the aggregation 'median' with the given arguments.",
    ):
        pd.crosstab(
            native_df["species"].values,
            native_df["favorite_food"].values,
            values=native_df["age"].values,
            aggfunc="median",
            dropna=False,
        )


@sql_count_checker(query_count=4)
def test_values_series_like_unsupported_aggfunc():
    # The query count above comes from building the DataFrame
    # that we pass in to pivot table.
    native_df = native_pd.DataFrame(
        {
            "species": ["dog", "cat", "dog", "dog", "cat", "cat", "dog", "cat"],
            "favorite_food": [
                "chicken",
                "fish",
                "fish",
                "beef",
                "chicken",
                "beef",
                "fish",
                "beef",
            ],
            "age": [7, 2, 8, 5, 9, 3, 6, 1],
        }
    )
    snow_df = pd.DataFrame(native_df)

    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas DataFrame.pivot_table does not yet support the aggregation 'median' with the given arguments.",
    ):
        snow_df = pd.crosstab(
            snow_df["species"],
            snow_df["favorite_food"],
            values=snow_df["age"],
            aggfunc="median",
            dropna=False,
        )
