#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result, create_test_dfs
import re
from tests.integ.utils.sql_counter import sql_count_checker
import json


@pytest.mark.parametrize(
    "by,level",
    [
        ("animal", None),
        (["animal"], None),
        (["animal", "height_in"], None),
        (None, (0, 1)),
        (None, (0,)),
        (None, "animal_index"),
    ],
)
@pytest.mark.parametrize("as_index", [True, False])
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize("dropna", [True, False])
@pytest.mark.parametrize("axis", ["index", 0])
@sql_count_checker(query_count=1)
def test_all_params(by, level, as_index, sort, dropna, axis):
    eval_snowpark_pandas_result(
        *create_test_dfs(
            [
                ("Chihuahua", "dog", 6.1),
                ("Beagle", "dog", 15.2),
                ("Chihuahua", "dog", 6.9),
                ("Persian", "cat", 9.2),
                ("Persian", None, None),
                ("Chihuahua", "dog", 7),
                ("Persian", "cat", 8.8),
            ],
            columns=["breed", "animal", "height_in"],
            index=native_pd.MultiIndex.from_tuples(
                [
                    ("c", "d"),
                    ("b", "d"),
                    ("c", "d"),
                    ("p", "c"),
                    (None, None),
                    ("c", "d"),
                    ("p", "c"),
                ],
                names=("breed_index", "animal_index"),
            ),
        ),
        lambda df: df.groupby(
            by=by, level=level, as_index=as_index, sort=sort, dropna=dropna, axis=axis
        )["breed"].unique(),
        # pandas fails to propagate attrs through SeriesGroupBy.unique()
        test_attrs=False
    )


@pytest.mark.xfail(
    strict=True, raises=json.decoder.JSONDecodeError, reason="SNOW-1859090"
)
def test_aggregating_string_column_with_nulls():
    eval_snowpark_pandas_result(
        *create_test_dfs(
            [
                (
                    "Chihuahua",
                    "dog",
                ),
                (
                    "Beagle",
                    "dog",
                ),
                (
                    "Chihuahua",
                    "dog",
                ),
                (
                    "Persian",
                    "cat",
                ),
                ("Persian", "cat"),
                ("Chihuahua", "dog"),
                (None, "cat"),
            ],
            columns=["breed", "animal"],
        ),
        lambda df: df.groupby("animal")["breed"].unique(),
        # pandas fails to propagate attrs through SeriesGroupBy.unique()
        test_attrs=False
    )


@sql_count_checker(query_count=0)
def test_axis_1():
    eval_snowpark_pandas_result(
        *create_test_dfs([["a", "a"], ["c", "d"]]),
        lambda df: df.groupby(0, axis=1)[1],
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match=re.escape(r"Cannot subset columns when using axis=1")
    )
