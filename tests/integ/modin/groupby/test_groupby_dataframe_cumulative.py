#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.utils import assert_frame_equal, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.fixture(scope="function")
def native_df_multiindex() -> native_pd.DataFrame:
    tuples = list(
        zip(
            *[
                ["bar", "bar", "baz", "baz", "foo", "foo", "qux", "qux"],
                ["one", "two", "one", "two", "one", "two", "one", "two"],
            ]
        )
    )
    index = native_pd.MultiIndex.from_tuples(tuples)
    columns = native_pd.MultiIndex.from_tuples(
        [("A", "cat"), ("B", "dog"), ("B", "cat"), ("A", "dog")]
    )
    # Generate seeded random data.
    rng = np.random.default_rng(12345)
    data = rng.random(size=(8, 4))
    native_df = native_pd.DataFrame(data, index=index, columns=columns)
    return native_df


@pytest.mark.parametrize(
    "by",
    [
        "LEXLUTHOR",
        ["LOBO", "DARKSEID"],
        "DARKSEID",
    ],
)
@pytest.mark.parametrize("func_name", ["cumsum", "cummin", "cummax"])
@pytest.mark.parametrize("dropna", [True, False])
@pytest.mark.parametrize("as_index", [True, False])
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize("group_keys", [True, False])
def test_groupby_cumulative(by, func_name, dropna, as_index, sort, group_keys):
    pandas_df = native_pd.DataFrame(
        data=[
            [1, 2, 3, 42.42, 42.42],
            [1, 5, 6, 55.55, 55.55],
            [2, 5, 8, None, None],
            [2, 6, 9, 90099.95, 90099.95],
            [None, 7, 10, 888.88, 888.88],
        ],
        columns=["LEXLUTHOR", "LOBO", "DARKSEID", "RATING", "RATING"],
        index=["tuna", "salmon", "catfish", "goldfish", "shark"],
    )
    snow_df = pd.DataFrame(pandas_df)
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            pandas_df,
            lambda df: getattr(
                df.groupby(
                    by=by,
                    dropna=dropna,
                    as_index=as_index,
                    sort=sort,
                    group_keys=group_keys,
                ),
                func_name,
            )(numeric_only=True),
        )


@pytest.mark.parametrize(
    "by",
    [
        "LEXLUTHOR",
        ["LOBO", "DARKSEID"],
        ["DARKSEID", "FRIENDS"],
        "FRIENDS",
    ],
)
@pytest.mark.parametrize("dropna", [True, False])
@pytest.mark.parametrize("as_index", [True, False])
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize("group_keys", [True, False])
@pytest.mark.parametrize("ascending", [True, False])
def test_groupby_cumcount(by, dropna, as_index, sort, group_keys, ascending):
    pandas_df = native_pd.DataFrame(
        data=[
            [1, 2, 3, "Lois", 42.42, 42.42],
            [1, 5, 6, "Lana", 55.55, 55.55],
            [2, 5, 8, "Luma", None, None],
            [2, 6, 9, None, 90099.95, 90099.95],
            [None, 7, 10, "Cat", 888.88, 888.88],
        ],
        columns=["LEXLUTHOR", "LOBO", "DARKSEID", "FRIENDS", "RATING", "RATING"],
        index=["tuna", "salmon", "catfish", "goldfish", "shark"],
    )
    snow_df = pd.DataFrame(pandas_df)
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            pandas_df,
            lambda df: df.groupby(
                by=by,
                dropna=dropna,
                as_index=as_index,
                sort=sort,
                group_keys=group_keys,
            ).cumcount(ascending=ascending),
        )


@pytest.mark.parametrize(
    "by",
    [
        ("A", "cat"),
    ],
)
@pytest.mark.parametrize("dropna", [True, False])
@pytest.mark.parametrize("as_index", [True, False])
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize("group_keys", [True, False])
@pytest.mark.parametrize("ascending", [True, False])
def test_groupby_cumcount_with_multiindex(
    native_df_multiindex, by, dropna, as_index, sort, group_keys, ascending
):
    snow_df = pd.DataFrame(native_df_multiindex)
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            native_df_multiindex,
            lambda df: df.groupby(
                by=by,
                dropna=dropna,
                as_index=as_index,
                sort=sort,
                group_keys=group_keys,
            ).cumcount(ascending=ascending),
        )


@pytest.mark.parametrize(
    "by",
    [
        1,
        [2, 3],
        [3, 4],
        4,
    ],
)
@pytest.mark.parametrize("dropna", [True, False])
@pytest.mark.parametrize("as_index", [True, False])
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize("group_keys", [True, False])
@pytest.mark.parametrize("ascending", [True, False])
def test_groupby_cumcount_with_numeric_names(
    by, dropna, as_index, sort, group_keys, ascending
):
    pandas_df = native_pd.DataFrame(
        data=[
            [1, 2, 3, "Lois", 42.42, 42.42],
            [1, 5, 6, "Lana", 55.55, 55.55],
            [2, 5, 8, "Luma", None, None],
            [2, 6, 9, None, 90099.95, 90099.95],
            [None, 7, 10, "Cat", 888.88, 888.88],
        ],
        columns=[1, 2, 3, 4, 5, 5],
        index=["tuna", "salmon", "catfish", "goldfish", "shark"],
    )
    snow_df = pd.DataFrame(pandas_df)
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            pandas_df,
            lambda df: df.groupby(
                by=by,
                dropna=dropna,
                as_index=as_index,
                sort=sort,
                group_keys=group_keys,
            ).cumcount(ascending=ascending),
        )


@pytest.mark.parametrize(
    "by, level, axis, error",
    [
        # RATING is duplicate
        ("RATING", None, 0, ValueError),
        # by and level are set
        ("LEXLUTHOR", 0, 0, NotImplementedError),
        # non-zero level
        (None, -1, 0, NotImplementedError),
        # non-zero axis
        ("LEXLUTHOR", 0, 1, NotImplementedError),
    ],
)
@sql_count_checker(query_count=0)
def test_groupby_cumcount_negative(by, level, axis, error):
    pandas_df = native_pd.DataFrame(
        data=[
            [1, 2, 3, "Lois", 42.42, 42.42],
            [1, 5, 6, "Lana", 55.55, 55.55],
            [2, 5, 8, "Luma", None, None],
            [2, 6, 9, None, 90099.95, 90099.95],
            [None, 7, 10, "Cat", 888.88, 888.88],
        ],
        columns=["LEXLUTHOR", "LOBO", "DARKSEID", "FRIENDS", "RATING", "RATING"],
        index=["tuna", "salmon", "catfish", "goldfish", "shark"],
    )
    snow_df = pd.DataFrame(pandas_df)
    with pytest.raises(error):
        eval_snowpark_pandas_result(
            snow_df,
            pandas_df,
            lambda df: df.groupby(
                by=by,
                level=level,
                axis=axis,
                dropna=True,
                as_index=True,
                sort=True,
                group_keys=True,
            ).cumcount(ascending=True),
        )


@pytest.mark.parametrize(
    "func_name, query_count", [("cumsum", 0), ("cummin", 1), ("cummax", 1)]
)
def test_groupby_cumlative_non_numeric(func_name, query_count):
    pandas_df = native_pd.DataFrame(
        data=[
            [1, 2, 3, "Lois", 42.42, 42.42],
            [1, 5, 6, "Lana", 55.55, 55.55],
            [2, 5, 8, "Luma", None, None],
            [2, 6, 9, None, 90099.95, 90099.95],
            [None, 7, 10, "Cat", 888.88, 888.88],
        ],
        columns=["LEXLUTHOR", "LOBO", "DARKSEID", "FRIENDS", "RATING", "RATING"],
        index=["tuna", "salmon", "catfish", "goldfish", "shark"],
    )
    snow_df = pd.DataFrame(pandas_df)

    with pytest.raises(NotImplementedError):
        # All of cummax, cummin, and cumsum raise NotImplementedError
        # for non-numeric columns in native pandas.
        # Note that numeric_only is False by default for cummax and cummin,
        # and is not a valid argument for cumsum.
        getattr(pandas_df.groupby(by="LEXLUTHOR"), func_name)()

    with SqlCounter(query_count=query_count):
        if func_name == "cumsum":
            # cumsum raises SnowparkSQLException
            # for non-numeric columns in Snowpark pandas.
            with pytest.raises(SnowparkSQLException):
                getattr(snow_df.groupby(by="LEXLUTHOR"), func_name)().to_pandas()
        else:
            # cummax and cummin succeed on non-numeric columns in Snowpark pandas.
            snow_df = getattr(snow_df.groupby(by="LEXLUTHOR"), func_name)().drop(
                columns=["FRIENDS"]
            )
            pandas_df = getattr(pandas_df.groupby(by="LEXLUTHOR"), func_name)(
                numeric_only=True
            )
            assert_frame_equal(
                snow_df,
                pandas_df,
                check_dtype=False,
            )
