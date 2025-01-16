#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.mark.parametrize(
    "df,groupby_columns",
    [
        (
            native_pd.DataFrame(
                {
                    "id": ["spam", "egg", "egg", "spam", "ham", "ham"],
                    "value1": [1, 5, 5, 2, 5, 5],
                    "value2": list("abbaxy"),
                }
            ),
            "id",
        ),
        (
            native_pd.DataFrame(
                {
                    "id": ["spam", None, "egg", "egg", "spam", "ham", "ham", None],
                    "value1": [1, None, 5, None, 5, 2, 5, 5],
                    "value2": list("abbaxy") + [None, None],
                }
            ),
            "id",
        ),
    ],
)
@pytest.mark.parametrize("dropna", [True, False])
@sql_count_checker(query_count=3)
def test_groupby_nunique(df, groupby_columns, dropna):
    snow_df = pd.DataFrame(df)

    # Test using nunique via agg
    eval_snowpark_pandas_result(
        snow_df,
        df,
        lambda df: df.groupby(groupby_columns).agg("nunique", dropna=dropna),
        # Some calls to the native pandas function propagate attrs while some do not, depending on the values of its arguments.
        test_attrs=False,
    )

    # Test invoking nunique directly
    eval_snowpark_pandas_result(
        snow_df,
        df,
        lambda df: df.groupby(groupby_columns).nunique(dropna),
        test_attrs=False,
    )

    # Test invoking per column.
    if dropna is False:
        # pandas does not respect its own documentation here, when passing in dropna together with a dict of functions/names
        # the parameter is not passed. Snowpark pandas API follows pandas documentation here.
        snow_ans = snow_df.groupby(groupby_columns).agg(
            {"value1": "count", "value2": "nunique"}, dropna=dropna
        )
        count_column = df[["id", "value1"]].groupby(groupby_columns).count()
        nunique_column = (
            df[["id", "value2"]].groupby(groupby_columns).nunique(dropna=False)
        )
        expected_ans = native_pd.merge(
            count_column, nunique_column, left_index=True, right_index=True
        )
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snow_ans, expected_ans
        )
    else:
        eval_snowpark_pandas_result(
            snow_df,
            df,
            lambda df: df.groupby(groupby_columns).agg(
                {"value1": "count", "value2": "nunique"}, dropna=dropna
            ),
            test_attrs=False,
        )


@pytest.mark.parametrize("by", ["A", "B", ["A", "B"]])
@sql_count_checker(query_count=1)
def test_timedelta(by):
    native_df = native_pd.DataFrame(
        {
            "A": native_pd.to_timedelta(
                ["1 days 06:05:01.00003", "16us", "nan", "16us"]
            ),
            "B": [8, 8, 12, 10],
            "C": ["the", "name", "is", "bond"],
        }
    )
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby(by).nunique(),
    )
