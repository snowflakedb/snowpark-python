#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import Any, Union

import pandas as native_pd
import pytest
from modin.pandas import DataFrame, Series

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter


def eval_result_and_query_with_no_join(
    snow: Union[DataFrame, Series],
    native: Union[native_pd.DataFrame, native_pd.Series],
    **kwargs: Any
) -> None:
    """
    Verify no join in the produced query for the result dataframe.
    """

    sql = snow._query_compiler._modin_frame.ordered_dataframe.queries["queries"][-1]
    assert "JOIN" not in sql
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow, native, **kwargs)


@pytest.mark.parametrize(
    "n",
    [1, None, 0, -1, -10, 5, 10],
)
def test_head_tail(n, default_index_snowpark_pandas_df, default_index_native_df):
    expected_query_count = 2 if n == 0 else 3
    with SqlCounter(query_count=expected_query_count):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df,
            default_index_native_df,
            lambda df: (df.head() if n is None else df.head(n)),
            comparator=eval_result_and_query_with_no_join,
        )

        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df,
            default_index_native_df,
            lambda df: (df.tail() if n is None else df.tail(n)),
            comparator=eval_result_and_query_with_no_join,
        )


@pytest.mark.parametrize(
    "n",
    [1, None, 0, -1, -10, 5, 10],
)
def test_empty_dataframe(n, empty_snowpark_pandas_df):
    expected_query_count = 2 if n == 0 else 3
    with SqlCounter(query_count=expected_query_count):
        eval_snowpark_pandas_result(
            empty_snowpark_pandas_df,
            native_pd.DataFrame(),
            lambda df: (df.head() if n is None else df.head(n)),
            comparator=eval_result_and_query_with_no_join,
            check_column_type=False,
        )

        eval_snowpark_pandas_result(
            empty_snowpark_pandas_df,
            native_pd.DataFrame(),
            lambda df: (df.tail() if n is None else df.tail(n)),
            comparator=eval_result_and_query_with_no_join,
            check_column_type=False,
        )
