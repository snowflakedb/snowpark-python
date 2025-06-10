#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

from tests.integ.modin.frame.test_head_tail import eval_result_and_query_with_no_join
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.mark.parametrize(
    "n",
    [1, None, 0, -1, -10, 5, 10],
)
@sql_count_checker(query_count=2)
def test_head_tail(
    n, default_index_snowpark_pandas_series, default_index_native_series
):
    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_series,
        default_index_native_series,
        lambda df: (df.head() if n is None else df.head(n)),
        comparator=eval_result_and_query_with_no_join,
    )

    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_series,
        default_index_native_series,
        lambda df: (df.tail() if n is None else df.tail(n)),
        comparator=eval_result_and_query_with_no_join,
    )


@pytest.mark.parametrize(
    "n",
    [1, None, 0, -1, -10, 5, 10],
)
@sql_count_checker(query_count=2)
def test_empty_dataframe(n, empty_snowpark_pandas_series, empty_pandas_series):
    eval_snowpark_pandas_result(
        empty_snowpark_pandas_series,
        empty_pandas_series,
        lambda df: (df.head() if n is None else df.head(n)),
        comparator=eval_result_and_query_with_no_join,
    )

    eval_snowpark_pandas_result(
        empty_snowpark_pandas_series,
        empty_pandas_series,
        lambda df: (df.tail() if n is None else df.tail(n)),
        comparator=eval_result_and_query_with_no_join,
    )
