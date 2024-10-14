#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import modin.pandas as pd
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


@pytest.mark.parametrize(
    "ops",
    [
        lambda df: df.head(),
        lambda df: df.iloc[1:100],
        lambda df: df.iloc[1000:100:-1],
    ],
)
@sql_count_checker(query_count=1)
def test_head_efficient_sql(session, ops):
    df = pd.Series({"a": [1] * 10000})
    with session.query_history() as query_listener:
        ops(df).to_pandas()
    eval_query = query_listener.queries[-1].sql_text.lower()
    # check no row count
    assert "count" not in eval_query
    # check orderBy behinds limit
    if session.sql_simplifier_enabled:
        # TODO SNOW-1731549 fix this issue in sql simplifier
        assert eval_query.index("limit") > eval_query.index("order by")
    else:
        assert "count" not in eval_query
        assert eval_query.index("limit") < eval_query.index("order by")
