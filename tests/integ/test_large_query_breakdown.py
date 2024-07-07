#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark.functions import avg, col, lit

"""
1. Sorted list is working correctly
2. Sort if broken down correctly
    i. Inner sort is materialized
    ii. Outer sort is not materialized
3. When there is no PIPELINE_BREAKER, the query is not broken down
4. Breakdown parameters can be set at runtime.
"""

pytestmark = [
    pytest.mark.xfail(
        "config.getoption('local_testing_mode', default=False)",
        reason="Breaking down queries is done for SQL translation",
        run=False,
    )
]


def test_large_query_breakdown(session):
    # TODO: add one testcase with UNION + SORT AT THE OUTER MOST LEVEL to test correctness
    base_df = session.sql("select 1 as A, 2 as B")
    with_columns_set_1 = base_df.with_column("A", col("A") + lit(1))
    with_columns_set_2 = base_df.with_column("B", col("B") + lit(1))
    x = 100
    for i in range(x):
        with_columns_set_1 = with_columns_set_1.with_column("A", col("A") + lit(i))

    for i in range(x):
        with_columns_set_2 = with_columns_set_2.with_column("B", col("B") + lit(i))

    with_columns_set_1 = with_columns_set_1.group_by(col("A")).agg(
        avg(col("B")).alias("B")
    )
    with_columns_set_2 = with_columns_set_2.group_by(col("B")).agg(
        avg(col("A")).alias("A")
    )
    union_df = with_columns_set_1.union(with_columns_set_2)

    final_df = union_df.with_column("A", col("A") + lit(1))
    final_df.collect()
