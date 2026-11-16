#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.functions import col, date_part, to_date


@pytest.mark.parametrize(
    "part, expected",
    [("week", (1, 2)), ("weekiso", (1, 2)), ("yearofweekiso", (2024, 2024))],
)
def test_pivot_nullable_integer_values_with_empty_cell(session, part, expected):
    # date_part returns a nullable integer column for these parts.
    df = session.create_dataframe(
        [["2024-01-01", "a"], ["2024-01-08", "a"], ["2024-01-08", "b"]],
        schema=["d", "p"],
    ).select(date_part(part, to_date(col("d"))).alias("v"), col("p"), col("d"))
    assert df.pivot("p", ["a", "b"]).sum("v").sort("d").collect() == [
        Row(D="2024-01-01", A=expected[0], B=None),
        Row(D="2024-01-08", A=expected[1], B=expected[1]),
    ]
