#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""
* test query complexity score calculation is correct --unit
* skip when transaction is enabled
* skip for views and dynamic tables
* set high upper bound for complexity score see no breakdown
* set low upper bound for complexity score see breakdown
* add test for valid node to breakdown
* add test to see post actions have delete temp tables
* add async test for large query breakdown
* updating complexity bounds change num partitions
* test large query breakdown works with cte optimized plan
"""

import pytest


@pytest.fixture(autouse=True)
def setup(session):
    is_large_query_breakdown_enabled = session.large_query_breakdown_enabled
    session._large_query_breakdown_enabled = True
    yield
    session._large_query_breakdown_enabled = is_large_query_breakdown_enabled
