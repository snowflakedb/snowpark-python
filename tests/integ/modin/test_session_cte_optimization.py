#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.session import _get_active_session

from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=0)
def test_cte_optimization_for_snowpark_pandas(db_parameters):
    session = _get_active_session()

    # Preserve original setting
    cte_optimization_original = session.cte_optimization_enabled

    # Ensure cte optimization is disabled for current session
    session.cte_optimization_enabled = False
    assert session.cte_optimization_enabled is False

    # Import Snowpark pandas
    import modin.pandas as pd
    import snowflake.snowpark.modin.plugin  # noqa: F401

    # Verify that cte optimization will now be enabled
    assert pd.session == session
    assert pd.session.cte_optimization_enabled is True

    # Revert to original setting
    session.cte_optimizatin_enabled = cte_optimization_original
    assert session.cte_optimizatin_enabled == cte_optimization_original
