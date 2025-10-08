#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
import pandas as native_pd
import threading

from snowflake.snowpark.session import _get_active_session

from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=0)
def test_cte_optimization_for_snowpark_pandas(db_parameters, caplog):
    session = _get_active_session()

    caplog.set_level(logging.WARNING)

    # Creating a dataframe creates a new thread which triggers the warning
    assert len(threading.enumerate()) == 1
    session.create_dataframe(
        native_pd.DataFrame([[1, 11], [2, 12], [2, 13]], columns=["A", "B"])
    )
    assert len(threading.enumerate()) == 2

    # Preserve original setting
    cte_optimization_original = session.cte_optimization_enabled

    # Ensure cte optimization is disabled for current session
    session.cte_optimization_enabled = False
    assert session.cte_optimization_enabled is False

    caplog.clear()

    # Import Snowpark pandas
    import modin.pandas as pd
    import snowflake.snowpark.modin.plugin  # noqa: F401

    # Verify that cte optimization will now be enabled
    assert pd.session == session
    assert pd.session.cte_optimization_enabled is True

    # Verify that the warning is filtered out
    assert (
        "You might have more than one threads sharing the Session object trying to update"
        not in caplog.text
    )

    # Revert to original setting
    session.cte_optimization_enabled = cte_optimization_original
    assert session.cte_optimization_enabled == cte_optimization_original
