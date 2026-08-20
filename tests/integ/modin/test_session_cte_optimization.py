#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
import threading
from contextlib import contextmanager
from typing import Any, Generator

import modin.pandas as pd
import snowflake.snowpark.modin.plugin  # noqa: F401

from snowflake.snowpark.session import _get_active_session

from tests.integ.utils.sql_counter import sql_count_checker


@contextmanager
def session_parameter_override(
    session: Any, parameter_name: str, value: Any
) -> Generator[None, None, None]:
    """Context manager to temporarily override a session parameter and restore it afterwards."""
    original_value = getattr(session, parameter_name)
    setattr(session, parameter_name, value)
    try:
        yield
    finally:
        setattr(session, parameter_name, original_value)


@sql_count_checker(query_count=0)
def test_cte_optimization_for_snowpark_pandas(caplog):
    session = _get_active_session()

    caplog.set_level(logging.WARNING)

    # The multithreading warning is emitted only while another thread is alive.
    stop = threading.Event()
    extra_thread = threading.Thread(
        target=stop.wait, name="cte-opt-warning-probe", daemon=True
    )
    extra_thread.start()
    try:
        assert len(threading.enumerate()) > 1

        with session_parameter_override(session, "cte_optimization_enabled", False):
            assert session.cte_optimization_enabled is False

            caplog.clear()

            assert pd.session == session
            assert pd.session.cte_optimization_enabled is True

            assert (
                "You might have more than one threads sharing the Session object trying to update"
                not in caplog.text
            )
    finally:
        stop.set()
        extra_thread.join()
