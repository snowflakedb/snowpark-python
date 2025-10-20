#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

import pytest

from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import StringType, IntegerType
from snowflake.snowpark.udf_profiler import UDFProfiler


def multi_thread_helper_function(pro: UDFProfiler):
    pro.set_active_profiler("LINE")
    pro.disable()


@pytest.fixture(scope="function", autouse=True)
def enable_udf_profiler(profiler_session):
    profiler_session.sql(
        "ALTER SESSION SET ENABLE_PYTHON_PROFILER_FOR_UDF = true;"
    ).collect()
    profiler_session.sql(
        "ALTER SESSION SET FEATURE_PYTHON_UDF_PROFILER = 'ENABLED';"
    ).collect()


@pytest.fixture(scope="function", autouse=True)
def setup(profiler_session, resources_path, local_testing_mode):
    if not local_testing_mode:
        profiler_session.add_packages("snowflake-snowpark-python")


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
def test_udf_profiler_basic(profiler_session):
    @udf(
        name="str_udf", replace=True, return_type=StringType(), session=profiler_session
    )
    def str_udf():
        return "success"

    pro = profiler_session.udf_profiler
    pro.register_modules(["str_udf"])

    pro.set_active_profiler("LINE")

    profiler_session.sql("select str_udf()").collect()
    # there is a latency in getting udf profiler result
    time.sleep(5)
    res = pro.get_output()
    pro.disable()

    pro.register_modules([])
    assert res is not None
    assert "Modules Profiled" in res


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
def test_anonymous_udf(profiler_session):
    add_one = udf(
        lambda x: x + 1,
        return_type=IntegerType(),
        input_types=[IntegerType()],
        session=profiler_session,
    )

    pro = profiler_session.udf_profiler
    pro.register_modules(["str_udf"])

    pro.set_active_profiler("LINE")

    df = profiler_session.create_dataframe([[1, 2], [3, 4]]).to_df("a", "b")
    df.select(add_one("a")).collect()
    # there is a latency in getting udf profiler result
    time.sleep(5)
    res = pro.get_output()
    pro.disable()

    pro.register_modules([])
    assert res is not None
    assert "Modules Profiled" in res


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
def test_set_incorrect_active_profiler(profiler_session, caplog):
    with caplog.at_level(logging.WARNING):
        profiler_session.stored_procedure_profiler.set_active_profiler("LINE")
        profiler_session.stored_procedure_profiler.get_output()
    assert (
        "You are seeing this warning because last executed stored procedure or UDF does not exist"
        in caplog.text
    )

    with pytest.raises(ValueError) as e:
        profiler_session.stored_procedure_profiler.set_active_profiler(
            "wrong_active_profiler"
        )
    assert "active_profiler expect 'LINE', 'MEMORY'" in str(e)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
def test_query_history_destroyed_after_finish_profiling(profiler_session):

    profiler_session.udf_profiler.set_active_profiler("LINE")
    assert (
        profiler_session.udf_profiler._query_history
        in profiler_session._conn._query_listeners
    )

    profiler_session.udf_profiler.disable()
    assert (
        profiler_session.udf_profiler._query_history
        not in profiler_session._conn._query_listeners
    )

    profiler_session.udf_profiler.register_modules()


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
def test_thread_safe_on_activate_and_disable(profiler_session):
    pro = profiler_session.udf_profiler
    pro.register_modules(["table_sp"])
    with ThreadPoolExecutor(max_workers=2) as tpe:
        for _ in range(6):
            tpe.submit(multi_thread_helper_function, pro)
    assert pro._query_history is None
    pro.register_modules()


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
def test_set_active_profiler_failed(profiler_session, caplog):
    pro = profiler_session.udf_profiler

    with mock.patch(
        "snowflake.snowpark.DataFrame._internal_collect_with_tag_no_telemetry",
        side_effect=Exception,
    ):
        with caplog.at_level(logging.WARNING):
            pro.set_active_profiler("Line")
            assert "Set active profiler failed because of" in caplog.text


def test_when_sp_profiler_not_enabled(profiler_session):
    pro = profiler_session.udf_profiler
    # direct call get_output when profiler is not enabled
    res = pro.get_output()
    assert res == ""
