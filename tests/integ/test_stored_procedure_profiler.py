#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import logging
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

import pytest

import snowflake.snowpark
from snowflake.snowpark import DataFrame
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import sproc
from snowflake.snowpark.stored_procedure_profiler import StoredProcedureProfiler
from tests.utils import Utils


def multi_thread_helper_function(pro: StoredProcedureProfiler):
    pro.set_active_profiler("LINE")
    pro.disable()


@pytest.fixture(scope="function")
def is_profiler_function_exist(profiler_session):
    functions = profiler_session.sql(
        "show functions like 'GET_PYTHON_PROFILER_OUTPUT' in snowflake.core"
    ).collect()
    if len(functions) == 0:
        pytest.skip("profiler function does not exist")


@pytest.fixture(scope="function")
def tmp_stage_name():
    tmp_stage_name = Utils.random_stage_name()
    yield tmp_stage_name


@pytest.fixture(scope="function", autouse=True)
def setup(profiler_session, resources_path, local_testing_mode):
    if not local_testing_mode:
        profiler_session.add_packages("snowflake-snowpark-python")


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
def test_profiler_function_exist(is_profiler_function_exist, profiler_session):
    res = profiler_session.sql(
        "show functions like 'GET_PYTHON_PROFILER_OUTPUT' in snowflake.core"
    ).collect()
    assert len(res) != 0


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
@pytest.mark.xfail(reason="stored proc registry changes not yet reflected.")
def test_profiler_with_profiler_class(
    is_profiler_function_exist, profiler_session, db_parameters, tmp_stage_name
):
    @sproc(name="table_sp", replace=True)
    def table_sp(session: snowflake.snowpark.Session) -> DataFrame:
        return session.sql("select 1")

    pro = profiler_session.stored_procedure_profiler
    pro.register_modules(["table_sp"])
    pro.set_target_stage(
        f"{db_parameters['database']}.{db_parameters['schema']}.{tmp_stage_name}"
    )

    pro.set_active_profiler("LINE")

    profiler_session.call("table_sp")
    res = pro.get_output()
    pro.disable()

    pro.register_modules([])
    assert res is not None
    assert "Modules Profiled" in res


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
@pytest.mark.xfail(reason="stored proc registry changes not yet reflected.")
def test_single_return_value_of_sp(
    is_profiler_function_exist, profiler_session, db_parameters, tmp_stage_name
):
    @sproc(name="single_value_sp", replace=True)
    def single_value_sp(session: snowflake.snowpark.Session) -> str:
        return "success"

    profiler_session.stored_procedure_profiler.register_modules(["single_value_sp"])
    profiler_session.stored_procedure_profiler.set_target_stage(
        f"{db_parameters['database']}.{db_parameters['schema']}.{tmp_stage_name}"
    )

    profiler_session.stored_procedure_profiler.set_active_profiler("LINE")

    profiler_session.call("single_value_sp")
    res = profiler_session.stored_procedure_profiler.get_output()

    profiler_session.stored_procedure_profiler.disable()

    profiler_session.stored_procedure_profiler.register_modules()
    assert res is not None
    assert "Modules Profiled" in res


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
@pytest.mark.xfail(reason="stored proc registry changes not yet reflected.")
def test_anonymous_procedure(
    is_profiler_function_exist, profiler_session, db_parameters, tmp_stage_name
):
    def single_value_sp(session: snowflake.snowpark.Session) -> str:
        return "success"

    single_value_sp = profiler_session.sproc.register(single_value_sp, anonymous=True)

    profiler_session.stored_procedure_profiler.set_target_stage(
        f"{db_parameters['database']}.{db_parameters['schema']}.{tmp_stage_name}"
    )

    profiler_session.stored_procedure_profiler.set_active_profiler("LINE")

    single_value_sp()
    res = profiler_session.stored_procedure_profiler.get_output()

    profiler_session.stored_procedure_profiler.disable()

    profiler_session.stored_procedure_profiler.register_modules()
    assert res is not None
    assert "Modules Profiled" in res


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
def test_set_incorrect_active_profiler(
    profiler_session, db_parameters, tmp_stage_name, caplog
):
    with pytest.raises(ValueError) as e:
        profiler_session.stored_procedure_profiler.set_target_stage(f"{tmp_stage_name}")
    assert "stage name must be fully qualified name" in str(e)

    with caplog.at_level(logging.WARNING):
        profiler_session.stored_procedure_profiler.set_target_stage(
            f"{db_parameters['database']}.{db_parameters['schema']}.{tmp_stage_name}"
        )
        profiler_session.stored_procedure_profiler.set_active_profiler("LINE")
        profiler_session.stored_procedure_profiler.get_output()
    assert "last executed stored procedure does not exist" in caplog.text

    with pytest.raises(ValueError) as e:
        profiler_session.stored_procedure_profiler.set_active_profiler(
            "wrong_active_profiler"
        )
    assert "active_profiler expect 'LINE', 'MEMORY'" in str(e)


@pytest.mark.parametrize(
    "sp_call_sql",
    [
        """WITH myProcedure AS PROCEDURE ()
      RETURNS TABLE ( )
      LANGUAGE PYTHON
      RUNTIME_VERSION = '3.9'
      PACKAGES = ( 'snowflake-snowpark-python==1.2.0', 'pandas==1.3.3' )
      IMPORTS = ( '@my_stage/file1.py', '@my_stage/file2.py' )
      HANDLER = 'my_function'
      RETURNS NULL ON NULL INPUT
    AS 'fake'
    CALL myProcedure()INTO :result
        """,
        """CALL MY_SPROC()""",
        """    CALL MY_SPROC()""",
        """WITH myProcedure AS PROCEDURE () CALL  myProcedure""",
        """   WITH myProcedure AS PROCEDURE ... CALL  myProcedure""",
    ],
)
def test_sp_call_match(profiler_session, sp_call_sql):
    pro = profiler_session.stored_procedure_profiler

    assert pro._is_sp_call(sp_call_sql)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
def test_query_history_destroyed_after_finish_profiling(
    profiler_session, db_parameters, tmp_stage_name
):
    profiler_session.stored_procedure_profiler.set_target_stage(
        f"{db_parameters['database']}.{db_parameters['schema']}.{tmp_stage_name}"
    )

    profiler_session.stored_procedure_profiler.set_active_profiler("LINE")
    assert (
        profiler_session.stored_procedure_profiler._query_history
        in profiler_session._conn._query_listeners
    )

    profiler_session.stored_procedure_profiler.disable()
    assert (
        profiler_session.stored_procedure_profiler._query_history
        not in profiler_session._conn._query_listeners
    )

    profiler_session.stored_procedure_profiler.register_modules()


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
def test_thread_safe_on_activate_and_disable(
    profiler_session, db_parameters, tmp_stage_name
):
    pro = profiler_session.stored_procedure_profiler
    pro.register_modules(["table_sp"])
    pro.set_target_stage(
        f"{db_parameters['database']}.{db_parameters['schema']}.{tmp_stage_name}"
    )
    with ThreadPoolExecutor(max_workers=2) as tpe:
        for _ in range(6):
            tpe.submit(multi_thread_helper_function, pro)
    assert pro._query_history is None
    pro.register_modules()


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
def test_create_temp_stage(profiler_session):
    pro = profiler_session.stored_procedure_profiler
    db_name = Utils.random_temp_database()
    schema_name = Utils.random_temp_schema()
    temp_stage = Utils.random_stage_name()
    current_db = profiler_session.sql("select current_database()").collect()[0][0]
    try:
        profiler_session.sql(f"create database {db_name}").collect()
        profiler_session.sql(f"create schema {schema_name}").collect()
        pro.set_target_stage(f"{db_name}.{schema_name}.{temp_stage}")

        res = profiler_session.sql(
            f"show stages like '{temp_stage}' in schema {db_name}.{schema_name}"
        ).collect()
        assert len(res) != 0
    finally:
        profiler_session.sql(f"drop database if exists {db_name}").collect()
        profiler_session.sql(f"use database {current_db}").collect()


@pytest.mark.skip(reason="SNOW-1945207")
@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
def test_stored_proc_error(
    is_profiler_function_exist, profiler_session, db_parameters, tmp_stage_name
):
    function_name = f"oom_sp_{Utils.random_function_name()}"

    @sproc(name=function_name, session=profiler_session, replace=True)
    def oom_sp(session: snowflake.snowpark.Session) -> str:
        raise ValueError("fake out of memory")

    profiler_session.stored_procedure_profiler.register_modules(["oom_sp"])
    profiler_session.stored_procedure_profiler.set_target_stage(
        f"{db_parameters['database']}.{db_parameters['schema']}.{tmp_stage_name}"
    )

    profiler_session.stored_procedure_profiler.set_active_profiler("LINE")

    with pytest.raises(SnowparkSQLException, match="fake out of memory") as err:
        profiler_session.call(function_name)
        query_id = profiler_session.stored_procedure_profiler._get_last_query_id()
        assert query_id in str(err)

    profiler_session.stored_procedure_profiler.disable()

    profiler_session.stored_procedure_profiler.register_modules()


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
def test_profiler_without_target_stage(profiler_session, caplog):
    pro = profiler_session.stored_procedure_profiler
    with caplog.at_level(logging.INFO):
        pro.set_active_profiler("LINE")
        assert (
            "Target stage for profiler not found, using default stage of current session."
            in str(caplog.text)
        )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
def test_set_active_profiler_failed(
    profiler_session, caplog, tmp_stage_name, db_parameters
):
    pro = profiler_session.stored_procedure_profiler
    pro.set_target_stage(
        f"{db_parameters['database']}.{db_parameters['schema']}.{tmp_stage_name}"
    )
    with mock.patch(
        "snowflake.snowpark.DataFrame._internal_collect_with_tag_no_telemetry",
        side_effect=Exception,
    ):
        with caplog.at_level(logging.WARNING):
            pro.set_active_profiler("Line")
            assert "Set active profiler failed because of" in caplog.text


def test_when_sp_profiler_not_enabled(profiler_session):
    pro = profiler_session.stored_procedure_profiler
    # direct call get_output when profiler is not enabled
    res = pro.get_output()
    assert res == ""
