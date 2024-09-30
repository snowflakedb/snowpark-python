#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import pytest

import snowflake.snowpark
from snowflake.snowpark import DataFrame
from snowflake.snowpark.functions import sproc
from tests.utils import Utils


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
def test_profiler_with_profiler_class(
    is_profiler_function_exist, profiler_session, db_parameters, tmp_stage_name
):
    @sproc(name="table_sp", replace=True)
    def table_sp(session: snowflake.snowpark.Session) -> DataFrame:
        return session.sql("select 1")

    pro = profiler_session.profiler
    pro.register_modules(["table_sp"])
    pro.set_targeted_stage(
        f"{db_parameters['database']}.{db_parameters['schema']}.{tmp_stage_name}"
    )

    pro.set_active_profiler("LINE")

    profiler_session.call("table_sp")
    res = pro.collect()

    pro.disable()

    pro.register_modules([])
    assert res is not None
    assert "Modules Profiled" in res


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
def test_single_return_value_of_sp(
    is_profiler_function_exist, profiler_session, db_parameters, tmp_stage_name
):
    @sproc(name="single_value_sp", replace=True)
    def single_value_sp(session: snowflake.snowpark.Session) -> str:
        return "success"

    profiler_session.profiler.register_modules(["single_value_sp"])
    profiler_session.profiler.set_targeted_stage(
        f"{db_parameters['database']}.{db_parameters['schema']}.{tmp_stage_name}"
    )

    profiler_session.profiler.set_active_profiler("LINE")

    profiler_session.call("single_value_sp")
    res = profiler_session.profiler.collect()

    profiler_session.profiler.disable()

    profiler_session.profiler.register_modules([])
    assert res is not None
    assert "Modules Profiled" in res


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
def test_anonymous_procedure(
    is_profiler_function_exist, profiler_session, db_parameters, tmp_stage_name
):
    def single_value_sp(session: snowflake.snowpark.Session) -> str:
        return "success"

    single_value_sp = profiler_session.sproc.register(single_value_sp, anonymous=True)

    profiler_session.profiler.set_targeted_stage(
        f"{db_parameters['database']}.{db_parameters['schema']}.{tmp_stage_name}"
    )

    profiler_session.profiler.set_active_profiler("LINE")

    single_value_sp()
    res = profiler_session.profiler.collect()

    profiler_session.profiler.disable()

    profiler_session.profiler.register_modules([])
    assert res is not None
    assert "Modules Profiled" in res


def test_set_incorrect_active_profiler(profiler_session):
    with pytest.raises(ValueError) as e:
        profiler_session.profiler.set_active_profiler("wrong_active_profiler")
    assert "active_profiler expect 'LINE', 'MEMORY' or empty string ''" in str(e)


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
def test_dump_profile_to_file(
    is_profiler_function_exist, profiler_session, db_parameters, tmpdir, tmp_stage_name
):
    file = tmpdir.join("profile.lprof")

    def single_value_sp(session: snowflake.snowpark.Session) -> str:
        return "success"

    single_value_sp = profiler_session.sproc.register(single_value_sp, anonymous=True)
    profiler_session.profiler.set_targeted_stage(
        f"{db_parameters['database']}.{db_parameters['schema']}.{tmp_stage_name}"
    )

    profiler_session.profiler.set_active_profiler("LINE")

    single_value_sp()
    profiler_session.profiler.dump(file)

    profiler_session.profiler.disable()

    profiler_session.profiler.register_modules([])
    with open(file) as f:
        assert "Modules Profiled" in f.read()


def test_sp_call_match(profiler_session):
    pro = profiler_session.profiler
    sp_call_sql = """WITH myProcedure AS PROCEDURE ()
  RETURNS TABLE ( )
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.8'
  PACKAGES = ( 'snowflake-snowpark-python==1.2.0', 'pandas==1.3.3' )
  IMPORTS = ( '@my_stage/file1.py', '@my_stage/file2.py' )
  HANDLER = 'my_function'
  RETURNS NULL ON NULL INPUT
AS 'fake'
CALL myProcedure()INTO :result
    """
    assert pro._is_sp_call(sp_call_sql)
