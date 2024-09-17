#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import pytest

import snowflake.snowpark
from snowflake.snowpark import DataFrame
from snowflake.snowpark.functions import sproc
from snowflake.snowpark.profiler import Profiler, profiler
from tests.utils import Utils

tmp_stage_name = Utils.random_stage_name()


@pytest.fixture(scope="module", autouse=True)
def setup(session, resources_path, local_testing_mode):
    if not local_testing_mode:
        Utils.create_stage(session, tmp_stage_name, is_temporary=True)
        session.add_packages("snowflake-snowpark-python")


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
def test_profiler_with_context_manager(session, db_parameters):
    @sproc(name="table_sp", replace=True)
    def table_sp(session: snowflake.snowpark.Session) -> DataFrame:
        return session.sql("select 1")

    session.register_profiler_modules(["table_sp"])
    with profiler(
        stage=f"{db_parameters['database']}.{db_parameters['schema']}.{tmp_stage_name}",
        active_profiler="LINE",
        session=session,
    ):
        session.call("table_sp").collect()
        res = session.show_profiles()
    session.register_profiler_modules([])
    assert res is not None
    print(type(res))
    assert "Modules Profiled" in res


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
def test_profiler_with_profiler_class(session, db_parameters):
    another_tmp_stage_name = Utils.random_stage_name()

    @sproc(name="table_sp", replace=True)
    def table_sp(session: snowflake.snowpark.Session) -> DataFrame:
        return session.sql("select 1")

    pro = Profiler()
    pro.register_profiler_modules(["table_sp"])
    pro.set_active_profiler("LINE")
    pro.set_targeted_stage(
        f"{db_parameters['database']}.{db_parameters['schema']}.{tmp_stage_name}"
    )
    session.register_profiler(pro)

    pro.set_targeted_stage(
        f"{db_parameters['database']}.{db_parameters['schema']}.{another_tmp_stage_name}"
    )

    pro.enable_profiler()

    session.call("table_sp").collect()
    res = session.show_profiles()

    pro.disable_profiler()

    pro.register_profiler_modules([])
    assert res is not None
    assert "Modules Profiled" in res


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
def test_single_return_value_of_sp(session, db_parameters):
    @sproc(name="single_value_sp", replace=True)
    def single_value_sp(session: snowflake.snowpark.Session) -> str:
        return "success"

    session.register_profiler_modules(["table_sp"])
    with profiler(
        stage=f"{db_parameters['database']}.{db_parameters['schema']}.{tmp_stage_name}",
        active_profiler="LINE",
        session=session,
    ):
        session.call("single_value_sp")
        res = session.show_profiles()
    session.register_profiler_modules([])
    assert res is not None
    assert "Modules Profiled" in res


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
def test_anonymous_procedure(session, db_parameters):
    def single_value_sp(session: snowflake.snowpark.Session) -> str:
        return "success"

    single_value_sp = session.sproc.register(single_value_sp, anonymous=True)
    session.register_profiler_modules(["table_sp"])
    with profiler(
        stage=f"{db_parameters['database']}.{db_parameters['schema']}.{tmp_stage_name}",
        active_profiler="LINE",
        session=session,
    ):
        single_value_sp()
        res = session.show_profiles()
    session.register_profiler_modules([])
    assert res is not None
    assert "Modules Profiled" in res


def test_not_set_profiler_error(session, tmpdir):
    with pytest.raises(ValueError) as e:
        session.show_profiles()
    assert (
        "profiler is not set, use session.register_profiler or profiler context manager"
        in str(e)
    )

    with pytest.raises(ValueError) as e:
        session.dump_profiles(tmpdir.join("file.txt"))
    assert (
        "profiler is not set, use session.register_profiler or profiler context manager"
        in str(e)
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
def test_register_module_without_profiler(session, db_parameters):
    session.register_profiler_modules(["fake_module"])
    res = session.sql("show parameters like 'python_profiler_modules'").collect()
    assert res[0].value == "fake_module"
    session.register_profiler_modules([])


def test_set_incorrect_active_profiler():
    pro = Profiler()
    with pytest.raises(ValueError) as e:
        pro.set_active_profiler("wrong_active_profiler")
    assert (
        "active_profiler expect 'LINE' or 'MEMORY', got wrong_active_profiler instead"
        in str(e)
    )


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="session.sql is not supported in localtesting",
)
def test_dump_profile_to_file(session, db_parameters, tmpdir):
    file = tmpdir.join("profile.lprof")

    def single_value_sp(session: snowflake.snowpark.Session) -> str:
        return "success"

    single_value_sp = session.sproc.register(single_value_sp, anonymous=True)
    session.register_profiler_modules(["table_sp"])
    with profiler(
        stage=f"{db_parameters['database']}.{db_parameters['schema']}.{tmp_stage_name}",
        active_profiler="LINE",
        session=session,
    ):
        single_value_sp()
        session.dump_profiles(file)
    session.register_profiler_modules([])
    with open(file) as f:
        assert "Modules Profiled" in f.read()
