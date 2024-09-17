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


def test_profiler_with_profiler_class(session, db_parameters):
    @sproc(name="table_sp", replace=True)
    def table_sp(session: snowflake.snowpark.Session) -> DataFrame:
        return session.sql("select 1")

    profiler = Profiler()
    profiler.register_profiler_modules(["table_sp"])
    profiler.set_active_profiler("LINE")
    profiler.set_targeted_stage(
        f"{db_parameters['database']}.{db_parameters['schema']}.{tmp_stage_name}"
    )
    session.register_profiler(profiler)

    profiler.enable_profiler()

    session.call("table_sp").collect()
    res = session.show_profiles()

    profiler.disable_profiler()

    profiler.register_profiler_modules([])
    assert res is not None
    assert "Modules Profiled" in res


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
