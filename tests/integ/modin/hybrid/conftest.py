#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

import modin.pandas as pd
from modin.config import context as config_context

from tests.utils import IS_WINDOWS, Utils


if IS_WINDOWS:
    # Ray startup is extremely flaky on Windows when multiple pytest workers are used.
    # https://snowflakecomputing.atlassian.net/browse/SNOW-2292908
    # https://github.com/modin-project/modin/issues/7387
    @pytest.fixture(scope="module", autouse=True)
    def skip_if_windows():
        pytest.skip(
            reason="skipping tests requiring Ray on Windows", allow_module_level=True
        )


@pytest.fixture(autouse=True, scope="function")
def enable_autoswitch():
    with config_context(AutoSwitchBackend=True):
        yield


@pytest.fixture(scope="module")
def module_scoped_test_table_name(session) -> str:
    test_table_name = f"{Utils.random_table_name()}TESTTABLENAME"
    try:
        yield test_table_name
    finally:
        Utils.drop_table(session, test_table_name)


@pytest.fixture(scope="module")
def revenue_transactions(module_scoped_test_table_name):
    session = pd.session
    session.sql(
        f"""
    CREATE OR REPLACE TEMP TABLE {module_scoped_test_table_name} (
        Transaction_ID STRING,
        Date DATE,
        Revenue FLOAT
    );"""
    ).collect()
    session.sql(
        f"""INSERT INTO {module_scoped_test_table_name} (Transaction_ID, Date, Revenue)
    SELECT
        UUID_STRING() AS Transaction_ID,
        DATEADD(DAY, UNIFORM(0, 800, RANDOM(0)), '2024-01-01') AS Date,
        UNIFORM(10, 1000, RANDOM(0)) AS Revenue
    FROM TABLE(GENERATOR(ROWCOUNT => 1000000));
    """
    ).collect()
    return module_scoped_test_table_name


@pytest.fixture
def us_holidays_data():
    return [
        ("New Year's Day", "2025-01-01"),
        ("Martin Luther King Jr. Day", "2025-01-20"),
        ("Presidents' Day", "2025-02-17"),
        ("Memorial Day", "2025-05-26"),
        ("Juneteenth National Independence Day", "2025-06-19"),
        ("Independence Day", "2025-07-04"),
        ("Labor Day", "2025-09-01"),
        ("Columbus Day", "2025-10-13"),
        ("Veterans Day", "2025-11-11"),
        ("Thanksgiving Day", "2025-11-27"),
        ("Christmas Day", "2025-12-25"),
    ]
