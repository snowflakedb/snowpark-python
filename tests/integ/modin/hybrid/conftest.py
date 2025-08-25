#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

import modin.pandas as pd
from modin.config import context as config_context

from tests.utils import IS_WINDOWS


if IS_WINDOWS:
    # Ray startup is extremely flaky on Windows when multiple pytest workers are used.
    # https://snowflakecomputing.atlassian.net/browse/SNOW-2292908
    # https://github.com/modin-project/modin/issues/7387
    pytest.skip(allow_module_level=True)


@pytest.fixture(autouse=True, scope="function")
def enable_autoswitch():
    with config_context(AutoSwitchBackend=True):
        yield


@pytest.fixture(scope="module")
def init_transaction_tables():
    session = pd.session
    session.sql(
        """
    CREATE OR REPLACE TABLE revenue_transactions (
        Transaction_ID STRING,
        Date DATE,
        Revenue FLOAT
    );"""
    ).collect()
    session.sql(
        """INSERT INTO revenue_transactions (Transaction_ID, Date, Revenue)
    SELECT
        UUID_STRING() AS Transaction_ID,
        DATEADD(DAY, UNIFORM(0, 800, RANDOM(0)), '2024-01-01') AS Date,
        UNIFORM(10, 1000, RANDOM(0)) AS Revenue
    FROM TABLE(GENERATOR(ROWCOUNT => 10000000));
    """
    ).collect()


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
