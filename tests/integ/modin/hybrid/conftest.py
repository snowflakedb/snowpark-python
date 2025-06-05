#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

import modin.pandas as pd
from modin.config import context as config_context
from snowflake.snowpark.modin.plugin._internal.utils import MODIN_IS_AT_LEAST_0_33_0


@pytest.fixture(autouse=True, scope="function")
def enable_autoswitch():
    if MODIN_IS_AT_LEAST_0_33_0:
        with config_context(AutoSwitchBackend=True):
            yield
    else:
        yield


@pytest.fixture(scope="module")
def init_transaction_tables():
    if MODIN_IS_AT_LEAST_0_33_0:
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
            """SET num_days = (SELECT DATEDIFF(DAY, '2024-01-01', CURRENT_DATE));"""
        ).collect()
        session.sql(
            """INSERT INTO revenue_transactions (Transaction_ID, Date, Revenue)
        SELECT
            UUID_STRING() AS Transaction_ID,
            DATEADD(DAY, UNIFORM(0, $num_days, RANDOM()), '2024-01-01') AS Date,
            UNIFORM(10, 1000, RANDOM()) AS Revenue
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
