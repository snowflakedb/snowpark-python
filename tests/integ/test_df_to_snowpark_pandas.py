#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

# Tests behavior of to_snowpark_pandas() without explicitly initializing Snowpark pandas.

import sys

import pytest

from snowflake.snowpark._internal.utils import TempObjectType
from tests.utils import Utils

pytestmark = [
    pytest.mark.xfail(
        "config.getoption('local_testing_mode', default=False)",
        reason="This is testing Snowpark pandas installation",
        run=False,
    )
]


@pytest.fixture(scope="module")
def tmp_table_basic(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(
        session, table_name, "id integer, foot_size float, shoe_model varchar"
    )
    session.sql(f"insert into {table_name} values (1, 32.0, 'medium')").collect()
    session.sql(f"insert into {table_name} values (2, 27.0, 'small')").collect()
    session.sql(f"insert into {table_name} values (3, 40.0, 'large')").collect()

    try:
        yield table_name
    finally:
        Utils.drop_table(session, table_name)


@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_to_snowpark_pandas_no_modin(session, tmp_table_basic, enforce_ordering):
    snowpark_df = session.table(tmp_table_basic)
    # If pandas isn't installed, then an error is raised immediately.
    try:
        import pandas  # noqa: F401
    except ModuleNotFoundError:
        with pytest.raises(ModuleNotFoundError, match="No module named 'pandas'"):
            snowpark_df.to_snowpark_pandas(enforce_ordering=enforce_ordering)
        return
    # Check if modin is installed (if so, we're running in Snowpark pandas; if not, we're just in Snowpark Python)
    try:
        import modin  # noqa: F401
    except ModuleNotFoundError:
        if sys.version_info.major == 3 and sys.version_info.minor == 8:
            # Snowpark pandas does not support Python 3.8
            ctx = pytest.raises(
                RuntimeError,
                match="Snowpark pandas does not support Python 3.8. Please update to Python 3.9 or later",
            )
        else:
            # This function call will raise a ModuleNotFoundError since modin is not installed
            ctx = pytest.raises(
                ModuleNotFoundError,
                match="Modin is not installed.",
            )
        with ctx:
            snowpark_df.to_snowpark_pandas(enforce_ordering=enforce_ordering)
    else:
        snowpark_df.to_snowpark_pandas(
            enforce_ordering=enforce_ordering
        )  # should have no errors
