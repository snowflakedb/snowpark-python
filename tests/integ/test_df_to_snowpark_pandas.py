#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

# Tests behavior of to_snowpark_pandas() without explicitly initializing Snowpark pandas.

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


def test_to_snowpark_pandas_no_modin(session, tmp_table_basic):
    snowpark_df = session.table(tmp_table_basic)
    # Check if modin is installed (if so, we're running in Snowpark pandas; if not, we're just in Snowpark Python)
    try:
        import modin  # noqa: F401
    except ModuleNotFoundError:
        # Current Snowpark Python installs pandas==2.2.2, but Snowpark pandas depends on modin
        # 0.28.1, which needs pandas==2.2.1. The pandas version check is currently performed
        # before Snowpark pandas checks whether modin is installed.
        # TODO: SNOW-1552497: after upgrading to modin 0.30.1, Snowpark pandas will support
        # all pandas 2.2.x, and this function call will raise a ModuleNotFoundError since
        # modin is not installed.
        with pytest.raises(
            RuntimeError,
            match="does not match the supported pandas version in Snowpark pandas",
        ):
            snowpark_df.to_snowpark_pandas()
    else:
        snowpark_df.to_snowpark_pandas()  # should have no errors
