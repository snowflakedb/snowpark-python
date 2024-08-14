#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

# Tests behavior of to_snowpark_pandas() without explicitly initializing Snowpark pandas.

import pytest

from snowflake.snowpark._internal.utils import TempObjectType
from tests.utils import Utils


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
    # Check if modin is installed
    try:
        import modin  # noqa: F401

        snowpark_df.to_snowpark_pandas()  # should have no errors
    except ModuleNotFoundError:
        with pytest.raises(ModuleNotFoundError, match="Modin is not installed."):
            snowpark_df.to_snowpark_pandas()
