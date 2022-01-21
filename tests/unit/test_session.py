#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark import Session


def test_aliases():
    assert Session.createDataFrame == Session.create_dataframe
