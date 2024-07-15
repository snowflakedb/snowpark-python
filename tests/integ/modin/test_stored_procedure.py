#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.functions import sproc
from snowflake.snowpark.types import StringType


def test_basic_snowpark_pandas_sproc_with_session_arg():
    def return1(session_):
        import modin.pandas as pd

        import snowflake.snowpark.modin.plugin  # noqa: F401

        return pd.DataFrame([["1"]]).iloc[0][0]

    packages = ["snowflake-snowpark-python", "modin"]
    return1_sp = sproc(return1, return_type=StringType(), packages=packages)
    assert return1_sp() == "1"


def test_basic_snowpark_pandas_sproc_with_no_session_arg():
    def return1():
        import modin.pandas as pd

        import snowflake.snowpark.modin.plugin  # noqa: F401

        # TODO: Remove the call to getOrCreate after Snowpark python v1.20.0
        # or higher is present on the server side.
        snowflake.snowpark.Session.SessionBuilder().getOrCreate()
        return pd.DataFrame([["1"]]).iloc[0][0]

    packages = ["snowflake-snowpark-python", "modin"]
    return1_sp = sproc(return1, return_type=StringType(), packages=packages)
    assert return1_sp() == "1"
