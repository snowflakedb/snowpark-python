#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import warnings

import numpy as np
import pytest

from snowflake.snowpark import Session


@pytest.fixture()
def snowpark_session() -> Session:
    return Session.builder.config("local_testing", True).create()


def test_pandas_error(snowpark_session: Session):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        snowpark_session.create_dataframe(
            [{"a": 1.1, "b": "s"}, {"a": np.nan, "b": "t"}], schema=["a", "b"]
        )
