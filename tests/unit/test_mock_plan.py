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

def test_pandas_replace_na(snowpark_session: Session):
    df = snowpark_session.create_dataframe(
        [{"a": 1.1, "b": "s"}, {"a": np.nan, "b": 't'}], schema=["a", "b"]
    )
    expected = [{"A": 1.1, "B": "s"}, {"A": None, "B": 't'}]
    df_as_list = [{k: x[k] for k in ['A', 'B']} for x in df.to_local_iterator()]
    assert expected == df_as_list

