import pytest
from snowflake.snowpark import Session
import warnings
import numpy as np


@pytest.fixture()
def snowpark_session() -> Session:
    return Session.builder.config("local_testing", True).create()


def test_pandas_error(snowpark_session: Session):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        snowpark_session.create_dataframe(
            [{"a": 1.1, "b": "s"}, {"a": np.nan, "b": "t"}],
            schema=["a", "b"]
        )
