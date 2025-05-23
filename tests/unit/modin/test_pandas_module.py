#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pickle
from unittest import mock

import modin.pandas as pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401


def test_pickle():
    """
    Test that we can pickle the pandas module.

    Using the pattern [1] to implement pd.session makes the module unpickleable
    unless we add the fix [2].

    [1] https://docs.python.org/3.12/reference/datamodel.html#customizing-module-attribute-access
    [2] https://github.com/cloudpipe/cloudpickle/issues/405#issuecomment-756085104
    """
    pickled = pickle.dumps(pd)
    assert pickle.loads(pickled) is pd


def test_get_missing_attribute():
    with pytest.raises(
        AttributeError,
        match="module 'modin.pandas' has no attribute 'missing_attribute'",
    ):
        pd.missing_attribute


@mock.patch("snowflake.snowpark.context.get_active_session")
def test_get_session_raises_exception_unrelated_to_session(mock_get_active_session):
    """Test getting session when get_active_session() raises an exception other than SnowparkSessionException."""
    mock_get_active_session.side_effect = KeyError
    with pytest.raises(KeyError):
        pd.session
    mock_get_active_session.assert_called_once()
