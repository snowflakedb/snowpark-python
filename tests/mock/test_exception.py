#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import traceback

import pytest

from snowflake.snowpark.mock.exceptions import SnowparkLocalTestingException


def test_exception():
    # make sure the traceback works
    try:
        SnowparkLocalTestingException.raise_from_error(ValueError("exception message"))
    except Exception as exc:
        tb = traceback.format_exc()
        assert isinstance(exc, SnowparkLocalTestingException)
        assert (
            '[Local Testing] Encountered exception "ValueError" with message "exception message"'
            in str(exc)
        )
        assert "ValueError: exception message" in tb

    # we do not re-wrap a local testing exception
    with pytest.raises(SnowparkLocalTestingException) as ex_info:
        SnowparkLocalTestingException.raise_from_error(
            SnowparkLocalTestingException("exception message")
        )
        assert "[Local Testing]" not in str(ex_info)
        assert "exception message" in str(ex_info)
