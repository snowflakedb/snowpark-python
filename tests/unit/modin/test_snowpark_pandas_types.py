#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import re
from dataclasses import FrozenInstanceError

import pytest

from snowflake.snowpark.modin.plugin._internal.snowpark_pandas_types import (
    TimedeltaType,
)


def test_timedelta_type_is_immutable():
    """
    Test that Timedelta instances are immutable.

    We neeed SnowparkPandasType subclasses to be immutable so that we can store
    them in InternalFrame.
    """
    with pytest.raises(
        FrozenInstanceError, match=re.escape("cannot assign to field 'x'")
    ):
        TimedeltaType().x = 3
