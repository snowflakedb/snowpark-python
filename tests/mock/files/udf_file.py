#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import pytest

pytestmark = pytest.mark.skip


def raise_err(x):
    raise RuntimeError("Oh no error raised")
