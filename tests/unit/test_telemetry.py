#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark._internal.telemetry import safe_telemetry


@safe_telemetry
def raise_exception():
    raise ValueError


# Test the safe telemetry decorator. Most of the telemetry code
# gets tested naturally as the tests run (which generate telemetry),
# but the safe_telemetry decorator only gets tested when there is an
# exception in the telemetry code (which doesn't happen every test run).
# So we create a function above that always raises and exception
# and then test the telemetry decorator that way
def test_safe_telemetry_decorator():
    raise_exception()
