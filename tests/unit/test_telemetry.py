#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import time
import math
from snowflake.snowpark._internal.telemetry import (
    safe_telemetry,
    ResourceUsageCollector,
)


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


def test_resource_usage_time():
    with ResourceUsageCollector() as resource_usage_collector:
        start_time = time.time()
        i = 0  # dummy counter
        while time.time() - start_time < 0.1:
            i += 1  # cpu work
    resource_usage = resource_usage_collector.get_resource_usage()
    assert math.isclose(resource_usage["wall_time"], 0.1, abs_tol=1e-3), resource_usage[
        "wall_time"
    ]
    assert math.isclose(resource_usage["cpu_time"], 0.1, abs_tol=1e-3), resource_usage[
        "cpu_time"
    ]


def test_resource_usage_io_time():
    with ResourceUsageCollector() as resource_usage_collector:
        start_time = time.time()
        long_string = "a" * 10**6
        with open("/dev/null", "wb") as f:
            f.write(long_string.encode())
    duration = time.time() - start_time
    resource_usage = resource_usage_collector.get_resource_usage()
    assert math.isclose(
        resource_usage["wall_time"], duration, abs_tol=1e-3
    ), resource_usage["wall_time"]
    assert math.isclose(
        resource_usage["io_time"], duration, abs_tol=1e-3
    ), resource_usage["io_time"]


def test_resource_usage_memory():
    with ResourceUsageCollector() as resource_usage_collector:
        _ = [1] * 10**6
    resource_usage = resource_usage_collector.get_resource_usage()
    assert resource_usage["memory_rss_kb"] > 8e6 / 1024, resource_usage["memory_rss_kb"]
