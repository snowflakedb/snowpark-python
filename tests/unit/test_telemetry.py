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
        _ = sum(i for i in range(10**6))
        duration = time.time() - start_time
    resource_usage = resource_usage_collector.get_resource_usage()
    wall_time = resource_usage["wall_time"]
    cpu_time = resource_usage["cpu_time"]
    assert wall_time > duration or math.isclose(
        wall_time, duration, abs_tol=1e-2
    ), wall_time
    assert cpu_time > duration or math.isclose(
        cpu_time, duration, abs_tol=1e-2
    ), cpu_time


def test_resource_usage_io_time():
    with ResourceUsageCollector() as resource_usage_collector:
        start_time = time.time()
        time.sleep(1)  # Stimulate I/O intensive operation
        duration = time.time() - start_time
    resource_usage = resource_usage_collector.get_resource_usage()
    wall_time = resource_usage["wall_time"]
    io_time = resource_usage["io_time"]
    assert wall_time > duration or math.isclose(
        wall_time, duration, abs_tol=1e-2
    ), wall_time
    assert math.isclose(io_time, duration, abs_tol=1e-2), io_time


def test_resource_usage_memory():
    with ResourceUsageCollector() as resource_usage_collector:
        _ = [1] * 10**6
    resource_usage = resource_usage_collector.get_resource_usage()
    # ideally we would assert that the memory usage increased by 8* 10**6 bytes
    # but the memory usage is not guaranteed to increase by exactly that amount
    # so we just check that it increased by some amount
    assert resource_usage["memory_rss_kb"] > 1000, resource_usage["memory_rss_kb"]
