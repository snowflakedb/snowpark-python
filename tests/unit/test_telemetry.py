#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import time
import math
import psutil
from unittest.mock import patch

import pytest
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
    # skipping cpu time check since due to parallelism in test suite
    # the cpu time can be much lower compared to the wall time
    assert cpu_time > 0.0, cpu_time


@patch("psutil.Process")
def test_resource_usage_memory(mock_process):
    # Configure the mock Process object
    mock_process_instance = mock_process.return_value
    mock_memory_info = mock_process_instance.memory_info.return_value
    mock_memory_info.rss = 0  # Set the start value mocked RSS value

    with ResourceUsageCollector() as resource_usage_collector:
        _ = [1] * 10**6
        mock_memory_info.rss = 500 * 1024.0

    resource_usage = resource_usage_collector.get_resource_usage()
    # ideally we would assert that the memory usage increased by 8* 10**6 bytes
    # but the memory usage is not guaranteed to increase by exactly that amount
    # so we just check that it increased by some amount
    assert resource_usage["memory_rss_kib"] == 500, resource_usage["memory_rss_kib"]


@patch("psutil.net_io_counters")
def test_resource_usage_error_safety(mock_net_io_counters):
    mock_net_io_counters.side_effect = OSError("mock error")
    with pytest.raises(OSError, match="mock error"):
        psutil.net_io_counters()

    # assert that the error due to psutil does not affect the rest of the code
    with ResourceUsageCollector() as resource_usage_collector:
        time.sleep(0.1)

    # tamper with the resource usage collector to simulate error when
    # get_resource_usage is called
    resource_usage_collector._end_cpu_time = None
    assert resource_usage_collector.get_resource_usage() == {}
