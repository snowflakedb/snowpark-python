#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import random
import time

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.functions import col, count, sum as sum_


@pytest.mark.localtest
def test_range(session):
    assert session.range(5).collect() == [Row(i) for i in range(5)]
    assert session.range(3, 5).collect() == [Row(i) for i in range(3, 5)]
    assert session.range(3, 10, 2).collect() == [Row(i) for i in range(3, 10, 2)]


@pytest.mark.localtest
def test_negative_test(session):
    with pytest.raises(ValueError) as ex_info:
        session.range(-3, 5, 0)
    assert "The step for range() cannot be 0." in str(ex_info)


@pytest.mark.localtest
def test_empty_result_and_negative_start_end_step(session):
    assert session.range(3, 5, -1).collect() == []
    assert session.range(-3, -5, 1).collect() == []

    assert session.range(-3, -10, -2).collect() == [Row(i) for i in range(-3, -10, -2)]
    assert session.range(10, 3, -3).collect() == [Row(i) for i in range(10, 3, -3)]


@pytest.mark.localtest
def test_range_api(session):
    res3 = session.range(1, -2).select("id")
    assert res3.count() == 0

    res10 = session.range(10).select("id")
    assert res10.count() == 10
    assert res10.agg(sum_(col("id")).as_("sumid")).collect() == [Row(45)]

    res11 = session.range(-1).select("id")
    assert res11.count() == 0

    res12 = session.range(3, 15, 3).select("id")
    assert res12.count() == 4
    assert res12.agg(sum_(col("id")).as_("sumid")).collect() == [Row(30)]

    n = 9 * 1000 * 1000 * 1000 * 1000 * 1000 * 1000
    res13 = session.range(-n, n, n / 9).select("id")
    assert res13.count() == 18

    res14 = session.range(0, 100, 2).to_df(["id"]).filter(col("id") >= 50)
    assert res14.count() == 25

    res15 = session.range(100, -100, -2).to_df(["id"]).filter(col("id") <= 0)
    assert res15.count() == 50

    res16 = session.range(-1500, 1500, 3).to_df(["id"]).filter(col("id") >= 0)
    assert res16.count() == 500


@pytest.mark.localtest
def test_range_with_randomized_parameters(session):
    MAX_NUM_STEPS = 10 * 1000
    MAX_VALUE = 2**31 - 1
    seed = int(time.time())
    random.seed(seed)

    def random_bound():
        n = random.randrange(MAX_VALUE) % (MAX_VALUE // (100 * MAX_NUM_STEPS))
        return n if random.randrange(2) else -n

    for _ in range(10):
        start = random_bound()
        end = random_bound()
        num_steps = random.randrange(MAX_VALUE) % MAX_NUM_STEPS + 1
        step_abs = abs(end - start) // num_steps + 1
        step = step_abs if start < end else -step_abs

        expected_count = len(range(start, end, step))
        expected_sum = sum(range(start, end, step))

        res = (
            session.range(start, end, step)
            .agg([count(col("id")), sum_(col("id"))])
            .collect()
        )
        assert len(res) != 0
        assert res[0][0] == expected_count
        if expected_count == 0:
            # this dataframe from range is an empty dataframe, so SUM(ID) should be None
            assert res[0][1] is None
        else:
            assert res[0][1] == expected_sum


@pytest.mark.localtest
def test_range_with_max_and_min(session):
    MAX_VALUE = 0x7FFFFFFFFFFFFFFF
    MIN_VALUE = -0x8000000000000000
    start = MAX_VALUE - 3
    end = MIN_VALUE + 2
    assert session.range(start, end, 1).collect() == []
    assert session.range(start, start, 1).collect() == []


@pytest.mark.localtest
def test_range_with_large_range_and_step(session):
    try:
        import numpy as np

        ints = np.array([691200000000000], dtype="int64")
        # Use a numpy int64 range with a python int step
        assert session.range(0, ints[0], 86400000000000).collect() != []
    except ImportError:
        pytest.skip("numpy is not installed, skipping the tests")
