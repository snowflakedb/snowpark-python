#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark import Row
from snowflake.snowpark.functions import (
    asc,
    asc_nulls_first,
    asc_nulls_last,
)
from tests.utils import TestData


def test_order(session):
    null_data1 = TestData.null_data1(session)
    assert null_data1.sort(asc(null_data1["A"])).collect() == [
        Row(None),
        Row(None),
        Row(1),
        Row(2),
        Row(3),
    ]
    assert null_data1.sort(asc_nulls_first(null_data1["A"])).collect() == [
        Row(None),
        Row(None),
        Row(1),
        Row(2),
        Row(3),
    ]
    assert null_data1.sort(asc_nulls_last(null_data1["A"])).collect() == [
        Row(1),
        Row(2),
        Row(3),
        Row(None),
        Row(None),
    ]
