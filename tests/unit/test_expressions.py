#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

import datetime
import decimal

import pytest

from snowflake.snowpark._internal.analyzer.expression import Literal
from snowflake.snowpark._internal.type_utils import _type_mappings
from snowflake.snowpark.exceptions import SnowparkPlanException


def test_literal():
    basic_data = [
        1,
        "one",
        1.0,
        datetime.datetime.strptime("2017-02-24 12:00:05.456", "%Y-%m-%d %H:%M:%S.%f"),
        datetime.datetime.strptime("20:57:06", "%H:%M:%S").time(),
        datetime.datetime.strptime("2017-02-25", "%Y-%m-%d").date(),
        True,
        bytearray("a", "utf-8"),
        bytes("a", "utf-8"),
        decimal.Decimal(0.5),
    ]

    structured_data = [(1, 1), [2, 2], {"1": 2}]

    for d in basic_data:
        assert isinstance(Literal(d).datatype, _type_mappings[type(d)])

    for d in structured_data:
        with pytest.raises(SnowparkPlanException) as ex_info:
            Literal(d)
        assert "Cannot create a Literal" in str(ex_info)
