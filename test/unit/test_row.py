#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import pickle

import pytest

from snowflake.snowpark.row import Row


def test_row_from_values():
    row = Row(1, 2, 3)
    assert row[0] == 1

    with pytest.raises(IndexError):
        value = row[3]
    with pytest.raises(TypeError):
        row[3] = 3
    with pytest.raises(TypeError):
        row["a"] = "a"
    with pytest.raises(AttributeError):
        row.a = "a"


def test_row_from_named_values():
    row = Row(a=1, b=2, c=3)
    assert row["a"] == 1
    assert row.a == 1
    assert row[0] == 1
    with pytest.raises(TypeError):
        row["a"] = 2
    with pytest.raises(AttributeError):
        row.a = 2
    with pytest.raises(KeyError):
        value = row["d"]


@pytest.mark.parametrize("row", [Row(1, 2, 3), Row(a=1, b=2, c=3)])
def test_row_pickle(row):
    pickled = pickle.dumps(row)
    restored = pickle.loads(pickled)
    assert row == restored
