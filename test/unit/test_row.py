#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
import pickle
from copy import copy

import pytest

from snowflake.snowpark import Row


def test_row_with_only_values():
    row = Row(1, 2, 3)
    assert row[0] == 1
    assert row[0:2] == Row(1, 2)

    with pytest.raises(IndexError):
        value = row[3]
    with pytest.raises(TypeError):
        row[3] = 3
    with pytest.raises(TypeError):
        row["a"] = "a"
    with pytest.raises(AttributeError):
        temp = row.a
    with pytest.raises(AttributeError):
        row.a = "a"
    with pytest.raises(KeyError):
        temp = row["a"]


def test_row_with_field_values():
    row = Row(a=1, b=2, c=3)
    assert row["a"] == 1
    assert row.a == 1
    assert row[0] == 1
    assert row[0:2] == Row(1, 2)
    with pytest.raises(TypeError):
        row["a"] = 2
    with pytest.raises(AttributeError):
        temp = row.d
    with pytest.raises(AttributeError):
        row.a = 2
    with pytest.raises(KeyError):
        value = row["d"]


def test_row_with_wrong_values():
    with pytest.raises(ValueError):
        row = Row("name", salary=10000)


def test_row_setting_fields():
    row = Row(1, 2, 3)
    row.__fields__ = ["a", "b", "c"]
    assert row[0] == 1
    assert row["a"] == 1
    assert row.a == 1


def test_row_setting_duplicate_fields():
    row = Row(1, 2, 3)
    row.__fields__ = ["a", "b", "b"]
    assert row[0] == 1
    assert row["a"] == 1
    assert row.a == 1
    assert row["b"] == 2  # return the 1st one with name b
    assert row.b == 2  # return the 1st one with name b


def test_row_setting_too_many_fields():
    row = Row(1, 2, 3)
    row.__fields__ = ["a", "b", "c", "d"]
    assert row[0] == 1
    assert row["a"] == 1
    assert row.a == 1
    with pytest.raises(KeyError):
        something = row["d"]
    with pytest.raises(AttributeError):
        something = row.e


def test_contains():
    row1 = Row(a=1, b=2, c=3)
    assert "a" in row1
    assert "d" not in row1

    row2 = Row(1, 2, 3)
    assert "a" not in row2


@pytest.mark.parametrize("row", [Row(1, 2, 3), Row(a=1, b=2, c=3)])
def test_copy_and_equals(row):
    copied = copy(row)
    assert row == copied and row is not copied


def test_equals_by_only_values():
    row1 = Row(a=1, b=2, c=3)
    row2 = Row(1, 2, 3)
    assert row1 == row2


def test_lt():
    row1 = Row(1, 2, 3)
    row2 = Row(1, 2, 4)
    assert row1 < row2


@pytest.mark.parametrize("row", [Row(1, 2, 3), Row(a=1, b=2, c=3)])
def test_hash(row):
    assert hash(row) == hash((1, 2, 3))


def test_repr():
    row1 = Row(1, 2, 3)
    assert repr(row1) == "Row(1, 2, 3)"
    row2 = Row(a=1, b=2, c=3)
    assert repr(row2) == "Row(a=1, b=2, c=3)"


@pytest.mark.parametrize("row", [Row(1, 2, 3), Row(a=1, b=2, c=3)])
def test_iter(row):
    it = iter(row)
    assert next(it) == 1
    assert next(it) == 2
    assert next(it) == 3


def test_asDict():
    row1 = Row(1, 2, 3)
    with pytest.raises(TypeError) as te:
        row1.asDict()
    row2 = Row(a=1, b=2, c=[Row(d=3), Row(d=4)])
    assert row2.asDict() == {"a": 1, "b": 2, "c": [Row(d=3), Row(d=4)]}


def test_asDict_recursive():
    row = Row(a=1, b=2, c=(Row(d=3), Row(d=4)))
    assert row.asDict(True) == {"a": 1, "b": 2, "c": [{"d": 3}, {"d": 4}]}


@pytest.mark.parametrize("row", [Row(1, 2, 3), Row(a=1, b=2, c=3)])
def test_index(row):
    assert row.index(2) == 1
    assert row.index(2, 1, 2) == 1


@pytest.mark.parametrize("row", [Row(1, 2, 3), Row(a=1, b=2, c=3)])
def test_len(row):
    assert len(row) == 3


@pytest.mark.parametrize("row", [Row(1, 2, 3, 2), Row(a=1, b=2, c=3, d=2)])
def test_count(row):
    assert row.count(2) == 2


@pytest.mark.parametrize("row", [Row(1, 2, 3), Row(a=1, b=2, c=3)])
def test_row_pickle(row):
    pickled = pickle.dumps(row)
    restored = pickle.loads(pickled)
    assert row == restored
    assert row._named_values == restored._named_values
    assert row.__fields__ == restored.__fields__


def test_dunder_call():
    Employee = Row("name", "salary")
    emp = Employee("John Berry", 10000)
    assert emp.name == "John Berry" and emp.salary == 10000

    Employee2 = Row(name="John Zee", salary=10000)
    emp2 = Employee2(name="James Zee")
    assert emp2.name == "James Zee" and emp2.salary == 10000


def test_negative_dunder_call():
    Employee = Row("name", "salary")
    with pytest.raises(ValueError):
        emp = Employee(name="John Berry", salary=10000)

    with pytest.raises(ValueError):
        emp = Employee("John Berry")

    with pytest.raises(ValueError):
        emp = Employee("John Berry", 10000, "something else")

    Employee2 = Row(name="John Berry", salary=10000)
    with pytest.raises(ValueError):
        emp2 = Employee2("James Zee", 10000)

    with pytest.raises(ValueError):
        empe = Employee(dept="sales")


@pytest.mark.parametrize("row", [Row(1, 2, 3), Row(a=1, b=2, c=3)])
def test_mul_rmul(row):
    assert row * 2 == Row(1, 2, 3, 1, 2, 3)
    assert 2 * row == Row(1, 2, 3, 1, 2, 3)
