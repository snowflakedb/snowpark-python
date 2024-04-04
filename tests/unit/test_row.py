#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
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
        _ = row[3]
    with pytest.raises(TypeError):
        row[3] = 3
    with pytest.raises(TypeError):
        row["a"] = "a"
    with pytest.raises(AttributeError):
        _ = row.a
    with pytest.raises(AttributeError):
        row.a = "a"
    with pytest.raises(KeyError):
        _ = row["a"]


def test_row_with_field_values():
    row = Row(a=1, b=2, c=3)
    assert row["a"] == 1
    assert row.a == 1
    assert row[0] == 1
    assert row[0:2] == Row(1, 2)
    with pytest.raises(TypeError):
        row["a"] = 2
    with pytest.raises(AttributeError):
        _ = row.d
    with pytest.raises(AttributeError):
        row.a = 2
    with pytest.raises(KeyError):
        _ = row["d"]


def test_row_with_wrong_values():
    with pytest.raises(ValueError):
        Row("name", salary=10000)


def test_row_setting_fields():
    row = Row(1, 2, 3)
    row._fields = ["a", "b", "c"]
    assert row[0] == 1
    assert row["a"] == 1
    assert row.a == 1


def test_row_setting_duplicate_fields():
    row = Row(1, 2, 3)
    row._fields = ["a", "b", "b"]
    assert row[0] == 1
    assert row["a"] == 1
    assert row.a == 1
    assert row["b"] == 2  # return the 1st one with name b
    assert row.b == 2  # return the 1st one with name b
    assert "a" in row
    assert "b" in row
    with pytest.raises(KeyError):
        _ = row["c"]
    with pytest.raises(AttributeError):
        _ = row.c
    with pytest.raises(
        ValueError,
        match="The Row object can't be called because it has duplicate fields",
    ):
        _ = row(4, 5, 6)


def test_row_setting_too_many_fields():
    row = Row(1, 2, 3)
    row._fields = ["a", "b", "c", "d"]
    assert row[0] == 1
    assert row["a"] == 1
    assert row.a == 1
    with pytest.raises(KeyError):
        _ = row["d"]
    with pytest.raises(AttributeError):
        _ = row.e


def test_contains():
    row1 = Row(a=1, b=2, c=3)
    assert "a" in row1
    assert "d" not in row1
    assert 1 not in row1

    row2 = Row(1, 2, 3)
    assert "a" not in row2
    assert 1 in row2


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
    row3 = Row(1, 2, 3)
    row3._fields = ("a", "b", "b")
    assert repr(row3) == "Row(a=1, b=2, b=3)"


@pytest.mark.parametrize("row", [Row(1, 2, 3), Row(a=1, b=2, c=3)])
def test_iter(row):
    it = iter(row)
    assert next(it) == 1
    assert next(it) == 2
    assert next(it) == 3


def test_asDict():
    row1 = Row(1, 2, 3)
    with pytest.raises(TypeError):
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
    assert row._fields == restored._fields


def test_dunder_call():
    Employee = Row("name", "salary")
    emp = Employee("John Berry", 10000)
    assert emp.name == "John Berry" and emp.salary == 10000

    Employee2 = Row(name="John Zee", salary=10000)
    emp2 = Employee2(name="James Zee")
    assert emp2.name == "James Zee" and emp2.salary == 10000

    with pytest.raises(
        ValueError,
        match="A Row instance should have either values or field-value pairs but not both.",
    ):
        _ = Employee("John", salary="1000")

    with pytest.raises(
        ValueError,
        match=r"Wrong keyword argument name1 for Row\(name='John Zee', salary=10000\)",
    ):
        _ = Employee2(name1="Tina", salary=10)

    Employee3 = Row("name", 1)
    with pytest.raises(
        ValueError,
        match="The called Row object and input values must have the same size and the called Row object shouldn't have any non-str fields.",
    ):
        _ = Employee3("John", 2)


def test_negative_dunder_call():
    Employee = Row("name", "salary")
    with pytest.raises(ValueError):
        Employee(name="John Berry", salary=10000)

    with pytest.raises(ValueError):
        Employee("John Berry")

    with pytest.raises(ValueError):
        Employee("John Berry", 10000, "something else")

    Employee2 = Row(name="John Berry", salary=10000)
    with pytest.raises(ValueError):
        Employee2("James Zee", 10000)

    with pytest.raises(ValueError):
        Employee(dept="sales")


@pytest.mark.parametrize("row", [Row(1, 2, 3), Row(a=1, b=2, c=3)])
def test_mul_rmul(row):
    assert row * 2 == Row(1, 2, 3, 1, 2, 3)
    assert 2 * row == Row(1, 2, 3, 1, 2, 3)


def test_aliases():
    assert Row.asDict == Row.as_dict


def test_case_sensitive():
    Employee = Row("name", "salary")  # default is case sensitive
    emp = Employee("Don Janaher", 1000)
    assert emp.name == "Don Janaher" and emp.salary == 1000

    with pytest.raises(AttributeError):
        emp.Name
    with pytest.raises(KeyError):
        emp["Salary"]

    Employee = Row._builder.build("name", "salary").set_case_sensitive(False).to_row()
    emp = Employee("Don Janaher", 1000)
    assert emp.name == "Don Janaher" and emp.salary == 1000
    assert emp.Name == "Don Janaher" and emp.Salary == 1000

    emp = (
        Row._builder.build(name="Don Janaher", salary=1000)
        .set_case_sensitive(False)
        .to_row()
    )
    assert emp.name == "Don Janaher" and emp.salary == 1000
    assert emp.Name == "Don Janaher" and emp.Salary == 1000

    with pytest.raises(
        ValueError, match="Either values or named_values is required but not both"
    ):
        Employee = Row("name", "salary", case_sensitive=False, department="Engineering")

    # case_sensitive is an acceptable named arg when creating row without row builder
    emp = Row(
        name="Don Janaher", salary=1000, case_sensitive=False, department="Engineering"
    )
    assert emp.as_dict() == {
        "name": "Don Janaher",
        "salary": 1000,
        "case_sensitive": False,
        "department": "Engineering",
    }

    with pytest.raises(AttributeError):
        emp.Name
    with pytest.raises(KeyError):
        emp["Salary"]

    # case_sensitive is an acceptable named arg when creating row with row builder
    emp = Row._builder.build(
        name="Don Janaher", salary=1000, case_sensitive=False, department="Engineering"
    ).to_row()
    assert emp.as_dict() == {
        "name": "Don Janaher",
        "salary": 1000,
        "case_sensitive": False,
        "department": "Engineering",
    }

    with pytest.raises(AttributeError):
        emp.Name
    with pytest.raises(KeyError):
        emp["Salary"]

    # when rows are quoted, fields are always case sensitive
    with pytest.raises(ValueError):
        Employee = (
            Row._builder.build('"name"', "salary").set_case_sensitive(False).to_row()
        )

    with pytest.raises(ValueError):
        Employee = (
            Row._builder.build("'name'", "salary").set_case_sensitive(False).to_row()
        )
