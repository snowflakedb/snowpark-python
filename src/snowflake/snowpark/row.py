#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

from decimal import Decimal
from functools import total_ordering
from typing import Any, AnyStr, Dict, Iterable, List, Union


def _restore_row_from_pickle(values, named_values, fields):
    if named_values:
        row = Row(**named_values)
    else:
        row = Row(*values)
    row.__fields__ = fields
    return row


class Row(tuple):
    """Represents a row in :class:`DataFrame`.
    It is immutable and works like a tuple or a named tuple.
    >>> row = Row(1, 2)
    >>> row
    Row(1, 2)
    >>> row[0]
    1
    >>> len(row)
    2
    >>> row[0:1]
    Row(1)
    >>> named_row = Row(name1=1, name2=2)
    >>> named_row
    Row(name1=1, name2=2)
    >>> named_row["name1"]
    1
    >>> named_row.name1
    1
    >>> row == named_row
    True
    A `Row` object is callable. You can use it to create other `Row` objects:
    >>> Employee = Row("name", "salary")
    >>> emp1 = Employee("John", 10000)
    >>> emp1
    Row(name='John', salary=10000)
    >>> emp2 = Employee("James", 20000)
    >>> emp2
    Row(name='James', salary=20000)
    """

    def __new__(cls, *values: Any, **named_values: Any):
        if values and named_values:
            raise ValueError("Either values or named_values is required but not both.")
        if named_values:
            # After py3.7, dict is ordered(not sorted) by item insertion sequence.
            # If we support 3.6 or older someday, this implementation needs changing.
            row = tuple.__new__(cls, tuple(named_values.values()))
            row.__dict__["_named_values"] = named_values
        else:
            row = tuple.__new__(cls, values)
            row.__dict__["_named_values"] = None

        # __fields__ is for internal use only. Users shouldn't set this attribute.
        # It's None unless the internal code sets it to a list of str values with duplicates.
        # snowflake DB can return duplicate column names, for instance, "select a, a from a_table."
        # When return a DataFrame from a sql, duplicate column names can happen.
        # But using duplicate column names is obviously a bad practice even though we allow it.
        # It's value is assigned in __setattr__ if internal code assign value explcitly.
        row.__dict__["__fields__"] = None
        return row

    def __getitem__(self, item: Union[int, str]):
        if isinstance(item, int):
            return super().__getitem__(item)
        elif isinstance(item, slice):
            return Row(*super().__getitem__(item))
        elif self.__fields__:
            try:
                index = self.__fields__.index(item)
                return super(Row, self).__getitem__(index)
            except (IndexError, ValueError):
                raise KeyError(item)
        else:
            try:
                return self._named_values[item]
            except TypeError:  # _named_values is None
                raise KeyError(item)

    def __setitem__(self, key, value):
        raise TypeError("Row object does not support item assignment")

    def __getattr__(self, item):
        if self.__fields__:  # So there are duplicates. Usually this doesn't happen.
            try:
                index = self.__fields__.index(item)  # may throw ValueError
                return self[index]  # may throw IndexError
            except (IndexError, ValueError):
                raise AttributeError(f"Row object has no attribute {item}")
        try:
            return self._named_values[item]
        except (KeyError, TypeError):
            raise AttributeError(f"Row object has no attribute {item}")

    def __setattr__(self, key, value):
        if key != "__fields__":
            raise AttributeError("Can't set attribute to Row object")
        if value is not None:
            if len(set(value)) != len(value):  # duplicate fields found
                self.__dict__["__fields__"] = value
            else:  # no duplidate fields, keep __field__ None
                self.__dict__["_named_values"] = {k: v for k, v in zip(value, self)}

    def __contains__(self, item):
        return self._named_values and item in self._named_values

    def __call__(self, *args, **kwargs):
        """Create a new Row from current row."""
        if args and kwargs:
            raise ValueError(
                "A Row instance should have either values or field-value pairs but not both."
            )
        elif args and len(args) != len(self):
            raise ValueError(f"{len(self)} values are expected.")
        if self._named_values:
            if args:
                raise ValueError(
                    "The Row object can't be called with a list of values"
                    "because it already has fields and values."
                )
            new_row = Row(**self._named_values)
            for input_key, input_value in kwargs.items():
                if input_key not in self._named_values:
                    raise ValueError(
                        f"Wrong keyword argument {input_key} for Row f{self}"
                    )
                new_row._named_values[input_key] = input_value
            return new_row
        else:
            if kwargs:
                raise ValueError(
                    "The Row object can't be called with field-value pairs "
                    "because it doesn't have field-value pairs"
                )
            if len(args) != len(self) or any(
                not isinstance(value, str) for value in self
            ):
                raise ValueError(
                    "The called Row object with and input values must have the same size."
                )
            return Row(**{k: v for k, v in zip(self, args)})

    def __copy__(self):
        if self._named_values:
            return Row(**self._named_values)
        return Row(*self)

    def __repr__(self):
        if self._named_values:
            return "Row({})".format(
                ", ".join("{}={!r}".format(k, v) for k, v in self._named_values.items())
            )
        else:
            return "Row({})".format(", ".join("{!r}".format(v) for v in self))

    def __reduce__(self):
        return (
            _restore_row_from_pickle,
            (tuple(self), self._named_values, self.__fields__),
        )

    def asDict(self, recursive=False):
        """Convert to a dict if this row object has both keys and values.
        Args:
            recursive: Recursively convert child `Row` objects to dicts. Default is False.
        >>> row = Row(name1=1, name2=2, name3=Row(childname=3))
        >>> row.asDict()
        {'name1': 1, 'name2': 2, 'name3': Row(childname=3)}
        >>> row.asDict(True)
        {'name1': 1, 'name2': 2, 'name3': {'childname': 3}}
        """
        if not self._named_values:
            raise TypeError("Cannot convert a Row without key values to a dict.")
        if not recursive:
            return dict(self._named_values)
        return self._convert_dict(self._named_values)

    def _convert_dict(self, obj):
        if isinstance(obj, Row):
            return obj.asDict(True)
        elif isinstance(obj, Dict):
            child_dict = {}
            for k, v in obj.items():
                child_dict[k] = self._convert_dict(v)
            return child_dict
        elif isinstance(obj, Iterable) and not isinstance(obj, (str, bytes)):
            return [self._convert_dict(x) for x in obj]

        return obj