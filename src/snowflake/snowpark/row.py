#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import sys
from typing import Any, Dict, Union

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable


def _restore_row_from_pickle(values, named_values, fields):
    if named_values:
        row = Row(**named_values)
    else:
        row = Row(*values)
    row._fields = fields
    return row


def is_already_quoted(value: str) -> bool:
    value = value.strip()
    return len(value) > 2 and (
        value.startswith("'")
        and value.endswith("'")
        or value.startswith('"')
        and value.endswith('"')
    )


def canonicalize_field(value: str) -> str:
    value = value.strip()
    return value.lower()


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

    A ``Row`` object is callable. You can use it to create other ``Row`` objects:

    >>> Employee = Row("name", "salary")
    >>> emp1 = Employee("John", 10000)
    >>> emp1
    Row(name='John', salary=10000)
    >>> emp2 = Employee("James", 20000)
    >>> emp2
    Row(name='James', salary=20000)

    """

    class _RowBuilder:
        def __init__(self) -> None:
            self._values = None
            self._named_values = None

        def build(self, *values: Any, **named_values: Any) -> "Row._RowBuilder":
            self._values = values
            self._named_values = named_values
            self._case_sensitive = True
            return self

        def set_case_sensitive(self, case_sensitive: bool) -> "Row._RowBuilder":
            self._case_sensitive = case_sensitive
            return self

        def to_row(self) -> "Row":
            if self._named_values and not self._case_sensitive:
                if any([is_already_quoted(val) for val in self._named_values.keys()]):
                    raise ValueError(
                        "Case insensitive fields is not supported in presence of quoted columns"
                    )
                self._named_values = {
                    canonicalize_field(k): v for k, v in self._named_values.items()
                }

            if self._values and not self._case_sensitive:
                if any([is_already_quoted(val) for val in self._values]):
                    raise ValueError(
                        "Case insensitive fields is not supported in presence of quoted columns"
                    )
                self._values = [canonicalize_field(value) for value in self._values]

            row = Row(*self._values, **self._named_values)
            row.__dict__["_case_sensitive"] = self._case_sensitive
            return row

    _builder = _RowBuilder()

    def __new__(cls, *values: Any, **named_values: Any):
        if values and named_values:
            raise ValueError("Either values or named_values is required but not both.")
        if named_values:
            # After py3.7, dict is ordered(not sorted) by item insertion sequence.
            # If we support 3.6 or older someday, this implementation needs changing.
            row = tuple.__new__(cls, tuple(named_values.values()))
            row.__dict__["_named_values"] = named_values
            row.__dict__["_fields"] = tuple(named_values.keys())
        else:
            row = tuple.__new__(cls, values)
            row.__dict__["_named_values"] = None
            row.__dict__["_fields"] = None

        # _fields is for internal use only. Users shouldn't set this attribute.
        # It contains a list of str representing column names. It also allows duplicates.
        # snowflake DB can return duplicate column names, for instance, "select a, a from a_table."
        # When return a DataFrame from a sql, duplicate column names can happen.
        # But using duplicate column names is obviously a bad practice even though we allow it.
        # It's value is assigned in __setattr__ if internal code assign value explicitly.
        row.__dict__["_has_duplicates"] = None
        row.__dict__["_case_sensitive"] = True
        return row

    def __getitem__(self, item: Union[int, str, slice]):
        if isinstance(item, int):
            return super().__getitem__(item)
        elif isinstance(item, slice):
            return Row(*super().__getitem__(item))
        else:  # str
            if not self._case_sensitive:
                item = canonicalize_field(item)
            self._populate_named_values_from_fields()
            # get from _named_values first
            if self._named_values:
                return self._named_values[item]
            # we have duplicated fields and _named_values is not populated,
            # so indexing fields
            elif self._fields and self._check_if_having_duplicates():
                try:
                    index = self._fields.index(item)  # may throw ValueError
                    return super().__getitem__(index)  # may throw IndexError
                except (IndexError, ValueError):
                    raise KeyError(item)
            # no column names/keys/fields
            else:
                raise KeyError(item)

    def __setitem__(self, key, value):
        raise TypeError("Row object does not support item assignment")

    def __getattr__(self, item):
        if not self._case_sensitive:
            item = canonicalize_field(item)
        self._populate_named_values_from_fields()
        if self._named_values and item in self._named_values:
            return self._named_values[item]
        elif self._fields and self._check_if_having_duplicates():
            try:
                index = self._fields.index(item)  # may throw ValueError
                return self[index]  # may throw IndexError
            except (IndexError, ValueError):
                raise AttributeError(f"Row object has no attribute {item}")
        else:
            raise AttributeError(f"Row object has no attribute {item}")

    def __setattr__(self, key, value):
        if key != "_fields":
            raise AttributeError("Can't set attribute to Row object")
        if value is not None:
            if not self._case_sensitive:
                value = [canonicalize_field(val) for val in value]
            self.__dict__["_fields"] = value

    def __contains__(self, item):
        self._populate_named_values_from_fields()
        if self._named_values:
            return item in self._named_values
        elif self._fields:
            return item in self._fields
        else:
            return super().__contains__(item)

    def __call__(self, *args, **kwargs):
        """Create a new Row from current row."""
        if args and kwargs:
            raise ValueError(
                "A Row instance should have either values or field-value pairs but not both."
            )
        elif args and len(args) != len(self):
            raise ValueError(f"{len(self)} values are expected.")
        self._populate_named_values_from_fields()
        if self._named_values:
            if args:
                raise ValueError(
                    "The Row object can't be called with a list of values "
                    "because it already has fields and values."
                )
            new_row = Row(**self._named_values)
            for input_key, input_value in kwargs.items():
                if input_key not in self._named_values:
                    raise ValueError(f"Wrong keyword argument {input_key} for {self}")
                new_row._named_values[input_key] = input_value
            return new_row
        elif self._fields and self._check_if_having_duplicates():
            raise ValueError(
                "The Row object can't be called because it has duplicate fields"
            )
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
                    "The called Row object and input values must have the same size and the called Row object shouldn't have any non-str fields."
                )
            new_row = Row(*args)
            # row might have duplicated column names
            new_row.__dict__["_fields"] = [col for col in self]
            new_row.__dict__["_case_sensitive"] = self._case_sensitive
            return new_row

    def __copy__(self):
        return _restore_row_from_pickle(self, self._named_values, self._fields)

    def __repr__(self):
        if self._fields:
            return "Row({})".format(
                ", ".join(f"{k}={v!r}" for k, v in zip(self._fields, self))
            )
        elif self._named_values:  # pragma: no cover
            # this might not be reachable because when there is self._named_values, there is always self._fields
            # Need to review later.
            return "Row({})".format(
                ", ".join(f"{k}={v!r}" for k, v in self._named_values.items())
            )

        else:
            return "Row({})".format(", ".join(f"{v!r}" for v in self))

    def __reduce__(self):
        return (
            _restore_row_from_pickle,
            (tuple(self), self._named_values, self._fields),
        )

    def as_dict(self, recursive: bool = False) -> Dict:
        """Convert to a dict if this row object has both keys and values.

        Args:
            recursive: Recursively convert child :class:`Row` objects to dicts. Default is False.

        >>> row = Row(name1=1, name2=2, name3=Row(childname=3))
        >>> row.as_dict()
        {'name1': 1, 'name2': 2, 'name3': Row(childname=3)}
        >>> row.as_dict(True)
        {'name1': 1, 'name2': 2, 'name3': {'childname': 3}}
        """
        self._populate_named_values_from_fields()
        if not self._named_values:
            raise TypeError(
                "Cannot convert a Row without key values or duplicated keys to a dict."
            )
        if not recursive:
            return dict(self._named_values)
        return self._convert_dict(self._named_values)

    def _convert_dict(
        self, obj: Union["Row", Dict, Iterable[Union["Row", Dict]]]
    ) -> Union[Dict, Iterable[Dict]]:
        if isinstance(obj, Row):
            return obj.as_dict(True)
        elif isinstance(obj, dict):
            child_dict = {}
            for k, v in obj.items():
                child_dict[k] = self._convert_dict(v)
            return child_dict
        elif isinstance(obj, Iterable) and not isinstance(obj, (str, bytes, bytearray)):
            return [self._convert_dict(x) for x in obj]

        return obj

    def _populate_named_values_from_fields(self) -> None:
        # populate _named_values dict if we have unduplicated fields
        if (
            self._named_values is None
            and self._fields
            and not self._check_if_having_duplicates()
        ):
            self.__dict__["_named_values"] = {k: v for k, v in zip(self._fields, self)}

    def _check_if_having_duplicates(self) -> bool:
        if self._has_duplicates is None:
            # Usually we don't have duplicate keys
            self.__dict__["_has_duplicates"] = bool(
                len(set(self._fields)) != len(self._fields)
            )
        return self._has_duplicates

    # Add aliases for user code migration
    asDict = as_dict
