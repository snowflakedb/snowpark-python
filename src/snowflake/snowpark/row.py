#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

from decimal import Decimal
from functools import total_ordering
from typing import List, Union


def _restore_row_from_pickle(values, named_values):
    if named_values:
        return Row(**named_values)
    return Row(*values)


@total_ordering
class Row:
    """ """

    def __init__(self, *values, **named_values):
        if values and named_values:
            raise ValueError(
                "A Row instance should have either values or named values but not both together."
            )
        if named_values:
            # After py3.7, dict is ordered (not sorted). If we support 3.6 or older, this implementation needs changing.
            self.__dict__["_values"] = tuple(named_values.values())
            self.__dict__["_named_values"] = named_values
        else:
            self.__dict__["_values"] = tuple(values) if values else ()
            self.__dict__["_named_values"] = None

    def __len__(self) -> int:
        return len(self._values)

    def __getitem__(self, item: Union[int, str]):
        if isinstance(item, int):
            return self._values[item]
        else:
            return self._named_values[item]

    def __setitem__(self, key, value):
        raise TypeError("'Row' object does not support item assignment")

    def __getattr__(self, item):
        return self._named_values.get(item)

    def __setattr__(self, key, value):
        if key not in ("_values", "_named_values"):
            raise AttributeError("Can't set attribute to 'Row' object")

    def __contains__(self, item):
        return item in self._named_values

    def __call__(self, *args, **kwargs):
        """create a new Row from current row."""
        if self._named_values:
            return Row(**{**self._name_values, **kwargs})
        else:
            return Row(**{k: v for k, v in zip(self._values, args)})

    def __copy__(self):
        return Row(*self._values, **self._named_values)

    def __eq__(self, other):
        return self._values == other._values

    def __lt__(
        self, other
    ):  # __le__(), __gt__(), and __ge__() are automatically defined by @total_ordering
        return self._values < other._values

    def __hash__(self):
        return hash(self._values)

    def __repr__(self):
        if self._named_values:
            return "Row(%s)" % ", ".join(
                "{}={!r}".format(k, v) for k, v in self._named_values.items()
            )
        else:
            return "Row(%s)" % ", ".join("%r" % v for v in self._values)

    def __iter__(self):
        return iter(self._values)

    def __reduce__(self):
        return (_restore_row_from_pickle, (self._values, self._named_values))

    def count(self, value):
        return self._values.count(value)

    def index(self, value, start=None, stop=None):
        return self._values.index(start, stop)

    def asDict(self, recursive=False):
        if not self._named_values:
            raise TypeError("Can not convert a Row without key values to a dict")
        if not recursive:
            return dict(self._named_values)
        return self._convert_dict(self._named_values)

    def _convert_dict(self, obj):
        if isinstance(obj, Row):
            return obj.asDict(True)
        elif isinstance(obj, list):
            return [self._convert_object(x) for x in obj]
        elif isinstance(obj, dict):
            return {k: self._convert_object(v) for k, v in obj.items()}
        return obj
