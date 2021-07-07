#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

from decimal import Decimal
from typing import List


class Row:
    def __init__(self, values):
        self.values = (
            [i for i in values] if isinstance(values, (list, tuple)) else [values]
        )

    def size(self):
        return self.length()

    def length(self):
        return len(self.values)

    def __len__(self) -> int:
        return self.length()

    def get(self, index):
        return self.values[index]

    def __getitem__(self, item: int):
        return self.get(item)

    def copy(self):
        return Row(self.values)

    def __eq__(self, other):
        if type(other) is not Row:
            return False
        else:
            if len(self.values) != other.size():
                return False
            for x, y in zip(self.to_list(), other.to_list()):
                if x != y:
                    return False
        return True

    # def hashcode(self):
    #    pass

    def is_null_at(self, index):
        return self.get(index) is None

    def get_boolean(self, index):
        return bool(self.get(index))

    def get_int(self, index):
        return int(self.get(index))

    def get_float(self, index):
        return float(self.get(index))

    # TODO look into the case of parsing string.
    #  >15 digits get truncated, we might have to use decimal
    def get_double(self, index):
        return float(self.get(index))

    def get_string(self, index):
        return str(self.get(index))

    def get_decimal(self, index):
        return Decimal(self.get(index))

    def get_list(self, index):
        return [self.get(index)]

    # TODO
    def get_map(self, index):
        pass

    # TODO
    def get_variant(self, index):
        pass

    # TODO
    def get_geography(self, index):
        pass

    # TODO
    def get_list_of_variant(self, index):
        pass

    # TODO
    def get_map_of_variant(self, index):
        pass

    # TODO
    def get_struct(self, index):
        pass

    # TODO
    def to_string(self):
        return f"Row[" + ",".join([str(self.get(i)) for i in range(self.size())]) + "]"

    def to_list(self):
        return [self.get(i) for i in range(self.size())]

    @classmethod
    def from_list(cls, values: List):
        return cls(values)

    # TODO
    def __repr__(self):
        return self.to_string()

    def __iter__(self):
        yield from self.values
