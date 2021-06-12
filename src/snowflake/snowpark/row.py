#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

from typing import List


class Row:

    def __init__(self, values):
        # TODO input validation
        self.values = [i for i in values] if hasattr(values, '__iter__') else [values]

    def size(self):
        return self.length()

    def length(self):
        return len(self.values)

    def __len__(self) -> int:
        return self.length()

    def get(self, index):
        return self.values[index]

    def copy(self):
        return Row(self.values)

    # TODO complete, need to determine NaN handling, consider switching to all for ineq-check
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

    # TODO do we need it? requires Mumrmurhash3
    def hashcode(self):
        pass

    def is_null_at(self, index):
        return self.get(index) is None

    def get_boolean(self, index):
        return bool(self.get(index))
    
    # TODO
    def get_byte(self, index):
        pass
    
    # TODO is this even applicable in python?
    def get_short(self, index):
        pass
        
    def get_int(self, index):
        return int(self.get(index))

    # TODO is this even applicable in python?
    def get_long(self, index):
        pass

    def get_float(self, index):
        return float(self.get(index))
    
    # TODO look into the case of parsing string.
    #  >15 digits get truncated, we might have to use decimal
    def get_double(self, index):
        return float(self.get(index))
    
    def get_string(self, index):
        return str(self.get(index))
    
    # TODO 
    def get_binary(self, index):
        pass

    # TODO 
    def get_decimal(self, index):
        pass

    # TODO 
    def get_date(self, index):
        pass

    # TODO 
    def get_time(self, index):
        pass

    # TODO 
    def get_local_date(self, index):
        pass

    # TODO 
    def get_timestamp(self, index):
        pass

    # TODO 
    def get_instant(self, index):
        pass

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
        return f"Row[" + ','.join([str(self.get(i)) for i in range(self.size())]) + "]"

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
