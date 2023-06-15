#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from typing import Callable, Dict, NoReturn, Optional, Union

import pandas as pd

from snowflake.snowpark.types import (
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)


class Operator:
    def op(self, *operands):
        pass


class Add(Operator):
    def op(self, *operands):
        if len(operands) == 1:
            return type(operands)


class Minus(Operator):
    ...


class Multiply(Operator):
    ...


class FunctionCall(Operator):
    ...


def calculate_type(
    t1: DataType, t2: DataType, *, op: Union[str, Callable[..., DataType]]
):
    t1t = type(t1)
    t2t = type(t2)
    if t1t == t2t:
        if t1t is DecimalType:
            return DecimalType(38, 0)
        elif t1t in (
            IntegerType,
            LongType,
            DoubleType,
            FloatType,
            StringType,
            TimestampType,
            DateType,
        ):
            return t1
    raise TypeError(f"t1 type: {t1t}, t2 type: {t2t}")


class TableEmulator(pd.DataFrame):
    _metadata = ["sf_types"]

    @property
    def _constructor(self):
        return TableEmulator

    @property
    def _constructor_sliced(self):
        return ColumnEmulator

    def __init__(
        self, *args, sf_types: Optional[Dict[str, DataType]] = None, **kwargs
    ) -> NoReturn:
        super().__init__(*args, **kwargs)
        self.sf_types = {} if not sf_types else sf_types

    def __getitem__(self, item):
        result = super().__getitem__(item)
        if isinstance(result, ColumnEmulator):  # pandas.Series
            result.sf_type = self.sf_types.get(item)
        elif isinstance(result, TableEmulator):  # pandas.DataFrame
            result.sf_types = self.sf_types
        else:
            # TODO: figure out what cases, it may can be removed
            # list of columns
            for ce in result:
                ce.sf_type = self.sf_types.get(ce.name)
        return result

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        if isinstance(value, ColumnEmulator):
            self.sf_types[key] = value.sf_type


class ColumnEmulator(pd.Series):
    @property
    def _constructor(self):
        return ColumnEmulator

    @property
    def _constructor_expanddim(self):
        return TableEmulator

    def __init__(self, *args, **kwargs) -> NoReturn:
        super().__init__(*args, **kwargs)
        self.sf_type = None

    def set_sf_type(self, value):
        self.sf_type = value

    def __add__(self, other):
        result = super().__add__(other)
        # TODO: set sf_type for result by converting from pandas dtype to sf type?
        result.sf_type = calculate_type(self.sf_type, other.sf_type, op="+")
        return result

    def __radd__(self, other):
        result = super().__radd__(other)
        result.sf_type = calculate_type(other.sf_type, self.sf_type, op="+")
        return result

    def __sub__(self, other):
        result = super().__sub__(other)
        result.sf_type = calculate_type(self.sf_type, other.sf_type, op="-")
        return result

    def __rsub__(self, other):
        result = super().__rsub__(other)
        result.sf_type = calculate_type(other.sf_type, self.sf_type, op="-")
        return result

    def __mul__(self, other):
        result = super().__mul__(other)
        result.sf_type = calculate_type(self.sf_type, other.sf_type, op="*")
        return result

    def __rmul__(self, other):
        result = super().__rmul__(other)
        result.sf_type = calculate_type(other.sf_type, self.sf_type, op="*")
        return result
