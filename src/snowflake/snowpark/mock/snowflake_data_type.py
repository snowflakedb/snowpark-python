#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import dataclasses
from typing import Callable, Dict, NoReturn, Optional, Type, Union

import pandas as pd

from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    GeographyType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    TimestampType,
    TimeType,
    VariantType,
)


class Operator:
    def op(self, *operands):
        pass


class Add(Operator):
    def op(self, *operands):
        if len(operands) == 1:
            return type(operands[0])


class Minus(Operator):
    ...


class Multiply(Operator):
    ...


class FunctionCall(Operator):
    ...


"""
https://docs.snowflake.com/en/sql-reference/data-type-conversion
"""


@dataclasses.dataclass
class SnowDataTypeConversion:
    from_type: Type[DataType]
    to_type: Type[DataType]
    castable: bool
    coercible: bool


SNOW_DATA_TYPE_CONVERSION_LIST = [
    SnowDataTypeConversion(ArrayType, StringType, True, False),
    SnowDataTypeConversion(ArrayType, VariantType, True, True),
    SnowDataTypeConversion(BinaryType, StringType, True, False),
    SnowDataTypeConversion(BinaryType, VariantType, True, False),
    SnowDataTypeConversion(BooleanType, DecimalType, True, False),
    SnowDataTypeConversion(BooleanType, StringType, True, True),
    SnowDataTypeConversion(BooleanType, VariantType, True, True),
    SnowDataTypeConversion(DateType, TimestampType, True, False),
    SnowDataTypeConversion(DateType, StringType, True, True),
    SnowDataTypeConversion(DateType, VariantType, True, False),
    SnowDataTypeConversion(FloatType, BooleanType, True, True),
    SnowDataTypeConversion(FloatType, DecimalType, True, True),
    SnowDataTypeConversion(FloatType, StringType, True, True),
    SnowDataTypeConversion(FloatType, VariantType, True, True),
    SnowDataTypeConversion(GeographyType, VariantType, True, False),
    # SnowDataTypeConversion(GeometryType, VariantType, True, False),  # GeometryType isn't available yet.
    SnowDataTypeConversion(DecimalType, BooleanType, True, True),
    SnowDataTypeConversion(DecimalType, FloatType, True, True),
    SnowDataTypeConversion(DecimalType, TimestampType, True, True),
    SnowDataTypeConversion(DecimalType, StringType, True, True),
    SnowDataTypeConversion(DecimalType, VariantType, True, True),
    SnowDataTypeConversion(MapType, ArrayType, True, False),
    SnowDataTypeConversion(MapType, StringType, True, False),
    SnowDataTypeConversion(MapType, VariantType, True, True),
    SnowDataTypeConversion(TimeType, StringType, True, True),
    SnowDataTypeConversion(TimeType, VariantType, True, False),
    SnowDataTypeConversion(TimestampType, DateType, True, True),
    SnowDataTypeConversion(TimestampType, TimeType, True, True),
    SnowDataTypeConversion(TimestampType, StringType, True, True),
    SnowDataTypeConversion(TimestampType, VariantType, True, False),
    SnowDataTypeConversion(StringType, BooleanType, True, True),
    SnowDataTypeConversion(StringType, DateType, True, True),
    SnowDataTypeConversion(StringType, FloatType, True, True),
    SnowDataTypeConversion(StringType, DecimalType, True, True),
    SnowDataTypeConversion(StringType, TimeType, True, True),
    SnowDataTypeConversion(StringType, TimestampType, True, True),
    SnowDataTypeConversion(StringType, VariantType, True, False),
    SnowDataTypeConversion(VariantType, DateType, True, True),
    SnowDataTypeConversion(VariantType, FloatType, True, True),
    SnowDataTypeConversion(VariantType, GeographyType, True, False),
    SnowDataTypeConversion(VariantType, DecimalType, True, True),
    SnowDataTypeConversion(VariantType, MapType, True, True),
    SnowDataTypeConversion(VariantType, TimeType, True, True),
    SnowDataTypeConversion(VariantType, TimestampType, True, True),
    SnowDataTypeConversion(VariantType, StringType, True, True),
]


SNOW_DATA_TYPE_CONVERSION_DICT = {
    (x.from_type, x.to_type): x for x in SNOW_DATA_TYPE_CONVERSION_LIST
}


def normalize_decimal(d: DecimalType):
    if d.scale > d.precision or d.scale > 38 or d.scale < 0 or d.precision < 0:
        raise ValueError(
            f"Inferred data type DecimalType({d.precision}, {d.scale}) is invalid."
        )
    if d.precision > 38:
        d.precision = 38


def calculate_type(
    t1: DataType, t2: Optional[DataType], op: Union[str, Callable[..., DataType]]
):
    """op, left, right decide what's next."""
    decimal_types = (IntegerType, LongType, DecimalType)
    if isinstance(t1, decimal_types) and isinstance(t2, decimal_types):
        p1, s1 = get_number_precision_scale(t1)
        p2, s2 = get_number_precision_scale(t2)
        if op == "/":
            division_min_scale = 6
            division_max_scale = 12
            l1 = p1 - s1
            res_scale = max(min(s1 + division_min_scale, division_max_scale), s1)
            res_lead = l1 + s2
            res_precision = res_scale + res_lead
            return DecimalType(res_precision, res_scale)
        elif op == "*":
            multiplication_max_scale = 12
            s1 = t1.scale
            s2 = t2.scale
            l1 = t1.precision - s1
            l2 = t2.precision - s2
            result_scale = min(s1 + s2, max(multiplication_max_scale, max(s1, s2)))
            result_precision = result_scale + l1 + l2
            result = DecimalType(result_precision, result_scale)
            normalize_decimal(result)
            return result
        elif op in ("+", "-"):
            return DecimalType(
                min(38, max(t1.precision, t2.precision) + 1), max(t1.scale, t2.scale)
            )
        elif op == "%":
            ...
        else:
            return None
    elif isinstance(
        t1, (FloatType, DoubleType) or isinstance(t2, (FloatType, DoubleType))
    ):
        return t1
    elif (
        isinstance(t1, decimal_types)
        and isinstance(t2, (FloatType, DoubleType))
        or (isinstance(t2, decimal_types) and isinstance(t1, (FloatType, DoubleType)))
    ):
        return FloatType()
    elif isinstance(t1, DateType) or isinstance(t2, DateType):
        if isinstance(t2, DateType):
            t1, t2 = t2, t1
        if t2 not in (
            IntegerType,
            LongType,
            DecimalType,
            FloatType,
            DoubleType,
        ) or op not in ("+", "-"):
            raise ValueError(
                f"Result data type can't be calculated: (type1: {t1}, op: '{op}', type2: {t2})."
            )
        return DateType

    raise TypeError(
        f"Result data type can't be calculated: (type1: {t1}, op: '{op}', type2: {t2})."
    )


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
        self.sf_types[key] = value.sf_type


def get_number_precision_scale(t: DataType):
    if isinstance(t, (IntegerType, LongType)):
        return 38, 0
    if isinstance(t, DecimalType):
        return t.precision, t.scale
    return None, None


class ColumnEmulator(pd.Series):
    @property
    def _constructor(self):
        return ColumnEmulator

    @property
    def _constructor_expanddim(self):
        return TableEmulator

    def __init__(self, *args, **kwargs) -> NoReturn:
        sf_type = kwargs.pop("sf_type", None)
        super().__init__(*args, **kwargs)
        self.sf_type = sf_type

    def set_sf_type(self, value):
        self.sf_type = value

    def __add__(self, other):
        if isinstance(self.sf_type, DateType):
            precision, scale = get_number_precision_scale(other)
            if scale == 0:
                result = super() + pd.DateOffset(1)
                result.sf_types = DateType()
                return result
        result = super().__add__(other)
        result.sf_type = calculate_type(self.sf_type, other.sf_type, op="+")
        return result

    def __radd__(self, other):
        if isinstance(self.sf_type, DateType):
            precision, scale = get_number_precision_scale(other)
            if scale == 0:
                result = super() + pd.DateOffset(1)
                result.sf_types = DateType()
                return result
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

    def __bool__(self):
        result = super().__bool__()
        result.sf_type = BooleanType()
        return result

    def __and__(self, other):
        result = super().__and__(other)
        result.sf_type = BooleanType()
        return result

    def __or__(self, other):
        result = super().__or__(other)
        result.sf_type = BooleanType()
        return result

    def __ne__(self, other):
        result = super().__ne__(other)
        result.sf_type = BooleanType()
        return result

    def __xor__(self, other):
        result = super().__xor__(other)
        result.sf_type = BooleanType()
        return result

    def __pow__(self, power, modulo=None):
        result = super().__pow__(power, modulo)
        result.sf_type = DoubleType()
        return result

    def __ge__(self, other):
        result = super().__ge__(other)
        result.sf_type = BooleanType()
        return result

    def __gt__(self, other):
        result = super().__gt__(other)
        result.sf_type = BooleanType()
        return result

    def __invert__(self):
        result = super().__invert__()
        result.sf_type = BooleanType()
        return result

    def __le__(self, other):
        result = super().__le__(other)
        result.sf_type = BooleanType()
        return result

    def __lt__(self, other):
        result = super().__lt__(other)
        result.sf_type = BooleanType()
        return result

    def __neg__(self):
        result = super().__neg__
        result.sf_type = self.sf_type
        return result

    def __rand__(self, other):
        result = super().__rand__(other)
        result.sf_type = BooleanType()
        return result

    def __mod__(self, other):
        result = super().__mod__(other)
        result.sf_type = calculate_type(self.sf_type, other.sf_type, op="%")
        return result

    def __rmod__(self, other):
        result = super().__mod__(other)
        result.sf_type = calculate_type(other.sf_type, self.sf_type, op="%")
        return result

    def __ror__(self, other):
        result = super().__ror__(other)
        result.sf_type = BooleanType()
        return result

    def __round__(self, n=None):
        result = super().__round__(n)
        if isinstance(self.sf_type, (FloatType, DoubleType)):
            result.sf_type = self.sf_type
        elif isinstance(self.sf_type, DecimalType):
            scale = n if self.sf_type.scale > n else self.sf_type.scale
            precision = (
                self.sf_type.precision
                if scale == self.sf_type.scale
                else min(self.sf_type.precision + 1, 38)
            )
            result.sf_type = DecimalType(precision, scale)
        return result

    def __rpow__(self, other):
        result = super().__rpow__(other)
        result.sf_type = FloatType()
        return result

    def __rtruediv__(self, other):
        result = super().__mul__(other)
        result.sf_type = calculate_type(other.sf_type, self.sf_type, op="/")
        return result

    def __truediv__(self, other):
        result = super().__mul__(other)
        result.sf_type = calculate_type(self.sf_type, other.sf_type, op="/")
        return result
