#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

import datetime
import json
import re
from array import array
from decimal import Decimal
from enum import Enum, auto
from typing import Dict, List, Optional, Union


class DataType:
    @property
    def type_name(self) -> str:
        """Returns a data type name."""
        return self.__repr__()

    def __repr__(self) -> str:
        # Strip the suffix 'type'
        return self.__class__.__name__[:-4]

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)


# Data types


class NullType(DataType):
    pass


class AtomicType(DataType):
    pass


class MapType(DataType):
    def __init__(self, key_type, value_type):
        self.key_type = key_type
        self.value_type = value_type

    def __repr__(self):
        return f"MapType[{str(self.key_type)},{str(self.value_type)}]"

    @property
    def type_name(self):
        return self.__repr__()


class VariantType(DataType):
    pass


# See also StructType in the end of this file.

# Atomic types


class BinaryType(AtomicType):
    pass


class BooleanType(AtomicType):
    """Boolean data type. Mapped to BOOLEAN Snowflake data type."""

    pass


class DateType(AtomicType):
    pass


class StringType(AtomicType):
    pass


class NumericType(AtomicType):
    pass


class TimestampType(AtomicType):
    pass


class TimeType(AtomicType):
    pass


# Numeric types
class IntegralType(NumericType):
    pass


class FractionalType(NumericType):
    pass


class ByteType(IntegralType):
    """Byte data type. Mapped to TINYINT Snowflake date type."""

    pass


class ShortType(IntegralType):
    """Short integer data type. Mapped to SMALLINT Snowflake date type."""

    pass


class IntegerType(IntegralType):
    """Integer data type. Mapped to INT Snowflake date type."""

    pass


class LongType(IntegralType):
    """Long integer data type. Mapped to BIGINT Snowflake date type."""

    pass


class FloatType(FractionalType):
    """Float data type. Mapped to FLOAT Snowflake date type."""

    pass


class DoubleType(FractionalType):
    """Double data type. Mapped to DOUBLE Snowflake date type."""

    pass


class DecimalType(FractionalType):
    """Decimal data type. Mapped to NUMBER Snowflake date type."""

    MAX_PRECISION = 38
    MAX_SCALE = 38

    def __init__(self, precision, scale):
        self.precision = precision
        self.scale = scale

    def __repr__(self):
        return f"Decimal({self.precision},{self.scale})"

    @property
    def type_name(self):
        """Returns Decimal Info. Decimal(precision, scale)."""
        return self.__repr__()


class ArrayType(DataType):
    def __init__(self, element_type: DataType):
        self.element_type = element_type

    def __repr__(self):
        return f"ArrayType[{str(self.element_type)}]"

    @property
    def type_name(self):
        """Returns Array Info. ArrayType(DataType)."""
        return self.__repr__()


# TODO complete
class ColumnIdentifier:
    def __init__(self, normalized_name):
        self.normalized_name = normalized_name

    def name(self) -> str:
        return ColumnIdentifier.strip_unnecessary_quotes(self.normalized_name)

    @property
    def quoted_name(self) -> str:
        return self.normalized_name

    def __eq__(self, other):
        if type(other) == str:
            return self.normalized_name == other
        elif type(other) == ColumnIdentifier:
            return self.normalized_name == other.normalized_name
        else:
            return False

    @staticmethod
    def strip_unnecessary_quotes(string: str) -> str:
        """Removes the unnecessary quotes from name.

        Remove quotes if name starts with _A-Z and only contains _0-9A-Z$, or starts with $ and
        is followed by digits.
        """
        remove_quote = re.compile('^"(([_A-Z]+[_A-Z0-9$]*)|(\\$\\d+))"$')
        result = remove_quote.search(string)
        return string[1:-1] if result else string


# TODO complete
class StructField:
    def __init__(
        self,
        column_identifier: Union[ColumnIdentifier, str],
        datatype: DataType,
        nullable: bool = True,
    ):
        self.column_identifier = (
            ColumnIdentifier(column_identifier)
            if type(column_identifier) == str
            else column_identifier
        )
        self.datatype = datatype
        self.nullable = nullable

    @property
    def name(self):
        return self.column_identifier.name()

    def __repr__(self):
        return f"StructField({self.name}, {self.datatype.type_name}, Nullable={self.nullable})"

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__


class StructType(DataType):
    def __init__(self, fields: List["StructField"]):
        self.fields = fields

    @classmethod
    def from_attributes(cls, attributes: list):
        return cls([StructField(a.name, a.datatype, a.nullable) for a in attributes])

    def to_attributes(self):
        from snowflake.snowpark.internal.analyzer.sf_attribute import Attribute

        return [
            Attribute(f.column_identifier.quoted_name, f.datatype, f.nullable)
            for f in self.fields
        ]

    def __repr__(self):
        return f"StructType[{', '.join(str(f) for f in self.fields)}]"

    @property
    def type_name(self) -> str:
        return self.__class__.__name__[:-4]

    @property
    def names(self):
        return [f.name for f in self.fields]


class GeographyType(AtomicType):
    def __repr__(self):
        """Returns GeographyType Info. Decimal(precision, scale)"""
        return "GeographyType"

    @property
    def type_name(self):
        """Returns GeographyType Info. GeographyType."""
        return self.__repr__()


def _convert_variant(target):
    def wrap(func):
        def inner_wrap(self, *args, **kwargs):
            if (
                self.datatype == target
                or self.datatype == self._VariantTypes.String
                or target == self._VariantTypes.String
                or (
                    self.datatype == self._VariantTypes.Number
                    and target
                    in [self._VariantTypes.Date, self._VariantTypes.Timestamp]
                )
                or (
                    self.datatype == self._VariantTypes.Boolean
                    and target == self._VariantTypes.Number
                )
            ):
                return func(self, *args, **kwargs)
            raise ValueError(
                f"Conversion from Variant of {self.datatype.name} to {target.name} is not supported."
            )

        return inner_wrap

    return wrap


class Variant:
    class _VariantType:
        def __repr__(self):
            return self.__class__.__name__

    class _VariantTypes(_VariantType, Enum):
        Number = auto()
        Boolean = auto()
        String = auto()
        Binary = auto()
        Time = auto()
        Date = auto()
        Timestamp = auto()

    def __encode_value(
        self, value, is_json_string: bool = False, datetime_format: Optional[str] = None
    ):
        try:
            # TODO: SNOW-388805 Use ISO format for datetime/time/date in Variant
            #  when using Python >= 3.7 so the user doesn't need to provide format
            if type(value) in [datetime.time, datetime.date, datetime.datetime]:
                # return json.dumps(value.isoformat())
                assert datetime_format, "The format of datetime should be specified."
                return json.dumps(value.strftime(datetime_format))
            elif type(value) == Decimal:
                return json.dumps(float(value))
            elif type(value) in [list, tuple]:
                return [self.__encode_value(item) for item in value]
            elif type(value) == array:
                return self.__encode_value(value.tolist())
            elif type(value) == dict:
                return {str(k): self.__encode_value(v) for k, v in value.items()}
            elif type(value) in [bytearray, bytes]:
                return json.dumps(value.decode("utf8").replace("'", '"'))
            elif type(value) == Variant:
                return value.value
            elif is_json_string:
                return json.loads(value)
            else:
                return json.dumps(value)
        except TypeError:
            raise TypeError(
                "Object {} of type {} is not supported for Snowflake Variant.".format(
                    value, value.__class__.__name__
                )
            )

    def __decode_value(self, value):
        if self.is_json_string:
            return value
        if type(value) == list:
            return [self.__decode_value(item) for item in value]
        elif type(value) == dict:
            return {k: self.__decode_value(v) for k, v in value.items()}
        else:
            return json.loads(value)

    def __init__(
        self, value, is_json_string: bool = False, datetime_format: Optional[str] = None
    ):
        """The constructor of Variant object, which accepts a variety of data types as the input.

        :param value: the data building the Variant object
        :parma is_json_string: True if the input is a valid json string
        :param datetime_format: The format string for datetime/time/date object
        """

        if is_json_string and type(value) != str:
            raise TypeError(
                "The input must be a string if `is_json_string` is set to True."
            )
        self.value = self.__encode_value(value, is_json_string, datetime_format)
        self.datetime_format = datetime_format
        self.is_json_string = is_json_string

        if type(value) in [int, float, Decimal]:
            self.datatype = self._VariantTypes.Number
        elif type(value) == bool:
            self.datatype = self._VariantTypes.Boolean
        elif type(value) in [str, list, tuple, array]:
            self.datatype = self._VariantTypes.String
        elif type(value) in [bytearray, bytes]:
            self.datatype = self._VariantTypes.Binary
        elif type(value) == datetime.time:
            self.datatype = self._VariantTypes.Time
        elif type(value) == datetime.date:
            self.datatype = self._VariantTypes.Date
        elif type(value) == datetime.datetime:
            self.datatype = self._VariantTypes.Timestamp
        else:
            self.datatype = self._VariantTypes.String

    @_convert_variant(_VariantTypes.Number)
    def as_int(self) -> int:
        return int(self.__decode_value(self.value))

    @_convert_variant(_VariantTypes.Number)
    def as_float(self) -> float:
        return float(self.__decode_value(self.value))

    @_convert_variant(_VariantTypes.Number)
    def as_decimal(self) -> Decimal:
        return Decimal(self.__decode_value(self.value))

    @_convert_variant(_VariantTypes.Boolean)
    def as_bool(self) -> bool:
        return bool(self.__decode_value(self.value))

    @_convert_variant(_VariantTypes.String)
    def as_string(self) -> str:
        return str(self.__decode_value(self.value))

    def as_json_string(self) -> str:
        return self.as_string()

    def __repr__(self) -> str:
        return self.as_string()

    @_convert_variant(_VariantTypes.Binary)
    def as_bytes(self) -> bytes:
        res = self.__decode_value(self.value)
        return bytes(res, "utf8") if type(res) == str else bytes(res)

    def as_bytearray(self) -> bytearray:
        return bytearray(self.as_bytes())

    @_convert_variant(_VariantTypes.Time)
    def as_time(self) -> datetime.time:
        # return datetime.time.fromisoformat(self.__decode_value(self.value))
        return datetime.datetime.strptime(
            self.__decode_value(self.value), self.datetime_format
        ).time()

    @_convert_variant(_VariantTypes.Date)
    def as_date(self) -> datetime.date:
        if self.datatype == self._VariantTypes.Number:
            return datetime.date.fromtimestamp(self.__decode_value(self.value))
        else:
            # return datetime.date.fromisoformat(self.__decode_value(self.value))
            return datetime.datetime.strptime(
                self.__decode_value(self.value), self.datetime_format
            ).date()

    @_convert_variant(_VariantTypes.Timestamp)
    def as_datetime(self) -> datetime.datetime:
        if self.datatype == self._VariantTypes.Number:
            return datetime.datetime.fromtimestamp(self.__decode_value(self.value))
        else:
            # return datetime.datetime.fromisoformat(self.__decode_value(self.value))
            return datetime.datetime.strptime(
                self.__decode_value(self.value), self.datetime_format
            )

    def as_list(self) -> List["Variant"]:
        return (
            [Variant(self.__decode_value(item)) for item in self.value]
            if type(self.value) == list
            else [Variant(self.__decode_value(self.value))]
        )

    def as_dict(self) -> Dict[str, "Variant"]:
        return {k: Variant(self.__decode_value(v)) for k, v in self.value.items()}

    def __eq__(self, other):
        return type(other) == Variant and self.value == other.value


# TODO
class Geography:
    def as_geo_json(self):
        pass
