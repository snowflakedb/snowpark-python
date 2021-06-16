#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

import re
from typing import (
    List,
    Union,
)


class DataType:

    @property
    def type_name(self) -> str:
        """ Returns a data type name. """
        # Strip the suffix 'type'
        return self.__class__.__name__[:-4]

    def to_string(self) -> str:
        """ Returns a data type name. Alias of [[type_name]] """
        return self.type_name

    def __repr__(self) -> str:
        return self.type_name

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

    def to_string(self):
        return f"MapType[{self.key_type.to_string()},{self.value_type.to_string()}]"

    @property
    def type_name(self):
        return self.to_string()

    def __repr__(self):
        self.to_string()


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

    def to_string(self):
        """Returns Decimal Info. Decimal(precision, scale)"""
        return f"Decimal({self.precision},{self.scale})"

    @property
    def type_name(self):
        """Returns Decimal Info. Decimal(precision, scale), Alias of [[toString]]"""
        return self.to_string()

    def __repr__(self):
        return self.to_string()


class ArrayType(DataType):

    def __init__(self, element_type: DataType):
        self.element_type = element_type

    def to_string(self):
        return f'ArrayType[{self.element_type.to_string()}]'

    @property
    def type_name(self):
        """Returns Array Info. ArrayType(DataType), Alias of [[toString]]"""
        return self.to_string()

    def __repr__(self):
        return self.to_string()


# TODO complete
class ColumnIdentifier:
    def __init__(self, normalized_name):
        self.normalized_name = normalized_name

    def name(self) -> str:
        return ColumnIdentifier.strip_unnecessary_quotes(self.normalized_name)

    @staticmethod
    def strip_unnecessary_quotes(string: str) -> str:
        """Removes the unnecessary quotes from name.

        Remove quotes if name starts with _A-Z and only contains _0-9A-Z$, or starts with $ and
        is followed by digits.
        """
        remove_quote = re.compile("^\"(([_A-Z]+[_A-Z0-9$]*)|(\\$\\d+))\"$")
        result = remove_quote.search(string)
        return string[1:-1] if result else string


# TODO complete
class StructField:
    def __init__(self, column_identifier: Union[ColumnIdentifier, str], data_type: DataType,
                 nullable: bool = True):
        self.column_identifier = ColumnIdentifier(column_identifier) if type(column_identifier) == str else column_identifier
        self.data_type = data_type
        self.nullable = nullable

    @property
    def name(self):
        return self.column_identifier.name()

    def to_string(self):
        return f"StructField({self.name}, {self.data_type.type_name}, Nullable={self.nullable})"

    def __repr__(self):
        return self.to_string()

    # TODO
    def tree_string(self, layer: int):
        raise Exception("Not Implemented tree_string()")


class StructType(DataType):

    def __init__(self, fields: List['StructField']):
        self.fields = fields

    @classmethod
    def from_attributes(cls, attributes: list):
        return cls([StructField(a.name, a.datatype, a.nullable) for a in attributes])

    def to_attributes(self):
        raise Exception("Not implemented StructType.toAttributes()")


class GeographyType(AtomicType):
    def to_string(self):
        """Returns GeographyType Info. Decimal(precision, scale)"""
        return "GeographyType"

    @property
    def type_name(self):
        """Returns GeographyType Info. GeographyType, Alias of [[toString]]"""
        return self.to_string()

    def __repr__(self):
        return self.to_string()


# TODO
class Variant:
    def __init__(self, value):
        pass

    def as_json_string(self):
        pass


# TODO
class Geography:
    def as_geo_json(self):
        pass
