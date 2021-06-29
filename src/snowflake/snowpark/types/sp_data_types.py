#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#


class AbstractDataType:
    pass


class DataType(AbstractDataType):
    @property
    def type_name(self):
        """Returns a data type name."""
        return self.__class__.__name__[:-4]

    @property
    def to_string(self):
        """Returns a data type name. Alias of [[type_name]]"""
        return self.type_name

    @property
    def simple_string(self):
        return self.type_name

    @property
    def sql(self):
        return self.simple_string.upper()


# Data types
class AtomicType(DataType):
    pass


class ArrayType(DataType):
    def __init__(self, element_type: DataType, contains_null: bool):
        self.element_type = element_type
        self.contains_null = contains_null


class MapType(DataType):
    def __init__(
        self, key_type: DataType, value_type: DataType, value_contains_null: bool
    ):
        self.key_type = key_type
        self.value_type = value_type
        self.value_contains_null = value_contains_null


class NullType(DataType):
    pass


# TODO might require more work
class StructType(DataType):
    def __init__(self, fields: list):
        self.fields = fields


# TODO might require more work
class StructField:
    def __init__(self, name: str, datatype: DataType, nullable: bool, metadata=None):
        self.name = name
        self.datatype = datatype
        self.nullable = nullable
        self.metadata = metadata


class VariantType(DataType):
    @property
    def sql(self):
        return "VARIANT"

    @property
    def simple_string(self):
        return "variant"

    @property
    def catalog_string(self):
        return "variant"


class GeographyType(DataType):
    def __init__(self, element_type: DataType):
        self.element_type = element_type

    @property
    def type_name(self) -> str:
        return f"GeographyType[${self.element_type.to_string}]"

    @property
    def to_string(self) -> str:
        """Returns a data type name. Alias of [[type_name]]"""
        return self.type_name

    @property
    def simple_string(self) -> str:
        return self.type_name

    @property
    def sql(self) -> str:
        return self.simple_string.upper()


# Atomic Types


class BooleanType(AtomicType):
    pass


class StringType(AtomicType):
    pass


class NumericType(AtomicType):
    pass


class DateType(AtomicType):
    pass


class TimestampType(AtomicType):
    pass


class TimeType(DataType):
    pass


class BinaryType(AtomicType):
    pass


# Numeric Types
class IntegralType(NumericType):
    pass


class FractionalType(NumericType):
    pass


# Integral types
class ShortType(IntegralType):
    pass


class ByteType(IntegralType):
    pass


class IntegerType(IntegralType):
    pass


class LongType(IntegralType):
    pass


# Fractional types
class FloatType(FractionalType):
    pass


class DoubleType(FractionalType):
    pass


class DecimalType(FractionalType):
    MAX_PRECISION = 38
    MAX_SCALE = 38

    def __init__(self, precision, scale):
        self.precision = precision
        self.scale = scale
