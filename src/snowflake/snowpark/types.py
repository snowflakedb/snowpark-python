#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
"""This package contains all Snowpark logical types."""
import re
from typing import List, Union


class DataType:
    """The base class of Snowpark data types"""
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
    """Represents a null type."""
    pass


class _AtomicType(DataType):
    pass


class MapType(DataType):
    """Map data type. This maps to OBJECT data type in Snowflake."""
    def __init__(self, key_type, value_type):
        self.key_type = key_type
        self.value_type = value_type

    def __repr__(self):
        return f"MapType[{str(self.key_type)}, {str(self.value_type)}]"

    @property
    def type_name(self):
        return self.__repr__()


class VariantType(DataType):
    """Variant data type. This maps to VARIANT data type in Snowflake."""
    pass


# See also StructType in the end of this file.

# Atomic types


class BinaryType(_AtomicType):
    """Binary data type. Mapped to BINARY Snowflake data type."""
    pass


class BooleanType(_AtomicType):
    """Boolean data type. Mapped to BOOLEAN Snowflake data type."""
    pass


class DateType(_AtomicType):
    """Date data type. Mapped to DATE Snowflake data type."""
    pass


class StringType(_AtomicType):
    """String data type. Mapped to VARCHAR Snowflake data type."""
    pass


class _NumericType(_AtomicType):
    pass


class TimestampType(_AtomicType):
    """Timestamp data type. Mapped to TIMESTAMP Snowflake data type."""
    pass


class TimeType(_AtomicType):
    """Time data type. Mapped to TIME Snowflake data type."""
    pass


# Numeric types
class _IntegralType(_NumericType):
    pass


class _FractionalType(_NumericType):
    pass


class ByteType(_IntegralType):
    """Byte data type. Mapped to TINYINT Snowflake date type."""
    pass


class ShortType(_IntegralType):
    """Short integer data type. Mapped to SMALLINT Snowflake date type."""
    pass


class IntegerType(_IntegralType):
    """Integer data type. Mapped to INT Snowflake date type."""
    pass


class LongType(_IntegralType):
    """Long integer data type. Mapped to BIGINT Snowflake date type."""
    pass


class FloatType(_FractionalType):
    """Float data type. Mapped to FLOAT Snowflake date type."""
    pass


class DoubleType(_FractionalType):
    """Double data type. Mapped to DOUBLE Snowflake date type."""
    pass


class DecimalType(_FractionalType):
    """Decimal data type. Mapped to NUMBER Snowflake date type."""

    MAX_PRECISION = 38
    MAX_SCALE = 38

    def __init__(self, precision: int = 38, scale: int = 0):
        self.precision = precision
        self.scale = scale

    def __repr__(self):
        return f"Decimal({self.precision}, {self.scale})"

    @property
    def type_name(self):
        """Returns Decimal Info. Decimal(precision, scale)."""
        return self.__repr__()


class ArrayType(DataType):
    """Array data type. Mapped to ARRAY Snowflake date type."""
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
    """Represents a Column Identifier."""
    def __init__(self, normalized_name):
        self.normalized_name = normalized_name

    def name(self) -> str:
        """ Returns the name of column. Name format:

        1. if the name is quoted:

            a. if it starts with _A-Z and is followed by _A-Z0-9$, remove quotes.
            b. if it starts with $ and is followed by digits, remove quotes.
            c. otherwise, do nothing.

        2. if not quoted:

            a. if it starts with _a-zA-Z and is followed by _a-zA-Z0-9$, upper case all letters.
            b. if it starts with $ and is followed by digits, do nothing.
            c. otherwise, quote name.

        More details can be found from
        https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html
        """
        return ColumnIdentifier.__strip_unnecessary_quotes(self.normalized_name)

    @property
    def quoted_name(self) -> str:
        """ Returns the quoted name of this column Name Format:

        1. if quoted, do nothing
        2. if not quoted:

            a. if it starts with _a-zA-Z and followed by _a-zA-Z0-9$, upper case all letters and then quote.
            b. otherwise, quote name.

        It is the same as :func:`name`, but quotes are always added. It is always safe
        to do string comparisons between quoted column names
        """
        return self.normalized_name

    def __eq__(self, other):
        if type(other) == str:
            return self.normalized_name == other
        elif type(other) == ColumnIdentifier:
            return self.normalized_name == other.normalized_name
        else:
            return False

    @staticmethod
    def __strip_unnecessary_quotes(string: str) -> str:
        """Removes the unnecessary quotes from name.

        Remove quotes if name starts with _A-Z and only contains _0-9A-Z$, or starts
        with $ and is followed by digits.
        """
        remove_quote = re.compile('^"(([_A-Z]+[_A-Z0-9$]*)|(\\$\\d+))"$')
        result = remove_quote.search(string)
        return string[1:-1] if result else string


# TODO complete
class StructField:
    """Represents the content of :class:`StructField`."""

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
        """Returns the column name."""
        return self.column_identifier.name()

    def __repr__(self):
        return f"StructField({self.name}, {self.datatype.type_name}, Nullable={self.nullable})"

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__


class StructType(DataType):
    """Represents a table schema. Contains :class:`StructField` for each column."""
    def __init__(self, fields: List["StructField"]):
        self.fields = fields

    @classmethod
    def _from_attributes(cls, attributes: list):
        return cls([StructField(a.name, a.datatype, a.nullable) for a in attributes])

    def _to_attributes(self):
        from snowflake.snowpark._internal.analyzer.sf_attribute import Attribute

        return [
            Attribute(f.column_identifier.quoted_name, f.datatype, f.nullable)
            for f in self.fields
        ]

    def __repr__(self):
        return f"StructType[{', '.join(str(f) for f in self.fields)}]"

    @property
    def type_name(self) -> str:
        """Returns a data type name."""
        return self.__class__.__name__[:-4]

    @property
    def names(self):
        """Returns the list of names of the 'class':`StructField`"""
        return [f.name for f in self.fields]


class GeographyType(_AtomicType):
    """Geography data type. This maps to GEOGRAPHY data type in Snowflake."""

    def __repr__(self):
        """Returns GeographyType Info. Decimal(precision, scale)"""
        return "GeographyType"

    @property
    def type_name(self):
        """Returns GeographyType Info. GeographyType."""
        return self.__repr__()
