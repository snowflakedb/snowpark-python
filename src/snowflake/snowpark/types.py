#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
"""This package contains all Snowpark logical types."""
import re
from typing import Generic, Iterable, List, Optional, TypeVar, Union

import snowflake.snowpark._internal.analyzer.expression as expression
from snowflake.connector.options import installed_pandas, pandas


class DataType:
    """The base class of Snowpark data types."""

    def __hash__(self):
        return hash(repr(self))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return f"{self.__class__.__name__}()"


# Data types
class NullType(DataType):
    """Represents a null type."""

    pass


class _AtomicType(DataType):
    pass


# Atomic types
class BinaryType(_AtomicType):
    """Binary data type. This maps to the BINARY data type in Snowflake."""

    pass


class BooleanType(_AtomicType):
    """Boolean data type. This maps to the BOOLEAN data type in Snowflake."""

    pass


class DateType(_AtomicType):
    """Date data type. This maps to the DATE data type in Snowflake."""

    pass


class StringType(_AtomicType):
    """String data type. This maps to the VARCHAR data type in Snowflake."""

    pass


class _NumericType(_AtomicType):
    pass


class TimestampType(_AtomicType):
    """Timestamp data type. This maps to the TIMESTAMP data type in Snowflake."""

    pass


class TimeType(_AtomicType):
    """Time data type. This maps to the TIME data type in Snowflake."""

    pass


# Numeric types
class _IntegralType(_NumericType):
    pass


class _FractionalType(_NumericType):
    pass


class ByteType(_IntegralType):
    """Byte data type. This maps to the TINYINT data type in Snowflake."""

    pass


class ShortType(_IntegralType):
    """Short integer data type. This maps to the SMALLINT data type in Snowflake."""

    pass


class IntegerType(_IntegralType):
    """Integer data type. This maps to the INT data type in Snowflake."""

    pass


class LongType(_IntegralType):
    """Long integer data type. This maps to the BIGINT data type in Snowflake."""

    pass


class FloatType(_FractionalType):
    """Float data type. This maps to the FLOAT data type in Snowflake."""

    pass


class DoubleType(_FractionalType):
    """Double data type. This maps to the DOUBLE data type in Snowflake."""

    pass


class DecimalType(_FractionalType):
    """Decimal data type. This maps to the NUMBER data type in Snowflake."""

    _MAX_PRECISION = 38
    _MAX_SCALE = 38

    def __init__(self, precision: int = 38, scale: int = 0) -> None:
        self.precision = precision
        self.scale = scale

    def __repr__(self) -> str:
        return f"DecimalType({self.precision}, {self.scale})"


class ArrayType(DataType):
    """Array data type. This maps to the ARRAY data type in Snowflake."""

    def __init__(self, element_type: Optional[DataType] = None) -> None:
        self.element_type = element_type if element_type else StringType()

    def __repr__(self) -> str:
        return f"ArrayType({repr(self.element_type) if self.element_type else ''})"


class MapType(DataType):
    """Map data type. This maps to the OBJECT data type in Snowflake."""

    def __init__(
        self, key_type: Optional[DataType] = None, value_type: Optional[DataType] = None
    ) -> None:
        self.key_type = key_type if key_type else StringType()
        self.value_type = value_type if value_type else StringType()

    def __repr__(self) -> str:
        return f"MapType({repr(self.key_type) if self.key_type else ''}, {repr(self.value_type) if self.value_type else ''})"


class ColumnIdentifier:
    """Represents a column identifier."""

    def __init__(self, normalized_name: str) -> None:
        self.normalized_name = normalized_name

    @property
    def name(self) -> str:
        """Returns the name of this column, with the following format:

        1. If the name is quoted:

            a. if it starts with ``_A-Z`` and is followed by ``_A-Z0-9$``, remove quotes.
            b. if it starts with ``$`` and is followed by digits, remove quotes.
            c. otherwise, do nothing.

        2. If not quoted:

            a. if it starts with ``_a-zA-Z`` and is followed by ``_a-zA-Z0-9$``, uppercase all letters.
            b. if it starts with ``$`` and is followed by digits, do nothing.
            c. otherwise, add quotes.

        For more information, see
        https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html
        """
        return ColumnIdentifier._strip_unnecessary_quotes(self.normalized_name)

    @property
    def quoted_name(self) -> str:
        """Returns the quoted name of this column, with the following format:

        1. If quoted, do nothing.
        2. If not quoted:

            a. if it starts with ``_a-zA-Z`` and followed by ``_a-zA-Z0-9$``, uppercase all letters and then add quotes.
            b. otherwise, add quotes.

        It is the same as :func:`name`, but quotes are always added. It is always safe
        to do string comparisons between quoted column names.
        """
        return self.normalized_name

    def __eq__(self, other):
        if isinstance(other, str):
            return self.normalized_name == other
        elif isinstance(other, ColumnIdentifier):
            return self.normalized_name == other.normalized_name
        else:
            return False

    @staticmethod
    def _strip_unnecessary_quotes(string: str) -> str:
        """Removes the unnecessary quotes from name.

        Remove quotes if name starts with _A-Z and only contains _0-9A-Z$, or starts
        with $ and is followed by digits.
        """
        remove_quote = re.compile('^"(([_A-Z]+[_A-Z0-9$]*)|(\\$\\d+))"$')
        result = remove_quote.search(string)
        return string[1:-1] if result else string


class StructField:
    """Represents the content of :class:`StructField`."""

    def __init__(
        self,
        column_identifier: Union[ColumnIdentifier, str],
        datatype: DataType,
        nullable: bool = True,
    ) -> None:
        self.column_identifier = (
            ColumnIdentifier(column_identifier)
            if isinstance(column_identifier, str)
            else column_identifier
        )
        self.datatype = datatype
        self.nullable = nullable

    @property
    def name(self) -> str:
        """Returns the column name."""
        return self.column_identifier.name

    @name.setter
    def name(self, n: str) -> None:
        self.column_identifier = ColumnIdentifier(n)

    def __repr__(self) -> str:
        return f"StructField({self.name!r}, {repr(self.datatype)}, nullable={self.nullable})"

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__


class StructType(DataType):
    """Represents a table schema. Contains :class:`StructField` for each column."""

    def __init__(self, fields: List["StructField"]) -> None:
        self.fields = fields

    @classmethod
    def _from_attributes(cls, attributes: list) -> "StructType":
        return cls([StructField(a.name, a.datatype, a.nullable) for a in attributes])

    def _to_attributes(self) -> List["expression.Attribute"]:
        return [
            expression.Attribute(
                f.column_identifier.quoted_name, f.datatype, f.nullable
            )
            for f in self.fields
        ]

    def __repr__(self) -> str:
        return f"StructType([{', '.join(repr(f) for f in self.fields)}])"

    @property
    def names(self) -> List[str]:
        """Returns the list of names of the :class:`StructField`"""
        return [f.name for f in self.fields]


class VariantType(DataType):
    """Variant data type. This maps to the VARIANT data type in Snowflake."""

    pass


class GeographyType(DataType):
    """Geography data type. This maps to the GEOGRAPHY data type in Snowflake."""

    pass


class _PandasType(DataType):
    pass


class PandasSeriesType(_PandasType):
    """Pandas Series data type."""

    def __init__(self, element_type: Optional[DataType]) -> None:
        self.element_type = element_type


class PandasDataFrameType(_PandasType):
    """
    Pandas DataFrame data type. The input should be a list of data types for all columns in order.
    It cannot be used as the return type of a Pandas UDF.
    """

    def __init__(self, col_types: Iterable[DataType]) -> None:
        self.col_types = col_types


#: The type hint for annotating Variant data when registering UDFs.
Variant = TypeVar("Variant")

#: The type hint for annotating Geography data when registering UDFs.
Geography = TypeVar("Geography")


if installed_pandas:
    _T = TypeVar("_T")

    class PandasSeries(pandas.Series, Generic[_T]):
        """The type hint for annotating Pandas Series data when registering UDFs."""

        pass

    from typing_extensions import TypeVarTuple

    _TT = TypeVarTuple("_TT")

    class PandasDataFrame(pandas.DataFrame, Generic[_TT]):
        """
        The type hint for annotating Pandas DataFrame data when registering UDFs.
        The input should be a list of data types for all columns in order.
        It cannot be used to annotate the return value of a Pandas UDF.
        """

        pass
