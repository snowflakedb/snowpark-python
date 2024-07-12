#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""This package contains all Snowpark logical types."""
import datetime
import re
import sys
from enum import Enum
from typing import Generic, List, Optional, Type, TypeVar, Union

import snowflake.snowpark._internal.analyzer.expression as expression
from snowflake.connector.options import installed_pandas, pandas
from snowflake.snowpark._internal.utils import quote_name

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable


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

    def is_primitive(self):
        return True


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
    """String data type. This maps to the VARCHAR data type in Snowflake.

    A ``StringType`` object can be created in the following ways::

        >>> string_t = StringType(23)  # this can be used to create a string type column which holds at most 23 chars
        >>> string_t = StringType()    # this can be used to create a string type column with maximum allowed length
    """

    _MAX_LENGTH = 16777216

    def __init__(self, length: Optional[int] = None) -> None:
        self.length = length

    def __repr__(self) -> str:
        if self.length:
            return f"StringType({self.length})"
        return "StringType()"

    def __eq__(self, other):
        if not isinstance(other, StringType):
            return False

        if self.length == other.length:
            return True

        # This is to ensure that we treat StringType() and StringType(_MAX_LENGTH)
        # the same because when a string type column is created on server side without
        # a length parameter, it is set the _MAX_LENGTH by default.
        if (
            self.length is None
            and other.length == StringType._MAX_LENGTH
            or other.length is None
            and self.length == StringType._MAX_LENGTH
        ):
            return True

        return False

    def __hash__(self):
        if self.length == StringType._MAX_LENGTH:
            return StringType().__hash__()
        return super().__hash__()


class _NumericType(_AtomicType):
    pass


class TimestampTimeZone(Enum):
    """
    `Snowflake Timestamp variations <https://docs.snowflake.com/en/sql-reference/data-types-datetime#timestamp-ltz-timestamp-ntz-timestamp-tz>`_.
    """

    DEFAULT = "default"
    # TIMESTAMP_NTZ
    NTZ = "ntz"
    # TIMESTAMP_LTZ
    LTZ = "ltz"
    # TIMESTAMP_TZ
    TZ = "tz"

    def __str__(self):
        return str(self.value)


class TimestampType(_AtomicType):
    """Timestamp data type. This maps to the TIMESTAMP data type in Snowflake."""

    def __init__(self, timezone: TimestampTimeZone = TimestampTimeZone.DEFAULT) -> None:
        self.tz = timezone  #: Timestamp variations

    def __repr__(self) -> str:
        tzinfo = f"tz={self.tz}" if self.tz != TimestampTimeZone.DEFAULT else ""
        return f"TimestampType({tzinfo})"


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

    def __init__(
        self, element_type: Optional[DataType] = None, structured: bool = False
    ) -> None:
        self.structured = structured
        self.element_type = element_type if element_type else StringType()

    def __repr__(self) -> str:
        return f"ArrayType({repr(self.element_type) if self.element_type else ''})"

    def is_primitive(self):
        return False


class MapType(DataType):
    """Map data type. This maps to the OBJECT data type in Snowflake if key and value types are not defined otherwise MAP."""

    def __init__(
        self,
        key_type: Optional[DataType] = None,
        value_type: Optional[DataType] = None,
        structured: bool = False,
    ) -> None:
        self.structured = structured
        self.key_type = key_type if key_type else StringType()
        self.value_type = value_type if value_type else StringType()

    def __repr__(self) -> str:
        return f"MapType({repr(self.key_type) if self.key_type else ''}, {repr(self.value_type) if self.value_type else ''})"

    def is_primitive(self):
        return False


class VectorType(DataType):
    """Vector data type. This maps to the VECTOR data type in Snowflake."""

    def __init__(
        self,
        element_type: Union[Type[int], Type[float], "int", "float"],
        dimension: int,
    ) -> None:
        if isinstance(element_type, str):
            self.element_type = element_type
        elif element_type == int:
            self.element_type = "int"
        elif element_type == float:
            self.element_type = "float"
        else:
            raise ValueError(
                f"VectorType does not support element type: {element_type}"
            )
        self.dimension = dimension

    def __repr__(self) -> str:
        return f"VectorType({self.element_type},{self.dimension})"

    def is_primitive(self):
        return False


class ColumnIdentifier:
    """Represents a column identifier."""

    def __init__(self, normalized_name: str) -> None:
        self.normalized_name = quote_name(normalized_name)

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
    """Represents a table schema or structured column. Contains :class:`StructField` for each field."""

    def __init__(
        self, fields: Optional[List["StructField"]] = None, structured=False
    ) -> None:
        self.structured = structured
        if fields is None:
            fields = []
        self.fields = fields

    def add(
        self,
        field: Union[str, ColumnIdentifier, "StructField"],
        datatype: Optional[DataType] = None,
        nullable: Optional[bool] = True,
    ) -> "StructType":
        if isinstance(field, StructField):
            self.fields.append(field)
        elif isinstance(field, (str, ColumnIdentifier)):
            if datatype is None:
                raise ValueError(
                    "When field argument is str or ColumnIdentifier, datatype must not be None."
                )
            self.fields.append(StructField(field, datatype, nullable))
        else:
            raise ValueError(
                f"field argument must be one of str, ColumnIdentifier or StructField. Got: '{type(field)}'"
            )
        return self

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

    def __getitem__(self, item: Union[str, int, slice]) -> StructField:
        """Access fields by name, index or slice."""
        if isinstance(item, str):
            for field in self.fields:
                if field.name == item:
                    return field
            raise KeyError(f"No StructField named {item}")
        elif isinstance(item, int):
            return self.fields[item]  # may throw ValueError
        elif isinstance(item, slice):
            return StructType(self.fields[item])
        else:
            raise TypeError(
                f"StructType items should be strings, integers or slices, but got {type(item).__name__}"
            )

    def __setitem__(self, key, value):
        raise TypeError("StructType object does not support item assignment")

    @property
    def names(self) -> List[str]:
        """Returns the list of names of the :class:`StructField`"""
        return [f.name for f in self.fields]


class VariantType(DataType):
    """Variant data type. This maps to the VARIANT data type in Snowflake."""

    def is_primitive(self):
        return False


class GeographyType(DataType):
    """Geography data type. This maps to the GEOGRAPHY data type in Snowflake."""

    pass


class GeometryType(DataType):
    """Geometry data type. This maps to the GEOMETRY data type in Snowflake."""

    pass


class _PandasType(DataType):
    pass


class PandasSeriesType(_PandasType):
    """pandas Series data type."""

    def __init__(self, element_type: Optional[DataType]) -> None:
        self.element_type = element_type


class PandasDataFrameType(_PandasType):
    """
    pandas DataFrame data type. The input should be a list of data types for all columns in order.
    It cannot be used as the return type of a pandas UDF.
    """

    def __init__(
        self, col_types: Iterable[DataType], col_names: Iterable[str] = None
    ) -> None:
        self.col_types = col_types
        self.col_names = col_names or []

    def get_snowflake_col_datatypes(self):
        """Get the column types of the dataframe as the input/output of a vectorized UDTF."""
        return [
            tp.element_type if isinstance(tp, PandasSeriesType) else tp
            for tp in self.col_types
        ]


#: The type hint for annotating Variant data when registering UDFs.
Variant = TypeVar("Variant")

#: The type hint for annotating Geography data when registering UDFs.
Geography = TypeVar("Geography")

#: The type hint for annotating Geometry data when registering UDFs.
Geometry = TypeVar("Geometry")

# TODO(SNOW-969479): Add a type hint that can be used to annotate Vector data. Python does not
# currently support integer type parameters (which are needed to represent a vector's dimension).
# typing.Annotate can be used as a temporary bypass once the minimum supported Python version is
# bumped to 3.9

#: The type hint for annotating TIMESTAMP_NTZ (e.g., ``Timestamp[NTZ]``) data when registering UDFs.
NTZ = TypeVar("NTZ")

#: The type hint for annotating TIMESTAMP_LTZ (e.g., ``Timestamp[LTZ]``) data when registering UDFs.
LTZ = TypeVar("LTZ")

#: The type hint for annotating TIMESTAMP_TZ (e.g., ``Timestamp[TZ]``) data when registering UDFs.
TZ = TypeVar("TZ")


_T = TypeVar("_T")


class Timestamp(datetime.datetime, Generic[_T]):
    """The type hint for annotating ``TIMESTAMP_*`` data when registering UDFs."""

    pass


if installed_pandas:  # pragma: no cover

    class PandasSeries(pandas.Series, Generic[_T]):
        """The type hint for annotating pandas Series data when registering UDFs."""

        pass

    from typing_extensions import TypeVarTuple

    _TT = TypeVarTuple("_TT")

    if sys.version_info >= (3, 11):
        from typing import Unpack

        class PandasDataFrame(pandas.DataFrame, Generic[Unpack[_TT]]):
            """
            The type hint for annotating pandas DataFrame data when registering UDFs.
            The input should be a list of data types for all columns in order.
            It cannot be used to annotate the return value of a pandas UDF.
            """

            pass

    else:

        class PandasDataFrame(pandas.DataFrame, Generic[_TT]):
            """
            The type hint for annotating pandas DataFrame data when registering UDFs.
            The input should be a list of data types for all columns in order.
            It cannot be used to annotate the return value of a pandas UDF.
            """

            pass
