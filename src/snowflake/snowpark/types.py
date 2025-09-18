#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""This package contains all Snowpark logical types."""
import datetime
import json
import re
import sys
from enum import Enum
from typing import Generic, List, Optional, Type, TypeVar, Union, Dict, Any

import snowflake.snowpark.context as context
import snowflake.snowpark._internal.analyzer.expression as expression
import snowflake.snowpark._internal.proto.generated.ast_pb2 as proto

# Use correct version from here:
from snowflake.snowpark._internal.utils import (
    installed_pandas,
    pandas,
    quote_name,
)

# TODO: connector installed_pandas is broken. If pyarrow is not installed, but pandas is this function returns the wrong answer.
# The core issue is that in the connector detection of both pandas/arrow are mixed, which is wrong.
# from snowflake.connector.options import installed_pandas, pandas


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

    @classmethod
    def type_name(cls) -> str:
        return cls.__name__[:-4].lower()

    def simple_string(self) -> str:
        return self.type_name()

    def json_value(self) -> Union[str, Dict[str, Any]]:
        return self.type_name()

    def json(self) -> str:
        return json.dumps(self.json_value(), separators=(",", ":"), sort_keys=True)

    typeName = type_name
    simpleString = simple_string
    jsonValue = json_value

    def _fill_ast(self, ast: proto.DataType) -> None:
        """Populates the provided DataType instance's fields with the values corresponding to this DataType's instance

        Args:
            ast (proto.DataType): A provided (previously created) instance of an DataType IR entity

        Raises:
            ValueError: If corresponding DataType IR entity is not available, raise an error
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} has not implemented this method to fill the DataType IR entity correctly"
        )


# Data types
class NullType(DataType):
    """Represents a null type."""

    def _fill_ast(self, ast: proto.DataType) -> None:
        ast.null_type = True


class _AtomicType(DataType):
    pass


# Atomic types
class BinaryType(_AtomicType):
    """Binary data type. This maps to the BINARY data type in Snowflake."""

    def _fill_ast(self, ast: proto.DataType) -> None:
        ast.binary_type = True


class BooleanType(_AtomicType):
    """Boolean data type. This maps to the BOOLEAN data type in Snowflake."""

    def _fill_ast(self, ast: proto.DataType) -> None:
        ast.boolean_type = True


class DateType(_AtomicType):
    """Date data type. This maps to the DATE data type in Snowflake."""

    def _fill_ast(self, ast: proto.DataType) -> None:
        ast.date_type = True


class StringType(_AtomicType):
    """String data type. This maps to the VARCHAR data type in Snowflake.

    A ``StringType`` object can be created in the following ways::

        >>> string_t = StringType(23)  # this can be used to create a string type column which holds at most 23 chars
        >>> string_t = StringType()    # this can be used to create a string type column with maximum allowed length
    """

    def __init__(self, length: Optional[int] = None, is_max_size: bool = False) -> None:
        self.length = length
        self._is_max_size = length is None or is_max_size

    def __repr__(self) -> str:
        if self.length and not self._is_max_size:
            return f"StringType({self.length})"
        return "StringType()"

    def __eq__(self, other):
        if not isinstance(other, StringType):
            return False

        if self.length == other.length:
            return True

        # This is to ensure that we treat StringType() and StringType(MAX_LENGTH)
        # the same because when a string type column is created on server side without
        # a length parameter, it is set the MAX_LENGTH by default.
        if (
            self.length is None
            and other._is_max_size
            or other.length is None
            and self._is_max_size
        ):
            return True

        return False

    def __hash__(self):
        if self._is_max_size and self.length is not None:
            return StringType().__hash__()
        return super().__hash__()

    def _fill_ast(self, ast: proto.DataType) -> None:
        ast.string_type.length.SetInParent()
        if self.length is not None:
            ast.string_type.length.value = self.length


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
        self.tzinfo = self.tz if self.tz != TimestampTimeZone.DEFAULT else ""

    def __repr__(self) -> str:
        return (
            f"TimestampType(timezone=TimestampTimeZone('{self.tz}'))"
            if self.tzinfo != ""
            else "TimestampType()"
        )

    def simple_string(self) -> str:
        return (
            f"{self.type_name()}_{self.tzinfo}"
            if self.tzinfo != ""
            else self.type_name()
        )

    def json_value(self) -> str:
        return self.simple_string()

    simpleString = simple_string
    jsonValue = json_value

    def _fill_ast(self, ast: proto.DataType) -> None:
        if self.tz.value == "default":
            ast.timestamp_type.time_zone.timestamp_time_zone_default = True
        elif self.tz.value == "ntz":
            ast.timestamp_type.time_zone.timestamp_time_zone_ntz = True
        elif self.tz.value == "ltz":
            ast.timestamp_type.time_zone.timestamp_time_zone_ltz = True
        elif self.tz.value == "tz":
            ast.timestamp_type.time_zone.timestamp_time_zone_tz = True


class TimeType(_AtomicType):
    """Time data type. This maps to the TIME data type in Snowflake."""

    def _fill_ast(self, ast: proto.DataType) -> None:
        ast.time_type = True


class _AnsiIntervalType(_AtomicType):
    """The interval type which conforms to the ANSI SQL standard."""

    pass


class YearMonthIntervalType(_AnsiIntervalType):
    """YearMonthIntervalType data type. This maps to the INTERVAL YEAR TO MONTH data type in Snowflake.

    Args:
        start_field: The start field of the interval (0=YEAR, 1=MONTH)
        end_field: The end field of the interval (0=YEAR, 1=MONTH)

    Notes:
        YearMonthIntervalType is currently in private preview since 1.38.0. It needs to be enabled by setting parameter `FEATURE_INTERVAL_TYPES` to `ENABLED`.

        YearMonthIntervalType is currently not supported in UDFs and Stored Procedures.
    """

    YEAR = 0  #: Constant representing the YEAR field for interval start/end positions
    MONTH = 1  #: Constant representing the MONTH field for interval start/end positions

    _FIELD_NAMES = {YEAR: "year", MONTH: "month"}

    _FIELD_VALUES_TO_NAMES = {value: key for key, value in _FIELD_NAMES.items()}

    def __init__(
        self, start_field: Optional[int] = None, end_field: Optional[int] = None
    ) -> None:
        if start_field is None and end_field is None:
            start_field = YearMonthIntervalType.YEAR
            end_field = YearMonthIntervalType.MONTH
        if start_field is not None and end_field is None:
            end_field = start_field

        fields = self._FIELD_NAMES.keys()
        if (
            start_field not in fields
            or end_field not in fields
            or start_field > end_field
        ):
            raise ValueError(f"interval {start_field} to {end_field} is invalid")

        self.start_field = start_field
        self.end_field = end_field

    def __repr__(self) -> str:
        return f"YearMonthIntervalType({self.start_field}, {self.end_field})"

    def simple_string(self) -> str:
        fields = self._FIELD_NAMES
        if self.start_field != self.end_field:
            return f"interval {fields[self.start_field]} to {fields[self.end_field]}"
        else:
            return f"interval {fields[self.start_field]}"

    def json_value(self) -> str:
        return self.simple_string()

    simpleString = simple_string
    jsonValue = json_value

    def _fill_ast(self, ast: proto.DataType) -> None:
        ast.year_month_interval_type.start_field = self.start_field
        ast.year_month_interval_type.end_field = self.end_field


class DayTimeIntervalType(_AnsiIntervalType):
    """DayTimeIntervalType data type. This maps to the INTERVAL DAY TO SECOND data type in Snowflake.

    Args:
        start_field: The start field of the interval (0=DAY, 1=HOUR, 2=MINUTE, 3=SECOND)
        end_field: The end field of the interval (0=DAY, 1=HOUR, 2=MINUTE, 3=SECOND)

    Notes:
        DayTimeIntervalType is currently in private preview since 1.38.0. It needs to be enabled by setting parameters `FEATURE_INTERVAL_TYPES` to `ENABLED`.

        DayTimeIntervalType is currently not supported in UDFs and Stored Procedures.
    """

    DAY = 0  #: Constant representing the DAY field for interval start/end positions
    HOUR = 1  #: Constant representing the HOUR field for interval start/end positions
    MINUTE = (
        2  #: Constant representing the MINUTE field for interval start/end positions
    )
    SECOND = (
        3  #: Constant representing the SECOND field for interval start/end positions
    )

    _FIELD_NAMES = {DAY: "day", HOUR: "hour", MINUTE: "minute", SECOND: "second"}

    _FIELD_VALUES_TO_NAMES = {value: key for key, value in _FIELD_NAMES.items()}

    def __init__(
        self, start_field: Optional[int] = None, end_field: Optional[int] = None
    ) -> None:
        if start_field is None and end_field is None:
            start_field = DayTimeIntervalType.DAY
            end_field = DayTimeIntervalType.SECOND
        if start_field is not None and end_field is None:
            end_field = start_field

        fields = self._FIELD_NAMES.keys()
        if (
            start_field not in fields
            or end_field not in fields
            or start_field > end_field
        ):
            raise ValueError(f"interval {start_field} to {end_field} is invalid")

        self.start_field = start_field
        self.end_field = end_field

    def __repr__(self) -> str:
        return f"DayTimeIntervalType({self.start_field}, {self.end_field})"

    def simple_string(self) -> str:
        fields = self._FIELD_NAMES
        if self.start_field != self.end_field:
            return f"interval {fields[self.start_field]} to {fields[self.end_field]}"
        else:
            return f"interval {fields[self.start_field]}"

    def json_value(self) -> str:
        return self.simple_string()

    simpleString = simple_string
    jsonValue = json_value

    def _fill_ast(self, ast: proto.DataType) -> None:
        ast.day_time_interval_type.start_field = self.start_field
        ast.day_time_interval_type.end_field = self.end_field


# Numeric types
class _IntegralType(_NumericType):
    pass


class _FractionalType(_NumericType):
    pass


class ByteType(_IntegralType):
    """Byte data type. This maps to the TINYINT data type in Snowflake."""

    def simple_string(self) -> str:
        return "tinyint"

    simpleString = simple_string

    def _fill_ast(self, ast: proto.DataType) -> None:
        ast.byte_type = True


class ShortType(_IntegralType):
    """Short integer data type. This maps to the SMALLINT data type in Snowflake."""

    def simple_string(self) -> str:
        return "smallint"

    simpleString = simple_string

    def _fill_ast(self, ast: proto.DataType) -> None:
        ast.short_type = True


class IntegerType(_IntegralType):
    """Integer data type. This maps to the INT data type in Snowflake."""

    def simple_string(self) -> str:
        return "int"

    simpleString = simple_string

    def _fill_ast(self, ast: proto.DataType) -> None:
        ast.integer_type = True


class LongType(_IntegralType):
    """Long integer data type. This maps to the BIGINT data type in Snowflake."""

    def simple_string(self) -> str:
        return "bigint"

    simpleString = simple_string

    def _fill_ast(self, ast: proto.DataType) -> None:
        ast.long_type = True


class FloatType(_FractionalType):
    """Float data type. This maps to the FLOAT data type in Snowflake."""

    def _fill_ast(self, ast: proto.DataType) -> None:
        ast.float_type = True


class DoubleType(_FractionalType):
    """Double data type. This maps to the DOUBLE data type in Snowflake."""

    def _fill_ast(self, ast: proto.DataType) -> None:
        ast.double_type = True


class DecimalType(_FractionalType):
    """Decimal data type. This maps to the NUMBER data type in Snowflake."""

    _MAX_PRECISION = 38
    _MAX_SCALE = 38

    def __init__(self, precision: int = 38, scale: int = 0) -> None:
        self.precision = precision
        self.scale = scale

    def __repr__(self) -> str:
        return f"DecimalType({self.precision}, {self.scale})"

    def simple_string(self) -> str:
        return f"decimal({self.precision},{self.scale})"

    def json_value(self) -> str:
        return f"decimal({self.precision},{self.scale})"

    simpleString = simple_string
    jsonValue = json_value

    def _fill_ast(self, ast: proto.DataType) -> None:
        ast.decimal_type.precision = self.precision
        ast.decimal_type.scale = self.scale


class ArrayType(DataType):
    """Array data type. This maps to the ARRAY data type in Snowflake."""

    def __init__(
        self,
        element_type: Optional[DataType] = None,
        structured: Optional[bool] = None,
        contains_null: bool = True,
    ) -> None:
        if context._should_use_structured_type_semantics():
            self.structured = (
                structured if structured is not None else element_type is not None
            )
            self.element_type = element_type
        else:
            self.structured = structured or False
            self.element_type = element_type if element_type else StringType()
        self.contains_null = contains_null

    def __repr__(self) -> str:
        return f"ArrayType({repr(self.element_type) if self.element_type else ''})"

    def _as_nested(self) -> "ArrayType":
        if not context._should_use_structured_type_semantics():
            return self
        element_type = self.element_type
        if isinstance(element_type, (ArrayType, MapType, StructType)):
            element_type = element_type._as_nested()
        return ArrayType(element_type, self.structured, self.contains_null)

    def is_primitive(self):
        return False

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "ArrayType":
        return ArrayType(
            _parse_datatype_json_value(
                json_dict["elementType"]
                if "elementType" in json_dict
                else json_dict["element_type"]
            ),
            contains_null=json_dict.get("contains_null", True),
        )

    def simple_string(self) -> str:
        return f"array<{self.element_type.simple_string()}>"

    def json_value(self) -> Dict[str, Any]:
        return {
            "type": self.type_name(),
            "element_type": self.element_type.json_value(),
            "contains_null": self.contains_null,
        }

    simpleString = simple_string
    jsonValue = json_value
    fromJson = from_json

    def _fill_ast(self, ast: proto.DataType) -> None:
        ast.array_type.structured = self.structured
        if self.element_type is not None:
            self.element_type._fill_ast(ast.array_type.ty)


class MapType(DataType):
    """Map data type. This maps to the OBJECT data type in Snowflake if key and value types are not defined otherwise MAP."""

    def __init__(
        self,
        key_type: Optional[DataType] = None,
        value_type: Optional[DataType] = None,
        structured: Optional[bool] = None,
        value_contains_null: bool = True,
    ) -> None:
        if context._should_use_structured_type_semantics():
            if key_type is None or value_type is None:
                raise ValueError("MapType requires key and value type be set.")
            self.structured = True
            self.key_type = key_type
            self.value_type = value_type
        else:
            self.structured = structured or False
            self.key_type = key_type if key_type else StringType()
            self.value_type = value_type if value_type else StringType()
        self.value_contains_null = value_contains_null

    def __repr__(self) -> str:
        type_str = ""
        if self.key_type and self.value_type:
            type_str = f"{repr(self.key_type)}, {repr(self.value_type)}"
        return f"MapType({type_str})"

    def is_primitive(self):
        return False

    def _as_nested(self) -> "MapType":
        if not context._should_use_structured_type_semantics():
            return self
        value_type = self.value_type
        if isinstance(value_type, (ArrayType, MapType, StructType)):
            value_type = value_type._as_nested()
        return MapType(self.key_type, value_type, self.structured)

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "MapType":
        return MapType(
            _parse_datatype_json_value(
                json_dict["keyType"]
                if "keyType" in json_dict
                else json_dict["key_type"]
            ),
            _parse_datatype_json_value(
                json_dict["valueType"]
                if "valueType" in json_dict
                else json_dict["value_type"]
            ),
        )

    def simple_string(self) -> str:
        return f"map<{self.key_type.simple_string()},{self.value_type.simple_string()}>"

    def json_value(self) -> Dict[str, Any]:
        return {
            "type": self.type_name(),
            "key_type": self.key_type.json_value(),
            "value_type": self.value_type.json_value(),
        }

    @property
    def keyType(self):
        return self.key_type

    @property
    def valueType(self):
        return self.value_type

    simpleString = simple_string
    jsonValue = json_value
    fromJson = from_json

    def _fill_ast(self, ast: proto.DataType) -> None:
        ast.map_type.structured = self.structured
        if self.key_type is not None and self.value_type is not None:
            self.key_type._fill_ast(ast.map_type.key_ty)
            self.value_type._fill_ast(ast.map_type.value_ty)


class VectorType(DataType):
    """Vector data type. This maps to the VECTOR data type in Snowflake."""

    def __init__(
        self,
        element_type: Union[Type[int], Type[float], "int", "float"],
        dimension: int,
    ) -> None:
        if isinstance(element_type, str) and element_type in ("int", "float"):
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

    def simple_string(self) -> str:
        return f"vector({self.element_type},{self.dimension})"

    def json_value(self) -> str:
        return f"vector({self.element_type},{self.dimension})"

    simpleString = simple_string
    jsonValue = json_value

    def _fill_ast(self, ast: proto.DataType) -> None:
        if self.element_type == "int":
            ast.vector_type.ty.integer_type = True
        elif self.element_type == "float":
            ast.vector_type.ty.float_type = True

        ast.vector_type.dimension = self.dimension


class FileType(DataType):
    """File data type. This maps to the FILE data type in Snowflake."""

    def _fill_ast(self, ast: proto.DataType) -> None:
        ast.file_type = True


class ColumnIdentifier:
    """Represents a column identifier."""

    def __init__(self, normalized_name: str) -> None:
        self.normalized_name = quote_name(normalized_name)
        self.case_sensitive_name = quote_name(normalized_name, keep_case=True)
        self._original_name = normalized_name

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

    def _fill_ast(self, ast: proto.ColumnIdentifier) -> None:
        ast.name = self._original_name


class StructField:
    """Represents the content of :class:`StructField`."""

    def __init__(
        self,
        column_identifier: Union[ColumnIdentifier, str],
        datatype: DataType,
        nullable: bool = True,
        _is_column: bool = True,
    ) -> None:
        self.original_column_identifier = column_identifier
        self.name = column_identifier
        self._is_column = _is_column
        self.datatype = datatype
        self.nullable = nullable

    @property
    def name(self) -> str:
        if self._is_column or not context._should_use_structured_type_semantics():
            return self.column_identifier.name
        else:
            return self._name

    @name.setter
    def name(self, n: Union[ColumnIdentifier, str]) -> None:
        if isinstance(n, ColumnIdentifier):
            self._name = n.name
            self.column_identifier = n
        else:
            self._name = n
            self.column_identifier = ColumnIdentifier(n)

    @property
    def case_sensitive_name(self):
        return self.column_identifier.case_sensitive_name

    def _as_nested(self) -> "StructField":
        if not context._should_use_structured_type_semantics():
            return self
        datatype = self.datatype
        if isinstance(datatype, (ArrayType, MapType, StructType)):
            datatype = datatype._as_nested()
        # Nested StructFields do not follow column naming conventions
        return StructField(self._name, datatype, self.nullable, _is_column=False)

    def __repr__(self) -> str:
        return f"StructField({self.name!r}, {repr(self.datatype)}, nullable={self.nullable})"

    def __eq__(self, other):
        return isinstance(other, self.__class__) and (
            (self.name, self._is_column, self.datatype, self.nullable)
            == (other.name, other._is_column, other.datatype, other.nullable)
        )

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "StructField":
        return StructField(
            json_dict["name"],
            _parse_datatype_json_value(json_dict["type"]),
            json_dict["nullable"],
        )

    def simple_string(self) -> str:
        return f"{self.name}:{self.datatype.simple_string()}"

    def json_value(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": self.datatype.json_value(),
            "nullable": self.nullable,
        }

    def json(self) -> str:
        return json.dumps(self.json_value(), separators=(",", ":"), sort_keys=True)

    def type_name(self) -> str:
        raise TypeError(
            "StructField does not have typeName. Use typeName on its type explicitly instead"
        )

    typeName = type_name
    simpleString = simple_string
    jsonValue = json_value
    fromJson = from_json

    def _fill_ast(self, ast: proto.StructField) -> None:
        if isinstance(self.original_column_identifier, ColumnIdentifier):
            self.original_column_identifier._fill_ast(
                ast.column_identifier.column_identifier
            )
        else:
            ast.column_identifier.column_name.name = self.original_column_identifier
        self.datatype._fill_ast(ast.data_type)
        ast.nullable = self.nullable


class StructType(DataType):
    """Represents a table schema or structured column. Contains :class:`StructField` for each field."""

    def __init__(
        self,
        fields: Optional[List["StructField"]] = None,
        structured: Optional[bool] = None,
    ) -> None:
        if context._should_use_structured_type_semantics():
            self.structured = (
                structured if structured is not None else fields is not None
            )
        else:
            self.structured = structured or False

        self.fields = []
        for field in fields or []:
            self.add(field)

    def add(
        self,
        field: Union[str, ColumnIdentifier, "StructField"],
        datatype: Optional[DataType] = None,
        nullable: Optional[bool] = True,
    ) -> "StructType":
        if isinstance(field, (str, ColumnIdentifier)):
            if datatype is None:
                raise ValueError(
                    "When field argument is str or ColumnIdentifier, datatype must not be None."
                )
            field = StructField(field, datatype, nullable)
        elif not isinstance(field, StructField):
            raise ValueError(
                f"field argument must be one of str, ColumnIdentifier or StructField. Got: '{type(field)}'"
            )

        # Nested data does not follow the same schema conventions as top level fields.
        if isinstance(field.datatype, (ArrayType, MapType, StructType)):
            field.datatype = field.datatype._as_nested()

        self.fields.append(field)
        return self

    def _as_nested(self) -> "StructType":
        if not context._should_use_structured_type_semantics():
            return self
        return StructType(
            [field._as_nested() for field in self.fields], self.structured
        )

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

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "StructType":
        return StructType([StructField.fromJson(f) for f in json_dict["fields"]])

    def simple_string(self) -> str:
        return f"struct<{','.join(f.simple_string() for f in self)}>"

    def json_value(self) -> Dict[str, Any]:
        return {"type": self.type_name(), "fields": [f.json_value() for f in self]}

    simpleString = simple_string
    jsonValue = json_value
    fieldNames = names
    fromJson = from_json

    def _fill_ast(self, ast: proto.DataType) -> None:
        ast.struct_type.structured = self.structured
        for field in self.fields or []:
            field._fill_ast(ast.struct_type.fields.add())


class VariantType(DataType):
    """Variant data type. This maps to the VARIANT data type in Snowflake."""

    def is_primitive(self):
        return False

    def _fill_ast(self, ast: proto.DataType) -> None:
        ast.variant_type = True


class GeographyType(DataType):
    """Geography data type. This maps to the GEOGRAPHY data type in Snowflake."""

    def _fill_ast(self, ast: proto.DataType) -> None:
        ast.geography_type = True


class GeometryType(DataType):
    """Geometry data type. This maps to the GEOMETRY data type in Snowflake."""

    def _fill_ast(self, ast: proto.DataType) -> None:
        ast.geometry_type = True


class _PandasType(DataType):
    pass


class PandasSeriesType(_PandasType):
    """pandas Series data type."""

    def __init__(self, element_type: Optional[DataType]) -> None:
        self.element_type = element_type

    def __repr__(self) -> str:
        return (
            f"PandasSeriesType({repr(self.element_type) if self.element_type else ''})"
        )

    @classmethod
    def type_name(cls) -> str:
        return "pandas_series"

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "PandasSeriesType":
        return PandasSeriesType(
            _parse_datatype_json_value(json_dict["element_type"])
            if json_dict["element_type"]
            else None
        )

    def simple_string(self) -> str:
        return f"pandas_series<{self.element_type.simple_string() if self.element_type else ''}>"

    def json_value(self) -> Dict[str, Any]:
        return {
            "type": self.type_name(),
            "element_type": self.element_type.json_value()
            if self.element_type
            else None,
        }

    simpleString = simple_string
    jsonValue = json_value
    fromJson = from_json
    typeName = type_name

    def _fill_ast(self, ast: proto.DataType) -> None:
        if self.element_type is not None:
            self.element_type._fill_ast(ast.pandas_series_type.el_ty)
        else:
            ast.pandas_series_type = True


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

    def __repr__(self) -> str:
        col_names = f", [{', '.join(self.col_names)}]" if self.col_names != [] else ""
        return f"PandasDataFrameType([{', '.join([repr(col) for col in self.col_types])}]{col_names})"

    def get_snowflake_col_datatypes(self):
        """Get the column types of the dataframe as the input/output of a vectorized UDTF."""
        return [
            tp.element_type if isinstance(tp, PandasSeriesType) else tp
            for tp in self.col_types
        ]

    @classmethod
    def type_name(cls) -> str:
        return "pandas_dataframe"

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "PandasDataFrameType":
        temp_col_names = []
        temp_col_types = []
        for cols in json_dict["fields"]:
            if cols["name"] != "":
                temp_col_names.append(cols["name"])
            temp_col_types.append(_parse_datatype_json_value(cols["type"]))
        return PandasDataFrameType(temp_col_types, temp_col_names)

    def simple_string(self) -> str:
        return f"pandas<{','.join(f.simple_string() for f in self.col_types)}>"

    def json_value(self) -> Dict[str, Any]:
        temp_col_name = (
            self.col_names
            if self.col_names != []
            else ["" for _ in range(len(list(self.col_types)))]
        )

        return {
            "type": self.type_name(),
            "fields": [
                self._json_value_helper(n, t)
                for (n, t) in zip(temp_col_name, self.col_types)
            ],
        }

    def _json_value_helper(self, col_name, col_type) -> Dict[str, Any]:
        return {"name": col_name, "type": col_type.json_value()}

    simpleString = simple_string
    jsonValue = json_value
    fromJson = from_json
    typeName = type_name

    def _fill_ast(self, ast: proto.DataType) -> None:
        for col_type in self.col_types:
            ast_col = ast.pandas_data_frame_type.col_types.add()
            col_type._fill_ast(ast_col)
        ast.pandas_data_frame_type.col_names.extend(self.col_names)


_atomic_types: List[Type[DataType]] = [
    StringType,
    BinaryType,
    BooleanType,
    DecimalType,
    FloatType,
    DoubleType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    DateType,
    NullType,
]

_timestamp_types: List[DataType] = [
    TimestampType(),
    TimestampType(timezone=TimestampTimeZone.NTZ),
    TimestampType(timezone=TimestampTimeZone.LTZ),
    TimestampType(timezone=TimestampTimeZone.TZ),
]

_all_atomic_types: Dict[str, Type[DataType]] = {t.typeName(): t for t in _atomic_types}
_all_timestamp_types: Dict[str, DataType] = {
    t.json_value(): t for t in _timestamp_types
}

_complex_types: List[Type[Union[ArrayType, MapType, StructType]]] = [
    ArrayType,
    MapType,
    StructType,
    PandasDataFrameType,
]
_all_complex_types: Dict[str, Type[Union[ArrayType, MapType, StructType]]] = {
    v.typeName(): v for v in _complex_types
}

_FIXED_VECTOR_PATTERN = re.compile(r"vector\(\s*(int|float)\s*,\s*(\d+)\s*\)")
_FIXED_DECIMAL_PATTERN = re.compile(r"decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)")
_INTERVAL_YEARMONTH_PATTERN = re.compile(r"interval (year|month)( to (year|month))?")
_INTERVAL_DAYTIME_PATTERN = re.compile(
    r"interval (day|hour|minute|second)( to (day|hour|minute|second))?"
)


def _parse_datatype_json_value(json_value: Union[dict, str]) -> DataType:
    if not isinstance(json_value, dict):
        if json_value in _all_atomic_types:
            return _all_atomic_types[json_value]()
        if json_value in _all_timestamp_types:
            return TimestampType(timezone=_all_timestamp_types[json_value].tz)
        elif json_value == "decimal":
            return DecimalType()
        elif json_value == "variant":
            return VariantType()
        elif _FIXED_DECIMAL_PATTERN.match(json_value):
            m = _FIXED_DECIMAL_PATTERN.match(json_value)
            return DecimalType(int(m.group(1)), int(m.group(2)))  # type: ignore[union-attr]
        elif _FIXED_VECTOR_PATTERN.match(json_value):
            m = _FIXED_VECTOR_PATTERN.match(json_value)
            return VectorType(m.group(1), int(m.group(2)))  # type: ignore[union-attr]
        elif _INTERVAL_YEARMONTH_PATTERN.match(json_value):
            m = _INTERVAL_YEARMONTH_PATTERN.match(json_value)
            field_values_to_names = YearMonthIntervalType._FIELD_VALUES_TO_NAMES
            first_field = field_values_to_names.get(m.group(1))  # type: ignore[union-attr]
            second_field = field_values_to_names.get(m.group(3))  # type: ignore[union-attr]
            if first_field is not None and second_field is None:
                return YearMonthIntervalType(first_field)
            return YearMonthIntervalType(first_field, second_field)
        elif _INTERVAL_DAYTIME_PATTERN.match(json_value):
            m = _INTERVAL_DAYTIME_PATTERN.match(json_value)
            field_values_to_names = DayTimeIntervalType._FIELD_VALUES_TO_NAMES
            first_field = field_values_to_names.get(m.group(1))  # type: ignore[union-attr]
            second_field = field_values_to_names.get(m.group(3))  # type: ignore[union-attr]
            if first_field is not None and second_field is None:
                return DayTimeIntervalType(first_field)
            return DayTimeIntervalType(first_field, second_field)
        else:
            raise ValueError(f"Cannot parse data type: {str(json_value)}")
    else:
        tpe = json_value["type"]
        if tpe in _all_complex_types:
            return _all_complex_types[tpe].fromJson(json_value)
        else:
            raise ValueError(f"Unsupported data type: {str(tpe)}")


#: The type hint for annotating Variant data when registering UDFs.
Variant = TypeVar("Variant")

#: The type hint for annotating Geography data when registering UDFs.
Geography = TypeVar("Geography")

#: The type hint for annotating Geometry data when registering UDFs.
Geometry = TypeVar("Geometry")

#: The type hint for annotating Geometry data when registering UDFs.
File = TypeVar("File")

#: The type hint for annotating YearMonthInterval data when registering UDFs.
YearMonthInterval = TypeVar("YearMonthInterval")

#: The type hint for annotating DayTimeInterval data when registering UDFs.
DayTimeInterval = TypeVar("DayTimeInterval")

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
