#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the Apache Software Foundation as part of the
# Apache Spark project, under the Apache License, Version 2.0.
import ast
import ctypes
import datetime
import decimal
import functools
import re
import sys
import typing  # noqa: F401
from array import array
from typing import (  # noqa: F401
    TYPE_CHECKING,
    Any,
    Dict,
    Generator,
    Iterator,
    List,
    NewType,
    Optional,
    Tuple,
    Type,
    Union,
    get_args,
    get_origin,
)

import snowflake.snowpark.context as context
import snowflake.snowpark.types  # type: ignore
from snowflake.connector.constants import FIELD_ID_TO_NAME
from snowflake.connector.cursor import ResultMetadata
from snowflake.connector.options import installed_pandas, pandas
from snowflake.snowpark._internal.utils import quote_name
from snowflake.snowpark.row import Row
from snowflake.snowpark.types import (
    LTZ,
    NTZ,
    TZ,
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    Geography,
    GeographyType,
    Geometry,
    GeometryType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    StructField,
    StructType,
    Timestamp,
    TimestampTimeZone,
    TimestampType,
    TimeType,
    Variant,
    VariantType,
    VectorType,
    _FractionalType,
    _IntegralType,
    _NumericType,
    FileType,
    File,
)

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
try:
    from typing import Iterable  # noqa: F401
except ImportError:
    from collections.abc import Iterable  # noqa: F401

if installed_pandas:
    from snowflake.snowpark.types import (
        PandasDataFrame,
        PandasDataFrameType,
        PandasSeries,
        PandasSeriesType,
    )

if TYPE_CHECKING:
    import snowflake.snowpark.column

    try:
        from snowflake.connector.cursor import ResultMetadataV2
    except ImportError:
        ResultMetadataV2 = ResultMetadata


def convert_metadata_to_sp_type(
    metadata: Union[ResultMetadata, "ResultMetadataV2"],
    max_string_size: int,
) -> DataType:
    column_type_name = FIELD_ID_TO_NAME[metadata.type_code]
    if column_type_name == "VECTOR":
        if not hasattr(metadata, "fields") or not hasattr(metadata, "vector_dimension"):
            raise NotImplementedError(
                "Vectors are not supported by your connector: Please update it to a newer version"
            )

        if metadata.fields is None:
            raise ValueError(
                "Invalid result metadata for vector type: expected sub-field metadata"
            )
        if len(metadata.fields) != 1:
            raise ValueError(
                "Invalid result metadata for vector type: expected a single sub-field metadata"
            )
        element_type_name = FIELD_ID_TO_NAME[metadata.fields[0].type_code]

        if metadata.vector_dimension is None:
            raise ValueError(
                "Invalid result metadata for vector type: expected a dimension"
            )

        if element_type_name == "FIXED":
            return VectorType(int, metadata.vector_dimension)
        elif element_type_name == "REAL":
            return VectorType(float, metadata.vector_dimension)
        else:
            raise ValueError(
                f"Invalid result metadata for vector type: invalid element type: {element_type_name}"
            )
    elif column_type_name in {"ARRAY", "MAP", "OBJECT"} and getattr(
        metadata, "fields", None
    ):
        # If fields is not defined or empty then the legacy type can be returned instead
        if column_type_name == "ARRAY":
            assert (
                len(metadata.fields) == 1
            ), "ArrayType columns should have one metadata field."
            return ArrayType(
                convert_metadata_to_sp_type(metadata.fields[0], max_string_size),
                structured=True,
                contains_null=metadata.fields[0]._is_nullable,
            )
        elif column_type_name == "MAP":
            assert (
                len(metadata.fields) == 2
            ), "MapType columns should have two metadata fields."
            return MapType(
                convert_metadata_to_sp_type(metadata.fields[0], max_string_size),
                convert_metadata_to_sp_type(metadata.fields[1], max_string_size),
                structured=True,
                value_contains_null=metadata.fields[1]._is_nullable,
            )
        else:
            assert all(
                getattr(field, "name", None) for field in metadata.fields
            ), "All fields of a StructType should be named."
            return StructType(
                [
                    StructField(
                        (
                            field.name
                            if context._should_use_structured_type_semantics()
                            else quote_name(field.name, keep_case=True)
                        ),
                        convert_metadata_to_sp_type(field, max_string_size),
                        nullable=field.is_nullable,
                        _is_column=False,
                    )
                    for field in metadata.fields
                ],
                structured=True,
            )
    else:
        return convert_sf_to_sp_type(
            column_type_name,
            metadata.precision or 0,
            metadata.scale or 0,
            metadata.internal_size or 0,
            max_string_size,
        )


def convert_sf_to_sp_type(
    column_type_name: str,
    precision: int,
    scale: int,
    internal_size: int,
    max_string_size: int,
) -> DataType:
    """Convert the Snowflake logical type to the Snowpark type."""
    semi_structured_fill = (
        None if context._should_use_structured_type_semantics() else StringType()
    )
    if column_type_name == "ARRAY":
        return ArrayType(semi_structured_fill)
    if column_type_name == "VARIANT":
        return VariantType()
    if context._should_use_structured_type_semantics() and column_type_name == "OBJECT":
        return StructType()
    if column_type_name in {"OBJECT", "MAP"}:
        return MapType(semi_structured_fill, semi_structured_fill)
    if column_type_name == "GEOGRAPHY":
        return GeographyType()
    if column_type_name == "GEOMETRY":
        return GeometryType()
    if column_type_name == "FILE":
        return FileType()
    if column_type_name == "BOOLEAN":
        return BooleanType()
    if column_type_name == "BINARY":
        return BinaryType()
    if column_type_name == "TEXT":
        if internal_size > 0:
            return StringType(internal_size, internal_size == max_string_size)
        elif internal_size == 0:
            return StringType()
        raise ValueError("Negative value is not a valid input for StringType")
    if column_type_name == "TIME":
        return TimeType()
    if column_type_name == "TIMESTAMP":
        return TimestampType(timezone=TimestampTimeZone.DEFAULT)
    if column_type_name == "TIMESTAMP_NTZ":
        return TimestampType(timezone=TimestampTimeZone.NTZ)
    if column_type_name == "TIMESTAMP_LTZ":
        return TimestampType(timezone=TimestampTimeZone.LTZ)
    if column_type_name == "TIMESTAMP_TZ":
        return TimestampType(timezone=TimestampTimeZone.TZ)
    if column_type_name == "DATE":
        return DateType()
    if column_type_name == "DECIMAL" or (
        (column_type_name == "FIXED" or column_type_name == "NUMBER") and scale != 0
    ):
        if precision != 0 or scale != 0:
            if precision > DecimalType._MAX_PRECISION:
                return DecimalType(
                    DecimalType._MAX_PRECISION,
                    scale + precision - DecimalType._MAX_SCALE,
                )
            else:
                return DecimalType(precision, scale)
        else:
            return DecimalType(38, 18)
    if column_type_name == "REAL":
        return DoubleType()
    if (column_type_name == "FIXED" or column_type_name == "NUMBER") and scale == 0:
        return LongType()
    raise NotImplementedError(
        "Unsupported type: {}, precision: {}, scale: {}".format(
            column_type_name, precision, scale
        )
    )


def convert_sp_to_sf_type(datatype: DataType, nullable_override=None) -> str:
    if isinstance(datatype, DecimalType):
        return f"NUMBER({datatype.precision}, {datatype.scale})"
    if isinstance(datatype, IntegerType):
        return "INT"
    if isinstance(datatype, ShortType):
        return "SMALLINT"
    if isinstance(datatype, ByteType):
        return "BYTEINT"
    if isinstance(datatype, LongType):
        return "BIGINT"
    if isinstance(datatype, FloatType):
        return "FLOAT"
    if isinstance(datatype, DoubleType):
        return "DOUBLE"
    # We regard NullType as String, which is required when creating
    # a dataframe from local data with all None values
    if isinstance(datatype, StringType):
        if datatype.length:
            return f"STRING({datatype.length})"
        return "STRING"
    if isinstance(datatype, NullType):
        return "STRING"
    if isinstance(datatype, BooleanType):
        return "BOOLEAN"
    if isinstance(datatype, DateType):
        return "DATE"
    if isinstance(datatype, TimeType):
        return "TIME"
    if isinstance(datatype, TimestampType):
        if datatype.tz == TimestampTimeZone.NTZ:
            return "TIMESTAMP_NTZ"
        elif datatype.tz == TimestampTimeZone.LTZ:
            return "TIMESTAMP_LTZ"
        elif datatype.tz == TimestampTimeZone.TZ:
            return "TIMESTAMP_TZ"
        else:
            return "TIMESTAMP"
    if isinstance(datatype, BinaryType):
        return "BINARY"
    if isinstance(datatype, ArrayType):
        if datatype.structured:
            nullable = (
                "" if datatype.contains_null or nullable_override else " NOT NULL"
            )
            return f"ARRAY({convert_sp_to_sf_type(datatype.element_type)}{nullable})"
        else:
            return "ARRAY"
    if isinstance(datatype, MapType):
        if datatype.structured:
            nullable = (
                "" if datatype.value_contains_null or nullable_override else " NOT NULL"
            )
            return f"MAP({convert_sp_to_sf_type(datatype.key_type)}, {convert_sp_to_sf_type(datatype.value_type)}{nullable})"
        else:
            return "OBJECT"
    if isinstance(datatype, StructType):
        if datatype.structured:
            fields = ", ".join(
                f"{field.case_sensitive_name} {convert_sp_to_sf_type(field.datatype)}"
                for field in datatype.fields
            )
            return f"OBJECT({fields})"
        else:
            return "OBJECT"
    if isinstance(datatype, VariantType):
        return "VARIANT"
    if isinstance(datatype, GeographyType):
        return "GEOGRAPHY"
    if isinstance(datatype, GeometryType):
        return "GEOMETRY"
    if isinstance(datatype, FileType):
        return "FILE"
    if isinstance(datatype, VectorType):
        return f"VECTOR({datatype.element_type},{datatype.dimension})"
    raise TypeError(f"Unsupported data type: {datatype.__class__.__name__}")


# Mapping Python types to DataType
NoneType = type(None)
PYTHON_TO_SNOW_TYPE_MAPPINGS = {
    NoneType: NullType,
    bool: BooleanType,
    int: LongType,
    float: FloatType,
    str: StringType,
    bytearray: BinaryType,
    decimal.Decimal: DecimalType,
    datetime.date: DateType,
    datetime.datetime: TimestampType,
    datetime.time: TimeType,
    bytes: BinaryType,
}
if installed_pandas:
    import numpy

    PYTHON_TO_SNOW_TYPE_MAPPINGS.update(
        {
            type(pandas.NaT): TimestampType,
            numpy.float64: DecimalType,
        }
    )


# TODO: these tuples of types can be used with isinstance, but not as a type-hints
VALID_PYTHON_TYPES_FOR_LITERAL_VALUE = (
    *PYTHON_TO_SNOW_TYPE_MAPPINGS.keys(),
    list,
    tuple,
    dict,
)
VALID_SNOWPARK_TYPES_FOR_LITERAL_VALUE = (
    *PYTHON_TO_SNOW_TYPE_MAPPINGS.values(),
    _NumericType,
    ArrayType,
    MapType,
    VariantType,
    FileType,
)

# Mapping Python array types to DataType
ARRAY_SIGNED_INT_TYPECODE_CTYPE_MAPPINGS = {
    "b": ctypes.c_byte,
    "h": ctypes.c_short,
    "i": ctypes.c_int,
    "l": ctypes.c_long,
    "q": ctypes.c_longlong,
}

ARRAY_UNSIGNED_INT_TYPECODE_CTYPE_MAPPINGS = {
    "B": ctypes.c_ubyte,
    "H": ctypes.c_ushort,
    "I": ctypes.c_uint,
    "L": ctypes.c_ulong,
    "Q": ctypes.c_ulonglong,
}


def int_size_to_type(size: int) -> Type[DataType]:
    """
    Return the Catalyst datatype from the size of integers.
    """
    if size <= 8:
        return ByteType
    if size <= 16:
        return ShortType
    if size <= 32:
        return IntegerType
    if size <= 64:
        return LongType


# The list of all supported array typecodes, is stored here
ARRAY_TYPE_MAPPINGS = {
    # Warning: Actual properties for float and double in C is not specified in C.
    # On almost every system supported by both python and JVM, they are IEEE 754
    # single-precision binary floating-point format and IEEE 754 double-precision
    # binary floating-point format. And we do assume the same thing here for now.
    "f": FloatType,
    "d": DoubleType,
}

# compute array typecode mappings for signed integer types
for _typecode in ARRAY_SIGNED_INT_TYPECODE_CTYPE_MAPPINGS.keys():
    size = ctypes.sizeof(ARRAY_SIGNED_INT_TYPECODE_CTYPE_MAPPINGS[_typecode]) * 8
    dt = int_size_to_type(size)
    if dt is not None:
        ARRAY_TYPE_MAPPINGS[_typecode] = dt

# compute array typecode mappings for unsigned integer types
for _typecode in ARRAY_UNSIGNED_INT_TYPECODE_CTYPE_MAPPINGS.keys():
    # JVM does not have unsigned types, so use signed types that is at least 1
    # bit larger to store
    size = ctypes.sizeof(ARRAY_UNSIGNED_INT_TYPECODE_CTYPE_MAPPINGS[_typecode]) * 8 + 1
    dt = int_size_to_type(size)
    if dt is not None:
        ARRAY_TYPE_MAPPINGS[_typecode] = dt

# Type code 'u' in Python's array is deprecated since version 3.3, and will be
# removed in version 4.0. See: https://docs.python.org/3/library/array.html
if sys.version_info[0] < 4:
    ARRAY_TYPE_MAPPINGS["u"] = StringType


def infer_type(obj: Any) -> DataType:
    """Infer the DataType from obj"""
    if obj is None:
        return NullType()

    datatype = PYTHON_TO_SNOW_TYPE_MAPPINGS.get(type(obj))
    if datatype is DecimalType:
        # the precision and scale of `obj` may be different from row to row.
        return DecimalType(38, 18)
    elif datatype is TimestampType and obj.tzinfo is not None:
        # infer tz-aware datetime to TIMESTAMP_TZ
        return datatype(TimestampTimeZone.TZ)

    elif datatype is not None:
        return datatype()

    if isinstance(obj, dict):
        for key, value in obj.items():
            if key is not None and value is not None:
                return MapType(infer_type(key), infer_type(value))
        return MapType(NullType(), NullType())
    elif isinstance(obj, Row) and context._should_use_structured_type_semantics():
        return infer_schema(obj)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            if v is not None:
                return ArrayType(infer_type(obj[0]))
        return ArrayType(NullType())
    elif isinstance(obj, array):
        if obj.typecode in ARRAY_TYPE_MAPPINGS:
            return ArrayType(ARRAY_TYPE_MAPPINGS[obj.typecode]())
        else:
            raise TypeError("not supported type: array(%s)" % obj.typecode)
    else:
        raise TypeError("not supported type: %s" % type(obj))


def infer_schema(
    row: Union[Dict, List, Tuple], names: Optional[List] = None
) -> StructType:
    if row is None or (isinstance(row, (tuple, list, dict)) and not row):
        items = zip(names if names else ["_1"], [None])
    else:
        if isinstance(row, dict):
            items = row.items()
        elif isinstance(row, (tuple, list)):
            row_fields = getattr(row, "_fields", None)
            if row_fields:  # Row or namedtuple
                items = zip(row_fields, row)
            else:
                if names is None:
                    names = [f"_{i}" for i in range(1, len(row) + 1)]
                elif len(names) < len(row):
                    names.extend(f"_{i}" for i in range(len(names) + 1, len(row) + 1))
                items = zip(names, row)
        elif isinstance(row, VALID_PYTHON_TYPES_FOR_LITERAL_VALUE):
            items = zip(names if names else ["_1"], [row])
        else:
            raise TypeError("Can not infer schema for type: %s" % type(row))

    fields = []
    for k, v in items:
        try:
            fields.append(StructField(k, infer_type(v), v is None))
        except TypeError as e:
            raise TypeError(f"Unable to infer the type of the field {k}.") from e
    return StructType(fields)


def merge_type(a: DataType, b: DataType, name: Optional[str] = None) -> DataType:
    # null type
    if isinstance(a, NullType):
        return b
    elif isinstance(b, NullType):
        return a
    elif type(a) is not type(b):
        err_msg = f"Cannot merge type {type(a)} and {type(b)}"
        if name:
            err_msg = f"{name}: {err_msg}"
        raise TypeError(err_msg)

    # same type
    if isinstance(a, StructType):
        name_to_datatype_b = {f.name: f.datatype for f in b.fields}
        name_to_nullable_b = {f.name: f.nullable for f in b.fields}
        fields = [
            StructField(
                f.name,
                merge_type(
                    f.datatype,
                    name_to_datatype_b.get(f.name, NullType()),
                    name=f"field {f.name} in {name}" if name else f"field {f.name}",
                ),
                f.nullable or name_to_nullable_b.get(f.name, True),
            )
            for f in a.fields
        ]
        names = {f.name for f in fields}
        for n in name_to_datatype_b:
            if n not in names:
                fields.append(StructField(n, name_to_datatype_b[n], True))
        return StructType(fields)

    elif isinstance(a, ArrayType):
        return ArrayType(
            merge_type(
                a.element_type, b.element_type, name="element in array %s" % name
            )
        )

    elif isinstance(a, MapType):
        return MapType(
            merge_type(a.key_type, b.key_type, name="key of map %s" % name),
            merge_type(a.value_type, b.value_type, name="value of map %s" % name),
        )
    else:
        return a


def python_value_str_to_object(value, tp: Optional[DataType]) -> Any:
    if tp is None:
        return None

    if isinstance(tp, StringType):
        return value

    if isinstance(
        tp,
        (
            _IntegralType,
            _FractionalType,
            BooleanType,
            BinaryType,
            TimeType,
            DateType,
            TimestampType,
        ),
    ):
        return eval(value)

    if isinstance(tp, ArrayType):
        curr_list = eval(value)
        if curr_list is None:
            return None
        element_tp = tp.element_type or StringType()
        return [python_value_str_to_object(val, element_tp) for val in curr_list]

    if isinstance(tp, MapType):
        curr_dict: dict = eval(value)
        if curr_dict is None:
            return None
        key_tp = tp.key_type or StringType()
        val_tp = tp.value_type or StringType()
        return {
            python_value_str_to_object(k, key_tp): python_value_str_to_object(v, val_tp)
            for k, v in curr_dict.items()
        }

    if isinstance(tp, (GeometryType, GeographyType, VariantType, FileType)):
        if value.strip() == "None":
            return None
        return value

    raise TypeError(
        f"Unsupported data type: {tp}, value {value} by python_value_str_to_object()"
    )


def python_type_str_to_object(
    tp_str: str, is_return_type_for_sproc: bool = False
) -> Type:
    # handle several special cases, which we want to support currently
    if tp_str == "Decimal":
        return decimal.Decimal
    elif tp_str == "date":
        return datetime.date
    elif tp_str == "time":
        return datetime.time
    elif tp_str == "datetime":
        return datetime.datetime
    # This check is to handle special case when stored procs are registered using
    # register_from_file where type hints are read as strings and we don't know if
    # the DataFrame is a snowflake.snowpark.DataFrame or not. Here, the assumption
    # is that when stored procedures are involved, the return type cannot be a
    # pandas.DataFrame, so we return snowpark DataFrame.
    elif tp_str == "DataFrame" and is_return_type_for_sproc:
        return snowflake.snowpark.DataFrame
    elif tp_str in ["Series", "pd.Series"] and installed_pandas:
        return pandas.Series
    elif tp_str in ["DataFrame", "pd.DataFrame"] and installed_pandas:
        return pandas.DataFrame
    else:
        return eval(tp_str)


def python_type_to_snow_type(
    tp: Union[str, Type], is_return_type_of_sproc: bool = False
) -> Tuple[DataType, bool]:
    """Converts a Python type or a Python type string to a Snowpark type.
    Returns a Snowpark type and whether it's nullable.
    """
    from snowflake.snowpark.dataframe import DataFrame

    # convert a type string to a type object
    if isinstance(tp, str):
        tp = python_type_str_to_object(tp, is_return_type_of_sproc)

    if tp is decimal.Decimal:
        return DecimalType(38, 18), False
    elif tp in PYTHON_TO_SNOW_TYPE_MAPPINGS:
        return PYTHON_TO_SNOW_TYPE_MAPPINGS[tp](), False

    tp_origin = get_origin(tp)
    tp_args = get_args(tp)

    # only typing.Optional[X], i.e., typing.Union[X, None] is accepted
    if (
        tp_origin
        and tp_origin == Union
        and tp_args
        and len(tp_args) == 2
        and tp_args[1] == NoneType
    ):
        return python_type_to_snow_type(tp_args[0], is_return_type_of_sproc)[0], True

    # typing.List, typing.Tuple, list, tuple
    list_tps = [list, tuple, List, Tuple]
    if tp in list_tps or (tp_origin and tp_origin in list_tps):
        element_type = (
            python_type_to_snow_type(tp_args[0], is_return_type_of_sproc)[0]
            if tp_args
            else None
        )
        return ArrayType(element_type), False

    # typing.Dict, dict
    dict_tps = [dict, Dict]
    if tp in dict_tps or (tp_origin and tp_origin in dict_tps):
        key_type = (
            python_type_to_snow_type(tp_args[0], is_return_type_of_sproc)[0]
            if tp_args
            else None
        )
        value_type = (
            python_type_to_snow_type(tp_args[1], is_return_type_of_sproc)[0]
            if tp_args
            else None
        )
        if (
            key_type is None or value_type is None
        ) and context._should_use_structured_type_semantics():
            return StructType(), False
        return MapType(key_type, value_type), False

    if installed_pandas:
        pandas_series_tps = [PandasSeries, pandas.Series]
        if tp in pandas_series_tps or (tp_origin and tp_origin in pandas_series_tps):
            return (
                PandasSeriesType(
                    python_type_to_snow_type(tp_args[0], is_return_type_of_sproc)[0]
                    if tp_args
                    else None
                ),
                False,
            )

        pandas_dataframe_tps = [PandasDataFrame, pandas.DataFrame]
        if tp in pandas_dataframe_tps or (
            tp_origin and tp_origin in pandas_dataframe_tps
        ):
            return (
                PandasDataFrameType(
                    [
                        python_type_to_snow_type(tp_arg, is_return_type_of_sproc)[0]
                        for tp_arg in tp_args
                    ]
                    if tp_args
                    else ()
                ),
                False,
            )

    if tp == DataFrame:
        return StructType(), False

    if tp == Variant:
        return VariantType(), False

    if tp == Geography:
        return GeographyType(), False

    if tp == Geometry:
        return GeometryType(), False

    if tp == File:
        return FileType(), False

    if tp == Timestamp or tp_origin == Timestamp:
        if not tp_args:
            timezone = TimestampTimeZone.DEFAULT
        elif tp_args[0] == NTZ:
            timezone = TimestampTimeZone.NTZ
        elif tp_args[0] == LTZ:
            timezone = TimestampTimeZone.LTZ
        elif tp_args[0] == TZ:
            timezone = TimestampTimeZone.TZ
        else:
            raise TypeError(
                f"Only Timestamp, Timestamp[NTZ], Timestamp[LTZ] and Timestamp[TZ] are allowed, but got {tp}"
            )
        return TimestampType(timezone), False

    raise TypeError(f"invalid type {tp}")


def snow_type_to_dtype_str(snow_type: DataType) -> str:
    if isinstance(
        snow_type,
        (
            BinaryType,
            BooleanType,
            FloatType,
            DoubleType,
            DateType,
            TimestampType,
            TimeType,
            GeographyType,
            GeometryType,
            VariantType,
            FileType,
        ),
    ):
        return snow_type.__class__.__name__[:-4].lower()
    if isinstance(snow_type, StringType):
        if snow_type.length:
            return f"string({snow_type.length})"
        return "string"
    if isinstance(snow_type, ByteType):
        return "tinyint"
    if isinstance(snow_type, ShortType):
        return "smallint"
    if isinstance(snow_type, IntegerType):
        return "int"
    if isinstance(snow_type, LongType):
        return "bigint"
    if isinstance(snow_type, ArrayType):
        return f"array<{snow_type_to_dtype_str(snow_type.element_type)}>"
    if isinstance(snow_type, DecimalType):
        return f"decimal({snow_type.precision},{snow_type.scale})"
    if isinstance(snow_type, MapType):
        return f"map<{snow_type_to_dtype_str(snow_type.key_type)},{snow_type_to_dtype_str(snow_type.value_type)}>"
    if isinstance(snow_type, StructType):
        return f"struct<{','.join([snow_type_to_dtype_str(field.datatype) for field in snow_type.fields])}>"
    if isinstance(snow_type, VectorType):
        return f"vector<{snow_type.element_type},{snow_type.dimension}>"

    raise TypeError(f"invalid DataType {snow_type}")


def retrieve_func_defaults_from_source(
    file_path: str,
    func_name: str,
    class_name: Optional[str] = None,
    _source: Optional[str] = None,
) -> Optional[List[Optional[str]]]:
    """
    Retrieve default values assigned to optional arguments of a function from a
    source file, or a source string (test only).

    Returns list of str(default value) if the function is found, None otherwise.
    """

    def parse_default_value(
        value: ast.expr, enquote_string: bool = False
    ) -> Optional[str]:
        # recursively parse the default value if it is tuple or list
        if isinstance(value, (ast.Tuple, ast.List)):
            return f"{[parse_default_value(e) for e in value.elts]}"
        # recursively parse the default keys and values if it is dict
        if isinstance(value, ast.Dict):
            key_val_tuples = [
                (parse_default_value(k), parse_default_value(v))
                for k, v in zip(value.keys, value.values)
            ]
            return f"{dict(key_val_tuples)}"
        # recursively parse the default value.value and extract value.attr
        if isinstance(value, ast.Attribute):
            return f"{parse_default_value(value.value)}.{value.attr}"
        # recursively parse value.value and extract value.arg
        if isinstance(value, ast.keyword):
            return f"{value.arg}={parse_default_value(value.value)}"
        # extract constant value
        if isinstance(value, ast.Constant):
            if isinstance(value.value, str) and enquote_string:
                return f"'{value.value}'"
            if value.value is None:
                return None
            return f"{value.value}"
        # extract value.id from Name
        if isinstance(value, ast.Name):
            return value.id
        # recursively parse value.func and extract value.args and value.keywords
        if isinstance(value, ast.Call):
            parsed_args = ", ".join(
                parse_default_value(arg, True) for arg in value.args
            )
            parsed_kwargs = ", ".join(
                parse_default_value(kw, True) for kw in value.keywords
            )
            combined_parsed_input = (
                f"{parsed_args}, {parsed_kwargs}"
                if parsed_args and parsed_kwargs
                else parsed_args or parsed_kwargs
            )
            return f"{parse_default_value(value.func)}({combined_parsed_input})"
        raise TypeError(f"invalid default value: {value}")

    class FuncNodeVisitor(ast.NodeVisitor):
        default_values = []
        func_exist = False

        def visit_FunctionDef(self, node):
            if node.name == func_name:
                for value in node.args.defaults:
                    self.default_values.append(parse_default_value(value))
                self.func_exist = True

    if not _source:
        with open(file_path) as f:
            _source = f.read()

    if class_name:

        class ClassNodeVisitor(ast.NodeVisitor):
            class_node = None

            def visit_ClassDef(self, node):
                if node.name == class_name:
                    self.class_node = node

        class_visitor = ClassNodeVisitor()
        class_visitor.visit(ast.parse(_source))
        if class_visitor.class_node is None:
            return None
        to_visit_node_for_func = class_visitor.class_node
    else:
        to_visit_node_for_func = ast.parse(_source)

    visitor = FuncNodeVisitor()
    visitor.visit(to_visit_node_for_func)
    if not visitor.func_exist:
        return None
    return visitor.default_values


def retrieve_func_type_hints_from_source(
    file_path: str,
    func_name: str,
    class_name: Optional[str] = None,
    _source: Optional[str] = None,
) -> Optional[Dict[str, str]]:
    """
    Retrieve type hints of a function from a source file, or a source string (test only).
    Returns None if the function is not found.
    """

    def parse_arg_annotation(annotation: ast.expr) -> str:
        if isinstance(annotation, (ast.Tuple, ast.List)):
            return ", ".join([parse_arg_annotation(e) for e in annotation.elts])
        if isinstance(annotation, ast.Attribute):
            return f"{parse_arg_annotation(annotation.value)}.{annotation.attr}"
        if isinstance(annotation, ast.Subscript):
            return f"{parse_arg_annotation(annotation.value)}[{parse_arg_annotation(annotation.slice)}]"
        if isinstance(annotation, ast.Index):
            return parse_arg_annotation(annotation.value)
        if isinstance(annotation, ast.Constant) and annotation.value is None:
            return "NoneType"
        if isinstance(annotation, ast.Name):
            return annotation.id
        raise TypeError(f"invalid type annotation: {annotation}")

    class FuncNodeVisitor(ast.NodeVisitor):
        type_hints = {}
        func_exist = False

        def visit_FunctionDef(self, node):
            if node.name == func_name:
                for arg in node.args.args:
                    if arg.annotation:
                        self.type_hints[arg.arg] = parse_arg_annotation(arg.annotation)
                if node.returns:
                    self.type_hints["return"] = parse_arg_annotation(node.returns)
                self.func_exist = True

    if not _source:
        with open(file_path) as f:
            _source = f.read()

    if class_name:

        class ClassNodeVisitor(ast.NodeVisitor):
            class_node = None

            def visit_ClassDef(self, node):
                if node.name == class_name:
                    self.class_node = node

        class_visitor = ClassNodeVisitor()
        class_visitor.visit(ast.parse(_source))
        if class_visitor.class_node is None:
            return None
        to_visit_node_for_func = class_visitor.class_node
    else:
        to_visit_node_for_func = ast.parse(_source)

    visitor = FuncNodeVisitor()
    visitor.visit(to_visit_node_for_func)
    if not visitor.func_exist:
        return None
    return visitor.type_hints


# Get a mapping from type string to type object, for cast() function
def get_data_type_string_object_mappings(
    to_fill_dict: Dict[str, Type[DataType]],
    data_type: Optional[Type[DataType]] = None,
) -> None:
    if data_type is None:
        get_data_type_string_object_mappings(to_fill_dict, DataType)
        return
    for child in data_type.__subclasses__():
        if not child.__name__.startswith("_") and child is not DecimalType:
            to_fill_dict[child.__name__[:-4].lower()] = child
        get_data_type_string_object_mappings(to_fill_dict, child)


DATA_TYPE_STRING_OBJECT_MAPPINGS = {}
get_data_type_string_object_mappings(DATA_TYPE_STRING_OBJECT_MAPPINGS)
# Add additional mappings to match snowflake db data types
DATA_TYPE_STRING_OBJECT_MAPPINGS["int"] = IntegerType
DATA_TYPE_STRING_OBJECT_MAPPINGS["smallint"] = ShortType
DATA_TYPE_STRING_OBJECT_MAPPINGS["byteint"] = ByteType
DATA_TYPE_STRING_OBJECT_MAPPINGS["bigint"] = LongType
DATA_TYPE_STRING_OBJECT_MAPPINGS["number"] = DecimalType
DATA_TYPE_STRING_OBJECT_MAPPINGS["numeric"] = DecimalType
DATA_TYPE_STRING_OBJECT_MAPPINGS["decimal"] = DecimalType
DATA_TYPE_STRING_OBJECT_MAPPINGS["object"] = MapType
DATA_TYPE_STRING_OBJECT_MAPPINGS["array"] = ArrayType
DATA_TYPE_STRING_OBJECT_MAPPINGS["timestamp_ntz"] = functools.partial(
    TimestampType, timezone=TimestampTimeZone.NTZ
)
DATA_TYPE_STRING_OBJECT_MAPPINGS["timestamp_tz"] = functools.partial(
    TimestampType, timezone=TimestampTimeZone.TZ
)
DATA_TYPE_STRING_OBJECT_MAPPINGS["timestamp_ltz"] = functools.partial(
    TimestampType, timezone=TimestampTimeZone.LTZ
)

DECIMAL_RE = re.compile(
    r"^\s*(numeric|number|decimal)\s*\(\s*(\s*)(\d*)\s*,\s*(\d*)\s*\)\s*$"
)
# support type string format like "  decimal  (  2  ,  1  )  "

STRING_RE = re.compile(r"^\s*(varchar|string|text)\s*\(\s*(\d*)\s*\)\s*$")
# support type string format like "  string  (  23  )  "

ARRAY_RE = re.compile(r"(?i)^\s*array\s*<")
# support type string format like starting with "array<..."

MAP_RE = re.compile(r"(?i)^\s*map\s*<")
# support type string format like starting with "map<..."

STRUCT_RE = re.compile(r"(?i)^\s*struct\s*<")
# support type string format like starting with "struct<..."

_NOT_NULL_PATTERN = re.compile(r"^(?P<base>.*?)\s+not\s+null\s*$", re.IGNORECASE)


def get_number_precision_scale(type_str: str) -> Optional[Tuple[int, int]]:
    decimal_matches = DECIMAL_RE.match(type_str)
    if decimal_matches:
        return int(decimal_matches.group(3)), int(decimal_matches.group(4))


def get_string_length(type_str: str) -> Optional[int]:
    string_matches = STRING_RE.match(type_str)
    if string_matches:
        return int(string_matches.group(2))


def extract_bracket_content(type_str: str, keyword: str) -> str:
    """
    Given a string that starts with e.g. "array<", returns the content inside the top-level <...>.
    e.g., "array<int>" => "int". It also parses the nested array like "array<array<...>>".
    Raises ValueError on mismatched or missing bracket.
    """
    type_str = type_str.strip()
    prefix_pattern = rf"(?i)^\s*{keyword}\s*<"
    match = re.match(prefix_pattern, type_str)
    if not match:
        raise ValueError(
            f"'{type_str}' does not match expected '{keyword}<...>' syntax."
        )

    start_index = match.end() - 1  # position at '<'
    bracket_depth = 0
    inside_chars: List[str] = []
    i = start_index
    while i < len(type_str):
        c = type_str[i]
        if c == "<":
            bracket_depth += 1
            # we don't store the opening bracket in 'inside_chars'
            # if bracket_depth was 0 -> 1, to skip the outer bracket
            if bracket_depth > 1:
                inside_chars.append(c)
        elif c == ">":
            bracket_depth -= 1
            if bracket_depth < 0:
                raise ValueError(f"Mismatched '>' in '{type_str}'")
            if bracket_depth == 0:
                if i != len(type_str) - 1:
                    raise ValueError(
                        f"Unexpected characters after closing '>' in '{type_str}'"
                    )
                # done
                return "".join(inside_chars).strip()
            inside_chars.append(c)
        else:
            inside_chars.append(c)
        i += 1

    raise ValueError(f"Missing closing '>' in '{type_str}'.")


def extract_nullable_keyword(type_str: str) -> Tuple[str, bool]:
    """
    Checks if `type_str` ends with something like 'NOT NULL' (ignoring
    case and allowing arbitrary space between NOT and NULL). If found,
    return the type substring minus that part, along with nullable=False.
    Otherwise, return (type_str, True).
    """
    trimmed = type_str.strip()
    match = _NOT_NULL_PATTERN.match(trimmed)
    if match:
        # Group 'base' is everything before 'not null'
        base_type_str = match.group("base").strip()
        return base_type_str, False

    # By default, the field is nullable
    return trimmed, True


def find_top_level_colon(field_def: str) -> int:
    """
    Returns the index of the first top-level colon in 'field_def',
    or -1 if there is no top-level colon. A colon is considered top-level
    if it is not enclosed in <...> or (...).

    Example:
      'a struct<i: integer>' => returns -1 (colon is nested).
      'x: struct<i: integer>' => returns index of the colon after 'x'.
    """
    bracket_depth = 0
    for i, ch in enumerate(field_def):
        if ch in ("<", "("):
            bracket_depth += 1
        elif ch in (">", ")"):
            bracket_depth -= 1
        elif ch == ":" and bracket_depth == 0:
            return i
    return -1


def parse_struct_field_list(fields_str: str) -> Optional[StructType]:
    """
    Parse something like "a: int, b: string, c: array<int>"
    into StructType([StructField('a', IntegerType()), ...]).
    """
    fields = []
    field_defs = split_top_level_comma_fields(fields_str)
    for field_def in field_defs:
        # Find first top-level colon (if any)
        colon_index = find_top_level_colon(field_def)
        if colon_index != -1:
            # We found a top-level colon => split on it
            left = field_def[:colon_index]
            right = field_def[colon_index + 1 :]
        else:
            # No top-level colon => fallback to whitespace-based split
            parts = field_def.split(None, 1)
            if len(parts) != 2:
                raise ValueError(f"Cannot parse struct field definition: '{field_def}'")
            left, right = parts[0], parts[1]

        field_name = left.strip()
        type_part = right.strip()
        if not field_name:
            raise ValueError(f"Struct field missing name in '{field_def}'")

        # 1) Check for trailing "NOT NULL" => sets nullable=False
        base_type_str, nullable = extract_nullable_keyword(type_part)
        # 2) Parse the base type
        try:
            field_type = type_string_to_type_object(base_type_str)
        except ValueError as ex:
            # Spark supports both `x: int` and `x int`. In our original implementation, we don't support x int,
            # and will raise this error. However, handling space is tricky because we need to handle something like
            # decimal(10, 2) containing space too, as a valid schema string (without a column name).
            # Therefore, if this error is raised, we just catch it and return None, then in next step,
            # we can process it again as a structured schema string (x int).
            if "is not a supported type" in str(ex):
                return None
            raise ex
        fields.append(StructField(field_name, field_type, nullable=nullable))

    return StructType(fields)


def split_top_level_comma_fields(s: str) -> List[str]:
    """
    Splits 's' by commas not enclosed in matching brackets.
    Example: "int, array<long>, decimal(10,2)" => ["int", "array<long>", "decimal(10,2)"].
    """
    parts = []
    bracket_depth = 0
    start_idx = 0
    for i, c in enumerate(s):
        if c in ["<", "("]:
            bracket_depth += 1
        elif c in [">", ")"]:
            bracket_depth -= 1
            if bracket_depth < 0:
                raise ValueError(f"Mismatched bracket in '{s}'.")
        elif c == "," and bracket_depth == 0:
            parts.append(s[start_idx:i].strip())
            start_idx = i + 1
    parts.append(s[start_idx:].strip())
    return parts


def is_likely_struct(s: str) -> bool:
    """
    Return True if there's a top-level colon, comma, or space.
    e.g. "arr array<integer>" => top-level space => struct
         "arr: array<int>" => colon => struct
         "a: int, b: string" => comma => struct
    """
    bracket_depth = 0
    top_level_space_found = False
    for ch in s:
        if ch in ("<", "("):
            bracket_depth += 1
        elif ch in (">", ")"):
            bracket_depth -= 1
        elif bracket_depth == 0:
            if ch in [":", ","]:
                return True
            elif ch == " ":
                top_level_space_found = True

    return top_level_space_found


def type_string_to_type_object(type_str: str) -> DataType:
    type_str = type_str.strip()
    if not type_str:
        raise ValueError("Empty type string")

    # First check if this might be a top-level multi-field struct
    #    (e.g. "a: int, b: string") even if not written as "struct<...>"
    if is_likely_struct(type_str):
        result = parse_struct_field_list(type_str)
        if result is not None:
            return result

    # Check for array<...>
    if ARRAY_RE.match(type_str):
        inner = extract_bracket_content(type_str, "array")
        element_type = type_string_to_type_object(inner)
        return ArrayType(element_type)

    # Check for map<key, value>
    if MAP_RE.match(type_str):
        inner = extract_bracket_content(type_str, "map")
        parts = split_top_level_comma_fields(inner)
        if len(parts) != 2:
            raise ValueError(f"Invalid map type definition: '{type_str}'")
        key_type = type_string_to_type_object(parts[0])
        val_type = type_string_to_type_object(parts[1])
        return MapType(key_type, val_type)

    # Check for explicit struct<...>
    if STRUCT_RE.match(type_str):
        inner = extract_bracket_content(type_str, "struct")
        return parse_struct_field_list(inner)

    precision_scale = get_number_precision_scale(type_str)
    if precision_scale:
        return DecimalType(*precision_scale)
    length = get_string_length(type_str)
    if length:
        return StringType(length)
    type_str = type_str.replace(" ", "")
    type_str = type_str.lower()
    try:
        return DATA_TYPE_STRING_OBJECT_MAPPINGS[type_str]()
    except KeyError:
        raise ValueError(f"'{type_str}' is not a supported type")


# Type hints
ColumnOrName = Union["snowflake.snowpark.column.Column", str]
ColumnOrLiteralStr = Union["snowflake.snowpark.column.Column", str]
ColumnOrSqlExpr = Union["snowflake.snowpark.column.Column", str]
LiteralType = Union[VALID_PYTHON_TYPES_FOR_LITERAL_VALUE]
ColumnOrLiteral = Union["snowflake.snowpark.column.Column", LiteralType]
