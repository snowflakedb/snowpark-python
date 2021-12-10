#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the Apache Software Foundation as part of the
# Apache Spark project, under the Apache License, Version 2.0.

import collections
import ctypes
import datetime
import decimal
import sys
from array import array
from typing import (
    Any,
    DefaultDict,
    Dict,
    List,
    Optional,
    OrderedDict,
    Tuple,
    Type,
    Union,
    get_args,
    get_origin,
)

from snowflake.snowpark._internal.sp_types.sp_data_types import (
    ArrayType as SPArrayType,
    BinaryType as SPBinaryType,
    BooleanType as SPBooleanType,
    ByteType as SPByteType,
    DataType as SPDataType,
    DateType as SPDateType,
    DecimalType as SPDecimalType,
    DoubleType as SPDoubleType,
    FloatType as SPFloatType,
    IntegerType as SPIntegerType,
    LongType as SPLongType,
    MapType as SPMapType,
    NullType as SPNullType,
    ShortType as SPShortType,
    StringType as SPStringType,
    StructField as SPStructField,
    StructType as SPStructType,
    TimestampType as SPTimestampType,
    TimeType as SPTimeType,
    VariantType as SPVariantType,
)
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    TimeType,
    VariantType,
    _NumericType,
)


def convert_to_sf_type(datatype: DataType) -> str:
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
    if isinstance(datatype, (StringType, NullType)):
        return "STRING"
    if isinstance(datatype, BooleanType):
        return "BOOLEAN"
    if isinstance(datatype, DateType):
        return "DATE"
    if isinstance(datatype, TimeType):
        return "TIME"
    if isinstance(datatype, TimestampType):
        return "TIMESTAMP"
    if isinstance(datatype, BinaryType):
        return "BINARY"
    if isinstance(datatype, ArrayType):
        return "ARRAY"
    if isinstance(datatype, MapType):
        return "OBJECT"
    if isinstance(datatype, VariantType):
        return "VARIANT"
    # if isinstance(datatype, GeographyType):
    #    return "GEOGRAPHY"
    raise TypeError(f"Unsupported data type: {datatype.type_name}")


def snow_type_to_sp_type(datatype: DataType) -> Optional[SPDataType]:
    """Mapping from snowflake data-types to SP data-types."""

    # handling the corner case that a UDF doesn't accept any input.
    if datatype is None:
        return None
    if type(datatype) == NullType:
        return SPNullType()
    if type(datatype) == BooleanType:
        return SPBooleanType()
    if type(datatype) == StringType:
        return SPStringType()
    if type(datatype) == StructType:
        return SPStructType(
            [
                SPStructField(
                    field.name, snow_type_to_sp_type(field.dataType), field.nullable
                )
                for field in datatype.fields
            ]
        )
    if type(datatype) == ByteType:
        return SPByteType()
    if type(datatype) == ShortType:
        return SPShortType()
    if type(datatype) == IntegerType:
        return SPIntegerType()
    if type(datatype) == LongType:
        return SPLongType()
    if type(datatype) == FloatType:
        return SPFloatType()
    if type(datatype) == DoubleType:
        return SPDoubleType()
    if type(datatype) == DateType:
        return SPDateType()
    if type(datatype) == TimeType:
        return SPTimeType()
    if type(datatype) == TimestampType:
        return SPTimestampType()
    if type(datatype) == BinaryType:
        return SPBinaryType()
    if type(datatype) == ArrayType:
        return SPArrayType(
            snow_type_to_sp_type(datatype.element_type), contains_null=True
        )
    if type(datatype) == MapType:
        return SPMapType(
            snow_type_to_sp_type(datatype.key_type),
            snow_type_to_sp_type(datatype.value_type),
            value_contains_null=True,
        )
    if type(datatype) == VariantType:
        return SPVariantType()
    if type(datatype) == DecimalType:
        return SPDecimalType(datatype.precision, datatype.scale)
    # if type(datatype) == GeographyType:
    #    return GeographyType(snow_type_to_sp_type(valueType))
    # raise internal error
    raise TypeError(f"Could not convert snowflake type {datatype}")


def to_sp_struct_type(struct_type: StructType) -> SPStructType:
    return snow_type_to_sp_type(struct_type)


def sp_type_to_snow_type(datatype: SPDataType) -> DataType:
    """Mapping from SP data-types, to snowflake data-types"""
    if type(datatype) == SPNullType:
        return NullType()
    if type(datatype) == SPBooleanType:
        return BooleanType()
    if type(datatype) == SPStringType:
        return StringType()
    if type(datatype) == SPStructType:
        return StructType(
            [
                StructField(
                    field.name, sp_type_to_snow_type(field.dataType), field.nullable
                )
                for field in datatype.fields
            ]
        )
    if type(datatype) == SPByteType:
        return ByteType()
    if type(datatype) == SPShortType:
        return ShortType()
    if type(datatype) == SPIntegerType:
        return IntegerType()
    if type(datatype) == SPLongType:
        return LongType()
    if type(datatype) == SPFloatType:
        return FloatType()
    if type(datatype) == SPDoubleType:
        return DoubleType()
    if type(datatype) == SPDateType:
        return DateType()
    if type(datatype) == SPTimeType:
        return TimeType()
    if type(datatype) == SPTimestampType:
        return TimestampType()
    if type(datatype) == SPBinaryType:
        return BinaryType()
    if type(datatype) == SPArrayType:
        return ArrayType(sp_type_to_snow_type(datatype.element_type))
    if type(datatype) == SPMapType:
        return MapType(
            sp_type_to_snow_type(datatype.key_type),
            sp_type_to_snow_type(datatype.value_type),
        )
    if type(datatype) == SPVariantType:
        return VariantType()
    if type(datatype) == SPDecimalType:
        return DecimalType(datatype.precision, datatype.scale)
    # if type(datatype) == GeographyType:
    #    return GeographyType(sp_type_to_snow_type(valueType))
    # raise internal error
    raise Exception("Could not convert spark type {}".format(datatype))


def to_snow_struct_type(struct_type: SPStructType) -> StructType:
    return sp_type_to_snow_type(struct_type)


# #####################################################################################
# Converting python types to SP-types
# Taken as is or modified from:
# https://spark.apache.org/docs/3.1.1/api/python/_modules/pyspark/sql/types.html

# Mapping Python types to Spark SQL DataType
_type_mappings = {
    type(None): SPNullType,
    bool: SPBooleanType,
    int: SPLongType,
    float: SPFloatType,
    str: SPStringType,
    bytearray: SPBinaryType,
    decimal.Decimal: SPDecimalType,
    datetime.date: SPDateType,
    datetime.datetime: SPTimestampType,
    datetime.time: SPTimeType,
    bytes: SPBinaryType,
}

_pandas_type_mappings = {
    "int8": "INTEGER",
    "int16": "INTEGER",
    "int32": "INTEGER",
    "int64": "INTEGER",
    "uint8": "INTEGER",
    "uint16": "INTEGER",
    "uint32": "INTEGER",
    "uint64": "INTEGER",
    "int": "INTEGER",
    "float32": "FLOAT",
    "float64": "FLOAT",
    "object": "VARCHAR",
    "string": "VARCHAR",
    "datetime64[ns]": "TIMESTAMP_NTZ",
    "datetime64": "TIMESTAMP_NTZ",
    "bool": "BOOLEAN",
    "boolean": "BOOLEAN",
}

_VALID_PYTHON_TYPES_FOR_LITERAL_VALUE = tuple(_type_mappings.keys())
_VALID_SNOWPARK_TYPES_FOR_LITERAL_VALUE = (
    *_type_mappings.values(),
    _NumericType,
)

# Mapping Python array types to Spark SQL DataType
# We should be careful here. The size of these types in python depends on C
# implementation. We need to make sure that this conversion does not lose any
# precision. Also, JVM only support signed types, when converting unsigned types,
# keep in mind that it require 1 more bit when stored as signed types.
#
# Reference for C integer size, see:
# ISO/IEC 9899:201x specification, chapter 5.2.4.2.1 Sizes of integer types <limits.h>.
# Reference for python array typecode, see:
# https://docs.python.org/2/library/array.html
# https://docs.python.org/3.6/library/array.html
# Reference for JVM's supported integral types:
# http://docs.oracle.com/javase/specs/jvms/se8/html/jvms-2.html#jvms-2.3.1

_array_signed_int_typecode_ctype_mappings = {
    "b": ctypes.c_byte,
    "h": ctypes.c_short,
    "i": ctypes.c_int,
    "l": ctypes.c_long,
    "q": ctypes.c_longlong,
}

_array_unsigned_int_typecode_ctype_mappings = {
    "B": ctypes.c_ubyte,
    "H": ctypes.c_ushort,
    "I": ctypes.c_uint,
    "L": ctypes.c_ulong,
    "Q": ctypes.c_ulonglong,
}


def _int_size_to_type(size):
    """
    Return the Catalyst datatype from the size of integers.
    """
    if size <= 8:
        return SPByteType
    if size <= 16:
        return SPShortType
    if size <= 32:
        return SPIntegerType
    if size <= 64:
        return SPLongType


# The list of all supported array typecodes, is stored here
_array_type_mappings = {
    # Warning: Actual properties for float and double in C is not specified in C.
    # On almost every system supported by both python and JVM, they are IEEE 754
    # single-precision binary floating-point format and IEEE 754 double-precision
    # binary floating-point format. And we do assume the same thing here for now.
    "f": SPFloatType,
    "d": SPDoubleType,
}

# compute array typecode mappings for signed integer types
for _typecode in _array_signed_int_typecode_ctype_mappings.keys():
    size = ctypes.sizeof(_array_signed_int_typecode_ctype_mappings[_typecode]) * 8
    dt = _int_size_to_type(size)
    if dt is not None:
        _array_type_mappings[_typecode] = dt

# compute array typecode mappings for unsigned integer types
for _typecode in _array_unsigned_int_typecode_ctype_mappings.keys():
    # JVM does not have unsigned types, so use signed types that is at least 1
    # bit larger to store
    size = ctypes.sizeof(_array_unsigned_int_typecode_ctype_mappings[_typecode]) * 8 + 1
    dt = _int_size_to_type(size)
    if dt is not None:
        _array_type_mappings[_typecode] = dt

# Type code 'u' in Python's array is deprecated since version 3.3, and will be
# removed in version 4.0. See: https://docs.python.org/3/library/array.html
if sys.version_info[0] < 4:
    _array_type_mappings["u"] = SPStringType


def _infer_type(obj):
    """Infer the DataType from obj"""
    if obj is None:
        return SPNullType()

    # user-defined types
    if hasattr(obj, "__UDT__"):
        return obj.__UDT__

    datatype = _type_mappings.get(type(obj))
    if datatype is SPDecimalType:
        # the precision and scale of `obj` may be different from row to row.
        return SPDecimalType(38, 18)
    elif datatype is not None:
        return datatype()

    if isinstance(obj, dict):
        for key, value in obj.items():
            if key is not None and value is not None:
                return SPMapType(_infer_type(key), _infer_type(value), True)
        return SPMapType(SPNullType(), SPNullType(), True)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            if v is not None:
                return SPArrayType(_infer_type(obj[0]), True)
        return SPArrayType(SPNullType(), True)
    elif isinstance(obj, array):
        if obj.typecode in _array_type_mappings:
            return SPArrayType(_array_type_mappings[obj.typecode](), False)
        else:
            raise TypeError("not supported type: array(%s)" % obj.typecode)
    else:
        raise TypeError("not supported type: %s" % type(obj))


def _infer_schema_from_list(row: List, names: Optional[List] = None) -> StructType:
    """Infer the schema from list"""
    if names is None:
        names = (
            ["_%d" % i for i in range(1, len(row) + 1)] if len(row) > 1 else ["VALUES"]
        )
    elif len(names) < len(row):
        names.extend("_%d" % i for i in range(len(names) + 1, len(row) + 1))

    fields = []
    for k, v in zip(names, row):
        try:
            fields.append(StructField(k, sp_type_to_snow_type(_infer_type(v)), True))
        except TypeError as e:
            raise TypeError(
                "Unable to infer the type of the field {}.".format(k)
            ) from e
    return StructType(fields)


def _merge_type(a: DataType, b: DataType, name: str = None) -> DataType:
    if name is None:
        new_msg = lambda msg: msg
        new_name = lambda n: "field %s" % n
    else:
        new_msg = lambda msg: "{}: {}".format(name, msg)
        new_name = lambda n: "field {} in {}".format(n, name)

    # null type
    if isinstance(a, NullType):
        return b
    elif isinstance(b, NullType):
        return a
    elif type(a) is not type(b):
        raise TypeError(new_msg("Cannot merge type {} and {}".format(type(a), type(b))))

    # same type
    if isinstance(a, StructType):
        nfs = {f.name: f.datatype for f in b.fields}
        fields = [
            StructField(
                f.name,
                _merge_type(
                    f.datatype, nfs.get(f.name, NullType()), name=new_name(f.name)
                ),
            )
            for f in a.fields
        ]
        names = {f.name for f in fields}
        for n in nfs:
            if n not in names:
                fields.append(StructField(n, nfs[n]))
        return StructType(fields)

    elif isinstance(a, ArrayType):
        return ArrayType(
            _merge_type(
                a.element_type, b.element_type, name="element in array %s" % name
            )
        )

    elif isinstance(a, MapType):
        return MapType(
            _merge_type(a.key_type, b.key_type, name="key of map %s" % name),
            _merge_type(a.value_type, b.value_type, name="value of map %s" % name),
        )
    else:
        return a


def _python_type_to_snow_type(tp: Type) -> Tuple[DataType, bool]:
    """Converts a Python type to a Snowpark type.
    Returns a Snowpark type and whether it's nullable.
    """
    if tp in _type_mappings:
        return sp_type_to_snow_type(_type_mappings[tp]()), False

    tp_origin = get_origin(tp)
    tp_args = get_args(tp)

    # only typing.Optional[X], i.e., typing.Union[X, None] is accepted
    if (
        tp_origin
        and tp_origin == Union
        and tp_args
        and len(tp_args) == 2
        and tp_args[1] == type(None)
    ):
        return _python_type_to_snow_type(tp_args[0])[0], True

    # typing.List, typing.Tuple, list, tuple
    list_tps = [list, tuple, List, Tuple]
    if tp in list_tps or (tp_origin and tp_origin in list_tps):
        element_type = (
            _python_type_to_snow_type(tp_args[0])[0] if tp_args else StringType()
        )
        return ArrayType(element_type), False

    # typing.Dict, typing.DefaultDict, typing.OrderDict, dict, defaultdict, OrderedDict
    dict_tps = [
        dict,
        collections.defaultdict,
        collections.OrderedDict,
        OrderedDict,
        Dict,
        DefaultDict,
    ]
    if tp in dict_tps or (tp_origin and tp_origin in dict_tps):
        key_type = _python_type_to_snow_type(tp_args[0])[0] if tp_args else StringType()
        value_type = (
            _python_type_to_snow_type(tp_args[1])[0] if tp_args else StringType()
        )
        return MapType(key_type, value_type), False

    if tp == Any:
        return VariantType(), False

    raise TypeError(f"invalid type {tp}")


# Type hints
ColumnOrName = Union["snowflake.snowpark.column.Column", str]
LiteralType = Union[_VALID_PYTHON_TYPES_FOR_LITERAL_VALUE]
ColumnOrLiteral = Union["snowflake.snowpark.column.Column", LiteralType]
