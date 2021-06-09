#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from array import array
import ctypes
import decimal
import datetime
import sys

from .sf_types import BinaryType, BooleanType, DataType, DateType, IntegerType, LongType, \
    DoubleType, FloatType, ShortType, ByteType, DecimalType, StringType, TimeType, VariantType, \
    TimestampType, StructType, MapType, ArrayType

from .sp_data_types import DataType as SPDataType, BooleanType as SPBooleanType, \
    StructType as SPStructType, StructField as SPStructField, StringType as SPStringType, \
    ByteType as SPByteType, ShortType as SPShortType, IntegerType as SPIntegerType, \
    LongType as SPLongType, FloatType as SPFloatType, DoubleType as SPDoubleType, \
    DateType as SPDateType, TimeType as SPTimeType, TimestampType as SPTimestampType, \
    BinaryType as SPBinaryType, ArrayType as SPArrayType, MapType as SPMapType, \
    VariantType as SPVariantType, DecimalType as SPDecimalType, NullType as SPNullType


def udf_option_supported(data_type: DataType) -> bool:
    if type(data_type) in [IntegerType, LongType, DoubleType, FloatType, ShortType, ByteType,
                           BooleanType]:
        return True
    else:
        return False


# TODO revisit when dealing with Java UDFs. We'll probably hard-code the return values.
def to_java_type(datatype):
    pass


# TODO revisit when dealing with Java UDFs. We'll probably hard-code the return values.
def to_udf_argument_type(datatype) -> str:
    pass


# TODO maybe change to isinstance()
def convert_to_sf_type(datatype: DataType) -> str:
    if type(datatype) == DecimalType:
        return f"NUMBER(${datatype.precision}, ${datatype.scale})"
    if type(datatype) == IntegerType:
        return "INT"
    if type(datatype) == ShortType:
        return "SMALLINT"
    if type(datatype) == ByteType:
        return "BYTEINT"
    if type(datatype) == LongType:
        return "BIGINT"
    if type(datatype) == FloatType:
        return "FLOAT"
    if type(datatype) == DoubleType:
        return "DOUBLE"
    if type(datatype) == StringType:
        return "STRING"
    if type(datatype) == BooleanType:
        return "BOOLEAN"
    if type(datatype) == DateType:
        return "DATE"
    if type(datatype) == TimeType:
        return "TIME"
    if type(datatype) == TimestampType:
        return "TIMESTAMP"
    if type(datatype) == BinaryType:
        return "BINARY"
    if type(datatype) == ArrayType:
        return "ARRAY"
    if type(datatype) == VariantType:
        return "VARIANT"
    # if type(data_type) is GeographyType:
    #    return "GEOGRAPHY"
    raise Exception(f"Unsupported data type: {datatype.type_name}")


def snow_type_to_sp_type(datatype: DataType) -> SPDataType:
    """ Mapping from snowflake data-types, to SP data-types """
    if type(datatype) == BooleanType:
        return SPBooleanType()
    if type(datatype) == StringType:
        return SPStringType()
    if type(datatype) == StructType:
        return SPStructType([
            SPStructField(field.name, snow_type_to_sp_type(field.dataType),
                          field.nullable)
            for field in datatype.fields])
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
    if type(datatype) == DateType():
        return SPDateType()
    if type(datatype) == TimeType:
        return SPTimeType()
    if type(datatype) == TimestampType:
        return SPTimestampType()
    if type(datatype) == BinaryType:
        return SPBinaryType()
    if type(datatype) == ArrayType:
        return SPArrayType(snow_type_to_sp_type(datatype.element_type),
                           contains_null=True)
    if type(datatype) == MapType:
        return SPMapType(snow_type_to_sp_type(datatype.key_type),
                         snow_type_to_sp_type(datatype.value_type),
                         value_contains_null=True)
    if type(datatype) == VariantType:
        return SPVariantType()
    if type(datatype) == DecimalType:
        return SPDecimalType(datatype.precision, datatype.scale)
    # if type(datatype) == GeographyType:
    #    return SPGeographyType(snowTypeToSpType(valueType))
    return None


def to_sp_struct_type(struct_type: StructType) -> SPStructType:
    return snow_type_to_sp_type(struct_type)


# TODO
def sp_type_to_snow_type(data_type: SPDateType) -> DataType:
    pass


def to_snow_struct_type(struct_type: SPStructType) -> StructType:
    return sp_type_to_snow_type(struct_type)


# #####################################################################################
# Converting python types to SP-types
# Taken from: https://spark.apache.org/docs/3.1.1/api/python/_modules/pyspark/sql/types.html

# Mapping Python types to Spark SQL DataType
_type_mappings = {
    type(None): SPNullType,
    bool: SPBooleanType,
    int: SPLongType,
    float: SPDoubleType,
    str: SPStringType,
    bytearray: SPBinaryType,
    decimal.Decimal: SPDecimalType,
    datetime.date: SPDateType,
    datetime.datetime: SPTimestampType,
    datetime.time: SPTimestampType,
    bytes: SPBinaryType,
}

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
    'b': ctypes.c_byte,
    'h': ctypes.c_short,
    'i': ctypes.c_int,
    'l': ctypes.c_long,
}

_array_unsigned_int_typecode_ctype_mappings = {
    'B': ctypes.c_ubyte,
    'H': ctypes.c_ushort,
    'I': ctypes.c_uint,
    'L': ctypes.c_ulong
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
    'f': SPFloatType,
    'd': SPDoubleType
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
    _array_type_mappings['u'] = SPStringType


def _infer_type(obj):
    """Infer the DataType from obj
    """
    if obj is None:
        return SPNullType()

    # user-defined types
    if hasattr(obj, '__UDT__'):
        return obj.__UDT__

    data_type = _type_mappings.get(type(obj))
    if data_type is SPDecimalType:
        # the precision and scale of `obj` may be different from row to row.
        return SPDecimalType(38, 18)
    elif data_type is not None:
        return data_type()

    if isinstance(obj, dict):
        for key, value in obj.items():
            if key is not None and value is not None:
                return SPMapType(_infer_type(key), _infer_type(value), True)
        return SPMapType(SPNullType(), SPNullType(), True)
    elif isinstance(obj, list):
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
        # try:
        #    return _infer_schema(obj)
        # except TypeError:
        raise TypeError("not supported type: %s" % type(obj))
