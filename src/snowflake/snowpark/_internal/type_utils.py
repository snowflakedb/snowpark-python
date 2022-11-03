#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the Apache Software Foundation as part of the
# Apache Spark project, under the Apache License, Version 2.0.
import ast
import ctypes
import datetime
import decimal
import re
import sys
import typing  # noqa: F401
from array import array
from typing import (  # noqa: F401
    Any,
    Dict,
    Generator,
    Iterable,
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

import snowflake.snowpark.types  # type: ignore
from snowflake.connector.options import installed_pandas, pandas
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
    Geography,
    GeographyType,
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
    Variant,
    VariantType,
    _NumericType,
)

if installed_pandas:
    from snowflake.snowpark.types import (
        PandasDataFrame,
        PandasDataFrameType,
        PandasSeries,
        PandasSeriesType,
    )


def convert_sf_to_sp_type(
    column_type_name: str, precision: int, scale: int
) -> DataType:
    """Convert the Snowflake logical type to the Snowpark type."""
    if column_type_name == "ARRAY":
        return ArrayType(StringType())
    if column_type_name == "VARIANT":
        return VariantType()
    if column_type_name == "OBJECT":
        return MapType(StringType(), StringType())
    if column_type_name == "GEOGRAPHY":
        return GeographyType()
    if column_type_name == "BOOLEAN":
        return BooleanType()
    if column_type_name == "BINARY":
        return BinaryType()
    if column_type_name == "TEXT":
        return StringType()
    if column_type_name == "TIME":
        return TimeType()
    if column_type_name in (
        "TIMESTAMP",
        "TIMESTAMP_LTZ",
        "TIMESTAMP_TZ",
        "TIMESTAMP_NTZ",
    ):
        return TimestampType()
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


def convert_sp_to_sf_type(datatype: DataType) -> str:
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
    if isinstance(datatype, GeographyType):
        return "GEOGRAPHY"
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
    elif datatype is not None:
        return datatype()

    if isinstance(obj, dict):
        for key, value in obj.items():
            if key is not None and value is not None:
                return MapType(infer_type(key), infer_type(value))
        return MapType(NullType(), NullType())
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
            fields.append(StructField(k, infer_type(v), True))
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
        nfs = {f.name: f.datatype for f in b.fields}
        fields = [
            StructField(
                f.name,
                merge_type(
                    f.datatype,
                    nfs.get(f.name, NullType()),
                    name=f"field {f.name} in {name}" if name else f"field {f.name}",
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


def python_type_str_to_object(tp_str: str) -> Type:
    # handle several special cases, which we want to support currently
    if tp_str == "Decimal":
        return decimal.Decimal
    elif tp_str == "date":
        return datetime.date
    elif tp_str == "time":
        return datetime.time
    elif tp_str == "datetime":
        return datetime.datetime
    elif tp_str in ["Series", "pd.Series"] and installed_pandas:
        return pandas.Series
    elif tp_str in ["DataFrame", "pd.DataFrame"] and installed_pandas:
        return pandas.DataFrame
    else:
        return eval(tp_str)


def python_type_to_snow_type(tp: Union[str, Type]) -> Tuple[DataType, bool]:
    """Converts a Python type or a Python type string to a Snowpark type.
    Returns a Snowpark type and whether it's nullable.
    """
    # convert a type string to a type object
    if isinstance(tp, str):
        tp = python_type_str_to_object(tp)

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
        return python_type_to_snow_type(tp_args[0])[0], True

    # typing.List, typing.Tuple, list, tuple
    list_tps = [list, tuple, List, Tuple]
    if tp in list_tps or (tp_origin and tp_origin in list_tps):
        element_type = (
            python_type_to_snow_type(tp_args[0])[0] if tp_args else StringType()
        )
        return ArrayType(element_type), False

    # typing.Dict, dict
    dict_tps = [dict, Dict]
    if tp in dict_tps or (tp_origin and tp_origin in dict_tps):
        key_type = python_type_to_snow_type(tp_args[0])[0] if tp_args else StringType()
        value_type = (
            python_type_to_snow_type(tp_args[1])[0] if tp_args else StringType()
        )
        return MapType(key_type, value_type), False

    if installed_pandas:
        pandas_series_tps = [PandasSeries, pandas.Series]
        if tp in pandas_series_tps or (tp_origin and tp_origin in pandas_series_tps):
            return (
                PandasSeriesType(
                    python_type_to_snow_type(tp_args[0])[0] if tp_args else None
                ),
                False,
            )

        pandas_dataframe_tps = [PandasDataFrame, pandas.DataFrame]
        if tp in pandas_dataframe_tps or (
            tp_origin and tp_origin in pandas_dataframe_tps
        ):
            return (
                PandasDataFrameType(
                    [python_type_to_snow_type(tp_arg)[0] for tp_arg in tp_args]
                    if tp_args
                    else ()
                ),
                False,
            )

    if tp == Variant:
        return VariantType(), False

    if tp == Geography:
        return GeographyType(), False

    raise TypeError(f"invalid type {tp}")


def retrieve_func_type_hints_from_source(
    file_path: str,
    func_name: str,
    class_name: Optional[str] = None,
    _source: Optional[str] = None,
) -> Dict[str, str]:
    """
    Retrieve type hints of a function from a source file, or a source string (test only).
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
            raise ValueError(f"class {class_name} is not found in file {file_path}")
        to_visit_node_for_func = class_visitor.class_node
    else:
        to_visit_node_for_func = ast.parse(_source)

    visitor = FuncNodeVisitor()
    visitor.visit(to_visit_node_for_func)
    if not visitor.func_exist:
        raise ValueError(
            f"function {class_name if class_name else ''}{'.' if class_name else ''}{func_name} is not found in file {file_path}"
        )
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
DATA_TYPE_STRING_OBJECT_MAPPINGS["object"] = MapType
DATA_TYPE_STRING_OBJECT_MAPPINGS["array"] = ArrayType

DECIMAL_RE = re.compile(
    r"^\s*(numeric|number|decimal)\s*\(\s*(\s*)(\d*)\s*,\s*(\d*)\s*\)\s*$"
)
# support type string format like "  decimal  (  2  ,  1  )  "


def get_number_precision_scale(type_str: str) -> Optional[Tuple[int, int]]:
    decimal_matches = DECIMAL_RE.match(type_str)
    if decimal_matches:
        return int(decimal_matches.group(3)), int(decimal_matches.group(4))


def type_string_to_type_object(type_str: str) -> DataType:
    precision_scale = get_number_precision_scale(type_str)
    if precision_scale:
        return DecimalType(*precision_scale)
    type_str = type_str.replace(" ", "")
    type_str = type_str.lower()
    try:
        return DATA_TYPE_STRING_OBJECT_MAPPINGS[type_str]()
    except KeyError:
        raise ValueError(f"'{type_str}' is not a supported type")


# Type hints
ColumnOrName = NewType("ColumnOrName", Union["snowflake.snowpark.column.Column", str])
ColumnOrLiteralStr = NewType(
    "ColumnOrLiteralStr", Union["snowflake.snowpark.column.Column", str]
)
ColumnOrSqlExpr = NewType(
    "ColumnOrSqlExpr", Union["snowflake.snowpark.column.Column", str]
)
LiteralType = NewType("LiteralType", Union[VALID_PYTHON_TYPES_FOR_LITERAL_VALUE])
ColumnOrLiteral = NewType(
    "ColumnOrLiteral", Union["snowflake.snowpark.column.Column", LiteralType]
)
