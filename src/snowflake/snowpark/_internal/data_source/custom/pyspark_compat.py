#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""Interoperability with data sources written against ``pyspark.sql.datasource``.

Snowpark doesn't depend on PySpark. PySpark objects are recognized by duck typing, and schemas
are converted through Spark's JSON schema format, which only needs the ``pyspark.sql.types``
module that the user's data source has already imported."""
import re
import sys
from typing import Any, Dict, Iterable, Optional

from snowflake.snowpark._internal.analyzer.analyzer_utils import unquote_if_quoted
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
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampTimeZone,
    TimestampType,
    TimeType,
    VariantType,
)

PYSPARK_PACKAGE = "pyspark"
_PYSPARK_TYPES_MODULE = "pyspark.sql.types"

_SPARK_SIMPLE_TYPES = {
    "string": StringType,
    "binary": BinaryType,
    "boolean": BooleanType,
    "date": DateType,
    "double": DoubleType,
    "float": FloatType,
    "byte": ByteType,
    "short": ShortType,
    "integer": IntegerType,
    "long": LongType,
    "variant": VariantType,
}
_SPARK_DECIMAL = re.compile(r"decimal\(\s*(\d+)\s*,\s*(-?\d+)\s*\)")
_SPARK_CHAR = re.compile(r"(?:var)?char\(\s*(\d+)\s*\)")
_SPARK_TIME = re.compile(r"time(?:\(\s*\d+\s*\))?")


def _is_pyspark_module(module_name: Optional[str]) -> bool:
    return isinstance(module_name, str) and (
        module_name == PYSPARK_PACKAGE or module_name.startswith(PYSPARK_PACKAGE + ".")
    )


def is_pyspark_class(cls: Any) -> bool:
    """Whether ``cls`` is a PySpark class or a subclass of one, checked without importing PySpark."""
    return isinstance(cls, type) and any(
        _is_pyspark_module(c.__module__) for c in cls.__mro__
    )


def uses_pyspark(objects: Iterable[Any]) -> bool:
    """Whether any of ``objects`` is an instance of a PySpark class or of a subclass of one."""
    return any(is_pyspark_class(type(obj)) for obj in objects)


def is_pyspark_struct_type(obj: Any) -> bool:
    return (
        _is_pyspark_module(type(obj).__module__)
        and type(obj).__name__ == "StructType"
        and callable(getattr(obj, "jsonValue", None))
    )


def _unsupported(spark_type: Any) -> NotImplementedError:
    return NotImplementedError(
        f"Spark type {spark_type!r} is not supported by Snowpark custom data sources."
    )


def spark_json_to_snowpark_type(spark_type: Any) -> DataType:
    """Converts a type in Spark's JSON schema format (``DataType.jsonValue()``) to a Snowpark type."""
    if isinstance(spark_type, str):
        name = spark_type.strip().lower()
        if name in _SPARK_SIMPLE_TYPES:
            return _SPARK_SIMPLE_TYPES[name]()
        if name == "timestamp":
            # Spark's TIMESTAMP has local time zone semantics
            return TimestampType(TimestampTimeZone.LTZ)
        if name == "timestamp_ntz":
            return TimestampType(TimestampTimeZone.NTZ)
        if name == "decimal":
            return DecimalType(10, 0)
        match = _SPARK_DECIMAL.fullmatch(name)
        if match:
            return DecimalType(int(match.group(1)), int(match.group(2)))
        match = _SPARK_CHAR.fullmatch(name)
        if match:
            return StringType(int(match.group(1)))
        if _SPARK_TIME.fullmatch(name):
            return TimeType()
        raise _unsupported(spark_type)
    if isinstance(spark_type, dict):
        kind = spark_type.get("type")
        if kind == "struct":
            return spark_json_to_snowpark_schema(spark_type)
        if kind == "array":
            return ArrayType(
                spark_json_to_snowpark_type(spark_type["elementType"]),
                contains_null=spark_type.get("containsNull", True),
            )
        if kind == "map":
            return MapType(
                spark_json_to_snowpark_type(spark_type["keyType"]),
                spark_json_to_snowpark_type(spark_type["valueType"]),
                value_contains_null=spark_type.get("valueContainsNull", True),
            )
        if kind == "udt" and "sqlType" in spark_type:
            return spark_json_to_snowpark_type(spark_type["sqlType"])
    raise _unsupported(spark_type)


def spark_json_to_snowpark_schema(spark_schema: Dict[str, Any]) -> StructType:
    if not isinstance(spark_schema, dict) or spark_schema.get("type") != "struct":
        raise TypeError(f"Expected a Spark struct type, got {spark_schema!r}.")
    # field names keep their original spelling in StructField._name and are matched
    # case-insensitively unless they need quoting, like unquoted names in Spark
    return StructType(
        [
            StructField(
                field["name"],
                spark_json_to_snowpark_type(field["type"]),
                field.get("nullable", True),
            )
            for field in spark_schema.get("fields", [])
        ]
    )


def pyspark_struct_type_to_snowpark(schema: Any) -> StructType:
    return spark_json_to_snowpark_schema(schema.jsonValue())


def _field_name(field: StructField) -> str:
    # the name as written by the user, e.g. "userId" instead of USERID
    return unquote_if_quoted(field._name) if field._name else field.name


def snowpark_type_to_spark_json(datatype: DataType) -> Any:
    """Converts a Snowpark type to Spark's JSON schema format."""
    if isinstance(datatype, StructType):
        return {
            "type": "struct",
            "fields": [
                {
                    "name": _field_name(field),
                    "type": snowpark_type_to_spark_json(field.datatype),
                    "nullable": field.nullable,
                    "metadata": {},
                }
                for field in datatype.fields
            ],
        }
    if isinstance(datatype, ArrayType):
        element_type = datatype.element_type or StringType()
        return {
            "type": "array",
            "elementType": snowpark_type_to_spark_json(element_type),
            "containsNull": getattr(datatype, "contains_null", True),
        }
    if isinstance(datatype, MapType):
        return {
            "type": "map",
            "keyType": snowpark_type_to_spark_json(datatype.key_type or StringType()),
            "valueType": snowpark_type_to_spark_json(
                datatype.value_type or StringType()
            ),
            "valueContainsNull": getattr(datatype, "value_contains_null", True),
        }
    if isinstance(datatype, DecimalType):
        return f"decimal({datatype.precision},{datatype.scale})"
    if isinstance(datatype, TimestampType):
        return "timestamp_ntz" if datatype.tz == TimestampTimeZone.NTZ else "timestamp"
    if isinstance(datatype, TimeType):
        return "time(6)"
    for name, snowpark_type in _SPARK_SIMPLE_TYPES.items():
        if type(datatype) is snowpark_type:
            return name
    raise NotImplementedError(
        f"Snowpark type {datatype} can't be converted to a Spark type."
    )


def to_pyspark_struct_type(schema: StructType) -> Any:
    """Converts a Snowpark schema to a ``pyspark.sql.types.StructType``, using the PySpark
    module already imported by the data source."""
    types_module = sys.modules.get(_PYSPARK_TYPES_MODULE)
    if types_module is None:  # pragma: no cover
        raise ImportError(
            "pyspark.sql.types must be imported to read a data source written with PySpark."
        )
    try:
        return types_module.StructType.fromJson(snowpark_type_to_spark_json(schema))
    except NotImplementedError:
        raise
    except Exception as exc:
        raise NotImplementedError(
            f"The schema {schema} can't be converted to a PySpark schema with PySpark "
            f"{getattr(sys.modules.get(PYSPARK_PACKAGE), '__version__', '')}: {exc!r}"
        ) from exc


def with_spark_timestamp_semantics(schema: StructType) -> StructType:
    """Spark's TIMESTAMP has local time zone semantics, so a plain TIMESTAMP in the DDL string
    of a PySpark data source is read as TIMESTAMP_LTZ."""

    def convert(datatype: DataType) -> DataType:
        if (
            isinstance(datatype, TimestampType)
            and datatype.tz == TimestampTimeZone.DEFAULT
        ):
            return TimestampType(TimestampTimeZone.LTZ)
        return datatype

    return StructType(
        [StructField(f._name, convert(f.datatype), f.nullable) for f in schema.fields]
    )
