#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""Schemas written the Spark way: DDL strings with backtick-quoted names, and
``pyspark.sql.types.StructType`` objects, which are converted through Spark's JSON schema format.

Snowpark doesn't depend on PySpark. PySpark objects are recognized by duck typing, and only
their ``jsonValue()`` method is used."""
import re
from typing import Any, Dict, Optional

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


_SIMPLE_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*$")


def spark_ddl_identifiers_to_snowflake(ddl: str) -> str:
    """Rewrites the backtick-quoted identifiers of a Spark DDL string, such as
    ``"id INT, `my col` STRING"``. A name that is a valid unquoted identifier is unquoted, like
    in Spark where backticks only escape. Other names become double-quoted identifiers followed
    by a colon, which the schema string parser accepts for names with spaces. Quoted strings
    are left unchanged."""
    result = []
    i = 0
    while i < len(ddl):
        ch = ddl[i]
        if ch == '"' or (
            ch == "'" and re.search(r"(?i)(?:^|\s)comment\s*$", "".join(result))
        ):
            # copy a quoted string, where a quote is escaped by doubling it (or by a backslash)
            j = i + 1
            while j < len(ddl):
                if ddl[j] == "\\" and ch == "'":
                    j += 2
                elif ddl[j] == ch:
                    if ddl[j + 1 : j + 2] == ch:
                        j += 2
                    else:
                        break
                else:
                    j += 1
            result.append(ddl[i : j + 1])
            i = j + 1
        elif ch == "`":
            name = []
            j = i + 1
            while j < len(ddl):
                if ddl[j] == "`":
                    if ddl[j + 1 : j + 2] == "`":  # an escaped backtick
                        name.append("`")
                        j += 2
                        continue
                    break
                name.append(ddl[j])
                j += 1
            if j >= len(ddl):
                raise ValueError(
                    f"Unterminated backtick-quoted identifier in the schema: {ddl!r}"
                )
            name = "".join(name)
            i = j + 1
            if _SIMPLE_IDENTIFIER.match(name):
                # backticks only escape in Spark, so this is the same as the unquoted name
                result.append(name)
                if ddl[i : i + 1] not in ("", ":") and not ddl[i : i + 1].isspace():
                    result.append(" ")
            else:
                result.append('"' + name.replace('"', '""') + '"')
                if not ddl[i:].lstrip().startswith(":"):
                    result.append(":")
        else:
            result.append(ch)
            i += 1
    return "".join(result)


def unquote_field_names(datatype: DataType) -> None:
    """Replaces the quoted names of the fields of a schema parsed from a DDL string, such as
    ``'"my col"'``, by the names themselves, in place and in nested types. Readers expect
    ``StructField._name`` to be the name as it is in the data, like in
    ``StructField("my col", ...)``, and quote it themselves. ``StructField.name`` still
    follows the quoted identifier, so a quoted name stays case-sensitive."""
    if isinstance(datatype, StructType):
        for field in datatype.fields:
            field._name = unquote_if_quoted(field._name)
            unquote_field_names(field.datatype)
    elif isinstance(datatype, ArrayType):
        if datatype.element_type is not None:
            unquote_field_names(datatype.element_type)
    elif isinstance(datatype, MapType):
        for nested in (datatype.key_type, datatype.value_type):
            if nested is not None:
                unquote_field_names(nested)
