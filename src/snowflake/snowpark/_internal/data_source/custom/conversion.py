#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""Converts the rows and Arrow data yielded by ``DataSourceReader.read()`` into the
format staged for ``COPY INTO``, or into Python rows."""

import datetime
import decimal
import json
import numbers
from io import BytesIO
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
)

from snowflake.snowpark._internal.analyzer.analyzer_utils import unquote_if_quoted
from snowflake.snowpark._internal.utils import PythonObjJSONEncoder
from snowflake.snowpark.exceptions import SnowparkDataframeReaderException
from snowflake.snowpark.row import Row
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    TimeType,
    VariantType,
    _FractionalType,
    _IntegralType,
)

if TYPE_CHECKING:
    import pyarrow

    import snowflake.snowpark

_SEMI_STRUCTURED_TYPES = (VariantType, ArrayType, MapType, StructType)
_SUPPORTED_TYPES = (
    _IntegralType,
    _FractionalType,
    StringType,
    BinaryType,
    BooleanType,
    DateType,
    TimeType,
    TimestampType,
) + _SEMI_STRUCTURED_TYPES


def validate_schema(schema: StructType) -> None:
    from snowflake.snowpark.types import DecFloatType

    if not schema.fields:
        raise ValueError(
            "The schema of a custom data source must have at least one field."
        )
    for field in schema.fields:
        if not isinstance(field.datatype, _SUPPORTED_TYPES) or isinstance(
            field.datatype, DecFloatType
        ):
            raise NotImplementedError(
                f"Column {field.name} has type {field.datatype}, which custom data sources don't support yet."
            )


def is_semi_structured(datatype: DataType) -> bool:
    return isinstance(datatype, _SEMI_STRUCTURED_TYPES)


def staging_schema(schema: StructType) -> StructType:
    """The schema of the temporary table that staged Parquet files are copied into.
    Semi-structured columns are staged as JSON strings and parsed when the table is read."""
    return StructType(
        [
            StructField(
                field.name,
                StringType() if is_semi_structured(field.datatype) else field.datatype,
                field.nullable,
            )
            for field in schema.fields
        ]
    )


def value_converters(schema: StructType) -> List[Optional[Callable[[Any], Any]]]:
    """For each schema field, the converter for the Python values of integer, decimal, date and
    time columns, or ``None``. They match the conversions local ingestion applies when it stages
    the values, and are used by local testing to return the same values."""
    return [_value_converter(field.datatype) for field in schema.fields]


def _value_converter(datatype: DataType) -> Optional[Callable[[Any], Any]]:
    if isinstance(datatype, _IntegralType):
        return _to_integer
    if isinstance(datatype, DecimalType):
        return _decimal_converter(datatype.scale)
    if isinstance(datatype, _FractionalType):
        return _to_float
    if isinstance(datatype, DateType):
        return _to_date
    if isinstance(datatype, TimeType):
        return _to_time
    if isinstance(datatype, BooleanType):
        return _to_bool
    if is_semi_structured(datatype):
        return _to_json_compatible
    return None


def check_not_null(row: Tuple, matcher: "FieldMatcher") -> None:
    for value, field in zip(row, matcher.fields):
        if value is None and not field.nullable:
            raise ValueError(f"NULL value in the NOT NULL column {field.name}")


def convert_row(
    row: Tuple, converters: Sequence[Optional[Callable[[Any], Any]]]
) -> Tuple:
    return tuple(
        value if converter is None or value is None else converter(value)
        for value, converter in zip(row, converters)
    )


def is_arrow_data(item: Any) -> bool:
    """Whether ``item`` is a ``pyarrow.RecordBatch`` or ``pyarrow.Table``, checked
    without importing pyarrow. A ``pyarrow.Array`` is not Arrow data, because it has no columns.
    """
    return (
        type(item).__module__.startswith("pyarrow")
        and hasattr(item, "to_pylist")
        and hasattr(item, "column")
    )


class FieldMatcher:
    """Matches input names (dict keys, Row fields, Arrow column names) to schema fields.
    Unquoted schema field names match case-insensitively; quoted names match exactly."""

    def __init__(self, schema: StructType) -> None:
        self.fields = schema.fields
        self.names = [unquote_if_quoted(f.name) for f in schema.fields]
        self._case_insensitive = [not f.name.startswith('"') for f in schema.fields]

    def match(self, input_names: Sequence[str]) -> List[Optional[int]]:
        """Returns, for each schema field, the index of the matching input name or ``None``."""
        exact = {}
        lower = {}
        for i, name in enumerate(input_names):
            if not isinstance(name, str):
                raise TypeError(
                    f"The names of the values yielded by DataSourceReader.read() must be strings, "
                    f"got {type(name).__name__}: {name!r}"
                )
            exact.setdefault(name, i)
            lower.setdefault(name.lower(), i)
        result = []
        for name, case_insensitive in zip(self.names, self._case_insensitive):
            idx = exact.get(name)
            if idx is None and case_insensitive:
                idx = lower.get(name.lower())
            result.append(idx)
        return result


def to_row_tuple(item: Any, matcher: FieldMatcher) -> Tuple:
    """Converts one item yielded by ``read()`` (other than Arrow data) into a tuple in schema order."""
    num_fields = len(matcher.fields)
    if isinstance(item, Row):
        if item._fields:
            return _match_named_values(list(item._fields), list(item), matcher, True)
        values = tuple(item)
    elif isinstance(item, dict):
        return _match_named_values(
            list(item.keys()), list(item.values()), matcher, False
        )
    elif isinstance(item, (tuple, list)):
        values = tuple(item)
    else:
        hint = (
            " Wrap a pyarrow.Array into a pyarrow.RecordBatch or pyarrow.Table."
            if type(item).__module__.startswith("pyarrow")
            else ""
        )
        raise TypeError(
            "DataSourceReader.read() must yield tuples, lists, Rows, dicts, pyarrow.RecordBatch "
            f"or pyarrow.Table objects, got {type(item).__name__}: {item!r}.{hint}"
        )
    if len(values) != num_fields:
        raise ValueError(
            f"DataSourceReader.read() yielded a row with {len(values)} values, but the schema "
            f"has {num_fields} fields: {item!r}"
        )
    return values


def _match_named_values(
    names: List[str], values: List[Any], matcher: FieldMatcher, strict: bool
) -> Tuple:
    indices = matcher.match(names)
    if strict:
        missing = [n for n, i in zip(matcher.names, indices) if i is None]
        if missing:
            raise ValueError(
                f"DataSourceReader.read() yielded a row without the schema fields {missing}: "
                f"{dict(zip(names, values))!r}"
            )
    return tuple(None if i is None else values[i] for i in indices)


def _to_json_compatible(value: Any) -> Any:
    # json encodes tuples (and so Rows) as arrays without calling the encoder, so Rows with
    # field names are converted to dicts first
    if isinstance(value, Row):
        if value._fields:
            return {k: _to_json_compatible(v) for k, v in zip(value._fields, value)}
        return [_to_json_compatible(v) for v in value]
    if isinstance(value, dict):
        return {k: _to_json_compatible(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_json_compatible(v) for v in value]
    return value


def _json_dumps(value: Any) -> Optional[str]:
    value = _to_json_compatible(value)
    if value is None:
        return None
    return json.dumps(value, cls=PythonObjJSONEncoder)


def _to_date(value: Any) -> Any:
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, str):
        return datetime.date.fromisoformat(value)
    return value


def _to_time(value: Any) -> Any:
    if isinstance(value, str):
        return datetime.time.fromisoformat(value)
    return value


def _to_timestamp_string(value: Any) -> Any:
    # timestamps are staged as ISO strings so that COPY INTO keeps the time zone offset for
    # TIMESTAMP_TZ and interprets naive values in the session time zone for TIMESTAMP_LTZ
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    return value


def _decimal_converter(scale: int) -> Callable[[Any], Any]:
    quantum = decimal.Decimal(1).scaleb(-scale)

    def convert(value: Any) -> Any:
        if isinstance(value, (float, str)):
            value = decimal.Decimal(str(value))
        if isinstance(value, decimal.Decimal):
            # the default precision of 28 digits is less than the 38 digits of a Snowflake decimal
            with decimal.localcontext() as ctx:
                ctx.prec = decimal.MAX_PREC
                return value.quantize(quantum, rounding=decimal.ROUND_HALF_UP)
        return value

    return convert


def _to_integer(value: Any) -> Any:
    # pyarrow silently truncates fractions like 1.7 when it converts them to integers, and
    # rejects booleans and strings
    if isinstance(value, (str, bytes)):
        raise ValueError(f"{value!r} is not an integer")
    if isinstance(value, int):  # includes bool
        return int(value)
    if isinstance(value, (decimal.Decimal, numbers.Real)):
        try:
            integral = value == int(value)  # int() raises for NaN and infinity
        except (ValueError, OverflowError):
            integral = False
        if not integral:
            raise ValueError(f"{value!r} is not an integer")
        return int(value)
    return value


def _to_bool(value: Any) -> Any:
    # pyarrow rejects integers and strings for booleans
    if isinstance(value, bool) or type(value).__name__ in ("bool_", "bool"):
        return bool(value)
    raise ValueError(f"{value!r} is not a boolean")


def _to_float(value: Any) -> Any:
    # pyarrow rejects decimals and strings
    if isinstance(value, (str, bytes)):
        raise ValueError(f"{value!r} is not a number")
    if isinstance(value, (decimal.Decimal, numbers.Real)) and not isinstance(
        value, float
    ):
        return float(value)
    return value


def _arrow_type_and_converter(
    datatype: DataType,
) -> Tuple["pyarrow.DataType", Optional[Callable[[Any], Any]]]:
    import pyarrow as pa

    if isinstance(datatype, _IntegralType):
        return pa.int64(), _to_integer
    if isinstance(datatype, DecimalType):
        return pa.decimal128(datatype.precision, datatype.scale), _decimal_converter(
            datatype.scale
        )
    if isinstance(datatype, _FractionalType):
        return pa.float64(), _to_float
    if isinstance(datatype, StringType):
        return pa.string(), None
    if isinstance(datatype, BinaryType):
        return pa.binary(), None
    if isinstance(datatype, BooleanType):
        return pa.bool_(), _to_bool
    if isinstance(datatype, DateType):
        return pa.date32(), _to_date
    if isinstance(datatype, TimeType):
        return pa.time64("us"), _to_time
    if isinstance(datatype, TimestampType):
        return pa.string(), _to_timestamp_string
    if is_semi_structured(datatype):
        return pa.string(), _json_dumps
    raise NotImplementedError(f"Unsupported type {datatype}")  # pragma: no cover


def _conversion_error(
    field: StructField, partition_idx: int, exc: Exception
) -> SnowparkDataframeReaderException:
    return SnowparkDataframeReaderException(
        f"Failed to convert the values of column {field.name} ({field.datatype}) read from "
        f"partition {partition_idx}: {exc!r}"
    )


def rows_to_arrow_table(
    rows: List[Tuple], matcher: FieldMatcher, partition_idx: int
) -> "pyarrow.Table":
    """Converts rows in schema order into an Arrow table with the staging column types."""
    import pyarrow as pa

    arrays = []
    for col_idx, field in enumerate(matcher.fields):
        arrow_type, converter = _arrow_type_and_converter(field.datatype)
        try:
            values = [row[col_idx] for row in rows]
            if not field.nullable and any(v is None for v in values):
                raise ValueError("NULL values in a NOT NULL column")
            if converter is not None:
                values = [None if v is None else converter(v) for v in values]
            arrays.append(pa.array(values, type=arrow_type))
        except Exception as exc:
            raise _conversion_error(field, partition_idx, exc) from exc
    return pa.Table.from_arrays(arrays, names=matcher.names)


def _arrow_column_indices(column_names: List[str], matcher: FieldMatcher) -> List[int]:
    indices = matcher.match(column_names)
    if all(i is not None for i in indices):
        return indices
    # columns are matched by position only if none of the names match
    if all(i is None for i in indices) and len(column_names) == len(matcher.fields):
        return list(range(len(column_names)))
    missing = [n for n, i in zip(matcher.names, indices) if i is None]
    raise SnowparkDataframeReaderException(
        f"DataSourceReader.read() yielded Arrow data with columns {column_names}, which don't "
        f"include the schema fields {missing}."
    )


def _arrow_column_to_pylist(column: Any) -> List[Any]:
    """Like ``to_pylist()``, but converts Arrow map values to dicts instead of lists of tuples."""
    import pyarrow as pa

    def has_map(arrow_type: "pyarrow.DataType") -> bool:
        if pa.types.is_map(arrow_type):
            return True
        if pa.types.is_struct(arrow_type):
            return any(
                has_map(arrow_type.field(i).type) for i in range(arrow_type.num_fields)
            )
        if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
            return has_map(arrow_type.value_type)
        return False

    def convert(value: Any, arrow_type: "pyarrow.DataType") -> Any:
        if value is None:
            return None
        if pa.types.is_map(arrow_type):
            return {k: convert(v, arrow_type.item_type) for k, v in value}
        if pa.types.is_struct(arrow_type):
            return {
                f.name: convert(value[f.name], f.type)
                for f in (arrow_type.field(i) for i in range(arrow_type.num_fields))
            }
        if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
            return [convert(v, arrow_type.value_type) for v in value]
        return value

    values = column.to_pylist()
    if not has_map(column.type):
        return values
    return [convert(v, column.type) for v in values]


def _check_arrow_source_type(
    arrow_type: "pyarrow.DataType", datatype: DataType
) -> None:
    """Rejects the conversions that rows don't support, instead of letting ``cast`` accept them,
    for example integers as timestamps, or strings as numbers."""
    import pyarrow as pa

    is_text = (
        pa.types.is_string(arrow_type)
        or pa.types.is_large_string(arrow_type)
        or pa.types.is_binary(arrow_type)
        or pa.types.is_large_binary(arrow_type)
    )
    if isinstance(datatype, (TimestampType, DateType, TimeType)):
        rejected = pa.types.is_integer(arrow_type) or pa.types.is_floating(arrow_type)
    elif isinstance(datatype, BooleanType):
        rejected = not (pa.types.is_boolean(arrow_type) or pa.types.is_null(arrow_type))
    elif isinstance(datatype, (_IntegralType, _FractionalType)):
        # rows convert strings to decimals
        rejected = is_text and not isinstance(datatype, DecimalType)
    else:
        rejected = False
    if rejected:
        raise ValueError(f"Arrow type {arrow_type} can't be converted to {datatype}")


def arrow_to_staged_table(
    data: Any, matcher: FieldMatcher, partition_idx: int
) -> "pyarrow.Table":
    """Converts a ``pyarrow.RecordBatch`` or ``pyarrow.Table`` into an Arrow table with the
    staging column types."""
    import pyarrow as pa

    indices = _arrow_column_indices(list(data.schema.names), matcher)
    arrays = []
    for field, col_idx in zip(matcher.fields, indices):
        column = data.column(col_idx)
        try:
            if not field.nullable and column.null_count:
                raise ValueError("NULL values in a NOT NULL column")
            _check_arrow_source_type(column.type, field.datatype)
            if is_semi_structured(field.datatype):
                column = pa.array(
                    [_json_dumps(v) for v in _arrow_column_to_pylist(column)],
                    type=pa.string(),
                )
            elif isinstance(field.datatype, TimestampType):
                # staged as ISO strings like the timestamps of rows, to keep the time zone offset
                if not (
                    pa.types.is_string(column.type)
                    or pa.types.is_large_string(column.type)
                ):
                    if not pa.types.is_timestamp(column.type):
                        column = column.cast(pa.timestamp("us"))
                    column = pa.array(
                        [
                            None if v is None else _to_timestamp_string(v)
                            for v in column.to_pylist()
                        ],
                        type=pa.string(),
                    )
            else:
                arrow_type, converter = _arrow_type_and_converter(field.datatype)
                if column.type != arrow_type:
                    if isinstance(field.datatype, DecimalType):
                        # rounds half-up like the decimals of rows, instead of raising or truncating
                        column = pa.array(
                            [
                                None if v is None else converter(v)
                                for v in column.to_pylist()
                            ],
                            type=arrow_type,
                        )
                    else:
                        column = column.cast(arrow_type)
        except Exception as exc:
            raise _conversion_error(field, partition_idx, exc) from exc
        arrays.append(column)
    return pa.Table.from_arrays(arrays, names=matcher.names)


def arrow_table_to_parquet_bytesio(table: "pyarrow.Table") -> Optional[BytesIO]:
    import pyarrow.parquet as pq

    if table.num_rows == 0:
        return None
    buffer = BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)
    return buffer


def arrow_to_row_tuples(data: Any, matcher: FieldMatcher) -> List[Tuple]:
    """Converts a ``pyarrow.RecordBatch`` or ``pyarrow.Table`` into rows in schema order."""
    indices = _arrow_column_indices(list(data.schema.names), matcher)
    columns = [_arrow_column_to_pylist(data.column(i)) for i in indices]
    return list(zip(*columns))


def normalize_items(items: Iterable[Any], matcher: FieldMatcher) -> Iterator[Tuple]:
    """Converts everything yielded by ``read()`` into rows in schema order."""
    for item in items:
        if is_arrow_data(item):
            yield from arrow_to_row_tuples(item, matcher)
        else:
            yield to_row_tuple(item, matcher)


def parse_semi_structured_columns(
    df: "snowflake.snowpark.DataFrame", schema: StructType, _emit_ast: bool = False
) -> "snowflake.snowpark.DataFrame":
    """Projects the staged table back to the target schema, parsing semi-structured JSON columns."""
    from snowflake.snowpark.functions import col, parse_json

    if not any(is_semi_structured(f.datatype) for f in schema.fields):
        return df
    columns = []
    for field in schema.fields:
        column = col(field.name)
        if isinstance(field.datatype, VariantType):
            column = parse_json(column)
        elif is_semi_structured(field.datatype):
            column = parse_json(column).cast(field.datatype)
        columns.append(column.alias(field.name))
    return df.select(columns, _emit_ast=_emit_ast)
