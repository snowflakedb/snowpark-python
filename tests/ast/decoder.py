#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import logging
from typing import Any, Optional, Iterable, List, Union, Dict, Tuple
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

import snowflake.snowpark._internal.proto.generated.ast_pb2 as proto

from google.protobuf.json_format import MessageToDict

from snowflake.snowpark import Session
import snowflake.snowpark.functions
from snowflake.snowpark.types import (
    DataType,
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    ColumnIdentifier,
    DateType,
    DoubleType,
    FloatType,
    GeographyType,
    GeometryType,
    IntegerType,
    LongType,
    NullType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimeType,
    VariantType,
    VectorType,
    DecimalType,
    MapType,
    PandasDataFrameType,
    PandasSeriesType,
    TimestampTimeZone,
    TimestampType,
)

logger = logging.getLogger(__name__)


class Decoder:
    def __init__(self, session: Optional[Session]):
        # Map from var_id to (symbol_name, value). symbol_name is the identifier used in the program to store value.
        self.symbol_table: Dict[int, Tuple[str, object]] = dict()
        try:
            self.session = session if session is not None else Session.builder.create()
        except Exception as e:
            self.session = None
            logger.warning("Error creating a Snowpark session for the decoder: %s", e)

    def capture_local_variable_name(self, assign_expr: proto.Assign) -> str:
        """
        Capture the local variable name from an assign expression.

        Parameters
        ----------
        assign_expr : proto.Assign
            The assign expression to capture the local variable name from.

        Returns
        -------
        str
            The local variable name.
        """
        return assign_expr.symbol.value

    def decode_dsl_map_expr(self, map_expr: Iterable) -> dict:
        """
        Given a map expression, return the result as a Python dictionary.
        Under the hood, protoc converts the key-value pairs into a list of Tuple_X_Y.

        Parameters
        ----------
        map_expr : Iterable[proto.Tuple_X_Y]
            The map expression to decode.

        Returns
        -------
        dict
            The decoded Python dictionary.
        """
        python_map = dict()
        for pair in map_expr:
            key = (
                self.decode_expr(pair._1)
                if isinstance(pair._1, proto.Expr)
                else pair._1
            )
            value = (
                self.decode_expr(pair._2)
                if isinstance(pair._2, proto.Expr)
                else pair._2
            )
            python_map[key] = value
        return python_map

    def decode_fn_name_expr(self, fn_name: proto.FnName) -> str:
        """
        Decode a function name expression to get the function name.

        Parameters
        ----------
        fn_name : proto.FnName
            The function name to decode.

        Returns
        -------
        str
            The decoded function name.
        """
        if hasattr(fn_name, "fn_name_flat"):
            return fn_name.fn_name_flat.name
        elif hasattr(fn_name, "fn_name_structured"):
            return fn_name.fn_name_structured.name
        else:
            raise ValueError("Function name not found in proto.FnName")

    def decode_table_name_expr(self, table_name: proto.SpTableName) -> str:
        """
        Decode a table name expression to get the table name.

        Parameters
        ----------
        table_name : proto.SpTableName
            The table name to decode.

        Returns
        -------
        str
            The decoded table name.
        """
        if hasattr(table_name, "sp_table_name_flat"):
            return table_name.sp_table_name_flat.name
        elif hasattr(table_name, "sp_table_name_structured"):
            return table_name.sp_table_name_structured.name
        else:
            raise ValueError("Table name not found in proto.SpTableName")

    def decode_fn_ref_expr(self, fn_ref_expr: proto.FnRefExpr) -> str:
        """
        Decode a function reference expression to get the function name.

        Parameters
        ----------
        expr : proto.FnRefExpr
            The expression to decode.

        Returns
        -------
        str
            The decoded function name.
        """
        match fn_ref_expr.WhichOneof("variant"):
            # case "trait_fn_id_ref_expr":
            #     pass
            # case "trait_fn_name_ref_expr":
            #     pass
            case "builtin_fn":
                return self.decode_fn_name_expr(fn_ref_expr.builtin_fn.name)
            # case "call_table_function_expr":
            #     pass
            # case "indirect_table_fn_id_ref":
            #     pass
            # case "indirect_table_fn_name_ref":
            #     pass
            # case "sp_fn_ref":
            #     pass
            # case "stored_procedure":
            #     pass
            # case "udaf":
            #     pass
            # case "udf":
            #     pass
            # case "udtf":
            #     pass
            case _:
                raise ValueError(
                    "Unknown function reference type: %s"
                    % fn_ref_expr.WhichOneof("variant")
                )

    def decode_dataframe_data_expr(self, df_data_expr: proto.SpDataframeData) -> List:
        """
        Decode a dataframe data expression to get the underlying data.

        Parameters
        ----------
        df_data_expr : proto.SpDataframeData
            The expr to decode.

        Returns
        -------
        List
            The decoded data.
        """
        match df_data_expr.WhichOneof("sealed_value"):
            case "sp_dataframe_data__list":
                # vs can be a list of Expr, a single Expr, or [].
                if hasattr(df_data_expr.sp_dataframe_data__list, "vs"):
                    if isinstance(df_data_expr.sp_dataframe_data__list.vs, Iterable):
                        return [
                            self.decode_expr(v)
                            for v in df_data_expr.sp_dataframe_data__list.vs
                        ]
                    else:
                        return [
                            self.decode_expr(df_data_expr.sp_dataframe_data__list.vs)
                        ]
                else:
                    return []
            # case "sp_dataframe_data__pandas":
            #     pass
            # case "sp_dataframe_data__tuple":
            #     pass
            case _:
                raise ValueError(
                    "Unknown dataframe data type: %s"
                    % df_data_expr.WhichOneof("variant")
                )

    def decode_dataframe_schema_expr(
        self, df_schema_expr: proto.SpDataframeSchema
    ) -> Union[List, None]:
        """
        Decode a dataframe schema expression to get the schema.

        Parameters
        ----------
        df_schema_expr : proto.SpDataframeSchema
            The expr to decode.

        Returns
        -------
        List
            The decoded schema.
        """
        match df_schema_expr.WhichOneof("sealed_value"):
            case "sp_dataframe_schema__list":
                # vs can be a list of Expr, a single Expr, or None.
                if hasattr(df_schema_expr.sp_dataframe_schema__list, "vs"):
                    if isinstance(
                        df_schema_expr.sp_dataframe_schema__list.vs, Iterable
                    ):
                        return [v for v in df_schema_expr.sp_dataframe_schema__list.vs]
                    else:
                        return [df_schema_expr.sp_dataframe_schema__list.vs]
                else:
                    return None
            # case "sp_dataframe_schema__struct":
            #     pass
            case _:
                raise ValueError(
                    "Unknown dataframe schema type: %s"
                    % df_schema_expr.WhichOneof("variant")
                )

    def decode_data_type_expr(
        self, data_type_expr: proto.SpDataType
    ) -> Union[DataType, StructField, ColumnIdentifier]:
        """
        Decode a data type expression to get the data type.

        Parameters
        ----------
        data_type_expr : proto.SpDataType
            The expression to decode.

        Returns
        -------
        DataType, StructField, or ColumnIdentifier
            The decoded data type.
        """
        match data_type_expr.WhichOneof("variant"):
            case "sp_array_type":
                structured = data_type_expr.sp_array_type.structured
                element_type = self.decode_data_type_expr(
                    data_type_expr.sp_array_type.ty
                )
                return ArrayType(element_type, structured)
            case "sp_binary_type":
                return BinaryType()
            case "sp_boolean_type":
                return BooleanType()
            case "sp_byte_type":
                return ByteType()
            case "sp_column_identifier":
                name = data_type_expr.sp_column_identifier.name
                return ColumnIdentifier(name)
            case "sp_date_type":
                return DateType()
            case "sp_decimal_type":
                precision = data_type_expr.sp_decimal_type.precision
                scale = data_type_expr.sp_decimal_type.scale
                return DecimalType(precision, scale)
            case "sp_double_type":
                return DoubleType()
            case "sp_float_type":
                return FloatType()
            case "sp_geography_type":
                return GeographyType()
            case "sp_geometry_type":
                return GeometryType()
            case "sp_integer_type":
                return IntegerType()
            case "sp_long_type":
                return LongType()
            case "sp_map_type":
                key_type = self.decode_data_type_expr(data_type_expr.sp_map_type.key_ty)
                value_type = self.decode_data_type_expr(
                    data_type_expr.sp_map_type.value_ty
                )
                structured = data_type_expr.sp_map_type.structured
                return MapType(key_type, value_type, structured)
            case "sp_null_type":
                return NullType()
            case "sp_pandas_data_frame_type":
                # Both col_types and col_names can be a list of Expr or a single Expr.
                if isinstance(
                    data_type_expr.sp_pandas_data_frame_type.col_types, Iterable
                ):
                    col_types = [
                        col_name
                        for col_name in data_type_expr.sp_pandas_data_frame_type.col_types
                    ]
                else:
                    col_types = [data_type_expr.sp_pandas_data_frame_type.col_types]
                if isinstance(
                    data_type_expr.sp_pandas_data_frame_type.col_names, Iterable
                ):
                    col_names = [
                        col_name
                        for col_name in data_type_expr.sp_pandas_data_frame_type.col_names
                    ]
                else:
                    col_names = [data_type_expr.sp_pandas_data_frame_type.col_names]
                return PandasDataFrameType(col_types, col_names)
            case "sp_pandas_series_type":
                # element_type is an optional field.
                element_type = (
                    self.decode_data_type_expr(
                        data_type_expr.sp_pandas_series_type.el_ty
                    )
                    if data_type_expr.sp_pandas_series_type.HasField("el_ty")
                    else None
                )
                return PandasSeriesType(element_type)
            case "sp_short_type":
                return ShortType()
            case "sp_string_type":
                length = (
                    data_type_expr.sp_string_type.length
                    if data_type_expr.sp_string_type.HasField("length")
                    and isinstance(data_type_expr.sp_string_type.length, int)
                    else None
                )
                return StringType(length)
            case "sp_struct_field":
                column_identifier = self.decode_data_type_expr(
                    data_type_expr.sp_struct_field.column_identifier
                )
                data_type = self.decode_data_type_expr(
                    data_type_expr.sp_struct_field.data_type
                )
                nullable = data_type_expr.sp_struct_field.nullable
                return StructField(column_identifier, data_type, nullable)
            case "sp_struct_type":
                # The fields can be a list of Expr, a single Expr, or None.
                if hasattr(data_type_expr.sp_struct_type, "fields"):
                    if isinstance(data_type_expr.sp_struct_type.fields, Iterable):
                        fields = [
                            self.decode_data_type_expr(field)
                            for field in data_type_expr.sp_struct_type.fields
                        ]
                    else:
                        fields = [
                            self.decode_data_type_expr(
                                data_type_expr.sp_struct_type.fields
                            )
                        ]
                else:
                    fields = None
                structured = data_type_expr.sp_struct_type.structured
                return StructType(fields, structured)
            case "sp_time_type":
                return TimeType()
            case "sp_timestamp_type":
                match data_type_expr.sp_timestamp_type.time_zone.WhichOneof("variant"):
                    case "sp_timestamp_time_zone_default":
                        tz = TimestampTimeZone.DEFAULT
                    case "sp_timestamp_time_zone_ltz":
                        tz = TimestampTimeZone.LTZ
                    case "sp_timestamp_time_zone_ntz":
                        tz = TimestampTimeZone.NTZ
                    case "sp_timestamp_time_zone_tz":
                        tz = TimestampTimeZone.TZ
                    case _:
                        raise ValueError(
                            "Unknown timezone: %s"
                            % data_type_expr.sp_timestamp_type.time_zone.WhichOneof(
                                "variant"
                            )
                        )
                return TimestampType(tz)
            case "sp_variant_type":
                return VariantType()
            case "sp_vector_type":
                dimension = data_type_expr.sp_vector_type.dimension
                # element_type is encoded as a SpDataType but the input to VectorType is supposed to be a Python type.
                element_type = self.decode_data_type_expr(
                    data_type_expr.sp_vector_type.ty
                )
                if isinstance(element_type, IntegerType):
                    element_type = int
                elif isinstance(element_type, FloatType):
                    element_type = float
                else:
                    raise ValueError(
                        "VectorType does not support element type: %s" % element_type
                    )
                return VectorType(element_type, dimension)
            case _:
                raise ValueError(
                    "Unknown data type: %s" % data_type_expr.WhichOneof("variant")
                )

    def decode_timezone_expr(self, tz_expr: proto.PythonTimeZone) -> Any:
        """
        Decode a Python timezone expression to get the timezone.

        Parameters
        ----------
        tz_expr : proto.PythonTimeZone
            The expression to decode.
        """
        tz_name = tz_expr.name.value
        offset_seconds = tz_expr.offset_seconds
        return timezone(offset=timedelta(seconds=offset_seconds), name=tz_name)

    def binop(self, ast, fn):
        return fn(self.decode_expr(ast.lhs), self.decode_expr(ast.rhs))

    def bitop(self, ast, fn):
        lhs = self.decode_expr(ast.lhs)
        rhs = self.decode_expr(ast.rhs)
        return getattr(lhs, fn)(rhs)

    def get_statement_params(self, d: Dict):
        statement_params = {}
        statement_params_list = d.get("statementParams", [])
        for statement_params_list_map in statement_params_list:
            statement_params[
                statement_params_list_map["1"]
            ] = statement_params_list_map["2"]
        return statement_params

    def decode_expr(self, expr: proto.Expr) -> Any:
        match expr.WhichOneof("variant"):
            # COLUMN BINARY OPERATIONS
            case "add":
                lhs = self.decode_expr(expr.add.lhs)
                rhs = self.decode_expr(expr.add.rhs)
                return lhs + rhs

            case "apply_expr":
                fn_name = self.decode_fn_ref_expr(expr.apply_expr.fn)
                fn = getattr(snowflake.snowpark.functions, fn_name)
                # The named arguments are stored as a list of Tuple_String_Expr.
                named_args = self.decode_dsl_map_expr(expr.apply_expr.named_args)
                # The positional args can be a list of Expr, a single Expr, or [].
                if hasattr(expr.apply_expr, "pos_args"):
                    if isinstance(expr.apply_expr.pos_args, Iterable):
                        pos_args = [
                            self.decode_expr(pos_arg)
                            for pos_arg in expr.apply_expr.pos_args
                        ]
                    else:
                        pos_args = [self.decode_expr(expr.apply_expr.pos_args)]
                else:
                    pos_args = []
                result = fn(*pos_args, **named_args)
                if hasattr(expr, "var_id"):
                    self.symbol_table[expr.var_id.bitfield1] = (
                        self.capture_local_variable_name(expr),
                        result,
                    )
                return result

            # PYTHON VALUE LITERALS
            case "big_decimal_val":
                # For values like nan, snan, inf, etc. "special" is a combination of a sign and a character representing
                # the special value.
                if hasattr(expr.big_decimal_val, "special"):
                    match expr.big_decimal_val.special.value:
                        case "+F":
                            return Decimal("Infinity")
                        case "-F":
                            return Decimal("-Infinity")
                        case "+n":
                            return Decimal("nan")
                        case "-n":
                            return Decimal("-nan")
                        case "+N":
                            return Decimal("snan")
                        case "-N":
                            return Decimal("-snan")
                        case "":
                            # If special is empty, it means that the value is a normal big decimal.
                            pass
                        case _:
                            raise ValueError(
                                "Big decimal special value not recognized: %s"
                                % expr.big_decimal_val.special.value
                            )
                unscaled_value = int.from_bytes(
                    expr.big_decimal_val.unscaled_value, byteorder="big", signed=True
                )
                scale = expr.big_decimal_val.scale
                return Decimal(unscaled_value) / Decimal(10**-scale)

            case "binary_val":
                return expr.binary_val.v

            case "bool_val":
                return expr.bool_val.v

            case "float64_val":
                return expr.float64_val.v

            case "int64_val":
                return expr.int64_val.v

            case "list_val":
                # vs can be a list of Expr, a single Expr, or [].
                if hasattr(expr.list_val, "vs"):
                    if isinstance(expr.list_val.vs, Iterable):
                        return [self.decode_expr(v) for v in expr.list_val.vs]
                    else:
                        return [self.decode_expr(expr.list_val.vs)]
                else:
                    return []

            case "null_val":
                return None

            case "python_date_val":
                return date(
                    year=expr.python_date_val.year,
                    month=expr.python_date_val.month,
                    day=expr.python_date_val.day,
                )

            case "python_time_val":
                return time(
                    hour=expr.python_time_val.hour,
                    minute=expr.python_time_val.minute,
                    second=expr.python_time_val.second,
                    microsecond=expr.python_time_val.microsecond,
                    tzinfo=self.decode_timezone_expr(expr.python_time_val.tz),
                )

            case "python_timestamp_val":
                return datetime(
                    year=expr.python_timestamp_val.year,
                    month=expr.python_timestamp_val.month,
                    day=expr.python_timestamp_val.day,
                    hour=expr.python_timestamp_val.hour,
                    minute=expr.python_timestamp_val.minute,
                    second=expr.python_timestamp_val.second,
                    microsecond=expr.python_timestamp_val.microsecond,
                    tzinfo=self.decode_timezone_expr(expr.python_timestamp_val.tz),
                )

            case "seq_map_val":
                return {
                    self.decode_expr(kv.vs[0]): self.decode_expr(kv.vs[1])
                    for kv in expr.seq_map_val.kvs
                }

            case "tuple_val":
                # vs can be a list of Expr, a single Expr, or ().
                if hasattr(expr.tuple_val, "vs"):
                    if isinstance(expr.tuple_val.vs, Iterable):
                        return tuple(self.decode_expr(v) for v in expr.tuple_val.vs)
                    else:
                        return tuple(self.decode_expr(expr.tuple_val.vs))
                else:
                    return tuple()

            case "string_val":
                return expr.string_val.v

            # COLUMN FUNCTIONS
            case "sp_column_alias":
                col = self.decode_expr(expr.sp_column_alias.col)
                alias = expr.sp_column_alias.name
                # Column.as if True; Column.alias if False, Column.name if None.
                variant = expr.sp_column_alias.variant_is_as.value
                if variant is True:
                    return col.as_(alias)
                elif variant is False:
                    return col.alias(alias)
                else:
                    return col.name(alias)

            case "sp_column_apply__int":
                col = self.decode_expr(expr.sp_column_apply__int.col)
                field = expr.sp_column_apply__int.idx
                return col[field]

            case "sp_column_apply__string":
                col = self.decode_expr(expr.sp_column_apply__string.col)
                field = expr.sp_column_apply__string.field
                return col[field]

            case "sp_column_between":
                col = self.decode_expr(expr.sp_column_between.col)
                lower = self.decode_expr(expr.sp_column_between.lower_bound)
                upper = self.decode_expr(expr.sp_column_between.upper_bound)
                return col.between(lower, upper)

            case "sp_column_cast":
                col = self.decode_expr(expr.sp_column_cast.col)
                to_dtype = self.decode_data_type_expr(expr.sp_column_cast.to)
                return col.cast(to_dtype)

            case "sp_column_equal_nan":
                col = self.decode_expr(expr.sp_column_equal_nan.col)
                return col.equal_nan()

            case "sp_column_equal_null":
                lhs = self.decode_expr(expr.sp_column_equal_null.lhs)
                rhs = self.decode_expr(expr.sp_column_equal_null.rhs)
                return lhs.equal_null(rhs)

            case "sp_column_in__seq":
                col = self.decode_expr(expr.sp_column_in__seq.col)
                if isinstance(expr.sp_column_in__seq.values, Iterable):
                    # The values should be passed in as positional arguments and not as a list.
                    return col.in_(
                        self.decode_expr(v) for v in expr.sp_column_in__seq.values
                    )
                else:
                    # The list case should be taken care of in this branch.
                    return col.in_(self.decode_expr(expr.sp_column_in__seq.values))

            case "sp_column_is_not_null":
                col = self.decode_expr(expr.sp_column_is_not_null.col)
                return col.is_not_null()

            case "sp_column_is_null":
                col = self.decode_expr(expr.sp_column_is_null.col)
                return col.is_null()

            case "sp_column_sql_expr":
                return expr.sp_column_sql_expr.sql

            case "sp_column_string_like":
                col = self.decode_expr(expr.sp_column_string_like.col)
                return col

            case "sp_column_string_regexp":
                col = self.decode_expr(expr.sp_column_string_regexp.col)
                return col

            case "sp_column_string_starts_with":
                col = self.decode_expr(expr.sp_column_string_starts_with.col)
                return col

            case "sp_column_string_substr":
                col = self.decode_expr(expr.sp_column_string_substr.col)
                return col

            case "sp_column_string_ends_with":
                col = self.decode_expr(expr.sp_column_string_ends_with.col)
                return col

            case "sp_column_string_collate":
                col = self.decode_expr(expr.sp_column_string_collate.col)
                return col

            case "sp_column_string_contains":
                col = self.decode_expr(expr.sp_column_string_contains.col)
                return col

            case "sp_column_try_cast":
                col = self.decode_expr(expr.sp_column_try_cast.col)
                to_dtype = self.decode_data_type_expr(expr.sp_column_try_cast.to)
                return col.try_cast(to_dtype)

            # Binary operations on columns:
            case "eq":
                return self.binop(expr.eq, lambda lhs, rhs: lhs == rhs)

            case "neq":
                return self.binop(expr.neq, lambda lhs, rhs: lhs != rhs)

            case "gt":
                return self.binop(expr.gt, lambda lhs, rhs: lhs > rhs)

            case "lt":
                return self.binop(expr.lt, lambda lhs, rhs: lhs < rhs)

            case "geq":
                return self.binop(expr.geq, lambda lhs, rhs: lhs >= rhs)

            case "leq":
                return self.binop(expr.leq, lambda lhs, rhs: lhs <= rhs)

            case "sub":
                return self.binop(expr.sub, lambda lhs, rhs: lhs - rhs)

            case "mul":
                return self.binop(expr.mul, lambda lhs, rhs: lhs * rhs)

            case "div":
                return self.binop(expr.div, lambda lhs, rhs: lhs / rhs)

            case "mod":
                return self.binop(expr.mod, lambda lhs, rhs: lhs % rhs)

            case "pow":
                return self.binop(expr.pow, lambda lhs, rhs: lhs**rhs)

            case "and":
                # "and" is reserved keyword in python - so have to use getattr here.
                return self.binop(getattr(expr, "and"), lambda lhs, rhs: lhs & rhs)

            case "or":
                # "or" is reserved keyword in python - so have to use getattr here.
                return self.binop(getattr(expr, "or"), lambda lhs, rhs: lhs | rhs)

            # bit operations on columns
            case "bit_and":
                return self.bitop(expr.bit_and, "bitwiseAnd")

            case "bit_or":
                return self.bitop(expr.bit_or, "bitwiseOR")

            case "bit_xor":
                return self.bitop(expr.bit_xor, "bitwiseXOR")

            # Unary operations on columns:
            case "neg":
                col = self.decode_expr(expr.neg.operand)
                return -col

            case "not":
                # not is a reserved word in python.
                col_expr = getattr(expr, "not").operand
                col = self.decode_expr(col_expr)
                return ~col

            # DATAFRAME FUNCTIONS
            case "sp_create_dataframe":
                data = self.decode_dataframe_data_expr(expr.sp_create_dataframe.data)
                schema = self.decode_dataframe_schema_expr(
                    expr.sp_create_dataframe.schema
                )
                df = self.session.create_dataframe(data=data, schema=schema)
                if hasattr(expr, "var_id"):
                    self.symbol_table[expr.var_id.bitfield1] = (
                        self.capture_local_variable_name(expr),
                        df,
                    )
                return df
            case "sp_dataframe_col":
                col_name = expr.sp_dataframe_col.col_name
                df = self.decode_expr(expr.sp_dataframe_col.df)
                return df[col_name]
            case "sp_dataframe_collect":
                df = self.symbol_table[expr.sp_dataframe_collect.id.bitfield1][1]
                d = MessageToDict(expr.sp_dataframe_count)
                statement_params = self.get_statement_params(d)
                log_on_exception = d["logOnException"]
                block = d["block"]
                case_sensitive = d["caseSensitive"]
                no_wait = d.get("noWait", False)
                if no_wait:
                    return df.collect_nowait(
                        statement_params=statement_params,
                        log_on_exception=log_on_exception,
                        case_sensitive=case_sensitive,
                    )
                else:
                    return df.collect(
                        statement_params=statement_params,
                        log_on_exception=log_on_exception,
                        block=block,
                        case_sensitive=case_sensitive,
                    )
            case "sp_dataframe_count":
                df = self.symbol_table[expr.sp_dataframe_count.id.bitfield1][1]
                d = MessageToDict(expr.sp_dataframe_count)
                statement_params = self.get_statement_params(d)
                block = d["block"]
                return df.count(
                    statement_params=statement_params,
                    block=block,
                )

            case "sp_dataframe_ref":
                return self.symbol_table[expr.sp_dataframe_ref.id.bitfield1][1]
            case "sp_dataframe_select__columns":
                df = self.decode_expr(expr.sp_dataframe_select__columns.df)
                # The columns can be a list of Expr or a single Expr.
                if isinstance(expr.sp_dataframe_select__columns.cols, Iterable):
                    cols = [
                        self.decode_expr(col)
                        for col in expr.sp_dataframe_select__columns.cols
                    ]
                else:
                    cols = [self.decode_expr(expr.sp_dataframe_select__columns.cols)]
                if hasattr(expr.sp_dataframe_select__columns, "variadic"):
                    val = df.select(*cols)
                else:
                    val = df.select(cols)
                if hasattr(expr, "var_id"):
                    self.symbol_table[expr.var_id.bitfield1] = (
                        self.capture_local_variable_name(expr),
                        val,
                    )
                return val
            case "sp_dataframe_show":
                df = self.decode_expr(
                    self.symbol_table[expr.sp_dataframe_show.id.bitfield1][1]
                )
                return df.show()
            case "sp_table":
                assert expr.sp_table.HasField("name")
                table_name = self.decode_table_name_expr(expr.sp_table.name)
                return self.session.table(table_name)

            case "sp_dataframe_cross_join":
                lhs = self.decode_expr(expr.sp_dataframe_cross_join.lhs)
                rhs = self.decode_expr(expr.sp_dataframe_cross_join.rhs)
                left_suffix = expr.sp_dataframe_cross_join.lsuffix.value
                right_suffix = expr.sp_dataframe_cross_join.rsuffix.value
                return lhs.cross_join(
                    right=rhs, lsuffix=left_suffix, rsuffix=right_suffix
                )

            case "sp_dataframe_flatten":
                df = self.decode_expr(expr.sp_dataframe_flatten.df)
                input = self.decode_expr(expr.sp_dataframe_flatten.input)
                mode = "BOTH"
                match expr.sp_dataframe_flatten.mode.WhichOneof("variant"):
                    case "sp_flatten_mode_both":
                        mode = "BOTH"
                    case "sp_flatten_mode_array":
                        mode = "ARRAY"
                    case "sp_flatten_mode_object":
                        mode = "OBJECT"

                path = expr.sp_dataframe_flatten.path.value

                outer = expr.sp_dataframe_flatten.outer
                recursive = expr.sp_dataframe_flatten.recursive
                if len(path) == 0:
                    return df.flatten(
                        input=input, mode=mode, outer=outer, recursive=recursive
                    )
                return df.flatten(
                    input=input, path=path, mode=mode, outer=outer, recursive=recursive
                )

            case _:
                raise NotImplementedError(
                    "Expression type not implemented yet: %s"
                    % expr.WhichOneof("variant")
                )

    def decode_stmt(self, stmt: proto.Assign | proto.Eval):
        """
        Given an assign/eval statement, return the result as a Python object.

        Parameters
        ----------
        stmt : proto.Request.Body
            The assign or eval statement to decode.
        """
        match stmt.WhichOneof("variant"):
            case "assign":
                val = self.decode_expr(stmt.assign.expr)
                val_symbol = self.capture_local_variable_name(stmt.assign)
                self.symbol_table[stmt.assign.var_id.bitfield1] = (
                    self.capture_local_variable_name(stmt.assign),
                    val,
                )
                logger.info(
                    f"assign result '{val_symbol} = {val}' at var_id {stmt.assign.var_id.bitfield1}"
                )

            case "eval":
                if hasattr(stmt, "parameters"):
                    val_symbol, val = self.symbol_table[stmt.parameters[0]]
                    logger.info(f"eval result: {val_symbol} = {val}")
            case _:
                raise ValueError(
                    "Unknown statement type: %s" % stmt.WhichOneof("variant")
                )
