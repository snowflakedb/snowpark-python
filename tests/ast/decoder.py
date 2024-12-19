#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import logging
from typing import Any, Optional, Iterable, List, Union, Dict, Tuple
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

import snowflake.snowpark._internal.proto.generated.ast_pb2 as proto

from google.protobuf.json_format import MessageToDict

from snowflake.snowpark import Session, Column
import snowflake.snowpark.functions
from snowflake.snowpark.functions import udf, when
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

    def decode_col_exprs(self, expr: proto.Expr, is_variadic: bool) -> List[Column]:
        """
        Decode a protobuf object to a list of column expressions.

        Parameters
        ----------
        expr : proto.Expr
            The protobuf object to decode.
        is_variadic : bool
            Whether the expression is variadic.

        Returns
        -------
        List[Column]
            The decoded columns.
        """
        if len(expr) == 1:
            # Prevent nesting the list in a list if there is only one expression.
            # This usually happens when the expression is a list_val.
            col_list = self.decode_expr(expr[0])
            if not isinstance(col_list, list) and not is_variadic:
                col_list = [col_list]
        else:
            col_list = [self.decode_expr(arg) for arg in expr]
        return col_list

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
            case "sp_fn_ref":
                return self.symbol_table[fn_ref_expr.sp_fn_ref.id.bitfield1][0]
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
                if hasattr(snowflake.snowpark.functions, fn_name):
                    fn = getattr(snowflake.snowpark.functions, fn_name)
                else:
                    fn = self.symbol_table[expr.apply_expr.fn.sp_fn_ref.id.bitfield1][1]
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

            case "none_val":
                return None

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

                match expr.sp_column_alias.fn.WhichOneof("variant"):
                    case "sp_column_alias_fn_alias":
                        return col.alias(alias)

                    case "sp_column_alias_fn_as":
                        return col.as_(alias)

                    case _:
                        return col.name(alias)

            case "sp_column_apply__int":
                col = self.decode_expr(expr.sp_column_apply__int.col)
                field = expr.sp_column_apply__int.idx
                return col[field]

            case "sp_column_apply__string":
                col = self.decode_expr(expr.sp_column_apply__string.col)
                field = expr.sp_column_apply__string.field
                return col[field]

            case "sp_column_asc":
                col = self.decode_expr(expr.sp_column_asc.col)
                match expr.sp_column_asc.null_order.WhichOneof("variant"):
                    case "sp_null_order_default":
                        return col.asc()
                    case "sp_null_order_nulls_first":
                        return col.asc_nulls_first()
                    case "sp_null_order_nulls_last":
                        return col.asc_nulls_last()
                    case _:
                        raise ValueError(
                            "Unknown null order for sp_column_asc: %s"
                            % expr.sp_column_asc.null_order.WhichOneof("variant")
                        )

            case "sp_column_between":
                col = self.decode_expr(expr.sp_column_between.col)
                lower = self.decode_expr(expr.sp_column_between.lower_bound)
                upper = self.decode_expr(expr.sp_column_between.upper_bound)
                return col.between(lower, upper)

            case "sp_column_case_when":
                # The cases can be chained as when(...).when(...).otherwise(...) or
                # when(...).otherwise(...).when(...).otherwise(...).
                ret_val = None
                for case in expr.sp_column_case_when.cases:
                    # If the condition field is empty, it is a call to `otherwise`, else it is a call to `when`.
                    value = self.decode_expr(case.value)
                    if hasattr(case, "condition") and str(case.condition).strip() != "":
                        condition = self.decode_expr(case.condition)
                        ret_val = (
                            when(condition, value)
                            if ret_val is None
                            else ret_val.when(condition, value)
                        )
                    else:
                        ret_val = ret_val.otherwise(value)
                return ret_val

            case "sp_column_cast":
                col = self.decode_expr(expr.sp_column_cast.col)
                to_dtype = self.decode_data_type_expr(expr.sp_column_cast.to)
                return col.cast(to_dtype)

            case "sp_column_desc":
                col = self.decode_expr(expr.sp_column_desc.col)
                match expr.sp_column_desc.null_order.WhichOneof("variant"):
                    case "sp_null_order_default":
                        return col.desc()
                    case "sp_null_order_nulls_first":
                        return col.desc_nulls_first()
                    case "sp_null_order_nulls_last":
                        return col.desc_nulls_last()
                    case _:
                        raise ValueError(
                            "Unknown null order for sp_column_desc: %s"
                            % expr.sp_column_desc.null_order.WhichOneof("variant")
                        )

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
                pattern = self.decode_expr(expr.sp_column_string_like.pattern)
                return col.like(pattern)

            case "sp_column_string_regexp":
                col = self.decode_expr(expr.sp_column_string_regexp.col)
                pattern = self.decode_expr(expr.sp_column_string_regexp.pattern)
                parameters = self.decode_expr(expr.sp_column_string_regexp.parameters)
                return col.regexp(pattern, parameters)

            case "sp_column_string_starts_with":
                col = self.decode_expr(expr.sp_column_string_starts_with.col)
                prefix = self.decode_expr(expr.sp_column_string_starts_with.prefix)
                return col.starts_with(prefix)

            case "sp_column_string_substr":
                col = self.decode_expr(expr.sp_column_string_substr.col)
                len = self.decode_expr(expr.sp_column_string_substr.len)
                pos = self.decode_expr(expr.sp_column_string_substr.pos)
                return col.substr(pos, len)

            case "sp_column_string_ends_with":
                col = self.decode_expr(expr.sp_column_string_ends_with.col)
                suffix = self.decode_expr(expr.sp_column_string_ends_with.suffix)
                return col.ends_with(suffix)

            case "sp_column_string_collate":
                col = self.decode_expr(expr.sp_column_string_collate.col)
                collation_spec = self.decode_expr(
                    expr.sp_column_string_collate.collation_spec
                )
                return col.collate(collation_spec)

            case "sp_column_string_contains":
                col = self.decode_expr(expr.sp_column_string_contains.col)
                pattern = self.decode_expr(expr.sp_column_string_contains.pattern)
                return col.contains(pattern)

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

            case "sp_dataframe_agg":
                df = self.decode_expr(expr.sp_dataframe_agg.df)
                exprs = [
                    self.decode_expr(arg) for arg in expr.sp_dataframe_agg.exprs.args
                ]
                if expr.sp_dataframe_agg.exprs.variadic:
                    return df.agg(*exprs)
                else:
                    return df.agg(exprs)

            case "sp_dataframe_col":
                col_name = expr.sp_dataframe_col.col_name
                df = self.decode_expr(expr.sp_dataframe_col.df)
                return df[col_name]

            case "sp_dataframe_collect":
                df = self.decode_expr(expr.sp_dataframe_first.df)
                d = MessageToDict(expr.sp_dataframe_collect)
                statement_params = self.get_statement_params(d)
                log_on_exception = d.get("logOnException", False)
                block = d.get("block", False)
                case_sensitive = d.get("caseSensitive", False)
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
                df = self.decode_expr(expr.sp_dataframe_first.df)
                d = MessageToDict(expr.sp_dataframe_count)
                statement_params = self.get_statement_params(d)
                block = d["block"]
                return df.count(
                    statement_params=statement_params,
                    block=block,
                )

            case "sp_dataframe_drop":
                df = self.decode_expr(expr.sp_dataframe_drop.df)
                cols = self.decode_col_exprs(
                    expr.sp_dataframe_drop.cols.args,
                    expr.sp_dataframe_drop.cols.variadic,
                )
                if expr.sp_dataframe_group_by.cols.variadic:
                    return df.drop(*cols)
                else:
                    return df.drop(cols)

            case "sp_dataframe_except":
                df = self.decode_expr(expr.sp_dataframe_except.df)
                other = self.decode_expr(expr.sp_dataframe_except.other)
                return df.except_(other)

            case "sp_dataframe_first":
                df = self.decode_expr(expr.sp_dataframe_first.df)
                block = expr.sp_dataframe_first.block
                num = expr.sp_dataframe_first.num
                statement_params = self.get_statement_params(
                    MessageToDict(expr.sp_dataframe_first)
                )
                return df.first(n=num, statement_params=statement_params, block=block)

            case "sp_dataframe_group_by":
                df = self.decode_expr(expr.sp_dataframe_group_by.df)
                cols = self.decode_col_exprs(
                    expr.sp_dataframe_group_by.cols.args,
                    expr.sp_dataframe_group_by.cols.variadic,
                )
                if expr.sp_dataframe_group_by.cols.variadic:
                    return df.group_by(*cols)
                else:
                    return df.group_by(cols)

            case "sp_dataframe_intersect":
                df = self.decode_expr(expr.sp_dataframe_intersect.df)
                other = self.decode_expr(expr.sp_dataframe_intersect.other)
                return df.intersect(other)

            case "sp_dataframe_ref":
                return self.symbol_table[expr.sp_dataframe_ref.id.bitfield1][1]

            case "sp_dataframe_select__columns":
                df = self.decode_expr(expr.sp_dataframe_select__columns.df)
                # The columns can be a list of Expr or a single Expr.
                cols = self.decode_col_exprs(
                    expr.sp_dataframe_select__columns.cols,
                    not hasattr(expr.sp_dataframe_select__columns, "variadic"),
                )
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

            case "sp_dataframe_sort":
                df = self.decode_expr(expr.sp_dataframe_sort.df)
                cols = self.decode_col_exprs(
                    expr.sp_dataframe_sort.cols, expr.sp_dataframe_sort.cols.variadic
                )
                ascending = self.decode_expr(expr.sp_dataframe_sort.ascending)
                if expr.sp_dataframe_sort.cols_variadic:
                    return df.sort(*cols, ascending)
                else:
                    return df.sort(cols, ascending)

            case "sp_relational_grouped_dataframe_agg":
                grouped_df = self.decode_expr(
                    expr.sp_relational_grouped_dataframe_agg.grouped_df
                )
                exprs = self.decode_col_exprs(
                    expr.sp_relational_grouped_dataframe_agg.exprs.args,
                    expr.sp_relational_grouped_dataframe_agg.cols.variadic,
                )
                if expr.sp_relational_grouped_dataframe_agg.exprs.variadic is True:
                    return grouped_df.agg(*exprs)
                else:
                    return grouped_df.agg(exprs)

            case "sp_relational_grouped_dataframe_apply_in_pandas":
                # TODO: SNOW-1830603 Flesh out this logic when implementing UDTFs. Need to create a dict to maintain
                #       all functions registered (here, `func`). Implement `decode_callable_expr`.
                # func = self.decode_callable_expr(expr.sp_relational_grouped_dataframe_apply_in_pandas.func)
                # grouped_df = self.decode_expr(expr.sp_relational_grouped_dataframe_apply_in_pandas.grouped_df)
                # kwargs = self.decode_dsl_map_expr(expr.sp_relational_grouped_dataframe_apply_in_pandas.kwargs)
                # output_schema = self.decode_expr(expr.sp_relational_grouped_dataframe_apply_in_pandas.output_schema)
                # return grouped_df.apply_in_pandas(func, output_schema, **kwargs)
                pass

            case "sp_relational_grouped_dataframe_builtin":
                grouped_df = self.decode_expr(
                    expr.sp_relational_grouped_dataframe_builtin.grouped_df
                )
                cols = self.decode_col_exprs(
                    expr.sp_relational_grouped_dataframe_builtin.cols.args,
                    expr.sp_relational_grouped_dataframe_builtin.cols.variadic,
                )
                agg_name = expr.sp_relational_grouped_dataframe_builtin.agg_name
                if (
                    expr.sp_relational_grouped_dataframe_builtin.cols.variadic
                    and isinstance(agg_name, list)
                ):
                    return grouped_df.function(*agg_name)(*cols)
                else:
                    return grouped_df.function(agg_name)(*cols)

            case "sp_relational_grouped_dataframe_ref":
                return self.symbol_table[
                    expr.sp_relational_grouped_dataframe_ref.id.bitfield1
                ][1]

            case "sp_table":
                assert expr.sp_table.HasField("name")
                table_name = self.decode_table_name_expr(expr.sp_table.name)
                return self.session.table(table_name)

            case "udf":
                return_type = self.decode_data_type_expr(expr.udf.return_type)
                input_types = [
                    self.decode_data_type_expr(input_type)
                    for input_type in expr.udf.input_types.list
                ]
                return udf(
                    lambda *args: None, return_type=return_type, input_types=input_types
                )

            case "udtf":
                # TODO: SNOW-1830603 Implement UDTF decoding.
                pass

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

            case "sp_dataframe_create_or_replace_view":
                df = self.decode_expr(expr.sp_dataframe_create_or_replace_view.df)
                name = [
                    qualified_name
                    for qualified_name in expr.sp_dataframe_create_or_replace_view.name
                ]

                statement_params = None
                if hasattr(
                    expr.sp_dataframe_create_or_replace_view, "statement_params"
                ):
                    d = MessageToDict(expr.sp_dataframe_create_or_replace_view)
                    statement_params = self.get_statement_params(d)

                comment = None
                if hasattr(expr.sp_dataframe_create_or_replace_view, "comment"):
                    comment = expr.sp_dataframe_create_or_replace_view.comment.value
                is_temp = expr.sp_dataframe_create_or_replace_view.is_temp
                if is_temp:
                    if len(comment) > 0:
                        return df.create_or_replace_temp_view(
                            name, comment=comment, statement_params=statement_params
                        )
                    else:
                        return df.create_or_replace_temp_view(
                            name, statement_params=statement_params
                        )
                else:
                    if len(comment) > 0:
                        return df.create_or_replace_view(
                            name, comment=comment, statement_params=statement_params
                        )
                    return df.create_or_replace_view(
                        name, statement_params=statement_params
                    )

            case "sp_dataframe_copy_into_table":
                df = self.decode_expr(expr.sp_dataframe_copy_into_table.df)
                name = [
                    qualified_name
                    for qualified_name in expr.sp_dataframe_copy_into_table.table_name
                ]
                files = [
                    file_name for file_name in expr.sp_dataframe_copy_into_table.files
                ]
                pattern = expr.sp_dataframe_copy_into_table.pattern.value
                validation_mode = (
                    expr.sp_dataframe_copy_into_table.validation_mode.value
                )
                target_columns = [
                    column_name
                    for column_name in expr.sp_dataframe_copy_into_table.target_columns
                ]
                transformations = [
                    self.decode_expr(transformation)
                    for transformation in expr.sp_dataframe_copy_into_table.transformations
                ]
                format_type_options = None
                if hasattr(expr.sp_dataframe_copy_into_table, "format_type_options"):
                    format_type_options = {
                        expr.sp_dataframe_copy_into_table.format_type_options[
                            i
                        ]._1: self.decode_expr(
                            expr.sp_dataframe_copy_into_table.format_type_options[i]._2
                        )
                        for i in range(
                            len(expr.sp_dataframe_copy_into_table.format_type_options)
                        )
                    }
                statement_params = None
                if hasattr(expr.sp_dataframe_copy_into_table, "statement_params"):
                    statement_params = {
                        expr.sp_dataframe_copy_into_table.statement_params[i]
                        ._1: expr.sp_dataframe_copy_into_table.statement_params[i]
                        ._2
                        for i in range(
                            len(expr.sp_dataframe_copy_into_table.statement_params)
                        )
                    }
                copy_options = None
                if hasattr(expr.sp_dataframe_copy_into_table, "copy_options"):
                    copy_options = {
                        expr.sp_dataframe_copy_into_table.copy_options[
                            i
                        ]._1: self.decode_expr(
                            expr.sp_dataframe_copy_into_table.copy_options[i]._2
                        )
                        for i in range(
                            len(expr.sp_dataframe_copy_into_table.copy_options)
                        )
                    }

                df.copy_into_table(
                    table_name=name,
                    files=files,
                    pattern=pattern,
                    validation_mode=validation_mode,
                    target_columns=target_columns,
                    transformations=transformations,
                    format_type_options=format_type_options,
                    statement_params=statement_params,
                    **copy_options,
                )

            case "sp_dataframe_cache_result":
                df = self.decode_expr(expr.sp_dataframe_cache_result.df)
                d = MessageToDict(expr.sp_dataframe_cache_result)
                statement_params = self.get_statement_params(d)
                return df.cache_result(statement_params=statement_params)

            case "sp_dataframe_create_or_replace_dynamic_table":
                df = self.decode_expr(
                    expr.sp_dataframe_create_or_replace_dynamic_table.df
                )
                name = [
                    qualified_name_part
                    for qualified_name_part in expr.sp_dataframe_create_or_replace_dynamic_table.name
                ]
                warehouse = expr.sp_dataframe_create_or_replace_dynamic_table.warehouse
                lag = expr.sp_dataframe_create_or_replace_dynamic_table.lag
                comment = (
                    expr.sp_dataframe_create_or_replace_dynamic_table.comment.value
                )
                mode = "overwrite"
                match expr.sp_dataframe_create_or_replace_dynamic_table.mode.WhichOneof(
                    "variant"
                ):
                    case "sp_save_mode_append":
                        mode = "append"

                    case "sp_save_mode_error_if_exists":
                        mode = "error_if_exists"

                    case "sp_save_mode_ignore":
                        mode = "ignore"

                    case "sp_save_mode_overwrite":
                        mode = "overwrite"

                    case "sp_save_mode_truncate":
                        mode = "truncate"

                refresh_mode = None
                if (
                    hasattr(
                        expr.sp_dataframe_create_or_replace_dynamic_table,
                        "refresh_mode",
                    )
                    and len(
                        expr.sp_dataframe_create_or_replace_dynamic_table.refresh_mode.value
                    )
                    > 0
                ):
                    refresh_mode = (
                        expr.sp_dataframe_create_or_replace_dynamic_table.refresh_mode.value
                    )
                initialize = None
                if (
                    hasattr(
                        expr.sp_dataframe_create_or_replace_dynamic_table, "initialize"
                    )
                    and len(
                        expr.sp_dataframe_create_or_replace_dynamic_table.initialize.value
                    )
                    > 0
                ):
                    initialize = (
                        expr.sp_dataframe_create_or_replace_dynamic_table.initialize.value
                    )
                clustering_keys = None
                if (
                    len(
                        expr.sp_dataframe_create_or_replace_dynamic_table.clustering_keys.list
                    )
                    > 0
                ):
                    clustering_keys = [
                        self.decode_expr(clustering_key)
                        for clustering_key in expr.sp_dataframe_create_or_replace_dynamic_table.clustering_keys.list
                    ]
                is_transient = (
                    expr.sp_dataframe_create_or_replace_dynamic_table.is_transient
                )
                data_retention_time = None
                if (
                    hasattr(
                        expr.sp_dataframe_create_or_replace_dynamic_table,
                        "data_retention_time",
                    )
                    and expr.sp_dataframe_create_or_replace_dynamic_table.data_retention_time.value
                    > 0
                ):
                    data_retention_time = (
                        expr.sp_dataframe_create_or_replace_dynamic_table.data_retention_time.value
                    )
                max_data_extension_time = None
                if (
                    hasattr(
                        expr.sp_dataframe_create_or_replace_dynamic_table,
                        "max_data_extension_time",
                    )
                    and expr.sp_dataframe_create_or_replace_dynamic_table.max_data_extension_time.value
                    > 0
                ):
                    max_data_extension_time = (
                        expr.sp_dataframe_create_or_replace_dynamic_table.max_data_extension_time.value
                    )
                d = MessageToDict(expr.sp_dataframe_create_or_replace_dynamic_table)
                statement_params = self.get_statement_params(d)
                iceberg_config = None
                if hasattr(
                    expr.sp_dataframe_create_or_replace_dynamic_table, "iceberg_config"
                ):
                    iceberg_config = (
                        expr.sp_dataframe_create_or_replace_dynamic_table.iceberg_config
                    )
                return df.create_or_replace_dynamic_table(
                    name=name,
                    warehouse=warehouse,
                    lag=lag,
                    comment=comment,
                    mode=mode,
                    refresh_mode=refresh_mode,
                    initialize=initialize,
                    clustering_keys=clustering_keys,
                    is_transient=is_transient,
                    data_retention_time=data_retention_time,
                    max_data_extension_time=max_data_extension_time,
                    statement_params=statement_params,
                    iceberg_config=iceberg_config,
                )

            case "sp_dataframe_write":
                df = self.decode_expr(expr.sp_dataframe_write.df)
                return df.write

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
