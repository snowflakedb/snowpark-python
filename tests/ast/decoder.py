#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import logging
import re
from typing import Any, Optional, Iterable, List, Union, Dict, Tuple, Callable
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

import snowflake.snowpark._internal.proto.generated.ast_pb2 as proto

from google.protobuf.json_format import MessageToDict

from build.lib.snowflake.snowpark.functions import call_table_function
from build.lib.snowflake.snowpark.relational_grouped_dataframe import GroupingSets
from snowflake.snowpark import Session, Column, DataFrameAnalyticsFunctions
import snowflake.snowpark.functions
from snowflake.snowpark.functions import udf, when, sproc
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

    def get_dataframe_analytics_function_column_formatter(
        self, sp_dataframe_analytics_expr: proto.Expr
    ) -> Callable:
        """
        Create a dataframe analytics function column formatter.
        This is mainly to pass the df_analytics_functions.test.

        Parameters
        ----------
        sp_dataframe_analytics_expr : proto.Expr
            The dataframe analytics expression.

        Returns
        -------
        Callable
            The dataframe analytics function column formatter.
        """
        if "formattedColNames" in MessageToDict(sp_dataframe_analytics_expr):
            formatted_col_names = list(sp_dataframe_analytics_expr.formatted_col_names)
            w_lambda_pattern = re.compile(r"^(\w+)_W_(\w+)$")
            xy_lambda_pattern = re.compile(r"^(\w+)_X_(\w+)_Y_(\w+)$")
            if all(re.match(xy_lambda_pattern, col) for col in formatted_col_names):
                return (
                    lambda input, agg, window_size: f"{input}_X_{agg}_Y_{window_size}"
                )
            elif all(re.match(w_lambda_pattern, col) for col in formatted_col_names):
                return lambda input, agg: f"{input}_W_{agg}"
            else:
                return lambda input_col, agg, window: f"{agg}_{input_col}_{window}"
        else:
            return DataFrameAnalyticsFunctions._default_col_formatter

    def decode_col_exprs(self, expr: proto.Expr) -> List[Column]:
        """
        Decode a protobuf object to a list of column expressions.

        Parameters
        ----------
        expr : proto.Expr
            The protobuf object to decode.

        Returns
        -------
        List[Column]
            The decoded columns.
        """
        if len(expr) == 1:
            # Prevent nesting the list in a list if there is only one expression.
            # This usually happens when the expression is a list_val.
            col_list = self.decode_expr(expr[0])
            if not isinstance(col_list, list):
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

    def convert_name_to_list(self, name: any) -> List:
        if isinstance(name, str):
            return [name]
        return [qualified_name for qualified_name in name]

    def decode_name_expr(self, table_name: proto.SpName) -> str:
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
        if table_name.name.HasField("sp_name_flat"):
            return table_name.name.sp_name_flat.name
        elif table_name.name.HasField("sp_name_structured"):
            return table_name.name.sp_name_structured.name
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
                return self.decode_name_expr(fn_ref_expr.builtin_fn.name)
            case "call_table_function_expr":
                return self.decode_name_expr(fn_ref_expr.call_table_function_expr.name)
            case "indirect_table_fn_id_ref":
                return self.symbol_table[
                    fn_ref_expr.indirect_table_fn_id_ref.id.bitfield1
                ][0]
            case "indirect_table_fn_name_ref":
                return self.decode_name_expr(
                    fn_ref_expr.indirect_table_fn_name_ref.name
                )
            case "sp_fn_ref":
                return self.symbol_table[fn_ref_expr.sp_fn_ref.id.bitfield1][0]
            case "stored_procedure":
                return self.decode_name_expr(fn_ref_expr.stored_procedure.name)
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
                    % df_schema_expr.WhichOneof("sealed_value")
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

    def decode_join_type(self, join_type: proto.SpJoinType) -> str:
        """
        Decode a join type expression to get the join type.

        Parameters
        ----------
        join_type : proto.SpJoinType
            The expression to decode.

        Returns
        -------
        str
            The decoded join type.
        """
        match join_type.WhichOneof("variant"):
            case "sp_join_type__asof":
                return "asof"
            case "sp_join_type__cross":
                return "cross"
            case "sp_join_type__full_outer":
                return "full"
            case "sp_join_type__inner":
                return "inner"
            case "sp_join_type__left_anti":
                return "anti"
            case "sp_join_type__left_outer":
                return "left"
            case "sp_join_type__left_semi":
                return "semi"
            case "sp_join_type__right_outer":
                return "right"
            case _:
                raise ValueError(
                    "Unknown join type: %s" % join_type.WhichOneof("variant")
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

    def decode_expr(self, expr: proto.Expr, **kwargs) -> Any:
        match expr.WhichOneof("variant"):
            # COLUMN BINARY OPERATIONS
            case "add":
                lhs = self.decode_expr(expr.add.lhs)
                rhs = self.decode_expr(expr.add.rhs)
                return lhs + rhs

            case "apply_expr":
                fn_name = self.decode_fn_ref_expr(expr.apply_expr.fn)
                if isinstance(fn_name, str):
                    if hasattr(snowflake.snowpark.functions, fn_name):
                        fn = getattr(snowflake.snowpark.functions, fn_name)
                    elif expr.apply_expr.fn.sp_fn_ref.id.bitfield1 in self.symbol_table:
                        fn = self.symbol_table[
                            expr.apply_expr.fn.sp_fn_ref.id.bitfield1
                        ][1]
                    else:
                        fn = None
                else:
                    # If fn_name is not a string, it is a collection of table functions. Convert it to a list.
                    fn_name = [name for name in fn_name]
                    fn = None

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

                if fn is None:
                    # Stored procedures, table functions, (and in the future I expect UDTFs maybe) will pass through
                    # here directly (not through their respective entities) when invoked. Call the right method.
                    # If a source is provided via kwargs, short-circuit with that.
                    source = kwargs.get("source", None)
                    match expr.apply_expr.fn.WhichOneof("variant"):
                        case "sp_fn_ref":
                            return self.session.call(fn_name, *pos_args, **named_args)
                        case "indirect_table_fn_id_ref":
                            return self.session.table_function(
                                self.symbol_table[
                                    expr.apply_expr.fn.indirect_table_fn_id_ref.id.bitfield1
                                ][1]
                            )
                        case "call_table_function_expr" | "indirect_table_fn_name_ref":
                            if source is "sp_session_table_function":
                                return self.session.table_function(
                                    fn_name, *pos_args, **named_args
                                )
                            else:
                                return call_table_function(
                                    fn_name, *pos_args, **named_args
                                )
                        case _:
                            raise ValueError(
                                "Unknown function reference type: %s"
                                % expr.apply_expr.fn.WhichOneof("variant")
                            )

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
                length = self.decode_expr(expr.sp_column_string_substr.len)
                pos = self.decode_expr(expr.sp_column_string_substr.pos)
                return col.substr(pos, length)

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

            case "object_get_item":
                args = [self.decode_expr(arg) for arg in expr.object_get_item.args][0]
                return self.symbol_table[expr.object_get_item.obj.bitfield1][1][args]

            # DATAFRAME FUNCTIONS
            case "sp_create_dataframe":
                data = self.decode_dataframe_data_expr(expr.sp_create_dataframe.data)
                d = MessageToDict(expr.sp_create_dataframe)
                schema = (
                    self.decode_dataframe_schema_expr(expr.sp_create_dataframe.schema)
                    if "schema" in d
                    else None
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

            case "sp_dataframe_alias":
                df = self.decode_expr(expr.sp_dataframe_alias.df)
                name = expr.sp_dataframe_alias.name
                return df.alias(name)

            case "sp_dataframe_analytics_compute_lag":
                df = self.decode_expr(expr.sp_dataframe_analytics_compute_lag.df)
                cols = [
                    self.decode_expr(col)
                    for col in expr.sp_dataframe_analytics_compute_lag.cols
                ]
                group_by = list(expr.sp_dataframe_analytics_compute_lag.group_by)
                lags = list(expr.sp_dataframe_analytics_compute_lag.lags)
                order_by = list(expr.sp_dataframe_analytics_compute_lag.order_by)
                col_formatter = self.get_dataframe_analytics_function_column_formatter(
                    expr.sp_dataframe_analytics_compute_lag
                )
                return df.analytics.compute_lag(
                    cols, lags, order_by, group_by, col_formatter
                )

            case "sp_dataframe_analytics_compute_lead":
                df = self.decode_expr(expr.sp_dataframe_analytics_compute_lead.df)
                cols = [
                    self.decode_expr(col)
                    for col in expr.sp_dataframe_analytics_compute_lead.cols
                ]
                group_by = list(expr.sp_dataframe_analytics_compute_lead.group_by)
                leads = list(expr.sp_dataframe_analytics_compute_lead.leads)
                order_by = list(expr.sp_dataframe_analytics_compute_lead.order_by)
                col_formatter = self.get_dataframe_analytics_function_column_formatter(
                    expr.sp_dataframe_analytics_compute_lead
                )
                return df.analytics.compute_lead(
                    cols, leads, order_by, group_by, col_formatter
                )

            case "sp_dataframe_analytics_cumulative_agg":
                df = self.decode_expr(expr.sp_dataframe_analytics_cumulative_agg.df)
                gen_aggs = self.decode_dsl_map_expr(
                    expr.sp_dataframe_analytics_cumulative_agg.aggs
                )
                # The aggs dict created has generator objects as the kv pairs. Convert them to strings/list of strings.
                aggs = {str(k): list(v) for k, v in gen_aggs.items()}
                group_by = list(expr.sp_dataframe_analytics_cumulative_agg.group_by)
                order_by = list(expr.sp_dataframe_analytics_cumulative_agg.order_by)
                is_forward = (
                    expr.sp_dataframe_analytics_cumulative_agg.is_forward
                    if hasattr(expr.sp_dataframe_analytics_cumulative_agg, "is_forward")
                    else False
                )
                col_formatter = self.get_dataframe_analytics_function_column_formatter(
                    expr.sp_dataframe_analytics_cumulative_agg
                )
                return df.analytics.cumulative_agg(
                    aggs, group_by, order_by, is_forward, col_formatter
                )

            case "sp_dataframe_analytics_moving_agg":
                df = self.decode_expr(expr.sp_dataframe_analytics_moving_agg.df)
                gen_aggs = self.decode_dsl_map_expr(
                    expr.sp_dataframe_analytics_moving_agg.aggs
                )
                # The aggs dict created has generator objects as the kv pairs. Convert them to strings/list of strings.
                aggs = {str(k): list(v) for k, v in gen_aggs.items()}
                group_by = list(expr.sp_dataframe_analytics_moving_agg.group_by)
                order_by = list(expr.sp_dataframe_analytics_moving_agg.order_by)
                window_sizes = list(expr.sp_dataframe_analytics_moving_agg.window_sizes)
                col_formatter = self.get_dataframe_analytics_function_column_formatter(
                    expr.sp_dataframe_analytics_moving_agg
                )
                return df.analytics.moving_agg(
                    aggs, window_sizes, order_by, group_by, col_formatter
                )

            case "sp_dataframe_analytics_time_series_agg":
                df = self.decode_expr(expr.sp_dataframe_analytics_time_series_agg.df)
                gen_aggs = self.decode_dsl_map_expr(
                    expr.sp_dataframe_analytics_time_series_agg.aggs
                )
                # The aggs dict created has generator objects as the kv pairs. Convert them to strings/list of strings.
                aggs = {str(k): list(v) for k, v in gen_aggs.items()}
                group_by = list(expr.sp_dataframe_analytics_time_series_agg.group_by)
                sliding_interval = (
                    expr.sp_dataframe_analytics_time_series_agg.sliding_interval
                )
                time_col = expr.sp_dataframe_analytics_time_series_agg.time_col
                windows = list(expr.sp_dataframe_analytics_time_series_agg.windows)
                col_formatter = self.get_dataframe_analytics_function_column_formatter(
                    expr.sp_dataframe_analytics_time_series_agg
                )
                return df.analytics.time_series_agg(
                    time_col, aggs, windows, group_by, sliding_interval, col_formatter
                )

            case "sp_dataframe_col":
                col_name = expr.sp_dataframe_col.col_name
                df = self.decode_expr(expr.sp_dataframe_col.df)
                return df[col_name]

            case "sp_dataframe_collect":
                df = self.symbol_table[expr.sp_dataframe_collect.id.bitfield1][1]
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

            case "sp_dataframe_cube":
                df = self.decode_expr(expr.sp_dataframe_cube.df)
                d = MessageToDict(expr.sp_dataframe_cube.cols)
                if "args" not in d:
                    return df.cube()
                cols = self.decode_col_exprs(expr.sp_dataframe_cube.cols.args)
                if d.get("variadic", False):
                    return df.cube(*cols)
                else:
                    return df.cube(cols)

            case "sp_dataframe_describe":
                df = self.decode_expr(expr.sp_dataframe_describe.df)
                d = MessageToDict(expr.sp_dataframe_describe.cols)
                if "args" not in d:
                    return df.describe()
                cols = self.decode_col_exprs(expr.sp_dataframe_describe.cols.args)
                if d.get("variadic", False):
                    return df.describe(*cols)
                else:
                    return df.describe(cols)

            case "sp_dataframe_distinct":
                df = self.decode_expr(expr.sp_dataframe_distinct.df)
                return df.distinct()

            case "sp_dataframe_drop":
                df = self.decode_expr(expr.sp_dataframe_drop.df)
                cols = self.decode_col_exprs(expr.sp_dataframe_drop.cols.args)
                if MessageToDict(expr.sp_dataframe_drop.cols).get("variadic", False):
                    return df.drop(*cols)
                else:
                    return df.drop(cols)

            case "sp_dataframe_drop_duplicates":
                df = self.decode_expr(expr.sp_dataframe_drop_duplicates.df)
                cols = list(expr.sp_dataframe_drop_duplicates.cols)
                if expr.sp_dataframe_drop_duplicates.variadic:
                    return df.drop_duplicates(*cols)
                else:
                    return df.drop_duplicates(cols)

            case "sp_dataframe_except":
                df = self.decode_expr(expr.sp_dataframe_except.df)
                other = self.decode_expr(expr.sp_dataframe_except.other)
                return df.except_(other)

            case "sp_dataframe_filter":
                df = self.decode_expr(expr.sp_dataframe_filter.df)
                condition = self.decode_expr(expr.sp_dataframe_filter.condition)
                return df.filter(condition)

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
                cols = self.decode_col_exprs(expr.sp_dataframe_group_by.cols.args)
                if MessageToDict(expr.sp_dataframe_group_by.cols).get(
                    "variadic", False
                ):
                    return df.group_by(*cols)
                else:
                    return df.group_by(cols)

            case "sp_dataframe_group_by_grouping_sets":
                df = self.decode_expr(expr.sp_dataframe_group_by_grouping_sets.df)
                grouping_sets = []
                for set in expr.sp_dataframe_group_by_grouping_sets.grouping_sets:
                    grouping_set = self.decode_col_exprs(set.sets.args)
                    if MessageToDict(set.sets).get("variadic", False):
                        grouping_sets.append(GroupingSets(*grouping_set))
                    else:
                        grouping_sets.append(GroupingSets(grouping_set))
                if MessageToDict(expr.sp_dataframe_group_by_grouping_sets).get(
                    "variadic", False
                ):
                    return df.group_by_grouping_sets(*grouping_sets)
                else:
                    return df.group_by_grouping_sets(grouping_sets)

            case "sp_dataframe_intersect":
                df = self.decode_expr(expr.sp_dataframe_intersect.df)
                other = self.decode_expr(expr.sp_dataframe_intersect.other)
                return df.intersect(other)

            case "sp_dataframe_join":
                d = MessageToDict(expr.sp_dataframe_join)
                join_expr = d.get("joinExpr", None)
                join_expr = (
                    self.decode_expr(expr.sp_dataframe_join.join_expr)
                    if join_expr
                    else None
                )
                join_type = d.get("joinType", None)
                join_type = (
                    self.decode_join_type(expr.sp_dataframe_join.join_type)
                    if join_type
                    else None
                )
                lhs = self.decode_expr(expr.sp_dataframe_join.lhs)
                rhs = self.decode_expr(expr.sp_dataframe_join.rhs)
                lsuffix = d.get("lsuffix", "")
                rsuffix = d.get("rsuffix", "")
                match_condition = d.get("matchCondition", None)
                match_condition = (
                    self.decode_expr(expr.sp_dataframe_join.match_condition)
                    if match_condition
                    else None
                )
                return lhs.join(
                    right=rhs,
                    on=join_expr,
                    how=join_type,
                    lsuffix=lsuffix,
                    rsuffix=rsuffix,
                    match_condition=match_condition,
                )

            case "sp_dataframe_limit":
                df = self.decode_expr(expr.sp_dataframe_limit.df)
                n = expr.sp_dataframe_limit.n
                offset = expr.sp_dataframe_limit.offset
                return df.limit(n, offset)

            case "sp_dataframe_natural_join":
                lhs = self.decode_expr(expr.sp_dataframe_natural_join.lhs)
                rhs = self.decode_expr(expr.sp_dataframe_natural_join.rhs)
                join_type = self.decode_join_type(
                    expr.sp_dataframe_natural_join.join_type
                )
                return lhs.natural_join(right=rhs, how=join_type)

            case "sp_dataframe_na_drop__python":
                df = self.decode_expr(expr.sp_dataframe_na_drop__python.df)
                how = expr.sp_dataframe_na_drop__python.how
                d = MessageToDict(expr.sp_dataframe_na_drop__python)
                thresh = d.get("thresh", None)
                thresh = int(thresh) if thresh is not None else thresh
                subset = d.get("subset", None)
                subset = (
                    list(expr.sp_dataframe_na_drop__python.subset.list)
                    if subset is not None
                    else subset
                )
                return df.na.drop(how, thresh, subset)

            case "sp_dataframe_na_fill":
                df = self.decode_expr(expr.sp_dataframe_na_fill.df)
                # Either value or value_map contains the `value` to fill.
                d = MessageToDict(expr.sp_dataframe_na_fill)
                if "value" in d:
                    value = self.decode_expr(expr.sp_dataframe_na_fill.value)
                else:
                    value = self.decode_dsl_map_expr(
                        expr.sp_dataframe_na_fill.value_map.list
                    )
                subset = d.get("subset", None)
                subset = (
                    list(expr.sp_dataframe_na_fill.subset.list)
                    if subset is not None
                    else subset
                )
                return df.na.fill(value, subset)

            case "sp_dataframe_na_replace":
                df = self.decode_expr(expr.sp_dataframe_na_replace.df)
                d = MessageToDict(expr.sp_dataframe_na_replace)
                # Either value or values contains the `value` to fill.
                if "value" in d:
                    value = self.decode_expr(expr.sp_dataframe_na_replace.value)
                else:
                    value = [
                        self.decode_expr(e)
                        for e in expr.sp_dataframe_na_replace.values.list
                    ]
                # The parameter `replace` can be populated by to_replace_value (single value), to_replace_list,
                # or replacement_map.
                if "toReplaceValue" in d:
                    to_replace = self.decode_expr(
                        expr.sp_dataframe_na_replace.to_replace_value
                    )
                elif "toReplaceList" in d:
                    to_replace = [
                        self.decode_expr(e)
                        for e in expr.sp_dataframe_na_replace.to_replace_list.list
                    ]
                else:
                    to_replace = self.decode_dsl_map_expr(
                        expr.sp_dataframe_na_replace.replacement_map.list
                    )
                subset = d.get("subset", None)
                subset = (
                    list(expr.sp_dataframe_na_replace.subset.list)
                    if subset is not None
                    else subset
                )
                return df.na.replace(to_replace, value, subset)

            case "sp_dataframe_pivot":
                df = self.decode_expr(expr.sp_dataframe_pivot.df)
                pivot_col = self.decode_expr(expr.sp_dataframe_pivot.pivot_col)
                default_on_null = self.decode_expr(
                    expr.sp_dataframe_pivot.default_on_null
                )
                match expr.sp_dataframe_pivot.values.WhichOneof("sealed_value"):
                    case "sp_pivot_value__dataframe":
                        values = self.decode_expr(
                            expr.sp_dataframe_pivot.values.sp_pivot_value__dataframe.v
                        )
                    case "sp_pivot_value__expr":
                        values = self.decode_expr(
                            expr.sp_dataframe_pivot.values.sp_pivot_value__expr.v
                        )
                    case _:
                        raise ValueError(
                            "Unknown pivot value: %s"
                            % expr.sp_dataframe_pivot.values.WhichOneof("sealed_value")
                        )
                return df.pivot(pivot_col, values, default_on_null)

            case "sp_dataframe_random_split":
                df = self.decode_expr(expr.sp_dataframe_random_split.df)
                weights = list(expr.sp_dataframe_random_split.weights)
                seed = expr.sp_dataframe_random_split.seed.value
                statement_params = self.get_statement_params(
                    MessageToDict(expr.sp_dataframe_random_split)
                )
                return df.random_split(weights, seed, statement_params=statement_params)

            case "sp_dataframe_ref":
                return self.symbol_table[expr.sp_dataframe_ref.id.bitfield1][1]

            case "sp_dataframe_rename":
                df = self.decode_expr(expr.sp_dataframe_rename.df)
                col_or_mapper = self.decode_expr(expr.sp_dataframe_rename.col_or_mapper)
                new_column = MessageToDict(expr.sp_dataframe_rename).get(
                    "newColumn", None
                )
                return df.rename(col_or_mapper, new_column)

            case "sp_dataframe_rollup":
                df = self.decode_expr(expr.sp_dataframe_rollup.df)
                cols = self.decode_col_exprs(expr.sp_dataframe_rollup.cols.args)
                if MessageToDict(expr.sp_dataframe_rollup.cols).get("variadic", False):
                    return df.rollup(*cols)
                else:
                    return df.rollup(cols)

            case "sp_dataframe_sample":
                df = self.decode_expr(expr.sp_dataframe_sample.df)
                probability_fraction = (
                    expr.sp_dataframe_sample.probability_fraction.value
                )
                num = expr.sp_dataframe_sample.num.value
                return df.sample(frac=probability_fraction, n=num)

            case "sp_dataframe_select__columns":
                df = self.decode_expr(expr.sp_dataframe_select__columns.df)
                # The columns can be a list of Expr or a single Expr.
                cols = self.decode_col_exprs(expr.sp_dataframe_select__columns.cols)
                if MessageToDict(expr.sp_dataframe_select__columns).get(
                    "variadic", False
                ):
                    val = df.select(*cols)
                else:
                    val = df.select(cols)
                if hasattr(expr, "var_id"):
                    self.symbol_table[expr.var_id.bitfield1] = (
                        self.capture_local_variable_name(expr),
                        val,
                    )
                return val

            case "sp_dataframe_select__exprs":
                df = self.decode_expr(expr.sp_dataframe_select__exprs.df)
                exprs = list(expr.sp_dataframe_select__exprs.exprs)
                if MessageToDict(expr.sp_dataframe_select__exprs).get(
                    "variadic", False
                ):
                    return df.select_expr(*exprs)
                else:
                    return df.select_expr(exprs)

            case "sp_dataframe_show":
                df = self.decode_expr(
                    self.symbol_table[expr.sp_dataframe_show.id.bitfield1][1]
                )
                return df.show()

            case "sp_dataframe_sort":
                df = self.decode_expr(expr.sp_dataframe_sort.df)
                cols = list(
                    self.decode_expr(col) for col in expr.sp_dataframe_sort.cols
                )
                ascending = self.decode_expr(expr.sp_dataframe_sort.ascending)

                if MessageToDict(expr.sp_dataframe_sort).get("colsVariadic", False):
                    return df.sort(*cols, ascending=ascending)
                else:
                    return df.sort(cols, ascending=ascending)

            case "sp_dataframe_stat_approx_quantile":
                d = MessageToDict(expr.sp_dataframe_stat_approx_quantile)
                if "df" in d:
                    df = self.decode_expr(expr.sp_dataframe_stat_approx_quantile.df)
                else:
                    df = self.symbol_table[
                        expr.sp_dataframe_stat_approx_quantile.id.bitfield1
                    ][1]
                cols = [
                    self.decode_expr(col)
                    for col in expr.sp_dataframe_stat_approx_quantile.cols
                ]
                percentile = list(expr.sp_dataframe_stat_approx_quantile.percentile)
                statement_params = self.get_statement_params(d)
                return df._stat.approx_quantile(
                    cols, percentile, statement_params=statement_params
                )

            case "sp_dataframe_stat_corr":
                df = self.symbol_table[expr.sp_dataframe_stat_corr.id.bitfield1][1]
                col1 = self.decode_expr(expr.sp_dataframe_stat_corr.col1)
                col2 = self.decode_expr(expr.sp_dataframe_stat_corr.col2)
                statement_params = self.get_statement_params(
                    MessageToDict(expr.sp_dataframe_stat_corr)
                )
                return df._stat.corr(col1, col2, statement_params=statement_params)

            case "sp_dataframe_stat_cov":
                df = self.symbol_table[expr.sp_dataframe_stat_cov.id.bitfield1][1]
                col1 = self.decode_expr(expr.sp_dataframe_stat_cov.col1)
                col2 = self.decode_expr(expr.sp_dataframe_stat_cov.col2)
                statement_params = self.get_statement_params(
                    MessageToDict(expr.sp_dataframe_stat_cov)
                )
                return df._stat.cov(col1, col2, statement_params=statement_params)

            case "sp_dataframe_stat_cross_tab":
                df = self.symbol_table[expr.sp_dataframe_stat_cross_tab.id.bitfield1][1]
                col1 = self.decode_expr(expr.sp_dataframe_stat_cross_tab.col1)
                col2 = self.decode_expr(expr.sp_dataframe_stat_cross_tab.col2)
                statement_params = self.get_statement_params(
                    MessageToDict(expr.sp_dataframe_stat_cross_tab)
                )
                return df._stat.crosstab(col1, col2, statement_params=statement_params)

            case "sp_dataframe_stat_sample_by":
                df = self.decode_expr(expr.sp_dataframe_stat_sample_by.df)
                col = self.decode_expr(expr.sp_dataframe_stat_sample_by.col)
                fractions = self.decode_dsl_map_expr(
                    expr.sp_dataframe_stat_sample_by.fractions
                )
                return df._stat.sample_by(col, fractions)

            case "sp_dataframe_to_df":
                df = self.decode_expr(expr.sp_dataframe_to_df.df)
                col_names = list(expr.sp_dataframe_to_df.col_names)
                if expr.sp_dataframe_to_df.variadic:
                    return df.to_df(*col_names)
                else:
                    return df.to_df(col_names)

            case "sp_dataframe_to_local_iterator":
                df = self.symbol_table[
                    expr.sp_dataframe_to_local_iterator.id.bitfield1
                ][1]
                statement_params = self.get_statement_params(
                    MessageToDict(expr.sp_dataframe_to_local_iterator)
                )
                block = expr.sp_dataframe_to_local_iterator.block
                case_sensitive = expr.sp_dataframe_to_local_iterator.case_sensitive
                return df.to_local_iterator(
                    statement_params=statement_params,
                    block=block,
                    case_sensitive=case_sensitive,
                )

            case "sp_dataframe_to_pandas":
                df = self.symbol_table[expr.sp_dataframe_to_pandas.id.bitfield1][1]
                statement_params = self.get_statement_params(
                    MessageToDict(expr.sp_dataframe_to_pandas)
                )
                block = expr.sp_dataframe_to_pandas.block
                return df.to_pandas(statement_params=statement_params, block=block)

            case "sp_dataframe_to_pandas_batches":
                df = self.symbol_table[
                    expr.sp_dataframe_to_pandas_batches.id.bitfield1
                ][1]
                statement_params = self.get_statement_params(
                    MessageToDict(expr.sp_dataframe_to_pandas_batches)
                )
                block = expr.sp_dataframe_to_pandas_batches.block
                return df.to_pandas_batches(
                    statement_params=statement_params, block=block
                )

            case "sp_dataframe_union":
                df = self.decode_expr(expr.sp_dataframe_union.df)
                other = self.decode_expr(expr.sp_dataframe_union.other)
                return df.union(other)

            case "sp_dataframe_union_all":
                df = self.decode_expr(expr.sp_dataframe_union_all.df)
                other = self.decode_expr(expr.sp_dataframe_union_all.other)
                return df.union_all(other)

            case "sp_dataframe_union_all_by_name":
                df = self.decode_expr(expr.sp_dataframe_union_all_by_name.df)
                other = self.decode_expr(expr.sp_dataframe_union_all_by_name.other)
                return df.union_all_by_name(other)

            case "sp_dataframe_union_by_name":
                df = self.decode_expr(expr.sp_dataframe_union_by_name.df)
                other = self.decode_expr(expr.sp_dataframe_union_by_name.other)
                return df.union_by_name(other)

            case "sp_dataframe_unpivot":
                df = self.decode_expr(expr.sp_dataframe_unpivot.df)
                column_list = [
                    self.decode_expr(e) for e in expr.sp_dataframe_unpivot.column_list
                ]
                name_column = expr.sp_dataframe_unpivot.name_column
                value_column = expr.sp_dataframe_unpivot.value_column
                # TODO SNOW-1866100: add logic for `include_nulls`.
                return df.unpivot(value_column, name_column, column_list)

            case "sp_dataframe_with_column":
                df = self.decode_expr(expr.sp_dataframe_with_column.df)
                col_name = expr.sp_dataframe_with_column.col_name
                col = self.decode_expr(expr.sp_dataframe_with_column.col)
                return df.with_column(col_name, col)

            case "sp_dataframe_with_column_renamed":
                df = self.decode_expr(expr.sp_dataframe_with_column_renamed.df)
                existing = self.decode_expr(expr.sp_dataframe_with_column_renamed.col)
                new = expr.sp_dataframe_with_column_renamed.new_name
                return df.with_column_renamed(existing, new)

            case "sp_dataframe_with_columns":
                df = self.decode_expr(expr.sp_dataframe_with_columns.df)
                col_names = list(expr.sp_dataframe_with_columns.col_names)
                values = [
                    self.decode_expr(e) for e in expr.sp_dataframe_with_columns.values
                ]
                return df.with_columns(col_names, values)

            case "sp_relational_grouped_dataframe_agg":
                grouped_df = self.decode_expr(
                    expr.sp_relational_grouped_dataframe_agg.grouped_df
                )
                exprs = self.decode_col_exprs(
                    expr.sp_relational_grouped_dataframe_agg.exprs.args
                )
                if MessageToDict(expr.sp_relational_grouped_dataframe_agg.exprs).get(
                    "variadic", False
                ):
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
                agg_name = expr.sp_relational_grouped_dataframe_builtin.agg_name
                if "cols" not in MessageToDict(
                    expr.sp_relational_grouped_dataframe_builtin
                ):
                    return getattr(grouped_df, agg_name)()
                cols = self.decode_col_exprs(
                    expr.sp_relational_grouped_dataframe_builtin.cols.args
                )
                if MessageToDict(expr.sp_relational_grouped_dataframe_builtin.cols).get(
                    "variadic", False
                ):
                    return getattr(grouped_df, agg_name)(*cols)
                else:
                    return getattr(grouped_df, agg_name)(cols)

            case "sp_relational_grouped_dataframe_ref":
                return self.symbol_table[
                    expr.sp_relational_grouped_dataframe_ref.id.bitfield1
                ][1]

            case "sp_session_table_function":
                # Here, self.decode_expr will most likely run Session.call since Session.call does not have an explicit
                # AST entity. To prevent Session.call from running, give context to self.decode_expr that the caller is
                # sp_session_table_function entity via kwargs.
                kwargs = {"source": "sp_session_table_function"}
                return self.decode_expr(expr.sp_session_table_function.fn, **kwargs)

            case "sp_table":
                assert expr.sp_table.HasField("name")
                table_name = self.decode_name_expr(expr.sp_table.name)
                return self.session.table(table_name)

            case "sp_to_snowpark_pandas":
                df = self.decode_expr(expr.sp_to_snowpark_pandas.df)
                d = MessageToDict(expr.sp_to_snowpark_pandas)
                index_col, columns = None, None
                if "indexCol" in d:
                    index_col = [
                        col for col in expr.sp_to_snowpark_pandas.index_col.list
                    ]
                if "columns" in d:
                    columns = [col for col in expr.sp_to_snowpark_pandas.columns.list]
                # Returning the result of to_snowpark_pandas causes recursion issues when local_testing_mode is enabled.
                # When disabled, to_snowpark_pandas will raise an error since df will be an empty Dataframe
                # (passing non-None values of index_col or columns will make the snowpark_to_pandas_helper complain
                # about columns that do not exist).
                # Therefore, silently execute to_snowpark_pandas to record the AST and return None.
                df.to_snowpark_pandas(index_col, columns)
                return None

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
                name = self.decode_name_expr(
                    expr.sp_dataframe_create_or_replace_view.name
                )
                if not isinstance(name, str):
                    name = self.convert_name_to_list(name)
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
                name = self.decode_name_expr(
                    expr.sp_dataframe_copy_into_table.table_name
                )
                name = self.convert_name_to_list(name)
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
                name = self.decode_name_expr(
                    expr.sp_dataframe_create_or_replace_dynamic_table.name
                )
                if not isinstance(name, str):
                    name = self.convert_name_to_list(name)
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

            case "stored_procedure":
                input_types = [
                    self.decode_data_type_expr(input_type)
                    for input_type in expr.stored_procedure.input_types.list
                ]
                execute_as = expr.stored_procedure.execute_as
                comment = expr.stored_procedure.comment.value
                registered_object_name = self.decode_name_expr(
                    expr.stored_procedure.func.object_name
                )
                return_type = self.decode_data_type_expr(
                    expr.stored_procedure.return_type
                )
                ret_sproc = sproc(
                    lambda *args: None,
                    return_type=return_type,
                    input_types=input_types,
                    execute_as=execute_as,
                    comment=comment,
                    _registered_object_name=registered_object_name,
                )
                return ret_sproc

            case "sp_flatten":
                input = self.decode_expr(expr.sp_flatten.input)

                path = expr.sp_flatten.path.value

                outer = expr.sp_flatten.outer

                recursive = expr.sp_flatten.recursive

                mode = "BOTH"
                match expr.sp_flatten.mode.WhichOneof("variant"):
                    case "sp_flatten_mode_both":
                        mode = "BOTH"
                    case "sp_flatten_mode_array":
                        mode = "ARRAY"
                    case "sp_flatten_mode_object":
                        mode = "OBJECT"

                if len(path) == 0:

                    return self.session.flatten(
                        input=input, outer=outer, recursive=recursive, mode=mode
                    )

                return self.session.flatten(
                    input=input, path=path, outer=outer, recursive=recursive, mode=mode
                )

            case "sp_generator":
                columns = [self.decode_expr(col) for col in expr.sp_generator.columns]
                row_count = expr.sp_generator.row_count
                time_limit_seconds = expr.sp_generator.time_limit_seconds
                if expr.sp_generator.variadic:
                    return self.session.generator(
                        *columns, rowcount=row_count, timelimit=time_limit_seconds
                    )
                else:
                    return self.session.generator(
                        columns, rowcount=row_count, timelimit=time_limit_seconds
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
