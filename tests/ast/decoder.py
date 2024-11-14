#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import logging
from base64 import b64decode
from typing import Any, Optional, Iterable
from datetime import datetime
from dateutil.tz import gettz

import snowflake.snowpark._internal.proto.generated.ast_pb2 as proto

from snowflake.snowpark import Session
import snowflake.snowpark.functions

logger = logging.getLogger(__name__)


class Decoder:
    def __init__(self, session: Optional[Session]):
        self.symbol_table = dict()  # Map from var_id to value
        try:
            self.session = session if session is not None else Session.builder.create()
        except Exception as e:
            self.session = None
            logger.warning("Error creating a Snowpark session for the decoder: %s", e)

    def decode_dsl_map_expr(self, map_expr: Iterable) -> dict:
        """
        Given a map expression, return the result as a Python dictionary.
        Under the hood, protoc converts the key-value pairs into a list of Tuple_X_Y.

        Parameters
        ----------
        map_expr : Iterable[proto.Tuple_X_Y]
            The map expression to decode.
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

    def decode_fn_name(self, fn_name: proto.FnName) -> str:
        """
        Decode a function name to get the function name.

        Parameters
        ----------
        fn_name : proto.FnName
            The function name to decode.
        """
        if hasattr(fn_name, "fn_name_flat"):
            return fn_name.fn_name_flat.name
        elif hasattr(fn_name, "fn_name_structured"):
            return fn_name.fn_name_structured.name
        else:
            raise ValueError("Function name not found in proto.FnName")

    def decode_table_name(self, table_name: proto.SpTableName) -> str:
        """
        Decode a table name to get the table name.

        Parameters
        ----------
        table_name : proto.SpTableName
            The table name to decode.
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
        """
        match fn_ref_expr.WhichOneof("variant"):
            # case "trait_fn_id_ref_expr":
            #     pass
            # case "trait_fn_name_ref_expr":
            #     pass
            case "builtin_fn":
                return self.decode_fn_name(fn_ref_expr.builtin_fn.name)
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

    def decode_dataframe_data(self, df_data_expr: proto.SpDataframeData) -> Any:
        """
        Decode and obtain the data from SpDataframeData.

        Parameters
        ----------
        df_data_expr : proto.SpDataframeData
            The expr to decode.
        """
        match df_data_expr.WhichOneof("sealed_value"):
            case "sp_dataframe_data__list":
                # vs can be a list of Expr or a single Expr.
                if isinstance(df_data_expr.sp_dataframe_data__list.vs, Iterable):
                    return [
                        self.decode_expr(v)
                        for v in df_data_expr.sp_dataframe_data__list.vs
                    ]
                else:
                    return [self.decode_expr(df_data_expr.sp_dataframe_data__list.vs)]
            # case "sp_dataframe_data__pandas":
            #     pass
            # case "sp_dataframe_data__tuple":
            #     pass
            case _:
                raise ValueError(
                    "Unknown dataframe data type: %s"
                    % df_data_expr.WhichOneof("variant")
                )

    def decode_dataframe_schema(self, df_schema_expr: proto.SpDataframeSchema) -> Any:
        """
        Decode and obtain the schema from SpDataframeSchema.

        Parameters
        ----------
        df_schema_expr : proto.SpDataframeSchema
            The expr to decode.
        """
        match df_schema_expr.WhichOneof("sealed_value"):
            case "sp_dataframe_schema__list":
                # vs can be a list of Expr or a single Expr.
                if isinstance(df_schema_expr.sp_dataframe_schema__list.vs, Iterable):
                    return [v for v in df_schema_expr.sp_dataframe_schema__list.vs]
                else:
                    return [df_schema_expr.sp_dataframe_schema__list.vs]
            # case "sp_dataframe_schema__struct":
            #     pass
            case _:
                raise ValueError(
                    "Unknown dataframe schema type: %s"
                    % df_schema_expr.WhichOneof("variant")
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
        timezone = gettz(tz_name)
        # Ensure that the correct timezone has been retrieved.
        assert timezone.utcoffset(datetime.now()).total_seconds() == offset_seconds
        return timezone

    def decode_expr(self, expr: proto.Expr) -> Any:
        match expr.WhichOneof("variant"):
            case "add":
                lhs = self.decode_expr(expr.add.lhs)
                rhs = self.decode_expr(expr.add.rhs)
                return lhs + rhs
            case "apply_expr":
                fn_name = self.decode_fn_ref_expr(expr.apply_expr.fn)
                fn = getattr(snowflake.snowpark.functions, fn_name)
                # The named arguments are stored as a list of Tuple_String_Expr.
                named_args = self.decode_dsl_map_expr(expr.apply_expr.named_args)
                # The positional args can be a list of Expr or a single Expr.
                if isinstance(expr.apply_expr.pos_args, Iterable):
                    pos_args = [
                        self.decode_expr(pos_arg)
                        for pos_arg in expr.apply_expr.pos_args
                    ]
                else:
                    pos_args = [self.decode_expr(expr.apply_expr.pos_args)]
                result = fn(*pos_args, **named_args)
                if hasattr(expr, "var_id"):
                    self.symbol_table[expr.var_id.bitfield1] = result
                return result
            case "float64_val":
                return expr.float64_val.v
            case "int64_val":
                return expr.int64_val.v
            case "list_val":
                # vs can be a list of Expr or a single Expr.
                if isinstance(expr.list_val.vs, Iterable):
                    return [self.decode_expr(v) for v in expr.list_val.vs]
                else:
                    return [self.decode_expr(expr.list_val.vs)]
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
            case "sp_create_dataframe":
                data = self.decode_dataframe_data(expr.sp_create_dataframe.data)
                schema = self.decode_dataframe_schema(expr.sp_create_dataframe.schema)
                df = self.session.create_dataframe(data=data, schema=schema)
                if hasattr(expr, "var_id"):
                    self.symbol_table[expr.var_id.bitfield1] = df
                return df
            case "sp_dataframe_col":
                col_name = expr.sp_dataframe_col.col_name
                df = self.decode_expr(expr.sp_dataframe_col.df)
                return df[col_name]
            case "sp_dataframe_ref":
                return self.symbol_table[expr.sp_dataframe_ref.id.bitfield1]
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
                val = df.select(cols)
                if hasattr(expr, "var_id"):
                    self.symbol_table[expr.var_id.bitfield1] = val
                return val
            case "sp_table":
                assert expr.sp_table.HasField("name")
                table_name = self.decode_table_name(expr.sp_table.name)
                return self.session.table(table_name)
            case "string_val":
                return expr.string_val.v
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
                if isinstance(
                    val, (snowflake.snowpark.DataFrame, snowflake.snowpark.table.Table)
                ):
                    print(val.show())
                self.symbol_table[stmt.assign.var_id.bitfield1] = val
                logger.info(
                    f"assign result {val} at var_id {stmt.assign.var_id.bitfield1}"
                )
                print(f"assign result {val} at var_id {stmt.assign.var_id.bitfield1}")

            case "eval":
                val = self.symbol_table[stmt.parameters[0]]
                logger.info(f"eval result: {val}")
            case _:
                raise ValueError(
                    "Unknown statement type: %s" % stmt.WhichOneof("variant")
                )


def decode_ast(ast: str, session: Optional[Session] = None):
    decoder = Decoder(session)
    decoder.session.use_warehouse("TESTWH_PYTHON")
    create_df = decoder.session.create_dataframe(
        data=[
            [1, "one"],
            [2, "two"],
            [3, "three"],
        ],
        schema=["A", "str"],
    )
    create_df.write.save_as_table("table1", mode="overwrite")

    # Turn base64 input into protobuf objects. ParseFromString can retrieve multiple statements.
    protobuf_request = proto.Request()
    protobuf_request.ParseFromString(b64decode(ast))

    # protobuf_request.body is a repeated field of assign/eval statements.
    for stmt in protobuf_request.body:
        decoder.decode_stmt(stmt)


def main():
    # 'EAEaERIPCg0KBWZpbmFsEAMYCCATIgQQARgX'  # this gives system info

    # select.test
    # input = "Cj8KPQovggwsEgoKCAoGdGFibGUxGhoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoGSICCAESBAoCZGYYASICCAEK4wEK4AEK0QGCCc0BClKSAU8KCxoJCgcKBQoDY29sGiT6DCEKGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSgbEgNTVFIiGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSgbClCSAU0KCxoJCgcKBQoDY29sGiL6DB8KGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSgbEgFBIhoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoGxIHggIECgIIARoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKBsgARIECgJkZhgCIgIIAhABGhESDwoNCgVmaW5hbBADGAogDyIEEAEYFw=="

    # short interval example
    # input = "CvkCCvYCCuYC+gXiAgq5Agq2AgqYAdIClAEKGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSgbEjr6AzcIASgBOhoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoG0ISCgUKA0VTVBCw8/7///////8BSNoPEjr6AzcIASgBOhoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoG0ISCgUKA0VTVBCw8/7///////8BSNsPCpgB0gKUAQoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKBsSOvoDNwgBKAE6GhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSgbQhIKBQoDRVNUELDz/v///////wFI3A8SOvoDNwgBKAE6GhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSgbQhIKBQoDRVNUELDz/v///////wFI3Q8SCAoGCgFhCgFiGhoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoGxIFCgNkZjEYASICCAEKmwUKmAUKiAWCCYQFCtoEggHWBAorygYoCgFhEgeCAgQKAggBGhoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoJBKKBJIBhgQKFRoTChEKDwoNbWFrZV9pbnRlcnZhbBItCghxdWFydGVycxIhwgIeChoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoJRABEisKBm1vbnRocxIhwgIeChoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoJRABEioKBXdlZWtzEiHCAh4KGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSglEAISKQoEZGF5cxIhwgIeChoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoJRACEioKBWhvdXJzEiHCAh4KGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSglEAISLAoHbWludXRlcxIhwgIeChoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoJRADEiwKB3NlY29uZHMSIcICHgoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKCUQAxIxCgxtaWxsaXNlY29uZHMSIcICHgoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKCUQAxIxCgxtaWNyb3NlY29uZHMSIcICHgoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKCUQBBIwCgtuYW5vc2Vjb25kcxIhwgIeChoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoJRAEIhoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoJRoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKCQSB4ICBAoCCAEaGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSgjIAESBQoDZGYyGAIiAggCEAEaERIPCg0KBWZpbmFsEAMYCiAPIgQQARgX"

    # interval.test
    # input = "CvkCCvYCCuYC+gXiAgq5Agq2AgqYAdIClAEKGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSgbEjr6AzcIASgBOhoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoG0ISCgUKA0VTVBCw8/7///////8BSNoPEjr6AzcIASgBOhoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoG0ISCgUKA0VTVBCw8/7///////8BSNsPCpgB0gKUAQoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKBsSOvoDNwgBKAE6GhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSgbQhIKBQoDRVNUELDz/v///////wFI3A8SOvoDNwgBKAE6GhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSgbQhIKBQoDRVNUELDz/v///////wFI3Q8SCAoGCgFhCgFiGhoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoGxIFCgNkZjEYASICCAEKmwUKmAUKiAWCCYQFCtoEggHWBAorygYoCgFhEgeCAgQKAggBGhoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoJBKKBJIBhgQKFRoTChEKDwoNbWFrZV9pbnRlcnZhbBItCghxdWFydGVycxIhwgIeChoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoJRABEisKBm1vbnRocxIhwgIeChoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoJRABEioKBXdlZWtzEiHCAh4KGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSglEAISKQoEZGF5cxIhwgIeChoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoJRACEioKBWhvdXJzEiHCAh4KGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSglEAISLAoHbWludXRlcxIhwgIeChoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoJRADEiwKB3NlY29uZHMSIcICHgoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKCUQAxIxCgxtaWxsaXNlY29uZHMSIcICHgoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKCUQAxIxCgxtaWNyb3NlY29uZHMSIcICHgoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKCUQBBIwCgtuYW5vc2Vjb25kcxIhwgIeChoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoJRAEIhoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoJRoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKCQSB4ICBAoCCAEaGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSgjIAESBQoDZGYyGAIiAggCCvMBCvABCuABggncAQqyAYIBrgEKK8oGKAoBYRIHggIECgIIARoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKDMSY5IBYAoVGhMKEQoPCg1tYWtlX2ludGVydmFsEisKBXllYXJzEiLCAh8KGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSgzENIJIhoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoMxoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKDMSB4ICBAoCCAEaGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSgzIAESBQoDZGY0GAMiAggDCoMECoAECvADggnsAwrCA4IBvgMKK8oGKAoBYRIHggIECgIIARoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKDYS8gKSAe4CChUaEwoRCg8KDW1ha2VfaW50ZXJ2YWwSLQoIcXVhcnRlcnMSIcICHgoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKDcQARIrCgZtb250aHMSIcICHgoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKDcQAhIqCgV3ZWVrcxIhwgIeChoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoNxADEikKBGRheXMSIcICHgoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKDcQBBIqCgVob3VycxIhwgIeChoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoNxAFEiwKB21pbnV0ZXMSIcICHgoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKDcQBhIsCgdzZWNvbmRzEiHCAh4KGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSg3EAciGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSg3GhoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoNhIHggIECgIIARoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKDUgARIFCgNkZjUYBCICCAQKmAUKlQUKhQWCCYEFCtcEggHTBAorygYoCgFhEgeCAgQKAggBGhoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoQxKHBJIBgwQKFRoTChEKDwoNbWFrZV9pbnRlcnZhbBIqCgV5ZWFycxIhwgIeChoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoRBABEisKBm1vbnRocxIhwgIeChoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoRBACEioKBXdlZWtzEiHCAh4KGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERShEEAMSKQoEZGF5cxIhwgIeChoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoRBAEEioKBWhvdXJzEiHCAh4KGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERShEEAUSLAoHbWludXRlcxIhwgIeChoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoRBAGEiwKB3NlY29uZHMSIcICHgoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKEQQBxIxCgxtaWxsaXNlY29uZHMSIcICHgoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKEQQCBIxCgxtaWNyb3NlY29uZHMSIcICHgoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKEQQCRIwCgtuYW5vc2Vjb25kcxIhwgIeChoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoRBAKIhoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoRBoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKEMSB4ICBAoCCAEaGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERShCIAESBQoDZGY2GAUiAggFCp0CCpoCCooCggmGAgrcAYIB2AEKK8oGKAoBYRIHggIECgIIARoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKFISjAGSAYgBChUaEwoRCg8KDW1ha2VfaW50ZXJ2YWwSKgoFd2Vla3MSIcICHgoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKFIQFRInCgRtaW5zEh/CAhwKGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERShSIhoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoUhoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKFISB4ICBAoCCAEaGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERShSIAESBQoDZGY3GAYiAggGEAEaERIPCg0KBWZpbmFsEAMYCiAPIgQQARgX"

    input = "Cj8KPQovggwsEgoKCAoGdGFibGUxGhoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoGyICCAESBAoCZGYYASICCAEKuwEKuAEKqAGCCaQBCnuiBHgKUJIBTQoLGgkKBwoFCgNjb2waIvoMHwoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKB0SAUEiGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSgdEgR0ZXN0GhoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoHSICCAESB4ICBAoCCAEaGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSgdIAESBQoDZGYxGAIiAggCCrkBCrYBCqYBggmiAQp5ogR2ClCSAU0KCxoJCgcKBQoDY29sGiL6DB8KGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSgfEgFBIhoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoHxIEdGVzdBoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKB8iABIHggIECgIIARoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKB8gARIFCgNkZjEYAyICCAMKtwEKtAEKpAGCCaABCneiBHQKUJIBTQoLGgkKBwoFCgNjb2waIvoMHwoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKCESAUEiGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSghEgR0ZXN0GhoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoIRIHggIECgIIARoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKCEgARIFCgNkZjEYBCICCAQK/wEK/AEK7AGCCegBCr4BogS6AQqVAYIBkQEKUJIBTQoLGgkKBwoFCgNjb2waIvoMHwoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKCMSAUEiGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSgjEiHCAh4KGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSgjEAEaGhoWU1JDX1BPU0lUSU9OX1RFU1RfTU9ERSgjEgR0ZXN0GhoaFlNSQ19QT1NJVElPTl9URVNUX01PREUoIxIHggIECgIIARoaGhZTUkNfUE9TSVRJT05fVEVTVF9NT0RFKCMgARIFCgNkZjEYBSICCAUQARoREg8KDQoFZmluYWwQAxgKIA8iBBABGBc="
    decode_ast(input)


if __name__ == "__main__":
    main()
