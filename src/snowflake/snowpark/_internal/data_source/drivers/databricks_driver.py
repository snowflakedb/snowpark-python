#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import logging
from typing import List, Any, TYPE_CHECKING

from snowflake.snowpark._internal.data_source.datasource_typing import (
    Cursor,
)
from snowflake.snowpark._internal.data_source.drivers import BaseDriver
from snowflake.snowpark._internal.type_utils import type_string_to_type_object
from snowflake.snowpark.functions import column, to_variant, parse_json
from snowflake.snowpark.types import (
    StructType,
    MapType,
    StructField,
    ArrayType,
    VariantType,
    TimestampType,
    TimestampTimeZone,
)

if TYPE_CHECKING:
    from snowflake.snowpark.session import Session  # pragma: no cover
    from snowflake.snowpark.dataframe import DataFrame  # pragma: no cover


logger = logging.getLogger(__name__)


class DatabricksDriver(BaseDriver):
    def get_raw_schema(
        self,
        table_or_query: str,
        cursor: "Cursor",
        is_query: bool,
        query_input_alias: str,
    ) -> None:
        # The following query gives a more detailed schema information than
        # just running "SELECT * FROM {table_or_query} WHERE 1 = 0"
        query = f"DESCRIBE QUERY SELECT * FROM ({table_or_query})"
        logger.debug(f"trying to get schema using query: {query}")
        raw_schema = cursor.execute(query).fetchall()
        self.raw_schema = raw_schema

    def to_snow_type(self, schema: List[Any]) -> StructType:
        # https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-describe-query
        # https://docs.databricks.com/aws/en/dev-tools/python-sql-connector#type-conversions
        # other types are the same as snowflake
        convert_map_to_use = {
            "tinyint": "byteint",
            "interval year to month": "string",
            "interval day to second": "string",
        }
        # TODO: SNOW-2044265 today we store databricks structured data as variant in regular table, in the
        #  future we might want native structured data support on iceberg table
        all_columns = []
        for column_name, column_type, _ in schema:
            column_type = convert_map_to_use.get(column_type, column_type)
            data_type = type_string_to_type_object(column_type)
            if column_type.lower() == "timestamp":
                # by default https://docs.databricks.com/aws/en/sql/language-manual/data-types/timestamp-type
                data_type = TimestampType(TimestampTimeZone.LTZ)
            all_columns.append(StructField(column_name, data_type, True))
        return StructType(all_columns)

    def udtf_class_builder(
        self, fetch_size: int = 1000, schema: StructType = None
    ) -> type:
        create_connection = self.create_connection

        class UDTFIngestion:
            def process(self, query: str):
                conn = create_connection()
                cursor = conn.cursor()

                # First get schema information
                describe_query = f"DESCRIBE QUERY SELECT * FROM ({query})"
                cursor.execute(describe_query)
                schema_info = cursor.fetchall()

                # Find which columns are array types based on column type description
                # databricks-sql-connector does not provide built-in output handler nor databricks provide simple
                # built-in function to do the transformation meeting our snowflake table requirement
                # from nd.array to list
                array_column_indices = []
                for idx, (_, column_type, _) in enumerate(schema_info):
                    if column_type.startswith("array<"):
                        array_column_indices.append(idx)

                # Execute the actual query
                cursor.execute(query)
                while True:
                    rows = cursor.fetchmany(fetch_size)
                    if not rows:
                        break
                    processed_rows = []
                    for row in rows:
                        processed_row = list(row)
                        # Handle array columns - convert ndarray to list
                        for idx in array_column_indices:
                            if (
                                idx < len(processed_row)
                                and processed_row[idx] is not None
                            ):
                                processed_row[idx] = processed_row[idx].tolist()

                        processed_rows.append(tuple(processed_row))
                    yield from processed_rows

        return UDTFIngestion

    @staticmethod
    def to_result_snowpark_df(
        session: "Session", table_name, schema, _emit_ast: bool = True
    ) -> "DataFrame":
        project_columns = []
        for field in schema.fields:
            if isinstance(
                field.datatype, (MapType, ArrayType, StructType, VariantType)
            ):
                project_columns.append(to_variant(column(field.name)).as_(field.name))
            else:
                project_columns.append(
                    column(field.name).cast(field.datatype).alias(field.name)
                )
        return session.table(table_name, _emit_ast=_emit_ast).select(
            project_columns, _emit_ast=_emit_ast
        )

    @staticmethod
    def to_result_snowpark_df_udtf(
        res_df: "DataFrame",
        schema: StructType,
        _emit_ast: bool = True,
    ):
        cols = []
        for field in schema.fields:
            if isinstance(
                field.datatype, (MapType, ArrayType, StructType, VariantType)
            ):
                cols.append(to_variant(parse_json(column(field.name))).as_(field.name))
            else:
                cols.append(res_df[field.name].cast(field.datatype).alias(field.name))
        return res_df.select(cols, _emit_ast=_emit_ast)
