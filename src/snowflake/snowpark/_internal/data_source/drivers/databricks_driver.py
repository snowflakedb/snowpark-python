#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import json
import logging
from typing import List, Any, TYPE_CHECKING

from snowflake.snowpark._internal.data_source.drivers import BaseDriver
from snowflake.snowpark._internal.type_utils import type_string_to_type_object
from snowflake.snowpark._internal.utils import PythonObjJSONEncoder
from snowflake.snowpark.exceptions import SnowparkDataframeReaderException
from snowflake.snowpark.types import (
    StructType,
    MapType,
    StructField,
    ArrayType,
    VariantType,
)
from snowflake.snowpark.functions import column, to_variant
from snowflake.connector.options import pandas as pd

if TYPE_CHECKING:
    from snowflake.snowpark.session import Session  # pragma: no cover
    from snowflake.snowpark.dataframe import DataFrame  # pragma: no cover


logger = logging.getLogger(__name__)


class DatabricksDriver(BaseDriver):
    def infer_schema_from_description(self, table_or_query: str) -> StructType:
        conn = self.create_connection()
        cursor = conn.cursor()
        try:
            # The following query gives a more detailed schema information than
            # just running "SELECT * FROM {table_or_query} WHERE 1 = 0"
            query = f"DESCRIBE QUERY SELECT * FROM ({table_or_query})"
            logger.debug(f"trying to get schema using query: {query}")
            raw_schema = cursor.execute(query).fetchall()
            return self.to_snow_type(raw_schema)
        except Exception as exc:
            raise SnowparkDataframeReaderException(
                f"Failed to infer Snowpark DataFrame schema from '{table_or_query}' due to {exc!r}.\n"
                f"To avoid auto inference, you can manually specify the Snowpark DataFrame schema using 'custom_schema' in `DataFrameReader.dbapi`.\n"
                f"Please check the stack trace for more details."
            ) from exc
        finally:
            cursor.close()
            conn.close()

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
            all_columns.append(StructField(column_name, data_type, True))
        return StructType(all_columns)

    @staticmethod
    def data_source_data_to_pandas_df(
        data: List[Any], schema: StructType
    ) -> "pd.DataFrame":
        df = BaseDriver.data_source_data_to_pandas_df(data, schema)
        # 1. Regular snowflake table (compared to Iceberg Table) does not support structured data
        # type (array, map, struct), thus we store structured data as variant in regular table
        # 2. map type needs special handling because:
        #   i. databricks sql returned it as a list of tuples, which needs to be converted to a dict
        #   ii. pandas parquet conversion does not support dict having int as key, we convert it to json string
        map_type_indexes = [
            i
            for i, field in enumerate(schema.fields)
            if isinstance(field.datatype, MapType)
        ]
        col_names = df.columns[map_type_indexes]
        df[col_names] = BaseDriver.df_map_method(df[col_names])(
            lambda x: json.dumps(dict(x), cls=PythonObjJSONEncoder)
        )
        return df

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
                project_columns.append(column(field.name))
        return session.table(table_name, _emit_ast=_emit_ast).select(
            project_columns, _emit_ast=_emit_ast
        )
