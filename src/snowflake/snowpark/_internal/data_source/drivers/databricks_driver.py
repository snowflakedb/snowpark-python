#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
from typing import List, Any

from snowflake.snowpark._internal.data_source.drivers import BaseDriver
from snowflake.snowpark.types import (
    ByteType,
    ShortType,
    StructType,
    LongType,
    StringType,
    IntegerType,
    DecimalType,
    BooleanType,
    DateType,
    DoubleType,
    TimestampType,
    FloatType,
    BinaryType,
    ArrayType,
    MapType,
    StructField,
)
from snowflake.connector.options import pandas as pd


logger = logging.getLogger(__name__)


class DatabricksDriver(BaseDriver):
    def to_snow_type(self, schema) -> StructType:
        # https://docs.databricks.com/aws/en/dev-tools/python-sql-connector#type-conversions#
        convert_map_to_use = {
            "tinyint": ByteType,
            "smallint": ShortType,
            "int": IntegerType,
            "bigint": LongType,
            "float": FloatType,
            "double": DoubleType,
            "decimal": DecimalType,
            "string": StringType,
            "boolean": BooleanType,
            "binary": BinaryType,
            "date": DateType,
            "timestamp": TimestampType,
            "array": ArrayType,
            "map": MapType,
            "struct": StructType,
        }

        fields = []
        for column in schema:
            # https://docs.databricks.com/aws/en/dev-tools/python-sql-connector#cursor-class
            name = column[0]
            type_code = column[1]
            snow_type = convert_map_to_use.get(type_code, None)
            if snow_type is None:
                raise NotImplementedError(
                    f"databricks sql type not supported: {type_code}"
                )
            if snow_type == DecimalType:
                precision = column[4]
                scale = column[5]
                if not self.validate_numeric_precision_scale(precision, scale):
                    logger.debug(
                        f"Snowpark does not support column"
                        f" {name} of type {type_code} with precision {precision} and scale {scale}. "
                        "The default Numeric precision and scale will be used."
                    )
                    precision, scale = None, None
                    data_type = snow_type(
                        precision if precision is not None else 38,
                        scale if scale is not None else 0,
                    )
            else:
                data_type = snow_type()
            fields.append(StructField(name, data_type, True))
        return StructType(fields)

    @staticmethod
    def data_source_data_to_pandas_df(
        data: List[Any], schema: StructType
    ) -> "pd.DataFrame":
        df = BaseDriver.data_source_data_to_pandas_df(data, schema)

        # The following code is to handle MapType in databricks-sql-connector which returns value of list type.
        # list can not be directly converted to MapType in snowflake, so we need to convert it to a dictionary first.

        # Get the indexes of columns in the schema whose type is MapType
        map_type_indexes = [
            idx
            for idx, field in enumerate(schema.fields)
            if isinstance(field.datatype, MapType)
        ]

        # Apply the conversion to dictionary for the columns whose type is MapType
        for idx in map_type_indexes:
            column_name = schema.fields[idx].name
            df[column_name] = df[column_name].apply(
                lambda x: dict(x) if x is not None else None
            )

        return df
