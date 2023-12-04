#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import math
from typing import TYPE_CHECKING, Any, List, Tuple

from snowflake.connector.options import pandas as pd
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    quote_name_without_upper_casing,
)
from snowflake.snowpark._internal.type_utils import infer_type
from snowflake.snowpark.exceptions import SnowparkClientException
from snowflake.snowpark.table import Table
from snowflake.snowpark.types import (
    ArrayType,
    BooleanType,
    DecimalType,
    DoubleType,
    LongType,
    MapType,
    NullType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    VariantType,
)

if TYPE_CHECKING:
    from snowflake.snowpark import DataFrame, Session


def _extract_schema_and_data_from_pandas_df(
    data: "pd.DataFrame",
) -> Tuple[StructType, List[List[Any]]]:
    """
    infer column types from the pandas data
    when running against snowflake, infer_schema (https://docs.snowflake.com/en/sql-reference/functions/infer_schema)
    is used to infer schema.

    pandas type related doc: https://pandas.pydata.org/docs/user_guide/basics.html#dtypes
    """
    import numpy

    # PANDAS_INTEGER_TYPES defined here to avoid module level referencing pandas lib
    # as pandas is optional to snowpark-python
    PANDAS_INTEGER_TYPES = (
        pd.Int8Dtype,
        pd.Int16Dtype,
        pd.Int32Dtype,
        pd.Int64Dtype,
        pd.UInt8Dtype,
        pd.UInt16Dtype,
        pd.UInt32Dtype,
        pd.UInt64Dtype,
    )

    col_names = [
        quote_name_without_upper_casing(name) for name in data.columns.values.tolist()
    ]
    plain_data = [data.iloc[i].tolist() for i in range(data.shape[0])]
    inferred_type_dict = (
        {}
    )  # this map is to store types for columns in which data are of primitive python objects
    for row_idx in range(data.shape[0]):
        for col_idx in range(data.shape[1]):
            if plain_data[row_idx][col_idx] is None:
                continue
            if isinstance(plain_data[row_idx][col_idx], (float, numpy.float_)):
                # in pandas, a float is represented in type numpy.float64
                # which can not be inferred by snowpark python, we cast to built-in float type
                if math.isnan(plain_data[row_idx][col_idx]):
                    # in snowflake, math.nan in a pandas DataFrame is treated as None
                    plain_data[row_idx][col_idx] = None
                else:
                    # pandas PANDAS_INTEGER_TYPES (e.g. INT8Dtye) will also store data in the format of float64
                    # here we use the col dtype info to convert data
                    plain_data[row_idx][col_idx] = (
                        int(data.iloc[row_idx][col_idx])
                        if isinstance(data.dtypes[col_idx], PANDAS_INTEGER_TYPES)
                        else float(str(data.iloc[row_idx][col_idx]))
                    )
            elif isinstance(plain_data[row_idx][col_idx], numpy.float32):
                # convert str first and then to float to avoid precision drift as its stored in float32 format
                plain_data[row_idx][col_idx] = float(str(plain_data[row_idx][col_idx]))
            elif isinstance(plain_data[row_idx][col_idx], numpy.bool_):
                plain_data[row_idx][col_idx] = bool(plain_data[row_idx][col_idx])
            elif isinstance(
                plain_data[row_idx][col_idx],
                (numpy.signedinteger, numpy.unsignedinteger),
            ):
                plain_data[row_idx][col_idx] = int(plain_data[row_idx][col_idx])
            elif isinstance(plain_data[row_idx][col_idx], pd.Timestamp):
                if isinstance(data.dtypes[col_idx], pd.DatetimeTZDtype):
                    # this is to align with the current snowflake behavior that it
                    # apply the tz diff to time and then removes the tz information during ingestion
                    plain_data[row_idx][col_idx] = (
                        plain_data[row_idx][col_idx]
                        .tz_convert("UTC")
                        .tz_localize(None)
                        .to_pydatetime()
                    )
                else:
                    # pandas.Timestamp.value gives nanoseconds
                    # snowpark will convert it to microseconds
                    plain_data[row_idx][col_idx] = int(
                        plain_data[row_idx][col_idx].value / 1000
                    )
            elif isinstance(plain_data[row_idx][col_idx], pd.Timedelta):
                # pandas.Timedetla.value gives nanoseconds
                # snowflake keeps the unit of nanoarrow seconds
                plain_data[row_idx][col_idx] = plain_data[row_idx][col_idx].value
            elif isinstance(plain_data[row_idx][col_idx], pd.Interval):

                def convert_to_python_obj(obj):
                    if isinstance(obj, numpy.float_):
                        return float(obj)
                    elif isinstance(obj, numpy.int_):
                        return int(obj)
                    elif isinstance(obj, pd.Timestamp):
                        return int(obj.value / 1000)
                    else:
                        raise NotImplementedError(
                            f"[Local Testing] {type(obj)} within pandas.Interval is not supported."
                        )

                plain_data[row_idx][col_idx] = {
                    "left": convert_to_python_obj(plain_data[row_idx][col_idx].left),
                    "right": convert_to_python_obj(plain_data[row_idx][col_idx].right),
                }
            elif isinstance(plain_data[row_idx][col_idx], str):
                pass
            elif isinstance(plain_data[row_idx][col_idx], pd.Period):
                # snowflake returns the ordinal of a period object
                plain_data[row_idx][col_idx] = plain_data[row_idx][col_idx].ordinal
            else:
                previous_inferred_type = inferred_type_dict.get(col_idx)
                data_type = infer_type(plain_data[row_idx][col_idx])
                if isinstance(data_type, (MapType, ArrayType)):
                    # snowflake converts python dict/array to variant
                    data_type = VariantType()
                if isinstance(data_type, DecimalType):
                    # we need to calculate the precision and scale
                    decimal_str = str(plain_data[row_idx][col_idx])
                    decimal_parts = decimal_str.split(".")
                    integer_len = (
                        len(decimal_str)
                        if len(decimal_parts) == 1
                        else len(decimal_parts[0])
                    )
                    scale = 0 if len(decimal_parts) == 1 else len(decimal_parts[1])
                    precision = integer_len + scale
                    if precision > 38:
                        raise SnowparkClientException(
                            f"[Local Testing] Column precision {precision} and scale {scale} are not supported."
                        )
                    # handle integer and float separately
                    data_type = DecimalType(precision=precision, scale=scale)
                if previous_inferred_type:
                    if isinstance(previous_inferred_type, NullType):
                        inferred_type_dict[col_idx] = data_type
                    if type(data_type) != type(previous_inferred_type):
                        raise SnowparkClientException(
                            f"[Local Testing] Detected type {type(data_type)} and type {type(previous_inferred_type)}"
                            f" in column, coercion is not currently supported"
                        )
                    if isinstance(inferred_type_dict[col_idx], DecimalType):
                        inferred_type_dict[col_idx] = DecimalType(
                            precision=max(
                                previous_inferred_type.precision, data_type.precision
                            ),
                            scale=max(previous_inferred_type.scale, data_type.scale),
                        )
                else:
                    inferred_type_dict[col_idx] = data_type

    fields = []
    for idx, pandas_type in enumerate(data.dtypes):
        if isinstance(pandas_type, pd.IntervalDtype):
            data_type = VariantType()
        elif isinstance(pandas_type, pd.DatetimeTZDtype):
            data_type = TimestampType()
        elif pandas_type.type == numpy.float64:
            data_type = DoubleType()
        elif isinstance(pandas_type, (pd.Float32Dtype, pd.Float64Dtype)):
            data_type = DoubleType()
        elif (
            pandas_type.type == numpy.int64
            or pandas_type.type == numpy.datetime64
            or pandas_type.type == numpy.timedelta64
        ):
            data_type = LongType()
        elif isinstance(pandas_type, PANDAS_INTEGER_TYPES):
            data_type = LongType()
        elif isinstance(pandas_type, pd.PeriodDtype):
            data_type = LongType()
        elif pandas_type.type == numpy.bool_:
            data_type = BooleanType()
        else:
            data_type = inferred_type_dict.get(idx, StringType(length=16777216))
        # snowpark write_pandas will ignore the nullability of pd dataframe and set nullable to True
        struct_field = StructField(col_names[idx], datatype=data_type, nullable=True)
        fields.append(struct_field)

    return StructType(fields=fields), plain_data


def _convert_dataframe_to_table(
    data: "DataFrame", table_name: str, session: "Session"
) -> Table:
    """
    used by create_dataframe from a pandas dataframe to convert a mocking dataframe into a table
    """
    df_select_statement, df_plan = data._select_statement, data._plan
    table = Table(table_name, session)
    # the original _select_statement & plan of Table is query table name
    # replace the table._select_statement & plan with the df mocking one
    table._select_statement, table._plan = df_select_statement, df_plan
    table.write.save_as_table(table_name)
    return table
