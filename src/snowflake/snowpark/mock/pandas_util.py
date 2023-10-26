#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import math
from typing import TYPE_CHECKING, Any, List, Tuple

import numpy
import pandas
import pandas as pd

from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    quote_name_without_upper_casing,
)
from snowflake.snowpark.table import Table
from snowflake.snowpark.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    VariantType,
)

if TYPE_CHECKING:
    from snowflake.snowpark import DataFrame, Session


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


def _extract_schema_and_data_from_pandas_df(
    data: pd.DataFrame,
) -> Tuple[StructType, List[List[Any]]]:
    """
    infer column types from the pandas data
    when running against snowflake, infer_schema (https://docs.snowflake.com/en/sql-reference/functions/infer_schema)
    is used to infer schema.

    pandas type related doc: https://pandas.pydata.org/docs/user_guide/basics.html#dtypes
    """
    col_names = [
        quote_name_without_upper_casing(name) for name in data.columns.values.tolist()
    ]
    plain_data = [data.iloc[i].tolist() for i in range(data.shape[0])]
    for row_idx in range(data.shape[0]):
        for col_idx in range(data.shape[1]):
            if plain_data[row_idx][col_idx] is None:
                continue
            if isinstance(plain_data[row_idx][col_idx], (float, numpy.float_)):
                # in pandas, a float is represented in type numpy.float64
                # which can not be inferred by snowpark python, we cast to built-in float type
                # pandas PANDAS_INTEGER_TYPES (e.g. INT8Dtye) will also stored data in the format of float64
                if math.isnan(plain_data[row_idx][col_idx]):
                    plain_data[row_idx][col_idx] = None
                else:
                    plain_data[row_idx][col_idx] = (
                        int(plain_data[row_idx][col_idx])
                        if isinstance(data.dtypes[col_idx], PANDAS_INTEGER_TYPES)
                        else float(plain_data[row_idx][col_idx])
                    )
            elif isinstance(plain_data[row_idx][col_idx], numpy.float32):
                # convert str first and then to float to avoid precision drift as its stored in float32 format
                plain_data[row_idx][col_idx] = float(str(plain_data[row_idx][col_idx]))
            elif isinstance(plain_data[row_idx][col_idx], numpy.bool_):
                plain_data[row_idx][col_idx] = bool(plain_data[row_idx][col_idx])
            elif isinstance(plain_data[row_idx][col_idx], numpy.int_):
                plain_data[row_idx][col_idx] = int(plain_data[row_idx][col_idx])
            elif isinstance(plain_data[row_idx][col_idx], pandas.Timestamp):
                if isinstance(data.dtypes[col_idx], pandas.DatetimeTZDtype):
                    # this is to align with the current snowflake behavior that it
                    # removes the tz information during ingestion
                    plain_data[row_idx][col_idx] = (
                        plain_data[row_idx][col_idx].tz_localize(None).to_pydatetime()
                    )
                else:
                    # pandas.Timestamp.value gives nanoseconds
                    # snowpark will convert it to microseconds
                    plain_data[row_idx][col_idx] = int(
                        plain_data[row_idx][col_idx].value / 1000
                    )
            elif isinstance(plain_data[row_idx][col_idx], pandas.Timedelta):
                # pandas.Timedetla.value gives nanoseconds
                # snowflake keeps the unit of nanoarrow seconds
                plain_data[row_idx][col_idx] = plain_data[row_idx][col_idx].value
            elif isinstance(plain_data[row_idx][col_idx], pandas.Interval):

                def convert_to_python_obj(obj):
                    if isinstance(obj, numpy.float_):
                        return float(obj)
                    elif isinstance(obj, numpy.int_):
                        return int(obj)
                    elif isinstance(obj, pandas.Timestamp):
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
            data_type = StringType(length=16777216)
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
    return table
