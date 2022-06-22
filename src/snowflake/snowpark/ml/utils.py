#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.functions import object_agg


def scaler_fitter(transformer, df: DataFrame, stats: [str]):
    if len(transformer._input_cols) != len(transformer._output_cols):
        raise ValueError("number of input column and output column does not match")
    df._describe(transformer.input_cols, stats=stats).select(
        [
            object_agg("summary", c).as_(fc)
            for c, fc in zip(transformer.input_cols, transformer._states_table_cols)
        ]
    ).write.save_as_table(transformer._states_table_name, create_temp_table=True)
