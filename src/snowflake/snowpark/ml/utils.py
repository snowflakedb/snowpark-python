#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.functions import builtin, col, count_distinct, lit, object_agg
from snowflake.snowpark.types import StringType, StructField


def scaler_fitter(transformer, df: DataFrame, stats: [str]):
    if len(transformer._input_cols) != len(transformer._output_cols):
        raise ValueError("number of input column and output column does not match")
    df._describe(transformer.input_cols, stats=stats).select(
        [
            object_agg("summary", c).as_(fc)
            for c, fc in zip(transformer.input_cols, transformer._states_table_cols)
        ]
    ).write.save_as_table(transformer._states_table_name, create_temp_table=True)


def encoder_fitter(transformer, df: DataFrame, encoder_type: str):
    if transformer.category == "auto":
        encoder_length = None
        for c, fc in zip(transformer.input_cols, transformer._states_table_cols):
            transformer._category_table_name[c] = random_name_for_temp_object(
                TempObjectType.TABLE
            )
            df_save = df._distinct([StructField(c, StringType())]).select(
                [
                    col(c).as_(fc),
                ]
            )
            if encoder_type == "onehot":
                if encoder_length:
                    encoder_length = encoder_length.join(
                        df_save.select([count_distinct(col(fc)).as_(f"{fc}_count")])
                    )
                else:
                    encoder_length = df_save.select(
                        [count_distinct(col(fc)).as_(f"{fc}_count")]
                    )
            df_save.with_column(f"{fc}_encoder", builtin("seq8")()).write.save_as_table(
                transformer._category_table_name[c], create_temp_table=True
            )
        if encoder_type == "onehot":
            temp = lit(0)
            for i in range(len(transformer._states_table_cols)):
                temp += encoder_length[f"{transformer._states_table_cols[i]}_count"]

            encoder_length.with_column("encoder_count", temp).write.save_as_table(
                transformer._encoder_count_table,
                create_temp_table=True,
                mode="overwrite",
            )
    else:
        session = df._session
        df_count = session.create_dataframe([1], schema=["temp"])
        encoder_length = 0
        for c, fc, cat in zip(
            transformer.input_cols, transformer._states_table_cols, transformer.category
        ):
            transformer._category_table_name[c] = random_name_for_temp_object(
                TempObjectType.TABLE
            )
            df_save = session.create_dataframe(cat, schema=[fc])
            if encoder_type == "onehot":
                df_count = df_count.with_column(f"{fc}_count", lit(len(cat)))
                encoder_length += len(cat)
            df_save.with_column(f"{fc}_encoder", builtin("seq8")()).write.save_as_table(
                transformer._category_table_name[c], create_temp_table=True
            )
        if encoder_type == "onehot":
            df_count.with_column(
                "encoder_count", lit(encoder_length)
            ).write.save_as_table(
                transformer._encoder_count_table, create_temp_table=True
            )
