#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from typing import List

import snowflake.snowpark
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.functions import builtin, col, count_distinct, lit, object_agg


def scaler_fit(
    transformer: "snowflake.snowpark.ml.Transformer", df: DataFrame, stats: List[str]
) -> "None":
    """
    This table stores the value required by standard scaler or minmax scaler. stddev, mean and minimum, maximum to be
    specific.
    given input of
    [
        [1, 4, 5],
        [2, 4, 5],
        [2, 3, 6]
    ]
    the table looks like:
    -------------------------------------------------------------------------------------------------------------------------
    |"STATES_A"                             |"STATES_B"                             |"STATES_C"                             |
    -------------------------------------------------------------------------------------------------------------------------
    |{                                      |{                                      |{                                      |
    |  "mean": 1.666667000000000e+00,       |  "mean": 3.666667000000000e+00,       |  "mean": 5.333333000000000e+00,       |
    |  "stddev_pop": 4.714042850887123e-01  |  "stddev_pop": 4.714042850887123e-01  |  "stddev_pop": 4.714042850887123e-01  |
    |}                                      |}                                      |}                                      |
    -------------------------------------------------------------------------------------------------------------------------
    """
    df._describe(transformer.input_cols, stats=stats).select(
        [
            object_agg("summary", input_col).as_(states_col)
            for input_col, states_col in zip(
                transformer.input_cols, transformer._states_table_cols
            )
        ]
    ).write.save_as_table(transformer._states_table_name, create_temp_table=True)


def encoder_fit(
    transformer: "snowflake.snowpark.ml.Transformer", df: DataFrame, encoder_type: str
) -> "None":
    """
    given input of
    [
        [1, 4, 5],
        [2, 4, 5],
        [2, 3, 6]
    ]
    we will create a table like this for each input column:
    -----------------------------------
    |"STATES_A"  |"STATES_A_ENCODER"  |
    -----------------------------------
    |2           |0                   |
    -----------------------------------
    this works for both ordinal encoder and onehot encoder

    When using fit() function of onehot encoder. A table which records the number of types in each input column will
    be created. given input of
    [
        [1, 4, 5],
        [2, 4, 5],
        [2, 3, 6]
    ]
    the table looks like:
    ----------------------------------------------------------------------------
    |"STATES_A_COUNT"  |"STATES_B_COUNT"  |"STATES_C_COUNT"  |"ENCODER_COUNT"  |
    ----------------------------------------------------------------------------
    |1                 |2                 |2                 |5                |
    ----------------------------------------------------------------------------
    where STATES_x_COUNT indicates number of types of each input column, ENCODER_COUNT indicates length of onehot
    array
    """
    if transformer._category == "auto":
        df_encoder_length = None
        for input_col, states_col in zip(
            transformer.input_cols, transformer._states_table_cols
        ):
            transformer._category_table_name[input_col] = random_name_for_temp_object(
                TempObjectType.TABLE
            )
            df_save = df._distinct([input_col]).select(
                [
                    col(input_col).as_(states_col),
                ]
            )
            if encoder_type == "onehot":
                if df_encoder_length:
                    df_encoder_length = df_encoder_length.join(
                        df_save.select(
                            [count_distinct(col(states_col)).as_(f"{states_col}_count")]
                        )
                    )
                else:
                    df_encoder_length = df_save.select(
                        [count_distinct(col(states_col)).as_(f"{states_col}_count")]
                    )
            df_save.with_column(
                f"{states_col}_encoder", builtin("seq8")()
            ).write.save_as_table(
                transformer._category_table_name[input_col], create_temp_table=True
            )
        if encoder_type == "onehot":
            temp = lit(0)
            for c in transformer._states_table_cols:
                temp += df_encoder_length[f"{c}_count"]

            df_encoder_length.with_column("encoder_count", temp).write.save_as_table(
                transformer._encoder_count_table,
                create_temp_table=True,
            )
    else:
        session = df._session
        df_count = {}
        df_encoder_length = 0
        for input_col, states_col, category in zip(
            transformer.input_cols,
            transformer._states_table_cols,
            transformer._category,
        ):
            transformer._category_table_name[input_col] = random_name_for_temp_object(
                TempObjectType.TABLE
            )
            df_save = session.create_dataframe(category, schema=[states_col])
            if encoder_type == "onehot":
                df_count[f"{states_col}_count"] = len(category)
                df_encoder_length += len(category)
            df_save.with_column(
                f"{states_col}_encoder", builtin("seq8")()
            ).write.save_as_table(
                transformer._category_table_name[input_col], create_temp_table=True
            )

        if encoder_type == "onehot":
            df_count = session.create_dataframe([df_count])
            df_count.with_column(
                "encoder_count", lit(df_encoder_length)
            ).write.save_as_table(
                transformer._encoder_count_table, create_temp_table=True
            )


def check_if_input_output_match(
    transformer: "snowflake.snowpark.ml.Transformer",
) -> "None":
    if len(transformer.input_cols) != len(transformer.output_cols):
        raise ValueError("The number of output column and input column does not match")
