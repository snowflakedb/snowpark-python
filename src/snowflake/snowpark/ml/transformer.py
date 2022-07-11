#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#


from typing import List, Optional, Tuple, Union

from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.functions import (
    array_agg,
    array_cat,
    array_construct,
    array_construct_compact,
    array_insert,
    array_slice,
    col,
    iff,
    lit,
)
from snowflake.snowpark.ml.utils import (
    check_if_input_output_match,
    encoder_fit,
    scaler_fit,
)


class Transformer:
    def __init__(
        self,
        input_cols: Optional[List[str]] = None,
        output_cols: Optional[List[str]] = None,
    ) -> None:
        self._input_cols = input_cols
        self._output_cols = output_cols
        self._fitted = False

    @property
    def input_cols(self) -> List[str]:
        return self._input_cols

    @input_cols.setter
    def input_cols(self, value: List[str]) -> None:
        self._input_cols = value

    @property
    def output_cols(self) -> List[str]:
        return self._output_cols

    @output_cols.setter
    def output_cols(self, value: List[str]) -> None:
        self._output_cols = value

    def fit(self, df: DataFrame) -> "None":
        self._fitted = True
        if not self._input_cols:
            raise ValueError("Input column can not be empty")

    def transform(self, df: DataFrame) -> None:
        if not self.input_cols:
            raise ValueError("Input column can not be empty")
        elif not self.output_cols:
            raise ValueError("Output column can not be empty")
        elif not self._fitted:
            raise ValueError(
                "The transformer is not fitted yet, call fit() function before transform"
            )


class StandardScaler(Transformer):
    def __init__(
        self,
        input_cols: Optional[List[str]] = None,
        output_cols: Optional[List[str]] = None,
    ) -> None:
        super().__init__(input_cols, output_cols)
        self._states_table_cols = (
            [f"states_{input_col}" for input_col in self._input_cols]
            if self._input_cols
            else None
        )
        self._states_table_name = random_name_for_temp_object(TempObjectType.TABLE)

    @Transformer.input_cols.setter
    def input_cols(self, value: List[str]) -> None:
        self._input_cols = value
        self._states_table_cols = (
            [f"states_{input_col}" for input_col in self._input_cols]
            if self._input_cols
            else None
        )

    def fit(self, df: DataFrame) -> "StandardScaler":
        super().fit(df)
        scaler_fit(self, df, ["mean", "stddev_pop"])
        return self

    def transform(self, df: DataFrame) -> DataFrame:
        """
        The standard score of a sample x is calculated as:
        z = (x - mean) / stddev
        """
        super().transform(df)
        check_if_input_output_match(self)
        states_table = df._session.table(self._states_table_name)
        res_column = df.columns
        for input_col, states_col, output_col in zip(
            self.input_cols, self._states_table_cols, self.output_cols
        ):
            # if stddev equals to 0, we will set it to 1 to avoid divided by 0 error
            res_column.append(
                (
                    (df[input_col] - col(states_col)["mean"])
                    / iff(
                        col(states_col)["stddev_pop"] == 0,
                        1,
                        col(states_col)["stddev_pop"],
                    )
                ).as_(output_col)
            )

        df_transform = states_table.join(df).select(res_column)
        return df_transform


class MinMaxScaler(Transformer):
    def __init__(
        self,
        feature_range: Tuple[float, float] = (0, 1),
        input_cols: Optional[List[str]] = None,
        output_cols: Optional[List[str]] = None,
    ) -> None:
        super().__init__(input_cols, output_cols)
        if feature_range[0] >= feature_range[1]:
            raise ValueError(
                "Maximum can not be equal or greater than minimum in feature range"
            )
        self._states_table_cols = (
            [f"states_{input_col}" for input_col in self._input_cols]
            if self._input_cols
            else None
        )
        self.minimum, self.maximum = feature_range
        self._states_table_name = random_name_for_temp_object(TempObjectType.TABLE)

    @Transformer.input_cols.setter
    def input_cols(self, value: List[str]) -> None:
        self._input_cols = value
        self._states_table_cols = (
            [f"states_{input_col}" for input_col in self._input_cols]
            if self._input_cols
            else None
        )

    def fit(self, df: DataFrame) -> "MinMaxScaler":
        super().fit(df)

        scaler_fit(self, df, ["min", "max"])
        return self

    def transform(self, df: DataFrame) -> DataFrame:
        """
        The transformation is given by:
        X_std = (X - min(X)) / (max(X) - min(X))
        X_scaled = X_std * (max - min) + min
        where min, max are provided by feature_range
        """
        super().transform(df)
        check_if_input_output_match(self)
        states_table = df._session.table(self._states_table_name)

        res_column = df.columns

        # if max(X)-min(X) equals to 0, we will set it as 1 to avoid divided by 0 error
        for input_col, states_col, output_col in zip(
            self.input_cols, self._states_table_cols, self.output_cols
        ):
            res_column.append(
                (
                    (df[input_col] - col(states_col)["min"])
                    / iff(
                        (col(states_col)["max"] - col(states_col)["min"]) == 0,
                        1,
                        col(states_col)["max"] - col(states_col)["min"],
                    )
                    * (self.maximum - self.minimum)
                    + self.minimum
                ).as_(output_col)
            )

        df_transform = states_table.join(df).select(res_column)
        return df_transform


class OneHotEncoder(Transformer):
    def __init__(
        self,
        input_cols: Optional[List[str]] = None,
        output_cols: Optional[List[str]] = None,
        category: Union[str, List[List[str]]] = "auto",
    ) -> None:
        super().__init__(input_cols, output_cols)
        self._category = category
        self._merge = False
        self._states_table_cols = (
            [f"states_{input_col}" for input_col in self._input_cols]
            if self._input_cols
            else None
        )
        self._encoder_table_cols = (
            [f"states_{input_col}_encoder" for input_col in self._input_cols]
            if self._input_cols
            else None
        )
        self._category_table_name = {}
        self._encoder_count_table = random_name_for_temp_object(TempObjectType.TABLE)

    @Transformer.input_cols.setter
    def input_cols(self, value: List[str]) -> None:
        self._input_cols = value
        self._states_table_cols = (
            [f"states_{input_col}" for input_col in self._input_cols]
            if self._input_cols
            else None
        )
        self._encoder_table_cols = (
            [f"states_{input_col}_encoder" for input_col in self._input_cols]
            if self._input_cols
            else None
        )

    def fit(self, df: DataFrame) -> "OneHotEncoder":
        super().fit(df)

        encoder_fit(self, df, "onehot")
        return self

    def transform(
        self,
        df: DataFrame,
        sparse: bool = True,
    ) -> DataFrame:
        super().transform(df)
        if len(self._output_cols) == 1:
            self._merge = True
        elif len(self._input_cols) == len(self._output_cols):
            self._merge = False
        else:
            raise ValueError(
                "The number of output column and input column does not match"
            )

        original_columns = df.columns
        encoder_structure = [] if sparse else array_construct()
        encoder_offset = lit(0)
        df_count_table = df._session.table(self._encoder_count_table)
        df = df.join(df_count_table)

        for states_col, encoder_col, (input_col, table_name) in zip(
            self._states_table_cols,
            self._encoder_table_cols,
            self._category_table_name.items(),
        ):
            category_table = df._session.table(table_name)
            # if sparse is set to true, convert category_table so that it stores onehot array
            if not sparse:
                # create a [0,0...0,0] array as the base of sparse encoder array
                df_dense_array = category_table.with_column("dense", lit(0)).select(
                    [array_agg(col("dense")).as_(f"{states_col}_dense")]
                )
                # join tables to get the length of each encoder array, STATES_X_DENSE looks like [0,...,0], length of
                # this array depends on how many categories in this column
                df_dense_array = category_table.join(df_dense_array).join(
                    df_count_table
                )
                """
                df_dense_array looks like:
                -------------------------------------------------------------------------------------------------
                |"STATES_A"  |"STATES_A_ENCODER"  |"STATES_A_DENSE"  |"STATES_A_COUNT"  |... |"ENCODER_COUNT"  |
                -------------------------------------------------------------------------------------------------
                |1           |0                   |[                  |2                 |... |6                |
                |            |                    |  0,               |                  |    |                 |
                |            |                    |  0                |                  |    |                 |
                |            |                    |]                  |                  |    |                 |
                |2           |1                   |[                  |2                 |... |6                |
                |            |                    |  0,               |                  |    |                 |
                |            |                    |  0                |                  |    |                 |
                |            |                    |]                  |                  |    |                 |
                -------------------------------------------------------------------------------------------------
                """
                # create sparse encoder array for each type in each category
                category_table = df_dense_array.select(
                    [
                        col(states_col),
                        array_slice(
                            array_insert(
                                col(f"{states_col}_dense"), col(encoder_col), lit(1)
                            ),
                            lit(0),
                            col(f"{states_col}_count"),
                        ).as_(encoder_col),
                    ]
                )
                """
                category_table looks like:
                -----------------------------------
                |"STATES_A"  |"STATES_A_ENCODER"  |
                -----------------------------------
                |1           |[                   |
                |            |  1,                |
                |            |  0                 |
                |            |]                   |
                |2           |[                   |
                |            |  0,                |
                |            |  1                 |
                |            |]                   |
                -----------------------------------
                """
                df = category_table.join(
                    df, df[input_col] == category_table[states_col], join_type="right"
                ).join(df_dense_array.select([col(f"{states_col}_dense")]).limit(1))
            else:
                df = category_table.join(
                    df, df[input_col] == category_table[states_col], join_type="right"
                )
            if self._merge:
                if sparse:
                    # encoder_offset is used to shift from one category to another category
                    encoder_structure.append(
                        iff(
                            col(encoder_col).is_null(),
                            col(encoder_col),
                            col(encoder_col) + encoder_offset,
                        )
                    )
                    encoder_offset += col(f"{states_col}_count")
                else:
                    encoder_structure = array_cat(
                        encoder_structure,
                        iff(
                            col(encoder_col).is_null(),
                            df[f"{states_col}_dense"],
                            col(encoder_col),
                        ),
                    )
        # depending on self._merge and sparse, prepare the final output
        if self._merge:
            if sparse:
                # null value will be ignored here
                encoder_structure = array_construct_compact(*encoder_structure)
                onehot_array = array_construct(
                    df["encoder_count"], encoder_structure, lit(1)
                )
            else:
                onehot_array = encoder_structure

            original_columns.append(self._output_cols[0])
            df = df.with_column(self._output_cols[0], onehot_array)
            df = df.select([col(input_col) for input_col in original_columns])
        else:
            onehot_arrays = []
            for states_col, output_col in zip(
                self._states_table_cols, self._output_cols
            ):
                if sparse:
                    onehot_array = array_construct_compact(
                        df[f"{states_col}_count"], df[f"{states_col}_encoder"], lit(1)
                    )
                else:
                    # df[f"{states_col}_encoder"] is null if df fed into transform has unknown category compare
                    # to category_table because of right join
                    onehot_array = iff(
                        df[f"{states_col}_encoder"].is_null(),
                        df[f"{states_col}_dense"],
                        df[f"{states_col}_encoder"],
                    )
                onehot_arrays.append(onehot_array)
                original_columns.append(output_col)
            df = df.with_columns(self._output_cols, onehot_arrays)
            df = df.select([col(input_col) for input_col in original_columns])
        return df


class OrdinalEncoder(Transformer):
    def __init__(
        self,
        input_cols: Optional[List[str]] = None,
        output_cols: Optional[List[str]] = None,
        category: Union[str, List[List[str]]] = "auto",
    ) -> None:
        super().__init__(input_cols, output_cols)
        self._category = category
        self._merge = False
        self._states_table_cols = (
            [f"states_{input_col}" for input_col in self._input_cols]
            if self._input_cols
            else None
        )
        self._encoder_table_cols = (
            [f"states_{input_col}_encoder" for input_col in self._input_cols]
            if self._input_cols
            else None
        )
        self._category_table_name = {}

    @Transformer.input_cols.setter
    def input_cols(self, value: List[str]) -> None:
        self._input_cols = value
        self._states_table_cols = (
            [f"states_{input_col}" for input_col in self._input_cols]
            if self._input_cols
            else None
        )
        self._encoder_table_cols = (
            [f"states_{input_col}_encoder" for input_col in self._input_cols]
            if self._input_cols
            else None
        )

    def fit(self, df: DataFrame) -> "OrdinalEncoder":
        super().fit(df)

        encoder_fit(self, df, "ordinal")
        return self

    def transform(
        self,
        df: DataFrame,
    ) -> DataFrame:
        """
        The encoded value will be null if there are unknown types in this dataframe compared to that dataframe used in
        fit() function.
        """
        super().transform(df)
        if len(self._output_cols) == 1:
            self._merge = True
        elif len(self._input_cols) == len(self._output_cols):
            self._merge = False
        else:
            raise ValueError(
                "The number of output column and input column does not match"
            )
        original_columns = df.columns
        encoder_structure = []

        # Unknown value will be set to null
        for states_col, encoder_col, (input_col, table_name) in zip(
            self._states_table_cols,
            self._encoder_table_cols,
            self._category_table_name.items(),
        ):
            category_table = df._session.table(table_name)
            df = category_table.join(
                df, df[input_col] == category_table[states_col], join_type="right"
            )
            if self._merge:
                encoder_structure.append(col(encoder_col))

        if self._merge:
            encoder_structure = array_construct(*encoder_structure)
            original_columns.append(self._output_cols[0])
            df = df.with_column(self._output_cols[0], encoder_structure)
            df = df.select([col(orginal_col) for orginal_col in original_columns])
        else:
            encoder_columns = []
            for states_col, output_col in zip(
                self._states_table_cols, self._output_cols
            ):
                encoder_columns.append(df[f"{states_col}_encoder"])
                original_columns.append(output_col)
            df = df.with_columns(self._output_cols, encoder_columns)
            df = df.select([col(orginal_col) for orginal_col in original_columns])
        return df


class Binarizer(Transformer):
    def __init__(
        self,
        input_cols: Optional[List[str]] = None,
        output_cols: Optional[List[str]] = None,
        threshold: Optional[float] = 0.0,
    ) -> None:
        super().__init__(input_cols, output_cols)
        self._threshold = threshold
        self._fitted = True

    def transform(self, df: DataFrame) -> DataFrame:
        super().transform(df)
        check_if_input_output_match(self)
        for input_col, output_col in zip(self._input_cols, self._output_cols):
            df = df.with_column(
                output_col,
                iff(
                    col(input_col).is_null() | (col(input_col) == float("nan")),
                    None,
                    iff(col(input_col) > self._threshold, lit(1), lit(0)),
                ),
            )

        return df
