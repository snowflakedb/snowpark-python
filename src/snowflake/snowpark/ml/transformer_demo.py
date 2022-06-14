#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from typing import List, Optional

from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.functions import col, object_agg


class Transformer:
    def __init__(
        self,
        input_cols: Optional[List[str]] = None,
        output_cols: Optional[List[str]] = None,
    ):
        self._input_cols = input_cols
        self._output_cols = output_cols

    @property
    def input_cols(self) -> List[str]:
        return self._input_cols

    @input_cols.setter
    def input_cols(self, value: List[str]):
        self._input_cols = value

    @property
    def output_cols(self) -> List[str]:
        return self._output_cols

    @output_cols.setter
    def output_cols(self, value: List[str]):
        self._output_cols = value

    def fit(self, df: DataFrame) -> "Transformer":
        pass

    def transform(self, df: DataFrame) -> DataFrame:
        pass


class StandardScaler(Transformer):
    def __init__(
        self,
        input_cols: Optional[List[str]] = None,
        output_cols: Optional[List[str]] = None,
    ):
        super().__init__(input_cols, output_cols)
        self._states_table_cols = (
            [f"states_{c}" for c in self._input_cols] if self._input_cols else None
        )
        self._states_table_name = random_name_for_temp_object(TempObjectType.TABLE)

    @property
    def input_cols(self) -> List[str]:
        return self._input_cols

    @input_cols.setter
    def input_cols(self, value: List[str]):
        self._input_cols = value
        self._states_table_cols = (
            [f"states_{c}" for c in self._input_cols] if self._input_cols else None
        )

    @property
    def output_cols(self) -> List[str]:
        return self._output_cols

    @output_cols.setter
    def output_cols(self, value: List[str]):
        self._output_cols = value

    def fit(self, df: DataFrame) -> "StandardScaler":
        df.describe(self.input_cols, stats=["mean", "stddev_pop"]).select(
            [
                object_agg("summary", c).as_(fc)
                for c, fc in zip(self.input_cols, self._states_table_cols)
            ]
        ).write.save_as_table(self._states_table_name, create_temp_table=True)
        return self

    def transform(self, df: DataFrame) -> DataFrame:
        states_table = df._session.table(self._states_table_name)
        return (
            states_table.join(df)
            .select(
                [
                    (df[c] - col(fc)["mean"]) / col(fc)["stddev_pop"]
                    for c, fc in zip(self.input_cols, self._states_table_cols)
                ]
            )
            .to_df(self.output_cols)
        )


class MinMaxScaler(Transformer):
    def __init__(
        self,
        feature_range: (float, float) = (0, 1),
        input_cols: Optional[List[str]] = None,
        output_cols: Optional[List[str]] = None,
    ):
        super().__init__(input_cols, output_cols)
        self._states_table_cols = (
            [f"states_{c}" for c in self._input_cols] if self._input_cols else None
        )
        self.minimum, self.maximum = feature_range
        self._states_table_name = random_name_for_temp_object(TempObjectType.TABLE)

    @property
    def input_cols(self) -> List[str]:
        return self._input_cols

    @input_cols.setter
    def input_cols(self, value: List[str]):
        self._input_cols = value
        self._states_table_cols = (
            [f"states_{c}" for c in self._input_cols] if self._input_cols else None
        )

    @property
    def output_cols(self) -> List[str]:
        return self._output_cols

    @output_cols.setter
    def output_cols(self, value: List[str]):
        self._output_cols = value

    def fit(self, df: DataFrame) -> "MinMaxScaler":
        df.describe(self.input_cols, stats=["min", "max"]).select(
            [
                object_agg("summary", c).as_(fc)
                for c, fc in zip(self.input_cols, self._states_table_cols)
            ]
        ).write.save_as_table(self._states_table_name, create_temp_table=True)
        return self

    def transform(self, df: DataFrame) -> DataFrame:
        states_table = df._session.table(self._states_table_name)
        return (
            states_table.join(df)
            .select(
                [
                    (df[c] - col(fc)["min"])
                    / (col(fc)["max"] - col(fc)["min"])
                    * (self.maximum - self.minimum)
                    + self.minimum
                    for c, fc in zip(self.input_cols, self._states_table_cols)
                ]
            )
            .to_df(self.output_cols)
        )
