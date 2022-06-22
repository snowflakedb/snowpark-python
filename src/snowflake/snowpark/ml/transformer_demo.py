#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#


from typing import List, Optional, Union

from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.functions import (
    array_append,
    array_construct,
    builtin,
    col,
    count_distinct,
    lit,
)
from snowflake.snowpark.ml.utils import scaler_fitter
from snowflake.snowpark.types import StringType, StructField


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
        scaler_fitter(self, df, ["mean", "stddev_pop"])
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
        scaler_fitter(self, df, ["min", "max"])
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


class OneHotEncoder(Transformer):
    def __init__(
        self,
        input_cols: Optional[List[str]] = None,
        output_cols: Optional[List[str]] = None,
        category: Union[str, List[List[str]]] = "auto",
    ):
        super().__init__(input_cols, output_cols)
        self.category = category
        self.merge = False
        self._states_table_cols = (
            [f"states_{c}" for c in self._input_cols] if self._input_cols else None
        )
        self._encoder_table_cols = (
            [f"states_{c}_encoder" for c in self._input_cols]
            if self._input_cols
            else None
        )
        self._states_table_name = random_name_for_temp_object(TempObjectType.TABLE)
        self._category_table_name = {}
        self._encoder_count_table = random_name_for_temp_object(TempObjectType.TABLE)

    @property
    def input_cols(self) -> List[str]:
        return self._input_cols

    @input_cols.setter
    def input_cols(self, value: List[str]):
        self._input_cols = value
        self._states_table_cols = (
            [f"states_{c}" for c in self._input_cols] if self._input_cols else None
        )
        self._encoder_table_cols = (
            [f"states_{c}_encoder" for c in self._input_cols]
            if self._input_cols
            else None
        )

    @property
    def output_cols(self) -> List[str]:
        return self._output_cols

    @output_cols.setter
    def output_cols(self, value: List[str]):
        self._output_cols = value

    def fit(self, df: DataFrame) -> "OneHotEncoder":
        if self._output_cols is None or len(self._output_cols) == 1:
            self.merge = True
        elif len(self._input_cols) == len(self._output_cols):
            self.merge = False
        else:
            raise ValueError("number of output colum and input column does not match")
        if self.category == "auto":
            encoder_length = None
            for c, fc in zip(self.input_cols, self._states_table_cols):
                self._category_table_name[c] = random_name_for_temp_object(
                    TempObjectType.TABLE
                )
                df_save = df._distinct([StructField(c, StringType())]).select(
                    [
                        col(c).as_(fc),
                    ]
                )
                if encoder_length:
                    encoder_length = encoder_length.join(
                        df_save.select([count_distinct(col(fc)).as_(f"{fc}_count")])
                    )
                else:
                    encoder_length = df_save.select(
                        [count_distinct(col(fc)).as_(f"{fc}_count")]
                    )
                df_save.with_column(
                    f"{fc}_encoder", builtin("seq8")()
                ).write.save_as_table(
                    self._category_table_name[c], create_temp_table=True
                )

            if len(self._states_table_cols) > 1:
                temp = encoder_length.select(
                    [
                        (
                            col(f"{self._states_table_cols[i]}_count")
                            + col(f"{self._states_table_cols[i + 1]}_count")
                        ).as_("encoder_count")
                        for i in range(len(self._states_table_cols) - 1)
                    ]
                )

                encoder_length.join(temp).write.save_as_table(
                    self._encoder_count_table, create_temp_table=True, mode="overwrite"
                )
            else:
                temp = encoder_length.select(
                    [(col(f"{self._states_table_cols[0]}_count")).as_("encoder_count")]
                )
                encoder_length.join(temp).write.save_as_table(
                    self._encoder_count_table, create_temp_table=True, mode="overwrite"
                )
        else:
            session = df._session
            df_count = session.create_dataframe([1], schema=["temp"])
            for c, fc, cat in zip(
                self.input_cols, self._states_table_cols, self.category
            ):
                self._category_table_name[c] = random_name_for_temp_object(
                    TempObjectType.TABLE
                )
                df_save = session.create_dataframe(cat, schema=[fc])
                df_count.with_column(f"{fc}_count", lit(len(cat)))
                df_save.with_column(
                    f"{fc}_encoder", builtin("seq8")()
                ).write.save_as_table(
                    self._category_table_name[c], create_temp_table=True
                )
            df_count.write.save_as_table(
                self._encoder_count_table, create_temp_table=True
            )
        return self

    def transform(
        self,
        df: DataFrame,
        sparse: bool = False,
    ) -> DataFrame:
        originalC = df.columns
        encoder_structure = array_construct()
        encoder_offset = lit(0)
        df_count_table = df._session.table(self._encoder_count_table)
        df = df.join(df_count_table)
        for fc, ec, (c, tabname) in zip(
            self._states_table_cols,
            self._encoder_table_cols,
            self._category_table_name.items(),
        ):
            cattable = df._session.table(tabname)
            df = cattable.join(df, df[c] == cattable[fc])
            encoder_structure = array_append(
                encoder_structure, col(ec) + encoder_offset
            )
            encoder_offset = col(f"{fc}_count")

        if self.merge:
            originalC.append("OnehotArray")
            df = df.with_column(
                "OnehotArray",
                array_construct(df["encoder_count"], encoder_structure, lit(1)),
            )
            df = df.select([col(c) for c in originalC])
        else:
            for fc, c in zip(self._states_table_cols, self._input_cols):
                df = df.with_column(
                    f"{c}_onehot",
                    array_construct(df[f"{fc}_count"], df[f"{fc}_encoder"], lit(1)),
                )
                originalC.append(f"{c}_onehot")
            df = df.select([col(c) for c in originalC])
        return df


class OrdinalEncoder(Transformer):
    def __init__(
        self,
        input_cols: Optional[List[str]] = None,
        output_cols: Optional[List[str]] = None,
        category: Union[str, List[List[str]]] = "auto",
    ):
        super().__init__(input_cols, output_cols)
        self.category = category
        self.merge = False
        self._states_table_cols = (
            [f"states_{c}" for c in self._input_cols] if self._input_cols else None
        )
        self._encoder_table_cols = (
            [f"states_{c}_encoder" for c in self._input_cols]
            if self._input_cols
            else None
        )
        self._states_table_name = random_name_for_temp_object(TempObjectType.TABLE)
        self._category_table_name = {}

    @property
    def input_cols(self) -> List[str]:
        return self._input_cols

    @input_cols.setter
    def input_cols(self, value: List[str]):
        self._input_cols = value
        self._states_table_cols = (
            [f"states_{c}" for c in self._input_cols] if self._input_cols else None
        )
        self._encoder_table_cols = (
            [f"states_{c}_encoder" for c in self._input_cols]
            if self._input_cols
            else None
        )

    @property
    def output_cols(self) -> List[str]:
        return self._output_cols

    @output_cols.setter
    def output_cols(self, value: List[str]):
        self._output_cols = value

    def fit(self, df: DataFrame) -> "OrdinalEncoder":
        if self._output_cols is None or len(self._output_cols) == 1:
            self.merge = True
        elif len(self._input_cols) == len(self._output_cols):
            self.merge = False
        else:
            raise ValueError("number of output colum and input column does not match")
        if self.category == "auto":
            for c, fc in zip(self.input_cols, self._states_table_cols):
                self._category_table_name[c] = random_name_for_temp_object(
                    TempObjectType.TABLE
                )
                df_save = df._distinct([StructField(c, StringType())]).select(
                    [
                        col(c).as_(fc),
                    ]
                )
                df_save.with_column(
                    f"{fc}_encoder", builtin("seq8")()
                ).write.save_as_table(
                    self._category_table_name[c], create_temp_table=True
                )
        else:
            session = df._session
            for c, fc, cat in zip(
                self.input_cols, self._states_table_cols, self.category
            ):
                self._category_table_name[c] = random_name_for_temp_object(
                    TempObjectType.TABLE
                )
                df_save = session.create_dataframe(cat, schema=[fc])
                df_save.with_column(
                    f"{fc}_encoder", builtin("seq8")()
                ).write.save_as_table(
                    self._category_table_name[c], create_temp_table=True
                )

        return self

    def transform(
        self,
        df: DataFrame,
        sparse: bool = False,
    ) -> DataFrame:
        originalC = df.columns
        encoder_structure = array_construct()
        for fc, ec, (c, tabname) in zip(
            self._states_table_cols,
            self._encoder_table_cols,
            self._category_table_name.items(),
        ):
            cattable = df._session.table(tabname)
            df = cattable.join(df, df[c] == cattable[fc])
            encoder_structure = array_append(encoder_structure, col(ec))

        if self.merge:
            originalC.append("OrdinalArray")
            df = df.with_column("OrdinalArray", encoder_structure)
            df = df.select([col(c) for c in originalC])
        else:
            for fc, c in zip(self._states_table_cols, self._input_cols):
                df = df.with_column(f"{c}_ordinal", df[f"{fc}_encoder"])
                originalC.append(f"{c}_ordinal")
            df = df.select([col(c) for c in originalC])
        return df
