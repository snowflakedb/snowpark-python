#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import json
import os
from typing import List, Optional

from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    attribute_to_schema_string,
    create_temp_table_statement,
    quote_name,
)
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.functions import array_agg, col, parse_json, sql_expr
from snowflake.snowpark.session import Session, _get_active_session
from snowflake.snowpark.types import ArrayType, StructField, StructType


class Transformer:
    def __init__(
        self,
        input_cols: Optional[List[str]] = None,
        output_cols: Optional[List[str]] = None,
    ):
        self.input_cols = input_cols
        self.output_cols = output_cols

    def fit(self, df: DataFrame) -> "Transformer":
        pass

    def transform(self, df: DataFrame) -> DataFrame:
        pass

    def fit_transform(self, df: DataFrame) -> DataFrame:
        pass

    def save(self, file_path: str, session: Optional[Session] = None) -> None:
        pass

    def load(self, file_path: str, session: Optional[Session] = None) -> "Transformer":
        pass


class StandardScaler(Transformer):
    def __init__(
        self,
        input_cols: Optional[List[str]] = None,
        output_cols: Optional[List[str]] = None,
    ):
        super().__init__(input_cols, output_cols)
        self._states_table_cols = (
            [f"states_{c}" for c in self.input_cols] if self.input_cols else None
        )
        self._states_table_name = random_name_for_temp_object(TempObjectType.TABLE)
        self._metadata_table_name = random_name_for_temp_object(TempObjectType.TABLE)

    def fit(self, df: DataFrame) -> "StandardScaler":
        df.describe(self.input_cols, stats=["mean", "stddev_pop"]).select(
            [
                array_agg(c).within_group("summary").as_(fc)
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
                    (df[c] - col(fc)[0]) / col(fc)[1]
                    for c, fc in zip(self.input_cols, self._states_table_cols)
                ]
            )
            .to_df(self.output_cols)
        )

    def save(self, file_path: str, session: Optional[Session] = None) -> None:
        session = session or _get_active_session()
        session.create_dataframe(
            [[self.input_cols, self.output_cols]], schema=["input_cols", "output_cols"]
        ).write.save_as_table(self._metadata_table_name, create_temp_table=True)
        remote_file_path = (
            f"{session.get_session_stage()}/{os.path.basename(file_path)}"
        )
        session.table(self._states_table_name).write.copy_into_location(
            f"{remote_file_path}/states.parquet",
            file_format_type="parquet",
            header=True,
            overwrite=True,
            single=True,
        )
        session.table(self._metadata_table_name).write.copy_into_location(
            f"{remote_file_path}/metadata.parquet",
            file_format_type="parquet",
            header=True,
            overwrite=True,
            single=True,
        )
        os.mkdir(file_path)
        session.file.get(remote_file_path, file_path)

    def load(
        self, file_path: str, session: Optional[Session] = None
    ) -> "StandardScaler":
        session = session or _get_active_session()
        remote_file_path = (
            f"{session.get_session_stage()}/{os.path.basename(file_path)}"
        )
        session.file.put(
            os.path.join(file_path, "states.parquet"),
            remote_file_path,
            auto_compress=False,
            overwrite=True,
        )
        session.file.put(
            os.path.join(file_path, "metadata.parquet"),
            remote_file_path,
            auto_compress=False,
            overwrite=True,
        )
        metadata = session.read.parquet(
            f"{remote_file_path}/metadata.parquet"
        ).collect()
        self.input_cols = json.loads(metadata[0][0])
        self.output_cols = json.loads(metadata[0][1])
        self._states_table_cols = [f"states_{c}" for c in self.input_cols]
        states_df = session.read.option("infer_schema", False).parquet(
            f"{remote_file_path}/states.parquet"
        )
        create_table_query = create_temp_table_statement(
            self._states_table_name,
            attribute_to_schema_string(
                StructType(
                    [StructField(c, ArrayType()) for c in self._states_table_cols]
                )._to_attributes()
            ),
        )
        session.sql(create_table_query).collect()
        states_df.copy_into_table(
            self._states_table_name,
            transformations=[
                parse_json(sql_expr(f"$1:{quote_name(c)}")).as_(c)
                for c in self._states_table_cols
            ],
        )
        return self
