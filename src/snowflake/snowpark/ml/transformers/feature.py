#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import json
import logging
from typing import Dict, Optional

from snowflake.snowpark import DataFrame, Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.ml.transformers.base import BaseEstimator, BaseTransformer
from snowflake.snowpark.ml.transformers.utils import (
    ColumnsMetadataColumn,
    NumericStatistics,
    StateTable,
)

_TEMP_TABLE = "table_temp"

__all__ = [
    "StandardScaler",
]


class StandardScaler(BaseEstimator, BaseTransformer):
    """Standardize features by removing the mean and scaling to unit variance.

    Parameters
    ----------
    input_col : Optional[str], defualt=None

    output_col : Optional[str], defualt=None

    with_mean : bool, default=True
        If True, center the data before scaling.
        This does not work (and will raise an exception) when attempted on
        sparse matrices, because centering them entails building a dense
        matrix which in common use cases is likely to be too large to fit in
        memory.

    with_std : bool, default=True
        If True, scale the data to unit variance (or equivalently,
        unit standard deviation).
    """

    _SESSION_SCHEMA = "standard_scaler"
    _logger = logging.getLogger("StandardScaler")

    def __init__(
        self,
        *,
        session: Session,
        with_mean: bool = True,
        with_std: bool = True,
        input_col: Optional[str] = None,
        output_col: Optional[str] = None,
    ) -> None:
        self.session = session
        self.with_mean = with_mean
        self.with_std = with_std
        self.input_col = input_col
        self.output_col = output_col
        self.mean = None
        self.stddev = None

    def fit(self, dataset: DataFrame) -> "StandardScaler":
        """Append a fitted row with mean and std to columns
        metadata. Compute the mean and std if basic_statics
        doesn't contain them.

        Parameters
        ----------
        dataset : DataFrame, defualt=None
            Input dataset.

        Returns
        -------
        self : object
            Fitted scaler.
        """
        columns_metadata = self.session.table(StateTable.COLUMNS_METADATA)
        metadata_df = columns_metadata.filter(
            col(ColumnsMetadataColumn.COLUMN_NAME) == self.input_col
        )
        stats_df = metadata_df.select(col(ColumnsMetadataColumn.NUMERIC_STATISTICS))
        original_numeric_stats = stats_df.collect()[0][0]
        numeric_stats = json.loads(original_numeric_stats)

        # missing mean or std
        if (
            self.with_mean
            and (NumericStatistics.MEAN not in numeric_stats)
            or self.with_std
            and (NumericStatistics.STDDEV not in numeric_stats)
        ):
            computed_stats = self.compute(dataset)
            if NumericStatistics.MEAN in numeric_stats:
                del computed_stats[NumericStatistics.MEAN]
            if NumericStatistics.STDDEV in numeric_stats:
                del computed_stats[NumericStatistics.STDDEV]
            numeric_stats.update(computed_stats)

        self.mean = numeric_stats[NumericStatistics.MEAN]
        self.stddev = numeric_stats[NumericStatistics.STDDEV]

        # append the fitted row to columns metadata
        updated_column_name = f"{self.input_col}_standard_scaler_fitted"
        temp_metadata = "temp_metadata"
        metadata_df.write.save_as_table(
            temp_metadata, mode="overwrite", create_temp_table=True
        )
        temp_metadata_table = self.session.table(temp_metadata)
        temp_metadata_table.update(
            {
                ColumnsMetadataColumn.COLUMN_NAME: updated_column_name,
                ColumnsMetadataColumn.NUMERIC_STATISTICS: json.dumps(numeric_stats),
            }
        )
        temp_metadata_table.write.mode("append").save_as_table(
            StateTable.COLUMNS_METADATA
        )

        # self.session.sql(
        #     f"insert into {columns_metadata.table_name} (VERSION, COLUMN_NAME, NUMERIC_STATISTICS) "
        #     f"select '0.0.1', '{updated_column_name}', "
        #     f"parse_json('{json.dumps(numeric_stats)}')"
        # ).collect()

        # metadata_df = metadata_df.with_columns(
        #     [ColumnsMetadataColumn.COLUMN_NAME.value],
        #     [as_varchar(lit(updated_column_name)).cast(StringType())]
        # )
        # metadata_df.show()
        # metadata_df.write.mode("append").save_as_table("standard_scaler_temp")
        # metadata_df.write.mode("append").save_as_table(StateTable.COLUMNS_METADATA)

        # row
        # MetadataRow = Row(ColumnsMetadataColumn.VERSION, ColumnsMetadataColumn.COLUMN_NAME,
        #                   ColumnsMetadataColumn.BASIC_STATISTICS, ColumnsMetadataColumn.NUMERIC_STATISTICS)
        # updated_metadata_row = MetadataRow(
        #     metadata_df[ColumnsMetadataColumn.VERSION],
        #     metadata_df[ColumnsMetadataColumn.COLUMN_NAME],
        #     metadata_df[ColumnsMetadataColumn.BASIC_STATISTICS],
        #     metadata_df[ColumnsMetadataColumn.NUMERIC_STATISTICS]
        # )
        # updated_metadata_df = self.session.create_dataframe([updated_metadata_row]).to_df([
        #     ColumnsMetadataColumn.VERSION.value, ColumnsMetadataColumn.COLUMN_NAME.value,
        #     ColumnsMetadataColumn.BASIC_STATISTICS.value, ColumnsMetadataColumn.NUMERIC_STATISTICS.value
        # ])
        # updated_metadata_df.show()

        # pandas
        # metadata_pandas = metadata_df.to_pandas()
        # self._logger.info(metadata_pandas.to_string())
        #
        # metadata_pandas[ColumnsMetadataColumn.COLUMN_NAME.value].iloc[0] = updated_column_name
        # metadata_pandas[ColumnsMetadataColumn.NUMERIC_STATISTICS.value].iloc[0] = json.dumps(numeric_stats)
        # self._logger.info(metadata_pandas.to_string())
        #
        # updated_metadata_df = self.session.create_dataframe(data=metadata_pandas, schema=metadata_df.schema)
        # updated_metadata_df.show()
        # updated_metadata_df.write.mode("append").save_as_table(StateTable.COLUMNS_METADATA)

        # update_df = pandas.DataFrame(
        #     {
        #         ColumnsMetadataColumn.COLUMN_NAME.value: [updated_column_name],
        #         ColumnsMetadataColumn.NUMERIC_STATISTICS.value: [numeric_stats],
        #     }
        # )
        # metadata_pandas.update(update_df)

        # metadata_df.show()
        # self._logger.info(msg=f"{metadata_df.select(col(ColumnsMetadataColumn.COLUMN_NAME)).collect()}")

        # metadata_df.update(
        #     {
        #         ColumnsMetadataColumn.COLUMN_NAME: updated_column_name,
        #         ColumnsMetadataColumn.NUMERIC_STATISTICS: json.dumps(numeric_stats),
        #     }
        # )

        return self

    def compute(self, dataset: DataFrame) -> Dict[str, float]:
        """Compute the mean and std.

        Parameters
        ----------
        dataset : DataFrame, defualt=None
            Input dataset.

        Returns
        -------
        dict : Dict[str, float]
            Dictionary with mean and std.
        """
        select_input_query = dataset.select(self.input_col)._plan.queries[-1].sql
        self.session.call(f"{self._SESSION_SCHEMA}.fit", select_input_query)
        artifacts = self.session.table(_TEMP_TABLE).collect()
        return {
            NumericStatistics.MEAN: artifacts[0][0],
            NumericStatistics.STDDEV: artifacts[0][1],
        }

    def transform(self, dataset: DataFrame) -> DataFrame:
        """Perform standardization by centering and scaling.

        Parameters
        ----------
        dataset : DataFrame, defualt=None
            Input dataset.

        Returns
        -------
        output_dataset : DataFrame
            Output dataset.
        """
        dataset = dataset.with_column(
            self.output_col, (dataset[0] - self.mean) / self.stddev
        )

        # udf
        # dataset = dataset.with_column(
        #     self.output_col, call_udf(f"{self._SESSION_SCHEMA}.transform", dataset[0])
        # )

        # save sql
        # dataset = dataset.with_column(
        #     self.output_col, (dataset.columns[0] - self.mean) + "/" + self.stddev
        # )
        return dataset
