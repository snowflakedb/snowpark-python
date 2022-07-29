#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import json
from typing import Any, Dict, Optional

from snowflake.snowpark import DataFrame, Session
from snowflake.snowpark.functions import call_udf, col
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

    _SESSION_SCHEMA = "standard_scaler_clone"

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
        metadata_df = columns_metadata.select().where(
            col(ColumnsMetadataColumn.COLUMN_NAME) == self.input_col
        )
        stats_df = metadata_df.select(col(ColumnsMetadataColumn.NUMERIC_STATISTICS))
        numeric_stats = json.loads(stats_df.collect()[0][0])

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

        # append the fitted row to columns metadata
        metadata_df.update(
            {
                ColumnsMetadataColumn.COLUMN_NAME: f"{self.input_col}_standard_scaler_fitted",
                ColumnsMetadataColumn.NUMERIC_STATISTICS: json.dumps(numeric_stats),
            }
        )
        columns_metadata.write.mode("append").save_as_table(columns_metadata)

        return self

    def compute(self, dataset: DataFrame) -> Dict[str, Any]:
        """Compute the mean and std.

        Parameters
        ----------
        dataset : DataFrame, defualt=None
            Input dataset.

        Returns
        -------
        dict : Dict[str, Any]
            Dictionary with mean and std.
        """
        select_input_query = (
            dataset.select(self.input_col)._DataFrame__plan.queries[-1].sql
        )
        # self.session.sql(
        #     f"call {self._SESSION_SCHEMA}.fit($${select_input_query}$$)"
        # )
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
        # transformed_df = builtin(f"{self._SESSION_SCHEMA}.transform")(dataset.select(col(self.input_col)))
        transformed_df = dataset.select(
            col(self.input_col),
            call_udf(f"{self._SESSION_SCHEMA}.transform", col(self.input_col)),
        ).as_(self.output_col)
        dataset.join(transformed_df, [self.input_col])
        return dataset
