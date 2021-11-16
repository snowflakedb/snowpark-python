#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from functools import reduce
from typing import Any, Dict, Iterable, List, Optional, Union

import snowflake.snowpark
from snowflake.snowpark import Column
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark.functions import (
    _to_col_if_str,
    approx_percentile_accumulate,
    approx_percentile_estimate,
    corr as corr_func,
    count,
    count_distinct,
    covar_samp,
)

_MAX_COLUMNS_PER_TABLE = 1000


class DataFrameStatFunctions:
    def __init__(self, df: "snowflake.snowpark.DataFrame"):
        self._df = df

    def approxQuantile(
        self,
        col: Union[Column, str, Iterable[Union[str, Column]]],
        percentile: Iterable[float],
    ) -> List[Union[float, List[float]]]:
        temp_col_name = "t"
        if not percentile or not col:
            return []
        if isinstance(col, (Column, str)):
            res = (
                self._df.select(approx_percentile_accumulate(col).as_(temp_col_name))
                .select(
                    [approx_percentile_estimate(temp_col_name, p) for p in percentile]
                )
                ._collect_with_tag()
            )
            return list(res[0])
        elif isinstance(col, Iterable):
            accumate_cols = [
                approx_percentile_accumulate(col_i).as_(f"{temp_col_name}_{i}")
                for i, col_i in enumerate(col)
            ]
            output_cols = [
                approx_percentile_estimate(f"{temp_col_name}_{i}", p)
                for i in range(len(accumate_cols))
                for p in percentile
            ]
            percentile_len = len(output_cols) // len(accumate_cols)
            res = self._df.select(accumate_cols).select(output_cols)._collect_with_tag()
            return [
                [x for x in res[0][j * percentile_len : (j + 1) * percentile_len]]
                for j in range(len(accumate_cols))
            ]
        else:
            raise TypeError(
                "'col' must be a column name, a column object, or a list of them."
            )

    def corr(
        self, col1: Union[Column, str], col2: Union[Column, str]
    ) -> Optional[float]:
        """Calculates the correlation coefficient for non-null pairs in two numeric columns."""
        res = self._df.select(corr_func(col1, col2))._collect_with_tag()
        return res[0][0] if res[0] is not None else None

    def cov(
        self, col1: Union[Column, str], col2: Union[Column, str]
    ) -> Optional[float]:
        res = self._df.select(covar_samp(col1, col2))._collect_with_tag()
        return res[0][0] if res[0] is not None else None

    def crosstab(self, col1: Union[Column, str], col2: Union[Column, str]):
        row_count = self._df.select(count_distinct(col2))._collect_with_tag()[0][0]
        if row_count > _MAX_COLUMNS_PER_TABLE:
            raise SnowparkClientExceptionMessages.DF_CROSS_TAB_COUNT_TOO_LARGE(
                row_count, _MAX_COLUMNS_PER_TABLE
            )
        column_names = [
            row[0] for row in self._df.select(col2).distinct()._collect_with_tag()
        ]
        return self._df.select(col1, col2).pivot(col2, column_names).agg(count(col2))

    def sampleBy(self, col: Union[Column, str], fractions: Dict[Any, float]):
        # TODO: Any should be replaced when we have a type-hint type for snowflake-supported datatypes
        #  https://snowflakecomputing.atlassian.net/browse/SNOW-500245
        if not fractions:
            return self._df.limit(0)
        col = _to_col_if_str(col, "sampleBy")
        res_df = reduce(
            lambda x, y: x.unionAll(y),
            [self._df.filter(col == k).sample(v) for k, v in fractions.items()],
        )
        return res_df
