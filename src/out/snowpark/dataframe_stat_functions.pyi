from typing import Any, Dict, Iterable, List, Optional, Union

import snowflake.snowpark
from snowflake.snowpark._internal.type_utils import (
    ColumnOrName as ColumnOrName,
    LiteralType as LiteralType,
)

class DataFrameStatFunctions:
    def __init__(self, df: snowflake.snowpark.DataFrame) -> None: ...
    def approx_quantile(
        self,
        col: Union[ColumnOrName, Iterable[ColumnOrName]],
        percentile: Iterable[float],
    ) -> Union[List[float], List[List[float]]]: ...
    def corr(self, col1: ColumnOrName, col2: ColumnOrName) -> Optional[float]: ...
    def cov(self, col1: ColumnOrName, col2: ColumnOrName) -> Optional[float]: ...
    def crosstab(
        self, col1: ColumnOrName, col2: ColumnOrName
    ) -> snowflake.snowpark.DataFrame: ...
    def sample_by(
        self, col: ColumnOrName, fractions: Dict[LiteralType, float]
    ) -> snowflake.snowpark.DataFrame: ...
    approxQuantile: Any
    sampleBy: Any
