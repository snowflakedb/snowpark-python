from typing import Dict, List, Optional, Tuple, Union

import snowflake.snowpark
from snowflake.snowpark._internal.sp_types.types_package import (
    ColumnOrName as ColumnOrName,
    LiteralType as LiteralType,
)

class DataFrameStatFunctions:
    def __init__(self, df: snowflake.snowpark.DataFrame) -> None: ...
    def approxQuantile(
        self,
        col: Union[ColumnOrName, List[ColumnOrName], Tuple[ColumnOrName, ...]],
        percentile: Union[List[float], Tuple[float, ...]],
    ) -> Union[List[float], List[List[float]]]: ...
    def corr(self, col1: ColumnOrName, col2: ColumnOrName) -> Optional[float]: ...
    def cov(self, col1: ColumnOrName, col2: ColumnOrName) -> Optional[float]: ...
    def crosstab(
        self, col1: ColumnOrName, col2: ColumnOrName
    ) -> snowflake.snowpark.DataFrame: ...
    def sampleBy(
        self, col: ColumnOrName, fractions: Dict[LiteralType, float]
    ) -> snowflake.snowpark.DataFrame: ...
