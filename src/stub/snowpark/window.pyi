from typing import Any, List, Tuple, Union

from snowflake.snowpark._internal.sp_expressions import (
    Expression as SPExpression,
    SortOrder as SPSortOrder,
    WindowFrame as SPWindowFrame,
)
from snowflake.snowpark._internal.sp_types.types_package import (
    ColumnOrName as ColumnOrName,
)

class Window:
    unboundedPreceding: int
    unboundedFollowing: int
    currentRow: int
    @staticmethod
    def partitionBy(
        *cols: Union[ColumnOrName, List[ColumnOrName], Tuple[ColumnOrName, ...]]
    ) -> WindowSpec: ...
    @staticmethod
    def orderBy(
        *cols: Union[ColumnOrName, List[ColumnOrName], Tuple[ColumnOrName, ...]]
    ) -> WindowSpec: ...
    @staticmethod
    def rowsBetween(start: int, end: int) -> WindowSpec: ...
    @staticmethod
    def rangeBetween(start: int, end: int) -> WindowSpec: ...

class WindowSpec:
    partition_spec: Any
    order_spec: Any
    frame: Any
    def __init__(
        self,
        partition_spec: List[SPExpression],
        order_spec: List[SPSortOrder],
        frame: SPWindowFrame,
    ) -> None: ...
    def partitionBy(
        self, *cols: Union[ColumnOrName, List[ColumnOrName], Tuple[ColumnOrName, ...]]
    ) -> WindowSpec: ...
    def orderBy(
        self, *cols: Union[ColumnOrName, List[ColumnOrName], Tuple[ColumnOrName, ...]]
    ) -> WindowSpec: ...
    def rowsBetween(self, start: int, end: int) -> WindowSpec: ...
    def rangeBetween(self, start: int, end: int) -> WindowSpec: ...
