from typing import Any, Iterable, List, Union

from snowflake.snowpark._internal.analyzer.expression import Expression
from snowflake.snowpark._internal.analyzer.sort_expression import SortOrder
from snowflake.snowpark._internal.analyzer.window_expression import (
    WindowFrame as WindowFrame,
)
from snowflake.snowpark._internal.type_utils import ColumnOrName as ColumnOrName

class Window:
    UNBOUNDED_PRECEDING: int
    unboundedPreceding: Any
    UNBOUNDED_FOLLOWING: int
    unboundedFollowing: Any
    currentRow: int
    @staticmethod
    def partition_by(
        *cols: Union[ColumnOrName, Iterable[ColumnOrName]]
    ) -> WindowSpec: ...
    @staticmethod
    def order_by(*cols: Union[ColumnOrName, Iterable[ColumnOrName]]) -> WindowSpec: ...
    @staticmethod
    def rows_between(start: int, end: int) -> WindowSpec: ...
    @staticmethod
    def range_between(start: int, end: int) -> WindowSpec: ...
    orderBy: Any
    partitionBy: Any
    rangeBetween: Any
    rowsBetween: Any

class WindowSpec:
    partition_spec: Any
    order_spec: Any
    frame: Any
    def __init__(
        self,
        partition_spec: List[Expression],
        order_spec: List[SortOrder],
        frame: WindowFrame,
    ) -> None: ...
    def partition_by(
        self, *cols: Union[ColumnOrName, Iterable[ColumnOrName]]
    ) -> WindowSpec: ...
    def order_by(
        self, *cols: Union[ColumnOrName, Iterable[ColumnOrName]]
    ) -> WindowSpec: ...
    def rows_between(self, start: int, end: int) -> WindowSpec: ...
    def range_between(self, start: int, end: int) -> WindowSpec: ...
    orderBy: Any
    partitionBy: Any
    rangeBetween: Any
    rowsBetween: Any
