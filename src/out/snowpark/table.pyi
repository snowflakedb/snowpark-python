from typing import Any, Dict, Iterable, NamedTuple, Optional, Union

import snowflake.snowpark
from snowflake.snowpark._internal.type_utils import ColumnOrLiteral as ColumnOrLiteral
from snowflake.snowpark.column import Column
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.row import Row as Row

class UpdateResult(NamedTuple):
    rows_updated: int
    multi_joined_rows_updated: int

class DeleteResult(NamedTuple):
    rows_deleted: int

class MergeResult(NamedTuple):
    rows_inserted: int
    rows_updated: int
    rows_deleted: int

class WhenMatchedClause:
    def __init__(self, condition: Optional[Column] = ...) -> None: ...
    def update(
        self, assignments: Dict[str, Union[ColumnOrLiteral]]
    ) -> WhenMatchedClause: ...
    def delete(self): ...

class WhenNotMatchedClause:
    def __init__(self, condition: Optional[Column] = ...) -> None: ...
    def insert(
        self, assignments: Union[Iterable[ColumnOrLiteral], Dict[str, ColumnOrLiteral]]
    ) -> WhenNotMatchedClause: ...

class Table(DataFrame):
    table_name: Any
    def __init__(
        self,
        table_name: str,
        session: Optional[snowflake.snowpark.session.Session] = ...,
    ) -> None: ...
    def __copy__(self) -> Table: ...
    def sample(
        self,
        frac: Optional[float] = ...,
        n: Optional[int] = ...,
        *,
        seed: Optional[float] = ...,
        sampling_method: Optional[str] = ...
    ) -> DataFrame: ...
    def update(
        self,
        assignments: Dict[str, ColumnOrLiteral],
        condition: Optional[Column] = ...,
        source: Optional[DataFrame] = ...,
    ) -> UpdateResult: ...
    def delete(
        self, condition: Optional[Column] = ..., source: Optional[DataFrame] = ...
    ) -> DeleteResult: ...
    def merge(
        self,
        source: DataFrame,
        join_expr: Column,
        clauses: Iterable[Union[WhenMatchedClause, WhenNotMatchedClause]],
    ) -> MergeResult: ...
