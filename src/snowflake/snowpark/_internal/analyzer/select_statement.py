#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from copy import copy
from functools import cached_property
from typing import List, Optional, Tuple, Union

from snowflake.snowpark._internal.analyzer.binary_expression import And
from snowflake.snowpark._internal.analyzer.expression import Expression
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import LogicalPlan


class Selectable(LogicalPlan):
    def __init__(self, session) -> None:
        super().__init__()
        self.session = session

    def to_sql(self) -> str:
        ...

    def to_schema_sql(self) -> str:
        ...

    def union(self, *selectables: "Selectable") -> "SelectStatement":
        ...

    @cached_property
    def _snowflake_plan(self):
        return self.session._analyzer.resolve(self)

    @cached_property
    def _columns(self) -> Tuple[str, ...]:
        return tuple(x.name for x in self._snowflake_plan.attributes)


class SelectableEntity(Selectable):
    def __init__(self, entity_name, *, session=None) -> None:
        super().__init__(session)
        self.entity_name = entity_name

    def to_sql(self) -> str:
        return self.entity_name

    def to_schema_sql(self) -> str:
        return f"select * from {self.entity_name}"

    def union(self, *selectables: "Selectable") -> "SelectStatement":
        new = SelectStatement()
        new.from_ = UnionStatement(self, *selectables)
        return new

    def _columns_changed_exp_from_parent(self):
        return False


class SelectSQL(Selectable):
    def __init__(self, sql, *, session=None) -> None:
        super().__init__(session)
        self.sql = sql

    def to_sql(self) -> str:
        return self.sql

    def to_schema_sql(self) -> str:
        return self.sql

    def union(self, *selectables: "Selectable") -> "SelectStatement":
        new = SelectStatement()
        new.from_ = UnionStatement(self, *selectables)
        return new

    def _columns_changed_exp_from_parent(self):
        # not able to judge whether columns have changed from a sql statement unless it's parsed in the client
        return True


class Join:
    def __init__(
        self,
        left: Selectable,
        right: Selectable,
        join_column: Optional[Expression],
        join_type: str,
        *,
        session=None,
    ) -> None:
        self.left = left
        self.right = right
        self.join_type = join_type
        self.join_column: Expression = join_column
        self.session = session

    def to_sql(self) -> str:
        return f""" {self.join_type} ({self.right.to_sql()}) on {self.session._analyzer.analyze(self.join_column)}"""


class SelectStatement(Selectable):
    def __init__(
        self,
        *,
        projection_: Optional[List[Union[Expression]]] = None,
        from_: Optional["Selectable"] = None,
        from_entity: Optional[str] = None,
        join_: Optional[List[Join]] = None,
        where_: Optional[Expression] = None,
        order_by_: Optional[List[Expression]] = None,
        limit_: Optional[int] = None,
        session=None,
    ) -> None:
        super().__init__(session)
        self.projection_: Optional[List[Expression]] = projection_
        self.from_: Optional["Selectable"] = from_
        self.from_entity: Optional[
            str
        ] = from_entity  # table, sql, SelectStatement, UnionStatement
        self.join_: Optional[List[Join]] = join_
        self.where_: Optional[Expression] = where_
        self.order_by_: Optional[List[Expression]] = order_by_
        self.limit_: Optional[int] = limit_

    def _no_clause(self) -> bool:
        return all(
            (
                not self.projection_,
                self.where_ is None,
                not self.order_by_,
                not self.limit_,
            )
        )

    def to_sql(self) -> str:
        if self._no_clause():
            return (
                self.from_.to_sql()
                if not isinstance(self.from_, SelectableEntity)
                else f"select * from {self.from_.to_sql()}"
            )
        analyzer = self.session._analyzer
        projection = (
            ",".join(analyzer.analyze(x) for x in self.projection_)
            if self.projection_
            else "*"
        )
        from_clause = (
            f"({self.from_.to_sql()})"
            if not isinstance(self.from_, SelectableEntity)
            else self.from_.to_sql()
        )
        join_clause = ", ".join(x.to_sql() for x in self.join_) if self.join_ else ""
        where_clause = (
            f" where {analyzer.analyze(self.where_)}" if self.where_ is not None else ""
        )
        order_by_clause = (
            f" order by {','.join(analyzer.analyze(x) for x in self.order_by_)}"
            if self.order_by_
            else ""
        )
        limit_clause = f" limit {self.limit_}" if self.limit_ else ""
        return f"select {projection} from {from_clause}{join_clause}{where_clause}{order_by_clause}{limit_clause}"

    def to_schema_sql(self) -> str:
        return self.to_sql()

    def filter(self, col: Expression) -> "SelectStatement":
        if self._columns_changed_exp_from_parent():
            return SelectStatement(from_=self, where_=col, session=self.session)
        new = copy(self)
        new.where_ = And(self.where_, col) if self.where_ is not None else col
        return new

    def select(self, cols) -> "SelectStatement":
        new = copy(self)
        new.projection_ = cols
        if self._columns_changed_exp_from_parent():
            new.from_ = self
        return new

    def sort(self, cols) -> "SelectStatement":
        new = copy(self)
        new.order_by_ = cols
        return new

    def join(
        self, other: Selectable, join_column: Optional[Expression], join_type: str
    ) -> "SelectStatement":
        new = copy(self)
        join_ = Join(self, other, join_column, join_type, session=self.session)
        new.join_ = new.join_.append(join_) if new.join_ else [join_]
        return new

    def union(self, *selectables: "Selectable") -> "SelectStatement":
        if isinstance(self.from_, UnionStatement) and self._no_clause():
            union_statement = self.from_.union(*selectables)
        else:
            selectables = [self, *selectables]
            union_statement = UnionStatement(*selectables)
        new = SelectStatement()
        new.from_ = union_statement
        return new

    def limit(self, n: int):
        new = copy(self)
        new.limit_ = n
        return new

    def _columns_changed_exp_from_parent(self):
        """Check whether any columns have changed expressions"""
        if self.projection_ and self.projection_ != ["*"]:
            parent_column_set = set(self.from_._columns)
            for col_exp in self.projection_:
                col_exp_str = self.session._analyzer.analyze(col_exp)
                if col_exp_str not in parent_column_set:
                    return True
        return False


class UnionStatement(Selectable):
    def __init__(self, *selectables: Selectable, session=None) -> None:
        super().__init__(session)
        self.selectables = selectables

    def to_sql(self) -> str:
        return " union ".join(f"({s.to_sql()})" for s in self.selectables)

    def to_schema_sql(self) -> str:
        return self.selectables[0].to_schema_sql()

    def union(self, *selectables: Selectable) -> SelectStatement:
        new = SelectStatement()
        new.from_ = UnionStatement(*self.selectables, *selectables)
        return new

    def _columns_changed_exp_from_parent(self):
        return True
