#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import AbstractSet, Dict, Optional, Type

from snowflake.snowpark._internal.analyzer.expression import (
    Expression,
    derive_dependent_columns,
)
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    PlanNodeCategory,
    sum_node_complexities,
)


class NullOrdering:
    sql: str


class NullsFirst(NullOrdering):
    sql = "NULLS FIRST"


class NullsLast(NullOrdering):
    sql = "NULLS LAST"


class SortDirection:
    sql: str
    default_null_ordering: Type[NullOrdering]


class Ascending(SortDirection):
    sql = "ASC"
    default_null_ordering = NullsFirst


class Descending(SortDirection):
    sql = "DESC"
    default_null_ordering = NullsLast


class SortOrder(Expression):
    def __init__(
        self,
        child: Expression,
        direction: SortDirection,
        null_ordering: Optional[NullOrdering] = None,
    ) -> None:
        super().__init__(child)
        self.child: Expression
        self.direction = direction
        self.null_ordering = (
            null_ordering if null_ordering else direction.default_null_ordering
        )
        self.datatype = child.datatype
        self.nullable = child.nullable

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(self.child)

    def calculate_cumulative_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        return sum_node_complexities(
            self.child.cumulative_node_complexity, self.individual_node_complexity
        )
