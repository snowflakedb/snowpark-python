#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import AbstractSet, Dict, List, Optional

from snowflake.snowpark._internal.analyzer.expression import (
    Expression,
    derive_dependent_columns,
)
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    PlanNodeCategory,
    sum_node_complexities,
)


class GroupingSet(Expression):
    def __init__(self, group_by_exprs: List[Expression]) -> None:
        super().__init__()
        self.group_by_exprs = group_by_exprs
        self.children = group_by_exprs

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(*self.group_by_exprs)

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        return PlanNodeCategory.LOW_IMPACT


class Cube(GroupingSet):
    pass


class Rollup(GroupingSet):
    pass


class GroupingSetsExpression(Expression):
    def __init__(self, args: List[List[Expression]]) -> None:
        super().__init__()
        self.args = args

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        flattened_args = [exp for sublist in self.args for exp in sublist]
        return derive_dependent_columns(*flattened_args)

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        return sum_node_complexities(
            {self.plan_node_category: 1},
            *(
                sum_node_complexities(
                    *(expr.cumulative_node_complexity for expr in arg)
                )
                for arg in self.args
            ),
        )

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        return PlanNodeCategory.LOW_IMPACT
