#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from collections import Counter
from functools import cached_property
from typing import AbstractSet, Dict, List, Optional

from snowflake.snowpark._internal.analyzer.complexity_stat import ComplexityStat
from snowflake.snowpark._internal.analyzer.expression import (
    Expression,
    derive_dependent_columns,
)


class GroupingSet(Expression):
    def __init__(self, group_by_exprs: List[Expression]) -> None:
        super().__init__()
        self.group_by_exprs = group_by_exprs
        self.children = group_by_exprs

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(*self.group_by_exprs)

    @property
    def individual_complexity_stat(self) -> Dict[str, int]:
        return Counter({ComplexityStat.LOW_IMPACT.value: 1})


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

    @cached_property
    def cumulative_complexity_stat(self) -> Dict[str, int]:
        return sum(
            (
                sum((expr.cumulative_complexity_stat for expr in arg), Counter())
                for arg in self.args
            ),
            self.individual_complexity_stat,
        )

    @property
    def individual_complexity_stat(self) -> Dict[str, int]:
        return Counter({ComplexityStat.LOW_IMPACT.value: 1})
