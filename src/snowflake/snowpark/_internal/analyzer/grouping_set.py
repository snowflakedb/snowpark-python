#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import AbstractSet, List, Optional

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
