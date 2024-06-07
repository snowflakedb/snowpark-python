#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import AbstractSet, List, Optional, Union

from snowflake.snowpark._internal.analyzer.expression import Expression


class GroupingSet(Expression):
    def __init__(self, group_by_exprs: List[Expression]) -> None:
        super().__init__()
        self.group_by_exprs = group_by_exprs
        self.children = group_by_exprs

    def dependent_column_expressions(
        self,
    ) -> Union[Optional[AbstractSet[str]], Optional[List[Expression]]]:
        return self.group_by_exprs


class Cube(GroupingSet):
    pass


class Rollup(GroupingSet):
    pass


class GroupingSetsExpression(Expression):
    def __init__(self, args: List[List[Expression]]) -> None:
        super().__init__()
        self.args = args

    def dependent_column_expressions(
        self,
    ) -> Union[Optional[AbstractSet[str]], Optional[List[Expression]]]:
        flattened_args = [exp for sublist in self.args for exp in sublist]
        return flattened_args
