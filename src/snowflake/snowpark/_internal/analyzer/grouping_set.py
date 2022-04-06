#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from typing import List

from snowflake.snowpark._internal.analyzer.expression import Expression


class GroupingSet(Expression):
    def __init__(self, group_by_exprs: List[Expression]):
        super().__init__()
        self.group_by_exprs = group_by_exprs
        self.children = group_by_exprs


class Cube(GroupingSet):
    pass


class Rollup(GroupingSet):
    pass


class GroupingSetsExpression(Expression):
    def __init__(self, args: List[List[Expression]]):
        super().__init__()
        self.args = args
