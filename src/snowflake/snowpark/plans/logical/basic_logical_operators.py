#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
""" See
https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/basicLogicalOperators.scala"""

from typing import List

from src.snowflake.snowpark.internal.sp_expressions import SortOrder

from .logical_plan import BinaryNode, LeafNode, LogicalPlan, UnaryNode


class Join(BinaryNode):
    def __init__(self, left, right, join_type, condition, hint):
        super().__init__()
        self.left = left
        self.right = right
        self.join_type = join_type
        self.condition = condition
        self.hint = hint
        self.children = [left, right]


class Range(LeafNode):
    def __init__(self, start, end, step, num_slices=1):
        super().__init__()
        self.start = start
        self.end = end
        self.step = step
        self.num_slices = num_slices


class Aggregate(UnaryNode):
    def __init__(self, grouping_expressions, aggregate_expressions, child):
        super().__init__()
        self.grouping_expressions = grouping_expressions
        self.aggregate_expressions = aggregate_expressions
        self.child = child
        self.children.append(child)


class Pivot(UnaryNode):
    def __init__(
        self, group_by_exprs_opt, pivot_column, pivot_values, aggregates, child
    ):
        super().__init__()
        self.group_by_exprs_opt = group_by_exprs_opt
        self.pivot_column = pivot_column
        self.pivot_values = pivot_values
        self.aggregate = aggregates
        self.child = child


class Sort(UnaryNode):
    def __init__(self, order: List["SortOrder"], is_global: bool, child: LogicalPlan):
        super().__init__()
        self.order = order
        self.is_global = is_global
        self.child = child
        self.children.append(child)
