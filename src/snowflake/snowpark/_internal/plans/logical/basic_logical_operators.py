#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the Apache Software Foundation as part of the
# Apache Spark project, under the Apache License, Version 2.0.
""" See
https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/basicLogicalOperators.scala"""

from typing import List, Optional

from snowflake.snowpark._internal.plans.logical.logical_plan import (
    BinaryNode,
    LeafNode,
    LogicalPlan,
    UnaryNode,
)
from snowflake.snowpark._internal.sp_expressions import SortOrder


class Join(BinaryNode):
    def __init__(
        self,
        left: "SnowflakePlan",
        right: "SnowflakePlan",
        join_type: "JoinType",
        condition: Optional["Expression"],
        hint: "JoinHint" = None,
    ):
        super().__init__()
        self.left = left
        self.right = right
        self.join_type = join_type
        self.condition = condition
        self.hint = hint
        self.children = [left, right]

    @property
    def sql(self):
        return self.join_type.sql


class Range(LeafNode):
    def __init__(self, start, end, step, num_slices=1):
        super().__init__()
        self.start = start
        self.end = end
        if step == 0:
            raise ValueError("The step for range() cannot be 0.")
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


class SetOperation(BinaryNode):
    def __init__(self, left: LogicalPlan, right: LogicalPlan):
        super().__init__()
        self.left = left
        self.right = right


class Intersect(SetOperation):
    def __init__(self, left: LogicalPlan, right: LogicalPlan):
        super().__init__(left=left, right=right)
        self.children = [self.left, self.right]

    def node_name(self):
        return self.__class__.__name__.upper()

    @property
    def sql(self):
        return self.node_name()


class Union(SetOperation):
    def __init__(self, left: LogicalPlan, right: LogicalPlan, is_all: bool):
        super().__init__(left=left, right=right)
        self.is_all = is_all
        self.children = [self.left, self.right]

    def node_name(self):
        return self.__class__.__name__.upper() + (" ALL" if self.is_all else "")

    @property
    def sql(self):
        return self.node_name()


class Except(SetOperation):
    def __init__(self, left: LogicalPlan, right: LogicalPlan):
        super().__init__(left=left, right=right)
        self.children = [self.left, self.right]

    def node_name(self):
        return self.__class__.__name__.upper()

    @property
    def sql(self):
        return self.node_name()
