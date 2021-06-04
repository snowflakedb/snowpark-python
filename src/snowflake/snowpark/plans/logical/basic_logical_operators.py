#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
""" See
https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/basicLogicalOperators.scala"""

from .logical_plan import LogicalPlan, LeafNode, BinaryNode


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