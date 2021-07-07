#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

from typing import List


class LogicalPlan:
    def __init__(self):
        self.children = []


class LeafNode(LogicalPlan):
    pass


class BinaryNode(LogicalPlan):
    left: LogicalPlan
    right: LogicalPlan


class NamedRelation(LogicalPlan):
    pass


class UnresolvedRelation(LeafNode, NamedRelation):
    def __init__(self, multipart_identifier):
        super().__init__()
        self.multipart_identifier = multipart_identifier


class UnaryNode(LogicalPlan):
    pass


class OrderPreservingUnaryNode(UnaryNode):
    pass


class Project(OrderPreservingUnaryNode):
    def __init__(self, project_list, child):
        super().__init__()
        self.project_list = project_list
        self.child = child
        self.children.append(child)
        self.resolved = False


class Filter(OrderPreservingUnaryNode):
    def __init__(self, condition, child):
        super().__init__()
        self.condition = condition
        self.child = child
        self.children.append(child)
