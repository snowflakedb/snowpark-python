#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the Apache Software Foundation as part of the
# Apache Spark project, under the Apache License, Version 2.0.

from typing import List, Optional, Dict, Any

from snowflake.snowpark.internal.sp_expressions import Expression


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


class CopyIntoNode(LeafNode):
    def __init__(self, table_name: str, transformations: List[Expression], options: Dict[str, Any], staged_file_reader: "StagedFiledReader"):
        super().__init__()
        self.table_name = table_name
        self.transformations = transformations
        self.options = options
        self.staged_file_reader = staged_file_reader


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


class Sample(UnaryNode):
    """Represents a sample operation in the logical plan"""

    def __init__(
        self,
        child: LogicalPlan,
        probability_fraction: Optional[float] = None,
        row_count: Optional[int] = None,
    ):
        super().__init__()
        if probability_fraction is None and row_count is None:
            raise ValueError(
                "probability_fraction and row_count cannot both be None. "
                "One of those values must be defined"
            )
        self.probability_fraction = probability_fraction
        self.row_count = row_count
        self.child = child
        self.children.append(child)
