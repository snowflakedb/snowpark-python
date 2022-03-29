#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark._internal.analyzer.expression import Expression
from snowflake.snowpark._internal.plans.logical.logical_plan import LogicalPlan


class Limit(LogicalPlan):
    def __init__(self, limit_expr: Expression, child: LogicalPlan):
        super().__init__()
        self.limit_expr = limit_expr
        self.child = child
        self.children = [child]
