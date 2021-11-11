#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from typing import List

from snowflake.snowpark._internal.sp_expressions import GroupingSetsExpression
from snowflake.snowpark.column import Column


class GroupingSets:
    """A Container of grouping sets that you pass to :meth:`DataFrame.groupByGroupingSets`"""

    def __init__(self, sets: List[List[Column]]):
        self.to_expression = GroupingSetsExpression(
            [c.expression for s in sets for c in s]
        )
