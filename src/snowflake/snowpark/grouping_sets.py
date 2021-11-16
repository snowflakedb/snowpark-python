#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from typing import List, Union

from snowflake.snowpark._internal.sp_expressions import GroupingSetsExpression
from snowflake.snowpark._internal.utils import Utils
from snowflake.snowpark.column import Column


class GroupingSets:
    """A Container of grouping sets that you pass to :meth:`DataFrame.groupByGroupingSets`"""

    def __init__(self, *sets: Union[List[Column], List[List[Column]]]):
        prepared_sets = (
            [*sets[0]]
            if isinstance(sets[0], tuple)
            else [sets[0]]
            if len(sets) == 1
            else [*sets]
        )
        self.to_expression = GroupingSetsExpression(
            [[c.expression for c in s] for s in prepared_sets]
        )
