#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from typing import Dict, List, Optional

from snowflake.snowpark._internal.analyzer.sp_identifiers import TableIdentifier
from snowflake.snowpark._internal.plans.logical.logical_plan import LogicalPlan


class ViewType:
    def __str__(self):
        return self.__class__.__name__[:-4]


class LocalTempView(ViewType):
    pass


class GlobalTempView(ViewType):
    pass


class PersistedView(ViewType):
    pass


class CreateViewCommand:
    def __init__(
        self,
        name: TableIdentifier,
        user_specified_columns: List[tuple],
        comment: Optional[str],
        properties: Dict,
        original_text: Optional[str],
        child: LogicalPlan,
        allow_existing: bool,
        replace: bool,
        view_type: ViewType,
    ):
        self.name = name
        self.user_specified_columns = user_specified_columns
        self.comment = comment
        self.properties = properties
        self.original_text = original_text
        self.child = child
        self.allow_existing = allow_existing
        self.replace = replace
        self.view_type = view_type

        self.children = [child]
        self.inner_children = [child]
