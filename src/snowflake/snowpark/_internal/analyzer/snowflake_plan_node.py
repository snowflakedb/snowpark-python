#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

import snowflake.snowpark
from snowflake.snowpark._internal.analyzer.expression import Attribute, Expression
from snowflake.snowpark.row import Row
from snowflake.snowpark.types import StructType


class LogicalPlan:
    def __init__(self):
        self.children = []


class LeafNode(LogicalPlan):
    pass


class Range(LeafNode):
    def __init__(self, start: int, end: int, step: int, num_slices: int = 1):
        super().__init__()
        if step == 0:
            raise ValueError("The step for range() cannot be 0.")
        self.start = start
        self.end = end
        self.step = step
        self.num_slices = num_slices


class UnresolvedRelation(LeafNode):
    def __init__(self, name: str):
        super().__init__()
        self.name = name


class SnowflakeValues(LeafNode):
    def __init__(self, output: list[Attribute], data: list[Row]):
        super().__init__()
        self.output = output
        self.data = data


class SaveMode(Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"
    ERROR_IF_EXISTS = "errorifexists"
    IGNORE = "ignore"


class SnowflakeCreateTable(LogicalPlan):
    def __init__(
        self,
        table_name: str,
        mode: SaveMode,
        query: LogicalPlan | None,
        create_temp_table: bool = False,
    ):
        super().__init__()
        self.table_name = table_name
        self.mode = mode
        self.query = query
        self.create_temp_table = create_temp_table
        self.children.append(query)


class Limit(LogicalPlan):
    def __init__(self, limit_expr: Expression, child: LogicalPlan):
        super().__init__()
        self.limit_expr = limit_expr
        self.child = child
        self.children.append(child)


class CopyIntoTableNode(LeafNode):
    def __init__(
        self,
        table_name: str,
        *,
        file_path: str | None = None,
        files: str | None = None,
        pattern: str | None = None,
        file_format: str | None = None,
        format_type_options: dict[str, Any] | None,
        column_names: list[str] | None = None,
        transformations: list[snowflake.snowpark.column.Column] | None = None,
        copy_options: dict[str, Any] | None = None,
        validation_mode: str | None = None,
        user_schema: StructType | None = None,
        cur_options: dict[str, Any] | None = None,  # the options of DataFrameReader
    ):
        super().__init__()
        self.table_name = table_name
        self.file_path = file_path
        self.files = files
        self.pattern = pattern
        self.file_format = file_format
        self.column_names = column_names
        self.transformations = transformations
        self.copy_options = copy_options
        self.format_type_options = format_type_options
        self.validation_mode = validation_mode
        self.user_schema = user_schema
        self.cur_options = cur_options


class CopyIntoLocationNode(LogicalPlan):
    def __init__(
        self,
        child: LogicalPlan,
        stage_location: str,
        *,
        partition_by: Expression | None = None,
        file_format_name: str | None = None,
        file_format_type: str | None = None,
        format_type_options: str | None = None,
        header: bool = False,
        copy_options: dict[str, Any],
    ):
        super().__init__()
        self.child = child
        self.children.append(child)
        self.stage_location = stage_location
        self.partition_by = partition_by
        self.format_type_options = format_type_options
        self.header = header
        self.file_format_name = file_format_name
        self.file_format_type = file_format_type
        self.copy_options = copy_options
