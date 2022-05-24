#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
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
    def __init__(self, output: List[Attribute], data: List[Row]):
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
        query: Optional[LogicalPlan],
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
        file_path: Optional[str] = None,
        files: Optional[str] = None,
        pattern: Optional[str] = None,
        file_format: Optional[str] = None,
        format_type_options: Optional[Dict[str, Any]],
        column_names: Optional[List[str]] = None,
        transformations: Optional[List["snowflake.snowpark.column.Column"]] = None,
        copy_options: Optional[Dict[str, Any]] = None,
        validation_mode: Optional[str] = None,
        user_schema: Optional[StructType] = None,
        cur_options: Optional[Dict[str, Any]] = None,  # the options of DataFrameReader
        create_table_from_infer_schema: bool = False,
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
        self.create_table_from_infer_schema = create_table_from_infer_schema


class CopyIntoLocationNode(LogicalPlan):
    def __init__(
        self,
        child: LogicalPlan,
        stage_location: str,
        *,
        partition_by: Optional[Expression] = None,
        file_format_name: Optional[str] = None,
        file_format_type: Optional[str] = None,
        format_type_options: Optional[str] = None,
        header: bool = False,
        copy_options: Dict[str, Any],
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
