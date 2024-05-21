#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import sys
from enum import Enum
from typing import Any, Dict, List, Optional

from snowflake.snowpark._internal.analyzer.expression import Attribute, Expression
from snowflake.snowpark.row import Row
from snowflake.snowpark.types import StructType

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable


class LogicalPlan:
    def __init__(self) -> None:
        self.children = []


class LeafNode(LogicalPlan):
    pass


class Range(LeafNode):
    def __init__(self, start: int, end: int, step: int, num_slices: int = 1) -> None:
        super().__init__()
        if step == 0:
            raise ValueError("The step for range() cannot be 0.")
        self.start = start
        self.end = end
        self.step = step
        self.num_slices = num_slices


class UnresolvedRelation(LeafNode):
    def __init__(self, name: str) -> None:
        super().__init__()
        self.name = name


class SnowflakeValues(LeafNode):
    def __init__(
        self,
        output: List[Attribute],
        data: List[Row],
        schema_query: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.output = output
        self.data = data
        self.schema_query = schema_query


class SaveMode(Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"
    ERROR_IF_EXISTS = "errorifexists"
    IGNORE = "ignore"
    TRUNCATE = "truncate"


class SnowflakeCreateTable(LogicalPlan):
    def __init__(
        self,
        table_name: Iterable[str],
        column_names: Optional[Iterable[str]],
        mode: SaveMode,
        query: Optional[LogicalPlan],
        table_type: str = "",
        clustering_exprs: Optional[Iterable[Expression]] = None,
        comment: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.table_name = table_name
        self.column_names = column_names
        self.mode = mode
        self.query = query
        self.table_type = table_type
        self.children.append(query)
        self.clustering_exprs = clustering_exprs or []
        self.comment = comment


class Limit(LogicalPlan):
    def __init__(
        self, limit_expr: Expression, offset_expr: Expression, child: LogicalPlan
    ) -> None:
        super().__init__()
        self.limit_expr = limit_expr
        self.offset_expr = offset_expr
        self.child = child
        self.children.append(child)


class CopyIntoTableNode(LeafNode):
    def __init__(
        self,
        table_name: Iterable[str],
        *,
        file_path: str,
        files: Optional[str] = None,
        pattern: Optional[str] = None,
        file_format: Optional[str] = None,
        format_type_options: Optional[Dict[str, Any]],
        column_names: Optional[List[str]] = None,
        transformations: Optional[List[Expression]] = None,
        copy_options: Dict[str, Any],
        validation_mode: Optional[str] = None,
        user_schema: Optional[StructType] = None,
        cur_options: Optional[Dict[str, Any]] = None,  # the options of DataFrameReader
        create_table_from_infer_schema: bool = False,
    ) -> None:
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
        format_type_options: Optional[Dict[str, str]] = None,
        header: bool = False,
        copy_options: Dict[str, Any],
    ) -> None:
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
