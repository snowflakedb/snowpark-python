#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import sys
from enum import Enum
from typing import Any, Dict, List, Optional

from snowflake.snowpark._internal.analyzer.expression import Attribute, Expression
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    PlanNodeCategory,
    Counter,
)
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
        self._cumulative_complexity_stat: Optional[Counter[str]] = None

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        """Returns the individual contribution of the logical plan node towards the
        overall compilation complexity of the generated sql.
        """
        return Counter()

    @property
    def cumulative_complexity_stat(self) -> Counter[str]:
        """Returns the aggregate sum complexity statistic from the subtree rooted at this
        logical plan node. Statistic of current node is included in the final aggregate.
        """
        if self._cumulative_complexity_stat is None:
            estimate = self.individual_complexity_stat
            for node in self.children:
                estimate += node.cumulative_complexity_stat

            self._cumulative_complexity_stat = estimate
        return self._cumulative_complexity_stat

    @cumulative_complexity_stat.setter
    def cumulative_complexity_stat(self, value: Counter[str]):
        self._cumulative_complexity_stat = value


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

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        # SELECT ( ROW_NUMBER()  OVER ( ORDER BY  SEQ8() ) -  1 ) * (step) + (start) AS id FROM ( TABLE (GENERATOR(ROWCOUNT => count)))
        return Counter(
            {
                PlanNodeCategory.WINDOW.value: 1,
                PlanNodeCategory.ORDER_BY.value: 1,
                PlanNodeCategory.LITERAL.value: 3,  # step, start, count
                PlanNodeCategory.COLUMN.value: 1,  # id column
                PlanNodeCategory.OTHERS.value: 2,  # ROW_NUMBER, GENERATOR
            }
        )


class UnresolvedRelation(LeafNode):
    def __init__(self, name: str) -> None:
        super().__init__()
        self.name = name

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        # SELECT * FROM name
        return Counter({PlanNodeCategory.COLUMN.value: 1})


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

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        # select $1, ..., $m FROM VALUES (r11, r12, ..., r1m), (rn1, ...., rnm)
        # TODO: use ARRAY_BIND_THRESHOLD
        return Counter(
            {
                PlanNodeCategory.COLUMN.value: len(self.output),
                PlanNodeCategory.LITERAL.value: len(self.data) * len(self.output),
            }
        )


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
        column_names: Optional[List[str]],
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

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        # CREATE OR REPLACE table_type TABLE table_name (col definition) clustering_expr AS SELECT * FROM (child)
        estimate = Counter(
            {PlanNodeCategory.OTHERS.value: 1, PlanNodeCategory.COLUMN.value: 1}
        )
        estimate += (
            Counter({PlanNodeCategory.COLUMN.value: len(self.column_names)})
            if self.column_names
            else Counter()
        )
        estimate += (
            sum(
                (expr.cumulative_complexity_stat for expr in self.clustering_exprs),
                Counter(),
            )
            if self.clustering_exprs
            else Counter()
        )
        return estimate


class Limit(LogicalPlan):
    def __init__(
        self, limit_expr: Expression, offset_expr: Expression, child: LogicalPlan
    ) -> None:
        super().__init__()
        self.limit_expr = limit_expr
        self.offset_expr = offset_expr
        self.child = child
        self.children.append(child)

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        # for limit and offset
        return (
            Counter({PlanNodeCategory.OTHERS.value: 2})
            + self.limit_expr.cumulative_complexity_stat
            + self.offset_expr.cumulative_complexity_stat
        )


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
