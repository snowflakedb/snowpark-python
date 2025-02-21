#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import sys
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from snowflake.snowpark._internal.analyzer.expression import Attribute, Expression
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    PlanNodeCategory,
    sum_node_complexities,
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

if TYPE_CHECKING:
    from snowflake.snowpark import Session


class LogicalPlan:
    def __init__(self) -> None:
        self.children = []
        self._cumulative_node_complexity: Optional[Dict[PlanNodeCategory, int]] = None
        # This flag is used to determine if the current node is a valid candidate for
        # replacement in plan tree during optimization stage using replace_child method.
        # Currently deepcopied nodes or nodes that are introduced during optimization stage
        # are not valid candidates for replacement.
        self._is_valid_for_replacement: bool = False

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        return PlanNodeCategory.OTHERS

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        """Returns the individual contribution of the logical plan node towards the
        overall compilation complexity of the generated sql.
        """
        return {self.plan_node_category: 1}

    @property
    def cumulative_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        """Returns the aggregate sum complexity statistic from the subtree rooted at this
        logical plan node. Statistic of current node is included in the final aggregate.
        """
        if self._cumulative_node_complexity is None:
            self._cumulative_node_complexity = sum_node_complexities(
                self.individual_node_complexity,
                *(node.cumulative_node_complexity for node in self.children),
            )
        return self._cumulative_node_complexity

    @cumulative_node_complexity.setter
    def cumulative_node_complexity(self, value: Dict[PlanNodeCategory, int]):
        self._cumulative_node_complexity = value

    def reset_cumulative_node_complexity(self) -> None:
        self._cumulative_node_complexity = None


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
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # SELECT ( ROW_NUMBER()  OVER ( ORDER BY  SEQ8() ) -  1 ) * (step) + (start) AS id FROM ( TABLE (GENERATOR(ROWCOUNT => count)))
        return {
            PlanNodeCategory.WINDOW: 1,
            PlanNodeCategory.ORDER_BY: 1,
            PlanNodeCategory.LITERAL: 3,  # step, start, count
            PlanNodeCategory.COLUMN: 1,  # id column
            PlanNodeCategory.FUNCTION: 3,  # ROW_NUMBER, SEQ, GENERATOR
        }


class SnowflakeTable(LeafNode):
    def __init__(
        self,
        name: str,
        *,
        session: "Session",
        is_temp_table_for_cleanup: bool = False,
    ) -> None:
        super().__init__()
        self.name = name
        # When `is_temp_table_for_cleanup` is True, it's a temp table
        # generated by Snowpark (currently only df.cache_result) under the hood
        # and users are not aware of it.
        if is_temp_table_for_cleanup:
            session._temp_table_auto_cleaner.add(self)

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # SELECT * FROM name
        return {PlanNodeCategory.COLUMN: 1}


class WithQueryBlock(LogicalPlan):
    """
    Logical plan node for common table expression (CTE) like
    WITH TEMP_CTE_XXXX AS (SELECT * FROM TEST_TABLE).

    The sql generated for all reference of this block is SELECT * from TEMP_CTE_XXX,
    similar as select from a SnowflakeTable.
    Note that SnowflakeTable is a leaf node, but this node is not.
    """

    def __init__(self, name: str, child: LogicalPlan) -> None:
        super().__init__()
        self.name = name
        self.children.append(child)

    @property
    def cumulative_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # Each WithQueryBlock is replaced by SELECT * FROM cte_name and adds
        # WITH cte_name AS (child) to the query.
        # The complexity score for the child is adjusted during query complexity
        # calculation.
        return {PlanNodeCategory.WITH_QUERY: 1, PlanNodeCategory.COLUMN: 1}


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
    def is_large_local_data(self) -> bool:
        from snowflake.snowpark._internal.analyzer.analyzer import ARRAY_BIND_THRESHOLD

        return len(self.data) * len(self.output) >= ARRAY_BIND_THRESHOLD

    @property
    def is_contain_illegal_null_value(self) -> bool:
        from snowflake.snowpark._internal.analyzer.analyzer import ARRAY_BIND_THRESHOLD

        rows_to_compare = min(
            ARRAY_BIND_THRESHOLD // len(self.output) + 1, len(self.data)
        )
        for j in range(len(self.output)):
            if not self.output[j].nullable:
                for i in range(rows_to_compare):
                    if self.data[i][j] is None:
                        return True
        return False

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        if self.is_large_local_data:
            # When the number of literals exceeds the threshold, we generate 3 queries:
            # 1. create table query
            # 2. insert into table query
            # 3. select * from table query
            # We only consider the complexity from the final select * query since other queries
            # are built based on it.
            return {
                PlanNodeCategory.COLUMN: 1,
            }

        # If we stay under the threshold, we generate a single query:
        # select $1, ..., $m FROM VALUES (r11, r12, ..., r1m), (rn1, ...., rnm)
        return {
            PlanNodeCategory.COLUMN: len(self.output),
            PlanNodeCategory.LITERAL: len(self.data) * len(self.output),
        }


class DynamicTableCreateMode(Enum):
    OVERWRITE = "overwrite"
    ERROR_IF_EXISTS = "errorifexists"
    IGNORE = "ignore"


class SaveMode(Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"
    ERROR_IF_EXISTS = "errorifexists"
    IGNORE = "ignore"
    TRUNCATE = "truncate"


class TableCreationSource(Enum):
    """The enum to indicate the source where SnowflakeCreateTable was created.

    CACHE_RESULT: SnowflakeCreateTable created by DataFrame.cache_result
    LARGE_QUERY_BREAKDOWN: SnowflakeCreateTable created by large query breakdown optimization
    OTHERS: SnowflakeCreateTable created by other sources like DataFrame.write.save_as_table
    """

    CACHE_RESULT = "cache_result"
    LARGE_QUERY_BREAKDOWN = "large_query_breakdown"
    OTHERS = "others"


class SnowflakeCreateTable(LogicalPlan):
    def __init__(
        self,
        table_name: Iterable[str],
        column_names: Optional[List[str]],
        mode: SaveMode,
        query: LogicalPlan,
        creation_source: TableCreationSource,
        table_type: str = "",
        clustering_exprs: Optional[Iterable[Expression]] = None,
        comment: Optional[str] = None,
        enable_schema_evolution: Optional[bool] = None,
        data_retention_time: Optional[int] = None,
        max_data_extension_time: Optional[int] = None,
        change_tracking: Optional[bool] = None,
        copy_grants: bool = False,
        iceberg_config: Optional[dict] = None,
        table_exists: Optional[bool] = None,
    ) -> None:
        super().__init__()

        assert (
            query is not None
        ), "there must be a child plan associated with the SnowflakeCreateTable"
        self.table_name = table_name
        self.column_names = column_names
        self.mode = mode
        self.query = query
        self.table_type = table_type
        self.children.append(query)
        self.clustering_exprs = clustering_exprs or []
        self.comment = comment
        self.creation_source = creation_source
        self.enable_schema_evolution = enable_schema_evolution
        self.data_retention_time = data_retention_time
        self.max_data_extension_time = max_data_extension_time
        self.change_tracking = change_tracking
        self.copy_grants = copy_grants
        self.iceberg_config = iceberg_config
        # whether the table already exists in the database
        # determines the compiled SQL for APPEND and TRUNCATE mode
        self.table_exists = table_exists

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # CREATE OR REPLACE table_type TABLE table_name (col definition) clustering_expr AS SELECT * FROM (query)
        complexity = {PlanNodeCategory.COLUMN: 1}
        complexity = (
            sum_node_complexities(
                complexity, {PlanNodeCategory.COLUMN: len(self.column_names)}
            )
            if self.column_names
            else complexity
        )
        complexity = (
            sum_node_complexities(
                complexity,
                *(expr.cumulative_node_complexity for expr in self.clustering_exprs),
            )
            if self.clustering_exprs
            else complexity
        )
        return complexity


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
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # for limit and offset
        return sum_node_complexities(
            {PlanNodeCategory.LOW_IMPACT: 2},
            self.limit_expr.cumulative_node_complexity,
            self.offset_expr.cumulative_node_complexity,
        )


class ReadFileNode(LeafNode):
    def __init__(
        self,
        path: str,
        format: str,
        options: Dict[str, str],
        schema: List[Attribute],
        schema_to_cast: Optional[List[Tuple[str, str]]] = None,
        transformations: Optional[List[str]] = None,
        metadata_project: Optional[List[str]] = None,
        metadata_schema: Optional[List[Attribute]] = None,
        use_user_schema: bool = False,
    ) -> None:
        super().__init__()
        self.path = path
        self.format = format
        self.options = options
        self.schema = schema
        self.schema_to_cast = schema_to_cast
        self.transformations = transformations
        self.metadata_project = metadata_project
        self.metadata_schema = metadata_schema
        self.use_user_schema = use_user_schema

    @classmethod
    def from_read_file_node(cls, read_file_node: "ReadFileNode"):
        return cls(
            read_file_node.path,
            read_file_node.format,
            read_file_node.options,
            read_file_node.schema,
            read_file_node.schema_to_cast,
            read_file_node.transformations,
            read_file_node.metadata_project,
            read_file_node.metadata_schema,
            read_file_node.use_user_schema,
        )


class SelectFromFileNode(ReadFileNode):
    pass


class SelectWithCopyIntoTableNode(ReadFileNode):
    pass


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
        iceberg_config: Optional[dict] = None,
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
        self.iceberg_config = iceberg_config


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
