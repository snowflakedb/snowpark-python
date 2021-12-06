from typing import Any, Dict, List, Optional

from snowflake.snowpark import Column as Column
from snowflake.snowpark._internal.analyzer.sf_attribute import Attribute
from snowflake.snowpark._internal.plans.logical.logical_plan import (
    LeafNode,
    LogicalPlan,
)
from snowflake.snowpark._internal.sp_expressions import (
    Attribute as SPAttribute,
    AttributeReference as SPAttributeReference,
)
from snowflake.snowpark._internal.utils import _SaveMode
from snowflake.snowpark.row import Row as Row
from snowflake.snowpark.types import StructType as StructType

class SnowflakePlan(LogicalPlan):
    class Decorator:
        @staticmethod
        def wrap_exception(func): ...
    queries: Any
    post_actions: Any
    expr_to_alias: Any
    session: Any
    source_plan: Any
    def __init__(
        self,
        queries,
        schema_query,
        post_actions: Any | None = ...,
        expr_to_alias: Any | None = ...,
        session: Any | None = ...,
        source_plan: Any | None = ...,
    ) -> None: ...
    def analyze_if_needed(self) -> None: ...
    def attributes(self) -> List[Attribute]: ...
    def output(self) -> List[SPAttributeReference]: ...
    def clone(self): ...
    def add_aliases(self, to_add: Dict): ...

class SnowflakePlanBuilder:
    CopyOption: Any
    pkg: Any
    def __init__(self, session) -> None: ...
    def build(
        self, sql_generator, child, source_plan, schema_query: Any | None = ...
    ): ...
    def query(self, sql, source_plan): ...
    def large_local_relation_plan(
        self,
        output: List[SPAttribute],
        data: List[Row],
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan: ...
    def table(self, table_name): ...
    def file_operation_plan(
        self, command, file_name, stage_location, options: Dict[str, str]
    ) -> SnowflakePlan: ...
    def project(self, project_list, child, source_plan, is_distinct: bool = ...): ...
    def aggregate(
        self,
        grouping_exprs: List[str],
        aggregate_exprs: List[str],
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan: ...
    def filter(self, condition, child, source_plan): ...
    def sample(
        self,
        child: SnowflakePlan,
        source_plan: LogicalPlan,
        probability_fraction: Optional[float] = ...,
        row_count: Optional[int] = ...,
    ): ...
    def sort(
        self, order: List[str], child: SnowflakePlan, source_plan: Optional[LogicalPlan]
    ): ...
    def set_operator(
        self,
        left: SnowflakePlan,
        right: SnowflakePlan,
        op: str,
        source_plan: Optional[LogicalPlan],
    ): ...
    def union(
        self, children: List[SnowflakePlan], source_plan: Optional[LogicalPlan]
    ): ...
    def join(self, left, right, join_type, condition, source_plan): ...
    def save_as_table(
        self, table_name: str, mode: _SaveMode, child: SnowflakePlan
    ) -> SnowflakePlan: ...
    def limit(
        self,
        limit_expr: str,
        child: SnowflakePlan,
        on_top_of_oder_by: bool,
        source_plan: Optional[LogicalPlan],
    ): ...
    def pivot(
        self,
        pivot_column: str,
        pivot_values: List[str],
        aggregate: str,
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan: ...
    def create_or_replace_view(
        self, name: str, child: SnowflakePlan, is_temp: bool
    ) -> SnowflakePlan: ...
    def read_file(
        self,
        path: str,
        format: str,
        options: Dict,
        fully_qualified_schema: str,
        schema: List[Attribute],
    ): ...
    def copy_into_table(
        self,
        file_format: str,
        table_name: str,
        path: Optional[str] = ...,
        files: Optional[str] = ...,
        pattern: Optional[str] = ...,
        format_type_options: Optional[Dict[str, Any]] = ...,
        copy_options: Optional[Dict[str, Any]] = ...,
        validation_mode: Optional[str] = ...,
        column_names: Optional[List[str]] = ...,
        transformations: Optional[List[str]] = ...,
        user_schema: Optional[StructType] = ...,
    ) -> SnowflakePlan: ...
    def lateral(
        self,
        table_function: str,
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan: ...
    def from_table_function(self, func: str) -> SnowflakePlan: ...
    def join_table_function(
        self, func: str, child: SnowflakePlan, source_plan: Optional[LogicalPlan]
    ) -> SnowflakePlan: ...

class Query:
    sql: Any
    query_id_place_holder: Any
    def __init__(
        self, sql: str, query_id_place_holder: Optional[str] = ...
    ) -> None: ...

class BatchInsertQuery(Query):
    rows: Any
    def __init__(self, sql: str, rows: Optional[List[Row]] = ...) -> None: ...

class SnowflakeValues(LeafNode):
    output: Any
    data: Any
    def __init__(self, output: List[SPAttribute], data: List[Row]) -> None: ...

class SnowflakeCreateTable(LogicalPlan):
    table_name: Any
    mode: Any
    def __init__(
        self, table_name: str, mode: _SaveMode, query: Optional[LogicalPlan]
    ) -> None: ...

class CopyIntoNode(LeafNode):
    table_name: Any
    file_path: Any
    files: Any
    pattern: Any
    file_format: Any
    column_names: Any
    transformations: Any
    copy_options: Any
    format_type_options: Any
    validation_mode: Any
    user_schema: Any
    cur_options: Any
    def __init__(
        self,
        table_name: str,
        *,
        file_path: Optional[str] = ...,
        files: Optional[str] = ...,
        pattern: Optional[str] = ...,
        file_format: Optional[str] = ...,
        format_type_options: Optional[Dict[str, Any]],
        column_names: Optional[List[str]] = ...,
        transformations: Optional[List[Column]] = ...,
        copy_options: Optional[Dict[str, Any]] = ...,
        validation_mode: Optional[str] = ...,
        user_schema: Optional[StructType] = ...,
        cur_options: Optional[Dict[str, Any]] = ...
    ) -> None: ...
