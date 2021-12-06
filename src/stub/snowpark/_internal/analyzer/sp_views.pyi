from typing import Any, Dict, List, Optional

from snowflake.snowpark._internal.analyzer.sp_identifiers import (
    TableIdentifier as TableIdentifier,
)
from snowflake.snowpark._internal.plans.logical.logical_plan import (
    LogicalPlan as LogicalPlan,
)

class ViewType: ...
class LocalTempView(ViewType): ...
class GlobalTempView(ViewType): ...
class PersistedView(ViewType): ...

class CreateViewCommand:
    name: Any
    user_specified_columns: Any
    comment: Any
    properties: Any
    original_text: Any
    child: Any
    allow_existing: Any
    replace: Any
    view_type: Any
    children: Any
    inner_children: Any
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
    ) -> None: ...
