#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import Dict, List, Optional, Union

from snowflake.snowpark._internal.analyzer.expression import Expression, NamedExpression
from snowflake.snowpark._internal.analyzer.snowflake_plan import LogicalPlan
from snowflake.snowpark._internal.analyzer.sort_expression import SortOrder


class UnaryNode(LogicalPlan):
    def __init__(self, child: LogicalPlan) -> None:
        super().__init__()
        self.child = child
        self.children.append(child)


class Sample(UnaryNode):
    def __init__(
        self,
        child: LogicalPlan,
        probability_fraction: Optional[float] = None,
        row_count: Optional[int] = None,
        seed: Optional[int] = None,
    ) -> None:
        super().__init__(child)
        self.probability_fraction = probability_fraction
        self.row_count = row_count
        self.seed = seed


class Sort(UnaryNode):
    def __init__(self, order: List[SortOrder], child: LogicalPlan) -> None:
        super().__init__(child)
        self.order = order


class Aggregate(UnaryNode):
    def __init__(
        self,
        grouping_expressions: List[Expression],
        aggregate_expressions: List[NamedExpression],
        child: LogicalPlan,
    ) -> None:
        super().__init__(child)
        self.grouping_expressions = grouping_expressions
        self.aggregate_expressions = aggregate_expressions


class Pivot(UnaryNode):
    def __init__(
        self,
        grouping_columns: List[Expression],
        pivot_column: Expression,
        pivot_values: Optional[Union[List[Expression], LogicalPlan]],
        aggregates: List[Expression],
        default_on_null: Optional[Expression],
        child: LogicalPlan,
    ) -> None:
        super().__init__(child)
        self.grouping_columns = grouping_columns
        self.pivot_column = pivot_column
        self.pivot_values = pivot_values
        self.aggregates = aggregates
        self.default_on_null = default_on_null


class Unpivot(UnaryNode):
    def __init__(
        self,
        value_column: str,
        name_column: str,
        column_list: List[Expression],
        child: LogicalPlan,
    ) -> None:
        super().__init__(child)
        self.value_column = value_column
        self.name_column = name_column
        self.column_list = column_list


class Rename(UnaryNode):
    def __init__(
        self,
        column_map: Dict[str, str],
        child: LogicalPlan,
    ) -> None:
        super().__init__(child)
        self.column_map = column_map


class Filter(UnaryNode):
    def __init__(self, condition: Expression, child: LogicalPlan) -> None:
        super().__init__(child)
        self.condition = condition


class Project(UnaryNode):
    def __init__(self, project_list: List[NamedExpression], child: LogicalPlan) -> None:
        super().__init__(child)
        self.project_list = project_list


class ViewType:
    def __str__(self):
        return self.__class__.__name__[:-4]


class LocalTempView(ViewType):
    pass


class PersistedView(ViewType):
    pass


class CreateViewCommand(UnaryNode):
    def __init__(
        self,
        name: str,
        view_type: ViewType,
        comment: Optional[str],
        child: LogicalPlan,
    ) -> None:
        super().__init__(child)
        self.name = name
        self.view_type = view_type
        self.comment = comment


class CreateDynamicTableCommand(UnaryNode):
    def __init__(
        self,
        name: str,
        warehouse: str,
        lag: str,
        comment: Optional[str],
        child: LogicalPlan,
    ) -> None:
        super().__init__(child)
        self.name = name
        self.warehouse = warehouse
        self.lag = lag
        self.comment = comment
