#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from typing import List, Optional

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
    ) -> None:
        super().__init__(child)
        if probability_fraction is None and row_count is None:
            raise ValueError(
                "probability_fraction and row_count cannot both be None. "
                "One of those values must be defined"
            )
        self.probability_fraction = probability_fraction
        self.row_count = row_count


class Sort(UnaryNode):
    def __init__(
        self, order: List[SortOrder], is_global: bool, child: LogicalPlan
    ) -> None:
        super().__init__(child)
        self.order = order
        self.is_global = is_global


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
        pivot_column: Expression,
        pivot_values: List[Expression],
        aggregates: List[Expression],
        child: LogicalPlan,
    ) -> None:
        super().__init__(child)
        self.pivot_column = pivot_column
        self.pivot_values = pivot_values
        self.aggregates = aggregates


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
        child: LogicalPlan,
    ) -> None:
        super().__init__(child)
        self.name = name
        self.view_type = view_type
