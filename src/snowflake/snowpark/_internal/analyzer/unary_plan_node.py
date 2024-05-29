#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import Dict, List, Optional, Union

from snowflake.snowpark._internal.analyzer.expression import (
    Expression,
    NamedExpression,
    ScalarSubquery,
)
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

    @property
    def individual_query_complexity(self) -> int:
        # child SAMPLE (probability) -- if probability is provided
        # child SAMPLE (row_count ROWS) -- if not probability but row count is provided
        return 2 + (1 if self.row_count else 0)


class Sort(UnaryNode):
    def __init__(self, order: List[SortOrder], child: LogicalPlan) -> None:
        super().__init__(child)
        self.order = order

    @property
    def individual_query_complexity(self) -> int:
        # child ORDER BY COMMA.join(order)
        return 1 + sum(expr.expression_complexity for expr in self.order)


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

    @property
    def individual_query_complexity(self) -> int:
        return sum(
            expr.expression_complexity for expr in self.grouping_expressions
        ) + sum(expr.expression_complexity for expr in self.aggregate_expressions)


class Pivot(UnaryNode):
    def __init__(
        self,
        grouping_columns: List[Expression],
        pivot_column: Expression,
        pivot_values: Optional[Union[List[Expression], ScalarSubquery]],
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

    @property
    def individual_query_complexity(self) -> int:
        # SELECT * FROM (child) PIVOT (aggregate FOR pivot_col in values)
        estimate = 3
        estimate += sum(expr.expression_complexity for expr in self.grouping_columns)
        estimate += self.pivot_column.expression_complexity
        if isinstance(self.pivot_values, ScalarSubquery):
            estimate += self.pivot_values.expression_complexity
        elif isinstance(self.pivot_values, List):
            estimate += sum(expr.expression_complexity for expr in self.pivot_values)
        else:
            # when pivot values is None
            estimate += 1

        if len(self.aggregates) > 0:
            estimate += self.aggregates[0].expression_complexity

        estimate += (
            self.default_on_null.expression_complexity + 1
            if self.default_on_null
            else 0
        )
        return estimate


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

    @property
    def individual_query_complexity(self) -> int:
        # SELECT * FROM (child) UNPIVOT (value_column FOR name_column IN (COMMA.join(column_list)))
        return 4 + sum(expr.expression_complexity for expr in self.column_list)


class Rename(UnaryNode):
    def __init__(
        self,
        column_map: Dict[str, str],
        child: LogicalPlan,
    ) -> None:
        super().__init__(child)
        self.column_map = column_map

    @property
    def individual_query_complexity(self) -> int:
        return 2 * len(self.column_map)


class Filter(UnaryNode):
    def __init__(self, condition: Expression, child: LogicalPlan) -> None:
        super().__init__(child)
        self.condition = condition

    @property
    def individual_query_complexity(self) -> int:
        return self.condition.expression_complexity


class Project(UnaryNode):
    def __init__(self, project_list: List[NamedExpression], child: LogicalPlan) -> None:
        super().__init__(child)
        self.project_list = project_list

    @property
    def individual_query_complexity(self) -> int:
        return sum(expr.expression_complexity for expr in self.project_list)


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

    @property
    def individual_query_complexity(self) -> int:
        estimate = 3
        estimate += 1 if isinstance(self.view_type, LocalTempView) else 0
        estimate += 1 if self.comment else 0
        return estimate


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

    @property
    def individual_query_complexity(self) -> int:
        # CREATE OR REPLACE DYNAMIC TABLE name LAG = lag WAREHOUSE = wh [comment] AS child
        estimate = 7
        estimate += 1 if self.comment else 0
        return estimate
