#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import Dict, List, Optional, Union

from snowflake.snowpark._internal.analyzer.expression import (
    Expression,
    NamedExpression,
    ScalarSubquery,
)
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    Counter,
    PlanNodeCategory,
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
    def individual_node_complexity(self) -> Counter[str]:
        # SELECT * FROM (child) SAMPLE (probability) -- if probability is provided
        # SELECT * FROM (child) SAMPLE (row_count ROWS) -- if not probability but row count is provided
        return Counter(
            {
                PlanNodeCategory.SAMPLE.value: 1,
                PlanNodeCategory.LITERAL.value: 1,
                PlanNodeCategory.COLUMN.value: 1,
            }
        )


class Sort(UnaryNode):
    def __init__(self, order: List[SortOrder], child: LogicalPlan) -> None:
        super().__init__(child)
        self.order = order

    @property
    def individual_node_complexity(self) -> Counter[str]:
        # child ORDER BY COMMA.join(order)
        return Counter({PlanNodeCategory.ORDER_BY.value: 1}) + sum(
            (col.cumulative_node_complexity for col in self.order), Counter()
        )


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
    def individual_node_complexity(self) -> Counter[str]:
        stat = Counter()
        if self.grouping_expressions:
            # GROUP BY grouping_exprs
            stat += Counter({PlanNodeCategory.GROUP_BY.value: 1}) + sum(
                (expr.cumulative_node_complexity for expr in self.grouping_expressions),
                Counter(),
            )
        else:
            # LIMIT 1
            stat += Counter({PlanNodeCategory.LOW_IMPACT.value: 1})

        stat += sum(
            (
                getattr(
                    expr,
                    "cumulative_node_complexity",
                    Counter({PlanNodeCategory.COLUMN.value: 1}),
                )  # type: ignore
                for expr in self.aggregate_expressions
            ),
            Counter(),
        )
        return stat


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
    def individual_node_complexity(self) -> Counter[str]:
        stat = Counter()
        # child stat adjustment if grouping cols
        if self.grouping_columns and self.aggregates and self.aggregates[0].children:
            # for additional projecting cols when grouping cols is not empty
            stat += sum(
                (col.cumulative_node_complexity for col in self.grouping_columns),
                Counter(),
            )
            stat += self.pivot_column.cumulative_node_complexity
            stat += self.aggregates[0].children[0].cumulative_node_complexity

        # pivot col
        if isinstance(self.pivot_values, ScalarSubquery):
            stat += self.pivot_values.cumulative_node_complexity
        elif isinstance(self.pivot_values, List):
            stat += sum(
                (val.cumulative_node_complexity for val in self.pivot_values), Counter()
            )
        else:
            # if pivot values is None, then we add OTHERS for ANY
            stat += Counter({PlanNodeCategory.LOW_IMPACT.value: 1})

        # aggregate stat
        stat += sum(
            (expr.cumulative_node_complexity for expr in self.aggregates), Counter()
        )

        # SELECT * FROM (child) PIVOT (aggregate FOR pivot_col in values)
        stat += Counter(
            {PlanNodeCategory.COLUMN.value: 2, PlanNodeCategory.PIVOT.value: 1}
        )
        return stat


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
    def individual_node_complexity(self) -> Counter[str]:
        # SELECT * FROM (child) UNPIVOT (value_column FOR name_column IN (COMMA.join(column_list)))
        stat = Counter(
            {PlanNodeCategory.UNPIVOT.value: 1, PlanNodeCategory.COLUMN.value: 3}
        )
        stat += sum(
            (expr.cumulative_node_complexity for expr in self.column_list), Counter()
        )
        return stat


class Rename(UnaryNode):
    def __init__(
        self,
        column_map: Dict[str, str],
        child: LogicalPlan,
    ) -> None:
        super().__init__(child)
        self.column_map = column_map

    @property
    def individual_node_complexity(self) -> Counter[str]:
        # SELECT * RENAME (before AS after, ...) FROM child
        return Counter(
            {
                PlanNodeCategory.COLUMN.value: 1 + 2 * len(self.column_map),
                PlanNodeCategory.LOW_IMPACT.value: 1 + len(self.column_map),
            }
        )


class Filter(UnaryNode):
    def __init__(self, condition: Expression, child: LogicalPlan) -> None:
        super().__init__(child)
        self.condition = condition

    @property
    def individual_node_complexity(self) -> Counter[str]:
        # child WHERE condition
        return (
            Counter({PlanNodeCategory.FILTER.value: 1})
            + self.condition.cumulative_node_complexity
        )


class Project(UnaryNode):
    def __init__(self, project_list: List[NamedExpression], child: LogicalPlan) -> None:
        super().__init__(child)
        self.project_list = project_list

    @property
    def individual_node_complexity(self) -> Counter[str]:
        if not self.project_list:
            return Counter({PlanNodeCategory.COLUMN.value: 1})

        return sum(
            (
                getattr(
                    col,
                    "cumulative_node_complexity",
                    Counter({PlanNodeCategory.COLUMN.value: 1}),
                )  # type: ignore
                for col in self.project_list
            ),
            Counter(),
        )


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
