#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import Any, Dict, Iterable, List, Optional, Union

from snowflake.snowpark._internal.analyzer.expression import (
    Expression,
    NamedExpression,
    ScalarSubquery,
)
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    PlanNodeCategory,
    sum_node_complexities,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import LogicalPlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    DynamicTableCreateMode,
)
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
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # SELECT * FROM (child) SAMPLE (probability) -- if probability is provided
        # SELECT * FROM (child) SAMPLE (row_count ROWS) -- if not probability but row count is provided
        return {
            PlanNodeCategory.SAMPLE: 1,
            PlanNodeCategory.LITERAL: 1,
            PlanNodeCategory.COLUMN: 1,
        }


class SampleBy(UnaryNode):
    def __init__(
        self, child: LogicalPlan, col: Expression, fractions: Dict[Any, float]
    ) -> None:
        super().__init__(child)
        self.col = col
        self.fractions = fractions

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        """
        select SNOWPARK_LEFT.* exclude __SNOWPARK_SEQ_RND from (    -- col 2
            select *,                                               -- col 1
                percent_rank() over                                 -- function 1, window 1
                    (partition by <col> order by random())          -- col 1, partition_by 1, order_by 1, function 1
                    as __SNOWPARK_SEQ_RND                           -- col 1
            from <child>
        ) SNOWPARK_LEFT
        join (                                                      -- join 1
            select KEY, VALUE                                       -- col 2
            from TABLE(FLATTEN(input => parse_json('<fractions>'))) -- function 1
        ) SNOWPARK_RIGHT
        on SNOWPARK_LEFT.<col> = SNOWPARK_RIGHT.KEY                 -- col 2
        where SNOWPARK_LEFT.__SNOWPARK_SEQ_RND <= SNOWPARK_RIGHT.VALUE;     -- col 2
        """
        return {
            PlanNodeCategory.COLUMN: 11,
            PlanNodeCategory.FUNCTION: 3,
            PlanNodeCategory.WINDOW: 1,
            PlanNodeCategory.ORDER_BY: 1,
            PlanNodeCategory.PARTITION_BY: 1,
            PlanNodeCategory.JOIN: 1,
            PlanNodeCategory.FILTER: 1,
        }


class Sort(UnaryNode):
    def __init__(self, order: List[SortOrder], child: LogicalPlan) -> None:
        super().__init__(child)
        self.order = order

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # child ORDER BY COMMA.join(order)
        return sum_node_complexities(
            {PlanNodeCategory.ORDER_BY: 1},
            *(col.cumulative_node_complexity for col in self.order),
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
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        if self.grouping_expressions:
            # GROUP BY grouping_exprs
            complexity = sum_node_complexities(
                {PlanNodeCategory.GROUP_BY: 1},
                *(
                    expr.cumulative_node_complexity
                    for expr in self.grouping_expressions
                ),
            )
        else:
            # LIMIT 1
            complexity = {PlanNodeCategory.LOW_IMPACT: 1}

        complexity = sum_node_complexities(
            complexity,
            *(
                getattr(
                    expr,
                    "cumulative_node_complexity",
                    {PlanNodeCategory.COLUMN: 1},
                )  # type: ignore
                for expr in self.aggregate_expressions
            ),
        )
        return complexity


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
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        complexity = {}
        # child complexity adjustment if grouping cols
        if self.grouping_columns and self.aggregates and self.aggregates[0].children:
            # for additional projecting cols when grouping cols is not empty
            complexity = sum_node_complexities(
                self.pivot_column.cumulative_node_complexity,
                self.aggregates[0].children[0].cumulative_node_complexity,
                *(col.cumulative_node_complexity for col in self.grouping_columns),
            )

        # pivot col
        if isinstance(self.pivot_values, ScalarSubquery):
            complexity = sum_node_complexities(
                complexity, self.pivot_values.cumulative_node_complexity
            )
        elif isinstance(self.pivot_values, List):
            complexity = sum_node_complexities(
                complexity,
                *(val.cumulative_node_complexity for val in self.pivot_values),
            )
        else:
            # if pivot values is None, then we add OTHERS for ANY
            complexity = sum_node_complexities(
                complexity, {PlanNodeCategory.LOW_IMPACT: 1}
            )

        # aggregate complexity
        complexity = sum_node_complexities(
            complexity,
            *(expr.cumulative_node_complexity for expr in self.aggregates),
        )

        # SELECT * FROM (child) PIVOT (aggregate FOR pivot_col in values)
        complexity = sum_node_complexities(
            complexity, {PlanNodeCategory.COLUMN: 2, PlanNodeCategory.PIVOT: 1}
        )
        return complexity


class Unpivot(UnaryNode):
    def __init__(
        self,
        value_column: str,
        name_column: str,
        column_list: List[Expression],
        include_nulls: bool,
        child: LogicalPlan,
    ) -> None:
        super().__init__(child)
        self.value_column = value_column
        self.name_column = name_column
        self.column_list = column_list
        self.include_nulls = include_nulls

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # SELECT * FROM (child) UNPIVOT (value_column FOR name_column IN (COMMA.join(column_list)))
        return sum_node_complexities(
            {PlanNodeCategory.UNPIVOT: 1, PlanNodeCategory.COLUMN: 3},
            *(expr.cumulative_node_complexity for expr in self.column_list),
        )


class Rename(UnaryNode):
    def __init__(
        self,
        column_map: Dict[str, str],
        child: LogicalPlan,
    ) -> None:
        super().__init__(child)
        self.column_map = column_map

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # SELECT * RENAME (before AS after, ...) FROM child
        return {
            PlanNodeCategory.COLUMN: 1 + len(self.column_map),
            PlanNodeCategory.LOW_IMPACT: 1 + len(self.column_map),
        }


class Filter(UnaryNode):
    def __init__(self, condition: Expression, child: LogicalPlan) -> None:
        super().__init__(child)
        self.condition = condition

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # child WHERE condition
        return sum_node_complexities(
            {PlanNodeCategory.FILTER: 1},
            self.condition.cumulative_node_complexity,
        )


class Project(UnaryNode):
    def __init__(self, project_list: List[NamedExpression], child: LogicalPlan) -> None:
        super().__init__(child)
        self.project_list = project_list

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        if not self.project_list:
            return {PlanNodeCategory.COLUMN: 1}

        return sum_node_complexities(
            *(
                getattr(
                    col,
                    "cumulative_node_complexity",
                    {PlanNodeCategory.COLUMN: 1},
                )  # type: ignore
                for col in self.project_list
            ),
        )


class Distinct(UnaryNode):
    def __init__(self, child: LogicalPlan) -> None:
        super().__init__(child)

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # SELECT DISTINCT * FROM child
        return {PlanNodeCategory.DISTINCT: 1, PlanNodeCategory.COLUMN: 1}


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
        replace: bool,
        child: LogicalPlan,
    ) -> None:
        super().__init__(child)
        self.name = name
        self.view_type = view_type
        self.comment = comment
        self.replace = replace


class CreateDynamicTableCommand(UnaryNode):
    def __init__(
        self,
        name: str,
        warehouse: str,
        lag: str,
        comment: Optional[str],
        create_mode: DynamicTableCreateMode,
        refresh_mode: Optional[str],
        initialize: Optional[str],
        clustering_exprs: Iterable[Expression],
        is_transient: bool,
        data_retention_time: Optional[int],
        max_data_extension_time: Optional[int],
        child: LogicalPlan,
        iceberg_config: Optional[dict] = None,
    ) -> None:
        super().__init__(child)
        self.name = name
        self.warehouse = warehouse
        self.lag = lag
        self.comment = comment
        self.create_mode = create_mode
        self.refresh_mode = refresh_mode
        self.initialize = initialize
        self.clustering_exprs = clustering_exprs
        self.is_transient = is_transient
        self.data_retention_time = data_retention_time
        self.max_data_extension_time = max_data_extension_time
        self.iceberg_config = iceberg_config
