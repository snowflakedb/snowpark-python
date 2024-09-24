#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from collections import Counter
from enum import Enum
from typing import Dict

from snowflake.snowpark._internal.analyzer.snowflake_plan_node import LogicalPlan


class PlanNodeCategory(Enum):
    """This enum class is used to account for different types of sql
    text generated by expressions and logical plan nodes. A bottom up
    aggregation of the number of occurrences of each enum type is
    done in Expression and LogicalPlan class to calculate and stat
    of overall query complexity in the context of compiling for the
    generated sql.
    """

    FILTER = "filter"
    ORDER_BY = "order_by"
    JOIN = "join"
    SET_OPERATION = "set_operation"  # UNION, EXCEPT, INTERSECT, UNION ALL
    SAMPLE = "sample"
    PIVOT = "pivot"
    UNPIVOT = "unpivot"
    WINDOW = "window"
    GROUP_BY = "group_by"
    PARTITION_BY = "partition_by"
    CASE_WHEN = "case_when"
    LITERAL = "literal"  # cover all literals like numbers, constant strings, etc
    COLUMN = "column"  # covers all cases where a table column is referred
    FUNCTION = (
        "function"  # cover all snowflake built-in function, table functions and UDXFs
    )
    IN = "in"
    LOW_IMPACT = "low_impact"
    OTHERS = "others"

    def __repr__(self) -> str:
        return self.name


def sum_node_complexities(
    *node_complexities: Dict[PlanNodeCategory, int]
) -> Dict[PlanNodeCategory, int]:
    """This is a helper function to sum complexity values from all complexity dictionaries. A node
    complexity is a dictionary of node category to node count mapping"""
    counter_sum = sum(
        (Counter(complexity) for complexity in node_complexities), Counter()
    )
    return dict(counter_sum)


def get_complexity_score(node: LogicalPlan) -> int:
    """Calculates the complexity score based on the cumulative node complexity"""
    score = sum(node.cumulative_node_complexity.values())
    with_query_blocks = node.get_with_query_blocks()
    for with_node, count in with_query_blocks.items():
        score -= (count - 1) * sum(with_node.cumulative_node_complexity.values())
    return score
