#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from collections import Counter
from enum import Enum
from typing import Dict


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
    WITH_QUERY = "with_query"
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


def get_complexity_score(node) -> int:
    """Calculates the complexity score based on the cumulative node complexity"""
    adjusted_cumulative_complexity = node.cumulative_node_complexity.copy()
    for with_query_block, count in node.referenced_ctes.items():
        for category, value in with_query_block.cumulative_node_complexity.items():
            adjusted_cumulative_complexity[category] -= (count - 1) * value

        # Adjustment for each WITH query block being replaced by select * from cte
        adjusted_cumulative_complexity[PlanNodeCategory.COLUMN] = (
            adjusted_cumulative_complexity.get(PlanNodeCategory.COLUMN, 0) + count
        )

    score = sum(adjusted_cumulative_complexity.values())
    return score
