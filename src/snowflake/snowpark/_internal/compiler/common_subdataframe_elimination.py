#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from collections import defaultdict
from typing import Dict, List, Set, Union

from snowflake.snowpark._internal.analyzer.select_statement import (
    Selectable,
    SelectSnowflakePlan,
    SelectStatement,
    SetStatement,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import LogicalPlan
from snowflake.snowpark._internal.compiler.query_generator import QueryGenerator
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)

TreeNode = Union[SnowflakePlan, Selectable]


class CommonSubDataframeElimination:
    _logical_plans: List[LogicalPlan]
    _node_count_map: Dict[LogicalPlan, int]
    _node_parents_map: Dict[LogicalPlan, Set[LogicalPlan]]
    _duplicated_nodes: Set[LogicalPlan]
    _query_generator: QueryGenerator

    def __init__(self, logical_plans: List[LogicalPlan], query_generator: QueryGenerator) -> None:
        self._logical_plans = logical_plans
        self._query_generator = query_generator
        self._node_count_map = defaultdict(int)
        self._node_parents_map = defaultdict(set)
        self._duplicated_nodes = set()


