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
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    LogicalPlan,
    WithObjectRef,
    WithQueryBlock,
)
from snowflake.snowpark._internal.optimizer.query_generator import QueryGenerator
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)

TreeNode = Union[SnowflakePlan, Selectable]

