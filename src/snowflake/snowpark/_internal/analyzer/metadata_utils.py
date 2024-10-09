#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import List, Optional, Tuple

from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import Limit, LogicalPlan


def infer_metadata(
    source_plan: LogicalPlan,
) -> Tuple[Optional[List[Attribute]], Optional[List[str]]]:
    """
    Infer metadata from the source plan.
    Returns the metadata including attributes (schema) and quoted identifiers (column names).
    """
    from snowflake.snowpark._internal.analyzer.select_statement import (
        Selectable,
        SelectStatement,
    )
    from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
    from snowflake.snowpark._internal.analyzer.unary_plan_node import (
        Filter,
        Sample,
        Sort,
    )

    attributes = None
    quoted_identifiers = None
    # If source_plan is a LogicalPlan, SQL simplfiier is not enabled
    # so we can try to infer the metadata from its child (SnowflakePlan)
    # When source_plan is Filter, Sort, Limit, Sample, metadata won't be changed
    # so we can use the metadata from its child directly
    if isinstance(source_plan, (Filter, Sort, Limit, Sample)):
        if isinstance(source_plan.child, SnowflakePlan):
            attributes = source_plan.child._attributes
            quoted_identifiers = source_plan.child._quoted_identifiers
    # If source_plan is a SelectStatement, SQL simplifier is enabled
    elif isinstance(source_plan, SelectStatement):
        # When source_plan._snowflake_plan is not None, `get_snowflake_plan` is called
        # to create a new SnowflakePlan and `infer_metadata` is already called on the new plan.
        if source_plan._snowflake_plan is not None:
            attributes = source_plan._snowflake_plan._attributes
            quoted_identifiers = source_plan._snowflake_plan._quoted_identifiers
        # When source_plan.from_ is a SelectSnowflakePlan and it doesn't have a projection,
        # it's a simple `SELECT * from ...`, which has the same metadata as it's child plan (source_plan.from_).
        elif (
            isinstance(source_plan.from_, Selectable)
            and source_plan.projection is None
            and source_plan.from_._snowflake_plan is not None
        ):
            attributes = source_plan.from_.snowflake_plan._attributes
            quoted_identifiers = source_plan.from_.snowflake_plan._quoted_identifiers

    # If attributes is available, we always set quoted_identifiers to None
    # as it can be retrieved later from attributes
    if attributes is not None:
        quoted_identifiers = None

    return attributes, quoted_identifiers
