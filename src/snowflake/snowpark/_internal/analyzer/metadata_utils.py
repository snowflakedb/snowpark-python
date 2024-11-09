#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from dataclasses import dataclass
from typing import TYPE_CHECKING, DefaultDict, Dict, List, Optional

from snowflake.snowpark._internal.analyzer.expression import Attribute, Expression, Star
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    Limit,
    LogicalPlan,
    SnowflakeValues,
)
from snowflake.snowpark._internal.analyzer.unary_expression import UnresolvedAlias

if TYPE_CHECKING:
    from snowflake.snowpark._internal.analyzer.analyzer import Analyzer


@dataclass(frozen=True)
class PlanMetadata:
    """
    Metadata of a plan including attributes (schema) and quoted identifiers (column names).
    """

    attributes: Optional[List[Attribute]]
    quoted_identifiers: Optional[List[str]]

    def __post_init__(self):
        # If attributes is not None, then quoted_identifiers will be explicitly set to None.
        # If quoted_identifiers is not None, then attributes will be None because we can't infer data types.
        assert not (self.attributes is not None and self.quoted_identifiers is not None)

    @property
    def has_cached_quoted_identifiers(self) -> bool:
        return self.attributes is not None or self.quoted_identifiers is not None


def infer_quoted_identifiers_from_expressions(
    expressions: List[Expression],
    analyzer: "Analyzer",
    df_aliased_col_name_to_real_col_name: DefaultDict[str, Dict[str, str]],
) -> Optional[List[str]]:
    """
    Infer quoted identifiers from (named) expressions.
    The list of quoted identifier will be only returned
    if and only if the identifier can be derived from all expressions.
    """
    from snowflake.snowpark._internal.analyzer.select_statement import parse_column_name
    from snowflake.snowpark._internal.utils import quote_name

    result = []
    for e in expressions:
        # If we do select *, we may not be able to get all current quoted identifiers
        # (e.g., when SQL simplifier is disabled), so we just be conservative and do
        # not perform any inference in this case.
        if isinstance(e, UnresolvedAlias) and isinstance(e.child, Star):
            return None
        column_name = parse_column_name(
            e, analyzer, df_aliased_col_name_to_real_col_name
        )
        if column_name is not None:
            result.append(quote_name(column_name))
        else:
            return None
    return result


def infer_metadata(
    source_plan: Optional[LogicalPlan],
    analyzer: "Analyzer",
    df_aliased_col_name_to_real_col_name: DefaultDict[str, Dict[str, str]],
) -> PlanMetadata:
    """
    Infer metadata from the source plan.
    Returns the metadata including attributes (schema) and quoted identifiers (column names).
    """
    from snowflake.snowpark._internal.analyzer.binary_plan_node import (
        Join,
        LeftAnti,
        LeftSemi,
    )
    from snowflake.snowpark._internal.analyzer.select_statement import SelectStatement
    from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
    from snowflake.snowpark._internal.analyzer.unary_plan_node import (
        Aggregate,
        Filter,
        Project,
        Sample,
        Sort,
    )

    attributes = None
    quoted_identifiers = None
    if analyzer.session.reduce_describe_query_enabled and source_plan is not None:
        # If source_plan is a LogicalPlan, SQL simplifier is not enabled
        # so we can try to infer the metadata from its child (SnowflakePlan)
        # When source_plan is Filter, Sort, Limit, Sample, metadata won't be changed
        # so we can use the metadata from its child directly
        if isinstance(source_plan, (Filter, Sort, Limit, Sample)):
            if isinstance(source_plan.child, SnowflakePlan):
                attributes = source_plan.child._metadata.attributes
                quoted_identifiers = source_plan.child._metadata.quoted_identifiers
        # When source_plan is a SnowflakeValues, metadata is already defined locally
        elif isinstance(source_plan, SnowflakeValues):
            attributes = source_plan.output
        # When source_plan is Aggregate or Project, we already have quoted_identifiers
        elif isinstance(source_plan, Aggregate):
            quoted_identifiers = infer_quoted_identifiers_from_expressions(
                source_plan.aggregate_expressions,  # type: ignore
                analyzer,
                df_aliased_col_name_to_real_col_name,
            )
        elif isinstance(source_plan, Project):
            quoted_identifiers = infer_quoted_identifiers_from_expressions(
                source_plan.project_list,  # type: ignore
                analyzer,
                df_aliased_col_name_to_real_col_name,
            )
        # When source_plan is a Join, we only infer its quoted identifiers
        # if two plans don't have any common quoted identifier
        # This is conservative and avoids parsing join condition, but works for Snowpark pandas,
        # because join DataFrames in Snowpark pandas guarantee that the column names are unique.
        elif isinstance(source_plan, Join):
            if (
                isinstance(source_plan.left, SnowflakePlan)
                and isinstance(source_plan.right, SnowflakePlan)
                and source_plan.left._metadata.has_cached_quoted_identifiers
                and source_plan.right._metadata.has_cached_quoted_identifiers
            ):
                quoted_identifiers = (
                    source_plan.left.quoted_identifiers
                    if isinstance(source_plan.join_type, (LeftAnti, LeftSemi))
                    else source_plan.left.quoted_identifiers
                    + source_plan.right.quoted_identifiers
                )
                # if there is common quoted identifier, reset it to None
                if len(quoted_identifiers) != len(set(quoted_identifiers)):
                    quoted_identifiers = None
        # If source_plan is a SelectStatement, SQL simplifier is enabled
        elif isinstance(source_plan, SelectStatement):
            # When attributes is cached on source_plan, just use it
            if source_plan._attributes is not None:
                attributes = source_plan._attributes
            # When _column_states.projection is available, we can just use it,
            # which is either (only one happen):
            # 1) cached on self._snowflake_plan._quoted_identifiers
            # 2) inferred in `derive_column_states_from_subquery` during `select()` call
            if source_plan._column_states is not None:
                quoted_identifiers = [
                    c.name for c in source_plan._column_states.projection
                ]
            # When source_plan doesn't have a projection, it's a simple `SELECT * from ...`,
            # which means source_plan has the same metadata as its child plan, we can use it directly
            if (
                source_plan.projection is None
                and source_plan.from_._snowflake_plan is not None
            ):
                # only set attributes and quoted_identifiers if they are not set in previous step
                if (
                    attributes is None
                    and source_plan.from_._snowflake_plan._metadata.attributes
                    is not None
                ):
                    attributes = source_plan.from_._snowflake_plan._metadata.attributes
                elif (
                    quoted_identifiers is None
                    and source_plan.from_._snowflake_plan._metadata.quoted_identifiers
                    is not None
                ):
                    quoted_identifiers = (
                        source_plan.from_._snowflake_plan._metadata.quoted_identifiers
                    )

        # If attributes is available, we always set quoted_identifiers to None
        # as it can be retrieved later from attributes
        if attributes is not None:
            quoted_identifiers = None

    return PlanMetadata(attributes=attributes, quoted_identifiers=quoted_identifiers)


def cache_metadata_if_select_statement(
    source_plan: Optional[LogicalPlan], metadata: PlanMetadata
) -> None:
    """
    Cache metadata on a SelectStatement source plan.
    """
    from snowflake.snowpark._internal.analyzer.select_statement import SelectStatement

    if (
        isinstance(source_plan, SelectStatement)
        and source_plan.analyzer.session.reduce_describe_query_enabled
    ):
        source_plan._attributes = metadata.attributes
        # When source_plan doesn't have a projection, it's a simple `SELECT * from ...`,
        # which means source_plan has the same metadata as its child plan,
        # we should cache it on the child plan too.
        # This is necessary SelectStatement.select() will need the column states of the child plan
        # (check the implementation of derive_column_states_from_subquery().
        if (
            source_plan.projection is None
            and source_plan.from_._snowflake_plan is not None
        ):
            source_plan.from_._snowflake_plan._metadata = metadata
