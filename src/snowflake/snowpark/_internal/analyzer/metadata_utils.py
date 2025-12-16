#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from enum import Enum
from dataclasses import dataclass
from typing import TYPE_CHECKING, DefaultDict, Dict, List, Optional, Union
from snowflake.snowpark.types import DataType

from snowflake.snowpark._internal.analyzer.expression import (
    Attribute,
    Expression,
    Literal,
    Star,
)
from snowflake.snowpark._internal.analyzer.datatype_mapper import to_sql
from snowflake.snowpark._internal.analyzer.unary_expression import Alias
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    Limit,
    LogicalPlan,
    SnowflakeValues,
)
from snowflake.snowpark._internal.analyzer.unary_expression import UnresolvedAlias
from snowflake.snowpark._internal.utils import ExprAliasUpdateDict

if TYPE_CHECKING:
    from snowflake.snowpark._internal.analyzer.analyzer import Analyzer


class DescribeQueryTelemetryField(Enum):
    TYPE_DESCRIBE_QUERY_DETAILS = "snowpark_describe_query_details"
    SQL_TEXT = "sql_text"
    E2E_TIME = "e2e_time"
    STACK_TRACE = "stack_trace"


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
    df_aliased_col_name_to_real_col_name: Union[
        DefaultDict[str, Dict[str, str]], DefaultDict[str, ExprAliasUpdateDict]
    ],
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


def _extract_inferable_attribute_names(
    attributes: Optional[List[Expression]],
) -> tuple[Optional[List[Attribute]], Optional[List[Attribute]]]:
    """
    Returns a list of attribute names that can be infered from a list of Expressions.
    Returns None if one or more attributes cannot be infered.
    """
    if attributes is None:
        return None, None

    new_attributes = []
    old_attributes = []
    for attr in attributes:
        # Attributes are already resolved and don't require inferrence
        if isinstance(attr, Attribute):
            old_attributes.append(attr)
            continue

        if isinstance(attr, Alias):
            # If the first non-aliased child of an Alias node is Literal or Attribute
            # the column can be inferred.
            if isinstance(attr.child, (Literal, Attribute)) and attr.datatype:
                attr = Attribute(attr.name, attr.datatype, attr.nullable)
        elif isinstance(attr, Literal) and type(attr.datatype) != DataType:
            # Names of literal values can be inferred
            attr = Attribute(
                to_sql(attr.value, attr.datatype), attr.datatype, attr.nullable
            )

        # If the attr has been coerced to attribute then it has been inferred
        if isinstance(attr, Attribute):
            new_attributes.append(attr)
        else:
            return None, None
    return old_attributes, new_attributes


def _extract_selectable_attributes(
    current_plan: LogicalPlan,
) -> Optional[List[Attribute]]:
    from snowflake.snowpark._internal.analyzer.select_statement import (
        SelectSQL,
        SelectSnowflakePlan,
        SelectStatement,
        SelectTableFunction,
        SelectableEntity,
    )

    """Extracts known attributes from a LogicalPlan. Uses the plans attributes if available, otherwise
    attempts to extract all known attributes from child plans."""
    attributes: Optional[List[Attribute]] = None
    if isinstance(current_plan, SelectStatement):
        # When attributes is cached on source_plan, just use it
        if current_plan.attributes is not None:
            attributes = current_plan.attributes
        else:
            # Get the attributes from the child plan
            from_attributes = _extract_selectable_attributes(current_plan.from_)
            (
                expected_attributes,
                new_attributes,
                # Extract expected attributes and knowable new attributes
                # from current plan
            ) = _extract_inferable_attribute_names(current_plan.projection)
            # Check that the expected attributes match the attributes from the child plan
            if (
                from_attributes is not None
                and expected_attributes is not None
                and new_attributes is not None
            ):
                missing_attrs = {attr.name for attr in expected_attributes} - {
                    attr.name for attr in from_attributes
                }
                if not missing_attrs and all(
                    isinstance(attr, (Attribute, Alias))
                    # If the attribute datatype is specifically DataType then it is not fully resolved
                    and type(attr.datatype) is not DataType
                    for attr in current_plan.projection or []
                ):
                    attributes = current_plan.projection  # type: ignore
    elif (
        isinstance(
            current_plan,
            (SelectSnowflakePlan, SelectTableFunction, SelectSQL),
        )
        and current_plan._snowflake_plan is not None
    ):
        # These types have a source snowflake plan.
        # Use its metadata if available.
        attributes = current_plan._snowflake_plan._metadata.attributes
    elif isinstance(current_plan, SelectableEntity):
        # Similar to the previous case, but could have attributes defined already
        if current_plan.attributes is not None:
            attributes = current_plan.attributes
        elif current_plan._snowflake_plan is not None:
            attributes = current_plan._snowflake_plan._metadata.attributes
    return attributes


def infer_metadata(
    source_plan: Optional[LogicalPlan],
    analyzer: "Analyzer",
    df_aliased_col_name_to_real_col_name: Union[
        DefaultDict[str, Dict[str, str]], DefaultDict[str, ExprAliasUpdateDict]
    ],
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
    from snowflake.snowpark._internal.analyzer.select_statement import (
        SelectStatement,
        SelectableEntity,
    )
    from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan
    from snowflake.snowpark._internal.analyzer.unary_plan_node import (
        Aggregate,
        Filter,
        Project,
        Sample,
        Sort,
        Distinct,
    )

    attributes = None
    quoted_identifiers = None
    if analyzer.session.reduce_describe_query_enabled and source_plan is not None:
        # If source_plan is a LogicalPlan, SQL simplifier is not enabled
        # so we can try to infer the metadata from its child (SnowflakePlan)
        # When source_plan is Filter, Sort, Limit, Sample, metadata won't be changed
        # so we can use the metadata from its child directly
        if isinstance(source_plan, (Filter, Sort, Limit, Sample, Distinct)):
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
        # If source_plan is a SelectableEntity or SelectStatement, SQL simplifier is enabled
        elif isinstance(source_plan, SelectableEntity):
            if source_plan.attributes is not None:
                attributes = source_plan.attributes
        elif isinstance(source_plan, SelectStatement):
            attributes = _extract_selectable_attributes(source_plan)
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
            if not source_plan.has_projection:
                # We can only retrieve the cached metadata when there is an underlying SnowflakePlan
                # or it's a SelectableEntity
                if source_plan.from_._snowflake_plan is not None:
                    # only set attributes and quoted_identifiers if they are not set in previous step
                    if (
                        attributes is None
                        and source_plan.from_._snowflake_plan._metadata.attributes
                        is not None
                    ):
                        attributes = (
                            source_plan.from_._snowflake_plan._metadata.attributes
                        )
                    elif (
                        quoted_identifiers is None
                        and source_plan.from_._snowflake_plan._metadata.quoted_identifiers
                        is not None
                    ):
                        quoted_identifiers = (
                            source_plan.from_._snowflake_plan._metadata.quoted_identifiers
                        )
                elif (
                    isinstance(source_plan.from_, SelectableEntity)
                    and source_plan.from_.attributes is not None
                ):
                    attributes = source_plan.from_.attributes

        # If attributes is available, we always set quoted_identifiers to None
        # as it can be retrieved later from attributes
        if attributes is not None and quoted_identifiers is not None:
            quoted_identifiers = None

    return PlanMetadata(attributes=attributes, quoted_identifiers=quoted_identifiers)


def cache_metadata_if_selectable(
    source_plan: Optional[LogicalPlan], metadata: PlanMetadata
) -> None:
    """
    Cache metadata on a Selectable source plan.
    """
    from snowflake.snowpark._internal.analyzer.select_statement import (
        SelectStatement,
        SelectableEntity,
        Selectable,
    )

    if (
        isinstance(source_plan, Selectable)
        and source_plan._session.reduce_describe_query_enabled
    ):
        if isinstance(source_plan, SelectableEntity):
            source_plan.attributes = metadata.attributes
        elif isinstance(source_plan, SelectStatement):
            source_plan.attributes = metadata.attributes
            # When source_plan doesn't have a projection, it's a simple `SELECT * from ...`,
            # which means source_plan has the same metadata as its child plan,
            # we should cache it on the child plan too.
            # This is necessary SelectStatement.select() will need the column states of the child plan
            # (check the implementation of derive_column_states_from_subquery().
            if not source_plan.has_projection:
                if source_plan.from_._snowflake_plan is not None:
                    source_plan.from_._snowflake_plan._metadata = metadata
                elif isinstance(source_plan.from_, SelectableEntity):
                    source_plan.from_.attributes = metadata.attributes
