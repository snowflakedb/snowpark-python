#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import copy
import difflib
from logging import getLogger
import re
import sys
import uuid
from collections import defaultdict, deque
from enum import Enum
from dataclasses import dataclass
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    DefaultDict,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    PlanNodeCategory,
    PlanState,
)
from snowflake.snowpark._internal.analyzer.table_function import (
    GeneratorTableFunction,
    TableFunctionRelation,
    TableFunctionJoin,
)
from snowflake.snowpark._internal.debug_utils import (
    get_df_transform_trace_message,
    get_python_source_from_sql_error,
    get_existing_object_context,
    get_missing_object_context,
)

if TYPE_CHECKING:
    from snowflake.snowpark._internal.analyzer.select_statement import (
        Selectable,
    )  # pragma: no cover
    import snowflake.snowpark.session
    import snowflake.snowpark.dataframe
    from snowflake.snowpark.udtf import UserDefinedTableFunction

import snowflake.connector
import snowflake.snowpark
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    quote_name_without_upper_casing,
    TEMPORARY_STRING_SET,
    aggregate_statement,
    attribute_to_schema_string,
    batch_insert_into_statement,
    copy_into_location,
    copy_into_table,
    create_file_format_statement,
    create_or_replace_dynamic_table_statement,
    create_or_replace_view_statement,
    create_table_as_select_statement,
    create_table_statement,
    delete_statement,
    drop_file_format_if_exists_statement,
    drop_table_if_exists_statement,
    file_operation_statement,
    filter_statement,
    insert_into_statement,
    join_statement,
    join_table_function_statement,
    lateral_statement,
    limit_statement,
    merge_statement,
    pivot_statement,
    project_statement,
    rename_statement,
    result_scan_statement,
    sample_by_statement,
    sample_statement,
    schema_cast_named,
    schema_cast_seq,
    schema_value_statement,
    select_from_path_with_format_statement,
    set_operator_statement,
    sort_statement,
    table_function_statement,
    unpivot_statement,
    update_statement,
)
from snowflake.snowpark._internal.analyzer.binary_plan_node import (
    JoinType,
    SetOperation,
)
from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.analyzer.metadata_utils import (
    PlanMetadata,
    cache_metadata_if_selectable,
    infer_metadata,
)
from snowflake.snowpark._internal.analyzer.schema_utils import (
    analyze_attributes,
    cached_analyze_attributes,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    DynamicTableCreateMode,
    LogicalPlan,
    ReadFileNode,
    SaveMode,
    SelectFromFileNode,
    SelectWithCopyIntoTableNode,
    TableCreationSource,
    WithQueryBlock,
)
from snowflake.snowpark._internal.compiler.cte_utils import (
    encode_node_id_with_query,
    find_duplicate_subtrees,
    merge_referenced_ctes,
)
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.utils import (
    INFER_SCHEMA_FORMAT_TYPES,
    XML_ROW_TAG_STRING,
    XML_ROW_DATA_COLUMN_NAME,
    TempObjectType,
    generate_random_alphanumeric,
    get_copy_into_table_options,
    is_sql_select_statement,
    merge_multiple_snowflake_plan_expr_to_alias,
    random_name_for_temp_object,
    ExprAliasUpdateDict,
    UNQUOTED_CASE_INSENSITIVE,
    remove_comments,
    get_line_numbers,
)
from snowflake.snowpark.row import Row
from snowflake.snowpark.types import StructType
import snowflake.snowpark.context as context

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

_logger = getLogger(__name__)


class SnowflakePlan(LogicalPlan):
    class Decorator:
        __wrap_exception_regex_match = re.compile(
            r"""(?s).*invalid identifier '"?([^'"]*)"?'.*"""
        )
        __wrap_exception_regex_match_with_double_quotes = re.compile(
            r"""(?s).*invalid identifier '([^']*)?'.*"""
        )
        __wrap_exception_regex_sub = re.compile(r"""^"|"$""")

        @staticmethod
        def wrap_exception(func):
            """This wrapper is used to wrap snowflake connector ProgrammingError into SnowparkSQLException.
            It also adds additional debug information to the raised exception when possible.
            """

            def wrap(*args, **kwargs):
                from snowflake.snowpark.context import (
                    _enable_dataframe_trace_on_error,
                    _enable_trace_sql_errors_to_dataframe,
                )

                try:
                    return func(*args, **kwargs)
                except snowflake.connector.errors.ProgrammingError as e:
                    from snowflake.snowpark._internal.analyzer.select_statement import (
                        Selectable,
                    )

                    query = getattr(e, "query", None)
                    tb = sys.exc_info()[2]
                    assert e.msg is not None
                    # extract top_plan, df_ast_id, stmt_cache from args
                    top_plan, df_ast_id, stmt_cache = None, None, None
                    for arg in args:
                        if isinstance(arg, SnowflakePlan):
                            top_plan = arg
                            break
                    if top_plan is not None and top_plan.df_ast_ids:
                        df_ast_id = top_plan.df_ast_ids[-1]
                        stmt_cache = top_plan.session._ast_batch._bind_stmt_cache

                    debug_context_arr = []
                    try:
                        if (
                            "SQL compilation error:" in e.msg
                            and "error line" in e.msg
                            and top_plan is not None
                            and _enable_trace_sql_errors_to_dataframe
                        ):
                            if error_source_context := get_python_source_from_sql_error(
                                top_plan, e.msg
                            ):
                                debug_context_arr.append(error_source_context)
                        if (
                            _enable_dataframe_trace_on_error
                            and df_ast_id is not None
                            and stmt_cache is not None
                        ):
                            if df_transform_trace := get_df_transform_trace_message(
                                df_ast_id, stmt_cache
                            ):
                                debug_context_arr.append(df_transform_trace)
                        if (
                            "does not exist or not authorized" in e.msg
                            and top_plan is not None
                            and _enable_trace_sql_errors_to_dataframe
                        ):
                            if missing_object_context := get_missing_object_context(
                                top_plan, e.msg
                            ):
                                debug_context_arr.append(missing_object_context)
                        if (
                            ("already exists" in e.msg)
                            and top_plan is not None
                            and context._enable_trace_sql_errors_to_dataframe
                        ):
                            if existing_object_context := get_existing_object_context(
                                top_plan, e.msg
                            ):
                                debug_context_arr.append(existing_object_context)
                    except Exception as trace_error:
                        # If we encounter an error when getting the df_transform_debug_trace,
                        # we will ignore the error and not add the debug trace to the error message.
                        _logger.info(
                            f"Error when getting the df_transform_debug_trace: {trace_error}"
                        )
                        pass

                    if debug_context_arr:
                        debug_header = "\n\n--- Additional Debug Information ---\n"
                        debug_context = debug_header + "\n".join(debug_context_arr)
                    else:
                        debug_context = ""

                    if "unexpected 'as'" in e.msg.lower():
                        ne = SnowparkClientExceptionMessages.SQL_PYTHON_REPORT_UNEXPECTED_ALIAS(
                            query, debug_context=debug_context
                        )
                        raise ne.with_traceback(tb) from None
                    elif e.sqlstate == "42000" and "invalid identifier" in e.msg:
                        match = (
                            SnowflakePlan.Decorator.__wrap_exception_regex_match.match(
                                e.msg
                            )
                        )
                        if not match:  # pragma: no cover
                            ne = SnowparkClientExceptionMessages.SQL_EXCEPTION_FROM_PROGRAMMING_ERROR(
                                e, debug_context=debug_context
                            )
                            raise ne.with_traceback(tb) from None
                        col = match.group(1)
                        children = [
                            arg for arg in args if isinstance(arg, SnowflakePlan)
                        ]
                        remapped = [
                            SnowflakePlan.Decorator.__wrap_exception_regex_sub.sub(
                                "", val
                            )
                            for child in children
                            for val in child.expr_to_alias.values()
                        ]
                        if col in remapped:
                            unaliased_cols = (
                                snowflake.snowpark.dataframe._get_unaliased(col)
                            )
                            orig_col_name = (
                                unaliased_cols[0] if unaliased_cols else "<colname>"
                            )
                            ne = SnowparkClientExceptionMessages.SQL_PYTHON_REPORT_INVALID_ID(
                                orig_col_name,
                                query,
                                debug_context=debug_context,
                            )
                            raise ne.with_traceback(tb) from None
                        elif (
                            len(
                                [
                                    unaliased
                                    for item in remapped
                                    for unaliased in snowflake.snowpark.dataframe._get_unaliased(
                                        item
                                    )
                                    if unaliased == col
                                ]
                            )
                            > 1
                        ):
                            ne = SnowparkClientExceptionMessages.SQL_PYTHON_REPORT_JOIN_AMBIGUOUS(
                                col, col, query, debug_context=debug_context
                            )
                            raise ne.with_traceback(tb) from None
                        else:
                            # We need the potential double quotes for invalid identifier
                            match = SnowflakePlan.Decorator.__wrap_exception_regex_match_with_double_quotes.match(
                                e.msg
                            )
                            if not match:  # pragma: no cover
                                ne = SnowparkClientExceptionMessages.SQL_EXCEPTION_FROM_PROGRAMMING_ERROR(
                                    e, debug_context=debug_context
                                )
                                raise ne.with_traceback(tb) from None
                            col = match.group(1)

                            quoted_identifiers = []
                            for child in children:
                                plan_nodes = child.children_plan_nodes
                                for node in plan_nodes:
                                    if isinstance(node, Selectable):
                                        quoted_identifiers.extend(
                                            node.snowflake_plan.quoted_identifiers
                                        )
                                    else:
                                        quoted_identifiers.extend(
                                            node.quoted_identifiers
                                        )

                            # No context available to enhance error message
                            if not quoted_identifiers:
                                ne = SnowparkClientExceptionMessages.SQL_EXCEPTION_FROM_PROGRAMMING_ERROR(
                                    e
                                )
                                raise ne.with_traceback(tb) from None

                            def add_single_quote(string: str) -> str:
                                return f"'{string}'"

                            # We can't display all column identifiers in the error message
                            if len(quoted_identifiers) > 10:
                                quoted_identifiers_str = f"[{', '.join(add_single_quote(q) for q in quoted_identifiers[:10])}, ...]"
                            else:
                                quoted_identifiers_str = f"[{', '.join(add_single_quote(q) for q in quoted_identifiers)}]"

                            msg = (
                                f"There are existing quoted column identifiers: {quoted_identifiers_str}. "
                                f"Please use one of them to reference the column. See more details on Snowflake identifier requirements "
                                f"https://docs.snowflake.com/en/sql-reference/identifiers-syntax"
                            )

                            # Currently, when Snowpark user a Python string as identifier to access a column:
                            # 1) if a column name is unquoted and
                            #   a) contains no special characters, it is automatically uppercased and quoted in SQL, or
                            #   b) if it includes special characters, it is simply quoted without uppercasing.
                            # 2) If the name is explicitly quoted by the user, Snowpark preserves it as-is.
                            # Therefore, if `col` is an invalid identifier, it is most likely due to 1a) above.
                            # We attempt to provide a more helpful error message by suggesting the closest valid identifier.
                            if UNQUOTED_CASE_INSENSITIVE.match(col):
                                identifier = quote_name_without_upper_casing(
                                    col.lower()
                                )
                                match = difflib.get_close_matches(
                                    identifier, quoted_identifiers
                                )
                                if match:
                                    # if there is an exact match, just remind users this one
                                    if identifier in match:
                                        match = [identifier]
                                    msg = f"{msg}\nDo you mean {' or '.join(add_single_quote(q) for q in match)}?"

                            e.msg = f"{e.msg}\n{msg}"
                            ne = SnowparkClientExceptionMessages.SQL_EXCEPTION_FROM_PROGRAMMING_ERROR(
                                e, debug_context=debug_context
                            )
                            raise ne.with_traceback(tb) from None
                    elif e.sqlstate == "42601" and "SELECT with no columns" in e.msg:
                        # This is a special case when the select statement has no columns,
                        # and it's a reading XML query.

                        def search_read_file_node(
                            node: Union[SnowflakePlan, Selectable]
                        ) -> Optional[ReadFileNode]:
                            source_plan = (
                                node.source_plan
                                if isinstance(node, SnowflakePlan)
                                else node.snowflake_plan.source_plan
                            )
                            if isinstance(source_plan, ReadFileNode):
                                return source_plan
                            for child in node.children_plan_nodes:
                                result = search_read_file_node(child)
                                if result:
                                    return result
                            return None

                        for arg in args:
                            if isinstance(arg, SnowflakePlan):
                                read_file_node = search_read_file_node(arg)
                                if (
                                    read_file_node
                                    and read_file_node.xml_reader_udtf is not None
                                ):
                                    row_tag = read_file_node.options.get(
                                        XML_ROW_TAG_STRING
                                    )
                                    file_path = read_file_node.path
                                    ne = SnowparkClientExceptionMessages.DF_XML_ROW_TAG_NOT_FOUND(
                                        row_tag, file_path
                                    )
                                    raise ne.with_traceback(tb) from None
                            # when the describe query fails, the arg is a query string
                            elif isinstance(arg, str):
                                if f'"{XML_ROW_DATA_COLUMN_NAME}"' in arg:
                                    ne = (
                                        SnowparkClientExceptionMessages.DF_XML_ROW_TAG_NOT_FOUND()
                                    )
                                    raise ne.with_traceback(tb) from None

                    ne = SnowparkClientExceptionMessages.SQL_EXCEPTION_FROM_PROGRAMMING_ERROR(
                        e, debug_context=debug_context
                    )
                    raise ne.with_traceback(tb) from None

            return wrap

    def __init__(
        self,
        queries: List["Query"],
        # schema_query will be None for the SnowflakePlan node build
        # during the compilation stage.
        schema_query: Optional[str],
        post_actions: Optional[List["Query"]] = None,
        expr_to_alias: Optional[Dict[uuid.UUID, str]] = None,
        source_plan: Optional[LogicalPlan] = None,
        is_ddl_on_temp_object: bool = False,
        api_calls: Optional[List[Dict]] = None,
        df_aliased_col_name_to_real_col_name: Optional[
            Union[
                DefaultDict[str, Dict[str, str]], DefaultDict[str, ExprAliasUpdateDict]
            ]
        ] = None,
        # This field records all the WithQueryBlocks and their reference count that are
        # referred by the current SnowflakePlan tree. This is needed for the final query
        # generation to generate the correct sql query with CTE definition.
        referenced_ctes: Optional[Dict[WithQueryBlock, int]] = None,
        *,
        session: "snowflake.snowpark.session.Session",
        from_selectable_uuid: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.queries = queries
        self.schema_query = schema_query
        self.post_actions = post_actions if post_actions else []
        self.expr_to_alias = (
            expr_to_alias
            if expr_to_alias
            else (ExprAliasUpdateDict() if session._join_alias_fix else {})
        )
        self.session = session
        self.source_plan = source_plan
        self.is_ddl_on_temp_object = is_ddl_on_temp_object
        # We need to copy this list since we don't want to change it for the
        # previous SnowflakePlan objects
        self.api_calls = api_calls.copy() if api_calls else []
        self._output_dict = None
        # Used for dataframe alias
        if df_aliased_col_name_to_real_col_name:
            self.df_aliased_col_name_to_real_col_name = (
                df_aliased_col_name_to_real_col_name
            )
        else:
            self.df_aliased_col_name_to_real_col_name = (
                defaultdict(ExprAliasUpdateDict)
                if self.session._join_alias_fix
                else defaultdict(dict)
            )
        self._uuid = from_selectable_uuid if from_selectable_uuid else str(uuid.uuid4())
        # We set the query line intervals for the last query in the queries list
        self.set_last_query_line_intervals()
        # In the placeholder query, subquery (child) is held by the ID of query plan
        # It is used for optimization, by replacing a subquery with a CTE
        # encode an id for CTE optimization. This is generated based on the main
        # query, query parameters and the node type. We use this id for equality
        # comparison to determine if two plans are the same.
        self.encoded_node_id_with_query = encode_node_id_with_query(self)
        self.referenced_ctes: Dict[WithQueryBlock, int] = (
            referenced_ctes.copy() if referenced_ctes else dict()
        )
        self._cumulative_node_complexity: Optional[Dict[PlanNodeCategory, int]] = None
        # UUID for the plan to uniquely identify the SnowflakePlan object. We also use this
        # to UUID track queries that are generated from the same plan.
        # Metadata for the plan
        self._metadata: PlanMetadata = infer_metadata(
            self.source_plan,
            self.session._analyzer,
            self.df_aliased_col_name_to_real_col_name,
        )
        self._plan_state: Optional[Dict[PlanState, Any]] = None
        # If the plan has an associated DataFrame, and this Dataframe has an ast_id,
        # we will store the ast_id here.
        self.df_ast_ids: Optional[List[int]] = None

    @property
    def uuid(self) -> str:
        return self._uuid

    @property
    def execution_queries(self) -> Dict["PlanQueryType", List["Query"]]:
        """
        Get the list of queries that will be sent over to server evaluation. The queries
        have optimizations applied of optimizations are enabled.

        Returns
        -------
        A mapping between the PlanQueryType and the list of Queries corresponds to the type.
        """
        from snowflake.snowpark._internal.compiler.plan_compiler import PlanCompiler

        compiler = PlanCompiler(self)
        return compiler.compile()

    @property
    def children_plan_nodes(self) -> List[Union["Selectable", "SnowflakePlan"]]:
        """
        This property is currently only used for traversing the query plan tree
        when performing CTE optimization.
        """
        from snowflake.snowpark._internal.analyzer.select_statement import Selectable

        if self.source_plan:
            if isinstance(self.source_plan, Selectable):
                return self.source_plan.children_plan_nodes
            else:
                return self.source_plan.children
        else:
            return []

    def with_subqueries(self, subquery_plans: List["SnowflakePlan"]) -> "SnowflakePlan":
        pre_queries = self.queries[:-1]
        new_schema_query = self.schema_query
        new_post_actions = [*self.post_actions]
        api_calls = [*self.api_calls]

        for plan in subquery_plans:
            for query in plan.queries[:-1]:
                if query not in pre_queries:
                    pre_queries.append(query)
            # when self.schema_query is None, that means no schema query is propagated during
            # the process, there is no need to update the schema query.
            if (new_schema_query is not None) and (plan.schema_query is not None):
                new_schema_query = new_schema_query.replace(
                    plan.queries[-1].sql, plan.schema_query
                )
            for action in plan.post_actions:
                if action not in new_post_actions:
                    new_post_actions.append(action)
            api_calls.extend(plan.api_calls)

        return SnowflakePlan(
            pre_queries + [self.queries[-1]],
            new_schema_query,
            post_actions=new_post_actions,
            expr_to_alias=self.expr_to_alias,
            session=self.session,
            source_plan=self.source_plan,
            api_calls=api_calls,
            df_aliased_col_name_to_real_col_name=self.df_aliased_col_name_to_real_col_name,
            referenced_ctes=self.referenced_ctes,
        )

    @property
    def quoted_identifiers(self) -> List[str]:
        # If self._metadata.quoted_identifiers is not None (self._metadata.attributes must be None),
        # retrieve quoted identifiers from self._metadata.quoted_identifiers.
        # otherwise, retrieve quoted identifiers from self.attributes
        # (which may trigger a describe query).
        if self._metadata.quoted_identifiers is not None:
            return self._metadata.quoted_identifiers
        else:
            return [attr.name for attr in self.attributes]

    @Decorator.wrap_exception
    def _analyze_attributes(self) -> List[Attribute]:
        assert (
            self.schema_query is not None
        ), "No schema query is available for the SnowflakePlan"
        if self.session.reduce_describe_query_enabled:
            return cached_analyze_attributes(self.schema_query, self.session)
        else:
            return analyze_attributes(self.schema_query, self.session)

    @property
    def attributes(self) -> List[Attribute]:
        if self._metadata.attributes is not None:
            return self._metadata.attributes
        assert (
            self.schema_query is not None
        ), "No schema query is available for the SnowflakePlan"
        attributes = self._analyze_attributes()
        self._metadata = PlanMetadata(attributes=attributes, quoted_identifiers=None)
        # We need to cache attributes on SelectStatement too because df._plan is not
        # carried over to next SelectStatement (e.g., check the implementation of df.filter()).
        cache_metadata_if_selectable(self.source_plan, self._metadata)
        # When the reduce_describe_query_enabled is enabled, we cache the attributes in
        # self._metadata using original schema query. Thus we can update the schema query
        # to simplify plans built on top of this plan.
        # When sql simplifier is disabled, we cannot build nested schema query for example
        #  SELECT  *  FROM (show schemas) LIMIT 1, therefore we need to built the schema
        # query based on the attributes.
        if (
            self.session.reduce_describe_query_enabled
            or not self.session.sql_simplifier_enabled
        ):
            self.schema_query = schema_value_statement(attributes)
        return attributes

    @cached_property
    def output(self) -> List[Attribute]:
        return [Attribute(a.name, a.datatype, a.nullable) for a in self.attributes]

    @property
    def output_dict(self) -> Dict[str, Any]:
        if self._output_dict is None:
            self._output_dict = {
                attr.name: (attr.datatype, attr.nullable) for attr in self.output
            }
        return self._output_dict

    @property
    def plan_state(self) -> Dict[PlanState, Any]:
        with self.session._plan_lock:
            if self._plan_state is not None:
                # return the cached plan state
                return self._plan_state

            from snowflake.snowpark._internal.analyzer.select_statement import (
                SelectStatement,
            )

            # calculate plan height and num_selects_with_complexity_merged
            height = 0
            num_selects_with_complexity_merged = 0
            current_level = [self]
            while len(current_level) > 0:
                next_level = []
                for node in current_level:
                    next_level.extend(node.children_plan_nodes)
                    if (
                        isinstance(node, SelectStatement)
                        and node._merge_projection_complexity_with_subquery
                    ):
                        num_selects_with_complexity_merged += 1
                height += 1
                current_level = next_level
            # calculate the repeated node status
            (
                cte_nodes,
                duplicated_node_complexity_distribution,
            ) = find_duplicate_subtrees(self, propagate_complexity_hist=True)

            self._plan_state = {
                PlanState.PLAN_HEIGHT: height,
                PlanState.NUM_SELECTS_WITH_COMPLEXITY_MERGED: num_selects_with_complexity_merged,
                PlanState.NUM_CTE_NODES: len(cte_nodes),
                PlanState.DUPLICATED_NODE_COMPLEXITY_DISTRIBUTION: duplicated_node_complexity_distribution,
            }
            return self._plan_state

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        if self.source_plan:
            return self.source_plan.individual_node_complexity
        return {}

    @property
    def cumulative_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        with self.session._plan_lock:
            if self._cumulative_node_complexity is None:
                # if source plan is available, the source plan complexity
                # is the snowflake plan complexity.
                if self.source_plan:
                    self._cumulative_node_complexity = (
                        self.source_plan.cumulative_node_complexity
                    )
                else:
                    self._cumulative_node_complexity = {}
            return self._cumulative_node_complexity

    @cumulative_node_complexity.setter
    def cumulative_node_complexity(self, value: Dict[PlanNodeCategory, int]):
        self._cumulative_node_complexity = value

    def reset_cumulative_node_complexity(self) -> None:
        super().reset_cumulative_node_complexity()
        if self.source_plan:
            self.source_plan.reset_cumulative_node_complexity()

    def set_last_query_line_intervals(self) -> None:
        """
        Sets the query line intervals for the last query in the queries list. When tracing
        sql errors is enabled, we expect the last query to have an SQL query that is commented with
        the uuids of its children. We use these uuids to construct the query line intervals,
        and then remove the comments to get the final SQL query. When tracing sql errors is not enabled,
        this function will do nothing.
        """
        if not context._enable_trace_sql_errors_to_dataframe:
            # No action is needed if feature is not enabled
            return
        from snowflake.snowpark._internal.analyzer.select_statement import (
            Selectable,
        )

        last_query = self.queries[-1]
        child_uuids = []
        for child in self.children_plan_nodes:
            if isinstance(child, (Selectable, SnowflakePlan)):
                child_uuids.append(child.uuid)

        query_line_intervals = get_line_numbers(
            last_query.sql,
            child_uuids,
            self.uuid,
        )
        final_sql = remove_comments(last_query.sql, child_uuids)
        last_query.sql = final_sql
        last_query.query_line_intervals = query_line_intervals

    def __copy__(self) -> "SnowflakePlan":
        if self.session._cte_optimization_enabled:
            plan = SnowflakePlan(
                copy.deepcopy(self.queries) if self.queries else [],
                self.schema_query,
                copy.deepcopy(self.post_actions) if self.post_actions else None,
                copy.copy(self.expr_to_alias) if self.expr_to_alias else None,
                self.source_plan,
                self.is_ddl_on_temp_object,
                copy.deepcopy(self.api_calls) if self.api_calls else None,
                self.df_aliased_col_name_to_real_col_name,
                session=self.session,
                referenced_ctes=self.referenced_ctes,
            )
        else:
            plan = SnowflakePlan(
                self.queries.copy() if self.queries else [],
                self.schema_query,
                self.post_actions.copy() if self.post_actions else None,
                copy.copy(self.expr_to_alias) if self.expr_to_alias else None,
                self.source_plan,
                self.is_ddl_on_temp_object,
                self.api_calls.copy() if self.api_calls else None,
                self.df_aliased_col_name_to_real_col_name,
                session=self.session,
                referenced_ctes=self.referenced_ctes,
            )
        plan.df_ast_ids = self.df_ast_ids
        return plan

    def __deepcopy__(self, memodict={}) -> "SnowflakePlan":  # noqa: B006
        if self.source_plan:
            copied_source_plan = copy.deepcopy(self.source_plan, memodict)
        else:
            copied_source_plan = None

        copied_plan = SnowflakePlan(
            queries=copy.deepcopy(self.queries) if self.queries else [],
            schema_query=self.schema_query,
            post_actions=(
                copy.deepcopy(self.post_actions) if self.post_actions else None
            ),
            expr_to_alias=(
                copy.deepcopy(self.expr_to_alias) if self.expr_to_alias else None
            ),
            source_plan=copied_source_plan,
            is_ddl_on_temp_object=self.is_ddl_on_temp_object,
            api_calls=copy.deepcopy(self.api_calls) if self.api_calls else None,
            df_aliased_col_name_to_real_col_name=(
                copy.deepcopy(self.df_aliased_col_name_to_real_col_name)
                if self.df_aliased_col_name_to_real_col_name
                else None
            ),
            # note that there is no copy of the session object, be careful when using the
            # session object after deepcopy
            session=self.session,
            referenced_ctes=self.referenced_ctes,
        )
        copied_plan._is_valid_for_replacement = True
        if copied_source_plan:
            copied_source_plan._is_valid_for_replacement = True
        copied_plan.df_ast_ids = self.df_ast_ids

        return copied_plan

    def add_aliases(self, to_add: Dict) -> None:
        if self.session._join_alias_fix:
            self.expr_to_alias.update(to_add)
        else:
            self.expr_to_alias = {**self.expr_to_alias, **to_add}

    def propagate_ast_id_to_select_sql(self, ast_id: int) -> None:
        """Propagate df_ast_id to associated SelectSQL instances."""
        from snowflake.snowpark._internal.analyzer.select_statement import SelectSQL

        for child in self.children_plan_nodes:
            if isinstance(child, SelectSQL):
                child.add_df_ast_id(ast_id)

    def add_df_ast_id(self, ast_id: int) -> None:
        """Method to add a df ast id to SnowflakePlan.
        This is used to track the df ast ids that are used in creating the
        sql for this SnowflakePlan.
        """
        if self.df_ast_ids is None:
            self.df_ast_ids = [ast_id]
        elif self.df_ast_ids[-1] != ast_id:
            self.df_ast_ids.append(ast_id)
        self.propagate_ast_id_to_select_sql(ast_id)


class SnowflakePlanBuilder:
    def __init__(
        self,
        session: "snowflake.snowpark.session.Session",
        skip_schema_query: bool = False,
    ) -> None:
        self.session = session
        # Whether skip the schema query build. If true, the schema_query associated
        # with the resolved plan will be None.
        # This option is currently only expected to be used for the query generator applied
        # on the optimized plan. During the final query generation, no schema query is needed,
        # this helps reduces un-necessary overhead for the describing call.
        self._skip_schema_query = skip_schema_query

    @SnowflakePlan.Decorator.wrap_exception
    def build(
        self,
        sql_generator: Callable[[str], str],
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
        schema_query: Optional[str] = None,
        is_ddl_on_temp_object: bool = False,
    ) -> SnowflakePlan:
        select_child = self.add_result_scan_if_not_select(child)
        queries = select_child.queries[:-1] + [
            Query(
                sql_generator(select_child.queries[-1].sql),
                query_id_place_holder="",
                is_ddl_on_temp_object=is_ddl_on_temp_object,
                params=select_child.queries[-1].params,
            )
        ]

        if self._skip_schema_query:
            new_schema_query = None
        else:
            assert (
                child.schema_query is not None
            ), "No schema query is available in child SnowflakePlan"
            new_schema_query = schema_query or sql_generator(child.schema_query)

        return SnowflakePlan(
            queries,
            new_schema_query,
            select_child.post_actions,
            select_child.expr_to_alias,
            source_plan,
            is_ddl_on_temp_object,
            api_calls=select_child.api_calls,
            df_aliased_col_name_to_real_col_name=child.df_aliased_col_name_to_real_col_name,
            session=self.session,
            referenced_ctes=child.referenced_ctes,
        )

    @SnowflakePlan.Decorator.wrap_exception
    def build_binary(
        self,
        sql_generator: Callable[[str, str], str],
        left: SnowflakePlan,
        right: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        select_left = self.add_result_scan_if_not_select(left)
        select_right = self.add_result_scan_if_not_select(right)
        if self._skip_schema_query:
            schema_query = None
        else:
            left_schema_query = schema_value_statement(select_left.attributes)
            right_schema_query = schema_value_statement(select_right.attributes)
            schema_query = sql_generator(left_schema_query, right_schema_query)

        if self.session._join_alias_fix:
            new_expr_to_alias = merge_multiple_snowflake_plan_expr_to_alias(
                [select_left, select_right]
            )
        else:
            common_columns = set(select_left.expr_to_alias.keys()).intersection(
                select_right.expr_to_alias.keys()
            )
            new_expr_to_alias = {
                k: v
                for k, v in {
                    **select_left.expr_to_alias,
                    **select_right.expr_to_alias,
                }.items()
                if k not in common_columns
            }

        api_calls = [*select_left.api_calls, *select_right.api_calls]

        # Need to do a deduplication to avoid repeated query.
        merged_queries = select_left.queries[:-1].copy()
        for query in select_right.queries[:-1]:
            if query not in merged_queries:
                merged_queries.append(copy.copy(query))

        post_actions = select_left.post_actions.copy()
        for post_action in select_right.post_actions:
            if post_action not in post_actions:
                post_actions.append(copy.copy(post_action))

        referenced_ctes: Dict[WithQueryBlock, int] = dict()
        if (
            self.session.cte_optimization_enabled
            and self.session._query_compilation_stage_enabled
        ):
            # When the cte optimization and the new compilation stage is enabled,
            # the referred cte tables are propagated from left and right can have
            # duplicated queries if there is a common CTE block referenced by
            # both left and right.
            referenced_ctes = merge_referenced_ctes(
                select_left.referenced_ctes, select_right.referenced_ctes
            )

        queries = merged_queries + [
            Query(
                sql_generator(
                    select_left.queries[-1].sql, select_right.queries[-1].sql
                ),
                params=[
                    *select_left.queries[-1].params,
                    *select_right.queries[-1].params,
                ],
            )
        ]

        return SnowflakePlan(
            queries,
            schema_query,
            post_actions,
            new_expr_to_alias,
            source_plan,
            api_calls=api_calls,
            session=self.session,
            referenced_ctes=referenced_ctes,
        )

    def query(
        self,
        sql: str,
        source_plan: Optional[LogicalPlan],
        api_calls: Optional[List[Dict]] = None,
        params: Optional[Sequence[Any]] = None,
        schema_query: Optional[str] = None,
    ) -> SnowflakePlan:
        return SnowflakePlan(
            queries=[Query(sql, params=params)],
            schema_query=schema_query or sql,
            session=self.session,
            source_plan=source_plan,
            api_calls=api_calls,
        )

    def large_local_relation_plan(
        self,
        output: List[Attribute],
        data: List[Row],
        source_plan: Optional[LogicalPlan],
        schema_query: Optional[str],
    ) -> SnowflakePlan:
        thread_safe_session_enabled = self.session._conn._thread_safe_session_enabled
        temp_table_name = (
            f"temp_name_placeholder_{generate_random_alphanumeric()}"
            if thread_safe_session_enabled
            else random_name_for_temp_object(TempObjectType.TABLE)
        )

        attributes = [
            Attribute(attr.name, attr.datatype, attr.nullable) for attr in output
        ]
        create_table_stmt = create_table_statement(
            temp_table_name,
            attribute_to_schema_string(attributes),
            replace=True,
            table_type="temporary",
            use_scoped_temp_objects=self.session._use_scoped_temp_objects,
            is_generated=True,
        )
        insert_stmt = batch_insert_into_statement(
            temp_table_name,
            [attr.name for attr in attributes],
            self.session._conn._conn._paramstyle,
        )
        select_stmt = project_statement([], temp_table_name)
        drop_table_stmt = drop_table_if_exists_statement(temp_table_name)
        if self._skip_schema_query:
            schema_query = None
        else:
            schema_query = schema_query or schema_value_statement(attributes)
        queries = [
            Query(
                create_table_stmt,
                is_ddl_on_temp_object=True,
                temp_obj_name_placeholder=(
                    (temp_table_name, TempObjectType.TABLE)
                    if thread_safe_session_enabled
                    else None
                ),
            ),
            BatchInsertQuery(insert_stmt, data),
            Query(select_stmt),
        ]
        new_plan = SnowflakePlan(
            queries=queries,
            schema_query=schema_query,
            post_actions=[
                Query(
                    drop_table_stmt,
                    is_ddl_on_temp_object=True,
                    temp_obj_name_placeholder=(
                        (temp_table_name, TempObjectType.TABLE)
                        if thread_safe_session_enabled
                        else None
                    ),
                )
            ],
            session=self.session,
            source_plan=source_plan,
        )
        for i in range(len(queries)):
            queries[i].query_line_intervals = [
                QueryLineInterval(0, queries[i].sql.count("\n"), new_plan.uuid)
            ]
        return new_plan

    def table(self, table_name: str, source_plan: LogicalPlan) -> SnowflakePlan:
        return self.query(project_statement([], table_name), source_plan)

    def file_operation_plan(
        self, command: str, file_name: str, stage_location: str, options: Dict[str, str]
    ) -> SnowflakePlan:
        return self.query(
            file_operation_statement(command, file_name, stage_location, options),
            None,
        )

    def project(
        self,
        project_list: List[str],
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
        is_distinct: bool = False,
    ) -> SnowflakePlan:

        return self.build(
            lambda x: project_statement(
                project_list,
                x,
                is_distinct=is_distinct,
                child_uuid=(
                    child.uuid
                    if context._enable_trace_sql_errors_to_dataframe
                    else None
                ),
            ),
            child,
            source_plan,
        )

    def aggregate(
        self,
        grouping_exprs: List[str],
        aggregate_exprs: List[str],
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        return self.build(
            lambda x: aggregate_statement(
                grouping_exprs,
                aggregate_exprs,
                x,
                child_uuid=(
                    child.uuid
                    if context._enable_trace_sql_errors_to_dataframe
                    else None
                ),
            ),
            child,
            source_plan,
        )

    def filter(
        self,
        condition: str,
        is_having: bool,
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        return self.build(
            lambda x: filter_statement(
                condition,
                is_having,
                x,
                child_uuid=(
                    child.uuid
                    if context._enable_trace_sql_errors_to_dataframe
                    else None
                ),
            ),
            child,
            source_plan,
        )

    def sample(
        self,
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
        probability_fraction: Optional[float] = None,
        row_count: Optional[int] = None,
    ) -> SnowflakePlan:
        """Builds the sample part of the resultant sql statement"""
        return self.build(
            lambda x: sample_statement(
                x,
                probability_fraction=probability_fraction,
                row_count=row_count,
                child_uuid=(
                    child.uuid
                    if context._enable_trace_sql_errors_to_dataframe
                    else None
                ),
            ),
            child,
            source_plan,
        )

    def sample_by(
        self,
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
        col: str,
        fractions: Dict[Any, float],
    ) -> SnowflakePlan:
        return self.build(
            lambda x: sample_by_statement(x, col=col, fractions=fractions),
            child,
            source_plan,
            schema_query=child.schema_query,
        )

    def sort(
        self,
        order: List[str],
        is_order_by_append: bool,
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        return self.build(
            lambda x: sort_statement(
                order,
                is_order_by_append,
                x,
                child_uuid=(
                    child.uuid
                    if context._enable_trace_sql_errors_to_dataframe
                    else None
                ),
            ),
            child,
            source_plan,
        )

    def set_operator(
        self,
        left: SnowflakePlan,
        right: SnowflakePlan,
        op: str,
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        return self.build_binary(
            lambda x, y: set_operator_statement(x, y, op),
            left,
            right,
            source_plan,
        )

    def join(
        self,
        left: SnowflakePlan,
        right: SnowflakePlan,
        join_type: JoinType,
        join_condition: str,
        match_condition: str,
        source_plan: Optional[LogicalPlan],
        use_constant_subquery_alias: bool,
    ):
        return self.build_binary(
            lambda x, y: join_statement(
                x,
                y,
                join_type,
                join_condition,
                match_condition,
                use_constant_subquery_alias,
                left_uuid=(
                    left.uuid if context._enable_trace_sql_errors_to_dataframe else None
                ),
                right_uuid=(
                    right.uuid
                    if context._enable_trace_sql_errors_to_dataframe
                    else None
                ),
            ),
            left,
            right,
            source_plan,
        )

    def save_as_table(
        self,
        table_name: Iterable[str],
        column_names: Optional[Iterable[str]],
        mode: SaveMode,
        table_type: str,
        clustering_keys: Iterable[str],
        comment: Optional[str],
        enable_schema_evolution: Optional[bool],
        data_retention_time: Optional[int],
        max_data_extension_time: Optional[int],
        change_tracking: Optional[bool],
        copy_grants: bool,
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
        use_scoped_temp_objects: bool,
        creation_source: TableCreationSource,
        child_attributes: Optional[List[Attribute]],
        iceberg_config: Optional[dict] = None,
        table_exists: Optional[bool] = None,
    ) -> SnowflakePlan:
        """Returns a SnowflakePlan to materialize the child plan into a table.

        Args:
            table_name: fully qualified table name
            column_names: names of columns for the table
            mode: APPEND, TRUNCATE, OVERWRITE, IGNORE, ERROR_IF_EXISTS
            table_type: temporary, transient, or permanent
            clustering_keys: list of clustering columns
            comment: comment associated with the table
            enable_schema_evolution: whether to enable schema evolution
            data_retention_time: data retention time in days
            max_data_extension_time: max data extension time in days
            change_tracking: whether to enable change tracking
            copy_grants: whether to copy grants
            child: the SnowflakePlan that is being materialized into a table
            source_plan: the source plan of the child
            use_scoped_temp_objects: should we use scoped temp objects
            creation_source: the creator for the SnowflakeCreateTable node, today it can come from the
                cache result api, compilation transformations like large query breakdown, or other like
                save_as_table call. This parameter is used to identify whether a table is internally
                generated with cache_result or by the compilation transformation, and some special
                check and handling needs to be applied to guarantee the correctness of generated query.
            child_attributes: child attributes will be none in the case of large query breakdown
                where we use ctas query to create the table which does not need to know the column
                metadata.
            iceberg_config: A dictionary that can contain the following iceberg configuration values:
                external_volume: specifies the identifier for the external volume where
                    the Iceberg table stores its metadata files and data in Parquet format
                catalog: specifies either Snowflake or a catalog integration to use for this table
                base_location: the base directory that snowflake can write iceberg metadata and files to
                catalog_sync: optionally sets the catalog integration configured for Polaris Catalog
                storage_serialization_policy: specifies the storage serialization policy for the table
                iceberg_version: Overrides the version of iceberg to use. Defaults to 2 when unset.
            table_exists: whether the table already exists in the database.
                Only used for APPEND and TRUNCATE mode.
        """
        is_generated = creation_source in (
            TableCreationSource.CACHE_RESULT,
            TableCreationSource.LARGE_QUERY_BREAKDOWN,
        )
        if is_generated and mode != SaveMode.ERROR_IF_EXISTS:
            # an internally generated save_as_table comes from two sources:
            #   1. df.cache_result: plan is created with create table + insert statement
            #   2. large query breakdown: plan is created using a CTAS statement
            # For these cases, we must use mode ERROR_IF_EXISTS
            raise ValueError(
                "Internally generated tables must be called with mode ERROR_IF_EXISTS"
            )

        if (
            child_attributes is None
            and creation_source != TableCreationSource.LARGE_QUERY_BREAKDOWN
        ):
            raise ValueError(
                "child attribute must be provided when table creation source is not large query breakdown"
            )

        full_table_name = ".".join(table_name)
        is_temp_table_type = table_type in TEMPORARY_STRING_SET
        # here get the column definition from the child attributes. In certain cases we have
        # the attributes set to ($1, VariantType()) which cannot be used as valid column name
        # in save as table. So we rename ${number} with COL{number}.
        hidden_column_pattern = r"\"\$(\d+)\""
        column_definition = None
        if child_attributes is not None:
            column_definition_with_hidden_columns = attribute_to_schema_string(
                child_attributes or []
            )
            column_definition = re.sub(
                hidden_column_pattern,
                lambda match: f'"COL{match.group(1)}"',
                column_definition_with_hidden_columns,
            )

        def get_create_table_as_select_plan(child: SnowflakePlan, replace, error):
            return self.build(
                lambda x: create_table_as_select_statement(
                    full_table_name,
                    x,
                    column_definition,
                    replace=replace,
                    error=error,
                    table_type=table_type,
                    clustering_key=clustering_keys,
                    comment=comment,
                    enable_schema_evolution=enable_schema_evolution,
                    data_retention_time=data_retention_time,
                    max_data_extension_time=max_data_extension_time,
                    change_tracking=change_tracking,
                    copy_grants=copy_grants,
                    iceberg_config=iceberg_config,
                    use_scoped_temp_objects=use_scoped_temp_objects,
                    is_generated=is_generated,
                ),
                child,
                source_plan,
                is_ddl_on_temp_object=is_temp_table_type,
            )

        def get_create_and_insert_plan(child: SnowflakePlan, replace, error):
            assert (
                column_definition is not None
            ), "column definition is required for create table statement"
            create_table = create_table_statement(
                full_table_name,
                column_definition,
                replace=replace,
                error=error,
                table_type=table_type,
                clustering_key=clustering_keys,
                comment=comment,
                enable_schema_evolution=enable_schema_evolution,
                data_retention_time=data_retention_time,
                max_data_extension_time=max_data_extension_time,
                change_tracking=change_tracking,
                copy_grants=copy_grants,
                use_scoped_temp_objects=use_scoped_temp_objects,
                is_generated=is_generated,
                iceberg_config=iceberg_config,
            )

            # so that dataframes created from non-select statements,
            # such as table sprocs, work
            child = self.add_result_scan_if_not_select(child)
            return SnowflakePlan(
                [
                    *child.queries[0:-1],
                    Query(create_table, is_ddl_on_temp_object=is_temp_table_type),
                    Query(
                        insert_into_statement(
                            table_name=full_table_name,
                            child=child.queries[-1].sql,
                            column_names=column_names,
                        ),
                        params=child.queries[-1].params,
                        is_ddl_on_temp_object=is_temp_table_type,
                    ),
                ],
                create_table,
                child.post_actions,
                {},
                source_plan,
                api_calls=child.api_calls,
                session=self.session,
                referenced_ctes=child.referenced_ctes,
            )

        if mode == SaveMode.APPEND:
            assert table_exists is not None
            if table_exists:
                return self.build(
                    lambda x: insert_into_statement(
                        table_name=full_table_name,
                        child=x,
                        column_names=column_names,
                    ),
                    child,
                    source_plan,
                )
            else:
                return get_create_and_insert_plan(child, replace=False, error=False)

        elif mode == SaveMode.TRUNCATE:
            assert table_exists is not None
            if table_exists:
                return self.build(
                    lambda x: insert_into_statement(
                        full_table_name, x, [x.name for x in child.attributes], True
                    ),
                    child,
                    source_plan,
                )
            else:
                return get_create_table_as_select_plan(child, replace=True, error=True)

        elif mode == SaveMode.OVERWRITE:
            return get_create_table_as_select_plan(child, replace=True, error=True)

        elif mode == SaveMode.IGNORE:
            return get_create_table_as_select_plan(child, replace=False, error=False)

        elif mode == SaveMode.ERROR_IF_EXISTS:
            if creation_source == TableCreationSource.CACHE_RESULT:
                # if the table is created from cache result, we use create and replace
                # table in order to avoid breaking any current transaction.
                return get_create_and_insert_plan(child, replace=False, error=True)

            return get_create_table_as_select_plan(child, replace=False, error=True)

    def limit(
        self,
        limit_expr: str,
        offset_expr: str,
        child: SnowflakePlan,
        on_top_of_oder_by: bool,
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        return self.build(
            lambda x: limit_statement(
                limit_expr,
                offset_expr,
                x,
                on_top_of_oder_by,
                child_uuid=(
                    child.uuid
                    if context._enable_trace_sql_errors_to_dataframe
                    else None
                ),
            ),
            child,
            source_plan,
        )

    def pivot(
        self,
        pivot_column: str,
        pivot_values: Optional[Union[str, List[str]]],
        aggregate: str,
        default_on_null: Optional[str],
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
        should_alias_column_with_agg: bool,
    ) -> SnowflakePlan:
        return self.build(
            lambda x: pivot_statement(
                pivot_column,
                pivot_values,
                aggregate,
                default_on_null,
                x,
                should_alias_column_with_agg,
                child_uuid=(
                    child.uuid
                    if context._enable_trace_sql_errors_to_dataframe
                    else None
                ),
            ),
            child,
            source_plan,
        )

    def unpivot(
        self,
        value_column: str,
        name_column: str,
        column_list: List[str],
        include_nulls: bool,
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        return self.build(
            lambda x: unpivot_statement(
                value_column,
                name_column,
                column_list,
                include_nulls,
                x,
                child_uuid=(
                    child.uuid
                    if context._enable_trace_sql_errors_to_dataframe
                    else None
                ),
            ),
            child,
            source_plan,
        )

    def rename(
        self,
        column_map: Dict[str, str],
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        return self.build(
            lambda x: rename_statement(column_map, x),
            child,
            source_plan,
        )

    def create_or_replace_view(
        self,
        name: str,
        child: SnowflakePlan,
        is_temp: bool,
        comment: Optional[str],
        replace: bool,
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        if len(child.queries) != 1:
            # If creating a temp view, we can't drop any temp object in the post_actions
            # It's okay to leave these temp objects in the current session for now.
            if is_temp:
                temp_object_names = {
                    query.temp_obj_name_placeholder[0]
                    for query in child.queries
                    if query.temp_obj_name_placeholder
                }
                child.post_actions = [
                    post_action
                    for post_action in child.post_actions
                    if post_action.temp_obj_name_placeholder
                    and post_action.temp_obj_name_placeholder[0]
                    not in temp_object_names
                ]
            else:
                raise SnowparkClientExceptionMessages.PLAN_CREATE_VIEW_FROM_DDL_DML_OPERATIONS()

        if not is_sql_select_statement(child.queries[-1].sql.lower().strip()):
            raise SnowparkClientExceptionMessages.PLAN_CREATE_VIEWS_FROM_SELECT_ONLY()

        return self.build(
            lambda x: create_or_replace_view_statement(
                name, x, is_temp, comment, replace
            ),
            child,
            source_plan,
        )

    def find_and_update_table_function_plan(self, plan: SnowflakePlan) -> SnowflakePlan:
        """This function is meant to find any udtf function call from a create dynamic table plan and
        replace '*' with explicit column identifier in the select of table function. Since we cannot
        differentiate udtf call from other table functions, we apply this change to all table functions.
        """
        from snowflake.snowpark._internal.analyzer.select_statement import (
            SelectTableFunction,
            Selectable,
        )
        from snowflake.snowpark._internal.compiler.utils import (
            create_query_generator,
            update_resolvable_node,
            replace_child_and_update_ancestors,
        )

        visited = set()
        node_parents_map = defaultdict(set)
        deepcopied_plan = copy.deepcopy(plan)
        query_generator = create_query_generator(plan)
        queue = deque()

        queue.append(deepcopied_plan)
        visited.add(deepcopied_plan)

        while queue:
            node = queue.popleft()
            visited.add(node)
            for child_node in reversed(node.children_plan_nodes):
                node_parents_map[child_node].add(node)
                if child_node not in visited:
                    queue.append(child_node)

            # the bug only happen when create dynamic table on top of a table function
            # this is meant to decide whether the plan is select from a table function
            if isinstance(node, SelectTableFunction) and isinstance(
                node.snowflake_plan.source_plan, TableFunctionJoin
            ):
                table_function_join_node = node.snowflake_plan.source_plan
                # if the plan has only 1 child and the source_plan.right_cols == '*', then we need to update the
                # plan with the output column identifiers.
                if len(
                    node.snowflake_plan.children_plan_nodes
                ) == 1 and table_function_join_node.right_cols == ["*"]:
                    child_plan: Union[
                        SnowflakePlan, Selectable
                    ] = node.snowflake_plan.children_plan_nodes[0]
                    if isinstance(child_plan, Selectable):
                        child_plan = child_plan.snowflake_plan

                    new_plan = copy.deepcopy(node)

                    # type assertions
                    assert isinstance(child_plan, SnowflakePlan)
                    assert isinstance(
                        new_plan.snowflake_plan.source_plan, TableFunctionJoin
                    )

                    new_plan.snowflake_plan.source_plan.right_cols = (
                        node.snowflake_plan.quoted_identifiers[
                            len(child_plan.quoted_identifiers) :
                        ]
                    )

                    update_resolvable_node(new_plan, query_generator)
                    replace_child_and_update_ancestors(
                        node, new_plan, node_parents_map, query_generator
                    )

        return deepcopied_plan

    def create_or_replace_dynamic_table(
        self,
        name: str,
        warehouse: str,
        lag: str,
        comment: Optional[str],
        create_mode: DynamicTableCreateMode,
        refresh_mode: Optional[str],
        initialize: Optional[str],
        clustering_keys: Iterable[str],
        is_transient: bool,
        data_retention_time: Optional[int],
        max_data_extension_time: Optional[int],
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
        iceberg_config: Optional[dict] = None,
    ) -> SnowflakePlan:

        child = self.find_and_update_table_function_plan(child)

        if len(child.queries) != 1:
            raise SnowparkClientExceptionMessages.PLAN_CREATE_DYNAMIC_TABLE_FROM_DDL_DML_OPERATIONS()

        if not is_sql_select_statement(child.queries[0].sql.lower().strip()):
            raise SnowparkClientExceptionMessages.PLAN_CREATE_DYNAMIC_TABLE_FROM_SELECT_ONLY()

        if create_mode == DynamicTableCreateMode.OVERWRITE:
            replace = True
            if_not_exists = False
        elif create_mode == DynamicTableCreateMode.ERROR_IF_EXISTS:
            replace = False
            if_not_exists = False
        elif create_mode == DynamicTableCreateMode.IGNORE:
            replace = False
            if_not_exists = True
        else:
            # should never reach here
            raise ValueError(f"Unknown create mode: {create_mode}")  # pragma: no cover

        return self.build(
            lambda x: create_or_replace_dynamic_table_statement(
                name=name,
                warehouse=warehouse,
                lag=lag,
                comment=comment,
                replace=replace,
                if_not_exists=if_not_exists,
                refresh_mode=refresh_mode,
                initialize=initialize,
                clustering_keys=clustering_keys,
                is_transient=is_transient,
                data_retention_time=data_retention_time,
                max_data_extension_time=max_data_extension_time,
                child=x,
                iceberg_config=iceberg_config,
            ),
            child,
            source_plan,
        )

    def _merge_file_format_options(
        self, file_format_options: Dict[str, Any], options: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Merges remotely defined file_format options with the local options set. This allows the client
        to override any locally defined options that are incompatible with the file format.
        """
        if "FORMAT_NAME" not in options:
            return file_format_options

        def process_list(list_property):
            split_list = list_property.lstrip("[").rstrip("]").split(", ")
            if len(split_list) == 1:
                return split_list[0]
            return tuple(split_list)

        type_map = {
            "String": str,
            "List": process_list,
            "Integer": int,
            "Boolean": lambda x: str(x).lower() == "true",
        }
        new_options = {**file_format_options}
        # SNOW-1628625: This query and subsequent merge operations should be done lazily
        file_format = self.session.sql(
            f"DESCRIBE FILE FORMAT {options['FORMAT_NAME']}"
        ).collect()

        for setting in file_format:
            if (
                setting["property_value"] == setting["property_default"]
                or setting["property"] in new_options
            ):
                continue

            new_options[str(setting["property"])] = type_map.get(
                str(setting["property_type"]), str
            )(setting["property_value"])
        return new_options

    def _create_xml_query(
        self,
        xml_reader_udtf: "UserDefinedTableFunction",
        file_path: str,
        options: Dict[str, str],
    ) -> str:
        """
        Creates a DataFrame from a UserDefinedTableFunction that reads XML files.
        """
        from snowflake.snowpark.functions import lit, col, seq8, flatten
        from snowflake.snowpark._internal.xml_reader import DEFAULT_CHUNK_SIZE

        worker_column_name = "WORKER"
        xml_row_number_column_name = "XML_ROW_NUMBER"
        row_tag = options[XML_ROW_TAG_STRING]
        mode = options.get("MODE", "PERMISSIVE").upper()
        column_name_of_corrupt_record = options.get(
            "COLUMNNAMEOFCORRUPTRECORD", "_corrupt_record"
        )
        ignore_namespace = options.get("IGNORENAMESPACE", True)
        attribute_prefix = options.get("ATTRIBUTEPREFIX", "_")
        exclude_attributes = options.get("EXCLUDEATTRIBUTES", False)
        value_tag = options.get("VALUETAG", "_VALUE")
        # NULLVALUE will be mapped to NULL_IF in pre-defined mapping in `dataframe_writer.py`
        null_value = options.get("NULL_IF", "")
        charset = options.get("CHARSET", "utf-8")
        ignore_surrounding_whitespace = options.get(
            "IGNORESURROUNDINGWHITESPACE", False
        )
        row_validation_xsd_path = options.get("ROWVALIDATIONXSDPATH", "")

        if mode not in {"PERMISSIVE", "DROPMALFORMED", "FAILFAST"}:
            raise ValueError(
                f"Invalid mode: {mode}. Must be one of PERMISSIVE, DROPMALFORMED, FAILFAST."
            )

        # TODO SNOW-1983360: make it an configurable option once the UDTF scalability issue is resolved.
        # Currently it's capped at 16.
        try:
            file_size = int(self.session.sql(f"ls {file_path}", _emit_ast=False).collect(_emit_ast=False)[0]["size"])  # type: ignore
        except IndexError:
            raise ValueError(f"{file_path} does not exist")
        num_workers = min(16, file_size // DEFAULT_CHUNK_SIZE + 1)

        # Create a range from 0 to N-1
        df = self.session.range(num_workers).to_df(worker_column_name)

        # Apply UDTF to the XML file and get each XML record as a Variant data,
        # and append a unique row number to each record.
        df = df.select(
            worker_column_name,
            seq8().as_(xml_row_number_column_name),
            xml_reader_udtf(
                lit(file_path),
                lit(num_workers),
                lit(row_tag),
                col(worker_column_name),
                lit(mode),
                lit(column_name_of_corrupt_record),
                lit(ignore_namespace),
                lit(attribute_prefix),
                lit(exclude_attributes),
                lit(value_tag),
                lit(null_value),
                lit(charset),
                lit(ignore_surrounding_whitespace),
                lit(row_validation_xsd_path),
            ),
        )

        # Flatten the Variant data to get the key-value pairs
        df = df.select(
            worker_column_name,
            xml_row_number_column_name,
            flatten(XML_ROW_DATA_COLUMN_NAME),
        ).select(worker_column_name, xml_row_number_column_name, "key", "value")

        # Apply dynamic pivot to get the flat table with dynamic schema
        df = df.pivot("key").max("value")

        # Exclude the worker and row number columns
        return f"SELECT * EXCLUDE ({worker_column_name}, {xml_row_number_column_name}) FROM ({df.queries['queries'][-1]})"

    def read_file(
        self,
        path: str,
        format: str,
        options: Dict[str, str],
        schema: List[Attribute],
        schema_to_cast: Optional[List[Tuple[str, str]]] = None,
        transformations: Optional[List[str]] = None,
        metadata_project: Optional[List[str]] = None,
        metadata_schema: Optional[List[Attribute]] = None,
        use_user_schema: bool = False,
        xml_reader_udtf: Optional["UserDefinedTableFunction"] = None,
        source_plan: Optional[ReadFileNode] = None,
    ) -> SnowflakePlan:
        thread_safe_session_enabled = self.session._conn._thread_safe_session_enabled

        if xml_reader_udtf is not None:
            xml_query = self._create_xml_query(xml_reader_udtf, path, options)
            return SnowflakePlan(
                [Query(xml_query)],
                # the schema query of dynamic pivot must be the same as the original query
                xml_query,
                None,
                {},
                source_plan=source_plan,
                session=self.session,
            )

        # Setting ENFORCE_EXISTING_FILE_FORMAT to True forces Snowpark to use the existing file format object,
        # disregarding any custom format options and preventing temporary file format creation.
        use_temp_file_format = not options.get("ENFORCE_EXISTING_FILE_FORMAT", False)

        if not use_temp_file_format and "FORMAT_NAME" not in options:
            raise ValueError(
                "Setting the ENFORCE_EXISTING_FILE_FORMAT option requires providing FORMAT_NAME."
            )

        format_type_options, copy_options = get_copy_into_table_options(options)

        if not use_temp_file_format and len(format_type_options) > 0:
            raise ValueError(
                "Option 'ENFORCE_EXISTING_FILE_FORMAT' can not be used with any format type options."
            )

        if use_temp_file_format:
            format_type_options = self._merge_file_format_options(
                format_type_options, options
            )
        pattern = options.get("PATTERN")
        # Can only infer the schema for parquet, orc and avro
        # csv and json in preview
        schema_available = (
            options.get("INFER_SCHEMA", True)
            if format in INFER_SCHEMA_FORMAT_TYPES
            else False
        ) or use_user_schema
        # tracking usage of pattern, will refactor this function in future
        if pattern:
            self.session._conn._telemetry_client.send_copy_pattern_telemetry()

        if format_type_options.get("PARSE_HEADER", False):
            # This option is only available for CSV file format
            # The options is used when specified with INFER_SCHEMA( ..., FILE_FORMAT => (.., PARSE_HEADER)) see
            # https://docs.snowflake.com/en/sql-reference/sql/create-file-format#format-type-options-formattypeoptions
            # PARSE_HEADER does not work with FILE_FORMAT when used with SELECT FROM LOCATION(FILE_FORMAT ...). Thus,
            # if user has set option("PARSE_HEADER", True), we have already read the header in
            # DataframeReader._infer_schema_for_file_format so now we must set skip_header = 1 to skip the header line.
            format_type_options["SKIP_HEADER"] = 1
        format_type_options.pop("PARSE_HEADER", None)

        if not copy_options:  # use select
            queries: List[Query] = []
            post_queries: List[Query] = []
            format_name = (
                (
                    self.session.get_fully_qualified_name_if_possible(
                        f"temp_name_placeholder_{generate_random_alphanumeric()}"
                    )
                    if thread_safe_session_enabled
                    else random_name_for_temp_object(TempObjectType.FILE_FORMAT)
                )
                if use_temp_file_format
                else options["FORMAT_NAME"]
            )

            if use_temp_file_format:
                queries.append(
                    Query(
                        create_file_format_statement(
                            format_name,
                            format,
                            format_type_options,
                            temp=True,
                            if_not_exist=True,
                            use_scoped_temp_objects=self.session._use_scoped_temp_objects,
                            is_generated=True,
                        ),
                        is_ddl_on_temp_object=True,
                        temp_obj_name_placeholder=(
                            (
                                format_name,
                                TempObjectType.FILE_FORMAT,
                            )
                            if thread_safe_session_enabled
                            else None
                        ),
                    )
                )

                post_queries.append(
                    Query(
                        drop_file_format_if_exists_statement(format_name),
                        is_ddl_on_temp_object=True,
                        temp_obj_name_placeholder=(
                            (
                                format_name,
                                TempObjectType.FILE_FORMAT,
                            )
                            if thread_safe_session_enabled
                            else None
                        ),
                    )
                )

            if schema_available:
                assert schema_to_cast is not None
                schema_project: List[str] = schema_cast_named(schema_to_cast)
            else:
                schema_project: List[str] = schema_cast_seq(schema)

            queries.append(
                Query(
                    select_from_path_with_format_statement(
                        (metadata_project or []) + schema_project,
                        path,
                        format_name,
                        pattern,
                    )
                )
            )

            source_plan = (
                SelectFromFileNode.from_read_file_node(source_plan)
                if source_plan
                else None
            )
            return SnowflakePlan(
                queries,
                schema_value_statement((metadata_schema or []) + schema),
                post_queries,
                {},
                source_plan=source_plan,
                session=self.session,
            )
        else:  # otherwise use COPY
            if "FORCE" in copy_options and str(copy_options["FORCE"]).lower() != "true":
                raise SnowparkClientExceptionMessages.PLAN_COPY_DONT_SUPPORT_SKIP_LOADED_FILES(
                    copy_options["FORCE"]
                )

            # set force to true.
            # it is useless since we always create new temp table.
            # setting it helps users to understand generated queries.

            copy_options_with_force = {**copy_options, "FORCE": True}

            # If we have inferred the schema, we want to use those column names
            temp_table_schema = (
                schema
                if schema_available
                else [
                    Attribute(f'"COL{index}"', att.datatype, att.nullable)
                    for index, att in enumerate(schema)
                ]
            )

            temp_table_name = (
                self.session.get_fully_qualified_name_if_possible(
                    f"temp_name_placeholder_{generate_random_alphanumeric()}"
                )
                if thread_safe_session_enabled
                else random_name_for_temp_object(TempObjectType.TABLE)
            )
            queries = [
                Query(
                    create_table_statement(
                        temp_table_name,
                        attribute_to_schema_string(temp_table_schema),
                        replace=True,
                        table_type="temporary",
                        use_scoped_temp_objects=self.session._use_scoped_temp_objects,
                        is_generated=True,
                    ),
                    is_ddl_on_temp_object=True,
                    temp_obj_name_placeholder=(
                        (temp_table_name, TempObjectType.TABLE)
                        if thread_safe_session_enabled
                        else None
                    ),
                ),
                Query(
                    copy_into_table(
                        temp_table_name,
                        path,
                        format,
                        format_type_options,
                        copy_options_with_force,
                        pattern,
                        transformations=transformations,
                    )
                ),
                Query(
                    project_statement(
                        [
                            f"{new_att.name} AS {input_att.name}"
                            for new_att, input_att in zip(temp_table_schema, schema)
                        ],
                        temp_table_name,
                    )
                ),
            ]

            post_actions = [
                Query(
                    drop_table_if_exists_statement(temp_table_name),
                    is_ddl_on_temp_object=True,
                    temp_obj_name_placeholder=(
                        (temp_table_name, TempObjectType.TABLE)
                        if thread_safe_session_enabled
                        else None
                    ),
                )
            ]
            source_plan = (
                SelectWithCopyIntoTableNode.from_read_file_node(source_plan)
                if source_plan
                else None
            )
            return SnowflakePlan(
                queries,
                schema_value_statement(schema),
                post_actions,
                {},
                source_plan=source_plan,
                session=self.session,
            )

    def copy_into_table(
        self,
        file_format: str,
        table_name: Iterable[str],
        path: str,
        source_plan: Optional[LogicalPlan],
        files: Optional[str] = None,
        pattern: Optional[str] = None,
        validation_mode: Optional[str] = None,
        column_names: Optional[List[str]] = None,
        transformations: Optional[List[str]] = None,
        user_schema: Optional[StructType] = None,
        create_table_from_infer_schema: bool = False,
        *,
        copy_options: Dict[str, Any],
        format_type_options: Dict[str, Any],
        iceberg_config: Optional[dict] = None,
    ) -> SnowflakePlan:
        # tracking usage of pattern, will refactor this function in future
        if pattern:
            self.session._conn._telemetry_client.send_copy_pattern_telemetry()

        full_table_name = ".".join(table_name)
        copy_command = copy_into_table(
            table_name=full_table_name,
            file_path=path,
            files=files,
            file_format_type=file_format,
            format_type_options=format_type_options,
            copy_options=copy_options,
            pattern=pattern,
            validation_mode=validation_mode,
            column_names=column_names,
            transformations=transformations,
        )
        if self.session._table_exists(table_name):
            queries = [Query(copy_command)]
        elif user_schema and (
            (file_format.upper() == "CSV" and not transformations)
            or (
                create_table_from_infer_schema
                and file_format.upper() in INFER_SCHEMA_FORMAT_TYPES
            )
        ):
            attributes = user_schema._to_attributes()
            queries = [
                Query(
                    create_table_statement(
                        full_table_name,
                        attribute_to_schema_string(attributes),
                        iceberg_config=iceberg_config,
                    ),
                    # This is an exception. The principle is to avoid surprising behavior and most of the time
                    # it applies to temp object. But this perm table creation is also one place where we create
                    # table on behalf of the user automatically.
                    is_ddl_on_temp_object=True,
                ),
                Query(copy_command),
            ]
        else:
            raise SnowparkClientExceptionMessages.DF_COPY_INTO_CANNOT_CREATE_TABLE(
                full_table_name
            )
        return SnowflakePlan(
            queries, copy_command, [], {}, source_plan, session=self.session
        )

    def copy_into_location(
        self,
        query: SnowflakePlan,
        stage_location: str,
        source_plan: Optional[LogicalPlan],
        partition_by: Optional[str] = None,
        file_format_name: Optional[str] = None,
        file_format_type: Optional[str] = None,
        format_type_options: Optional[Dict[str, Any]] = None,
        header: bool = False,
        **copy_options: Optional[Any],
    ) -> SnowflakePlan:
        return self.build(
            lambda x: copy_into_location(
                query=x,
                stage_location=stage_location,
                partition_by=partition_by,
                file_format_name=file_format_name,
                file_format_type=file_format_type,
                format_type_options=format_type_options,
                header=header,
                **copy_options,
            ),
            query,
            source_plan,
            query.schema_query,
        )

    def update(
        self,
        table_name: str,
        assignments: Dict[str, str],
        condition: Optional[str],
        source_data: Optional[SnowflakePlan],
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        if source_data:
            return self.build(
                lambda x: update_statement(
                    table_name,
                    assignments,
                    condition,
                    x,
                ),
                source_data,
                source_plan,
            )
        else:
            return self.query(
                update_statement(
                    table_name,
                    assignments,
                    condition,
                    None,
                ),
                source_plan,
            )

    def delete(
        self,
        table_name: str,
        condition: Optional[str],
        source_data: Optional[SnowflakePlan],
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        if source_data:
            return self.build(
                lambda x: delete_statement(
                    table_name,
                    condition,
                    x,
                ),
                source_data,
                source_plan,
            )
        else:
            return self.query(
                delete_statement(
                    table_name,
                    condition,
                    None,
                ),
                source_plan,
            )

    def merge(
        self,
        table_name: str,
        source_data: SnowflakePlan,
        join_expr: str,
        clauses: List[str],
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        return self.build(
            lambda x: merge_statement(table_name, x, join_expr, clauses),
            source_data,
            source_plan,
        )

    def lateral(
        self,
        table_function: str,
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
    ) -> SnowflakePlan:
        return self.build(
            lambda x: lateral_statement(
                table_function,
                x,
                child_uuid=(
                    child.uuid
                    if context._enable_trace_sql_errors_to_dataframe
                    else None
                ),
            ),
            child,
            source_plan,
        )

    def from_table_function(
        self, func: str, source_plan: TableFunctionRelation
    ) -> SnowflakePlan:
        if isinstance(source_plan.table_function, GeneratorTableFunction):
            return self.query(
                table_function_statement(func, source_plan.table_function.operators),
                source_plan,
            )
        return self.query(table_function_statement(func), None)

    def join_table_function(
        self,
        func: str,
        child: SnowflakePlan,
        source_plan: Optional[LogicalPlan],
        left_cols: List[str],
        right_cols: List[str],
        use_constant_subquery_alias: bool,
    ) -> SnowflakePlan:
        return self.build(
            lambda x: join_table_function_statement(
                func,
                x,
                left_cols,
                right_cols,
                use_constant_subquery_alias,
                child_uuid=(
                    child.uuid
                    if context._enable_trace_sql_errors_to_dataframe
                    else None
                ),
            ),
            child,
            source_plan,
        )

    def select_statement(self, selectable: "Selectable") -> SnowflakePlan:
        return selectable.snowflake_plan

    def add_result_scan_if_not_select(self, plan: SnowflakePlan) -> SnowflakePlan:
        if isinstance(plan.source_plan, SetOperation):
            return plan
        elif is_sql_select_statement(plan.queries[-1].sql):
            return plan
        else:
            new_queries = plan.queries + [
                Query(
                    result_scan_statement(plan.queries[-1].query_id_place_holder),
                )
            ]
            return SnowflakePlan(
                new_queries,
                schema_value_statement(plan.attributes),
                plan.post_actions,
                plan.expr_to_alias,
                plan.source_plan,
                api_calls=plan.api_calls,
                session=self.session,
                referenced_ctes=plan.referenced_ctes,
            )

    def with_query_block(
        self,
        with_query_block: WithQueryBlock,
        child: SnowflakePlan,
        source_plan: LogicalPlan,
    ) -> SnowflakePlan:
        if not self._skip_schema_query:
            raise ValueError(
                "schema query for WithQueryBlock is currently not supported"
            )
        name = with_query_block.name
        new_query = project_statement([], name)

        # note we do not propagate the query parameter of the child here,
        # the query parameter will be propagate along with the definition during
        # query generation stage.
        queries = child.queries[:-1] + [Query(sql=new_query)]
        # propagate the WithQueryBlock references
        referenced_ctes = merge_referenced_ctes(
            child.referenced_ctes, {with_query_block: 1}
        )

        return SnowflakePlan(
            queries,
            schema_query=None,
            post_actions=child.post_actions,
            expr_to_alias=child.expr_to_alias,
            source_plan=source_plan,
            api_calls=child.api_calls,
            session=self.session,
            referenced_ctes=referenced_ctes,
        )


class PlanQueryType(Enum):
    # the queries to execute for the plan
    QUERIES = "queries"
    # the post action queries needs to be executed after the other queries associated
    # to the plan are finished
    POST_ACTIONS = "post_actions"


@dataclass
class QueryLineInterval:
    start: int
    end: int
    uuid: str


class Query:
    def __init__(
        self,
        sql: str,
        *,
        query_id_place_holder: Optional[str] = None,
        is_ddl_on_temp_object: bool = False,
        temp_obj_name_placeholder: Optional[Tuple[str, TempObjectType]] = None,
        params: Optional[Sequence[Any]] = None,
        query_line_intervals: Optional[List[QueryLineInterval]] = None,
    ) -> None:
        self.sql = sql
        self.query_id_place_holder = (
            query_id_place_holder
            if query_id_place_holder
            else f"query_id_place_holder_{generate_random_alphanumeric()}"
        )
        # This is to handle the case when a snowflake plan is created in the following way
        # in a multi-threaded environment:
        #   1. Create a temp object
        #   2. Use the temp object in a query
        #   3. Drop the temp object
        # When step 3 in thread A is executed before step 2 in thread B, the query in thread B will fail with
        # temp object not found. To handle this, we replace temp object names with placeholders in the query
        # and track the temp object placeholder name and temp object type here. During query execution, we replace
        # the placeholders with the actual temp object names for the given execution.
        self.temp_obj_name_placeholder = temp_obj_name_placeholder
        self.is_ddl_on_temp_object = is_ddl_on_temp_object
        self.params = params or []
        self.query_line_intervals = query_line_intervals or []

    def __repr__(self) -> str:
        return (
            "Query("
            + f"{self.sql!r}, "
            + f"query_id_place_holder={self.query_id_place_holder!r}, "
            + f"is_ddl_on_temp_object={self.is_ddl_on_temp_object}, "
            + f"params={self.params}"
            + ")"
        )

    def __eq__(self, other: "Query") -> bool:
        return (
            self.sql == other.sql
            and self.query_id_place_holder == other.query_id_place_holder
            and self.is_ddl_on_temp_object == other.is_ddl_on_temp_object
            and self.temp_obj_name_placeholder == other.temp_obj_name_placeholder
            and self.params == other.params
        )


class BatchInsertQuery(Query):
    def __init__(
        self,
        sql: str,
        rows: Optional[List[Row]] = None,
    ) -> None:
        super().__init__(sql)
        self.rows = rows
