#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import copy
import datetime
import itertools
import random
import re
import sys
from collections import Counter
from functools import cached_property
from logging import getLogger
from types import ModuleType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Union,
    overload,
)
from zoneinfo import ZoneInfo

import snowflake.snowpark
import snowflake.snowpark.context as context
import snowflake.snowpark._internal.proto.generated.ast_pb2 as proto
from snowflake.connector.options import installed_pandas, pandas, pyarrow

from snowflake.snowpark._internal.analyzer.binary_plan_node import (
    AsOf,
    Cross,
    Except,
    FullOuter,
    Inner,
    Intersect,
    Join,
    JoinType,
    LeftAnti,
    LeftOuter,
    LeftSemi,
    NaturalJoin,
    RightOuter,
    Union as UnionPlan,
    UsingJoin,
    create_join_type,
)
from snowflake.snowpark._internal.analyzer.analyzer_utils import unquote_if_quoted
from snowflake.snowpark._internal.analyzer.expression import (
    Attribute,
    Expression,
    Literal,
    NamedExpression,
    Star,
    UnresolvedAttribute,
)
from snowflake.snowpark._internal.analyzer.select_statement import (
    SET_EXCEPT,
    SET_INTERSECT,
    SET_UNION,
    SET_UNION_ALL,
    SelectSnowflakePlan,
    SelectStatement,
    SelectTableFunction,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import PlanQueryType
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    CopyIntoTableNode,
    DynamicTableCreateMode,
    Limit,
    LogicalPlan,
    SaveMode,
    SnowflakeCreateTable,
    TableCreationSource,
    ReadFileNode,
)
from snowflake.snowpark._internal.analyzer.sort_expression import (
    Ascending,
    Descending,
    SortOrder,
)
from snowflake.snowpark._internal.analyzer.table_function import (
    FlattenFunction,
    Lateral,
    TableFunctionExpression,
    TableFunctionJoin,
)
from snowflake.snowpark._internal.analyzer.unary_plan_node import (
    CreateDynamicTableCommand,
    CreateViewCommand,
    Distinct,
    Filter,
    LocalTempView,
    PersistedView,
    Project,
    Rename,
    Sample,
    Sort,
    Unpivot,
    ViewType,
)
from snowflake.snowpark._internal.ast.utils import (
    add_intermediate_stmt,
    build_expr_from_dict_str_str,
    build_expr_from_python_val,
    build_expr_from_snowpark_column,
    build_expr_from_snowpark_column_or_col_name,
    build_expr_from_snowpark_column_or_sql_str,
    build_expr_from_snowpark_column_or_table_fn,
    build_indirect_table_fn_apply,
    debug_check_missing_ast,
    fill_ast_for_column,
    fill_save_mode,
    with_src_position,
    DATAFRAME_AST_PARAMETER,
    build_view_name,
    build_table_name,
    build_name,
)
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.open_telemetry import open_telemetry_context_manager
from snowflake.snowpark._internal.telemetry import (
    ResourceUsageCollector,
    add_api_call,
    adjust_api_subcalls,
    df_api_usage,
    df_collect_api_telemetry,
    df_to_relational_group_df_api_usage,
)
from snowflake.snowpark._internal.type_utils import (
    ColumnOrName,
    ColumnOrSqlExpr,
    LiteralType,
    format_day_time_interval_for_display,
    format_year_month_interval_for_display,
    snow_type_to_dtype_str,
    type_string_to_type_object,
)
from snowflake.snowpark._internal.udf_utils import add_package_to_existing_packages
from snowflake.snowpark._internal.utils import (
    SKIP_LEVELS_THREE,
    SKIP_LEVELS_TWO,
    TempObjectType,
    check_agg_exprs,
    check_flatten_mode,
    column_to_bool,
    create_or_update_statement_params_with_query_tag,
    deprecated,
    escape_quotes,
    experimental,
    generate_random_alphanumeric,
    get_copy_into_table_options,
    is_snowflake_quoted_id_case_insensitive,
    is_snowflake_unquoted_suffix_case_insensitive,
    is_sql_select_statement,
    parse_positional_args_to_list,
    parse_positional_args_to_list_variadic,
    parse_table_name,
    prepare_pivot_arguments,
    publicapi,
    quote_name,
    random_name_for_temp_object,
    str_to_enum,
    validate_object_name,
    global_counter,
    string_half_width,
    warning,
)
from snowflake.snowpark._internal.data_source.utils import (
    track_data_source_statement_params,
)
from snowflake.snowpark.async_job import AsyncJob, _AsyncResultType
from snowflake.snowpark.column import Column, _to_col_if_sql_expr, _to_col_if_str
from snowflake.snowpark.dataframe_ai_functions import DataFrameAIFunctions
from snowflake.snowpark.dataframe_analytics_functions import DataFrameAnalyticsFunctions
from snowflake.snowpark.dataframe_na_functions import DataFrameNaFunctions
from snowflake.snowpark.dataframe_stat_functions import DataFrameStatFunctions
from snowflake.snowpark.dataframe_writer import DataFrameWriter
from snowflake.snowpark.exceptions import SnowparkDataframeException
from snowflake.snowpark._internal.debug_utils import QueryProfiler
from snowflake.snowpark.functions import (
    abs as abs_,
    col,
    count,
    hash as hash_,
    lit,
    max as max_,
    mean,
    min as min_,
    random as random_,
    row_number,
    sql_expr,
    stddev,
    to_char,
)
from snowflake.snowpark.mock._select_statement import MockSelectStatement
from snowflake.snowpark.row import Row
from snowflake.snowpark.table_function import (
    TableFunctionCall,
    _create_table_function_expression,
    _ExplodeFunctionCall,
    _get_cols_after_explode_join,
    _get_cols_after_join_table,
)
from snowflake.snowpark.types import (
    ArrayType,
    DataType,
    DayTimeIntervalType,
    MapType,
    PandasDataFrameType,
    StringType,
    StructField,
    StructType,
    _NumericType,
    _FractionalType,
    TimestampType,
    TimestampTimeZone,
    YearMonthIntervalType,
)

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

if TYPE_CHECKING:
    import modin.pandas  # pragma: no cover
    from table import Table  # pragma: no cover

_logger = getLogger(__name__)

_ONE_MILLION = 1000000
_NUM_PREFIX_DIGITS = 4
_UNALIASED_REGEX = re.compile(f"""._[a-zA-Z0-9]{{{_NUM_PREFIX_DIGITS}}}_(.*)""")


def _generate_prefix(prefix: str) -> str:
    return f"{prefix}_{generate_random_alphanumeric(_NUM_PREFIX_DIGITS)}_"


def _generate_deterministic_prefix(prefix: str, exclude_prefixes: List[str]):
    """
    Generate deterministic prefix while ensuring it doesn't exist in the exclude list.
    """
    counter = global_counter.next()
    candidate_prefix = f"{prefix}_{counter:04}_"
    while any([p.startswith(candidate_prefix) for p in exclude_prefixes]):
        candidate_prefix = f"{prefix}_{counter:04}_"
        counter = global_counter.next()
    return candidate_prefix


def _get_unaliased(col_name: str) -> List[str]:
    unaliased = []
    c = col_name
    while match := _UNALIASED_REGEX.match(c):
        c = match.group(1)
        unaliased.append(c)

    return unaliased


def _alias_if_needed(
    df: "DataFrame",
    c: str,
    prefix: Optional[str],
    suffix: Optional[str],
    common_col_names: List[str],
):
    col = df.col(c, _emit_ast=False)
    unquoted_col_name = c.strip('"')
    if c in common_col_names:
        if suffix:
            column_case_insensitive = is_snowflake_quoted_id_case_insensitive(c)
            suffix_unqouted_case_insensitive = (
                is_snowflake_unquoted_suffix_case_insensitive(suffix)
            )
            return col.alias(
                f'"{unquoted_col_name}{suffix.upper()}"'
                if column_case_insensitive and suffix_unqouted_case_insensitive
                else f'''"{unquoted_col_name}{escape_quotes(suffix.strip('"'))}"'''
            )
        return col.alias(f'"{prefix}{unquoted_col_name}"')
    else:
        return col.alias(f'"{unquoted_col_name}"')


def _disambiguate(
    lhs: "DataFrame",
    rhs: "DataFrame",
    join_type: JoinType,
    using_columns: Iterable[str],
    *,
    lsuffix: str = "",
    rsuffix: str = "",
) -> Tuple["DataFrame", "DataFrame"]:
    if lsuffix == rsuffix and lsuffix:
        raise ValueError(
            f"'lsuffix' and 'rsuffix' must be different if they're not empty. You set {lsuffix!r} to both."
        )
    # Normalize the using columns.
    normalized_using_columns = {quote_name(c) for c in using_columns}
    #  Check if the LHS and RHS have columns in common. If they don't just return them as-is. If
    #  they do have columns in common, alias the common columns with randomly generated l_
    #  and r_ prefixes for the left and right sides respectively.
    #  We assume the column names from the schema are normalized and quoted.
    lhs_names = [attr.name for attr in lhs._output]
    rhs_names = [attr.name for attr in rhs._output]
    common_col_names = [
        n
        for n in lhs_names
        if n in set(rhs_names) and n not in normalized_using_columns
    ]
    all_names = [unquote_if_quoted(n) for n in lhs_names + rhs_names]

    if common_col_names:
        # We use the session of the LHS DataFrame to report this telemetry
        lhs._session._conn._telemetry_client.send_alias_in_join_telemetry()

    lsuffix = lsuffix or lhs._alias
    rsuffix = rsuffix or rhs._alias
    suffix_provided = lsuffix or rsuffix
    lhs_prefix = (
        _generate_deterministic_prefix("l", all_names) if not suffix_provided else ""
    )
    rhs_prefix = (
        _generate_deterministic_prefix("r", all_names) if not suffix_provided else ""
    )

    lhs_remapped = lhs.select(
        [
            _alias_if_needed(
                lhs,
                name,
                lhs_prefix,
                lsuffix,
                [] if isinstance(join_type, (LeftSemi, LeftAnti)) else common_col_names,
            )
            for name in lhs_names
        ],
        _emit_ast=False,
    )

    rhs_remapped = rhs.select(
        [
            _alias_if_needed(rhs, name, rhs_prefix, rsuffix, common_col_names)
            for name in rhs_names
        ],
        _emit_ast=False,
    )
    return lhs_remapped, rhs_remapped


class DataFrame:
    """Represents a lazily-evaluated relational dataset that contains a collection
    of :class:`Row` objects with columns defined by a schema (column name and type).

    A DataFrame is considered lazy because it encapsulates the computation or query
    required to produce a relational dataset. The computation is not performed until
    you call a method that performs an action (e.g. :func:`collect`).

    **Creating a DataFrame**

    You can create a DataFrame in a number of different ways, as shown in the examples
    below.

    Creating tables and data to run the sample code:
        >>> session.sql("create or replace temp table prices(product_id varchar, amount number(10, 2))").collect()
        [Row(status='Table PRICES successfully created.')]
        >>> session.sql("insert into prices values ('id1', 10.0), ('id2', 20.0)").collect()
        [Row(number of rows inserted=2)]
        >>> # Create a CSV file to demo load
        >>> import tempfile
        >>> with tempfile.NamedTemporaryFile(mode="w+t") as t:
        ...     t.writelines(["id1, Product A", "\\n" "id2, Product B"])
        ...     t.flush()
        ...     create_stage_result = session.sql("create temp stage test_stage").collect()
        ...     put_result = session.file.put(t.name, "@test_stage/test_dir")

    Example 1
        Creating a DataFrame by reading a table in Snowflake::

            >>> df_prices = session.table("prices")

    Example 2
        Creating a DataFrame by reading files from a stage::

            >>> from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType
            >>> df_catalog = session.read.schema(StructType([StructField("id", StringType()), StructField("name", StringType())])).csv("@test_stage/test_dir")
            >>> df_catalog.show()
            ---------------------
            |"ID"  |"NAME"      |
            ---------------------
            |id1   | Product A  |
            |id2   | Product B  |
            ---------------------
            <BLANKLINE>

    Example 3
        Creating a DataFrame by specifying a sequence or a range::

            >>> session.create_dataframe([(1, "one"), (2, "two")], schema=["col_a", "col_b"]).show()
            ---------------------
            |"COL_A"  |"COL_B"  |
            ---------------------
            |1        |one      |
            |2        |two      |
            ---------------------
            <BLANKLINE>
            >>> session.range(1, 10, 2).to_df("col1").show()
            ----------
            |"COL1"  |
            ----------
            |1       |
            |3       |
            |5       |
            |7       |
            |9       |
            ----------
            <BLANKLINE>

    Example 4
        Create a new DataFrame by applying transformations to other existing DataFrames::

            >>> df_merged_data = df_catalog.join(df_prices, df_catalog["id"] == df_prices["product_id"])

    **Performing operations on a DataFrame**

    Broadly, the operations on DataFrame can be divided into two types:

    - **Transformations** produce a new DataFrame from one or more existing DataFrames. Note that transformations are lazy and don't cause the DataFrame to be evaluated. If the API does not provide a method to express the SQL that you want to use, you can use :func:`functions.sqlExpr` as a workaround.
    - **Actions** cause the DataFrame to be evaluated. When you call a method that performs an action, Snowpark sends the SQL query for the DataFrame to the server for evaluation.

    **Transforming a DataFrame**

    The following examples demonstrate how you can transform a DataFrame.

    Example 5
        Using the :func:`select()` method to select the columns that should be in the
        DataFrame (similar to adding a ``SELECT`` clause)::

            >>> # Return a new DataFrame containing the product_id and amount columns of the prices table.
            >>> # This is equivalent to: SELECT PRODUCT_ID, AMOUNT FROM PRICES;
            >>> df_price_ids_and_amounts = df_prices.select(col("product_id"), col("amount"))

    Example 6
        Using the :func:`Column.as_` method to rename a column in a DataFrame (similar
        to using ``SELECT col AS alias``)::

            >>> # Return a new DataFrame containing the product_id column of the prices table as a column named
            >>> # item_id. This is equivalent to: SELECT PRODUCT_ID AS ITEM_ID FROM PRICES;
            >>> df_price_item_ids = df_prices.select(col("product_id").as_("item_id"))

    Example 7
        Using the :func:`filter` method to filter data (similar to adding a ``WHERE`` clause)::

            >>> # Return a new DataFrame containing the row from the prices table with the ID 1.
            >>> # This is equivalent to:
            >>> # SELECT * FROM PRICES WHERE PRODUCT_ID = 1;
            >>> df_price1 = df_prices.filter((col("product_id") == 1))

    Example 8
        Using the :func:`sort()` method to specify the sort order of the data (similar to adding an ``ORDER BY`` clause)::

            >>> # Return a new DataFrame for the prices table with the rows sorted by product_id.
            >>> # This is equivalent to: SELECT * FROM PRICES ORDER BY PRODUCT_ID;
            >>> df_sorted_prices = df_prices.sort(col("product_id"))

    Example 9
        Using :meth:`agg` method to aggregate results.

            >>> import snowflake.snowpark.functions as f
            >>> df_prices.agg(("amount", "sum")).collect()
            [Row(SUM(AMOUNT)=Decimal('30.00'))]
            >>> df_prices.agg(f.sum("amount")).collect()
            [Row(SUM(AMOUNT)=Decimal('30.00'))]
            >>> # rename the aggregation column name
            >>> df_prices.agg(f.sum("amount").alias("total_amount"), f.max("amount").alias("max_amount")).collect()
            [Row(TOTAL_AMOUNT=Decimal('30.00'), MAX_AMOUNT=Decimal('20.00'))]

    Example 10
        Using the :func:`group_by()` method to return a
        :class:`RelationalGroupedDataFrame` that you can use to group and aggregate
        results (similar to adding a ``GROUP BY`` clause).

        :class:`RelationalGroupedDataFrame` provides methods for aggregating results, including:

        - :func:`RelationalGroupedDataFrame.avg()` (equivalent to AVG(column))
        - :func:`RelationalGroupedDataFrame.count()` (equivalent to COUNT())
        - :func:`RelationalGroupedDataFrame.max()` (equivalent to MAX(column))
        - :func:`RelationalGroupedDataFrame.median()` (equivalent to MEDIAN(column))
        - :func:`RelationalGroupedDataFrame.min()` (equivalent to MIN(column))
        - :func:`RelationalGroupedDataFrame.sum()` (equivalent to SUM(column))

        >>> # Return a new DataFrame for the prices table that computes the sum of the prices by
        >>> # category. This is equivalent to:
        >>> #  SELECT CATEGORY, SUM(AMOUNT) FROM PRICES GROUP BY CATEGORY
        >>> df_total_price_per_category = df_prices.group_by(col("product_id")).sum(col("amount"))
        >>> # Have multiple aggregation values with the group by
        >>> import snowflake.snowpark.functions as f
        >>> df_summary = df_prices.group_by(col("product_id")).agg(f.sum(col("amount")).alias("total_amount"), f.avg("amount")).sort(col("product_id"))
        >>> df_summary.show()
        -------------------------------------------------
        |"PRODUCT_ID"  |"TOTAL_AMOUNT"  |"AVG(AMOUNT)"  |
        -------------------------------------------------
        |id1           |10.00           |10.00000000    |
        |id2           |20.00           |20.00000000    |
        -------------------------------------------------
        <BLANKLINE>

    Example 11
        Using windowing functions. Refer to :class:`Window` for more details.

            >>> from snowflake.snowpark import Window
            >>> from snowflake.snowpark.functions import row_number
            >>> df_prices.with_column("price_rank",  row_number().over(Window.order_by(col("amount").desc()))).show()
            ------------------------------------------
            |"PRODUCT_ID"  |"AMOUNT"  |"PRICE_RANK"  |
            ------------------------------------------
            |id2           |20.00     |1             |
            |id1           |10.00     |2             |
            ------------------------------------------
            <BLANKLINE>

    Example 12
        Handling missing values. Refer to :class:`DataFrameNaFunctions` for more details.

            >>> df = session.create_dataframe([[1, None, 3], [4, 5, None]], schema=["a", "b", "c"])
            >>> df.na.fill({"b": 2, "c": 6}).show()
            -------------------
            |"A"  |"B"  |"C"  |
            -------------------
            |1    |2    |3    |
            |4    |5    |6    |
            -------------------
            <BLANKLINE>

    **Performing an action on a DataFrame**

    The following examples demonstrate how you can perform an action on a DataFrame.

    Example 13
        Performing a query and returning an array of Rows::

            >>> df_prices.collect()
            [Row(PRODUCT_ID='id1', AMOUNT=Decimal('10.00')), Row(PRODUCT_ID='id2', AMOUNT=Decimal('20.00'))]

    Example 14
        Performing a query and print the results::

            >>> df_prices.show()
            ---------------------------
            |"PRODUCT_ID"  |"AMOUNT"  |
            ---------------------------
            |id1           |10.00     |
            |id2           |20.00     |
            ---------------------------
            <BLANKLINE>

    Example 15
        Calculating statistics values. Refer to :class:`DataFrameStatFunctions` for more details.

            >>> df = session.create_dataframe([[1, 2], [3, 4], [5, -1]], schema=["a", "b"])
            >>> df.stat.corr("a", "b")
            -0.5960395606792697

    Example 16
        Performing a query asynchronously and returning a list of :class:`Row` objects::

            >>> df = session.create_dataframe([[float(4), 3, 5], [2.0, -4, 7], [3.0, 5, 6], [4.0, 6, 8]], schema=["a", "b", "c"])
            >>> async_job = df.collect_nowait()
            >>> async_job.result()
            [Row(A=4.0, B=3, C=5), Row(A=2.0, B=-4, C=7), Row(A=3.0, B=5, C=6), Row(A=4.0, B=6, C=8)]

    Example 17
        Performing a query and transforming it into :class:`pandas.DataFrame` asynchronously::

            >>> async_job = df.to_pandas(block=False)
            >>> async_job.result()
                 A  B  C
            0  4.0  3  5
            1  2.0 -4  7
            2  3.0  5  6
            3  4.0  6  8
    """

    @publicapi
    def __init__(
        self,
        session: Optional["snowflake.snowpark.Session"] = None,
        plan: Optional[LogicalPlan] = None,
        is_cached: bool = False,
        _ast_stmt: Optional[proto.Bind] = None,
        _emit_ast: bool = True,
    ) -> None:
        """
        :param int _ast_stmt: The AST Bind atom corresponding to this dataframe value. We track its assigned ID in the
                             slot self._ast_id. This allows this value to be referred to symbolically when it's
                             referenced in subsequent dataframe expressions.
        """
        self._session = session
        if plan is not None:
            self._plan = self._session._analyzer.resolve(plan)
        else:
            self._plan = None

        if isinstance(plan, (SelectStatement, MockSelectStatement)):
            self._select_statement = plan
            plan.expr_to_alias.update(self._plan.expr_to_alias)
            plan.df_aliased_col_name_to_real_col_name.update(
                self._plan.df_aliased_col_name_to_real_col_name
            )
        else:
            self._select_statement = None

        # Setup the ast id for the dataframe.
        self.__ast_id = None
        if _emit_ast:
            self._ast_id = _ast_stmt.uid if _ast_stmt is not None else None

        self._statement_params = None
        self.is_cached: bool = is_cached  #: Whether the dataframe is cached.
        self._ops_after_agg = None

        # Whether all columns are VARIANT data type,
        # which support querying nested fields via dot notations
        # If SQL simplifier is enabled, we need to get logical plan from plan.from_
        if isinstance(plan, (SelectStatement, MockSelectStatement)):
            self._all_variant_cols = (
                isinstance(plan.from_, SelectSnowflakePlan)
                and isinstance(plan.from_.snowflake_plan.source_plan, ReadFileNode)
                and plan.from_.snowflake_plan.source_plan.xml_reader_udtf is not None
            )
        else:
            self._all_variant_cols = bool(
                isinstance(plan, ReadFileNode) and plan.xml_reader_udtf is not None
            )

        self._reader: Optional["snowflake.snowpark.DataFrameReader"] = None
        self._writer = DataFrameWriter(self, _emit_ast=False)

        self._stat = DataFrameStatFunctions(self)
        self._analytics = DataFrameAnalyticsFunctions(self)
        self.approxQuantile = self.approx_quantile = self._stat.approx_quantile
        self.corr = self._stat.corr
        self.cov = self._stat.cov
        self.crosstab = self._stat.crosstab
        self.sampleBy = self.sample_by = self._stat.sample_by

        self._na = DataFrameNaFunctions(self)
        self.dropna = self._na.drop
        self.fillna = self._na.fill
        self.replace = self._na.replace

        self._ai = DataFrameAIFunctions(self)

        self._alias: Optional[str] = None

        if context._debug_eager_schema_validation:
            # Getting the plan attributes may run a describe query
            # and populates the schema for the dataframe.
            self._plan.attributes

    def _set_ast_ref(self, dataframe_expr_builder: Any) -> None:
        """
        Given a field builder expression of the AST type Expr, points the builder to reference this dataframe.
        """
        # TODO SNOW-1762262: remove once we generate the correct AST.
        debug_check_missing_ast(self._ast_id, self._session, self)
        dataframe_expr_builder.dataframe_ref.id = self._ast_id

    @property
    def stat(self) -> DataFrameStatFunctions:
        return self._stat

    @property
    def analytics(self) -> DataFrameAnalyticsFunctions:
        return self._analytics

    @property
    def _ast_id(self) -> Optional[int]:
        return self.__ast_id

    @_ast_id.setter
    def _ast_id(self, value: Optional[int]) -> None:
        self.__ast_id = value
        if self._plan is not None and value is not None:
            self._plan.add_df_ast_id(value)
        if self._select_statement is not None and value is not None:
            self._select_statement.add_df_ast_id(value)

    @publicapi
    @overload
    def collect(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        log_on_exception: bool = False,
        case_sensitive: bool = True,
        _emit_ast: bool = True,
    ) -> List[Row]:
        ...  # pragma: no cover

    @publicapi
    @overload
    def collect(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = False,
        log_on_exception: bool = False,
        case_sensitive: bool = True,
        _emit_ast: bool = True,
    ) -> AsyncJob:
        ...  # pragma: no cover

    @df_collect_api_telemetry
    @publicapi
    def collect(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        log_on_exception: bool = False,
        case_sensitive: bool = True,
        _emit_ast: bool = True,
    ) -> Union[List[Row], AsyncJob]:
        """Executes the query representing this DataFrame and returns the result as a
        list of :class:`Row` objects.

        Args:
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            block: A bool value indicating whether this function will wait until the result is available.
                When it is ``False``, this function executes the underlying queries of the dataframe
                asynchronously and returns an :class:`AsyncJob`.
            case_sensitive: A bool value which controls the case sensitivity of the fields in the
                :class:`Row` objects returned by the ``collect``. Defaults to ``True``.

        See also:
            :meth:`collect_nowait()`
        """

        kwargs = {}
        if _emit_ast:
            # Add an Bind node that applies DataframeCollect() to the input, followed by its Eval.
            stmt = self._session._ast_batch.bind()
            expr = with_src_position(stmt.expr.dataframe_collect)
            self._set_ast_ref(expr.df)
            if statement_params is not None:
                build_expr_from_dict_str_str(expr.statement_params, statement_params)
            expr.block = block
            expr.case_sensitive = case_sensitive
            expr.log_on_exception = log_on_exception
            expr.no_wait = False

            self._session._ast_batch.eval(stmt)

            # Flush the AST and encode it as part of the query.
            _, kwargs[DATAFRAME_AST_PARAMETER] = self._session._ast_batch.flush(stmt)

        with open_telemetry_context_manager(self.collect, self):
            return self._internal_collect_with_tag_no_telemetry(
                statement_params=statement_params,
                block=block,
                log_on_exception=log_on_exception,
                case_sensitive=case_sensitive,
                **kwargs,
            )

    @df_collect_api_telemetry
    @publicapi
    def collect_nowait(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        log_on_exception: bool = False,
        case_sensitive: bool = True,
        _emit_ast: bool = True,
    ) -> AsyncJob:
        """Executes the query representing this DataFrame asynchronously and returns: class:`AsyncJob`.
        It is equivalent to ``collect(block=False)``.

        Args:
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            case_sensitive: A bool value which is controls the case sensitivity of the fields in the
                :class:`Row` objects after collecting the result using :meth:`AsyncJob.result`. Defaults to
                ``True``.

        See also:
            :meth:`collect()`
        """
        kwargs = {}
        if _emit_ast:
            # Add an Bind node that applies DataframeCollect() to the input, followed by its Eval.
            stmt = self._session._ast_batch.bind()
            expr = with_src_position(stmt.expr.dataframe_collect)
            self._set_ast_ref(expr.df)
            if statement_params is not None:
                build_expr_from_dict_str_str(expr.statement_params, statement_params)
            expr.case_sensitive = case_sensitive
            expr.log_on_exception = log_on_exception
            expr.no_wait = True

            self._session._ast_batch.eval(stmt)

            # Flush AST and encode it as part of the query.
            _, kwargs[DATAFRAME_AST_PARAMETER] = self._session._ast_batch.flush(stmt)

        with open_telemetry_context_manager(self.collect_nowait, self):
            return self._internal_collect_with_tag_no_telemetry(
                statement_params=statement_params,
                block=False,
                data_type=_AsyncResultType.ROW,
                log_on_exception=log_on_exception,
                case_sensitive=case_sensitive,
                **kwargs,
            )

    def _internal_collect_with_tag_no_telemetry(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        data_type: _AsyncResultType = _AsyncResultType.ROW,
        log_on_exception: bool = False,
        case_sensitive: bool = True,
        **kwargs: Any,
    ) -> Union[List[Row], AsyncJob]:
        # When executing a DataFrame in any method of snowpark (either public or private),
        # we should always call this method instead of collect(), to make sure the
        # query tag is set properly.
        statement_params = track_data_source_statement_params(
            self, statement_params or self._statement_params
        )
        return self._session._conn.execute(
            self._plan,
            block=block,
            data_type=data_type,
            _statement_params=create_or_update_statement_params_with_query_tag(
                statement_params or self._statement_params,
                self._session.query_tag,
                SKIP_LEVELS_THREE,
                collect_stacktrace=self._session.conf.get(
                    "collect_stacktrace_in_query_tag"
                ),
            ),
            log_on_exception=log_on_exception,
            case_sensitive=case_sensitive,
            **kwargs,
        )

    _internal_collect_with_tag = df_collect_api_telemetry(
        _internal_collect_with_tag_no_telemetry
    )

    @df_collect_api_telemetry
    def _execute_and_get_query_id(
        self, *, statement_params: Optional[Dict[str, str]] = None
    ) -> str:
        """This method is only used in stored procedures."""
        with open_telemetry_context_manager(self._execute_and_get_query_id, self):
            return self._session._conn.get_result_query_id(
                self._plan,
                _statement_params=create_or_update_statement_params_with_query_tag(
                    statement_params or self._statement_params,
                    self._session.query_tag,
                    SKIP_LEVELS_THREE,
                    collect_stacktrace=self._session.conf.get(
                        "collect_stacktrace_in_query_tag"
                    ),
                ),
            )

    @overload
    @publicapi
    def to_local_iterator(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        case_sensitive: bool = True,
        _emit_ast: bool = True,
    ) -> Iterator[Row]:
        ...  # pragma: no cover

    @overload
    @publicapi
    def to_local_iterator(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = False,
        case_sensitive: bool = True,
        _emit_ast: bool = True,
    ) -> AsyncJob:
        ...  # pragma: no cover

    @df_collect_api_telemetry
    @publicapi
    def to_local_iterator(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        case_sensitive: bool = True,
        _emit_ast: bool = True,
    ) -> Union[Iterator[Row], AsyncJob]:
        """Executes the query representing this DataFrame and returns an iterator
        of :class:`Row` objects that you can use to retrieve the results.

        Unlike :meth:`collect`, this method does not load all data into memory
        at once.

        Example::

            >>> df = session.table("prices")
            >>> for row in df.to_local_iterator():
            ...     print(row)
            Row(PRODUCT_ID='id1', AMOUNT=Decimal('10.00'))
            Row(PRODUCT_ID='id2', AMOUNT=Decimal('20.00'))

        Args:
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            block: A bool value indicating whether this function will wait until the result is available.
                When it is ``False``, this function executes the underlying queries of the dataframe
                asynchronously and returns an :class:`AsyncJob`.
            case_sensitive: A bool value which controls the case sensitivity of the fields in the
                :class:`Row` objects returned by the ``to_local_iterator``. Defaults to ``True``.
        """

        kwargs = {}
        if _emit_ast:
            # Add an Bind node that applies DataframeToLocalIterator() to the input, followed by its Eval.
            stmt = self._session._ast_batch.bind()
            expr = with_src_position(stmt.expr.dataframe_to_local_iterator)

            self._set_ast_ref(expr.df)
            if statement_params is not None:
                build_expr_from_dict_str_str(expr.statement_params, statement_params)
            expr.block = block
            expr.case_sensitive = case_sensitive

            self._session._ast_batch.eval(stmt)

            # Flush the AST and encode it as part of the query.
            _, kwargs[DATAFRAME_AST_PARAMETER] = self._session._ast_batch.flush(stmt)

        return self._session._conn.execute(
            self._plan,
            to_iter=True,
            block=block,
            data_type=_AsyncResultType.ITERATOR,
            _statement_params=create_or_update_statement_params_with_query_tag(
                statement_params or self._statement_params,
                self._session.query_tag,
                SKIP_LEVELS_THREE,
                collect_stacktrace=self._session.conf.get(
                    "collect_stacktrace_in_query_tag"
                ),
            ),
            case_sensitive=case_sensitive,
            **kwargs,
        )

    def _copy_plan(self) -> LogicalPlan:
        """Returns a shallow copy of the plan of the DataFrame."""
        if self._select_statement:
            new_plan = copy.copy(self._select_statement)
            new_plan.column_states = self._select_statement.column_states
            new_plan._projection_in_str = self._select_statement.projection_in_str
            new_plan._schema_query = self._select_statement.schema_query
            new_plan._query_params = self._select_statement.query_params
            return new_plan
        else:
            return copy.copy(self._plan)

    def _copy_without_ast(self) -> "DataFrame":
        """Returns a shallow copy of the DataFrame without AST generation."""
        return DataFrame(self._session, self._copy_plan(), _emit_ast=False)

    def __copy__(self) -> "DataFrame":
        """Implements shallow copy protocol for copy.copy(...)."""
        stmt = None
        if self._session.ast_enabled:
            stmt = self._session._ast_batch.bind()
            with_src_position(stmt.expr.dataframe_ref, stmt)
            self._set_ast_ref(stmt.expr)
        return DataFrame(
            self._session,
            self._copy_plan(),
            _ast_stmt=stmt,
            _emit_ast=self._session.ast_enabled,
        )

    if installed_pandas:
        import pandas  # pragma: no cover

        @publicapi
        @overload
        def to_pandas(
            self,
            *,
            statement_params: Optional[Dict[str, str]] = None,
            block: bool = True,
            _emit_ast: bool = True,
            **kwargs: Dict[str, Any],
        ) -> pandas.DataFrame:
            ...  # pragma: no cover

    @publicapi
    @overload
    def to_pandas(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = False,
        _emit_ast: bool = True,
        **kwargs: Dict[str, Any],
    ) -> AsyncJob:
        ...  # pragma: no cover

    @df_collect_api_telemetry
    @publicapi
    def to_pandas(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        _emit_ast: bool = True,
        **kwargs: Dict[str, Any],
    ) -> Union["pandas.DataFrame", AsyncJob]:
        """
        Executes the query representing this DataFrame and returns the result as a
        `pandas DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_.

        When the data is too large to fit into memory, you can use :meth:`to_pandas_batches`.

        Args:
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            block: A bool value indicating whether this function will wait until the result is available.
                When it is ``False``, this function executes the underlying queries of the dataframe
                asynchronously and returns an :class:`AsyncJob`.

        Note:
            1. This method is only available if pandas is installed and available.

            2. If you use :func:`Session.sql` with this method, the input query of
            :func:`Session.sql` can only be a SELECT statement.

            3. For TIMESTAMP columns:
            - TIMESTAMP_LTZ and TIMESTAMP_TZ are both converted to `datetime64[ns, tz]` in pandas,
            as pandas cannot distinguish between the two.
            - TIMESTAMP_NTZ is converted to `datetime64[ns]` (without timezone).
        """

        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_to_pandas, stmt)
            self._set_ast_ref(ast.df)
            if statement_params is not None:
                build_expr_from_dict_str_str(ast.statement_params, statement_params)
            ast.block = block
            self._session._ast_batch.eval(stmt)

            # Flush the AST and encode it as part of the query.
            _, kwargs[DATAFRAME_AST_PARAMETER] = self._session._ast_batch.flush(stmt)

        with open_telemetry_context_manager(self.to_pandas, self):
            result = self._session._conn.execute(
                self._plan,
                to_pandas=True,
                block=block,
                data_type=_AsyncResultType.PANDAS,
                _statement_params=create_or_update_statement_params_with_query_tag(
                    statement_params or self._statement_params,
                    self._session.query_tag,
                    SKIP_LEVELS_TWO,
                    collect_stacktrace=self._session.conf.get(
                        "collect_stacktrace_in_query_tag"
                    ),
                ),
                **kwargs,
            )

        if block:
            if not isinstance(result, pandas.DataFrame):
                query = self._plan.queries[-1].sql.strip().lower()
                is_select_statement = is_sql_select_statement(query)
                if is_select_statement:
                    _logger.warning(
                        "The query result format is set to JSON. "
                        "The result of to_pandas() may not align with the result returned in the ARROW format. "
                        "For best compatibility with to_pandas(), set the query result format to ARROW."
                    )
                return pandas.DataFrame(
                    result,
                    columns=[
                        (
                            unquote_if_quoted(attr.name)
                            if is_select_statement
                            else attr.name
                        )
                        for attr in self._plan.attributes
                    ],
                )

        return result

    if installed_pandas:
        import pandas

        @publicapi
        @overload
        def to_pandas_batches(
            self,
            *,
            statement_params: Optional[Dict[str, str]] = None,
            block: bool = True,
            _emit_ast: bool = True,
            **kwargs: Dict[str, Any],
        ) -> Iterator[pandas.DataFrame]:
            ...  # pragma: no cover

    @publicapi
    @overload
    def to_pandas_batches(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = False,
        _emit_ast: bool = True,
        **kwargs: Dict[str, Any],
    ) -> AsyncJob:
        ...  # pragma: no cover

    @df_collect_api_telemetry
    @publicapi
    def to_pandas_batches(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        _emit_ast: bool = True,
        **kwargs: Dict[str, Any],
    ) -> Union[Iterator["pandas.DataFrame"], AsyncJob]:
        """
        Executes the query representing this DataFrame and returns an iterator of
        pandas dataframes (containing a subset of rows) that you can use to
        retrieve the results.

        Unlike :meth:`to_pandas`, this method does not load all data into memory
        at once.

        Example::

            >>> df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
            >>> for pandas_df in df.to_pandas_batches():
            ...     print(pandas_df)
               A  B
            0  1  2
            1  3  4

        Args:
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            block: A bool value indicating whether this function will wait until the result is available.
                When it is ``False``, this function executes the underlying queries of the dataframe
                asynchronously and returns an :class:`AsyncJob`.

        Note:
            1. This method is only available if pandas is installed and available.

            2. If you use :func:`Session.sql` with this method, the input query of
            :func:`Session.sql` can only be a SELECT statement.
        """
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_to_pandas_batches, stmt)
            self._set_ast_ref(ast.df)
            if statement_params is not None:
                build_expr_from_dict_str_str(ast.statement_params, statement_params)
            ast.block = block
            self._session._ast_batch.eval(stmt)

            # Flush the AST and encode it as part of the query.
            _, kwargs[DATAFRAME_AST_PARAMETER] = self._session._ast_batch.flush(stmt)

        return self._session._conn.execute(
            self._plan,
            to_pandas=True,
            to_iter=True,
            block=block,
            data_type=_AsyncResultType.PANDAS_BATCH,
            _statement_params=create_or_update_statement_params_with_query_tag(
                statement_params or self._statement_params,
                self._session.query_tag,
                SKIP_LEVELS_TWO,
                collect_stacktrace=self._session.conf.get(
                    "collect_stacktrace_in_query_tag"
                ),
            ),
            **kwargs,
        )

    @experimental(version="1.28.0")
    @df_collect_api_telemetry
    @publicapi
    def to_arrow(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        _emit_ast: bool = True,
        **kwargs: Dict[str, Any],
    ) -> Union["pyarrow.Table", AsyncJob]:
        """
        Executes the query representing this DataFrame and returns the result as a
        `pyarrow Table <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`.

        When the data is too large to fit into memory, you can use :meth:`to_arrow_batches`.

        This function requires the optional dependenct snowflake-snowpark-python[pandas] be installed.

        Args:
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            block: A bool value indicating whether this function will wait until the result is available.
                When it is ``False``, this function executes the underlying queries of the dataframe
                asynchronously and returns an :class:`AsyncJob`.
        """
        return self._session._conn.execute(
            self._plan,
            to_pandas=False,
            to_iter=False,
            to_arrow=True,
            block=block,
            _statement_params=create_or_update_statement_params_with_query_tag(
                statement_params or self._statement_params,
                self._session.query_tag,
                SKIP_LEVELS_TWO,
                collect_stacktrace=self._session.conf.get(
                    "collect_stacktrace_in_query_tag"
                ),
            ),
            **kwargs,
        )

    @experimental(version="1.28.0")
    @df_collect_api_telemetry
    @publicapi
    def to_arrow_batches(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        _emit_ast: bool = True,
        **kwargs: Dict[str, Any],
    ) -> Union[Iterator["pyarrow.Table"], AsyncJob]:
        """
        Executes the query representing this DataFrame and returns an iterator of
        pyarrow Tables (containing a subset of rows) that you can use to
        retrieve the results.

        Unlike :meth:`to_arrow`, this method does not load all data into memory
        at once.

        Args:
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            block: A bool value indicating whether this function will wait until the result is available.
                When it is ``False``, this function executes the underlying queries of the dataframe
                asynchronously and returns an :class:`AsyncJob`.
        """
        return self._session._conn.execute(
            self._plan,
            to_pandas=False,
            to_iter=True,
            to_arrow=True,
            block=block,
            data_type=_AsyncResultType.ITERATOR,
            _statement_params=create_or_update_statement_params_with_query_tag(
                statement_params or self._statement_params,
                self._session.query_tag,
                SKIP_LEVELS_TWO,
                collect_stacktrace=self._session.conf.get(
                    "collect_stacktrace_in_query_tag"
                ),
            ),
            **kwargs,
        )

    @df_api_usage
    @publicapi
    def to_df(
        self, *names: Union[str, Iterable[str]], _emit_ast: bool = True
    ) -> "DataFrame":
        """
        Creates a new DataFrame containing columns with the specified names.

        The number of column names that you pass in must match the number of columns in the existing
        DataFrame.

        Examples::

            >>> df1 = session.range(1, 10, 2).to_df("col1")
            >>> df2 = session.range(1, 10, 2).to_df(["col1"])

        Args:
            names: list of new column names
        """
        col_names, is_variadic = parse_positional_args_to_list_variadic(*names)
        if not all(isinstance(n, str) for n in col_names):
            raise TypeError(
                "Invalid input type in to_df(), expected str or a list of strs."
            )

        if len(self._output) != len(col_names):
            raise ValueError(
                f"The number of columns doesn't match. "
                f"Old column names ({len(self._output)}): "
                f"{','.join(attr.name for attr in self._output)}. "
                f"New column names ({len(col_names)}): {','.join(col_names)}."
            )

        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_to_df, stmt)
            for col in col_names:
                build_expr_from_python_val(ast.col_names.args.add(), col)
            ast.col_names.variadic = is_variadic
            self._set_ast_ref(ast.df)

        new_cols = []
        for attr, name in zip(self._output, col_names):
            new_cols.append(Column(attr).alias(name))
        df = self.select(new_cols, _ast_stmt=stmt, _emit_ast=_emit_ast)

        if _emit_ast:
            df._ast_id = stmt.uid

        return df

    @df_collect_api_telemetry
    @publicapi
    def to_snowpark_pandas(
        self,
        index_col: Optional[Union[str, List[str]]] = None,
        columns: Optional[List[str]] = None,
        enforce_ordering: bool = False,
        _emit_ast: bool = True,
    ) -> "modin.pandas.DataFrame":
        """
        Convert the Snowpark DataFrame to Snowpark pandas DataFrame.

        Args:
            index_col: A column name or a list of column names to use as index.
            columns: A list of column names for the columns to select from the Snowpark DataFrame. If not specified, select
                all columns except ones configured in index_col.
            enforce_ordering: If False, Snowpark pandas will provide relaxed consistency and ordering guarantees for the returned
                DataFrame object. Otherwise, strict consistency and ordering guarantees are provided. Please refer to the
                documentation of :func:`~modin.pandas.read_snowflake` for more details. If DDL or DML queries have been
                used in this query this parameter is ignored and ordering is enforced.


        Returns:
            :class:`~modin.pandas.DataFrame`
                A Snowpark pandas DataFrame contains index and data columns based on the snapshot of the current
                Snowpark DataFrame, which triggers an eager evaluation.

                If index_col is provided, the specified index_col is selected as the index column(s) for the result dataframe,
                otherwise, a default range index from 0 to n - 1 is created as the index column, where n is the number
                of rows. Please note that is also used as the start row ordering for the dataframe, but there is no
                guarantee that the default row ordering is the same for two Snowpark pandas dataframe created from
                the same Snowpark Dataframe.

                If columns are provided, the specified columns are selected as the data column(s) for the result dataframe,
                otherwise, all Snowpark DataFrame columns (exclude index_col) are selected as data columns.

        Note:
            Transformations performed on the returned Snowpark pandas Dataframe do not affect the Snowpark DataFrame
            from which it was created. Call
            - :func:`modin.pandas.to_snowpark <modin.pandas.to_snowpark>`
            to transform a Snowpark pandas DataFrame back to a Snowpark DataFrame.

            The column names used for columns or index_cols must be Normalized Snowflake Identifiers, and the
            Normalized Snowflake Identifiers of a Snowpark DataFrame can be displayed by calling df.show().
            For details about Normalized Snowflake Identifiers, please refer to the Note in :func:`~modin.pandas.read_snowflake`

            `to_snowpark_pandas` works only when the environment is set up correctly for Snowpark pandas. This environment
            may require version of Python and pandas different from what Snowpark Python uses If the environment is setup
            incorrectly, an error will be raised when `to_snowpark_pandas` is called.

            For Python version support information, please refer to:
            - the prerequisites section https://docs.snowflake.com/en/developer-guide/snowpark/python/snowpark-pandas#prerequisites
            - the installation section https://docs.snowflake.com/en/developer-guide/snowpark/python/snowpark-pandas#installing-the-snowpark-pandas-api

        See also:
            - :func:`modin.pandas.to_snowpark <modin.pandas.to_snowpark>`
            - :func:`modin.pandas.DataFrame.to_snowpark <modin.pandas.DataFrame.to_snowpark>`
            - :func:`modin.pandas.Series.to_snowpark <modin.pandas.Series.to_snowpark>`

        Example::
            >>> df = session.create_dataframe([[1, 2, 3]], schema=["a", "b", "c"])
            >>> snowpark_pandas_df = df.to_snowpark_pandas()  # doctest: +SKIP
            >>> snowpark_pandas_df      # doctest: +SKIP +NORMALIZE_WHITESPACE
               A  B  C
            0  1  2  3

            >>> snowpark_pandas_df = df.to_snowpark_pandas(index_col='A')  # doctest: +SKIP
            >>> snowpark_pandas_df      # doctest: +SKIP +NORMALIZE_WHITESPACE
               B  C
            A
            1  2  3
            >>> snowpark_pandas_df = df.to_snowpark_pandas(index_col='A', columns=['B'])  # doctest: +SKIP
            >>> snowpark_pandas_df      # doctest: +SKIP +NORMALIZE_WHITESPACE
               B
            A
            1  2
            >>> snowpark_pandas_df = df.to_snowpark_pandas(index_col=['B', 'A'], columns=['A', 'C', 'A'])  # doctest: +SKIP
            >>> snowpark_pandas_df      # doctest: +SKIP +NORMALIZE_WHITESPACE
                 A  C  A
            B A
            2 1  1  3  1
        """
        # black and isort disagree on how to format this section with isort: skip
        # fmt: off
        import snowflake.snowpark.modin.plugin  # isort: skip  # noqa: F401
        # If snowflake.snowpark.modin.plugin was successfully imported, then modin.pandas is available
        import modin.pandas as pd  # isort: skip
        # fmt: on

        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.to_snowpark_pandas, stmt)
            self._set_ast_ref(ast.df)
            if index_col is not None:
                ast.index_col.extend(
                    index_col if isinstance(index_col, list) else [index_col]
                )
            if columns is not None:
                ast.columns.extend(columns if isinstance(columns, list) else [columns])
        has_existing_ddl_dml_queries = False
        if not enforce_ordering and len(self.queries["queries"]) > 1:
            has_existing_ddl_dml_queries = True
            warning(
                "enforce_ordering_ddl",
                "enforce_ordering is enabled when using DML/DDL operations regardless of user setting",
                warning_times=1,
            )

        if enforce_ordering or has_existing_ddl_dml_queries:
            # create a temporary table out of the current snowpark dataframe
            temporary_table_name = random_name_for_temp_object(
                TempObjectType.TABLE
            )  # pragma: no cover
            ast_id = self._ast_id
            self._ast_id = None  # set the AST ID to None to prevent AST emission.
            self.write.save_as_table(
                temporary_table_name,
                mode="errorifexists",
                table_type="temporary",
                _emit_ast=False,
            )  # pragma: no cover
            self._ast_id = ast_id  # reset the AST ID.

            snowpandas_df = pd.read_snowflake(
                name_or_query=temporary_table_name,
                index_col=index_col,
                columns=columns,
                enforce_ordering=True,
            )  # pragma: no cover
        else:
            snowpandas_df = pd.read_snowflake(
                name_or_query=self.queries["queries"][0],
                index_col=index_col,
                columns=columns,
                enforce_ordering=False,
            )  # pragma: no cover

        if _emit_ast:
            # Set the Snowpark DataFrame AST ID to the AST ID of this pandas query.
            snowpandas_df._query_compiler._modin_frame.ordered_dataframe._dataframe_ref.snowpark_dataframe._ast_id = (
                stmt.uid
            )

        return snowpandas_df

    def __getitem__(self, item: Union[str, Column, List, Tuple, int]):

        _emit_ast = self._ast_id is not None and self._session.ast_enabled

        if isinstance(item, str):
            return self.col(item, _emit_ast=_emit_ast)
        elif isinstance(item, Column):
            return self.filter(item, _emit_ast=_emit_ast)
        elif isinstance(item, (list, tuple)):
            return self.select(item, _emit_ast=_emit_ast)
        elif isinstance(item, int):
            return self.__getitem__(self.columns[item])
        else:
            raise TypeError(f"Unexpected item type: {type(item)}")

    def __getattr__(self, name: str):
        # Snowflake DB ignores cases when there is no quotes.
        if name.lower() not in [c.lower() for c in self.columns]:
            raise AttributeError(
                f"{self.__class__.__name__} object has no attribute {name}"
            )
        return self.col(name)

    @property
    def columns(self) -> List[str]:
        """Returns all column names as a list.

        The returned column names are consistent with the Snowflake database object `identifier syntax <https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html>`_.

        ==================================   ==========================
        Column name used to create a table   Column name returned in str
        ==================================   ==========================
        a                                    'A'
        A                                    'A'
        "a"                                  '"a"'
        "a b"                                '"a b"'
        "a""b"                               '"a""b"'
        ==================================   ==========================
        """
        return self.schema.names

    @publicapi
    def col(self, col_name: str, _emit_ast: bool = True) -> Column:
        """Returns a reference to a column in the DataFrame."""
        expr = None
        if _emit_ast:
            expr = proto.Expr()
            col_expr_ast = with_src_position(expr.dataframe_col)
            self._set_ast_ref(col_expr_ast.df)
            col_expr_ast.col_name = col_name
        if col_name == "*":
            return Column(Star(self._output), _ast=expr)
        else:
            return Column(self._resolve(col_name), _ast=expr)

    @df_api_usage
    @publicapi
    def select(
        self,
        *cols: Union[
            Union[ColumnOrName, TableFunctionCall],
            Iterable[Union[ColumnOrName, TableFunctionCall]],
        ],
        _ast_stmt: Optional[proto.Bind] = None,
        _emit_ast: bool = True,
    ) -> "DataFrame":
        """Returns a new DataFrame with the specified Column expressions as output
        (similar to SELECT in SQL). Only the Columns specified as arguments will be
        present in the resulting DataFrame.

        You can use any :class:`Column` expression or strings for named columns.

        Example 1::
            >>> df = session.create_dataframe([[1, "some string value", 3, 4]], schema=["col1", "col2", "col3", "col4"])
            >>> df_selected = df.select(col("col1"), col("col2").substr(0, 10), df["col3"] + df["col4"])

        Example 2::

            >>> df_selected = df.select("col1", "col2", "col3")

        Example 3::

            >>> df_selected = df.select(["col1", "col2", "col3"])

        Example 4::

            >>> df_selected = df.select(df["col1"], df.col2, df.col("col3"))

        Example 5::

            >>> from snowflake.snowpark.functions import table_function
            >>> split_to_table = table_function("split_to_table")
            >>> df.select(df.col1, split_to_table(df.col2, lit(" ")), df.col("col3")).show()
            -----------------------------------------------
            |"COL1"  |"SEQ"  |"INDEX"  |"VALUE"  |"COL3"  |
            -----------------------------------------------
            |1       |1      |1        |some     |3       |
            |1       |1      |2        |string   |3       |
            |1       |1      |3        |value    |3       |
            -----------------------------------------------
            <BLANKLINE>

        Note:
            A `TableFunctionCall` can be added in `select` when the dataframe results from another join. This is possible because we know
            the hierarchy in which the joins are applied.

        Args:
            *cols: A :class:`Column`, :class:`str`, :class:`table_function.TableFunctionCall`, or a list of those. Note that at most one
                   :class:`table_function.TableFunctionCall` object is supported within a select call.
            _ast_stmt: when invoked internally, supplies the AST to use for the resulting dataframe.
            _emit_ast: Whether to emit AST statements.
        """
        exprs, is_variadic = parse_positional_args_to_list_variadic(*cols)
        if not exprs:
            raise ValueError("The input of select() cannot be empty")

        names = []
        table_func = None
        table_func_col_names = None
        string_col_names = []
        join_plan = None

        ast_cols = []

        for e in exprs:
            if isinstance(e, Column):
                if self._all_variant_cols:
                    names.append(
                        Column(
                            e._expr1, e._expr2, e._ast, _is_qualified_name=True
                        )._named()
                    )
                else:
                    names.append(e._named())
                if isinstance(e._expression, (Attribute, UnresolvedAttribute)):
                    string_col_names.append(e._expression.name)
                if _emit_ast and _ast_stmt is None:
                    ast_cols.append(e._ast)

            elif isinstance(e, str):
                col_expr_ast = None
                if _emit_ast and _ast_stmt is None:
                    col_expr_ast = proto.Expr()
                    fill_ast_for_column(col_expr_ast, e, None)
                    ast_cols.append(col_expr_ast)

                col = Column(
                    e, _ast=col_expr_ast, _is_qualified_name=self._all_variant_cols
                )
                names.append(col._named())
                string_col_names.append(e)

            elif isinstance(e, TableFunctionCall):
                if table_func:
                    raise ValueError(
                        f"At most one table function can be called inside a select(). "
                        f"Called '{table_func.user_visible_name}' and '{e.user_visible_name}'."
                    )
                table_func = e
                if _emit_ast and _ast_stmt is None:
                    add_intermediate_stmt(self._session._ast_batch, table_func)
                    ast_col = proto.Expr()
                    build_indirect_table_fn_apply(ast_col, table_func)
                    ast_cols.append(ast_col)

                func_expr = _create_table_function_expression(func=table_func)

                if isinstance(e, _ExplodeFunctionCall):
                    new_cols, alias_cols = _get_cols_after_explode_join(
                        e, self._plan, self._select_statement
                    )
                else:
                    # this join plan is created here to extract output columns after the join. If a better way
                    # to extract this information is found, please update this function.
                    temp_join_plan = self._session._analyzer.resolve(
                        TableFunctionJoin(self._plan, func_expr)
                    )
                    _, new_cols, alias_cols = _get_cols_after_join_table(
                        func_expr, self._plan, temp_join_plan
                    )

                # when generating join table expression, we inculcate aliased column into the initial
                # query like so,
                #
                #     SELECT T_LEFT.*, T_RIGHT."COL1" AS "COL1_ALIASED", ... FROM () AS T_LEFT JOIN TABLE() AS T_RIGHT
                #
                # Therefore if columns names are aliased, then subsequent select must use the aliased name.
                names.extend(alias_cols or new_cols)
                table_func_col_names = [
                    self._session._analyzer.analyze(col, {}) for col in new_cols
                ]
            else:
                raise TypeError(
                    "The input of select() must be Column, column name, TableFunctionCall, or a list of them"
                )

        if table_func is not None:
            """
            When the select statement contains a table function, and all columns are strings, we can generate
            a better SQL query that does not have any collisions.
                SELECT T_LEFT.*, T_RIGHT."COL1" AS "COL1_ALIASED", ... FROM () AS T_LEFT JOIN TABLE() AS T_RIGHT

            Case 1:
                df.select(table_function(...))

            This is a special case when dataframe.select only selects the output of table
            function join, we set left_cols = []. This is done in-order to handle the
            overlapping column case of DF and table function output with no aliases.
            This generates a sql like so:
                SELECT T_RIGHT."COL1" FROM () AS T_LEFT JOIN TABLE() AS T_RIGHT

            Case 2:
                df.select("col1", "col2", table_function(...))

            In this case, all columns are strings except for the table function. This is a simpler case
            where generating the join plan like below is simple.
                SELECT T_LEFT."COL1", T_LEFT."COL2", T_RIGHT."COL1" AS "COL1_ALIASED", ... FROM () AS T_LEFT JOIN TABLE() AS T_RIGHT

            Case 3:
                df.select(col("col1"), col("col2").cast(IntegerType()), table_function(...))

            In this case, the ideal SQL generation would be
                SELECT T_LEFT."COL1", CAST(T_LEFT."COL2" AS INTEGER), T_RIGHT."COL1" AS "COL1_ALIASED", ... FROM () AS T_LEFT JOIN TABLE() AS T_RIGHT
            However, this is not possible with the current SQL generation so we generate the join plan like below.
                SELECT T_LEFT.*, T_RIGHT."COL1" AS "COL1_ALIASED", ... FROM () AS T_LEFT JOIN TABLE() AS T_RIGHT
            """
            if len(string_col_names) + 1 == len(exprs):
                # This covers both Case 1 and Case 2.
                left_cols = string_col_names
            else:
                left_cols = ["*"]
            join_plan = self._session._analyzer.resolve(
                TableFunctionJoin(
                    self._plan,
                    func_expr,
                    left_cols=left_cols,
                    right_cols=table_func_col_names,
                )
            )

        # AST.
        stmt = _ast_stmt
        ast = None

        # Note it's intentional the column expressions are AST serialized earlier (ast_cols) to ensure any
        # AST IDs created preceed the AST ID of the select statement so they are deserialized in dependent order.
        if _emit_ast and _ast_stmt is None:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_select, stmt)
            self._set_ast_ref(ast.df)
            ast.cols.variadic = is_variadic
            ast.expr_variant = False

            # Add columns after the statement to ensure any dependent columns have lower ast id.
            for ast_col in ast_cols:
                if ast_col is not None:
                    ast.cols.args.append(ast_col)

        if self._select_statement:
            if join_plan:
                return self._with_plan(
                    self._session._analyzer.create_select_statement(
                        from_=self._session._analyzer.create_select_snowflake_plan(
                            join_plan, analyzer=self._session._analyzer
                        ),
                        analyzer=self._session._analyzer,
                    ).select(names),
                    _ast_stmt=stmt,
                )

            return self._with_plan(self._select_statement.select(names), _ast_stmt=stmt)

        return self._with_plan(Project(names, join_plan or self._plan), _ast_stmt=stmt)

    @df_api_usage
    @publicapi
    def select_expr(
        self,
        *exprs: Union[str, Iterable[str]],
        _ast_stmt: proto.Bind = None,
        _emit_ast: bool = True,
    ) -> "DataFrame":
        """
        Projects a set of SQL expressions and returns a new :class:`DataFrame`.
        This method is equivalent to ``select(sql_expr(...))`` with :func:`select`
        and :func:`functions.sql_expr`.

        :func:`selectExpr` is an alias of :func:`select_expr`.

        Args:
            exprs: The SQL expressions.

        Examples::

            >>> df = session.create_dataframe([-1, 2, 3], schema=["a"])  # with one pair of [], the dataframe has a single column and 3 rows.
            >>> df.select_expr("abs(a)", "a + 2", "cast(a as string)").show()
            --------------------------------------------
            |"ABS(A)"  |"A + 2"  |"CAST(A AS STRING)"  |
            --------------------------------------------
            |1         |1        |-1                   |
            |2         |4        |2                    |
            |3         |5        |3                    |
            --------------------------------------------
            <BLANKLINE>

        """
        exprs, is_variadic = parse_positional_args_to_list_variadic(*exprs)
        if not exprs:
            raise ValueError("The input of select_expr() cannot be empty")

        # AST.
        stmt = None
        if _emit_ast:
            if _ast_stmt is None:
                stmt = self._session._ast_batch.bind()
                ast = with_src_position(stmt.expr.dataframe_select, stmt)
                self._set_ast_ref(ast.df)
                ast.cols.variadic = is_variadic
                ast.expr_variant = True
                for expr in exprs:
                    build_expr_from_python_val(ast.cols.args.add(), expr)
            else:
                stmt = _ast_stmt

        return self.select(
            [
                sql_expr(expr, _emit_ast=False)
                for expr in parse_positional_args_to_list(*exprs)
            ],
            _ast_stmt=stmt,
        )

    selectExpr = select_expr

    @publicapi
    def drop(
        self, *cols: Union[ColumnOrName, Iterable[ColumnOrName]], _emit_ast: bool = True
    ) -> "DataFrame":
        """Returns a new DataFrame that excludes the columns with the specified names
        from the output.

        This is functionally equivalent to calling :func:`select()` and passing in all
        columns except the ones to exclude. This is a no-op if schema does not contain
        the given column name(s).

        Example::

            >>> df = session.create_dataframe([[1, 2, 3]], schema=["a", "b", "c"])
            >>> df.drop("a", "b").show()
            -------
            |"C"  |
            -------
            |3    |
            -------
            <BLANKLINE>

        Args:
            *cols: the columns to exclude, as :class:`str`, :class:`Column` or a list
                of those.

        Raises:
            :class:`SnowparkClientException`: if the resulting :class:`DataFrame`
                contains no output columns.
        """
        # An empty list of columns should be accepted as dropping nothing
        if not cols:
            raise ValueError("The input of drop() cannot be empty")
        exprs, is_variadic = parse_positional_args_to_list_variadic(*cols)

        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_drop, stmt)
            self._set_ast_ref(ast.df)
            for c in exprs:
                build_expr_from_snowpark_column_or_col_name(ast.cols.args.add(), c)
            ast.cols.variadic = is_variadic

        with ResourceUsageCollector() as resource_usage_collector:
            names = []
            for c in exprs:
                if isinstance(c, str):
                    names.append(c)
                elif isinstance(c, Column) and isinstance(c._expression, Attribute):
                    from snowflake.snowpark.mock._connection import MockServerConnection

                    if isinstance(self._session._conn, MockServerConnection):
                        self.schema  # to execute the plan and populate expr_to_alias

                    names.append(
                        self._plan.expr_to_alias.get(
                            c._expression.expr_id, c._expression.name
                        )
                    )
                elif (
                    isinstance(c, Column)
                    and isinstance(c._expression, UnresolvedAttribute)
                    and c._expression.df_alias
                ):
                    names.append(
                        self._plan.df_aliased_col_name_to_real_col_name.get(
                            c._expression.name, c._expression.name
                        )
                    )
                elif isinstance(c, Column) and isinstance(
                    c._expression, NamedExpression
                ):
                    names.append(c._expression.name)
                else:
                    raise SnowparkClientExceptionMessages.DF_CANNOT_DROP_COLUMN_NAME(
                        str(c)
                    )

            normalized_names = {quote_name(n) for n in names}
            existing_names = [attr.name for attr in self._output]
            keep_col_names = [c for c in existing_names if c not in normalized_names]
            if not keep_col_names:
                raise SnowparkClientExceptionMessages.DF_CANNOT_DROP_ALL_COLUMNS()

            if self._select_statement and self._session.conf.get(
                "use_simplified_query_generation"
            ):
                # Only drop the columns that exist in the DataFrame.
                drop_normalized_names = [
                    name for name in normalized_names if name in existing_names
                ]
                if not drop_normalized_names:
                    df = self._with_plan(self._select_statement)
                else:
                    df = self._with_plan(
                        self._select_statement.exclude(
                            drop_normalized_names, keep_col_names
                        )
                    )
            else:
                df = self.select(list(keep_col_names), _emit_ast=False)

        if self._session.conf.get("use_simplified_query_generation"):
            add_api_call(
                df,
                "DataFrame.drop[exclude]",
                resource_usage_collector.get_resource_usage(),
            )
        else:
            adjust_api_subcalls(
                df,
                "DataFrame.drop[select]",
                len_subcalls=1,
                resource_usage=resource_usage_collector.get_resource_usage(),
            )

        if _emit_ast:
            df._ast_id = stmt.uid

        return df

    @df_api_usage
    @publicapi
    def col_ilike(
        self,
        pattern: str,
        _ast_stmt: proto.Bind = None,
        _emit_ast: bool = True,
    ) -> "DataFrame":
        """Returns a new DataFrame with only the columns whose names match the specified
        pattern using case-insensitive ILIKE matching (similar to SELECT * ILIKE 'pattern' in SQL).

        Args:
            pattern: The ILIKE pattern to match column names against. You can use the following wildcards:
                - Use an underscore (_) to match any single character.
                - Use a percent sign (%) to match any sequence of zero or more characters.
                - To match a sequence anywhere within the column name, begin and end the pattern with %.

        Returns:
            DataFrame: A new DataFrame containing only columns matching the pattern.

        Raises:
            ValueError: If SQL simplifier is not enabled.
            SnowparkSQLException: If no columns match the specified pattern.

        Examples::

            >>> # Select all columns containing 'id' (case-insensitive)
            >>> df = session.create_dataframe([[1, "John", 101], [2, "Jane", 102]],
            ...                                 schema=["USER_ID", "Name", "dept_id"])
            >>> df.col_ilike("%id%").show()
            -------------------------
            |"USER_ID"  |"DEPT_ID"  |
            -------------------------
            |1          |101        |
            |2          |102        |
            -------------------------
            <BLANKLINE>
        """
        # AST.
        stmt = None
        if _emit_ast:
            if _ast_stmt is None:
                stmt = self._session._ast_batch.bind()
                ast = with_src_position(stmt.expr.dataframe_col_ilike, stmt)
                ast.pattern = pattern
                self._set_ast_ref(ast.df)
            else:
                stmt = _ast_stmt

        if self._select_statement:  # sql simplifier is enabled
            df = self._with_plan(self._select_statement.ilike(pattern), _ast_stmt=stmt)
        else:
            df = self._with_plan(
                Project([], self._plan, ilike_pattern=pattern), _ast_stmt=stmt
            )

        add_api_call(df, "DataFrame.col_ilike")
        return df

    @df_api_usage
    @publicapi
    def filter(
        self,
        expr: ColumnOrSqlExpr,
        _ast_stmt: proto.Bind = None,
        _emit_ast: bool = True,
    ) -> "DataFrame":
        """Filters rows based on the specified conditional expression (similar to WHERE
        in SQL).

        Examples::

            >>> df = session.create_dataframe([[1, 2], [3, 4]], schema=["A", "B"])
            >>> df_filtered = df.filter((col("A") > 1) & (col("B") < 100))  # Must use parenthesis before and after operator &.

            >>> # The following two result in the same SQL query:
            >>> df.filter(col("a") > 1).collect()
            [Row(A=3, B=4)]
            >>> df.filter("a > 1").collect()  # use SQL expression
            [Row(A=3, B=4)]

        Args:
            expr: a :class:`Column` expression or SQL text.
            _ast_stmt: when invoked internally, supplies the AST to use for the resulting dataframe.

        :meth:`where` is an alias of :meth:`filter`.
        """

        # This code performs additional type checks, run first.
        filter_col_expr = _to_col_if_sql_expr(expr, "filter/where")._expression

        # AST.
        stmt = None
        if _emit_ast:
            if _ast_stmt is None:
                stmt = self._session._ast_batch.bind()
                ast = with_src_position(stmt.expr.dataframe_filter, stmt)
                self._set_ast_ref(ast.df)
                build_expr_from_snowpark_column_or_sql_str(ast.condition, expr)
            else:
                stmt = _ast_stmt

        # In snowpark_connect_compatible mode, we need to handle
        # the filtering for dataframe after aggregation without nesting using HAVING
        if (
            context._is_snowpark_connect_compatible_mode
            and self._ops_after_agg is not None
            and "filter" not in self._ops_after_agg
        ):
            having_plan = Filter(filter_col_expr, self._plan, is_having=True)
            if self._select_statement:
                df = self._with_plan(
                    self._session._analyzer.create_select_statement(
                        from_=self._session._analyzer.create_select_snowflake_plan(
                            having_plan, analyzer=self._session._analyzer
                        ),
                        analyzer=self._session._analyzer,
                    ),
                    _ast_stmt=stmt,
                )
            else:
                df = self._with_plan(having_plan, _ast_stmt=stmt)
            df._ops_after_agg = self._ops_after_agg.copy()
            df._ops_after_agg.add("filter")
            return df
        else:
            if self._select_statement:
                return self._with_plan(
                    self._select_statement.filter(filter_col_expr),
                    _ast_stmt=stmt,
                )
            return self._with_plan(
                Filter(
                    filter_col_expr,
                    self._plan,
                    is_having=False,
                ),
                _ast_stmt=stmt,
            )

    @df_api_usage
    @publicapi
    def sort(
        self,
        *cols: Union[ColumnOrName, Iterable[ColumnOrName]],
        ascending: Optional[Union[bool, int, List[Union[bool, int]]]] = None,
        _emit_ast: bool = True,
    ) -> "DataFrame":
        """Sorts a DataFrame by the specified expressions (similar to ORDER BY in SQL).

        Examples::

            >>> from snowflake.snowpark.functions import col

            >>> df = session.create_dataframe([[1, 2], [3, 4], [1, 4]], schema=["A", "B"])
            >>> df.sort(col("A"), col("B").asc()).show()
            -------------
            |"A"  |"B"  |
            -------------
            |1    |2    |
            |1    |4    |
            |3    |4    |
            -------------
            <BLANKLINE>

            >>> df.sort(col("a"), ascending=False).show()
            -------------
            |"A"  |"B"  |
            -------------
            |3    |4    |
            |1    |2    |
            |1    |4    |
            -------------
            <BLANKLINE>

            >>> # The values from the list overwrite the column ordering.
            >>> df.sort(["a", col("b").desc()], ascending=[1, 1]).show()
            -------------
            |"A"  |"B"  |
            -------------
            |1    |2    |
            |1    |4    |
            |3    |4    |
            -------------
            <BLANKLINE>

        Args:
            *cols: A column name as :class:`str` or :class:`Column`, or a list of
             columns to sort by.
            ascending: A :class:`bool` or a list of :class:`bool` for sorting the
             DataFrame, where ``True`` sorts a column in ascending order and ``False``
             sorts a column in descending order . If you specify a list of multiple
             sort orders, the length of the list must equal the number of columns.
        """
        if not cols:
            raise ValueError("sort() needs at least one sort expression.")
        # This code performs additional type checks, run first.
        exprs = self._convert_cols_to_exprs("sort()", *cols)
        if not exprs:
            raise ValueError("sort() needs at least one sort expression.")

        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            # Parsing args separately since the original column expr or string
            # needs to be recorded.
            _cols, is_variadic = parse_positional_args_to_list_variadic(*cols)
            ast = with_src_position(stmt.expr.dataframe_sort, stmt)
            for c in _cols:
                build_expr_from_snowpark_column_or_col_name(ast.cols.args.add(), c)
            ast.cols.variadic = is_variadic
            self._set_ast_ref(ast.df)

        orders = []
        # `ascending` is represented by Expr in the AST.
        # Therefore, construct the required bool, int, or list and copy from that.
        asc_expr_ast = None
        if _emit_ast:
            asc_expr_ast = proto.Expr()
        if ascending is not None:
            if isinstance(ascending, (list, tuple)):
                orders = [Ascending() if asc else Descending() for asc in ascending]
                if _emit_ast:
                    # Here asc_expr_ast is a list of bools and ints.
                    for asc in ascending:
                        asc_ast = proto.Expr()
                        if isinstance(asc, bool):
                            asc_ast.bool_val.v = asc
                        else:
                            asc_ast.int64_val.v = asc
                        asc_expr_ast.list_val.vs.append(asc_ast)
            elif isinstance(ascending, (bool, int)):
                orders = [Ascending() if ascending else Descending()]
                if _emit_ast:
                    # Here asc_expr_ast is either a bool or an int.
                    if isinstance(ascending, bool):
                        asc_expr_ast.bool_val.v = ascending
                    else:
                        asc_expr_ast.int64_val.v = ascending
            else:
                raise TypeError(
                    "ascending can only be boolean or list,"
                    " but got {}".format(str(type(ascending)))
                )
            if _emit_ast:
                ast.ascending.CopyFrom(asc_expr_ast)
            if len(exprs) != len(orders):
                raise ValueError(
                    "The length of col ({}) should be same with"
                    " the length of ascending ({}).".format(len(exprs), len(orders))
                )

        sort_exprs = []
        for idx in range(len(exprs)):
            # orders will overwrite current orders in expression (but will not overwrite null ordering)
            # if no order is provided, use ascending order
            if isinstance(exprs[idx], SortOrder):
                sort_exprs.append(
                    SortOrder(exprs[idx].child, orders[idx], exprs[idx].null_ordering)
                    if orders
                    else exprs[idx]
                )
            else:
                sort_exprs.append(
                    SortOrder(exprs[idx], orders[idx] if orders else Ascending())
                )

        # In snowpark_connect_compatible mode, we need to handle
        # the sorting for dataframe after aggregation without nesting
        if (
            context._is_snowpark_connect_compatible_mode
            and self._ops_after_agg is not None
            and "sort" not in self._ops_after_agg
        ):
            sort_plan = Sort(sort_exprs, self._plan, is_order_by_append=True)
            if self._select_statement:
                df = self._with_plan(
                    self._session._analyzer.create_select_statement(
                        from_=self._session._analyzer.create_select_snowflake_plan(
                            sort_plan, analyzer=self._session._analyzer
                        ),
                        analyzer=self._session._analyzer,
                    ),
                    _ast_stmt=stmt,
                )
            else:
                df = self._with_plan(sort_plan, _ast_stmt=stmt)
            df._ops_after_agg = self._ops_after_agg.copy()
            df._ops_after_agg.add("sort")
            return df
        else:
            df = (
                self._with_plan(self._select_statement.sort(sort_exprs))
                if self._select_statement
                else self._with_plan(
                    Sort(sort_exprs, self._plan, is_order_by_append=False)
                )
            )

            if _emit_ast:
                df._ast_id = stmt.uid

            return df

    @experimental(version="1.5.0")
    @publicapi
    def alias(self, name: str, _emit_ast: bool = True):
        """Returns an aliased dataframe in which the columns can now be referenced to using `col(<df alias>, <column name>)`.

        Examples::
            >>> from snowflake.snowpark.functions import col
            >>> df1 = session.create_dataframe([[1, 6], [3, 8], [7, 7]], schema=["col1", "col2"])
            >>> df2 = session.create_dataframe([[1, 2], [3, 4], [5, 5]], schema=["col1", "col2"])

            Join two dataframes with duplicate column names
            >>> df1.alias("L").join(df2.alias("R"), col("L", "col1") == col("R", "col1")).select(col("L", "col1"), col("R", "col2")).show()
            ---------------------
            |"COL1L"  |"COL2R"  |
            ---------------------
            |1        |2        |
            |3        |4        |
            ---------------------
            <BLANKLINE>

            Self join:
            >>> df1.alias("L").join(df1.alias("R"), on="col1").select(col("L", "col1"), col("R", "col2")).show()
            --------------------
            |"COL1"  |"COL2R"  |
            --------------------
            |1       |6        |
            |3       |8        |
            |7       |7        |
            --------------------
            <BLANKLINE>

        Args:
            name: The alias as :class:`str`.
        """

        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_alias, stmt)
            ast.name = name
            self._set_ast_ref(ast.df)

        _copy = self._copy_without_ast()
        _copy._alias = name
        for attr in self._plan.attributes:
            if _copy._select_statement:
                _copy._select_statement.df_aliased_col_name_to_real_col_name[name][
                    attr.name
                ] = attr.name  # attr is quoted already
            _copy._plan.df_aliased_col_name_to_real_col_name[name][
                attr.name
            ] = attr.name

        if _emit_ast:
            _copy._ast_id = stmt.uid
        return _copy

    @df_api_usage
    @publicapi
    def agg(
        self,
        *exprs: Union[Column, Tuple[ColumnOrName, str], Dict[str, str]],
        _emit_ast: bool = True,
    ) -> "DataFrame":
        """Aggregate the data in the DataFrame. Use this method if you don't need to
        group the data (:func:`group_by`).

        Args:
            exprs: A variable length arguments list where every element is

                - A Column object
                - A tuple where the first element is a column object or a column name and the second element is the name of the aggregate function
                - A list of the above

                or a ``dict`` maps column names to aggregate function names.

        Examples::

            >>> from snowflake.snowpark.functions import col, stddev, stddev_pop

            >>> df = session.create_dataframe([[1, 2], [3, 4], [1, 4]], schema=["A", "B"])
            >>> df.agg(stddev(col("a"))).show()
            ----------------------
            |"STDDEV(A)"         |
            ----------------------
            |1.1547003940416753  |
            ----------------------
            <BLANKLINE>

            >>> df.agg(stddev(col("a")), stddev_pop(col("a"))).show()
            -------------------------------------------
            |"STDDEV(A)"         |"STDDEV_POP(A)"     |
            -------------------------------------------
            |1.1547003940416753  |0.9428091005076267  |
            -------------------------------------------
            <BLANKLINE>

            >>> df.agg(("a", "min"), ("b", "max")).show()
            -----------------------
            |"MIN(A)"  |"MAX(B)"  |
            -----------------------
            |1         |4         |
            -----------------------
            <BLANKLINE>

            >>> df.agg({"a": "count", "b": "sum"}).show()
            -------------------------
            |"COUNT(A)"  |"SUM(B)"  |
            -------------------------
            |3           |10        |
            -------------------------
            <BLANKLINE>

        Note:
            The name of the aggregate function to compute must be a valid Snowflake `aggregate function
            <https://docs.snowflake.com/en/sql-reference/functions-aggregation.html>`_.

        See also:
            - :meth:`RelationalGroupedDataFrame.agg`
            - :meth:`DataFrame.group_by`
        """

        check_agg_exprs(exprs)

        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            expr = with_src_position(stmt.expr.dataframe_agg, stmt)
            exprs, is_variadic = parse_positional_args_to_list_variadic(*exprs)
            for e in exprs:
                build_expr_from_python_val(expr.exprs.args.add(), e)
            expr.exprs.variadic = is_variadic
            self._set_ast_ref(expr.df)

        df = self.group_by(_emit_ast=False).agg(*exprs, _emit_ast=False)

        if _emit_ast:
            df._ast_id = stmt.uid

        return df

    @df_to_relational_group_df_api_usage
    @publicapi
    def rollup(
        self, *cols: Union[ColumnOrName, Iterable[ColumnOrName]], _emit_ast: bool = True
    ) -> "snowflake.snowpark.RelationalGroupedDataFrame":
        """Performs a SQL
        `GROUP BY ROLLUP <https://docs.snowflake.com/en/sql-reference/constructs/group-by-rollup.html>`_.
        on the DataFrame.

        Args:
            cols: The columns to group by rollup.
        """

        # This code performs additional type checks, run first.
        rollup_exprs = self._convert_cols_to_exprs("rollup()", *cols)

        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            expr = with_src_position(stmt.expr.dataframe_rollup, stmt)
            self._set_ast_ref(expr.df)
            col_list, expr.cols.variadic = parse_positional_args_to_list_variadic(*cols)
            for c in col_list:
                build_expr_from_snowpark_column_or_col_name(expr.cols.args.add(), c)

        return snowflake.snowpark.RelationalGroupedDataFrame(
            self,
            rollup_exprs,
            snowflake.snowpark.relational_grouped_dataframe._RollupType(),
            _ast_stmt=stmt,
        )

    @df_to_relational_group_df_api_usage
    @publicapi
    def group_by(
        self,
        *cols: Union[ColumnOrName, Iterable[ColumnOrName]],
        _ast_stmt: Optional[proto.Bind] = None,
        _emit_ast: bool = True,
    ) -> "snowflake.snowpark.RelationalGroupedDataFrame":
        """Groups rows by the columns specified by expressions (similar to GROUP BY in
        SQL).

        This method returns a :class:`RelationalGroupedDataFrame` that you can use to
        perform aggregations on each group of data.

        Args:
            *cols: The columns to group by.

        Valid inputs are:

            - Empty input
            - One or multiple :class:`Column` object(s) or column name(s) (:class:`str`)
            - A list of :class:`Column` objects or column names (:class:`str`)

        Examples:

            >>> from snowflake.snowpark.functions import col, lit, sum as sum_, max as max_
            >>> df = session.create_dataframe([(1, 1),(1, 2),(2, 1),(2, 2),(3, 1),(3, 2)], schema=["a", "b"])
            >>> df.group_by().agg(sum_("b")).collect()
            [Row(SUM(B)=9)]
            >>> df.group_by("a").agg(sum_("b")).sort("a").collect()
            [Row(A=1, SUM(B)=3), Row(A=2, SUM(B)=3), Row(A=3, SUM(B)=3)]
            >>> df.group_by("a").agg(sum_("b").alias("sum_b"), max_("b").alias("max_b")).sort("a").collect()
            [Row(A=1, SUM_B=3, MAX_B=2), Row(A=2, SUM_B=3, MAX_B=2), Row(A=3, SUM_B=3, MAX_B=2)]
            >>> df.group_by(["a", lit("snow")]).agg(sum_("b")).sort("a").collect()
            [Row(A=1, LITERAL()='snow', SUM(B)=3), Row(A=2, LITERAL()='snow', SUM(B)=3), Row(A=3, LITERAL()='snow', SUM(B)=3)]
            >>> df.group_by("a").agg((col("*"), "count"), max_("b")).sort("a").collect()
            [Row(A=1, COUNT(LITERAL())=2, MAX(B)=2), Row(A=2, COUNT(LITERAL())=2, MAX(B)=2), Row(A=3, COUNT(LITERAL())=2, MAX(B)=2)]
            >>> df.group_by("a").median("b").sort("a").collect()
            [Row(A=1, MEDIAN(B)=Decimal('1.500')), Row(A=2, MEDIAN(B)=Decimal('1.500')), Row(A=3, MEDIAN(B)=Decimal('1.500'))]
            >>> df.group_by("a").function("avg")("b").sort("a").collect()
            [Row(A=1, AVG(B)=Decimal('1.500000')), Row(A=2, AVG(B)=Decimal('1.500000')), Row(A=3, AVG(B)=Decimal('1.500000'))]
        """
        # This code performs additional type checks, run first.
        grouping_exprs = self._convert_cols_to_exprs("group_by()", *cols)

        # AST.
        stmt = None
        if _emit_ast:
            if _ast_stmt is None:
                stmt = self._session._ast_batch.bind()
                expr = with_src_position(stmt.expr.dataframe_group_by, stmt)
                col_list, expr.cols.variadic = parse_positional_args_to_list_variadic(
                    *cols
                )
                for c in col_list:
                    build_expr_from_snowpark_column_or_col_name(expr.cols.args.add(), c)

                self._set_ast_ref(expr.df)
            else:
                stmt = _ast_stmt

        df = snowflake.snowpark.RelationalGroupedDataFrame(
            self,
            grouping_exprs,
            snowflake.snowpark.relational_grouped_dataframe._GroupByType(),
            _ast_stmt=stmt,
        )

        if _emit_ast:
            df._ast_id = stmt.uid

        return df

    @df_to_relational_group_df_api_usage
    @publicapi
    def group_by_grouping_sets(
        self,
        *grouping_sets: Union[
            "snowflake.snowpark.GroupingSets",
            Iterable["snowflake.snowpark.GroupingSets"],
        ],
        _emit_ast: bool = True,
    ) -> "snowflake.snowpark.RelationalGroupedDataFrame":
        """Performs a SQL
        `GROUP BY GROUPING SETS <https://docs.snowflake.com/en/sql-reference/constructs/group-by-grouping-sets.html>`_.
        on the DataFrame.

        GROUP BY GROUPING SETS is an extension of the GROUP BY clause
        that allows computing multiple GROUP BY clauses in a single statement.
        The group set is a set of dimension columns.

        GROUP BY GROUPING SETS is equivalent to the UNION of two or
        more GROUP BY operations in the same result set.


        Examples::

            >>> from snowflake.snowpark import GroupingSets
            >>> df = session.create_dataframe([[1, 2, 10], [3, 4, 20], [1, 4, 30]], schema=["A", "B", "C"])
            >>> df.group_by_grouping_sets(GroupingSets([col("a")])).count().sort("a").collect()
            [Row(A=1, COUNT=2), Row(A=3, COUNT=1)]
            >>> df.group_by_grouping_sets(GroupingSets(col("a"))).count().sort("a").collect()
            [Row(A=1, COUNT=2), Row(A=3, COUNT=1)]
            >>> df.group_by_grouping_sets(GroupingSets([col("a")], [col("b")])).count().sort("a", "b").collect()
            [Row(A=None, B=2, COUNT=1), Row(A=None, B=4, COUNT=2), Row(A=1, B=None, COUNT=2), Row(A=3, B=None, COUNT=1)]
            >>> df.group_by_grouping_sets(GroupingSets([col("a"), col("b")], [col("c")])).count().sort("a", "b", "c").collect()
            [Row(A=None, B=None, C=10, COUNT=1), Row(A=None, B=None, C=20, COUNT=1), Row(A=None, B=None, C=30, COUNT=1), Row(A=1, B=2, C=None, COUNT=1), Row(A=1, B=4, C=None, COUNT=1), Row(A=3, B=4, C=None, COUNT=1)]


        Args:
            grouping_sets: The list of :class:`GroupingSets` to group by.
        """

        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            expr = with_src_position(stmt.expr.dataframe_group_by_grouping_sets, stmt)
            self._set_ast_ref(expr.df)
            (
                grouping_set_list,
                expr.grouping_sets.variadic,
            ) = parse_positional_args_to_list_variadic(*grouping_sets)
            for gs in grouping_set_list:
                expr.grouping_sets.args.append(gs._ast)

        return snowflake.snowpark.RelationalGroupedDataFrame(
            self,
            [gs._to_expression for gs in parse_positional_args_to_list(*grouping_sets)],
            snowflake.snowpark.relational_grouped_dataframe._GroupByType(),
            _ast_stmt=stmt,
        )

    @df_to_relational_group_df_api_usage
    @publicapi
    def cube(
        self, *cols: Union[ColumnOrName, Iterable[ColumnOrName]], _emit_ast: bool = True
    ) -> "snowflake.snowpark.RelationalGroupedDataFrame":
        """Performs a SQL
        `GROUP BY CUBE <https://docs.snowflake.com/en/sql-reference/constructs/group-by-cube.html>`_.
        on the DataFrame.

        Args:
            cols: The columns to group by cube.
        """

        # This code performs additional type checks, run first.
        cube_exprs = self._convert_cols_to_exprs("cube()", *cols)

        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            expr = with_src_position(stmt.expr.dataframe_cube, stmt)
            self._set_ast_ref(expr.df)
            col_list, expr.cols.variadic = parse_positional_args_to_list_variadic(*cols)
            for c in col_list:
                build_expr_from_snowpark_column_or_col_name(expr.cols.args.add(), c)

        return snowflake.snowpark.RelationalGroupedDataFrame(
            self,
            cube_exprs,
            snowflake.snowpark.relational_grouped_dataframe._CubeType(),
            _ast_stmt=stmt,
        )

    @publicapi
    def distinct(
        self, _ast_stmt: proto.Bind = None, _emit_ast: bool = True
    ) -> "DataFrame":
        """Returns a new DataFrame that contains only the rows with distinct values
        from the current DataFrame.

        This is equivalent to performing a SELECT DISTINCT in SQL.
        """

        # AST.
        stmt = None
        if _emit_ast:
            if _ast_stmt is None:
                stmt = self._session._ast_batch.bind()
                ast = with_src_position(stmt.expr.dataframe_distinct, stmt)
                self._set_ast_ref(ast.df)
            else:
                stmt = _ast_stmt
                ast = None

        if self._session.conf.get("use_simplified_query_generation"):
            with ResourceUsageCollector() as resource_usage_collector:
                if self._select_statement:
                    df = self._with_plan(
                        self._select_statement.distinct(), _ast_stmt=stmt
                    )
                else:
                    df = self._with_plan(Distinct(self._plan), _ast_stmt=stmt)
            add_api_call(
                df,
                "DataFrame.distinct[select]",
                resource_usage=resource_usage_collector.get_resource_usage(),
            )
        else:
            with ResourceUsageCollector() as resource_usage_collector:
                df = self.group_by(
                    [
                        self.col(quote_name(f.name), _emit_ast=False)
                        for f in self.schema.fields
                    ],
                    _emit_ast=False,
                ).agg(_emit_ast=False)
            adjust_api_subcalls(
                df,
                "DataFrame.distinct[group_by]",
                len_subcalls=2,
                resource_usage=resource_usage_collector.get_resource_usage(),
            )

        if _emit_ast:
            df._ast_id = stmt.uid

        return df

    @publicapi
    def drop_duplicates(
        self,
        *subset: Union[str, Iterable[str]],
        _ast_stmt: proto.Bind = None,
        _emit_ast: bool = True,
    ) -> "DataFrame":
        """Creates a new DataFrame by removing duplicated rows on given subset of columns.

        If no subset of columns is specified, this function is the same as the :meth:`distinct` function.
        The result is non-deterministic when removing duplicated rows from the subset of columns but not all columns.

        For example, if we have a DataFrame ``df``, which has columns ("a", "b", "c") and contains three rows ``(1, 1, 1), (1, 1, 2), (1, 2, 3)``,
        the result of ``df.dropDuplicates("a", "b")`` can be either
        ``(1, 1, 1), (1, 2, 3)``
        or
        ``(1, 1, 2), (1, 2, 3)``

        Args:
            subset: The column names on which duplicates are dropped.

        :meth:`dropDuplicates` is an alias of :meth:`drop_duplicates`.
        """
        subset, is_variadic = parse_positional_args_to_list_variadic(*subset)

        # AST.
        stmt = None
        if _emit_ast:
            if _ast_stmt is None:
                stmt = self._session._ast_batch.bind()
                ast = with_src_position(stmt.expr.dataframe_drop_duplicates, stmt)
                ast.cols.variadic = is_variadic
                for arg in subset:
                    build_expr_from_python_val(ast.cols.args.add(), arg)
                self._set_ast_ref(ast.df)
            else:
                stmt = _ast_stmt

        if not subset:
            df = self.distinct(_emit_ast=False)
            adjust_api_subcalls(df, "DataFrame.drop_duplicates", len_subcalls=1)

            if _emit_ast:
                df._ast_id = stmt.uid
            return df

        with ResourceUsageCollector() as resource_usage_collector:
            filter_cols = [self.col(x) for x in subset]
            output_cols = [self.col(col_name) for col_name in self.columns]
            rownum = row_number().over(
                snowflake.snowpark.Window.partition_by(*filter_cols).order_by(
                    *filter_cols
                )
            )
            rownum_name = generate_random_alphanumeric()
            df = (
                self.select(*output_cols, rownum.as_(rownum_name), _emit_ast=False)
                .where(col(rownum_name) == 1, _emit_ast=False)
                .select(output_cols, _emit_ast=False)
            )
        # Reformat the extra API calls
        adjust_api_subcalls(
            df,
            "DataFrame.drop_duplicates",
            len_subcalls=3,
            resource_usage=resource_usage_collector.get_resource_usage(),
        )

        if _emit_ast:
            df._ast_id = stmt.uid

        return df

    @df_to_relational_group_df_api_usage
    @publicapi
    def pivot(
        self,
        pivot_col: ColumnOrName,
        values: Optional[
            Union[Iterable[LiteralType], "snowflake.snowpark.DataFrame"]
        ] = None,
        default_on_null: Optional[LiteralType] = None,
        _emit_ast: bool = True,
    ) -> "snowflake.snowpark.RelationalGroupedDataFrame":
        """Rotates this DataFrame by turning the unique values from one column in the input
        expression into multiple columns and aggregating results where required on any
        remaining column values.

        Only one aggregate is supported with pivot.

        Example::

            >>> create_result = session.sql('''create or replace temp table monthly_sales(empid int, amount int, month text)
            ... as select * from values
            ... (1, 10000, 'JAN'),
            ... (1, 400, 'JAN'),
            ... (2, 4500, 'JAN'),
            ... (2, 35000, 'JAN'),
            ... (1, 5000, 'FEB'),
            ... (1, 3000, 'FEB'),
            ... (2, 200, 'FEB') ''').collect()
            >>> df = session.table("monthly_sales")
            >>> df.pivot("month", ['JAN', 'FEB']).sum("amount").sort(df["empid"]).show()
            -------------------------------
            |"EMPID"  |"'JAN'"  |"'FEB'"  |
            -------------------------------
            |1        |10400    |8000     |
            |2        |39500    |200      |
            -------------------------------
            <BLANKLINE>

            >>> df = session.table("monthly_sales")
            >>> df.pivot("month").sum("amount").sort("empid").show()
            -------------------------------
            |"EMPID"  |"'FEB'"  |"'JAN'"  |
            -------------------------------
            |1        |8000     |10400    |
            |2        |200      |39500    |
            -------------------------------
            <BLANKLINE>

            >>> subquery_df = session.table("monthly_sales").select(col("month")).filter(col("month") == "JAN")
            >>> df = session.table("monthly_sales")
            >>> df.pivot("month", values=subquery_df).sum("amount").sort("empid").show()
            ---------------------
            |"EMPID"  |"'JAN'"  |
            ---------------------
            |1        |10400    |
            |2        |39500    |
            ---------------------
            <BLANKLINE>

        Args:
            pivot_col: The column or name of the column to use.
            values: A list of values in the column,
                or dynamic based on the DataFrame query,
                or None (default) will use all values of the pivot column.
            default_on_null: Expression to replace empty result values.
        """
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_pivot, stmt)
            self._set_ast_ref(ast.df)
            build_expr_from_snowpark_column_or_col_name(ast.pivot_col, pivot_col)
            build_expr_from_python_val(ast.values, values)
            build_expr_from_python_val(ast.default_on_null, default_on_null)

        target_df, pc, pivot_values, default_on_null = prepare_pivot_arguments(
            self, "DataFrame.pivot", pivot_col, values, default_on_null
        )

        return snowflake.snowpark.RelationalGroupedDataFrame(
            target_df,
            [],
            snowflake.snowpark.relational_grouped_dataframe._PivotType(
                pc[0], pivot_values, default_on_null
            ),
            _ast_stmt=stmt,
        )

    @df_api_usage
    @publicapi
    def unpivot(
        self,
        value_column: str,
        name_column: str,
        column_list: List[ColumnOrName],
        include_nulls: bool = False,
        _emit_ast: bool = True,
    ) -> "DataFrame":
        """Rotates a table by transforming columns into rows.
        UNPIVOT is a relational operator that accepts two columns (from a table or subquery), along with a list of columns, and generates a row for each column specified in the list. In a query, it is specified in the FROM clause after the table name or subquery.
        Note that UNPIVOT is not exactly the reverse of PIVOT as it cannot undo aggregations made by PIVOT.

        Args:
            value_column: The name to assign to the generated column that will be populated with the values from the columns in the column list.
            name_column: The name to assign to the generated column that will be populated with the names of the columns in the column list.
            column_list: The names of the columns in the source table or subequery that will be narrowed into a single pivot column. The column names will populate ``name_column``, and the column values will populate ``value_column``.
            include_nulls: If True, include rows with NULL values in ``name_column``. The default value is False.
        Example::

            >>> df = session.create_dataframe([
            ...     (1, 'electronics', 100, 200),
            ...     (2, 'clothes', 100, 300)
            ... ], schema=["empid", "dept", "jan", "feb"])
            >>> df = df.unpivot("sales", "month", ["jan", "feb"]).sort("empid")
            >>> df.show()
            ---------------------------------------------
            |"EMPID"  |"DEPT"       |"MONTH"  |"SALES"  |
            ---------------------------------------------
            |1        |electronics  |JAN      |100      |
            |1        |electronics  |FEB      |200      |
            |2        |clothes      |JAN      |100      |
            |2        |clothes      |FEB      |300      |
            ---------------------------------------------
            <BLANKLINE>
        """
        # This code performs additional type checks, run first.
        column_exprs = self._convert_cols_to_exprs("unpivot()", column_list)

        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_unpivot, stmt)
            self._set_ast_ref(ast.df)
            ast.value_column = value_column
            ast.name_column = name_column
            ast.include_nulls = include_nulls
            for c in column_list:
                build_expr_from_snowpark_column_or_col_name(ast.column_list.add(), c)

        unpivot_plan = Unpivot(
            value_column, name_column, column_exprs, include_nulls, self._plan
        )

        # TODO: Support unpivot in MockServerConnection.
        from snowflake.snowpark.mock._connection import MockServerConnection

        df: DataFrame = (
            self._with_plan(
                SelectStatement(
                    from_=SelectSnowflakePlan(
                        unpivot_plan, analyzer=self._session._analyzer
                    ),
                    analyzer=self._session._analyzer,
                )
            )
            if self._select_statement
            and not (
                isinstance(self._session._conn, MockServerConnection)
                and self._session._conn._suppress_not_implemented_error
            )
            else self._with_plan(unpivot_plan, _ast_stmt=stmt)
        )

        if _emit_ast:
            df._ast_id = stmt.uid
        return df

    @df_api_usage
    @publicapi
    def limit(
        self,
        n: int,
        offset: int = 0,
        _ast_stmt: proto.Bind = None,
        _emit_ast: bool = True,
    ) -> "DataFrame":
        """Returns a new DataFrame that contains at most ``n`` rows from the current
        DataFrame, skipping ``offset`` rows from the beginning (similar to LIMIT and OFFSET in SQL).

        Note that this is a transformation method and not an action method.

        Args:
            n: Number of rows to return.
            offset: Number of rows to skip before the start of the result set. The default value is 0.
            _ast_stmt: Overridding AST statement. Used in cases where this function is invoked internally.
            _emit_ast: Whether to emit AST statements.

        Example::

            >>> df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
            >>> df.limit(1).show()
            -------------
            |"A"  |"B"  |
            -------------
            |1    |2    |
            -------------
            <BLANKLINE>
            >>> df.limit(1, offset=1).show()
            -------------
            |"A"  |"B"  |
            -------------
            |3    |4    |
            -------------
            <BLANKLINE>
        """
        # AST.
        if _emit_ast:
            if _ast_stmt is None:
                stmt = self._session._ast_batch.bind()
                ast = with_src_position(stmt.expr.dataframe_limit, stmt)
                self._set_ast_ref(ast.df)
                ast.n = n
                ast.offset = offset
            else:
                stmt = _ast_stmt
                ast = None
        else:
            stmt = None

        # In snowpark_connect_compatible mode, we need to handle
        # the limit for dataframe after aggregation without nesting
        if (
            context._is_snowpark_connect_compatible_mode
            and self._ops_after_agg is not None
            and "limit" not in self._ops_after_agg
        ):
            limit_plan = Limit(
                Literal(n), Literal(offset), self._plan, is_limit_append=True
            )
            if self._select_statement:
                df = self._with_plan(
                    self._session._analyzer.create_select_statement(
                        from_=self._session._analyzer.create_select_snowflake_plan(
                            limit_plan, analyzer=self._session._analyzer
                        ),
                        analyzer=self._session._analyzer,
                    ),
                    _ast_stmt=stmt,
                )
            else:
                df = self._with_plan(limit_plan, _ast_stmt=stmt)
            df._ops_after_agg = self._ops_after_agg.copy()
            df._ops_after_agg.add("limit")
            return df
        else:
            if self._select_statement:
                return self._with_plan(
                    self._select_statement.limit(n, offset=offset), _ast_stmt=stmt
                )
            return self._with_plan(
                Limit(Literal(n), Literal(offset), self._plan), _ast_stmt=stmt
            )

    @df_api_usage
    @publicapi
    def union(self, other: "DataFrame", _emit_ast: bool = True) -> "DataFrame":
        """Returns a new DataFrame that contains all the rows in the current DataFrame
        and another DataFrame (``other``), excluding any duplicate rows. Both input
        DataFrames must contain the same number of columns.

        Example::
            >>> df1 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
            >>> df2 = session.create_dataframe([[0, 1], [3, 4]], schema=["c", "d"])
            >>> df1.union(df2).sort("a").show()
            -------------
            |"A"  |"B"  |
            -------------
            |0    |1    |
            |1    |2    |
            |3    |4    |
            -------------
            <BLANKLINE>

        Args:
            other: the other :class:`DataFrame` that contains the rows to include.
        """
        # AST.
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_union, stmt)
            ast.all = False
            ast.by_name = False
            ast.allow_missing_columns = False
            other._set_ast_ref(ast.other)
            self._set_ast_ref(ast.df)

        df = (
            self._with_plan(
                self._select_statement.set_operator(
                    other._select_statement
                    or SelectSnowflakePlan(
                        other._plan, analyzer=self._session._analyzer
                    ),
                    operator=SET_UNION,
                )
            )
            if self._select_statement
            else self._with_plan(UnionPlan(self._plan, other._plan, is_all=False))
        )

        if _emit_ast:
            df._ast_id = stmt.uid

        return df

    @df_api_usage
    @publicapi
    def union_all(self, other: "DataFrame", _emit_ast: bool = True) -> "DataFrame":
        """Returns a new DataFrame that contains all the rows in the current DataFrame
        and another DataFrame (``other``), including any duplicate rows. Both input
        DataFrames must contain the same number of columns.

        Example::

            >>> df1 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
            >>> df2 = session.create_dataframe([[0, 1], [3, 4]], schema=["c", "d"])
            >>> df1.union_all(df2).show()
            -------------
            |"A"  |"B"  |
            -------------
            |1    |2    |
            |3    |4    |
            |0    |1    |
            |3    |4    |
            -------------
            <BLANKLINE>

        Args:
            other: the other :class:`DataFrame` that contains the rows to include.
        """

        # AST.
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_union, stmt)
            ast.all = True
            ast.by_name = False
            ast.allow_missing_columns = False
            other._set_ast_ref(ast.other)
            self._set_ast_ref(ast.df)

        df = (
            self._with_plan(
                self._select_statement.set_operator(
                    other._select_statement
                    or SelectSnowflakePlan(
                        other._plan, analyzer=self._session._analyzer
                    ),
                    operator=SET_UNION_ALL,
                )
            )
            if self._select_statement
            else self._with_plan(UnionPlan(self._plan, other._plan, is_all=True))
        )

        if _emit_ast:
            df._ast_id = stmt.uid

        return df

    @df_api_usage
    @publicapi
    def union_by_name(
        self,
        other: "DataFrame",
        allow_missing_columns: bool = False,
        _emit_ast: bool = True,
    ) -> "DataFrame":
        """Returns a new DataFrame that contains all the rows in the current DataFrame
        and another DataFrame (``other``), excluding any duplicate rows.

        This method matches the columns in the two DataFrames by their names, not by
        their positions. The columns in the other DataFrame are rearranged to match
        the order of columns in the current DataFrame.

        Example::

            >>> df1 = session.create_dataframe([[1, 2]], schema=["a", "b"])
            >>> df2 = session.create_dataframe([[2, 1]], schema=["b", "a"])
            >>> df1.union_by_name(df2).show()
            -------------
            |"A"  |"B"  |
            -------------
            |1    |2    |
            -------------
            <BLANKLINE>

        Example::

            >>> df1 = session.create_dataframe([[1, 2]], schema=["a", "b"])
            >>> df2 = session.create_dataframe([[2, 1, 3]], schema=["b", "a", "c"])
            >>> df1.union_by_name(df2, allow_missing_columns=True).sort("c").show()
            --------------------
            |"A"  |"B"  |"C"   |
            --------------------
            |1    |2    |NULL  |
            |1    |2    |3     |
            --------------------
            <BLANKLINE>

        Args:
            other: the other :class:`DataFrame` that contains the rows to include.
            allow_missing_columns: When true includes missing columns in the final result. Missing values are Null filled. Default False.
        """
        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_union, stmt)
            ast.all = False
            ast.by_name = True
            ast.allow_missing_columns = allow_missing_columns
            self._set_ast_ref(ast.df)
            other._set_ast_ref(ast.other)

        return self._union_by_name_internal(
            other,
            is_all=False,
            allow_missing_columns=allow_missing_columns,
            _ast_stmt=stmt,
        )

    @df_api_usage
    @publicapi
    def union_all_by_name(
        self,
        other: "DataFrame",
        allow_missing_columns: bool = False,
        _emit_ast: bool = True,
    ) -> "DataFrame":
        """Returns a new DataFrame that contains all the rows in the current DataFrame
        and another DataFrame (``other``), including any duplicate rows.

        This method matches the columns in the two DataFrames by their names, not by
        their positions. The columns in the other DataFrame are rearranged to match
        the order of columns in the current DataFrame.

        Example::

            >>> df1 = session.create_dataframe([[1, 2]], schema=["a", "b"])
            >>> df2 = session.create_dataframe([[2, 1]], schema=["b", "a"])
            >>> df1.union_all_by_name(df2).show()
            -------------
            |"A"  |"B"  |
            -------------
            |1    |2    |
            |1    |2    |
            -------------
            <BLANKLINE>

        Example::

            >>> df1 = session.create_dataframe([[1, 2], [1, 2]], schema=["a", "b"])
            >>> df2 = session.create_dataframe([[2, 1, 3]], schema=["b", "a", "c"])
            >>> df1.union_all_by_name(df2, allow_missing_columns=True).show()
            --------------------
            |"A"  |"B"  |"C"   |
            --------------------
            |1    |2    |NULL  |
            |1    |2    |NULL  |
            |1    |2    |3     |
            --------------------
            <BLANKLINE>

        Args:
            other: the other :class:`DataFrame` that contains the rows to include.
            allow_missing_columns: When true includes missing columns in the final result. Missing values are Null filled. Default False.
        """
        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_union, stmt)
            ast.all = True
            ast.by_name = True
            ast.allow_missing_columns = allow_missing_columns
            self._set_ast_ref(ast.df)
            other._set_ast_ref(ast.other)

        return self._union_by_name_internal(
            other,
            is_all=True,
            allow_missing_columns=allow_missing_columns,
            _ast_stmt=stmt,
        )

    def _union_by_name_internal(
        self,
        other: "DataFrame",
        is_all: bool = False,
        allow_missing_columns: bool = False,
        _ast_stmt: proto.Bind = None,
    ) -> "DataFrame":
        left_cols = {attr.name for attr in self._output}
        left_attr_map = {attr.name: attr for attr in self._output}
        right_cols = {attr.name for attr in other._output}
        right_attr_map = {attr.name: attr for attr in other._output}

        missing_left = right_cols - left_cols
        missing_right = left_cols - right_cols

        def add_nulls(
            missing_cols: Set[str], to_df: DataFrame, from_df: DataFrame
        ) -> DataFrame:
            """
            Adds null filled columns to a dataframe using typing information from another dataframe.
            """
            # schema has to be resolved in order to get correct type information for missing columns
            dt_map = {field.name: field.datatype for field in from_df.schema.fields}
            # depending on how column names are handled names may differ from attributes to schema
            materialized_names = {
                StructField(col, DataType()).name for col in missing_cols
            }

            return to_df.select(
                "*",
                *[lit(None).cast(dt_map[col]).alias(col) for col in materialized_names],
            )

        if missing_left or missing_right:
            if allow_missing_columns:
                left = self
                right = other
                if missing_left:
                    left = add_nulls(missing_left, left, right)
                if missing_right:
                    right = add_nulls(missing_right, right, left)
                return left._union_by_name_internal(
                    right, is_all=is_all, _ast_stmt=_ast_stmt
                )
            else:
                raise SnowparkClientExceptionMessages.DF_CANNOT_RESOLVE_COLUMN_NAME_AMONG(
                    missing_left, missing_right
                )

        names = [right_attr_map[col] for col in left_attr_map.keys()]
        sql_simplifier_enabled = self._session.sql_simplifier_enabled
        if sql_simplifier_enabled and other._select_statement:
            right_child = self._with_plan(other._select_statement.select(names))
        else:
            right_child = self._with_plan(Project(names, other._plan))

        if sql_simplifier_enabled:
            df = self._with_plan(
                self._select_statement.set_operator(
                    right_child._select_statement
                    or SelectSnowflakePlan(
                        right_child._plan, analyzer=self._session._analyzer
                    ),
                    operator=SET_UNION_ALL if is_all else SET_UNION,
                ),
                _ast_stmt=_ast_stmt,
            )
        else:
            df = self._with_plan(
                UnionPlan(self._plan, right_child._plan, is_all), _ast_stmt=_ast_stmt
            )
        return df

    @df_api_usage
    @publicapi
    def intersect(self, other: "DataFrame", _emit_ast: bool = True) -> "DataFrame":
        """Returns a new DataFrame that contains the intersection of rows from the
        current DataFrame and another DataFrame (``other``). Duplicate rows are
        eliminated.

        Example::

            >>> df1 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
            >>> df2 = session.create_dataframe([[1, 2], [5, 6]], schema=["c", "d"])
            >>> df1.intersect(df2).show()
            -------------
            |"A"  |"B"  |
            -------------
            |1    |2    |
            -------------
            <BLANKLINE>

        Args:
            other: the other :class:`DataFrame` that contains the rows to use for the
                intersection.
        """
        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_intersect, stmt)
            other._set_ast_ref(ast.other)
            self._set_ast_ref(ast.df)

        df = (
            self._with_plan(
                self._select_statement.set_operator(
                    other._select_statement
                    or SelectSnowflakePlan(
                        other._plan, analyzer=self._session._analyzer
                    ),
                    operator=SET_INTERSECT,
                )
            )
            if self._select_statement
            else self._with_plan(Intersect(self._plan, other._plan))
        )

        if _emit_ast:
            df._ast_id = stmt.uid

        return df

    @df_api_usage
    @publicapi
    def except_(self, other: "DataFrame", _emit_ast: bool = True) -> "DataFrame":
        """Returns a new DataFrame that contains all the rows from the current DataFrame
        except for the rows that also appear in the ``other`` DataFrame. Duplicate rows are eliminated.

        Example::

            >>> df1 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
            >>> df2 = session.create_dataframe([[1, 2], [5, 6]], schema=["c", "d"])
            >>> df1.subtract(df2).show()
            -------------
            |"A"  |"B"  |
            -------------
            |3    |4    |
            -------------
            <BLANKLINE>

        :meth:`minus` and :meth:`subtract` are aliases of :meth:`except_`.

        Args:
            other: The :class:`DataFrame` that contains the rows to exclude.
        """
        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_except, stmt)
            other._set_ast_ref(ast.other)
            self._set_ast_ref(ast.df)

        if self._select_statement:
            df = self._with_plan(
                self._select_statement.set_operator(
                    other._select_statement
                    or SelectSnowflakePlan(
                        other._plan, analyzer=self._session._analyzer
                    ),
                    operator=SET_EXCEPT,
                )
            )
        else:
            df = self._with_plan(Except(self._plan, other._plan))

        if _emit_ast:
            df._ast_id = stmt.uid

        return df

    @df_api_usage
    @publicapi
    def natural_join(
        self,
        right: "DataFrame",
        how: Optional[str] = None,
        _emit_ast: bool = True,
        **kwargs,
    ) -> "DataFrame":
        """Performs a natural join of the specified type (``how``) with the
        current DataFrame and another DataFrame (``right``).

        Args:
            right: The other :class:`DataFrame` to join.
            how: We support the following join types:

                - Inner join: "inner" (the default value)
                - Left outer join: "left", "leftouter"
                - Right outer join: "right", "rightouter"
                - Full outer join: "full", "outer", "fullouter"

                You can also use ``join_type`` keyword to specify this condition.
                Note that to avoid breaking changes, currently when ``join_type`` is specified,
                it overrides ``how``.

        Examples::
            >>> df1 = session.create_dataframe([[1, 2], [3, 4], [5, 6]], schema=["a", "b"])
            >>> df2 = session.create_dataframe([[1, 7], [3, 8]], schema=["a", "c"])
            >>> df1.natural_join(df2).show()
            -------------------
            |"A"  |"B"  |"C"  |
            -------------------
            |1    |2    |7    |
            |3    |4    |8    |
            -------------------
            <BLANKLINE>

            >>> df1 = session.create_dataframe([[1, 2], [3, 4], [5, 6]], schema=["a", "b"])
            >>> df2 = session.create_dataframe([[1, 7], [3, 8]], schema=["a", "c"])
            >>> df1.natural_join(df2, "left").show()
            --------------------
            |"A"  |"B"  |"C"   |
            --------------------
            |1    |2    |7     |
            |3    |4    |8     |
            |5    |6    |NULL  |
            --------------------
            <BLANKLINE>
        """
        join_type = create_join_type(kwargs.get("join_type") or how or "inner")
        join_plan = Join(
            self._plan,
            right._plan,
            NaturalJoin(join_type),
            None,
            None,
        )
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_natural_join, stmt)
            self._set_ast_ref(ast.lhs)
            right._set_ast_ref(ast.rhs)
            if isinstance(join_type, Inner):
                ast.join_type.join_type__inner = True
            elif isinstance(join_type, LeftOuter):
                ast.join_type.join_type__left_outer = True
            elif isinstance(join_type, RightOuter):
                ast.join_type.join_type__right_outer = True
            elif isinstance(join_type, FullOuter):
                ast.join_type.join_type__full_outer = True
            else:
                raise ValueError(f"Unsupported join type {join_type}")

        if self._select_statement:
            select_plan = self._session._analyzer.create_select_statement(
                from_=self._session._analyzer.create_select_snowflake_plan(
                    join_plan,
                    analyzer=self._session._analyzer,
                ),
                analyzer=self._session._analyzer,
            )
            return self._with_plan(select_plan, _ast_stmt=stmt)
        return self._with_plan(join_plan, _ast_stmt=stmt)

    @df_api_usage
    @publicapi
    def join(
        self,
        right: "DataFrame",
        on: Optional[Union[ColumnOrName, Iterable[str]]] = None,
        how: Optional[str] = None,
        *,
        lsuffix: str = "",
        rsuffix: str = "",
        match_condition: Optional[Column] = None,
        _emit_ast: bool = True,
        **kwargs,
    ) -> "DataFrame":
        """Performs a join of the specified type (``how``) with the current
        DataFrame and another DataFrame (``right``) on a list of columns
        (``on``).

        Args:
            right: The other :class:`DataFrame` to join.
            on: A column name or a :class:`Column` object or a list of them to be used for the join.
                When a list of column names are specified, this method assumes the named columns are present in both dataframes.
                You can use keyword ``using_columns`` to specify this condition. Note that to avoid breaking changes, when
                `using_columns`` is specified, it overrides ``on``.
            how: We support the following join types:

                - Inner join: "inner" (the default value)
                - Left outer join: "left", "leftouter"
                - Right outer join: "right", "rightouter"
                - Full outer join: "full", "outer", "fullouter"
                - Left semi join: "semi", "leftsemi"
                - Left anti join: "anti", "leftanti"
                - Cross join: "cross"
                - Asof join: "asof"

                You can also use ``join_type`` keyword to specify this condition.
                Note that to avoid breaking changes, currently when ``join_type`` is specified,
                it overrides ``how``.
            lsuffix: Suffix to add to the overlapping columns of the left DataFrame.
            rsuffix: Suffix to add to the overlapping columns of the right DataFrame.
            match_condition: The match condition for asof join.

        Note:
            When both ``lsuffix`` and ``rsuffix`` are empty, the overlapping columns will have random column names in the resulting DataFrame.
            You can reference to these randomly named columns using :meth:`Column.alias` (See the first usage in Examples).

        See Also:
            - Usage notes for asof join: https://docs.snowflake.com/sql-reference/constructs/asof-join#usage-notes

        Examples::
            >>> from snowflake.snowpark.functions import col
            >>> df1 = session.create_dataframe([[1, 2], [3, 4], [5, 6]], schema=["a", "b"])
            >>> df2 = session.create_dataframe([[1, 7], [3, 8]], schema=["a", "c"])
            >>> df1.join(df2, df1.a == df2.a).select(df1.a.alias("a_1"), df2.a.alias("a_2"), df1.b, df2.c).show()
            -----------------------------
            |"A_1"  |"A_2"  |"B"  |"C"  |
            -----------------------------
            |1      |1      |2    |7    |
            |3      |3      |4    |8    |
            -----------------------------
            <BLANKLINE>
            >>> # refer a single column "a"
            >>> df1.join(df2, "a").select(df1.a.alias("a"), df1.b, df2.c).show()
            -------------------
            |"A"  |"B"  |"C"  |
            -------------------
            |1    |2    |7    |
            |3    |4    |8    |
            -------------------
            <BLANKLINE>
            >>> # rename the ambiguous columns
            >>> df3 = df1.to_df("df1_a", "b")
            >>> df4 = df2.to_df("df2_a", "c")
            >>> df3.join(df4, col("df1_a") == col("df2_a")).select(col("df1_a").alias("a"), "b", "c").show()
            -------------------
            |"A"  |"B"  |"C"  |
            -------------------
            |1    |2    |7    |
            |3    |4    |8    |
            -------------------
            <BLANKLINE>

            >>> # join multiple columns
            >>> mdf1 = session.create_dataframe([[1, 2], [3, 4], [5, 6]], schema=["a", "b"])
            >>> mdf2 = session.create_dataframe([[1, 2], [3, 4], [7, 6]], schema=["a", "b"])
            >>> mdf1.join(mdf2, ["a", "b"]).show()
            -------------
            |"A"  |"B"  |
            -------------
            |1    |2    |
            |3    |4    |
            -------------
            <BLANKLINE>
            >>> mdf1.join(mdf2, (mdf1["a"] < mdf2["a"]) & (mdf1["b"] == mdf2["b"])).select(mdf1["a"].as_("new_a"), mdf1["b"].as_("new_b")).show()
            ---------------------
            |"NEW_A"  |"NEW_B"  |
            ---------------------
            |5        |6        |
            ---------------------
            <BLANKLINE>
            >>> # use lsuffix and rsuffix to resolve duplicating column names
            >>> mdf1.join(mdf2, (mdf1["a"] < mdf2["a"]) & (mdf1["b"] == mdf2["b"]), lsuffix="_left", rsuffix="_right").show()
            -----------------------------------------------
            |"A_LEFT"  |"B_LEFT"  |"A_RIGHT"  |"B_RIGHT"  |
            -----------------------------------------------
            |5         |6         |7          |6          |
            -----------------------------------------------
            <BLANKLINE>
            >>> mdf1.join(mdf2, (mdf1["a"] < mdf2["a"]) & (mdf1["b"] == mdf2["b"]), rsuffix="_right").show()
            -------------------------------------
            |"A"  |"B"  |"A_RIGHT"  |"B_RIGHT"  |
            -------------------------------------
            |5    |6    |7          |6          |
            -------------------------------------
            <BLANKLINE>
            >>> # examples of different joins
            >>> df5 = session.create_dataframe([3, 4, 5, 5, 6, 7], schema=["id"])
            >>> df6 = session.create_dataframe([5, 6, 7, 7, 8, 9], schema=["id"])
            >>> # inner join
            >>> df5.join(df6, "id", "inner").sort("id").show()
            --------
            |"ID"  |
            --------
            |5     |
            |5     |
            |6     |
            |7     |
            |7     |
            --------
            <BLANKLINE>
            >>> # left/leftouter join
            >>> df5.join(df6, "id", "left").sort("id").show()
            --------
            |"ID"  |
            --------
            |3     |
            |4     |
            |5     |
            |5     |
            |6     |
            |7     |
            |7     |
            --------
            <BLANKLINE>
            >>> # right/rightouter join
            >>> df5.join(df6, "id", "right").sort("id").show()
            --------
            |"ID"  |
            --------
            |5     |
            |5     |
            |6     |
            |7     |
            |7     |
            |8     |
            |9     |
            --------
            <BLANKLINE>
            >>> # full/outer/fullouter join
            >>> df5.join(df6, "id", "full").sort("id").show()
            --------
            |"ID"  |
            --------
            |3     |
            |4     |
            |5     |
            |5     |
            |6     |
            |7     |
            |7     |
            |8     |
            |9     |
            --------
            <BLANKLINE>
            >>> # semi/leftsemi join
            >>> df5.join(df6, "id", "semi").sort("id").show()
            --------
            |"ID"  |
            --------
            |5     |
            |5     |
            |6     |
            |7     |
            --------
            <BLANKLINE>
            >>> # anti/leftanti join
            >>> df5.join(df6, "id", "anti").sort("id").show()
            --------
            |"ID"  |
            --------
            |3     |
            |4     |
            --------
            <BLANKLINE>

        Note:
            When performing chained operations, this method will not work if there are
            ambiguous column names. For example,

            >>> df1.filter(df1.a == 1).join(df2, df1.a == df2.a).select(df1.a.alias("a"), df1.b, df2.c) # doctest: +SKIP

            will not work because ``df1.filter(df1.a == 1)`` has produced a new dataframe and you
            cannot refer to ``df1.a`` anymore. Instead, you can do either

            >>> df1.join(df2, (df1.a == 1) & (df1.a == df2.a)).select(df1.a.alias("a"), df1.b, df2.c).show()
            -------------------
            |"A"  |"B"  |"C"  |
            -------------------
            |1    |2    |7    |
            -------------------
            <BLANKLINE>

            or

            >>> df3 = df1.filter(df1.a == 1)
            >>> df3.join(df2, df3.a == df2.a).select(df3.a.alias("a"), df3.b, df2.c).show()
            -------------------
            |"A"  |"B"  |"C"  |
            -------------------
            |1    |2    |7    |
            -------------------
            <BLANKLINE>

        Examples::
            >>> # asof join examples
            >>> df1 = session.create_dataframe([['A', 1, 15, 3.21],
            ...                                 ['A', 2, 16, 3.22],
            ...                                 ['B', 1, 17, 3.23],
            ...                                 ['B', 2, 18, 4.23]],
            ...                                schema=["c1", "c2", "c3", "c4"])
            >>> df2 = session.create_dataframe([['A', 1, 14, 3.19],
            ...                                 ['B', 2, 16, 3.04]],
            ...                                schema=["c1", "c2", "c3", "c4"])
            >>> df1.join(df2, on=["c1", "c2"], how="asof", match_condition=(df1.c3 >= df2.c3)) \\
            ...     .select(df1.c1, df1.c2, df1.c3.alias("C3_1"), df1.c4.alias("C4_1"), df2.c3.alias("C3_2"), df2.c4.alias("C4_2")) \\
            ...     .order_by("c1", "c2").show()
            ---------------------------------------------------
            |"C1"  |"C2"  |"C3_1"  |"C4_1"  |"C3_2"  |"C4_2"  |
            ---------------------------------------------------
            |A     |1     |15      |3.21    |14      |3.19    |
            |A     |2     |16      |3.22    |NULL    |NULL    |
            |B     |1     |17      |3.23    |NULL    |NULL    |
            |B     |2     |18      |4.23    |16      |3.04    |
            ---------------------------------------------------
            <BLANKLINE>
            >>> df1.join(df2, on=(df1.c1 == df2.c1) & (df1.c2 == df2.c2), how="asof",
            ...     match_condition=(df1.c3 >= df2.c3), lsuffix="_L", rsuffix="_R") \\
            ...     .order_by("C1_L", "C2_L").show()
            -------------------------------------------------------------------------
            |"C1_L"  |"C2_L"  |"C3_L"  |"C4_L"  |"C1_R"  |"C2_R"  |"C3_R"  |"C4_R"  |
            -------------------------------------------------------------------------
            |A       |1       |15      |3.21    |A       |1       |14      |3.19    |
            |A       |2       |16      |3.22    |NULL    |NULL    |NULL    |NULL    |
            |B       |1       |17      |3.23    |NULL    |NULL    |NULL    |NULL    |
            |B       |2       |18      |4.23    |B       |2       |16      |3.04    |
            -------------------------------------------------------------------------
            <BLANKLINE>
            >>> df1 = df1.alias("L")
            >>> df2 = df2.alias("R")
            >>> df1.join(df2, using_columns=["c1", "c2"], how="asof",
            ...         match_condition=(df1.c3 >= df2.c3)).order_by("C1", "C2").show()
            -----------------------------------------------
            |"C1"  |"C2"  |"C3L"  |"C4L"  |"C3R"  |"C4R"  |
            -----------------------------------------------
            |A     |1     |15     |3.21   |14     |3.19   |
            |A     |2     |16     |3.22   |NULL   |NULL   |
            |B     |1     |17     |3.23   |NULL   |NULL   |
            |B     |2     |18     |4.23   |16     |3.04   |
            -----------------------------------------------
            <BLANKLINE>
        """

        using_columns = kwargs.get("using_columns") or on
        original_join_type = kwargs.get("join_type") or how
        join_type_arg = original_join_type or "inner"
        join_type = create_join_type(join_type_arg)
        if isinstance(right, DataFrame):
            if self is right or self._plan is right._plan:
                raise SnowparkClientExceptionMessages.DF_SELF_JOIN_NOT_SUPPORTED()

            if isinstance(join_type, Cross) or (
                isinstance(join_type, str)
                and join_type.strip().lower().replace("_", "").startswith("cross")
            ):
                if column_to_bool(using_columns):
                    raise Exception("Cross joins cannot take columns as input.")

            if (
                isinstance(join_type, AsOf)
                or isinstance(join_type, str)
                and join_type.strip().lower() == "asof"
            ):
                if match_condition is None:
                    raise ValueError(
                        "match_condition cannot be None when performing asof join."
                    )
            else:
                if match_condition is not None:
                    raise ValueError(
                        f"match_condition is only accepted with join type 'asof' given: '{original_join_type}'"
                    )

            # Parse using_columns arg
            if column_to_bool(using_columns) is False:
                using_columns = []
            elif isinstance(using_columns, str):
                using_columns = [using_columns]
            elif isinstance(using_columns, Column):
                using_columns = using_columns
            elif (
                isinstance(using_columns, Iterable)
                and len(using_columns) > 0
                and not all([isinstance(col, str) for col in using_columns])
            ):
                bad_idx, bad_col = next(
                    (idx, col)
                    for idx, col in enumerate(using_columns)
                    if not isinstance(col, str)
                )
                raise TypeError(
                    f"All list elements for 'on' or 'using_columns' must be string type. "
                    f"Got: '{type(bad_col)}' at index {bad_idx}"
                )
            elif not isinstance(using_columns, Iterable):
                raise TypeError(
                    f"Invalid input type for join column: {type(using_columns)}"
                )

            # AST.
            stmt = None
            if _emit_ast:
                stmt = self._session._ast_batch.bind()
                ast = with_src_position(stmt.expr.dataframe_join, stmt)
                self._set_ast_ref(ast.lhs)
                right._set_ast_ref(ast.rhs)
                if isinstance(join_type, Inner):
                    ast.join_type.join_type__inner = True
                elif isinstance(join_type, LeftOuter):
                    ast.join_type.join_type__left_outer = True
                elif isinstance(join_type, RightOuter):
                    ast.join_type.join_type__right_outer = True
                elif isinstance(join_type, FullOuter):
                    ast.join_type.join_type__full_outer = True
                elif isinstance(join_type, Cross):
                    ast.join_type.join_type__cross = True
                elif isinstance(join_type, LeftSemi):
                    ast.join_type.join_type__left_semi = True
                elif isinstance(join_type, LeftAnti):
                    ast.join_type.join_type__left_anti = True
                elif isinstance(join_type, AsOf):
                    ast.join_type.join_type__asof = True
                else:
                    raise ValueError(f"Unsupported join type {join_type_arg}")

                join_cols = kwargs.get("using_columns", on)
                if join_cols is not None:
                    if isinstance(join_cols, (Column, str)):
                        build_expr_from_snowpark_column_or_col_name(
                            ast.join_expr, join_cols
                        )
                    elif isinstance(join_cols, Iterable):
                        for c in join_cols:
                            build_expr_from_snowpark_column_or_col_name(
                                ast.join_expr.list_val.vs.add(), c
                            )
                    else:
                        raise TypeError(
                            f"Invalid input type for join column: {type(join_cols)}"
                        )
                if match_condition is not None:
                    build_expr_from_snowpark_column(
                        ast.match_condition, match_condition
                    )
                if lsuffix:
                    ast.lsuffix.value = lsuffix
                if rsuffix:
                    ast.rsuffix.value = rsuffix

            return self._join_dataframes(
                right,
                using_columns,
                join_type,
                lsuffix=lsuffix,
                rsuffix=rsuffix,
                match_condition=match_condition,
                _ast_stmt=stmt,
            )

        raise TypeError("Invalid type for join. Must be Dataframe")

    @df_api_usage
    @publicapi
    def join_table_function(
        self,
        func: Union[str, List[str], TableFunctionCall],
        *func_arguments: ColumnOrName,
        _emit_ast: bool = True,
        **func_named_arguments: ColumnOrName,
    ) -> "DataFrame":
        """Lateral joins the current DataFrame with the output of the specified table function.

        References: `Snowflake SQL functions <https://docs.snowflake.com/en/sql-reference/functions-table.html>`_.

        Example 1
            Lateral join a table function by using the name and parameters directly:

            >>> df = session.sql("select 'James' as name, 'address1 address2 address3' as addresses")
            >>> df.join_table_function("split_to_table", df["addresses"], lit(" ")).show()
            --------------------------------------------------------------------
            |"NAME"  |"ADDRESSES"                 |"SEQ"  |"INDEX"  |"VALUE"   |
            --------------------------------------------------------------------
            |James   |address1 address2 address3  |1      |1        |address1  |
            |James   |address1 address2 address3  |1      |2        |address2  |
            |James   |address1 address2 address3  |1      |3        |address3  |
            --------------------------------------------------------------------
            <BLANKLINE>

        Example 2
            Lateral join a table function by calling:

            >>> from snowflake.snowpark.functions import table_function
            >>> split_to_table = table_function("split_to_table")
            >>> df = session.sql("select 'James' as name, 'address1 address2 address3' as addresses")
            >>> df.join_table_function(split_to_table(df["addresses"], lit(" "))).show()
            --------------------------------------------------------------------
            |"NAME"  |"ADDRESSES"                 |"SEQ"  |"INDEX"  |"VALUE"   |
            --------------------------------------------------------------------
            |James   |address1 address2 address3  |1      |1        |address1  |
            |James   |address1 address2 address3  |1      |2        |address2  |
            |James   |address1 address2 address3  |1      |3        |address3  |
            --------------------------------------------------------------------
            <BLANKLINE>

        Example 3
            Lateral join a table function with the partition and order by clause:

            >>> from snowflake.snowpark.functions import table_function
            >>> split_to_table = table_function("split_to_table")
            >>> df = session.create_dataframe([
            ...     ["John", "James", "address1 address2 address3"],
            ...     ["Mike", "James", "address4 address5 address6"],
            ...     ["Cathy", "Stone", "address4 address5 address6"],
            ... ],
            ... schema=["first_name", "last_name", "addresses"])
            >>> df.join_table_function(split_to_table(df["addresses"], lit(" ")).over(partition_by="last_name", order_by="first_name")).show()
            ----------------------------------------------------------------------------------------
            |"FIRST_NAME"  |"LAST_NAME"  |"ADDRESSES"                 |"SEQ"  |"INDEX"  |"VALUE"   |
            ----------------------------------------------------------------------------------------
            |John          |James        |address1 address2 address3  |1      |1        |address1  |
            |John          |James        |address1 address2 address3  |1      |2        |address2  |
            |John          |James        |address1 address2 address3  |1      |3        |address3  |
            |Mike          |James        |address4 address5 address6  |2      |1        |address4  |
            |Mike          |James        |address4 address5 address6  |2      |2        |address5  |
            |Mike          |James        |address4 address5 address6  |2      |3        |address6  |
            |Cathy         |Stone        |address4 address5 address6  |3      |1        |address4  |
            |Cathy         |Stone        |address4 address5 address6  |3      |2        |address5  |
            |Cathy         |Stone        |address4 address5 address6  |3      |3        |address6  |
            ----------------------------------------------------------------------------------------
            <BLANKLINE>

        Example 4
            Lateral join a table function with aliasing the output column names:

            >>> from snowflake.snowpark.functions import table_function
            >>> split_to_table = table_function("split_to_table")
            >>> df = session.sql("select 'James' as name, 'address1 address2 address3' as addresses")
            >>> df.join_table_function(split_to_table(col("addresses"), lit(" ")).alias("seq", "idx", "val")).show()
            ------------------------------------------------------------------
            |"NAME"  |"ADDRESSES"                 |"SEQ"  |"IDX"  |"VAL"     |
            ------------------------------------------------------------------
            |James   |address1 address2 address3  |1      |1      |address1  |
            |James   |address1 address2 address3  |1      |2      |address2  |
            |James   |address1 address2 address3  |1      |3      |address3  |
            ------------------------------------------------------------------
            <BLANKLINE>

        Args:

            func_name: The SQL function name.
            func_arguments: The positional arguments for the SQL function.
            func_named_arguments: The named arguments for the SQL function, if it accepts named arguments.

        Returns:
            A new :class:`DataFrame` that has the columns carried from this :class:`DataFrame`, plus new columns and rows from the lateral join with the table function.

        See Also:
            - :meth:`Session.table_function`, which creates a new :class:`DataFrame` by using the SQL table function.

        """

        stmt = None
        ast = None
        if _emit_ast:
            add_intermediate_stmt(self._session._ast_batch, func)
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_join_table_function, stmt)
            self._set_ast_ref(ast.lhs)
            build_indirect_table_fn_apply(
                ast.fn,
                func,
                *func_arguments,
                **func_named_arguments,
            )

        func_expr = _create_table_function_expression(
            func, *func_arguments, **func_named_arguments
        )

        project_cols = None
        new_col_names = None
        if func_expr.aliases:
            temp_join_plan = self._session._analyzer.resolve(
                TableFunctionJoin(self._plan, func_expr)
            )
            old_cols, new_cols, alias_cols = _get_cols_after_join_table(
                func_expr, self._plan, temp_join_plan
            )
            new_col_names = [
                self._session._analyzer.analyze(col, {}) for col in new_cols
            ]
            # when generating join table expression, we inculcate aliased column into the initial
            # query like so,
            #
            #     SELECT T_LEFT.*, T_RIGHT."COL1" AS "COL1_ALIASED", ... FROM () AS T_LEFT JOIN TABLE() AS T_RIGHT
            #
            # Therefore if columns names are aliased, then subsequent select must use the aliased name.
            join_plan = self._session._analyzer.resolve(
                TableFunctionJoin(self._plan, func_expr, right_cols=new_col_names)
            )
            project_cols = [*old_cols, *alias_cols]

        if self._session.sql_simplifier_enabled:
            select_plan = self._session._analyzer.create_select_statement(
                from_=SelectTableFunction(
                    func_expr,
                    other_plan=self._plan,
                    analyzer=self._session._analyzer,
                    right_cols=new_col_names,
                ),
                analyzer=self._session._analyzer,
            )
            if project_cols:
                select_plan = select_plan.select(project_cols)
            return self._with_plan(select_plan, _ast_stmt=stmt)
        if project_cols:
            return self._with_plan(Project(project_cols, join_plan), _ast_stmt=stmt)

        return self._with_plan(
            TableFunctionJoin(self._plan, func_expr, right_cols=new_col_names),
            _ast_stmt=stmt,
        )

    @df_api_usage
    @publicapi
    def cross_join(
        self,
        right: "DataFrame",
        *,
        lsuffix: str = "",
        rsuffix: str = "",
        _emit_ast: bool = True,
    ) -> "DataFrame":
        """Performs a cross join, which returns the Cartesian product of the current
        :class:`DataFrame` and another :class:`DataFrame` (``right``).

        If the current and ``right`` DataFrames have columns with the same name, and
        you need to refer to one of these columns in the returned DataFrame, use the
        :func:`col` function on the current or ``right`` DataFrame to disambiguate
        references to these columns.

        Example::

            >>> df1 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
            >>> df2 = session.create_dataframe([[5, 6], [7, 8]], schema=["c", "d"])
            >>> df1.cross_join(df2).sort("a", "b", "c", "d").show()
            -------------------------
            |"A"  |"B"  |"C"  |"D"  |
            -------------------------
            |1    |2    |5    |6    |
            |1    |2    |7    |8    |
            |3    |4    |5    |6    |
            |3    |4    |7    |8    |
            -------------------------
            <BLANKLINE>
            >>> df3 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
            >>> df4 = session.create_dataframe([[5, 6], [7, 8]], schema=["a", "b"])
            >>> df3.cross_join(df4, lsuffix="_l", rsuffix="_r").sort("a_l", "b_l", "a_r", "b_r").show()
            ---------------------------------
            |"A_L"  |"B_L"  |"A_R"  |"B_R"  |
            ---------------------------------
            |1      |2      |5      |6      |
            |1      |2      |7      |8      |
            |3      |4      |5      |6      |
            |3      |4      |7      |8      |
            ---------------------------------
            <BLANKLINE>

        Args:
            right: the right :class:`DataFrame` to join.
            lsuffix: Suffix to add to the overlapping columns of the left DataFrame.
            rsuffix: Suffix to add to the overlapping columns of the right DataFrame.

        Note:
            If both ``lsuffix`` and ``rsuffix`` are empty, the overlapping columns will have random column names in the result DataFrame.
            If either one is not empty, the overlapping columns won't have random names.
        """
        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_cross_join, stmt)
            self._set_ast_ref(ast.lhs)
            right._set_ast_ref(ast.rhs)
            if lsuffix:
                ast.lsuffix.value = lsuffix
            if rsuffix:
                ast.rsuffix.value = rsuffix

        return self._join_dataframes_internal(
            right,
            create_join_type("cross"),
            None,
            lsuffix=lsuffix,
            rsuffix=rsuffix,
            _ast_stmt=stmt,
        )

    def _join_dataframes(
        self,
        right: "DataFrame",
        using_columns: Union[Column, Iterable[str]],
        join_type: JoinType,
        *,
        lsuffix: str = "",
        rsuffix: str = "",
        match_condition: Optional[Column] = None,
        _ast_stmt: proto.Expr = None,
    ) -> "DataFrame":
        if isinstance(using_columns, Column):
            return self._join_dataframes_internal(
                right,
                join_type,
                join_exprs=using_columns,
                lsuffix=lsuffix,
                rsuffix=rsuffix,
                match_condition=match_condition,
                _ast_stmt=_ast_stmt,
            )

        if isinstance(join_type, (LeftSemi, LeftAnti)):
            # Create a Column with expression 'true AND <expr> AND <expr> .."
            join_cond = Column(Literal(True))
            for c in using_columns:
                quoted = quote_name(c)
                join_cond = join_cond & (self.col(quoted) == right.col(quoted))
            return self._join_dataframes_internal(
                right,
                join_type,
                join_cond,
                lsuffix=lsuffix,
                rsuffix=rsuffix,
                _ast_stmt=_ast_stmt,
            )
        else:
            lhs, rhs = _disambiguate(
                self,
                right,
                join_type,
                using_columns,
                lsuffix=lsuffix,
                rsuffix=rsuffix,
            )
            if not isinstance(
                join_type, Cross
            ):  # cross joins does not allow specifying columns
                join_type = UsingJoin(join_type, using_columns)
            join_logical_plan = Join(
                lhs._plan,
                rhs._plan,
                join_type,
                None,
                match_condition._expression if match_condition is not None else None,
            )
            if self._select_statement:
                return self._with_plan(
                    self._session._analyzer.create_select_statement(
                        from_=self._session._analyzer.create_select_snowflake_plan(
                            join_logical_plan, analyzer=self._session._analyzer
                        ),
                        analyzer=self._session._analyzer,
                    ),
                    _ast_stmt=_ast_stmt,
                )
            return self._with_plan(join_logical_plan, _ast_stmt=_ast_stmt)

    def _join_dataframes_internal(
        self,
        right: "DataFrame",
        join_type: JoinType,
        join_exprs: Optional[Column],
        *,
        lsuffix: str = "",
        rsuffix: str = "",
        match_condition: Optional[Column] = None,
        _ast_stmt: proto.Expr = None,
    ) -> "DataFrame":
        (lhs, rhs) = _disambiguate(
            self, right, join_type, [], lsuffix=lsuffix, rsuffix=rsuffix
        )
        join_condition_expr = join_exprs._expression if join_exprs is not None else None
        match_condition_expr = (
            match_condition._expression if match_condition is not None else None
        )
        join_logical_plan = Join(
            lhs._plan,
            rhs._plan,
            join_type,
            join_condition_expr,
            match_condition_expr,
        )
        if self._select_statement:
            return self._with_plan(
                self._session._analyzer.create_select_statement(
                    from_=self._session._analyzer.create_select_snowflake_plan(
                        join_logical_plan,
                        analyzer=self._session._analyzer,
                    ),
                    analyzer=self._session._analyzer,
                ),
                _ast_stmt=_ast_stmt,
            )
        return self._with_plan(join_logical_plan, _ast_stmt=_ast_stmt)

    @df_api_usage
    @publicapi
    def with_column(
        self,
        col_name: str,
        col: Union[Column, TableFunctionCall],
        *,
        keep_column_order: bool = False,
        ast_stmt: proto.Expr = None,
        _emit_ast: bool = True,
    ) -> "DataFrame":
        """
        Returns a DataFrame with an additional column with the specified name
        ``col_name``. The column is computed by using the specified expression ``col``.

        If a column with the same name already exists in the DataFrame, that column is
        replaced by the new column.

        Example 1::

            >>> df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
            >>> df.with_column("mean", (df["a"] + df["b"]) / 2).show()
            ------------------------
            |"A"  |"B"  |"MEAN"    |
            ------------------------
            |1    |2    |1.500000  |
            |3    |4    |3.500000  |
            ------------------------
            <BLANKLINE>

        Example 2::

            >>> from snowflake.snowpark.functions import udtf
            >>> @udtf(output_schema=["number"])
            ... class sum_udtf:
            ...     def process(self, a: int, b: int) -> Iterable[Tuple[int]]:
            ...         yield (a + b, )
            >>> df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
            >>> df.with_column("total", sum_udtf(df.a, df.b)).sort(df.a).show()
            -----------------------
            |"A"  |"B"  |"TOTAL"  |
            -----------------------
            |1    |2    |3        |
            |3    |4    |7        |
            -----------------------
            <BLANKLINE>

        Args:
            col_name: The name of the column to add or replace.
            col: The :class:`Column` or :class:`table_function.TableFunctionCall` with single column output to add or replace.
            keep_column_order: If ``True``, the original order of the columns in the DataFrame is preserved when reaplacing a column.
        """
        if ast_stmt is None and _emit_ast:
            ast_stmt = self._session._ast_batch.bind()
            expr = with_src_position(ast_stmt.expr.dataframe_with_column, ast_stmt)
            expr.col_name = col_name
            build_expr_from_snowpark_column_or_table_fn(expr.col, col)
            self._set_ast_ref(expr.df)

        df = self.with_columns(
            [col_name],
            [col],
            keep_column_order=keep_column_order,
            _ast_stmt=ast_stmt,
            _emit_ast=False,
        )

        if _emit_ast:
            df._ast_id = ast_stmt.uid

        return df

    @df_api_usage
    @publicapi
    def with_columns(
        self,
        col_names: List[str],
        values: List[Union[Column, TableFunctionCall]],
        *,
        keep_column_order: bool = False,
        _ast_stmt: proto.Expr = None,
        _emit_ast: bool = True,
    ) -> "DataFrame":
        """Returns a DataFrame with additional columns with the specified names
        ``col_names``. The columns are computed by using the specified expressions
        ``values``.

        If columns with the same names already exist in the DataFrame, those columns
        are removed and appended at the end by new columns.

        Example 1::

            >>> from snowflake.snowpark.functions import udtf
            >>> @udtf(output_schema=["number"])
            ... class sum_udtf:
            ...     def process(self, a: int, b: int) -> Iterable[Tuple[int]]:
            ...         yield (a + b, )
            >>> df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
            >>> df.with_columns(["mean", "total"], [(df["a"] + df["b"]) / 2, sum_udtf(df.a, df.b)]).sort(df.a).show()
            ----------------------------------
            |"A"  |"B"  |"MEAN"    |"TOTAL"  |
            ----------------------------------
            |1    |2    |1.500000  |3        |
            |3    |4    |3.500000  |7        |
            ----------------------------------
            <BLANKLINE>

        Example 2::

            >>> from snowflake.snowpark.functions import table_function
            >>> split_to_table = table_function("split_to_table")
            >>> df = session.sql("select 'James' as name, 'address1 address2 address3' as addresses")
            >>> df.with_columns(["seq", "idx", "val"], [split_to_table(df.addresses, lit(" "))]).show()
            ------------------------------------------------------------------
            |"NAME"  |"ADDRESSES"                 |"SEQ"  |"IDX"  |"VAL"     |
            ------------------------------------------------------------------
            |James   |address1 address2 address3  |1      |1      |address1  |
            |James   |address1 address2 address3  |1      |2      |address2  |
            |James   |address1 address2 address3  |1      |3      |address3  |
            ------------------------------------------------------------------
            <BLANKLINE>

        Args:
            col_names: A list of the names of the columns to add or replace.
            values: A list of the :class:`Column` objects or :class:`table_function.TableFunctionCall` object
                    to add or replace.
            keep_column_order: If ``True``, the original order of the columns in the DataFrame is preserved when reaplacing a column.
        """
        # Get a list of the new columns and their dedupped values
        qualified_names = [quote_name(n) for n in col_names]
        new_column_names = set(qualified_names)

        if len(col_names) != len(new_column_names):
            raise ValueError(
                "The same column name is used multiple times in the col_names parameter."
            )

        num_table_func_calls = sum(
            1 if isinstance(col, TableFunctionCall) else 0 for col in values
        )
        if num_table_func_calls == 0:
            if len(col_names) != len(values):
                raise ValueError(
                    f"The size of column names ({len(col_names)}) is not equal to the size of columns ({len(values)})"
                )
            new_cols = [col.as_(name) for name, col in zip(qualified_names, values)]
        elif num_table_func_calls > 1:
            raise ValueError(
                f"Only one table function call accepted inside with_columns call, ({num_table_func_calls}) provided"
            )
        else:
            if len(col_names) < len(values):
                raise ValueError(
                    "The size of column names must be equal to the size of the output columns. Fewer columns provided."
                )
            new_cols = []
            offset = 0
            for i in range(len(values)):
                col = values[i]
                if isinstance(col, Column):
                    name = col_names[i + offset]
                    new_cols.append(col.as_(name))
                else:
                    offset = len(col_names) - len(values)
                    names = col_names[i : i + offset + 1]
                    new_cols.append(col.as_(*names))

        # AST
        if _ast_stmt is None and _emit_ast:
            _ast_stmt = self._session._ast_batch.bind()
            expr = with_src_position(_ast_stmt.expr.dataframe_with_columns, _ast_stmt)
            for col_name in col_names:
                expr.col_names.append(col_name)
            for value in values:
                build_expr_from_snowpark_column_or_table_fn(expr.values.add(), value)
            self._set_ast_ref(expr.df)

        # If there's a table function call or keep_column_order=False,
        # we do the original "remove old columns and append new ones" logic.
        if num_table_func_calls > 0 or not keep_column_order:
            old_cols = [
                Column(field)
                for field in self._output
                if field.name not in new_column_names
            ]
            final_cols = [*old_cols, *new_cols]
        else:
            # keep_column_order=True and no table function calls
            # Re-insert replaced columns in their original positions if they exist
            replaced_map = {
                name: new_col for name, new_col in zip(qualified_names, new_cols)
            }
            final_cols = []
            used = set()  # track which new cols we've inserted

            for field in self._output:
                field_quoted = quote_name(field.name)
                # If this old column name is being replaced, insert the new col at the same position
                if field_quoted in replaced_map:
                    final_cols.append(replaced_map[field_quoted])
                    used.add(field_quoted)
                else:
                    # keep the original col
                    final_cols.append(Column(field))

            # For any new columns that didn't exist in the old schema, append them at the end
            for name, c in replaced_map.items():
                if name not in used:
                    final_cols.append(c)

        # Construct the final DataFrame
        df = self.select(final_cols, _ast_stmt=_ast_stmt, _emit_ast=False)

        if _emit_ast:
            df._ast_id = _ast_stmt.uid

        return df

    @overload
    @publicapi
    def count(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        _emit_ast: bool = True,
    ) -> int:
        ...  # pragma: no cover

    @overload
    @publicapi
    def count(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = False,
        _emit_ast: bool = True,
    ) -> AsyncJob:
        ...  # pragma: no cover

    @publicapi
    def count(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        _emit_ast: bool = True,
    ) -> Union[int, AsyncJob]:
        """Executes the query representing this DataFrame and returns the number of
        rows in the result (similar to the COUNT function in SQL).

        Args:
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            block: A bool value indicating whether this function will wait until the result is available.
                When it is ``False``, this function executes the underlying queries of the dataframe
                asynchronously and returns an :class:`AsyncJob`.
        """

        kwargs = {}
        if _emit_ast:
            # Add an Bind node that applies DataframeCount() to the input, followed by its Eval.
            stmt = self._session._ast_batch.bind()
            expr = with_src_position(stmt.expr.dataframe_count)
            self._set_ast_ref(expr.df)
            if statement_params is not None:
                build_expr_from_dict_str_str(expr.statement_params, statement_params)
            expr.block = block

            self._session._ast_batch.eval(stmt)

            # Flush AST and encode it as part of the query.
            _, kwargs[DATAFRAME_AST_PARAMETER] = self._session._ast_batch.flush(stmt)

        with open_telemetry_context_manager(self.count, self):
            df = self.agg(("*", "count"), _emit_ast=False)
            adjust_api_subcalls(df, "DataFrame.count", len_subcalls=1)
            result = df._internal_collect_with_tag(
                statement_params=statement_params,
                block=block,
                data_type=_AsyncResultType.COUNT,
                **kwargs,
            )
            return result[0][0] if block else result

    @property
    def write(self) -> DataFrameWriter:
        """Returns a new :class:`DataFrameWriter` object that you can use to write the data in the :class:`DataFrame` to
        a Snowflake database or a stage location

        Example::
            >>> df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
            >>> df.write.mode("overwrite").save_as_table("saved_table", table_type="temporary")
            >>> session.table("saved_table").show()
            -------------
            |"A"  |"B"  |
            -------------
            |1    |2    |
            |3    |4    |
            -------------
            <BLANKLINE>
            >>> stage_created_result = session.sql("create temp stage if not exists test_stage").collect()
            >>> df.write.copy_into_location("@test_stage/copied_from_dataframe")  # default CSV
            [Row(rows_unloaded=2, input_bytes=8, output_bytes=28)]
        """
        if (
            self._ast_id is not None
            and self._writer._ast is None
            and self._session.ast_enabled
        ):
            writer = proto.Expr()
            with_src_position(writer.dataframe_writer)
            self._writer._ast = writer
            self._set_ast_ref(self._writer._ast.dataframe_writer.df)
        return self._writer

    @df_collect_api_telemetry
    @publicapi
    def copy_into_table(
        self,
        table_name: Union[str, Iterable[str]],
        *,
        files: Optional[Iterable[str]] = None,
        pattern: Optional[str] = None,
        validation_mode: Optional[str] = None,
        target_columns: Optional[Iterable[str]] = None,
        transformations: Optional[Iterable[ColumnOrName]] = None,
        format_type_options: Optional[Dict[str, Any]] = None,
        statement_params: Optional[Dict[str, str]] = None,
        iceberg_config: Optional[dict] = None,
        _emit_ast: bool = True,
        **copy_options: Any,
    ) -> List[Row]:
        """Executes a `COPY INTO <table> <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html>`__ command to load data from files in a stage location into a specified table.

        It returns the load result described in `OUTPUT section of the COPY INTO <table> command <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#output>`__.
        The returned result also depends on the value of ``validation_mode``.

        It's slightly different from the ``COPY INTO`` command in that this method will automatically create a table if the table doesn't exist and the input files are CSV files whereas the ``COPY INTO <table>`` doesn't.

        To call this method, this DataFrame must be created from a :class:`DataFrameReader`.

        Example::

            >>> # Create a CSV file to demo load
            >>> import tempfile
            >>> with tempfile.NamedTemporaryFile(mode="w+t") as t:
            ...     t.writelines(["id1, Product A", "\\n" "id2, Product B"])
            ...     t.flush()
            ...     create_stage_result = session.sql("create temp stage if not exists test_stage").collect()
            ...     put_result = session.file.put(t.name, "@test_stage/copy_into_table_dir", overwrite=True)
            >>> # user_schema is used to read from CSV files. For other files it's not needed.
            >>> from snowflake.snowpark.types import StringType, StructField, StringType
            >>> from snowflake.snowpark.functions import length
            >>> user_schema = StructType([StructField("product_id", StringType()), StructField("product_name", StringType())])
            >>> # Use the DataFrameReader (session.read below) to read from CSV files.
            >>> df = session.read.schema(user_schema).csv("@test_stage/copy_into_table_dir")
            >>> # specify target column names.
            >>> target_column_names = ["product_id", "product_name"]
            >>> drop_result = session.sql("drop table if exists copied_into_table").collect()  # The copy will recreate the table.
            >>> copied_into_result = df.copy_into_table("copied_into_table", target_columns=target_column_names, force=True)
            >>> session.table("copied_into_table").show()
            ---------------------------------
            |"PRODUCT_ID"  |"PRODUCT_NAME"  |
            ---------------------------------
            |id1           | Product A      |
            |id2           | Product B      |
            ---------------------------------
            <BLANKLINE>

        The arguments of this function match the optional parameters of the `COPY INTO <table> <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#optional-parameters>`__.

        Args:
            table_name: A string or list of strings representing table name.
                If input is a string, it represents the table name; if input is of type iterable of strings,
                it represents the fully-qualified object identifier (database name, schema name, and table name).
            files: Specific files to load from the stage location.
            pattern: The regular expression that is used to match file names of the stage location.
            validation_mode: A ``str`` that instructs the ``COPY INTO <table>`` command to validate the data files instead of loading them into the specified table.
                Values can be "RETURN_n_ROWS", "RETURN_ERRORS", or "RETURN_ALL_ERRORS". Refer to the above mentioned ``COPY INTO <table>`` command optional parameters for more details.
            target_columns: Name of the columns in the table where the data should be saved.
            transformations: A list of column transformations.
            format_type_options: A dict that contains the ``formatTypeOptions`` of the ``COPY INTO <table>`` command.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            iceberg_config: A dictionary that can contain the following iceberg configuration values:

                * external_volume: specifies the identifier for the external volume where
                    the Iceberg table stores its metadata files and data in Parquet format

                * catalog: specifies either Snowflake or a catalog integration to use for this table

                * base_location: the base directory that snowflake can write iceberg metadata and files to

                * catalog_sync: optionally sets the catalog integration configured for Polaris Catalog

                * storage_serialization_policy: specifies the storage serialization policy for the table

                * iceberg_version: Overrides the version of iceberg to use. Defaults to 2 when unset.

            copy_options: The kwargs that is used to specify the ``copyOptions`` of the ``COPY INTO <table>`` command.
        """

        # AST.
        kwargs = {}
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            expr = with_src_position(stmt.expr.dataframe_copy_into_table, stmt)

            build_table_name(expr.table_name, table_name)
            if files is not None:
                expr.files.extend(files)
            if pattern is not None:
                expr.pattern.value = pattern
            if validation_mode is not None:
                expr.validation_mode.value = validation_mode
            if target_columns is not None:
                expr.target_columns.extend(target_columns)
            if transformations is not None:
                for t in transformations:
                    build_expr_from_python_val(expr.transformations.add(), t)
            if format_type_options is not None:
                for k in format_type_options:
                    entry = expr.format_type_options.add()
                    entry._1 = k
                    build_expr_from_python_val(entry._2, format_type_options[k])
            if statement_params is not None:
                build_expr_from_dict_str_str(expr.statement_params, statement_params)
            if copy_options is not None:
                for k in copy_options:
                    entry = expr.copy_options.add()
                    entry._1 = k
                    build_expr_from_python_val(entry._2, copy_options[k])
            if iceberg_config is not None:
                for k, v in iceberg_config.items():
                    t = expr.iceberg_config.add()
                    t._1 = k
                    build_expr_from_python_val(t._2, v)
            self._set_ast_ref(expr.df)

            self._session._ast_batch.eval(stmt)

            # Flush the AST and encode it as part of the query.
            _, kwargs[DATAFRAME_AST_PARAMETER] = self._session._ast_batch.flush(stmt)

        # TODO: Support copy_into_table in MockServerConnection.
        from snowflake.snowpark.mock._connection import MockServerConnection

        if (
            isinstance(self._session._conn, MockServerConnection)
            and self._session._conn._suppress_not_implemented_error
        ):
            # Allow AST tests to pass.
            df = DataFrame(
                self._session,
                CopyIntoTableNode(
                    table_name,
                    file_path="test_file_path",  # Dummy file path.
                    copy_options=copy_options,
                    format_type_options=format_type_options,
                ),
                _ast_stmt=stmt,
                _emit_ast=_emit_ast,
            )

            return df._internal_collect_with_tag_no_telemetry(
                statement_params=statement_params, **kwargs
            )

        if not self._reader or not self._reader._file_path:
            raise SnowparkDataframeException(
                "To copy into a table, the DataFrame must be created from a DataFrameReader and specify a file path."
            )
        target_columns = tuple(target_columns) if target_columns else None
        transformations = tuple(transformations) if transformations else None
        if (
            target_columns
            and transformations
            and len(target_columns) != len(transformations)
        ):
            raise ValueError(
                f"Number of column names provided to copy into does not match the number of transformations provided. Number of column names: {len(target_columns)}, number of transformations: {len(transformations)}"
            )

        full_table_name = (
            table_name if isinstance(table_name, str) else ".".join(table_name)
        )
        validate_object_name(full_table_name)
        table_name = (
            parse_table_name(table_name) if isinstance(table_name, str) else table_name
        )
        pattern = pattern or self._reader._cur_options.get("PATTERN")
        reader_format_type_options, reader_copy_options = get_copy_into_table_options(
            self._reader._cur_options
        )
        format_type_options = format_type_options or reader_format_type_options
        target_columns = target_columns or self._reader._cur_options.get(
            "TARGET_COLUMNS"
        )
        transformations = transformations or self._reader._cur_options.get(
            "TRANSFORMATIONS"
        )
        # We only want to set this if the user does not have any target columns or transformations set
        # Otherwise we operate in the mode where we don't know the schema
        create_table_from_infer_schema = False
        if self._reader._infer_schema and not (transformations or target_columns):
            transformations = self._reader._infer_schema_transformations
            target_columns = self._reader._infer_schema_target_columns
            create_table_from_infer_schema = True

        transformations = (
            [_to_col_if_str(column, "copy_into_table") for column in transformations]
            if transformations
            else None
        )
        copy_options = copy_options or reader_copy_options
        validation_mode = validation_mode or self._reader._cur_options.get(
            "VALIDATION_MODE"
        )
        normalized_column_names = (
            [quote_name(col_name) for col_name in target_columns]
            if target_columns
            else None
        )
        transformation_exps = (
            [
                column._expression if isinstance(column, Column) else column
                for column in transformations
            ]
            if transformations
            else None
        )
        df = DataFrame(
            self._session,
            CopyIntoTableNode(
                table_name,
                file_path=self._reader._file_path,
                files=files,
                file_format=self._reader._file_type,
                pattern=pattern,
                column_names=normalized_column_names,
                transformations=transformation_exps,
                copy_options=copy_options,
                format_type_options=format_type_options,
                validation_mode=validation_mode,
                user_schema=self._reader._user_schema,
                cur_options=self._reader._cur_options,
                create_table_from_infer_schema=create_table_from_infer_schema,
                iceberg_config=iceberg_config,
            ),
            _ast_stmt=stmt,
            _emit_ast=_emit_ast,
        )

        # TODO SNOW-1776638: Add Eval and pass dataframeAst as part of kwargs.
        return df._internal_collect_with_tag_no_telemetry(
            statement_params=statement_params, **kwargs
        )

    @df_collect_api_telemetry
    @publicapi
    def show(
        self,
        n: int = 10,
        max_width: int = 50,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        _emit_ast: bool = True,
    ) -> None:
        """Evaluates this DataFrame and prints out the first ``n`` rows with the
        specified maximum number of characters per column.

        Args:
            n: The number of rows to print out.
            max_width: The maximum number of characters to print out for each column.
                If the number of characters exceeds the maximum, the method prints out
                an ellipsis (...) at the end of the column.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
        """
        with open_telemetry_context_manager(self.show, self):
            print(  # noqa: T201: we need to print here.
                self._show_string(
                    n,
                    max_width,
                    _statement_params=create_or_update_statement_params_with_query_tag(
                        statement_params or self._statement_params,
                        self._session.query_tag,
                        SKIP_LEVELS_TWO,
                        collect_stacktrace=self._session.conf.get(
                            "collect_stacktrace_in_query_tag"
                        ),
                    ),
                    _emit_ast=_emit_ast,
                )
            )

    @deprecated(
        version="0.7.0",
        extra_warning_text="Use `DataFrame.join_table_function()` instead.",
        extra_doc_string="Use :meth:`join_table_function` instead.",
    )
    @df_api_usage
    @publicapi
    def flatten(
        self,
        input: ColumnOrName,
        path: Optional[str] = None,
        outer: bool = False,
        recursive: bool = False,
        mode: str = "BOTH",
        _emit_ast: bool = True,
    ) -> "DataFrame":
        """Flattens (explodes) compound values into multiple rows.

        It creates a new ``DataFrame`` from this ``DataFrame``, carries the existing columns to the new ``DataFrame``,
        and adds the following columns to it:

            - SEQ
            - KEY
            - PATH
            - INDEX
            - VALUE
            - THIS

        References: `Snowflake SQL function FLATTEN <https://docs.snowflake.com/en/sql-reference/functions/flatten.html>`_.

        If this ``DataFrame`` also has columns with the names above, you can disambiguate the columns by renaming them.

        Example::

            >>> table1 = session.sql("select parse_json(numbers) as numbers from values('[1,2]') as T(numbers)")
            >>> flattened = table1.flatten(table1["numbers"])
            >>> flattened.select(table1["numbers"], flattened["value"].as_("flattened_number")).show()
            ----------------------------------
            |"NUMBERS"  |"FLATTENED_NUMBER"  |
            ----------------------------------
            |[          |1                   |
            |  1,       |                    |
            |  2        |                    |
            |]          |                    |
            |[          |2                   |
            |  1,       |                    |
            |  2        |                    |
            |]          |                    |
            ----------------------------------
            <BLANKLINE>

        Args:
            input: The name of a column or a :class:`Column` instance that will be unseated into rows.
                The column data must be of Snowflake data type VARIANT, OBJECT, or ARRAY.
            path: The path to the element within a VARIANT data structure which needs to be flattened.
                The outermost element is to be flattened if path is empty or ``None``.
            outer: If ``False``, any input rows that cannot be expanded, either because they cannot be accessed in the ``path``
                or because they have zero fields or entries, are completely omitted from the output.
                Otherwise, exactly one row is generated for zero-row expansions
                (with NULL in the KEY, INDEX, and VALUE columns).
            recursive: If ``False``, only the element referenced by ``path`` is expanded.
                Otherwise, the expansion is performed for all sub-elements recursively.
            mode: Specifies which types should be flattened "OBJECT", "ARRAY", or "BOTH".

        Returns:
            A new :class:`DataFrame` that has the columns carried from this :class:`DataFrame`, the flattened new columns and new rows.

        See Also:
            - :meth:`Session.flatten`, which creates a new :class:`DataFrame` by flattening compound values into multiple rows.
        """

        check_flatten_mode(mode)

        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            expr = with_src_position(stmt.expr.dataframe_flatten, stmt)
            self._set_ast_ref(expr.df)
            build_expr_from_python_val(expr.input, input)
            if path is not None:
                expr.path.value = path
            expr.outer = outer
            expr.recursive = recursive

            mode = mode.upper()
            if mode.upper() == "OBJECT":
                expr.mode.flatten_mode_object = True
            elif mode.upper() == "ARRAY":
                expr.mode.flatten_mode_array = True
            else:
                expr.mode.flatten_mode_both = True

        if isinstance(input, str):
            input = self.col(input)

        return self._lateral(
            FlattenFunction(input._expression, path, outer, recursive, mode),
            _ast_stmt=stmt,
        )

    def _lateral(
        self, table_function: TableFunctionExpression, _ast_stmt: proto.Bind = None
    ) -> "DataFrame":
        from snowflake.snowpark.mock._connection import MockServerConnection

        if isinstance(self._session._conn, MockServerConnection):
            return DataFrame(self._session, _ast_stmt=_ast_stmt)

        result_columns = [
            attr.name
            for attr in self._session._analyzer.resolve(
                Lateral(self._plan, table_function)
            ).attributes
        ]
        common_col_names = [k for k, v in Counter(result_columns).items() if v > 1]
        if len(common_col_names) == 0:
            return DataFrame(
                self._session, Lateral(self._plan, table_function), _ast_stmt=_ast_stmt
            )
        prefix = _generate_prefix("a")
        child = self.select(
            [
                _alias_if_needed(
                    self,
                    attr.name,
                    prefix,
                    suffix=None,
                    common_col_names=common_col_names,
                )
                for attr in self._output
            ],
            _emit_ast=False,
        )
        return DataFrame(
            self._session, Lateral(child._plan, table_function), _ast_stmt=_ast_stmt
        )

    def _get_result_and_meta_for_show(self, n: int, _emit_ast: bool, **kwargs):
        query = self._plan.queries[-1].sql.strip().lower()

        if _emit_ast:
            # Add an Bind node that applies DataframeShow() to the input, followed by its Eval.
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_show)
            self._set_ast_ref(ast.df)
            ast.n = n
            self._session._ast_batch.eval(stmt)

            _, kwargs[DATAFRAME_AST_PARAMETER] = self._session._ast_batch.flush(stmt)

        if is_sql_select_statement(query):
            result, meta = self._session._conn.get_result_and_metadata(
                self.limit(n, _emit_ast=False)._plan, **kwargs
            )
        else:
            res, meta = self._session._conn.get_result_and_metadata(
                self._plan, **kwargs
            )
            result = res[:n]

        return result, meta

    def _show_string(
        self, n: int = 10, max_width: int = 50, _emit_ast: bool = True, **kwargs
    ) -> str:
        result, meta = self._get_result_and_meta_for_show(n, _emit_ast, **kwargs)

        # The query has been executed
        col_count = len(meta)
        col_width = []
        header = []
        for field in meta:
            name = field.name
            col_width.append(len(name))
            header.append(name)

        body = []
        for row in result:
            lines = []
            for i, v in enumerate(row):
                texts = str(v).split("\n") if v is not None else ["NULL"]
                for t in texts:
                    col_width[i] = max(len(t), col_width[i])
                    col_width[i] = min(max_width, col_width[i])
                lines.append(texts)

            # max line number in this row
            line_count = max(len(li) for li in lines)
            res = []
            for line_number in range(line_count):
                new_line = []
                for colIndex in range(len(lines)):
                    n = (
                        lines[colIndex][line_number]
                        if len(lines[colIndex]) > line_number
                        else ""
                    )
                    new_line.append(n)
                res.append(new_line)
            body.extend(res)

        # Add 2 more spaces in each column
        col_width = [w + 2 for w in col_width]

        total_width = sum(col_width) + col_count + 1
        line = "-" * total_width + "\n"

        def row_to_string(row: List[str]) -> str:
            tokens = []
            if row:
                for segment, size in zip(row, col_width):
                    if len(segment) > max_width:
                        # if truncated, add ... to the end
                        formatted = (segment[: max_width - 3] + "...").ljust(size, " ")
                    else:
                        formatted = segment.ljust(size, " ")
                    tokens.append(formatted)
            else:
                tokens = [" " * size for size in col_width]
            return f"|{'|'.join(tok for tok in tokens)}|\n"

        return (
            line
            + row_to_string(header)
            + line
            # `body` of an empty df is empty
            + ("".join(row_to_string(b) for b in body) if body else row_to_string([]))
            + line
        )

    def _show_string_spark(
        self,
        num_rows: int = 20,
        truncate: Union[bool, int] = True,
        vertical: bool = False,
        _spark_column_names: List[str] = None,
        _spark_session_tz: str = None,
        **kwargs,
    ) -> str:
        """Spark's show() logic - translated from scala to python."""
        # Fetch one more rows to check whether the result is truncated.
        result, meta = self._get_result_and_meta_for_show(
            num_rows + 1, _emit_ast=False, **kwargs
        )

        # handle empty dataframe
        if len(meta) == 1 and meta[0].name == '""':
            meta = []
            result = []
            _spark_column_names = []

        def cell_to_str(cell: Any, datatype: DataType) -> str:
            def format_timestamp_spark(dt: datetime.datetime) -> str:
                # we don't want to use dt.strftime() to format dates since it's platform-specific and might give different results in different environments.
                if dt.microsecond == 0:
                    return f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d} {dt.hour:02d}:{dt.minute:02d}:{dt.second:02d}"
                else:
                    base_format = f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d} {dt.hour:02d}:{dt.minute:02d}:{dt.second:02d}.{dt.microsecond:06d}"
                    return base_format.rstrip("0").rstrip(".")

            # Special handling for cell printing in Spark
            # TODO: this operation can be pushed down to Snowflake for execution.
            if cell is None:
                res = "NULL"
            elif isinstance(cell, bool):
                res = "true" if cell else "false"
            elif isinstance(cell, bytes) or isinstance(cell, bytearray):
                res = f"[{' '.join([format(b, '02X') for b in cell])}]"
            elif (
                isinstance(cell, datetime.datetime)
                and isinstance(datatype, TimestampType)
                and datatype.tz == TimestampTimeZone.NTZ
            ):
                res = format_timestamp_spark(cell)
            elif isinstance(cell, datetime.datetime) and isinstance(
                datatype, TimestampType
            ):
                if _spark_session_tz and cell.tzinfo is not None:
                    converted_dt = cell.astimezone(ZoneInfo(_spark_session_tz)).replace(
                        tzinfo=None
                    )
                    res = format_timestamp_spark(converted_dt)
                else:
                    res = format_timestamp_spark(cell)
            elif isinstance(cell, list) and isinstance(datatype, ArrayType):
                res = (
                    "["
                    + ", ".join(
                        [
                            cell_to_str(v, datatype.element_type or StringType())
                            for v in cell
                        ]
                    )
                    + "]"
                )
            elif isinstance(cell, dict) and isinstance(datatype, MapType):
                res = (
                    "{"
                    + ", ".join(
                        [
                            f"{cell_to_str(k, datatype.key_type or StringType())} -> {cell_to_str(v, datatype.value_type or StringType())}"
                            for k, v in sorted(cell.items())
                        ]
                    )
                    + "}"
                )
            elif isinstance(cell, dict) and isinstance(datatype, StructType):
                res = (
                    "{"
                    + ", ".join(
                        [
                            f"{cell_to_str(cell[field.name], field.datatype or StringType())}"
                            for field in datatype.fields
                        ]
                    )
                    + "}"
                )
            elif isinstance(cell, float) and isinstance(datatype, _FractionalType):
                if cell == float("inf"):
                    res = "Infinity"
                elif cell == float("-inf"):
                    res = "-Infinity"
                else:
                    res = str(cell).replace("e+", "E").replace("e-", "E-")
            elif isinstance(cell, str) and isinstance(datatype, YearMonthIntervalType):
                start_field = getattr(
                    datatype, "start_field", YearMonthIntervalType.YEAR
                )
                end_field = getattr(datatype, "end_field", YearMonthIntervalType.MONTH)
                res = format_year_month_interval_for_display(
                    cell, start_field, end_field
                )
            elif isinstance(cell, (str, datetime.timedelta)) and isinstance(
                datatype, DayTimeIntervalType
            ):
                start_field = getattr(datatype, "start_field", DayTimeIntervalType.DAY)
                end_field = getattr(datatype, "end_field", DayTimeIntervalType.SECOND)
                res = format_day_time_interval_for_display(cell, start_field, end_field)
            else:
                res = str(cell)
            return res.replace("\n", "\\n")

        # Escape field names
        header = (
            [field.name for field in meta]
            if _spark_column_names is None
            else _spark_column_names
        )

        # Process each row
        res_rows = []
        for res_row in result:
            processed_row = []
            for i, res_cell in enumerate(res_row):
                # Convert res_cell to string and escape meta-characters
                str_value = cell_to_str(res_cell, self.schema.fields[i].datatype)
                # Truncate string if necessary
                if isinstance(truncate, bool):
                    truncate_length = 20 if truncate else 0
                else:
                    truncate_length = truncate
                if 0 < truncate_length < len(str_value):
                    if truncate_length < 4:
                        str_value = str_value[:truncate_length]
                    else:
                        str_value = str_value[: truncate_length - 3] + "..."
                processed_row.append(str_value)
            res_rows.append(processed_row)

        # Get rows represented as a list of lists of strings
        tmp_rows = [header] + res_rows
        has_more_data = len(tmp_rows) - 1 > num_rows
        rows = tmp_rows[: num_rows + 1]

        sb = []
        num_cols = len(meta)
        minimum_col_width = 3

        if not vertical:
            # Initialize the width of each column to a minimum value
            col_widths = [minimum_col_width] * num_cols

            # Compute the width of each column
            for row in rows:
                for i, cell in enumerate(row):
                    col_widths[i] = max(col_widths[i], string_half_width(cell))

            # Pad the rows
            padded_rows = [
                [
                    (
                        cell.rjust(col_widths[i])
                        if truncate > 0
                        else cell.ljust(col_widths[i])
                    )
                    for i, cell in enumerate(row)
                ]
                for row in rows
            ]

            # Create the separator line
            sep = "+" + "+".join("-" * width for width in col_widths) + "+\n"

            # Add column names
            sb.append(sep)
            sb.append("|" + "|".join(padded_rows[0]) + "|\n")
            sb.append(sep)

            # Add data rows
            for row in padded_rows[1:]:
                sb.append("|" + "|".join(row) + "|\n")
            sb.append(sep)
        else:
            # Extended display mode enabled
            field_names = rows[0]
            data_rows = rows[1:]

            # Compute the width of the field name and data columns
            field_name_col_width = max(
                minimum_col_width, max(string_half_width(name) for name in field_names)
            )
            data_col_width = (
                minimum_col_width
                if len(data_rows) == 0
                else max(
                    minimum_col_width,
                    max(string_half_width(cell) for row in data_rows for cell in row),
                )
            )

            for i, row in enumerate(data_rows):
                # Header for each record
                row_header = f"-RECORD {i}".ljust(
                    field_name_col_width + data_col_width + 5, "-"
                )
                sb.append(row_header + "\n")
                for j, cell in enumerate(row):
                    field_name = field_names[j].ljust(field_name_col_width)
                    data = cell.ljust(data_col_width)
                    sb.append(f" {field_name} | {data} \n")

        # Footer
        if vertical and len(rows[1:]) == 0:
            sb.append("(0 rows)")
        elif has_more_data:
            row_string = "row" if num_rows == 1 else "rows"
            sb.append(f"only showing top {num_rows} {row_string}\n")

        return "".join(sb)

    def _format_name_for_view(
        self, func_name: str, name: Union[str, Iterable[str]]
    ) -> str:
        """Helper function for views to create correct name. Raises TypeError invalid name"""
        if isinstance(name, str):
            return name

        if isinstance(name, (list, tuple)) and all(isinstance(n, str) for n in name):
            return ".".join(name)

        raise TypeError(
            f"The input name of {func_name}() must be a str or list/tuple of strs."
        )

    @df_collect_api_telemetry
    @publicapi
    def create_or_replace_view(
        self,
        name: Union[str, Iterable[str]],
        *,
        comment: Optional[str] = None,
        statement_params: Optional[Dict[str, str]] = None,
        copy_grants: bool = False,
        _emit_ast: bool = True,
    ) -> List[Row]:
        """Creates a view that captures the computation expressed by this DataFrame.

        For ``name``, you can include the database and schema name (i.e. specify a
        fully-qualified name). If no database name or schema name are specified, the
        view will be created in the current database or schema.

        ``name`` must be a valid `Snowflake identifier <https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html>`_.

        Args:
            name: The name of the view to create or replace. Can be a list of strings
                that specifies the database name, schema name, and view name.
            comment: Adds a comment for the created view. See
                `COMMENT <https://docs.snowflake.com/en/sql-reference/sql/comment>`_.
            copy_grants: A boolean value that specifies whether to retain the access permissions from the original view
                when a new view is created. Defaults to False.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
        """

        formatted_name = self._format_name_for_view("create_or_replace_view", name)

        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            expr = with_src_position(stmt.expr.dataframe_create_or_replace_view, stmt)
            expr.is_temp = False
            expr.copy_grants = copy_grants
            self._set_ast_ref(expr.df)
            build_view_name(expr.name, name)
            if comment is not None:
                expr.comment.value = comment
            if statement_params is not None:
                build_expr_from_dict_str_str(expr.statement_params, statement_params)
        return self._do_create_or_replace_view(
            formatted_name,
            PersistedView(),
            comment=comment,
            copy_grants=copy_grants,
            _statement_params=create_or_update_statement_params_with_query_tag(
                statement_params or self._statement_params,
                self._session.query_tag,
                SKIP_LEVELS_TWO,
                collect_stacktrace=self._session.conf.get(
                    "collect_stacktrace_in_query_tag"
                ),
            ),
            _ast_stmt=stmt,
        )

    @df_collect_api_telemetry
    @publicapi
    def create_or_replace_dynamic_table(
        self,
        name: Union[str, Iterable[str]],
        *,
        warehouse: str,
        lag: str,
        comment: Optional[str] = None,
        mode: str = "overwrite",
        refresh_mode: Optional[str] = None,
        initialize: Optional[str] = None,
        clustering_keys: Optional[Iterable[ColumnOrName]] = None,
        is_transient: bool = False,
        data_retention_time: Optional[int] = None,
        max_data_extension_time: Optional[int] = None,
        statement_params: Optional[Dict[str, str]] = None,
        iceberg_config: Optional[dict] = None,
        copy_grants: bool = False,
        _emit_ast: bool = True,
    ) -> List[Row]:
        """Creates a dynamic table that captures the computation expressed by this DataFrame.

        For ``name``, you can include the database and schema name (i.e. specify a
        fully-qualified name). If no database name or schema name are specified, the
        dynamic table will be created in the current database or schema.

        ``name`` must be a valid `Snowflake identifier <https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html>`_.

        Args:
            name: The name of the dynamic table to create or replace. Can be a list of strings
                that specifies the database name, schema name, and view name.
            warehouse: The name of the warehouse used to refresh the dynamic table.
            lag: specifies the target data freshness
            comment: Adds a comment for the created table. See
                `COMMENT <https://docs.snowflake.com/en/sql-reference/sql/comment>`_.
            mode: Specifies the behavior of create dynamic table. Allowed values are:
                - "overwrite" (default): Overwrite the table by dropping the old table.
                - "errorifexists": Throw and exception if the table already exists.
                - "ignore": Ignore the operation if table already exists.
            refresh_mode: Specifies the refresh mode of the dynamic table. The value can be "AUTO",
                "FULL", or "INCREMENTAL".
            initialize: Specifies the behavior of initial refresh. The value can be "ON_CREATE" or
                "ON_SCHEDULE".
            clustering_keys: Specifies one or more columns or column expressions in the table as the clustering key.
                See `Clustering Keys & Clustered Tables <https://docs.snowflake.com/en/user-guide/tables-clustering-keys>`_
                for more details.
            is_transient: A boolean value that specifies whether the dynamic table is transient.
            data_retention_time: Specifies the retention period for the dynamic table in days so that
                Time Travel actions can be performed on historical data in the dynamic table.
            max_data_extension_time: Specifies the maximum number of days for which Snowflake can extend
                the data retention period of the dynamic table to prevent streams on the dynamic table
                from becoming stale.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            iceberg_config: A dictionary that can contain the following iceberg configuration values:

                - external_volume: specifies the identifier for the external volume where
                  the Iceberg table stores its metadata files and data in Parquet format.
                - catalog: specifies either Snowflake or a catalog integration to use for this table.
                - base_location: the base directory that snowflake can write iceberg metadata and files to.
                - catalog_sync: optionally sets the catalog integration configured for Polaris Catalog.
                - storage_serialization_policy: specifies the storage serialization policy for the table.
            copy_grants: A boolean value that specifies whether to retain the access permissions from the original view
                when a new view is created. Defaults to False.


        Note:
            See `understanding dynamic table refresh <https://docs.snowflake.com/en/user-guide/dynamic-tables-refresh>`_.
            for more details on refresh mode.
        """

        formatted_name = self._format_name_for_view(
            "create_or_replace_dynamic_table", name
        )

        if not isinstance(warehouse, str):
            raise TypeError(
                "The warehouse input of create_or_replace_dynamic_table() can only be a str."
            )

        if not isinstance(lag, str):
            raise TypeError(
                "The lag input of create_or_replace_dynamic_table() can only be a str."
            )

        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            expr = with_src_position(
                stmt.expr.dataframe_create_or_replace_dynamic_table, stmt
            )
            self._set_ast_ref(expr.df)
            build_table_name(expr.name, name)
            expr.warehouse = warehouse
            expr.lag = lag
            if comment is not None:
                expr.comment.value = comment

            fill_save_mode(expr.mode, mode)
            if refresh_mode is not None:
                expr.refresh_mode.value = refresh_mode
            if initialize is not None:
                expr.initialize.value = initialize
            if clustering_keys is not None:
                for col_or_name in clustering_keys:
                    build_expr_from_snowpark_column_or_col_name(
                        expr.clustering_keys.add(), col_or_name
                    )
            expr.is_transient = is_transient
            if data_retention_time is not None:
                expr.data_retention_time.value = data_retention_time
            if max_data_extension_time is not None:
                expr.max_data_extension_time.value = max_data_extension_time

            if statement_params is not None:
                build_expr_from_dict_str_str(expr.statement_params, statement_params)
            expr.copy_grants = copy_grants
        # TODO: Support create_or_replace_dynamic_table in MockServerConnection.
        from snowflake.snowpark.mock._connection import MockServerConnection

        if (
            isinstance(self._session._conn, MockServerConnection)
            and self._session._conn._suppress_not_implemented_error
        ):
            _logger.error(
                "create_or_replace_dynamic_table not supported in local testing mode, returning empty result."
            )
            # Allow AST tests to pass.
            return []

        create_mode = str_to_enum(mode.lower(), DynamicTableCreateMode, "`mode`")

        return self._do_create_or_replace_dynamic_table(
            name=formatted_name,
            warehouse=warehouse,
            lag=lag,
            create_mode=create_mode,
            comment=comment,
            refresh_mode=refresh_mode,
            initialize=initialize,
            clustering_keys=clustering_keys,
            is_transient=is_transient,
            data_retention_time=data_retention_time,
            max_data_extension_time=max_data_extension_time,
            _statement_params=create_or_update_statement_params_with_query_tag(
                statement_params,
                self._session.query_tag,
                SKIP_LEVELS_TWO,
                collect_stacktrace=self._session.conf.get(
                    "collect_stacktrace_in_query_tag"
                ),
            ),
            iceberg_config=iceberg_config,
            copy_grants=copy_grants,
        )

    @df_collect_api_telemetry
    @publicapi
    def create_or_replace_temp_view(
        self,
        name: Union[str, Iterable[str]],
        *,
        comment: Optional[str] = None,
        statement_params: Optional[Dict[str, str]] = None,
        copy_grants: bool = False,
        _emit_ast: bool = True,
    ) -> List[Row]:
        """Creates or replace a temporary view that returns the same results as this DataFrame.

        You can use the view in subsequent SQL queries and statements during the
        current session. The temporary view is only available in the session in which
        it is created.

        For ``name``, you can include the database and schema name (i.e. specify a
        fully-qualified name). If no database name or schema name are specified, the
        view will be created in the current database or schema.

        ``name`` must be a valid `Snowflake identifier <https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html>`_.

        Args:
            name: The name of the view to create or replace. Can be a list of strings
                that specifies the database name, schema name, and view name.
            comment: Adds a comment for the created view. See
                `COMMENT <https://docs.snowflake.com/en/sql-reference/sql/comment>`_.
            copy_grants: A boolean value that specifies whether to retain the access permissions from the original view
                when a new view is created. Defaults to False.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
        """

        formatted_name = self._format_name_for_view("create_or_replace_temp_view", name)

        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            expr = with_src_position(stmt.expr.dataframe_create_or_replace_view, stmt)
            expr.is_temp = True
            self._set_ast_ref(expr.df)
            build_view_name(expr.name, name)
            if comment is not None:
                expr.comment.value = comment
            if statement_params is not None:
                build_expr_from_dict_str_str(expr.statement_params, statement_params)
            expr.copy_grants = copy_grants

        return self._do_create_or_replace_view(
            formatted_name,
            LocalTempView(),
            comment=comment,
            copy_grants=copy_grants,
            _statement_params=create_or_update_statement_params_with_query_tag(
                statement_params or self._statement_params,
                self._session.query_tag,
                SKIP_LEVELS_TWO,
                collect_stacktrace=self._session.conf.get(
                    "collect_stacktrace_in_query_tag"
                ),
            ),
            _ast_stmt=stmt,
        )

    @df_collect_api_telemetry
    @publicapi
    def create_temp_view(
        self,
        name: Union[str, Iterable[str]],
        *,
        comment: Optional[str] = None,
        statement_params: Optional[Dict[str, str]] = None,
        _emit_ast: bool = True,
    ) -> List[Row]:
        """Creates a temporary view that returns the same results as this DataFrame.
        If it already exists, an exception will be raised.

        You can use the view in subsequent SQL queries and statements during the
        current session. The temporary view is only available in the session in which
        it is created.

        For ``name``, you can include the database and schema name (i.e. specify a
        fully-qualified name). If no database name or schema name are specified, the
        view will be created in the current database or schema.

        ``name`` must be a valid `Snowflake identifier <https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html>`_.

        Args:
            name: The name of the view to create or replace. Can be a list of strings
                that specifies the database name, schema name, and view name.
            comment: Adds a comment for the created view. See
                `COMMENT <https://docs.snowflake.com/en/sql-reference/sql/comment>`_.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
        """

        formatted_name = self._format_name_for_view("create_or_temp_view", name)

        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            expr = with_src_position(stmt.expr.dataframe_create_or_replace_view, stmt)
            expr.is_temp = True
            self._set_ast_ref(expr.df)
            build_view_name(expr.name, name)
            if comment is not None:
                expr.comment.value = comment
            if statement_params is not None:
                build_expr_from_dict_str_str(expr.statement_params, statement_params)

        return self._do_create_or_replace_view(
            formatted_name,
            LocalTempView(),
            comment=comment,
            replace=False,
            _statement_params=create_or_update_statement_params_with_query_tag(
                statement_params or self._statement_params,
                self._session.query_tag,
                SKIP_LEVELS_TWO,
                collect_stacktrace=self._session.conf.get(
                    "collect_stacktrace_in_query_tag"
                ),
            ),
            _ast_stmt=stmt,
        )

    def _do_create_or_replace_view(
        self,
        view_name: str,
        view_type: ViewType,
        comment: Optional[str],
        replace: bool = True,
        copy_grants: bool = False,
        _ast_stmt: Optional[proto.Bind] = None,
        **kwargs,
    ):
        validate_object_name(view_name)
        cmd = CreateViewCommand(
            name=view_name,
            view_type=view_type,
            comment=comment,
            replace=replace,
            copy_grants=copy_grants,
            child=self._plan,
        )

        return self._session._conn.execute(
            self._session._analyzer.resolve(cmd), **kwargs
        )

    def _do_create_or_replace_dynamic_table(
        self,
        name: str,
        warehouse: str,
        lag: str,
        create_mode: DynamicTableCreateMode,
        comment: Optional[str] = None,
        refresh_mode: Optional[str] = None,
        initialize: Optional[str] = None,
        clustering_keys: Optional[Iterable[ColumnOrName]] = None,
        is_transient: bool = False,
        data_retention_time: Optional[int] = None,
        max_data_extension_time: Optional[int] = None,
        iceberg_config: Optional[dict] = None,
        copy_grants: bool = False,
        **kwargs,
    ):
        validate_object_name(name)
        clustering_exprs = (
            [
                _to_col_if_str(
                    col, "DataFrame.create_or_replace_dynamic_table"
                )._expression
                for col in clustering_keys
            ]
            if clustering_keys
            else []
        )
        cmd = CreateDynamicTableCommand(
            name=name,
            warehouse=warehouse,
            lag=lag,
            comment=comment,
            create_mode=create_mode,
            refresh_mode=refresh_mode,
            initialize=initialize,
            clustering_exprs=clustering_exprs,
            is_transient=is_transient,
            data_retention_time=data_retention_time,
            max_data_extension_time=max_data_extension_time,
            child=self._plan,
            iceberg_config=iceberg_config,
            copy_grants=copy_grants,
        )

        return self._session._conn.execute(
            self._session._analyzer.resolve(cmd), **kwargs
        )

    @overload
    @publicapi
    def first(
        self,
        n: Optional[int] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        _emit_ast: bool = True,
    ) -> Union[Optional[Row], List[Row]]:
        ...  # pragma: no cover

    @overload
    @publicapi
    def first(
        self,
        n: Optional[int] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = False,
        _emit_ast: bool = True,
    ) -> AsyncJob:
        ...  # pragma: no cover

    @publicapi
    def first(
        self,
        n: Optional[int] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        _emit_ast: bool = True,
    ) -> Union[Optional[Row], List[Row], AsyncJob]:
        """Executes the query representing this DataFrame and returns the first ``n``
        rows of the results.

        Args:
            n: The number of rows to return.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            block: A bool value indicating whether this function will wait until the result is available.
                When it is ``False``, this function executes the underlying queries of the dataframe
                asynchronously and returns an :class:`AsyncJob`.

        Returns:
             A list of the first ``n`` :class:`Row` objects if ``n`` is not ``None``. If ``n`` is negative or
             larger than the number of rows in the result, returns all rows in the
             results. ``n`` is ``None``, it returns the first :class:`Row` of
             results, or ``None`` if it does not exist.
        """

        if not isinstance(n, int) and n is not None:
            raise ValueError(f"Invalid type of argument passed to first(): {type(n)}")

        # AST.
        kwargs = {}
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_first, stmt)
            if statement_params is not None:
                build_expr_from_dict_str_str(ast.statement_params, statement_params)
            self._set_ast_ref(ast.df)
            ast.block = block
            ast.num = 1 if n is None else n

            self._session._ast_batch.eval(stmt)

            # Flush the AST and encode it as part of the query.
            _, kwargs[DATAFRAME_AST_PARAMETER] = self._session._ast_batch.flush(stmt)

        if n is None:
            df = self.limit(1, _emit_ast=False)
            adjust_api_subcalls(df, "DataFrame.first", len_subcalls=1)
            result = df._internal_collect_with_tag(
                statement_params=statement_params, block=block, **kwargs
            )
            if not block:
                return result
            return result[0] if result else None
        elif n < 0:
            add_api_call(self, "DataFrame.first")
            return self._internal_collect_with_tag(
                statement_params=statement_params, block=block, **kwargs
            )
        else:
            df = self.limit(n, _emit_ast=False)
            adjust_api_subcalls(df, "DataFrame.first", len_subcalls=1)
            return df._internal_collect_with_tag(
                statement_params=statement_params, block=block, **kwargs
            )

    take = first

    @df_api_usage
    @publicapi
    def sample(
        self,
        frac: Optional[float] = None,
        n: Optional[int] = None,
        _emit_ast: bool = True,
    ) -> "DataFrame":
        """Samples rows based on either the number of rows to be returned or a
        percentage of rows to be returned.

        Args:
            frac: the percentage of rows to be sampled.
            n: the number of rows to sample in the range of 0 to 1,000,000 (inclusive).
        Returns:
            a :class:`DataFrame` containing the sample of rows.
        """
        DataFrame._validate_sample_input(frac, n)

        stmt = None
        if _emit_ast:
            # AST.
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_sample, stmt)
            if frac:
                ast.probability_fraction.value = frac
            if n:
                ast.num.value = n
            self._set_ast_ref(ast.df)

        sample_plan = Sample(self._plan, probability_fraction=frac, row_count=n)
        if self._select_statement:
            return self._with_plan(
                self._session._analyzer.create_select_statement(
                    from_=self._session._analyzer.create_select_snowflake_plan(
                        sample_plan, analyzer=self._session._analyzer
                    ),
                    analyzer=self._session._analyzer,
                ),
                _ast_stmt=stmt,
            )
        return self._with_plan(sample_plan, _ast_stmt=stmt)

    @staticmethod
    def _validate_sample_input(frac: Optional[float] = None, n: Optional[int] = None):
        if frac is None and n is None:
            raise ValueError(
                "'frac' and 'n' cannot both be None. "
                "One of those values must be defined"
            )
        if frac is not None and (frac < 0.0 or frac > 1.0):
            raise ValueError(
                f"'frac' value {frac} "
                f"is out of range (0 <= probability_fraction <= 1)"
            )
        if n is not None and n < 0:
            raise ValueError(f"'n' value {n} must be greater than 0")

    @property
    def na(self) -> DataFrameNaFunctions:
        """
        Returns a :class:`DataFrameNaFunctions` object that provides functions for
        handling missing values in the DataFrame.
        """
        return self._na

    @property
    def ai(self) -> DataFrameAIFunctions:
        """
        Returns a :class:`DataFrameAIFunctions` object that provides AI-powered functions
        for the DataFrame.
        """
        return self._ai

    @property
    def session(self) -> "snowflake.snowpark.Session":
        """
        Returns a :class:`snowflake.snowpark.Session` object that provides access to the session the current DataFrame is relying on.
        """
        return self._session

    @publicapi
    def describe(
        self,
        *cols: Union[str, List[str]],
        strings_include_math_stats=False,
        _emit_ast: bool = True,
    ) -> "DataFrame":
        """
        Computes basic statistics for numeric columns, which includes
        ``count``, ``mean``, ``stddev``, ``min``, and ``max``. If no columns
        are provided, this function computes statistics for all numerical or
        string columns. Non-numeric and non-string columns will be ignored
        when calling this method.

        Example::
            >>> df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
            >>> desc_result = df.describe().sort("SUMMARY").show()
            -------------------------------------------------------
            |"SUMMARY"  |"A"                 |"B"                 |
            -------------------------------------------------------
            |count      |2.0                 |2.0                 |
            |max        |3.0                 |4.0                 |
            |mean       |2.0                 |3.0                 |
            |min        |1.0                 |2.0                 |
            |stddev     |1.4142135623730951  |1.4142135623730951  |
            -------------------------------------------------------
            <BLANKLINE>

        Args:
            cols: The names of columns whose basic statistics are computed.
            strings_include_math_stats: Whether StringType columns should have mean and stddev stats included.
        """
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            expr = with_src_position(stmt.expr.dataframe_describe, stmt)
            self._set_ast_ref(expr.df)
            col_list, expr.cols.variadic = parse_positional_args_to_list_variadic(*cols)
            for c in col_list:
                build_expr_from_snowpark_column_or_col_name(expr.cols.args.add(), c)
            expr.strings_include_math_stats = strings_include_math_stats

        cols = parse_positional_args_to_list(*cols)
        df = self.select(cols, _emit_ast=False) if len(cols) > 0 else self

        # ignore non-numeric and non-string columns
        numerical_string_col_type_dict = {
            field.name: field.datatype
            for field in df.schema.fields
            if isinstance(field.datatype, (StringType, _NumericType))
        }

        stat_func_dict = {
            "count": count,
            "mean": mean,
            "stddev": stddev,
            "min": min_,
            "max": max_,
        }

        # if no columns should be selected, just return stat names
        if len(numerical_string_col_type_dict) == 0:
            df = self._session.create_dataframe(
                list(stat_func_dict.keys()), schema=["summary"], _emit_ast=False
            )
            # We need to set the API calls for this to same API calls for describe
            # Also add the new API calls for creating this DataFrame to the describe subcalls
            adjust_api_subcalls(
                df,
                "DataFrame.describe",
                precalls=self._plan.api_calls,
                subcalls=df._plan.api_calls,
            )

            if _emit_ast:
                df._ast_id = stmt.uid

            return df

        # otherwise, calculate stats
        with ResourceUsageCollector() as resource_usage_collector:
            res_df = None
            for name, func in stat_func_dict.items():
                agg_cols = []
                for c, t in numerical_string_col_type_dict.items():
                    # for string columns, we need to convert all stats to string
                    # such that they can be fitted into one column
                    if isinstance(t, StringType):
                        if strings_include_math_stats or name not in (
                            "mean",
                            "stddev",
                        ):
                            agg_cols.append(to_char(func(c)))
                        else:
                            agg_cols.append(to_char(func(lit(None))).as_(c))
                    else:
                        agg_cols.append(func(c))
                agg_stat_df = (
                    self.agg(agg_cols, _emit_ast=False)
                    .to_df(list(numerical_string_col_type_dict.keys()), _emit_ast=False)
                    .select(
                        lit(name).as_("summary"),
                        *numerical_string_col_type_dict.keys(),
                        _emit_ast=False,
                    )
                )
                res_df = (
                    res_df.union(agg_stat_df, _emit_ast=False)
                    if res_df
                    else agg_stat_df
                )

        adjust_api_subcalls(
            res_df,
            "DataFrame.describe",
            precalls=self._plan.api_calls,
            subcalls=res_df._plan.api_calls.copy(),
            resource_usage=resource_usage_collector.get_resource_usage(),
        )

        if _emit_ast:
            res_df._ast_id = stmt.uid

        return res_df

    @df_api_usage
    @publicapi
    def rename(
        self,
        col_or_mapper: Union[ColumnOrName, dict],
        new_column: str = None,
        _emit_ast: bool = True,
    ):
        """
        Returns a DataFrame with the specified column ``col_or_mapper`` renamed as ``new_column``. If ``col_or_mapper``
        is a dictionary, multiple columns will be renamed in the returned DataFrame.

        Example::
            >>> # This example renames the column `A` as `NEW_A` in the DataFrame.
            >>> df = session.sql("select 1 as A, 2 as B")
            >>> df_renamed = df.rename(col("A"), "NEW_A")
            >>> df_renamed.show()
            -----------------
            |"NEW_A"  |"B"  |
            -----------------
            |1        |2    |
            -----------------
            <BLANKLINE>
            >>> # This example renames the column `A` as `NEW_A` and `B` as `NEW_B` in the DataFrame.
            >>> df = session.sql("select 1 as A, 2 as B")
            >>> df_renamed = df.rename({col("A"): "NEW_A", "B":"NEW_B"})
            >>> df_renamed.show()
            ---------------------
            |"NEW_A"  |"NEW_B"  |
            ---------------------
            |1        |2        |
            ---------------------
            <BLANKLINE>

        Args:
            col_or_mapper: The old column instance or column name to be renamed, or the dictionary mapping from column instances or columns names to their new names (string)
            new_column: The new column name (string value), if a single old column is given
        """
        # AST.
        _ast_stmt = None
        expr = None
        if _emit_ast:
            _ast_stmt = self._session._ast_batch.bind()
            expr = with_src_position(_ast_stmt.expr.dataframe_rename, _ast_stmt)
            self._set_ast_ref(expr.df)
            if new_column is not None:
                expr.new_column.value = new_column
            build_expr_from_python_val(expr.col_or_mapper, col_or_mapper)

        if new_column is not None:
            return self.with_column_renamed(
                col_or_mapper, new_column, _ast_stmt=_ast_stmt, _emit_ast=False
            )

        if not isinstance(col_or_mapper, dict):
            raise ValueError(
                f"If new_column parameter is not specified, col_or_mapper needs to be of type dict, "
                f"not {type(col_or_mapper).__name__}"
            )

        if len(col_or_mapper) == 0:
            raise ValueError("col_or_mapper dictionary cannot be empty")

        column_or_name_list, rename_list = zip(*col_or_mapper.items())
        for name in rename_list:
            if not isinstance(name, str):
                raise TypeError(
                    f"You cannot rename a column using value {name} of type {type(name).__name__} as it "
                    f"is not a string."
                )

        names = self._get_column_names_from_column_or_name_list(column_or_name_list)
        normalized_name_list = [quote_name(n) for n in names]
        rename_map = {k: v for k, v in zip(normalized_name_list, rename_list)}
        rename_plan = Rename(rename_map, self._plan)

        if self._select_statement:
            select_plan = self._session._analyzer.create_select_statement(
                from_=self._session._analyzer.create_select_snowflake_plan(
                    rename_plan, analyzer=self._session._analyzer
                ),
                analyzer=self._session._analyzer,
            )
            return self._with_plan(select_plan, _ast_stmt=_ast_stmt)

        return self._with_plan(rename_plan, _ast_stmt=_ast_stmt)

    @df_api_usage
    @publicapi
    def with_column_renamed(
        self,
        existing: ColumnOrName,
        new: str,
        _ast_stmt: Optional[proto.Bind] = None,
        _emit_ast: bool = True,
    ) -> "DataFrame":
        """Returns a DataFrame with the specified column ``existing`` renamed as ``new``.

        Example::

            >>> # This example renames the column `A` as `NEW_A` in the DataFrame.
            >>> df = session.sql("select 1 as A, 2 as B")
            >>> df_renamed = df.with_column_renamed(col("A"), "NEW_A")
            >>> df_renamed.show()
            -----------------
            |"NEW_A"  |"B"  |
            -----------------
            |1        |2    |
            -----------------
            <BLANKLINE>

        Args:
            existing: The old column instance or column name to be renamed.
            new: The new column name.
        """
        new_quoted_name = quote_name(new)
        if isinstance(existing, str):
            old_name = quote_name(existing)
        elif isinstance(existing, Column):
            if isinstance(existing._expression, Attribute):
                from snowflake.snowpark.mock._connection import MockServerConnection

                if isinstance(self._session._conn, MockServerConnection):
                    self.schema

                att = existing._expression
                old_name = self._plan.expr_to_alias.get(att.expr_id, att.name)
            elif (
                isinstance(existing._expression, UnresolvedAttribute)
                and existing._expression.df_alias
            ):
                old_name = self._plan.df_aliased_col_name_to_real_col_name.get(
                    existing._expression.name, existing._expression.name
                )
            elif isinstance(existing._expression, NamedExpression):
                old_name = existing._expression.name
            else:
                raise ValueError(
                    f"Unable to rename column {existing} because it doesn't exist."
                )
        else:
            raise TypeError(f"{str(existing)} must be a column name or Column object.")

        to_be_renamed = [x for x in self._output if x.name.upper() == old_name.upper()]
        if not to_be_renamed:
            raise ValueError(
                f'Unable to rename column "{existing}" because it doesn\'t exist.'
            )
        elif len(to_be_renamed) > 1:
            raise SnowparkClientExceptionMessages.DF_CANNOT_RENAME_COLUMN_BECAUSE_MULTIPLE_EXIST(
                old_name, new_quoted_name, len(to_be_renamed)
            )
        new_columns = [
            Column(att).as_(new_quoted_name) if old_name == att.name else Column(att)
            for att in self._output
        ]

        # AST.
        if _ast_stmt is None and _emit_ast:
            _ast_stmt = self._session._ast_batch.bind()
            expr = with_src_position(
                _ast_stmt.expr.dataframe_with_column_renamed, _ast_stmt
            )
            self._set_ast_ref(expr.df)
            expr.new_name = new
            build_expr_from_snowpark_column_or_col_name(expr.col, existing)
        return self.select(new_columns, _ast_stmt=_ast_stmt, _emit_ast=False)

    @df_collect_api_telemetry
    @publicapi
    def cache_result(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        _emit_ast: bool = True,
    ) -> "Table":
        """Caches the content of this DataFrame to create a new cached Table DataFrame.

        All subsequent operations on the returned cached DataFrame are performed on the cached data
        and have no effect on the original DataFrame.

        You can use :meth:`Table.drop_table` or the ``with`` statement to clean up the cached result when it's not needed.
        Refer to the example code below.

        Note:
            An error will be thrown if a cached result is cleaned up and it's used again,
            or any other DataFrames derived from the cached result are used again.

        Examples::
            >>> create_result = session.sql("create temp table RESULT (NUM int)").collect()
            >>> insert_result = session.sql("insert into RESULT values(1),(2)").collect()

            >>> df = session.table("RESULT")
            >>> df.collect()
            [Row(NUM=1), Row(NUM=2)]

            >>> # Run cache_result and then insert into the original table to see
            >>> # that the cached result is not affected
            >>> df1 = df.cache_result()
            >>> insert_again_result = session.sql("insert into RESULT values (3)").collect()
            >>> df1.collect()
            [Row(NUM=1), Row(NUM=2)]
            >>> df.collect()
            [Row(NUM=1), Row(NUM=2), Row(NUM=3)]

            >>> # You can run cache_result on a result that has already been cached
            >>> df2 = df1.cache_result()
            >>> df2.collect()
            [Row(NUM=1), Row(NUM=2)]

            >>> df3 = df.cache_result()
            >>> # Drop RESULT and see that the cached results still exist
            >>> drop_table_result = session.sql(f"drop table RESULT").collect()
            >>> df1.collect()
            [Row(NUM=1), Row(NUM=2)]
            >>> df2.collect()
            [Row(NUM=1), Row(NUM=2)]
            >>> df3.collect()
            [Row(NUM=1), Row(NUM=2), Row(NUM=3)]
            >>> # Clean up the cached result
            >>> df3.drop_table()
            >>> # use context manager to clean up the cached result after it's use.
            >>> with df2.cache_result() as df4:
            ...     df4.collect()
            [Row(NUM=1), Row(NUM=2)]

        Args:
            statement_params: Dictionary of statement level parameters to be set while executing this action.

        Returns:
             A :class:`Table` object that holds the cached result in a temporary table.
             All operations on this new DataFrame have no effect on the original.

        Note:
            A temporary table is created to store the cached result and a :class:`Table` object is returned.
            You can retrieve the table name by accessing :attr:`Table.table_name`. Note that this temporary
            Snowflake table

                - may be automatically removed when the Table object is no longer referenced if
                  :attr:`Session.auto_clean_up_temp_table_enabled` is set to ``True``.

                - will be dropped after the session is closed.

            To retain a persistent table, consider using :meth:`DataFrameWriter.save_as_table` to persist
            the cached result.
        """
        with open_telemetry_context_manager(self.cache_result, self):
            from snowflake.snowpark.mock._connection import MockServerConnection

        temp_table_name = self._session.get_fully_qualified_name_if_possible(
            f'"{random_name_for_temp_object(TempObjectType.TABLE)}"'
        )

        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            expr = with_src_position(stmt.expr.dataframe_cache_result, stmt)
            self._set_ast_ref(expr.df)
            if statement_params is not None:
                build_expr_from_dict_str_str(expr.statement_params, statement_params)

            # We do not treat cache_result() (alias: cache()) as eval. Instead, in AST given it returns a
            # Dataframe we follow a similar model to other APIs. However, this API will materialize the data before in
            # a temporary table. We store a reference to this entity as part of the AST.
            build_name(
                temp_table_name, stmt.expr.dataframe_cache_result.object_name.name
            )

        if isinstance(self._session._conn, MockServerConnection):
            ast_id = self._ast_id
            self._ast_id = None  # set the AST ID to None to prevent AST emission.
            self.write.save_as_table(
                temp_table_name, create_temp_table=True, _emit_ast=False
            )
            self._ast_id = ast_id  # restore the original AST ID.
        else:
            df = self._with_plan(
                SnowflakeCreateTable(
                    [temp_table_name],
                    None,
                    SaveMode.ERROR_IF_EXISTS,
                    self._plan,
                    creation_source=TableCreationSource.CACHE_RESULT,
                    table_type="temp",
                )
            )
            statement_params_for_cache_result = {
                **(statement_params or self._statement_params or {}),
                "cache_result_temp_table": temp_table_name,
            }
            self._session._conn.execute(
                df._plan,
                _statement_params=create_or_update_statement_params_with_query_tag(
                    statement_params_for_cache_result,
                    self._session.query_tag,
                    SKIP_LEVELS_TWO,
                    collect_stacktrace=self._session.conf.get(
                        "collect_stacktrace_in_query_tag"
                    ),
                ),
            )
        cached_df = snowflake.snowpark.table.Table(
            temp_table_name,
            self._session,
            is_temp_table_for_cleanup=True,
            _emit_ast=False,
        )
        cached_df.is_cached = True

        if _emit_ast:
            cached_df._ast_id = stmt.uid

        # Preserve query history from the original dataframe for profiling
        if (
            self._session.dataframe_profiler._query_history is not None
            and self._plan.uuid
            in self._session.dataframe_profiler._query_history._dataframe_queries
        ):
            # Copy the original dataframe's query history to the cached dataframe's UUID
            original_queries = (
                self._session.dataframe_profiler._query_history._dataframe_queries[
                    self._plan.uuid
                ]
            )
            self._session.dataframe_profiler._query_history._dataframe_queries[
                cached_df._plan.uuid
            ] = original_queries.copy()
        return cached_df

    @publicapi
    def random_split(
        self,
        weights: List[float],
        seed: Optional[int] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        _emit_ast: bool = True,
    ) -> List["DataFrame"]:
        """
        Randomly splits the current DataFrame into separate DataFrames,
        using the specified weights.

        Args:
            weights: Weights to use for splitting the DataFrame. If the
                weights don't add up to 1, the weights will be normalized.
                Every number in ``weights`` has to be positive. If only one
                weight is specified, the returned DataFrame list only includes
                the current DataFrame.
            seed: The seed used by the randomness generator for splitting.

                .. caution:: By default, reusing a seed value doesn't guarantee reproducible results.
            statement_params: Dictionary of statement level parameters to be set while executing this action.

        Example::

            >>> df = session.range(10000)
            >>> weights = [0.1, 0.2, 0.3]
            >>> df_parts = df.random_split(weights)
            >>> len(df_parts) == len(weights)
            True

        Note:
            1. When multiple weights are specified, the current DataFrame will
            be cached before being split.

            2. When a weight or a normailized weight is less than ``1e-6``, the
            corresponding split dataframe will be empty.

            3. To get reproducible seeding behavior, configure the DataFrame's :py:class:`Session`
            to use simplified querying:

            .. code-block::

                >>> session.conf.set("use_simplified_query_generation", True)
        """

        if not weights:
            raise ValueError(
                "weights can't be None or empty and its values must be positive numbers."
            )

        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_random_split, stmt)
            for w in weights:
                ast.weights.append(w)
            if seed:
                ast.seed.value = seed
            if statement_params:
                build_expr_from_dict_str_str(ast.statement_params, statement_params)
            self._set_ast_ref(ast.df)

        if len(weights) == 1:
            return [self]
        else:
            for w in weights:
                if w <= 0:
                    raise ValueError("weights must be positive numbers")

            temp_column_name = random_name_for_temp_object(TempObjectType.COLUMN)
            with ResourceUsageCollector() as resource_usage_collector:
                if self._session.conf.get("use_simplified_query_generation"):
                    api_name = "DataFrame.random_split[hash]"
                    if seed is not None:
                        local_random = random.Random(seed)
                        python_seed = local_random.random()
                    else:
                        python_seed = random.random()
                    intermediate_df = self.with_column(
                        temp_column_name,
                        abs_(
                            hash_(
                                "*", lit(python_seed, _emit_ast=False), _emit_ast=False
                            ),
                            _emit_ast=False,
                        )
                        % _ONE_MILLION,
                        _emit_ast=False,
                    )
                else:
                    api_name = "DataFrame.random_split[cache_result]"
                    intermediate_df = self.with_column(
                        temp_column_name,
                        abs_(random_(seed, _emit_ast=False), _emit_ast=False)
                        % _ONE_MILLION,
                        _emit_ast=False,
                    ).cache_result(statement_params=statement_params, _emit_ast=False)
                sum_weights = sum(weights)
                normalized_cum_weights = [0] + [
                    int(w * _ONE_MILLION)
                    for w in list(
                        itertools.accumulate([w / sum_weights for w in weights])
                    )
                ]
                normalized_boundaries = zip(
                    normalized_cum_weights[:-1], normalized_cum_weights[1:]
                )
                res_dfs = [
                    intermediate_df.where(
                        (col(temp_column_name) >= lower_bound)
                        & (col(temp_column_name) < upper_bound),
                        _emit_ast=False,
                    ).drop(temp_column_name, _emit_ast=False)
                    for lower_bound, upper_bound in normalized_boundaries
                ]

            for df in res_dfs:
                # adjust for .with_column().where().drop()
                adjust_api_subcalls(
                    df,
                    api_name,
                    precalls=self._plan.api_calls,
                    subcalls=df._plan.api_calls[-3:],
                    resource_usage=resource_usage_collector.get_resource_usage(),
                )

            if _emit_ast:
                # Assign each Dataframe in res_dfs a __getitem__ from random_split.
                for i, df in enumerate(res_dfs):
                    obj_stmt = self._session._ast_batch.bind()

                    # To enable symbol capture for multiple targets (e.g., a, b, c = ...), we hint
                    # with_src_position with a target_idx.
                    obj_expr = with_src_position(
                        obj_stmt.expr.object_get_item, obj_stmt, target_idx=i
                    )
                    obj_expr.obj = stmt.uid

                    arg = obj_expr.args.add()
                    build_expr_from_python_val(arg, i)

                    df._ast_id = obj_stmt.uid

            return res_dfs

    @experimental(version="1.36.0")
    def get_execution_profile(
        self, output_file: Optional[str] = None, verbose: bool = False
    ) -> None:
        """
        Get the execution profile of the dataframe. Output is written to the file specified by output_file if provided,
        otherwise it is written to the console.
        """
        if self._session.dataframe_profiler._query_history is None:
            _logger.warning(
                "No query history found. Enable dataframe profiler using session.dataframe_profiler.enable()"
            )
            return
        query_history = self._session.dataframe_profiler._query_history
        if self._plan.uuid not in query_history.dataframe_queries:
            _logger.warning(
                f"No queries found for dataframe with plan uuid {self._plan.uuid}. Make sure to evaluate the dataframe before calling get_execution_profile."
            )
            return
        try:
            profiler = QueryProfiler(self._session, output_file)
            if self._plan.uuid in query_history._describe_queries:
                profiler.print_describe_queries(
                    query_history._describe_queries[self._plan.uuid]
                )
            for query_id in query_history.dataframe_queries[self._plan.uuid]:
                profiler.profile_query(query_id, verbose)
        finally:
            profiler.close()

    @property
    def queries(self) -> Dict[str, List[str]]:
        """
        Returns a ``dict`` that contains a list of queries that will be executed to
        evaluate this DataFrame with the key `queries`, and a list of post-execution
        actions (e.g., queries to clean up temporary objects) with the key `post_actions`.
        """
        plan_queries = self._plan.execution_queries
        return {
            "queries": [
                query.sql.strip() for query in plan_queries[PlanQueryType.QUERIES]
            ],
            "post_actions": [
                query.sql.strip() for query in plan_queries[PlanQueryType.POST_ACTIONS]
            ],
        }

    def explain(self) -> None:
        """
        Prints the list of queries that will be executed to evaluate this DataFrame.
        Prints the query execution plan if only one SELECT/DML/DDL statement will be executed.

        For more information about the query execution plan, see the
        `EXPLAIN <https://docs.snowflake.com/en/sql-reference/sql/explain.html>`_ command.
        """
        print(self._explain_string())  # noqa: T201: we need to print here.

    def _explain_string(self) -> str:
        plan_queries = self._plan.execution_queries[PlanQueryType.QUERIES]
        output_queries = "\n---\n".join(
            f"{i+1}.\n{query.sql.strip()}" for i, query in enumerate(plan_queries)
        )
        msg = f"""---------DATAFRAME EXECUTION PLAN----------
Query List:
{output_queries}"""
        # if query list contains more then one queries, skip execution plan
        if len(plan_queries) == 1:
            exec_plan = self._session._explain_query(plan_queries[0].sql)
            if exec_plan:
                msg = f"{msg}\nLogical Execution Plan:\n{exec_plan}"
            else:
                msg = f"{plan_queries[0].sql} can't be explained"

        return f"{msg}\n--------------------------------------------"

    def _resolve(self, col_name: str) -> Union[Expression, NamedExpression]:
        normalized_col_name = quote_name(col_name)
        cols = list(
            filter(
                lambda attr: quote_name(attr.name) == normalized_col_name, self._output
            )
        )

        # Remove UnresolvedAttributes. This is an artifact of the analyzer for regular Snowpark and local test mode
        # being largely incompatible and not adhering to the same protocols when defining input and output schemas.
        cols = list(
            filter(lambda attr: not isinstance(attr, UnresolvedAttribute), cols)
        )

        if len(cols) == 1:
            return cols[0].with_name(normalized_col_name)
        else:
            all_cols = [attr.name for attr in self._output]
            raise SnowparkClientExceptionMessages.DF_CANNOT_RESOLVE_COLUMN_NAME(
                col_name, all_cols
            )

    @cached_property
    def _output(self) -> List[Attribute]:
        return (
            self._select_statement.column_states.projection
            if self._select_statement
            else self._plan.output
        )

    @cached_property
    def schema(self) -> StructType:
        """The definition of the columns in this DataFrame (the "relational schema" for
        the DataFrame).
        """
        return StructType._from_attributes(self._plan.attributes)

    @cached_property
    def dtypes(self) -> List[Tuple[str, str]]:
        dtypes = [
            (name, snow_type_to_dtype_str(field.datatype))
            for name, field in zip(self.schema.names, self.schema.fields)
        ]
        return dtypes

    def _with_plan(self, plan, _ast_stmt=None) -> "DataFrame":
        """
        :param proto.Bind ast_stmt: The AST statement protobuf corresponding to this value.
        """
        df = DataFrame(self._session, plan, _ast_stmt=_ast_stmt)
        df._statement_params = self._statement_params

        if _ast_stmt is not None:
            df._ast_id = _ast_stmt.uid

        return df

    def _get_column_names_from_column_or_name_list(
        self, exprs: List[ColumnOrName]
    ) -> List:
        names = []
        for c in exprs:
            if isinstance(c, str):
                names.append(c)
            elif isinstance(c, Column) and isinstance(c._expression, Attribute):
                from snowflake.snowpark.mock._connection import MockServerConnection

                if isinstance(self._session._conn, MockServerConnection):
                    self.schema  # to execute the plan and populate expr_to_alias

                names.append(
                    self._plan.expr_to_alias.get(
                        c._expression.expr_id, c._expression.name
                    )
                )
            elif (
                isinstance(c, Column)
                and isinstance(c._expression, UnresolvedAttribute)
                and c._expression.df_alias
            ):
                names.append(
                    self._plan.df_aliased_col_name_to_real_col_name.get(
                        c._expression.name, c._expression.name
                    )
                )
            elif isinstance(c, Column) and isinstance(c._expression, NamedExpression):
                names.append(c._expression.name)
            else:
                raise TypeError(f"{str(c)} must be a column name or Column object.")

        return names

    def _convert_cols_to_exprs(
        self,
        calling_method: str,
        *cols: Union[ColumnOrName, Iterable[ColumnOrName]],
    ) -> List[Expression]:
        """Convert a string or a Column, or a list of string and Column objects to expression(s)."""

        def convert(col: ColumnOrName) -> Expression:
            if isinstance(col, str):
                return Column(col)._expression
            elif isinstance(col, Column):
                return col._expression
            else:
                raise TypeError(
                    "{} only accepts str and Column objects, or a list containing str and"
                    " Column objects".format(calling_method)
                )

        exprs = [convert(col) for col in parse_positional_args_to_list(*cols)]
        return exprs

    def _format_schema(
        self,
        level: Optional[int] = None,
        translate_columns: Optional[dict] = None,
        translate_types: Optional[dict] = None,
    ) -> str:
        def _format_datatype(name, dtype, nullable=None, depth=0):
            if level is not None and depth >= level:
                return ""
            prefix = " |  " * depth
            depth = depth + 1

            nullable_str = (
                f" (nullable = {str(nullable)})" if nullable is not None else ""
            )
            extra_lines = []
            type_str = dtype.__class__.__name__

            translated = None
            if translate_types:
                translated = translate_types.get(type_str, type_str)

            # Structured Type format their parameters on multiple lines.
            if isinstance(dtype, ArrayType):
                extra_lines = [
                    _format_datatype("element", dtype.element_type, depth=depth),
                ]
            elif isinstance(dtype, MapType):
                extra_lines = [
                    _format_datatype("key", dtype.key_type, depth=depth),
                    _format_datatype("value", dtype.value_type, depth=depth),
                ]
            elif isinstance(dtype, StructType):
                extra_lines = [
                    _format_datatype(
                        quote_name(field.name, keep_case=True),
                        field.datatype,
                        field.nullable,
                        depth,
                    )
                    for field in dtype.fields
                ]
            else:
                # By default include all parameters in type string instead
                type_str = str(dtype)

            return "\n".join(
                [
                    f"{prefix} |-- {name}: {translated or type_str}{nullable_str}",
                ]
                + [f"{line}" for line in extra_lines if line]
            )

        translate_columns = translate_columns or {}

        schema_tmp_str = "\n".join(
            [
                _format_datatype(
                    translate_columns.get(attr.name, attr.name),
                    attr.datatype,
                    attr.nullable,
                )
                for attr in self._plan.attributes
            ]
        )

        return f"root\n{schema_tmp_str}"

    def print_schema(self, level: Optional[int] = None) -> None:
        """
        Prints the schema of a dataframe in tree format.

        Args:
            level: The level to print to for nested schemas.

        Examples::
            >>> df = session.create_dataframe([(1, "a"), (2, "b")], schema=["a", "b"])
            >>> df.print_schema()
            root
             |-- "A": LongType() (nullable = False)
             |-- "B": StringType() (nullable = False)
        """
        print(self._format_schema(level))  # noqa: T201: we need to print here.

    where = filter

    # Add the following lines so API docs have them
    approxQuantile = approx_quantile = DataFrameStatFunctions.approx_quantile
    corr = DataFrameStatFunctions.corr
    cov = DataFrameStatFunctions.cov
    crosstab = DataFrameStatFunctions.crosstab
    sampleBy = sample_by = DataFrameStatFunctions.sample_by

    dropna = DataFrameNaFunctions.drop
    fillna = DataFrameNaFunctions.fill
    replace = DataFrameNaFunctions.replace

    # Add aliases for user code migration
    createOrReplaceTempView = create_or_replace_temp_view
    createOrReplaceView = create_or_replace_view
    createTempView = create_temp_view
    crossJoin = cross_join
    dropDuplicates = drop_duplicates
    groupBy = group_by
    minus = subtract = except_
    toDF = to_df
    toPandas = to_pandas
    unionAll = union_all
    unionAllByName = union_all_by_name
    unionByName = union_by_name
    withColumn = with_column
    withColumnRenamed = with_column_renamed
    toLocalIterator = to_local_iterator
    randomSplit = random_split
    order_by = sort
    orderBy = order_by
    printSchema = print_schema

    # These methods are not needed for code migration. So no aliases for them.
    # groupByGrouping_sets = group_by_grouping_sets
    # joinTableFunction = join_table_function
    # naturalJoin = natural_join
    # withColumns = with_columns


def map(
    dataframe: DataFrame,
    func: Callable,
    output_types: List[StructType],
    *,
    output_column_names: Optional[List[str]] = None,
    imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
    packages: Optional[List[Union[str, ModuleType]]] = None,
    immutable: bool = False,
    partition_by: Optional[Union[ColumnOrName, List[ColumnOrName]]] = None,
    vectorized: bool = False,
    max_batch_size: Optional[int] = None,
):
    """Returns a new DataFrame with the result of applying `func` to each of the
    rows of the specified DataFrame.

    This function registers a temporary `UDTF
    <https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-tabular-functions>`_ and
    returns a new DataFrame with the result of applying the `func` function to each row of the
    given DataFrame.

    Args:
        dataframe: The DataFrame instance.
        func: A function to be applied to every row of the DataFrame.
        output_types: A list of types for values generated by the ``func``
        output_column_names: A list of names to be assigned to the resulting columns.
        imports: A list of imports that are required to run the function. This argument is passed
            on when registering the UDTF.
        packages: A list of packages that are required to run the function. This argument is passed
            on when registering the UDTF.
        immutable: A flag to specify if the result of the func is deterministic for the same input.
        partition_by: Specify the partitioning column(s) for the UDTF.
        vectorized: A flag to determine if the UDTF process should be vectorized. See
            `vectorized UDTFs <https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-tabular-vectorized#udtfs-with-a-vectorized-process-method>`_.
        max_batch_size: The maximum number of rows per input pandas DataFrame when using vectorized option.

    Example 1::

        >>> from snowflake.snowpark.types import IntegerType
        >>> from snowflake.snowpark.dataframe import map
        >>> import pandas as pd
        >>> df = session.create_dataframe([[10, "a", 22], [20, "b", 22]], schema=["col1", "col2", "col3"])
        >>> new_df = map(df, lambda row: row[0] * row[0], output_types=[IntegerType()])
        >>> new_df.order_by("c_1").show()
        ---------
        |"C_1"  |
        ---------
        |100    |
        |400    |
        ---------
        <BLANKLINE>

    Example 2::

        >>> new_df = map(df, lambda row: (row[1], row[0] * 3), output_types=[StringType(), IntegerType()])
        >>> new_df.order_by("c_1").show()
        -----------------
        |"C_1"  |"C_2"  |
        -----------------
        |a      |30     |
        |b      |60     |
        -----------------
        <BLANKLINE>

    Example 3::

        >>> new_df = map(
        ...     df,
        ...     lambda row: (row[1], row[0] * 3),
        ...     output_types=[StringType(), IntegerType()],
        ...     output_column_names=['col1', 'col2']
        ... )
        >>> new_df.order_by("col1").show()
        -------------------
        |"COL1"  |"COL2"  |
        -------------------
        |a       |30      |
        |b       |60      |
        -------------------
        <BLANKLINE>

    Example 4::

        >>> new_df = map(df, lambda pdf: pdf['COL1']*3, output_types=[IntegerType()], vectorized=True, packages=["pandas"])
        >>> new_df.order_by("c_1").show()
        ---------
        |"C_1"  |
        ---------
        |30     |
        |60     |
        ---------
        <BLANKLINE>

    Example 5::

        >>> new_df = map(
        ...     df,
        ...     lambda pdf: (pdf['COL1']*3, pdf['COL2']+"b"),
        ...     output_types=[IntegerType(), StringType()],
        ...     output_column_names=['A', 'B'],
        ...     vectorized=True,
        ...     packages=["pandas"],
        ... )
        >>> new_df.order_by("A").show()
        -------------
        |"A"  |"B"  |
        -------------
        |30   |ab   |
        |60   |bb   |
        -------------
        <BLANKLINE>

    Example 6::

        >>> new_df = map(
        ...     df,
        ...     lambda pdf: ((pdf.shape[0],) * len(pdf), (pdf.shape[1],) * len(pdf)),
        ...     output_types=[IntegerType(), IntegerType()],
        ...     output_column_names=['rows', 'cols'],
        ...     partition_by="col3",
        ...     vectorized=True,
        ...     packages=["pandas"],
        ... )
        >>> new_df.show()
        -------------------
        |"ROWS"  |"COLS"  |
        -------------------
        |2       |3       |
        |2       |3       |
        -------------------
        <BLANKLINE>

    Note:
        1. The result of the `func` function must be either a scalar value or
        a tuple containing the same number of elements as specified in the
        `output_types` argument.

        2. When using the `vectorized` option, the `func` function must accept
        a pandas DataFrame as input and return either a pandas DataFrame, or a
        tuple of pandas Series/arrays.
    """
    if len(output_types) == 0:
        raise ValueError("output_types cannot be empty.")

    if output_column_names is None:
        output_column_names = [f"c_{i+1}" for i in range(len(output_types))]
    elif len(output_column_names) != len(output_types):
        raise ValueError(
            "'output_column_names' and 'output_types' must be of the same size."
        )

    df_columns = dataframe.columns
    packages = packages or list(dataframe._session.get_packages().values())
    packages = add_package_to_existing_packages(packages, "snowflake-snowpark-python")

    input_types = [field.datatype for field in dataframe.schema.fields]
    udtf_output_cols = [f"c{i}" for i in range(len(output_types))]
    output_schema = StructType(
        [StructField(col, type_) for col, type_ in zip(udtf_output_cols, output_types)]
    )

    if vectorized:
        # If the map is vectorized, we need to add pandas to packages if not
        # already added. Also update the input_types and output_schema to
        # be PandasDataFrameType.
        packages = add_package_to_existing_packages(packages, pandas)
        input_types = [PandasDataFrameType(input_types)]
        output_schema = PandasDataFrameType(output_types, udtf_output_cols)

    output_columns = [
        col(f"${i + len(df_columns) + 1}").alias(
            col_name
        )  # this is done to avoid collision with original table columns
        for i, col_name in enumerate(output_column_names)
    ]

    if vectorized:

        def wrap_result(result):
            if isinstance(result, pandas.DataFrame) or isinstance(result, tuple):
                return result
            return (result,)

        class _MapFunc:
            def process(self, pdf):
                return wrap_result(func(pdf))

    else:

        def wrap_result(result):
            if isinstance(result, Row):
                return tuple(result)
            elif isinstance(result, tuple):
                return result
            else:
                return (result,)

        class _MapFunc:
            def process(self, *argv):
                input_args_to_row = Row(*df_columns)
                yield wrap_result(func(input_args_to_row(*argv)))

    map_udtf = dataframe._session.udtf.register(
        _MapFunc,
        output_schema=output_schema,
        input_types=input_types,
        input_names=df_columns,
        imports=imports,
        packages=packages,
        immutable=immutable,
        max_batch_size=max_batch_size,
    )

    return dataframe.join_table_function(
        map_udtf(*df_columns).over(partition_by=partition_by)
    ).select(*output_columns)


def map_in_pandas(
    dataframe: DataFrame,
    func: Callable,
    schema: Union[StructType, str],
    *,
    partition_by: Optional[Union[ColumnOrName, List[ColumnOrName]]] = None,
    imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
    packages: Optional[List[Union[str, ModuleType]]] = None,
    immutable: bool = False,
    max_batch_size: Optional[int] = None,
):
    """Returns a new DataFrame with the result of applying `func` to each batch of data in
    the dataframe. Func is expected to be a python function that takes an iterator of pandas
    DataFrames as both input and provides them as output. Number of input and output DataFrame
    batches can be different.

    This function registers a temporary `UDTF
    <https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-tabular-functions>`_

    Args:
        dataframe: The DataFrame instance.
        func: A function to be applied to the batches of rows.
        schema: A StructType or type string that represents the expected output schema
            of the `func` parameter.
        partition_by: A column or list of columns that will be used to partition the data
            before passing it to the func.
        imports: A list of imports that are required to run the function. This argument is passed
            on when registering the UDTF.
        packages: A list of packages that are required to run the function. This argument is passed
            on when registering the UDTF.
        immutable: A flag to specify if the result of the func is deterministic for the same input.
        max_batch_size: The maximum number of rows per input pandas DataFrame when using vectorized option.

    Example 1::

        >>> from snowflake.snowpark.dataframe import map_in_pandas
        >>> df = session.create_dataframe([(1, 21), (2, 30), (3, 30)], schema=["ID", "AGE"])
        >>> def filter_func(iterator):
        ...     for pdf in iterator:
        ...         yield pdf[pdf.ID == 1]
        ...
        >>> map_in_pandas(df, filter_func, df.schema).show()
        ----------------
        |"ID"  |"AGE"  |
        ----------------
        |1     |21     |
        ----------------
        <BLANKLINE>

    Example 2::

        >>> def mean_age(iterator):
        ...     for pdf in iterator:
        ...         yield pdf.groupby("ID").mean().reset_index()
        ...
        >>> map_in_pandas(df, mean_age, "ID: bigint, AGE: double").order_by("ID").show()
        ----------------
        |"ID"  |"AGE"  |
        ----------------
        |1     |21.0   |
        |2     |30.0   |
        |3     |30.0   |
        ----------------
        <BLANKLINE>

    Example 3::

        >>> def double_age(iterator):
        ...     for pdf in iterator:
        ...         pdf["DOUBLE_AGE"] = pdf["AGE"] * 2
        ...         yield pdf
        ...
        >>> map_in_pandas(df, double_age, "ID: bigint, AGE: bigint, DOUBLE_AGE: bigint").order_by("ID").show()
        -------------------------------
        |"ID"  |"AGE"  |"DOUBLE_AGE"  |
        -------------------------------
        |1     |21     |42            |
        |2     |30     |60            |
        |3     |30     |60            |
        -------------------------------
        <BLANKLINE>

    Example 4::

        >>> def count(iterator):
        ...     for pdf in iterator:
        ...         rows, _ = pdf.shape
        ...         pdf["COUNT"] = rows
        ...         yield pdf
        >>> map_in_pandas(df, count, "ID: bigint, AGE: bigint, COUNT: bigint", partition_by="AGE", max_batch_size=2).order_by("ID").show()
        --------------------------
        |"ID"  |"AGE"  |"COUNT"  |
        --------------------------
        |1     |21     |1        |
        |2     |30     |2        |
        |3     |30     |2        |
        --------------------------
        <BLANKLINE>
    """
    if isinstance(schema, str):
        schema = type_string_to_type_object(schema)

    partition_by = partition_by or dataframe.columns

    def wrapped_func(input):  # pragma: no cover
        import pandas

        return pandas.concat(func([input]))

    return dataframe.group_by(partition_by).applyInPandas(
        wrapped_func,
        schema,
        imports=imports,
        packages=packages,
        immutable=immutable,
        max_batch_size=max_batch_size,
    )


mapInPandas = map_in_pandas
