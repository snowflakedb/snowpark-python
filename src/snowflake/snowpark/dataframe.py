#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import copy
import itertools
import re
import sys
from collections import Counter
from functools import cached_property
from logging import getLogger
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
    overload,
)

import snowflake.snowpark
from snowflake.connector.options import installed_pandas
from snowflake.snowpark._internal.analyzer.binary_plan_node import (
    AsOf,
    Cross,
    Except,
    Intersect,
    Join,
    JoinType,
    LeftAnti,
    LeftSemi,
    NaturalJoin,
    Union as UnionPlan,
    UsingJoin,
    create_join_type,
)
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
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import (
    CopyIntoTableNode,
    Limit,
    LogicalPlan,
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
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.open_telemetry import open_telemetry_context_manager
from snowflake.snowpark._internal.telemetry import (
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
    snow_type_to_dtype_str,
)
from snowflake.snowpark._internal.utils import (
    SKIP_LEVELS_THREE,
    SKIP_LEVELS_TWO,
    TempObjectType,
    check_is_pandas_dataframe_in_to_pandas,
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
    parse_table_name,
    prepare_pivot_arguments,
    quote_name,
    random_name_for_temp_object,
    validate_object_name,
)
from snowflake.snowpark.async_job import AsyncJob, _AsyncResultType
from snowflake.snowpark.column import Column, _to_col_if_sql_expr, _to_col_if_str
from snowflake.snowpark.dataframe_analytics_functions import DataFrameAnalyticsFunctions
from snowflake.snowpark.dataframe_na_functions import DataFrameNaFunctions
from snowflake.snowpark.dataframe_stat_functions import DataFrameStatFunctions
from snowflake.snowpark.dataframe_writer import DataFrameWriter
from snowflake.snowpark.exceptions import SnowparkDataframeException
from snowflake.snowpark.functions import (
    abs as abs_,
    col,
    count,
    lit,
    max as max_,
    mean,
    min as min_,
    random,
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
from snowflake.snowpark.types import StringType, StructType, _NumericType

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

if TYPE_CHECKING:
    from table import Table  # pragma: no cover

_logger = getLogger(__name__)

_ONE_MILLION = 1000000
_NUM_PREFIX_DIGITS = 4
_UNALIASED_REGEX = re.compile(f"""._[a-zA-Z0-9]{{{_NUM_PREFIX_DIGITS}}}_(.*)""")


def _generate_prefix(prefix: str) -> str:
    return f"{prefix}_{generate_random_alphanumeric(_NUM_PREFIX_DIGITS)}_"


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
    col = df.col(c)
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

    if common_col_names:
        # We use the session of the LHS DataFrame to report this telemetry
        lhs._session._conn._telemetry_client.send_alias_in_join_telemetry()

    lsuffix = lsuffix or lhs._alias
    rsuffix = rsuffix or rhs._alias
    suffix_provided = lsuffix or rsuffix
    lhs_prefix = _generate_prefix("l") if not suffix_provided else ""
    rhs_prefix = _generate_prefix("r") if not suffix_provided else ""

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
        ]
    )

    rhs_remapped = rhs.select(
        [
            _alias_if_needed(rhs, name, rhs_prefix, rsuffix, common_col_names)
            for name in rhs_names
        ]
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
        >>> df_summary = df_prices.group_by(col("product_id")).agg(f.sum(col("amount")).alias("total_amount"), f.avg("amount"))
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

    def __init__(
        self,
        session: Optional["snowflake.snowpark.Session"] = None,
        plan: Optional[LogicalPlan] = None,
        is_cached: bool = False,
    ) -> None:
        self._session = session
        self._plan = self._session._analyzer.resolve(plan)
        if isinstance(plan, (SelectStatement, MockSelectStatement)):
            self._select_statement = plan
            plan.expr_to_alias.update(self._plan.expr_to_alias)
            plan.df_aliased_col_name_to_real_col_name.update(
                self._plan.df_aliased_col_name_to_real_col_name
            )
        else:
            self._select_statement = None
        self._statement_params = None
        self.is_cached: bool = is_cached  #: Whether the dataframe is cached.

        self._reader: Optional["snowflake.snowpark.DataFrameReader"] = None
        self._writer = DataFrameWriter(self)

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

        self._alias: Optional[str] = None

    @property
    def stat(self) -> DataFrameStatFunctions:
        return self._stat

    @property
    def analytics(self) -> DataFrameAnalyticsFunctions:
        return self._analytics

    @overload
    def collect(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        log_on_exception: bool = False,
        case_sensitive: bool = True,
    ) -> List[Row]:
        ...  # pragma: no cover

    @overload
    def collect(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = False,
        log_on_exception: bool = False,
        case_sensitive: bool = True,
    ) -> AsyncJob:
        ...  # pragma: no cover

    @df_collect_api_telemetry
    def collect(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        log_on_exception: bool = False,
        case_sensitive: bool = True,
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
        with open_telemetry_context_manager(self.collect, self):
            return self._internal_collect_with_tag_no_telemetry(
                statement_params=statement_params,
                block=block,
                log_on_exception=log_on_exception,
                case_sensitive=case_sensitive,
            )

    @df_collect_api_telemetry
    def collect_nowait(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        log_on_exception: bool = False,
        case_sensitive: bool = True,
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
        with open_telemetry_context_manager(self.collect_nowait, self):
            return self._internal_collect_with_tag_no_telemetry(
                statement_params=statement_params,
                block=False,
                data_type=_AsyncResultType.ROW,
                log_on_exception=log_on_exception,
                case_sensitive=case_sensitive,
            )

    def _internal_collect_with_tag_no_telemetry(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        data_type: _AsyncResultType = _AsyncResultType.ROW,
        log_on_exception: bool = False,
        case_sensitive: bool = True,
    ) -> Union[List[Row], AsyncJob]:
        # When executing a DataFrame in any method of snowpark (either public or private),
        # we should always call this method instead of collect(), to make sure the
        # query tag is set properly.
        return self._session._conn.execute(
            self._plan,
            block=block,
            data_type=data_type,
            _statement_params=create_or_update_statement_params_with_query_tag(
                statement_params or self._statement_params,
                self._session.query_tag,
                SKIP_LEVELS_THREE,
            ),
            log_on_exception=log_on_exception,
            case_sensitive=case_sensitive,
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
                ),
            )

    @overload
    def to_local_iterator(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        case_sensitive: bool = True,
    ) -> Iterator[Row]:
        ...  # pragma: no cover

    @overload
    def to_local_iterator(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = False,
        case_sensitive: bool = True,
    ) -> AsyncJob:
        ...  # pragma: no cover

    @df_collect_api_telemetry
    def to_local_iterator(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        case_sensitive: bool = True,
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
        return self._session._conn.execute(
            self._plan,
            to_iter=True,
            block=block,
            data_type=_AsyncResultType.ITERATOR,
            _statement_params=create_or_update_statement_params_with_query_tag(
                statement_params or self._statement_params,
                self._session.query_tag,
                SKIP_LEVELS_THREE,
            ),
            case_sensitive=case_sensitive,
        )

    def __copy__(self) -> "DataFrame":
        if self._select_statement:
            new_plan = copy.copy(self._select_statement)
            new_plan.column_states = self._select_statement.column_states
            new_plan._projection_in_str = self._select_statement.projection_in_str
            new_plan._schema_query = self._select_statement.schema_query
            new_plan._query_params = self._select_statement.query_params
        else:
            new_plan = copy.copy(self._plan)
        return DataFrame(self._session, new_plan)

    if installed_pandas:
        import pandas  # pragma: no cover

        @overload
        def to_pandas(
            self,
            *,
            statement_params: Optional[Dict[str, str]] = None,
            block: bool = True,
            **kwargs: Dict[str, Any],
        ) -> pandas.DataFrame:
            ...  # pragma: no cover

    @overload
    def to_pandas(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = False,
        **kwargs: Dict[str, Any],
    ) -> AsyncJob:
        ...  # pragma: no cover

    @df_collect_api_telemetry
    def to_pandas(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
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
        """
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
                ),
                **kwargs,
            )

        # if the returned result is not a pandas dataframe, raise Exception
        # this might happen when calling this method with non-select commands
        # e.g., session.sql("create ...").to_pandas()
        if block:
            check_is_pandas_dataframe_in_to_pandas(result)

        return result

    if installed_pandas:
        import pandas

        @overload
        def to_pandas_batches(
            self,
            *,
            statement_params: Optional[Dict[str, str]] = None,
            block: bool = True,
            **kwargs: Dict[str, Any],
        ) -> Iterator[pandas.DataFrame]:
            ...  # pragma: no cover

    @overload
    def to_pandas_batches(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = False,
        **kwargs: Dict[str, Any],
    ) -> AsyncJob:
        ...  # pragma: no cover

    @df_collect_api_telemetry
    def to_pandas_batches(
        self,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
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
            ),
            **kwargs,
        )

    @df_api_usage
    def to_df(self, *names: Union[str, Iterable[str]]) -> "DataFrame":
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
        col_names = parse_positional_args_to_list(*names)
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

        new_cols = []
        for attr, name in zip(self._output, col_names):
            new_cols.append(Column(attr).alias(name))
        return self.select(new_cols)

    @df_collect_api_telemetry
    def to_snowpark_pandas(
        self,
        index_col: Optional[Union[str, List[str]]] = None,
        columns: Optional[List[str]] = None,
    ) -> "snowflake.snowpark.modin.pandas.DataFrame":
        """
        Convert the Snowpark DataFrame to Snowpark pandas DataFrame.

        Args:
            index_col: A column name or a list of column names to use as index.
            columns: A list of column names for the columns to select from the Snowpark DataFrame. If not specified, select
                all columns except ones configured in index_col.

        Returns:
            :class:`~snowflake.snowpark.modin.pandas.DataFrame`
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
            - :func:`snowflake.snowpark.modin.pandas.to_snowpark <snowflake.snowpark.modin.pandas.to_snowpark>`
            to transform a Snowpark pandas DataFrame back to a Snowpark DataFrame.

            The column names used for columns or index_cols must be Normalized Snowflake Identifiers, and the
            Normalized Snowflake Identifiers of a Snowpark DataFrame can be displayed by calling df.show().
            For details about Normalized Snowflake Identifiers, please refer to the Note in :func:`~snowflake.snowpark.modin.pandas.read_snowflake`

            `to_snowpark_pandas` works only when the environment is set up correctly for Snowpark pandas. This environment
            may require version of Python and pandas different from what Snowpark Python uses If the environment is setup
            incorrectly, an error will be raised when `to_snowpark_pandas` is called.

            For Python version support information, please refer to:
            - the prerequisites section https://docs.snowflake.com/en/developer-guide/snowpark/python/snowpark-pandas#prerequisites
            - the installation section https://docs.snowflake.com/en/developer-guide/snowpark/python/snowpark-pandas#installing-the-snowpark-pandas-api

        See also:
            - :func:`snowflake.snowpark.modin.pandas.to_snowpark <snowflake.snowpark.modin.pandas.to_snowpark>`
            - :func:`snowflake.snowpark.modin.pandas.DataFrame.to_snowpark <snowflake.snowpark.modin.pandas.DataFrame.to_snowpark>`
            - :func:`snowflake.snowpark.modin.pandas.Series.to_snowpark <snowflake.snowpark.modin.pandas.Series.to_snowpark>`

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
        import snowflake.snowpark.modin.pandas as pd  # pragma: no cover

        # create a temporary table out of the current snowpark dataframe
        temporary_table_name = random_name_for_temp_object(
            TempObjectType.TABLE
        )  # pragma: no cover
        self.write.save_as_table(
            temporary_table_name, mode="errorifexists", table_type="temporary"
        )  # pragma: no cover

        snowpandas_df = pd.read_snowflake(
            name_or_query=temporary_table_name, index_col=index_col, columns=columns
        )  # pragma: no cover

        return snowpandas_df

    def __getitem__(self, item: Union[str, Column, List, Tuple, int]):
        if isinstance(item, str):
            return self.col(item)
        elif isinstance(item, Column):
            return self.filter(item)
        elif isinstance(item, (list, tuple)):
            return self.select(item)
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

    def col(self, col_name: str) -> Column:
        """Returns a reference to a column in the DataFrame."""
        if col_name == "*":
            return Column(Star(self._output))
        else:
            return Column(self._resolve(col_name))

    @df_api_usage
    def select(
        self,
        *cols: Union[
            Union[ColumnOrName, TableFunctionCall],
            Iterable[Union[ColumnOrName, TableFunctionCall]],
        ],
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
        """
        exprs = parse_positional_args_to_list(*cols)
        if not exprs:
            raise ValueError("The input of select() cannot be empty")

        names = []
        table_func = None
        join_plan = None

        for e in exprs:
            if isinstance(e, Column):
                names.append(e._named())
            elif isinstance(e, str):
                names.append(Column(e)._named())
            elif isinstance(e, TableFunctionCall):
                if table_func:
                    raise ValueError(
                        f"At most one table function can be called inside a select(). "
                        f"Called '{table_func.user_visible_name}' and '{e.user_visible_name}'."
                    )
                table_func = e
                func_expr = _create_table_function_expression(func=table_func)

                if isinstance(e, _ExplodeFunctionCall):
                    new_cols, alias_cols = _get_cols_after_explode_join(e, self._plan)
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
                new_col_names = [
                    self._session._analyzer.analyze(col, {}) for col in new_cols
                ]

                # a special case when dataframe.select only selects the output of table
                # function join, we set left_cols = []. This is done in-order to handle the
                # overlapping column case of DF and table function output with no aliases.
                # This generates a sql like so,
                #
                #     SELECT T_RIGHT."COL1" FROM () AS T_LEFT JOIN TABLE() AS T_RIGHT
                #
                # In the above case, if the original DF had a column named "COL1", we would not
                # have any collisions.
                join_plan = self._session._analyzer.resolve(
                    TableFunctionJoin(
                        self._plan,
                        func_expr,
                        left_cols=[] if len(exprs) == 1 else ["*"],
                        right_cols=new_col_names,
                    )
                )
            else:
                raise TypeError(
                    "The input of select() must be Column, column name, TableFunctionCall, or a list of them"
                )

        if self._select_statement:
            if join_plan:
                return self._with_plan(
                    self._session._analyzer.create_select_statement(
                        from_=self._session._analyzer.create_select_snowflake_plan(
                            join_plan, analyzer=self._session._analyzer
                        ),
                        analyzer=self._session._analyzer,
                    ).select(names)
                )
            return self._with_plan(self._select_statement.select(names))

        return self._with_plan(Project(names, join_plan or self._plan))

    @df_api_usage
    def select_expr(self, *exprs: Union[str, Iterable[str]]) -> "DataFrame":
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
        return self.select(
            [sql_expr(expr) for expr in parse_positional_args_to_list(*exprs)]
        )

    selectExpr = select_expr

    @df_api_usage
    def drop(
        self,
        *cols: Union[ColumnOrName, Iterable[ColumnOrName]],
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
        exprs = parse_positional_args_to_list(*cols)

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
                raise SnowparkClientExceptionMessages.DF_CANNOT_DROP_COLUMN_NAME(str(c))

        normalized_names = {quote_name(n) for n in names}
        existing_names = [attr.name for attr in self._output]
        keep_col_names = [c for c in existing_names if c not in normalized_names]
        if not keep_col_names:
            raise SnowparkClientExceptionMessages.DF_CANNOT_DROP_ALL_COLUMNS()
        else:
            return self.select(list(keep_col_names))

    @df_api_usage
    def filter(self, expr: ColumnOrSqlExpr) -> "DataFrame":
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

        :meth:`where` is an alias of :meth:`filter`.
        """
        if self._select_statement:
            return self._with_plan(
                self._select_statement.filter(
                    _to_col_if_sql_expr(expr, "filter/where")._expression
                )
            )
        return self._with_plan(
            Filter(
                _to_col_if_sql_expr(expr, "filter/where")._expression,
                self._plan,
            )
        )

    @df_api_usage
    def sort(
        self,
        *cols: Union[ColumnOrName, Iterable[ColumnOrName]],
        ascending: Optional[Union[bool, int, List[Union[bool, int]]]] = None,
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
        exprs = self._convert_cols_to_exprs("sort()", *cols)
        if not exprs:
            raise ValueError("sort() needs at least one sort expression.")
        orders = []
        if ascending is not None:
            if isinstance(ascending, (list, tuple)):
                orders = [Ascending() if asc else Descending() for asc in ascending]
            elif isinstance(ascending, (bool, int)):
                orders = [Ascending() if ascending else Descending()]
            else:
                raise TypeError(
                    "ascending can only be boolean or list,"
                    " but got {}".format(str(type(ascending)))
                )
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

        if self._select_statement:
            return self._with_plan(self._select_statement.sort(sort_exprs))
        return self._with_plan(Sort(sort_exprs, self._plan))

    @experimental(version="1.5.0")
    def alias(self, name: str):
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
        _copy = copy.copy(self)
        _copy._alias = name
        for attr in self._plan.attributes:
            if _copy._select_statement:
                _copy._select_statement.df_aliased_col_name_to_real_col_name[name][
                    attr.name
                ] = attr.name  # attr is quoted already
            _copy._plan.df_aliased_col_name_to_real_col_name[name][
                attr.name
            ] = attr.name
        return _copy

    @df_api_usage
    def agg(
        self,
        *exprs: Union[Column, Tuple[ColumnOrName, str], Dict[str, str]],
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
        return self.group_by().agg(*exprs)

    @df_to_relational_group_df_api_usage
    def rollup(
        self,
        *cols: Union[ColumnOrName, Iterable[ColumnOrName]],
    ) -> "snowflake.snowpark.RelationalGroupedDataFrame":
        """Performs a SQL
        `GROUP BY ROLLUP <https://docs.snowflake.com/en/sql-reference/constructs/group-by-rollup.html>`_.
        on the DataFrame.

        Args:
            cols: The columns to group by rollup.
        """
        rollup_exprs = self._convert_cols_to_exprs("rollup()", *cols)
        return snowflake.snowpark.RelationalGroupedDataFrame(
            self,
            rollup_exprs,
            snowflake.snowpark.relational_grouped_dataframe._RollupType(),
        )

    @df_to_relational_group_df_api_usage
    def group_by(
        self,
        *cols: Union[ColumnOrName, Iterable[ColumnOrName]],
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
            >>> df.group_by("a").agg(sum_("b")).collect()
            [Row(A=1, SUM(B)=3), Row(A=2, SUM(B)=3), Row(A=3, SUM(B)=3)]
            >>> df.group_by("a").agg(sum_("b").alias("sum_b"), max_("b").alias("max_b")).collect()
            [Row(A=1, SUM_B=3, MAX_B=2), Row(A=2, SUM_B=3, MAX_B=2), Row(A=3, SUM_B=3, MAX_B=2)]
            >>> df.group_by(["a", lit("snow")]).agg(sum_("b")).collect()
            [Row(A=1, LITERAL()='snow', SUM(B)=3), Row(A=2, LITERAL()='snow', SUM(B)=3), Row(A=3, LITERAL()='snow', SUM(B)=3)]
            >>> df.group_by("a").agg((col("*"), "count"), max_("b")).collect()
            [Row(A=1, COUNT(LITERAL())=2, MAX(B)=2), Row(A=2, COUNT(LITERAL())=2, MAX(B)=2), Row(A=3, COUNT(LITERAL())=2, MAX(B)=2)]
            >>> df.group_by("a").median("b").collect()
            [Row(A=2, MEDIAN(B)=Decimal('1.500')), Row(A=3, MEDIAN(B)=Decimal('1.500')), Row(A=1, MEDIAN(B)=Decimal('1.500'))]
            >>> df.group_by("a").function("avg")("b").collect()
            [Row(A=1, AVG(B)=Decimal('1.500000')), Row(A=2, AVG(B)=Decimal('1.500000')), Row(A=3, AVG(B)=Decimal('1.500000'))]
        """
        grouping_exprs = self._convert_cols_to_exprs("group_by()", *cols)
        return snowflake.snowpark.RelationalGroupedDataFrame(
            self,
            grouping_exprs,
            snowflake.snowpark.relational_grouped_dataframe._GroupByType(),
        )

    @df_to_relational_group_df_api_usage
    def group_by_grouping_sets(
        self,
        *grouping_sets: Union[
            "snowflake.snowpark.GroupingSets",
            Iterable["snowflake.snowpark.GroupingSets"],
        ],
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
            >>> df.group_by_grouping_sets(GroupingSets([col("a")])).count().collect()
            [Row(A=1, COUNT=2), Row(A=3, COUNT=1)]
            >>> df.group_by_grouping_sets(GroupingSets(col("a"))).count().collect()
            [Row(A=1, COUNT=2), Row(A=3, COUNT=1)]
            >>> df.group_by_grouping_sets(GroupingSets([col("a")], [col("b")])).count().collect()
            [Row(A=1, B=None, COUNT=2), Row(A=3, B=None, COUNT=1), Row(A=None, B=2, COUNT=1), Row(A=None, B=4, COUNT=2)]
            >>> df.group_by_grouping_sets(GroupingSets([col("a"), col("b")], [col("c")])).count().collect()
            [Row(A=None, B=None, C=10, COUNT=1), Row(A=None, B=None, C=20, COUNT=1), Row(A=None, B=None, C=30, COUNT=1), Row(A=1, B=2, C=None, COUNT=1), Row(A=3, B=4, C=None, COUNT=1), Row(A=1, B=4, C=None, COUNT=1)]


        Args:
            grouping_sets: The list of :class:`GroupingSets` to group by.
        """
        return snowflake.snowpark.RelationalGroupedDataFrame(
            self,
            [gs._to_expression for gs in parse_positional_args_to_list(*grouping_sets)],
            snowflake.snowpark.relational_grouped_dataframe._GroupByType(),
        )

    @df_to_relational_group_df_api_usage
    def cube(
        self,
        *cols: Union[ColumnOrName, Iterable[ColumnOrName]],
    ) -> "snowflake.snowpark.RelationalGroupedDataFrame":
        """Performs a SQL
        `GROUP BY CUBE <https://docs.snowflake.com/en/sql-reference/constructs/group-by-cube.html>`_.
        on the DataFrame.

        Args:
            cols: The columns to group by cube.
        """
        cube_exprs = self._convert_cols_to_exprs("cube()", *cols)
        return snowflake.snowpark.RelationalGroupedDataFrame(
            self,
            cube_exprs,
            snowflake.snowpark.relational_grouped_dataframe._CubeType(),
        )

    @df_api_usage
    def distinct(self) -> "DataFrame":
        """Returns a new DataFrame that contains only the rows with distinct values
        from the current DataFrame.

        This is equivalent to performing a SELECT DISTINCT in SQL.
        """
        return self.group_by(
            [self.col(quote_name(f.name)) for f in self.schema.fields]
        ).agg()

    def drop_duplicates(self, *subset: Union[str, Iterable[str]]) -> "DataFrame":
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
        if not subset:
            df = self.distinct()
            adjust_api_subcalls(df, "DataFrame.drop_duplicates", len_subcalls=1)
            return df
        subset = parse_positional_args_to_list(*subset)

        filter_cols = [self.col(x) for x in subset]
        output_cols = [self.col(col_name) for col_name in self.columns]
        rownum = row_number().over(
            snowflake.snowpark.Window.partition_by(*filter_cols).order_by(*filter_cols)
        )
        rownum_name = generate_random_alphanumeric()
        df = (
            self.select(*output_cols, rownum.as_(rownum_name))
            .where(col(rownum_name) == 1)
            .select(output_cols)
        )
        # Reformat the extra API calls
        adjust_api_subcalls(df, "DataFrame.drop_duplicates", len_subcalls=3)
        return df

    @df_to_relational_group_df_api_usage
    def pivot(
        self,
        pivot_col: ColumnOrName,
        values: Optional[
            Union[Iterable[LiteralType], "snowflake.snowpark.DataFrame"]
        ] = None,
        default_on_null: Optional[LiteralType] = None,
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
        target_df, pc, pivot_values, default_on_null = prepare_pivot_arguments(
            self, "DataFrame.pivot", pivot_col, values, default_on_null
        )

        return snowflake.snowpark.RelationalGroupedDataFrame(
            target_df,
            [],
            snowflake.snowpark.relational_grouped_dataframe._PivotType(
                pc[0], pivot_values, default_on_null
            ),
        )

    @df_api_usage
    def unpivot(
        self, value_column: str, name_column: str, column_list: List[ColumnOrName]
    ) -> "DataFrame":
        """Rotates a table by transforming columns into rows.
        UNPIVOT is a relational operator that accepts two columns (from a table or subquery), along with a list of columns, and generates a row for each column specified in the list. In a query, it is specified in the FROM clause after the table name or subquery.
        Note that UNPIVOT is not exactly the reverse of PIVOT as it cannot undo aggregations made by PIVOT.

        Args:
            value_column: The name to assign to the generated column that will be populated with the values from the columns in the column list.
            name_column: The name to assign to the generated column that will be populated with the names of the columns in the column list.
            column_list: The names of the columns in the source table or subequery that will be narrowed into a single pivot column. The column names will populate ``name_column``, and the column values will populate ``value_column``.

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
        column_exprs = self._convert_cols_to_exprs("unpivot()", column_list)
        unpivot_plan = Unpivot(value_column, name_column, column_exprs, self._plan)

        if self._select_statement:
            return self._with_plan(
                SelectStatement(
                    from_=SelectSnowflakePlan(
                        unpivot_plan, analyzer=self._session._analyzer
                    ),
                    analyzer=self._session._analyzer,
                )
            )
        return self._with_plan(unpivot_plan)

    @df_api_usage
    def limit(self, n: int, offset: int = 0) -> "DataFrame":
        """Returns a new DataFrame that contains at most ``n`` rows from the current
        DataFrame, skipping ``offset`` rows from the beginning (similar to LIMIT and OFFSET in SQL).

        Note that this is a transformation method and not an action method.

        Args:
            n: Number of rows to return.
            offset: Number of rows to skip before the start of the result set. The default value is 0.

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
        if self._select_statement:
            return self._with_plan(self._select_statement.limit(n, offset=offset))
        return self._with_plan(Limit(Literal(n), Literal(offset), self._plan))

    @df_api_usage
    def union(self, other: "DataFrame") -> "DataFrame":
        """Returns a new DataFrame that contains all the rows in the current DataFrame
        and another DataFrame (``other``), excluding any duplicate rows. Both input
        DataFrames must contain the same number of columns.

        Example::
            >>> df1 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
            >>> df2 = session.create_dataframe([[0, 1], [3, 4]], schema=["c", "d"])
            >>> df1.union(df2).show()
            -------------
            |"A"  |"B"  |
            -------------
            |1    |2    |
            |3    |4    |
            |0    |1    |
            -------------
            <BLANKLINE>

        Args:
            other: the other :class:`DataFrame` that contains the rows to include.
        """
        if self._select_statement:
            return self._with_plan(
                self._select_statement.set_operator(
                    other._select_statement
                    or SelectSnowflakePlan(
                        other._plan, analyzer=self._session._analyzer
                    ),
                    operator=SET_UNION,
                )
            )
        return self._with_plan(UnionPlan(self._plan, other._plan, is_all=False))

    @df_api_usage
    def union_all(self, other: "DataFrame") -> "DataFrame":
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
        if self._select_statement:
            return self._with_plan(
                self._select_statement.set_operator(
                    other._select_statement
                    or SelectSnowflakePlan(
                        other._plan, analyzer=self._session._analyzer
                    ),
                    operator=SET_UNION_ALL,
                )
            )
        return self._with_plan(UnionPlan(self._plan, other._plan, is_all=True))

    @df_api_usage
    def union_by_name(self, other: "DataFrame") -> "DataFrame":
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

        Args:
            other: the other :class:`DataFrame` that contains the rows to include.
        """
        return self._union_by_name_internal(other, is_all=False)

    @df_api_usage
    def union_all_by_name(self, other: "DataFrame") -> "DataFrame":
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

        Args:
            other: the other :class:`DataFrame` that contains the rows to include.
        """
        return self._union_by_name_internal(other, is_all=True)

    def _union_by_name_internal(
        self, other: "DataFrame", is_all: bool = False
    ) -> "DataFrame":
        left_output_attrs = self._output
        right_output_attrs = other._output
        right_output_attr_by_name = {rattr.name: rattr for rattr in right_output_attrs}

        try:
            right_project_list = [
                right_output_attr_by_name[lattr.name] for lattr in left_output_attrs
            ]
        except KeyError:
            missing_lattrs = [
                lattr.name
                for lattr in left_output_attrs
                if lattr.name not in right_output_attr_by_name
            ]
            raise SnowparkClientExceptionMessages.DF_CANNOT_RESOLVE_COLUMN_NAME_AMONG(
                ", ".join(missing_lattrs),
                ", ".join(list(right_output_attr_by_name.keys())),
            )

        not_found_attrs = [
            rattr for rattr in right_output_attrs if rattr not in right_project_list
        ]

        names = right_project_list + not_found_attrs
        if self._session.sql_simplifier_enabled and other._select_statement:
            right_child = self._with_plan(other._select_statement.select(names))
        else:
            right_child = self._with_plan(Project(names, other._plan))

        if self._session.sql_simplifier_enabled:
            df = self._with_plan(
                self._select_statement.set_operator(
                    right_child._select_statement
                    or SelectSnowflakePlan(
                        right_child._plan, analyzer=self._session._analyzer
                    ),
                    operator=SET_UNION_ALL if is_all else SET_UNION,
                )
            )
        else:
            df = self._with_plan(UnionPlan(self._plan, right_child._plan, is_all))
        return df

    @df_api_usage
    def intersect(self, other: "DataFrame") -> "DataFrame":
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
        if self._select_statement:
            return self._with_plan(
                self._select_statement.set_operator(
                    other._select_statement
                    or SelectSnowflakePlan(
                        other._plan, analyzer=self._session._analyzer
                    ),
                    operator=SET_INTERSECT,
                )
            )
        return self._with_plan(Intersect(self._plan, other._plan))

    @df_api_usage
    def except_(self, other: "DataFrame") -> "DataFrame":
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
        if self._select_statement:
            return self._with_plan(
                self._select_statement.set_operator(
                    other._select_statement
                    or SelectSnowflakePlan(
                        other._plan, analyzer=self._session._analyzer
                    ),
                    operator=SET_EXCEPT,
                )
            )
        return self._with_plan(Except(self._plan, other._plan))

    @df_api_usage
    def natural_join(
        self, right: "DataFrame", how: Optional[str] = None, **kwargs
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
        join_type = kwargs.get("join_type") or how
        join_plan = Join(
            self._plan,
            right._plan,
            NaturalJoin(create_join_type(join_type or "inner")),
            None,
            None,
        )
        if self._select_statement:
            select_plan = self._session._analyzer.create_select_statement(
                from_=self._session._analyzer.create_select_snowflake_plan(
                    join_plan,
                    analyzer=self._session._analyzer,
                ),
                analyzer=self._session._analyzer,
            )
            return self._with_plan(select_plan)
        return self._with_plan(join_plan)

    @df_api_usage
    def join(
        self,
        right: "DataFrame",
        on: Optional[Union[ColumnOrName, Iterable[str]]] = None,
        how: Optional[str] = None,
        *,
        lsuffix: str = "",
        rsuffix: str = "",
        match_condition: Optional[Column] = None,
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
                - [Preview Feature] Asof join: "asof"

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
        join_type = kwargs.get("join_type") or how
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
                        f"match_condition is only accepted with join type 'asof' given: '{join_type}'"
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

            return self._join_dataframes(
                right,
                using_columns,
                create_join_type(join_type or "inner"),
                lsuffix=lsuffix,
                rsuffix=rsuffix,
                match_condition=match_condition,
            )

        raise TypeError("Invalid type for join. Must be Dataframe")

    @df_api_usage
    def join_table_function(
        self,
        func: Union[str, List[str], TableFunctionCall],
        *func_arguments: ColumnOrName,
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
            return self._with_plan(select_plan)
        if project_cols:
            return self._with_plan(Project(project_cols, join_plan))

        return self._with_plan(
            TableFunctionJoin(self._plan, func_expr, right_cols=new_col_names)
        )

    @df_api_usage
    def cross_join(
        self,
        right: "DataFrame",
        *,
        lsuffix: str = "",
        rsuffix: str = "",
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
        return self._join_dataframes_internal(
            right,
            create_join_type("cross"),
            None,
            lsuffix=lsuffix,
            rsuffix=rsuffix,
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
    ) -> "DataFrame":
        if isinstance(using_columns, Column):
            return self._join_dataframes_internal(
                right,
                join_type,
                join_exprs=using_columns,
                lsuffix=lsuffix,
                rsuffix=rsuffix,
                match_condition=match_condition,
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
                    )
                )
            return self._with_plan(join_logical_plan)

    def _join_dataframes_internal(
        self,
        right: "DataFrame",
        join_type: JoinType,
        join_exprs: Optional[Column],
        *,
        lsuffix: str = "",
        rsuffix: str = "",
        match_condition: Optional[Column] = None,
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
                )
            )
        return self._with_plan(join_logical_plan)

    @df_api_usage
    def with_column(
        self, col_name: str, col: Union[Column, TableFunctionCall]
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
        """
        return self.with_columns([col_name], [col])

    @df_api_usage
    def with_columns(
        self, col_names: List[str], values: List[Union[Column, TableFunctionCall]]
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

        # Get a list of existing column names that are not being replaced
        old_cols = [
            Column(field)
            for field in self._output
            if field.name not in new_column_names
        ]

        # Put it all together
        return self.select([*old_cols, *new_cols])

    @overload
    def count(
        self, *, statement_params: Optional[Dict[str, str]] = None, block: bool = True
    ) -> int:
        ...  # pragma: no cover

    @overload
    def count(
        self, *, statement_params: Optional[Dict[str, str]] = None, block: bool = False
    ) -> AsyncJob:
        ...  # pragma: no cover

    def count(
        self, *, statement_params: Optional[Dict[str, str]] = None, block: bool = True
    ) -> Union[int, AsyncJob]:
        """Executes the query representing this DataFrame and returns the number of
        rows in the result (similar to the COUNT function in SQL).

        Args:
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            block: A bool value indicating whether this function will wait until the result is available.
                When it is ``False``, this function executes the underlying queries of the dataframe
                asynchronously and returns an :class:`AsyncJob`.
        """
        with open_telemetry_context_manager(self.count, self):
            df = self.agg(("*", "count"))
            add_api_call(df, "DataFrame.count")
            result = df._internal_collect_with_tag(
                statement_params=statement_params,
                block=block,
                data_type=_AsyncResultType.COUNT,
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

        return self._writer

    @df_collect_api_telemetry
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
            copy_options: The kwargs that is used to specify the ``copyOptions`` of the ``COPY INTO <table>`` command.
        """
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
        return DataFrame(
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
            ),
        )._internal_collect_with_tag_no_telemetry(statement_params=statement_params)

    @df_collect_api_telemetry
    def show(
        self,
        n: int = 10,
        max_width: int = 50,
        *,
        statement_params: Optional[Dict[str, str]] = None,
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
                    ),
                )
            )

    @deprecated(
        version="0.7.0",
        extra_warning_text="Use `DataFrame.join_table_function()` instead.",
        extra_doc_string="Use :meth:`join_table_function` instead.",
    )
    @df_api_usage
    def flatten(
        self,
        input: ColumnOrName,
        path: Optional[str] = None,
        outer: bool = False,
        recursive: bool = False,
        mode: str = "BOTH",
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
        mode = mode.upper()
        if mode not in ("OBJECT", "ARRAY", "BOTH"):
            raise ValueError("mode must be one of ('OBJECT', 'ARRAY', 'BOTH')")

        if isinstance(input, str):
            input = self.col(input)
        return self._lateral(
            FlattenFunction(input._expression, path, outer, recursive, mode)
        )

    def _lateral(self, table_function: TableFunctionExpression) -> "DataFrame":
        result_columns = [
            attr.name
            for attr in self._session._analyzer.resolve(
                Lateral(self._plan, table_function)
            ).attributes
        ]
        common_col_names = [k for k, v in Counter(result_columns).items() if v > 1]
        if len(common_col_names) == 0:
            return DataFrame(self._session, Lateral(self._plan, table_function))
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
            ]
        )
        return DataFrame(self._session, Lateral(child._plan, table_function))

    def _show_string(self, n: int = 10, max_width: int = 50, **kwargs) -> str:
        query = self._plan.queries[-1].sql.strip().lower()

        if is_sql_select_statement(query):
            result, meta = self._session._conn.get_result_and_metadata(
                self.limit(n)._plan, **kwargs
            )
        else:
            res, meta = self._session._conn.get_result_and_metadata(
                self._plan, **kwargs
            )
            result = res[:n]

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

    @df_collect_api_telemetry
    def create_or_replace_view(
        self,
        name: Union[str, Iterable[str]],
        *,
        comment: Optional[str] = None,
        statement_params: Optional[Dict[str, str]] = None,
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
            statement_params: Dictionary of statement level parameters to be set while executing this action.
        """
        if isinstance(name, str):
            formatted_name = name
        elif isinstance(name, (list, tuple)) and all(isinstance(n, str) for n in name):
            formatted_name = ".".join(name)
        else:
            raise TypeError(
                "The input of create_or_replace_view() can only a str or list of strs."
            )

        return self._do_create_or_replace_view(
            formatted_name,
            PersistedView(),
            comment=comment,
            _statement_params=create_or_update_statement_params_with_query_tag(
                statement_params or self._statement_params,
                self._session.query_tag,
                SKIP_LEVELS_TWO,
            ),
        )

    @df_collect_api_telemetry
    def create_or_replace_dynamic_table(
        self,
        name: Union[str, Iterable[str]],
        *,
        warehouse: str,
        lag: str,
        comment: Optional[str] = None,
        statement_params: Optional[Dict[str, str]] = None,
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
            statement_params: Dictionary of statement level parameters to be set while executing this action.
        """
        if isinstance(name, str):
            formatted_name = name
        elif isinstance(name, (list, tuple)) and all(isinstance(n, str) for n in name):
            formatted_name = ".".join(name)
        else:
            raise TypeError(
                "The name input of create_or_replace_dynamic_table() can only be a str or list of strs."
            )

        if not isinstance(warehouse, str):
            raise TypeError(
                "The warehouse input of create_or_replace_dynamic_table() can only be a str."
            )

        if not isinstance(lag, str):
            raise TypeError(
                "The lag input of create_or_replace_dynamic_table() can only be a str."
            )

        return self._do_create_or_replace_dynamic_table(
            formatted_name,
            warehouse,
            lag,
            comment,
            _statement_params=create_or_update_statement_params_with_query_tag(
                statement_params, self._session.query_tag, SKIP_LEVELS_TWO
            ),
        )

    @df_collect_api_telemetry
    def create_or_replace_temp_view(
        self,
        name: Union[str, Iterable[str]],
        *,
        comment: Optional[str] = None,
        statement_params: Optional[Dict[str, str]] = None,
    ) -> List[Row]:
        """Creates a temporary view that returns the same results as this DataFrame.

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
        if isinstance(name, str):
            formatted_name = name
        elif isinstance(name, (list, tuple)) and all(isinstance(n, str) for n in name):
            formatted_name = ".".join(name)
        else:
            raise TypeError(
                "The input of create_or_replace_temp_view() can only a str or list of strs."
            )

        return self._do_create_or_replace_view(
            formatted_name,
            LocalTempView(),
            comment=comment,
            _statement_params=create_or_update_statement_params_with_query_tag(
                statement_params or self._statement_params,
                self._session.query_tag,
                SKIP_LEVELS_TWO,
            ),
        )

    def _do_create_or_replace_view(
        self, view_name: str, view_type: ViewType, comment: Optional[str], **kwargs
    ):
        validate_object_name(view_name)
        cmd = CreateViewCommand(
            view_name,
            view_type,
            comment,
            self._plan,
        )

        return self._session._conn.execute(
            self._session._analyzer.resolve(cmd), **kwargs
        )

    def _do_create_or_replace_dynamic_table(
        self, name: str, warehouse: str, lag: str, comment: Optional[str], **kwargs
    ):
        validate_object_name(name)
        cmd = CreateDynamicTableCommand(
            name,
            warehouse,
            lag,
            comment,
            self._plan,
        )

        return self._session._conn.execute(
            self._session._analyzer.resolve(cmd), **kwargs
        )

    @overload
    def first(
        self,
        n: Optional[int] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
    ) -> Union[Optional[Row], List[Row]]:
        ...  # pragma: no cover

    @overload
    def first(
        self,
        n: Optional[int] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = False,
    ) -> AsyncJob:
        ...  # pragma: no cover

    def first(
        self,
        n: Optional[int] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
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
        if n is None:
            df = self.limit(1)
            add_api_call(df, "DataFrame.first")
            result = df._internal_collect_with_tag(
                statement_params=statement_params, block=block
            )
            if not block:
                return result
            return result[0] if result else None
        elif not isinstance(n, int):
            raise ValueError(f"Invalid type of argument passed to first(): {type(n)}")
        elif n < 0:
            return self._internal_collect_with_tag(
                statement_params=statement_params, block=block
            )
        else:
            df = self.limit(n)
            add_api_call(df, "DataFrame.first")
            return df._internal_collect_with_tag(
                statement_params=statement_params, block=block
            )

    take = first

    @df_api_usage
    def sample(
        self, frac: Optional[float] = None, n: Optional[int] = None
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
        sample_plan = Sample(self._plan, probability_fraction=frac, row_count=n)
        if self._select_statement:
            return self._with_plan(
                self._session._analyzer.create_select_statement(
                    from_=self._session._analyzer.create_select_snowflake_plan(
                        sample_plan, analyzer=self._session._analyzer
                    ),
                    analyzer=self._session._analyzer,
                )
            )
        return self._with_plan(sample_plan)

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
    def session(self) -> "snowflake.snowpark.Session":
        """
        Returns a :class:`snowflake.snowpark.Session` object that provides access to the session the current DataFrame is relying on.
        """
        return self._session

    def describe(self, *cols: Union[str, List[str]]) -> "DataFrame":
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
        """
        cols = parse_positional_args_to_list(*cols)
        df = self.select(cols) if len(cols) > 0 else self

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
                list(stat_func_dict.keys()), schema=["summary"]
            )
            # We need to set the API calls for this to same API calls for describe
            # Also add the new API calls for creating this DataFrame to the describe subcalls
            adjust_api_subcalls(
                df,
                "DataFrame.describe",
                precalls=self._plan.api_calls,
                subcalls=df._plan.api_calls,
            )
            return df

        # otherwise, calculate stats
        res_df = None
        for name, func in stat_func_dict.items():
            agg_cols = []
            for c, t in numerical_string_col_type_dict.items():
                # for string columns, we need to convert all stats to string
                # such that they can be fitted into one column
                if isinstance(t, StringType):
                    if name in ["mean", "stddev"]:
                        agg_cols.append(to_char(func(lit(None))).as_(c))
                    else:
                        agg_cols.append(to_char(func(c)))
                else:
                    agg_cols.append(func(c))
            agg_stat_df = (
                self.agg(agg_cols)
                .to_df(list(numerical_string_col_type_dict.keys()))
                .select(
                    lit(name).as_("summary"), *numerical_string_col_type_dict.keys()
                )
            )
            res_df = res_df.union(agg_stat_df) if res_df else agg_stat_df

        adjust_api_subcalls(
            res_df,
            "DataFrame.describe",
            precalls=self._plan.api_calls,
            subcalls=res_df._plan.api_calls.copy(),
        )
        return res_df

    @df_api_usage
    def rename(
        self,
        col_or_mapper: Union[ColumnOrName, dict],
        new_column: str = None,
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
        if new_column is not None:
            return self.with_column_renamed(col_or_mapper, new_column)

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
            return self._with_plan(select_plan)

        return self._with_plan(rename_plan)

    @df_api_usage
    def with_column_renamed(self, existing: ColumnOrName, new: str) -> "DataFrame":
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
        return self.select(new_columns)

    @df_collect_api_telemetry
    def cache_result(
        self, *, statement_params: Optional[Dict[str, str]] = None
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
        """
        from snowflake.snowpark.mock._connection import MockServerConnection

        temp_table_name = self._session.get_fully_qualified_name_if_possible(
            f'"{random_name_for_temp_object(TempObjectType.TABLE)}"'
        )

        if isinstance(self._session._conn, MockServerConnection):
            self.write.save_as_table(temp_table_name, create_temp_table=True)
        else:
            create_temp_table = self._session._analyzer.plan_builder.create_temp_table(
                temp_table_name,
                self._plan,
                use_scoped_temp_objects=self._session._use_scoped_temp_objects,
                is_generated=True,
            )
            self._session._conn.execute(
                create_temp_table,
                _statement_params=create_or_update_statement_params_with_query_tag(
                    statement_params or self._statement_params,
                    self._session.query_tag,
                    SKIP_LEVELS_TWO,
                ),
            )
        cached_df = self._session.table(temp_table_name)
        cached_df.is_cached = True
        return cached_df

    @df_collect_api_telemetry
    def random_split(
        self,
        weights: List[float],
        seed: Optional[int] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
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
            seed: The seed for sampling.
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
        """
        if not weights:
            raise ValueError(
                "weights can't be None or empty and must be positive numbers"
            )
        elif len(weights) == 1:
            return [self]
        else:
            for w in weights:
                if w <= 0:
                    raise ValueError("weights must be positive numbers")

            temp_column_name = random_name_for_temp_object(TempObjectType.COLUMN)
            cached_df = self.with_column(
                temp_column_name, abs_(random(seed)) % _ONE_MILLION
            ).cache_result(statement_params=statement_params)
            sum_weights = sum(weights)
            normalized_cum_weights = [0] + [
                int(w * _ONE_MILLION)
                for w in list(itertools.accumulate([w / sum_weights for w in weights]))
            ]
            normalized_boundaries = zip(
                normalized_cum_weights[:-1], normalized_cum_weights[1:]
            )
            res_dfs = [
                cached_df.where(
                    (col(temp_column_name) >= lower_bound)
                    & (col(temp_column_name) < upper_bound)
                ).drop(temp_column_name)
                for lower_bound, upper_bound in normalized_boundaries
            ]
            return res_dfs

    @property
    def queries(self) -> Dict[str, List[str]]:
        """
        Returns a ``dict`` that contains a list of queries that will be executed to
        evaluate this DataFrame with the key `queries`, and a list of post-execution
        actions (e.g., queries to clean up temporary objects) with the key `post_actions`.
        """
        plan = self._plan.replace_repeated_subquery_with_cte()
        return {
            "queries": [query.sql.strip() for query in plan.queries],
            "post_actions": [query.sql.strip() for query in plan.post_actions],
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
        plan = self._plan.replace_repeated_subquery_with_cte()
        output_queries = "\n---\n".join(
            f"{i+1}.\n{query.sql.strip()}" for i, query in enumerate(plan.queries)
        )
        msg = f"""---------DATAFRAME EXECUTION PLAN----------
Query List:
{output_queries}"""
        # if query list contains more then one queries, skip execution plan
        if len(plan.queries) == 1:
            exec_plan = self._session._explain_query(plan.queries[0].sql)
            if exec_plan:
                msg = f"{msg}\nLogical Execution Plan:\n{exec_plan}"
            else:
                msg = f"{plan.queries[0].sql} can't be explained"

        return f"{msg}\n--------------------------------------------"

    def _resolve(self, col_name: str) -> Union[Expression, NamedExpression]:
        normalized_col_name = quote_name(col_name)
        cols = list(filter(lambda attr: attr.name == normalized_col_name, self._output))
        if len(cols) == 1:
            return cols[0].with_name(normalized_col_name)
        else:
            raise SnowparkClientExceptionMessages.DF_CANNOT_RESOLVE_COLUMN_NAME(
                col_name
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

    def _with_plan(self, plan) -> "DataFrame":
        df = DataFrame(self._session, plan)
        df._statement_params = self._statement_params
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
                return self._resolve(col)
            elif isinstance(col, Column):
                return col._expression
            else:
                raise TypeError(
                    "{} only accepts str and Column objects, or a list containing str and"
                    " Column objects".format(calling_method)
                )

        exprs = [convert(col) for col in parse_positional_args_to_list(*cols)]
        return exprs

    def print_schema(self) -> None:
        schema_tmp_str = "\n".join(
            [
                f" |-- {attr.name}: {attr.datatype} (nullable = {str(attr.nullable)})"
                for attr in self._plan.attributes
            ]
        )
        print(f"root\n{schema_tmp_str}")  # noqa: T201: we need to print here.

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
