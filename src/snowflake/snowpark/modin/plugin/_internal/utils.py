#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import json
import logging
import re
import traceback
from collections.abc import Hashable, Iterable, Sequence
from enum import Enum
from typing import TYPE_CHECKING, Any, Optional, Union
from packaging import version

import modin.pandas as pd
import numpy as np
import pandas as native_pd
from pandas._typing import AnyArrayLike, Scalar
from pandas.core.dtypes.base import ExtensionDtype
from pandas.core.dtypes.common import (
    is_bool_dtype,
    is_integer_dtype,
    is_object_dtype,
    is_scalar,
)
from pandas.core.dtypes.inference import is_list_like

import snowflake.snowpark.modin.plugin._internal.statement_params_constants as STATEMENT_PARAMS
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    DOUBLE_QUOTE,
    EMPTY_STRING,
    quote_name_without_upper_casing,
)
from snowflake.snowpark._internal.analyzer.expression import Literal
from snowflake.snowpark._internal.type_utils import LiteralType
from snowflake.snowpark._internal.utils import (
    SNOWFLAKE_OBJECT_RE_PATTERN,
    TempObjectType,
    generate_random_alphanumeric,
    get_temp_type_for_object,
    random_name_for_temp_object,
)
from snowflake.snowpark.column import Column
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import (
    col,
    equal_nan,
    floor,
    iff,
    to_char,
    to_timestamp_ntz,
    to_timestamp_tz,
    typeof,
)
from snowflake.snowpark.modin.plugin._internal.ordered_dataframe import (
    DataFrameReference,
    OrderedDataFrame,
    OrderingColumn,
)
from snowflake.snowpark.modin.plugin._internal.snowpark_pandas_types import (
    SnowparkPandasType,
    TimedeltaType,
    ensure_snowpark_python_type,
)
from snowflake.snowpark.modin.plugin._typing import LabelTuple
from snowflake.snowpark.modin.plugin.utils.exceptions import (
    SnowparkPandasErrorCode,
    SnowparkPandasException,
)
from snowflake.snowpark.modin.plugin.utils.warning_message import (
    ORDER_BY_IN_SQL_QUERY_NOT_GUARANTEED_WARNING,
    WarningMessage,
)
from snowflake.snowpark.types import (
    ArrayType,
    DataType,
    DecimalType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampTimeZone,
    TimestampType,
    VariantType,
    _FractionalType,
)

if TYPE_CHECKING:
    from snowflake.snowpark.modin.plugin._internal import frame
    from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
        SnowflakeQueryCompiler,
    )

ROW_POSITION_COLUMN_LABEL = "row_position"
MAX_ROW_POSITION_COLUMN_LABEL = f"MAX_{ROW_POSITION_COLUMN_LABEL}"
SAMPLED_ROW_POSITION_COLUMN_LABEL = f"sampled_{ROW_POSITION_COLUMN_LABEL}"
INDEX_LABEL = "index"
# label used for data column to create the snowflake quoted identifier when the pandas
# label for the column is None
DEFAULT_DATA_COLUMN_LABEL = "data"
LEVEL_LABEL = "level"
ITEM_VALUE_LABEL = "item_value"
ORDERING_COLUMN_LABEL = "ordering"
READ_ONLY_TABLE_SUFFIX = "READONLY"
METADATA_ROW_POSITION_COLUMN = "METADATA$ROW_POSITION"
# read only table is only supported for base table or base temporary table
READ_ONLY_TABLE_SUPPORTED_TABLE_KINDS = ["LOCAL TEMPORARY", "BASE TABLE"]
UNDEFINED = "undefined"
# number of digits used to generate a random suffix
_NUM_SUFFIX_DIGITS = 4
ROW_COUNT_COLUMN_LABEL = "row_count"

# max number of retries used when generating conflict free quoted identifiers
_MAX_NUM_RETRIES = 3
_MAX_IDENTIFIER_LENGTH = 32

_logger = logging.getLogger(__name__)


# Flag guarding certain features available only in newer modin versions.
# Snowpark pandas supports the newest two released versions of modin; update this flag and remove legacy
# code as needed when we bump dependency versions.
MODIN_IS_AT_LEAST_0_34_0 = version.parse(pd.__version__) >= version.parse("0.34.0")


# This is the default statement parameters for queries from Snowpark pandas API. It provides the fine grain metric for
# the server to track all pandas API usage.
def get_default_snowpark_pandas_statement_params() -> dict[str, str]:
    return {STATEMENT_PARAMS.SNOWPARK_API: STATEMENT_PARAMS.PANDAS_API}


class FillNAMethod(Enum):
    """
    Enum that defines the fillna methods - ffill for forward filling, and bfill for backfilling.
    """

    FFILL_METHOD = "ffill"
    BFILL_METHOD = "bfill"

    @classmethod
    def get_enum_for_string_method(cls, method_name: str) -> "FillNAMethod":
        """
        Returns the appropriate Enum member for the given method.

        Args:
            method_name : str
                The name of the method to use for fillna.

        Returns:
            FillNAMethod : The instance of the Enum corresponding to the specified method name,
                           or a ValueError if none match.

        Notes:
            This method is necessary since the two methods (ffill and bfill) have aliases - pad for ffill
            and backfill for bfill. Rather than having four members of this Enum, we'd rather just map
            `pad` to FillNAMethod.FFILL_METHOD and `backfill` to FillNAMethod.BFILL_METHOD so we don't have
            to check if a method is one of two Enum values that are functionally the same.
        """
        try:
            return cls(method_name)
        except ValueError:
            if method_name == "pad":
                return cls("ffill")
            elif method_name == "backfill":
                return cls("bfill")
            else:
                raise ValueError(
                    f"Invalid fillna method: {method_name}. Expected one of ['ffill', 'pad', 'bfill', 'backfill']"
                )


def _is_table_name(table_name_or_query: str) -> bool:
    """
    Checks whether the provided string is a table name or not.

    Args:
        table_name_or_query: the string to check

    Returns:
        True if it is a valid table name.
    """
    # SNOWFLAKE_OBJECT_RE_PATTERN contains the pattern for identifiers in Snowflake.
    # If we do not get a match for the SNOWFLAKE_OBJECT_RE_PATTERN, we know that
    # the string passed in is not a valid identifier, so it cannot be a table name.
    return SNOWFLAKE_OBJECT_RE_PATTERN.match(table_name_or_query) is not None


def _check_if_sql_query_contains_order_by_and_warn_user(
    sql_query_text: str,
) -> bool:
    """
    Checks whether the sql query passed in contains an order by clause
    and warns the user that ORDER BY will be ignored currently.

    Args:
        sql_query_text: The SQL Query to check.

    Returns:
        Whether or not the query contains an order by.
    """
    # We need to determine if the query contains an ORDER BY. We previously looked
    # at the logical plan in order to determine if there was an ORDER BY; however,
    # the output schema of the `EXPLAIN` SQL statement (which was used to generate
    # the logical plan) seems to not be stable across releases, so instead, we
    # check to see if the text of the SQL query contains "ORDER BY".
    # Note: This method may sometimes raise false positives, e.g.:
    # SELECT "ORDER BY COLUMN", * FROM TABLE;
    # The above query shouldn't raise a warning, but since we are using
    # string matching, we will get a false positive and raise a warning.
    order_by_pattern = r"\s+order\s+by\s+"
    contains_order_by = re.search(order_by_pattern, sql_query_text.lower()) is not None
    if contains_order_by:
        # If the query contains an ORDER BY, we need to warn the user that
        # the ordering induced by the ORDER BY is not guaranteed to be preserved
        # in the ordering of the returned DataFrame, and that they should use
        # sort_values on the returned object to guarantee an ordering.
        WarningMessage.single_warning(ORDER_BY_IN_SQL_QUERY_NOT_GUARANTEED_WARNING)
    return contains_order_by


def _extract_base_table_from_simple_select_star_query(sql_query: str) -> str:
    """
    Takes a SQL Query or table name as input, and attempts to reduce it to its base table name
    if it is of the form SELECT * FROM table. Otherwise, returns the original query or table name.

    Returns:
        str
        The base table name or SQL Query.
    """
    base_table_name = None
    if not _is_table_name(sql_query):
        # We first need to find all occurences of `select * from`, since the query may be nested.
        select_star_match = re.match(r"select \* from ", sql_query.lower())
        if select_star_match is not None:
            snowflake_object_match = re.fullmatch(
                SNOWFLAKE_OBJECT_RE_PATTERN, sql_query[select_star_match.end() :]  # type: ignore[union-attr]
            )
            # snowflake_object_match will only be None if whatever followed `select * from` in
            # our original query did not match the regex for a Snowflake Object. This could be the
            # case, e.g., when our query looks like `select * from (select * from OBJECT)`.
            # If it is not None, then we should extract the object that was found as the
            # base table name.
            if snowflake_object_match:
                base_table_name = snowflake_object_match.group()
    return sql_query if base_table_name is None else base_table_name


def _create_read_only_table(
    table_name: str,
    materialize_into_temp_table: bool,
    materialization_reason: Optional[str] = None,
) -> str:
    """
    create read only table for the given table.

    Args:
        table_name: the table to create read only table on top of.
        materialize_into_temp_table: whether to create a temp table of the given table. If true, the
                    read only table will be created on top of the temp table instead of
                    the original table.
                    Read only table creation is only supported for temporary table or regular
                    table at this moment. If the table is not those two types, you can set
                    materialize_into_temp_table to True to create a temporary table out of it for
                    read only table creation. Otherwise, creation will fail.
        materialization_reason: why materialization into temp table is needed for creation of the read
                    only table. This is only needed when materialization_into_temp_table is true.
    Returns:
        The name of the read only table created.
    """
    session = pd.session

    # use random_name_for_temp_object, there is a check at server side for
    # temp object used in snowpark stored proc, which needs to match the following regExpr
    # "^SNOWPARK_TEMP_(TABLE|VIEW|STAGE|FUNCTION|TABLE_FUNCTION|FILE_FORMAT|PROCEDURE)_[0-9A-Z]+$")
    readonly_table_name = (
        f"{random_name_for_temp_object(TempObjectType.TABLE)}{READ_ONLY_TABLE_SUFFIX}"
    )
    use_scoped_temp_table = session._use_scoped_temp_read_only_table
    # If we need to materialize into a temp table our create table expression
    # needs to be SELECT * FROM (object).
    if materialize_into_temp_table:
        ctas_query = f"SELECT * FROM {table_name}"
        temp_table_name = random_name_for_temp_object(TempObjectType.TABLE)

        _logger.warning(
            f"Snapshot source table/view '{table_name}' failed due to reason: `{materialization_reason}'. Data from "
            f"source table/view '{table_name}' is being copied into a new "
            f"temporary table '{temp_table_name}' for snapshotting. DataFrame creation might take some time."
        )

        statement_params = get_default_snowpark_pandas_statement_params()
        # record 1) original table name (which may not be an actual table)
        #        2) the name for the new temp table that has been created/materialized
        #        3) the reason why materialization happens
        new_params = {
            STATEMENT_PARAMS.MATERIALIZATION_TABLE_NAME: temp_table_name,
            STATEMENT_PARAMS.MATERIALIZATION_REASON: materialization_reason
            if materialization_reason is not None
            else STATEMENT_PARAMS.UNKNOWN,
        }
        statement_params.update(new_params)
        session.sql(
            f"CREATE OR REPLACE {get_temp_type_for_object(use_scoped_temp_objects=use_scoped_temp_table, is_generated=True)} TABLE {temp_table_name} AS {ctas_query}"
        ).collect(statement_params=statement_params)
        table_name = temp_table_name

    statement_params = get_default_snowpark_pandas_statement_params()
    # record the actual table that the read only table is created on top of, and also the name of the
    # read only table that is created.
    statement_params.update(
        {
            STATEMENT_PARAMS.READONLY_SOURCE_TABLE_NAME: table_name,
            STATEMENT_PARAMS.READONLY_TABLE_NAME: readonly_table_name,
        }
    )
    # TODO (SNOW-1669224): pushing read only table creation down to snowpark for general usage
    session.sql(
        f"CREATE OR REPLACE {get_temp_type_for_object(use_scoped_temp_objects=use_scoped_temp_table, is_generated=True)} READ ONLY TABLE {readonly_table_name} CLONE {table_name}",
        _emit_ast=False,
    ).collect(statement_params=statement_params, _emit_ast=False)

    return readonly_table_name


def create_initial_ordered_dataframe(
    table_name_or_query: Union[str, Iterable[str]],
    enforce_ordering: bool,
) -> tuple[OrderedDataFrame, str]:
    """
    create read only temp table on top of the existing table or Snowflake query if required, and create a OrderedDataFrame
    with row position column using the read only temp table created or directly using the existing table.

    Args:
        table_name_or_query: A string or list of strings that specify the table name or
            fully-qualified object identifier (database name, schema name, and table name) or SQL query.
        enforce_ordering: If True, create a read only temp table on top of the existing table or Snowflake query,
            and create the OrderedDataFrame using the read only temp table created.
            Otherwise, directly using the existing table.

    Returns:
        OrderedDataFrame with row position column.
        snowflake quoted identifier for the row position column.
    """
    if not isinstance(table_name_or_query, str) and isinstance(
        table_name_or_query, Iterable
    ):
        table_name_or_query = ".".join(table_name_or_query)

    session = pd.session
    # `table_name_or_query` can be either a table name or a query. If it is a query of the form
    # SELECT * FROM table, we can parse out the base table name, and treat it as though the user
    # called `pd.read_snowflake("table")` instead of treating it as a SQL query, which will result
    # in the materialization of an additional temporary table. Since that is the case, we first
    # see if the coercion can happen, before determining if we are dealing with a query or not.
    table_name_or_query = _extract_base_table_from_simple_select_star_query(
        table_name_or_query
    )
    is_query = not _is_table_name(table_name_or_query)
    if not is_query or not enforce_ordering:
        if enforce_ordering:
            try:
                readonly_table_name = _create_read_only_table(
                    table_name=table_name_or_query,
                    materialize_into_temp_table=False,
                )
            except SnowparkSQLException as ex:
                _logger.debug(
                    f"Failed to create read only table for {table_name_or_query}: {ex}"
                )
                # Creation of read only table fails for following cases which are not possible
                # (or very difficult) to detect on client side in advance. We explicitly check
                # for these errors and create a temporary table by copying the content of the
                # original table and then create the read only table on the top of this
                # temporary table.
                # 1. Row access Policy:
                #   If the table has row access policy associated, read only table creation will
                #   fail. SNOW-850878 is created to support the query for row access policy on
                #   server side.
                # 2. Table can not be cloned:
                #   Clone is not supported for tables that are imported from a share, views etc.
                # 3. Table doesn't support read only table creation:
                #   Includes iceberg table, hybrid table etc.
                known_errors = (
                    "Row access policy is not supported on read only table",  # case 1
                    "Cannot clone",  # case 2
                    "Unsupported feature",  # case 3
                    "Clone Iceberg table should use CREATE ICEBERG TABLE CLONE command",  # case 3
                )
                if any(error in ex.message for error in known_errors):
                    readonly_table_name = _create_read_only_table(
                        table_name=table_name_or_query,
                        materialize_into_temp_table=True,
                        materialization_reason=ex.message,
                    )
                else:
                    raise SnowparkPandasException(
                        f"Failed to create Snowpark pandas DataFrame out of table {table_name_or_query} with error {ex}",
                        error_code=SnowparkPandasErrorCode.GENERAL_SQL_EXCEPTION.value,
                    ) from ex

        if is_query:
            # If the string passed in to `pd.read_snowflake` is a SQL query, we can simply create
            # a Snowpark DataFrame, and convert that to a Snowpark pandas DataFrame, and extract
            # the OrderedDataFrame and row_position_snowflake_quoted_identifier from there.
            # If there is an ORDER BY in the query, we should log it.
            contains_order_by = _check_if_sql_query_contains_order_by_and_warn_user(
                table_name_or_query
            )
            statement_params = get_default_snowpark_pandas_statement_params()
            statement_params[STATEMENT_PARAMS.CONTAINS_ORDER_BY] = str(
                contains_order_by
            ).upper()

        initial_ordered_dataframe = OrderedDataFrame(
            DataFrameReference(session.table(readonly_table_name, _emit_ast=False))
            if enforce_ordering
            else DataFrameReference(session.sql(table_name_or_query, _emit_ast=False))
            if is_query
            else DataFrameReference(session.table(table_name_or_query, _emit_ast=False))
        )
        # generate a snowflake quoted identifier for row position column that can be used for aliasing
        snowflake_quoted_identifiers = (
            initial_ordered_dataframe.projected_column_snowflake_quoted_identifiers
        )
        row_position_snowflake_quoted_identifier = (
            initial_ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=[ROW_POSITION_COLUMN_LABEL],
                wrap_double_underscore=True,
            )[0]
        )

        # create snowpark dataframe with columns: row_position_snowflake_quoted_identifier + snowflake_quoted_identifiers
        # if no snowflake_quoted_identifiers is specified, all columns will be selected
        if enforce_ordering:
            row_position_column_str = f"{METADATA_ROW_POSITION_COLUMN} as {row_position_snowflake_quoted_identifier}"
        else:
            row_position_column_str = f"ROW_NUMBER() OVER (ORDER BY 1) - 1 as {row_position_snowflake_quoted_identifier}"

        columns_to_select = ", ".join(
            [row_position_column_str] + snowflake_quoted_identifiers
        )
        # Create or get the row position columns requires access to the metadata column of the table.
        # However, snowpark_df = session().table(table_name) generates query (SELECT * from <table_name>),
        # which creates a view without metadata column, we won't be able to access the metadata columns
        # with the created snowpark dataframe. In order to get the metadata column access in the created
        # dataframe, we create dataframe through sql which access the corresponding metadata column.
        if enforce_ordering:
            dataframe_sql = f"SELECT {columns_to_select} FROM {readonly_table_name}"
        else:
            dataframe_sql = f"SELECT {columns_to_select} FROM ({table_name_or_query})"
        snowpark_df = session.sql(dataframe_sql, _emit_ast=False)
        # assert dataframe_sql is None

        result_columns_quoted_identifiers = [
            row_position_snowflake_quoted_identifier
        ] + snowflake_quoted_identifiers
        ordered_dataframe = OrderedDataFrame(
            DataFrameReference(snowpark_df, result_columns_quoted_identifiers),
            projected_column_snowflake_quoted_identifiers=result_columns_quoted_identifiers,
            ordering_columns=[OrderingColumn(row_position_snowflake_quoted_identifier)],
            row_position_snowflake_quoted_identifier=row_position_snowflake_quoted_identifier,
        )
    else:
        assert is_query and enforce_ordering

        # If the string passed in to `pd.read_snowflake` is a SQL query, we can simply create
        # a Snowpark DataFrame, and convert that to a Snowpark pandas DataFrame, and extract
        # the OrderedDataFrame and row_position_snowflake_quoted_identifier from there.
        # If there is an ORDER BY in the query, we should log it.
        contains_order_by = _check_if_sql_query_contains_order_by_and_warn_user(
            table_name_or_query
        )
        statement_params = get_default_snowpark_pandas_statement_params()
        statement_params[STATEMENT_PARAMS.CONTAINS_ORDER_BY] = str(
            contains_order_by
        ).upper()
        try:
            # When we call `to_snowpark_pandas`, Snowpark will create a temporary table out of the
            # Snowpark DataFrame, and then call `pd.read_snowflake` on it, which will create a
            # Read only clone of that temporary table. We need to create the second table (instead
            # of just using the temporary table Snowpark creates with a row position column as our
            # backing table) because there are no guarantees that a temporary table cannot be modified
            # so we lose the data isolation quality of pandas that we are attempting to replicate. By
            # creating a read only clone, we ensure that the underlying data cannot be modified by anyone
            # else.
            snowpark_pandas_df = session.sql(table_name_or_query).to_snowpark_pandas(
                enforce_ordering=enforce_ordering
            )
        except SnowparkSQLException as ex:
            raise SnowparkPandasException(
                f"Failed to create Snowpark pandas DataFrame out of query {table_name_or_query} with error {ex}",
                error_code=SnowparkPandasErrorCode.GENERAL_SQL_EXCEPTION.value,
            ) from ex
        ordered_dataframe = (
            snowpark_pandas_df._query_compiler._modin_frame.ordered_dataframe
        )
        row_position_snowflake_quoted_identifier = (
            ordered_dataframe.row_position_snowflake_quoted_identifier
        )
    # Set the materialized row count
    materialized_row_count = ordered_dataframe._dataframe_ref.snowpark_dataframe.count(
        statement_params=get_default_snowpark_pandas_statement_params(), _emit_ast=False
    )
    ordered_dataframe.row_count = materialized_row_count
    ordered_dataframe.row_count_upper_bound = materialized_row_count
    return ordered_dataframe, row_position_snowflake_quoted_identifier


def generate_snowflake_quoted_identifiers_helper(
    *,
    pandas_labels: list[Hashable],
    excluded: Optional[list[str]] = None,
    wrap_double_underscore: Optional[bool] = False,
) -> list[str]:
    """
    Args:
        pandas_labels: a list of pandas labels to generate snowflake quoted identifiers for.
            For debug-ability the newly generated name will be generated by appending a number to resolve name
            conflicts.
        excluded: a list of snowflake quoted identifiers as strings. If not None, generated snowflake identifiers
            can not be from this list and will not have conflicts among themselves.
            When excluded is None, not conflict resolution happens, generated snowflake quoted snowflake identifiers
            may have conflicts.
        wrap_double_underscore: optional parameter to wrap the resolved prefix to produce a name '__<resolved prefix>__'

    Generate a unique snowflake quoted identifier for each label in `pandas_labels`, that does not
    conflict with identifiers submitted by `excluded`. If treating label as snowflake quoted identifier
    leads to a conflict, attempt once to resolve conflict by appending a random suffix.
    Fail when single attempt of appending a random suffix does not resolve conflict. The default
    length of the random suffix is defined through _NUM_SUFFIX_DIGITS.

    Examples:
        generate_snowflake_quoted_identifiers(excluded=['"A"', '"B"', '"C"'], pandas_label='X')
            returns ['"X"'] because it doesn't conflict with 'A', 'B', 'C'
        generate_snowflake_quoted_identifiers(excluded=['"A"', '"B"', '"C"'], pandas_label='A')
            returns ['"A_<random_suffix>"'] because '"A"' already exists
        generate_snowflake_quoted_identifiers(excluded=['"__A__"', '"B"', '"C"'], pandas_label='A')
            returns ['"A"']
        generate_snowflake_quoted_identifiers(excluded=['"__A__"', '"B"', '"C"'], pandas_label='A', wrap_double_underscore=True)
            returns ['"__A_<random_suffix>__"'] (e.g., <random_suffix> can be "a1b2") because
            wrapping 'A' with __ would conflict with '"__A__"'

    Raises:
        ValueError if we fail to resolve conflict after appending the random suffix.

    Returns:
        A list of Snowflake quoted identifiers, that are conflict free.

    Note:
        There is a similar version of function inside OrderedDataFrame, which should be
        used in general. This method should only be used when an OrderedDataFrame is not available,
        e.g., in `from_pandas()`.
    """
    resolve_conflicts = excluded is not None
    if resolve_conflicts:
        # verify that identifiers in 'excluded' are valid snowflake quoted identifiers.
        for identifier in excluded:  # type: ignore[union-attr]
            if not is_valid_snowflake_quoted_identifier(identifier):
                raise ValueError(
                    "'excluded' must have quoted identifiers."
                    f" Found unquoted identifier='{identifier}'"
                )
        excluded_set = set(excluded)  # type: ignore[arg-type]
    else:
        excluded_set = set()
    quoted_identifiers = []
    for pandas_label in pandas_labels:
        quoted_identifier = quote_name_without_upper_casing(
            f"__{pandas_label}__" if wrap_double_underscore else f"{pandas_label}"
        )
        if resolve_conflicts:
            num_retries = 0
            while quoted_identifier in excluded_set and num_retries < _MAX_NUM_RETRIES:
                if len(quoted_identifier) > _MAX_IDENTIFIER_LENGTH:
                    quoted_identifier = quote_name_without_upper_casing(
                        generate_column_identifier_random(_MAX_IDENTIFIER_LENGTH)
                    )
                else:
                    suffix = generate_column_identifier_random()
                    quoted_identifier = quote_name_without_upper_casing(
                        f"__{pandas_label}_{suffix}__"
                        if wrap_double_underscore
                        else f"{pandas_label}_{suffix}"
                    )
                num_retries += 1

            if quoted_identifier in excluded_set:
                raise ValueError(
                    f"Failed to generate quoted identifier for pandas label: '{pandas_label}' "
                    f"the generated identifier '{quoted_identifier}' conflicts with {excluded_set}"
                )
        quoted_identifiers.append(quoted_identifier)
        excluded_set.add(quoted_identifier)
    return quoted_identifiers


def generate_new_labels(
    *, pandas_labels: list[Hashable], excluded: Optional[list[str]] = None
) -> list[str]:
    """
    Helper function to generate new (string) pandas labels which do not conflict with the list of strings in excluded.

     Args:
        pandas_labels: a list of pandas labels to generate new string labels for.
        excluded: a list of pandas string labels to exclude.
            When excluded is None, not conflict resolution happens, generated string labels
            may have conflicts.
    """
    if not excluded:
        return list(map(str, pandas_labels))

    excluded_set = set(excluded)
    new_labels = []
    for pandas_label in pandas_labels:
        new_label = f"{pandas_label}"
        num_retries = 0
        while new_label in excluded_set and num_retries < _MAX_NUM_RETRIES:
            if len(new_label) > _MAX_IDENTIFIER_LENGTH:
                new_label = generate_column_identifier_random(_MAX_IDENTIFIER_LENGTH)
            else:
                suffix = generate_column_identifier_random()
                new_label = f"{pandas_label}_{suffix}"
            num_retries += 1

        if new_label in excluded_set:
            raise ValueError(  # pragma: no cover
                f"Failed to generate string label {pandas_label} "
                f"the generated label '{new_label}' conflicts with {excluded_set}"
            )
        new_labels.append(new_label)
        excluded_set.add(new_label)
    return new_labels


def serialize_pandas_labels(pandas_labels: list[Hashable]) -> list[str]:
    """
    Serialize the hashable pandas labels into a string.  If it is a tuple, then json serialization is used as a best
    effort for better readability, however, if it fails, then the regular string representation is used.
    """
    serialized_pandas_labels = []
    for pandas_label in pandas_labels:
        if isinstance(pandas_label, tuple):
            try:
                # We prefer a json compatible serialization of the pandas label for the column header so that we
                # split the labels in SQL if needed.  For example, in transpose operation a multi-level pandas label
                # column name like (a,b) would become transposed into 2 new index columns (a) and (b) values.  This
                # currently does not handle cases where pandas label is not json serializable, so if there is a failure
                # we will use the python string representation.
                # TODO (SNOW-886400) Multi-level non-str pandas label not handled.
                pandas_label = json.dumps(list(pandas_label))
            except json.JSONDecodeError:
                pass
            except TypeError:
                pass
        pandas_label = str(pandas_label)
        serialized_pandas_labels.append(pandas_label)
    return serialized_pandas_labels


def is_json_serializable_pandas_labels(pandas_labels: list[Hashable]) -> bool:
    """
    Returns True if all the pandas_labels can be json serialized.
    """
    for pandas_label in pandas_labels:
        try:
            json.dumps(pandas_label)
        except (TypeError, ValueError):
            return False

    return True


def unquote_name_if_quoted(name: str) -> str:
    """
    For a given name unquote the name if the name is quoted, and also unescape the
    quotes in the name.
    """
    if name.startswith(DOUBLE_QUOTE) and name.endswith(DOUBLE_QUOTE):
        name = name[1:-1]
    return name.replace(DOUBLE_QUOTE + DOUBLE_QUOTE, DOUBLE_QUOTE)


def extract_pandas_label_from_snowflake_quoted_identifier(
    snowflake_identifier: str,
) -> str:
    """
    This function extracts pandas label from given snowflake identifier.
    To extract pandas label from snowflake identifier we simply remove surrounding double quotes and unescape quotes.

    Args:
        snowflake_identifier: a quoted snowflake identifier, must be a quoted string.

    Examples:
        extract_pandas_label_from_snowflake_quoted_identifier('"abc"') -> 'abc'
        extract_pandas_label_from_snowflake_quoted_identifier('"a""bc"') -> 'a"bc'

    Returns:
        pandas label.
    """
    assert is_valid_snowflake_quoted_identifier(
        snowflake_identifier
    ), f"invalid snowflake_identifier {snowflake_identifier}"
    return snowflake_identifier[1:-1].replace(DOUBLE_QUOTE + DOUBLE_QUOTE, DOUBLE_QUOTE)


def get_snowflake_quoted_identifier_to_pandas_label_mapping(
    original_snowflake_quoted_identifiers_list: list[str],
    original_pandas_labels: list[str],
    new_snowflake_quoted_identifiers_list: str,
) -> dict[str, str]:
    """
    This function maps a list of snowflake_quoted_identifiers to the corresponding pandas label.
    If the snowflake_quoted_identifier is found in the input query compiler's data column list, then,
    the pandas label is returned from the query compiler.  If it is not - we parse it and generate
    the corresponding pandas label.
    Args:
        snowflake_quoted_identifiers: list of quoted snowflake identifiers, must be a list of quoted strings.
        pandas_labels: list of pandas labels corresponding the quoted identifiers.
    Returns:
        map of snowflake_quoted_identifier to corresponding pandas label.
    """
    ret_val = {}

    qc_column_names_map = dict(
        zip(original_snowflake_quoted_identifiers_list, original_pandas_labels)
    )

    for snowflake_quoted_identifier in new_snowflake_quoted_identifiers_list:
        if snowflake_quoted_identifier in qc_column_names_map:
            ret_val[snowflake_quoted_identifier] = qc_column_names_map[
                snowflake_quoted_identifier
            ]
        else:
            ret_val[
                snowflake_quoted_identifier
            ] = extract_pandas_label_from_snowflake_quoted_identifier(
                snowflake_quoted_identifier
            )

    return ret_val


def parse_object_construct_snowflake_quoted_identifier_and_extract_pandas_label(
    object_construct_snowflake_quoted_identifier: str,
    num_levels: int,
) -> tuple[Hashable, dict[str, Any]]:
    """
    This function parses the corresponding pandas label tuples as well as additional map keys from the json object
    provided as a snowflake quoted identifier.  This is done by parsing into a map and then extracting the pandas label
    tuple using a 0-based integer index look up.  Keys not related to the index look up (not in range(0,num_levels-1))
    are returned as a separate key value map.

    For example, '{"0":"abc","2":"ghi", "row": 10}' would return (("abc", None, "ghi"), {"row": 10})

    For other examples of the indexed object construct extraction, see documentation for
    extract_pandas_label_tuple_from_object_construct_snowflake_quoted_identifier

    Arguments:
        object_construct_snowflake_quoted_identifier: The snowflake quoted identifier.
        num_levels: Number of levels in expected pandas labels

    Returns:
        Tuple containing the corresponding pandas labels and any other additional key, value pairs.
    """
    obj_construct_map = parse_snowflake_object_construct_identifier_to_map(
        object_construct_snowflake_quoted_identifier
    )

    pandas_label = extract_pandas_label_from_object_construct_map(
        obj_construct_map, num_levels
    )
    other_kw_map = extract_non_pandas_label_from_object_construct_map(
        obj_construct_map, num_levels
    )

    return (pandas_label, other_kw_map)


def parse_snowflake_object_construct_identifier_to_map(
    object_construct_snowflake_quoted_identifier: str,
) -> dict[Any, Any]:
    """
    Parse snowflake identifier based on object_construct (json object) into a map of the constituent parts.

    Examples:
        parse_snowflake_object_construct_identifier_to_map('"{""0"":""a"", ""2"":""b"", ""row"": 10}"')
            returns {"0": "a", "2": "b", "row": 10}

    Arguments:
        object_construct_snowflake_quoted_identifier: snowflake quoted identifier

    Returns:
        dict of object_construct snowflake quoted identifier
    """
    identifiers_by_level_json_obj = (
        extract_pandas_label_from_snowflake_quoted_identifier(
            object_construct_snowflake_quoted_identifier
        )
    )
    identifiers_by_level_json_obj = convert_snowflake_string_constant_to_python_string(
        identifiers_by_level_json_obj
    )

    # SNOW-853416: There are other encoding issues to handle, but this double slash decode is to undo
    # escaping that happens as part of the pivot snowflake to display column name.  For instance, a raw
    # data value "shi\'ny" would pivot to column name "'shi\\''ny', we need to get back to the original.
    identifiers_by_level_json_obj = identifiers_by_level_json_obj.replace("\\\\", "\\")
    obj_construct_map = json.loads(identifiers_by_level_json_obj)

    return obj_construct_map


def extract_pandas_label_from_object_construct_map(
    obj_construct_map: dict[Any, Any], num_levels: int
) -> Hashable:
    """
    Extract pandas label from object_construct map

    Examples:
        extract_pandas_label_tuple_from_object_construct_map({"0": "a"}, 1) returns ("a")
        extract_pandas_label_tuple_from_object_construct_map({"0": "a", "1": "b", "row": 10}, 2) returns ("a", "b")
        extract_pandas_label_tuple_from_object_construct_map({"0": "a", "1": "b", "2": "c"}, 3) returns ("a", "b", "c")
        extract_pandas_label_tuple_from_object_construct_map({"0": "a", "2": "b", "row": 10}, 4)
            returns ("a", None, "b", None)

    Arguments:
        obj_construct_map: Map of object_construct key, values including positional pandas label
        num_levels: Number of pandas label levels

    Returns:
        pandas label extracted from object_construct map
    """
    label_tuples = []

    for level in range(num_levels):
        level_str = str(level)
        label_tuples.append(
            obj_construct_map[level_str] if level_str in obj_construct_map else None
        )

    return to_pandas_label(tuple(label_tuples))


def extract_non_pandas_label_from_object_construct_map(
    obj_construct_map: dict[Any, Any], num_levels: int
) -> dict[Any, Any]:
    """
    Extract non-pandas label from object_construct map

    Examples:
        extract_non_pandas_label_from_object_construct_map({"0": "a", "1": "b"}, 2) returns {}
        extract_non_pandas_label_from_object_construct_map({"0": "a", "foo": "bar"}, 2) returns {"foo": "bar"}
        extract_non_pandas_label_from_object_construct_map({"0": "a", "1": "b", "foo": "bar"}, 2) returns {"foo": "bar"}
        extract_non_pandas_label_from_object_construct_map({"2": "val"}, 2) returns {"2": "val"}

    Arguments:
        obj_construct_map: Map of object_construct key, values including positional pandas label as well as other
            key, value pairs.
        num_levels: Number of pandas label levels

    Returns:
        Key value information from object_construct map not related to pandas label
    """
    non_pandas_label_map: dict[Any, Any] = {}

    key_str_range = [str(num) for num in range(num_levels)]

    for key in obj_construct_map.keys():
        if key not in key_str_range:
            non_pandas_label_map[key] = obj_construct_map[key]

    return non_pandas_label_map


def extract_pandas_label_from_object_construct_snowflake_quoted_identifier(
    object_construct_snowflake_quoted_identifier: str,
    num_levels: int,
) -> Hashable:
    """
    This function extracts the corresponding pandas labels from a snowflake quoted identifier which was constructed
    via object_construct key:value mapping using a 0-based integer index value.  The snowflake quoted identifier
    is expected to be a valid json encoding.  For example, '{"0":"abc","2":"ghi"}' would extract pandas labels
    as ("abc", None, "ghi"), here are more examples:

    Examples:
        extract_pandas_label_from_object_construct_snowflake_quoted_identifier('{"0":"abc","1":"def"}', 2)
            -> ("abc", "def")
        extract_pandas_label_from_object_construct_snowflake_quoted_identifier('{"0":"ab\\"c","1":"def"}', 2)
            -> ('ab"c', "def")
        extract_pandas_label_from_object_construct_snowflake_quoted_identifier('{"0":"abc\\"\\"","1":"def"}', 2)
            -> ('abc""', "def")
        extract_pandas_label_from_object_construct_snowflake_quoted_identifier('{"0":"\\",\\"abc","1":"def"}', 2)
            -> ('","abc', "def")
        extract_pandas_label_from_object_construct_snowflake_quoted_identifier('{"0":"abc","2":"ghi"}', 3)
            -> ("abc", None, "ghi")
        extract_pandas_label_from_object_construct_snowflake_quoted_identifier('{"1":"def"}', 3) -> (None, "def", None)
        extract_pandas_label_from_object_construct_snowflake_quoted_identifier("{}", 3) -> (None, None, None)

    Arguments:
        object_construct_snowflake_quoted_identifier: The snowflake quoted identifier.
        num_levels: Number of levels in expected pandas labels

    Returns:
        Tuple containing the corresponding pandas labels.
    """
    return parse_object_construct_snowflake_quoted_identifier_and_extract_pandas_label(
        object_construct_snowflake_quoted_identifier, num_levels
    )[0]


def is_valid_snowflake_quoted_identifier(identifier: str) -> bool:
    """
    Check whether identifier is a properly quoted Snowflake identifier or not
    Performs following checks:
    1. Length must be > 2
    2. Must have surrounding quotes.
    2. Double quotes which are part of identifier must be properly escaped.
    Args:
        identifier: string representing a Snowflake identifier
    Returns:
        True if input string is properly quoted snowflake identifier, False otherwise
    """
    return (
        len(identifier) > 2
        and identifier[0] == DOUBLE_QUOTE
        and identifier[-1] == DOUBLE_QUOTE
        and DOUBLE_QUOTE
        not in identifier[1:-1].replace(DOUBLE_QUOTE + DOUBLE_QUOTE, EMPTY_STRING)
    )


def traceback_as_str_from_exception(exception_object: Exception) -> str:
    """
    return python traceback as string from exception object
    Args:
        exception_object: exception object

    Returns: string containing description usually printed by interpreter to stderr

    """
    exception_lines = traceback.format_exception(
        None, exception_object, exception_object.__traceback__
    )
    formatted_traceback = "".join(exception_lines)
    return formatted_traceback


def extract_all_duplicates(elements: Sequence[Hashable]) -> Sequence[Hashable]:
    """
    Find duplicated elements for the given list of elements.

    Args:
        elements: the list of elements to check for duplications
    Returns:
        List[Hashable]
            list of unique elements that contains duplication in the original list
    """
    duplicated_elements = list(filter(lambda el: elements.count(el) > 1, elements))
    unique_duplicated_elements = list(dict.fromkeys(duplicated_elements))

    return unique_duplicated_elements


def is_duplicate_free(names: Sequence[Hashable]) -> bool:
    """
    check whether names contains duplicates
    Args:
        names: sequence of hashable objects to check for duplicates based on __hash__ method. I.e., two
               elements are considered duplicates if their hashes match.

    Returns:
        True if no duplicates, False else
    """
    return len(extract_all_duplicates(names)) == 0


def assert_duplicate_free(names: Sequence[str], type: str) -> None:
    """
    Checks going one-by-one through the sequence 'names', by comparing if the current given name in the sequence
    has been seen before or not. An assertion error is produced containing information about all element in names
    that have duplicates.
    Args:
        names: A sequence of strings to check for duplicates
        type: a string to describe the elements when producing the assertion error.

    Returns:
        None
    """
    duplicates = extract_all_duplicates(names)

    if len(duplicates) == 1:
        raise AssertionError(f"Found duplicate of type {type}: {duplicates[0]}")
    elif len(duplicates) > 1:
        raise AssertionError(f"Found duplicates of type {type}: {duplicates}")


def to_pandas_label(label: LabelTuple) -> Hashable:
    """
    get the pandas label used for identify pandas column/rows in pandas dataframe
    """
    assert (
        len(label) >= 1
    ), "label in Snowpark pandas must have at least one label component"

    if len(label) == 1:
        return label[0]
    else:
        return label


def from_pandas_label(pandas_label: Hashable, num_levels: int) -> LabelTuple:
    """
    Creates the internal pandas label representation with a given pandas hashable label
    Args:
        pandas_label: The pandas label to be convered to LabelTuple
        num_levels: The number of levels of column as pandas index. If it is large than 1, it
            indicates the label is not on a MultiIndex column and `pandas_label` must be a tuple
            with length larger than 1.
    Returns:
        LabelTuple for internal label representation, which is Tuple[LabelComponent,...]
    """
    if num_levels > 1:
        assert (
            isinstance(pandas_label, tuple) and len(pandas_label) > 1
        ), f"pandas label on MultiIndex column must be a tuple with length larger than 1, but got {pandas_label}"
        return pandas_label
    else:
        return (pandas_label,)


def is_all_label_components_none(pandas_label: Hashable) -> bool:
    """
    Check if a pandas label None, or if all tuple components are None if the pandas label is a tuple.
    For example:
        is_all_label_components_none returns True for pandas label like None, (None, None), but not for
        label like (None, 'A').
    """
    if isinstance(pandas_label, tuple):
        return all(item is None for item in pandas_label)
    return pandas_label is None


def fill_missing_levels_for_pandas_label(
    pandas_label: Hashable,
    target_num_levels: int,
    origin_label_start_level: int,
    fill_value: Hashable,
) -> Hashable:
    """
    Given a pandas label, return a pandas label with target number of levels by filling up the missing levels
    in front and after with given fill_value. The origin_label_start_level specifies level where the given pandas
    label should start with in the final label. The final label will be in form like following:
    (fill_value, ..., fill_value, pandas_label, fill_value, ... fill_value)

     Examples:
        1) fill_missing_levels_for_pandas_label(('a', 'b'), 3, 0, 'c') gives result ('a', 'b', 'c'). The
            original label ('a', 'b') has 2 levels, and starts at level 0, there is 1 missing level to reach
            target level 3, and the level is filled with 'c'.
        2) fill_missing_levels_for_pandas_label(('a', 'b'), 4, 1, 'c') gives result ('c', 'a', 'b', 'c'). The
            original label has two levels, and start at level 1 in the finale label, level 0 and level 3 is
            missing and filled with fill_value 'c'.

    Args:
        pandas_label: The pandas label to fill missing levels with
        target_num_levels: The target number of levels of the final pandas level
        origin_label_start_level: The level where the pandas label should start with in the final label
        fill_value: The value to fill for missing levels. If the filling value is a tuple, it is
            treated as single label component for filling.

    Returns:
        pandas label with target_num_levels
    Raise:
        ValueError if the constructed label exceeds the target number of label levels


    """
    assert (
        origin_label_start_level < target_num_levels
    ), f"level for the pandas label to start {origin_label_start_level} exceeds the target label level {target_num_levels}"

    if target_num_levels == 1:
        # if target number of level is 1, no filling is needed
        return pandas_label

    label_tuple = pandas_label if isinstance(pandas_label, tuple) else (pandas_label,)

    fill_value_tuple = (fill_value,)
    final_label = fill_value_tuple * origin_label_start_level + label_tuple
    missing = target_num_levels - len(final_label)
    if missing < 0:
        # the constructed label is exceeding the target level
        raise ValueError(
            f"Constructed Label has {target_num_levels - missing} levels, which is larger than target level {target_num_levels}."
        )
    # fill the missing levels
    final_label += fill_value_tuple * missing

    return final_label


def infer_snowpark_types_from_pandas(
    df: native_pd.DataFrame,
) -> tuple[list[Optional[SnowparkPandasType]], list[DataType]]:
    """
    Infer Snowpark pandas and Snowpark Python types for a pandas dataframe.

    Args:
        df: native_pd.DataFrame
            The native pandas dataframe out of which we will create a Snowpark
            pandas dataframe.

    Returns:
        Tuple containing
            1) The list of Snowpark pandas types for each column in the dataframe.
            2) The list of Snowpark Python types for each column in the dataframe.
    """
    from snowflake.snowpark.modin.plugin._internal.type_utils import infer_series_type

    snowpark_pandas_types = []
    snowpark_types = []
    for _, column in df.items():
        snowflake_type = infer_series_type(column)
        if isinstance(snowflake_type, SnowparkPandasType):
            snowpark_types.append(snowflake_type.snowpark_type)
            snowpark_pandas_types.append(snowflake_type)
        else:
            snowpark_types.append(snowflake_type)
            # For types that are not SnowparkPandasType, we need to ask
            # Snowflake for the type, so we mark the type as unknown by
            # returning None.
            snowpark_pandas_types.append(None)
    return snowpark_pandas_types, snowpark_types


def create_ordered_dataframe_from_pandas(
    df: native_pd.DataFrame,
    snowflake_quoted_identifiers: list[str],
    snowpark_types: list[DataType],
    ordering_columns: Optional[list[OrderingColumn]] = None,
    row_position_snowflake_quoted_identifier: Optional[str] = None,
) -> OrderedDataFrame:
    """
    Create Ordered dataframe from pandas dataframe with proper pre-processing.
    The preprocess includes:
      - convert timedelta64 to int64
      - replace all pandas missing values (np.nan, pd.NA, pd.NaT, None) in the values with None
        before it is passed to snowpark create_dataframe, which will be mapped to NULL in Snowflake.

    Args:
        df: native_pd.DataFrame, the native pandas dataframe that is used to create the snowpark dataframe
        snowflake_quoted_identifiers: List[str], list of snowflake quoted identifiers that are used as the
            snowflake quoted identifiers for the snowpark dataframe
        snowpark_types: The column types for the Snowpark dataframe.
        ordering_columns: a list of OrderingColumns
        row_position_snowflake_quoted_identifier: a row position snowflake quoted identifier

    Returns:
        Ordered dataframe that is created out of the data of the given native pandas dataframe
    """
    assert len(snowflake_quoted_identifiers) == len(
        df.columns
    ), "length of snowflake quoted identifier must be the same as length of dataframe columns"

    # marker for the missing values for the original dataframe, pandas treated
    # np.nan, pd.NA, pd.NaT and None all as missing value.
    isna_data = df.isna().to_numpy().tolist()

    # in pandas, missing value can be represented ad np.nan, pd.NA, pd.NaT and None,
    # and should be mapped to None in Snowpark input, and eventually be mapped to NULL
    # in snowflake. Here we do one process to the data to replace all na values with None
    # according to the isna_data of the original input dataframe.
    data: list[list[Union[np.generic, Scalar]]] = df.to_numpy().tolist()
    for x in range(len(data)):
        for y in range(len(data[x])):
            data[x][y] = (
                None
                if isna_data[x][y]
                else (
                    data[x][y].item()
                    if isinstance(data[x][y], np.generic)
                    else ensure_snowpark_python_type(data[x][y])
                )
            )

    snowpark_df = pd.session.create_dataframe(
        data,
        schema=StructType(
            [
                StructField(column_identifier=id, datatype=each_datatype)
                for id, each_datatype in zip(
                    snowflake_quoted_identifiers, snowpark_types
                )
            ]
        ),
    )
    ordered_df = OrderedDataFrame(
        DataFrameReference(snowpark_df, snowflake_quoted_identifiers),
        projected_column_snowflake_quoted_identifiers=snowflake_quoted_identifiers,
        ordering_columns=ordering_columns,
        row_position_snowflake_quoted_identifier=row_position_snowflake_quoted_identifier,
    )
    # Set the materialized row count
    ordered_df.row_count = df.shape[0]
    ordered_df.row_count_upper_bound = df.shape[0]
    return ordered_df


def fill_none_in_index_labels(
    index_labels: list[Optional[Hashable]],
    existing_labels: Optional[list[Hashable]] = None,
) -> list[Hashable]:
    """
    Replaces None in index labels with string values. The behavior of this function is similar to
    the assigned pandas labels for index columns when reset_index() is called. When index_labels
    only has one element (i.e., single index), `INDEX_LABEL` is used to replace None if
    `INDEX_LABEL` is not in existing labels, otherwise `LEVEL_LABEL_{pos}` is used; When
    index_labels has more than one element (i.e., MultiIndex), `LEVEL_LABEL_{pos}` is used
    to replace None.

    Args:
        index_labels: a list of index labels
        existing_labels: a list of existing pandas labels (in the dataframe). To keep consistent
            with pandas behavior, when `INDEX_LABEL` already exists and there is only single index,
            we use `LEVEL_LABEL_{pos}` to replace None.
    """
    if not index_labels:
        return []
    if len(index_labels) == 1:
        if index_labels[0]:
            return index_labels
        elif not existing_labels or INDEX_LABEL not in existing_labels:
            return [INDEX_LABEL]
        else:
            return [f"{LEVEL_LABEL}_0"]
    else:
        return [label or f"{LEVEL_LABEL}_{i}" for i, label in enumerate(index_labels)]


def is_snowpark_pandas_dataframe_or_series_type(obj: Any) -> bool:
    # Return True if result is (Snowpark pandas) DataFrame/Series type.
    # Note: Native pandas.DataFrame/Series return False
    return isinstance(obj, (pd.DataFrame, pd.Series))


def convert_snowflake_string_constant_to_python_string(identifier: str) -> str:
    """
    Convert a snowflake string constant to a python constant, this removes surrounding single quotes
    and de-duplicates interior single quotes added in snowflake string.
    """
    if len(identifier) > 1 and identifier.startswith("'") and identifier.endswith("'"):
        identifier = identifier[1:-1].replace("''", "'")
    return identifier


# TODO: (SNOW-857474) Refactor for common group by validations.
def check_valid_pandas_labels(
    pandas_labels: list[Hashable],
    ref_pandas_labels: list[Hashable],
) -> None:
    """
    Check pandas labels provided are within the reference pandas label list, and there are no duplicates.

    Args:
        pandas_labels: pandas labels
        ref_pandas_labels: Reference pandas labels

    Returns:
        Raise an exception matching pandas library if not valid pandas label, otherwise returns None.
    """
    if None in pandas_labels:
        raise TypeError("'NoneType' object is not callable")

    found_pandas_labels = set()
    for pandas_label in pandas_labels:
        if pandas_label in found_pandas_labels:
            raise ValueError(f"Grouper for '{pandas_label}' not 1-dimensional")
        found_pandas_labels.add(pandas_label)
        found_ref_cnt = len(
            list(
                filter(
                    lambda label: label == pandas_label,
                    ref_pandas_labels,
                )
            )
        )
        if found_ref_cnt == 0:
            raise KeyError(pandas_label)
        elif found_ref_cnt > 1:
            raise ValueError(f"Grouper for '{pandas_label}' not 1-dimensional")


def check_snowpark_pandas_object_in_arg(arg: Any) -> bool:
    """
    Return True if `arg` is a Snowpark pandas object (DataFrame or Series) or
    contains any Snowpark pandas object. It is used for checking args and keywords of
    Snowpark pandas api call, which and can be list, tuple or dict.
    """
    if isinstance(arg, (list, tuple)):
        for v in arg:
            if check_snowpark_pandas_object_in_arg(v):
                return True
    elif isinstance(arg, dict):
        # Snowpark pandas object can't be the key because it's not hashable
        # so we only need to check values
        for v in arg.values():
            if check_snowpark_pandas_object_in_arg(v):
                return True
    else:
        from modin.pandas import DataFrame, Series

        return isinstance(arg, (DataFrame, Series))

    return False


def snowpark_to_pandas_helper(
    frame: "frame.InternalFrame",
    *,
    index_only: bool = False,
    statement_params: Optional[dict[str, str]] = None,
    **kwargs: Any,
) -> Union[native_pd.Index, native_pd.DataFrame]:
    """
    The helper function retrieves a pandas dataframe from an OrderedDataFrame. Performs necessary type
    conversions including
    1. For VARIANT types, OrderedDataFrame.to_pandas may convert datetime like types to string. So we add one `typeof`
    column for each variant column and use that metadata to convert datetime like types back to their original types.
    2. For TIMESTAMP_TZ type, OrderedDataFrame.to_pandas will convert them into the local session timezone and lose the
    original timezone. So we cast TIMESTAMP_TZ columns to string first and then convert them back after to_pandas to
    preserve the original timezone. Note that the actual timezone will be lost in Snowflake backend but only the offset
    preserved.
    3. For Timedelta columns, since currently we represent the values using integers, here we need to explicitly cast
    them back to Timedelta.

    Args:
        frame: The internal frame to convert to pandas Dataframe (or Index if index_only is true)
        index_only: if true, only turn the index columns into a pandas Index
        statement_params: Dictionary of statement level parameters to be passed to conversion function of ordered
        dataframe abstraction.
        kwargs: Additional keyword-only args to pass to internal `to_pandas` conversion for ordered dataframe
        abstraction.

    Returns:
        pandas dataframe
    """
    ids = frame.index_column_snowflake_quoted_identifiers
    cached_snowpark_pandas_types = frame.cached_index_column_snowpark_pandas_types

    if not index_only:
        ids += frame.data_column_snowflake_quoted_identifiers
        cached_snowpark_pandas_types += frame.cached_data_column_snowpark_pandas_types

    ordered_dataframe = frame.ordered_dataframe.select(*ids)
    # Step 1: preprocessing on Snowpark pandas types
    # Here we convert Timedelta to string before to_pandas to avoid precision loss.
    if cached_snowpark_pandas_types is not None:
        astype_mapping = {}
        column_type_map = {
            f.column_identifier.quoted_name: f.datatype
            for f in ordered_dataframe.schema.fields
        }
        for col_id, snowpark_pandas_type in zip(ids, cached_snowpark_pandas_types):
            if (
                snowpark_pandas_type is not None
                and snowpark_pandas_type == TimedeltaType()
            ):
                col_td = col(col_id)
                if isinstance(column_type_map[col_id], _FractionalType):
                    if isinstance(column_type_map[col_id], DecimalType):
                        check_non = col_td.cast(DoubleType())
                    else:
                        check_non = col_td
                    check_non = equal_nan(check_non)
                    # Timedelta's underneath Snowflake type may not always be int after other operations, so
                    # explicitly floor them to integer first before converting to string. Note if it is float nan,
                    # we have to keep it as is, otherwise it will raise exception when casting to integer.
                    astype_mapping[col_id] = iff(
                        check_non,
                        col_td,
                        floor(col_td).cast(LongType()).cast(StringType()),
                    )
                else:  # integer type
                    astype_mapping[col_id] = col_td.cast(StringType())
        if astype_mapping:
            (
                frame,
                old_to_new_id_mapping,
            ) = frame.update_snowflake_quoted_identifiers_with_expressions(
                quoted_identifier_to_column_map=astype_mapping,
            )
            ordered_dataframe = frame.ordered_dataframe.select(
                [old_to_new_id_mapping.get(id, id) for id in ids]
            )

    # Step 2: Retrieve schema of Snowpark dataframe and
    # capture information about each quoted identifier and its corresponding datatype, store
    # as list to keep information about order of columns.
    columns_info = [
        (f.column_identifier.quoted_name, f.datatype)
        for f in ordered_dataframe.schema.fields
    ]
    column_identifiers = list(map(lambda t: t[0], columns_info))

    # extract all identifiers with variant type
    variant_type_columns_info = list(
        filter(lambda t: isinstance(t[1], VariantType), columns_info)
    )
    variant_type_identifiers = list(map(lambda t: t[0], variant_type_columns_info))

    # Step 3.1: Create for each variant type column a separate type column (append at end), and retrieve data values
    # (and types for variant type columns).
    variant_type_typeof_identifiers = (
        ordered_dataframe.generate_snowflake_quoted_identifiers(
            pandas_labels=[
                f"{unquote_name_if_quoted(id)}_typeof"
                for id in variant_type_identifiers
            ],
            excluded=column_identifiers,
        )
    )

    if 0 != len(variant_type_identifiers):
        ordered_dataframe = append_columns(
            ordered_dataframe,
            variant_type_typeof_identifiers,
            [typeof(col(id)) for id in variant_type_identifiers],
        )

    # Step 3.2: cast timestamp_tz to string to preserve their original timezone offsets
    timestamp_tz_identifiers = [
        info[0]
        for info in columns_info
        if info[1] == TimestampType(TimestampTimeZone.TZ)
    ]
    timestamp_tz_str_identifiers = (
        ordered_dataframe.generate_snowflake_quoted_identifiers(
            pandas_labels=[
                f"{unquote_name_if_quoted(id)}_str" for id in timestamp_tz_identifiers
            ],
            excluded=column_identifiers,
        )
    )
    if len(timestamp_tz_identifiers):
        ordered_dataframe = append_columns(
            ordered_dataframe,
            timestamp_tz_str_identifiers,
            [
                to_char(col(id), format="YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM")
                for id in timestamp_tz_identifiers
            ],
        )

    # ensure that snowpark_df has unique identifiers, so the native pandas DataFrame object created here
    # also does have unique column names which is a prerequisite for the post-processing logic following.
    assert is_duplicate_free(
        column_identifiers
        + variant_type_typeof_identifiers
        + timestamp_tz_str_identifiers
    ), "Snowpark DataFrame to convert must have unique column identifiers"
    pandas_df = ordered_dataframe.to_pandas(statement_params=statement_params, **kwargs)

    # Step 4: perform post-processing
    # If the dataframe has no rows, do not perform this. Using the result of the `apply` on
    # an empty frame would erroneously update the dtype of the column to be `float64` instead of `object`.
    # TODO SNOW-982779: verify correctness of this behavior
    if pandas_df.shape[0] != 0:
        if 0 != len(variant_type_identifiers):
            # Step 3a: post-process variant type columns, if any exist.
            id_to_label_mapping = dict(
                zip(
                    column_identifiers
                    + variant_type_typeof_identifiers
                    + timestamp_tz_str_identifiers,
                    pandas_df.columns,
                )
            )

            def convert_variant_type_to_pandas(row: native_pd.Series) -> Any:
                value, type = row

                # pass NULL, None, NaN values through
                if native_pd.isna(value):
                    return value

                # Decode time-related strings via to_datetime.
                if type in ["TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"]:
                    return native_pd.to_datetime(value.replace('"', ""))
                if type == "DATE":
                    return native_pd.to_datetime(value.replace('"', "")).date()
                if type == "TIME":
                    return native_pd.to_datetime(value.replace('"', "")).time()
                # Variant is stored as JSON internally, so decode here always.
                return json.loads(value)

            for id, type_of_id in zip(
                variant_type_identifiers, variant_type_typeof_identifiers
            ):
                pandas_df[id_to_label_mapping[id]] = pandas_df[
                    [id_to_label_mapping[id], id_to_label_mapping[type_of_id]]
                ].apply(convert_variant_type_to_pandas, axis=1)

        if any(isinstance(dtype, (ArrayType, MapType)) for (_, dtype) in columns_info):
            # Step 3b: post-process semi-structured columns for both ArrayType, MapType as they are by
            #          default returned as strings when converting the Snowpark Dataframe via to_pandas.
            id_to_label_mapping = dict(zip(column_identifiers, pandas_df.columns))
            for quoted_name, datatype in columns_info:
                if isinstance(datatype, (ArrayType, MapType)):
                    pandas_df[id_to_label_mapping[quoted_name]] = pandas_df[
                        id_to_label_mapping[quoted_name]
                    ].apply(lambda value: None if value is None else json.loads(value))

        # Convert timestamp_tz in string back to datetime64tz.
        if any(
            dtype == TimestampType(TimestampTimeZone.TZ) for (_, dtype) in columns_info
        ):
            id_to_label_mapping = dict(
                zip(
                    column_identifiers
                    + variant_type_typeof_identifiers
                    + timestamp_tz_str_identifiers,
                    pandas_df.columns,
                )
            )
            for ts_id, ts_str_id in zip(
                timestamp_tz_identifiers, timestamp_tz_str_identifiers
            ):
                pandas_df[id_to_label_mapping[ts_id]] = native_pd.to_datetime(
                    pandas_df[id_to_label_mapping[ts_str_id]]
                )

    # Step 5. Return the original amount of columns by stripping any typeof(...) columns appended if
    # schema contained VariantType.
    downcast_pandas_df = pandas_df[pandas_df.columns[: len(columns_info)]]

    # Step 6. postprocessing for Snowpark pandas types
    if cached_snowpark_pandas_types is not None:
        timedelta_t = TimedeltaType()

        def convert_str_to_timedelta(x: str) -> pd.Timedelta:
            return (
                x
                if pd.isna(x)
                else pd.NaT
                if x == "NaN"
                else timedelta_t.to_pandas(int(x))
            )

        for pandas_label, snowpark_pandas_type in zip(
            downcast_pandas_df.columns, cached_snowpark_pandas_types
        ):
            if snowpark_pandas_type is not None and snowpark_pandas_type == timedelta_t:
                # By default, pandas warns, "A value is trying to be set on a
                # copy of a slice from a DataFrame" here because we are
                # assigning a column to downcast_pandas_df, which is a copy of
                # a slice of pandas_df. We don't care what happens to pandas_df,
                # so the warning isn't useful to us.
                with native_pd.option_context("mode.chained_assignment", None):
                    downcast_pandas_df[pandas_label] = pandas_df[pandas_label].apply(
                        convert_str_to_timedelta
                    )

    # Step 7. postprocessing for return types
    if index_only:
        index_values = downcast_pandas_df.values
        if frame.is_multiindex(axis=0):
            value_tuples = [tuple(row) for row in index_values]
            return native_pd.MultiIndex.from_tuples(
                value_tuples, names=frame.index_column_pandas_labels
            )
        else:
            # We have one index column. Fill in the type correctly.
            index_identifier = frame.index_column_snowflake_quoted_identifiers[0]
            from snowflake.snowpark.modin.plugin._internal.type_utils import TypeMapper

            index_type = TypeMapper.to_pandas(
                frame.get_snowflake_type(index_identifier)
            )
            ret = native_pd.Index(
                [row[0] for row in index_values],
                name=frame.index_column_pandas_labels[0],
                # setting tupleize_cols=False to avoid creating a MultiIndex
                # otherwise, when labels are tuples (e.g., [("A", "a"), ("B", "b")]),
                # a MultiIndex will be created incorrectly
                tupleize_cols=False,
            )
            # When pd.Index() failed to reduce dtype to a numpy or pandas extension type, it will be object type. For
            # example, an empty dataframe will be object dtype by default, or a variant, or a timestamp column with
            # multiple timezones. So here we cast the index to the index_type when ret = pd.Index(...) above cannot
            # figure out a non-object dtype. Note that the index_type is a logical type may not be 100% accurate.
            # We exclude the case where ret.dtype is object dtype while index_dtype is bool dtype. This is because
            # casting None values to bool converts them to False, which results in a descripency with the pandas
            # behavior.
            if (
                is_object_dtype(ret.dtype)
                and not is_object_dtype(index_type)
                and not is_bool_dtype(index_type)
            ):
                # TODO: SNOW-1657460 fix index_type for timestamp_tz
                try:
                    ret = ret.astype(index_type)
                except ValueError:  # e.g., Tz-aware datetime.datetime cannot be converted to datetime64
                    pass
            return ret

    # to_pandas() does not preserve the index information and will just return a
    # RangeIndex. Therefore, we need to set the index column manually
    downcast_pandas_df.set_index(
        [
            extract_pandas_label_from_snowflake_quoted_identifier(identifier)
            for identifier in frame.index_column_snowflake_quoted_identifiers
        ],
        inplace=True,
    )
    # set index name
    downcast_pandas_df.index = downcast_pandas_df.index.set_names(
        frame.index_column_pandas_labels
    )

    # set column names and potential casting
    downcast_pandas_df.columns = frame.data_columns_index
    return downcast_pandas_df


def is_integer_list_like(arr: Union[list, np.ndarray]) -> bool:
    """
    check whether array is list-like made up of integer like arguments
    Args:
        arr: list-like to check for

    Returns: True if only contains integers, False else

    """
    if isinstance(arr, np.ndarray):
        return is_integer_dtype(arr.dtype)

    return all([is_integer_dtype(type(val)) for val in arr])


def label_prefix_match(
    label: Hashable,
    prefix_map: dict[Hashable, Any],
    level: Optional[int] = None,
) -> Optional[Any]:
    """
    Get the value from the 'prefix_map' if label match the key or prefix of the key (key is a tuple). For example,
        1. label: "a", prefix_map: {"a": 3}; return 3
        2. label: ("a", "b"), prefix_map: {"a": 3}; return 3
        3. label: ("a", "b"), prefix_map: {("a", "b"): 3}; return 3
        4. label: ("a", "b", "c"), prefix_map: {("a", "b"): 3}; return 3
        5. label: "a", prefix_map: {"b": 3}; return None
        6. label: ("a", "b"), prefix_map: {("a", "c"): 3}; return None
        7. label: ("a", "b"), prefix_map: {("a", None): 3}; return None
    Note that if multiple matches exist, it always respect the first match, for example,
        8. label: ("a", "b"), prefix_map: {("a",): 3, ("a", "b"): 4}; return 3
    If 'level' is given we only check against given level of 'label'.
        9. label: ("a", "b"), prefix_map: {"a": 3}, level=0; return 3
        10. label: ("a", "b"), prefix_map: {"a": 3}, level=1; return None
        11. label: ("a", "b"), prefix_map: {"b": 3}, level=1; return 3
    Args:
        label: dataframe column label
        prefix_map: label to value map
        level: optional level of label to match against. If provided we only check
            against given level of label, otherwise, we check against all levels of
            label including prefixes. Note this argument is ignored if 'label' is not
            a tuple which implies we have single level index.

    Returns:
        The matched value if label match any key or prefix of a key. Otherwise, None is returned
    """
    # Single level index
    if not isinstance(label, tuple):
        return prefix_map.get(label)

    # MultiIndex, match against specified 'level'
    if level is not None:
        if level >= len(label):
            return None
        return prefix_map.get(label[level])

    # MultiIndex, match against all levels
    for key, value in prefix_map.items():
        if label == key or label[0] == key:
            return value
        for i in range(1, len(label)):
            if label[:i] == key:
                return value
    return None


def fillna_label_to_value_map(
    value: dict, columns: native_pd.Index
) -> dict[Hashable, Any]:
    """
    Create a mapping from pandas label to fillna scalar value. The main purpose for this helper method is to handle
    multiindex correctly.
    Args:
        value: preprocessed value in a mapping
        columns: the column index of the dataframe to be filled

    Returns:
        label_to_value_map for fillna
    """
    label_to_value_map = {}
    for label in columns:
        val = label_prefix_match(label, value)
        if val is not None:
            label_to_value_map[label] = val
    return label_to_value_map


def convert_numpy_pandas_scalar_to_snowpark_literal(value: Any) -> LiteralType:
    """
    Converts a numpy scalar value, or a pandas Timestamp, or a pandas NA value
    to a Snowpark literal value.
    """
    assert is_scalar(value), f"{value} is not a scalar"
    if isinstance(value, np.generic):
        value = value.item()
    elif isinstance(value, native_pd.Timestamp):
        raise ValueError(
            "Internal error: cannot represent Timestamp as a Snowpark literal. "
            + "Represent as a string and call to_timestamp() instead."
        )
    elif native_pd.isna(value):
        value = None
    return value


def pandas_lit(value: Any, datatype: Optional[DataType] = None) -> Column:
    """
    Returns a Snowpark column for a literal value. Being differnet from Snowpark's lit()
    function, it also handles numpy scalar values and pandas Timestamp and pandas NA values.
    TODO SNOW-904405: Whether and how to support lit() for pandas and numpy types is still
    an open question for Snowpark Python API. Further discussion is need to have a more general
    approach.
    Args:
        value: value can be Python object or Snowpark Column
        datatype: optional datatype, if None infer datatype from value

    Returns:
        Snowpark Column expression
    """
    if isinstance(value, native_pd.Timestamp):
        # Reuse the Timestamp literal conversion code from
        # snowflake.snowpark.Session.create_dataframe:
        # https://github.com/snowflakedb/snowpark-python/blob/19a9139be1cb41eb1e6179f3cd3c427618c0e6b1/src/snowflake/snowpark/session.py#L2775
        return (to_timestamp_ntz if value.tz is None else to_timestamp_tz)(
            Column(Literal(str(value)))
        )

    snowpark_pandas_type = SnowparkPandasType.get_snowpark_pandas_type_for_pandas_type(
        type(value)
    )
    if snowpark_pandas_type:
        return Column(Literal(type(snowpark_pandas_type).from_pandas(value)))

    value = (
        convert_numpy_pandas_scalar_to_snowpark_literal(value)
        if is_scalar(value)
        else value
    )

    if isinstance(value, Column):
        return value  # pragma: no cover
    elif isinstance(value, native_pd.DateOffset):
        # Construct an Interval from DateOffset
        from snowflake.snowpark.modin.plugin._internal.timestamp_utils import (
            convert_dateoffset_to_interval,
        )

        return Column(convert_dateoffset_to_interval(value))
    else:
        # Construct a Literal directly in order to pass in `datatype`. `lit()` function does not support datatype.
        return Column(Literal(value, datatype))


def is_repr_truncated(
    row_count: int, col_count: int, num_rows_to_display: int, num_cols_to_display: int
) -> bool:
    """
    check whether result is truncated and information {} rows x {} columns should be displayed or not.
    Args:
        row_count: number of rows DataFrame or Series contains
        col_count: number of columns DataFrame contains
        num_rows_to_display: 0 to display all (no truncation), else number of rows to display
        num_cols_to_display: 0 to display all (no truncation), else number of columns to display

    Returns:
        True if information should be added, else False
    """
    if 0 == num_cols_to_display and 0 == num_rows_to_display:
        return False
    elif 0 == num_cols_to_display:
        return row_count > num_rows_to_display
    elif 0 == num_rows_to_display:
        return col_count > num_cols_to_display
    else:
        return row_count > num_rows_to_display or col_count > num_cols_to_display


def try_convert_to_simple_slice(s: Any) -> Optional[slice]:
    """
    Try converting to simple slice. Only slice or range like object with step 1 and non-negative start and stop are
    counted as simple slice and return; Otherwise, None will be returned.

    Args:
        s: the input to convert to slice

    Returns:
        The simple slice if possible; otherwise None.
    """
    from snowflake.snowpark.modin.plugin.extensions.indexing_overrides import (
        is_range_like,
    )

    if not isinstance(s, slice) and not is_range_like(s):
        return None
    if s.step is None and s.start is None and s.stop is None:
        return None
    # start can be None or non-negative
    if s.start is not None and s.start < 0:
        return None
    start = s.start if s.start is not None else 0
    # stop can only be non-negative
    if s.stop is None or s.stop < 0:
        return None
    stop = s.stop
    if s.step is not None and s.step != 1:
        return None
    return slice(start, stop, 1)


def get_mapping_from_left_to_right_columns_by_label(
    left_pandas_labels: list[Hashable],
    left_snowflake_quoted_identifiers: list[str],
    right_pandas_labels: list[Hashable],
    right_snowflake_quoted_identifiers: list[str],
) -> dict[str, Optional[str]]:
    """
    For each column on the left, find the column on the right that has the same pandas label, and produce a mapping
    between the left quoted identifier to the corresponding identifier on the right:
    1) if there is exactly one match on right, the left quoted identifier is mapped to the quoted identifier for
        the column on the right.
    2) if there is no match, the left quoted identifier is mapped to None
    e) if there is more than one match, the last matching column is used.

    For example:
     left_pandas_labels are ['A', 'B', 'C', 'C', 'E'], left_snowflake_quoted_identifiers are ['"A"', '"B"', '"C"', '"C_l1"', '"E"']
     right_pandas_labels are ['A', 'B', 'B', 'C', 'F'], left_snowflake_quoted_identifiers are ['"A_r"', '"B_r"', '"B_r1"', '"C_r"', '"F"']
     The result mapping should be
        {('"A"', '"A_r"'), ('"B"', '"B_r1"'), ('"C"', '"C_r1"'), ('"C_l1"', '"C_r"'), ('"E"', None)}
    """
    # the length of the left_pandas_labels and left_snowflake_quoted_identifiers must be the same
    assert len(left_pandas_labels) == len(left_snowflake_quoted_identifiers)
    # the length of the right_pandas_labels and right_snowflake_quoted_identifiers must be the same
    assert len(right_pandas_labels) == len(right_snowflake_quoted_identifiers)

    left_identifier_to_right_identifier: dict[str, Optional[str]] = {}
    # construct a map from right pandas label to right snowflake quoted_identifiers, since
    # python is an ordered map, if the same label occurred more than once, the last occurrence
    # is used.
    right_pandas_label_to_identifier_map = dict(
        zip(right_pandas_labels, right_snowflake_quoted_identifiers)
    )
    for pandas_label, quoted_identifier in zip(
        left_pandas_labels, left_snowflake_quoted_identifiers
    ):
        left_identifier_to_right_identifier[
            quoted_identifier
        ] = right_pandas_label_to_identifier_map.get(pandas_label, None)
    return left_identifier_to_right_identifier


def generate_column_identifier_random(n: int = _NUM_SUFFIX_DIGITS) -> str:
    """
    Generates a suffix for a Snowflake column identifier.
    This function can be used to de-duplicate the column identifier when
    pandas labels are duplicate.
    The default value of num_digits is 4 (_NUM_SUFFIX_DIGITS). The probability of a conflict is
    1 / ((26 + 10) ** 4) ~= 1e-7 (26 is the number of lower case letters
    and 10 is the number of digits), which is pretty small.
    """
    return generate_random_alphanumeric(n)


def get_distinct_rows(df: OrderedDataFrame) -> OrderedDataFrame:
    """
    Returns a new Snowpark DataFrame that contains only the rows with
    distinct values from the current Snowpark DataFrame.
    """
    return df.group_by(df.projected_column_snowflake_quoted_identifiers)


def count_rows(df: OrderedDataFrame) -> int:
    """
    Returns the number of rows of a Snowpark DataFrame.
    """
    if df.row_count is not None:
        return df.row_count
    df = df.ensure_row_count_column()
    rowset = df.select(df.row_count_snowflake_quoted_identifier).limit(1).collect()
    row_count = 0 if len(rowset) == 0 else rowset[0][0]
    df.row_count = row_count
    df.row_count_upper_bound = row_count
    return row_count


def append_columns(
    df: OrderedDataFrame,
    column_identifiers: Union[str, list[str]],
    column_objects: Union[Column, list[Column]],
) -> OrderedDataFrame:
    """
    Appends Snowpark columns to the end of a Snowpark DataFrame.
    If there are conflicting column identifiers, a ValueError will be raised.

    Args:
        df: Snowpark DataFrame
        column_identifiers: A list of column identifiers to be appended
        column_objects: A list of column objects to be appended

    Example 1:

        A DataFrame `df` with columns [A, B, C], we call append_columns(df, "D", col(...)),
        the result DataFrame has columns [A, B, C, D].

    Example 2:

        A DataFrame `df` with columns [A, B, C], we call append_columns(df, "C", col(...)),
        a ValueError will be raised.
    """
    if isinstance(column_identifiers, str):
        column_identifiers = [column_identifiers]
    if isinstance(column_objects, Column):
        column_objects = [column_objects]
    assert len(column_identifiers) == len(
        column_objects
    ), f"The number of column identifiers ({len(column_identifiers)}) is not equal to the number of column objects ({len(column_objects)})"

    new_columns = [
        column_object.as_(column_identifier)
        for column_identifier, column_object in zip(column_identifiers, column_objects)
    ]
    return df.select("*", *new_columns)


def cache_result(ordered_dataframe: OrderedDataFrame) -> OrderedDataFrame:
    """
    Cache the Snowpark dataframe result by creating a temp table out of the dataframe, and
    then recreate a snowpark dataframe out of the temp table.
    """
    session = ordered_dataframe.session
    temp_table_name = f'{session.get_current_database()}.{session.get_current_schema()}."{random_name_for_temp_object(TempObjectType.TABLE)}"'
    ordered_dataframe.write.save_as_table(
        temp_table_name,
        mode="errorifexists",
        table_type="temporary",
        statement_params=get_default_snowpark_pandas_statement_params(),
    )
    return OrderedDataFrame(
        DataFrameReference(session.table(temp_table_name)),
        projected_column_snowflake_quoted_identifiers=ordered_dataframe.projected_column_snowflake_quoted_identifiers,
        ordering_columns=ordered_dataframe.ordering_columns,
        row_position_snowflake_quoted_identifier=ordered_dataframe.row_position_snowflake_quoted_identifier,
    )


def create_frame_with_data_columns(
    frame: "frame.InternalFrame", data_column_pandas_labels: list[Hashable]
) -> "frame.InternalFrame":
    """
    Returns a new InternalFrame whose data columns
    are based on both the values and order of `pandas_labels`.

    Parameters
    ----------
    frame : InternalFrame
        Frame to select from.

    data_column_pandas_labels : List[Hashable]
        Subset of `frame`'s data columns to include in the new frame.

    Returns
    -------
    InternalFrame
        New InternalFrame with updated data columns.

    Raises
    ------
    ValueError
        If a label in `data_column_pandas_labels` does not exist in `frame`.
    """
    from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame

    new_frame_data_column_pandas_labels = []
    new_frame_data_column_snowflake_quoted_identifier = []
    new_frame_data_column_types = []

    data_column_label_to_snowflake_quoted_identifier = {
        data_column_pandas_label: data_column_snowflake_quoted_identifier
        for data_column_pandas_label, data_column_snowflake_quoted_identifier in zip(
            frame.data_column_pandas_labels,
            frame.data_column_snowflake_quoted_identifiers,
        )
    }

    for pandas_label in data_column_pandas_labels:
        snowflake_quoted_identifier = (
            data_column_label_to_snowflake_quoted_identifier.get(pandas_label, None)
        )

        if snowflake_quoted_identifier is None:
            raise ValueError(f"Label {pandas_label} does not exist in frame's columns.")

        new_frame_data_column_pandas_labels.append(pandas_label)
        new_frame_data_column_snowflake_quoted_identifier.append(
            snowflake_quoted_identifier
        )
        new_frame_data_column_types.append(
            frame.snowflake_quoted_identifier_to_snowpark_pandas_type.get(
                snowflake_quoted_identifier, None
            )
        )

    return InternalFrame.create(
        ordered_dataframe=frame.ordered_dataframe,
        data_column_pandas_labels=new_frame_data_column_pandas_labels,
        data_column_snowflake_quoted_identifiers=new_frame_data_column_snowflake_quoted_identifier,
        data_column_pandas_index_names=frame.data_column_pandas_index_names,
        index_column_pandas_labels=frame.index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=frame.index_column_snowflake_quoted_identifiers,
        data_column_types=new_frame_data_column_types,
        index_column_types=frame.cached_index_column_snowpark_pandas_types,
    )


def rindex(lst: list, value: int) -> int:
    """Find the last index in the list of item value."""
    return len(lst) - lst[::-1].index(value) - 1


def error_checking_for_init(
    index: Any, dtype: Union[str, np.dtype, ExtensionDtype]
) -> None:
    """
    Common error messages for the Series and DataFrame constructors.

    Parameters
    ----------
    index: Any
        The index to check.
    dtype: str, numpy.dtype, or ExtensionDtype
        The dtype to check.
    """
    from modin.pandas import DataFrame

    if isinstance(index, DataFrame):  # pandas raises the same error
        raise ValueError("Index data must be 1-dimensional")

    if dtype == "category":
        raise NotImplementedError("pandas type category is not implemented")


def assert_fields_are_none(
    class_name: str, data: Any, index: Any, dtype: Any, columns: Any = None
) -> None:
    assert (
        data is None
    ), f"Invalid {class_name} construction! The `data` parameter is not supported when `query_compiler` is given."
    assert (
        index is None
    ), f"Invalid {class_name} construction! The `index` parameter is not supported when `query_compiler` is given."
    assert (
        dtype is None
    ), f"Invalid {class_name} construction! The `dtype` parameter is not supported when `query_compiler` is given."
    assert (
        columns is None
    ), f"Invalid {class_name} construction! The `columns` parameter is not supported when `query_compiler` is given."


def convert_index_to_qc(index: Any) -> "SnowflakeQueryCompiler":
    """
    Method to convert an object representing an index into a query compiler for set_index or reindex.

    Parameters
    ----------
    index: Any
        The object to convert to a query compiler.

    Returns
    -------
    SnowflakeQueryCompiler
        The converted query compiler.
    """
    from modin.pandas import Series

    from snowflake.snowpark.modin.plugin.extensions.index import Index

    if isinstance(index, Index):
        idx_qc = index.to_series()._query_compiler
    elif isinstance(index, Series):
        # The name of the index comes from the Series' name, not the index name. `reindex` does not handle this,
        # so we need to set the name of the index to the name of the Series.
        index.index.name = index.name
        idx_qc = index._query_compiler
    else:
        idx_qc = Series(index)._query_compiler
    return idx_qc


def convert_index_to_list_of_qcs(index: Any) -> list:
    """
    Method to convert an object representing an index into a list of query compilers for set_index.

    Parameters
    ----------
    index: Any
        The object to convert to a list of query compilers.

    Returns
    -------
    list
        The list of query compilers.
    """
    from modin.pandas import Series

    from snowflake.snowpark.modin.plugin.extensions.index import Index

    if (
        not isinstance(index, (native_pd.MultiIndex, Series, Index))
        and is_list_like(index)
        and len(index) > 0
        and all((is_list_like(i) and not isinstance(i, tuple)) for i in index)
    ):
        # If given a list of lists, convert it to a MultiIndex.
        index = native_pd.MultiIndex.from_arrays(index)
    if isinstance(index, native_pd.MultiIndex):
        index_qc_list = [
            s._query_compiler
            for s in [
                Series(index.get_level_values(level)) for level in range(index.nlevels)
            ]
        ]
    else:
        index_qc_list = [convert_index_to_qc(index)]
    return index_qc_list


def add_extra_columns_and_select_required_columns(
    query_compiler: "SnowflakeQueryCompiler",
    columns: Union[AnyArrayLike, list],
) -> "SnowflakeQueryCompiler":
    """
    Method to add extra columns to and select the required columns from the provided query compiler.
    This is used in DataFrame construction in the following cases:
    - general case when data is a DataFrame
    - data is a named Series, and this name is in `columns`

    Parameters
    ----------
    query_compiler: Any
        The query compiler to select columns from, i.e., data's query compiler.
    columns: AnyArrayLike or list
        The columns to select from the query compiler.
    """
    from modin.pandas import DataFrame

    data_columns = query_compiler.get_columns().to_list()
    # The `columns` parameter is used to select the columns from `data` that will be in the resultant DataFrame.
    # If a value in `columns` is not present in data's columns, it will be added as a new column filled with NaN values.
    # These columns are tracked by the `extra_columns` variable.
    if data_columns is not None and columns is not None:
        extra_columns = [col for col in columns if col not in data_columns]
        if extra_columns is not []:
            # To add these new columns to the DataFrame, perform `__setitem__` only with the extra columns
            # and set them to None.
            extra_columns_df = DataFrame(query_compiler=query_compiler)
            # In the case that the columns are MultiIndex but not all extra columns are tuples, we need to flatten the
            # columns to ensure that the columns are a single-level index. If not, `__setitem__` will raise an error
            # when trying to add new columns that are not in the expected tuple format.
            if not all(isinstance(col, tuple) for col in extra_columns) and isinstance(
                query_compiler.get_columns(), native_pd.MultiIndex
            ):
                flattened_columns = extra_columns_df.columns.to_flat_index()
                extra_columns_df.columns = flattened_columns
            extra_columns_df[extra_columns] = None
            query_compiler = extra_columns_df._query_compiler

    # To select the columns for the resultant DataFrame, perform `take_2d_labels` on the created query compiler.
    # This is the equivalent of `__getitem__` for a DataFrame.
    # This step is performed to ensure that the right columns are picked from the InternalFrame since we never
    # explicitly drop the unwanted columns. This also ensures that the columns in the resultant DataFrame are in the
    # same order as the columns in the `columns` parameter.
    return query_compiler.take_2d_labels(slice(None), columns)
