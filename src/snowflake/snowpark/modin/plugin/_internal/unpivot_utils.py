#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import json
import typing
from collections.abc import Hashable
from typing import Optional

from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    quote_name_without_upper_casing,
)
from snowflake.snowpark.column import CaseExpr, Column as SnowparkColumn
from snowflake.snowpark.functions import (
    cast,
    coalesce,
    col,
    get,
    get_path,
    is_null,
    lit,
    object_construct,
    parse_json,
    to_variant,
    when,
)
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin._internal.ordered_dataframe import (
    OrderedDataFrame,
    OrderingColumn,
)
from snowflake.snowpark.modin.plugin._internal.utils import (
    append_columns,
    generate_column_identifier_random,
    pandas_lit,
    unquote_name_if_quoted,
)
from snowflake.snowpark.types import ArrayType, MapType, StringType, VariantType

# Separate set of columns for unpivot w/o transpose for
# clarity
UNPIVOT_INDEX = "UNPIVOT_IDX"
# unpivot value column used in unpivot
UNPIVOT_VALUE_COLUMN = "UNPIVOT_VALUE"
# unpivot name column used in unpivot
UNPIVOT_NAME_COLUMN = "UNPIVOT_VARIABLE"
# unpivot json parsed object name
UNPIVOT_OBJ_NAME_COLUMN = "UNPIVOT_OBJ_NAME"

VALUE_COLUMN_FOR_SINGLE_ROW = '\'{"0":"NULL","row":0}\''
ROW_KEY = "row"
# the value used to replace the NULL for unpivot columns
UNPIVOT_NULL_REPLACE_VALUE = "NULL_REPLACE"
UNPIVOT_ORDERING_COLUMN_PREFIX = "UNPIVOT_ORDERING_"
UNPIVOT_SINGLE_INDEX_PREFIX = "UNPIVOT_SINGLE_INDEX"

# Default column names for pandas melt
DEFAULT_PANDAS_UNPIVOT_VARIABLE_NAME = "variable"
DEFAULT_PANDAS_UNPIVOT_VALUE_NAME = "value"


class UnpivotResultInfo(typing.NamedTuple):
    """
    Structure that stores information about the unpivot result.

    Parameters
    ----------
    ordered_dataframe: OrderedDataFrame
        Resulting ordered dataframe.
    index_snowflake_quoted_identifier: str
        index column used in unpivot.
    new_value_quoted_identifier: str
        value column used in unpivot.
    variable_name_quoted_snowflake_identifier: str
        variable name column used in unpivot.
    object_name_quoted_snowflake_identifier: str
        json parsed object column used in unpivot.
    pandas_id_columns: list[Hashable]
        list of columns which are "identifier" columns in the
        unpivot which are untouched by the unpivot operation
    snowflake_id_quoted_columns: list[str]
        list of pandas_id_columns, quoted.

    """

    ordered_dataframe: OrderedDataFrame
    index_snowflake_quoted_identifier: str
    new_value_quoted_identifier: str
    variable_name_quoted_snowflake_identifier: str
    object_name_quoted_snowflake_identifier: str
    pandas_id_columns: list[Hashable]
    snowflake_id_quoted_columns: list[str]


def unpivot(
    original_frame: InternalFrame,
    pandas_id_columns: list[Hashable],
    pandas_value_columns: list[Hashable],
    pandas_var_name: Optional[Hashable],
    pandas_value_name: Optional[Hashable],
    ignore_index: Optional[bool],
) -> InternalFrame:
    """
    Performs an unpivot/melt operation using one of two methods, a faster method which does not support
    preserving an index and duplicate columns and a slower method which uses the same unpivot
    operation used for transpose. If the dataframe has these complications we must use the more general
    method which moves the column data in and out of json and handles complex indexes.

    Args:
        original_frame: InternalFrame prior to unpivot
        pandas_id_columns: a list of identity columns to preserve in the output (unpivoted)
        pandas_value_columns: a list of value columns to unpivot
        pandas_var_name: the name of the "variable" column
        pandas_value_name: the name of the "value" column
        ignore_index: whether to ignore the index or not - default is ignore, and it uses the simple unpivot

    Returns:
        An InternalFrame as a result of the unpivot
    """
    if _can_use_simple_unpivot(
        ignore_index=ignore_index, pandas_value_columns=pandas_value_columns
    ):
        return _simple_unpivot(
            original_frame=original_frame,
            pandas_id_columns=pandas_id_columns,
            pandas_value_columns=pandas_value_columns,
            pandas_var_name=pandas_var_name,
            pandas_value_name=pandas_value_name,
        )

    return _general_unpivot(
        original_frame=original_frame,
        pandas_id_columns=pandas_id_columns,
        pandas_value_columns=pandas_value_columns,
        pandas_var_name=pandas_var_name,
        pandas_value_name=pandas_value_name,
        ignore_index=ignore_index,
    )


def _can_use_simple_unpivot(
    ignore_index: Optional[bool], pandas_value_columns: list[Hashable]
) -> bool:
    """
    Determines if the simplified unpivot can be used.

    Args:
        ignore_index: are we supposed to ignore the index
        pandas_value_columns: a list of value columns to unpivot
    Returns:
        True if we can use the simple unpivot, false otherwise
    """
    # df.melt defaults to ignoring the index
    if ignore_index is False:
        return False
    # to use the simple unpivot, all columns should be strings
    if not all(isinstance(col, str) for col in pandas_value_columns):
        return False
    # columns should not have duplicates
    if len(set(pandas_value_columns)) != len(pandas_value_columns):
        return False
    return True


def _general_unpivot(
    original_frame: InternalFrame,
    pandas_id_columns: list[Hashable],
    pandas_value_columns: list[Hashable],
    pandas_var_name: Optional[Hashable],
    pandas_value_name: Optional[Hashable],
    ignore_index: Optional[bool],
) -> InternalFrame:
    unpivot_result = _prepare_unpivot_internal(
        original_frame=original_frame,
        ordered_dataframe=original_frame.ordered_dataframe,
        is_single_row=False,
        index_column_name=UNPIVOT_INDEX,
        value_column_name=UNPIVOT_VALUE_COLUMN,
        variable_column_name=UNPIVOT_NAME_COLUMN,
        object_column_name=UNPIVOT_OBJ_NAME_COLUMN,
        pandas_id_columns=pandas_id_columns,
        pandas_value_columns=pandas_value_columns,
    )

    return clean_up_unpivot(
        original_frame=original_frame,
        ordered_unpivoted_df=unpivot_result.ordered_dataframe,
        unpivot_index_snowflake_identifier=unpivot_result.index_snowflake_quoted_identifier,
        new_value_quoted_snowflake_identifier=unpivot_result.new_value_quoted_identifier,
        variable_final_column_name=DEFAULT_PANDAS_UNPIVOT_VARIABLE_NAME
        if pandas_var_name is None
        else pandas_var_name,
        value_final_column_name=DEFAULT_PANDAS_UNPIVOT_VALUE_NAME
        if pandas_value_name is None
        else pandas_value_name,
        pandas_id_columns=unpivot_result.pandas_id_columns,
        snowflake_id_quoted_columns=unpivot_result.snowflake_id_quoted_columns,
        ignore_index=ignore_index,
    )


def _prepare_unpivot_internal(
    original_frame: InternalFrame,
    ordered_dataframe: OrderedDataFrame,
    is_single_row: bool,
    index_column_name: Hashable,
    value_column_name: Hashable,
    variable_column_name: Hashable,
    object_column_name: Hashable,
    pandas_id_columns: Optional[list[Hashable]] = None,
    pandas_value_columns: Optional[list[Hashable]] = None,
) -> UnpivotResultInfo:  # type: ignore[name-defined] # noqa: F821
    """
    Performs the first steps required to unpivot or transpose this QueryCompiler. This includes constructing a temporary index
    with position information, and then applying an unpivot operation.
    When is_single_row is true, the pandas label for the result column will be lost, and set to "None".

    Args:
        original_frame: InternalFrame prior to unpivot
        is_single_row: indicator to short-circuit some behavior for unpivot
        index_column_name: internal name used for the index reference column
        value_column_name: internal name used for the value column from the unpivot operation
        variable_column_name: internal name used for the variable column from the unpivot
        object_column_name: internal name used for storing serialized column names and positions
        pandas_id_columns: list of passthrough identity columns which are untouched by the unpivot
        pandas_value_vars: list of columns to unpivot, if None, all will be unpivoted

    Returns:
        a list consisting of the unpivoted OrderedDataFrame and a group of quoted identifiers that are required for
        the following transpose steps of pivot and cleanup (or just cleanup).
    """
    ##############################################################################
    # Unpivot / Transpose are Complicated operations. The following example
    # dataframe is used to show the intermediate results of the dataframe at each step
    # using the melt operation (unpivot).
    #
    # data = {"abc": ["A", "B", np.nan], "123": [1, np.nan, 3], "state": ["CA", "WA", "NY"]}
    # index = npd.MultiIndex.from_tuples([("one", "there"), ("two", "be"), ("two", "dragons")],
    #                                     names=["L1", "L2"])
    # df = npd.DataFrame(data, index=index)
    # df
    #              abc  123 state
    # L1  L2
    # one there      A  1.0    CA
    # two be         B  NaN    WA
    #     dragons  NaN  3.0    NY
    #
    # df.melt(id_vars=["state"],
    #         value_vars=["abc", "123"],
    #         ignore_index=False,
    #         var_name = "independent",
    #         value_name = "dependent")
    #
    #             state independent dependent
    # L1  L2
    # one there      CA         abc         A
    # two be         WA         abc         B
    #     dragons    NY         abc       NaN
    # one there      CA         123       1.0
    # two be         WA         123       NaN
    #     dragons    NY         123       3.0
    #
    # ordered_frame.to_pandas() prior to executing this function
    #       __L1__   __L2__   abc  123 state  __row_position__
    # 0    one    there     A  1.0    CA                 0
    # 1    two       be     B  NaN    WA                 1
    # 2    two  dragons  None  3.0    NY                 2
    #
    if pandas_id_columns is None:
        pandas_id_columns = []
    if pandas_value_columns is None:
        pandas_value_columns = []

    row_position_snowflake_quoted_identifier = (
        original_frame.row_position_snowflake_quoted_identifier
    )
    # ordered_dataframe.to_pandas() at this point
    #   __L1__   __L2__   abc  123 state  __row_position__
    # 0    one    there     A  1.0    CA                -1 <--- DUMMY ROW (For transpose)
    # 1    one    there     A  1.0    CA                 0
    # 2    two       be     B  NaN    WA                 1
    # 3    two  dragons  None  3.0    NY                 2
    #
    # The following two steps correspond to STEPS (1) and (2) in the four steps described in
    # SnowflakeQueryCompiler.transpose().

    # STEP 1) Construct a temporary index column that contains the original index with position, so for example if
    # there was a multi-level index ['name', 'score'] with index values ('alice', 9.5), ('bob', 8) this would
    # be serialized into a single column with values {"0":"alice","1":9.5,"row":0}, {"0":"bob","1":8,"row":1} where
    # the key refers to the relative index level and row refers to the row_position.
    index_object_construct_key_values = [
        pandas_lit(ROW_KEY),
        col(row_position_snowflake_quoted_identifier),
    ]
    for i, snowflake_quoted_identifier in enumerate(
        original_frame.index_column_snowflake_quoted_identifiers
    ):
        index_object_construct_key_values.append(pandas_lit(str(i)))
        index_object_construct_key_values.append(col(snowflake_quoted_identifier))

    unpivot_index_snowflake_identifier = (
        original_frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
            pandas_labels=[index_column_name],
        )[0]
    )

    normalize_unpivot_select_list = [
        object_construct(*index_object_construct_key_values)
        .cast(StringType())
        .as_(unpivot_index_snowflake_identifier)
    ]

    # For the remaining data columns, we need to also transpose the position since this information later need
    # to be mapped to the row_position to match expected ordering.  We do this by aliasing the column name to
    # include the column position.  For example, columns 'employed', 'kids' would be aliased to json array
    # [1, "employed"] and [2, "kids"] respectively.  Note that the unpivot columns must have same type across
    # all unpivot columns, so we also cast to variant here if not all data types of the data columns are the same.

    # If the original frame had *all* the same data types, then we can preserve this here otherwise
    # we need to default to variant.
    frame_data_type_map = original_frame.quoted_identifier_to_snowflake_type()
    original_data_types = {
        frame_data_type_map.get(snowflake_quoted_identifier)
        for snowflake_quoted_identifier in original_frame.data_column_snowflake_quoted_identifiers
    }
    output_data_type = (
        original_data_types.pop() if len(original_data_types) == 1 else VariantType()
    )
    # If the computed data type is ARRAY or MAP type, then we must convert it to VARIANT. This
    # is particularly important when this unpivot opertion is used as part of a transpose operation
    # because the PIVOT does not allow for aggregation.
    # Since pandas represents array types with the `object` dtype, which VARIANT is converted
    # to in post-processing, this does not cause any differences in behavior.
    if isinstance(output_data_type, (ArrayType, MapType)):
        output_data_type = VariantType()

    unpivot_columns = []
    passthrough_columns = []
    passthrough_quoted_columns = []
    for i, (pandas_label, snowflake_quoted_identifier) in enumerate(
        zip(
            original_frame.data_column_pandas_labels,
            original_frame.data_column_snowflake_quoted_identifiers,
        )
    ):
        # Filter columns from the unpivot list if needed
        is_id_col = len(pandas_id_columns) > 0 and pandas_label in pandas_id_columns

        is_var_col = (
            len(pandas_value_columns) == 0 or pandas_label in pandas_value_columns
        )
        if is_id_col:
            passthrough_columns.append(pandas_label)
            passthrough_quoted_columns.append(snowflake_quoted_identifier)
            continue
        if not is_var_col:
            continue
        # Generate a random suffix to avoid conflict if there is already label [i, pandas_label].
        # Since the serialized_name must be a valid json format, we add the suffix as an extra component
        # of the list, instead of de-conflict on top of the serialized_name. The suffix will be
        # automatically discarded during label extraction to get the correct label.
        serialized_name = quote_name_without_upper_casing(
            json.dumps([i, pandas_label, generate_column_identifier_random()])
        )
        normalize_unpivot_select_list.append(
            # Replace NULLs in the column with value UNPIVOT_NULL_REPLACE_VALUE. The column is cast to
            # variant column first, so that we can replace NULLs with a value without considering the column
            # data type.
            coalesce(
                to_variant(snowflake_quoted_identifier),
                to_variant(pandas_lit(UNPIVOT_NULL_REPLACE_VALUE)),
            ).as_(serialized_name)
        )
        unpivot_columns.append(serialized_name)

    ordered_dataframe = ordered_dataframe.select(
        normalize_unpivot_select_list
        + original_frame.data_column_snowflake_quoted_identifiers
    )

    # ordered_dataframe.to_pandas() at this point
    #                              UNPIVOT_IDX [0, "abc", "sxi8"]     [1, "123", "uhkz"]   abc  123 state
    # 0   {"0":"one","1":"there","row":-1}                "A"  1.000000000000000e+00     A  1.0    CA
    # 1    {"0":"one","1":"there","row":0}                "A"  1.000000000000000e+00     A  1.0    CA
    # 2       {"0":"two","1":"be","row":1}                "B"         "NULL_REPLACE"     B  NaN    WA
    # 3  {"0":"two","1":"dragons","row":2}     "NULL_REPLACE"  3.000000000000000e+00  None  3.0    NY

    # STEP 2) Perform an unpivot which flattens the original data columns into a single name and value rows
    # grouped by the temporary transpose index column.  In the earlier example, this would flatten the non-index
    # data into individual rows grouped by the index (UNPIVOT_INDEX) which later becomes the transposed
    # column labels.
    (
        unpivot_value_quoted_snowflake_identifier,
        unpivot_name_quoted_snowflake_identifier,
        unpivot_object_name_quoted_snowflake_identifier,
    ) = ordered_dataframe.generate_snowflake_quoted_identifiers(
        pandas_labels=[
            value_column_name,
            variable_column_name,
            object_column_name,
        ],
    )

    ordered_dataframe = ordered_dataframe.unpivot(
        unpivot_value_quoted_snowflake_identifier,
        unpivot_name_quoted_snowflake_identifier,
        unpivot_columns,
    )

    # ordered_dataframe.to_pandas() at this point
    #                          UNPIVOT_IDX   abc  123 state    UNPIVOT_VARIABLE          UNPIVOT_VALUE
    # 0   {"0":"one","1":"there","row":-1}     A  1.0    CA  [0, "abc", "sxi8"]                    "A"
    # 1   {"0":"one","1":"there","row":-1}     A  1.0    CA  [1, "123", "uhkz"]  1.000000000000000e+00
    # 2    {"0":"one","1":"there","row":0}     A  1.0    CA  [0, "abc", "sxi8"]                    "A"
    # 3    {"0":"one","1":"there","row":0}     A  1.0    CA  [1, "123", "uhkz"]  1.000000000000000e+00
    # 4       {"0":"two","1":"be","row":1}     B  NaN    WA  [0, "abc", "sxi8"]                    "B"
    # 5       {"0":"two","1":"be","row":1}     B  NaN    WA  [1, "123", "uhkz"]         "NULL_REPLACE"
    # 6  {"0":"two","1":"dragons","row":2}  None  3.0    NY  [0, "abc", "sxi8"]         "NULL_REPLACE"
    # 7  {"0":"two","1":"dragons","row":2}  None  3.0    NY  [1, "123", "uhkz"]  3.000000000000000e+00
    assert (
        len(original_frame.data_column_snowflake_quoted_identifiers) > 0
    ), "no data column to unpivot"

    # Replace the null value back by checking if the value in the origin data column is null and also the
    # unpivot name column value is original data column name.
    case_conditions: list[SnowparkColumn] = []
    for origin_data_column, serialized_name in zip(
        original_frame.data_column_snowflake_quoted_identifiers, unpivot_columns
    ):
        unquoted_serialized_name = unquote_name_if_quoted(serialized_name)
        case_conditions.append(
            (
                col(unpivot_value_quoted_snowflake_identifier)
                == pandas_lit(UNPIVOT_NULL_REPLACE_VALUE)
            )
            & is_null(origin_data_column)
            & (
                col(unpivot_name_quoted_snowflake_identifier)
                == pandas_lit(unquoted_serialized_name)
            )
        )
    case_expr: CaseExpr = when(case_conditions[0], pandas_lit(None))
    for case_condition in case_conditions[1:]:
        case_expr = case_expr.when(case_condition, pandas_lit(None))

    # add otherwise clause
    case_column = case_expr.otherwise(col(unpivot_value_quoted_snowflake_identifier))
    unpivot_value_column = (
        value_column_name
        if not is_single_row
        # Since step 3 is skipped for single-row dataframes, the value below is chosen such that it
        # simulates the output of step 3 and becomes compatible with step 4.
        else VALUE_COLUMN_FOR_SINGLE_ROW
    )
    new_unpivot_value_quoted_identifier = (
        ordered_dataframe.generate_snowflake_quoted_identifiers(
            pandas_labels=[unpivot_value_column],
        )[0]
    )
    # cast the column back to the desired data type output_data_type
    case_column = cast(case_column, output_data_type).as_(
        new_unpivot_value_quoted_identifier
    )
    select_col_names = [] + passthrough_quoted_columns
    select_col_names += [unpivot_name_quoted_snowflake_identifier]
    if not is_single_row:
        select_col_names += [unpivot_index_snowflake_identifier]
    ordered_dataframe = ordered_dataframe.select(
        *select_col_names,
        case_column,
    )
    # ordered_dataframe.to_pandas() at this point
    #   state    UNPIVOT_VARIABLE                        UNPIVOT_IDX      UNPIVOT_VALUE_brl1
    # 0    CA  [0, "abc", "sxi8"]   {"0":"one","1":"there","row":-1}                    "A"
    # 1    CA  [1, "123", "uhkz"]   {"0":"one","1":"there","row":-1}  1.000000000000000e+00
    # 2    CA  [0, "abc", "sxi8"]    {"0":"one","1":"there","row":0}                    "A"
    # 3    CA  [1, "123", "uhkz"]    {"0":"one","1":"there","row":0}  1.000000000000000e+00
    # 4    WA  [0, "abc", "sxi8"]       {"0":"two","1":"be","row":1}                    "B"
    # 5    WA  [1, "123", "uhkz"]       {"0":"two","1":"be","row":1}                   None
    # 6    NY  [0, "abc", "sxi8"]  {"0":"two","1":"dragons","row":2}                   None
    # 7    NY  [1, "123", "uhkz"]  {"0":"two","1":"dragons","row":2}  3.000000000000000e+00
    # Parse the json object unpivot name column because we will need to extract the row position and, in the case
    # of multi-level index, parse each level into a different index column.
    ordered_dataframe = append_columns(
        ordered_dataframe,
        unpivot_object_name_quoted_snowflake_identifier,
        parse_json(unpivot_name_quoted_snowflake_identifier),
    )
    # ordered_dataframe.to_pandas() at this point
    #   state    UNPIVOT_VARIABLE                        UNPIVOT_IDX     UNPIVOT_VALUE_brl1                UNPIVOT_OBJ_NAME
    # 0    CA  [0, "abc", "sxi8"]   {"0":"one","1":"there","row":-1}                    "A"  [\n  0,\n  "abc",\n  "sxi8"\n]
    # 1    CA  [1, "123", "uhkz"]   {"0":"one","1":"there","row":-1}  1.000000000000000e+00  [\n  1,\n  "123",\n  "uhkz"\n]
    # 2    CA  [0, "abc", "sxi8"]    {"0":"one","1":"there","row":0}                    "A"  [\n  0,\n  "abc",\n  "sxi8"\n]
    # 3    CA  [1, "123", "uhkz"]    {"0":"one","1":"there","row":0}  1.000000000000000e+00  [\n  1,\n  "123",\n  "uhkz"\n]
    # 4    WA  [0, "abc", "sxi8"]       {"0":"two","1":"be","row":1}                    "B"  [\n  0,\n  "abc",\n  "sxi8"\n]
    # 5    WA  [1, "123", "uhkz"]       {"0":"two","1":"be","row":1}                   None  [\n  1,\n  "123",\n  "uhkz"\n]
    # 6    NY  [0, "abc", "sxi8"]  {"0":"two","1":"dragons","row":2}                   None  [\n  0,\n  "abc",\n  "sxi8"\n]
    # 7    NY  [1, "123", "uhkz"]  {"0":"two","1":"dragons","row":2}  3.000000000000000e+00  [\n  1,\n  "123",\n  "uhkz"\n]
    return UnpivotResultInfo(
        ordered_dataframe,
        unpivot_index_snowflake_identifier,
        new_unpivot_value_quoted_identifier,
        unpivot_name_quoted_snowflake_identifier,
        unpivot_object_name_quoted_snowflake_identifier,
        passthrough_columns,
        passthrough_quoted_columns,
    )


def clean_up_unpivot(
    original_frame: InternalFrame,
    ordered_unpivoted_df: OrderedDataFrame,
    unpivot_index_snowflake_identifier: str,
    new_value_quoted_snowflake_identifier: str,
    variable_final_column_name: Hashable,
    value_final_column_name: Hashable,
    pandas_id_columns: Optional[list[Hashable]] = None,
    snowflake_id_quoted_columns: Optional[list[str]] = None,
    ignore_index: Optional[bool] = False,
) -> InternalFrame:
    """
    Cleans up an unpivot operation and reconstructs the index.

    Args:
        original_frame: The original InternalFrame for the transpose
        ordered_transposed_df: The transposed ordered dataframe
        unpivot_index_snowflake_identifier: column name of the unpivot index
        new_value_quoted_snowflake_identifier: intermediate column name for the "value" column
        variable_final_column_name: pandas column name for the "variable" of the unpivot
        value_final_column_name: pandas column name for the "value" of the unpivot
        pandas_id_columns: set of columns left untouched by the pivot operation
        snowflake_id_quoted_columns: quoted version of the passthrough columns
        ignore_index: if False, reconstruct the index of the original dataframe

    Returns:
        The unpivoted InternalFrame.
    """
    # ordered_dataframe.to_pandas() at this point
    #   state    UNPIVOT_VARIABLE                        UNPIVOT_IDX     UNPIVOT_VALUE_brl1                UNPIVOT_OBJ_NAME
    # 0    CA  [0, "abc", "sxi8"]    {"0":"one","1":"there","row":0}                    "A"  [\n  0,\n  "abc",\n  "sxi8"\n]
    # 1    CA  [1, "123", "uhkz"]    {"0":"one","1":"there","row":0}  1.000000000000000e+00  [\n  1,\n  "123",\n  "uhkz"\n]
    # 2    WA  [0, "abc", "sxi8"]       {"0":"two","1":"be","row":1}                    "B"  [\n  0,\n  "abc",\n  "sxi8"\n]
    # 3    WA  [1, "123", "uhkz"]       {"0":"two","1":"be","row":1}                   None  [\n  1,\n  "123",\n  "uhkz"\n]
    # 4    NY  [0, "abc", "sxi8"]  {"0":"two","1":"dragons","row":2}                   None  [\n  0,\n  "abc",\n  "sxi8"\n]
    # 5    NY  [1, "123", "uhkz"]  {"0":"two","1":"dragons","row":2}  3.000000000000000e+00  [\n  1,\n  "123",\n  "uhkz"\n]

    if pandas_id_columns is None:
        pandas_id_columns = []
    if snowflake_id_quoted_columns is None:
        snowflake_id_quoted_columns = []

    value_column_quoted = f'"{value_final_column_name}"'
    variables_column_quoted = f'"{variable_final_column_name}"'
    value_column = col(new_value_quoted_snowflake_identifier).as_(value_column_quoted)

    ordering_column_names = ordered_unpivoted_df.generate_snowflake_quoted_identifiers(
        pandas_labels=[
            "col_order" + generate_column_identifier_random(),
            "row_order" + generate_column_identifier_random(),
        ],
    )

    # Extract new ordering columns
    col_order_column = get(col(UNPIVOT_OBJ_NAME_COLUMN), 0).as_(
        ordering_column_names[0]
    )
    row_order_column = get_path(
        parse_json(col(unpivot_index_snowflake_identifier)), lit(ROW_KEY)
    ).as_(ordering_column_names[1])

    # Reconstruct the index
    index_columns = []
    index_column_names = [None]
    index_column_pandas_names = [None]
    is_index_set = original_frame.num_index_columns > 0
    is_multi_index = (
        len(original_frame.index_column_pandas_labels) > 1
        and original_frame.index_column_pandas_labels[0] is not None
    )
    if ignore_index is False and is_index_set:
        if is_multi_index:
            index_column_names = (
                original_frame.index_column_snowflake_quoted_identifiers
            )
            index_column_pandas_names = original_frame.index_column_pandas_labels
        else:
            index_column_names = (
                ordered_unpivoted_df.generate_snowflake_quoted_identifiers(
                    pandas_labels=[
                        UNPIVOT_SINGLE_INDEX_PREFIX
                        + generate_column_identifier_random(),
                    ],
                )
            )
        for level in range(len(index_column_names)):
            index_column_name = index_column_names[level]
            index_columns.append(
                get_path(
                    parse_json(col(unpivot_index_snowflake_identifier)),
                    lit(f'"{level}"'),
                ).as_(index_column_name)
            )

    # extract the variable column and rename
    variable_column = get(col(UNPIVOT_OBJ_NAME_COLUMN), 1).as_(variables_column_quoted)

    projected_columns = (
        index_columns
        + snowflake_id_quoted_columns
        + [
            col_order_column,
            row_order_column,
            variable_column,
            value_column,
        ]
    )
    ordered_dataframe = ordered_unpivoted_df.select(projected_columns)

    # ordered_dataframe.to_pandas() at this point
    #   __L1__     __L2__ col_orderb6wa row_ordery6hw {variable_final_column_name} {value_final_column_name}
    # 0  "one"    "there"             0             0                        "abc"                       "A"
    # 1  "one"    "there"             1             0                        "123"     1.000000000000000e+00
    # 2  "two"       "be"             0             1                        "abc"                       "B"
    # 3  "two"       "be"             1             1                        "123"                      None
    # 4  "two"  "dragons"             0             2                        "abc"                      None
    # 5  "two"  "dragons"             1             2                        "123"     3.000000000000000e+00

    # sort by the ordering columns
    ordered_dataframe = ordered_dataframe.sort(
        OrderingColumn(ordering_column_names[0]),
        OrderingColumn(ordering_column_names[1]),
    )

    final_pandas_labels = pandas_id_columns + [
        variable_final_column_name,
        value_final_column_name,
    ]
    final_snowflake_qouted_identfiers = snowflake_id_quoted_columns + [
        variables_column_quoted,
        value_column_quoted,
    ]
    ordered_dataframe = ordered_dataframe.ensure_row_position_column()

    # setup the index names for the internal frame
    index_column_quoted_names = [
        ordered_dataframe.row_position_snowflake_quoted_identifier
    ]
    if not ignore_index and is_index_set:
        index_column_quoted_names = index_column_names

    new_internal_frame = InternalFrame.create(
        ordered_dataframe=ordered_dataframe,
        data_column_pandas_labels=final_pandas_labels,
        data_column_pandas_index_names=[None],
        data_column_snowflake_quoted_identifiers=final_snowflake_qouted_identfiers,
        index_column_pandas_labels=index_column_pandas_names,
        index_column_snowflake_quoted_identifiers=index_column_quoted_names,
    )

    # Rename the data column snowflake quoted identifiers to be closer to pandas labels, normalizing names
    # will remove information like row position that may have temporarily been included in column names to track
    # during earlier steps.
    new_internal_frame = (
        new_internal_frame.normalize_snowflake_quoted_identifiers_with_pandas_label()
    )
    # full ordered_dataframe.to_pandas() at this point
    #       L1         L2 col_ordermg7c row_orderiq3v independent              dependent                        UNPIVOT_IDX   abc  123 state    UNPIVOT_VARIABLE          UNPIVOT_VALUE  __row_position__
    # 0  "one"    "there"             0             0       "abc"                    "A"    {"0":"one","1":"there","row":0}     A  1.0    CA  [0, "abc", "z851"]                    "A"                 0
    # 1  "two"       "be"             0             1       "abc"                    "B"       {"0":"two","1":"be","row":1}     B  NaN    WA  [0, "abc", "z851"]                    "B"                 1
    # 2  "two"  "dragons"             0             2       "abc"                   None  {"0":"two","1":"dragons","row":2}  None  3.0    NY  [0, "abc", "z851"]         "NULL_REPLACE"                 2
    # 3  "one"    "there"             1             0       "123"  1.000000000000000e+00    {"0":"one","1":"there","row":0}     A  1.0    CA  [1, "123", "kuxa"]  1.000000000000000e+00                 3
    # 4  "two"       "be"             1             1       "123"                   None       {"0":"two","1":"be","row":1}     B  NaN    WA  [1, "123", "kuxa"]         "NULL_REPLACE"                 4
    # 5  "two"  "dragons"             1             2       "123"  3.000000000000000e+00  {"0":"two","1":"dragons","row":2}  None  3.0    NY  [1, "123", "kuxa"]  3.000000000000000e+00                 5
    return new_internal_frame


def _simple_unpivot(
    original_frame: InternalFrame,
    pandas_id_columns: list[Hashable],
    pandas_value_columns: list[Hashable],
    pandas_var_name: Optional[Hashable],
    pandas_value_name: Optional[Hashable],
) -> InternalFrame:
    """
    Performs a melt/unpivot on a a dataframe, when the index can be
    ignored. Does not handle multi-index or duplicate column names.

    Args:
        original_frame: InternalFrame prior to unpivot
        pandas_id_columns: identity columns which should be retained, untouched in the result
        pandas_value_columns: columns to unpivot, if empty all columns are unpivoted
        pandas_var_name: name used for the variable column from the unpivot
        pandas_value_name: name used for the value column from the unpivot operation

    Returns:
        An unpivoted dataframe, similar to the melt semantics
    """
    frame = original_frame
    ordered_dataframe = frame.ordered_dataframe

    ##########################################
    # OrderedDataFrame at this Point
    ##########################################
    #    __index__   abc   123  __row_position__
    # 0          0     A     1                 0
    # 1          1  None     2                 1
    # 2          2     C  None                 2

    # create a column name to be used for ordering after the melt, based on
    # column posiiton
    ordering_column_name = ordered_dataframe.generate_snowflake_quoted_identifiers(
        pandas_labels=[UNPIVOT_ORDERING_COLUMN_PREFIX],
    )[0]

    # output columns for variable and values
    (
        var_quoted,
        value_quoted,
    ) = ordered_dataframe.generate_snowflake_quoted_identifiers(
        pandas_labels=[
            DEFAULT_PANDAS_UNPIVOT_VARIABLE_NAME
            if pandas_var_name is None
            else pandas_var_name,
            pandas_value_name,
        ],
    )

    # create the initial set of columns to be retained as identifiers and those
    # which will be unpivoted. Collect data type information.
    unpivot_quoted_columns = []
    ordering_decode_conditions = []
    id_col_names = []
    id_col_quoted_identifiers = []
    for (pandas_label, snowflake_quoted_identifier) in zip(
        frame.data_column_pandas_labels,
        frame.data_column_snowflake_quoted_identifiers,
    ):
        is_id_col = pandas_label in pandas_id_columns
        is_var_col = pandas_label in pandas_value_columns
        if is_var_col:
            # create the ordering clause for this variable column
            # to maintain a consistent ordering with pandas. This
            # is used in the case statement below.
            ordering_decode_conditions.append(
                col(var_quoted) == pandas_lit(pandas_label)
            )
            unpivot_quoted_columns.append(snowflake_quoted_identifier)
        if is_id_col:
            id_col_names.append(pandas_label)
            id_col_quoted_identifiers.append(snowflake_quoted_identifier)

    # create the case expressions used for the final result set ordering based
    # on the column position. This clause will be appled after the unpivot
    order_by_case_lit = 0
    ordering_column_case_expr: CaseExpr = when(
        ordering_decode_conditions[0], pandas_lit(order_by_case_lit)
    )
    for case_condition in ordering_decode_conditions[1:]:
        order_by_case_lit = order_by_case_lit + 1
        ordering_column_case_expr = ordering_column_case_expr.when(
            case_condition, pandas_lit(order_by_case_lit)
        )
    ordering_column_case_expr = ordering_column_case_expr.otherwise(
        pandas_lit(0)
    ).alias(ordering_column_name)

    # Normalize the input columns to the same type, if necessary
    unpivot_columns_normalized_types = []
    renamed_quoted_unpivot_cols = []

    suffix_to_unpivot_map: dict[str, str] = {}
    cast_suffix = generate_column_identifier_random()
    null_replace_value = UNPIVOT_NULL_REPLACE_VALUE + "_" + cast_suffix

    for c in unpivot_quoted_columns:
        # Rename the columns to unpivot
        unquoted_col_name = c.strip('"') + "_" + cast_suffix
        renamed_quoted_unpivot_col = (
            ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=[unquoted_col_name],
            )[0]
        )
        # coalese the values to unpivot and preserve null values This code
        # can be removed when UNPIVOT_INCLUDE_NULLS is enabled
        unpivot_columns_normalized_types.append(
            coalesce(to_variant(c), to_variant(pandas_lit(null_replace_value))).alias(
                renamed_quoted_unpivot_col
            )
        )
        renamed_quoted_unpivot_cols.append(renamed_quoted_unpivot_col)
        # create the column name mapper which is passed to unpivot
        suffix_to_unpivot_map[renamed_quoted_unpivot_col] = c

    # select a subset of casted columns
    normalized_projection = unpivot_columns_normalized_types + id_col_quoted_identifiers
    ordered_dataframe = ordered_dataframe.select(normalized_projection)

    ##################################################
    # OrderedDataFrame at this point, prior to unpivot
    ##################################################
    #               abc_tavu             123_tavu
    # 0                  "A"                  "1"
    # 1  "NULL_REPLACE_tavu"                  "2"
    # 2                  "C"  "NULL_REPLACE_tavu"

    # Perform the unpivot
    ordered_dataframe = ordered_dataframe.unpivot(
        value_column=value_quoted,
        name_column=var_quoted,
        column_list=renamed_quoted_unpivot_cols,
        col_mapper=suffix_to_unpivot_map,
    )

    ##############################################
    # OrderedDataFrame at this point after unpivot
    ##############################################
    #      variable                value
    # 0      123                  "1"
    # 1      123                  "2"
    # 2      123  "NULL_REPLACE_tavu"
    # 3      abc                  "A"
    # 4      abc                  "C"
    # 5      abc  "NULL_REPLACE_tavu"

    corrected_value_column_name = (
        ordered_dataframe.generate_snowflake_quoted_identifiers(
            pandas_labels=["corrected_value_" + generate_column_identifier_random()],
        )[0]
    )
    corrected_null_replace_case_expr: CaseExpr = when(
        (col(value_quoted) == pandas_lit(null_replace_value)), pandas_lit(None)
    )

    # add otherwise clause to complete the normalization of values
    corrected_null_replace_column = corrected_null_replace_case_expr.otherwise(
        col(value_quoted)
    ).alias(corrected_value_column_name)

    # Reorder the resulting expression to match pandas based on the original column order,
    # which is now in the "variable" column
    unpivoted_columns = (
        ordered_dataframe._get_active_column_snowflake_quoted_identifiers()
    )
    ordered_dataframe = ordered_dataframe.select(
        *unpivoted_columns, ordering_column_case_expr, corrected_null_replace_column
    ).sort(OrderingColumn(ordering_column_name))
    ordered_dataframe = ordered_dataframe.ensure_row_position_column()

    ###########################################################################################
    # OrderedDataFrame at this point, prior to creation of the new InternalFrame
    ###########################################################################################
    #   variable                value  UNPIVOT_ORDERING_ corrected_value_8ofo  __row_position__
    # 0      abc                  "A"                  0                  "A"                 0
    # 1      abc  "NULL_REPLACE_tavu"                  0                 None                 1
    # 2      abc                  "C"                  0                  "C"                 2
    # 3      123                  "1"                  1                  "1"                 3
    # 4      123                  "2"                  1                  "2"                 4
    # 5      123  "NULL_REPLACE_tavu"                  1                 None                 5

    final_pandas_labels = id_col_names + [pandas_var_name, pandas_value_name]

    final_snowflake_quoted_cols = id_col_quoted_identifiers + [
        var_quoted,
        corrected_value_column_name,
    ]

    # Create the new frame and compiler
    return InternalFrame.create(
        ordered_dataframe=ordered_dataframe,
        data_column_pandas_labels=final_pandas_labels,
        data_column_pandas_index_names=[None],
        data_column_snowflake_quoted_identifiers=final_snowflake_quoted_cols,
        index_column_pandas_labels=[None],
        index_column_snowflake_quoted_identifiers=[
            ordered_dataframe.row_position_snowflake_quoted_identifier
        ],
    )


def unpivot_empty_df() -> "SnowflakeQueryCompiler":  # type: ignore[name-defined] # noqa: F821
    """
    Special casing when the data frame is empty entirely. Similar to
    transpose_empty_df.
    """
    import pandas as native_pd

    from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
        SnowflakeQueryCompiler,
    )

    return SnowflakeQueryCompiler.from_pandas(
        native_pd.DataFrame(
            {
                DEFAULT_PANDAS_UNPIVOT_VARIABLE_NAME: [],
                DEFAULT_PANDAS_UNPIVOT_VALUE_NAME: [],
            }
        )
    )
