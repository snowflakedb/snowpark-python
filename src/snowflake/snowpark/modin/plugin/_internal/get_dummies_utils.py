#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from collections.abc import Hashable

from snowflake.snowpark.functions import (
    col,
    min as min_,
)

from snowflake.snowpark.modin.plugin._internal.ordered_dataframe import OrderingColumn
from snowflake.snowpark.modin.plugin._internal.utils import (
    pandas_lit,
    extract_pandas_label_from_snowflake_quoted_identifier,
)
from snowflake.snowpark.modin.plugin._internal import (
    join_utils,
)

from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin.utils.error_message import ErrorMessage


ROW_POSITION_INDEX_COLUMN_PANDAS_LABEL = "row_position_index"
DUMMY_COLUMN_PANDAS_LABEL = "lit_one"


def single_get_dummies_pivot(
    internal_frame: InternalFrame,
    column: Hashable,
    prefix: Hashable,
    prefix_sep: str,
    columns_to_keep_snowflake_quoted_identifiers: list[str],
    columns_to_keep_pandas_labels: list[Hashable],
) -> InternalFrame:
    """
    Helper function for get dummies to perform a single pivot on the encoded column.
    Args:
        internal_frame: The original internal frame to perform pivot on.
            Note: the input internal frame must have a row position column and dummy lit(1) column
                as the last data column
        column: The encoded column, which is the column to pivot on
        prefix: String to append to newly generated column names after pivot
        prefix_sep: The separator used between the prefix and new column names
        columns_to_keep_snowflake_quoted_identifiers: The snowflake quoted identifier in the
            internal_frame to keep as the data column of final result internal frame.
        columns_to_keep_pandas_labels: The pandas label in the internal_frame to keep as the
            data_column of final result internal frame.

        Note: columns_to_keep_snowflake_quoted_identifiers must be the same length as columns_to_keep_pandas_labels
    Returns:
        InternalFrame: An InternalFrame whose data columns are the pivoted result columns + the columns_to_keep,
            and the row position column of the original internal_frame as index column.

    Example:
        With the following DataFrame, where lit_one is the dummy lit one column:

             A  C lit_one
          0  a  1   1
          1  b  2   1
          2  a  3   1

        the result of calling single_get_dummies_pivot with ['"C"]' as column_to_keep,
        "A" as prefix, and "_" as prefix_sep will be the following:

            C  A_a  A_b
         0  1    1    0
         1  2    0    1
         2  3    1    0
    """

    # find the quoted identifier for pivot column
    grouped_quoted_identifiers = (
        internal_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
            [column], include_index=False
        )
    )
    if (len(grouped_quoted_identifiers) == 0) or (
        len(grouped_quoted_identifiers[0]) == 0
    ):
        raise KeyError(f"Column {column} does not exist")
    if len(grouped_quoted_identifiers[0]) > 1:
        ErrorMessage.not_implemented(f"get_dummies with duplicated columns {column}")
    pivot_column_snowflake_quoted_identifier = grouped_quoted_identifiers[0][0]

    # get the row position column and dummy lit one column
    assert internal_frame.row_position_snowflake_quoted_identifier is not None
    row_position_snowflake_quoted_identifier = (
        internal_frame.row_position_snowflake_quoted_identifier
    )
    dummy_column_snowflake_quoted_identifier = (
        internal_frame.data_column_snowflake_quoted_identifiers[-1]
    )
    # for the dataframe passed for pivot, we keep the row position column, dummy lit one column,
    # pivot column, and the specified columns_to_keep
    columns_snowflake_quoted_identifier = [
        row_position_snowflake_quoted_identifier,
        pivot_column_snowflake_quoted_identifier,
        dummy_column_snowflake_quoted_identifier,
    ] + columns_to_keep_snowflake_quoted_identifiers
    ordered_dataframe = internal_frame.ordered_dataframe.select(
        columns_snowflake_quoted_identifier
    )
    # Perform pivot on the pivot column with dummy lit one column as value column.
    # With the above example, the result of pivot will be:
    #
    #    C    a    b
    # 0  1    1    0
    # 1  2    0    1
    # 2  3    1    0
    pivoted_ordered_dataframe = ordered_dataframe.pivot(
        col(str(pivot_column_snowflake_quoted_identifier)),
        None,
        0,
        min_(dummy_column_snowflake_quoted_identifier),
    )
    pivoted_ordered_dataframe = pivoted_ordered_dataframe.sort(
        OrderingColumn(row_position_snowflake_quoted_identifier)
    )

    # Next: We need to find out the snowflake quoted identifiers for
    # the new columns - i.e. the columns that came from the values of
    # the column we were pivoting on.
    origin_column_snowflake_quoted_identifiers = [
        row_position_snowflake_quoted_identifier
    ] + columns_to_keep_snowflake_quoted_identifiers
    pivot_result_column_snowflake_quoted_identifiers = [
        quoted_identifier
        for quoted_identifier in pivoted_ordered_dataframe.projected_column_snowflake_quoted_identifiers
        if (quoted_identifier not in origin_column_snowflake_quoted_identifiers)
    ]

    # Next handle the prefix for the pivot result column
    # We then need to get the new result columns.
    # new_result_columns = ["A_a", "A_b"]
    if prefix is None:
        prefix = ""
        prefix_sep = ""

    pivot_result_column_pandas_labels = []
    for quoted_identifier in pivot_result_column_snowflake_quoted_identifiers:
        pandas_col_label = extract_pandas_label_from_snowflake_quoted_identifier(
            quoted_identifier
        )
        if (
            isinstance(pandas_col_label, str)
            and pandas_col_label.startswith("'")
            and pandas_col_label.endswith("'")
        ):
            pandas_col_label = pandas_col_label[1:-1]
        new_pandas_col_label = f"{prefix}{prefix_sep}{pandas_col_label}"
        pivot_result_column_pandas_labels.append(new_pandas_col_label)

    result_internal_frame = InternalFrame.create(
        ordered_dataframe=pivoted_ordered_dataframe,
        data_column_pandas_labels=columns_to_keep_pandas_labels.copy()
        + pivot_result_column_pandas_labels,
        data_column_pandas_index_names=internal_frame.data_column_pandas_index_names,
        data_column_snowflake_quoted_identifiers=columns_to_keep_snowflake_quoted_identifiers.copy()
        + pivot_result_column_snowflake_quoted_identifiers,
        # set the row position column as index column for later join
        index_column_pandas_labels=[ROW_POSITION_INDEX_COLUMN_PANDAS_LABEL],
        index_column_snowflake_quoted_identifiers=[
            row_position_snowflake_quoted_identifier
        ],
        data_column_types=None,
        index_column_types=None,
    )

    return result_internal_frame


def get_dummies_helper(
    internal_frame: InternalFrame,
    columns: list[Hashable],
    prefixes: list[Hashable],
    prefix_sep: str,
) -> InternalFrame:
    """
    Helper function for get dummies to perform encoding on given columns
    """
    if len(columns) == 0:
        return internal_frame

    # append a lit one column as value column for pivot
    new_internal_frame = internal_frame.ensure_row_position_column().append_column(
        DUMMY_COLUMN_PANDAS_LABEL, pandas_lit(1)
    )
    # the dummy column is appended as the last data column of the new_internal_frame
    row_position_column_snowflake_quoted_identifier = (
        new_internal_frame.row_position_snowflake_quoted_identifier
    )

    remaining_data_column_pandas_labels = []
    remaining_data_column_snowflake_quoted_identifiers = []
    for (pandas_label, snowflake_quoted_identifiers) in zip(
        internal_frame.index_column_pandas_labels,
        internal_frame.index_column_snowflake_quoted_identifiers,
    ):
        if (
            snowflake_quoted_identifiers
            != row_position_column_snowflake_quoted_identifier
        ):
            remaining_data_column_pandas_labels.append(pandas_label)
            remaining_data_column_snowflake_quoted_identifiers.append(
                snowflake_quoted_identifiers
            )

    for (pandas_label, snowflake_quoted_identifiers) in zip(
        internal_frame.data_column_pandas_labels,
        internal_frame.data_column_snowflake_quoted_identifiers,
    ):
        if (
            (pandas_label not in columns)
            and snowflake_quoted_identifiers
            != row_position_column_snowflake_quoted_identifier
        ):
            remaining_data_column_pandas_labels.append(pandas_label)
            remaining_data_column_snowflake_quoted_identifiers.append(
                snowflake_quoted_identifiers
            )

    # do the first pivot by keeping all re
    result_internal_frame = single_get_dummies_pivot(
        internal_frame=new_internal_frame,
        column=columns[0],
        prefix=prefixes[0],
        prefix_sep=prefix_sep,
        columns_to_keep_snowflake_quoted_identifiers=remaining_data_column_snowflake_quoted_identifiers,
        columns_to_keep_pandas_labels=remaining_data_column_pandas_labels,
    )

    for (pandas_column, column_prefix) in zip(columns[1:], prefixes[1:]):
        pivoted_internal_frame = single_get_dummies_pivot(
            internal_frame=new_internal_frame,
            column=pandas_column,
            prefix=column_prefix,
            prefix_sep=prefix_sep,
            columns_to_keep_snowflake_quoted_identifiers=[],
            columns_to_keep_pandas_labels=[],
        )
        result_internal_frame = join_utils.join(
            result_internal_frame,
            pivoted_internal_frame,
            left_on=result_internal_frame.index_column_snowflake_quoted_identifiers,
            right_on=pivoted_internal_frame.index_column_snowflake_quoted_identifiers,
            how="inner",
        ).result_frame

    # reset the original index back
    data_column_pandas_label = []
    data_column_snowflake_quoted_identifiers = []
    for (pandas_label, snowflake_quoted_identifiers) in zip(
        result_internal_frame.data_column_pandas_labels,
        result_internal_frame.data_column_snowflake_quoted_identifiers,
    ):
        if (
            snowflake_quoted_identifiers
            not in internal_frame.index_column_snowflake_quoted_identifiers
        ):
            data_column_pandas_label.append(pandas_label)
            data_column_snowflake_quoted_identifiers.append(
                snowflake_quoted_identifiers
            )

    result_internal_frame = InternalFrame.create(
        ordered_dataframe=result_internal_frame.ordered_dataframe,
        data_column_pandas_labels=data_column_pandas_label,
        data_column_pandas_index_names=internal_frame.data_column_pandas_index_names,
        data_column_snowflake_quoted_identifiers=data_column_snowflake_quoted_identifiers,
        index_column_pandas_labels=internal_frame.index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=internal_frame.index_column_snowflake_quoted_identifiers,
        data_column_types=None,
        index_column_types=None,
    )

    # do an optimization
    return result_internal_frame
