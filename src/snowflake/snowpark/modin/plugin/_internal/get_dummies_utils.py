#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from collections.abc import Hashable

from snowflake.snowpark.functions import (
    col,
    min as min_,
)

from snowflake.snowpark.modin.plugin._internal.ordered_dataframe import (
    OrderedDataFrame,
    OrderingColumn,
)
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
LIT_TRUE_COLUMN_PANDAS_LABEL = "lit_true"
NULL_COLUMN_ID = '"NULL"'


def single_get_dummies_pivot(
    internal_frame: InternalFrame,
    prefix: Hashable,
    prefix_sep: str,
    pivot_column_snowflake_quoted_identifier: str,
    columns_to_keep_snowflake_quoted_identifiers: list[str],
    columns_to_keep_pandas_labels: list[Hashable],
) -> InternalFrame:
    """
    Helper function for get dummies to perform a single pivot on the encoded column.
    Args:
        internal_frame: The original internal frame to perform pivot on.
            Note: the input internal frame must have a row position column and dummy lit(1) column
                as the last data column
        prefix: String to append to newly generated column names after pivot
        prefix_sep: The separator used between the prefix and new column names
        pivot_column_snowflake_quoted_identifier: The encoded column, which is the column to pivot on
        columns_to_keep_snowflake_quoted_identifiers: The snowflake quoted identifier in the
            internal_frame to keep as the data column of final result internal frame.
        columns_to_keep_pandas_labels: The pandas label in the internal_frame to keep as the
            data_column of final result internal frame.

        Note: columns_to_keep_snowflake_quoted_identifiers must be the same length as columns_to_keep_pandas_labels
    Returns:
        InternalFrame: An InternalFrame whose data columns are the pivoted result columns + the columns_to_keep,
            and the row position column of the original internal_frame as index column.

    Example:
        With the following DataFrame, where lit_true is the dummy lit true column:

             A  C lit_true
          0  a  1   True
          1  b  2   True
          2  a  3   True

        the result of calling single_get_dummies_pivot with ['"C"]' as column_to_keep,
        "A" as prefix, and "_" as prefix_sep will be the following:

            C   A_a     A_b
         0  1   True   False
         1  2   False  True
         2  3   True   False
    """

    # get the row position column and dummy lit true column
    assert internal_frame.row_position_snowflake_quoted_identifier is not None
    row_position_snowflake_quoted_identifier = (
        internal_frame.row_position_snowflake_quoted_identifier
    )
    lit_true_column_snowflake_quoted_identifier = (
        internal_frame.data_column_snowflake_quoted_identifiers[-1]
    )
    # for the dataframe passed for pivot, we keep the row position column, dummy lit true column,
    # pivot column, and the specified columns_to_keep
    columns_snowflake_quoted_identifier = [
        row_position_snowflake_quoted_identifier,
        pivot_column_snowflake_quoted_identifier,
        lit_true_column_snowflake_quoted_identifier,
    ] + columns_to_keep_snowflake_quoted_identifiers
    ordered_dataframe = internal_frame.ordered_dataframe.select(
        columns_snowflake_quoted_identifier
    )
    # Perform pivot on the pivot column with dummy lit true column as value column.
    # With the above example, the result of pivot will be:
    #
    #    C    a       b
    # 0  1    True    False
    # 1  2   False     True
    # 2  3    True    False
    pivoted_ordered_dataframe = ordered_dataframe.pivot(
        col(str(pivot_column_snowflake_quoted_identifier)),
        None,
        0,
        min_(lit_true_column_snowflake_quoted_identifier),
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
    # Remove the NULL column if it exists.
    if NULL_COLUMN_ID in pivot_result_column_snowflake_quoted_identifiers:
        pivot_result_column_snowflake_quoted_identifiers.remove(NULL_COLUMN_ID)

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

    # Rename the pivot result column to avoid duplicated column names. Snowpark
    # dataframe can have duplicate columns names if multiple columns are pivoted,
    # and they have at least one common value (including null).
    pivot_result_column_new_snowflake_quoted_identifiers = (
        pivoted_ordered_dataframe.generate_snowflake_quoted_identifiers(
            pandas_labels=pivot_result_column_pandas_labels
        )
    )
    return result_internal_frame.rename_snowflake_identifiers(
        dict(
            zip(
                pivot_result_column_snowflake_quoted_identifiers,
                pivot_result_column_new_snowflake_quoted_identifiers,
            )
        )
    )


def get_dummies_helper(
    internal_frame: InternalFrame,
    columns: list[Hashable],
    prefixes: list[Hashable],
    prefix_sep: str,
) -> InternalFrame:
    """
    Helper function for get dummies to perform encoding on given columns

    Example:
        With the following DataFrame:

           A  B  C
        0  a  a  1
        1  b  a  2
        2  a  a  3

        the result of calling get_dummies_helper with columns = ["A", "B"],
        ["A", "B"] as prefix, and "_" as prefix_sep will be the following:

            C  A_a  A_b  B_a
         0  1    1    0    1
         1  2    0    1    1
         2  3    1    0    1
    """
    if len(columns) == 0:
        return internal_frame

    grouped_quoted_identifiers = (
        internal_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
            columns, include_index=False
        )
    )

    for (pandas_label, quoted_identifiers) in zip(columns, grouped_quoted_identifiers):
        if len(quoted_identifiers) == 0:
            raise KeyError(f"Column {pandas_label} does not exist")
        if len(quoted_identifiers) > 1:
            ErrorMessage.not_implemented(
                f"get_dummies with duplicated columns {pandas_label}"
            )

    # append a lit true column as value column for pivot
    new_internal_frame = internal_frame.ensure_row_position_column().append_column(
        LIT_TRUE_COLUMN_PANDAS_LABEL, pandas_lit(True)
    )
    # the dummy column is appended as the last data column of the new_internal_frame
    row_position_column_snowflake_quoted_identifier = (
        new_internal_frame.row_position_snowflake_quoted_identifier
    )

    # Find all columns that are not encode columns that will be kept in the result dataframe.
    # The row position column is excluded in the list, we will always keep the row
    # position column in the final result and handled independently.
    remaining_data_column_pandas_labels = []
    remaining_data_column_snowflake_quoted_identifiers = []
    # check the index columns
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
    # check the data columns
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

    # Do the first pivot with the first column and keep all remaining columns.
    # With the example given above, the first pivot is performed on column A, and we will
    # get the following result:
    #    C  A_a  A_b
    # 0  1    1    0
    # 1  2    0    1
    # 2  3    1    0
    result_internal_frame = single_get_dummies_pivot(
        internal_frame=new_internal_frame,
        prefix=prefixes[0],
        prefix_sep=prefix_sep,
        pivot_column_snowflake_quoted_identifier=grouped_quoted_identifiers[0][0],
        columns_to_keep_snowflake_quoted_identifiers=remaining_data_column_snowflake_quoted_identifiers,
        columns_to_keep_pandas_labels=remaining_data_column_pandas_labels,
    )

    # Perform pivot on rest columns and join on the row position column to form the final result.
    for i in range(1, len(columns)):
        # With the example given above, the pivot result with second column will be
        #    B_a
        # 0    1
        # 1    1
        # 2    1
        pivoted_internal_frame = single_get_dummies_pivot(
            internal_frame=new_internal_frame,
            prefix=prefixes[i],
            prefix_sep=prefix_sep,
            pivot_column_snowflake_quoted_identifier=grouped_quoted_identifiers[i][0],
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

    # optimization: keep the original row position column as the result ordered frame
    # row position to avoid unnecessary row position column in later operation, which
    # is an expensive operation.
    result_ordered_frame = result_internal_frame.ordered_dataframe
    result_ordered_frame = OrderedDataFrame(
        dataframe_ref=result_ordered_frame._dataframe_ref,
        projected_column_snowflake_quoted_identifiers=result_ordered_frame.projected_column_snowflake_quoted_identifiers,
        ordering_columns=result_ordered_frame.ordering_columns,
        row_position_snowflake_quoted_identifier=row_position_column_snowflake_quoted_identifier,
        row_count_snowflake_quoted_identifier=result_ordered_frame.row_count_snowflake_quoted_identifier,
    )

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
        ordered_dataframe=result_ordered_frame,
        data_column_pandas_labels=data_column_pandas_label,
        data_column_pandas_index_names=internal_frame.data_column_pandas_index_names,
        data_column_snowflake_quoted_identifiers=data_column_snowflake_quoted_identifiers,
        # keep the original index columns
        index_column_pandas_labels=internal_frame.index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=internal_frame.index_column_snowflake_quoted_identifiers,
        data_column_types=None,
        index_column_types=None,
    )

    return result_internal_frame
