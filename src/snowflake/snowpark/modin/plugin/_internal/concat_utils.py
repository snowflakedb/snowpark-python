#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from collections.abc import Hashable, Sequence
from typing import Literal, Optional, Union

import pandas as native_pd

from snowflake.snowpark._internal.type_utils import ColumnOrName
from snowflake.snowpark.functions import array_construct
from snowflake.snowpark.modin.plugin._internal import join_utils
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin._internal.ordered_dataframe import OrderingColumn
from snowflake.snowpark.modin.plugin._internal.utils import (
    INDEX_LABEL,
    append_columns,
    pandas_lit,
)
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage

CONCAT_POSITION_COLUMN_LABEL = "concat_position"


def add_keys_as_column_levels(
    columns: native_pd.Index,
    frames: list[InternalFrame],
    keys: Sequence[Hashable],
    names: Union[list[Hashable], None],
) -> native_pd.Index:
    """
    Concat all column names from given ``frames``. Also add ``keys`` as outermost
    level of column labels.
    Args:
        columns: Column index of concatenated frame.
        frames: A list of internal frames.
        keys: A list of hashable to be used as keys. Length of keys must be same as
          length of frames.
        names: Optional names for levels in column index.

    Returns:
        Concatenated column names as native pandas index.
    """
    assert len(keys) == len(frames), "Length of keys must be same as length of frames"

    key_values = []
    for key, frame in zip(keys, frames):
        key_values.extend([key] * len(frame.data_column_pandas_labels))
    keys_index = native_pd.Index(key_values)
    # Add 'keys' as outermost level to column labels.
    arrays = [keys_index.get_level_values(i) for i in range(keys_index.nlevels)] + [
        columns.get_level_values(i) for i in range(columns.nlevels)
    ]
    columns = native_pd.MultiIndex.from_arrays(arrays)
    names = names or []
    # Fill with 'None' to match the number of levels in column index
    while len(names) < columns.nlevels:
        names.append(None)
    return columns.set_names(names)


def convert_to_single_level_index(frame: InternalFrame, axis: int) -> InternalFrame:
    """
    If index on given axis is a MultiIndex, convert it to single level index of tuples.
    Do nothing if index on given axis has only one level.

    On axis=1, this is equivalent to following operation in pandas.
    df.columns = df.columns.to_flat_index()
    For example a frame if columns index
    pd.MultiIndex.from_tuples([('a', 'b'), ('c', 'd')], names=['x', 'y'])
    will be converted to a frame with column index
    pd.Index([('a', 'b'), ('c', 'd')])

    Similarly on axis=0 this is equivalent to following operations in pandas
    df.index = df.index.to_flat_index()

    NOTE: Original level names are lost during this operation becomes None.

    Args:
        frame: A InternalFrame.
        axis: int: {0, 1}

    Returns:
        New InternalFrame with single level index.

    """
    assert axis in (0, 1), f"Invalid axis {axis}, allowed values are 0 and 1"
    # Because we break up and store a MultiIndex with several Snowpark columns, we can
    # perform the single-level index conversion as a no-op.
    if frame.num_index_levels(axis=axis) == 1:
        return frame
    if axis == 1:
        return InternalFrame.create(
            ordered_dataframe=frame.ordered_dataframe,
            data_column_pandas_labels=frame.data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers=frame.data_column_snowflake_quoted_identifiers,
            # Setting length of index names to 1 will convert column labels from
            # multi-index to single level index.
            data_column_pandas_index_names=[None],
            index_column_pandas_labels=frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=frame.index_column_snowflake_quoted_identifiers,
            data_column_types=frame.cached_data_column_snowpark_pandas_types,
            index_column_types=frame.cached_index_column_snowpark_pandas_types,
        )
    else:
        WarningMessage.tuples_stored_as_array(
            "MultiIndex values are compressed to single index of tuples.Snowflake"
            " backend doesn't support tuples datatype. Tuple row labels are stored as"
            "ARRAY"
        )
        index_identifier = (
            frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=[INDEX_LABEL],
            )[0]
        )
        ordered_dataframe = append_columns(
            frame.ordered_dataframe,
            index_identifier,
            array_construct(*frame.index_column_snowflake_quoted_identifiers),
        )
        return InternalFrame.create(
            ordered_dataframe=ordered_dataframe,
            index_column_pandas_labels=[None],
            index_column_snowflake_quoted_identifiers=[index_identifier],
            data_column_pandas_labels=frame.data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers=frame.data_column_snowflake_quoted_identifiers,
            data_column_pandas_index_names=frame.data_column_pandas_index_names,
            data_column_types=None,
            index_column_types=None,
        )


def _dedupe_identifiers_without_reorder(t: tuple) -> tuple:
    return tuple(item for index, item in enumerate(t) if item not in t[:index])


def union_all(
    frame1: InternalFrame,
    frame2: InternalFrame,
    join: Literal["inner", "outer"],
    sort: Optional[bool] = False,
) -> InternalFrame:
    """
    Concatenate frames on index axis by taking using UNION operator.
    Snowflake identifiers of output frame are based on snowflake identifiers from first
    frame.
    Args:
        frame1: First frame
        frame2: Second frame
        join: How to handle column index
            'inner': Output frame contains only overlapping columns from both frames.
            'outer': Output frame contains union of columns from both frames.
        sort: Sort column axis if True.

    Returns:
        New InternalFrame after taking union of given frames.
    """
    columns1 = frame1.data_columns_index
    columns2 = frame2.data_columns_index

    if join == "inner":
        # Preserves the order from calling index.
        # For example:
        # pd.Index([3, 1, 2]).intersection(pd.Index([1, 2, 3]) will result in
        # pd.Index([3, 1, 2])
        data_column_labels = columns1.intersection(columns2, sort=False)
    elif join == "outer":
        # Preserves the order from calling index. And for labels not in calling index
        # preserves the order from argument index.
        # For example:
        # pd.Index([3, 1, 2]).union(pd.Index([1, 4, 2, 3, 5]) will result in
        # pd.Index([3, 1, 2, 4, 5])
        data_column_labels = columns1.union(columns2, sort=False)
    else:
        raise AssertionError(
            f"Invalid join type '{join}'. Accepted values are 'inner' and 'outer'"
        )
    if sort:
        data_column_labels = data_column_labels.sort_values()

    frame1 = _select_columns(frame1, data_column_labels.tolist())
    frame2 = _select_columns(frame2, data_column_labels.tolist())

    frame1, frame2 = join_utils.convert_incompatible_types_to_variant(
        frame1,
        frame2,
        frame1.ordered_dataframe.projected_column_snowflake_quoted_identifiers,
        frame2.ordered_dataframe.projected_column_snowflake_quoted_identifiers,
    )

    # select data + index + ordering columns for union all
    # TODO SNOW-956072: remove the following code after removing convert_incompatible_types_to_variant
    frame1_identifiers_for_union_all = (
        frame1.index_column_snowflake_quoted_identifiers
        + frame1.data_column_snowflake_quoted_identifiers
        + frame1.ordering_column_snowflake_quoted_identifiers
    )
    frame2_identifiers_for_union_all = (
        frame2.index_column_snowflake_quoted_identifiers
        + frame2.data_column_snowflake_quoted_identifiers
        + frame2.ordering_column_snowflake_quoted_identifiers
    )

    # ensure there is no overlap in column identifiers before the union, this occurs when there is an
    # existing row_position column used for ordering and the index
    frame1_identifiers_for_union_all = _dedupe_identifiers_without_reorder(
        frame1_identifiers_for_union_all
    )
    frame2_identifiers_for_union_all = _dedupe_identifiers_without_reorder(
        frame2_identifiers_for_union_all
    )

    # In Snowflake UNION ALL operator, the names of the output columns are based on the
    # names of the columns of the first query. So here we copy identifiers from
    # first frame.
    # Reference: https://docs.snowflake.com/en/sql-reference/operators-query
    ordered_dataframe1 = frame1.ordered_dataframe.select(
        frame1_identifiers_for_union_all
    )
    ordered_dataframe2 = frame2.ordered_dataframe.select(
        frame2_identifiers_for_union_all
    )
    ordered_unioned_dataframe = ordered_dataframe1.union_all(ordered_dataframe2)
    ordered_dataframe = ordered_unioned_dataframe.sort(frame1.ordering_columns)
    return InternalFrame.create(
        ordered_dataframe=ordered_dataframe,
        data_column_pandas_labels=frame1.data_column_pandas_labels,
        data_column_snowflake_quoted_identifiers=frame1.data_column_snowflake_quoted_identifiers,
        data_column_pandas_index_names=frame1.data_column_pandas_index_names,
        index_column_pandas_labels=frame1.index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=frame1.index_column_snowflake_quoted_identifiers,
        data_column_types=None,
        index_column_types=None,
    )


def add_key_as_index_columns(frame: InternalFrame, key: Hashable) -> InternalFrame:
    """
    Add given 'key' as outermost index columns to given 'frame'.
    If 'key' is a tuple multiple columns are added for each element in tuple.

    Args:
        frame: InternalFrame
        key: key to add as index column

    Returns:
        A InternalFrame after adding 'key' as index columns.
    """
    if not isinstance(key, tuple):
        key = tuple([key])
    new_identifiers = frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
        pandas_labels=[INDEX_LABEL] * len(key),
    )
    col_values = [pandas_lit(value) for value in key]
    ordered_dataframe = append_columns(
        frame.ordered_dataframe, new_identifiers, col_values
    )

    # Add key as outermost index columns.
    index_column_pandas_labels = [None] * len(key) + frame.index_column_pandas_labels
    index_column_snowflake_quoted_identifiers = (
        new_identifiers + frame.index_column_snowflake_quoted_identifiers
    )

    return InternalFrame.create(
        ordered_dataframe=ordered_dataframe,
        data_column_pandas_labels=frame.data_column_pandas_labels,
        data_column_snowflake_quoted_identifiers=frame.data_column_snowflake_quoted_identifiers,
        data_column_pandas_index_names=frame.data_column_pandas_index_names,
        index_column_pandas_labels=index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=index_column_snowflake_quoted_identifiers,
        data_column_types=None,
        index_column_types=None,
    )


def _select_columns(
    frame: InternalFrame, data_column_labels: list[Hashable]
) -> InternalFrame:
    """
    Select only the given labels from given frame. If any data column label is missing
    in frame add new column with NULL values.

    Args:
        frame: An InternalFrame
        data_column_labels: A list of pandas labels.

    Returns:
        New InternalFrame after only with given data columns.

    """
    select_list: list[ColumnOrName] = []

    # Add index columns
    select_list.extend(frame.index_column_snowflake_quoted_identifiers)

    # Add ordering columns
    select_list.extend(frame.ordering_column_snowflake_quoted_identifiers)

    snowflake_ids = frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
        data_column_labels, include_index=False
    )
    # Add data columns
    data_column_snowflake_identifiers = []
    # A map to keep track number of times a label is already seen.
    # Native pandas fails with IndexError when either frame has duplicate labels, with
    # the exception when both frames have exact same lables and exact same order.
    # In Snowpark pandas, we don't fail concat when duplicates lables are present but
    # try to match as many columns as possible from the frames.
    label_count_map: dict[Hashable, int] = {}
    for label, id_tuple in zip(data_column_labels, snowflake_ids):
        if len(id_tuple) <= label_count_map.get(label, 0):
            # if missing add new column to frame with NULL values.
            snowflake_id = (
                frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                    pandas_labels=[label]
                )[0]
            )
            select_list.append(pandas_lit(None).as_(snowflake_id))
        else:
            index = label_count_map.get(label, 0)
            snowflake_id = id_tuple[index]
            select_list.append(snowflake_id)
            label_count_map[label] = index + 1

        data_column_snowflake_identifiers.append(snowflake_id)
    return InternalFrame.create(
        ordered_dataframe=frame.ordered_dataframe.select(select_list),
        data_column_pandas_labels=data_column_labels,
        data_column_snowflake_quoted_identifiers=data_column_snowflake_identifiers,
        data_column_pandas_index_names=frame.data_column_pandas_index_names,
        index_column_pandas_labels=frame.index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=frame.index_column_snowflake_quoted_identifiers,
        data_column_types=None,
        index_column_types=None,
    )


def add_global_ordering_columns(frame: InternalFrame, position: int) -> InternalFrame:
    """
    To create global ordering for concat (axis=0) operation we first ensure a
    row position column for local ordering within the frame. Then add another
    column to indicate position of this frame among concat frames given by 'position'
    parameter.
    Now these two columns can be used to determine global ordering.
    Args:
        frame: Internal frame.
        position: position of this frame among all frames being concatenated.

    Returns:
        A new frame with updated ordering columns.

    """
    frame = frame.ensure_row_position_column()
    ordered_dataframe = frame.ordered_dataframe.sort(
        [OrderingColumn(frame.row_position_snowflake_quoted_identifier)]
    )
    identifier = ordered_dataframe.generate_snowflake_quoted_identifiers(
        pandas_labels=[CONCAT_POSITION_COLUMN_LABEL],
    )[0]
    ordered_dataframe = append_columns(
        ordered_dataframe, identifier, pandas_lit(position)
    )
    ordered_dataframe = ordered_dataframe.sort(
        OrderingColumn(identifier), *ordered_dataframe.ordering_columns
    )
    return InternalFrame.create(
        ordered_dataframe=ordered_dataframe,
        data_column_pandas_labels=frame.data_column_pandas_labels,
        data_column_snowflake_quoted_identifiers=frame.data_column_snowflake_quoted_identifiers,
        data_column_pandas_index_names=frame.data_column_pandas_index_names,
        index_column_pandas_labels=frame.index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=frame.index_column_snowflake_quoted_identifiers,
        data_column_types=frame.cached_data_column_snowpark_pandas_types,
        index_column_types=frame.cached_index_column_snowpark_pandas_types,
    )
