#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from collections.abc import Hashable
from typing import Literal, Optional

from snowflake.snowpark._internal.type_utils import ColumnOrName
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin._internal.join_utils import align_on_index
from snowflake.snowpark.modin.plugin._internal.utils import pandas_lit


def align_axis_0_left(
    frame: InternalFrame, other_frame: InternalFrame, join: str
) -> tuple[InternalFrame, InternalFrame, list[str], list[str]]:
    """
    Gets the left align results.

    Args:
        frame: original frame
        other_frame: other frame
        join: type of alignment to be performed.

    Returns:
        Tuple containing:
            InternalFrame result of join_utils.align_on_index,
            final left_frame,
            list of left_frame_data_ids,
            list of left_index_ids
    """
    if join == "right":
        left_result, left_column_mapper = align_on_index(other_frame, frame, how="left")
        left_frame_data_ids = left_column_mapper.map_right_quoted_identifiers(
            frame.data_column_snowflake_quoted_identifiers
        )
        left_index_ids = left_result.index_column_snowflake_quoted_identifiers
        left_frame = left_result.ordered_dataframe.select(
            left_frame_data_ids + left_index_ids
        )
    else:
        left_result, left_column_mapper = align_on_index(frame, other_frame, how=join)
        left_frame_data_ids = left_column_mapper.map_left_quoted_identifiers(
            frame.data_column_snowflake_quoted_identifiers
        )
        left_index_ids = left_result.index_column_snowflake_quoted_identifiers
        left_frame = left_result.ordered_dataframe.select(
            left_frame_data_ids + left_index_ids
        )
    return left_result, left_frame, left_frame_data_ids, left_index_ids


def align_axis_0_right(
    frame: InternalFrame, other_frame: InternalFrame, join: str
) -> tuple[InternalFrame, InternalFrame, list[str], list[str]]:
    """
    Gets the right align results.

    Args:
        frame: original frame
        other_frame: other frame
        join: type of alignment to be performed.

    Returns:
        Tuple containing:
            InternalFrame result of join_utils.align_on_index,
            final right_frame,
            list of right_frame_data_ids,
            list of right_index_ids
    """
    if join == "left":
        right_result, right_column_mapper = align_on_index(frame, other_frame, how=join)
        right_frame_data_ids = right_column_mapper.map_right_quoted_identifiers(
            other_frame.data_column_snowflake_quoted_identifiers
        )
        right_index_ids = right_result.index_column_snowflake_quoted_identifiers
        right_frame = right_result.ordered_dataframe.select(
            right_frame_data_ids + right_index_ids
        )
    elif join == "right":
        right_result, right_column_mapper = align_on_index(
            other_frame, frame, how="left"
        )
        right_frame_data_ids = right_column_mapper.map_left_quoted_identifiers(
            other_frame.data_column_snowflake_quoted_identifiers
        )
        right_index_ids = right_result.index_column_snowflake_quoted_identifiers
        right_frame = right_result.ordered_dataframe.select(
            right_frame_data_ids + right_index_ids
        )
    else:
        right_result, right_column_mapper = align_on_index(other_frame, frame, how=join)
        right_frame_data_ids = right_column_mapper.map_left_quoted_identifiers(
            other_frame.data_column_snowflake_quoted_identifiers
        )
        right_index_ids = right_result.index_column_snowflake_quoted_identifiers
        right_frame = right_result.ordered_dataframe.select(
            right_frame_data_ids + right_index_ids
        )
    return right_result, right_frame, right_frame_data_ids, right_index_ids


def align_axis_1(
    frame1: InternalFrame,
    frame2: InternalFrame,
    join: Literal["inner", "outer", "left", "right"],
    sort: Optional[bool] = False,
) -> tuple[InternalFrame, InternalFrame]:
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
            'left': Output frame contains columns from left frame.
            'right': Output frame contains columns from right frame.
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
    elif join == "left":
        data_column_labels = columns1
    elif join == "right":
        data_column_labels = columns2
    else:
        raise AssertionError(
            f"Invalid join type '{join}'. Accepted values are 'inner', 'outer', 'left', and 'right'."
        )
    if sort:
        data_column_labels = data_column_labels.sort_values()

    frame1 = _select_columns(frame1, data_column_labels.tolist())
    frame2 = _select_columns(frame2, data_column_labels.tolist())
    return frame1, frame2


def _select_columns(
    frame: InternalFrame, data_column_labels: list[Hashable]
) -> InternalFrame:
    """
    Select only the given labels from given frame for align. If any data column label is missing
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
            select_list.append(pandas_lit("nan").cast("float").as_(snowflake_id))
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
