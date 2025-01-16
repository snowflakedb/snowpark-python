#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from collections import Counter
from typing import Literal

from snowflake.snowpark._internal.type_utils import ColumnOrName
from snowflake.snowpark.functions import col
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin._internal.join_utils import align_on_index
from snowflake.snowpark.modin.plugin._internal.utils import pandas_lit


def align_axis_0_left(
    frame: InternalFrame, other_frame: InternalFrame, join: str
) -> InternalFrame:
    """
    Gets the left align results.

    Args:
        frame: original frame
        other_frame: other frame
        join: type of alignment to be performed.

    Returns:
        New InternalFrame representing aligned left frame.
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

    return InternalFrame.create(
        ordered_dataframe=left_frame,
        data_column_snowflake_quoted_identifiers=left_frame_data_ids,
        data_column_pandas_labels=frame.data_column_pandas_labels,
        data_column_pandas_index_names=frame.data_column_pandas_index_names,
        data_column_types=frame.cached_data_column_snowpark_pandas_types,
        index_column_snowflake_quoted_identifiers=left_index_ids,
        index_column_pandas_labels=left_result.index_column_pandas_labels,
        index_column_types=left_result.cached_index_column_snowpark_pandas_types,
    )


def align_axis_0_right(
    frame: InternalFrame, other_frame: InternalFrame, join: str
) -> InternalFrame:
    """
    Gets the right align results.

    Args:
        frame: original frame
        other_frame: other frame
        join: type of alignment to be performed.

    Returns:
        New InternalFrame representing aligned right frame.
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

    return InternalFrame.create(
        ordered_dataframe=right_frame,
        data_column_snowflake_quoted_identifiers=right_frame_data_ids,
        data_column_pandas_labels=other_frame.data_column_pandas_labels,
        data_column_pandas_index_names=other_frame.data_column_pandas_index_names,
        data_column_types=other_frame.cached_data_column_snowpark_pandas_types,
        index_column_snowflake_quoted_identifiers=right_index_ids,
        index_column_pandas_labels=right_result.index_column_pandas_labels,
        index_column_types=right_result.cached_index_column_snowpark_pandas_types,
    )


def align_axis_1(
    frame1: InternalFrame,
    frame2: InternalFrame,
    join: Literal["inner", "outer", "left", "right"],
) -> tuple[InternalFrame, InternalFrame]:
    """
    Aligns frames on their columns.

    Args:
        frame1: First frame
        frame2: Second frame
        join: How to handle column index
            'inner': Output frame contains only overlapping columns from both frames.
            'outer': Output frame contains union of columns from both frames.
            'left': Output frame contains columns from left frame.
            'right': Output frame contains columns from right frame.

    Returns:
        tuple representing aligned left and right InternalFrames.
    """
    columns1 = frame1.data_columns_index
    columns2 = frame2.data_columns_index

    inner_data_column_labels = columns1.intersection(columns2, sort=False).tolist()

    frame1_data_column_pandas_labels = frame1.data_column_pandas_labels
    frame2_data_column_pandas_labels = frame2.data_column_pandas_labels
    full_data_column_pandas_labels = get_full_label_list(
        frame1_data_column_pandas_labels,
        frame2_data_column_pandas_labels,
        inner_data_column_labels,
        join=join,
    )

    if join == "right":
        frame1 = align_axis_1_right_helper(
            frame1, full_data_column_pandas_labels, frame1_data_column_pandas_labels
        )
        frame2 = align_axis_1_left_helper(
            frame2, full_data_column_pandas_labels, frame1_data_column_pandas_labels
        )
    else:
        frame1 = align_axis_1_left_helper(
            frame1, full_data_column_pandas_labels, frame2_data_column_pandas_labels
        )
        frame2 = align_axis_1_right_helper(
            frame2, full_data_column_pandas_labels, frame2_data_column_pandas_labels
        )
    return frame1, frame2


def align_axis_1_left_helper(
    frame: InternalFrame,
    data_column_labels: list[str],
    other_frame_labels: list[str],
) -> InternalFrame:
    """
    Select the given labels from data_column_labels for aligned left frame. If any data column label is missing
    in frame add new column with NULL values. Duplicate column names will also be duplicated.

    Args:
        frame: An InternalFrame
        data_column_labels: A list of pandas labels.
        other_frame_labels: list of other frame labels

    Returns:
        New InternalFrame representing left aligned frame.

    """
    select_list: list[ColumnOrName] = []

    # Add index and ordering columns
    select_list.extend(frame.index_column_snowflake_quoted_identifiers)
    select_list.extend(frame.ordering_column_snowflake_quoted_identifiers)

    data_column_snowflake_identifiers = []
    other_counter = Counter(other_frame_labels)

    snowflake_ids = frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
        data_column_labels, include_index=False
    )

    curr_label_count_map: Counter = Counter()
    curr_label_index_map: Counter = Counter()

    # if snowflake_ids = [(D), (B1, B2), (B1, B2), (A1, A2), (A1, A2), (A1, A2), (B1, B2), (B1, B2), (A1, A2),
    # (A1, A2), (A1, A2)],
    # resulting aligned left frame column values will be
    # D B1 B1 A1 A1 A1 B2 B2 A2 A2 A2
    # where duplicate columns are selected from the tuple according to their tuple order
    for label, id_tuple in zip(data_column_labels, snowflake_ids):
        if (
            curr_label_count_map[label] > 0
            and curr_label_count_map[label] % other_counter[label] == 0
        ):
            curr_label_index_map[label] += 1
        index = curr_label_index_map[label]

        if len(id_tuple) == 0:
            # if missing, add new column to frame with NULL values.
            snowflake_id = (
                frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                    pandas_labels=[label]
                )[0]
            )
            select_list.append(pandas_lit("nan").cast("float").as_(snowflake_id))

        elif id_tuple[index] in data_column_snowflake_identifiers:
            # if col already exists, copy col values to frame.
            snowflake_id = id_tuple[index]
            new_column_snowflake_quoted_id = (
                frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                    pandas_labels=[label],
                    excluded=frame.data_column_snowflake_quoted_identifiers,
                )[0]
            )
            select_list.append(col(snowflake_id).as_(new_column_snowflake_quoted_id))
            snowflake_id = new_column_snowflake_quoted_id
            curr_label_count_map[label] += 1
        else:
            snowflake_id = id_tuple[index]
            select_list.append(snowflake_id)
            curr_label_count_map[label] += 1
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


def align_axis_1_right_helper(
    frame: InternalFrame,
    data_column_labels: list[str],
    frame_labels: list[str],
) -> InternalFrame:
    """
    Select the given labels from data_column_labels for aligned right frame. If any data column label is missing
    in frame add new column with NULL values. Duplicate column names will also be duplicated.

    Args:
        frame: An InternalFrame
        data_column_labels: A list of pandas labels.
        frame_labels: list of frame labels

    Returns:
       New InternalFrame representing right aligned frame.

    """
    select_list: list[ColumnOrName] = []

    # Add index and ordering columns
    select_list.extend(frame.index_column_snowflake_quoted_identifiers)
    select_list.extend(frame.ordering_column_snowflake_quoted_identifiers)

    data_column_snowflake_identifiers = []
    counter = Counter(frame_labels)

    snowflake_ids = frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
        data_column_labels, include_index=False
    )

    curr_label_count_map: Counter = Counter()
    curr_label_index_map: Counter = Counter()

    # if snowflake_ids = [(D), (B1, B2), (B1, B2), (A1, A2, A3),(B1, B2), (B1, B2), (A1, A2, A3)],
    # resulting aligned right frame column values will be
    # D B1 B2 A1 A2 A3 B1 B2 A1 A2 A3
    # where duplicate columns are selected from the tuple in the order they appear in the orig frame
    for label, id_tuple in zip(data_column_labels, snowflake_ids):
        if (
            curr_label_count_map[label] > 0
            and curr_label_count_map[label] % counter[label] != 0
        ):
            curr_label_index_map[label] += 1
        else:
            curr_label_index_map[label] = 0
        index = curr_label_index_map[label]

        if len(id_tuple) == 0:
            # if missing, add new column to frame with NULL values.
            snowflake_id = (
                frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                    pandas_labels=[label]
                )[0]
            )
            select_list.append(pandas_lit("nan").cast("float").as_(snowflake_id))

        elif id_tuple[index] in data_column_snowflake_identifiers:
            # if col already exists, copy col values to frame.
            snowflake_id = id_tuple[index]
            new_column_snowflake_quoted_id = (
                frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                    pandas_labels=[label],
                    excluded=frame.data_column_snowflake_quoted_identifiers,
                )[0]
            )
            select_list.append(col(snowflake_id).as_(new_column_snowflake_quoted_id))
            snowflake_id = new_column_snowflake_quoted_id
            curr_label_count_map[label] += 1
        else:
            snowflake_id = id_tuple[index]
            select_list.append(snowflake_id)
            curr_label_count_map[label] += 1
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


def get_full_label_list(
    frame1_labels: list[str],
    frame2_labels: list[str],
    inner_data_column_labels: list[str],
    join: str,
) -> list[str]:
    """
    Gets the final aligned frame labels.

    Args:
        frame1_labels: list of frame1 labels.
        frame2_labels:list of frame2 labels
        inner_data_column_labels: intersection of frame1 and frame2 labels.
        join: Type of alignment to be performed.
            left: use only keys from left frame, preserve key order.
            right: use only keys from right frame, preserve key order.
            outer: use union of keys from both frames, sort keys lexicographically.
            inner: use intersection of keys from both frames, preserve the order of the left keys.

    Returns:
        List of the final frame labels.
    """
    count1 = Counter(frame1_labels)
    count2 = Counter(frame2_labels)

    result_list = []

    # final label list is similar to a cross join of frame1 and frame 2 column labels. For ex,
    # if frame1 has cols ["D", "B", "C", "A", "B", "A", "E"] and frame2 has cols ["A", "B", "B", "C", "D", "A", "A"],
    # result_list for join="outer" is ["A", "A", "A", "A", "A", "A", "B", "B", "B", "B", "C", "D", "E"]

    # outer join sorts keys lexicographically, and the rest of the joins preserves key order including inner which
    # preserves left key order.

    if join == "inner":
        # Add elements from frame1 to result_list, based on frequency in frame2 if element is in intersection list
        for item1 in frame1_labels:
            if item1 in inner_data_column_labels:
                result_list.extend([item1] * count2[item1])

    elif join == "outer":
        # Add elements from frame1 to result_list, based on frequency in frame2. Add remaining items from frame2 and
        # then sort.
        for item1 in frame1_labels:
            if item1 in frame2_labels:
                result_list.extend([item1] * count2[item1])
            else:
                result_list.extend([item1])
        for item2 in frame2_labels:
            if item2 not in frame1_labels:
                result_list.extend([item2])
        result_list.sort()

    elif join == "left":
        # Add elements from frame1 to result_list, based on frequency in frame2.
        for item1 in frame1_labels:
            if item1 in frame2_labels:
                result_list.extend([item1] * count2[item1])
            else:
                result_list.extend([item1])

    elif join == "right":
        # Add elements from frame2 to result_list, based on frequency in frame1.
        for item2 in frame2_labels:
            if item2 in frame1_labels:
                result_list.extend([item2] * count1[item2])
            else:
                result_list.extend([item2])

    return result_list
