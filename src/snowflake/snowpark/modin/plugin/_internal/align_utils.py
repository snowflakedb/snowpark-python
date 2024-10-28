#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin._internal.join_utils import align_on_index


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
