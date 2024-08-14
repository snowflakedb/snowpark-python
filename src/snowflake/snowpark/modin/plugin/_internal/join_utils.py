#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from collections.abc import Hashable, Sequence
from enum import Enum, IntFlag, auto
from typing import NamedTuple, Optional, Union, get_args

import pandas.core.common as common
from pandas._typing import IndexLabel, Suffixes

from snowflake.snowpark._internal.utils import generate_random_alphanumeric
from snowflake.snowpark.functions import coalesce, to_variant
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin._internal.ordered_dataframe import (
    OrderedDataFrame,
    OrderingColumn,
)
from snowflake.snowpark.modin.plugin._internal.type_utils import (
    is_compatible_snowpark_types,
)
from snowflake.snowpark.modin.plugin._internal.utils import (
    ORDERING_COLUMN_LABEL,
    append_columns,
    extract_pandas_label_from_snowflake_quoted_identifier,
)
from snowflake.snowpark.modin.plugin._typing import AlignTypeLit, JoinTypeLit
from snowflake.snowpark.modin.plugin.compiler import snowflake_query_compiler
from snowflake.snowpark.types import VariantType


class JoinKeyCoalesceConfig(Enum):
    # replace lkey with coalesce(lkey, rkey) and remove rkey from merged frame.
    LEFT = "left"
    # replace rkey with coalesce(rkey, lkey) and remove lkey from merged frame.
    RIGHT = "right"
    # no coalesce is performed.
    NONE = "none"


class InheritJoinIndex(IntFlag):
    FROM_LEFT = auto()
    FROM_RIGHT = auto()
    FROM_BOTH = FROM_LEFT | FROM_RIGHT


class JoinOrAlignResultColumnMapper:
    """
    Join or Align result helper class that keeps the quoted identifier mapping from the original left
    and right dataframe to the result dataframe of the join or align.
    """

    # Map from the quoted identifiers of the original left frame to the quoted
    # identifiers of corresponding columns in the result frame.
    left_quoted_identifiers_map: dict[str, str]
    # Map from the quoted identifiers of the original right frame to the quoted
    # identifiers of corresponding columns in the result frame.
    right_quoted_identifiers_map: dict[str, str]

    def __init__(
        self,
        left_quoted_identifiers_map: dict[str, str],
        right_quoted_identifiers_map: dict[str, str],
    ) -> None:
        self.left_quoted_identifiers_map = left_quoted_identifiers_map
        self.right_quoted_identifiers_map = right_quoted_identifiers_map

    def map_left_quoted_identifiers(self, quoted_identifiers: list[str]) -> list[str]:
        """
        For a given set of quoted_identifiers from the original left frame, find the corresponding
        columns in the join or align result frame.
        """
        return [
            self.left_quoted_identifiers_map[quoted_identifier]
            for quoted_identifier in quoted_identifiers
        ]

    def map_right_quoted_identifiers(self, quoted_identifiers: list[str]) -> list[str]:
        """
        For a given set of quoted_identifiers from the original right frame, find the corresponding
        columns in the join or align result frame.
        """
        return [
            self.right_quoted_identifiers_map[quoted_identifier]
            for quoted_identifier in quoted_identifiers
        ]


class JoinOrAlignInternalFrameResult(NamedTuple):
    # The InternalFrame representation for the join or align result
    result_frame: InternalFrame
    # A column mapper that provides mapping from the column snowflake quoted identifiers of the
    # left and right frame to the corresponding mapped column snowflake quoted identifiers in the
    # result frame. The mapper contains mapping for index, data, ordering columns, and row position
    # column if exists.
    result_column_mapper: JoinOrAlignResultColumnMapper


def join(
    left: InternalFrame,
    right: InternalFrame,
    how: JoinTypeLit,
    left_on: list[str],
    right_on: list[str],
    sort: Optional[bool] = False,
    join_key_coalesce_config: Optional[list[JoinKeyCoalesceConfig]] = None,
    inherit_join_index: InheritJoinIndex = InheritJoinIndex.FROM_LEFT,
) -> JoinOrAlignInternalFrameResult:
    """
    Join ``left`` and ``right`` frames.

    Args:
        left: An internal frame to use on left side of join.
        right: An internal frame to use on right side of join.
        how: Type of join. Can be any of {'left', 'right', 'outer', 'inner', 'cross'}
        left_on: List of snowflake identifiers to join on from 'left' frame.
        right_on: List of snowflake identifiers to join on from 'right' frame.
            left_on and right_on must be lists of equal length.
        sort: If True order merged frame on join keys. If False, ordering behavior
            depends on join type as follows:
            For "right" join use right ordering and then left ordering.
            For every other type of join use left ordering and then right ordering
        join_key_coalesce_config: Optional list of coalesce config to indicate how to
            coalesce join columns in output frame or not. If provided, length of this
            list must be same as length of 'left_on'. If not provided, no coalesce is
            performed on join columns.
            Coalesce config can have the following values:
            - LEFT: replace left key with coalesce(lkey, rkey) and remove right key from
              merged frame.
            - RIGHT: replace right key with coalesce(lkey, rkey) and remove left key
              from merged frame.
            - NONE: no coalesce is performed.
        inherit_join_index:
            Indicates how to create index for merged frame.
            If FROM_LEFT, inherit from left frame.
            if FROM_RIGHT: inherit from right frame.
            if FROM_BOTH: inherit from left and right both.

    Returns:
        JoinOrAlignInternalFrameResult which is a NamedTuple contains the following:
            A InternalFrame for the InternalFrame join result.
            A JoinOrAlignResultColumnMapper that provides quoted identifiers mapping from the
                original left and right dataframe to the joined dataframe, it is guaranteed to
                include mapping for index + data columns, ordering columns and row position column
                if exists.
    """
    assert len(left_on) == len(
        right_on
    ), "left_on and right_on must be of same length or both be None"
    if join_key_coalesce_config is not None:
        assert len(join_key_coalesce_config) == len(
            left_on
        ), "join_key_coalesce_config must be of same length as left_on and right_on"
    assert how in get_args(
        JoinTypeLit
    ), f"Invalid join type: {how}. Allowed values are {get_args(JoinTypeLit)}"

    # Re-project the active columns to make sure all active columns of the internal frame participate
    # in the join operation, and unnecessary columns are dropped from the projected columns.
    left = left.select_active_columns()
    right = right.select_active_columns()

    joined_ordered_dataframe = left.ordered_dataframe.join(
        right.ordered_dataframe, left_on_cols=left_on, right_on_cols=right_on, how=how
    )

    return _create_internal_frame_with_join_or_align_result(
        joined_ordered_dataframe,
        left,
        right,
        how,
        left_on,
        right_on,
        sort,
        join_key_coalesce_config,
        inherit_join_index,
    )


def _create_internal_frame_with_join_or_align_result(
    result_ordered_frame: OrderedDataFrame,
    left: InternalFrame,
    right: InternalFrame,
    how: Union[JoinTypeLit, AlignTypeLit],
    left_on: list[str],
    right_on: list[str],
    sort: Optional[bool] = False,
    key_coalesce_config: Optional[list[JoinKeyCoalesceConfig]] = None,
    inherit_index: InheritJoinIndex = InheritJoinIndex.FROM_LEFT,
) -> JoinOrAlignInternalFrameResult:
    """
    Given the join or align result (result_ordered_frame), and the original left InternalFrame and right
    InternalFrame along with other join/align information, create the final result InternalFrame with
    all fields set correctly.

    Args:
        result_ordered_frame: OrderedDataFrame. The ordered dataframe result for the join/align operation.
        left: InternalFrame. The original left internal frame used for the join/align.
        right: InternalFrame. The original right internal frame used for the join/align.
        left_on: List[str]. The columns in original left internal frame used for join/align.
        right_on: List[str]. The columns in original right internal frame used for join/align.
        how: Union[JoinTypeLit, AlignTypeLit] join or align type.
        sort: Optional[bool] = False. Whether to sort the result lexicographically on the join/align keys.
        key_coalesce_config: Optional[List[JoinKeyCoalesceConfig]]. Optional list of coalesce config to
            indicate how to coalesce join/align columns in output frame or not. If provided, length of this
            list must be same as length of 'left_on'. If not provided, no coalesce is performed.
        inherit_index: InheritJoinIndex. Indicates how to create index for the merged frame.
            If FROM_LEFT, inherit from left frame.
            if FROM_RIGHT: inherit from right frame.
            if FROM_BOTH: inherit from left and right both.

    Returns:
        InternalFrame for the join/aligned result with all fields set accordingly.
    """

    result_helper = JoinOrAlignOrderedDataframeResultHelper(
        left.ordered_dataframe,
        right.ordered_dataframe,
        result_ordered_frame,
        left_on,
        right_on,
        how=how,
        sort=sort,
    )
    # get the join or aligned result with sort configuration
    result_ordered_frame = result_helper.join_or_align_result

    # Ordering behavior for data columns: left data columns + right data columns
    data_column_pandas_labels = (
        left.data_column_pandas_labels + right.data_column_pandas_labels
    )
    data_column_snowflake_quoted_identifiers = (
        result_helper.map_left_quoted_identifiers(
            left.data_column_snowflake_quoted_identifiers
        )
        + result_helper.map_right_quoted_identifiers(
            right.data_column_snowflake_quoted_identifiers
        )
    )

    index_column_pandas_labels = []
    index_column_snowflake_quoted_identifiers = []

    left_quoted_identifiers_map = (
        result_helper.result_column_mapper.left_quoted_identifiers_map.copy()
    )
    right_quoted_identifiers_map = (
        result_helper.result_column_mapper.right_quoted_identifiers_map.copy()
    )

    # inherit_join_index is a flag for which either FROM_LEFT, FROM_RIGHT or both can be set
    # to check whether FROM_LEFT, FROM_RIGHT or FROM_LEFT and FROM_RIGHT apply use in similar to
    # & in C/C++ when checking a flag
    if InheritJoinIndex.FROM_LEFT in inherit_index:
        index_column_pandas_labels.extend(left.index_column_pandas_labels)
        index_column_snowflake_quoted_identifiers.extend(
            result_helper.map_left_quoted_identifiers(
                left.index_column_snowflake_quoted_identifiers,
            )
        )
    if InheritJoinIndex.FROM_RIGHT in inherit_index:
        index_column_pandas_labels.extend(right.index_column_pandas_labels)
        index_column_snowflake_quoted_identifiers.extend(
            result_helper.map_right_quoted_identifiers(
                right.index_column_snowflake_quoted_identifiers,
            )
        )

    if key_coalesce_config:
        coalesce_column_identifiers = []
        coalesce_column_values = []
        for origin_left_col, origin_right_col, coalesce_config in zip(
            left_on, right_on, key_coalesce_config
        ):
            if coalesce_config == JoinKeyCoalesceConfig.NONE:
                continue
            left_col = result_helper.map_left_quoted_identifiers([origin_left_col])[0]
            right_col = result_helper.map_right_quoted_identifiers([origin_right_col])[
                0
            ]
            # Coalescing is only required for 'outer' join or align.
            # For 'inner' and 'left' join we use left join keys and for 'right' join we
            # use right join keys.
            # For 'left' and 'coalesce' align we use left join keys.
            if how == "outer":
                # Generate an expression equivalent of
                # "COALESCE('left_col', 'right_col') as 'left_col'"
                coalesce_column_identifier = (
                    result_ordered_frame.generate_snowflake_quoted_identifiers(
                        pandas_labels=[
                            extract_pandas_label_from_snowflake_quoted_identifier(
                                left_col
                            )
                        ],
                    )[0]
                )
                coalesce_column_identifiers.append(coalesce_column_identifier)
                coalesce_column_values.append(coalesce(left_col, right_col))
            elif how == "right":
                # No coalescing required for 'right' join. Simply use right join key
                # as output column.
                coalesce_column_identifier = right_col
            elif how in ("inner", "left", "coalesce"):
                # No coalescing required for 'left' or 'inner' join and for 'left' or
                # 'coalesce' align. Simply use left join key as output column.
                coalesce_column_identifier = left_col
            else:
                raise AssertionError(f"Unsupported join/align type {how}")

            if coalesce_config == JoinKeyCoalesceConfig.RIGHT:
                # swap left_col and right_col
                left_col, right_col = right_col, left_col

            # To provide same behavior as native pandas, remove duplicate join column.
            if right_col in data_column_snowflake_quoted_identifiers:
                # Remove duplicate data column.
                index = data_column_snowflake_quoted_identifiers.index(right_col)
                data_column_snowflake_quoted_identifiers.pop(index)
                data_column_pandas_labels.pop(index)
            elif right_col in index_column_snowflake_quoted_identifiers:
                # Remove duplicate index column if present.
                index = index_column_snowflake_quoted_identifiers.index(right_col)
                index_column_snowflake_quoted_identifiers.pop(index)
                index_column_pandas_labels.pop(index)

            # Update data/index column identifier
            data_column_snowflake_quoted_identifiers = [
                coalesce_column_identifier if x == left_col else x
                for x in data_column_snowflake_quoted_identifiers
            ]
            index_column_snowflake_quoted_identifiers = [
                coalesce_column_identifier if x == left_col else x
                for x in index_column_snowflake_quoted_identifiers
            ]
            # map the original left and right col to the new coalesced column
            left_quoted_identifiers_map[origin_left_col] = coalesce_column_identifier
            right_quoted_identifiers_map[origin_right_col] = coalesce_column_identifier

        if coalesce_column_identifiers:
            # This might change order of identifiers in snowpark dataframe. But we
            # don't depend on order of identifiers in snowpark dataframe so, it's okay to
            # do this.
            result_ordered_frame = append_columns(
                result_ordered_frame,
                coalesce_column_identifiers,
                coalesce_column_values,
            )

    if not is_column_index_compatible(left, right):
        # Flatten column labels if joining frames have incompatible index levels
        # Example:
        # >>> import pandas as pd
        # >>> df1 = pd.DataFrame(['x', 'y'], columns=pd.MultiIndex.from_tuples([('A', 0)]))
        # >>> df2 = pd.DataFrame({"B": [0, 1]})
        # >>> df1.join(df2)
        # 	 (A, 0)	B
        # 0	  x	    0
        # 1	  y	    1

        # Number of column index levels are decided by length of
        # 'data_column_pandas_index_names'. So setting it to an array of length one
        # will flatten column index levels in resultant InternalFrame.
        data_column_pandas_index_names = [None]
    else:
        data_column_pandas_index_names = left.data_column_pandas_index_names

    result_internal_frame = InternalFrame.create(
        ordered_dataframe=result_ordered_frame,
        data_column_pandas_labels=data_column_pandas_labels,
        data_column_snowflake_quoted_identifiers=data_column_snowflake_quoted_identifiers,
        index_column_pandas_labels=index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=index_column_snowflake_quoted_identifiers,
        data_column_pandas_index_names=data_column_pandas_index_names,
    )
    result_column_mapper = JoinOrAlignResultColumnMapper(
        left_quoted_identifiers_map,
        right_quoted_identifiers_map,
    )

    return JoinOrAlignInternalFrameResult(result_internal_frame, result_column_mapper)


def is_column_index_compatible(left: InternalFrame, right: InternalFrame) -> bool:
    """
    Return true if column index of 'right' frame is compatible with column index of
    'left' frame. Column index is considered compatible if
    1. Both the frames have same number of column index levels OR
    2. Right column index has one level but, all the labels in it are tuple with length
       same as number of levels in left frame.
    Args:
        left: the left internal frame to check the index against
        right: the right internal frame to check the index compatibility for

    Returns:
        True if column index of 'right' frame is compatible with column index of
        'left' frame, False otherwise.
    """
    if left.num_index_levels(axis=1) == right.num_index_levels(axis=1):
        return True
    # Check if all labels in 'right' frame are tuples with length same as number of
    # levels in left frame.
    left_num_levels = left.num_index_levels(axis=1)
    if right.num_index_levels(axis=1) == 1 and all(
        [
            isinstance(label, tuple) and len(label) == left_num_levels
            for label in right.data_column_pandas_labels
        ]
    ):
        return True
    return False


def rename_conflicting_data_column_labels(
    left: "snowflake_query_compiler.SnowflakeQueryCompiler",
    right: "snowflake_query_compiler.SnowflakeQueryCompiler",
    common_join_keys: list[Hashable],
    suffixes: Suffixes,
) -> tuple[InternalFrame, InternalFrame]:
    """
    Rename conflicting data column labels from given query compilers.
    Conflicting here means if same column label is present in both the frames.

    Same as native pandas we follow these rules when renaming conflicting labels.
    1. Suffix is added to labels only if there is conflict. We don't add it all the
      labels. For example left frame with columns ["A", "B", "C"] is merged with right
      frame with columns ["A", "C", "D"] as
      left.merge(right, on="A", suffixes=("_x". "_y")).
      This will result in a frame with columns ["A", "B", "C_x", "C_y", "D"]. Here "A"
      is common_join_key hence coalesced in merged frame. "B" and "D" has no conflicts.
      "C" has conflict these are renamed to "C_x" and "C_y" for left and right frame
      respectively.
    2. Even though we check for the whole label to detect conflict, when we apply
      rename, it is applied to the first level that is the same as the conflict label.
      In case of multiIndex columns, suffix is added to only first level. For example
      a conflicting label ("A", "a") will become ("A_x", "a") and ("A_y", "a") in left
      and right frames respectively.
    3. When suffix is added to non-str label component it will change to str. For
      example a conflicting label (1, 2) will become ("1_x", 2) and ("1_y", 2) in left
      and right frames respectively.
    4. When we rename a conflicting level, we rename all the labels with same first
      level. So as a side effect we might rename column labels which were not really
      conflicting.
      Consider following scenario as an example:
      Columns in left frame: [("A", 1), ("A", 2), ("B", 3)]
      Columns in right frame: [("A", 1), ("B", 3), ("C", 4)]
      After performing left.merge(right, on=[("B", 3)] operation
      Columns in merged frame: [("A_x", 1), ("A_x", 2), ("B", 3), ("A_y", 1), ("C", 4)]
      In above example second column we have a conflict for ("A", 1) that results in
      renamed first level from "A" to "A_x" for the columns in left frame. As a  result
      ("A", 2) was renamed to ("A_x", 2) even though there was no conflicting label in
      right frame.
      Also note that in above example ("B", 3) was present in both frames but this is
      the common join key so this is not renamed and merged frame will have only one
      column with coalesced values.

    Args:
        left: Left query compiler to merge.
        right:  Right query compiler to merge.
        common_join_keys: A list of common join labels.
        suffixes: Suffix to addd to conflicting data column labels.

    Returns:
        Tuple of left and right InternalFrame with renamed labels.

    """
    first_level_of_conflicting_labels = []
    for label in left._modin_frame.data_column_pandas_labels:
        if (
            label not in common_join_keys
            and label in right._modin_frame.data_column_pandas_labels
        ):
            first_level = label[0] if left.is_multiindex(axis=1) else label
            first_level_of_conflicting_labels.append(first_level)

    if not first_level_of_conflicting_labels:
        # If no conflicts, return frames from original query compilers.
        return left._modin_frame, right._modin_frame

    if suffixes[0] is not None:
        left = left.rename(
            columns_renamer=lambda x: str(x) + str(suffixes[0])
            if x in first_level_of_conflicting_labels
            else x,
            level=0,  # Rename only first level of columns labels to be consistent with native pandas.
        )
    if suffixes[1] is not None:
        right = right.rename(
            columns_renamer=lambda x: str(x) + str(suffixes[1])
            if x in first_level_of_conflicting_labels
            else x,
            level=0,  # Rename only first level of columns labels to be consistent with native pandas.
        )
    return left._modin_frame, right._modin_frame


def map_labels_to_renamed_frame(
    original_labels: list[Hashable],
    original_frame: InternalFrame,
    renamed_frame: InternalFrame,
) -> list[Hashable]:
    """
    Args:
        original_labels: A list of pandas labels.
        original_frame: Original frame. Given 'original_labels' belong to this frame.
        renamed_frame: Renamed frame. Frame created after renaming zero or more labels
            of 'original_frame'.

    Returns:
        A list of pandas labels from renamed_frame which maps to 'original_labels' from
        'original_frame'.
    """
    # Create a map from original pandas labels to renamed pandas labels. This map
    # contains all data labels even if they were not renamed.
    renamed_labels_map = dict(
        zip(
            original_frame.data_column_pandas_labels,
            renamed_frame.data_column_pandas_labels,
        )
    )
    # 'original_labels' might have index labels as well, such labels will not have a
    # corresponding entry in 'renamed_labels_map'. For such labels keep the original
    # value.
    return [renamed_labels_map.get(x, x) for x in original_labels]


def get_join_keys(
    left: InternalFrame,
    right: InternalFrame,
    on: Optional[IndexLabel] = None,
    left_on: Optional[IndexLabel] = None,
    right_on: Optional[IndexLabel] = None,
    left_index: Optional[bool] = False,
    right_index: Optional[bool] = False,
) -> tuple[Sequence[Hashable], Sequence[Hashable]]:
    """
    Get join keys (pandas labels) for given frames using join arguments.
    This method doesn't do any error checking For example, 'on' and 'left_on' both
    are provided etc. This method assumes caller has already done error checking and
    inputs arguments are valid.

    Args:
        left: Dataframe on left side of join.
        right: Dataframe on right side of join.
        on: Labels or list of such to join on.
        left_on: Labels or list of such to join on in the left frame.
        right_on: Labels or list of such to join on in the right frame.
        left_index: If True, use index from left frame as join keys.
        right_index: If True, use index from right frame as join keys.

    Returns:
        A tuple of two sequences. Where first sequence is join_keys from left frame and
        second sequence is join_keys from right frame.

    """
    # If no join columns are provided we join on common data columns from both the
    # frames.
    # If there are no common columns it's an error condition which is already taken care
    # of by frontend layer. So here 'on' will be a non-empty list.
    # Take ordered intersection. If sort=True, we need to preserve order of join keys
    # to create ordering columns in correct order.
    if not (on or left_on or right_on or left_index or right_index):
        right_labels = set(right.data_column_pandas_labels)
        on = [
            label for label in left.data_column_pandas_labels if label in right_labels
        ]

    # Populate left join keys.
    left_keys = []
    if on is not None:
        left_keys = on
    elif left_on is not None:
        left_keys = left_on
    elif left_index:
        left_keys = left.index_column_pandas_labels

    # Populate right join keys.
    right_keys = []
    if on is not None:
        right_keys = on
    elif right_on is not None:
        right_keys = right_on
    elif right_index:
        right_keys = right.index_column_pandas_labels

    # Convert 'left_keys' and 'right_keys' to list if not a list or tuple.
    if not isinstance(left_keys, (list, tuple)):
        left_keys = [left_keys]
    if not isinstance(right_keys, (list, tuple)):
        right_keys = [right_keys]

    return left_keys, right_keys


def insert_external_join_keys_into_join_frames(
    left_query_compiler: "snowflake_query_compiler.SnowflakeQueryCompiler",
    left_keys: Sequence[
        Union[Hashable, "snowflake_query_compiler.SnowflakeQueryCompiler"]
    ],
    right_query_compiler: "snowflake_query_compiler.SnowflakeQueryCompiler",
    right_keys: Sequence[
        Union[Hashable, "snowflake_query_compiler.SnowflakeQueryCompiler"]
    ],
) -> tuple[
    "snowflake_query_compiler.SnowflakeQueryCompiler",
    list[Hashable],
    "snowflake_query_compiler.SnowflakeQueryCompiler",
    list[Hashable],
    list[str],
]:
    """
    Insert external data join keys as columns into corresponding query compilers.
    Args:
        left_query_compiler: the query compiler for left side of join
        left_keys: sequence of left join keys.
        right_query_compiler: the query compiler for right side of join
        right_keys: sequence of right join keys.

    Returns:
      A tuple of
        1. Updated left snowflake query compiler.
        2. Updated list of left join keys. All elements are pandas labels.
        3. Updated right snowflake query compiler.
        4. Update list of right join keys. All elements are pandas labels.
        5. List of pandas labels for all external join keys.
    """
    updated_left_keys = []
    updated_right_keys = []
    key_suffix = 0
    external_key_labels = []
    for lkey, rkey in zip(left_keys, right_keys):
        is_left_qc = isinstance(lkey, snowflake_query_compiler.SnowflakeQueryCompiler)
        is_right_qc = isinstance(rkey, snowflake_query_compiler.SnowflakeQueryCompiler)
        if is_left_qc and is_right_qc:
            # Error checking should already be done in frontend to ensure that key_<N>
            # label does not conflict with existing data column in dataframe. This
            # column will show up in merged frame.
            # Note: Label generation logic here is same as native pandas.
            key_label = f"key_{key_suffix}"
            key_suffix = key_suffix + 1
        elif is_left_qc or is_right_qc:
            # Generate a label with random suffix to avoid any conflict with existing
            # data labels. This column is only temporary, during merge operation
            # values of this column will be coalesced with other join key and this
            # column is removed from merged frame.
            key_label = f"join_key_{generate_random_alphanumeric()}"
        else:
            # will not be used
            key_label = None

        if key_label is not None:
            external_key_labels.append(key_label)

        if is_left_qc:
            left_query_compiler = left_query_compiler.insert(0, key_label, lkey)
            lkey = key_label
        updated_left_keys.append(lkey)

        if is_right_qc:
            right_query_compiler = right_query_compiler.insert(0, key_label, rkey)
            rkey = key_label
        updated_right_keys.append(rkey)
    return (
        left_query_compiler,
        updated_left_keys,
        right_query_compiler,
        updated_right_keys,
        external_key_labels,
    )


class IndexJoinInfo(NamedTuple):
    """
    The information required to perform join on index operation.
    """

    # the snowflake quoted identifiers for the index columns to perform join on in the left frame
    left_join_quoted_identifiers: list[str]
    # the snowflake quoted identifiers for the index columns to perform join on in the right frame
    right_join_quoted_identifiers: list[str]
    # the expected pandas labels in order for index columns in the result frame
    result_index_labels: list[Hashable]


def _get_index_columns_to_join(
    left: InternalFrame,
    right: InternalFrame,
    how: Union[JoinTypeLit, AlignTypeLit],
) -> IndexJoinInfo:
    """
    Decide the index columns that need to participate in join. Depends on single or multiindex situation
    of left and right, following rules are applied when deriving the index columns to join (which is
    the same as Native pandas):
    1. If both are single index:
        Join on index column.
        Index column label is inherited from left frame
        NOTE: In pandas 1.5.3 index column label is reset to None but this is
            fixed in pandas 2.x). We follow pandas 2.x behavior here which is also
            consistent with joining on data columns.
        Index values from both frames are coalesced together to produce index column of
        merged frame.

    2. If both are multiindex and left.index_column_pandas_labels == right.index_column_pandas_labels
        Join on all index columns.
        Index columns labels are inherited from left frame.
        Index values from both frames are coalesced together to produce index column of
        merged frame.
        NOTE: this case is similar to case #1 in behavior.

    3. If both are multiindex and left.index_column_pandas_labels != right.index_column_pandas_labels:
        Join on common index columns.
        Index of merged frame contains index columns from both the frames (so it will
        have more index columns than any of input frames) but common index columns are
        not duplicated and values for such index columns are also coalesces together.
        (This is similar to joining on data columns when join labels is same on both sides)
        Order of index columns in output frame =
            common index columns (value are coalesced)
            + remaining left index columns
            + remaining right index columns
        Reference: https://github.com/pandas-dev/pandas/blob/v1.5.3/pandas/core/indexes/base.py#L4764
        Example:
           left: index = ['A', 'B']   columns = ['c1', 'c2']
           right: index = ['B', 'C'] columns = ['c3']
           left.merge(right, left_index=True, right_index=True)
           join condition: left.B == right.B
           result: index = ['B', 'A', 'C'] columns = ['c1', 'c2', 'c3']

    4. If only one frame has multiindex:
        This is same as case #2 above except for order of index columns in output frame.
        In this case order of index columns is inherited from multiindex of input frame
        (left for right doesn't matter).
        Reference: https://github.com/pandas-dev/pandas/blob/v1.5.3/pandas/core/indexes/base.py#L4789
        Example 1 (multi to single):
            left: index = ['A', 'B']   columns = ['c1', 'c2']
            right: index = ['B'] columns = ['c3']
            left.merge(right, left_index=True, right_index=True)
            join condition: left.B == right.B
            result: index = ['A', 'B'] columns = ['c1', 'c2', 'c3']

        Example 2 (single to multi):
            left: index = ['B']   columns = ['c1', 'c2']
            right: index = ['A', 'B'] columns = ['c3']
            left.merge(right, left_index=True, right_index=True)
            join condition: left.B == right.B
            result: index = ['A', 'B'] columns = ['c1', 'c2', 'c3']

    Args:
        left: Dataframe on left side of join.
        right: Dataframe on right side of join.
        how: Join or align type.

    Returns:
        Tuple contains:
            1) the quoted identifier for index columns in left used for join
            2) the quited identifier for index columns in right used for join
            3) the final pandas labels for index columns of the join result
    """
    is_left_multiindex = left.is_multiindex(axis=0)
    is_right_multiindex = right.is_multiindex(axis=0)
    if (
        not is_left_multiindex
        and not is_right_multiindex
        or (left.index_column_pandas_labels == right.index_column_pandas_labels)
    ):
        # Case 1 & Case 2
        left_ids = left.index_column_snowflake_quoted_identifiers
        right_ids = right.index_column_snowflake_quoted_identifiers
        expected_index_labels = left.index_column_pandas_labels
    else:
        # Case 3 and Case 4 (They only differ on output order of index columns)
        #
        # When joining on index, if either frame has multiindex and index column labels
        # are different we join on common index labels.
        #
        # Take ordered intersection of left index columns and right index columns.
        # This order is required to construct index of merged frame.
        right_labels = set(right.index_column_pandas_labels)
        common_labels = [
            left_label
            for left_label in left.index_column_pandas_labels
            if left_label in right_labels
        ]
        if common.any_none(common_labels):
            # 'None' does not participate when computing overlapping index names.
            # So it can not be only overlapping index label.
            # https://github.com/pandas-dev/pandas/blob/v1.5.3/pandas/core/indexes/base.py#L4729
            # This is already handled by frontend layer. Add this check for extra
            # safety.
            assert (
                len(common_labels) > 1
            ), "'None' can not be only overlapping index label"

        if len(common_labels) == 0:
            raise ValueError("cannot join with no overlapping index names")
        # Error checking for duplicate labels is already done in frontend layer, so
        # it's safe to use first element from mapped identifiers.
        left_ids = [
            ids[0]
            for ids in left.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                common_labels
            )
        ]
        right_ids = [
            ids[0]
            for ids in right.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                common_labels
            )
        ]
        if is_left_multiindex and is_right_multiindex:
            # Case 3
            # Order of index columns in output frame =
            # right index columns + remaining left index columns (if how != right)
            # left index columns + remaining right index columns otherwise (otherwise)
            left_remaining_labels = [
                label
                for label in left.index_column_pandas_labels
                if label not in common_labels
            ]
            right_remaining_labels = [
                label
                for label in right.index_column_pandas_labels
                if label not in common_labels
            ]
            if how == "right":
                expected_index_labels = (
                    right.index_column_pandas_labels + left_remaining_labels
                )
            else:
                expected_index_labels = (
                    left.index_column_pandas_labels + right_remaining_labels
                )
        else:
            # Case 4
            expected_index_labels = (
                left.index_column_pandas_labels
                if is_left_multiindex
                else right.index_column_pandas_labels
            )

    return IndexJoinInfo(
        left_join_quoted_identifiers=left_ids,
        right_join_quoted_identifiers=right_ids,
        result_index_labels=expected_index_labels,
    )


def _reorder_index_columns(
    frame: InternalFrame, target_index_labels: list[Hashable]
) -> InternalFrame:
    """
    Reorder the index column for a given InternalFrame to the target_index_labels.

    Note: the reorder is only valid when 1) the length of index column of the frame is the
        same as the target_index_labels. 2) all labels in target_index_labels are unique and
        occurs inside the index column of the frame.

    Args:
        frame: InternalFrame. The internal frame whose index needs to be reordered.
        target_index_labels: List[Hashable]. The final index label order.

    Returns:
        An InternalFrame with index columns reordered to the target.
    """
    # Returned frame from join_utils has index columns in order:
    # left index columns + right index columns
    # Update it according to expected order
    current_index_column_pandas_labels = frame.index_column_pandas_labels
    if current_index_column_pandas_labels != target_index_labels:
        # reorder needed
        assert len(target_index_labels) == len(current_index_column_pandas_labels)
        assert len(current_index_column_pandas_labels) == len(
            set(current_index_column_pandas_labels)
        ), "reorder index columns with duplication is not allowed"

        index_column_snowflake_quoted_identifiers = []
        for label in target_index_labels:
            assert (
                label in current_index_column_pandas_labels
            ), f"can not find column with label {label}"
            i = current_index_column_pandas_labels.index(label)
            index_column_snowflake_quoted_identifiers.append(
                frame.index_column_snowflake_quoted_identifiers[i]
            )
        return InternalFrame.create(
            ordered_dataframe=frame.ordered_dataframe,
            index_column_pandas_labels=target_index_labels,
            index_column_snowflake_quoted_identifiers=index_column_snowflake_quoted_identifiers,
            data_column_pandas_labels=frame.data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers=frame.data_column_snowflake_quoted_identifiers,
            data_column_pandas_index_names=frame.data_column_pandas_index_names,
        )
    else:
        return frame


def join_on_index_columns(
    left: InternalFrame,
    right: InternalFrame,
    how: JoinTypeLit,
    sort: bool,
) -> JoinOrAlignInternalFrameResult:
    """
    Perform join operation on index columns with the specified method (`how`), and preserves order based on sort.
    Refer to _get_index_columns_to_join for details about how index columns used for join is decided.

    Args:
        left: Dataframe on left side of join.
        right: Dataframe on right side of join.
        how: Join type.
        sort: If True, sort the result on join columns.

    Returns:
        An InternalFrame for the joined result.
        A JoinOrAlignResultColumnMapper that provides quited identifiers mapping from the
            original left and right dataframe to the joined dataframe, it is guaranteed to
            include mapping for index + data columns, ordering columns and row position column
            if exists.
    """
    index_join_info = _get_index_columns_to_join(left, right, how)

    joined_frame, result_column_mapper = join(
        left,
        right,
        how=how,
        left_on=index_join_info.left_join_quoted_identifiers,
        right_on=index_join_info.right_join_quoted_identifiers,
        sort=sort,
        # To match native pandas behavior, join index columns are coalesced.
        join_key_coalesce_config=[JoinKeyCoalesceConfig.LEFT]
        * len(index_join_info.left_join_quoted_identifiers),
        inherit_join_index=InheritJoinIndex.FROM_BOTH,
    )

    joined_frame = _reorder_index_columns(
        joined_frame, index_join_info.result_index_labels
    )

    return JoinOrAlignInternalFrameResult(joined_frame, result_column_mapper)


def convert_incompatible_types_to_variant(
    left: InternalFrame,
    right: InternalFrame,
    left_ids: list[str],
    right_ids: list[str],
) -> tuple[InternalFrame, InternalFrame]:
    """
    Check types of given identifiers if they are not compatible covert them to variant.
    Two types are considered compatible if both same or both are numeric types.
    Args:
        left: Left frame.
        right: Right frame
        left_ids: List of Snowflake identifiers to check in left frame.
        right_ids: List of Snowflake identifiers to check in right frame. Length of
            this list must be same as length of 'left_ids'

    Returns:
        Tuple of left and right frames with updated columns.
    """
    assert len(left_ids) == len(right_ids)

    left_id_to_type_map = left.quoted_identifier_to_snowflake_type()
    right_id_to_type_map = right.quoted_identifier_to_snowflake_type()

    left_to_variant = {}
    right_to_variant = {}

    for left_id, right_id in zip(left_ids, right_ids):
        left_type = left_id_to_type_map[left_id]
        right_type = right_id_to_type_map[right_id]
        if not is_compatible_snowpark_types(left_type, right_type):
            if left_type != VariantType:
                left_to_variant[left_id] = to_variant(left_id)
            if right_type != VariantType:
                right_to_variant[right_id] = to_variant(right_id)

    left = left.update_snowflake_quoted_identifiers_with_expressions(
        left_to_variant
    ).frame
    right = right.update_snowflake_quoted_identifiers_with_expressions(
        right_to_variant
    ).frame

    return left, right


def align(
    left: InternalFrame,
    right: InternalFrame,
    left_on: list[str],
    right_on: list[str],
    how: AlignTypeLit = "outer",
) -> JoinOrAlignInternalFrameResult:
    """
    Align the left and the right frame on given columns 'left_on' and 'right_on' with
    given join method (`how`).

    Args:
        left: Left Internal frame
        right: Right Internal frame
        left_on: List of snowflake identifiers to align on from 'left' frame.
        right_on: List of snowflake identifiers to align on from 'right' frame. Length
            of this list must be same as length 'left_on'.
        how:
            * left: use only index from left frame, preserve left order.
            * coalesce: use only index from left frame, preserve left order. If left
              frame is empty left_on columns are coalesced with right_on columns.
            * outer: use union of index from both frames, sort index lexicographically.
    Returns:
        New aligned InternalFrame by aligning left frame with right frame.
    """
    assert len(left_on) == len(right_on), "left_on and right_on must be of same length"
    # Example 1 (left is non-empty):
    # left:
    # li  A  left_row_pos
    # 1   a  0
    # 2   b  1
    #
    # right:
    # ri  B  right_row_pos
    # 3   d  0
    # 4   e  1
    #

    # Example 2 (left is empty):
    # left:
    # li  A  left_row_pos
    # <no rows>
    #
    # right:
    # ri  B  right_row_pos
    # 3   d  0
    # 4   e  1
    aligned_ordered_frame = left.ordered_dataframe.align(
        right.ordered_dataframe,
        left_on_cols=left_on,
        right_on_cols=right_on,
        how=how,
    )
    # aligned_ordered_frame after aligning on row_position columns
    # Example 1 (left is empty not empty):
    # aligned_ordered_frame:
    # li  A  left_row_pos  row_count ri  B  right_row_pos
    # 1   a  0             2         3   d  1
    # 2   b  1             2         4   e  2

    # Example 2 (left is empty):
    # aligned_ordered_frame:
    # li    A    left_row_pos  row_count ri  B  right_row_pos
    # NULL  NULL 1             NULL      3   d  1
    # NULL  NULL 2             NULL      4   e  2
    coalesce_key_config = None
    inherit_join_index = InheritJoinIndex.FROM_LEFT
    # When it is `outer` align, we need to coalesce the align columns. However, if the
    # ordering columns of aligned result is the same as the left frame, that means the
    # join columns of left and right matches, then there is no need to coalesce the join
    # keys, simply inherent from left gives the correct result.
    # Retaining the original columns also helps avoid unnecessary join in later steps.
    if (
        how == "outer"
        and aligned_ordered_frame.ordering_columns != left.ordering_columns
    ):
        coalesce_key_config = [JoinKeyCoalesceConfig.LEFT] * len(left_on)
        inherit_join_index = InheritJoinIndex.FROM_BOTH
    (
        aligned_frame,
        result_column_mapper,
    ) = _create_internal_frame_with_join_or_align_result(
        aligned_ordered_frame,
        left,
        right,
        left_on=left_on,
        right_on=right_on,
        how=how,
        sort=False,
        key_coalesce_config=coalesce_key_config,
        inherit_index=inherit_join_index,
    )
    return JoinOrAlignInternalFrameResult(aligned_frame, result_column_mapper)


def align_on_index(
    left: InternalFrame,
    right: InternalFrame,
    how: AlignTypeLit = "outer",
) -> JoinOrAlignInternalFrameResult:
    """
    Align the left and the right frame on the index columns with given join method (`how`).

    The index columns used for align are decided in the same way as join on index, please refer to
    _get_index_columns_to_join for details about how index columns used for align is decided.

    Please refer to align operator in OrderedDataFrame for details about how align operation is
    performed.

    Args:
        left: Left DataFrame.
        right: right DataFrame.
        how: the align method {{'left', 'coalesce', 'outer'}}, by default is outer
            * left: use only index from left frame, preserve left order.
            * coalesce: if left frame has non-zero rows use only index from left
                frame, preserve left order otherwise use only right index and preserver
                right order.
            * outer: use union of index from both frames, sort index lexicographically.
    Returns:
        An InternalFrame for the aligned result.
        A JoinOrAlignResultColumnMapper that provides quited identifiers mapping from the
            original left and right dataframe to the aligned dataframe, it is guaranteed to
            include mapping for index + data columns, ordering columns and row position column
            if exists.
    """

    index_join_info = _get_index_columns_to_join(left, right, how)
    # Re-project the active columns to make sure all active columns of the internal frame participate in
    # the align operation, and unnecessary columns are dropped from the projection.
    left = left.select_active_columns()
    right = right.select_active_columns()

    aligned_frame, result_column_mapper = align(
        left,
        right,
        left_on=index_join_info.left_join_quoted_identifiers,
        right_on=index_join_info.right_join_quoted_identifiers,
        how=how,
    )
    if how == "outer":
        # index reorder should only be needed for outer join since this is the only method inherent
        # index from both side and coalesces the align on keys.
        aligned_frame = _reorder_index_columns(
            aligned_frame, target_index_labels=index_join_info.result_index_labels
        )
    return JoinOrAlignInternalFrameResult(aligned_frame, result_column_mapper)


class JoinOrAlignOrderedDataframeResultHelper:
    """
    Helper class for join or aligned result that does the following:
    1) Handles ordering of final result according to sort value.
    2) Provide interfaces that help map the snowflake quoted identifiers from the original
        ordered dataframe to the result dataframe.

    Note that
        1) self.join_or_align_result gives the join or aligned result with the correct order
            based on the sort configuration, not the original join or align result. The caller
            should always use the join_or_align_result of this helper class for any post-processing left to do.
        2) this class operates on the ordered dataframe
    """

    # The result ordered frame after join or align, sorted on join keys if
    # sort is set True during init.
    join_or_align_result: OrderedDataFrame
    # The join or align on columns in the original left frame
    _left_on: list[str]
    # The join or align on columns in the original right frame
    _right_on: list[str]
    # Join or Align type
    _how: Union[JoinTypeLit, AlignTypeLit]
    result_column_mapper: JoinOrAlignResultColumnMapper

    def __init__(
        self,
        left: OrderedDataFrame,
        right: OrderedDataFrame,
        origin_join_or_align_res: OrderedDataFrame,
        left_on: list[str],
        right_on: list[str],
        how: Union[JoinTypeLit, AlignTypeLit],
        sort: Optional[bool] = None,
    ) -> None:
        self._left_on = left_on
        self._right_on = right_on
        # create a mapping between
        original_left_quoted_identifiers = (
            left.projected_column_snowflake_quoted_identifiers
        )
        original_right_quoted_identifiers = (
            right.projected_column_snowflake_quoted_identifiers
        )
        result_quoted_identifiers = (
            origin_join_or_align_res.projected_column_snowflake_quoted_identifiers
        )
        # build a map between the quoted identifiers for the original left and right frame to the
        # corresponding quoted identifiers in result_ordered_frame. The projected columns of result_ordered_frame
        # for both join and align is guaranteed to be in the order of
        # <left projected columns> + <right projected columns with deduplication> + <extra ordering column>
        left_len = len(original_left_quoted_identifiers)
        right_len = len(original_right_quoted_identifiers)
        result_left_quoted_identifier = result_quoted_identifiers[:left_len]
        result_right_quoted_identifier = result_quoted_identifiers[
            left_len : (left_len + right_len)
        ]
        self.result_column_mapper = JoinOrAlignResultColumnMapper(
            left_quoted_identifiers_map=dict(
                zip(original_left_quoted_identifiers, result_left_quoted_identifier)
            ),
            right_quoted_identifiers_map=dict(
                zip(original_right_quoted_identifiers, result_right_quoted_identifier)
            ),
        )

        self.join_or_align_result = origin_join_or_align_res
        self._how = how
        if sort is True:
            # if sort is True, sort the result frame on the join keys
            self._sort_on_join_keys()

    def _sort_on_join_keys(self) -> None:
        """
        Sort join_or_align_result by join keys, which is done by set the preceding ordering
        columns to coalesce(left_col, right_col) for each pair of left_on and right_on.
        Update the join_or_align_result to the sorted frame.
        """
        if len(self._left_on) == 0:
            return

        join_or_align_result = self.join_or_align_result
        # Native pandas takes the union of join keys from both the frames and orders
        # the merged frame using this union of keys.
        # In Snowpark pandas API we implement this by coalescing join keys together and
        # using these coalesced columns for ordering.
        # This coalescing is only required for 'outer' join. For 'inner' and 'left' join
        # we use left join keys and for 'right' join we use right join keys.

        mapped_left_on = self.map_left_quoted_identifiers(self._left_on)
        mapped_right_on = self.map_right_quoted_identifiers(self._right_on)
        if self._how == "outer":
            ordering_column_identifiers = join_or_align_result.generate_snowflake_quoted_identifiers(
                # Use 'ordering' as prefix for generated identifiers.
                pandas_labels=[ORDERING_COLUMN_LABEL]
                * len(self._left_on),
            )
            # Generate an expression equivalent of
            # "COALESCE('left_col', 'right_col') AS 'ordering_col_<N>'"
            ordering_column_values = [
                coalesce(left_col, right_col)
                for left_col, right_col in zip(mapped_left_on, mapped_right_on)
            ]

            join_or_align_result = append_columns(
                join_or_align_result,
                ordering_column_identifiers,
                ordering_column_values,
            )
        elif self._how == "right":
            ordering_column_identifiers = mapped_right_on
        else:  # left join, inner join, left align, coalesce align
            ordering_column_identifiers = mapped_left_on

        # When sort is True using only join columns for ordering is not sufficient
        # for stable ordering because join keys may have duplicates. To provide same
        # behavior as native pandas duplicate values in join columns should preserve the
        # order from input frames.
        # So we append ordering columns from input frames to break the tie and provide
        # stable ordering as native pandas.
        ordering_columns = [
            OrderingColumn(key) for key in ordering_column_identifiers
        ] + join_or_align_result.ordering_columns

        # reset the order of the ordered_dataframe to the final order
        self.join_or_align_result = join_or_align_result.sort(ordering_columns)

    def map_left_quoted_identifiers(self, quoted_identifiers: list[str]) -> list[str]:
        """
        For a given set of quoted_identifiers from the original left frame, find the corresponding
        columns in the join or align result frame.
        """
        return self.result_column_mapper.map_left_quoted_identifiers(quoted_identifiers)

    def map_right_quoted_identifiers(self, quoted_identifiers: list[str]) -> list[str]:
        """
        For a given set of quoted_identifiers from the original right frame, find the corresponding
        columns in the join or align result frame.
        """
        return self.result_column_mapper.map_right_quoted_identifiers(
            quoted_identifiers
        )
