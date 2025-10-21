#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import typing
from collections.abc import Hashable, Sized
from enum import Enum
from typing import Any, Literal, Optional, Union

import modin.pandas as pd
import numpy as np
import pandas as native_pd
from pandas._typing import AnyArrayLike, Scalar
from pandas.api.types import is_list_like
from pandas.core.common import is_bool_indexer
from pandas.core.dtypes.common import is_bool_dtype, is_float_dtype, is_integer_dtype
from pandas.core.dtypes.inference import is_integer, is_scalar
from pandas.core.indexing import IndexingError

from snowflake.snowpark._internal.type_utils import ColumnOrName
from snowflake.snowpark.functions import (
    Column,
    coalesce,
    col,
    get,
    greatest,
    iff,
    is_null,
    lag,
    last_value,
    least,
    lit,
    max as max_,
    min as min_,
    trunc,
)
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin._internal.join_utils import (
    InheritJoinIndex,
    JoinKeyCoalesceConfig,
    align,
    align_on_index,
    join,
)
from snowflake.snowpark.modin.plugin._internal.ordered_dataframe import OrderingColumn
from snowflake.snowpark.modin.plugin._internal.snowpark_pandas_types import (
    SnowparkPandasColumn,
    SnowparkPandasType,
)
from snowflake.snowpark.modin.plugin._internal.type_utils import (
    NUMERIC_SNOWFLAKE_TYPES_TUPLE,
)
from snowflake.snowpark.modin.plugin._internal.utils import (
    DEFAULT_DATA_COLUMN_LABEL,
    ITEM_VALUE_LABEL,
    MAX_ROW_POSITION_COLUMN_LABEL,
    ROW_COUNT_COLUMN_LABEL,
    ROW_POSITION_COLUMN_LABEL,
    append_columns,
    pandas_lit,
    rindex,
)
from snowflake.snowpark.modin.plugin.compiler import snowflake_query_compiler
from snowflake.snowpark.modin.utils import MODIN_UNNAMED_SERIES_LABEL, ErrorMessage
from snowflake.snowpark.types import (
    ArrayType,
    BooleanType,
    _FractionalType,
    _IntegralType,
)
from snowflake.snowpark.window import Window

UNALIGNABLE_INDEXING_ERROR = IndexingError(
    "Unalignable boolean Series provided as indexer (index of the boolean Series and of the indexed object do not "
    "match)."
)
ILOC_INT_ONLY_INDEXING_ERROR_MESSAGE = (
    "Location based indexing can only have [integer, integer slice (START point is "
)
"INCLUDED, END point is EXCLUDED), list-like of integers, boolean array] types."

MULTIPLE_ELLIPSIS_INDEXING_ERROR_MESSAGE = "indexer may only contain one '...' entry"
TOO_FEW_INDEXERS_INDEXING_ERROR_MESSAGE = "Too few indexers"
TOO_MANY_INDEXERS_INDEXING_ERROR_MESSAGE = "Too many indexers"
CANNOT_REINDEX_ON_DUPLICATE_ERROR_MESSAGE = (
    "cannot reindex on an axis with duplicate labels"
)
SETTING_NON_UNIQUE_COLUMNS_IS_NOT_ALLOWED_ERROR_MESSAGE = (
    "Setting with non-unique columns is not allowed."
)
LAST_VALUE_COLUMN = "last_value"
LOC_SET_ITEM_SHAPE_MISMATCH_ERROR_MESSAGE = (
    "shape mismatch: the number of columns {} from the item does not match with the number of columns {} "
    "to set"
)
LOC_SET_ITEM_KV_MISMATCH_ERROR_MESSAGE = (
    "Must have equal len keys and value when setting with an iterable"
)
LOC_SET_ITEM_EMPTY_ERROR = "The length of the value/item to set is empty"
_LOC_SET_NON_TIMEDELTA_TO_TIMEDELTA_ERROR = "Snowpark pandas does not yet support assigning timedelta values to an existing column."


# Used for `first_valid_index` and `last_valid_index` Snowpark pandas APIs
class ValidIndex(Enum):
    FIRST = "first"
    LAST = "last"


def get_valid_index_values(
    frame: InternalFrame,
    first_or_last: ValidIndex,
    dummy_row_pos_mode: bool = False,
) -> "Optional[Row]":  # type: ignore # noqa
    """
    Given an InternalFrame consisting of an array of booleans, filter for True values
    and get first or last index.

    Parameters
    ----------
    frame: InternalFrame
    first_or_last: Enum specifying which valid index to return.
        Can be either ValidIndex.FIRST or ValidIndex.LAST.

    Returns
    -------
    Optional[Row]: The desired index (a Snowpark Row) if it exists, else None.
    """
    frame = frame.ensure_row_position_column(dummy_row_pos_mode)
    index_quoted_identifier = frame.index_column_snowflake_quoted_identifiers
    data_quoted_identifier = frame.data_column_snowflake_quoted_identifiers
    row_position_quoted_identifier = frame.row_position_snowflake_quoted_identifier
    ordered_dataframe = frame.ordered_dataframe.filter(
        col(data_quoted_identifier[0]) == pandas_lit(True)
    )
    # Since first_valid_index and last_valid_index return a Scalar, we are always going to eagerly compute
    if first_or_last is ValidIndex.FIRST:
        valid_index_values = (
            ordered_dataframe.select(index_quoted_identifier).limit(1).collect()
        )
    else:
        assert first_or_last is ValidIndex.LAST, "first_or_last is not ValidIndex.LAST"
        valid_index_values = (
            ordered_dataframe.sort(
                [OrderingColumn(row_position_quoted_identifier, ascending=False)]
            )
            .select(index_quoted_identifier)
            .limit(1)
            .collect()
        )

    # If no valid indices, return None
    if len(valid_index_values) == 0:
        return None
    # Else return the desired Row
    else:
        return valid_index_values[0]


def convert_snowpark_row_to_pandas_index(
    valid_index_values: "Row",  # type: ignore # noqa
    index_dtypes: list[np.dtype],
) -> Union[Scalar, tuple[Scalar]]:
    """
    Converts Snowpark Row to pandas index.

    Parameters
    ----------
    valid_index_values: Snowpark Row representing an Index
    index_dtypes: Corresponding dtypes for given Index

    Returns
    -------
    scalar or None, Tuple of scalars if MultiIndex
    """
    valid_index = valid_index_values[:-1]
    valid_index_list: list[Scalar] = []
    for level in range(len(index_dtypes)):
        # Special Case where index=None
        if not valid_index[level]:
            # If index dtype is Object, index=None
            if index_dtypes[level] == np.dtype("O"):
                valid_index_list.append(None)
            # If index dtype is not Object, index=np.nan
            else:
                valid_index_list.append(np.nan)
        else:
            valid_index_list.append(valid_index[level])
    # MultiIndex case we return a Tuple
    if len(valid_index_list) > 1:
        return tuple(valid_index_list)
    # Return a Scalar if non-MultiIndex
    return valid_index_list[0]


def validate_out_of_bound(key_max: Any, key_min: Any, axis_len: int) -> None:
    """
    Raise indexError if any of value of key is out of bound.

    For both list-like or series type, if key is a tuple containing list-like or series, e.g. key = (Series([-7.9]), ),
    or key = ([-7.9], ), in native pandas the valid range is -row_len <= value < row_len. Whereas in Snowpark pandas API
    the valid range is -row_len-1 < value < row_len.
    If key is just list-like or series, e.g. key = Series([-7.9]) or key = [-7.9] in both native pandas and Snowpark
    pandas API the valid range is -row_len-1 < value < row_len

    Parameters
    ----------
    key_max: Union[float, int, np.Numerical]
    key_min: Union[float, int, np.Numerical]
    axis_len: length of the axis we are validating.

    Raises
    ----------
    IndexError
    """
    if key_max >= axis_len or key_min <= -axis_len - 1:
        raise IndexError("positional indexers are out-of-bounds")


def get_frame_by_row_pos_frame(
    internal_frame: InternalFrame,
    key: InternalFrame,
    dummy_row_pos_mode: bool = False,
) -> InternalFrame:
    """
    Select rows from this internal_frame by row positions in the key frame
    Args:
        internal_frame: the main frame to select
        key: the frame containing row positions

    Returns:
        The selected frame
    """
    # check data type
    key_datatype = key.get_snowflake_type(
        key.data_column_snowflake_quoted_identifiers[0]
    )
    # implicitly allow float types to be compatible with pandas
    assert isinstance(
        key_datatype, NUMERIC_SNOWFLAKE_TYPES_TUPLE
    ), f"get_frame_by_row_pos_frame's key data type must be bool or int, not {key_datatype=}"

    # boolean indexer
    if isinstance(key_datatype, BooleanType):
        return _get_frame_by_row_pos_boolean_frame(
            internal_frame, key, dummy_row_pos_mode
        )

    # int indexer
    if isinstance(key_datatype, _FractionalType):
        # implicitly convert float to int using trunc, e.g., -1.1 => -1, 2.9=> 2
        key_val_id = key.data_column_snowflake_quoted_identifiers[0]
        key = key.append_column("int_value", trunc(col(key_val_id), scale=0))
        # drop the original float column from data columns
        key = InternalFrame.create(
            ordered_dataframe=key.ordered_dataframe,
            data_column_snowflake_quoted_identifiers=key.data_column_snowflake_quoted_identifiers[
                1:
            ],
            data_column_pandas_labels=key.data_column_pandas_labels[1:],
            data_column_pandas_index_names=key.data_column_pandas_index_names,
            index_column_snowflake_quoted_identifiers=key.index_column_snowflake_quoted_identifiers,
            index_column_pandas_labels=key.index_column_pandas_labels,
            data_column_types=key.cached_data_column_snowpark_pandas_types[1:],
            index_column_types=key.cached_index_column_snowpark_pandas_types,
        )
    return _get_frame_by_row_pos_int_frame(internal_frame, key, dummy_row_pos_mode)


def _get_frame_by_row_pos_boolean_frame(
    internal_frame: InternalFrame,
    key: InternalFrame,
    dummy_row_pos_mode: bool = False,
) -> InternalFrame:
    """
    Select rows using the boolean frame positional key. The two frames will be inner joined on their row position column
    and only rows with true key value will be selected.

    Args:
        internal_frame: the frame to perform positional selection
        key: the boolean frame to

    Returns:
        new frame with selected rows
    """
    internal_frame = internal_frame.ensure_row_position_column(dummy_row_pos_mode)
    key = key.ensure_row_position_column(dummy_row_pos_mode)

    # inner join internal_frame with key frame on row_position
    joined_frame, result_column_mapper = join(
        internal_frame,
        key,
        how="inner",
        left_on=[internal_frame.row_position_snowflake_quoted_identifier],
        right_on=[key.row_position_snowflake_quoted_identifier],
        dummy_row_pos_mode=dummy_row_pos_mode,
    )

    # only true value in key's data column will be selected
    filtered = joined_frame.filter(
        joined_frame.data_column_snowflake_quoted_identifiers[-1]
    )

    # Note: we still use internal_frame's pandas labels to handle multiindex correctly.
    return InternalFrame.create(
        ordered_dataframe=filtered.ordered_dataframe,
        data_column_pandas_labels=internal_frame.data_column_pandas_labels,
        data_column_pandas_index_names=internal_frame.data_column_pandas_index_names,
        data_column_snowflake_quoted_identifiers=result_column_mapper.map_left_quoted_identifiers(
            internal_frame.data_column_snowflake_quoted_identifiers
        ),
        index_column_pandas_labels=internal_frame.index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=result_column_mapper.map_left_quoted_identifiers(
            internal_frame.index_column_snowflake_quoted_identifiers
        ),
        data_column_types=internal_frame.cached_data_column_snowpark_pandas_types,
        index_column_types=internal_frame.cached_index_column_snowpark_pandas_types,
    )


def _get_frame_by_row_pos_int_frame(
    internal_frame: InternalFrame,
    key: InternalFrame,
    dummy_row_pos_mode: bool = False,
) -> InternalFrame:
    """
    Select rows using the int frame positional key. The two frames will be inner joined on the internal_frame's row
    position column and the key's data column (i.e., the positional values).

    Args:
        internal_frame: the frame to perform positional selection
        key: the boolean frame to

    Returns:
        new frame with selected rows
    """
    joined_key_frame = _get_adjusted_key_frame_by_row_pos_int_frame(
        internal_frame, key, dummy_row_pos_mode
    )

    internal_frame = internal_frame.ensure_row_position_column(dummy_row_pos_mode)

    # join the new value column of the key with the row position column of the original dataframe
    # to get the new rows. The result will be ordered by the key frame order followed by internal
    # frame order.
    joined, result_column_mapper = join(
        joined_key_frame,
        internal_frame,
        left_on=joined_key_frame.data_column_snowflake_quoted_identifiers,
        right_on=[internal_frame.row_position_snowflake_quoted_identifier],
        how="inner",
        inherit_join_index=InheritJoinIndex.FROM_RIGHT,
        dummy_row_pos_mode=dummy_row_pos_mode,
    )

    # Note:
    # 1. keep using internal_frame's index and column labels
    # 2. use the quoted identifiers from the joined result
    return InternalFrame.create(
        ordered_dataframe=joined.ordered_dataframe,
        data_column_pandas_labels=internal_frame.data_column_pandas_labels,
        data_column_pandas_index_names=internal_frame.data_column_pandas_index_names,
        data_column_snowflake_quoted_identifiers=result_column_mapper.map_right_quoted_identifiers(
            internal_frame.data_column_snowflake_quoted_identifiers
        ),
        index_column_pandas_labels=internal_frame.index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=result_column_mapper.map_right_quoted_identifiers(
            internal_frame.index_column_snowflake_quoted_identifiers
        ),
        data_column_types=internal_frame.cached_data_column_snowpark_pandas_types,
        index_column_types=internal_frame.cached_index_column_snowpark_pandas_types,
    )


def _get_adjusted_key_frame_by_row_pos_int_frame(
    internal_frame: InternalFrame,
    key: InternalFrame,
    dummy_row_pos_mode: bool = False,
) -> InternalFrame:
    """
    Return the key frame with any negative row positions adjusted by the internal frame.  For example, if the original
    frame has row_count=5 and the key frame has values [1, -1, -2, 3, -4] the resulting frame would be [1, 4, 3, 3, 1]

    Args:
        internal_frame: the frame relative row positions reference
        key: the row position frame, including potential negative positions.

    Returns:
        new frame with adjusted row positions
    """
    internal_frame = internal_frame.ensure_row_position_column(dummy_row_pos_mode)
    key = key.ensure_row_position_column(dummy_row_pos_mode)

    # The value in the key can be negative, and can be transformed into a positive value by adding the row count of the
    # internal frame. For example: if the key is pd.Series([-1, 0, 2, -2]), and the internal frame has total 4 rows,
    # the final row we should get is pd.Series([-1 + 4, 0, 2, -2 + 4]), which is pd.Series([3, 0, 2, 2]).
    # In order to achieve this, we do 1) append the total row count of internal frame as a new column to key use
    # cross join. Note this needs to be done with cross join since the total row count comes from another dataframe,
    # otherwise we can use count(*) over() if it is for the same dataframe.
    # 2) Then project a new column by adding the total row count to the origin value if it is negative.
    # First, get total row count of the internal frame.
    # create a new frame (count_frame) with a single row that contains row count of the internal frame as
    # the data column, and a row position column (0 since there is only one row after aggregation) as index column.
    # step 1: ensure the internal frame has a row count column, the identifier is de-conflicted against
    # the key frame, so there will be no conflict when join with the key.
    count_ordered_dataframe = internal_frame.ensure_row_count_column().ordered_dataframe
    row_count_snowflake_quoted_identifier = (
        count_ordered_dataframe.row_count_snowflake_quoted_identifier
    )
    # We just want the row count - its the same for every row, so we don't need to sort.
    count_ordered_dataframe = count_ordered_dataframe.limit(1, sort=False)

    # step 2: add a row position column (actually, value 0) as index column\
    count_ordered_dataframe = count_ordered_dataframe.ensure_row_position_column(
        dummy_row_pos_mode
    )

    count_frame = InternalFrame.create(
        ordered_dataframe=count_ordered_dataframe,
        data_column_pandas_labels=[ROW_COUNT_COLUMN_LABEL],
        data_column_snowflake_quoted_identifiers=[
            row_count_snowflake_quoted_identifier
        ],
        index_column_pandas_labels=[ROW_POSITION_COLUMN_LABEL],
        index_column_snowflake_quoted_identifiers=[
            count_ordered_dataframe.row_position_snowflake_quoted_identifier
        ],
        data_column_pandas_index_names=[None],
        data_column_types=[None],
        index_column_types=[None],
    )

    # cross join the count with the key to append the count column with the key frame. For example: if the
    # key frame is the following:
    #       value
    # 0     -1
    # 1     4
    # 2     2
    # 3     -2
    # and the total count of internal frame is 5, the joined result would be
    #       value       row_count       row_position
    # 0     -1          5               0
    # 1     4           5               0
    # 2     2           5               0
    # 3     -1          5               0
    # the new data column for the joined key frame would be [value, row_count]
    joined_key_frame, result_column_mapper = join(
        key,
        count_frame,
        "cross",
        inherit_join_index=InheritJoinIndex.FROM_LEFT,
        dummy_row_pos_mode=dummy_row_pos_mode,
    )

    # project a new column to create the new value column
    original_value_column = col(
        result_column_mapper.map_left_quoted_identifiers(
            key.data_column_snowflake_quoted_identifiers
        )[0]
    )
    left_row_cnt_column = col(
        result_column_mapper.map_right_quoted_identifiers(
            count_frame.data_column_snowflake_quoted_identifiers
        )[0]
    )
    joined_key_frame = joined_key_frame.project_columns(
        [joined_key_frame.data_column_pandas_labels[0]],
        [
            iff(
                original_value_column < 0,
                original_value_column + left_row_cnt_column,
                original_value_column,
            )
        ],
    )

    return joined_key_frame


def get_frame_by_row_pos_slice_frame(
    internal_frame: InternalFrame,
    key: slice,
    dummy_row_pos_mode: bool = False,
) -> InternalFrame:
    """
    Select rows using the slice frame positional key. A filter will be applied on the frame based on the slice.

    Args:
        internal_frame: the frame to perform positional selection
        key: slice

    Returns:
        new frame with selected rows
    """
    if key == slice(None):
        return internal_frame
    if key.step is not None and key.step == 0:
        raise ValueError("slice step cannot be zero.")

    # Row position column required for left and right bound comparison.
    frame = internal_frame.ensure_row_position_column(dummy_row_pos_mode)
    row_pos_col = col(frame.row_position_snowflake_quoted_identifier)

    def get_count_col() -> Column:
        """
        The purpose of this helper function is to avoid creating row count column if it is not needed. Creating row
        count column will cause the query to scan the whole table.
        """
        nonlocal frame
        frame = frame.ensure_row_count_column()
        return col(frame.row_count_snowflake_quoted_identifier)

    ordering_columns = internal_frame.ordering_columns
    start, stop, step = key.start, key.stop, key.step

    def make_positive(val: int) -> Column:
        # Helper to turn negative start and stop values to positive values. Example: -1 --> num_rows - 1.
        return val + get_count_col() if val < 0 else pandas_lit(val)

    step = 1 if step is None else step

    # if `limit_n` is not None, use limit in the query to narrow down the search.
    limit_n = None
    if step < 0:
        # Switch start and stop; convert given slice key into a similar slice key with positive step.
        start, stop = stop, start
    if (stop is not None and stop >= 0) and (start is None or start >= 0):
        # set limit_n = abs(stop - start) // step when we exactly know how many rows it is going to return.
        limit_n = stop
        if start is not None:
            limit_n = abs(limit_n - start)
        if step < 0 and start is None:
            limit_n += 1  # e.g., df.iloc[1:None:-1] return 2 rows
        limit_n = 1 + (limit_n - 1) // abs(step)

    if step < 0:
        # Set ascending to False if step is negative.
        ordering_columns = [
            OrderingColumn(
                internal_frame.row_position_snowflake_quoted_identifier, ascending=False
            )
        ]
        start = 0 if start is None else make_positive(start) + 1
        stop = get_count_col() - 1 if stop is None else make_positive(stop)
    else:  # step > 0
        # Assign default values or convert to positive values.
        start = pandas_lit(0) if start is None else make_positive(start)
        stop = (get_count_col() if stop is None else make_positive(stop)) - 1

    # Both start and stop are inclusive.
    left_bound_filter = row_pos_col >= start
    right_bound_filter = row_pos_col <= stop

    if step > 1:
        # start can be negative --> make the lower-bound 0.
        step_bound_filter = (
            (row_pos_col - greatest(pandas_lit(0), least(start, get_count_col() - 1)))
            % pandas_lit(step)
        ) == 0
    elif step < -1:
        # Similarly, stop can be too large --> make the upper-bound the max row number.
        step_bound_filter = (
            (greatest(pandas_lit(0), least(stop, get_count_col() - 1)) - row_pos_col)
            % pandas_lit(abs(step))
        ) == 0
    else:  # abs(step) == 1, so all values in range are included.
        step_bound_filter = pandas_lit(True)

    filter_cond = left_bound_filter & right_bound_filter & step_bound_filter
    ordered_dataframe = frame.ordered_dataframe.filter(filter_cond)
    if limit_n is not None:
        # adding limit here could improve performance when the filter is applied directly on a table, because it can
        # exit the scanning of table once enough record is found
        ordered_dataframe = ordered_dataframe.limit(limit_n, sort=False)
    ordered_dataframe = ordered_dataframe.sort(ordering_columns)
    return InternalFrame.create(
        ordered_dataframe=ordered_dataframe,
        data_column_pandas_labels=internal_frame.data_column_pandas_labels,
        data_column_pandas_index_names=internal_frame.data_column_pandas_index_names,
        data_column_snowflake_quoted_identifiers=internal_frame.data_column_snowflake_quoted_identifiers,
        index_column_pandas_labels=internal_frame.index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=internal_frame.index_column_snowflake_quoted_identifiers,
        data_column_types=internal_frame.cached_data_column_snowpark_pandas_types,
        index_column_types=internal_frame.cached_index_column_snowpark_pandas_types,
    )


class LocSetColInfo(typing.NamedTuple):
    """Loc setitem information context for columns"""

    # positions for existing columns in the dataframe that needs to be set, can have duplicates and order matters
    existing_column_positions: list[int]

    # number of times each column in the original frame needs to be duplicated in the result frame
    existing_column_duplication_cardinality_map: dict[int, int]

    # pandas labels for new columns that needs to be inserted in the result frame
    new_column_pandas_labels: list[Hashable]

    # pandas labels for the columns to set
    column_pandas_labels: list[Hashable]


def _extract_loc_set_col_info(
    internal_frame: InternalFrame,
    columns: Union[
        "snowflake_query_compiler.SnowflakeQueryCompiler",
        Scalar,
        tuple,
        slice,
        list,
        "pd.Index",
        np.ndarray,
    ],
) -> LocSetColInfo:
    """
    Extract the column information needed for loc set

    Args:
        internal_frame: the main frame to perform loc set
        columns: the column locator
    Returns:
        A LocSetColumnInfo named tuple
    """
    # Note: enlargement can happen in loc set especially when ``index`` and ``columns`` are scalar. Furthermore,
    # enlargement can happen when the columns type is any array like and data type is not boolean. Specifically,
    # Two types of enlargement cases on columns are possible:
    #   1. duplicate existing columns
    #   2. append new columns
    # E.g., existing columns are ["C", "A", "B"] and the columns are ["C", "B", "E", 1, "B", "X", "C", 2, "C" ], then
    # the result columns will be ["C", "C", "C", "A", "B", "B", "E", 1, "X", 2]
    # Here we track those two cases using the following two variables:
    existing_column_duplication_cardinality_map: dict[int, int] = {}
    """e.g., {1: 2, 2:1}, i.e., "A" needs to duplicate twice and "C" needs to be duplicated once"""
    new_column_pandas_labels = []
    """e.g., ["E", 1, "X", 2]"""

    # column enlargement may happen when the ``columns`` type is not slice and not boolean indexer.
    enlargement_may_happen = False
    if is_scalar(columns):
        columns = [columns]
    if not isinstance(columns, slice):
        from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
            SnowflakeQueryCompiler,
        )

        if isinstance(columns, SnowflakeQueryCompiler):
            if not is_bool_dtype(columns.dtypes[0]):
                columns = columns.to_pandas().iloc[:, 0].to_list()
                enlargement_may_happen = True
        elif not is_bool_indexer(columns):
            enlargement_may_happen = True

    original_columns = columns
    if enlargement_may_happen:
        frame_data_columns = internal_frame.data_columns_index
        # This list contains the columns after loc set
        union_data_columns = frame_data_columns.union(columns)
        # Check length of the union columns when frame data columns have duplicates. Note: pandas raise ValueError:
        # "cannot reindex on an axis with duplicate labels" when the length mismatches:
        # E.g.,
        #   frame_data_columns = ["A", "A", "B"], columns = ["C"]
        #   frame_data_columns = ["A", "A", "B"], columns = ["B", "B"]
        #   frame_data_columns = ["A", "A", "B"], columns = ["A", "A", "A"]
        # Only when frame data has no duplicate or the length matches, pandas treats as valid cases:
        # E.g.,
        #   frame_data_columns = ["A", "B"], columns = ["A"]
        #   frame_data_columns = ["A", "B"], columns = ["C"]
        #   frame_data_columns = ["A", "A", "B"], columns = ["A", "B", "A"]
        if frame_data_columns.has_duplicates and len(frame_data_columns) != len(
            union_data_columns
        ):
            raise ValueError(CANNOT_REINDEX_ON_DUPLICATE_ERROR_MESSAGE)
        # split labels into existing and new
        new_column_pandas_labels = [
            label for label in columns if label not in frame_data_columns
        ]
        columns = [label for label in columns if label in frame_data_columns]
        before = frame_data_columns.value_counts()
        after = union_data_columns.value_counts()
        frame_data_col_labels = frame_data_columns.tolist()
        for label in after.index:
            if label in frame_data_columns:
                cnt_after = after[label]
                cnt_before = before[label]
                assert cnt_after >= cnt_before
                if cnt_before < cnt_after:
                    col_pos = frame_data_col_labels.index(label)
                    existing_column_duplication_cardinality_map[col_pos] = (
                        cnt_after - cnt_before
                    )

    existing_column_positions = get_valid_col_positions_from_col_labels(
        internal_frame, columns
    )

    # Generate the list of labels need to be set:
    # When column enlargement may happen, get the list of pandas labels corresponding column key; otherwise, i.e., when
    # it is slice or boolean indexer, use the corresponding column position to get the labels
    column_pandas_labels = (
        original_columns
        if enlargement_may_happen
        else [
            internal_frame.data_column_pandas_labels[pos]
            for pos in existing_column_positions
        ]
    )

    return LocSetColInfo(
        existing_column_positions,
        existing_column_duplication_cardinality_map,
        new_column_pandas_labels,
        column_pandas_labels,
    )


def get_valid_col_positions_from_col_labels(
    internal_frame: InternalFrame,
    col_loc: Union[
        "snowflake_query_compiler.SnowflakeQueryCompiler",
        Scalar,
        tuple,
        slice,
        list,
        "pd.Index",
        np.ndarray,
    ],
) -> list[int]:
    """
    The helper function to get valid column positions from labels. Out of bound labels will be ignored.

    Args:
        internal_frame: the main frame
        col_loc: the column labels in different types

    Raises:
        KeyError: when values are missing from `internal_frame` columns

    Returns:
        Column position list
    """
    from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
        SnowflakeQueryCompiler,
    )

    is_column_multiindex = internal_frame.is_multiindex(axis=1)
    columns = internal_frame.data_columns_index
    num_columns = len(columns)
    all_col_pos = list(range(num_columns))

    if is_column_multiindex and isinstance(col_loc, tuple) and len(col_loc) == 0:
        # multiindex column treats empty tuple as select all columns
        return all_col_pos

    if isinstance(col_loc, slice):
        if col_loc == slice(None):
            return all_col_pos
        # use native pandas' way to convert label slice indexer to position slice
        return all_col_pos[
            columns.slice_indexer(col_loc.start, col_loc.stop, col_loc.step)
        ]

    if isinstance(col_loc, SnowflakeQueryCompiler):
        if is_bool_dtype(col_loc.dtypes.iloc[0]):
            # for boolean series, we first get the index where value is True
            # E.g., assuming col_loc = pd.Series([True, False, True], index = ["a", "b", "c"])
            # we use filter below to get its indices with value true which are ["a", "c"]
            col_loc = SnowflakeQueryCompiler(
                col_loc._modin_frame.filter(
                    col(
                        col_loc._modin_frame.data_column_snowflake_quoted_identifiers[0]
                    )
                )
            )
            col_loc = col_loc.index
            if isinstance(col_loc, pd.Index):
                col_loc = col_loc.to_pandas()
            # get the position of the selected labels
            return [pos for pos, label in enumerate(columns) if label in col_loc]
        else:
            # for other series, convert to list and process later
            col_loc = col_loc.to_pandas().iloc[:, 0].to_list()
    elif is_bool_indexer(col_loc):
        # return the selected column positions. Note Snowpark pandas allows length mismatch bool indexer.
        return [pos for pos, val in enumerate(col_loc) if val and pos < len(columns)]

    if is_column_multiindex:
        # First, convert col_loc to a list of locators
        if is_scalar(col_loc) or isinstance(col_loc, tuple):
            col_loc = [col_loc]

        # Second, truncate oversize tuple, i.e., nlevels > columns.nlevels
        if is_list_like(col_loc) and any([is_list_like(item) for item in col_loc]):
            # consistent with row key handling, i.e., list of tuple keys perform exact match. Note oversize tuple will
            # be truncated
            col_loc = [
                item[: columns.nlevels] if len(item) >= columns.nlevels else item
                for item in col_loc
            ]

        # Last, convert columns locators into indexer, i.e., the position indices of selected columns. For example,
        # assuming the index is
        #
        # MultiIndex([('bar', 'one'),
        #             ('bar', 'two'),
        #             ('baz', 'one'),
        #             ('baz', 'two'),
        #             ('foo', 'one'),
        #             ('foo', 'two'),
        #             ('quz', 'one'),
        #             ('quz', 'two')],
        #            names=['first', 'second'])
        #
        # col_loc can be a list of column locators ["bar", ("bar", "one"), (slice("bar", "foo"), "one")]. For each
        # locator, we will generate the selected position indices and then append them together.
        # for "bar", the position indices will be [0,1]
        # for ("bar", "one"), the indices will be [0]
        # for (slice("bar", "foo"), "one"), the indices for level 0 will be [0, 1, 2, 3, 4, 5] and the indices for
        # level1 will be [0, 2, 4, 6], and then the intersected indices will be [0, 2, 4] which is the result indices
        # The final result for the list of locators will be [0, 1, 0, 0, 2, 4]
        indexer = []
        for col_loc_item in col_loc:
            # for each locator, we perform prefix matching for multiindex
            if is_scalar(col_loc_item):
                col_loc_item = (col_loc_item,)
            # May throw KeyError if any labels are not found in columns
            item_indexer = columns.get_locs(col_loc_item)
            indexer.extend(item_indexer)
    else:
        if is_scalar(col_loc):
            if col_loc not in columns:
                raise KeyError(f"{col_loc}")
            col_loc = [col_loc]
        elif isinstance(col_loc, tuple):
            col_loc = [col_loc] if col_loc in columns else list(col_loc)
        # Throw a KeyError in case there are any missing column labels
        if len(col_loc) > 0 and all(label not in columns for label in col_loc):
            raise KeyError(f"None of {native_pd.Index(col_loc)} are in the [columns]")
        elif any(label not in columns for label in col_loc):
            raise KeyError(f"{[k for k in col_loc if k not in columns]} not in index")
        # Convert col_loc to Index with object dtype since _get_indexer_strict() converts None values in lists to
        # np.nan. This does not filter columns with label None and errors. Not using np.array(col_loc) as the key since
        # np.array(["A", 12]) turns into array(['A', '12'].
        col_loc = native_pd.Index(
            [label for label in col_loc if label in columns], dtype=object
        )

        # `Index._get_indexer_strict` returns position index from label index
        _, indexer = columns._get_indexer_strict(col_loc, "columns")
    return list(indexer)


def get_frame_by_col_label(
    internal_frame: InternalFrame,
    col_loc: Union[
        "snowflake_query_compiler.SnowflakeQueryCompiler",
        Scalar,
        tuple,
        slice,
        list,
        "pd.Index",
        np.ndarray,
    ],
) -> InternalFrame:
    """
    The util method first finds all column positions based on the column labels, and then call get_frame_by_col_pos to
    return a frame with the selected columns

    Args:
        internal_frame: main frame to select columns
        col_loc: column label locator

    Returns:
        New frame with selected columns
    """
    origin_col_loc = col_loc

    is_column_multiindex = internal_frame.is_multiindex(axis=1)

    if is_column_multiindex and isinstance(col_loc, tuple) and len(col_loc) == 0:
        # multiindex column treats empty tuple as select all columns
        return internal_frame

    if isinstance(col_loc, slice) and col_loc == slice(None):
        return internal_frame

    col_pos = get_valid_col_positions_from_col_labels(internal_frame, col_loc)
    result = get_frame_by_col_pos(internal_frame, col_pos)

    # TODO SNOW-962197 Support df/series.droplevel and reuse it here
    multiindex_col_nlevels_to_drop = 0
    if is_column_multiindex:
        nlevels = internal_frame.num_index_levels(axis=1)
        if is_scalar(origin_col_loc):
            multiindex_col_nlevels_to_drop = 1
        elif isinstance(origin_col_loc, tuple):
            multiindex_col_nlevels_to_drop = len(origin_col_loc)
        if nlevels == multiindex_col_nlevels_to_drop:
            multiindex_col_nlevels_to_drop = 0
    if multiindex_col_nlevels_to_drop:
        # Note: only pandas labels need to be changed, no need to change snowflake_quoted_identifiers
        new_data_column_pandas_labels = result.data_columns_index.droplevel(
            list(range(multiindex_col_nlevels_to_drop))
        )
        new_data_column_pandas_index_names = result.data_column_pandas_index_names[
            multiindex_col_nlevels_to_drop:
        ]

        result = InternalFrame.create(
            ordered_dataframe=result.ordered_dataframe,
            data_column_pandas_labels=new_data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers=result.data_column_snowflake_quoted_identifiers,
            data_column_pandas_index_names=new_data_column_pandas_index_names,
            index_column_pandas_labels=result.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=result.index_column_snowflake_quoted_identifiers,
            data_column_types=result.cached_data_column_snowpark_pandas_types,
            index_column_types=result.cached_index_column_snowpark_pandas_types,
        )
    return result


def get_valid_col_pos_list_from_columns(
    columns: Union[
        "snowflake_query_compiler.SnowflakeQueryCompiler", slice, int, AnyArrayLike
    ],
    num_frame_data_columns: int,
) -> list[int]:
    """
    Helper to get list of column positions based on a Series containing column positions.  This handles if the
    key Series is a bool indexer, contains floats, negative positions (within -base_frame_len <= key <= base_frame_len.)
    Note that for columns as SnowflakeQueryCompiler this will cause a query to fetch the column positions.

    Parameters
    ----------
    columns: 0-based column positions in one of the following forms:
             - A SnowflakeQueryCompiler expression that produces a scalar series.
             - A slice.
             - An index value. Negative values are handled the same as Python arrays.
             - A list of index values.
             - A list of Booleans. These values are logically zipped with columns, and the columns matching up with True are selected.
    num_frame_data_columns: Length of the base frame the positions will index into.

    Return
    ------
    Returns list of column positions.
    """
    if isinstance(columns, slice):
        pos_list = list(range(*columns.indices(num_frame_data_columns)))
    elif isinstance(columns, int):
        pos_list = [columns]
    else:
        if isinstance(columns, snowflake_query_compiler.SnowflakeQueryCompiler):
            # Note we don't bother checking for empty to avoid a round-trip query, since we'll do a query
            # any ways which will return no rows if it is empty.
            pos_array = columns.to_numpy().flatten()
        elif isinstance(columns, list) or is_list_like(columns):
            pos_array = np.array(columns)
        else:
            raise ValueError(f"Unexpected argument type {columns=}")  # pragma: no cover

        if is_bool_dtype(pos_array.dtype):
            pos_list = [index for index, val in enumerate(pos_array) if val]

        # convert float like keys to integers
        elif not is_integer_dtype(pos_array.dtype):
            assert is_float_dtype(
                pos_array.dtype
            ), "list-like key must be list of int or float"
            pos_list = pos_array.astype(int)
        else:
            pos_list = pos_array

    # Unlike native pandas, we do not raise `IndexError()` for invalid indices. Instead, we silently ignore them.
    mapped_negative_indices = [
        num_frame_data_columns + i if i < 0 else i for i in pos_list
    ]
    valid_indices = [
        i for i in mapped_negative_indices if 0 <= i < num_frame_data_columns
    ]
    return valid_indices


def get_frame_by_col_pos(
    internal_frame: InternalFrame,
    columns: Union[
        "snowflake_query_compiler.SnowflakeQueryCompiler",
        slice,
        int,
        list,
        AnyArrayLike,
    ],
) -> InternalFrame:
    """
    Select columns from `internal_frame` by column positions in the `columns` frame.
    Args:
        internal_frame: the source frame.
        columns: 0-based column positions in one of the following forms:
                 - A SnowflakeQueryCompiler expression that produces a scalar series.
                 - A slice.
                 - An index value. Negative values are handled the same as Python arrays.
                 - A list of index values.
                 - A list of Booleans. These values are logically zipped with columns, and the columns matching up with
                   True are selected.
    Returns:
        Source frame with only the specified columns.
    """
    num_frame_columns = len(internal_frame.data_column_pandas_labels)
    valid_indices = get_valid_col_pos_list_from_columns(columns, num_frame_columns)

    if valid_indices == list(range(num_frame_columns)):
        return internal_frame

    frame_data_column_pandas_labels_list: list[
        Hashable
    ] = internal_frame.data_column_pandas_labels
    frame_data_column_quoted_identifiers_list: list[
        str
    ] = internal_frame.data_column_snowflake_quoted_identifiers
    data_column_pandas_labels: list[Hashable] = [
        frame_data_column_pandas_labels_list[col_index] for col_index in valid_indices
    ]

    # Get the Snowpark columns that are selected, for example, if the internal frame
    # has the following label_to_snowflake_quoted_identifier pairs for data columns:
    #    ('A', '"A"'), ('B', '"B"'), ('C', '"C"'), ('D', '"D"')
    # and the valid indices for selected columns are [1, 3], the selected Snowpark columns
    # are ['"B"', '"D"'].
    # Note that the selected columns can have duplicates, for example, [1, 1, 3]. Under
    # this case, we will also duplicate the Snowpark columns with an alias name, and the
    # selected Snowpark columns for [1, 1, 3] are ['"B"', col('"B"').as('"B_A03f"'), '"D"'].
    selected_columns: list[ColumnOrName] = []
    # the snowflake quoted identifiers for the selected Snowpark columns
    selected_columns_quoted_identifiers: list[str] = []
    selected_columns_types = []

    for col_index in valid_indices:
        snowflake_quoted_identifier = frame_data_column_quoted_identifiers_list[
            col_index
        ]

        selected_columns_types.append(
            internal_frame.snowflake_quoted_identifier_to_snowpark_pandas_type[
                snowflake_quoted_identifier
            ]
        )

        pandas_label = frame_data_column_pandas_labels_list[col_index]
        if snowflake_quoted_identifier in selected_columns_quoted_identifiers:
            # if the current column has already been selected, duplicate the column with
            # an alias name
            new_identifier = (
                internal_frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                    pandas_labels=[pandas_label],
                    excluded=selected_columns_quoted_identifiers,
                )[0]
            )
            selected_columns.append(
                col(snowflake_quoted_identifier).alias(new_identifier)
            )
            selected_columns_quoted_identifiers.append(new_identifier)
        else:
            selected_columns.append(snowflake_quoted_identifier)
            selected_columns_quoted_identifiers.append(snowflake_quoted_identifier)

    ordered_dataframe = internal_frame.ordered_dataframe.select(
        internal_frame.index_column_snowflake_quoted_identifiers + selected_columns
    )
    return InternalFrame.create(
        ordered_dataframe=ordered_dataframe,
        data_column_pandas_labels=data_column_pandas_labels,
        data_column_pandas_index_names=internal_frame.data_column_pandas_index_names,
        data_column_snowflake_quoted_identifiers=selected_columns_quoted_identifiers,
        index_column_pandas_labels=internal_frame.index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=internal_frame.index_column_snowflake_quoted_identifiers,
        data_column_types=selected_columns_types,
        index_column_types=internal_frame.cached_index_column_snowpark_pandas_types,
    )


def get_index_frame_by_row_label_slice(
    internal_frame: InternalFrame,
    key: slice,
) -> InternalFrame:
    """
    Returns a frame containing the selected slice key row labels as resulting data columns.

    Args:
        internal_frame: the main frame
        key: the key containing the labels to be selected as a slice

    Returns:
        The frame with selected rows
    """
    row_frame = _get_frame_by_row_label_slice(internal_frame, key)
    return row_frame.project_columns(
        [DEFAULT_DATA_COLUMN_LABEL] * len(internal_frame.index_column_pandas_labels),
        [col(id) for id in row_frame.index_column_snowflake_quoted_identifiers],
    )


def get_frame_by_row_label(
    internal_frame: InternalFrame,
    key: Union[InternalFrame, slice, tuple],
    dummy_row_pos_mode: bool = False,
) -> InternalFrame:
    """
    Select rows by labels in the key.
    Args:
        internal_frame: the main frame
        key: the key containing the labels to be selected

    Returns:
        The frame with selected rows
    """
    if isinstance(key, slice):
        return _get_frame_by_row_label_slice(internal_frame, key)
    elif isinstance(key, tuple):
        # The preprocessing makes sure the tuple key is only presented here when it is a multiindex lookup, i.e., the
        # row index is multiindex and the row key is a scalar or tuple (Note scalar has been preprocessed as a single
        # value tuple already). A prefix match will be handled here.
        return _get_frame_by_row_multiindex_label_tuple(internal_frame, key)

    assert isinstance(
        key, InternalFrame
    ), f"frontend should convert key to the supported types but got {type(key)}"

    # check data type
    key_datatype = key.get_snowflake_type(
        key.data_column_snowflake_quoted_identifiers[0]
    )

    # boolean indexer
    if isinstance(key_datatype, BooleanType):
        return _get_frame_by_row_label_boolean_frame(
            internal_frame, key, dummy_row_pos_mode
        )

    return _get_frame_by_row_label_non_boolean_frame(
        internal_frame, key, dummy_row_pos_mode
    )


def _get_frame_by_row_multiindex_label_tuple(
    internal_frame: InternalFrame,
    key: tuple,
) -> InternalFrame:
    """
    Get multiindex frame selected by the tuple label using prefix match. Note that the index levels used in this match
    will be dropped.

    Args:
        internal_frame: the multiindex frame
        key: tuple key

    Returns:
        Selected rows using prefix match
    """
    nlevels = internal_frame.num_index_levels(axis=0)
    key_len = len(key)
    if key_len > nlevels:
        raise IndexingError(TOO_MANY_INDEXERS_INDEXING_ERROR_MESSAGE)

    if key_len == 0:
        # multiindex row treats empty tuple as select all rows
        return internal_frame

    # prefix match: only matching levels existing in the key
    filtered_frame = internal_frame
    for i in range(key_len):
        if isinstance(key[i], slice):
            filtered_frame = _get_frame_by_row_label_slice(
                filtered_frame, key[i], key_level=i
            )
        else:
            filter_expr = col(
                filtered_frame.index_column_snowflake_quoted_identifiers[i]
            ).equal_null(key[i])
            filtered_frame = filtered_frame.filter(filter_expr)

    # TODO SNOW-962197 Support df/series.droplevel and reuse it here
    # Check if levels needs to be dropped, e.g., if a 3 level multiindex (state, county, city) is selected by a two
    # level tuple (state, county), the result frame will drop the first two index state and county and only city left.
    # When key_len == nlevels, pandas does not drop any columns
    levels_to_drop = (
        0
        if key_len == nlevels or (key_len == 1 and isinstance(key[0], slice))
        else key_len
    )

    new_index_column_pandas_labels = filtered_frame.index_column_pandas_labels[
        levels_to_drop:
    ]
    new_index_column_snowflake_quoted_identifiers = (
        filtered_frame.index_column_snowflake_quoted_identifiers[levels_to_drop:]
    )
    new_index_types = filtered_frame.cached_index_column_snowpark_pandas_types[
        levels_to_drop:
    ]

    return InternalFrame.create(
        ordered_dataframe=filtered_frame.ordered_dataframe,
        data_column_pandas_labels=filtered_frame.data_column_pandas_labels,
        data_column_snowflake_quoted_identifiers=filtered_frame.data_column_snowflake_quoted_identifiers,
        data_column_pandas_index_names=filtered_frame.data_column_pandas_index_names,
        index_column_pandas_labels=new_index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=new_index_column_snowflake_quoted_identifiers,
        data_column_types=filtered_frame.cached_data_column_snowpark_pandas_types,
        index_column_types=new_index_types,
    )


def _get_frame_by_row_label_slice(
    internal_frame: InternalFrame,
    key: slice,
    key_level: Optional[int] = None,
    dummy_row_pos_mode: bool = False,
) -> InternalFrame:
    """
    Select rows with slice key, e.g., slice(start, stop, step).
    - If both the start and stop labels are present in the index, then rows between the two (including them) are
      returned.
    - If either the start or stop is absent, but the index is sorted (either monotonic increasing or decreasing), then
      the slicing will still work as expected, by selecting labels which rank between the two.
    - However, if at least one of the two is absent and the index is not sorted, then rows in between will be returned
      which is different from native pandas where an error will be raised.
    - Also, if the index has duplicate labels and either the start or the stop label is duplicated, we will include all
      duplicates while native pandas will raise an error.
    - For multiindex, slice use prefix match to select rows, e.g., if the start is a tuple with 1 item and stop is a
      tuple with 2 items, then the left bound will be the row with first level matches with the start and the right
      bound will be the row with both first and second level match.
      For example,
          df:
                        c1	c2
          first	second
          bar	one	    0	2
                two	    1	2
          baz	one	    2	2
                two	    3	2
          foo	one	    4	2
                two	    5	2
          qux	one	    6	2
                two	    7	2

          df_mi[slice(('foo',), ('qux', 'one'))]:
                        c1	c2
          first	second
          foo	one	    4	2
                two	    5	2
          qux	one	    6	2

    Args:
        internal_frame: the main frame
        key: the slice key
        key_level: the level of the key if specified

    Returns:
        Frame with selected rows
    """
    if key == slice(None):
        return internal_frame
    if key.step is not None:
        if not is_integer(key.step):
            raise TypeError("slice step must be integer.")
        if key.step == 0:
            raise ValueError("slice step cannot be zero.")

    # Use this helper method to convert all slice start and stop to tuple, so we have single code path for both single
    # index and multiindex
    def to_tuple(slice_val: Any) -> Optional[tuple]:
        if slice_val is None:
            return slice_val
        if not isinstance(slice_val, tuple):
            slice_val = (slice_val,)
        slice_val_len = len(slice_val)
        if slice_val_len > internal_frame.num_index_levels(axis=0):
            raise IndexingError(TOO_MANY_INDEXERS_INDEXING_ERROR_MESSAGE)
        return slice_val

    start, stop, step = to_tuple(key.start), to_tuple(key.stop), key.step
    frame = internal_frame.ensure_row_position_column(dummy_row_pos_mode)
    row_pos_col = col(frame.row_position_snowflake_quoted_identifier)

    if key_level is not None:
        assert (
            (not isinstance(start, tuple) or len(start) == 1)
            and (not isinstance(stop, tuple) or len(stop) == 1)
            and internal_frame.num_index_levels(axis=0) > 1
        ), "key_level is only used where key does not contain multiple levels and internal_frame has multiindex"
        index_cols = [
            col(id) for id in frame.get_snowflake_identifiers_for_levels([key_level])
        ]
        nlevels = 1
    else:
        # the number of levels matters in both start and stop of the key , e.g., if start has length 1 and stop has
        # length 2, the nlevels will be 2 which means it will include the first two level index columns to generate the
        # rest helper columns which can be shared to build the left and right bound. In this case, the left bound only
        # need to check the first level but the right bound need to check the first two levels
        nlevels = max(
            1 if start is None else len(start), 1 if stop is None else len(stop)
        )

        index_cols = [
            col(id)
            for id in frame.get_snowflake_identifiers_for_levels(list(range(nlevels)))
        ]

    # create a lag_index column, so we can compare current index vs. index in previous row, e.g.,
    # +-------+-----------+
    # | index | lag_index |
    # +-------+-----------+
    # | "a"   | null      |
    # +-------+-----------+
    # | "b"   | "a"       |
    # +-------+-----------+
    # | "c"   | "b"       |
    # +-------+-----------+
    for i, index_col in enumerate(index_cols):
        frame = frame.append_column(
            f"lag_index_{i}", lag(index_col).over(Window.order_by(row_pos_col))
        )
    lag_index_cols = [
        col(id) for id in frame.data_column_snowflake_quoted_identifiers[-nlevels:]
    ]
    # create a last row position column which will be used to check step
    frame = frame.append_column("last_row_pos", max_(row_pos_col).over())

    # The helper method to generate prefix match expression
    def prefix_match_compare_ops(
        lhs: list[Column], rhs: Union[tuple, list[Column]], ops: Literal[">", "<", "=="]
    ) -> Column:
        if isinstance(rhs, tuple):
            rhs = [pandas_lit(item) for item in rhs]
        expr = None
        if ops == "==":
            # the condition to find the label is present, so all levels need to equal
            for i, val in enumerate(rhs):
                eq = lhs[i].equal_null(val)
                expr = eq if expr is None else expr & eq
        else:
            # the condition to find the index are larger or less than the bound. Taking a two level prefix as an
            # example, either the first level index needs to larger or less than the first level of the bound, or the
            # first index equals and the second level index needs to larger or less than the second level of the bound
            for i, val in enumerate(rhs):
                level_match = lhs[i] > val if ops == ">" else lhs[i] < val
                if i == 0:
                    expr = level_match
                else:
                    temp = None
                    for k in range(i):
                        eq = lhs[k].equal_null(rhs[k])
                        temp = eq if temp is None else temp & eq
                    expr = expr | (temp & level_match)
        return expr

    # create is_monotonic_decreasing column to check whether it is in descending order
    is_monotonic_decreasing = prefix_match_compare_ops(index_cols, lag_index_cols, "<")
    frame = frame.append_column(
        "is_monotonic_decreasing",
        coalesce(min_(is_monotonic_decreasing).over(), pandas_lit(False)),
    )
    last_row_pos_col = col(frame.data_column_snowflake_quoted_identifiers[-2])
    is_monotonic_decreasing_col = col(
        frame.data_column_snowflake_quoted_identifiers[-1]
    )

    reverse_order = key.step is not None and key.step < 0
    if reverse_order:
        # also switch start and stop
        start, stop = stop, start

    # the helper function to generate left or right bound column
    def generate_bound_column(
        _frame: InternalFrame, bound_type: Literal["left", "right"], bound: tuple
    ) -> tuple[InternalFrame, Column]:
        """
        Taking left bound L as an example, the new left bound column will be:
            coalesce(
                min(iff(col("index") == L, col(row_pos), None)).over(), -- case 1
                min(iff(col("is_monotonic_decreasing") and col("index") < L, col(row_pos), None)).over(), -- case 2
                min(iff(not col("is_monotonic_decreasing") and col("index") > L, col(row_pos), None)).over(), -- case 3
            )
        For example:
        - if the left bound 1 is present in the index [0,1,2,3], then the new left bound column will match case 1 and
          will contain all values as row_pos = 1
        - if the left bound 5 is absent and the index is monotonic decreasing [3,2,1,0], then the new left bound will
          match the case 2 and the values will be row_pos = 0
        - otherwise, e.g., left bound -1 and the index is [0,1,2,3] or [0,2,3,1], the new left bound will be row_pos = 0
        """
        if bound_type == "left":
            agg_method = min_
            monotonic_decreasing_bound = is_monotonic_decreasing_col & (
                prefix_match_compare_ops(index_cols, bound, "<")
            )
            non_monotonic_decreasing_bound = ~is_monotonic_decreasing_col & (
                prefix_match_compare_ops(index_cols, bound, ">")
            )
        else:
            agg_method = max_
            monotonic_decreasing_bound = is_monotonic_decreasing_col & (
                prefix_match_compare_ops(index_cols, bound, ">")
            )
            non_monotonic_decreasing_bound = ~is_monotonic_decreasing_col & (
                prefix_match_compare_ops(index_cols, bound, "<")
            )

        bound_present = prefix_match_compare_ops(index_cols, bound, "==")
        bound_col = coalesce(
            # if the bound presents in the index, then include all between the bounds
            agg_method(iff(bound_present, row_pos_col, pandas_lit(None))).over(),
            # else if the index is monotonic decreasing, then include all smaller than the left and larger than the
            # right
            agg_method(
                iff(monotonic_decreasing_bound, row_pos_col, pandas_lit(None))
            ).over(),
            # otherwise, include all larger than the left and smaller than the right
            agg_method(
                iff(non_monotonic_decreasing_bound, row_pos_col, pandas_lit(None))
            ).over(),
        )
        _frame = _frame.append_column(f"{bound_type}_bound", bound_col)
        return _frame, col(_frame.data_column_snowflake_quoted_identifiers[-1])

    if start is not None:
        frame, left_bound_col = generate_bound_column(frame, "left", start)
        left_bound_filter = row_pos_col >= left_bound_col
    else:
        # when start is None, no left bound exists
        # so the filter is true and the column is always the first row 0
        left_bound_filter = pandas_lit(True)
        left_bound_col = pandas_lit(0)

    if stop is not None:
        frame, right_bound_col = generate_bound_column(frame, "right", stop)
        right_bound_filter = row_pos_col <= right_bound_col
    else:
        # when stop is None, no right bound exists
        # so the filter is true and the column is the last row position
        right_bound_filter = pandas_lit(True)
        right_bound_col = last_row_pos_col

    step_bound_filter = pandas_lit(True)
    if step is not None and abs(step) != 1:
        if step > 0:
            step_bound_filter = (
                (row_pos_col - left_bound_col) % pandas_lit(abs(step))
            ) == 0
        else:
            step_bound_filter = (
                (right_bound_col - row_pos_col) % pandas_lit(abs(step))
            ) == 0
    ordered_dataframe = frame.ordered_dataframe.filter(
        left_bound_filter & right_bound_filter & step_bound_filter
    )

    ordering_columns = (
        [column.reverse() for column in ordered_dataframe.ordering_columns]
        if reverse_order
        else ordered_dataframe.ordering_columns
    )
    ordered_dataframe = ordered_dataframe.sort(ordering_columns)
    return InternalFrame.create(
        ordered_dataframe=ordered_dataframe,
        data_column_pandas_labels=internal_frame.data_column_pandas_labels,
        data_column_pandas_index_names=internal_frame.data_column_pandas_index_names,
        data_column_snowflake_quoted_identifiers=internal_frame.data_column_snowflake_quoted_identifiers,
        index_column_pandas_labels=internal_frame.index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=internal_frame.index_column_snowflake_quoted_identifiers,
        data_column_types=internal_frame.cached_data_column_snowpark_pandas_types,
        index_column_types=internal_frame.cached_index_column_snowpark_pandas_types,
    )


def _get_frame_by_row_label_boolean_frame(
    internal_frame: InternalFrame,
    key: InternalFrame,
    dummy_row_pos_mode: bool = False,
) -> InternalFrame:
    """
    Select rows with boolean frame key. Here, if the frame and key's index are aligned, then the join is on their row
    position; otherwise, join on their indices.

    Args:
        internal_frame: the main frame
        key: the key holding the boolean labels

    Returns:
        Frame with selected rows
    """
    # joined_frame should keep the original index, so we do not coalesce on join keys and do not inherit join index
    # to join_on_row_position_if_matching_index_otherwise_join_on_index with how argument
    joined_frame, result_column_mapper = align_on_index(
        internal_frame,
        key,
        "coalesce",
        dummy_row_pos_mode,
    )

    key_bool_val_col = col(
        result_column_mapper.map_right_quoted_identifiers(
            key.data_column_snowflake_quoted_identifiers
        )[0]
    )
    # only select rows where key's boolean value is True
    # pd.Series([1,2,3], index=[1,2,3]).loc[pd.Series([True, True, True, True], index = [0,1,2,3])]
    # The result does not include index = 0
    filtered_frame = joined_frame.filter(key_bool_val_col)
    # Note:
    # 1. only use left's data columns
    # 2. keep using left row position column as the ordering column
    return InternalFrame.create(
        ordered_dataframe=filtered_frame.ordered_dataframe,
        data_column_pandas_labels=internal_frame.data_column_pandas_labels,
        data_column_pandas_index_names=internal_frame.data_column_pandas_index_names,
        data_column_snowflake_quoted_identifiers=result_column_mapper.map_left_quoted_identifiers(
            internal_frame.data_column_snowflake_quoted_identifiers
        ),
        index_column_pandas_labels=internal_frame.index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=result_column_mapper.map_left_quoted_identifiers(
            internal_frame.index_column_snowflake_quoted_identifiers
        ),
        data_column_types=internal_frame.cached_data_column_snowpark_pandas_types,
        index_column_types=internal_frame.cached_index_column_snowpark_pandas_types,
    )


def _get_frame_by_row_label_non_boolean_frame(
    internal_frame: InternalFrame,
    key: InternalFrame,
    dummy_row_pos_mode: bool = False,
) -> InternalFrame:
    """
    Select rows where its index is equal to the index in the key value.

    Args:
        internal_frame: the main frame
        key: the frame/series holding the labels

    Returns:
        Frame with selected rows
    """
    # This method is going to join internal_frame with key to select the index in the key values
    # The default left_on is the internal_frame's index columns and the default right_on is the key's data columns
    left_on = key.data_column_snowflake_quoted_identifiers
    right_on = internal_frame.index_column_snowflake_quoted_identifiers
    if internal_frame.is_multiindex(axis=0):
        # When row index is multiindex, if key value is array type (list like), then loc does exact match
        # Otherwise, when key value is not array type, loc does prefix match, i.e., match the top level only
        # e.g., if the internal frame has multiindex ["foo", "bar"]
        if isinstance(
            key.get_snowflake_type(key.data_column_snowflake_quoted_identifiers[0]),
            ArrayType,
        ):
            # if the key is array type, pandas performs exact match, so the value in the array needs to be exact
            # ["foo", "bar"]
            num_levels = internal_frame.num_index_levels(axis=0)
            for level in range(num_levels):
                # get return NULL if the level does not exist, and then the later join won't match too
                mi_level_key = get(
                    col(key.data_column_snowflake_quoted_identifiers[0]), level
                )
                key = key.append_column(
                    internal_frame.index_column_pandas_labels[level], mi_level_key
                )
            left_on = (
                key.ordered_dataframe.projected_column_snowflake_quoted_identifiers[
                    -num_levels:
                ]
            )
        else:
            # if the key is not array, pandas performs prefix match, e.g., the value needs to be "foo" to match with
            # multiindex ["foo", "bar"]
            right_on = internal_frame.index_column_snowflake_quoted_identifiers[:1]

    # Note: join internal_frame's index column with key's data column
    joined_frame, result_column_mapper = join(
        key,
        internal_frame,
        how="inner",
        left_on=left_on,
        right_on=right_on,
        inherit_join_index=InheritJoinIndex.FROM_RIGHT,
        dummy_row_pos_mode=dummy_row_pos_mode,
    )

    # Note: reuse pandas labels from internal frame
    return InternalFrame.create(
        ordered_dataframe=joined_frame.ordered_dataframe,
        data_column_pandas_labels=internal_frame.data_column_pandas_labels,
        data_column_snowflake_quoted_identifiers=result_column_mapper.map_right_quoted_identifiers(
            internal_frame.data_column_snowflake_quoted_identifiers
        ),
        data_column_pandas_index_names=internal_frame.data_column_pandas_index_names,
        index_column_pandas_labels=joined_frame.index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=result_column_mapper.map_right_quoted_identifiers(
            internal_frame.index_column_snowflake_quoted_identifiers
        ),
        data_column_types=internal_frame.cached_data_column_snowpark_pandas_types,
        index_column_types=internal_frame.cached_index_column_snowpark_pandas_types,
    )


def _propagate_last_row_if_columns_are_short(
    frame: InternalFrame,
    columns_to_ffill: list[str],
    window_row_position_snowflake_identifier: str,
    frame_row_position_snowflake_identifier: str,
) -> InternalFrame:
    """
    This helper method handles propagating the last row of a frame when it is too short, e.g.
    if we were to do df[:] = [[1, 2, 3]], where df has 3 rows, we'd need to propagate the last row
    of item so that the item frame has 3 rows. Performs a forward-fill for the specified columns.

    Args:
        frame: the internal frame whose rows to propagate.
        columns: the subset of columns to forward fill.
        window_row_position_snowflake_identifier: the order column to use for the Window.
        frame_row_position_snowflake_identifier: the row position column to use to determine the last value.

    Returns:
        the frame with the specified columns forward filled.
    """
    return (
        frame.update_snowflake_quoted_identifiers_with_expressions(
            {
                c: iff(
                    is_null(window_row_position_snowflake_identifier),
                    last_value(c, ignore_nulls=True).over(
                        Window.order_by(
                            frame_row_position_snowflake_identifier
                        ).rows_between(Window.UNBOUNDED_PRECEDING, Window.CURRENT_ROW)
                    ),
                    col(c),
                )
                for c in columns_to_ffill
            }
        )
    ).frame


def _set_2d_labels_helper_for_frame_item(
    internal_frame: InternalFrame,
    index: Union[slice, InternalFrame],
    item: InternalFrame,
    matching_item_columns_by_label: bool,
    matching_item_rows_by_label: bool,
    col_info: LocSetColInfo,
    index_is_bool_indexer: bool,
    dummy_row_pos_mode: bool = False,
) -> InternalFrame:
    """
    This set 2d label helper method handles df[index, columns] = item where index is a non-boolean indexer and item is a
    dataframe. It uses a general 2-join approach:
    First, join index and item
    Second, let the internal_frame to join the result from the first step

    Args:
        internal_frame: the internal frame for the main dataframe/series
        index: the internal frame for the index. Note that index can be None, and we can save one join for this case.
        item: the internal frame for the item
        matching_item_columns_by_label: whether matching item columns by labels or positions
        matching_item_rows_by_label: whether matching item rows by labels or positions
        col_info: the column information extracted from columns input
        index_is_bool_indexer: if True, the index is a boolean indexer

    Returns:
        the frame joined with internal frame, index, and item
    """
    if not matching_item_columns_by_label:
        expected_num_cols_item = len(col_info.column_pandas_labels)
        actual_num_cols_item = len(item.data_column_pandas_labels)
        if expected_num_cols_item != actual_num_cols_item:
            # when matching item by position, pandas requires the number of columns match between df and item
            raise ValueError(
                LOC_SET_ITEM_SHAPE_MISMATCH_ERROR_MESSAGE.format(
                    actual_num_cols_item, expected_num_cols_item
                )
            )

    if index_is_bool_indexer:
        result_frame = align_on_index(
            internal_frame,
            index,
            "coalesce",
        ).result_frame

        return align_on_index(
            result_frame,
            item,
            "coalesce",
        ).result_frame

    if index == slice(None):
        # No need to join index, only need one join between the internal_frame and item
        if matching_item_rows_by_label:
            return align_on_index(
                internal_frame,
                item,
                "coalesce",
            ).result_frame
        else:
            # We are in this case if RHS was originally an array object (so not a Snowpark pandas
            # object). In this case, if item is too short, we want to propagate the last non-null
            # values of each column, to match pandas broadcasting behavior in the case that we are
            # doing a loc set of multiple rows, but only provide 1 row (in which case pandas would
            # just set all of the rows to the provided rows).
            internal_frame = internal_frame.ensure_row_position_column(
                dummy_row_pos_mode
            )
            item = item.ensure_row_position_column(dummy_row_pos_mode)
            frame, mapping = join(
                internal_frame,
                item,
                how="left",
                left_on=[internal_frame.row_position_snowflake_quoted_identifier],
                right_on=[item.row_position_snowflake_quoted_identifier],
                dummy_row_pos_mode=dummy_row_pos_mode,
            )
            return _propagate_last_row_if_columns_are_short(
                frame,
                columns_to_ffill=mapping.map_right_quoted_identifiers(
                    item.data_column_snowflake_quoted_identifiers
                ),
                window_row_position_snowflake_identifier=mapping.map_right_quoted_identifiers(
                    [item.row_position_snowflake_quoted_identifier]
                )[
                    0
                ],
                frame_row_position_snowflake_identifier=mapping.map_left_quoted_identifiers(
                    [internal_frame.row_position_snowflake_quoted_identifier]
                )[
                    0
                ],
            )

    assert isinstance(index, InternalFrame)
    if not item.has_unique_index(axis=1):
        raise ValueError(SETTING_NON_UNIQUE_COLUMNS_IS_NOT_ALLOWED_ERROR_MESSAGE)

    # Let's use an example to walk through the code below. Assuming we will perform the following operations:
    #
    # >>> df = pd.DataFrame([[1, 2, 3, 4], [4, 5, 6, 7], [7, 8, 9, 10]], columns=["A", "B", "C", "D"])
    #
    # >>> df
    # 	A	B	C	D
    # 0	1	2	3	4
    # 1	4	5	6	7
    # 2	7	8	9	10
    #
    # >>> column_key = ["B", "E", 1, "B", "C", "X", "C", 2, "C", "D"]
    #
    # >>> item = pd.DataFrame(
    #     [[91, 92, 93, 94], [94, 95, 96, 97], [97, 98, 99, 100]],
    #     columns=["A", "B", "C", "X"],
    #     index=[
    #         3,  # 3 does not exist in the row key, so it will be skipped
    #         2,
    #         1,
    #     ],
    # )
    # >>> row_key = pd.Series(
    #     [
    #         0,  # 0 does not exist in item, so the row values will be set to NULL
    #         1,
    #     ]
    # )
    #
    # >>> df.loc[row_key, col_key] = item
    # >>> df
    # 	A B    B    C    C    C    D    E   1   X     2
    # 0	1 NaN  NaN  NaN  NaN  NaN  NaN  NaN NaN NaN   NaN
    # 1	4 98.0 98.0 99.0 99.0 99.0 NaN  NaN NaN 100.0 NaN
    # 2	7 8.0  8.0  9.0	 9.0  9.0  10.0 NaN NaN NaN   NaN
    #
    # We can summarize the pandas behavior for loc set as follows:
    # 1. The result keeps the original row and column order, e.g., row is still from 0 to 2 and columns is from A to D.
    # 2. The result can append duplicate and new columns (i.e., Column enlargement) which is defined in ``column_key``
    #    in the above example (i.e., ``duplicate_data_column_pos_to_count_map`` and
    #    ``new_data_column_pandas_labels_to_append`` from the arguments). The duplicated columns will be appended after
    #    the original ones. And new columns will be appended after original columns, e.g., "E", 1, "X", 2. Note row
    #    enlargement is not allowed in this case. It only happens when index is a scalar or tuple for multiindex.
    # 3. Any cells which matches with the ``row_key`` and ``column_key`` will be updated. If the same row and column
    #    index exist in ``item``, it will be updated using the item value (e.g., row 1, col "C"); otherwise, it will be
    #    NaN (e.g., row 0, col "B").
    # In this case, ``set_frame_2d_labels``'s arguments will be
    #   columns = [1, 2, 3]
    #   duplicate_data_column_pos_to_count_map = {1: 2, 2: 3}
    #   new_data_column_pandas_labels_to_append = ["E", 1, "X", 2]

    # first, left join index and item on columns representing the row index. So the row indices that do not exist in
    # ``index`` will be skipped. Using the above example, the ``index_with_item`` will contain row index [0, 1] and
    # the ``index`` and ``item``'s data columns: [key."__reduced__", "A", "B", "C", "X"]
    assert len(index.data_column_snowflake_quoted_identifiers) == len(
        item.index_column_snowflake_quoted_identifiers
    ), "TODO: SNOW-966427 handle it well in multiindex case"

    if not matching_item_rows_by_label:
        index = index.ensure_row_position_column(dummy_row_pos_mode)
        left_on = [index.row_position_snowflake_quoted_identifier]
        item = item.ensure_row_position_column(dummy_row_pos_mode)
        right_on = [item.row_position_snowflake_quoted_identifier]
    else:
        left_on = index.data_column_snowflake_quoted_identifiers
        right_on = item.index_column_snowflake_quoted_identifiers

    index_with_item, mapping = join(
        left=index,
        right=item,
        left_on=left_on,
        right_on=right_on,
        how="left",
        dummy_row_pos_mode=dummy_row_pos_mode,
    )

    index_with_item = index_with_item.strip_duplicates(
        quoted_identifiers=[index.data_column_snowflake_quoted_identifiers[0]],
        dummy_row_pos_mode=dummy_row_pos_mode,
    )

    if not matching_item_rows_by_label:
        # We are in this case if RHS was originally an array object (so not a Snowpark pandas
        # object). In this case, if item is too short, we want to propagate the last non-null
        # values of each column, to match pandas broadcasting behavior in the case that we are
        # doing a loc set of multiple rows, but only provide 1 row (in which case pandas would
        # just set all of the rows to the provided rows).
        index_with_item = _propagate_last_row_if_columns_are_short(
            index_with_item,
            columns_to_ffill=mapping.map_right_quoted_identifiers(
                item.data_column_snowflake_quoted_identifiers
            ),
            window_row_position_snowflake_identifier=mapping.map_right_quoted_identifiers(
                [item.row_position_snowflake_quoted_identifier]
            )[
                0
            ],
            frame_row_position_snowflake_identifier=mapping.map_left_quoted_identifiers(
                [index.row_position_snowflake_quoted_identifier]
            )[0],
        )
    # second, left join with main frame on columns representing the row index. Similarly, row index does not exist
    # in ``internal_frame`` will be skipped. So ``result_frame`` contains original row index [0,1,2] and 8 data
    # columns: ["A", "B", "C", "D", key."__reduced__", "A_alias", "B_alias", "C_alias", "X"]
    return join(
        left=internal_frame,
        right=index_with_item,
        left_on=internal_frame.index_column_snowflake_quoted_identifiers,
        right_on=index_with_item.data_column_snowflake_quoted_identifiers[:1],
        how="left",
        dummy_row_pos_mode=dummy_row_pos_mode,
    ).result_frame


def _set_2d_labels_helper_for_non_frame_item(
    internal_frame: InternalFrame,
    index: Union[slice, Scalar, InternalFrame],
    index_is_bool_indexer: bool,
    dummy_row_pos_mode: bool = False,
) -> InternalFrame:
    """
    The helper method for the case where item is not an internal frame
    Args:
        internal_frame: the internal frame.
        index: the index frame or scalar or slice(None).
        index_is_bool_indexer: if True, the index is a boolean indexer.
    Returns:
        result frame
    """
    if is_scalar(index):
        # we use outer join to handle scalar index which may enlarge/append the new row to the frame and use
        # JoinKeyCoalesceConfig.LEFT to set the result index = coalesce(left, right)
        # For example, if index is "a", if "a" exists in the internal frame, the outer join will be
        #
        #  __index__ | ... | __reduced__ |
        #     "a"    | ... |      NULL   |
        #     "b"    | ... |      NULL   |
        #     "c"    | ... |      NULL   |
        #
        # If index is "x" and does not exist in the internal frame, the outer join will be
        #  __index__ | ... | __reduced__ |
        #     "a"    | ... |      NULL   |
        #     "b"    | ... |      NULL   |
        #     "c"    | ... |      NULL   |
        #     "x"    | ... |      NULL   |
        #
        index_frame = pd.Series([None], index=[index])._query_compiler._modin_frame
        return join(
            left=internal_frame,
            right=index_frame,
            left_on=internal_frame.index_column_snowflake_quoted_identifiers,
            right_on=index_frame.index_column_snowflake_quoted_identifiers,
            how="outer",
            join_key_coalesce_config=[JoinKeyCoalesceConfig.LEFT],
            dummy_row_pos_mode=dummy_row_pos_mode,
        ).result_frame
    elif index_is_bool_indexer:
        return align_on_index(
            internal_frame,
            index,
            "coalesce",
        ).result_frame
    elif isinstance(index, InternalFrame):
        index = index.strip_duplicates(
            quoted_identifiers=[index.data_column_snowflake_quoted_identifiers[0]],
            dummy_row_pos_mode=dummy_row_pos_mode,
        )

        return join(
            left=internal_frame,
            right=index,
            left_on=internal_frame.index_column_snowflake_quoted_identifiers,
            right_on=index.data_column_snowflake_quoted_identifiers,
            how="left",
            dummy_row_pos_mode=dummy_row_pos_mode,
        ).result_frame

    # No need to join index, only need one join between the internal_frame and item
    return internal_frame


# TODO: SNOW-985231 check how duplication works for when index and item have duplication, and also see if we can unify
#  code path for single column item and regular dataframe item
def _set_2d_labels_helper_for_single_column_wise_item(
    internal_frame: InternalFrame,
    index: InternalFrame,
    item_values: list,
    item_data_column_pandas_labels: list[Hashable],
    index_is_bool_indexer: bool,
    enforce_match_item_by_row_labels: bool,
    dummy_row_pos_mode: bool = False,
) -> InternalFrame:
    """
    # If it's a single column with an item list, then we set the item values column-wise, for example,
    # df.loc[['x','y'],'B'] = [97,96] would result in:
    #       ... | B  | ...
    #    -------+----+-----
    #       'x' | 97 | ...
    #       'y' | 96 | ...
    #       ... | .. | ...

    Args:
        internal_frame: the internal frame
        index: the index frame
        item_values: the item values in a list
        index_is_bool_indexer: if True, the index is a boolean indexer.
        enforce_match_item_by_row_labels: if True, label matching must be used.

    Returns:
        The result frame contains original frame, index, and, item values.
    """
    assert (
        len(item_data_column_pandas_labels) == 1
    ), "Please ensure it is a single column."

    item = pd.DataFrame(
        item_values,
        columns=item_data_column_pandas_labels,
    )._query_compiler._modin_frame

    item = item.ensure_row_position_column(dummy_row_pos_mode)
    index = index.ensure_row_position_column(dummy_row_pos_mode)

    if index_is_bool_indexer:
        index = _get_frame_by_row_pos_boolean_frame(
            internal_frame, key=index, dummy_row_pos_mode=dummy_row_pos_mode
        )
        index = index.project_columns(
            [DEFAULT_DATA_COLUMN_LABEL],
            [col(id) for id in index.index_column_snowflake_quoted_identifiers],
        ).ensure_row_position_column(dummy_row_pos_mode)

    # First, we join the index and item based on the relative row positions.
    index_with_item_res = align(
        left=index,
        right=item,
        left_on=[index.row_position_snowflake_quoted_identifier],
        right_on=[item.row_position_snowflake_quoted_identifier],
        how="coalesce",
        dummy_row_pos_mode=dummy_row_pos_mode,
    )
    index_with_item = index_with_item_res.result_frame
    index_with_item_mapper = index_with_item_res.result_column_mapper
    item_row_position_column = index_with_item_mapper.map_right_quoted_identifiers(
        [item.row_position_snowflake_quoted_identifier]
    )[0]

    new_left_ids = index_with_item_mapper.map_left_quoted_identifiers(
        index.data_column_snowflake_quoted_identifiers
    )
    # If the item values is shorter than the index, we will fill in with the last item value.
    last_item_id = index_with_item.data_column_snowflake_quoted_identifiers[-1]
    index_with_item = index_with_item.project_columns(
        index_with_item.data_column_pandas_labels,
        [col(col_id) for col_id in new_left_ids]
        + [
            iff(
                col(item_row_position_column).is_null(),
                pandas_lit(item_values[-1]),
                col(last_item_id),
            )
        ],
        column_types=[
            index_with_item.snowflake_quoted_identifier_to_snowpark_pandas_type[id]
            for id in new_left_ids
        ]
        + [
            index_with_item.snowflake_quoted_identifier_to_snowpark_pandas_type[
                last_item_id
            ]
        ],
    )

    if index_is_bool_indexer or enforce_match_item_by_row_labels:
        # Here, we are using label matching, so we want to match the labels of internal_frame (the index column)
        # with the labels of index, which become the first data column of index_with_item after the join.
        # deduplicate row index in index_with_item and use the last_value for the duplicate index
        index_with_item = index_with_item.strip_duplicates(
            quoted_identifiers=[index.data_column_snowflake_quoted_identifiers[0]],
            dummy_row_pos_mode=dummy_row_pos_mode,
        )
        left_on = internal_frame.index_column_snowflake_quoted_identifiers
        right_on = index_with_item.data_column_snowflake_quoted_identifiers[:1]
    else:
        internal_frame = internal_frame.ensure_row_position_column(dummy_row_pos_mode)
        left_on = [internal_frame.row_position_snowflake_quoted_identifier]
        index_with_item = index_with_item.ensure_row_position_column(dummy_row_pos_mode)
        right_on = [index_with_item.row_position_snowflake_quoted_identifier]

    return align(
        left=internal_frame,
        right=index_with_item,
        left_on=left_on,
        right_on=right_on,
        how="coalesce",
        dummy_row_pos_mode=dummy_row_pos_mode,
    ).result_frame


def _convert_series_item_to_row_for_set_frame_2d_labels(
    columns: Union[
        "snowflake_query_compiler.SnowflakeQueryCompiler",
        tuple,
        slice,
        list,
        "pd.Index",
        np.ndarray,
    ],
    col_info: LocSetColInfo,
    item: InternalFrame,
) -> InternalFrame:
    """
    Helper method to convert a Series to a row for a locset.

    Args:
        columns: the column labels to set
        col_info: information about the column labels to set
        item: the new values to set
    Returns:
        New item frame that has been converted from Series (single column) to single
        row - in effect a transpose.
    """
    from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
        SnowflakeQueryCompiler,
    )

    col_len = len(col_info.existing_column_positions)

    if isinstance(columns, SnowflakeQueryCompiler):
        # In the following step, we convert the Series item value to a single row.
        # The column names for that row will come from the Series index. In the
        # remainder of this set operation, we will use label based matching to
        # match the column names of the item value and the column names of the
        # frame that we are setting. If we are passed in columns as a Series,
        # we want to match the values positionally (example below). To do this,
        # we set the index of the item frame to the columns key passed in, so
        # that after the next step, the item frame (single row frame) will have
        # the columns key as the columns.
        # Example:
        # df.loc[:, pd.Series(["C", "B", "A"])] = pd.Series([1, 2, 3])
        # In the above example, we want to set column C to 1, column B
        # to 2, and column A to 3. If we didn't do the step below (the set_index
        # step), then after the transpose, the item would look like this:
        #    0  1  2
        # 0  1  2  3
        # but we want instead for it to look like this:
        #    C  B  A
        # 0  1  2  3
        # so by setting the index to ["C", "B", "A"], then we get the desired result.
        item = SnowflakeQueryCompiler(item).set_index_from_series(columns)._modin_frame

    # Here we do the `reset_index`, since it may be the case that we set the index
    # in the previous step to the columns key, rather than what the index was previously,
    # which can lead to Snowflake internal errors if, for example, the index of the internal
    # frame we are setting is of type int, but the columns are of type string, since the
    # new index of item after the transpose will be of type string rather than of type int
    # causing a join error.
    item = (
        SnowflakeQueryCompiler(
            get_item_series_as_single_row_frame(item, col_len, move_index_to_cols=True)
        )
        .reset_index(drop=True)
        ._modin_frame
    )
    return item


def _add_scalar_index_to_item_and_convert_index_to_internal_frame(
    item: InternalFrame, index: Scalar
) -> tuple[InternalFrame, InternalFrame]:
    """
    Helper method to convert a scalar index to an InternalFrame and add it to the item InternalFrame.

    Args:
        item: the new values to set
        index: the row labels to set. None means all rows are included.
    Returns:
        New item value with the index value as its index, and new index value converted
        to a Series with a single value.
    """
    from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
        SnowflakeQueryCompiler,
    )

    index = pd.Series([index])._query_compiler
    item = SnowflakeQueryCompiler(item).set_index_from_series(index)._modin_frame
    return item, index._modin_frame


def set_frame_2d_labels(
    internal_frame: InternalFrame,
    index: Union[Scalar, slice, InternalFrame],
    columns: Union[
        "snowflake_query_compiler.SnowflakeQueryCompiler",
        tuple,
        slice,
        list,
        "pd.Index",
        np.ndarray,
    ],
    item: Union[Scalar, AnyArrayLike, InternalFrame],
    matching_item_columns_by_label: bool,
    matching_item_rows_by_label: bool,
    index_is_bool_indexer: bool,
    deduplicate_columns: bool,
    frame_is_df_and_item_is_series: bool,
    dummy_row_pos_mode: bool = False,
) -> InternalFrame:
    """
    Helper function to handle the general loc set functionality. The general idea here is to join the key from ``index``
    and ``columns`` with the ``item`` first and then let the ``internal_frame`` performs a left join on the result from
    the first step. Then we do postprocessing on the joined frame to update the rows and columns.

    Args:
        internal_frame: the main frame
        index: the row labels to set. None means all rows are included.
        columns: the column labels to set
        item: the new values to set
        matching_item_columns_by_label: if True (e.g., df.loc[row_key, col_key] = item), only ``item``'s column labels
            match with col_key are used to set df values; otherwise, (e.g., df.loc[row_key_only] = item), use item's
            column position to match with the main frame. E.g., df has columns ["A", "B", "C"] and item has columns
            ["C", "B", "A"], df.loc[:] = item will update df's columns "A", "B", "C" using item column "C", "B", "A"
            respectively.
        matching_item_rows_by_label: if True (e.g., df.loc[row_key, col_key] = item), only ``item``'s row labels match
            with row_key are used to set df values; otherwise, (e.g., df.loc[col_key_only] = item), use item's
            row position to match with the main frame. E.g., df has rows ["A", "B", "C"] and item is a 2D NumPy Array
            df.loc[:] = item will update df's rows "A", "B", "C" using item's rows 0, 1, 2 respectively.
            `matching_item_rows_by_label` diverges from pandas behavior due to the lazy nature of snowpandas. In native
            pandas, if the length of the objects that we are joining is not equivalent, then pandas would error out
            because the shape is not broadcastable; while here, we use standard left join behavior.
        index_is_bool_indexer: if True, the index is a boolean indexer. Note we only handle boolean indexer with
                item is a SnowflakeQueryCompiler here.
        deduplicate_columns: if True, deduplicate columns from ``columns``.
        frame_is_df_and_item_is_series: Whether item is from a Series object and is being assigned to a DataFrame object
    Returns:
        New frame where values have been set
    """
    # TODO SNOW-962260 support multiindex
    # TODO SNOW-966481 support series

    # Let's use an example to walk through the code below. Assuming we will perform the following operations:
    #
    # >>> df = pd.DataFrame([[1, 2, 3, 4], [4, 5, 6, 7], [7, 8, 9, 10]], columns=["A", "B", "C", "D"])
    #
    # >>> df
    # 	A	B	C	D
    # 0	1	2	3	4
    # 1	4	5	6	7
    # 2	7	8	9	10
    #
    # >>> column_key = ["B", "E", 1, "B", "C", "X", "C", 2, "C", "D"]
    #
    # >>> item = pd.DataFrame(
    #     [[91, 92, 93, 94], [94, 95, 96, 97], [97, 98, 99, 100]],
    #     columns=["A", "B", "C", "X"],
    #     index=[
    #         3,  # 3 does not exist in the row key, so it will be skipped
    #         2,
    #         1,
    #     ],
    # )
    # >>> row_key = pd.Series(
    #     [
    #         0,  # 0 does not exist in item, so the row values will be set to NULL
    #         1,
    #     ]
    # )
    #
    # >>> df.loc[row_key, col_key] = item
    # >>> df
    # 	A B    B    C    C    C    D    E   1   X     2
    # 0	1 NaN  NaN  NaN  NaN  NaN  NaN  NaN NaN NaN   NaN
    # 1	4 98.0 98.0 99.0 99.0 99.0 NaN  NaN NaN 100.0 NaN
    # 2	7 8.0  8.0  9.0	 9.0  9.0  10.0 NaN NaN NaN   NaN
    #
    # We can summarize the pandas behavior for loc set as follows:
    # 1. The result keeps the original row and column order, e.g., row is still from 0 to 2 and columns is from A to D.
    # 2. The result can append duplicate and new columns (i.e., Column enlargement) which is defined in ``column_key``
    #    in the above example (i.e., ``duplicate_data_column_pos_to_count_map`` and
    #    ``new_data_column_pandas_labels_to_append`` from the arguments). The duplicated columns will be appended after
    #    the original ones. And new columns will be appended after original columns, e.g., "E", 1, "X", 2. Note row
    #    enlargement is not allowed in this case. It only happens when index is a scalar or tuple for multiindex.
    # 3. Any cells which matches with the ``row_key`` and ``column_key`` will be updated. If the same row and column
    #    index exist in ``item``, it will be updated using the item value (e.g., row 1, col "C"); otherwise, it will be
    #    NaN (e.g., row 0, col "B").
    # In this case, ``set_frame_2d_labels``'s arguments will be
    #   columns = [1, 2, 3]
    #   duplicate_data_column_pos_to_count_map = {1: 2, 2: 3}
    #   new_data_column_pandas_labels_to_append = ["E", 1, "X", 2]
    col_info = _extract_loc_set_col_info(internal_frame, columns)

    # Some variables shared in this method
    index_is_scalar = is_scalar(index)
    index_is_frame = isinstance(index, InternalFrame)
    item_is_frame = isinstance(item, InternalFrame)
    item_is_scalar = is_scalar(item)
    original_index = index

    assert not isinstance(index, slice) or index == slice(
        None
    ), "Should only handle slice(None) here"

    # stores the column values from list like item
    item_column_values = []
    # map from item's data column label to its position in the joined frame or item_column_values
    item_data_col_label_to_pos_map: dict[Hashable, int] = {}
    if item_is_frame:
        from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
            SnowflakeQueryCompiler,
        )

        # If `item` is from a Series (rather than a Dataframe), flip the series item values to apply them
        # across columns rather than rows.
        is_multi_col_set = (
            isinstance(columns, Sized) and len(columns) > 1
        ) or isinstance(columns, (slice, SnowflakeQueryCompiler))
        if frame_is_df_and_item_is_series and is_multi_col_set:
            # If columns is slice(None), we are setting all columns in the InternalFrame.
            matching_item_columns_by_label = True
            matching_item_rows_by_label = False
            item = _convert_series_item_to_row_for_set_frame_2d_labels(
                columns, col_info, item
            )
            if is_scalar(index):
                (
                    item,
                    index,
                ) = _add_scalar_index_to_item_and_convert_index_to_internal_frame(
                    item, index
                )

        # when item is not frame, this map will be initialized later
        item_data_col_label_to_pos_map = {
            label: pos
            for pos, label in enumerate(
                item.data_column_pandas_labels
                if matching_item_columns_by_label
                else col_info.column_pandas_labels
            )
        }

        result_frame = _set_2d_labels_helper_for_frame_item(
            internal_frame,
            index,
            item,
            matching_item_columns_by_label,
            matching_item_rows_by_label,
            col_info,
            index_is_bool_indexer,
            dummy_row_pos_mode,
        )

    if item_is_scalar:
        result_frame = _set_2d_labels_helper_for_non_frame_item(
            internal_frame, index, index_is_bool_indexer, dummy_row_pos_mode
        )
    elif not item_is_frame:
        assert is_list_like(item) and not any(
            is_list_like(i) for i in item
        ), "Only support 1d item list here."

        if len(item) == 0:
            # pandas will raise length mismatch error for empty list case
            raise ValueError(LOC_SET_ITEM_EMPTY_ERROR)

        # When item is a list like item,
        # 1. If it is a single value item, we handle it similar to scalar item
        # 2. If it is a single column with an item list with size > 1, then we set the item values column-wise. For
        # example, df.loc[['x','y'],'B'] = [97,96] would result in:
        #
        #       ... | B  | ...
        #    -------+----+-----
        #       'x' | 97 | ...
        #       'y' | 96 | ...
        #       ... | .. | ...
        # 3. Otherwise, it sets multiple columns with an item with size > 1, we set the item values along the row. For
        # example, df.loc[['x', 'y'], ['B', 'C']] = [97, 96] would result in:
        #
        #       ... | B  | C  |...
        #    -------+----+----+-----
        #       'x' | 97 | 96 | ...
        #       'y' | 97 | 96 | ...
        #       ... | .. | .. | ...
        item_values = (
            item.tolist() if isinstance(item, (pd.Index, np.ndarray)) else item
        )

        item_data_column_pandas_labels = col_info.column_pandas_labels

        if len(item_values) == 1:
            # handle single value list similar to scalar
            result_frame = _set_2d_labels_helper_for_non_frame_item(
                internal_frame, index, index_is_bool_indexer, dummy_row_pos_mode
            )
            item_is_scalar = True
            item = item_values[0]
        else:
            # item has been updated with labels corresponding to the columns to be replaced.
            matching_item_columns_by_label = True
            item_data_col_label_to_pos_map = {
                label: pos for pos, label in enumerate(item_data_column_pandas_labels)
            }

            expected_item_col_len = len(item_data_column_pandas_labels)
            if expected_item_col_len == 1:
                # set the item values column-wise
                if isinstance(index, slice):
                    index = get_index_frame_by_row_label_slice(internal_frame, index)

                # If `item` is a pandas Index, or our indexer is a Snowpark pandas object,
                # we need to enforce matching by row labels.
                enforce_match_item_by_row_labels = (
                    isinstance(item, pd.Index) or index_is_frame
                )
                result_frame = _set_2d_labels_helper_for_single_column_wise_item(
                    internal_frame,
                    index,
                    item_values,
                    item_data_column_pandas_labels,
                    index_is_bool_indexer,
                    enforce_match_item_by_row_labels,
                )
                # we convert bool indexer to non-bool one above so set it to False now
                index_is_bool_indexer = False
            else:
                # set the item values along the row
                if len(item_values) != expected_item_col_len:
                    raise ValueError(LOC_SET_ITEM_KV_MISMATCH_ERROR_MESSAGE)
                item_column_values = item_values
                result_frame = _set_2d_labels_helper_for_non_frame_item(
                    internal_frame, index, index_is_bool_indexer, dummy_row_pos_mode
                )
    # After 2-joins, we need to
    #   1) update original columns existed in ``columns``, e.g., "B", "C", "D";
    #   2) create duplicated columns, e.g., duplicated "B" and "C";
    #   3) append new columns, e.g., "E", 1, "X", 2
    origin_num_data_cols = len(internal_frame.data_column_pandas_labels)
    # col position for the first column from ``item``
    # When index is None, no index is joined so the offset is equal to origin_num_data_cols
    item_data_col_offset = origin_num_data_cols
    if isinstance(index, InternalFrame):
        item_data_col_offset += len(index.data_column_pandas_labels)

    # First Handling existing columns:
    # If the column is not set, keep its original values
    # If the column is set and has values in item, use the item value;
    # Otherwise, set the value to NULL

    # the data column from ``index`` frame, e.g., the index's data column [0, 1]
    index_data_col = (
        col(result_frame.data_column_snowflake_quoted_identifiers[origin_num_data_cols])
        if index_is_frame or index_is_scalar
        else None
    )
    result_frame_index_col = col(
        result_frame.index_column_snowflake_quoted_identifiers[0]
    )

    def generate_updated_expr_for_existing_col(
        col_pos: int, origin_col_type: Optional[SnowparkPandasType]
    ) -> SnowparkPandasColumn:
        """
        Helper function to generate the updated existing column based on the item value.

        Args:
            col_pos: the existing column position from internal_frame
            origin_col_type: the original column type

        Returns:
            The updated SnowparkPandasColumn
        """
        original_col = col(
            result_frame.data_column_snowflake_quoted_identifiers[col_pos]
        )
        # col_pos can be any column in the original frame, i.e., internal_frame. So if it is not in
        # existing_column_positions, we can just return the original column
        if col_pos not in col_info.existing_column_positions:
            return SnowparkPandasColumn(original_col, origin_col_type)

        col_label = result_frame.data_column_pandas_labels[col_pos]
        # col will be updated
        col_obj_type = None
        if item_is_scalar:
            col_obj = pandas_lit(item)
            col_obj_type = SnowparkPandasType.get_snowpark_pandas_type_for_pandas_type(
                type(item)
            )
        elif item_column_values:
            item_val = item_column_values[item_data_col_label_to_pos_map[col_label]]
            col_obj = pandas_lit(item_val)
            col_obj_type = SnowparkPandasType.get_snowpark_pandas_type_for_pandas_type(
                type(item_val)
            )
        elif not matching_item_columns_by_label:
            # columns in item is matched by position not label here. E.g., assuming df as A, B, C three columns,
            # df[["A", "B"]] = item will treat the first item's column as A and the second as B. However,
            # df[["B", "A"]] = item will treat the first item's column as B and the second as A. Also, if column key
            # contains duplicates, e.g, df[["A", "A"]] = item, then only the right index matters, i.e., the second
            # column will be treated as A.
            offset = item_data_col_offset + rindex(
                col_info.existing_column_positions, col_pos
            )
            col_obj = col(result_frame.data_column_snowflake_quoted_identifiers[offset])
            col_obj_type = result_frame.cached_data_column_snowpark_pandas_types[offset]
        elif (
            matching_item_columns_by_label
            and col_label in item_data_col_label_to_pos_map
        ):
            # col may have value in item, e.g., column "X"
            offset = item_data_col_offset + item_data_col_label_to_pos_map[col_label]
            col_obj = col(result_frame.data_column_snowflake_quoted_identifiers[offset])
            col_obj_type = result_frame.cached_data_column_snowpark_pandas_types[offset]
        else:
            # e.g., columns "E", i.e., column exists in the column key but not in item
            col_obj = pandas_lit(None)

        if index_is_scalar:
            col_obj = iff(
                result_frame_index_col.equal_null(pandas_lit(original_index)),
                col_obj,
                original_col,
            )
        elif index_is_bool_indexer:
            # update col_obj only iff index_data_col is True
            col_obj = iff(index_data_col, col_obj, original_col)
        elif index_is_frame:
            col_obj = iff(index_data_col.is_null(), original_col, col_obj)

        if (
            # In these cases, we can infer that the resulting column has a
            # SnowparkPandasType of `col_obj_type`:
            # Case 1: The values we are inserting have the same type as the
            # original column. For example, we are inserting Timedelta values
            # into a timedelta column, or int values into an int column. In
            # this case, we just propagate the original column type.
            col_obj_type == origin_col_type
            or  # noqa: W504
            # Case 2: We are inserting a null value. Inserting a scalar null
            # value should not change a column from TimedeltaType to a
            # non-timedelta type, or vice versa.
            (is_scalar(item) and pd.isna(item))
            or  # noqa: W504
            # Case 3: We are inserting a list-like of null values. Inserting
            # null values should not change a column from TimedeltaType to a
            # non-timedelta type, or vice versa.
            (item_column_values and (pd.isna(v) for v in item_column_values))
        ):
            final_col_obj_type = origin_col_type
        else:
            # In these cases, we can't necessarily infer the type of the
            # resulting column. For example, inserting 3 timedelta values
            # into a column of 3 integer values would change the
            # SnowparkPandasType from None to TimedeltaType, but inserting
            # only 1 timedelta value into a column of 3 integer values would
            # produce a mixed column of integers and timedelta values.
            # TODO(SNOW-1738952): Deduce the result types in these cases.
            ErrorMessage.not_implemented(_LOC_SET_NON_TIMEDELTA_TO_TIMEDELTA_ERROR)
        return SnowparkPandasColumn(col_obj, final_col_obj_type)

    def generate_updated_expr_for_new_col(
        col_label: Hashable,
    ) -> SnowparkPandasColumn:
        """
        Helper function to generate the newly added column.

        Args:
            col_label: the label of the new column

        Returns:
            The new SnowparkPandasColumn
        """
        if item_is_scalar:
            new_column = pandas_lit(item)
            col_obj_type = SnowparkPandasType.get_snowpark_pandas_type_for_pandas_type(
                type(item)
            )
        elif item_column_values:
            new_column = item_column_values[item_data_col_label_to_pos_map[col_label]]
            col_obj_type = SnowparkPandasType.get_snowpark_pandas_type_for_pandas_type(
                type(new_column)
            )
        elif not matching_item_columns_by_label:
            offset = item_data_col_offset + item_data_col_label_to_pos_map[col_label]
            new_column = col(
                result_frame.data_column_snowflake_quoted_identifiers[offset]
            )
            col_obj_type = result_frame.cached_data_column_snowpark_pandas_types[offset]
        elif (
            matching_item_columns_by_label
            and col_label in item_data_col_label_to_pos_map
        ):
            offset = item_data_col_offset + item_data_col_label_to_pos_map[col_label]
            new_column = col(
                result_frame.data_column_snowflake_quoted_identifiers[offset]
            )
            col_obj_type = result_frame.cached_data_column_snowpark_pandas_types[offset]
        else:
            return SnowparkPandasColumn(pandas_lit(None), None)
        if index_is_scalar:
            new_column = iff(
                result_frame_index_col.equal_null(pandas_lit(original_index)),
                new_column,
                pandas_lit(None),
            )
        elif index_is_bool_indexer:
            # new_column will be None if index_data_col is not True
            new_column = iff(index_data_col, new_column, pandas_lit(None))
        elif index_is_frame:
            new_column = iff(index_data_col.is_null(), pandas_lit(None), new_column)
        return SnowparkPandasColumn(new_column, col_obj_type)

    # The rest of code is to generate the list of project columns and labels for a loc set operation that can involve
    # replacing existing column values as well as adding new columns. The caller must provide the callables to select
    # the appropriate column replacement or new column logic.
    #
    # We use project_columns methods to handle all columns in one go
    project_labels, project_columns, project_column_types = [], [], []

    num_data_columns = len(internal_frame.data_column_pandas_labels)
    duplicate_data_column_pos_to_count_map = (
        col_info.existing_column_duplication_cardinality_map
    )
    new_data_column_pandas_labels_to_append = col_info.new_column_pandas_labels

    if deduplicate_columns:
        # Remove all duplicate columns from the result frame. This is used by setitem only.
        # First, empty duplicate map
        duplicate_data_column_pos_to_count_map = {}
        dedup = []
        # Second, remove duplicate labels for new column to append
        for label in new_data_column_pandas_labels_to_append:
            if label not in dedup:
                dedup.append(label)
        new_data_column_pandas_labels_to_append = dedup

    # add original data columns and their duplicates
    for col_pos in range(num_data_columns):
        col_label = result_frame.data_column_pandas_labels[col_pos]
        project_labels.append(col_label)
        origin_col_type = result_frame.cached_data_column_snowpark_pandas_types[col_pos]
        snowpark_pandas_col = generate_updated_expr_for_existing_col(
            col_pos, origin_col_type
        )
        project_columns.append(snowpark_pandas_col.snowpark_column)
        project_column_types.append(snowpark_pandas_col.snowpark_pandas_type)

        # When duplicate is needed, pandas will duplicate the column right after the original columns.
        if col_pos in duplicate_data_column_pos_to_count_map:
            cnt = duplicate_data_column_pos_to_count_map[col_pos]
            project_labels += [col_label] * cnt
            project_columns += [snowpark_pandas_col.snowpark_column] * cnt
            project_column_types += [snowpark_pandas_col.snowpark_pandas_type] * cnt

    # Last, append new columns
    for col_label in new_data_column_pandas_labels_to_append:
        new_snowpark_pandas_column = generate_updated_expr_for_new_col(col_label)
        project_labels.append(col_label)
        project_columns.append(new_snowpark_pandas_column.snowpark_column)
        project_column_types.append(new_snowpark_pandas_column.snowpark_pandas_type)

    return result_frame.project_columns(
        pandas_labels=project_labels,
        column_objects=project_columns,
        column_types=project_column_types,
    )


def set_frame_2d_positional(
    internal_frame: InternalFrame,
    index: InternalFrame,
    columns: list[int],
    set_as_coords: bool,
    item: Union[InternalFrame, Scalar],
    is_item_series: bool,
    dummy_row_pos_mode: bool = False,
) -> InternalFrame:
    """
    Helper function to handle the general (worst case) 2-join case where index (aka row_key) and item are both frames.

    This general case involves a query with 2 joins, one for joining index and item (row_key and item values), and then
    joining with the original internal_frame.  For the case where item is based on a series (item_is_series), we need
    to transpose the series row values into columns to match pandas semantics.

    Note if len(index) > len(item) then the resulting 2d frame projects the last item values to align with the index.
    For example, if index=[0,1,2] with item = [99, 100] the result would be kv-frame index-item being set would be
    [0,99], [1,100], [2,100].

    For example:
        df = pd.DataFrame([
            [1, 2, 3, 4],
            [5, 6, 7, 8],
            [9, 10, 11, 12],
            [13, 14, 15, 16]
        ])

        pd.iloc[
            pd.Series([1,2,3]),
            pd.Series([0,2,3])
        ] = pd.DataFrame([
            [91, 92, 93],
            [94, 95, 96],
            [97, 98, 99]
        ])

        the resulting frame would be:

        __index__ |  0  |  1  |  2  |  3  | __row_position__
        ==========|=====|=====|=====|=====|==================
             0    |  1  |  2  |  3  |  4  |       0
             1    |  91 |  2  |  92 |  93 |       1
             2    |  94 |  2  |  95 |  96 |       2
             3    |  97 |  2  |  96 |  99 |       3

    Parameters
    ----------
    internal_frame: The original frame to set the item values into
    index: The row key values that specify the row positions of the internal_frame to set.
    columns: The col key values that specify the column positions of the internal_frame to set.
    set_as_coords: To set coordinates (row, col) pairs rather than per row and col alignment.
    item: The item values to set into the internal_frame, this could be an internal_frame or scalar value.
    is_item_series: Whether the item frame came from Series

    Results
    -------
    The result is a frame that has the indexed row and columns replaced with item values.
    """
    index_data_type = index.get_snowflake_type(
        index.data_column_snowflake_quoted_identifiers[0]
    )

    # If index is a bool_indexer then convert to same-sized position index, False values will be null.
    if isinstance(index_data_type, BooleanType):
        index = get_row_position_index_from_bool_indexer(index)
    else:
        index = _get_adjusted_key_frame_by_row_pos_int_frame(
            internal_frame, index, dummy_row_pos_mode
        )

    assert isinstance(index_data_type, (_IntegralType, BooleanType))
    if isinstance(item, InternalFrame):
        # If item is Series (rather than a Dataframe), then we need to flip the series item values so they apply across
        # columns rather than rows.
        if is_item_series and len(columns) > 1:
            item = get_item_series_as_single_row_frame(item, len(columns))

        # Combine the index (key) and item (values) into one key-value frame.  Note that the column length of index
        # may be changed here and we need this to properly index into the kv_frame later.
        kv_frame = get_kv_frame_from_index_and_item_frames(
            index, item, dummy_row_pos_mode
        )
        item_data_columns_len = len(item.data_column_snowflake_quoted_identifiers)
    else:
        item_type = SnowparkPandasType.get_snowpark_pandas_type_for_pandas_type(
            type(item)
        )
        kv_frame = index.append_column(ITEM_VALUE_LABEL, pandas_lit(item), item_type)
        item_data_columns_len = 1

    # Next we join the key-value frame with the original frame based on row_position and row key
    # values.  In this example, assuming the original df (LHS) has data columns A, B, C, D would result in:
    # (note some columns are omitted for brevity here):
    #
    #   [df]      [df]  [df]  [df]  [df]        [df]           [index]    [item]  [item]  [item]
    # __index__ |  A  |  B  |  C  |  D  | __row_position__ | __reduced__ | col_1 | col_2 | col_3
    # ==========|=====|=====|=====|=====|==================|=============|=======|=======|=======
    #     0     |  1  |  2  |  3  |  4  |        0         |     null    |  null |  null | null
    #     1     |  5  |  6  |  7  |  8  |        1         |      1      |   91  |   92  |  93
    #     2     |  9  |  10 |  11 |  12 |        2         |      2      |   94  |   95  |  96
    #     3     |  13 |  14 |  15 |  16 |        3         |      3      |   97  |   98  |  99
    #
    # In the join result, the original dataframe data is aligned with the items that are to replace for the rows.

    frame = internal_frame.ensure_row_position_column(dummy_row_pos_mode)
    kv_frame = kv_frame.ensure_row_position_column(dummy_row_pos_mode)
    item_type = kv_frame.cached_data_column_snowpark_pandas_types[-1]

    # To match the columns of the original dataframe (see [df] above) and the item values (see [item] above) we
    # use the "columns" containing the column position indices.  For example, if columns=[0, 2, 3] this would
    # map to the frame data columns, by corresponding position, [A, C, D].  To find the corresponding offset
    # within the item data columns, we look up the index in columns, for example, column 2 is index 1 in
    # the "columns" list.  To find the absolute item offset, we calculate after the frame and index data columns.
    item_data_columns_offset = len(frame.data_column_pandas_labels) + len(
        index.data_column_pandas_labels
    )

    df_kv_frame, result_column_mapper = join(
        left=frame,
        right=kv_frame,
        how="left",
        left_on=[frame.row_position_snowflake_quoted_identifier],
        right_on=[kv_frame.data_column_snowflake_quoted_identifiers[0]],
        dummy_row_pos_mode=dummy_row_pos_mode,
    )

    # Get the row position column from the index of the combined join (frame, index, item)
    assert kv_frame.row_position_snowflake_quoted_identifier is not None
    kv_row_position_snowflake_quoted_identifier = (
        result_column_mapper.map_right_quoted_identifiers(
            [kv_frame.row_position_snowflake_quoted_identifier]
        )[0]
    )

    select_list = frame.index_column_snowflake_quoted_identifiers

    # Generate a projection that replaces the original df columns (A,B,C,D) with item columns (col_1, col_2, col_3)
    # at the respective column positions based on whether the row key column (__reduced__) is null or not.
    #
    #    __index__ |  A  |  B  |  C  |  D  | __row_position__
    #    ==========|=====|=====|=====|=====|==================
    #         0    |  1  |  2  |  3  |  4  |       0
    #         1    |  91 |  6  |  92 |  93 |       1
    #         2    |  94 |  10 |  95 |  96 |       2
    #         3    |  97 |  14 |  96 |  99 |       3

    new_data_column_snowflake_quoted_identifiers = (
        df_kv_frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
            pandas_labels=frame.data_column_pandas_labels,
        )
    )

    df_kv_frame = df_kv_frame.ensure_row_position_column(dummy_row_pos_mode)

    new_data_column_types = []
    for col_pos, snowflake_quoted_identifier_pair in enumerate(
        zip(
            frame.data_column_snowflake_quoted_identifiers,
            new_data_column_snowflake_quoted_identifiers,
        )
    ):
        (
            original_snowflake_quoted_identifier,
            new_snowflake_quoted_identifier,
        ) = snowflake_quoted_identifier_pair

        if col_pos in columns and item_data_columns_len > 0:
            # Calculate the offset into the item values of the joined frame.  We can't go past the size of
            # the kv data column length, so in that case we use the last item value column.  For example, suppose the
            # columns are [1,2,3,4] and the item column values [5,6] then will be filled with [5,6,6,6].  We also
            # need to look up based on the last index position to handle column duplicates properly, for example
            # if the columns are [0,2,1,2] with item column values [5, 6, 7, 8] should be filled with [5, 7, 8].
            column_index_position = rindex(columns, col_pos)
            kv_item_data_col_pos = min(column_index_position, item_data_columns_len - 1)

            # Calculate the column position within the joined frame of the current item data column to replace.
            df_snowflake_quoted_identifier = col(original_snowflake_quoted_identifier)
            item_snowflake_quoted_identifier = col(
                df_kv_frame.data_column_snowflake_quoted_identifiers[
                    item_data_columns_offset + kv_item_data_col_pos
                ]
            )

            set_cond_expr = (
                col(kv_row_position_snowflake_quoted_identifier)
                == pandas_lit(column_index_position)
                if set_as_coords
                else col(kv_row_position_snowflake_quoted_identifier).is_not_null()
            )

            select_list.append(
                iff(
                    set_cond_expr,
                    item_snowflake_quoted_identifier,
                    df_snowflake_quoted_identifier,
                ).as_(new_snowflake_quoted_identifier)
            )
            original_type = frame.snowflake_quoted_identifier_to_snowpark_pandas_type[
                original_snowflake_quoted_identifier
            ]
            if is_scalar(item) and pd.isna(item):
                new_data_column_types.append(original_type)
            elif original_type == item_type:
                new_data_column_types.append(item_type)
            else:
                new_data_column_types.append(None)
        else:
            select_list.append(
                col(original_snowflake_quoted_identifier).as_(
                    new_snowflake_quoted_identifier
                )
            )
            new_data_column_types.append(
                frame.snowflake_quoted_identifier_to_snowpark_pandas_type[
                    original_snowflake_quoted_identifier
                ]
            )

    select_list.append(df_kv_frame.row_position_snowflake_quoted_identifier)
    ordered_dataframe = df_kv_frame.ordered_dataframe.select(select_list)
    return InternalFrame.create(
        ordered_dataframe=ordered_dataframe,
        data_column_pandas_labels=frame.data_column_pandas_labels,
        data_column_pandas_index_names=frame.data_column_pandas_index_names,
        data_column_snowflake_quoted_identifiers=new_data_column_snowflake_quoted_identifiers,
        index_column_pandas_labels=frame.index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=frame.index_column_snowflake_quoted_identifiers,
        data_column_types=new_data_column_types,
        index_column_types=frame.cached_index_column_snowpark_pandas_types,
    )


def get_kv_frame_from_index_and_item_frames(
    index: InternalFrame,
    item: InternalFrame,
    dummy_row_pos_mode: bool = False,
) -> InternalFrame:
    """
    Return the key-value frame from the key (index) and item (values) frames by aligning on the row positions.
    If the len(index) > len(item) then the resulting key-value frame projects the last item values down.  For
    example, if index=[0,1,2] with item = [99,100] the result would be kv-frame [0,99], [1,100], [2,100].

    Parameters
    ----------
    index: The row key values that specify the row positions of the internal_frame to set.
    item: The item values to set into the internal_frame.

    Results
    -------
    frame: The result is a frame that joins the index and item so they are aligned based on the index row positions.
    """
    # The first step is to join the index (aka row_key) and item into a single aligned dataframe.  To do this,
    # we join based on their row positions.  In the example earlier, in the resulting key-value snowpark dataframe
    # would look like (the headers show where the columns come from either [index] or [item]):
    #
    #    [index]      [index]         [index]         [item]      [item]  [item]  [item]     [item]
    #   __index__ | __reduced__ | __row_position__ | __index__2 | col_1 | col_2 | col_3 | __row_position__
    # ============|=============|==================|============|=======|=======|=======|==================|
    #      0      |       1     |         0        |     0      |   91  |  92   |   93  |        0
    #      1      |       2     |         1        |     1      |   94  |  95   |   96  |        1
    #      2      |       3     |         2        |     2      |   97  |  98   |   99  |        2
    #
    # Note that the column "__reduced__" above is essentially the row key values, ie. the row positions that are
    # to be selected to replace with item values.  If the item has fewer rows, we follow the join with
    # a conditional lag to project down the last item row to the empty item rows.

    index = index.ensure_row_position_column(dummy_row_pos_mode)
    item = item.ensure_row_position_column(dummy_row_pos_mode)

    kv_frame, result_column_mapper = join(
        left=index,
        right=item,
        left_on=[index.row_position_snowflake_quoted_identifier],
        right_on=[item.row_position_snowflake_quoted_identifier],
        how="left",
        dummy_row_pos_mode=dummy_row_pos_mode,
    )

    index_row_position_snowflake_quoted_identifier = (
        result_column_mapper.map_left_quoted_identifiers(
            [index.row_position_snowflake_quoted_identifier]
        )[0]
    )
    item_row_snowflake_quoted_identifier = (
        result_column_mapper.map_right_quoted_identifiers(
            [item.row_position_snowflake_quoted_identifier]
        )[0]
    )

    # If len(item) < len(index), then the join result will contain a null row position and values.  In this case we
    # want to use the last matching item values.  To do this, we check if the item row position is null, and if so,
    # then use the lag value of the item data column.   For example, if index=[0,1,2] with item = [99,100] the result
    # would be kv-frame [0,99], [1,100], [2,100].
    num_index_data_columns = len(index.data_column_snowflake_quoted_identifiers)
    kv_item_data_column_pandas_labels = kv_frame.data_column_pandas_labels[
        num_index_data_columns:
    ]
    kv_item_data_column_snowflake_identifiers = (
        kv_frame.data_column_snowflake_quoted_identifiers[num_index_data_columns:]
    )
    new_item_data_column_snowflake_identifiers = (
        kv_frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
            pandas_labels=[MAX_ROW_POSITION_COLUMN_LABEL]
            + kv_item_data_column_pandas_labels,
        )
    )
    max_row_position_snowflake_quoted_identifier = (
        new_item_data_column_snowflake_identifiers[0]
    )
    new_item_data_column_snowflake_identifiers = (
        new_item_data_column_snowflake_identifiers[1:]
    )

    kv_frame_ordered_dataframe = kv_frame.ordered_dataframe
    for snowflake_quoted_identifier, new_snowflake_quoted_identifier in zip(
        kv_item_data_column_snowflake_identifiers,
        new_item_data_column_snowflake_identifiers,
    ):
        kv_frame_ordered_dataframe = append_columns(
            kv_frame_ordered_dataframe,
            new_snowflake_quoted_identifier,
            iff(
                col(item_row_snowflake_quoted_identifier).is_null(),
                lag(col(snowflake_quoted_identifier), ignore_nulls=True).over(
                    Window.order_by(index_row_position_snowflake_quoted_identifier)
                ),
                col(snowflake_quoted_identifier),
            ),
        )

    # If there are duplicates, we need to filter them out and use the last matching item value data.  To find the last
    # matching item values, we calculate the max_row_position across each row position (__reduced__) window and filter
    # out any earlier duplicates.  For example, consider a frame with the following index, row positions (other
    # columns are omitted for brevity), we add a temporary max_row_position column to filter with row_positions.
    #
    #    [index]      [index]         [index]             [item]     |
    #   __index__ | __reduced__ | __row_position__ |   ..(values)..  |   [max_row_position]
    # ============|=============|==================|=================|=======================
    #      0      |       1     |         0        |     ..(1)..     |         0
    #      1      |       2     |         1        |     ..(2)..     |         3
    #      2      |       3     |         2        |     ..(3)..     |         2
    #      3      |       2     |         3        |     ..(4)..     |         3
    #
    # would yield the following result (notice original row_positions 1 and 3 have duplicate index.__reduced__ values,
    # so in the result we only need to join with the last item value in this case from row_position=3):
    #
    #    [index]      [index]         [index]             [item]
    #   __index__ | __reduced__ | __row_position__ |       ...
    # ============|=============|==================|=================
    #      0      |       1     |         0        |     ..(1)..
    #      2      |       3     |         2        |     ..(3)..
    #      3      |       2     |         3        |     ..(4)..

    kv_frame_ordered_dataframe = append_columns(
        kv_frame_ordered_dataframe,
        max_row_position_snowflake_quoted_identifier,
        max_(index_row_position_snowflake_quoted_identifier).over(
            Window.partition_by(
                col(index.data_column_snowflake_quoted_identifiers[0])
            ).order_by(col(index.data_column_snowflake_quoted_identifiers[0]))
        ),
    ).filter(
        col(max_row_position_snowflake_quoted_identifier)
        == col(index_row_position_snowflake_quoted_identifier)
    )
    kv_frame_ordered_dataframe = kv_frame_ordered_dataframe.sort(
        kv_frame.ordering_columns
    )

    new_kv_frame = InternalFrame.create(
        ordered_dataframe=kv_frame_ordered_dataframe,
        data_column_pandas_labels=kv_frame.data_column_pandas_labels[
            :num_index_data_columns
        ]
        + kv_item_data_column_pandas_labels,
        data_column_pandas_index_names=kv_frame.data_column_pandas_index_names,
        data_column_snowflake_quoted_identifiers=kv_frame.data_column_snowflake_quoted_identifiers[
            :num_index_data_columns
        ]
        + new_item_data_column_snowflake_identifiers,
        index_column_pandas_labels=kv_frame.index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=kv_frame.index_column_snowflake_quoted_identifiers,
        data_column_types=kv_frame.cached_data_column_snowpark_pandas_types,
        index_column_types=kv_frame.cached_index_column_snowpark_pandas_types,
    )

    return new_kv_frame


def get_item_series_as_single_row_frame(
    item: InternalFrame,
    num_columns: int,
    move_index_to_cols: Optional[bool] = False,
    dummy_row_pos_mode: bool = False,
) -> InternalFrame:
    """
    Get an internal frame that transpose single data column into frame with single row.  For example, if the
    original series is pd.Series([1, 2, 4]) then:

    __index__ | __reduced__ | __row_position__
    ==========|=============|==================
        0     |     1       |        0
        1     |     2       |        1
        2     |     4       |        2

    the returned frame would be:

    __index__ | "0" | "1" | "2" | __row_position__
    ==========|=====|=====|=====|==================
        0     |  1  |  2  |  4  |         0

    Parameters
    ----------
        num_columns: Number of columns in the return frame
        item: Item frame that contains a single column of values.
        move_index_to_cols: Whether to use the index as the column names.

    Returns
    -------
        Frame containing single row with columns for each row.
    """
    item = item.ensure_row_position_column(dummy_row_pos_mode)
    item_series_pandas_labels = (
        list(range(num_columns))
        if not move_index_to_cols
        else item.index_columns_pandas_index().values
    )

    # This is a 2 step process.
    #
    # 1. Filter each row and column, above example the intermediate result would be:
    #
    #  "0"  | "1"  | "2"  | last_col
    # ======|======|======|=========
    #   1   | null | null |   4
    #  null |  2   | null |   4
    #  null | null |  4   |   4
    #
    # 2. Collapse each column to a single column value which is present.  If there is no value present then
    # use the last value.  For this example, the result would be:
    #
    #  "0"  | "1"  | "2"
    # ======|======|======
    #   1   |  2   |  4
    # and add __index_ and __row_position__ so conforming frame.
    #
    # To handle the case where item.num_rows < num_columns we fill in the remaining with the last item value.  So
    # for example:
    # get_item_series_as_single_row_frame([100, 102, 105], 5) returns [100, 102, 105, 105, 105]

    item_frame = item.append_column(
        LAST_VALUE_COLUMN,
        last_value(col(item.data_column_snowflake_quoted_identifiers[0])).over(
            Window.order_by(col(item.row_position_snowflake_quoted_identifier))
        ),
        item.cached_data_column_snowpark_pandas_types[0],
    )
    last_value_snowflake_quoted_identifier = (
        item_frame.data_column_snowflake_quoted_identifiers[-1]
    )

    item_series_snowflake_quoted_identifiers: list[str] = []
    item_series_column_exprs: list[Column] = []
    item_series_data_column_types = []

    for row_position, pandas_label in enumerate(item_series_pandas_labels):
        new_snowflake_quoted_identifier = (
            item_frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=[pandas_label],
                excluded=item_series_snowflake_quoted_identifiers,
            )[0]
        )
        new_column_expr = coalesce(
            min_(
                iff(
                    col(item.row_position_snowflake_quoted_identifier)
                    == lit(row_position),
                    col(item.data_column_snowflake_quoted_identifiers[0]),
                    None,
                )
            ).over(),
            min_(col(last_value_snowflake_quoted_identifier)).over(),
        )
        item_series_snowflake_quoted_identifiers.append(new_snowflake_quoted_identifier)
        item_series_column_exprs.append(new_column_expr)
        item_series_data_column_types.append(
            item.cached_data_column_snowpark_pandas_types[0]
        )

    item_ordered_dataframe = append_columns(
        item_frame.ordered_dataframe,
        item_series_snowflake_quoted_identifiers,
        item_series_column_exprs,
    )
    item_ordered_dataframe = item_ordered_dataframe.select(
        item.index_column_snowflake_quoted_identifiers[0],
        *item_series_snowflake_quoted_identifiers,
        item.row_position_snowflake_quoted_identifier,
    ).filter(col(item.row_position_snowflake_quoted_identifier) == 0)
    item_ordered_dataframe = item_ordered_dataframe.sort(
        OrderingColumn(item.row_position_snowflake_quoted_identifier)
    )
    item = InternalFrame.create(
        ordered_dataframe=item_ordered_dataframe,
        data_column_pandas_labels=item_series_pandas_labels,
        data_column_pandas_index_names=item.data_column_pandas_index_names,
        data_column_snowflake_quoted_identifiers=item_series_snowflake_quoted_identifiers,
        index_column_pandas_labels=item.index_column_pandas_labels[:1],
        index_column_snowflake_quoted_identifiers=item.index_column_snowflake_quoted_identifiers[
            :1
        ],
        data_column_types=item_series_data_column_types,
        index_column_types=item.cached_index_column_snowpark_pandas_types[:1],
    )
    return item


def get_row_position_index_from_bool_indexer(
    index: InternalFrame, dummy_row_pos_mode: bool = False
) -> InternalFrame:
    """
    Get the index positions for a bool_indexer frame.  Note that we the number of rows in the resulting dataframe
    can be less since rows with False value are omitted in the result.

    For example, for the index frame "pd.Series([False, True, True, False, True]":

    __index__ | __reduced__ | __row_position__
    ==========|=============|==================
        0     |     False   |        0
        1     |     True    |        1
        2     |     True    |        2
        3     |     False   |        3
        4     |     True    |        4

    the row position index frame returned would be:

    __index__ | __reduced__ | __row_position__
    ==========|=============|==================
        0     |     1       |        0
        1     |     2       |        1
        2     |     4       |        2

    Parameters
    ----------
    index: Frame with boolean data for each row on whether is index position

    Returns
    -------
    Returns frame with positions selected by boolean indexer.
    """

    # This is 2-step process.
    #
    # First, filter down to the index positions that are to be included, in this case intermediate result is:
    #
    # __index__ | __reduced__ | __row_position__
    # ==========|=============|==================
    # 1     |     True    |        1
    # 2     |     True    |        2
    # 4     |     True    |        4
    #
    # Next, regenerate the index and row position to be 0-based consecutive.
    #
    # __index__ | __reduced__ | __row_position__
    # ==========|=============|==================
    #     1     |     1       |        0
    #     2     |     2       |        1
    #     4     |     4       |        2

    index = index.ensure_row_position_column(dummy_row_pos_mode)

    (
        index_column_snowflake_quoted_identifier,
        data_column_snowflake_quoted_identifier,
    ) = index.ordered_dataframe.generate_snowflake_quoted_identifiers(
        pandas_labels=[
            index.index_column_pandas_labels[0],
            index.data_column_pandas_labels[0],
        ],
    )

    index_ordered_dataframe = index.ordered_dataframe.filter(
        col(index.data_column_snowflake_quoted_identifiers[0])
    ).select(
        [
            col(index.index_column_snowflake_quoted_identifiers[0]).as_(
                index_column_snowflake_quoted_identifier
            ),
            col(index.row_position_snowflake_quoted_identifier).as_(
                data_column_snowflake_quoted_identifier
            ),
        ]
    )
    index_ordered_dataframe = index_ordered_dataframe.sort(
        OrderingColumn(data_column_snowflake_quoted_identifier)
    )
    index = InternalFrame.create(
        ordered_dataframe=index_ordered_dataframe,
        data_column_pandas_labels=index.data_column_pandas_labels[:1],
        data_column_pandas_index_names=index.data_column_index_names,
        data_column_snowflake_quoted_identifiers=[
            data_column_snowflake_quoted_identifier
        ],
        index_column_pandas_labels=index.index_column_pandas_labels[:1],
        index_column_snowflake_quoted_identifiers=[
            index_column_snowflake_quoted_identifier
        ],
        data_column_types=[
            index.snowflake_quoted_identifier_to_snowpark_pandas_type.get(
                data_column_snowflake_quoted_identifier, None
            )
        ],
        index_column_types=[
            index.snowflake_quoted_identifier_to_snowpark_pandas_type.get(
                index_column_snowflake_quoted_identifier, None
            )
        ],
    )
    return index


def get_row_pos_frame_from_row_key(
    key: Union[
        "snowflake_query_compiler.SnowflakeQueryCompiler", Scalar, list, slice, tuple
    ],
    frame: InternalFrame,
    dummy_row_pos_mode: bool = False,
) -> InternalFrame:
    """
    Return a frame that contains the row positions if provided as a scalar/list/slice/etc

    Parameters
    ----------
    key: the row key positions

    Returns
    -------
    Returns a frame containing the row position values as a frame.
    """
    from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
        SnowflakeQueryCompiler,
    )

    if isinstance(key, SnowflakeQueryCompiler):
        return key._modin_frame

    if isinstance(key, slice):
        # If it is a slice, then use the row_position from the original frame and filter down based on the slice.
        frame = frame.ensure_row_position_column(dummy_row_pos_mode)

        key_frame = frame.project_columns(
            [MODIN_UNNAMED_SERIES_LABEL],
            [col(frame.row_position_snowflake_quoted_identifier)],
        )

        key_frame = get_frame_by_row_pos_slice_frame(key_frame, key)
        return key_frame
    else:
        if isinstance(key, int):
            key = [key]
        else:
            key = list(key)

        return pd.Series(key)._query_compiler._modin_frame


def get_snowflake_filter_for_row_label(
    frame: InternalFrame, label: Any, level: Optional[int] = None
) -> Optional[Column]:
    """
    Get Snowflake filter expression for given 'label' to match against index labels
    in given 'frame'
    Args:
        frame: Internal frame
        label: 'label' to generate expression for.
        level: Optional level to use from index. If None, generate expression to match
            against all index levels/columns.

    Returns:
        A snowflake filter expression represented by Snowpark Column.
        None implies no match.

    """
    index_column_identifiers = frame.index_column_snowflake_quoted_identifiers
    # Single level index
    if not frame.is_multiindex(axis=0):
        return col(index_column_identifiers[0]) == pandas_lit(label)

    # MultiIndex and level is specified.
    if level is not None:
        if level < frame.num_index_levels(axis=0):
            return col(index_column_identifiers[level]) == pandas_lit(label)
        else:
            return None

    # MultiIndex and level is not specified. If given label is not a tuple match
    # against first level.
    if not isinstance(label, tuple):
        return col(index_column_identifiers[0]) == pandas_lit(label)

    # MultiIndex and level is not specified. If given label is a tuple match
    # against all levels by treating given 'label' as prefix.
    # Special case handling: Empty tuple matches with everything.
    # This behavior is same as native pandas.
    if len(label) == 0:
        return pandas_lit(True)

    # Try to match with individual columns from multi index
    if len(label) <= frame.num_index_levels(axis=0):
        mi_filter = None
        for index_col_id, k in zip(index_column_identifiers, label):
            col_filter = col(index_col_id) == pandas_lit(k)
            mi_filter = col_filter if mi_filter is None else mi_filter & col_filter
        return mi_filter
    return None
