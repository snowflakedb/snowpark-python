#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from collections.abc import Hashable
from typing import Optional, Union

import pandas as native_pd
from modin.core.dataframe.algebra.default2pandas import DataFrameDefault  # type: ignore

from snowflake.snowpark.functions import any_value, get, lit
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin._internal.ordered_dataframe import (
    OrderedDataFrame,
    OrderingColumn,
)
from snowflake.snowpark.modin.plugin._internal.snowpark_pandas_types import (
    SnowparkPandasType,
)
from snowflake.snowpark.modin.plugin._internal.unpivot_utils import (
    UnpivotResultInfo,
    _prepare_unpivot_internal,
)
from snowflake.snowpark.modin.plugin._internal.utils import (
    INDEX_LABEL,
    LEVEL_LABEL,
    ROW_POSITION_COLUMN_LABEL,
    is_all_label_components_none,
    is_json_serializable_pandas_labels,
    pandas_lit,
    parse_object_construct_snowflake_quoted_identifier_and_extract_pandas_label,
    serialize_pandas_labels,
)
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage

TRANSPOSE_INDEX = "TRANSPOSE_IDX"
# transpose value column used in unpivot
TRANSPOSE_VALUE_COLUMN = "TRANSPOSE_VAL"
# transpose name column used in unpivot
TRANSPOSE_NAME_COLUMN = "TRANSPOSE_COL_NAME"
# transpose json parsed object name
TRANSPOSE_OBJ_NAME_COLUMN = "TRANSPOSE_OBJ_NAME"


def transpose_empty_df(
    original_frame: InternalFrame,
) -> "SnowflakeQueryCompiler":  # type: ignore[name-defined] # noqa: F821
    from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
        SnowflakeQueryCompiler,
    )
    from snowflake.snowpark.modin.plugin.extensions.utils import (
        try_convert_index_to_native,
    )

    return SnowflakeQueryCompiler.from_pandas(
        native_pd.DataFrame(
            columns=original_frame.index_columns_pandas_index(),
            index=try_convert_index_to_native(original_frame.data_columns_index),
        )
    )


def prepare_and_unpivot_for_transpose(
    original_frame: InternalFrame,
    query_compiler: "SnowflakeQueryCompiler",  # type: ignore[name-defined] # noqa: F821
    is_single_row: bool = False,
    dummy_row_pos_mode: bool = False,
) -> Union[UnpivotResultInfo, "SnowflakeQueryCompiler"]:  # type: ignore[name-defined] # noqa: F821

    # Check if the columns are all json serializable, if not, then go through fallback path.  The transpose approach
    # here requires json serializable labels because we use sql parse_json to split out row position and multi-level
    # index values as described below.
    #
    # TODO (SNOW-886400) Multi-level non-json serializable pandas label not handled.
    if not is_json_serializable_pandas_labels(original_frame.data_column_pandas_labels):
        return DataFrameDefault.register(native_pd.DataFrame.transpose)(query_compiler)

    # Ensure there is a row position since preserving order is important for unpivot and transpose.
    original_frame = original_frame.ensure_row_position_column(dummy_row_pos_mode)

    # Transpose is implemented with unpivot followed by pivot. However when the input dataframe is empty, there are two issues
    # 1) unpivot on empty table returns empty, which results in missing values in UNPIVOT_NAME_COLUMN
    # 2) pivot values can not be empty.
    # In order to overcome these, we add a dummy row to ordered_dataframe with row position value -1 to make sure
    # there is always atleast one row in the table, and drop the dummy column associated with row position -1 after pivot.
    ordered_dataframe = original_frame.ordered_dataframe
    row_position_snowflake_quoted_identifier = (
        original_frame.row_position_snowflake_quoted_identifier
    )
    if not is_single_row:
        quoted_identifiers = (
            ordered_dataframe.projected_column_snowflake_quoted_identifiers
        )
        new_columns = []
        for identifier in quoted_identifiers:
            if identifier == row_position_snowflake_quoted_identifier:
                new_columns.append((pandas_lit(-1)).as_(identifier))
            else:
                # We use any_value to select a value in the dummy column to make sure its dtypes are
                # the same as the column in the original dataframe. This helps avoid type incompatibility
                # issues in union_all.  To ensure the results are deterministic we filter the results to
                # empty (WHERE false) so any_value returns null values but preserves the data type information.
                new_columns.append(any_value(identifier).as_(identifier))
        dummy_df = ordered_dataframe.filter(lit(False)).agg(new_columns)
        ordered_dataframe = ordered_dataframe.union_all(dummy_df)

    return _prepare_unpivot_internal(
        original_frame=original_frame,
        ordered_dataframe=ordered_dataframe,
        is_single_row=is_single_row,
        index_column_name=TRANSPOSE_INDEX,
        value_column_name=TRANSPOSE_VALUE_COLUMN,
        variable_column_name=TRANSPOSE_NAME_COLUMN,
        object_column_name=TRANSPOSE_OBJ_NAME_COLUMN,
    )


def _convert_transpose_result_snowpark_pandas_column_labels_to_pandas(
    pandas_label: Union[Hashable, tuple[Hashable]],
    cached_types: list[Optional[SnowparkPandasType]],
) -> Union[Hashable, tuple[Hashable]]:
    """
    Convert a transpose result's SnowparkPandasType column labels, if they exist, to pandas.

    When we transpose a frame where the type of at least one level of the index
    is a SnowparkPandasType, the intermediate transpose result for each column
    uses the Snowpark representation of the row label rather than the Snowpark
    pandas representation. For example, if a row has pandas label
    pd.Timedelta(7), then that row's label in Snowpark is the number 7, so the
    intermediate transpose result would have a column named 7 instead of
    pd.Timedelta(7). This method uses the index types of the original frame to
    fix the pandas labels of column levels that come from SnowparkPandasType
    index levels.

    Args
    ----
        pandas_label: transpose result label. This is a tuple if the result has
                      multiple column levels.
        cached_types: SnowparkPandasType for each index level of the original
                      frame.

    Returns
    -------
        The pandas label with levels that are instances of SnowparkPandasType
        converted to the corresponding pandas type.

    Examples
    --------

    >>> from snowflake.snowpark.modin.plugin._internal.snowpark_pandas_types import TimedeltaType


    Transposing a frame with a single timedelta index level:

    >>> _convert_transpose_result_snowpark_pandas_column_labels_to_pandas(native_pd.Timedelta(1), [TimedeltaType()])
    Timedelta('0 days 00:00:00.000000001')

    Transposing a frame with a timedelta index level and a string level:

    >>> _convert_transpose_result_snowpark_pandas_column_labels_to_pandas(("a", native_pd.Timedelta(1)), [None, TimedeltaType()])
    ('a', Timedelta('0 days 00:00:00.000000001'))

    """
    if isinstance(pandas_label, tuple):
        return tuple(
            (
                index_type.to_pandas(level_label)
                if index_type is not None
                else level_label
            )
            for index_type, level_label in zip(cached_types, pandas_label)
        )
    assert len(cached_types) == 1, (
        "Internal error: If the transpose result has a single column level, "
        + "then the input should have a single index level with a single "
        + "SnowparkPandasType."
    )
    cached_type = cached_types[0]
    return (
        cached_type.to_pandas(pandas_label) if cached_type is not None else pandas_label
    )


def clean_up_transpose_result_index_and_labels(
    original_frame: InternalFrame,
    ordered_transposed_df: OrderedDataFrame,
    transpose_name_quoted_snowflake_identifier: str,
    transpose_object_name_quoted_snowflake_identifier: str,
) -> InternalFrame:
    """
    Creates an internal frame based on the original frame and the data transposed snowpark dataframe.  This
    cleans up and normalizes the labels and index values so they conform with expectations for pandas transpose.

    Example:
        If the original frame had:
            data column labels ('a', 'x'), ('a', 'y'), ('b', 'w'), ('b', 'z') and index column values (g, h, i)
        and transposed snowpark dataframe had:
            schema ('"TRANSPOSE_OBJ_NAME"',
                '"{""0"":""g"", ""row"":0}"', '"{""0"":""h"", ""row"":1}"', '"{""0"":""i"", ""row"":2}"')
            and values for TRANSPOSE_OBJ_NAME: [0, ["a", "x"]], [1, ["a", "y"]], [2, ["b", "w"]], [3, ["b", "z"]]
        then the dataframe index is split into multi-columns and labels are cleaned up.

        The resulting frame would have (transposed indexes):
            data column labels: (g, h, i) and index column values ('a', 'x'), ('a', 'y'), ('b', 'w'), ('b', 'z')
        and normalized snowpark dataframe:
            schema ('"row_position"', '"level"', '"level_1"', '"g"', '"h"' ,'"i"')
            and values (0, a, x), (1, a, y), (2, b, w), (3, b, z) for values __row_position, level, level_1

    Args:
        original_frame: The original InternalFrame for the transpose
        ordered_transposed_df: The transposed ordered dataframe
        transpose_name_quoted_snowflake_identifier: variable name identifier from the unpivot
        transpose_object_name_quoted_snowflake_identifier: values from the unpivot

    Returns:
        The transposed InternalFrame.
    """
    # The remaining columns are the resulting output columns of the transpose, except for the TRANSPOSE_NAME_COLUMN
    # which becomes the new index of the resulting table.
    data_column_snowflake_quoted_identifiers = (
        ordered_transposed_df.projected_column_snowflake_quoted_identifiers
    )
    data_column_snowflake_quoted_identifiers.remove(
        transpose_name_quoted_snowflake_identifier
    )
    data_column_snowflake_quoted_identifiers.remove(
        transpose_object_name_quoted_snowflake_identifier
    )
    data_column_object_identifier_pairs = [
        (
            parse_object_construct_snowflake_quoted_identifier_and_extract_pandas_label(
                snowflake_quoted_identifier,
                len(original_frame.index_column_pandas_labels),
            ),
            snowflake_quoted_identifier,
        )
        for snowflake_quoted_identifier in data_column_snowflake_quoted_identifiers
    ]

    # Extract the position information that was previously serialized into the column names, then sort and
    # re-organize the column names to maintain the original ordering from the pre-transpose rows.
    data_column_object_identifier_pairs.sort(
        key=lambda obj_ident: obj_ident[0][1]["row"]
    )

    # Drop the identifiers associated with dummy column row:-1 generated from the dummy row in transpose.
    if len(data_column_object_identifier_pairs) > 0:
        if data_column_object_identifier_pairs[0][0][1]["row"] == -1:
            data_column_object_identifier_pairs.remove(
                data_column_object_identifier_pairs[0]
            )

    # If it's a single level, we store the label, otherwise we store tuple for each level.
    new_data_column_pandas_labels = [
        _convert_transpose_result_snowpark_pandas_column_labels_to_pandas(
            pandas_label, original_frame.cached_index_column_snowpark_pandas_types
        )
        for (pandas_label, _), _ in data_column_object_identifier_pairs
    ]

    new_data_column_snowflake_quoted_identifiers = [
        snowflake_quoted_identifier
        for _, snowflake_quoted_identifier in data_column_object_identifier_pairs
    ]

    # We need to split out the TRANSPOSE_OBJ_NAME_COLUMN with two cases:
    #
    # If it is a single index, the format will be [1, "employed"] and result in new columns with values:
    #       (row_position, 1), ("__level__", "employed")
    #
    # If it is a multi-index, the format will be [1, ["status", "employed"]] and result in new columns with values:
    #       (row_position, 1), ("__level_1__", "status"), ("__level_2__", "employed")
    new_index_column_pandas_labels: list[Hashable] = []
    new_index_column_snowflake_quoted_identifiers: list[str] = []
    for i, pandas_label in enumerate(original_frame.data_column_pandas_index_names):
        if is_all_label_components_none(pandas_label):
            index_label = LEVEL_LABEL
            if i >= 1:
                index_label += f"_{i}"
        else:
            index_label = pandas_label

        snowflake_quoted_identifier = (
            ordered_transposed_df.generate_snowflake_quoted_identifiers(
                pandas_labels=serialize_pandas_labels([index_label]),
                excluded=new_data_column_snowflake_quoted_identifiers
                + new_index_column_snowflake_quoted_identifiers,
            )[0]
        )

        new_index_column_pandas_labels.append(pandas_label)
        new_index_column_snowflake_quoted_identifiers.append(
            snowflake_quoted_identifier
        )

    # Extract the new row position and pandas label object from column
    # transpose_object_name_quoted_snowflake_identifier, which is an array column
    # with value [row_position, label object] like [0, "score"]. The label object
    # for multi-index can look like {"0": "A", "1": "B"} for panda label ("A", "B").

    # Generate the snowflake quoted identifier for extracted row position and pandas
    # label object columns.
    row_position_and_index_snowflake_quoted_identifier = (
        ordered_transposed_df.generate_snowflake_quoted_identifiers(
            pandas_labels=[ROW_POSITION_COLUMN_LABEL, INDEX_LABEL],
            excluded=new_data_column_snowflake_quoted_identifiers
            + new_index_column_snowflake_quoted_identifiers,
        )
    )
    pivot_with_index_select_list = [
        get(transpose_object_name_quoted_snowflake_identifier, i).as_(
            snowflake_quoted_identifier
        )
        for i, snowflake_quoted_identifier in enumerate(
            row_position_and_index_snowflake_quoted_identifier
        )
    ] + new_data_column_snowflake_quoted_identifiers

    ordered_transposed_df = ordered_transposed_df.select(pivot_with_index_select_list)

    row_position_snowflake_quoted_identifier = (
        row_position_and_index_snowflake_quoted_identifier[0]
    )
    index_snowflake_quoted_identifier = (
        row_position_and_index_snowflake_quoted_identifier[1]
    )
    # Handle the multi-index case by further parsing out each level to a separate level_# columns.
    if len(new_index_column_snowflake_quoted_identifiers) > 1:
        pivot_with_multi_index_select_list = (
            [row_position_snowflake_quoted_identifier]
            + [
                get(index_snowflake_quoted_identifier, i).as_(
                    snowflake_quoted_identifier
                )
                for i, snowflake_quoted_identifier in enumerate(
                    new_index_column_snowflake_quoted_identifiers
                )
            ]
            + new_data_column_snowflake_quoted_identifiers
        )

        ordered_transposed_df = ordered_transposed_df.select(
            pivot_with_multi_index_select_list
        )
    else:
        # If it is a single level then no more extraction is needed after separating the row position and index.
        new_index_column_snowflake_quoted_identifiers = [
            index_snowflake_quoted_identifier
        ]

    # Create new internal frame with resulting ordering column and transposed index values.
    ordered_transposed_df = ordered_transposed_df.sort(
        OrderingColumn(row_position_snowflake_quoted_identifier)
    )

    original_frame_data_column_types = (
        original_frame.cached_data_column_snowpark_pandas_types
    )
    if all(t is None for t in original_frame_data_column_types):
        new_data_column_types = None
    elif len(set(original_frame_data_column_types)) == 1:
        # unique type
        new_data_column_types = [original_frame_data_column_types[0]] * len(
            new_data_column_snowflake_quoted_identifiers
        )
    else:
        # transpose will lose the type
        new_data_column_types = None
        WarningMessage.lost_type_warning(
            "transpose",
            ", ".join(
                [
                    type(t).__name__
                    for t in set(original_frame_data_column_types)
                    if t is not None
                ]
            ),
        )

    new_internal_frame = InternalFrame.create(
        ordered_dataframe=ordered_transposed_df,
        data_column_pandas_labels=new_data_column_pandas_labels,
        data_column_pandas_index_names=original_frame.index_column_pandas_labels,
        data_column_snowflake_quoted_identifiers=new_data_column_snowflake_quoted_identifiers,
        index_column_pandas_labels=new_index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=new_index_column_snowflake_quoted_identifiers,
        data_column_types=new_data_column_types,
        index_column_types=None,
    )

    # Rename the data column snowflake quoted identifiers to be closer to pandas labels, normalizing names
    # will remove information like row position that may have temporarily been included in column names to track
    # during earlier steps.
    new_internal_frame = (
        new_internal_frame.normalize_snowflake_quoted_identifiers_with_pandas_label()
    )

    return new_internal_frame
