#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

#
# This file contains utils functions used by the groupby functionalities.
#
#

import functools
from collections.abc import Hashable
from typing import Any, Literal, Optional, Union, List

import pandas as native_pd
from pandas._typing import IndexLabel
from pandas.core.dtypes.common import is_list_like

from snowflake.snowpark._internal.type_utils import ColumnOrName
from snowflake.snowpark.column import Column as SnowparkColumn
from snowflake.snowpark.functions import (
    col,
    count,
    count_distinct,
    dense_rank,
    iff,
    rank,
    sum_distinct,
    when,
)
from snowflake.snowpark.modin.plugin._internal.join_utils import (
    join,
    InheritJoinIndex,
    JoinKeyCoalesceConfig,
)
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin._internal.ordered_dataframe import OrderedDataFrame
from snowflake.snowpark.modin.plugin._internal.utils import (
    get_distinct_rows,
    pandas_lit,
)
from snowflake.snowpark.modin.plugin._internal.resample_utils import (
    compute_resample_start_and_end_date,
    perform_resample_binning_on_frame,
    rule_to_snowflake_width_and_slice_unit,
    get_expected_resample_bins_frame,
    RULE_WEEK_TO_YEAR,
    validate_resample_supported_by_snowflake,
)
from snowflake.snowpark.modin.plugin.utils.error_message import ErrorMessage
from snowflake.snowpark.modin.plugin.compiler import snowflake_query_compiler
from snowflake.snowpark.modin.utils import hashable
from snowflake.snowpark.window import Window

BaseInternalKeyType = Union[
    "modin.pandas.Series",  # type: ignore [name-defined] # noqa: F821
    Hashable,
]

NO_GROUPKEY_ERROR = ValueError("No group keys passed!")


def validate_grouper(val: native_pd.Grouper) -> None:
    """
    Raise an exception if the grouper object has fields unsupported in Snowpark pandas.
    """
    # Pairs of parameter names + condition for parameter being invalid
    is_timegrouper = isinstance(val, native_pd.core.resample.TimeGrouper)
    # We do not validate closed/label/convention because their default values change depending
    # on the specified freq.
    unsupported_params = [
        (
            "sort",
            not val.sort if is_timegrouper else val.sort,
        ),  # defaults to True if TimeGrouper, False otherwise
        (
            "origin",
            is_timegrouper and val.origin not in ("start_day", "start"),
        ),  # start_day is the default, but we also support start
        ("offset", is_timegrouper and val.offset is not None),
        ("dropna", not val.dropna),  # defaults to True
    ]
    found_unsupported_params = [
        param for param, invalid in unsupported_params if invalid
    ]
    if len(found_unsupported_params) > 0:
        ErrorMessage.not_implemented(
            "Invalid parameter(s) passed to Grouper object: "
            + ", ".join(found_unsupported_params)
            + "\nSnowpark pandas does not yet support any of the following parameters in Grouper objects: "
            + ", ".join(param for param, _ in unsupported_params)
        )


def validate_groupby_resample_supported_by_snowflake(
    resample_kwargs: dict[str, Any]
) -> None:
    """
    Checks whether execution with Snowflake engine is available for groupby resample operation.

    Parameters:
    ----------
    resample_kwargs : Dict[str, Any]
        keyword arguments of Resample operation. rule, axis, axis, etc.

    Raises
    ------
    NotImplementedError
        Raises a NotImplementedError if a keyword argument of resample has an
        unsupported parameter-argument combination.
    """
    # groupby resample specific validation
    rule = resample_kwargs.get("rule")
    _, slice_unit = rule_to_snowflake_width_and_slice_unit(rule)
    if slice_unit in RULE_WEEK_TO_YEAR:
        ErrorMessage.not_implemented(
            f"Groupby resample with rule offset {rule} is not yet implemented."
        )
    validate_resample_supported_by_snowflake(resample_kwargs)
    return None


def is_groupby_value_label_like(val: Any) -> bool:
    """
    Check if the groupby value can be treated as pandas label.
    """
    from modin.pandas import Series

    if isinstance(val, native_pd.Grouper):
        validate_grouper(val)

    # A pandas label is a hashable, and we exclude the callable, and Series which are
    # by values that should not be handled as pandas label of the dataframe.
    # Grouper objects that specify either `key`, `level`, or `freq` are accepted, as we can convert
    # a `level` or `freq` into an index label.
    return (
        hashable(val)
        and not callable(val)
        and not isinstance(val, Series)
        and not (
            isinstance(val, native_pd.Grouper)
            and val.key is None
            and val.level is None
            and val.freq is None
        )
    )


def get_column_label_from_grouper(
    frame: InternalFrame, grouper: native_pd.Grouper
) -> Hashable:
    """
    Convert a Grouper object to a list column label.

    The constructor of the Grouper object will already have verified that `by` and `level` are
    not simultaneously set.
    """
    if grouper.level is not None:
        # Groupers can only accept scalar level
        if is_list_like(grouper.level):
            raise ValueError("`level` parameter of Grouper must be scalar")
        # Must always return exactly one level (will raise KeyError internally if specified level is invalid)
        return frame.get_pandas_labels_for_levels(
            frame.parse_levels_to_integer_levels([grouper.level], allow_duplicates=True)
        )[0]
    if grouper.key is None:
        if grouper.freq is not None:
            # If freq is specified, it implicitly references the first index column
            # TODO verify if this is the case for dataframes
            return frame.index_column_pandas_labels[0]
        # in this scenario, pandas raises the very unhelpful "TypeError: 'NoneType' is not callable"
        raise ValueError("Grouper must have key, freq, or level")
    return grouper.key


def get_column_labels_from_by_list(
    frame: InternalFrame, by_list: List[Hashable]
) -> List[Hashable]:
    """
    Filter labels in the list that can be mapped to a column label.

    If any element of the list is an instance of pd.Grouper with no level, then its key field is used.
    """
    return [
        get_column_label_from_grouper(frame, val)
        if isinstance(val, native_pd.Grouper)
        else val
        for val in by_list
        if is_groupby_value_label_like(val)
    ]


def check_is_groupby_supported_by_snowflake(
    by: Any, level: Optional[IndexLabel], axis: int
) -> bool:
    """
    Check if execution with snowflake engine is available for the groupby operations.

    Args:
        by: mapping, callable, label, pd.Grouper, SnowSeries, list of such.
            Used to determine the groups for the groupby.
        level: Optional[IndexLabel]. The IndexLabel can be int, level name, or sequence of such.
            If the axis is a MultiIndex (hierarchical), group by a particular level or levels.
        axis : 0, 1
    Returns:
        bool
            Whether operations can be executed with snowflake sql engine.
    """
    # snowflake execution is not support for groupby along rows
    def check_non_grouper_supported(
        by: Any, level: Optional[IndexLabel], axis: int
    ) -> bool:
        """
        Helper function checking if the passed arguments are supported if `by` is not a `pd.Grouper` object.
        """
        if axis != 0:
            return False

        if by is not None and level is not None:
            # the typical usage for by and level both configured is when dict is used as by items. For example:
            # {"one": 0, "two": 0, "three": 1}, which maps label "one" and "two" to level 0, and "three"
            # to level 1. For detailed example, please check test_groupby_level_mapper.
            # Since we do not have distributed support for by as a mapper, we do not provide distributed support
            # when both by and level is configured for now.
            return False

        # Check if by is already a list, if not, construct a list of the element for uniform process in later step.
        # Note that here we check list type specifically instead of is_list_like because tuple (('a', 'b')) and
        # SnowSeries are also treated as list like, but we want to construct a list of the whole element like [('a', 'b')],
        # instead of converting the element to list type like ['a', 'b'] for checking.
        by_list = by if isinstance(by, list) else [by]
        # validate by columns, the distributed implementation only supports columns that
        # is columns belong to the current dataframe, which is represented as pandas hashable label.
        # Please notice that callable is also a hashable, so a separate check of callable
        # is applied.
        if any(not is_groupby_value_label_like(o) for o in by_list):
            return False
        return True

    if isinstance(by, native_pd.Grouper):
        # Per pandas docs, level and axis arguments of the grouper object take precedence over
        # level and axis passed explicitly.
        validate_grouper(by)
        return check_non_grouper_supported(
            by.key,
            by.level if by.level is not None else level,
            by.axis if by.axis is not None else axis,
        )
    else:
        return check_non_grouper_supported(by, level, axis)


def validate_groupby_columns(
    query_compiler: "SnowflakeQueryCompiler",  # type: ignore[name-defined] # noqa: F821
    by: Union[BaseInternalKeyType, list[BaseInternalKeyType]],
    axis: int,
    level: Optional[IndexLabel],
) -> None:
    """
    Check whether the groupby items are valid. Detailed check is only available along column-wise (axis=0),
    row-wise (axis = 1) calls fallback today, detailed check will be done within the fallback call by pandas.

    Raises:
        ValueError if no by/key item is passed
        KeyError if a hashable label in by (groupby items) can not be found in the current dataframe
        ValueError if more than one column can be found for the groupby item
        ValueError or IndexError if no corresponding level can be found in the current dataframe
    """
    if by is not None and level is not None:
        # no distributed implementation support is available when both by and level are configured
        return

    if by is not None:  # perform check on by items
        # convert by to list if it is not a list for easy process, and also calculate is_external_by.
        # The is_external_by is used to indicate whether the length of the by if by is a list is the
        # same as the length of the dataframe along the axis. For example, if we have a dataframe with 4
        # rows (axis=0), if the by items is a list with 4 elements like [1, 1, 2, 2], the is_external_by
        # is True, otherwise it is false. When the length is the same, pandas views the by item as a valid
        # external array, and skip the validation check. Even if the list may contain pandas label which will
        # be treated as groupby column later.
        # 'is_external_by' is False for following cases:
        # 1. If 'by' is not a list.
        # 2. 'by' is a list and all elements in it are valid internal labels.
        # 3. OR 'by' is a list and length does not match with length of dataframe.

        # If the list includes pd.Grouper objects, some may specify a by label while others specify a level.

        if not isinstance(by, list):
            by_list = [by]
            is_external_by = False
        else:
            by_list = by
            _, internal_by = groupby_internal_columns(
                query_compiler._modin_frame, by_list
            )
            if len(internal_by) == len(by_list):
                # If all elements in by_list are valid internal labels we don't need to
                # check length of by_list against length of dataframe.
                is_external_by = False
            else:
                is_external_by = query_compiler.get_axis_len(axis) == len(by_list)

        if len(by_list) == 0:
            raise NO_GROUPKEY_ERROR

        # This check is only applied when is_external_by is False, this is the same as pandas behavior.
        # axis is 1 currently calls fallback, we skip the client side check for now.
        if axis == 0 and not is_external_by:
            # get the list of groupby item that is hashable but not a callable
            by_label_list = get_column_labels_from_by_list(
                query_compiler._modin_frame, by_list
            )
            internal_frame = query_compiler._modin_frame

            for pandas_label, snowflake_quoted_identifiers in zip(
                by_label_list,
                internal_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                    by_label_list
                ),
            ):
                if len(snowflake_quoted_identifiers) == 0:
                    if pandas_label is None:
                        # This is to stay consistent with the pandas error, if no corresponding
                        # column has pandas label None, it raises type error since None is not
                        # a callable also.
                        raise TypeError("'NoneType' object is not callable")
                    raise KeyError(pandas_label)
                # pandas does not allow one group by column name to be mapped to multiple
                # columns, and ValueError is raised under such situation
                if len(snowflake_quoted_identifiers) > 1:
                    raise ValueError(f"Grouper for '{pandas_label}' not 1-dimensional")
    elif level is not None:  # perform validation on level
        level_list = [level] if not isinstance(level, (tuple, list)) else level
        if len(level_list) == 0:
            raise NO_GROUPKEY_ERROR
        if len(level_list) > 1 and not query_compiler.is_multiindex(axis=axis):
            raise ValueError("multiple levels only valid with MultiIndex")

        # call parse_level_to_integer_level to perform check that all levels are valid
        # levels in the current dataframe. Note that parse_level_to_integer_level is only
        # called for validation purpose, the returned result (the corresponding integer
        # levels) is not used.
        _ = query_compiler._modin_frame.parse_levels_to_integer_levels(
            level, allow_duplicates=True
        )


def groupby_internal_columns(
    frame: InternalFrame,
    by: Union[BaseInternalKeyType, list[BaseInternalKeyType]],
) -> tuple[list[BaseInternalKeyType], list[Hashable]]:
    """
    Extract internal columns from by argument of groupby. The internal
    columns are columns from the current dataframe.

    Parameters
    ----------
    frame: the internal frame to apply groupby on
    by : Snowpark pandas Series, column/index label or list of the above

    Returns
    -------
    by : list of Snowpark pandas Series, column or index label
    internal_by : list of str
        List of internal column name to be dropped during groupby.
    """

    internal_by: list[Hashable]
    return_by: list[BaseInternalKeyType]
    if not isinstance(by, list):
        by_list = [by] if by is not None else []
    else:
        by_list = by

    # Extract keys from Grouper objects, which must each specify either key or level
    by_list = [
        get_column_label_from_grouper(frame, val)
        if isinstance(val, native_pd.Grouper)
        else val
        for val in by_list
    ]

    # this part of code relies on the fact that all internal by columns have been
    # processed into column labels. SnowSeries that does not belong to the current
    # dataframe remains as SnowSeries, and not counted as internal groupby columns.
    internal_by = [
        o
        for o in by_list
        if hashable(o)
        and o in frame.data_column_pandas_labels + frame.index_column_pandas_labels
    ]
    return_by = by_list
    return return_by, internal_by


def get_groups_for_ordered_dataframe(
    ordered_dataframe: OrderedDataFrame,
    groupby_snowflake_quoted_identifiers: list[str],
) -> OrderedDataFrame:
    """
    Get all distinct groups for the dataframe.

    Args:
        ordered_dataframe: OrderedDataFrame. Dataframe to extract groups.
        groupby_snowflake_quoted_identifiers: quoted identifiers for columns to group on for extracting
            the distinct groups.

    Returns:
        OrderedDataFrame contains only the groupby columns with distinct group values.
    """
    return get_distinct_rows(
        ordered_dataframe.select(groupby_snowflake_quoted_identifiers)
    )


def extract_groupby_column_pandas_labels(
    query_compiler: "snowflake_query_compiler.SnowflakeQueryCompiler",
    by: Any,
    level: Optional[IndexLabel],
) -> Optional[list[Hashable]]:

    """
    Extracts the groupby pandas labels from the by and level parameters and returns as a list.
    Parameters
    ----------
    query_compiler: the query compiler of the internal frame to group on.
    by: mapping, series, callable, lable, pd.Grouper, BaseQueryCompiler, list of such
        Used to determine the groups for the groupby.
    level: int, level name, or sequence of such, default None. If the axis is a
        MultiIndex(hierarchical), group by a particular level or levels. Do not specify
        both by and level.
    """
    internal_frame = query_compiler._modin_frame

    # Distributed implementation support is currently unavailable when both by and level are configured,
    # and the check is done by check_groupby_agg_distribute_execution_capability_by_args. Once reach here,
    # only one of by or level can be None.
    #
    # Get the groupby columns (by_list) and record the by columns that are index columns of the original
    # dataframe (index_by_columns).
    # The index_by_columns is used for as_index = False, when as_index is False, it drops all by columns
    # that are index columns in the original dataframe, but still retains all by columns that are data columns
    # from originally data frame. For example:
    # for a dataframe with index = [`A`, `B`], data = [`C`, `D`, `E`],
    #  with groupby([`A`, `C`], as_index=True).max(), the result will have index=[`A`, `C`], data=[`D`, `E`]
    #  with groupby([`A`, `C`], as_index=False).max(), the result will have index=[None] (default range index),
    #  data=[`C`, `D`, `E`], columns `A` is dropped, and `C` is retained
    if by is not None:
        # extract the internal by columns which are groupby columns from the current dataframe
        # internal_by: a list of column labels that are columns from the current dataframe
        # by: all by columns in the form of list, contains both internal and external groupby columns
        # when len(by) > len(internal_by) that means there are external groupby columns
        by_list, internal_by = groupby_internal_columns(internal_frame, by)

        # when len(by_list) > len(internal_by), there are groupby columns that do not belong
        # to the current dataframe. we do not support this case.
        if len(by_list) > len(internal_by):
            return None
        by_list = internal_by
    elif level is not None:  # if by is None, level must not be None
        int_levels = internal_frame.parse_levels_to_integer_levels(
            level, allow_duplicates=True
        )
        by_list = internal_frame.get_pandas_labels_for_levels(int_levels)
    else:
        # we should never reach here
        raise ValueError("Neither level or by is configured!")  # pragma: no cover
    return by_list


def resample_and_extract_groupby_column_pandas_labels(
    query_compiler: "snowflake_query_compiler.SnowflakeQueryCompiler",
    by: Any,
    level: Optional[IndexLabel],
    *,
    skip_resample: bool = False,
) -> tuple[
    "snowflake_query_compiler.snowflake_query_compiler", Optional[list[Hashable]]
]:
    """
    Extract the pandas labels of grouping columns specified by the `by` and `level` parameters.

    If `by` is a list and any item is a `pd.Grouper` object specifying a `freq`, then a new column
    will be added with the resampled values of the index. If the operation is an upsample, then
    NULL values are interpolated in the other columns.

    Parameters
    ----------
    query_compiler: the query compiler of the internal frame to group on.
    by: mapping, series, callable, lable, pd.Grouper, BaseQueryCompiler, list of such
        Used to determine the groups for the groupby.
    level: int, level name, or sequence of such, default None. If the axis is a
        MultiIndex(hierarchical), group by a particular level or levels. Do not specify
        both by and level.
    skip_resample: bool, default False
        If specified, do not peform resampling, and only extract column labels from the groupers.

    Returns
    -------
    tuple[SnowflakeQueryCompiler, Optional[list[Hashable]]]
        A pair of (query compiler, grouping labels). The returned query compiler may be the same
        as the original passed in, depending on whether or not resampling was performed and a new
        column added.
    """
    frame = query_compiler._modin_frame

    def find_resample_columns(
        frame: InternalFrame, by: Any
    ) -> tuple[Any, list[tuple[Hashable, native_pd.Grouper]]]:
        """
        Identify which columns need to be resampled.

        Returns a pair with two items:
        - The input `by` list with any datetime Grouper objects replaced by a label for the resampled column.
        - A list of (original column label, Grouper) tuples.

        If the by argument is a Series, function, or None, then it is returned directly, and the returned
        resample column list is empty.

        TODO: if we support other time Grouper parameters (offset, closed, convention), then these
        will need to be passed as well.
        """
        # If by is None, then assume `level` was passed. This case is handled by extract_column_pandas_labels
        # We currently do not support passing `freq` directly to the groupby call (only via Grouper objects).
        # Also short-circuit if the passed object is a Snowpark pandas Series or a callable, as
        # those cannot be treated as column labels.
        if by is None or (
            not isinstance(by, list) and not is_groupby_value_label_like(by)
        ):
            return by, []
        resample_list = []
        # Use an explicit list check instead of is_list_like to allow for referencing
        # multiindex labels as tuples.
        if isinstance(by, list):
            by_list = by
        else:
            by_list = [by]
        new_by_list: List[Any] = []
        for by_item in by_list:
            if (
                not skip_resample
                and isinstance(by_item, native_pd.Grouper)
                and by_item.freq is not None
            ):
                if by_item.level is not None:
                    int_levels = frame.parse_levels_to_integer_levels(
                        [by_item.level],
                        allow_duplicates=True,
                    )
                    col_label = frame.get_pandas_labels_for_levels(int_levels)[0]
                elif by_item.key is None:
                    if by_item.freq is None:
                        raise ValueError("Grouper must have key, freq, or level")
                    else:
                        # If a freq is specified without a key, then take the first index label
                        col_label = frame.index_column_pandas_labels[0]
                else:
                    col_label = by_item.key
                resample_list.append((col_label, by_item))
                new_by_list.append(col_label)
            else:
                new_by_list.append(by_item)
        return new_by_list, resample_list

    by, to_resample = find_resample_columns(frame, by)
    if len(to_resample) > 0:
        original_labels, groupers = zip(*to_resample)
        identifiers_to_resample = [
            identifier[0]
            for identifier in frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                original_labels, include_index=True
            )
        ]
        if len(set(identifiers_to_resample)) != len(identifiers_to_resample):
            # Because we need to return a label, we don't currently support resampling the same column
            # multiple times as we replace the original column.
            ErrorMessage.not_implemented(
                "Resampling the same column multiple times is not yet supported in Snowpark pandas."
            )
        # 1. For every column, determine the start and end dates of the resample intervals.
        start_and_end_dates = {
            identifier: compute_resample_start_and_end_date(
                frame,
                identifier,
                grouper.freq,
                origin_is_start_day=grouper.origin == "start_day",
            )
            for identifier, grouper in zip(identifiers_to_resample, groupers)
        }
        # 2. For every column to resample,
        #    a. Relabel the original column with values converted to bin edges.
        #    b. Interpolate other columns with empty values if any column is upsampled (this incurs a join).
        # This breaks if the same grouping column is resampled twice, but this edge case is annoying to support.
        for original_identifier, original_label, grouper in zip(
            identifiers_to_resample, original_labels, groupers
        ):
            freq = grouper.freq
            slice_width, slice_unit = rule_to_snowflake_width_and_slice_unit(freq)
            start_date, end_date = start_and_end_dates[original_identifier]
            binned_frame = perform_resample_binning_on_frame(
                frame,
                original_identifier,
                start_date,
                slice_width,
                slice_unit,
                resample_output_col_identifier=original_identifier,
            )
            # Manual copy-paste of some code from resample_utils.fill_missing_resample_bins_for_frame,
            # but without any assumptions on whether the column is an index
            expected_resample_bins_frame = get_expected_resample_bins_frame(
                freq, start_date, end_date, index_label=original_label
            )
            joined_frame = join(
                left=binned_frame,
                right=expected_resample_bins_frame,
                # Perform an outer join to preserve additional index columns.
                how="outer",
                # identifier might get mangled by binning operation; look it up again
                left_on=binned_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                    [original_label]
                )[
                    0
                ],
                right_on=expected_resample_bins_frame.index_column_snowflake_quoted_identifiers,
                # Inherit the index from both sides to preserve additional index columns if
                # the original frame had a MultiIndex.
                # The resampled column will be replaced during the join operation by coalescing
                # from the right in order to support upsampling.
                join_key_coalesce_config=[JoinKeyCoalesceConfig.RIGHT],
                inherit_join_index=InheritJoinIndex.FROM_BOTH,
            ).result_frame
            # After the join, the index columns may be out of order, so we need to look up the appropriate identifiers
            # instead of accessing them directly.
            new_index_identifiers = [
                identifier[0]
                for identifier in joined_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                    binned_frame.index_column_pandas_labels
                )
            ]
            frame = InternalFrame.create(
                ordered_dataframe=joined_frame.ordered_dataframe,
                data_column_pandas_labels=binned_frame.data_column_pandas_labels,
                data_column_snowflake_quoted_identifiers=binned_frame.data_column_snowflake_quoted_identifiers,
                index_column_pandas_labels=binned_frame.index_column_pandas_labels,
                index_column_snowflake_quoted_identifiers=new_index_identifiers,
                data_column_pandas_index_names=binned_frame.data_column_pandas_index_names,
                data_column_types=binned_frame.cached_data_column_snowpark_pandas_types,
                index_column_types=binned_frame.cached_index_column_snowpark_pandas_types,
            )
        query_compiler = snowflake_query_compiler.SnowflakeQueryCompiler(frame)
    return query_compiler, extract_groupby_column_pandas_labels(
        query_compiler, by, level
    )


# TODO: SNOW-939239 clean up fallback logic
def get_frame_with_groupby_columns_as_index(
    query_compiler: "snowflake_query_compiler.SnowflakeQueryCompiler",
    by: Any,
    level: Optional[Union[Hashable, int]],
    dropna: bool,
) -> Optional["snowflake_query_compiler.SnowflakeQueryCompiler"]:
    """
    Returns a new dataframe with the following properties:
    1) The groupby columns are used as the new index columns
    2) An index column of the original dataframe that doesn't belong to the new dataframe is dropped
    3) All data columns in the original dataframe are retained even if it becomes an index column
    4) If a grouping column is a Datetime/Timestamp index and a pd.Grouper object is passed with
       a `freq` argument, then a new column is added with the adjusted bins.

    df = pd.DataFrame({"A": [0, 1, 2], "B": [2, 1, 1], "C": [2, 2, 0], "D": [3,4,5]})
    df = df.set_index(['A', 'B'])
          C  D
    A  B
    0  2  2  3
    1  1  2  4
    2  1  0  5

    get_frame_with_groupby_columns_as_index(query_compiler, ['A', 'C'], None, True)

    the frame returned to the caller would be:
          C  D
    A  C
    0  2  2  3
    1  2  2  4
    2  0  0  5

    Example with a pd.Grouper with `freq` specified:

    >>> dates = pd.date_range("2000-10-01 23:00:00", "2000-10-01 23:16:00", freq='4min')
    >>> ts = pd.Series(np.arange(len(dates)), index=dates)
    >>> ts
    2000-10-01 23:00:00    0
    2000-10-01 23:04:00    1
    2000-10-01 23:08:00    2
    2000-10-01 23:12:00    3
    2000-10-01 23:16:00    4
    Freq: None, dtype: int64

    (Snowpark pandas drops the freq field)

    Upsampling will fill other columns with NULL values, under the assumption that they will be
    coalesced away by the resulting groupby operation:

    get_frame_with_groupby_columns_as_index(ts._query_compiler, pd.Grouper(freq="2min"), None, True)
    +----------------------+-------------+
    |       __index__      | __reduced__ |
    +----------------------+-------------+
    |  2000-10-01 23:00:00 |           0 |
    |  2000-10-01 23:02:00 |        NULL |
    |  2000-10-01 23:04:00 |           1 |
    |  2000-10-01 23:06:00 |        NULL |
    |  2000-10-01 23:08:00 |           2 |
    |  2000-10-01 23:10:00 |        NULL |
    |  2000-10-01 23:12:00 |           3 |
    |  2000-10-01 23:14:00 |        NULL |
    |  2000-10-01 23:16:00 |           4 |
    +----------------------+-------------+

    Conversely, downsampling will forward-fill values into the resampled time column to represent
    that they belong to the same bin. Note that in this example, the bins are shifted because the
    Grouper defaults to origin="start_date".

    get_frame_with_groupby_columns_as_index(ts._query_compiler, pd.Grouper(freq="8min"), None, True)
    +----------------------+-------------+
    |       __index__      | __reduced__ |
    +----------------------+-------------+
    |  2000-10-01 22:56:00 |           0 |
    |  2000-10-01 23:04:00 |           1 |
    |  2000-10-01 23:04:00 |           2 |
    |  2000-10-01 23:12:00 |           3 |
    |  2000-10-01 23:12:00 |           4 |
    +----------------------+-------------+

    Parameters
    ----------
    query_compiler: the query compiler of the internal frame to group on.
    by: mapping, series, callable, label, pd.Grouper, BaseQueryCompiler, list of such
        Used to determine the groups for the groupby.
    level: int, level name, or sequence of such, default None. If the axis is a
        MultiIndex(hierarchical), group by a particular level or levels. Do not specify
        both by and level.
    dropna: bool. if dropna is set to True, the returned dataframe will exclude rows
        that contain NA values.

    Returns
    -------
    SnowflakeQueryCompiler that contains a new internal frame. The function
    will return None when both level and by are configured.
    """

    from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
        SnowflakeQueryCompiler,
    )

    query_compiler, by_list = resample_and_extract_groupby_column_pandas_labels(
        query_compiler, by, level
    )

    if by_list is None:
        return None

    # reset the index for the frame to the groupby columns, and drop off the
    # index columns that are not used as groupby columns (append = False), also
    # retains the original data column if it is used as groupby columns (drop = False).
    qc = query_compiler.set_index_from_columns(by_list, drop=False, append=False)

    internal_frame = qc._modin_frame
    ordered_dataframe = internal_frame.ordered_dataframe

    # drop the rows if any value in groupby key is NaN
    if dropna:
        ordered_dataframe = ordered_dataframe.dropna(
            subset=internal_frame.index_column_snowflake_quoted_identifiers
        )

    return SnowflakeQueryCompiler(
        InternalFrame.create(
            ordered_dataframe=ordered_dataframe,
            index_column_pandas_labels=internal_frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=internal_frame.index_column_snowflake_quoted_identifiers,
            data_column_pandas_labels=internal_frame.data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers=internal_frame.data_column_snowflake_quoted_identifiers,
            data_column_pandas_index_names=internal_frame.data_column_pandas_index_names,
            data_column_types=internal_frame.cached_data_column_snowpark_pandas_types,
            index_column_types=internal_frame.cached_index_column_snowpark_pandas_types,
        )
    )


def make_groupby_rank_col_for_method(
    col_ident: str,
    by_list: list[str],
    method: Literal["min", "first", "dense", "max", "average"],
    na_option: Literal["keep", "top", "bottom"],
    ascending: bool,
    pct: bool,
    ordering_column_identifiers: list[str],
    count_val: ColumnOrName,
    dropna: bool,
) -> SnowparkColumn:
    """
    Helper function to get the rank Snowpark Column for method parameters {"min", "first", "dense", "max", "average"} and
    na_option parameters {"keep", "top", "bottom"}.

    Parameters
    ----------
    col_ident : str
        Column quoted identifier
    by_list: List[str]
        List of column quoted identifiers used to group by
    method: str
        Rank method value from {"min", "first", "dense", "max", "average"}
    na_option: str
        Rank na_option value from {"keep", "top", "bottom"}
    ascending: bool
        Whether the elements should be ranked in ascending order.
    pct: bool
        Whether to display the returned rankings in percentile form.
    ordering_column_identifiers: List[str]
        List of ordering column quoted identifiers to get row value
    count_val: str
        Ordering column quoted identifier to get count value
    dropna: bool
        Whether NA values should be dropped.


    Returns
    -------
    Column
        The SnowparkColumn corresponding to the rank column.
    """

    # When na_option is 'top', null values are assigned the lowest rank. They need to be sorted before
    # non-null values.
    # For all other na_option {'keep', 'bottom'}, null values can be sorted after non-null values.
    if ascending:
        if na_option == "top":
            col_ident_value = col(col_ident).asc_nulls_first()
        else:
            col_ident_value = col(col_ident).asc_nulls_last()
    else:
        # If ascending is false, need to sort column in descending order
        if na_option == "top":
            col_ident_value = col(col_ident).desc_nulls_first()
        else:
            col_ident_value = col(col_ident).desc_nulls_last()

    # use Snowflake DENSE_RANK function when method is 'dense'.
    if method == "dense":
        rank_func = dense_rank()
    else:  # methods 'min' and 'first' use RANK function
        rank_func = rank()

    # We want to calculate the rank within the ordered group of column values
    order_by_list = [col_ident_value]
    # When method is 'first', rank is assigned in order of the values appearing in the column.
    # So we need to also order by the row position value.
    if method == "first":
        order_by_list.extend(ordering_column_identifiers)
    # For na_option {'keep', 'bottom'}, the rank column is calculated with the specified rank function and
    # the order by clause
    rank_col = rank_func.over(Window.partition_by(by_list).order_by(order_by_list))

    if dropna:
        dropna_cond = functools.reduce(
            lambda combined_col, col: combined_col | col,
            map(
                lambda by_snowflake_quoted_identifier: col(
                    by_snowflake_quoted_identifier
                ).is_null(),
                by_list,
            ),
        )

        rank_col = iff(dropna_cond, pandas_lit(None), rank_col)

    if method == "max":
        rank_col = rank_col - 1 + count_val

    if method == "average":
        rank_col = (2 * rank_col - 1 + count_val) / 2

    # For na_option 'keep', if the value is null then we assign it a null rank
    if na_option == "keep":
        rank_col = when(col(col_ident).is_null(), None).otherwise(rank_col)

    if pct:
        window = (
            Window.partition_by(by_list)
            .order_by(col_ident_value)
            .rows_between(Window.UNBOUNDED_PRECEDING, Window.UNBOUNDED_FOLLOWING)
        )
        if method == "dense":
            # dense rank uses the number of distinct values in column for percentile denominator to make sure rank
            # scales to 100% while non-dense rank uses the total number of values for percentile denominator.
            if na_option == "keep":
                # percentile denominator for dense rank is the number of distinct non-null values in the column
                total_cols = count_distinct(col(col_ident)).over(window)
            else:
                # percentile denominator for dense rank is the distinct values in a column including nulls
                total_cols = (count_distinct(col(col_ident)).over(window)) + (
                    sum_distinct(iff(col(col_ident).is_null(), 1, 0)).over(window)
                )
        else:
            if na_option == "keep":
                # percentile denominator for rank is the number of non-null values in the column
                total_cols = count(col(col_ident)).over(window)
            else:
                # percentile denominator for rank is the total number of values in the column including nulls
                total_cols = count("*").over(window)
        rank_col = rank_col / total_cols
    return rank_col


def fill_missing_groupby_resample_bins_for_frame(
    frame: InternalFrame,
    rule: str,
    by_list: list,
    orig_datetime_index_col_label: str,
    datetime_index_col_identifier: str,
) -> InternalFrame:
    """
    Returns a new InternalFrame created using 2 rules.
    1. Missing resample bins in `frame`'s DatetimeIndex column will be created.
    2. Missing rows in data column will be filled with `None`.

    Parameters:
    ----------
    frame : InternalFrame
        A frame with a single DatetimeIndex column.

    rule : str
        The offset string or object representing target conversion.

    by_list : list
        The list of column labels to group by.

    orig_datetime_index_col_label : str
        The original DatetimeIndex column label.

    datetime_index_col_identifier : str
        The DatetimeIndex column quoted identifier

    Returns
    -------
    frame : InternalFrame
        A new internal frame with no missing rows in the resample operation.
    """
    from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
        SnowflakeQueryCompiler,
    )

    # For example, if we have the following frame with rule='3min', we will fill in the missing
    # resample bins for each group of by_list by adding index (0, 2000-01-01 00:03:00) and (5, 2000-01-01 00:03:00)

    #                        b  c
    # a index
    # 0 2000-01-01 00:00:00  2  4
    #   2000-01-01 00:06:00  1  2
    # 5 2000-01-01 00:00:00  1  2
    #   2000-01-01 00:06:00  1  2

    unique_by_idx_vals = (
        frame.index_columns_pandas_index()
        .droplevel(orig_datetime_index_col_label)
        .unique()
        .to_list()
    )
    subframes = {}
    sub_qcs = []

    for value in unique_by_idx_vals:
        # value is a tuple when there are multiple groupby columns
        if isinstance(value, tuple):
            col_list = [
                col(frame.index_column_snowflake_quoted_identifiers[i])
                == pandas_lit(value[i])
                for i in range(len(by_list))
            ]
        else:
            col_list = [
                col(frame.index_column_snowflake_quoted_identifiers[0])
                == pandas_lit(value)
            ]

        # This creates the filter expression col_1 == value_1 & col_2 == value_2 & ... col_n == value_n
        # for cols in col_list. This is used to filter the frame to get the rows with the specific
        # index col values.
        filter_cond = functools.reduce(lambda x, y: x & y, col_list)
        subframes[value] = frame.filter(filter_cond)
        start_date, end_date = compute_resample_start_and_end_date(
            subframes[value],
            datetime_index_col_identifier,
            rule,
        )
        expected_resample_bins_sub_frame = get_expected_resample_bins_frame(
            rule, start_date, end_date
        )

        if isinstance(value, tuple):
            for i in range(len(by_list)):
                expected_resample_bins_sub_frame = (
                    expected_resample_bins_sub_frame.append_column(
                        by_list[i], pandas_lit(value[i])
                    )
                )
        else:
            expected_resample_bins_sub_frame = (
                expected_resample_bins_sub_frame.append_column(
                    by_list[0], pandas_lit(value)
                )
            )

        qc_subframe = SnowflakeQueryCompiler(
            expected_resample_bins_sub_frame
        ).reset_index()
        new_idx_labels = (
            by_list + expected_resample_bins_sub_frame.index_column_pandas_labels
        )
        new_idx_qc = qc_subframe.set_index(new_idx_labels)

        sub_qcs.append(new_idx_qc)
    concat_qc_idx = sub_qcs[0].concat(axis=0, other=sub_qcs[1:])

    # Join with multi_expected_resample_bins_snowpark_frame to fill in missing resample bins.
    multi_expected_resample_bins_snowpark_frame = concat_qc_idx._modin_frame
    joined_frame = join(
        frame,
        multi_expected_resample_bins_snowpark_frame,
        how="right",
        left_on=frame.index_column_snowflake_quoted_identifiers,
        right_on=multi_expected_resample_bins_snowpark_frame.index_column_snowflake_quoted_identifiers,
        sort=False,
        # To match native pandas behavior, join index columns are coalesced.
        inherit_join_index=InheritJoinIndex.FROM_RIGHT,
    ).result_frame

    # Ensure data_column_pandas_index_names is correct.
    return InternalFrame.create(
        ordered_dataframe=joined_frame.ordered_dataframe,
        data_column_pandas_labels=frame.data_column_pandas_labels,
        data_column_snowflake_quoted_identifiers=frame.data_column_snowflake_quoted_identifiers,
        index_column_pandas_labels=frame.index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=joined_frame.index_column_snowflake_quoted_identifiers,
        data_column_pandas_index_names=frame.data_column_pandas_index_names,
        data_column_types=frame.cached_data_column_snowpark_pandas_types,
        index_column_types=frame.cached_index_column_snowpark_pandas_types,
    )
