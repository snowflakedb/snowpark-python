#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
#
# This file contains utils functions used by the groupby functionalities.
#
#

import functools
from collections.abc import Hashable
from typing import Any, Literal, Optional, Union

import pandas as native_pd
from pandas._typing import IndexLabel

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
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin._internal.ordered_dataframe import OrderedDataFrame
from snowflake.snowpark.modin.plugin._internal.utils import (
    get_distinct_rows,
    pandas_lit,
)
from snowflake.snowpark.modin.plugin.compiler import snowflake_query_compiler
from snowflake.snowpark.modin.utils import hashable
from snowflake.snowpark.window import Window

BaseInternalKeyType = Union[
    "snowflake.snowpark.modin.pandas.series.Series",  # type: ignore [name-defined] # noqa: F821
    Hashable,
]

NO_GROUPKEY_ERROR = ValueError("No group keys passed!")


def is_groupby_value_label_like(val: Any) -> bool:
    """
    Check if the groupby value can be treated as pandas label.
    """
    from snowflake.snowpark.modin.pandas.series import Series

    # A pandas label is a hashable, and we exclude the callable, Series and Grouper, which are
    # by values that should not be handled as pandas label of the dataframe.
    return (
        hashable(val)
        and (not callable(val))
        and not (isinstance(val, Series))
        and not (isinstance(val, native_pd.Grouper))
    )


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
            by_label_list = list(filter(is_groupby_value_label_like, by_list))
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

    by_list = extract_groupby_column_pandas_labels(query_compiler, by, level)

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
