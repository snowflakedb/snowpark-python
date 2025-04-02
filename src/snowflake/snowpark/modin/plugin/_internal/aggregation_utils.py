#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

#
# This file contains utils functions used by aggregation functions.
#
import functools
from collections import defaultdict
from collections.abc import Hashable, Iterable
from functools import partial
from inspect import getmembers
from types import BuiltinFunctionType, MappingProxyType
from typing import Any, Callable, Literal, Mapping, NamedTuple, Optional, Union

import numpy as np
from pandas._typing import AggFuncType, AggFuncTypeBase
from pandas.core.dtypes.common import (
    is_dict_like,
    is_list_like,
    is_named_tuple,
    is_numeric_dtype,
    is_scalar,
)

from snowflake.snowpark._internal.type_utils import ColumnOrName
from snowflake.snowpark.column import CaseExpr, Column as SnowparkColumn
from snowflake.snowpark.functions import (
    Column,
    array_agg,
    array_construct,
    array_construct_compact,
    array_contains,
    array_flatten,
    array_max,
    array_min,
    array_position,
    builtin,
    cast,
    coalesce,
    col,
    count,
    count_distinct,
    get,
    greatest,
    iff,
    is_null,
    least,
    listagg,
    lit,
    max as max_,
    mean,
    median,
    min as min_,
    parse_json,
    skew,
    stddev,
    stddev_pop,
    sum as sum_,
    trunc,
    var_pop,
    variance,
    when,
)
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin._internal.ordered_dataframe import (
    OrderedDataFrame,
    OrderingColumn,
)
from snowflake.snowpark.modin.plugin._internal.snowpark_pandas_types import (
    TimedeltaType,
)
from snowflake.snowpark.modin.plugin._internal.utils import (
    from_pandas_label,
    pandas_lit,
    to_pandas_label,
)
from snowflake.snowpark.modin.plugin._typing import PandasLabelToSnowflakeIdentifierPair
from snowflake.snowpark.types import (
    BooleanType,
    DataType,
    DoubleType,
    IntegerType,
    StringType,
)

AGG_NAME_COL_LABEL = "AGG_FUNC_NAME"
_NUMPY_FUNCTION_TO_NAME = {
    function: name for name, function in getmembers(np) if callable(function)
}


def _array_agg_keepna(
    column_to_aggregate: ColumnOrName, ordering_columns: Iterable[OrderingColumn]
) -> Column:
    """
    Aggregate a column, including nulls, into an array by the given ordering columns.
    """
    # array_agg drops nulls, but we can use the solution [1] to work around
    # that by turning each element `v` into the array `[v]`...
    # except that we can't use array_construct(NULL) and instead have to use
    # parse_json(lit("null")) per [2].
    # [1] https://stackoverflow.com/a/77422662
    # [2] https://github.com/snowflakedb/snowflake-connector-python/issues/1388#issuecomment-1371091831

    # HOWEVER it appears that this workaround only works for integer values.
    # See details in SNOW-1859090.
    return array_flatten(
        array_agg(
            array_construct(
                iff(
                    is_null(column_to_aggregate),
                    parse_json(lit("null")),
                    Column(column_to_aggregate),
                )
            )
        ).within_group(
            [ordering_column.snowpark_column for ordering_column in ordering_columns]
        )
    )


def column_quantile(
    column: SnowparkColumn,
    interpolation: Literal["linear", "lower", "higher", "midpoint", "nearest"],
    q: float,
) -> SnowparkColumn:
    assert interpolation in (
        "linear",
        "nearest",
    ), f"unsupported interpolation method '{interpolation}'"
    # PERCENTILE_CONT interpolates between the nearest values if needed, while
    # PERCENTILE_DISC finds the nearest value
    agg_method = "percentile_cont" if interpolation == "linear" else "percentile_disc"
    # PERCENTILE_* returns DECIMAL; we cast to DOUBLE
    # example sql: SELECT CAST(PERCENTILE_COUNT(0.25) WITHIN GROUP(ORDER BY a) AS DOUBLE) AS a FROM table
    return builtin(agg_method)(pandas_lit(q)).within_group(column).cast(DoubleType())


def _columns_coalescing_idxmax_idxmin_helper(
    *cols: SnowparkColumn,
    axis: Literal[0, 1],
    func: Literal["idxmax", "idxmin"],
    keepna: bool,
    pandas_column_labels: list,
    is_groupby: bool = False,
) -> SnowparkColumn:
    """
    Computes the index corresponding to the func for each row if axis=1 or column if axis=0.
    If all values in a row/column are NaN, then the result will be NaN.

    Parameters
    ----------
    *cols: SnowparkColumn
        A tuple of Snowpark Columns.
    axis: {0, 1}
        The axis to apply the func on.
    func: {"idxmax", "idxmin"}
        The function to apply.
    keepna: bool
        Whether to skip NaN Values.
    pandas_column_labels: list
        pandas index/column names.

    Returns
    -------
    Callable
    """
    if axis == 0:
        extremum = max_(*cols) if func == "idxmax" else min_(*cols)

        # TODO SNOW-1316602: Support MultiIndex for DataFrame, Series, and DataFrameGroupBy cases.
        if len(pandas_column_labels) > 1:
            # The index is a MultiIndex, current logic does not support this.
            raise NotImplementedError(
                f"{func} is not yet supported when the index is a MultiIndex."
            )

        # TODO SNOW-1270521: max_by and min_by are not guaranteed to break tiebreaks deterministically
        extremum_position = (
            get(
                builtin("max_by")(
                    Column(pandas_column_labels[0]),
                    Column(*cols),
                    1,
                ),
                0,
            )
            if func == "idxmax"
            else get(
                builtin("min_by")(
                    Column(pandas_column_labels[0]),
                    Column(*cols),
                    1,
                ),
                0,
            )
        )

        if is_groupby and keepna:
            # When performing groupby, if a group has any NaN values in its column, the idxmax/idxmin of that column
            # will always be NaN. Therefore, we need to check whether there are any NaN values in each group.
            return iff(
                builtin("count_if")(Column(*cols).is_null()) > 0,
                pandas_lit(None),
                extremum_position,
            )
        else:
            # if extremum is null, i.e. there are no columns or all columns are
            # null, mark extremum_position as null, because our final expression has
            # to evaluate to null.
            return builtin("nvl2")(extremum, extremum_position, lit(None))

    else:
        column_array = array_construct(*cols)
        # extremum is null if there are no columns or all columns are null.
        # otherwise, extremum contains the extremal column, i.e. the max column for
        # idxmax and the min column for idxmin.
        extremum = (array_max if func == "idxmax" else array_min)(column_array)
        # extremum_position is the position of the first column with a value equal
        # to extremum.
        extremum_position = array_position(extremum, column_array)

        if keepna:
            # if any of the columns is null, mark extremum_position as null,
            # because our final expression has to evaluate to null. That's how we
            # "keep NA."
            extremum_position = iff(
                array_contains(lit(None), column_array), lit(None), extremum_position
            )
        else:
            # if extremum is null, i.e. there are no columns or all columns are
            # null, mark extremum_position as null, because our final expression has
            # to evalute to null.
            extremum_position = builtin("nvl2")(extremum, extremum_position, lit(None))

        # If extremum_position is null, return null.
        return builtin("nvl2")(
            extremum_position,
            # otherwise, we create an array of all the column names using pandas_column_labels
            # and get the element of that array that is at extremum_position.
            get(
                array_construct(*(lit(c) for c in pandas_column_labels)),
                cast(extremum_position, "int"),
            ),
            lit(None),
        )


class _SnowparkPandasAggregation(NamedTuple):
    """
    A representation of a Snowpark pandas aggregation.

    This structure gives us a common representation for an aggregation that may
    have multiple aliases, like "sum" and np.sum.
    """

    # This field tells whether if types of all the inputs of the function are
    # the same instance of SnowparkPandasType, the type of the result is the
    # same instance of SnowparkPandasType. Note that this definition applies
    # whether the aggregation is on axis=0 or axis=1. For example, the sum of
    # a single timedelta column on axis 0 is another timedelta column.
    # Equivalently, the sum of two timedelta columns along axis 1 is also
    # another timedelta column. Therefore, preserves_snowpark_pandas_types for
    # sum would be True.
    preserves_snowpark_pandas_types: bool

    # Whether Snowflake PIVOT supports this aggregation on axis 0. It seems
    # that Snowflake PIVOT supports any aggregation expressed as as single
    # function call applied to a single column, e.g. MAX(A), BOOLOR_AND(A)
    supported_in_pivot: bool

    # This callable takes a single Snowpark column as input and aggregates the
    # column on axis=0. If None, Snowpark pandas does not support this
    # aggregation on axis=0.
    axis_0_aggregation: Optional[Callable] = None

    # This callable takes one or more Snowpark columns as input and
    # the columns on axis=1 with skipna=True, i.e. not including nulls in the
    # aggregation. If None, Snowpark pandas does not support this aggregation
    # on axis=1 with skipna=True.
    axis_1_aggregation_skipna: Optional[Callable] = None

    # This callable takes one or more Snowpark columns as input and
    # the columns on axis=1 with skipna=False, i.e. including nulls in the
    # aggregation. If None, Snowpark pandas does not support this aggregation
    # on axis=1 with skipna=False.
    axis_1_aggregation_keepna: Optional[Callable] = None


class SnowflakeAggFunc(NamedTuple):
    """
    A Snowflake aggregation, including information about how the aggregation acts on SnowparkPandasType.
    """

    # The aggregation function in Snowpark.
    # For aggregation on axis=0, this field should take a single Snowpark
    # column and return the aggregated column.
    # For aggregation on axis=1, this field should take an arbitrary number
    # of Snowpark columns and return the aggregated column.
    snowpark_aggregation: Callable

    # This field tells whether if types of all the inputs of the function are
    # the same instance of SnowparkPandasType, the type of the result is the
    # same instance of SnowparkPandasType. Note that this definition applies
    # whether the aggregation is on axis=0 or axis=1. For example, the sum of
    # a single timedelta column on axis 0 is another timedelta column.
    # Equivalently, the sum of two timedelta columns along axis 1 is also
    # another timedelta column. Therefore, preserves_snowpark_pandas_types for
    # sum would be True.
    preserves_snowpark_pandas_types: bool

    # Whether Snowflake PIVOT supports this aggregation on axis 0. It seems
    # that Snowflake PIVOT supports any aggregation expressed as as single
    # function call applied to a single column, e.g. MAX(A), BOOLOR_AND(A).
    # This field only makes sense for axis 0 aggregation.
    supported_in_pivot: bool


class AggFuncWithLabel(NamedTuple):
    """
    This class is used to process NamedAgg's internally, and represents an AggFunc that
    also includes a label to be used on the column that it generates.
    """

    # The aggregate function
    func: AggFuncTypeBase

    # The label to provide the new column produced by `func`.
    pandas_label: Hashable


class AggFuncInfo(NamedTuple):
    """
    Information needed to distinguish between dummy and normal aggregate functions.
    """

    # The aggregate function
    func: AggFuncTypeBase

    # If true, the aggregate function is applied to "NULL" rather than a column
    is_dummy_agg: bool

    # If specified, the pandas label to provide the new column generated by this aggregate
    # function. Used in conjunction with pd.NamedAgg.
    post_agg_pandas_label: Optional[Hashable] = None


class AggregationSupportResult(NamedTuple):
    """
    Information needed to return the first unsupported aggregate function if any.
    """

    # Whether the function is supported for aggregation in snowflake.
    is_valid: bool

    # The unsupported function used for aggregation.
    unsupported_function: str

    # The kwargs for the unsupported function.
    unsupported_kwargs: dict[str, Any]


def _columns_coalescing_min(*cols: SnowparkColumn) -> Callable:
    """
    Computes the minimum value in each row, skipping NaN values. If all values in a row are NaN,
    then the result will be NaN.

    Example SQL:
    SELECT ARRAY_MIN(ARRAY_CONSTRUCT_COMPACT(a, b, c)) AS min
    FROM VALUES (10, 1, NULL), (NULL, NULL, NULL) AS t (a, b, c);

    Result:
    --------
    |  min |
    --------
    |    1 |
    --------
    | NULL |
    --------
    """
    return array_min(array_construct_compact(*cols))


def _columns_coalescing_max(*cols: SnowparkColumn) -> Callable:
    """
    Computes the maximum value in each row, skipping NaN values. If all values in a row are NaN,
    then the result will be NaN.

    Example SQL:
    SELECT ARRAY_MAX(ARRAY_CONSTRUCT_COMPACT(a, b, c)) AS max
    FROM VALUES (10, 1, NULL), (NULL, NULL, NULL) AS t (a, b, c);

    Result:
    --------
    |  max |
    --------
    |   10 |
    --------
    | NULL |
    --------
    """
    return array_max(array_construct_compact(*cols))


def _columns_count(*cols: SnowparkColumn) -> Callable:
    """
    Counts the number of non-NULL values in each row.

    Example SQL:
    SELECT NVL2(a, 1, 0) + NVL2(b, 1, 0) + NVL2(c, 1, 0) AS count
    FROM VALUES (10, 1, NULL), (NULL, NULL, NULL) AS t (a, b, c);

    Result:
    ---------
    | count |
    ---------
    |     2 |
    ---------
    |     0 |
    ---------
    """
    # IMPORTANT: count and sum use python builtin sum to invoke __add__ on each column rather than Snowpark
    # sum_, since Snowpark sum_ gets the sum of all rows within a single column.
    # NVL2(col, x, y) returns x if col is NULL, and y otherwise.
    return sum(builtin("nvl2")(col, pandas_lit(1), pandas_lit(0)) for col in cols)


def _columns_count_keep_nulls(*cols: SnowparkColumn) -> Callable:
    """
    Counts the number of values (including NULL) in each row.
    """
    # IMPORTANT: count and sum use python builtin sum to invoke __add__ on each column rather than Snowpark
    # sum_, since Snowpark sum_ gets the sum of all rows within a single column.
    return sum(pandas_lit(1) for _ in cols)


def _columns_coalescing_sum(*cols: SnowparkColumn) -> Callable:
    """
    Sums all non-NaN elements in each row. If all elements are NaN, returns 0.

    Example SQL:
    SELECT ZEROIFNULL(a) + ZEROIFNULL(b) + ZEROIFNULL(c) AS sum
    FROM VALUES (10, 1, NULL), (NULL, NULL, NULL) AS t (a, b, c);

    Result:
    -------
    | sum |
    -------
    |  11 |
    -------
    |   0 |
    -------
    """
    # IMPORTANT: count and sum use python builtin sum to invoke __add__ on each column rather than Snowpark
    # sum_, since Snowpark sum_ gets the sum of all rows within a single column.
    return sum(builtin("zeroifnull")(col) for col in cols)


def _column_first_value(
    column: SnowparkColumn,
    row_position_snowflake_quoted_identifier: str,
    ignore_nulls: bool,
) -> SnowparkColumn:
    """
    Returns the first value (ordered by `row_position_snowflake_identifier`) over the specified group.

    Parameters
    ----------
    col: Snowpark Column
        The Snowpark column to aggregate.
    row_position_snowflake_quoted_identifier: str
        The Snowflake quoted identifier of the column to order by.
    ignore_nulls: bool
        Whether or not to ignore nulls.

    Returns
    -------
        The aggregated Snowpark Column.
    """
    if ignore_nulls:
        col_to_min_by = iff(
            col(column).is_null(),
            pandas_lit(None),
            col(row_position_snowflake_quoted_identifier),
        )
    else:
        col_to_min_by = col(row_position_snowflake_quoted_identifier)
    return builtin("min_by")(col(column), col_to_min_by)


def _column_last_value(
    column: SnowparkColumn,
    row_position_snowflake_quoted_identifier: str,
    ignore_nulls: bool,
) -> SnowparkColumn:
    """
    Returns the last value (ordered by `row_position_snowflake_identifier`) over the specified group.

    Parameters
    ----------
    col: Snowpark Column
        The Snowpark column to aggregate.
    row_position_snowflake_quoted_identifier: str
        The Snowflake quoted identifier of the column to order by.
    ignore_nulls: bool
        Whether or not to ignore nulls.

    Returns
    -------
        The aggregated Snowpark Column.
    """
    if ignore_nulls:
        col_to_max_by = iff(
            col(column).is_null(),
            pandas_lit(None),
            col(row_position_snowflake_quoted_identifier),
        )
    else:
        col_to_max_by = col(row_position_snowflake_quoted_identifier)
    return builtin("max_by")(col(column), col_to_max_by)


def _create_pandas_to_snowpark_pandas_aggregation_map(
    pandas_functions: Iterable[AggFuncTypeBase],
    snowpark_pandas_aggregation: _SnowparkPandasAggregation,
) -> MappingProxyType[AggFuncTypeBase, _SnowparkPandasAggregation]:
    """
    Create a map from the given pandas functions to the given _SnowparkPandasAggregation.

    Args;
        pandas_functions: The pandas functions that map to the given aggregation.
        snowpark_pandas_aggregation: The aggregation to map to

    Returns:
        The map.
    """
    return MappingProxyType({k: snowpark_pandas_aggregation for k in pandas_functions})


# Map between the pandas input aggregation function (str or numpy function) and
# _SnowparkPandasAggregation representing information about applying the
# aggregation in Snowpark pandas.
_PANDAS_AGGREGATION_TO_SNOWPARK_PANDAS_AGGREGATION: MappingProxyType[
    AggFuncTypeBase, _SnowparkPandasAggregation
] = MappingProxyType(
    {
        "count": _SnowparkPandasAggregation(
            axis_0_aggregation=count,
            axis_1_aggregation_skipna=_columns_count,
            preserves_snowpark_pandas_types=False,
            supported_in_pivot=True,
        ),
        **_create_pandas_to_snowpark_pandas_aggregation_map(
            (len, "size"),
            _SnowparkPandasAggregation(
                # We must count the total number of rows regardless of if they're null.
                axis_0_aggregation=lambda _: builtin("count_if")(pandas_lit(True)),
                axis_1_aggregation_keepna=_columns_count_keep_nulls,
                axis_1_aggregation_skipna=_columns_count_keep_nulls,
                preserves_snowpark_pandas_types=False,
                supported_in_pivot=False,
            ),
        ),
        "first": _SnowparkPandasAggregation(
            axis_0_aggregation=_column_first_value,
            axis_1_aggregation_keepna=lambda *cols: cols[0],
            axis_1_aggregation_skipna=lambda *cols: coalesce(*cols),
            preserves_snowpark_pandas_types=True,
            supported_in_pivot=False,
        ),
        "last": _SnowparkPandasAggregation(
            axis_0_aggregation=_column_last_value,
            axis_1_aggregation_keepna=lambda *cols: cols[-1],
            axis_1_aggregation_skipna=lambda *cols: coalesce(*(cols[::-1])),
            preserves_snowpark_pandas_types=True,
            supported_in_pivot=False,
        ),
        **_create_pandas_to_snowpark_pandas_aggregation_map(
            ("mean", np.mean),
            _SnowparkPandasAggregation(
                axis_0_aggregation=mean,
                preserves_snowpark_pandas_types=True,
                supported_in_pivot=True,
            ),
        ),
        **_create_pandas_to_snowpark_pandas_aggregation_map(
            ("min", np.min, min),
            _SnowparkPandasAggregation(
                axis_0_aggregation=min_,
                axis_1_aggregation_keepna=least,
                axis_1_aggregation_skipna=_columns_coalescing_min,
                preserves_snowpark_pandas_types=True,
                supported_in_pivot=True,
            ),
        ),
        **_create_pandas_to_snowpark_pandas_aggregation_map(
            ("max", np.max, max),
            _SnowparkPandasAggregation(
                axis_0_aggregation=max_,
                axis_1_aggregation_keepna=greatest,
                axis_1_aggregation_skipna=_columns_coalescing_max,
                preserves_snowpark_pandas_types=True,
                supported_in_pivot=True,
            ),
        ),
        **_create_pandas_to_snowpark_pandas_aggregation_map(
            ("sum", np.sum, sum),
            _SnowparkPandasAggregation(
                axis_0_aggregation=sum_,
                # IMPORTANT: count and sum use python builtin sum to invoke
                # __add__ on each column rather than Snowpark sum_, since
                # Snowpark sum_ gets the sum of all rows within a single column.
                axis_1_aggregation_keepna=lambda *cols: sum(cols),
                axis_1_aggregation_skipna=_columns_coalescing_sum,
                preserves_snowpark_pandas_types=True,
                supported_in_pivot=True,
            ),
        ),
        **_create_pandas_to_snowpark_pandas_aggregation_map(
            ("median", np.median),
            _SnowparkPandasAggregation(
                axis_0_aggregation=median,
                preserves_snowpark_pandas_types=True,
                supported_in_pivot=True,
            ),
        ),
        "idxmax": _SnowparkPandasAggregation(
            axis_0_aggregation=functools.partial(
                _columns_coalescing_idxmax_idxmin_helper, func="idxmax"
            ),
            axis_1_aggregation_keepna=_columns_coalescing_idxmax_idxmin_helper,
            axis_1_aggregation_skipna=_columns_coalescing_idxmax_idxmin_helper,
            preserves_snowpark_pandas_types=False,
            supported_in_pivot=False,
        ),
        "idxmin": _SnowparkPandasAggregation(
            axis_0_aggregation=functools.partial(
                _columns_coalescing_idxmax_idxmin_helper, func="idxmin"
            ),
            axis_1_aggregation_skipna=_columns_coalescing_idxmax_idxmin_helper,
            axis_1_aggregation_keepna=_columns_coalescing_idxmax_idxmin_helper,
            preserves_snowpark_pandas_types=False,
            supported_in_pivot=False,
        ),
        "skew": _SnowparkPandasAggregation(
            axis_0_aggregation=skew,
            preserves_snowpark_pandas_types=True,
            supported_in_pivot=True,
        ),
        "all": _SnowparkPandasAggregation(
            # all() for a column with no non-null values is NULL in Snowflake, but True in pandas.
            axis_0_aggregation=lambda c: coalesce(
                builtin("booland_agg")(col(c)), pandas_lit(True)
            ),
            preserves_snowpark_pandas_types=False,
            supported_in_pivot=False,
        ),
        "any": _SnowparkPandasAggregation(
            # any() for a column with no non-null values is NULL in Snowflake, but False in pandas.
            axis_0_aggregation=lambda c: coalesce(
                builtin("boolor_agg")(col(c)), pandas_lit(False)
            ),
            preserves_snowpark_pandas_types=False,
            supported_in_pivot=False,
        ),
        **_create_pandas_to_snowpark_pandas_aggregation_map(
            ("std", np.std),
            _SnowparkPandasAggregation(
                axis_0_aggregation=stddev,
                preserves_snowpark_pandas_types=True,
                supported_in_pivot=True,
            ),
        ),
        **_create_pandas_to_snowpark_pandas_aggregation_map(
            ("var", np.var),
            _SnowparkPandasAggregation(
                axis_0_aggregation=variance,
                # variance units are the square of the input column units, so
                # variance does not preserve types.
                preserves_snowpark_pandas_types=False,
                supported_in_pivot=True,
            ),
        ),
        "array_agg": _SnowparkPandasAggregation(
            axis_0_aggregation=array_agg,
            preserves_snowpark_pandas_types=False,
            supported_in_pivot=False,
        ),
        "quantile": _SnowparkPandasAggregation(
            axis_0_aggregation=column_quantile,
            preserves_snowpark_pandas_types=True,
            supported_in_pivot=False,
        ),
        "nunique": _SnowparkPandasAggregation(
            axis_0_aggregation=count_distinct,
            preserves_snowpark_pandas_types=False,
            supported_in_pivot=False,
        ),
    }
)


class AggregateColumnOpParameters(NamedTuple):
    """
    Parameters/Information needed to apply aggregation on a Snowpark column correctly.
    """

    # Snowflake quoted identifier for the column to apply aggregation on
    snowflake_quoted_identifier: ColumnOrName

    # The Snowpark data type for the column to apply aggregation on
    data_type: DataType

    # pandas label for the new column produced after aggregation
    agg_pandas_label: Optional[Hashable]

    # Snowflake quoted identifier for the new Snowpark column produced after aggregation
    agg_snowflake_quoted_identifier: str

    # the snowflake aggregation function to apply on the column
    snowflake_agg_func: SnowflakeAggFunc

    # the columns specifying the order of rows in the column. This is only
    # relevant for aggregations that depend on row order, e.g. summing a string
    # column.
    ordering_columns: Iterable[OrderingColumn]


def is_snowflake_agg_func(agg_func: AggFuncTypeBase) -> bool:
    return agg_func in _PANDAS_AGGREGATION_TO_SNOWPARK_PANDAS_AGGREGATION


def get_snowflake_agg_func(
    agg_func: AggFuncTypeBase,
    agg_kwargs: dict[str, Any],
    axis: Literal[0, 1],
    _is_df_agg: bool = False,
) -> Optional[SnowflakeAggFunc]:
    """
    Get the corresponding Snowflake/Snowpark aggregation function for the given aggregation function.
    If no corresponding snowflake aggregation function can be found, return None.
    """
    if axis == 1:
        return _generate_rowwise_aggregation_function(agg_func, agg_kwargs)

    snowpark_pandas_aggregation = (
        _PANDAS_AGGREGATION_TO_SNOWPARK_PANDAS_AGGREGATION.get(agg_func)
    )

    if snowpark_pandas_aggregation is None:
        # We don't have any implementation at all for this aggregation.
        return None

    snowpark_aggregation = snowpark_pandas_aggregation.axis_0_aggregation

    if snowpark_aggregation is None:
        # We don't have an implementation on axis=0 for this aggregation.
        return None

    # Rewrite some aggregations according to `agg_kwargs.`
    if snowpark_aggregation == stddev or snowpark_aggregation == variance:
        # for aggregation function std and var, we only support ddof = 0 or ddof = 1.
        # when ddof is 1, std is mapped to stddev, var is mapped to variance
        # when ddof is 0, std is mapped to stddev_pop, var is mapped to var_pop
        # TODO (SNOW-892532): support std/var for ddof that is not 0 or 1
        ddof = agg_kwargs.get("ddof", 1)
        if ddof != 1 and ddof != 0:
            return None
        if ddof == 0:
            snowpark_aggregation = (
                stddev_pop if snowpark_aggregation == stddev else var_pop
            )
    elif snowpark_aggregation == column_quantile:
        interpolation = agg_kwargs.get("interpolation", "linear")
        q = agg_kwargs.get("q", 0.5)
        if interpolation not in ("linear", "nearest"):
            return None
        if not is_scalar(q):
            # SNOW-1062878 Because list-like q would return multiple rows, calling quantile
            # through the aggregate frontend in this manner is unsupported.
            return None

        def snowpark_aggregation(col: SnowparkColumn) -> SnowparkColumn:
            return column_quantile(col, interpolation, q)

    elif (
        snowpark_aggregation == _column_first_value
        or snowpark_aggregation == _column_last_value
    ):
        if _is_df_agg:
            # First and last are not supported for df.agg.
            return None
        ignore_nulls = agg_kwargs.get("skipna", True)
        row_position_snowflake_quoted_identifier = agg_kwargs.get(
            "_first_last_row_pos_col", None
        )
        snowpark_aggregation = functools.partial(
            snowpark_aggregation,
            ignore_nulls=ignore_nulls,
            row_position_snowflake_quoted_identifier=row_position_snowflake_quoted_identifier,
        )

    assert (
        snowpark_aggregation is not None
    ), "Internal error: Snowpark pandas should have identified a Snowpark aggregation."
    return SnowflakeAggFunc(
        snowpark_aggregation=snowpark_aggregation,
        preserves_snowpark_pandas_types=snowpark_pandas_aggregation.preserves_snowpark_pandas_types,
        supported_in_pivot=snowpark_pandas_aggregation.supported_in_pivot,
    )


def _generate_rowwise_aggregation_function(
    agg_func: AggFuncTypeBase, agg_kwargs: dict[str, Any]
) -> Optional[SnowflakeAggFunc]:
    """
    Get a callable taking *arg columns to apply for an aggregation.

    Unlike get_snowflake_agg_func, this function may return a wrapped composition of
    Snowflake builtin functions depending on the values of the specified kwargs.
    """
    snowpark_pandas_aggregation = (
        _PANDAS_AGGREGATION_TO_SNOWPARK_PANDAS_AGGREGATION.get(agg_func)
    )
    if snowpark_pandas_aggregation is None:
        return None
    snowpark_aggregation = (
        snowpark_pandas_aggregation.axis_1_aggregation_skipna
        if agg_kwargs.get("skipna", True)
        else snowpark_pandas_aggregation.axis_1_aggregation_keepna
    )
    if snowpark_aggregation is None:
        return None
    min_count = agg_kwargs.get("min_count", 0)
    if min_count > 0:
        original_aggregation = snowpark_aggregation

        # Create a case statement to check if the number of non-null values exceeds min_count
        # when min_count > 0, if the number of not NULL values is < min_count, return NULL.
        def snowpark_aggregation(*cols: SnowparkColumn) -> SnowparkColumn:
            return when(_columns_count(*cols) < min_count, pandas_lit(None)).otherwise(
                original_aggregation(*cols)
            )

    return SnowflakeAggFunc(
        snowpark_aggregation,
        preserves_snowpark_pandas_types=snowpark_pandas_aggregation.preserves_snowpark_pandas_types,
        supported_in_pivot=snowpark_pandas_aggregation.supported_in_pivot,
    )


def _is_supported_snowflake_agg_func(
    agg_func: AggFuncTypeBase,
    agg_kwargs: dict[str, Any],
    axis: Literal[0, 1],
    _is_df_agg: bool = False,
) -> AggregationSupportResult:
    """
    check if the aggregation function is supported with snowflake. Current supported
    aggregation functions are the functions that can be mapped to snowflake builtin function.

    Args:
        agg_func: str or Callable. the aggregation function to check
        agg_kwargs: keyword argument passed for the aggregation function, such as ddof, min_count etc.
                    The value can be different for different aggregation functions.
    Returns:
        is_valid: bool. Whether the function is supported for aggregation in snowflake.
        unsupported_function: str. The unsupported function used for aggregation.
        unsupported_kwargs: dict. The kwargs for the unsupported function
    """
    if isinstance(agg_func, tuple) and len(agg_func) == 2:
        # For named aggregations, like `df.agg(new_col=("old_col", "sum"))`,
        # take the second part of the named aggregation.
        agg_func = agg_func[0]

    if get_snowflake_agg_func(agg_func, agg_kwargs, axis, _is_df_agg) is None:
        return AggregationSupportResult(
            is_valid=False, unsupported_function=agg_func, unsupported_kwargs=agg_kwargs
        )
    return AggregationSupportResult(
        is_valid=True, unsupported_function="", unsupported_kwargs={}
    )


def _are_all_agg_funcs_supported_by_snowflake(
    agg_funcs: list[AggFuncTypeBase],
    agg_kwargs: dict[str, Any],
    axis: Literal[0, 1],
    _is_df_agg: bool = False,
) -> AggregationSupportResult:
    """
    Check if all aggregation functions in the given list are snowflake supported
    aggregation functions.

    Returns:
        is_valid: bool. Whether it is valid to implement with snowflake or not.
        unsupported_function: str. The unsupported function used for aggregation.
        unsupported_kwargs: dict. The kwargs for the unsupported function.
    """
    is_supported_bools: list[bool] = []
    unsupported_list: list[str] = []
    unsupported_kwargs_list: list[dict[str, Any]] = []
    for func in agg_funcs:
        (
            is_supported,
            unsupported_func,
            unsupported_kwargs,
        ) = _is_supported_snowflake_agg_func(func, agg_kwargs, axis, _is_df_agg)
        is_supported_bools.append(is_supported)
        if not is_supported:
            unsupported_list.append(unsupported_func)
            unsupported_kwargs_list.append(unsupported_kwargs)

    unsupported_func = unsupported_list[0] if len(unsupported_list) > 0 else ""
    unsupported_kwargs = (
        unsupported_kwargs_list[0] if len(unsupported_kwargs_list) > 0 else {}
    )
    is_valid = all(is_supported_bools)
    return AggregationSupportResult(is_valid, unsupported_func, unsupported_kwargs)


def check_is_aggregation_supported_in_snowflake(
    agg_func: AggFuncType,
    agg_kwargs: dict[str, Any],
    axis: Literal[0, 1],
    _is_df_agg: bool = False,
) -> AggregationSupportResult:
    """
    check if distributed implementation with snowflake is available for the aggregation
    based on the input arguments.

    Args:
        agg_func: the aggregation function to apply
        agg_kwargs: keyword argument passed for the aggregation function, such as ddof, min_count etc.
                    The value can be different for different aggregation function.
        _is_df_agg: whether or not this is being called by df.agg, since some functions are only supported
                    for groupby_agg.
    Returns:
        is_supported_func: bool. Whether it is valid to implement with snowflake or not.
        unsupported_func: str. The unsupported function used for aggregation.
        unsupported_kwargs: dict. The kwargs for the unsupported function.
    """
    # validate agg_func, only snowflake builtin agg function or dict of snowflake builtin agg
    # function can be implemented in distributed way.
    # If there are multiple unsupported functions, the first unsupported function will be returned.
    unsupported_func = ""
    unsupported_kwargs: dict[str, Any] = {}
    is_supported_func = True
    if is_dict_like(agg_func):
        for value in agg_func.values():
            if is_list_like(value) and not is_named_tuple(value):
                (
                    is_supported_func,
                    unsupported_func,
                    unsupported_kwargs,
                ) = _are_all_agg_funcs_supported_by_snowflake(
                    value, agg_kwargs, axis, _is_df_agg
                )

            else:
                (
                    is_supported_func,
                    unsupported_func,
                    unsupported_kwargs,
                ) = _is_supported_snowflake_agg_func(
                    value, agg_kwargs, axis, _is_df_agg
                )
            if not is_supported_func:
                return AggregationSupportResult(
                    is_supported_func, unsupported_func, unsupported_kwargs
                )

    elif is_list_like(agg_func):
        (
            is_supported_func,
            unsupported_func,
            unsupported_kwargs,
        ) = _are_all_agg_funcs_supported_by_snowflake(
            agg_func, agg_kwargs, axis, _is_df_agg
        )
    else:
        (
            is_supported_func,
            unsupported_func,
            unsupported_kwargs,
        ) = _is_supported_snowflake_agg_func(agg_func, agg_kwargs, axis, _is_df_agg)
    return AggregationSupportResult(
        is_supported_func, unsupported_func, unsupported_kwargs
    )


def _is_snowflake_numeric_type_required(snowflake_agg_func: Callable) -> bool:
    """
    Is the given snowflake aggregation function needs to be applied on the numeric column.
    """
    return snowflake_agg_func in [
        mean,
        median,
        skew,
        sum_,
        stddev,
        stddev_pop,
        variance,
        var_pop,
        column_quantile,
    ]


def drop_non_numeric_data_columns(
    query_compiler: "snowflake_query_compiler.SnowflakeQueryCompiler",  # type: ignore[name-defined] # noqa: F821
    pandas_labels_for_columns_to_exclude: list[Hashable],
) -> "snowflake_query_compiler.SnowflakeQueryCompiler":  # type: ignore[name-defined] # noqa: F821
    """
    Drop the data columns of the internal frame that are non-numeric if numeric_only is True.

    Args:
        query_compiler: The query compiler for the internal frame to process on
        pandas_labels_for_columns_to_exclude: List of pandas labels to exclude from dropping even if the
            corresponding column is non-numeric.
    Returns:
        SnowflakeQueryCompiler that contains the processed new frame with non-numeric data columns dropped
    """
    from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
        SnowflakeQueryCompiler,
    )

    original_frame = query_compiler._modin_frame
    # get all data column to retain, a data column is retained if the pandas label for the column
    data_column_to_retain: list[PandasLabelToSnowflakeIdentifierPair] = [
        PandasLabelToSnowflakeIdentifierPair(
            original_frame.data_column_pandas_labels[i],
            original_frame.data_column_snowflake_quoted_identifiers[i],
        )
        for i, data_type in enumerate(query_compiler.dtypes.values)
        if is_numeric_dtype(data_type)
        or (
            original_frame.data_column_pandas_labels[i]
            in pandas_labels_for_columns_to_exclude
        )
    ]

    # get the original pandas labels and snowflake quoted identifiers for the numeric data columns
    new_data_column_pandas_labels: list[Hashable] = [
        col.pandas_label for col in data_column_to_retain
    ]
    new_data_column_snowflake_quoted_identifiers: list[str] = [
        col.snowflake_quoted_identifier for col in data_column_to_retain
    ]

    new_data_column_types = [
        type
        for id, type in original_frame.snowflake_quoted_identifier_to_snowpark_pandas_type.items()
        if id in new_data_column_snowflake_quoted_identifiers
    ]

    return SnowflakeQueryCompiler(
        InternalFrame.create(
            ordered_dataframe=original_frame.ordered_dataframe,
            data_column_pandas_labels=new_data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers=new_data_column_snowflake_quoted_identifiers,
            data_column_pandas_index_names=original_frame.data_column_pandas_index_names,
            index_column_pandas_labels=original_frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=original_frame.index_column_snowflake_quoted_identifiers,
            data_column_types=new_data_column_types,
            index_column_types=original_frame.cached_index_column_snowpark_pandas_types,
        )
    )


def _generate_aggregation_column(
    agg_column_op_params: AggregateColumnOpParameters,
    agg_kwargs: dict[str, Any],
    is_groupby_agg: bool,
    index_column_snowflake_quoted_identifier: Optional[list[str]] = None,
) -> SnowparkColumn:
    """
    Generate the aggregation column for the given column and aggregation function.

    Args:
        agg_column_op_params: AggregateColumnOpParameters. The aggregation parameter for a Snowpark column, contains following:
            - snowflake_quoted_identifier: the snowflake quoted identifier for the column to apply aggregation on
            - data_type: the Snowpark datatype for the column to apply aggregation on
            - agg_snowflake_quoted_identifier: The snowflake quoted identifier used for the result column after aggregation
            - snowflake_agg_func: The Snowflake aggregation function to apply on the given column
            - ordering_columns: the list of snowflake quoted identifiers corresponding to the ordering columns
        agg_kwargs: keyword argument passed for the aggregation function, such as ddof, min_count etc.
        is_groupby_agg: is the aggregation function applied after groupby or not.
        index_column_snowflake_quoted_identifier: The Snowflake quoted identifier corresponding to the index column

    Returns:
        SnowparkColumn after the aggregation function. The column is also aliased back to the original name
    """
    snowpark_column = agg_column_op_params.snowflake_quoted_identifier
    snowflake_agg_func = agg_column_op_params.snowflake_agg_func.snowpark_aggregation

    if snowflake_agg_func in (variance, var_pop) and isinstance(
        agg_column_op_params.data_type, TimedeltaType
    ):
        raise TypeError("timedelta64 type does not support var operations")

    if _is_snowflake_numeric_type_required(snowflake_agg_func) and isinstance(
        agg_column_op_params.data_type, BooleanType
    ):
        # if the column is a boolean column and the aggregation function requires numeric values,
        # we cast the boolean column to integer (True mapped to 1, and False mapped to 0). This is
        # to stay consistent with pandas behavior, where boolean type in pandas is treated as numeric type.
        snowpark_column = cast(
            agg_column_op_params.snowflake_quoted_identifier, IntegerType()
        )

    if snowflake_agg_func == sum_:
        if isinstance(agg_column_op_params.data_type, StringType):
            agg_snowpark_column = listagg(snowpark_column).within_group(
                [
                    ordering_column.snowpark_column
                    for ordering_column in agg_column_op_params.ordering_columns
                ]
            )
        else:
            # There is a slightly different behavior for sum in terms of missing value in pandas and Snowflake,
            # where sum on a column with all NaN in pandas result in 0, but sum on a column with all NULL result
            # in NULL. Therefore, a post process on the result to replace the NULL result with 0 using coalesce.
            agg_snowpark_column = coalesce(
                snowflake_agg_func(snowpark_column), pandas_lit(0)
            )
    elif snowflake_agg_func == array_agg:
        # Array aggregation requires the ordering columns, which we have to
        # pass in here.
        # note that we always assume keepna for array_agg. TODO(SNOW-1040398):
        # make keepna treatment consistent across array_agg and other
        # aggregation methods.
        agg_snowpark_column = _array_agg_keepna(
            snowpark_column, ordering_columns=agg_column_op_params.ordering_columns
        )
    elif (
        isinstance(snowflake_agg_func, partial)
        and snowflake_agg_func.func == _columns_coalescing_idxmax_idxmin_helper
    ):
        agg_snowpark_column = _columns_coalescing_idxmax_idxmin_helper(
            snowpark_column,
            axis=0,
            func=snowflake_agg_func.keywords["func"],
            keepna=not agg_kwargs.get("skipna", True),
            pandas_column_labels=index_column_snowflake_quoted_identifier,  # type: ignore
            is_groupby=is_groupby_agg,
        )
    elif snowflake_agg_func == count_distinct:
        if agg_kwargs.get("dropna", True) is False:
            # count_distinct does only count distinct non-NULL values.
            # Check if NULL is contained, then add +1 in this case.
            if not isinstance(snowpark_column, SnowparkColumn):
                snowpark_column = col(snowpark_column)
            agg_snowpark_column = snowflake_agg_func(snowpark_column) + iff(
                sum_(snowpark_column.is_null().cast(IntegerType())) > pandas_lit(0),
                pandas_lit(1),
                pandas_lit(0),
            )
        else:
            agg_snowpark_column = snowflake_agg_func(snowpark_column)
    else:
        agg_snowpark_column = snowflake_agg_func(snowpark_column)

    # Handle min_count and skipna parameters
    min_count = -1
    skipna = True
    is_groupby_min_max = is_groupby_agg and snowflake_agg_func in [min_, max_]
    if snowflake_agg_func is sum_ or is_groupby_min_max:
        # min_count parameter is only valid for groupby min/max/sum, dataframe sum and series sum
        min_count = agg_kwargs.get("min_count", -1)
    if not is_groupby_agg:
        # skipna parameter is valid for all supported none-groupby aggregation function
        skipna = agg_kwargs.get("skipna", True)

    if not skipna or min_count > 0:
        case_expr: Optional[CaseExpr] = None
        if not skipna:
            # TODO(SNOW-1040398): Use a different aggregation function map for
            # skipna=False, and set the skipna value at an earlier layer.
            # when skipna is False, return NULL as far as there is NULL in the column. This is achieved by first
            # converting the column to boolean with is_null, and call max on the boolean column. If NULL exists,
            # the result of max will be True, otherwise, False.
            # For example: [1, NULL, 2, 3] will be [False, True, False, False] with is_null, and max on the boolean
            # result is True.
            case_expr = when(
                max_(is_null(agg_column_op_params.snowflake_quoted_identifier)),
                pandas_lit(None),
            )
        if min_count > 0:
            # when min_count > 0, if the number of not NULL values is < min_count, return NULL.
            min_count_cond = (
                count(agg_column_op_params.snowflake_quoted_identifier) < min_count
            )
            case_expr = (
                case_expr.when(min_count_cond, pandas_lit(None))
                if (case_expr is not None)
                else when(min_count_cond, pandas_lit(None))
            )

        assert (
            case_expr is not None
        ), f"No case expression is constructed with skipna({skipna}), min_count({min_count})"
        agg_snowpark_column = case_expr.otherwise(agg_snowpark_column)

    if (
        isinstance(agg_column_op_params.data_type, TimedeltaType)
        and agg_column_op_params.snowflake_agg_func.preserves_snowpark_pandas_types
    ):
        # timedelta aggregations that produce timedelta results might produce
        # a decimal type in snowflake, e.g.
        # pd.Series([pd.Timestamp(1), pd.Timestamp(2)]).mean() produces 1.5 in
        # Snowflake. We truncate the decimal part of the result, as pandas
        # does.
        agg_snowpark_column = cast(
            trunc(agg_snowpark_column), agg_column_op_params.data_type.snowpark_type
        )

    # rename the column to agg_column_quoted_identifier
    agg_snowpark_column = agg_snowpark_column.as_(
        agg_column_op_params.agg_snowflake_quoted_identifier
    )

    return agg_snowpark_column


def aggregate_with_ordered_dataframe(
    ordered_dataframe: OrderedDataFrame,
    agg_col_ops: list[AggregateColumnOpParameters],
    agg_kwargs: dict[str, Any],
    groupby_columns: Optional[list[str]] = None,
    index_column_snowflake_quoted_identifier: Optional[list[str]] = None,
) -> OrderedDataFrame:
    """
    Perform aggregation on the snowpark dataframe based on the given column to aggregation function map.

    Args:
        ordered_dataframe: a OrderedDataFrame to perform aggregation on
        agg_col_ops: mapping between the columns to apply aggregation on and the corresponding aggregation to apply
        agg_kwargs: keyword argument passed for the aggregation function, such as ddof, min_count etc.
        groupby_columns: If provided, groupby the dataframe with the given columns before apply aggregate. Otherwise,
                no groupby will be performed.
        index_column_snowflake_quoted_identifier: The Snowflake quoted identifier corresponding to the index column

    Returns:
        OrderedDataFrame with all aggregated columns.
    """

    is_groupby_agg = groupby_columns is not None
    agg_list: list[SnowparkColumn] = [
        _generate_aggregation_column(
            agg_column_op_params=agg_col_op,
            agg_kwargs=agg_kwargs,
            is_groupby_agg=is_groupby_agg,
            index_column_snowflake_quoted_identifier=index_column_snowflake_quoted_identifier,
        )
        for agg_col_op in agg_col_ops
    ]

    if is_groupby_agg:
        agg_ordered_dataframe = ordered_dataframe.group_by(groupby_columns, *agg_list)
    else:
        agg_ordered_dataframe = ordered_dataframe.agg(*agg_list)
    return agg_ordered_dataframe


def convert_agg_func_arg_to_col_agg_func_map(
    internal_frame: InternalFrame,
    agg_func: AggFuncType,
    pandas_labels_for_columns_to_exclude_when_agg_on_all: list[Hashable],
) -> dict[
    PandasLabelToSnowflakeIdentifierPair, Union[AggFuncTypeBase, list[AggFuncTypeBase]]
]:
    """
    Convert the agg_func arguments to column to aggregation function maps, which is a map between
    the Snowpark pandas column (represented as a PandasLabelToSnowflakeIdentifierPair) to the corresponding
    aggregation functions needs to be applied on this column. Following rules are applied:
    1) If agg_func is a base aggregation (str or callable) or a list of base aggregation function, then all
        aggregation functions are applied on each data column of the internal frame.
    2) If agg_func is already in a dict format (column label to aggregation functions map), only the columns
        occur in the dictionary key is considered for aggregation.

    Args:
        internal_frame: InternalFrame. The internal frame to apply aggregation on
        agg_func: AggFuncType (str or callable, or a list of str or callable, or a dict between label and str or callable or list of str or callable)
            The aggregations functions to apply on the internal frame.
        pandas_labels_for_columns_to_exclude_when_agg_on_all: List[Hashable]
            List of pandas labels for the columns to exclude from aggregation when the aggregation needs to be applied on
            all data columns, which is the case when rule 1) described above is applied.

    Returns:
        Dict[PandasLabelToSnowflakeIdentifierPair, Union[AggFuncTypeBase, List[AggFuncTypeBase]]]
            Map between Snowpandas column and the aggregation functions needs to be applied on the column
    """
    col_agg_func_map: dict[
        PandasLabelToSnowflakeIdentifierPair,
        Union[AggFuncTypeBase, list[AggFuncTypeBase]],
    ] = {}

    if is_dict_like(agg_func):
        for label, fn in agg_func.items():
            # for each column configured in the map, look for the corresponding columns
            col_quoted_identifiers = (
                internal_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                    [label],
                    include_index=False,
                )
            )[0]

            for quoted_identifier in col_quoted_identifiers:
                col_agg_func_map[
                    PandasLabelToSnowflakeIdentifierPair(label, quoted_identifier)
                ] = fn
    else:
        # if the aggregation function is str or callable or a list of str or callable, apply the aggregations
        # functions on each data column.
        for label, quoted_identifier in zip(
            internal_frame.data_column_pandas_labels,
            internal_frame.data_column_snowflake_quoted_identifiers,
        ):
            if label not in pandas_labels_for_columns_to_exclude_when_agg_on_all:
                col_agg_func_map[
                    PandasLabelToSnowflakeIdentifierPair(label, quoted_identifier)
                ] = agg_func

    return col_agg_func_map


def get_agg_func_to_col_map(
    col_to_agg_func_map: dict[
        PandasLabelToSnowflakeIdentifierPair,
        Union[AggFuncTypeBase, list[AggFuncTypeBase]],
    ]
) -> dict[AggFuncTypeBase, list[PandasLabelToSnowflakeIdentifierPair]]:
    """
    Convert the column to aggregation function map to aggregation function to columns map, and keeps the order of
    the occurrence in the original map.

    For example:
        Given col_to_agg_func_map {(col1, "col1") : ["min", "max"], (col2, "col2"): ["max", "sum"]}
        The aggregation func to columns map is {"min": [(col1, "col1")], "max": [(col1, "col1"), (col2, "col2")], "sum": [(col2, "col2")]}
    """
    agg_func_to_col_map: dict[
        AggFuncTypeBase, list[PandasLabelToSnowflakeIdentifierPair]
    ] = defaultdict(list)
    for column_identifier, agg_funcs in col_to_agg_func_map.items():
        # iterate over each aggregation function
        agg_funcs_list = agg_funcs if is_list_like(agg_funcs) else [agg_funcs]
        for agg_func in agg_funcs_list:
            agg_func_to_col_map[agg_func].append(column_identifier)

    return agg_func_to_col_map


def get_pandas_aggr_func_name(aggfunc: AggFuncTypeBase) -> str:
    """
    Returns the friendly name for the aggr function.  For example, if it is a callable, it will return __name__
    otherwise the same string name value.
    """
    return (
        getattr(aggfunc, "__name__", str(aggfunc))
        if not isinstance(aggfunc, str)
        else aggfunc
    )


def _generate_pandas_labels_for_agg_result_columns(
    pandas_label: Hashable,
    num_levels: int,
    agg_func_list: list[AggFuncInfo],
    include_agg_func_in_agg_label: bool,
    include_pandas_label_in_agg_label: bool,
) -> list[Hashable]:
    """
    Generate the pandas labels for the result columns after apply agg_func to the pandas column with given
    pandas label. One aggregation column will be produced for each aggregation function in the given list. If
    include_agg_func_in_agg_label is true, the aggregation function name will be appended to the original pandas
    label to produce the new pandas label, otherwise the original pandas label is used.
    For example: Given pandas label 'A', and agg_func [min, max]
        if include_agg_func_in_agg_label is False and include_pandas_label_in_agg_label is True, the result labels will be ['A', 'A']
        if include_agg_func_in_agg_label is True and include_pandas_label_in_agg_label is True, the result labels will be [('A', 'min'), ('A', 'max')]
        if include_agg_func_in_agg_label is True and include_pandas_label_in_agg_label is False, the result label will be ('min', 'max')

    Note that include_agg_func_in_agg_label and include_pandas_label_in_agg_label can not be both False.

    Args:
        pandas_label: Hashable
            The pandas label for the column to apply aggregation function on
        num_levels: int
            The number of levels for the pandas label
        agg_func_list: List[AggFuncTypeBase]
            List of aggregation functions to be applied on the pandas column
        include_agg_func_in_agg_label: bool
            Whether to include the aggregation function in the label for the aggregation result column
        include_pandas_label_in_agg_label: bool,
            Whether to include the original pandas label in the label for the aggregation result column

    Returns:
        List[Hashable]
            List of pandas labels for the result aggregation columns, the length is the same as agg_func_list.
    """
    assert (
        include_pandas_label_in_agg_label or include_agg_func_in_agg_label
    ), "the result aggregation label must at least contain at least the original label or the aggregation function name."
    agg_func_column_labels = []
    for agg_func in agg_func_list:
        if agg_func.post_agg_pandas_label is None:
            label_tuple = (
                from_pandas_label(pandas_label, num_levels)
                if include_pandas_label_in_agg_label
                else ()
            )
            aggr_func_label = (
                (get_pandas_aggr_func_name(agg_func.func),)
                if include_agg_func_in_agg_label
                else ()
            )
            label_tuple = label_tuple + aggr_func_label
        else:
            label_tuple = (agg_func.post_agg_pandas_label,)
        agg_func_column_labels.append(to_pandas_label(label_tuple))

    return agg_func_column_labels


def generate_column_agg_info(
    internal_frame: InternalFrame,
    column_to_agg_func: dict[
        PandasLabelToSnowflakeIdentifierPair,
        Union[AggFuncInfo, list[AggFuncInfo]],
    ],
    agg_kwargs: dict[str, Any],
    include_agg_func_only_in_result_label: bool,
) -> tuple[list[AggregateColumnOpParameters], list[Hashable]]:
    """
    Generate the ColumnAggregationInfo for the internal frame based on the column_to_agg_func map.

    Args:
        internal_frame: InternalFrame
            The internal frame to apply aggregation on
        column_to_agg_func: Dict[PandasLabelToSnowflakeIdentifierPair, Union[AggFuncInfo, List[AggFuncInfo]]],
            Map between the Snowpark pandas column needs to apply aggregation on and the aggregation functions to apply
            for the column. The Snowpark pandas column is represented as a pair of the pandas label and the quoted
            identifier for the columns. The aggregation function can be marked as dummy. In this case, it will be
            applied to "Null" rahter than the column.
        agg_kwargs: Dict[str, Any]
            keyword argument passed for the aggregation function
        include_agg_func_only_in_result_label: bool
            should the result label only contains the aggregation function name if it is included in the result label.


    Returns:
        List[AggregateColumnOpParameters]
            Each AggregateColumnOpParameters contains information of the quoted identifier for the column to apply
            aggregation on, the snowflake aggregation function to apply on the column, and the quoted identifier
            and pandas label to use for the result aggregation column.
        List[Hashable]
            The new index data column index names for the dataframe after aggregation
    """
    num_levels: int = internal_frame.num_index_levels(axis=1)
    # reserve all index column name and ordering column names
    identifiers_to_exclude: list[str] = (
        internal_frame.index_column_snowflake_quoted_identifiers
        + internal_frame.ordering_column_snowflake_quoted_identifiers
    )
    column_agg_ops: list[AggregateColumnOpParameters] = []
    # if any value in the dictionary is a list, the aggregation function name is added as
    # an extra level to the final pandas label, otherwise not. When any value in the dictionary is a list,
    # the aggregation function name will be added as an extra level for the result label.
    # One exception to this rule is when the user passes in pd.NamedAgg for the aggregations
    # instead of using the aggfunc argument. Then, each aggregation (even if on the same column)
    # has a unique name, and so we do not need to insert the additional level.
    agg_func_level_included = any(
        is_list_like(fn)
        and not is_named_tuple(fn)
        and not any(f.post_agg_pandas_label is not None for f in fn)
        for fn in column_to_agg_func.values()
    )
    pandas_label_level_included = (
        not agg_func_level_included or not include_agg_func_only_in_result_label
    )

    identifier_to_snowflake_type = internal_frame.quoted_identifier_to_snowflake_type(
        [pair.snowflake_quoted_identifier for pair in column_to_agg_func.keys()]
    )

    for pandas_label_to_identifier, agg_func in column_to_agg_func.items():
        pandas_label, quoted_identifier = pandas_label_to_identifier
        agg_func_list = (
            [agg_func]
            if not is_list_like(agg_func) or is_named_tuple(agg_func)
            else agg_func
        )
        # generate the pandas label and quoted identifier for the result aggregation columns, one
        # for each aggregation function to apply.
        agg_col_labels = _generate_pandas_labels_for_agg_result_columns(
            pandas_label_to_identifier.pandas_label,
            num_levels,
            agg_func_list,  # type: ignore[arg-type]
            agg_func_level_included,
            pandas_label_level_included,
        )
        agg_col_identifiers = (
            internal_frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=agg_col_labels, excluded=identifiers_to_exclude
            )
        )
        identifiers_to_exclude += agg_col_identifiers
        # construct the ColumnAggregationInfo for each aggregation
        for func_info, label, identifier in zip(
            agg_func_list, agg_col_labels, agg_col_identifiers
        ):
            func = func_info.func
            is_dummy_agg = func_info.is_dummy_agg
            agg_func_col = pandas_lit(None) if is_dummy_agg else quoted_identifier
            snowflake_agg_func = get_snowflake_agg_func(func, agg_kwargs, axis=0)
            # once reach here, we require all func have a corresponding snowflake aggregation function.
            # check_is_aggregation_supported_in_snowflake can be used to help performing the check.
            assert (
                snowflake_agg_func
            ), f"no snowflake aggregation function found for {func}"
            column_agg_ops.append(
                AggregateColumnOpParameters(
                    snowflake_quoted_identifier=agg_func_col,
                    data_type=identifier_to_snowflake_type[quoted_identifier],
                    agg_pandas_label=label,
                    agg_snowflake_quoted_identifier=identifier,
                    snowflake_agg_func=snowflake_agg_func,
                    ordering_columns=internal_frame.ordering_columns,
                )
            )

    new_data_column_index_names: list[Hashable] = []
    if pandas_label_level_included:
        new_data_column_index_names += internal_frame.data_column_pandas_index_names
    if agg_func_level_included:
        new_data_column_index_names += [None]

    return column_agg_ops, new_data_column_index_names


def using_named_aggregations_for_func(func: Any) -> bool:
    """
    Helper method to check if func is formatted in a way that indicates that we are using named aggregations.

    If the user specifies named aggregations, we parse them into the func variable as a dictionary mapping
    Hashable pandas labels to either a single AggFuncWithLabel or a list of AggFuncWithLabel NamedTuples. To know if
    a SnowflakeQueryCompiler aggregation method (agg(), groupby_agg()) was called with named aggregations, we can check
    if the `func` argument passed in obeys this formatting.
    This function checks the following:
    1. `func` is dict-like.
    2. Every value in `func` is either:
        a) an AggFuncWithLabel object
        b) a list of AggFuncWithLabel objects.
    If both conditions are met, that means that this func is the result of our internal processing of an aggregation
    API with named aggregations specified by the user.
    """
    return is_dict_like(func) and all(
        isinstance(value, AggFuncWithLabel)
        or (
            isinstance(value, list)
            and all(isinstance(v, AggFuncWithLabel) for v in value)
        )
        for value in func.values()
    )


def repr_aggregate_function(agg_func: AggFuncType, agg_kwargs: Mapping) -> str:
    """
    Represent an aggregation function as a string.

    Use this function to represent aggregation functions in error message to
    the user. This function will hide sensitive information, like axis labels or
    names of callables, in the function description.

    Args:
        agg_func: AggFuncType
            The aggregation function from the user. This may be a list-like or a
            dictionary containing multiple aggregations.
        agg_kwargs: Mapping
            The keyword arguments for the aggregation function.

    Returns:
        str
            The representation of the aggregation function.
    """
    if using_named_aggregations_for_func(agg_func):
        # New axis labels are sensitive, so replace them with "new_label."
        # Existing axis labels are sensitive, so replace them with "label."
        # This is checking whether the named aggregations are for a DataFrame,
        # in which case they are of the format new_col_name = (col_to_operate_on,
        # function), or for a Series, in which case they are of the format
        # new_col_name=function, in order to ensure we parse the functions out
        # from the keyword args correctly.
        if is_list_like(list(agg_kwargs.values())[0]):
            return ", ".join(
                f"new_label=(label, {repr_aggregate_function(f, agg_kwargs)})"
                for _, f in agg_kwargs.values()
            )
        else:
            return ", ".join(
                f"new_label=(label, {repr_aggregate_function(f, agg_kwargs)})"
                for f in agg_kwargs.values()
            )
    if isinstance(agg_func, str):
        # Strings functions represent names of pandas functions, e.g.
        # "sum" means to aggregate with pandas.Series.sum. string function
        # identifiers are not sensitive.
        return repr(agg_func)
    if is_dict_like(agg_func):
        # axis labels in the dictionary keys are sensitive, so replace them with
        # "label."
        return (
            "{"
            + ", ".join(
                f"label: {repr_aggregate_function(agg_func[key], agg_kwargs)}"
                for key in agg_func.keys()
            )
            + "}"
        )
    if is_list_like(agg_func):
        return f"[{', '.join(repr_aggregate_function(func, agg_kwargs) for func in agg_func)}]"
    if isinstance(agg_func, BuiltinFunctionType):
        return repr(agg_func)

    # for built-in classes like `list`, return "list" as opposed to repr(list),
    # i.e. <class 'list'>, which would be confusing because the user is using
    # `list` as a callable in this context.
    if agg_func is list:
        return "list"
    if agg_func is tuple:
        return "tuple"
    if agg_func is set:
        return "set"
    if agg_func is str:
        return "str"

    # Format numpy aggregations, e.g. np.argmin should become "np.argmin"
    if agg_func in _NUMPY_FUNCTION_TO_NAME:
        return f"np.{_NUMPY_FUNCTION_TO_NAME[agg_func]}"

    # agg_func should be callable at this point. pandas error messages at this
    # point are not consistent, so choose one style of error message.
    if not callable(agg_func):
        raise ValueError("aggregation function is not callable")

    # Return a constant string instead of some kind of function name to avoid
    # exposing sensitive user input in the NotImplemented error message and
    # thus in telemetry.
    return "Callable"


def is_first_last_in_agg_funcs(
    column_to_agg_func: dict[str, Union[list[AggFuncInfo], AggFuncInfo]]
) -> bool:
    """
    Helper function to check if the `first` or `last` aggregation functions have been specified.

    Parameters
    ----------
    column_to_agg_func: dict[str, Union[list[AggFuncInfo], AggFuncInfo]]
        The mapping of column name to aggregation function (or functions) to apply.

    Returns
    -------
    bool
        Whether any of the functions to apply are either `first` or `last`.
    """

    def _is_first_last_agg_func(value: AggFuncInfo) -> bool:
        return value.func in ["first", "last"]

    return any(
        (isinstance(val, AggFuncInfo) and _is_first_last_agg_func(val))
        or (isinstance(val, list) and any(_is_first_last_agg_func(v) for v in val))
        for val in column_to_agg_func.values()
    )
