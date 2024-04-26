#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from collections import namedtuple
from collections.abc import Generator, Hashable
from functools import reduce
from itertools import product
from typing import Any, Callable, Optional, Union

from pandas._typing import AggFuncType, AggFuncTypeBase, Scalar

from snowflake.snowpark.column import Column as SnowparkColumn
from snowflake.snowpark.functions import (
    coalesce,
    col,
    count,
    iff,
    min as min_,
    object_construct,
    sum as sum_,
)
from snowflake.snowpark.modin.plugin._internal.aggregation_utils import (
    get_pandas_aggr_func_name,
    get_snowflake_agg_func,
)
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin._internal.groupby_utils import (
    get_groups_for_ordered_dataframe,
)
from snowflake.snowpark.modin.plugin._internal.ordered_dataframe import (
    OrderedDataFrame,
    OrderingColumn,
)
from snowflake.snowpark.modin.plugin._internal.utils import (
    TempObjectType,
    append_columns,
    cache_result,
    convert_snowflake_string_constant_to_python_string,
    extract_pandas_label_from_object_construct_snowflake_quoted_identifier,
    extract_pandas_label_from_snowflake_quoted_identifier,
    from_pandas_label,
    get_distinct_rows,
    is_supported_snowflake_pivot_agg_func,
    pandas_lit,
    random_name_for_temp_object,
    to_pandas_label,
)
from snowflake.snowpark.modin.plugin._typing import (
    LabelComponent,
    LabelTuple,
    PandasLabelToSnowflakeIdentifierPair,
)
from snowflake.snowpark.types import DoubleType, StringType

TEMP_PIVOT_COLUMN_PREFIX = "PIVOT_"
DEFAULT_MARGINS_NAME = "All"

PivotAggrGrouping = namedtuple(
    "PivotAggrGrouping",
    ["aggfunc", "prefix_label", "aggr_label_identifier_pair"],
)


def pivot_helper(
    pivot_frame: InternalFrame,
    pivot_aggr_groupings: list[PivotAggrGrouping],
    expand_with_cartesian_product: bool,
    sort_first_level: bool,
    columns: Any,
    groupby_snowflake_quoted_identifiers: list[str],
    pivot_snowflake_quoted_identifiers: list[str],
    index: Optional[list],
) -> InternalFrame:
    """
    Helper function that that performs a full pivot on an InternalFrame.

    Args:
        pivot_frame: Original InternalFrame to pivot.
        pivot_aggr_groupings: A list of PivotAggrGroupings that define the aggregations to apply.
        expand_with_cartesian_product: Whether to ensure the cartesian product of index/groupby rows.
        sort_first_level: Whether to sort the first level of the pandas labels explicitly.
        columns: The columns argument passed to `pivot_table`. Will become the pandas labels for the data column index.
        groupby_snowflake_quoted_identifiers: Group by identifiers
        pivot_snowflake_quoted_identifiers: Pivot identifiers
        index: The index argument passed to `pivot_table` if specified. Will become the pandas labels for the index column.
    Returns:
        InternalFrame
        The result of performing the pivot.
    """
    ordered_dataframe = pivot_frame.ordered_dataframe
    # We may call snowpark dynamic pivot multiple times for a single call to `pivot_table` since pandas pivot
    # supports performing multiple aggregation functions in a single call, and each aggregation function can be
    # performed on multiple columns, whereas SQL pivot supports performing a single aggregation function on a single
    # column - therefore, we need to  call dynamic pivot once for every <aggregation_function, column> pairing.
    # TODO(SNOW-916206): Because we call snowpark dynamic pivot multiple times, we first materialize the original
    # snowpark dataframe, to avoid repeating materialize on each internal single pivot call.
    # In some cases the snowpark dataframe is backed by a transient temporary table, and if so, will not exist
    # at a later time when the schema is retrieved.  For now, we will materialize the source dataframe if there
    # are any post actions (like dropping the transient temp table).
    if ordered_dataframe.queries.get("post_actions"):
        ordered_dataframe = cache_result(ordered_dataframe)

    last_ordered_dataframe = None
    data_column_pandas_labels: list[Hashable] = []
    data_column_snowflake_quoted_identifiers: list[str] = []

    # To generate the correct multi-level pivot_table output we need several nested loops.
    # 1. Loop through list of aggregation values
    # 2. Loop through list of aggregation functions relevant to aggregation value.
    #
    # Note that order of (1) and (2) may be reversed in some cases, so we call a specialized generator to
    # generate the correct ordering here. The order is reversed when the aggregation functions passed in
    # to `pivot_table` is a list - as then the outermost layer of the index for the data columns must be
    # the aggregation function. E.g.:
    # In [1]: import pandas as native_pd

    # In [2]: df = native_pd.DataFrame({"A": ["foo", "foo", "foo", "foo", "foo",
    #    ...:                          "bar", "bar", "bar", "bar"],
    #    ...:                    "B": ["one", "one", "one", "two", "two",
    #    ...:                          "one", "one", "two", "two"],
    #    ...:                    "C": ["small", "large", "large", "small",
    #    ...:                          "small", "large", "small", "small",
    #    ...:                          "large"],
    #    ...:                    "D": [1, 2, 2, 3, 3, 4, 5, 6, 7],
    #    ...:                    "E": [2, 4, 5, 5, 6, 6, 8, 9, 9]})
    #
    # In [3]: table = native_pd.pivot_table(df, values=['D', 'E'], index=['A', 'B'],
    #    ...:                        columns=['C'], aggfunc={"D": ["sum", "max"], "E": ["sum", "max"]})
    #    ...: table
    # Out[3]:
    #             D                       E
    #           max         sum         max         sum
    # C       large small large small large small large small
    # A   B
    # bar one   4.0   5.0   4.0   5.0   6.0   8.0   6.0   8.0
    #     two   7.0   6.0   7.0   6.0   9.0   9.0   9.0   9.0
    # foo one   2.0   1.0   4.0   1.0   5.0   2.0   9.0   2.0
    #     two   NaN   3.0   NaN   6.0   NaN   6.0   NaN  11.0
    #
    # In [4]: table = native_pd.pivot_table(df, values=['D', 'E'], index=['A', 'B'],
    #    ...:                        columns=['C'], aggfunc=["sum", "max"])
    #    ...: table
    # Out[4]:
    #           sum                     max
    #             D           E           D           E
    # C       large small large small large small large small
    # A   B
    # bar one   4.0   5.0   6.0   8.0   4.0   5.0   6.0   8.0
    #     two   7.0   6.0   9.0   9.0   7.0   6.0   9.0   9.0
    # foo one   4.0   1.0   9.0   2.0   2.0   1.0   5.0   2.0
    #     two   NaN   6.0   NaN  11.0   NaN   3.0   NaN   6.0
    #
    # In the first example above, we iterate through the aggregation values first, but in the second example, we must
    # iterate through the aggregation functions first.
    #
    # 3. Perform pivot on the pivot columns for this aggregation value + aggfunc combination.
    #
    # The multi-level pandas prefix label that includes the aggregation value and function labels is also
    # constructed and passed into the single pivot operation to prepend the remaining of the pandas labels.
    for pivot_aggr_grouping in pivot_aggr_groupings:
        existing_snowflake_quoted_identifiers = groupby_snowflake_quoted_identifiers
        if last_ordered_dataframe is not None:
            existing_snowflake_quoted_identifiers = (
                last_ordered_dataframe.projected_column_snowflake_quoted_identifiers
            )

        (
            new_pivot_ordered_dataframe,
            new_data_column_snowflake_quoted_identifiers,
            new_data_column_pandas_labels,
        ) = single_pivot_helper(
            ordered_dataframe,
            existing_snowflake_quoted_identifiers,
            groupby_snowflake_quoted_identifiers,
            pivot_snowflake_quoted_identifiers,
            pivot_aggr_grouping.aggr_label_identifier_pair,
            pivot_aggr_grouping.aggfunc,
            pivot_aggr_grouping.prefix_label,
        )

        if last_ordered_dataframe:
            last_ordered_dataframe = last_ordered_dataframe.join(
                right=new_pivot_ordered_dataframe,
                left_on_cols=groupby_snowflake_quoted_identifiers,
                right_on_cols=groupby_snowflake_quoted_identifiers,
                how="left",
            )
        else:
            last_ordered_dataframe = new_pivot_ordered_dataframe

        data_column_snowflake_quoted_identifiers.extend(
            new_data_column_snowflake_quoted_identifiers
        )
        data_column_pandas_labels.extend(new_data_column_pandas_labels)

    ordered_dataframe = last_ordered_dataframe

    index_column_snowflake_quoted_identifiers = (
        ordered_dataframe.projected_column_snowflake_quoted_identifiers[
            0 : len(groupby_snowflake_quoted_identifiers)
        ]
    )
    index = index or [None] * len(index_column_snowflake_quoted_identifiers)

    # Generate the data column pandas index names
    if not isinstance(columns, list):
        columns = [columns]
    columns = [None] * len(pivot_aggr_groupings[0].prefix_label) + columns

    if expand_with_cartesian_product:
        # Ensure the cartesian product of index / group by rows.  For example, if there are index values
        # (a, b) and (c, z), then the cartesian product would be (a, b), (a, z), (c, b), (c, z).
        ordered_dataframe = expand_dataframe_with_cartesian_product_on_index(
            index_column_snowflake_quoted_identifiers, ordered_dataframe
        )

        # Ensure the cartesian product of pivot output columns based on the pandas labels.  For example, if there
        # are output data columns (a, b) and (c, z) then the cartesian product would be (a, b), (a, z), (c, b),
        # and (c, z).
        (
            data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers,
            ordered_dataframe,
        ) = expand_dataframe_with_cartesian_product_on_pivot_output(
            data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers,
            index_column_snowflake_quoted_identifiers,
            ordered_dataframe,
            sort_first_level,
        )

    # order by index column by default
    ordered_dataframe = ordered_dataframe.sort(
        [
            OrderingColumn(quoted_identifier)
            for quoted_identifier in index_column_snowflake_quoted_identifiers
        ]
    )
    return InternalFrame.create(
        ordered_dataframe=ordered_dataframe,
        data_column_pandas_labels=data_column_pandas_labels,
        data_column_pandas_index_names=columns,
        data_column_snowflake_quoted_identifiers=data_column_snowflake_quoted_identifiers,
        index_column_pandas_labels=index,
        index_column_snowflake_quoted_identifiers=index_column_snowflake_quoted_identifiers,
    )


def single_pivot_helper(
    pivot_ordered_dataframe: OrderedDataFrame,
    existing_snowflake_quoted_identifiers: list[str],
    groupby_snowflake_quoted_identifiers: Optional[list[str]],
    pivot_snowflake_quoted_identifiers: Optional[list[str]],
    value_label_to_identifier_pair: PandasLabelToSnowflakeIdentifierPair,
    pandas_aggr_func_name: str,
    prefix_pandas_labels: tuple[LabelComponent],
) -> tuple[OrderedDataFrame, list[str], list[Hashable]]:
    """
    Helper function that is a building block for generating a single pivot, that can be used by other pivot like
    functions or multi-pivot composition.

    Args:
        pivot_ordered_dataframe: Original ordered DataFrame
        existing_snowflake_quoted_identifiers: Existing snowflake quoted identifiers that should not be used here.
        groupby_snowflake_quoted_identifiers: Group by identifiers, or None if not specified.
        pivot_snowflake_quoted_identifiers: Pivot identifiers, or None if not specified.
        value_label_to_identifier_pair: Aggregation value pandas label and snowflake quoted identifier mapping
        pandas_aggr_func_name: pandas label for aggregation function (since used as a label)
        prefix_pandas_labels: Any prefix labels that should be added to the result pivot column name, such as
            the aggregation function or other labels.

    Returns:
        Tuple of:
            Ordered DataFrame: pivot results joined with any last_pivot_ordered_dataframe
            data_column_snowflake_quoted_identifiers: new data column snowflake quoted identifiers this pivot result
            data_column_pandas_labels: new data column pandas labels for this pivot result
    """
    snowpark_aggr_func = get_snowflake_agg_func(pandas_aggr_func_name, {})
    if not is_supported_snowflake_pivot_agg_func(snowpark_aggr_func):
        # TODO: (SNOW-853334) Add support for any non-supported snowflake pivot aggregations
        raise KeyError(pandas_aggr_func_name)

    pandas_aggr_label, aggr_snowflake_quoted_identifier = value_label_to_identifier_pair

    data_column_snowflake_quoted_identifiers = []
    data_column_pandas_labels = []

    groupby_snowflake_quoted_identifiers = groupby_snowflake_quoted_identifiers or []

    # Select only the required columns so we narrow down the pivot to only the group by, pivot
    # and aggregation columns used in the underlying pivot source query.
    project_snowflake_quoted_identifiers = groupby_snowflake_quoted_identifiers.copy()
    if pivot_snowflake_quoted_identifiers:
        project_snowflake_quoted_identifiers.extend(pivot_snowflake_quoted_identifiers)

    project_snowflake_quoted_identifiers += [aggr_snowflake_quoted_identifier]

    pivot_ordered_dataframe = pivot_ordered_dataframe.select(
        project_snowflake_quoted_identifiers
    )

    index_snowflake_quoted_identifiers = (
        groupby_snowflake_quoted_identifiers or pivot_snowflake_quoted_identifiers or []
    )

    if not pivot_snowflake_quoted_identifiers or not aggr_snowflake_quoted_identifier:
        if not groupby_snowflake_quoted_identifiers:
            raise ValueError("No group keys passed!")

        # If there are no pivot columns, then we do group by and aggregation only.

        # TODO (SNOW-838808): Look at moving this to call groupby_agg so will handle arbitrary
        # group-by constructs like grouper and proper ordering, etc.  Right now this would require
        # dropping all non-relevant columns but drop currently doesn't drop __row_position__ or
        # __index__ so it would break the other path where groupby is done inside pivot.
        if aggr_snowflake_quoted_identifier:
            pivot_ordered_dataframe = pivot_ordered_dataframe.group_by(
                groupby_snowflake_quoted_identifiers,
                snowpark_aggr_func(aggr_snowflake_quoted_identifier).as_(
                    aggr_snowflake_quoted_identifier
                ),
            )
        else:
            # Snowpark doesn't allow a group-by without aggregation, so we do a distinct query instead.
            pivot_ordered_dataframe = get_groups_for_ordered_dataframe(
                pivot_ordered_dataframe, groupby_snowflake_quoted_identifiers
            )

    else:
        # If multiple pivot columns, then we need to generate all permutations of the pivot column names in output.
        if len(pivot_snowflake_quoted_identifiers) > 1:
            temp_pivot_column_name = f"{TEMP_PIVOT_COLUMN_PREFIX}{random_name_for_temp_object(TempObjectType.COLUMN)}"
            pivot_snowflake_quoted_identifier = (
                pivot_ordered_dataframe.generate_snowflake_quoted_identifiers(
                    pandas_labels=[temp_pivot_column_name],
                    excluded=existing_snowflake_quoted_identifiers,
                )[0]
            )

            # Generate an object with 0-based key index, and value being the pivot column name.  For example, if we
            # are pivoting columns with snowflake quoted identifiers A and B, then we would generate object
            # object_construct("0", col(A), "1", col(B)).  Note that the key index literal is required to be a string
            # so we convert to string in python if it is an int.
            object_construct_key_values = [
                pandas_lit(str(kv)) if isinstance(kv, int) else kv
                for sub_key_values_list in list(
                    enumerate(
                        [
                            col(snowflake_quoted_identifier)
                            for snowflake_quoted_identifier in pivot_snowflake_quoted_identifiers
                        ]
                    )
                )
                for kv in sub_key_values_list
            ]

            # We use the OBJECT_CONSTRUCT to generate in this case serializing as a json object of values.
            select_snowflake_quoted_identifiers = (
                groupby_snowflake_quoted_identifiers
                + [
                    object_construct(*object_construct_key_values)
                    .cast(StringType())
                    .as_(pivot_snowflake_quoted_identifier)
                ]
                + [aggr_snowflake_quoted_identifier]
            )

            pivot_ordered_dataframe = pivot_ordered_dataframe.select(
                *select_snowflake_quoted_identifiers
            )
        else:
            pivot_snowflake_quoted_identifier = pivot_snowflake_quoted_identifiers[0]

        (
            pivot_ordered_dataframe,
            snowpark_aggr_func,
        ) = prepare_pivot_aggregation_for_handling_missing_and_null_values(
            aggr_snowflake_quoted_identifier,
            groupby_snowflake_quoted_identifiers + [pivot_snowflake_quoted_identifier],
            pivot_ordered_dataframe,
            snowpark_aggr_func,
        )

        # Perform the snowpark pivot operation grouping followed by the aggregation.
        pivot_ordered_dataframe = pivot_ordered_dataframe.pivot(
            pivot_snowflake_quoted_identifier,
            None,
            None,
            snowpark_aggr_func(aggr_snowflake_quoted_identifier),
        )

    if not groupby_snowflake_quoted_identifiers:
        # If there are no groupby columns, then use the aggregation column label.
        if aggr_snowflake_quoted_identifier and isinstance(
            pivot_snowflake_quoted_identifiers, list
        ):
            pivot_ordered_dataframe = pivot_ordered_dataframe.select(
                pandas_lit(pandas_aggr_label).as_(
                    pivot_snowflake_quoted_identifiers[0]
                ),
                "*",
            )

    # Go through each of the non-group by columns and
    # 1. Generate corresponding pandas label (without prefix)
    # 2. Drop any that are None
    # 3. Add prefix pandas label if provided
    # 4. Generate output data_columns
    for (
        snowflake_quoted_identifier
    ) in pivot_ordered_dataframe.projected_column_snowflake_quoted_identifiers[
        len(index_snowflake_quoted_identifiers) :
    ]:
        if (
            pivot_snowflake_quoted_identifiers
            and len(pivot_snowflake_quoted_identifiers) > 1
            and aggr_snowflake_quoted_identifier
        ):
            pandas_label = (
                extract_pandas_label_from_object_construct_snowflake_quoted_identifier(
                    snowflake_quoted_identifier, len(pivot_snowflake_quoted_identifiers)
                )
            )

            # Drop any multi-index that contains None values.
            if None in pandas_label:
                continue

        else:
            pandas_label = convert_snowflake_string_constant_to_python_string(
                extract_pandas_label_from_snowflake_quoted_identifier(
                    snowflake_quoted_identifier
                )
            )

        # If there are prefix pandas labels, then rename the snowflake columns to include the prefix.  This helps
        # produce pandas matching output as well as disambiguating joins if there is a last pivot df provided.
        if prefix_pandas_labels:
            pandas_label = prefix_pandas_labels + (
                pandas_label if isinstance(pandas_label, tuple) else (pandas_label,)
            )
            pandas_label_column = str(pandas_label)
        else:
            pandas_label_column = pandas_label

        # If the snowflake quoted identifier conflicts with an earlier identifier, ensure it is unique in snowflake
        renamed_snowflake_quoted_identifier = (
            pivot_ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=[pandas_label_column],
                excluded=existing_snowflake_quoted_identifiers,
            )[0]
        )

        if renamed_snowflake_quoted_identifier != snowflake_quoted_identifier:
            pivot_ordered_dataframe = append_columns(
                pivot_ordered_dataframe,
                renamed_snowflake_quoted_identifier,
                col(snowflake_quoted_identifier),
            )
            snowflake_quoted_identifier = renamed_snowflake_quoted_identifier

        data_column_snowflake_quoted_identifiers.append(snowflake_quoted_identifier)
        data_column_pandas_labels.append(pandas_label)

    return (
        pivot_ordered_dataframe,
        data_column_snowflake_quoted_identifiers,
        data_column_pandas_labels,
    )


def prepare_pivot_aggregation_for_handling_missing_and_null_values(
    aggr_snowflake_quoted_identifier: str,
    grouping_snowflake_quoted_identifiers: list[str],
    pivot_ordered_dataframe: OrderedDataFrame,
    snowpark_aggr_func: Union[Callable, str],
) -> tuple[OrderedDataFrame, Union[Callable, str]]:
    """
    Generates the pre-pivot aggregation required for sum and count to match pandas behavior.  This method is
    intended to be called within single_pivot_helper prior to performing a pivot with count or sum aggfunc.

    pandas and snowflake pivot have subtle different behavior for sum and count with respect to None (np.nan)
    and empty values.  If there are only None values in the grouping then pandas sum and count will
    return 0 as pivot value, however, if there are *no* values in the grouping then pandas returns None.
    On the other hand, snowflake does not distinguish no values from all null (mapped to None/np.nan in pandas)
    values, in these cases snowflake pivot returns 0 for count and null for sum.  To streamline the behavior
    here, we do an explicit group-by and aggregation of the grouping columns *AND* pivot column prior to
    snowflake pivot to ensure they would have 0 in this case and any empty groupings would return null.

    An example that demonstrates the issue is:

    df_data_small = pd.DataFrame(data={
        "A": [ "foo", "foo", "bar", ],
        "B": [ "one", "two", "one", ],
        "C": [0, 1, None, ],
    })

    df_data_small.pivot_table(index=["A"], columns="B", values="C", aggfunc=["count", "sum"])

    In this case, notice there are groupings like (bar, one) with only null values as well as missing
    groupings (bar, two) that are not in the dataset.  The snowflake pivot relies on object_agg and get
    together which do not distinguish between null values and no values.  For example, when expanding the pivot
    output columns, for sum both (bar, one) and (bar, two) return null and for count both
    (bar, one) and (bar, two) return 0.  The expected pandas pivot result:

           | count | count | sum | sum
       B   | one   | two   | one | two
       A   |       |       |     |
     ------+-------+-------+-----+-------
      bar  | 0.0   | Nan   | 0.0 | Nan
      foo  | 1.0   | 1.0   | 0.0 | 1.0

    To match pandas behavior, we do an upfront group-by aggregation for count and sum to get the correct
    values for all null values via snowflake query:

    select a, b, coalesce(sum(C), 0) as sum_c, count(C) as cnt_c from df_small_data group by a, b;

      A   | B   | SUM_C | CNT_C
     -----+-----+-------+-------
      foo | one | 0     | 1
      foo | two | 1     | 1
      bar | one | 0     | 0

    Notice (bar, one) with all None values has the matching aggregation result, and (bar, two) is missing
    but will aggregate as null also matching pandas behavior via the pivot operation itself.

    Args:
        aggr_snowflake_quoted_identifier: Aggregation column snowflake quoted identifier
        grouping_snowflake_quoted_identifiers: Grouping snowflake quoted identififers
        pivot_ordered_dataframe: Snowpark df
        snowpark_aggr_func: Aggregation function to be performed.

    Returns:
        Snowpark dataframe that has done an pre-pivot aggregation needed for matching pandas pivot behavior as
        described earlier.
    """
    if snowpark_aggr_func in [sum_, count]:
        agg_expr = (
            coalesce(sum_(aggr_snowflake_quoted_identifier), pandas_lit(0)).as_(
                aggr_snowflake_quoted_identifier
            )
            if snowpark_aggr_func == sum_
            else count(aggr_snowflake_quoted_identifier).as_(
                aggr_snowflake_quoted_identifier
            )
        )
        pre_pivot_ordered_dataframe = pivot_ordered_dataframe.group_by(
            grouping_snowflake_quoted_identifiers, agg_expr
        )

        # Since we have pre-warmed the snowflake pivot aggregation, we do min for simplicity to pick the aggregation
        # value.  Most aggregation functions would work here since there will only be one value for the grouping.
        return pre_pivot_ordered_dataframe, min_

    return pivot_ordered_dataframe, snowpark_aggr_func


def generate_pivot_aggregation_value_label_snowflake_quoted_identifier_mappings(
    values: Union[list[Optional[str]], str],
    internal_frame: InternalFrame,
) -> list[PandasLabelToSnowflakeIdentifierPair]:
    """
    Generate the pivot values list so it can be used for iteration of the single pivots, this returns the pandas label
    and corresponding snowflake quoted identifiers.  This includes the following items:

    1) If a value is provided as a single value and not a list, it is converted as a list
    2) If there are no values, then a single [None] list is returned for both values and snowflake quoted identifiers.
    3) If a value is a duplicate, then value for both duplicates (corresponding to different snowflake quoted
    identifiers are returned)

    Args:
        values: Pivot values (aggregation columns)
        internal_frame: Pivot internal frame

    Returns:
          List of pandas label to snowflake quoted identifiers pairs
    """
    assert values is not None

    values = [values] if isinstance(values, str) else values

    values_label_to_identifiers = []

    # It's okay in pandas not to have any values in this case, it's just a group-by.
    if len(values) == 0:
        return [PandasLabelToSnowflakeIdentifierPair(None, None)]

    for value, snowflake_quoted_identifiers in zip(
        values,
        internal_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
            values, include_index=False
        ),
    ):
        if len(snowflake_quoted_identifiers) == 0:
            raise KeyError(value)

        for snowflake_quoted_identifier in snowflake_quoted_identifiers:
            values_label_to_identifiers.append(
                PandasLabelToSnowflakeIdentifierPair(value, snowflake_quoted_identifier)
            )

    return values_label_to_identifiers


def generate_single_pivot_labels(
    values_pandas_label_to_identifiers: list[PandasLabelToSnowflakeIdentifierPair],
    aggfunc: AggFuncType,
    has_pivot_columns: bool,
    include_aggr_label_in_pandas_label: bool,
    sort: bool,
) -> Generator[PivotAggrGrouping, None, None]:
    """
    Generator to generate the correct ordering for pandas labels.  There are two cases we have with pandas pivot_table,
    if the aggfunc is a List, then the topmost level is the aggregation function followed by aggregation value.
    Otherwise, the topmost level is the aggregation value followed by the aggregation functions applies to that value.

    There are also some rules to determine if labels (aggregation function label or aggregation value label) are
    omitted, such as if aggregation values is a single value (not a list) and there is at least one pivot column.

    The prefix_pandas_labels that is returned is a prefix that needs to be added to the underlying single pivot, that
    includes any aggregation function or aggregation value name.  The single_pivot_helper later combines this with
    the multi-pivot pandas labels that are produced to get the resulting output pandas labels.  For example:

    df_data.pivot_table(index=['A'], columns=['B', 'C'], values=['D', 'E'], aggfunc={'D': ['count', 'sum'], 'E': 'max'})

    for the single pivot involving value='D', aggfunc='count' the generated prefix_pandas_label would be ('D', 'count')
    and the underlying pivot concatenates with the multi-pivot labels ('one', 'dull'), ('one', 'shiny'), ... etc
    to generate the full output pandas labels ('D', 'count', 'one', 'dull'), (D', 'count', 'one', 'shiny'), ... to
    matches the expected pandas output.

    Args:
        values_pandas_label_to_identifiers: Normalized list of aggregation values and snowflake quoted identifiers
        aggfunc: Aggregation function specification, could be single aggfunc, list or dictionary mapping.
        has_pivot_columns: Whether there is at least one pivot column specified.
        include_aggr_label_in_pandas_label: Whether to default to including the aggregation label in the pandas label
        sort: Whether sorting is specified, if True then the order of column labels such as aggregration
            functions will be sorted, otherwise if False will be the order of the original aggfunc list.

    Returns:
        Tuple of inputs for the next single pivot operation in expected ordering to match pandas pivot_table.
            prefix_pandas_labels: Prefix to apply to pandas label, may include aggregation or value.
            value_pandas_label_to_identifiers: Aggregation value pandas label to snowflake quoted identifier
            pandas_single_aggr_func: pandas aggregation function to apply to pandas aggregation label
    """
    if isinstance(aggfunc, list):
        # Fetch all aggregation functions, it will be the same aggregation function list for each aggregation value.
        (
            pandas_aggfunc_list,
            include_aggfunc_prefix,
        ) = get_pandas_aggr_func_and_prefix(aggfunc, None, sort)
        # In this case the aggfunc is a list of agg functions and to match pandas behavior, we always
        # include the aggfunc name in the resulting pandas labels.
        assert (
            include_aggfunc_prefix is True
        ), "aggr func should add prefix to resulting pandas label"
        assert pandas_aggfunc_list is not None

        # 1. Loop through all aggregation functions for this aggregation value.
        for pandas_single_aggr_func in pandas_aggfunc_list:
            # 2. Loop through all aggregation values
            for value_pandas_label_to_identifier in values_pandas_label_to_identifiers:
                pandas_aggr_label, _ = value_pandas_label_to_identifier
                prefix_pandas_labels = (
                    [get_pandas_aggr_func_name(pandas_single_aggr_func)]
                ) + (
                    [pandas_aggr_label]
                    if has_pivot_columns and include_aggr_label_in_pandas_label
                    else []
                )

                yield PivotAggrGrouping(  # type: ignore[misc]
                    prefix_label=tuple(prefix_pandas_labels),
                    aggr_label_identifier_pair=value_pandas_label_to_identifier,
                    aggfunc=pandas_single_aggr_func,
                )
    else:
        # 1. Loop through all aggregation values
        for value_pandas_label_to_identifier in values_pandas_label_to_identifiers:
            pandas_aggr_label, _ = value_pandas_label_to_identifier

            # Fetch all aggregation functions that apply to this aggregation value.
            (
                pandas_aggfunc_list,
                include_aggfunc_prefix,
            ) = get_pandas_aggr_func_and_prefix(aggfunc, pandas_aggr_label, sort)

            if not pandas_aggfunc_list:
                continue

            # 2. Loop through all aggregation functions for this aggregation value.
            for pandas_single_aggr_func in pandas_aggfunc_list:
                # pandas only adds aggregation value as label if provided as a list
                # Insert the aggregation function into the label at expected level
                prefix_pandas_labels = (
                    [pandas_aggr_label] if include_aggr_label_in_pandas_label else []
                ) + (
                    [get_pandas_aggr_func_name(pandas_single_aggr_func)]
                    if include_aggfunc_prefix
                    else []
                )

                yield PivotAggrGrouping(  # type: ignore[misc]
                    prefix_label=tuple(prefix_pandas_labels),
                    aggr_label_identifier_pair=value_pandas_label_to_identifier,
                    aggfunc=pandas_single_aggr_func,
                )


def get_pandas_aggr_func_and_prefix(
    aggfunc: AggFuncType,
    aggr_pandas_label: Optional[Hashable],
    sort: bool,
) -> tuple[Optional[list[AggFuncTypeBase]], bool]:
    """
    Retrieve the aggfunc for this aggregation value along with whether to include the aggfunc label in the output label.

    1) if aggfunc=['count', 'min'] then aggfunc label is the top-most level (0), ie. (aggfunc_label, aggr_col, ...)
    2) if aggfunc are single for each aggr_col, such as aggfunc='min' or aggfunc=={'D': 'min', 'E': 'max'} then
    the aggfunc label is omitted, ie. (aggr_col, ...)
    3) if aggfunc is a dict with multiple values for a label, such as aggfunc={'D': ['min', 'max'], 'E': 'count'}
    then the aggfunc label occurs at the second-level (1), ie. (aggr_col, aggrfunc_label, ...)

    Args:
        aggfunc: Aggregation function name, list or dictionary.
        aggr_pandas_label: Aggregation pandas label the aggfunc will apply to.
        sort: Whether to sort if aggfunc is a dictionary with list value.

    Returns:
        Tuple of
            List of aggregation functions to apply for this aggr pandas label.
            Whether the aggregation function should be added as a prefix.

        If pandas_aggr_func None is returned then the aggregation should be skipped because the specification was
        missing (this can happen in cases of a dictionary that doesn't include the aggregation label as a key.)
    """
    if isinstance(aggfunc, dict):
        # If they provide a dict for aggfuncs (mapping between column and its aggfuncs(s)), if the current
        # pivot values column is not in the aggfunc, then we'll skip this values column.
        if aggr_pandas_label not in aggfunc:
            pandas_aggr_func = None
        else:
            pandas_aggr_func = aggfunc[aggr_pandas_label]

            if not isinstance(pandas_aggr_func, list):
                pandas_aggr_func = [pandas_aggr_func]

            if sort:
                pandas_aggr_func.sort(key=lambda func: get_pandas_aggr_func_name(func))

        include_prefix = any([isinstance(af, list) for af in aggfunc.values()])

    elif isinstance(aggfunc, list):
        pandas_aggr_func = aggfunc

        if len(pandas_aggr_func) == 0:
            raise ValueError("Expected at least one aggregation function")

        include_prefix = True
    else:
        pandas_aggr_func = [aggfunc]
        include_prefix = False

    return pandas_aggr_func, include_prefix


def expand_dataframe_with_cartesian_product_on_index(
    groupby_snowflake_quoted_identifiers: list[str],
    ordered_dataframe: OrderedDataFrame,
) -> OrderedDataFrame:
    """
    Generate the cartesian product on group by snowflake identifiers.  For example, if there are only
    two group-by rows (bar, one) and (foo, two), then resulting snowpark dataframe would additionally have
    null rows for (bar, two) and (foo, one) so the full cartesian product of group-by snowflake quoted
    identifiers are present in the resulting dataframe.

    Example:
        df = pd.DataFrame({'A': ['bar', 'foo'], 'B': ['one', 'two'], 'F': [1, 2])

        -------------------
        | "A" | "B" | "F" |
        -------------------
        | bar | one | 1   |
        | foo | two | 2   |
        -------------------

        expand_dataframe_with_cartesian_product_on_group_by_snowflake_identifiers(['A', 'B'], df) returns

        --------------------
        | "A" | "B" | "F"  |
        --------------------
        | bar | one | 1    |
        | bar | two | None |
        | foo | one | None |
        | foo | two | 2    |
        --------------------

        In this case, the rows (bar, two), (foo, one) were added.

    Args:
            groupby_snowflake_quoted_identifiers: Group by snowflake quoted identifiers that will be expanded to
                the full cartesian product in the output.
            ordered_dataframe: Ordered dataframe

    Returns:
        Resulting snowpark dataframe containing full cartesian product of group by snowflake quoted identififers.
    """

    # Since we want to generate null rows that would otherwise not exist in the snowflake output, we must
    # create them using cross join of all the distinct group-by column combinations.
    distinct_groupby_ordered_dataframes = [
        get_distinct_rows(ordered_dataframe.select(snowflake_quoted_identifier))
        for snowflake_quoted_identifier in groupby_snowflake_quoted_identifiers
    ]

    full_na_ordered_dataframe = reduce(
        lambda df1, df2: df1.join(df2, how="cross"),
        distinct_groupby_ordered_dataframes,
    )

    # Join the full set of group-by permutations with original data to create null rows for any missing.
    ordered_dataframe = full_na_ordered_dataframe.join(
        right=ordered_dataframe,
        left_on_cols=groupby_snowflake_quoted_identifiers,
        right_on_cols=groupby_snowflake_quoted_identifiers,
        how="outer",
    )

    return ordered_dataframe


def expand_dataframe_with_cartesian_product_on_pivot_output(
    data_column_pandas_labels: list[Hashable],
    data_column_snowflake_quoted_identifiers: list[str],
    index_column_snowflake_quoted_identifiers: list[str],
    ordered_dataframe: OrderedDataFrame,
    sort_first_level: bool,
) -> tuple[list[Hashable], list[str], OrderedDataFrame]:
    """
    This expands the dataframe to contain the full cartesian product of pandas labels.

    Example:
        Suppose there is dataframe with
            data column panda labels: (E, min, a, x), (E, min, b, y), (F, max, a, x), (F, max, b, y)
            with corresponding data snowflake quoted identifiers.
        then the output would be:
            data column panda labels:
                (E, min, a, x), (E, min, a, y), (E, min, b, x), (E, min, b, y),
                (E, max, a, x), (E, max, a, y), (E, max, b, x), (E, max, b, y),
                (F, min, a, x), (F, min, a, y), (F, min, b, x), (F, min, b, y)
                (F, max, a, x), (F, max, a, y), (F, max, b, x), (F, max, b, y)
            with corresponding data snowflake quoted identifiers.  The new columns are
            added with null values.

    Args:
        data_column_pandas_labels : data column pandas labels
        data_column_snowflake_quoted_identifiers : data column snowflake quoted identifiers
        index_column_snowflake_quoted_identifiers : index column snowflake quoted identifiers
        ordered_dataframe : Ordered dataframe
        sort_first_level : whether to sort the first level of the pandas label explicitly

    Returns:
        Tuple of
            Expanded data pandas labels
            Expanded data snowflake quoted identifiers
            Snowpark dataframe including margin columns and final margin
    """
    pandas_label_by_level: dict[int, list[str]] = {}
    pandas_label_tuple_to_snowflake_quoted_identifier: dict[LabelTuple, str] = {}

    # First break down the pandas_labels by level, so we have a level -> label str mapping.
    for pandas_label, snowflake_quoted_identifier in zip(
        data_column_pandas_labels, data_column_snowflake_quoted_identifiers
    ):
        pandas_label_tuple = from_pandas_label(
            pandas_label,
            len(pandas_label) if isinstance(pandas_label, tuple) else 1,
        )

        for level, pandas_level_label in enumerate(pandas_label_tuple):
            if level not in pandas_label_by_level:
                pandas_label_by_level[level] = []
            if pandas_level_label not in pandas_label_by_level[level]:
                pandas_label_by_level[level].append(pandas_level_label)

        pandas_label_tuple_to_snowflake_quoted_identifier[
            pandas_label_tuple
        ] = snowflake_quoted_identifier

    # Generate a list of the labels (sorted) and expected at each level.
    cartesian_product_pandas_labels_list = []
    for level in range(0, len(pandas_label_by_level)):
        pandas_labels_at_level = pandas_label_by_level[level]

        # If the aggfunc is a List then the top-level label (the aggregation function name) is not sorted but
        # retains its original ordering.  Otherwise, note this is always sorted regardless of whether sort is specified.
        if level >= 1 or sort_first_level:
            pandas_labels_at_level.sort()

        cartesian_product_pandas_labels_list.append(pandas_labels_at_level)

    # Generate the cartesian product based on the level labels.
    pandas_cartesian_product_labels = list(
        product(*cartesian_product_pandas_labels_list)
    )

    # Check if the size matches, if so, this means we already have the cartesian product and can skip this.
    if len(pandas_cartesian_product_labels) != len(data_column_pandas_labels):
        expanded_data_column_pandas_labels = []
        expanded_new_data_column_snowflake_quoted_identifiers = []

        select_snowflake_quoted_identifiers_with_null_columns = (
            index_column_snowflake_quoted_identifiers.copy()
        )

        # For the cartesian product labels, if it's an existing pandas label in the dataframe, then reference it
        # otherwise generate a new snowflake quoted identifier with null initial value.
        for pandas_label_tuple in pandas_cartesian_product_labels:
            pandas_label = to_pandas_label(pandas_label_tuple)
            if pandas_label_tuple in pandas_label_tuple_to_snowflake_quoted_identifier:
                snowflake_quoted_identifier = (
                    pandas_label_tuple_to_snowflake_quoted_identifier[
                        pandas_label_tuple
                    ]
                )
                select_snowflake_quoted_identifiers_with_null_columns.append(
                    snowflake_quoted_identifier
                )
            else:
                snowflake_quoted_identifier = (
                    ordered_dataframe.generate_snowflake_quoted_identifiers(
                        pandas_labels=[pandas_label],
                    )[0]
                )
                select_snowflake_quoted_identifiers_with_null_columns.append(
                    pandas_lit(None).cast(DoubleType()).as_(snowflake_quoted_identifier)
                )

            expanded_data_column_pandas_labels.append(pandas_label)
            expanded_new_data_column_snowflake_quoted_identifiers.append(
                snowflake_quoted_identifier
            )

        ordered_dataframe = ordered_dataframe.select(
            select_snowflake_quoted_identifiers_with_null_columns
        )
    else:
        expanded_data_column_pandas_labels = data_column_pandas_labels
        expanded_new_data_column_snowflake_quoted_identifiers = (
            data_column_snowflake_quoted_identifiers
        )

    return (
        expanded_data_column_pandas_labels,
        expanded_new_data_column_snowflake_quoted_identifiers,
        ordered_dataframe,
    )


def apply_fill_value_to_snowpark_column(
    col: SnowparkColumn,
    fill_value: Scalar,
) -> SnowparkColumn:
    """
    Returns snowpark column that has the fill_value applied to the respective column if needed.
    Argunents:
        col: Snowpark column
        fill_value: Fill value, reply on snowflake server to type check.
    Returns:
        Returns snowpark column with fill_value applied.
    """
    return coalesce(col, pandas_lit(fill_value))


def get_margin_aggregation(
    aggfunc: Union[Callable, str],
    snowflake_quoted_identifier: str,
) -> SnowparkColumn:
    """
    Normalizes the output of aggregation functions that are slightly different between pandas and snowflake.  For
    example, SUM will return 0 in pandas and null in snowflake if all values are null.

    Args:
        aggfunc: aggregation function, either a callable or string name
        snowflake_quoted_identifier: snowflake quoted identifier

    Returns:
        Snowpark column expression for the aggregation function result.
    """
    resolved_aggfunc = get_snowflake_agg_func(aggfunc, {})

    # This would have been resolved during the original pivot at an early stage.
    assert resolved_aggfunc is not None

    aggfunc_expr = resolved_aggfunc(snowflake_quoted_identifier)

    if resolved_aggfunc == sum_:
        aggfunc_expr = coalesce(aggfunc_expr, pandas_lit(0))

    return aggfunc_expr


def expand_pivot_result_with_pivot_table_margins(
    pivot_aggr_groupings: list[PivotAggrGrouping],
    groupby_snowflake_quoted_identifiers: list[str],
    pivot_snowflake_quoted_identifiers: list[str],
    original_ordered_dataframe: OrderedDataFrame,
    pivoted_qc: "SnowflakeQueryCompiler",  # type: ignore[name-defined] # noqa: F821
    margins_name: Optional[str] = None,
    fill_value: Optional[Scalar] = None,
) -> "SnowflakeQueryCompiler":  # type: ignore[name-defined] # noqa: F821
    """
    Expand dataframe with pivot table margins.  This includes adding a margin column for each pivot aggregation
    grouping and a final margin row with totals for each of the columns.  The resulting row position ordering is
    consistent with pandas.

    Args:
        pivot_aggr_groupings: List of pivot aggregation groupings composed of
            Label component prefix of the corresponding pandas labels
            pandas Label to snowflake identifier pair
            Aggregation function
        groupby_snowflake_quoted_identifiers : Group by snowflake quoted identifiers
        pivot_snowflake_quoted_identifiers : Pivot snowflake quoted identifiers
        original_ordered_dataframe : Original ordered dataframe (pre-pivot)
        pivoted_qc : The SnowflakeQueryCompiler result after regular pivot
        margins_name : Name of the margins, or default 'All' if None specified.
        fill_value: value used to fill the na elements for the margin columns/rows

    Returns:
        An SnowflakeQueryCompiler result with margin columns and rows appended to the pivot result.
    """
    margins_name = margins_name or DEFAULT_MARGINS_NAME

    # To calculate margins, we need to figure out the pivot result groupings since we need to add a margin column
    # to each pivot aggregation column.  To do this, we extract the pandas label prefix that is associated with
    # a grouping.  For example, if there is a pandas label ('min', 'A', 'x', 'y') then ('min', 'A') would be the
    # shared prefix of all pandas labels in that pivot result grouping and we would add a margin column such as
    # ('min', 'A', None, 'All') after this grouping.

    # Generate a map of prefix -> aggfunc for quick look up later.
    aggr_groupings_aggfunc_map = {
        grouping.prefix_label: grouping.aggfunc for grouping in pivot_aggr_groupings
    }

    # Generate a map of prefix -> aggregation snowflake quoted identifier for quick look up later.
    aggr_groupings_snowflake_quoted_identifier_map = {
        grouping.prefix_label: grouping.aggr_label_identifier_pair.snowflake_quoted_identifier
        for grouping in pivot_aggr_groupings
    }

    # Collect the resulting pandas label and snowflake quoted identifiers
    updated_data_column_pandas_labels = []
    updated_data_column_snowflake_quoted_identifiers = []

    # The margin aggregations are calculated through the following steps.  Consider hypothetical pivot table input:
    #    pivot_snowflake_quoted_identifiers = ['B', 'C']
    #    groupby_snowflake_quoted_identifiers = ['A']
    #    data_column_pandas_labels = [(count, D, foo, red), (count, D, bar, blue),
    #                                 (sum, E, foo, red), (sum, E, bar, blue)]
    #    pivot_aggr_groupings = [(aggfunc=count, prefix=(count, D), aggr_pandas_label=[(D, ), "D"),
    #                            (aggfunc=sum, prefix=(sum, E), aggr_pandas_label=[(E, ), "E"])]
    #
    #          count         sum
    #          D             E
    #   A   B  foo     bar   foo     bar
    #       C  red     blue  red     blue
    # ---------------------------------------
    #     cat  5.0     NaN   3.0     NaN
    #     dog  7.0     2.0   5.0     1.0
    #
    # The final result in expanding margins includes new margin column (per grouping) corresponding final margin row.
    #
    #           count                 sum
    #           D                     E
    #   A   B   foo     bar   All     foo     bar      All
    #       C   red     blue          red     blue
    # --------------------------------------------------------
    #     cat   5.0     NaN     5      3.0     NaN     3
    #     dog   7.0     2.0     9      5.0     1.0     6
    #     All  12.0     2.0     14     8.0     1.0     9
    #
    # ie, the final "All" row, always at the bottom regardless of sort order and new data columns including:
    #   (count, D, All, ""), (sum, E, All, "")
    #
    # Note that in general, result pandas label can include aggfunc names, aggregation or pivot labels depending on the
    # specific parameters of the pivot_table.  Here we are only concerned with unique groupings related to single
    # pivot operation (pivot_aggr_groupings), so we track via the corresponding pandas label ie. [(count, D), (sum, E)]
    # in this example.

    # The projection for the final row margin which contains totals of each column.

    # margin_row_aggregations accumulates the expression for the final margin row aggregation, in this example:
    #     All  12.0     2.0     14     8.0     1.0     9
    margin_row_aggregations = []

    # margin_column_aggregations accumulates the expressions for the margin column aggregation, in this example:
    #     count    sum
    #         D      E
    #       All    All
    #
    #   --------------
    #         5      3
    #         9      6
    #        14      9
    margin_columns_aggregations = []

    # Step 1) Generate mapping of prefix to data columns aligned with each grouping.  In this example would generate:
    # (count, D) -> [(count, D, foo, red), (count, D, bar, blue)]
    # (sum, E) -> [(sum, E, foo, red), (sum, E, bar, blue)]
    prefix_len = len(pivot_aggr_groupings[0].prefix_label)
    assert all(
        len(pivot_aggr_groupings[0].prefix_label) == len(g.prefix_label)
        for g in pivot_aggr_groupings
    )
    num_levels = max(len(pivot_snowflake_quoted_identifiers), 1) + prefix_len

    pivoted_frame = pivoted_qc._modin_frame
    data_column_prefix_groupings = generate_column_prefix_groupings(
        pivoted_frame.data_column_pandas_labels,
        pivoted_frame.data_column_snowflake_quoted_identifiers,
        num_levels,
        prefix_len,
    )

    # Step 2) Iterate through each data column grouping, in this example for each of:
    #   (count, D) -> [(count, D, foo, red), (count, D, bar, blue)]
    #   (sum, E) -> [(sum, E, foo, red), (sum, E, bar, blue)]
    for data_column_prefix, data_column_grouping in data_column_prefix_groupings:
        # Step 2 (A):
        # Look up the aggfunc and aggregation snowflake quoted identifier for this grouping.  We need this to
        # perform the margin aggregation on the original dataframe.  In this example, it would return per iteration:
        #   iteration #1:   (count, D) -> aggfunc=count, aggr_snowflake_identifier='D'
        #   iteration #2:   (sum, E) -> aggfunc=sum, aggr_snowflake_identifier='E'
        original_aggr_func = aggr_groupings_aggfunc_map[data_column_prefix]
        aggr_snowflake_quoted_identifier = (
            aggr_groupings_snowflake_quoted_identifier_map[data_column_prefix]
        )

        # Step 2 (B):
        # For each data colum in the data column grouping, go through each data column and identifier, and generate
        # the corresponding final row margin expression.  This is an aggregation (aggfunc) on the designated
        # aggr_snowflake_identifier filtered down to the particular pivot column result.  In this example each
        # iteration would be set of data columns:
        #   iteration #1: [(count, D, foo, red), (count, D, bar, blue)]
        #   iteration #2: [(sum, E, foo, red), (sum, E, bar, blue)]
        for (
            data_column_pandas_label,
            data_column_snowflake_quoted_identifier,
        ) in data_column_grouping:
            # Output the data column to the expanded projection for the final result since pandas label and
            # snowflake identifiers are not changing here.
            updated_data_column_pandas_labels.append(data_column_pandas_label)
            updated_data_column_snowflake_quoted_identifiers.append(
                data_column_snowflake_quoted_identifier
            )

            # Extract the non-prefix part which provides the pivot parts in the result, this would be the
            # suffix label components in this example: [(foo, red,), (bar, blue,)]
            pivot_output_label_components = from_pandas_label(
                data_column_pandas_label, num_levels
            )[prefix_len:]

            # Step 2 (B.1):
            # Putting this together, we generate the margin aggregation (aggfunc) on the aggr snowflake identifier
            # (obtained from Step 2 (A)), filtering to the pivot_snowflake_quoted_identifiers=['B', 'C'] on the
            # original dataframe (note, this is not the pivot result dataframe!)  For example:
            #   iteration #1:
            #       aggfunc=count, aggr_snowflake_identifier='D'
            #       COUNT(IFF(col('B') == lit('foo') AND col('C') == lit('red'), col('D'), null))
            #
            #   iteration #1:
            #       aggfunc=sum, aggr_snowflake_identifier='E'
            #       SUM(IFF(col('B') == lit('foo') AND col('C') == lit('red'), col('E'), null))
            #
            # We also apply fill_value (if applicable) since pandas does this on the data columns, however, it does
            # not apply fill_value on the new margin columns.
            margin_row_aggregations.append(
                apply_fill_value_to_snowpark_column(
                    get_margin_aggregation(
                        original_aggr_func,
                        iff(
                            reduce(
                                lambda b1, b2: b1 & b2,
                                [
                                    (
                                        col(pivot_snowflake_quoted_identifier)
                                        == pandas_lit(pivot_value)
                                    )
                                    for pivot_value, pivot_snowflake_quoted_identifier in zip(
                                        pivot_output_label_components, pivot_snowflake_quoted_identifiers  # type: ignore[arg-type]
                                    )
                                ],
                            ),
                            col(aggr_snowflake_quoted_identifier),
                            None,
                        ),
                    ),
                    fill_value,
                ).as_(data_column_snowflake_quoted_identifier)
            )

        # Step 2 (C):
        # After each data column grouping, we generate a margin column expression which aggregates across pivot values.

        # Generate pandas label for the margin column, for example (for reference see the final result with expanded
        # margins in the original example earlier):
        #   iteration #1: [(count, D, All, "")]
        #   iteration #2: [(sum, E, All, ""]]
        margin_column_pandas_label = to_pandas_label(
            tuple(
                list(data_column_prefix)
                + [margins_name]
                + [""] * (num_levels - prefix_len - 1)
            )
        )

        # Generate the corresponding margin column snowflake quoted identifier for above pandas label.
        margin_column_aggr_snowflake_quoted_identifier = (
            pivoted_frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=[margin_column_pandas_label],
            )[0]
        )

        # For each iteration, the margin aggregation expression would be in this example:
        #   iteration #1: COUNT(col('D'))
        #   iteration #2: SUM(col('E'))
        margin_aggregation_expression = get_margin_aggregation(
            original_aggr_func, aggr_snowflake_quoted_identifier
        ).as_(margin_column_aggr_snowflake_quoted_identifier)

        # Add the margin aggregation expression (above) to the list of margin column aggregations.  These are
        # later grouped by the groupby_snowflake_quoted_identifiers to get the final result.
        margin_columns_aggregations.append(margin_aggregation_expression)

        # Add the margin aggregation for the final margin row margin, not grouped as it totals across all pivot values.
        margin_row_aggregations.append(margin_aggregation_expression)

        # Step 2 (D):
        # Add the margin column to the updated pandas label and corresponding snowflake identifiers, in this example:
        #   iteration #1: (count, D, All, "")
        #   iteration #2: (sum, E, All, "")
        updated_data_column_pandas_labels.append(margin_column_pandas_label)
        updated_data_column_snowflake_quoted_identifiers.append(
            margin_column_aggr_snowflake_quoted_identifier
        )

    # Step 3)
    # To generate the margin column aggregations we need to group by the groupby_snowflake_quoted_identifiers and join
    # back into the pivot result dataframe.  Note the resulting columns are already accumulated and ordered
    # in updated_pandas_label and updated_snowflake_quoted_identifiers, per iterations in Step 2 (B) and Step 2 (D).
    # In this example with margin_column_aggregations generated earlier and groupby_snowflake_quoted_identifiers=['A']
    #
    #   SELECT *
    #   FROM (
    #      SELECT A,
    #              COUNT(col('D')) AS "(count, D, All, '')",
    #              SUM(col('E')) AS "(sum, E, All, '')"
    #      FROM <original_snowpark_df>
    #      GROUP BY A
    #   ) T1 JOIN <pivot_snowpark_df> T2
    #   ON T1.A = T2.A
    margin_columns_ordered_dataframe = original_ordered_dataframe.group_by(
        groupby_snowflake_quoted_identifiers, *margin_columns_aggregations
    )

    pivoted_ordered_dataframe = pivoted_frame.ordered_dataframe.join(
        right=margin_columns_ordered_dataframe,
        left_on_cols=groupby_snowflake_quoted_identifiers,
        right_on_cols=groupby_snowflake_quoted_identifiers,
        how="outer",
    )
    pivoted_ordered_dataframe = pivoted_ordered_dataframe.sort(
        pivoted_frame.ordering_columns
    )

    pivoted_frame_with_column_margin = InternalFrame.create(
        ordered_dataframe=pivoted_ordered_dataframe,
        data_column_pandas_labels=updated_data_column_pandas_labels,
        data_column_snowflake_quoted_identifiers=updated_data_column_snowflake_quoted_identifiers,
        data_column_pandas_index_names=pivoted_frame.data_column_pandas_index_names,
        index_column_pandas_labels=pivoted_frame.index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=pivoted_frame.index_column_snowflake_quoted_identifiers,
    )

    from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
        SnowflakeQueryCompiler,
    )

    pivoted_qc = SnowflakeQueryCompiler(pivoted_frame_with_column_margin)

    # At this step, the pivot_snowpark_df not has the margin columns (although not yet ordered, that happens later).
    #
    #           count         sum              count   sum
    #           D             E                D       E
    #   A   B   foo     bar   foo     bar      All     All
    #       C   red     blue  red     blue
    # ------------------------------------------------------------
    #     cat   5.0     NaN   3.0      NaN     5       3
    #     dog   7.0     2.0   5.0    1 .0      9       6

    # Step 5)
    # Generate the final margin expanded dataframe by adding the margin row aggregations and ordering as expected.

    # Generate the dataframe for the final row margin aggregations which is the margin_row_aggregations
    # accumulated from Step 2 (B.1) and Step 2 (C).  Note that because we are adding a new index value for
    # the margin which are all string type.

    # create the values for the index column of the margin row
    margins_groupby_label_tuple = tuple(
        [margins_name]
        + [""]
        * (
            len(
                pivoted_frame_with_column_margin.index_column_snowflake_quoted_identifiers
            )
            - 1
        )
    )
    margin_row_groupby_select_list = [
        pandas_lit(label).cast(StringType()).as_(snowflake_quoted_identifier)
        for label, snowflake_quoted_identifier in zip(
            margins_groupby_label_tuple,
            pivoted_frame_with_column_margin.index_column_snowflake_quoted_identifiers,
        )
    ]

    ordering_columns = [
        OrderingColumn(quoted_identifier)
        for quoted_identifier in pivoted_frame_with_column_margin.index_column_snowflake_quoted_identifiers
    ]
    margin_row_df = original_ordered_dataframe.agg(
        *(margin_row_groupby_select_list + margin_row_aggregations)
    )
    margin_row_df = margin_row_df.sort(ordering_columns)
    margin_row_df_identifiers = (
        margin_row_df.projected_column_snowflake_quoted_identifiers
    )

    margin_row_frame = InternalFrame.create(
        ordered_dataframe=margin_row_df,
        data_column_pandas_labels=pivoted_frame_with_column_margin.data_column_pandas_labels,
        data_column_snowflake_quoted_identifiers=margin_row_df_identifiers[
            len(groupby_snowflake_quoted_identifiers) :
        ],
        data_column_pandas_index_names=pivoted_frame_with_column_margin.data_column_pandas_index_names,
        index_column_pandas_labels=pivoted_frame_with_column_margin.index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=margin_row_df_identifiers[
            0 : len(groupby_snowflake_quoted_identifiers)
        ],
    )
    single_row_qc = SnowflakeQueryCompiler(margin_row_frame)

    # append the margin_row_frame to the pivoted_frame_with_column_margin using concat to create the
    # final result frame.
    # Return the final dataframe and updated pandas labels including the margin row and columns.
    #
    #           count                 sum                     row_position
    #           D                     E
    #   A   B   foo     bar   All     foo     bar      All
    #       C   red     blue          red     blue
    # --------------------------------------------------------------------
    #     cat   5.0     NaN     5      3.0     NaN     3      0
    #     dog   7.0     2.0     9      5.0     1.0     6      1
    #     All  12.0     2.0     14     8.0     1.0     9      2
    pivoted_qc_with_margin = pivoted_qc.concat(axis=0, other=[single_row_qc])

    return pivoted_qc_with_margin


def generate_column_prefix_groupings(
    pandas_labels: list[Hashable],
    snowflake_quoted_identifiers: list[str],
    num_levels: int,
    prefix_len: int,
) -> list[tuple[tuple[LabelComponent], list[PandasLabelToSnowflakeIdentifierPair]]]:
    """
    Generate column prefix groupings.  Given a list of pandas label and corresponding snowflake identifiers,
    return a mapping of each prefix group.  The prefix is based on the pandas label prefix_len components, for
    example, if the pandas label is (a,b,c,d,e) and prefix_len=3 then we say the prefix is (a,b,c).

    Args:
        pandas_labels: data column pandas labels
        snowflake_quoted_identifiers: snowflake quoted identifiers
        num_levels: number of levels in pandas label
        prefix_len: prefix length for defining prefix of pandas label

    Returns:
        List of tuples
            Prefix
            pandas Label to Snowflake Identifier pair
    """
    margin_data_column_prefixes: list[tuple[LabelComponent]] = []
    margin_data_column_groupings: list[list[PandasLabelToSnowflakeIdentifierPair]] = []

    # Since the data columns may have been expanded, we go through and formally group them by prefix so we can
    # generate the correct projection with margin columns later on.
    last_pivot_grouping = None
    for pandas_label, snowflake_quoted_identifier in zip(
        pandas_labels, snowflake_quoted_identifiers
    ):
        pandas_label_prefix = from_pandas_label(pandas_label, num_levels)[:prefix_len]

        if last_pivot_grouping != pandas_label_prefix:
            margin_data_column_groupings.append([])
            last_pivot_grouping = pandas_label_prefix
            margin_data_column_prefixes.append(pandas_label_prefix)

        margin_data_column_groupings[-1].append(
            PandasLabelToSnowflakeIdentifierPair(
                pandas_label, snowflake_quoted_identifier
            )
        )

    return list(zip(margin_data_column_prefixes, margin_data_column_groupings))
