#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pandas as native_pd

from snowflake.snowpark.column import Column as SnowparkColumn
from snowflake.snowpark.functions import (
    array_construct,
    array_contains,
    cast,
    coalesce,
    col,
    to_variant,
)
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin._internal.indexing_utils import set_frame_2d_labels
from snowflake.snowpark.modin.plugin._internal.snowpark_pandas_types import (
    SnowparkPandasType,
)
from snowflake.snowpark.modin.plugin._internal.type_utils import infer_series_type
from snowflake.snowpark.modin.plugin._internal.utils import (
    append_columns,
    generate_new_labels,
    is_duplicate_free,
    pandas_lit,
)
from snowflake.snowpark.modin.plugin._typing import ListLike
from snowflake.snowpark.types import DataType, DoubleType, VariantType, _IntegralType


def convert_values_to_list_of_literals_and_return_type(
    values: ListLike,
) -> tuple[DataType, list[SnowparkColumn]]:
    """
    Given list-like (scalar) values, return a tuple of the datatype of a literal expression all values can attain,
    and a list of Snowpark literal expressions
    Args:
        values: list-like values to convert to literals.

    Returns:
        Tuple of datatype and list of literal expressions.
    """
    # helper function to convert list-like values to list of Snowpark literal expressions. Returns
    # the datatype for these literals as well.
    values_dtype = infer_series_type(native_pd.Series(values))

    # Use variant literals for heterogenous types within series, because in SQL for a query like
    # SELECT 'test' IN (7 :: INT, 'test', '89.9' :: FLOAT) Snowflake will implicitly attempt to cast the values
    # to FLOAT and produce an error because 'test' can't be cast.
    if isinstance(values_dtype, VariantType):
        return values_dtype, [pandas_lit(value, VariantType()) for value in values]
    else:
        return values_dtype, [pandas_lit(value) for value in values]


def scalar_isin_expression(
    quoted_identifier: str,
    values: list[SnowparkColumn],
    column_dtype: DataType,
    values_dtype: DataType,
) -> SnowparkColumn:
    """
    Generates isin-equivalent expression to be compatible with pandas behavior. Addresses the following cases for values:
        1. empty list.
        2. numeric values on either side requiring upcasting to float.
        3. isin involving variant on either side.

    Args:
        quoted_identifier: quoted identifier for which to apply isin expression, i.e. quoted_identifier.isin(values).
        values: values to check in-relationwship with quoted identifier.
        column_dtype: type of the column indexed through quoted_identifier.
        values_dtype: type of the values given as list of Snowpark expressions.

    Returns:
        Snowpark columnar expression for pandas-equivalent isin logic.
    """

    # Case 1: empty list: return False.
    if isinstance(values, list) and 0 == len(values):
        return pandas_lit(False)

    column = col(quoted_identifier)

    # Case 2: If either type of values/col(quoted_identifier) is float, upcast to float because of ORM mismatch.
    # Handle here col(quoted_identifier) being double type:
    if isinstance(values_dtype, _IntegralType) and isinstance(column_dtype, DoubleType):
        values = [cast(value, DoubleType()) for value in values]

    # Handle here values being double type:
    elif isinstance(values_dtype, DoubleType) and isinstance(
        column_dtype, _IntegralType
    ):
        column = cast(column, DoubleType())

    # Case 3: If column's and values' data type differs
    # perform isin over variant type when either side is variant type.
    elif values_dtype != column_dtype and (
        isinstance(values_dtype, VariantType) or isinstance(column_dtype, VariantType)
    ):
        # Ensure values are list of literals.
        values = [
            pandas_lit(literal_expr._expression.value, VariantType())
            for literal_expr in values
        ]

    # Case 4: If column's and values' data type differs and any of the type is SnowparkPandasType
    elif values_dtype != column_dtype and (
        isinstance(values_dtype, SnowparkPandasType)
        or isinstance(column_dtype, SnowparkPandasType)
    ):
        return pandas_lit(False)

    values = array_construct(*values)

    # to_variant is a requirement for array_contains, else an error is produced.
    return array_contains(to_variant(column), values)


def compute_isin_with_series(
    frame: InternalFrame, values_series: InternalFrame
) -> InternalFrame:
    """
    Computes new InternalFrame holding the result of DataFrame.isin(<Series obj>).

    Note that frame must be a non-empty DataFrame, i.e. frame must have row_count > 0.
    Assumes further that index.is_unique() holds for values_series.

    Args:
        frame: InternalFrame, lhs of the isin operation.
        values_series: InternalFrame representing the Series object

    Returns:
        InternalFrame
    """
    # For each row in this dataframe
    # align the index with the index of the values Series object.
    # If it matches, return True, else False

    # create new label and new identifier to store result of aggregating values into a single array representing
    # the unique values, i.e. array_agg(distinct ${data_column_quoted_identifier})
    agg_label = generate_new_labels(
        pandas_labels=["agg"],
        excluded=frame.data_column_pandas_labels,
    )[0]

    new_frame = set_frame_2d_labels(
        frame,
        slice(None),
        [agg_label],
        values_series,
        matching_item_columns_by_label=False,
        matching_item_rows_by_label=True,
        index_is_bool_indexer=False,
        deduplicate_columns=False,
        frame_is_df_and_item_is_series=False,
    )

    # apply isin operation for all columns except the appended agg_label/agg_identifier column.
    agg_identifier = new_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
        [agg_label]
    )[0][0]
    data_column_quoted_identifiers = set(
        new_frame.data_column_snowflake_quoted_identifiers
    ) - {agg_identifier}

    # to replicate NULL behavior like in other APIs, preserve NULLs here
    new_frame = new_frame.update_snowflake_quoted_identifiers_with_expressions(
        {
            quoted_identifier: coalesce(
                col(quoted_identifier) == col(agg_identifier),
                pandas_lit(False),
            )
            for quoted_identifier in data_column_quoted_identifiers
        }
    ).frame

    # local import to avoid circular import
    from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
        SnowflakeQueryCompiler,
    )

    # return internal frame but remove temporary agg column.
    return SnowflakeQueryCompiler(new_frame).drop(columns=[agg_label])._modin_frame


def compute_isin_with_dataframe(
    frame: InternalFrame, values_frame: InternalFrame
) -> InternalFrame:
    """
    Computes new InternalFrame holding the result of DataFrame.isin(<DataFra e obj>).

    Note that frame must be a non-empty DataFrame, i.e. frame must have row_count > 0.
    Assumes further that index.is_unique() holds for values_frame.

    Args:
        frame: InternalFrame, lhs of the isin operation.
        values_series: InternalFrame representing the DataFrame object (rhs)

    Returns:
        InternalFrame
    """
    # similar logic to series, however do not create a single column but multiple colunms
    # set values via set_frame_2d_labels then

    # duplicate all matching column labels, then overwrite with new value using set_frame_2d_labels,
    self_data_labels = frame.data_column_pandas_labels
    values_data_labels = values_frame.data_column_pandas_labels  # type: ignore[union-attr]

    # now generate new labels for matching column and prefix with isin_
    # produce here pandas compatible error that is commented in dataframe.py:
    # if not (values.columns.is_unique and values.index.is_unique):
    #    raise ValueError("cannot compute isin with a duplicate axis.")
    if not is_duplicate_free(values_data_labels):
        raise ValueError("cannot compute isin with a duplicate axis.")

    unique_matching_labels = sorted(
        list(set(values_data_labels) & set(self_data_labels))
    )

    new_labels = generate_new_labels(
        pandas_labels=[f"isin_{label}" for label in unique_matching_labels],
        excluded=self_data_labels,
    )
    new_ordered_frame = frame.ordered_dataframe
    new_identifiers = new_ordered_frame.generate_snowflake_quoted_identifiers(
        pandas_labels=new_labels
    )

    # For each column in values_frame, for which a matching label in frame exists, append
    # a column with NULL
    new_ordered_frame = append_columns(
        new_ordered_frame,
        new_identifiers,
        [pandas_lit(None)] * len(new_identifiers),
    )

    # Append duplicate columns and create new internal frame from it.
    new_frame = InternalFrame.create(
        ordered_dataframe=new_ordered_frame,
        data_column_pandas_labels=frame.data_column_pandas_labels + new_labels,
        data_column_pandas_index_names=frame.data_column_pandas_index_names,
        data_column_snowflake_quoted_identifiers=frame.data_column_snowflake_quoted_identifiers
        + new_identifiers,
        index_column_pandas_labels=frame.index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=frame.index_column_snowflake_quoted_identifiers,
        data_column_types=None,
        index_column_types=None,
    )

    # local import to avoid circular import
    from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
        SnowflakeQueryCompiler,
    )

    values_frame_with_matching_columns_only = (
        SnowflakeQueryCompiler(values_frame)
        .drop(  # type: ignore[union-attr]
            None,
            columns=list(
                set(values_frame.data_column_pandas_labels)
                - set(unique_matching_labels)
            ),
        )
        ._modin_frame
    )

    new_frame = set_frame_2d_labels(
        new_frame,
        slice(None),
        new_labels,
        values_frame_with_matching_columns_only,
        False,
        True,
        False,
        False,
        False,
    )

    isin_identifiers = [
        group[0]
        for group in new_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
            new_labels, False
        )
    ]

    # create pairs now, i.e. which original identifier to compare with which isin identifier.
    data_pairs = [
        (label, identifier)
        for label, identifier in zip(
            new_frame.data_column_pandas_labels,
            new_frame.data_column_snowflake_quoted_identifiers,
        )
        if label in unique_matching_labels
    ]
    isin_lookup = dict(zip(unique_matching_labels, isin_identifiers))

    pairs = [(identifier, isin_lookup[label]) for label, identifier in data_pairs]

    # replace by default all entries with False to reach pandas compatibility
    replace_dict = {
        quoted_identifier: pandas_lit(False)
        for quoted_identifier in new_frame.data_column_snowflake_quoted_identifiers
    }
    # matching columns are updated based on the match from the set_frame_2d
    replace_dict.update(
        {
            quoted_identifier: coalesce(
                col(quoted_identifier) == col(isin_quoted_identifier),
                pandas_lit(False),
            )
            for quoted_identifier, isin_quoted_identifier in pairs
        }
    )

    new_frame = new_frame.update_snowflake_quoted_identifiers_with_expressions(
        replace_dict
    ).frame

    # return query compiler but remove temporary agg column.
    return SnowflakeQueryCompiler(new_frame).drop(columns=new_labels)._modin_frame
