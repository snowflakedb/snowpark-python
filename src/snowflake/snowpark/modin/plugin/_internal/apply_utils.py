#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import json
import sys
from collections import namedtuple
from collections.abc import Hashable
from enum import Enum, auto
from typing import Any, Callable, Literal, Optional, Union

import cloudpickle
import numpy as np
import pandas as native_pd
from pandas._typing import AggFuncType
from pandas.api.types import is_scalar

from snowflake.snowpark._internal.type_utils import PYTHON_TO_SNOW_TYPE_MAPPINGS
from snowflake.snowpark._internal.udf_utils import get_types_from_type_hints
from snowflake.snowpark.column import Column as SnowparkColumn
from snowflake.snowpark.functions import builtin, col, dense_rank, udf, udtf
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin._internal.ordered_dataframe import (
    OrderedDataFrame,
    OrderingColumn,
)
from snowflake.snowpark.modin.plugin._internal.utils import (
    TempObjectType,
    generate_snowflake_quoted_identifiers_helper,
    parse_object_construct_snowflake_quoted_identifier_and_extract_pandas_label,
    parse_snowflake_object_construct_identifier_to_map,
)
from snowflake.snowpark.modin.utils import MODIN_UNNAMED_SERIES_LABEL
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import (
    ArrayType,
    DataType,
    IntegerType,
    LongType,
    MapType,
    PandasDataFrameType,
    PandasSeriesType,
    StringType,
    VariantType,
)
from snowflake.snowpark.udf import UserDefinedFunction
from snowflake.snowpark.udtf import UserDefinedTableFunction
from snowflake.snowpark.window import Window

APPLY_LABEL_COLUMN_QUOTED_IDENTIFIER = '"LABEL"'
APPLY_VALUE_COLUMN_QUOTED_IDENTIFIER = '"VALUE"'

# Default partition size to use when applying a UDTF. A higher value results in less parallelism, less contention and higher batching.
DEFAULT_UDTF_PARTITION_SIZE = 1000

# Use the workaround described below to use functions that are attributes of
# this module in UDFs and UDTFs. Without this workaround, we can't pickle
# those functions.
# https://github.com/cloudpipe/cloudpickle?tab=readme-ov-file#overriding-pickles-serialization-mechanism-for-importable-constructs
cloudpickle.register_pickle_by_value(sys.modules[__name__])


class GroupbyApplySortMethod(Enum):
    """
    A rule for sorting the rows resulting from groupby.apply.
    """

    UNSET = auto()

    # order by order of the input row that each output row originated from.
    ORIGINAL_ROW_ORDER = auto()
    # order by 1) comparing the group keys to each other 2) resolving
    # ties by the order within the result for each group. this is like
    # "sort=True" for groupby aggregations.
    GROUP_KEY_COMPARISON_ORDER = auto()
    # order by 1) ordering by the order in which the group keys appear
    # in the original frame 2) resolving ties by the order within the
    # result for each group. this is like "sort=false" for groupby
    # aggregations.
    GROUP_KEY_APPEARANCE_ORDER = auto()


def check_return_variant_and_get_return_type(func: Callable) -> tuple[bool, DataType]:
    """Check whether the function returns a variant in Snowflake, and get its return type."""
    return_type, _ = get_types_from_type_hints(func, TempObjectType.FUNCTION)
    if return_type is None or isinstance(
        return_type, (VariantType, PandasSeriesType, PandasDataFrameType)
    ):
        # By default, we assume it is a series-to-series function
        # However, vectorized UDF only allows returning one column
        # We will convert the result series to a list, which will be
        # returned as a Variant
        return_variant = True
    else:
        return_variant = False
    return return_variant, return_type


def create_udtf_for_apply_axis_1(
    row_position_snowflake_quoted_identifier: str,
    func: Union[Callable, UserDefinedFunction],
    raw: bool,
    result_type: Optional[Literal["expand", "reduce", "broadcast"]],
    args: tuple,
    column_index: native_pd.Index,
    input_types: list[DataType],
    session: Session,
    **kwargs: Any,
) -> UserDefinedTableFunction:
    """
    Creates a wrapper UDTF for `func` to produce narrow table results for row-wise `df.apply` (i.e., `axis=1`).
    The UDTF produces 3 columns: row position column, label column and value column.

    The label column maintains a json string from a dict, which contains
    a pandas label in the current series, and its occurrence. We need to
    record the occurrence to deduplicate the duplicate labels so the later pivot
    operation on the label column can create separate columns on duplicate labels.
    The value column maintains the value of the result after applying `func`.

    Args:
        row_position_snowflake_quoted_identifier: quoted identifier identifying the row position column passed into the UDTF.
        func: The UDF to apply row-wise.
        raw: pandas parameter controlling apply within the UDTF.
        result_type: pandas parameter controlling apply within the UDTF.
        args: pandas parameter controlling apply within the UDTF.
        column_index: The columns of the callee DataFrame, i.e. df.columns as pd.Index object.
        input_types: Snowpark column types of the input data columns.
        **kwargs: pandas parameter controlling apply within the UDTF.

    Returns:
        Snowpark vectorized UDTF producing 3 columns.
    """

    # If given as Snowpark function, extract packages.
    udf_packages = []
    if isinstance(func, UserDefinedFunction):
        # TODO: Cover will be achieved with SNOW-1261830.
        udf_packages = func._packages  # pragma: no cover
        func = func.func  # pragma: no cover

    class ApplyFunc:
        def end_partition(self, df):  # type: ignore[no-untyped-def] # pragma: no cover
            # First column is row position, set as index.
            df = df.set_index(df.columns[0])

            df.columns = column_index
            df = df.apply(
                func, axis=1, raw=raw, result_type=result_type, args=args, **kwargs
            )
            # When a dataframe is returned from `df.apply`,
            # `func` is a series-to-series function, e.g.,
            # def func(row):
            #    result = row + 1
            #    result.index.name = 'new_index_name'
            #    return result
            #
            # For example, the original dataframe is
            #    a  b  b
            # 0  0  1  2
            #
            # the result dataframe from `df.apply` is
            # new_index_name  a  b  b
            # 0               1  2  3
            # After the transformation below, we will get a dataframe with two
            # columns. Each row in the result represents the series result
            # at a particular position.
            #                                              "LABEL"  "VALUE"
            # 0  {"pos": 0, "0": "a", "names": ["new_index_name"]}        1
            # 1  {"pos": 1, "0": "b", "names": ["new_index_name"]}        2
            # 2  {"pos": 2, "0": "b", "names": ["new_index_name"]}        3
            # where:
            # - `pos` indicates the position within the series.
            # - The integer keys like "0" map from index level to the result's
            #   label at that level. In this case, the result only has one
            #   index level.
            # - `names` contains the names of the result's index levels.
            # - VALUE contains the result at this position.
            if isinstance(df, native_pd.DataFrame):
                result = []
                for row_position_index, series in df.iterrows():

                    for i, (label, value) in enumerate(series.items()):
                        # If this is a tuple then we store each component with a 0-based
                        # lookup.  For example, (a,b,c) is stored as (0:a, 1:b, 2:c).
                        if isinstance(label, tuple):
                            obj_label = {k: v for k, v in enumerate(list(label))}
                        else:
                            obj_label = {0: label}
                        obj_label["names"] = series.index.names
                        obj_label["pos"] = i
                        result.append(
                            [
                                row_position_index,
                                json.dumps(obj_label),
                                value,
                            ]
                        )
                # use object type so the result is json-serializable
                result = native_pd.DataFrame(
                    result, columns=["__row__", "label", "value"], dtype=object
                )
            # When a series is returned from `df.apply`,
            # `func` is a series-to-scalar function, e.g., `np.sum`
            # For example, the original dataframe is
            #    a  b
            # 0  1  2
            # and the result series from `df.apply` is
            # 0    3
            # dtype: int64
            # After the transformation below, we will get a dataframe with two columns:
            #        "LABEL"                        "VALUE"
            # 0  {'0': MODIN_UNNAMED_SERIES_LABEL}        3
            elif isinstance(df, native_pd.Series):
                result = df.to_frame(name="value")
                result.insert(0, "label", json.dumps({"0": MODIN_UNNAMED_SERIES_LABEL}))
                result.reset_index(names="__row__", inplace=True)
            else:
                raise TypeError(f"Unsupported data type {df} from df.apply")

            result["value"] = (
                result["value"]
                .apply(
                    lambda v: handle_missing_value_in_variant(
                        convert_numpy_int_result_to_int(v)
                    )
                )
                .astype(object)
            )
            return result

    ApplyFunc.end_partition._sf_vectorized_input = native_pd.DataFrame  # type: ignore[attr-defined]

    packages = list(session.get_packages().values()) + udf_packages
    func_udtf = udtf(
        ApplyFunc,
        output_schema=PandasDataFrameType(
            [LongType(), StringType(), VariantType()],
            [
                row_position_snowflake_quoted_identifier,
                APPLY_LABEL_COLUMN_QUOTED_IDENTIFIER,
                APPLY_VALUE_COLUMN_QUOTED_IDENTIFIER,
            ],
        ),
        input_types=[PandasDataFrameType([LongType()] + input_types)],
        # We have to use the current pandas version to ensure the behavior consistency
        packages=[native_pd] + packages,
        session=session,
    )

    return func_udtf


def convert_groupby_apply_dataframe_result_to_standard_schema(
    func_input_df: native_pd.DataFrame,
    func_output_df: native_pd.DataFrame,
    input_row_positions: native_pd.Series,
    include_index_columns: bool,
) -> native_pd.DataFrame:  # pragma: no cover: this function runs inside a UDTF, so coverage tools can't detect that we are testing it.
    """
    Take the result of applying the user-provided function to a dataframe, and convert it to a dataframe with known schema that we can output from a vUDTF.

    Args:
        func_input_df: The input to `func`, where `func` is the Python function
                       that the  user originally passed to apply().
        func_output_df: The output of `func`.
        input_row_positions: The original row positions of the rows that
                             func_input_df came from.
        include_index_columns: Whether to include the result's index columns in
                               the output.

    Returns:
        A 5-column dataframe that represents the function result per the
        description in create_udtf_for_groupby_apply.

    """
    result_rows = []
    result_index_names = func_output_df.index.names
    is_transform = func_output_df.index.equals(func_input_df.index)
    for row_number, (index_label, row) in enumerate(func_output_df.iterrows()):
        output_row_number = input_row_positions.iloc[row_number] if is_transform else -1
        if include_index_columns:
            if isinstance(index_label, tuple):
                for k, v in enumerate(index_label):
                    result_rows.append(
                        [
                            json.dumps({"index_pos": k, "name": result_index_names[k]}),
                            row_number,
                            v,
                            output_row_number,
                        ]
                    )
            else:
                result_rows.append(
                    [
                        json.dumps({"index_pos": 0, "name": result_index_names[0]}),
                        row_number,
                        index_label,
                        output_row_number,
                    ]
                )
        for col_number, (label, value) in enumerate(row.items()):
            obj_label: dict[Any, Any] = {}
            if isinstance(label, tuple):
                obj_label = {k: v for k, v in enumerate(list(label))}
            else:
                obj_label = {0: label}
            obj_label["data_pos"] = col_number
            obj_label["names"] = row.index.names
            result_rows.append(
                [
                    json.dumps(obj_label),
                    row_number,
                    convert_numpy_int_result_to_int(value),
                    output_row_number,
                ]
            )
    # use object type so the result is json-serializable
    result_df = native_pd.DataFrame(
        result_rows,
        columns=[
            "label",
            "row_position_within_group",
            "value",
            "original_row_number",
        ],
        dtype=object,
    )
    result_df["value"] = (
        result_df["value"]
        .apply(
            lambda v: handle_missing_value_in_variant(
                convert_numpy_int_result_to_int(v)
            )
        )
        .astype(object)
    )
    result_df["first_position_for_group"] = input_row_positions.iloc[0]
    return result_df


def create_groupby_transform_func(
    func: Callable, by: str, level: Any, *args: Any, **kwargs: Any
) -> Callable:
    """
    Helper function to create the groupby lambda required for DataFrameGroupBy.transform.
    This is a workaround to prevent pickling DataFrame objects: the pickle module will
    try to pickle all objects accessible to the function passed in.

    Args
    ----
    func: The function to create the groupby lambda required for DataFrameGroupBy.
    by: The column(s) to group by.
    level: If the axis is a MultiIndex (hierarchical), group by a particular level or levels.
           Do not specify both by and level.
    args: Function's positional arguments.
    kwargs: Function's keyword arguments.


    Returns
    -------
    A lambda function that can be used in place of func in groupby transform.
    """
    # - `dropna` controls whether the NA values should be included as a group/be present
    #    in the group keys. Therefore, it must be False to ensure that no values are excluded.
    # Setting `dropna=True` here raises the IndexError: "cannot do a non-empty take from an empty axes."
    # This is because any dfs created from the NA group keys result in empty dfs to work with,
    # which cannot be used with the `take` method.
    #
    # - `group_keys` controls whether the grouped column(s) are included in the index.
    # - `sort` controls whether the group keys are sorted.
    # - `as_index` controls whether the groupby object has group labels as the index.

    # The index of the result of any transform call is guaranteed to be the original
    # index. Therefore, the groupby parameters group_keys, sort, and as_index do not
    # affect the result of transform, and are not explicitly specified.

    return lambda df: (
        df.groupby(by=by, level=level, dropna=False).transform(func, *args, **kwargs)
    )


def create_udtf_for_groupby_apply(
    func: Callable,
    args: tuple,
    kwargs: dict,
    data_column_index: native_pd.Index,
    index_column_names: list,
    input_data_column_types: list[DataType],
    input_index_column_types: list[DataType],
    session: Session,
    series_groupby: bool,
    by_types: list[DataType],
    existing_identifiers: list[str],
) -> UserDefinedTableFunction:
    """
    Create a UDTF from the Python function for groupby.apply.

    The UDTF takes as input the following columns in the listed order:
    1. The original row position within the dataframe (not just within the group)
    2. All the by columns (these are constant across the group, but in the case
    #  of SeriesGroupBy, we need these so we can name each input series by the
    #  group label)
    3. All the index columns
    4. All the data columns

    The UDF returns as output the following columns in the listed order. There is
    one row per result row and per result column.
    1. The label for the row or index level value. This is a json string of a dict
       representing the label.

        For output rows representing data values, this looks like e.g. if the
        data column ('a', 'int_col') is the 4th column, and the entire column
        index has names ('l1', 'l2'):
            {"data_pos": 4, "0": "a", "1": "int_col", "names": ["l1", "l2"]}

        Note that "names" is common across all data columns.

        For values of an index level, this looks like e.g. if the index level
        3 has name "level_3":
            {"index_pos": 3, name: "level_3"}
    2. The row position of this result row within the group.
    3. The value of the index level or the data column at this row.
    4. For transforms, this gives the position of the input row that produced
       this result row. We need this for transforms when group_keys=False
       because we have to reindex the final result according to original row
       position. If `func` is not a transform, this position is -1.
    5. The position of the first row from the input dataframe that fell into
       this group. For example, if we are grouping by column "A", we divide
       the input dataframe into groups where column A is equal to "a1", where
       it's equal to "a2", etc. We then apply `func` to each group. If "a2"
       first appears in row position 0, then all output rows resulting from the
       "a2" group get a value of 0 for this column. If "a1" first appears in
       row position 1, then all output rows resulting from the "a1" group get
       a value of 1 for this column. e.g.:

        Input dataframe
        ---------------
        position      A     B
        0             a2   b0
        1             a1   b1
        2             a2   b2


        Input Groups
        ------------

        for group_key == a1:

        A    B
        a1   b1

        for group_key == a2:

        A    B
        a1   b1

        Output Groups
        -------------

        for group_key == a1:

        first_appearance_position       other result columns...
        1                               other result values....

        for group_key == a2:

        first_appearance_position       other result columns...
        0                               other result values....
        0                               other result values....

    Args
    ----
    func: The function we need to apply to each group
    args: Function's positional arguments
    kwargs: Function's keyword arguments
    data_column_index: Column labels for the input dataframe
    index_column_names: Names of the input dataframe's index
    input_data_column_types: Types of the input dataframe's data columns
    input_index_column_types: Types of the input dataframe's index columns
    session: the current session
    series_groupby: Whether we are performing a SeriesGroupBy.apply() instead of DataFrameGroupBy.apply()
    by_types: The snowflake types of the by columns.
    existing_identifiers: List of existing column identifiers; these are omitted when creating new column identifiers.

    Returns
    -------
    A UDTF that will apply the provided function to a group and return a
    dataframe representing all the data and metadata of the result.
    """

    # Get the length of this list outside the vUDTF function because the vUDTF
    # doesn't have access to the Snowpark module, which defines these types.
    num_by = len(by_types)
    from snowflake.snowpark.modin.pandas.utils import try_convert_index_to_native

    data_column_index = try_convert_index_to_native(data_column_index)

    class ApplyFunc:
        def end_partition(self, df: native_pd.DataFrame):  # type: ignore[no-untyped-def] # pragma: no cover: adding type hint causes an error when creating udtf. also, skip coverage for this function because coverage tools can't tell that we're executing this function because we execute it in a UDTF.
            """
            Apply the user-provided function to the group represented by this partition.

            Args
            ----
            df: The dataframe representing one group

            Returns
            -------
            A dataframe representing the result of applying the user-provided
            function to this group.
            """
            current_column_position = 0

            # The first column is row position. Save it for later.
            row_position_column_number = 0
            row_positions = df.iloc[:, row_position_column_number]
            current_column_position = row_position_column_number + 1

            # The next columns are the by columns. Since we are only looking at
            # one group, every row in the by columns is the same, so get the
            # group label from the first row.
            group_label = tuple(
                df.iloc[0, current_column_position : current_column_position + num_by]
            )
            current_column_position = current_column_position + num_by
            if len(group_label) == 1:
                group_label = group_label[0]

            df = df.iloc[:, current_column_position:]
            # Snowflake names the original columns "ARG1", "ARG2", ... "ARGN".
            # the columns after the by columns are the index columns.
            df.set_index(
                [
                    f"ARG{i}"
                    for i in range(
                        1 + current_column_position,
                        1 + current_column_position + len(index_column_names),
                    )
                ],
                inplace=True,
            )
            df.index.names = index_column_names
            if series_groupby:
                # For SeriesGroupBy, there should be only one data column.
                num_columns = len(df.columns)
                assert (
                    num_columns == 1
                ), f"Internal error: SeriesGroupBy func should apply to series, but input data had {num_columns} columns."
                input_object = df.iloc[:, 0].rename(group_label)
            else:
                input_object = df.set_axis(data_column_index, axis="columns")
            # Use infer_objects() because integer columns come as floats
            # TODO: file snowpark bug about that. Asked about this here:
            # https://github.com/snowflakedb/snowpandas/pull/823/files#r1507286892
            input_object = input_object.infer_objects()
            func_result = func(input_object, *args, **kwargs)
            if isinstance(func_result, native_pd.Series):
                if series_groupby:
                    func_result_as_frame = func_result.to_frame()
                    func_result_as_frame.columns = [MODIN_UNNAMED_SERIES_LABEL]
                else:
                    # If function returns series, we have to transpose the series
                    # and change its metadata a little bit, but after that we can
                    # continue largely as if the function has returned a dataframe.
                    #
                    # If the series has a 1-dimensional index, the series name
                    # becomes the name of the column index. For example, if
                    # `func` returned the series native_pd.Series([1], name='a'):
                    #
                    # 0    1
                    # Name: a, dtype: int64
                    #
                    # The result needs to use the dataframe
                    # pd.DataFrame([1], columns=pd.Index([0], name='a'):
                    #
                    # a  0
                    # 0  1
                    #
                    name = func_result.name
                    func_result.name = None
                    func_result_as_frame = func_result.to_frame().T
                    if func_result_as_frame.columns.nlevels == 1:
                        func_result_as_frame.columns.name = name
                return convert_groupby_apply_dataframe_result_to_standard_schema(
                    input_object,
                    func_result_as_frame,
                    row_positions,
                    # For DataFrameGroupBy, we don't need to include any
                    # information about the index of `func_result_as_frame`.
                    # The series only has one index, and that index becomes the
                    # columns of `func_result_as_frame`. For SeriesGroupBy, we
                    # do include the result's index in the result.
                    include_index_columns=series_groupby,
                )
            if isinstance(func_result, native_pd.DataFrame):
                return convert_groupby_apply_dataframe_result_to_standard_schema(
                    input_object, func_result, row_positions, include_index_columns=True
                )
            # At this point, we know the function result was not a DataFrame
            # or Series
            return native_pd.DataFrame(
                {
                    "label": [
                        json.dumps({"0": MODIN_UNNAMED_SERIES_LABEL, "data_pos": 0})
                    ],
                    "row_position_within_group": [0],
                    "value": [convert_numpy_int_result_to_int(func_result)],
                    "original_row_number": [-1],
                    "first_position_for_group": [row_positions.iloc[0]],
                },
                # use object dtype so result is JSON-serializable
                dtype=object,
            )

    input_types = [
        # first input column is the integer row number. the row number integer
        # becomes a float inside the UDTF due to SNOW-1184587
        LongType(),
        # the next columns are the by columns...
        *by_types,
        # then the index columns for the input dataframe or series...
        *input_index_column_types,
        # ...then the data columns for the input dataframe or series.
        *input_data_column_types,
    ]

    col_labels = [
        "LABEL",
        "ROW_POSITION_WITHIN_GROUP",
        "VALUE",
        "ORIGINAL_ROW_POSITION",
        "APPLY_FIRST_GROUP_KEY_OCCURRENCE_POSITION",
    ]
    # Generate new column identifiers for all required UDTF columns with the helper below to prevent collisions in
    # column identifiers.
    col_names = generate_snowflake_quoted_identifiers_helper(
        pandas_labels=col_labels,
        excluded=existing_identifiers,
        wrap_double_underscore=False,
    )
    return udtf(
        ApplyFunc,
        output_schema=PandasDataFrameType(
            [StringType(), IntegerType(), VariantType(), IntegerType(), IntegerType()],
            col_names,
        ),
        input_types=[PandasDataFrameType(col_types=input_types)],
        # We have to specify the local pandas package so that the UDF's pandas
        # behavior is consistent with client-side pandas behavior.
        packages=[native_pd] + list(session.get_packages().values()),
        session=session,
    )


def create_udf_for_series_apply(
    func: Union[Callable, UserDefinedFunction],
    return_type: DataType,
    input_type: DataType,
    na_action: Optional[Literal["ignore"]],
    session: Session,
    args: tuple[Any, ...],
    **kwargs: Any,
) -> UserDefinedFunction:
    """
    Creates Snowpark user defined function to use like a columnar expression from given func or existing Snowpark user defined function.

    Args:
        func: a Python function or Snowpark user defined function.
        return_type: return type of the function as Snowpark type.
        input_type: input type of the function as Snowpark type.
        na_action: if "ignore", use strict mode.
        session: Snowpark session, should be identical with pd.session
        args: positional arguments to pass to the UDF
        **kwargs: keyword arguments to pass to the UDF

    Returns:
        Snowpark user defined function.
    """

    # Start with session packages.
    packages = list(session.get_packages().values())

    # Snowpark function with annotations, extract underlying func to wrap.
    if isinstance(func, UserDefinedFunction):
        # Ensure return_type specified is identical.
        assert func._return_type == return_type

        # Append packages from function.
        if func._packages:
            packages += func._packages

        # Below the function func is wrapped again, extract here the underlying Python function.
        func = func.func

    if isinstance(return_type, VariantType):

        def apply_func(x):  # type: ignore[no-untyped-def] # pragma: no cover
            result = []
            # When the return type is Variant, the return value must be json-serializable
            # Calling tolist() convert np.int*, np.bool*, etc. (which is not
            # json-serializable) to python native values
            for e in x.apply(func, args=args, **kwargs).tolist():
                result.append(
                    handle_missing_value_in_variant(convert_numpy_int_result_to_int(e))
                )
            return result

    else:

        def apply_func(x):  # type: ignore[no-untyped-def] # pragma: no cover
            return x.apply(func, args=args, **kwargs)

    func_udf = udf(
        apply_func,
        return_type=PandasSeriesType(return_type),
        input_types=[PandasSeriesType(input_type)],
        strict=bool(na_action == "ignore"),
        session=session,
        packages=packages,
    )
    return func_udf


def handle_missing_value_in_variant(value: Any) -> Any:
    """
    Returns the correct NULL value in a variant column when a UDF is applied.

    Snowflake supports two types of NULL values, JSON NULL and SQL NULL in variant data.
    In Snowflake Python UDF, a VARIANT JSON NULL is translated to Python None and A SQL NULL is
    translated to a Python object, which has the `is_sql_null` attribute.
    See details in
    https://docs.snowflake.com/en/user-guide/semistructured-considerations#null-values
    https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-designing#null-values

    In Snowpark pandas apply/applymap API with a variant column, we return JSON NULL if a Python
    None is returned in UDF (follow the same as Python UDF), and return SQL null for all other
    pandas missing values (np.nan, pd.NA, pd.NaT). Note that pd.NA, pd.NaT are not
    json-serializable, so we need to return a json-serializable value anyway (None or SqlNullWrapper())
    """

    class SqlNullWrapper:
        def __init__(self) -> None:
            self.is_sql_null = True

    if is_scalar(value) and native_pd.isna(value):
        if value is None:
            return None
        else:
            return SqlNullWrapper()
    else:
        return value


def convert_numpy_int_result_to_int(value: Any) -> Any:
    """
    If the result is a numpy int, convert it to a python int.

    Use this function to make UDF results JSON-serializable. numpy ints are not
    JSON-serializable, but python ints are. Note that this function cannot make
    all results JSON-serializable, e.g. it will not convert make
    [1, np.int64(3)]  or [[np.int64(3)]] serializable by converting the numpy
    ints to python ints. However, it's very common for functions to return
    numpy integers or dataframes or series thereof, so if we apply this function
    to the result (in case the function returns an integer) or each element of
    the result (in case the function returns a dataframe or series), we can
    make sure that we return a JSON-serializable column to snowflake.

    Args
    ----
    value: The value to fix

    Returns
    -------
    int(value) if the value is a numpy int, otherwise the value.
    """
    return int(value) if np.issubdtype(type(value), np.integer) else value


def deduce_return_type_from_function(
    func: Union[AggFuncType, UserDefinedFunction]
) -> Optional[DataType]:
    """
    Deduce return type if possible from a function, list, dict or type object. List will be mapped to ArrayType(),
    dict to MapType(), and if a type object (e.g., str) is given a mapping will be consulted.
    Args:
        func: callable function, object or Snowpark UserDefinedFunction that can be passed in pandas to reference a function.

    Returns:
        Snowpark Datatype or None if no return type could be deduced.
    """

    # Does function have an @udf decorator? Then return type from it directly.
    if isinstance(func, UserDefinedFunction):
        return func._return_type

    # get the return type of type hints
    # PYTHON_TO_SNOW_TYPE_MAPPINGS contains some Python builtin functions that
    # can only return the certain type (e.g., `str` will return string)
    # if we can't get the type hints from the function,
    # use variant as the default, which can hold any type of value
    if isinstance(func, list):
        return ArrayType()
    elif isinstance(func, dict):
        return MapType()
    elif func in PYTHON_TO_SNOW_TYPE_MAPPINGS:
        return PYTHON_TO_SNOW_TYPE_MAPPINGS[func]()
    else:
        # handle special case 'object' type, in this case use Variant Type.
        # Catch potential TypeError exception here from python_type_to_snow_type.
        # If it is not the object type, return None to indicate that type hint could not be extracted successfully.
        try:
            return get_types_from_type_hints(func, TempObjectType.FUNCTION)[0]
        except TypeError as te:
            if str(te) == "invalid type <class 'object'>":
                return VariantType()
            return None


def sort_apply_udtf_result_columns_by_pandas_positions(
    positions: list[int],
    pandas_labels: list[Hashable],
    snowflake_quoted_identifiers: list[str],
) -> tuple[list[Hashable], list[str]]:
    """
    Sort the columns resulting from a UDTF according the position they should take in the resulting pandas dataframe.

    Args
    ----
    positions: Positions the columns should take in the resulting pandas dataframe.
    pandas_labels: The pandas labels of the columns
    snowflake_quoted_identifiers: The snowflake quoted identifiers of the columns.

    Returns:
    -------
    tuple where first element has the sorted pandas labels, and second has the sorted quoted identifiers.
    """
    # We group the column information together as a tuple (position, pandas
    # label, snowflake identifier) to make it easier for sorting as needed.
    ColumnInfo = namedtuple(
        "ColumnInfo",
        ["position", "pandas_label", "snowflake_quoted_identifier"],
    )

    column_info = [
        ColumnInfo(position, pandas_label, snowflake_quoted_identifier)
        for position, pandas_label, snowflake_quoted_identifier in zip(
            positions,
            pandas_labels,
            snowflake_quoted_identifiers,
        )
    ]

    # Sort based on the column position information.
    column_info.sort(key=lambda x: x.position)

    pandas_labels = [info.pandas_label for info in column_info]
    snowflake_quoted_identifiers = [
        info.snowflake_quoted_identifier for info in column_info
    ]
    return pandas_labels, snowflake_quoted_identifiers


def get_metadata_from_groupby_apply_pivot_result_column_names(
    func_result_snowflake_quoted_identifiers: list[str],
) -> tuple[list[Hashable], list[Hashable], list[str], list[Hashable], list[str]]:
    """
    Extract the pandas and snowflake metadata from the column names of the pivot result for groupby.apply.

    Args:
        func_result_snowflake_quoted_identifiers:
            The identifiers of the columns that represent the function result.

    Returns:
        A tuple containing the following, in the order below:
            1. A list containing the names of the column index for the resulting dataframe
            2. A list containing the pandas labels of the data columns in the function result
            3. A list containing the snowflake quoted identifiers of the data columns in the function result.
            4. A list containing the pandas labels of the index columns in the function result
            5. A list containing the snowflake quoted identifiers of the index columns in the function result

    Examples
    --------
    # not doing a doctest because it seems to choke on some of the input characters
    # due to the escaping.

    input:

    get_metadata_from_groupby_apply_pivot_result_column_names([
                 # this representa a data column named ('a', 'group_key') at position 0
                 '"\'{""0"": ""a"", ""1"": ""group_key"", ""data_pos"": 0, ""names"": [""c1"", ""c2""]}\'"',
                 # this represents a data column named  ('b', 'int_col') at position 1
                '"\'{""0"": ""b"", ""1"": ""int_col"", ""data_pos"": 1, ""names"": [""c1"", ""c2""]}\'"',
                 # this repesents a data column named ('b', 'string_col') at position 2
                 '"\'{""0"": ""b"", ""1"": ""string_col"", ""data_pos"": 2, ""names"": [""c1"", ""c2""]}\'"',
                 # this represents an index column for an index level named "i1"
                 '"\'{""index_pos"": 0, ""name"": ""i1""}\'"',
                # this represents an index column for an index level named "i2"
                 '"\'{""index_pos"": 1, ""name"": ""i2""}\'"'
        ])

    output:

    (
        # these are the column index's names
        ['c1', 'c2'],
        # these are data column labels
        [('a', 'group_key'), ('b', 'int_col'), ('b', 'string_col')],
        # these are the snowflake quoted identifiers of the data columns
        ['"\'{""0"": ""a"", ""1"": ""group_key"", ""data_pos"": 0, ""names"": [""c1"", ""c2""]}\'"',
        '"\'{""0"": ""b"", ""1"": ""int_col"", ""data_pos"": 1, ""names"": [""c1"", ""c2""]}\'"',
        '"\'{""0"": ""b"", ""1"": ""string_col"", ""data_pos"": 2, ""names"": [""c1"", ""c2""]}\'"'
        ],
        # these are the names of the index levels
        ['i1', 'i2'],
        # these are the snowflake quoted identifiers of the index columns
        ['"\'{""index_pos"": 0, ""name"": ""i1""}\'"', '"\'{""index_pos"": 1, ""name"": ""i2""}\'"']
    )

    """
    index_column_snowflake_quoted_identifiers = []
    data_column_snowflake_quoted_identifiers = []
    data_column_kv_maps = []
    index_column_kv_maps = []
    index_column_pandas_labels = []
    data_column_pandas_labels = []
    column_index_names = None
    for identifier in func_result_snowflake_quoted_identifiers:
        object_map = parse_snowflake_object_construct_identifier_to_map(identifier)
        if "index_pos" in object_map:
            index_column_snowflake_quoted_identifiers.append(identifier)
            index_column_pandas_labels.append(object_map["name"])
            index_column_kv_maps.append(object_map)
        else:
            if column_index_names is None:
                # if the object map has no 'names', it represents an
                # aggregation, i.e. `func` returned a scalar instead of a
                # dataframe or series. The result's columns always have a
                # single level named `None`.
                column_index_names = object_map.get("names", [None])
            (
                data_column_pandas_label,
                data_column_kv_map,
            ) = parse_object_construct_snowflake_quoted_identifier_and_extract_pandas_label(
                identifier, num_levels=len(column_index_names)
            )
            data_column_pandas_labels.append(data_column_pandas_label)
            data_column_kv_maps.append(data_column_kv_map)
            data_column_snowflake_quoted_identifiers.append(identifier)
    assert (
        column_index_names is not None
    ), "Pivot result should include at least one data column"

    data_column_positions = [kv["data_pos"] for kv in data_column_kv_maps]
    index_column_positions = [kv["index_pos"] for kv in index_column_kv_maps]

    # ignore these cases because we have to merge the different column
    # indices
    # TODO(SNOW-1232208): Handle this case. Note that the pandas behavior for
    # this case when func returns a series is contested
    # https://github.com/pandas-dev/pandas/issues/54992
    if len(set(data_column_positions)) != len(data_column_positions):
        # We can end up here if the column indices differ either in their names
        # or in their values. For example:
        # 1) one group returns a dataframe whose columns are pd.Index(['col_0'], name="group_1_columns"),
        #    and another group returns a dataframe whose columns are pd.Index(['col_0'], name="group_2_columns").
        #
        #    In this case, the snowflake labels for each result's 0th column are like
        #      {0: "col_0", "data_pos": 0, "names": ["group_1_columns"]},
        #      {0: "col_0", "data_pos", 0, "names": ["group_2_columns"]}
        #
        # 2) one group returns a dataframe whose columns are pd.Index(['col_0'], name="columns"),
        #    and another group returns a dataframe whose columns are pd.Index(['col_1']), name="columns").
        #
        #    In this case, the snowflake labels for each result's 0th column are like
        #      {0: "col_0", "data_pos": 0, "names": ["columns"]},
        #      {0: "col_1", "data_pos", 0, "names": ["columns"]}
        raise NotImplementedError(
            "No support for applying a function that returns two dataframes that have different labels for the column at a given position, "
            + "a function that returns two dataframes that have different column index names, "
            + "or a function that returns two series with different names or conflicting labels for the row at a given position."
        )
    if len(set(index_column_positions)) != len(index_column_positions):
        raise NotImplementedError(
            "No support for applying a function that returns two dataframes that have different names for a given index level"
        )

    (
        data_column_pandas_labels,
        data_column_snowflake_quoted_identifiers,
    ) = sort_apply_udtf_result_columns_by_pandas_positions(
        data_column_positions,
        data_column_pandas_labels,
        data_column_snowflake_quoted_identifiers,
    )
    (
        index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers,
    ) = sort_apply_udtf_result_columns_by_pandas_positions(
        index_column_positions,
        index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers,
    )

    return (
        column_index_names,
        data_column_pandas_labels,
        data_column_snowflake_quoted_identifiers,
        index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers,
    )


def groupby_apply_pivot_result_to_final_ordered_dataframe(
    ordered_dataframe: OrderedDataFrame,
    agg_func: Callable,
    by_snowflake_quoted_identifiers_list: list[str],
    sort_method: GroupbyApplySortMethod,
    as_index: bool,
    original_row_position_snowflake_quoted_identifier: str,
    group_key_appearance_order_quoted_identifier: str,
    row_position_within_group_snowflake_quoted_identifier: str,
    data_column_snowflake_quoted_identifiers: list[str],
    index_column_snowflake_quoted_identifiers: list[str],
    renamed_data_column_snowflake_quoted_identifiers: list[str],
    renamed_index_column_snowflake_quoted_identifiers: list[str],
    new_index_identifier: str,
    func_returned_dataframe: bool,
) -> OrderedDataFrame:
    """
    Convert the intermediate groupby.apply result to the final OrderedDataFrame.

    Sort in the correct order and rename index and data columns as needed. Add
    an index column if as_index=False.

    Args:
        ordered_dataframe:
            The intermediate result.
        agg_func:
            The original function passed to groupby.apply
        by_snowflake_quoted_identifiers_list:
            identifiers for columns we're grouping by
        sort_method:
            How to sort the result
        as_index:
            If true, add group keys as levels in the index. Otherwise, generate a
            new index that is equivalent to the new row positions.
        original_row_position_snowflake_quoted_identifier:
            The label of the original row that each result row originates from.
        group_key_appearance_order_quoted_identifier:
            The identifier for the column that tells the position of the row
            where this group key first occurred in the input dataframe.
        row_position_within_group_snowflake_quoted_identifier:
            The label of the row position within each group result.
        data_column_snowflake_quoted_identifiers:
            The identifiers of the data columns of the function results.
        index_column_snowflake_quoted_identifiers:
            The identifiers of the index columns of the function results.
        renamed_data_column_snowflake_quoted_identifiers:
            What to rename the data columns to
        renamed_index_column_snowflake_quoted_identifiers:
            What to rename the index columns to
        new_index_identifier:
            The identifier for the new index level that we add if as_index=False.
        func_returned_dataframe:
            Whether `agg_func` returned a pandas DataFrame
    Returns:
        Ordered dataframe in correct order with all the final snowflake identifiers.

    """
    return_variant, return_type = check_return_variant_and_get_return_type(agg_func)
    return ordered_dataframe.sort(
        *(
            OrderingColumn(x)
            for x in (
                *(
                    by_snowflake_quoted_identifiers_list
                    if sort_method is GroupbyApplySortMethod.GROUP_KEY_COMPARISON_ORDER
                    else [
                        group_key_appearance_order_quoted_identifier,
                    ]
                    if sort_method is GroupbyApplySortMethod.GROUP_KEY_APPEARANCE_ORDER
                    else [original_row_position_snowflake_quoted_identifier]
                ),
                row_position_within_group_snowflake_quoted_identifier,
            )
        )
    ).select(
        *(
            # For `func` returning a dataframe:
            #   if as_index=True:
            #       the group keys, i.e. the by columns, become the first
            #       levels of the result index
            #   If as_index=False:
            #       We drop the group keys.
            # Otherwise:
            #   We always include the group keys.
            by_snowflake_quoted_identifiers_list
            if (not func_returned_dataframe or as_index)
            else []
        ),
        *(
            # Whether `func` returns a dataframe or not, when as_index=False, we
            # we need to add a new index level that shows where the groups came
            # from.
            #   if sorting by original row order:
            #       the original row position  itself is the new index level.
            #   Otherwise:
            #       sort the groups (either in GROUP_KEY_COMPARISON_ORDER or
            #       in GROUP_KEY_APPEARANCE_ORDER) and assign the
            #       label i to all rows that came from func(group_i).
            [
                original_row_position_snowflake_quoted_identifier
                if sort_method is GroupbyApplySortMethod.ORIGINAL_ROW_ORDER
                else (
                    dense_rank().over(
                        Window.order_by(
                            *(
                                SnowparkColumn(col).asc_nulls_last()
                                for col in (
                                    by_snowflake_quoted_identifiers_list
                                    if sort_method
                                    is GroupbyApplySortMethod.GROUP_KEY_COMPARISON_ORDER
                                    else [group_key_appearance_order_quoted_identifier]
                                )
                            )
                        )
                    )
                    - 1
                ).as_(new_index_identifier)
            ]
            if not as_index
            else []
        ),
        *[
            (
                col(old_quoted_identifier).as_(quoted_identifier)
                if return_variant
                else col(old_quoted_identifier).cast(return_type).as_(quoted_identifier)
            )
            for old_quoted_identifier, quoted_identifier in zip(
                data_column_snowflake_quoted_identifiers
                + index_column_snowflake_quoted_identifiers,
                renamed_data_column_snowflake_quoted_identifiers
                + renamed_index_column_snowflake_quoted_identifiers,
            )
        ],
    )


def groupby_apply_create_internal_frame_from_final_ordered_dataframe(
    ordered_dataframe: OrderedDataFrame,
    func_returned_dataframe: bool,
    as_index: bool,
    group_keys: bool,
    by_pandas_labels: list[Hashable],
    by_snowflake_quoted_identifiers: list[str],
    func_result_data_column_pandas_labels: list[Hashable],
    func_result_data_column_snowflake_quoted_identifiers: list[str],
    func_result_index_column_pandas_labels: list[Hashable],
    func_result_index_column_snowflake_quoted_identifiers: list[str],
    column_index_names: list[str],
    new_index_identifier: str,
    original_data_column_pandas_labels: list[Hashable],
) -> InternalFrame:
    """
    Create the InternalFrame for the groupby.apply result from the final OrderedDataFrame.

    Designate the appropriate snowflake columns as data columns and index
    columns.

    Args:
        ordered_dataframe:
            The final, sorted OrderedDataFrame with the result of groupby.apply
        func_returned_dataframe:
            Whether the function returned a pandas DataFrame.
        as_index:
            Whether to include groups in the index.
        group_keys:
            The group_keys argument to groupby()
        by_pandas_labels:
            The labels of the grouping columns.
        by_snowflake_quoted_identifiers:
            The snowflake identifiers of the grouping columns.
        func_result_data_column_pandas_labels:
            The pandas labels for the columns resulting from calling func() on
            each group. Note that these are assumed to be the same across groups.
        func_result_data_column_snowflake_quoted_identifiers:
            Snowflake identifiers for the columns resulting from calling func()
            on each group. Note that these are assumed to be the same across groups.
        func_result_index_column_pandas_labels:
            The pandas labels for the index levels resulting from calling func() on
            each group. Note that these are assumed to be the same across groups.
        func_result_index_column_snowflake_quoted_identifiers:
            Snowflake identifiers for the index levels resulting from calling func()
            on each group. Note that these are assumed to be the same across groups.
        column_index_names:
            The names of the result's column index.
        new_index_identifier:
            If as_index=False, use this identifier for a new index level that
            indicates which group each chunk of the result came from.
        original_data_column_pandas_labels:
            The data column pandas labels of the original dataframe.

    Returns:
        An InternalFrame representing the final result.
    """
    if not as_index and not func_returned_dataframe:
        # If func has not returned a dataframe and as_index=False, we put some
        # of the by columns in the result instead of in the index.
        # note we only include columns from the original frame, and we don't
        # include any index levels that we grouped by:
        # https://github.com/pandas-dev/pandas/blob/654c6dd5199cb2d6d522dde4c4efa7836f971811/pandas/core/groupby/groupby.py#L1308-L1311
        data_column_pandas_labels = []
        data_column_snowflake_quoted_identifiers = []
        for label, identifier in zip(by_pandas_labels, by_snowflake_quoted_identifiers):
            if label in original_data_column_pandas_labels:
                data_column_pandas_labels.append(label)
                data_column_snowflake_quoted_identifiers.append(identifier)
        # If func returned a scalar (i.e. not a dataframe or series), we need to
        # call the column with the function result None instead of
        # MODIN_UNNAMED_SERIES_LABEL.
        if func_result_data_column_pandas_labels == [MODIN_UNNAMED_SERIES_LABEL]:
            data_column_pandas_labels.append(None)
        else:
            data_column_pandas_labels.extend(func_result_data_column_pandas_labels)
        data_column_snowflake_quoted_identifiers.extend(
            func_result_data_column_snowflake_quoted_identifiers
        )
    else:
        # Otherwise, the final result's data columns are exactly the columns
        # that `func` returned.
        data_column_pandas_labels = func_result_data_column_pandas_labels
        data_column_snowflake_quoted_identifiers = (
            func_result_data_column_snowflake_quoted_identifiers
        )

    if (not func_returned_dataframe) or group_keys:
        # in these cases, we have to prepend index level(s) that indicate which
        # group each chunk came from. If as_index=True, these levels are the
        # grouping columns themselves. Otherwise, use the new column containing
        # the sequential group numbers.
        if as_index:
            group_pandas_labels = by_pandas_labels
            group_quoted_identifiers = by_snowflake_quoted_identifiers
        else:
            group_pandas_labels = [None]
            group_quoted_identifiers = [new_index_identifier]
    else:
        group_pandas_labels = []
        group_quoted_identifiers = []

    return InternalFrame.create(
        ordered_dataframe=ordered_dataframe,
        data_column_pandas_labels=data_column_pandas_labels,
        data_column_pandas_index_names=column_index_names,
        data_column_snowflake_quoted_identifiers=data_column_snowflake_quoted_identifiers,
        index_column_pandas_labels=group_pandas_labels
        + func_result_index_column_pandas_labels,
        index_column_snowflake_quoted_identifiers=group_quoted_identifiers
        + func_result_index_column_snowflake_quoted_identifiers,
    )


def groupby_apply_sort_method(
    sort: bool,
    group_keys: bool,
    original_row_position_quoted_identifier: str,
    ordered_dataframe_before_sort: OrderedDataFrame,
    func_returned_dataframe: bool,
) -> GroupbyApplySortMethod:
    """
    Get the sort method that groupby.apply should use on the result rows.

    This function implements the following pandas logic from [1], where
    "transform" [2] is a function that returns a result whose index is the
    same as the index of the dataframe being grouped.

    if func did not return a dataframe, group_keys=True, or this is not a transform:
        if sort:
            sort in order of increasing group key values
        else:
            sort in order of first appearance of group key values
    else:
        reindex result to the original dataframe's order.

    [1] https://github.com/pandas-dev/pandas/blob/e14a9bd41d8cd8ac52c5c958b735623fe0eae064/pandas/core/groupby/groupby.py#L1196
    [2] https://pandas.pydata.org/docs/user_guide/groupby.html#transformation

    Args:
        sort:
            The `sort` argument to groupby()
        group_keys:
            The `group_keys` argument to groupby()
        is_transform_quoted_identifier:
            The snowflake identifier of the column in the ordered dataframe
            that tells whether each row comes from a function that acted
            like a transform.
        ordered_dataframe_before_sort:
            Ordered dataframe containing the intermediate, unsorted
            groupby.apply result.
        func_returned_dataframe:
            Whether the user's `func` returned a dataframe.

    Returns:
        enum telling how to sort.

    """
    if not func_returned_dataframe or group_keys:
        return (
            GroupbyApplySortMethod.GROUP_KEY_COMPARISON_ORDER
            if sort
            else GroupbyApplySortMethod.GROUP_KEY_APPEARANCE_ORDER
        )
    # to distinguish between transforms and non-transforms, we need to
    # execute an extra query to compare the index of the result to the
    # index of the original dataframe.
    # https://github.com/pandas-dev/pandas/issues/57656#issuecomment-1969454704
    # Need to wrap column name in IDENTIFIER, or else bool agg function
    # will treat the name as a string literal
    is_transform: bool = not ordered_dataframe_before_sort.agg(
        builtin("boolor_agg")(
            SnowparkColumn(original_row_position_quoted_identifier) == -1
        ).as_("is_transform")
    ).collect()[0][0]
    return (
        GroupbyApplySortMethod.ORIGINAL_ROW_ORDER
        if is_transform
        else (
            GroupbyApplySortMethod.GROUP_KEY_COMPARISON_ORDER
            if sort
            else GroupbyApplySortMethod.GROUP_KEY_APPEARANCE_ORDER
        )
    )
