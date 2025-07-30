#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import inspect
import json
import logging
import sys
from collections import namedtuple
from collections.abc import Hashable
from enum import Enum, auto
from typing import Any, Callable, Literal
from types import ModuleType
from datetime import datetime
import dataclasses
from dataclasses import dataclass

import cloudpickle
import numpy as np
import pandas as native_pd
from pandas._typing import AggFuncType
from pandas.api.types import is_scalar

from snowflake.snowpark import functions
from snowflake.snowpark._internal.type_utils import PYTHON_TO_SNOW_TYPE_MAPPINGS
from collections.abc import Mapping
from snowflake.snowpark._internal.udf_utils import (
    get_types_from_type_hints,
    pickle_function,
)
import functools
from snowflake.snowpark.column import Column as SnowparkColumn
from snowflake.snowpark.modin.plugin._internal.snowpark_pandas_types import (
    TimedeltaType,
)
from snowflake.snowpark.modin.plugin._internal.type_utils import (
    infer_object_type,
    pandas_lit,
    is_compatible_snowpark_types,
)
from snowflake.snowpark import functions as sp_func
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin._internal.ordered_dataframe import (
    OrderedDataFrame,
    OrderingColumn,
)
from snowflake.snowpark.modin.plugin._internal.utils import (
    INDEX_LABEL,
    TempObjectType,
    append_columns,
    generate_snowflake_quoted_identifiers_helper,
    generate_column_identifier_random,
    get_default_snowpark_pandas_statement_params,
    parse_object_construct_snowflake_quoted_identifier_and_extract_pandas_label,
    parse_snowflake_object_construct_identifier_to_map,
)
from snowflake.snowpark.modin.plugin.utils.error_message import ErrorMessage
import itertools
from collections import defaultdict
from snowflake.snowpark.modin.utils import MODIN_UNNAMED_SERIES_LABEL
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    _IntegralType,
    _FractionalType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    PandasDataFrameType,
    PandasSeriesType,
    StringType,
    TimestampType,
    VariantType,
)
from snowflake.snowpark.udf import UserDefinedFunction
from snowflake.snowpark.udtf import UserDefinedTableFunction
from snowflake.snowpark.window import Window

_logger = logging.getLogger(__name__)

APPLY_LABEL_COLUMN_QUOTED_IDENTIFIER = '"LABEL"'
APPLY_VALUE_COLUMN_QUOTED_IDENTIFIER = '"VALUE"'
APPLY_WITH_SNOWPARK_OBJECT_ERROR_MSG = (
    "Snowpark pandas only allows native pandas and not Snowpark objects in `apply()`. "
    + "Instead, try calling `to_pandas()` on any DataFrame or Series objects passed to `apply()`. See Limitations"
    + "(https://docs.snowflake.com/developer-guide/snowpark/python/pandas-on-snowflake#limitations) section of"
    + "the Snowpark pandas documentation for more details."
)

# Default partition size to use when applying a UDTF. A higher value results in less parallelism, less contention and higher batching.
DEFAULT_UDTF_PARTITION_SIZE = 1000

# Use the workaround described below to use functions that are attributes of
# this module in UDFs and UDTFs. Without this workaround, we can't pickle
# those functions.
# https://github.com/cloudpipe/cloudpickle?tab=readme-ov-file#overriding-pickles-serialization-mechanism-for-importable-constructs
cloudpickle.register_pickle_by_value(sys.modules[__name__])

SUPPORTED_SNOWPARK_PYTHON_FUNCTIONS_IN_APPLY = {
    sp_func.exp,
    sp_func.ln,
    sp_func.log,
    sp_func._log2,
    sp_func._log10,
    sp_func.sin,
    sp_func.cos,
    sp_func.tan,
    sp_func.sinh,
    sp_func.cosh,
    sp_func.tanh,
    sp_func.ceil,
    sp_func.floor,
    sp_func.trunc,
    sp_func.sqrt,
}

try:
    import snowflake.cortex

    SUPPORTED_SNOWFLAKE_CORTEX_FUNCTIONS_IN_APPLY = {
        snowflake.cortex.Summarize,
        snowflake.cortex.Sentiment,
        snowflake.cortex.ClassifyText,
        snowflake.cortex.Translate,
        snowflake.cortex.ExtractAnswer,
    }

    ALL_SNOWFLAKE_CORTEX_FUNCTIONS = tuple(
        i[1] for i in inspect.getmembers(snowflake.cortex)
    )
except ImportError:
    SUPPORTED_SNOWFLAKE_CORTEX_FUNCTIONS_IN_APPLY = set()
    ALL_SNOWFLAKE_CORTEX_FUNCTIONS = tuple()


PERSIST_KWARG_NAME = "snowflake_udf_params"


@dataclass(eq=True, frozen=True)
class UDxFPersistParams:
    """
    Parameters describing persistence properties of a UDxF.

    If its name is unspecified, then a name will be automatically generated.

    If `replace=False`, the original UDxF is not checked to ensure its typing matches the
    requirements of this function call.
    """

    udf_name: str
    stage_name: str
    immutable: bool
    # Ignore replace and if_not_exists for caching purposes since we cache on the contents
    # of the pickled bytes of the function.
    replace: bool | None = dataclasses.field(default=None, hash=None, compare=False)
    if_not_exists: bool | None = dataclasses.field(
        default=None, hash=None, compare=False
    )


# Maps lambda functions to an auto-generated name. If a lambda with a given pickle blob was already seen,
# we should re-use its name.
generated_lambda_names: dict[bytes, str] = {}


def process_persist_params(
    f: Callable, blob: bytes, persist_info: dict | None
) -> UDxFPersistParams | None:
    """
    Process parameters from the "snowflake_udf_params" argument of an `apply` call.

    `blob` is the pickled contents of `f`'s wrapper function/class. It is used to reuse auto-generated
    names if the function is a lambda.

    These parameters describe information about a UDF persistence. If absent, the UDxF is assumed
    to be transient, and this function should return None.
    """
    if persist_info is None:
        return None
    STAGE_KEY = "stage_location"
    NAME_KEY = "name"
    REPLACE_KEY = "replace"
    IF_NOT_EXISTS_KEY = "if_not_exists"
    IMMUTABLE_KEY = "immutable"
    # If is_permanent is unspecified, assume it is transient.
    if STAGE_KEY not in persist_info:
        raise ValueError(
            f"`{STAGE_KEY}` must be set when calling apply/map/transform when creating a permanent UDF with `{PERSIST_KWARG_NAME}`."
        )
    supported_args = {
        STAGE_KEY,
        NAME_KEY,
        REPLACE_KEY,
        IF_NOT_EXISTS_KEY,
        IMMUTABLE_KEY,
    }
    unsupported_args = [f"`{key}`" for key in persist_info if key not in supported_args]
    if len(unsupported_args) > 0:
        if len(unsupported_args) > 1:
            unsupported_args[-1] = "and " + unsupported_args[-1]
            be = "are"
        else:
            be = "is"
        joiner = ", " if len(unsupported_args) > 2 else " "
        raise ValueError(
            f"{joiner.join(unsupported_args)} {be} unsupported in `{PERSIST_KWARG_NAME}` when creating a permanent UDF in Snowpark pandas."
        )
    if not hasattr(f, "__name__") or f.__name__.startswith("<lambda>"):
        generated_lambda_names[blob] = generated_lambda_names.get(
            blob, "lambda_" + generate_column_identifier_random()
        )
    name = persist_info.get(NAME_KEY, generated_lambda_names.get(blob, f.__name__))
    return UDxFPersistParams(
        name,
        persist_info[STAGE_KEY],
        persist_info.get(IMMUTABLE_KEY, False),
        persist_info.get(REPLACE_KEY),
        persist_info.get(IF_NOT_EXISTS_KEY),
    )


# Reuse UDFs that have already been constructed in each session.
@dataclass(eq=True, frozen=True)
class UDFCacheKey:
    # The result of calling pickle_function(func), where func is the wrapper method that
    # is used in the UDF. This pickled object itself is not necessarily the same as what's uploaded
    # to the server, but we pickle the wrapper to ensure closure-captured variables are checked.
    func_pickle_blob: bytes
    return_type: DataType  # The inferred return type of the column
    input_type: DataType
    strict: bool
    # No packages param in the key; we assume packages will be the same for the UDF every time
    persist_params: UDxFPersistParams | None


session_udf_cache: dict[Session, dict[UDFCacheKey, UserDefinedFunction]] = defaultdict(
    dict
)


# Reuse UDTFs that have already been constructed in each session
@dataclass(eq=True, frozen=True)
class UDTFCacheKey:
    # The result of calling pickle_function(func), where func is the end_partition method of the
    # wrapper class is used in the UDTF. We cannot pickle the wrapper class directly because pickle
    # will serialize each class differently even if they have the same body and closure variables
    # because they are technically different classes.
    func_pickle_blob: bytes
    col_types: tuple[DataType, ...]
    col_names: tuple[str, ...]
    input_types: tuple[DataType, ...]
    # Because UDTFs allow user-specified packages when a UDF is passed, we need to consider package names in the key as well.
    package_names: tuple[str, ...]
    persist_params: UDxFPersistParams | None


# Each UDTF construction helper function manipulates the input function slightly differently, and thus needs a different cache.
session_apply_axis_1_udtf_cache: dict[
    Session, dict[UDTFCacheKey, UserDefinedTableFunction]
] = defaultdict(dict)
session_groupby_apply_no_pivot_udtf_cache: dict[
    Session, dict[UDTFCacheKey, UserDefinedTableFunction]
] = defaultdict(dict)
session_groupby_apply_udtf_cache: dict[
    Session, dict[UDTFCacheKey, UserDefinedTableFunction]
] = defaultdict(dict)


all_caches: list[
    dict[Session, dict[UDFCacheKey, UserDefinedFunction]]
    | dict[
        Session,
        dict[UDTFCacheKey, UserDefinedTableFunction],
    ]
] = [
    session_udf_cache,
    session_apply_axis_1_udtf_cache,
    session_groupby_apply_no_pivot_udtf_cache,
    session_groupby_apply_udtf_cache,
]


def get_cached_lambda_names(session: Session) -> list[str]:
    """
    Return the auto-generated names of every lambda in this session.
    """
    return list(generated_lambda_names.values())


def drop_named_udf_and_udtfs() -> None:
    """
    Issue a DROP FUNCTION IF EXISTS directive for every cached function in this session with a persist_params field.
    """
    for session_cache in all_caches:
        for session, function_cache in session_cache.items():
            for cache_key in function_cache.keys():
                if cache_key.persist_params:
                    types = (
                        [cache_key.input_type.simple_string()]
                        if isinstance(cache_key, UDFCacheKey)
                        else [tpe.simple_string() for tpe in cache_key.col_types]
                    )
                    session.sql(
                        f"DROP FUNCTION IF EXISTS {cache_key.persist_params.udf_name}({', '.join(types)})"
                    )


def clear_session_udf_and_udtf_caches() -> None:
    """
    Clear all cached UDF and UDTFs. If any were persisted to the server, they are unaffected by
    this function.
    """
    generated_lambda_names.clear()
    for cache in all_caches:
        cache.clear()


def cache_key_to_init_params(
    key: UDFCacheKey | UDTFCacheKey,
) -> dict[str, str | bool | None]:
    if key.persist_params is None:
        return {}
    return {
        "is_permanent": True,
        "name": key.persist_params.udf_name,
        "stage_location": key.persist_params.stage_name,
        "immutable": key.persist_params.immutable,
        "replace": key.persist_params.replace,
        "if_not_exists": key.persist_params.if_not_exists,
    }


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


class GroupbyApplyFuncType(Enum):
    """
    The type of function that is being applied to each group in a groupby apply.
    """

    AGGREGATE = auto()
    TRANSFORM = auto()
    OTHER = auto()  # If apply function is neither aggregate nor transform.


class GroupbyApplyOutputSchema:
    """
    Class to hold the inferred output schema of a groupby apply function.
    """

    def __init__(self) -> None:
        # List of pandas labels (includes index columns).
        self.column_labels: list[Hashable] = []
        # List of snowflake column types (includes index columns).
        self.column_types: list[DataType] = []
        # List of snowflake quoted identifiers (includes index columns).
        self.column_ids: list[str] = []
        # Number of index columns.
        self.num_index_columns = 0
        # Type of function passed to groupby apply.
        self.func_type = GroupbyApplyFuncType.OTHER
        # Column index names.
        self.column_index_names: list[Hashable] = []

    def add_column(self, label: Hashable, column_type: DataType) -> None:
        """
        Add a column to the schema.
        Args:
            label: The label of the column.
            column_type: The Snowflake type of the column.
        Returns:

        """
        self.column_labels.append(label)
        self.column_types.append(column_type)


def check_return_variant_and_get_return_type(func: Callable) -> tuple[bool, DataType]:
    """Check whether the function returns a variant in Snowflake, and get its return type."""
    return_type = deduce_return_type_from_function(func, None)
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
    func: Callable | UserDefinedFunction,
    raw: bool,
    result_type: Literal["expand", "reduce", "broadcast"] | None,
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
    col_types = [LongType(), StringType(), VariantType()]
    col_identifiers = [
        row_position_snowflake_quoted_identifier,
        APPLY_LABEL_COLUMN_QUOTED_IDENTIFIER,
        APPLY_VALUE_COLUMN_QUOTED_IDENTIFIER,
    ]
    try:
        persist_info = kwargs.pop(
            PERSIST_KWARG_NAME, None
        )  # Pop persistence params from kwargs before pickling.
        pickled = pickle_function(ApplyFunc.end_partition)
        cache_key = UDTFCacheKey(
            pickled,
            tuple(col_types),
            tuple(col_identifiers),
            tuple([LongType()] + input_types),
            tuple(
                pkg.__name__ if isinstance(pkg, ModuleType) else pkg for pkg in packages
            ),
            process_persist_params(func, pickled, persist_info),
        )
        cache = session_apply_axis_1_udtf_cache[session]
        if cache_key not in cache:
            cache[cache_key] = sp_func.udtf(
                ApplyFunc,
                **cache_key_to_init_params(cache_key),
                output_schema=PandasDataFrameType(
                    col_types,
                    col_identifiers,
                ),
                input_types=[PandasDataFrameType([LongType()] + input_types)],
                # We have to use the current pandas version to ensure the behavior consistency
                packages=[native_pd] + packages,
                session=session,
                statement_params=get_default_snowpark_pandas_statement_params(),
            )
        return cache[cache_key]
    except NotImplementedError:  # pragma: no cover
        # When a Snowpark object is passed to a UDF, a NotImplementedError with message
        # 'Snowpark pandas does not yet support the method DataFrame.__reduce__' is raised. Instead,
        # catch this exception and return a more user-friendly error message.
        raise ValueError(APPLY_WITH_SNOWPARK_OBJECT_ERROR_MSG)


def convert_groupby_apply_dataframe_result_to_standard_schema(
    func_output_df: native_pd.DataFrame,
    input_row_positions: native_pd.Series,
    func_type: GroupbyApplyFuncType,
) -> native_pd.DataFrame:  # pragma: no cover: this function runs inside a UDTF, so coverage tools can't detect that we are testing it.
    """
    Take the result of applying the user-provided function to a dataframe, and convert it to a dataframe with known schema that we can output from a vUDTF.

    Args:
        func_input_df: The input to `func`, where `func` is the Python function
                       that the  user originally passed to apply().
        func_output_df: The output of `func`.
        input_row_positions: The original row positions of the rows that
                             func_input_df came from.
        func_type: Type of function passed to groupby apply.

    Returns:
        A 5-column dataframe that represents the function result per the
        description in create_udtf_for_groupby_apply.

    """
    result_rows = []
    result_index_names = func_output_df.index.names
    # We don't need to include index columns if func_type is aggregate.
    include_index_columns = func_type != GroupbyApplyFuncType.AGGREGATE
    for row_number, (index_label, row) in enumerate(func_output_df.iterrows()):
        output_row_number = (
            input_row_positions.iloc[row_number]
            if func_type == GroupbyApplyFuncType.TRANSFORM
            else -1
        )
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


def apply_groupby_func_to_df(
    df: native_pd.DataFrame,
    num_by: int,
    index_column_names: list[Hashable],
    series_groupby: bool,
    data_column_index: native_pd.Index,
    func: Callable,
    args: tuple,
    kwargs: dict,
    force_list_like_to_series: bool = False,
) -> tuple[native_pd.DataFrame, native_pd.Series, GroupbyApplyFuncType, tuple]:
    """
    Restore input dataframe received in udtf to original schema.
    Args:
        df: Native pandas dataframe.
        num_by: Number of by columns.
        index_column_names: Index column names.
        series_groupby:  Whether we are performing a SeriesGroupBy.apply().
        data_column_index: Data column index.
        func: The function we need to apply to each group.
        args: Function's positional arguments.
        kwargs: Function's keyword arguments.
        force_list_like_to_series: Force the function result to series if it is list-like.

    Returns:
        A Tuple of
         1. Result of applying the function to input dataframe.
         2. rows positions
         3. Function type (aggregate, transform, or other).
         4. The group label.
    """
    # The first column is row position. Save it for later.
    col_offset = 0
    row_positions = df.iloc[:, col_offset]
    col_offset = col_offset + 1

    # The next columns are the by columns. Since we are only looking at
    # one group, every row in the by columns is the same, so get the
    # group label from the first row.
    group_label = tuple(df.iloc[0, col_offset : col_offset + num_by])
    col_offset = col_offset + num_by

    df = df.iloc[:, col_offset:]
    # The columns after the by columns are the index columns. Set index and also rename
    # them to the original index names.
    df = df.set_index(df.columns.to_list()[: len(index_column_names)])
    df.index.names = index_column_names
    if series_groupby:
        # For SeriesGroupBy, there should be only one data column.
        num_columns = len(df.columns)
        assert (
            num_columns == 1
        ), f"Internal error: SeriesGroupBy func should apply to series, but input data had {num_columns} columns."
        series_name = group_label[0] if len(group_label) == 1 else group_label
        input_object = df.iloc[:, 0].rename(series_name)
    else:
        input_object = df.set_axis(data_column_index, axis="columns")
    # Use infer_objects() because integer columns come as floats
    # TODO: file snowpark bug about that. Asked about this here:
    # https://github.com/snowflakedb/snowpandas/pull/823/files#r1507286892
    input_object = input_object.infer_objects()
    func_result = func(input_object, *args, **kwargs)
    if (
        force_list_like_to_series
        and not isinstance(func_result, native_pd.Series)
        and native_pd.api.types.is_list_like(func_result)
    ):
        if len(func_result) == 1:
            func_result = func_result[0]
        else:
            func_result = native_pd.Series(func_result)
            if len(func_result) == len(df.index):
                func_result.index = df.index
    func_type = GroupbyApplyFuncType.OTHER
    if isinstance(func_result, native_pd.Series):
        if series_groupby:
            func_result_as_frame = func_result.to_frame()
            func_result_as_frame.columns = [MODIN_UNNAMED_SERIES_LABEL]
            if input_object.index.equals(func_result_as_frame.index):
                func_type = GroupbyApplyFuncType.TRANSFORM
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
            # For DataFrameGroupBy, if func returns a series, we should treat it as an
            # aggregate.
            func_type = GroupbyApplyFuncType.AGGREGATE

    elif isinstance(func_result, native_pd.DataFrame):
        func_result_as_frame = func_result
        if input_object.index.equals(func_result_as_frame.index):
            func_type = GroupbyApplyFuncType.TRANSFORM
    else:
        # At this point, we know the function result was not a DataFrame
        # or Series
        func_result_as_frame = native_pd.DataFrame(
            {MODIN_UNNAMED_SERIES_LABEL: [func_result]}
        )
        func_type = GroupbyApplyFuncType.AGGREGATE
    return func_result_as_frame, row_positions, func_type, group_label


def create_udtf_for_groupby_no_pivot(
    func: Callable,
    args: tuple,
    kwargs: dict,
    data_column_index: native_pd.Index,
    index_column_names: list,
    input_column_types: list[DataType],
    session: Session,
    series_groupby: bool,
    by_labels: list[Hashable],
    output_schema: GroupbyApplyOutputSchema,
    force_list_like_to_series: bool = False,
) -> UserDefinedTableFunction:
    """
    Creates snowpark python UDTF for groupby.transform.

    The UDTF takes as input the following columns in the listed order:
    1. The original row position within the dataframe (not just within the group)
    2. All the by columns (these are constant across the group, but in the case
    #  of SeriesGroupBy, we need these so we can name each input series by the
    #  group label)
    3. All the index columns
    4. All the data columns

    The UDF returns as output the following columns in the listed order.
    1. row position column.
    2. All the index columns.
    3. All the data columns with transformed values (except by columns).

    Args
    ----
    func: The function we need to apply to each group
    args: Function's positional arguments
    kwargs: Function's keyword arguments
    data_column_index: Column labels for the input dataframe
    index_column_names: Names of the input dataframe's index
    input_column_types: List of types for input frame passed to UDTF.
    session: the current session
    series_groupby: Whether we are performing a SeriesGroupBy.apply() instead of DataFrameGroupBy.apply()
    by_labels: The pandas labels of the by columns.
    output_schema: Output schema for the UDTF.
    force_list_like_to_series: Force the function result to series if it is list-like

    Returns
    -------
    A UDTF that will apply the provided function to a group and return a
    dataframe representing all the data and metadata of the result.
    """

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
            (
                func_result,
                row_positions,
                func_type,
                group_label,
            ) = apply_groupby_func_to_df(
                df,
                len(by_labels),
                index_column_names,
                series_groupby,
                data_column_index,
                func,
                args,
                kwargs,
                force_list_like_to_series,
            )
            # 'min_row_position' is used to sort partitions and 'row_positions' is used
            # to sort rows within a partition.
            min_row_position = row_positions.iloc[0]
            if func_type == GroupbyApplyFuncType.AGGREGATE:
                row_positions = -1
            elif func_type == GroupbyApplyFuncType.OTHER:
                func_result.reset_index(inplace=True, drop=False)
                row_positions = list(range(0, len(func_result)))
            func_result = func_result.applymap(
                lambda x: handle_missing_value_in_variant(
                    convert_numpy_int_result_to_int(x)
                )
            ).astype("object")
            if func_type == GroupbyApplyFuncType.TRANSFORM:
                func_result.reset_index(inplace=True, drop=False)

            # Add by columns.
            for i, value in enumerate(group_label):
                func_result.insert(i, f"group_label_{i}", value, allow_duplicates=True)
            # Add row position columns.
            func_result.insert(0, "__row_position__", row_positions)
            func_result.insert(0, "__min_row_position__", min_row_position)
            return func_result

    try:
        persist_info = kwargs.pop(
            PERSIST_KWARG_NAME, None
        )  # Pop persistence params from kwargs before pickling.
        pickled = pickle_function(ApplyFunc.end_partition)
        cache_key = UDTFCacheKey(
            pickled,
            tuple(output_schema.column_types),
            tuple(output_schema.column_ids),
            tuple(input_column_types),
            tuple(session.get_packages().values()),
            process_persist_params(func, pickled, persist_info),
        )
        cache = session_groupby_apply_no_pivot_udtf_cache[session]
        if cache_key not in cache:
            cache[cache_key] = sp_func.udtf(
                ApplyFunc,
                **cache_key_to_init_params(cache_key),
                output_schema=PandasDataFrameType(
                    output_schema.column_types, output_schema.column_ids
                ),
                input_types=[PandasDataFrameType(col_types=input_column_types)],
                # We have to specify the local pandas package so that the UDF's pandas
                # behavior is consistent with client-side pandas behavior.
                packages=[native_pd] + list(session.get_packages().values()),
                session=session,
                statement_params=get_default_snowpark_pandas_statement_params(),
            )
        return cache[cache_key]
    except NotImplementedError:  # pragma: no cover
        # When a Snowpark object is passed to a UDF, a NotImplementedError with message
        # 'Snowpark pandas does not yet support the method DataFrame.__reduce__' is raised. Instead,
        # catch this exception and return a more user-friendly error message.
        raise ValueError(APPLY_WITH_SNOWPARK_OBJECT_ERROR_MSG)


def infer_output_schema_for_apply(
    func: Callable,
    args: tuple,
    kwargs: dict,
    by_labels: list[Hashable],
    data_column_index: native_pd.Index,
    index_column_names: list,
    input_types: list[DataType],
    series_groupby: bool,
    force_list_like_to_series: bool = False,
) -> GroupbyApplyOutputSchema | None:
    """
    Infers the output schema of a groupby apply function. Returns None if the schema
    cannot be inferred.

    Args:
        func: The function we need to apply to each group
        args: Function's positional arguments
        kwargs: Function's keyword arguments
        by_labels: List of labels for 'by' columns.
        data_column_index: Column labels for the input dataframe
        index_column_names: Names of the input dataframe's index
        input_types: Types of the input dataframe's columns
        series_groupby: Whether we are performing a SeriesGroupBy.apply() instead of DataFrameGroupBy.apply()
        force_list_like_to_series: Force the function result to series if it is list-like.

    Returns:
        GroupbyApplyOutputSchema or None.
    """
    size = 10

    def get_dummy_data(typ: DataType) -> Any:
        if isinstance(typ, _IntegralType):
            return np.arange(size)
        if isinstance(typ, _FractionalType):
            return np.arange(size) + 0.5
        if isinstance(typ, StringType):
            return [f"{chr(97 + i)}" for i in range(size)]
        if isinstance(typ, BooleanType):
            return [True, False] * int(size / 2)
        if isinstance(typ, TimestampType):
            return native_pd.date_range(start="2025-01-01 03:04:05", periods=size)
        if isinstance(typ, BinaryType):
            return [bytes("snow", "utf-8")] * size

    input_data = {}
    for i, typ in enumerate(input_types):
        dummy_data = get_dummy_data(typ)
        if dummy_data is None:
            # unknown type, fail the schema inference
            return None
        input_data[f"ARG{i+1}"] = dummy_data

    input_df = native_pd.DataFrame(input_data)
    num_by = len(by_labels)
    try:
        func_result, _, func_type, _ = apply_groupby_func_to_df(
            input_df,
            num_by,
            index_column_names,
            series_groupby,
            data_column_index,
            func,
            args,
            kwargs,
            force_list_like_to_series,
        )
    except Exception as e:
        _logger.info(f"Failed to infer schema with error={e}")
        return None
    schema = GroupbyApplyOutputSchema()
    schema.num_index_columns = num_by
    if func_type != GroupbyApplyFuncType.AGGREGATE:
        schema.num_index_columns += func_result.index.nlevels
    schema.func_type = func_type
    schema.column_index_names = func_result.columns.names

    # First two columns are always row position columns.
    schema.add_column("__min_row_position__", LongType())
    schema.add_column("__row_position__", LongType())

    # Always include the by columns in the output.
    for i, by_label in enumerate(by_labels):
        schema.add_column(by_label, input_types[1 + i])

    # Add index columns to the schema.
    if func_type != GroupbyApplyFuncType.AGGREGATE:
        for i, label in enumerate(func_result.index.names):
            schema.add_column(
                label,
                input_types[1 + num_by + i]
                if func_type == GroupbyApplyFuncType.TRANSFORM
                else VariantType(),
            )

    # Add data columns to the schema.
    for label in func_result.columns:
        schema.add_column(label, VariantType())
    return schema


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
    by_labels: list[Hashable],
    by_types: list[DataType],
    existing_identifiers: list[str],
    force_list_like_to_series: bool,
    is_transform: bool,
    force_single_group: bool,
) -> tuple[GroupbyApplyOutputSchema | None, UserDefinedTableFunction]:
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
    by_labels: The pandas lables of the by columns.
    by_types: The snowflake types of the by columns.
    existing_identifiers: List of existing column identifiers; these are omitted when creating new column identifiers.
    force_list_like_to_series: Force the function result to series if it is list-like
    is_transform: Whether the function is a transform or not.
    force_single_group: Force single group (empty set of group by labels) useful for DataFrame.apply() with axis=0

    Returns
    -------
    A Tuple of:
        1. Groupby apply output schema. This will be None if the schema cannot be
           inferred.
        2. A UDTF that will apply the provided function to a group and return a
           dataframe representing all the data and metadata of the result.
    """

    assert len(index_column_names) == len(
        input_index_column_types
    ), "list of index column names and list of index column types must have the same length"
    assert len(data_column_index) == len(
        input_data_column_types
    ), "list of data column names and list of data column types must have the same length"

    # Get the length of this list outside the vUDTF function because the vUDTF
    # doesn't have access to the Snowpark module, which defines these types.
    num_by = len(by_types)

    from snowflake.snowpark.modin.plugin.extensions.utils import (
        try_convert_index_to_native,
    )

    data_column_index = try_convert_index_to_native(data_column_index)

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
    output_schema = None
    if is_transform:
        # For transform, the UDTF will return same number of columns as input.
        output_schema = GroupbyApplyOutputSchema()
        output_schema.column_labels = [
            "__min_row_position__",
            "__row_position__",
            *by_labels,
            *index_column_names,
        ] + [col for col in data_column_index if col not in by_labels]
        column_types = [LongType(), LongType()] + by_types + input_index_column_types
        num_data_cols = len(output_schema.column_labels) - len(column_types)
        output_schema.column_types = column_types + ([VariantType()] * num_data_cols)
        output_schema.num_index_columns = len(index_column_names) + num_by
        output_schema.func_type = GroupbyApplyFuncType.TRANSFORM
        output_schema.column_index_names = data_column_index.names
    elif force_single_group:
        # TODO: SNOW-1801328: Enable schema inference for DataFrame.apply and  avoid
        #  pivot step.
        output_schema = None
    else:
        # Attempt to infer output schema for UDTF
        output_schema = infer_output_schema_for_apply(
            func,
            args,
            kwargs,
            by_labels,
            data_column_index,
            index_column_names,
            input_types,
            series_groupby,
        )

    if output_schema is not None:
        # Generate new column identifiers for all required UDTF columns with the helper
        # below to prevent collisions in column identifiers.
        output_schema.column_ids = generate_snowflake_quoted_identifiers_helper(
            pandas_labels=output_schema.column_labels,
            excluded=existing_identifiers,
            wrap_double_underscore=False,
        )
        return output_schema, create_udtf_for_groupby_no_pivot(
            func,
            args,
            kwargs,
            data_column_index,
            index_column_names,
            input_types,
            session,
            series_groupby,
            by_labels,
            output_schema,
            force_list_like_to_series,
        )

    # Failed to infer schema, fallback to old implementation
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
            func_result, row_positions, func_type, _ = apply_groupby_func_to_df(
                df,
                num_by,
                index_column_names,
                series_groupby,
                data_column_index,
                func,
                args,
                kwargs,
                force_list_like_to_series,
            )
            return convert_groupby_apply_dataframe_result_to_standard_schema(
                func_result, row_positions, func_type
            )

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
    col_types = [
        StringType(),
        IntegerType(),
        VariantType(),
        IntegerType(),
        IntegerType(),
    ]
    try:
        persist_info = kwargs.pop(
            PERSIST_KWARG_NAME, None
        )  # Pop persistence params from kwargs before pickling.
        pickled = pickle_function(ApplyFunc.end_partition)
        cache_key = UDTFCacheKey(
            pickled,
            tuple(col_types),
            tuple(col_names),
            tuple(input_types),
            tuple(session.get_packages().values()),
            process_persist_params(func, pickled, persist_info),
        )
        cache = session_groupby_apply_udtf_cache[session]
        if cache_key not in cache:
            cache[cache_key] = sp_func.udtf(
                ApplyFunc,
                **cache_key_to_init_params(cache_key),
                output_schema=PandasDataFrameType(
                    col_types,
                    col_names,
                ),
                input_types=[PandasDataFrameType(col_types=input_types)],
                # We have to specify the local pandas package so that the UDF's pandas
                # behavior is consistent with client-side pandas behavior.
                packages=[native_pd] + list(session.get_packages().values()),
                session=session,
                statement_params=get_default_snowpark_pandas_statement_params(),
            )
        return None, cache[cache_key]
    except NotImplementedError:  # pragma: no cover
        # When a Snowpark object is passed to a UDF, a NotImplementedError with message
        # 'Snowpark pandas does not yet support the method DataFrame.__reduce__' is raised. Instead,
        # catch this exception and return a more user-friendly error message.
        raise ValueError(APPLY_WITH_SNOWPARK_OBJECT_ERROR_MSG)


def create_udf_for_series_apply(
    func: Callable | UserDefinedFunction,
    return_type: DataType,
    input_type: DataType,
    na_action: Literal["ignore"] | None,
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
        assert (
            func._return_type == return_type
        ), f"UserDefinedFunction has invalid return type {func.return_type} vs. {return_type}"

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
            # TODO SNOW-1874779: Add verification here to ensure inferred type matches
            #  actual type.
            return x.apply(func, args=args, **kwargs)

    strict = na_action == "ignore"
    try:
        persist_info = kwargs.pop(
            PERSIST_KWARG_NAME, None
        )  # Pop persistence params from kwargs before pickling.
        pickled = pickle_function(apply_func)
        cache_key = UDFCacheKey(
            pickled,
            return_type,
            input_type,
            strict,
            process_persist_params(func, pickled, persist_info),
        )
        cache = session_udf_cache[session]
        if cache_key not in cache:
            cache[cache_key] = sp_func.udf(
                apply_func,
                **cache_key_to_init_params(cache_key),
                return_type=PandasSeriesType(return_type),
                input_types=[PandasSeriesType(input_type)],
                strict=strict,
                session=session,
                packages=packages,
                statement_params=get_default_snowpark_pandas_statement_params(),
            )
        return cache[cache_key]
    except NotImplementedError:  # pragma: no cover
        # When a Snowpark object is passed to a UDF, a NotImplementedError with message
        # 'Snowpark pandas does not yet support the method DataFrame.__reduce__' is raised. Instead,
        # catch this exception and return a more user-friendly error message.
        raise ValueError(APPLY_WITH_SNOWPARK_OBJECT_ERROR_MSG)


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
    If the result is a numpy int (or bool), convert it to a python int (or bool.)

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
    int(value) if the value is a numpy int,
    bool(value) if the value is a numpy bool, otherwise the value.
    """
    return (
        int(value)
        if np.issubdtype(type(value), np.integer)
        else (bool(value) if np.issubdtype(type(value), np.bool_) else value)
    )


DUMMY_BOOL_INPUT = native_pd.Series([False, True])
# Note: we use only small dummy values here to avoid the risk of certain callables
# taking a long time to execute (where execution time is a function of the input value).
# As a downside this reduces diversity in input data so will reduce the effectiveness
# type inference framework in some rare cases.
DUMMY_INT_INPUT = native_pd.Series([-37, -9, -2, -1, 0, 2, 3, 5, 7, 9, 13, 16, 20, 101])
DUMMY_FLOAT_INPUT = native_pd.Series(
    [-9.9, -2.2, -1.0, 0.0, 0.5, 0.33, None, 0.99, 2.0, 3.0, 5.0, 7.7, 9.898989, 100.1]
)
DUMMY_STRING_INPUT = native_pd.Series(
    ["", "a", "A", "0", "1", "01", "123", "-1", "-12", "true", "True", "false", "False"]
    + [None, "null", "Jane Smith", "janesmith@snowflake.com", "janesmith@gmail.com"]
    + ["650-592-4563", "Jane Smith, 123 Main St., Anytown, CA 12345"]
    + ["2020-12-23", "2020-12-23 12:34:56", "08/08/2024", "07-08-2022", "12:34:56"]
    + ["ABC", "bat-man", "super_man", "1@#$%^&*()_+", "<>?:{}|[]\\;'/.,", "<tag>"]
)
DUMMY_BINARY_INPUT = native_pd.Series(
    [bytes("snow", "utf-8"), bytes("flake", "utf-8"), bytes("12", "utf-8"), None]
)
DUMMY_TIMESTAMP_INPUT = native_pd.to_datetime(
    ["2020-12-31 00:00:00", "2020-01-01 00:00:00", native_pd.Timestamp.min]  # past
    + ["2090-01-01 00:00:00", "2090-12-31 00:00:00", native_pd.Timestamp.max]  # future
    + [datetime.today(), None],  # current
    format="mixed",
)


def infer_return_type_using_dummy_data(
    func: Callable, input_type: DataType, **kwargs: Any
) -> DataType | None:
    """
    Infer the return type of given function by applying it to a dummy input.
    This method only supports the following input types: _IntegralType, _FractionalType,
     StringType, BooleanType, TimestampType, BinaryType.
    Args:
        func: The function to infer the return type from.
        input_type: The input type of the function.
        **kwargs : Additional keyword arguments to pass as keywords arguments to func.
    Returns:
        The inferred return type of the function. If the return type cannot be inferred,
         return None.
    """
    if input_type is None:
        return None
    input_data = None
    if isinstance(input_type, _IntegralType):
        input_data = DUMMY_INT_INPUT
    elif isinstance(input_type, _FractionalType):
        input_data = DUMMY_FLOAT_INPUT
    elif isinstance(input_type, StringType):
        input_data = DUMMY_STRING_INPUT
    elif isinstance(input_type, BooleanType):
        input_data = DUMMY_BOOL_INPUT
    elif isinstance(input_type, TimestampType):
        input_data = DUMMY_TIMESTAMP_INPUT
    elif isinstance(input_type, BinaryType):
        input_data = DUMMY_BINARY_INPUT
    else:
        return None

    def merge_types(t1: DataType, t2: DataType) -> DataType:
        """
        Merge two types into one as per the following rules:
        - Null + T = T
        - T + Null = T
        - T1 + T2 = T1 where T1 == T2
        - T1 + T2 = Variant where T1 != T2
        Args:
            t1: first type to merge.
            t2: second type to merge.

        Returns:
            Merged type of t1 and t2.
        """
        # treat NullType as None
        t1 = None if t1 == NullType() else t1
        t2 = None if t2 == NullType() else t2

        if t1 is None:
            return t2
        if t2 is None:
            return t1
        if t1 == t2:
            return t1
        if isinstance(t1, MapType) and isinstance(t2, MapType):
            return MapType(
                merge_types(t1.key_type, t2.key_type),
                merge_types(t1.value_type, t2.value_type),
            )
        if isinstance(t1, ArrayType) and isinstance(t2, ArrayType):
            return ArrayType(merge_types(t1.element_type, t2.element_type))
        return VariantType()

    inferred_type = None
    for x in input_data:
        try:
            inferred_type = merge_types(
                inferred_type, infer_object_type(func(x, **kwargs))
            )
        except Exception:
            pass

    if isinstance(inferred_type, TimedeltaType):
        # TODO: SNOW-1619940: pd.Timedelta is encoded as string.
        return StringType()
    return inferred_type


def deduce_return_type_from_function(
    func: AggFuncType | UserDefinedFunction,
    input_type: DataType | None,
    **kwargs: Any,
) -> DataType | None:
    """
    Deduce return type if possible from a function, list, dict or type object. List will be mapped to ArrayType(),
    dict to MapType(), and if a type object (e.g., str) is given a mapping will be consulted.
    Args:
        func: callable function, object or Snowpark UserDefinedFunction that can be passed in pandas to reference a function.
        input_type: input data type this function is applied to.
        **kwargs : Additional keyword arguments to pass as keywords arguments to func.

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
        # If it is not the object type, return None to indicate that type hint could not
        # be extracted successfully.
        try:
            return_type = get_types_from_type_hints(func, TempObjectType.FUNCTION)[0]
            if return_type is not None:
                return return_type
        except TypeError as te:
            if str(te) == "invalid type <class 'object'>":
                return VariantType()
        # infer return type using dummy data.
        return infer_return_type_using_dummy_data(func, input_type, **kwargs)


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
                 # this represents a data column named ('a', 'group_key') at position 0
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
                sp_func.col(original_row_position_snowflake_quoted_identifier).as_(
                    new_index_identifier
                )
                if sort_method is GroupbyApplySortMethod.ORIGINAL_ROW_ORDER
                else (
                    sp_func.dense_rank().over(
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
                sp_func.col(old_quoted_identifier).as_(quoted_identifier)
                if return_variant
                else sp_func.col(old_quoted_identifier)
                .cast(return_type)
                .as_(quoted_identifier)
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
        data_column_types=None,
        index_column_types=None,
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
        sp_func.builtin("boolor_agg")(
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


def is_supported_snowpark_python_function(func: AggFuncType) -> bool:
    """Return True if the `func` is a supported Snowpark Python function."""
    func_module = inspect.getmodule(func)
    if functions != func_module:
        return False
    if func not in SUPPORTED_SNOWPARK_PYTHON_FUNCTIONS_IN_APPLY:
        ErrorMessage.not_implemented(
            f"Snowpark Python function `{func.__name__}` is not supported yet."
        )
    return True


def make_series_map_snowpark_function(
    mapping: Mapping | native_pd.Series, self_type: DataType
) -> Callable[[SnowparkColumn], SnowparkColumn]:
    """
    Make a snowpark function that implements Series.map() with a dict mapping.

    Args:
        mapping: The mapping. If this is a defaultdict, default_value must not
                 be None.
        self_type: The Snowpark type of this series.

    Returns:
        A function mapping the existing snowpark column to a snowpark column
        that reflects the mapping.
    """

    if (
        isinstance(mapping, dict)
        and not isinstance(mapping, defaultdict)
        and hasattr(mapping, "__missing__")
    ):
        # TODO(SNOW-1804017): Implement this case.
        raise NotImplementedError(
            "Cannot handle objects other than defaultdict with __missing__"
        )

    if isinstance(mapping, defaultdict):
        # We should exclude this case earlier because applying built-in
        # Snowpark functions cannot emulate raising a KeyError for values that
        # are not in the mapping.
        assert mapping.default_factory is not None

    def do_map(col: SnowparkColumn) -> SnowparkColumn:
        if len(mapping) == 0:
            # An empty dict-like is an edge case that should not produce a
            # conditional expression. Instead, we replace the column with the
            # default value, which is mapping.default_factory() for
            # defaultdict and None for other mappings.
            return (
                pandas_lit(mapping.default_factory())  # type: ignore
                if isinstance(mapping, defaultdict)
                else pandas_lit(None)
            )

        def make_condition(key: Any) -> SnowparkColumn:
            # Cast one of the values in the comparison to variant so that we
            # we can compare types that are otherwise not comparable in
            # Snowflake, like timestamp and int.
            return col.equal_null(sp_func.to_variant(pandas_lit(key)))

        # If any of the values we are mapping to have types that are
        # incompatible with the current column's type, we have to cast the new
        # values to variant type. For example, this could happen if we mapped a
        # column of strings with the mapping {"cat": 1}. Some of the result
        # values could have value 1. Note that we have to cast all the result
        # values to variant even if only one of them is of an incompatible
        # type, because otherwise Snowflake might coerce the values to an
        # unexpected type.
        should_cast_result_to_variant = any(
            not is_compatible_snowpark_types(infer_object_type(v), self_type)
            for _, v in mapping.items()
        ) or (
            isinstance(mapping, defaultdict)
            and not is_compatible_snowpark_types(
                infer_object_type(mapping.default_factory()), self_type  # type: ignore
            )
        )

        def make_result(value: Any) -> SnowparkColumn:
            value_expression = pandas_lit(value)
            return (
                sp_func.to_variant(value_expression)
                if should_cast_result_to_variant
                else value_expression
            )

        map_items = mapping.items()
        first_key, first_value = next(iter(map_items))
        case_expression = functools.reduce(
            lambda case_expression, key_and_value: case_expression.when(
                make_condition(key_and_value[0]), make_result(key_and_value[1])
            ),
            itertools.islice(map_items, 1, None),
            sp_func.when(make_condition(first_key), make_result(first_value)),
        )
        if isinstance(mapping, defaultdict):
            case_expression = case_expression.otherwise(
                make_result(pandas_lit(mapping.default_factory()))  # type: ignore
            )
        return case_expression

    return do_map


def create_internal_frame_for_groupby_apply_no_pivot_result(
    input_frame: InternalFrame,
    ordered_dataframe: OrderedDataFrame,
    output_schema: GroupbyApplyOutputSchema,
    num_by: int,
    is_transform: bool,
    group_keys: bool,
    as_index: bool,
    sort: bool,
) -> InternalFrame:
    """
    Create InternalFrame for groupy apply no pivot output.
    Args:
        input_frame: Input internal frame.
        ordered_dataframe: Output ordered dataframe from groupby apply udtf.
        output_schema: Inferred output schema for groupby apply.
        num_by: Number of by columns.
        is_transform: Whether this is groupy.transform or not.
        group_keys: The `group_keys` argument to groupby()
        as_index: The `as_index` argument to groupby()
        sort: The `sort` argument to groupby()

    Returns:
        Final InternalFrame for groupby apply no pivot output.
    """
    output_cols = list(zip(output_schema.column_labels, output_schema.column_ids))
    num_index_cols = output_schema.num_index_columns
    # Output ordered dataframe has the following columns in order.
    # 1. min row position. This is used to find order of group when sort=False.
    min_row_position_id = output_cols[0][1]
    # 2. Original row position or row position within group.
    row_position_id = output_cols[1][1]
    # 3. index columns (will always include by columns)
    index_cols = output_cols[2 : 2 + num_index_cols]
    by_ids = [output_col[1] for output_col in output_cols[2 : 2 + num_by]]
    # 4. data columns
    data_cols = output_cols[2 + num_index_cols :]

    # df.groupby(group_keys=False).apply(transform_func) is equivalent to
    # df.groupby().transform(transform_func)
    if output_schema.func_type == GroupbyApplyFuncType.TRANSFORM and not group_keys:
        is_transform = True

    # Note: group_keys is ignored for aggregate. Set to default value.
    if output_schema.func_type == GroupbyApplyFuncType.AGGREGATE:
        group_keys = True

    # Note: 'sort' and 'as_index' arguments are ignored for transform.
    # https://pandas.pydata.org/docs/user_guide/groupby.html#transformation
    if is_transform:
        as_index = True
        sort = True

    new_index_order_ids = by_ids if sort else [min_row_position_id]
    if not as_index:
        new_index_id = ordered_dataframe.generate_snowflake_quoted_identifiers(
            pandas_labels=[INDEX_LABEL]
        )[0]
        new_index_col = (
            sp_func.dense_rank().over(
                Window.order_by(
                    *(
                        SnowparkColumn(col_id).asc_nulls_last()
                        for col_id in new_index_order_ids
                    )
                )
            )
            - 1
        )
        ordered_dataframe = append_columns(
            ordered_dataframe, new_index_id, new_index_col
        )
        # For aggregation if as_index is False 'by' columns should appear
        # in data columns (excluding 'by' index columns).
        # https://pandas.pydata.org/pandas-docs/stable/user_guide/groupby.html#aggregation
        if output_schema.func_type == GroupbyApplyFuncType.AGGREGATE:
            cols_to_move = []
            for index_col in index_cols[:num_by]:
                if index_col[0] in input_frame.data_column_pandas_labels:
                    cols_to_move.append(index_col)
            data_cols = cols_to_move + data_cols
            index_cols = [(None, new_index_id)]
            data_cols = [
                (None if label == MODIN_UNNAMED_SERIES_LABEL else label, sfid)
                for label, sfid in data_cols
            ]
        else:
            index_cols = [(None, new_index_id)] + index_cols[num_by:]

    if not group_keys and len(index_cols) > num_by:
        index_cols = index_cols[num_by:]

    sort_ids = (
        [row_position_id] if is_transform else new_index_order_ids + [row_position_id]
    )
    ordered_dataframe = ordered_dataframe.sort(*(OrderingColumn(id) for id in sort_ids))

    return InternalFrame.create(
        ordered_dataframe=ordered_dataframe,
        data_column_pandas_labels=[c[0] for c in data_cols],
        data_column_pandas_index_names=output_schema.column_index_names,
        data_column_snowflake_quoted_identifiers=[c[1] for c in data_cols],
        index_column_pandas_labels=[c[0] for c in index_cols],
        index_column_snowflake_quoted_identifiers=[c[1] for c in index_cols],
        data_column_types=None,
        index_column_types=None,
    )
